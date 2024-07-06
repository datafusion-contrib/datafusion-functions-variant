// TODO: make this codebase not care about whether there is more data after
// the value.
// TODO: implement function to shrink to the slice where the value is, if people
// want that.

use super::{BasicType, PrimitiveTypeId};

/// A view into a variant data buffer.
#[derive(Clone)]
pub struct VariantRef<'a>(&'a [u8]);

// TODO: a nice debug implementation would be awesome. TBH could use debug_struct?

impl<'a> VariantRef<'a> {
    pub fn try_new(data: &'a [u8]) -> Result<Self, String> {
        if data.is_empty() {
            return Err("Empty buffer".into());
        }
        Ok(Self(data))
    }

    pub fn basic_type(&self) -> BasicType {
        let header = self.0[0];
        (header & 0b11).try_into().expect("Invalid BasicType")
    }

    pub fn primitive_type_id(&self) -> PrimitiveTypeId {
        let header = self.0[0];
        (header >> 2).try_into().expect("Invalid PrimitiveTypeId")
    }

    pub fn get_bool(&self) -> bool {
        match self.primitive_type_id() {
            PrimitiveTypeId::BoolTrue => true,
            PrimitiveTypeId::BoolFalse => false,
            _ => panic!("Not a boolean"),
        }
    }

    pub fn get_i64(&self) -> i64 {
        if !matches!(self.primitive_type_id(), PrimitiveTypeId::Int64) {
            panic!("Not an i64");
        }
        // debug_assert_eq!(self.0.len(), 9); // 1 byte header + 8 byte i64
        i64::from_le_bytes(self.0[1..9].try_into().unwrap())
    }

    pub fn get_i128(&self) -> i128 {
        if !matches!(self.primitive_type_id(), PrimitiveTypeId::Decimal16) {
            panic!("Not an i128");
        }
        // 1 byte header + 16 byte i128
        i128::from_le_bytes(self.0[1..17].try_into().unwrap())
    }

    pub fn get_f64(&self) -> f64 {
        if !matches!(self.primitive_type_id(), PrimitiveTypeId::Float64) {
            panic!("Not an f64");
        }
        // debug_assert_eq!(self.0.len(), 9); // 1 byte header + 8 byte f64
        f64::from_le_bytes(self.0[1..9].try_into().unwrap())
    }

    pub fn get_string<'b>(&'b self) -> &'a str {
        if !matches!(self.primitive_type_id(), PrimitiveTypeId::String) {
            panic!("Not a string");
        }
        let size = i32::from_le_bytes(self.0[1..5].try_into().unwrap()) as usize;
        let start = 5;
        let end = start + size;
        std::str::from_utf8(&self.0[start..end]).unwrap()
    }

    pub fn get_object<'b>(&'b self) -> Result<ObjectRef<'a>, String> {
        ObjectRef::try_new(self)
    }

    pub fn get_array<'b>(&'b self) -> Result<ArrayRef<'a>, String> {
        ArrayRef::try_new(self)
    }

    /// Get a field from an object or an element from an array.
    ///
    /// Returns None if the variant is not an object or an array.
    /// Returns an error if the field_id is out of bounds, or if the variant
    /// data is invalid.
    pub fn field<'b>(&'b self, field_id: usize) -> Result<Option<VariantRef<'a>>, String> {
        match self.basic_type() {
            BasicType::Object => Ok(self.get_object()?.get_field(field_id)),
            BasicType::Array => Ok(self.get_array()?.get_element(field_id)),
            _ => Ok(None),
        }
    }
}

/// A view into an object variant data buffer.
///
/// This has been validated that it is an object.
pub struct ObjectRef<'a> {
    len: usize,
    field_id_width: u8,
    offset_width: u8,
    field_ids: &'a [u8],
    offsets: &'a [u8],
    values: &'a [u8],
}

impl<'a> ObjectRef<'a> {
    /// Try to create a new ObjectRef from a VariantRef.
    ///
    /// Will return an error if the VariantRef is not an object. Also returns
    /// an error if the object is not valid.
    pub fn try_new(data: &VariantRef<'a>) -> Result<Self, String> {
        if !matches!(data.basic_type(), BasicType::Object) {
            return Err("Not an object".into());
        }
        let mut data = data.0;

        // Parse out the header
        let header = data[0] >> 2;
        let offset_width = (header & 0b11) + 1;
        let field_id_width = ((header >> 2) & 0b11) + 1;
        let is_large = (header >> 4) & 1;
        data = &data[1..];

        let len = if is_large == 1 {
            // i32 for number of elements
            let len = i32::from_le_bytes(data[..4].try_into().unwrap()) as usize;
            data = &data[4..];
            len
        } else {
            // i8 for number of elements
            let len = i8::from_le_bytes(data[..1].try_into().unwrap()) as usize;
            data = &data[1..];
            len
        };

        let field_id_len = len * field_id_width as usize;
        let field_ids = &data[..field_id_len];
        data = &data[field_id_len..];

        let offset_len = (len + 1) * offset_width as usize;
        let offsets = &data[..offset_len];
        data = &data[offset_len..];

        Ok(Self {
            len,
            field_id_width,
            offset_width,
            field_ids,
            offsets,
            values: data,
        })
    }

    pub fn get_field<'b>(&'b self, field_id: usize) -> Option<VariantRef<'a>> {
        // Fields are required to be sorted by field_id, so we can binary search
        let field_id = field_id as u64;
        let mut left = 0;
        let mut right = self.len as u64;
        while left < right {
            let mid = left + (right - left) / 2;
            let mid_field_id = self.get_field_id(mid as usize);
            match mid_field_id.cmp(&field_id) {
                std::cmp::Ordering::Equal => return Some(VariantRef(self.get_value(mid as usize))),
                std::cmp::Ordering::Less => left = mid + 1,
                std::cmp::Ordering::Greater => right = mid,
            }
        }
        None
    }

    fn get_value<'b>(&'b self, idx: usize) -> &'a [u8] {
        let start = self.get_offset(idx);

        // Offsets are NOT guaranteed to be monotonic. It's a substantial
        // computation to find the end of the value or the next offset,
        // so instead we provide the buffer starting at the variant.
        // let end = (0..(self.len + 1))
        //     .map(|i| self.get_offset(i))
        //     .filter(|offset| *offset > start)
        //     .min()
        //     .expect("No other offset found");
        let end = self.get_offset(self.len);
        &self.values[start..end]
    }

    fn get_field_id(&'a self, idx: usize) -> u64 {
        let start = idx * self.field_id_width as usize;
        let end = start + self.field_id_width as usize;
        match self.field_id_width {
            1 => u8::from_le_bytes(self.field_ids[start..end].try_into().unwrap()) as u64,
            2 => u16::from_le_bytes(self.field_ids[start..end].try_into().unwrap()) as u64,
            4 => u32::from_le_bytes(self.field_ids[start..end].try_into().unwrap()) as u64,
            8 => u64::from_le_bytes(self.field_ids[start..end].try_into().unwrap()),
            _ => unreachable!(),
        }
    }

    fn get_offset(&'a self, idx: usize) -> usize {
        let start = idx * self.offset_width as usize;
        let end = start + self.offset_width as usize;
        match self.offset_width {
            1 => u8::from_le_bytes(self.offsets[start..end].try_into().unwrap()) as usize,
            2 => u16::from_le_bytes(self.offsets[start..end].try_into().unwrap()) as usize,
            4 => u32::from_le_bytes(self.offsets[start..end].try_into().unwrap()) as usize,
            8 => u64::from_le_bytes(self.offsets[start..end].try_into().unwrap()) as usize,
            _ => unreachable!(),
        }
    }
}

/// A view into an array variant data buffer.
///
/// This has been validated that it is an array.
pub struct ArrayRef<'a> {
    len: usize,
    offset_width: u8,
    offsets: &'a [u8],
    values: &'a [u8],
}

impl<'a> ArrayRef<'a> {
    pub fn try_new(data: &VariantRef<'a>) -> Result<Self, String> {
        if !matches!(data.basic_type(), BasicType::Array) {
            return Err("Not an array".into());
        }
        let mut data = data.0;

        let header = data[0] >> 2;
        let is_large = header >> 2 & 1 == 1;
        let offset_width = (header & 0b11) + 1;

        data = &data[1..];

        let len = if is_large {
            // i32 for number of elements
            let len = i32::from_le_bytes(data[..4].try_into().unwrap()) as usize;
            data = &data[4..];
            len
        } else {
            // i8 for number of elements
            let len = i8::from_le_bytes(data[..1].try_into().unwrap()) as usize;
            data = &data[1..];
            len
        };

        let offset_len = (len + 1) * offset_width as usize;
        let offsets = &data[..offset_len];
        let values = &data[offset_len..];

        Ok(Self {
            len,
            offset_width,
            offsets,
            values,
        })
    }

    pub fn get_element<'b>(&'b self, index: usize) -> Option<VariantRef<'a>> {
        if index >= self.len {
            return None;
        }
        let start = self.get_offset(index);
        let end = self.get_offset(index + 1);
        Some(VariantRef(&self.values[start..end]))
    }

    fn get_offset(&self, idx: usize) -> usize {
        let start = idx * self.offset_width as usize;
        let end = start + self.offset_width as usize;
        match self.offset_width {
            1 => u8::from_le_bytes(self.offsets[start..end].try_into().unwrap()) as usize,
            2 => u16::from_le_bytes(self.offsets[start..end].try_into().unwrap()) as usize,
            4 => u32::from_le_bytes(self.offsets[start..end].try_into().unwrap()) as usize,
            8 => u64::from_le_bytes(self.offsets[start..end].try_into().unwrap()) as usize,
            _ => unreachable!(),
        }
    }
}
