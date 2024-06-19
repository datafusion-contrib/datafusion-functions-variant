use crate::{metadata::MetadataRef, utils::push_offset};

use super::{BasicType, PrimitiveTypeId};

fn primitive_header(primitive_type_id: PrimitiveTypeId) -> u8 {
    // 7                                  2 1          0
    // +------------------------------------+------------+
    // |            value_header            | basic_type |
    // +------------------------------------+------------+
    let basic_type = 0;
    basic_type | (primitive_type_id as u8) << 2
}

pub fn write_bool(buffer: &mut Vec<u8>, value: bool) {
    // Booleans are just headers
    let header = match value {
        true => primitive_header(PrimitiveTypeId::BoolTrue),
        false => primitive_header(PrimitiveTypeId::BoolFalse),
    };
    buffer.push(header);
}

// TODO: Make generic and support others.
pub fn write_i64(buffer: &mut Vec<u8>, value: i64) {
    let header = primitive_header(PrimitiveTypeId::Int64);
    buffer.push(header);
    buffer.extend_from_slice(&value.to_le_bytes());
}

pub fn write_f64(buffer: &mut Vec<u8>, value: f64) {
    let header = primitive_header(PrimitiveTypeId::Float64);
    buffer.push(header);
    buffer.extend_from_slice(&value.to_le_bytes());
}

pub fn write_decimal(buffer: &mut Vec<u8>, value: i128, scale: u8) {
    if scale > 38 {
        panic!("Decimal scale must be between 0 and 38.");
    }
    if value < i32::MAX as i128 {
        buffer.push(primitive_header(PrimitiveTypeId::Decimal4));
        buffer.push(scale.to_le());
        buffer.extend_from_slice(&(value as i32).to_le_bytes());
    } else if value < i64::MAX as i128 {
        buffer.push(primitive_header(PrimitiveTypeId::Decimal8));
        buffer.push(scale.to_le());
        buffer.extend_from_slice(&(value as i64).to_le_bytes());
    } else {
        buffer.push(primitive_header(PrimitiveTypeId::Decimal16));
        buffer.push(scale.to_le());
        buffer.extend_from_slice(&value.to_le_bytes());
    };
}

pub fn write_string(buffer: &mut Vec<u8>, value: &str) {
    let header = primitive_header(PrimitiveTypeId::String);
    buffer.push(header);
    buffer.extend_from_slice(&(value.len() as i32).to_le_bytes());
    buffer.extend_from_slice(value.as_bytes());
}

// See: https://github.com/apache/spark/tree/master/common/variant#value-data-for-array-basic_type3
pub struct ArrayBuilder<'a> {
    buffer: &'a mut Vec<u8>,
    field_offset_width: u8,
    // This is used to hold the value data as we collect. Once finished, it will
    // be appended to the buffer.
    tmp_buffer: Vec<u8>,
}

// See: https://github.com/apache/spark/tree/master/common/variant#value-data-for-object-basic_type2
impl<'a> ArrayBuilder<'a> {
    pub fn new(buffer: &'a mut Vec<u8>, num_elements: usize) -> Self {
        let field_offset_width = crate::utils::get_offset_size(num_elements);
        let is_large = if num_elements > i8::MAX as usize {
            1
        } else {
            0
        };
        let num_elements_width = if is_large == 1 { 4 } else { 1 };

        let mut capacity_needed = 1 + num_elements_width; // header plus num_elements
        capacity_needed += field_offset_width as usize * (num_elements + 1); // offsets
        capacity_needed += num_elements; // for value headers
        buffer.reserve(capacity_needed);

        // Array header layout
        //  5         3  2  1     0
        // +-----------+---+-------+
        // |           |   |       |
        // +-----------+---+-------+
        //               ^     ^
        //               |     +-- field_offset_size_minus_one
        //               +-- is_large
        let header = is_large << 2 | (field_offset_width - 1);
        let header = header << 2 | BasicType::Array as u8;
        buffer.push(header);

        push_offset(buffer, num_elements, num_elements_width as u8);
        // Offsets always start at 0.
        push_offset(buffer, 0, field_offset_width);
        Self {
            buffer,
            field_offset_width,
            tmp_buffer: Vec::new(),
        }
    }

    pub fn append_value(&mut self, value: &[u8]) {
        self.tmp_buffer.extend_from_slice(value);
        let size = self.tmp_buffer.len();
        push_offset(self.buffer, size, self.field_offset_width);
    }

    pub fn finish(self) {
        // Append the collected data.
        self.buffer.extend_from_slice(&self.tmp_buffer);
    }
}

/// TODO: how can we make the builders re-useable?
pub struct ObjectBuilder<'a> {
    buffer: &'a mut Vec<u8>,
    // Offset into buffer where the header is. This is used to update the width
    // of the field offset values.
    header_offset: usize,
    // Pairs of field id and offset. (The final offset is managed separately.)
    field_id_and_offsets: Vec<(usize, usize)>,
    // This is used to hold the value data as we collect. Once finished, it will
    // be appended to the buffer.
    tmp_buffer: Vec<u8>,
    metadata: &'a MetadataRef<'a>,
}

// We should pass down the object size
// Then we can pre-allocate for the field ids, offsets and value headers.
//
// The field ids and field offsets must be in lexicographical order of the
// corresponding field names in the metadata dictionary. We can assume the field
// ids themselves have already been sorted, and thus we just need to sort the
// field ids in numeric order.
impl<'a> ObjectBuilder<'a> {
    pub fn with_capacity(
        buffer: &'a mut Vec<u8>,
        metadata: &'a MetadataRef<'a>,
        num_elements: usize, // TODO: make this function like capacity, and make not required.
    ) -> Self {
        // Object Header
        //   5   4  3     2 1     0
        // +---+---+-------+-------+
        // |   |   |       |       |
        // +---+---+-------+-------+
        //       ^     ^       ^
        //       |     |       +-- field_offset_size_minus_one
        //       |     +-- field_id_size_minus_one
        //       +-- is_large
        let is_large = if num_elements > i8::MAX as usize {
            1 // Use 64-bit size
        } else {
            0 // Use 8-bit size
        };
        let num_elements_width = if is_large > 0 { 4 } else { 1 };
        let field_id_size = crate::utils::get_offset_size(num_elements);
        // We skip field offset until the end.
        let header = is_large << 4 | (field_id_size - 1) << 2;
        let header = header << 2 | BasicType::Object as u8;

        // TODO: this is all deferred so we might as well do a reservation in finish()
        // Reserve lower bound of space needed for object.
        let mut needed_capacity = 1 + num_elements_width; // for header and size
        needed_capacity += num_elements * field_id_size as usize; // for field ids
        needed_capacity += 1 + num_elements; // for field offsets (We don't know width, so we assume 1 byte for now.)
        needed_capacity += num_elements; // for value headers
        buffer.reserve(needed_capacity);

        let header_offset = buffer.len();
        buffer.push(header);

        // Append num elements
        push_offset(buffer, num_elements, num_elements_width as u8);

        Self {
            buffer,
            header_offset,
            field_id_and_offsets: Vec::with_capacity(num_elements),
            tmp_buffer: Vec::new(),
            metadata,
        }
    }

    fn append(
        &mut self,
        field_name: &str,
        appender: impl FnOnce(&mut Vec<u8>),
    ) -> Result<(), String> {
        let field_id = self.metadata.find_string(field_name).ok_or_else(|| {
            format!(
                "Key '{}' is not present in metadata dictionary.",
                field_name
            )
        })?;
        let offset = self.tmp_buffer.len();
        self.field_id_and_offsets.push((field_id, offset));
        appender(&mut self.tmp_buffer);
        Ok(())
    }

    pub fn append_value(&mut self, field_name: &str, value: &[u8]) -> Result<(), String> {
        self.append(field_name, |buffer| buffer.extend_from_slice(value))
    }

    pub fn append_string(&mut self, field_name: &str, value: &str) -> Result<(), String> {
        self.append(field_name, |buffer| write_string(buffer, value))
    }

    pub fn append_i64(&mut self, field_name: &str, value: i64) -> Result<(), String> {
        self.append(field_name, |buffer| write_i64(buffer, value))
    }

    pub fn append_f64(&mut self, field_name: &str, value: f64) -> Result<(), String> {
        self.append(field_name, |buffer| write_f64(buffer, value))
    }

    pub fn append_decimal(
        &mut self,
        field_name: &str,
        value: i128,
        scale: u8,
    ) -> Result<(), String> {
        self.append(field_name, |buffer| write_decimal(buffer, value, scale))
    }

    pub fn finish(mut self) {
        let final_offset = self.tmp_buffer.len();
        let offset_width = crate::utils::get_offset_size(final_offset);
        let max_field_id = self
            .field_id_and_offsets
            .iter()
            .map(|(field_id, _offset)| *field_id)
            .max()
            .unwrap_or_default();
        let field_id_width = crate::utils::get_offset_size(max_field_id);

        // Since it was unknown as the time, we did not set the offset width
        // in the header, so we do that now.
        let current_header = self.buffer[self.header_offset];
        self.buffer[self.header_offset] = current_header | (offset_width - 1) << 2;

        let mut needed_capacity = field_id_width as usize * self.field_id_and_offsets.len();
        needed_capacity += offset_width as usize * self.field_id_and_offsets.len();
        needed_capacity += self.buffer.len();
        self.buffer.reserve(needed_capacity);

        // Sort by field id.
        self.field_id_and_offsets
            .sort_unstable_by_key(|(field_id, _offset)| *field_id);

        for (field_id, _offset) in &self.field_id_and_offsets {
            push_offset(self.buffer, *field_id, field_id_width);
        }

        for (_field_id, offset) in self.field_id_and_offsets {
            push_offset(self.buffer, offset, offset_width);
        }
        push_offset(self.buffer, final_offset, offset_width);

        self.buffer.extend_from_slice(&self.tmp_buffer);
    }
}

#[cfg(test)]
mod tests {
    use crate::{metadata::build_metadata, variant::VariantRef};

    use super::*;

    #[test]
    fn test_write_bool() {
        let mut buffer = Vec::new();
        write_bool(&mut buffer, true);

        assert_eq!(buffer.len(), 1);

        let variant = VariantRef(&buffer);
        assert_eq!(variant.get_basic_type(), BasicType::Primitive);
        assert_eq!(variant.get_primitive_type_id(), PrimitiveTypeId::BoolTrue);

        buffer.clear();
        write_bool(&mut buffer, false);

        assert_eq!(buffer.len(), 1);

        let variant = VariantRef(&buffer);
        assert_eq!(variant.get_basic_type(), BasicType::Primitive);
        assert_eq!(variant.get_primitive_type_id(), PrimitiveTypeId::BoolFalse);
    }

    #[test]
    fn test_write_i64() {
        let mut buffer = Vec::new();

        for value in [0, -100, 100, i64::MAX, i64::MIN] {
            write_i64(&mut buffer, value);

            let variant = VariantRef(&buffer);
            assert_eq!(variant.get_basic_type(), BasicType::Primitive);
            assert_eq!(variant.get_primitive_type_id(), PrimitiveTypeId::Int64);

            let roundtripped = variant.get_i64();
            assert_eq!(value, roundtripped);

            buffer.clear();
        }
    }

    #[test]
    fn test_write_object() {
        let mut buffer = Vec::new();

        // We insert in non-lexographical order so we can test it gets ordered
        // correctly later.
        let metadata = build_metadata(["user_id", "date", "score"].into_iter());
        let metadata_ref = MetadataRef::new(&metadata);

        let mut object_builder = ObjectBuilder::with_capacity(&mut buffer, &metadata_ref, 3);
        let mut inner_buffer = Vec::new();

        write_i64(&mut inner_buffer, 42);
        object_builder
            .append_value("user_id", &inner_buffer)
            .unwrap();
        inner_buffer.clear();

        write_string(&mut inner_buffer, "2024-01-01");
        object_builder.append_value("date", &inner_buffer).unwrap();
        inner_buffer.clear();

        write_f64(&mut inner_buffer, 23.0);
        object_builder.append_value("score", &inner_buffer).unwrap();
        inner_buffer.clear();

        // Should error if we pass non-existent field name
        let res = object_builder.append_value("non-existent", &[]);
        assert!(matches!(res, Err(err) if err.contains("not present in metadata dictionary")));

        object_builder.finish();

        let variant = VariantRef(&buffer);

        let field_id = metadata_ref.find_string("user_id").unwrap();
        let user_id = variant.get_object_value(field_id).unwrap();
        assert_eq!(user_id.get_i64(), 42);

        let field_id = metadata_ref.find_string("date").unwrap();
        let date = variant.get_object_value(field_id).unwrap();
        assert_eq!(date.get_string(), "2024-01-01");

        let field_id = metadata_ref.find_string("score").unwrap();
        let score = variant.get_object_value(field_id).unwrap();
        assert_eq!(score.get_f64(), 23.0);

        assert!(variant.get_object_value(42).is_none());
    }

    #[test]
    fn test_write_array() {
        let mut buffer = Vec::new();

        let mut builder = ArrayBuilder::new(&mut buffer, 3);

        let mut tmp_buf = Vec::new();

        write_i64(&mut tmp_buf, 42);
        builder.append_value(&tmp_buf);
        tmp_buf.clear();

        write_f64(&mut tmp_buf, 32.0);
        builder.append_value(&tmp_buf);
        tmp_buf.clear();

        write_string(&mut tmp_buf, "hello world");
        builder.append_value(&tmp_buf);
        tmp_buf.clear();

        builder.finish();

        let variant = VariantRef(&buffer);
        assert!(matches!(variant.get_basic_type(), BasicType::Array));

        // TODO
        // let first = variant.get_array_element(0);
        // assert!(first.is_some());
    }
}
