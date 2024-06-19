mod read;
pub mod write;

pub use read::VariantRef;

/// Basic type of a variant value.
///
/// For [`BasicType::Primitive`], a more specific type is given by [`PrimitiveTypeId`].
#[repr(u8)]
#[derive(Debug, PartialEq)]
pub enum BasicType {
    Primitive = 0,
    ShortString = 1,
    Object = 2,
    Array = 3,
}

impl TryFrom<u8> for BasicType {
    type Error = ();

    /// Convert from u8 to [`BasicType`]. Will return an error if the value is not a valid [`BasicType`].
    fn try_from(value: u8) -> Result<Self, ()> {
        match value {
            0 => Ok(BasicType::Primitive),
            1 => Ok(BasicType::ShortString),
            2 => Ok(BasicType::Object),
            3 => Ok(BasicType::Array),
            _ => Err(()),
        }
    }
}

/// Specific type of a primitive variant value.
#[repr(u8)]
#[derive(Debug, PartialEq)]
#[non_exhaustive]
pub enum PrimitiveTypeId {
    Null = 0,
    BoolTrue = 1,
    BoolFalse = 2,
    Int8 = 3,
    Int16 = 4,
    Int32 = 5,
    Int64 = 6,
    Float32 = 14,
    Float64 = 7,
    Decimal4 = 8,   // 32-bit
    Decimal8 = 9,   // 64-bit
    Decimal16 = 10, // 128-bit
    Date32 = 11,
    TimestampMicro = 12,    // (with timezone)
    TimestampMicroNTZ = 13, // (without timezone)
    // 14 is Float32
    Binary = 15,
    String = 16,
    BinaryFromDictionary = 17,
    StringFromDictionary = 18,
}

impl TryFrom<u8> for PrimitiveTypeId {
    type Error = ();

    /// Convert from i8 to [`PrimitiveTypeId`]. Will return an error if the value is not a valid [`PrimitiveTypeId`].
    fn try_from(value: u8) -> Result<Self, ()> {
        match value {
            0 => Ok(PrimitiveTypeId::Null),
            1 => Ok(PrimitiveTypeId::BoolTrue),
            2 => Ok(PrimitiveTypeId::BoolFalse),
            3 => Ok(PrimitiveTypeId::Int8),
            4 => Ok(PrimitiveTypeId::Int16),
            5 => Ok(PrimitiveTypeId::Int32),
            6 => Ok(PrimitiveTypeId::Int64),
            7 => Ok(PrimitiveTypeId::Float64),
            8 => Ok(PrimitiveTypeId::Decimal4),
            9 => Ok(PrimitiveTypeId::Decimal8),
            10 => Ok(PrimitiveTypeId::Decimal16),
            11 => Ok(PrimitiveTypeId::Date32),
            12 => Ok(PrimitiveTypeId::TimestampMicro),
            13 => Ok(PrimitiveTypeId::TimestampMicroNTZ),
            14 => Ok(PrimitiveTypeId::Float32),
            15 => Ok(PrimitiveTypeId::Binary),
            16 => Ok(PrimitiveTypeId::String),
            17 => Ok(PrimitiveTypeId::BinaryFromDictionary),
            18 => Ok(PrimitiveTypeId::StringFromDictionary),
            _ => Err(()),
        }
    }
}
