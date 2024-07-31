mod array;
mod cast;
#[cfg(feature = "json")]
pub mod json;
mod utils;

pub use array::VariantArray;
use arrow_schema::{DataType, Field, Fields};
pub use cast::cast_to_variant;

pub const VARIANT_METADATA_FIELD: &str = "metadata";
pub const VARIANT_VALUES_FIELD: &str = "values";

pub fn variant_metadata_type() -> DataType {
    // TODO: can we be flexible about this type?
    // TODO: should we use REE for this?
    DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Binary))
}

pub fn variant_values_type() -> DataType {
    // TODO: BinaryView?
    DataType::Binary
}

fn variant_fields() -> Fields {
    vec![
        Field::new(VARIANT_METADATA_FIELD, variant_metadata_type(), false),
        Field::new(VARIANT_VALUES_FIELD, variant_values_type(), true),
    ]
    .into()
}

pub fn variant_type() -> DataType {
    DataType::Struct(variant_fields())
}
