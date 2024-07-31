//! Cast Arrow data types to Variant type.

use std::sync::Arc;

use arrow_array::{
    builder::BinaryBuilder, cast::AsArray, Array, ArrayRef, BinaryArray, BooleanArray, StructArray,
};
use arrow_cast::cast::CastOptions;
use arrow_schema::{ArrowError, DataType};
use open_variant::{metadata::build_metadata, values::write::serialize_bool};

use crate::{utils::make_repeated_dict_array, variant_fields};

pub fn cast_to_variant(array: &dyn Array, _options: &CastOptions) -> Result<ArrayRef, ArrowError> {
    match array.data_type() {
        DataType::Boolean => cast_to_variant_bool(array.as_boolean()),
        _ => Err(ArrowError::NotYetImplemented(format!(
            "Casting {:?} to Variant",
            array.data_type()
        ))),
    }
}

fn cast_to_variant_bool(array: &BooleanArray) -> Result<ArrayRef, ArrowError> {
    let metadata = empty_metadata(array.len());

    let mut values = BinaryBuilder::with_capacity(
        array.len(),
        array.len() - array.null_count(), // Each value is a single byte
    );

    for i in 0..array.len() {
        if array.is_null(i) {
            values.append_null();
        } else {
            let value = array.value(i);
            values.append_value([serialize_bool(value)]);
        }
    }

    let values = values.finish();

    let null_buffer = values.nulls().cloned();
    Ok(Arc::new(StructArray::new(
        variant_fields(),
        vec![metadata, Arc::new(values) as ArrayRef],
        null_buffer,
    )) as ArrayRef)
}

fn empty_metadata(len: usize) -> ArrayRef {
    let metadata = build_metadata(std::iter::empty());
    let metadata = BinaryArray::new_scalar(metadata);
    make_repeated_dict_array(metadata, len)
}

#[cfg(test)]
mod tests {
    use arrow_array::BooleanArray;

    use crate::array::VariantArray;

    use super::*;

    #[test]
    fn test_bool_to_variant() {
        let data = BooleanArray::from_iter(vec![Some(true), Some(false), None]);
        let options = CastOptions::default();
        let result = cast_to_variant(&data, &options).unwrap();
        assert_eq!(result.len(), 3);

        let variant = VariantArray::try_new(&result).unwrap();
        assert!(variant.value(0).unwrap().unwrap().get_bool());
        assert!(!variant.value(1).unwrap().unwrap().get_bool());
        assert!(variant.value(2).unwrap().is_none());
    }
}
