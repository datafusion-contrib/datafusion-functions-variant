use arrow_array::{cast::AsArray, types::Int8Type, Array, BinaryArray, Int8Array};
use arrow_schema::ArrowError;
use open_variant::{metadata::MetadataRef, values::VariantRef};

use crate::variant_type;

/// A wrapper around a `StructArray` that represents a variant array.
pub struct VariantArray<'a> {
    /// All the unique metadatas.
    metadatas: Vec<MetadataRef<'a>>,
    /// Indices into `metadatas` for each value.
    metadata_indices: &'a Int8Array,
    /// Array with the variant data
    values: &'a BinaryArray,
}

impl<'a> VariantArray<'a> {
    pub fn try_new(array: &'a dyn Array) -> Result<VariantArray<'a>, ArrowError> {
        // Validate it's the right type.
        if array.data_type() != &variant_type() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Expected a variant array, got {:?}",
                array.data_type()
            )));
        }
        let struct_array = array.as_struct();
        let metadata_array = struct_array.column(0).as_dictionary::<Int8Type>();
        let metadata_indices = metadata_array.keys();

        let metadatas = metadata_array
            .values()
            .as_binary::<i32>()
            .iter()
            .filter_map(|v| v.map(MetadataRef::new))
            .collect();

        let values = struct_array.column(1).as_binary::<i32>();

        Ok(Self {
            metadatas,
            metadata_indices,
            values,
        })
    }

    pub fn metadata(&self, index: usize) -> Option<&MetadataRef> {
        if self.metadata_indices.is_null(index) {
            None
        } else {
            let index = self.metadata_indices.value(index);
            self.metadatas.get(index as usize)
        }
    }

    pub fn value(&self, index: usize) -> Result<Option<VariantRef>, ArrowError> {
        if self.values.is_null(index) {
            Ok(None)
        } else {
            let value = self.values.value(index);
            Ok(Some(
                VariantRef::try_new(value).map_err(ArrowError::ParseError)?,
            ))
        }
    }
}
