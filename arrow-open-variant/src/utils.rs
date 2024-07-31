use std::sync::Arc;

use arrow_array::{ArrayRef, BinaryArray, DictionaryArray, Scalar};

pub fn make_repeated_dict_array(scalar: Scalar<BinaryArray>, length: usize) -> ArrayRef {
    let dict_keys = std::iter::repeat(0_i8).take(length).collect::<Vec<_>>();
    let metadata =
        DictionaryArray::new(dict_keys.into(), Arc::new(scalar.into_inner()) as ArrayRef);
    Arc::new(metadata)
}
