//! Parse JSON data into variant data.

use std::{collections::BTreeSet, sync::Arc};

use arrow_array::builder::BinaryBuilder;
use arrow_array::{
    cast::AsArray, Array, ArrayRef, BinaryArray, DictionaryArray, Scalar, StructArray,
};
use arrow_buffer::NullBuffer;
use arrow_schema::{ArrowError, DataType, Field};
use open_variant::metadata::{build_metadata, MetadataRef};
use open_variant::values::write::{self, ArrayBuilder, ObjectBuilder};

pub fn variant_from_json(array: &dyn Array) -> Result<ArrayRef, ArrowError> {
    // TODO: there's probably an optimal implementation that uses jiter, but that's
    // more complex to implement.

    // First, use jitter to parse the JSON string into a JSON object

    // Create a generic iterator so we don't have to monomorphize over every
    // string and binary array type.
    let bytes_iter = bytes_iter_from_array(array)?;
    let jsons = bytes_iter
        .map(|bytes| match bytes {
            Some(bytes) => jiter::JsonValue::parse(bytes, true)
                .map_err(|e| ArrowError::ComputeError(format!("Failed to parse JSON: {}", e))),
            None => Ok(jiter::JsonValue::Null),
        })
        .collect::<Result<Vec<_>, _>>()?;

    // Next, instantiate collector for the dictionary. Then collect the values
    // for this dictionary.
    // For now we just collect object keys.
    // TODO: also support collecting common strings from values.
    let strings = BTreeSet::new();
    // TODO: actually collect the keys by walking the JSON objects

    let metadata = build_metadata(strings.into_iter());
    let metadata = BinaryArray::new_scalar(metadata);
    let metadata = make_repeated_dict_array(metadata, array.len());
    let metadata_ref = metadata
        .as_any_dictionary()
        .keys()
        .as_binary::<i32>()
        .value(0);
    let metadata_ref = MetadataRef::new(metadata_ref);

    let data: BinaryArray =
        values_from_json(&jsons, array.null_count(), array.nulls(), &metadata_ref)?;

    // Finally, create the StructArray
    let fields = vec![
        Field::new("metadata", DataType::Binary, false),
        Field::new("data", DataType::Binary, true),
    ];
    Ok(Arc::new(StructArray::new(
        fields.into(),
        vec![Arc::new(metadata) as ArrayRef, Arc::new(data) as ArrayRef],
        array.nulls().cloned(),
    )) as ArrayRef)
}

fn bytes_iter_from_array(
    array: &dyn Array,
) -> Result<Box<dyn Iterator<Item = Option<&[u8]>> + '_>, ArrowError> {
    match array.data_type() {
        DataType::Utf8 => Ok(Box::new(
            array
                .as_string::<i32>()
                .into_iter()
                .map(|s| s.map(|v| v.as_bytes())),
        )),
        DataType::LargeUtf8 => Ok(Box::new(
            array
                .as_string::<i64>()
                .into_iter()
                .map(|s| s.map(|v| v.as_bytes())),
        )),
        DataType::Binary => Ok(Box::new(array.as_binary::<i32>().into_iter())),
        DataType::LargeBinary => Ok(Box::new(array.as_binary::<i64>().into_iter())),
        DataType::Utf8View => Ok(Box::new(
            array
                .as_string_view()
                .into_iter()
                .map(|s| s.map(|v| v.as_bytes())),
        )),
        DataType::BinaryView => Ok(Box::new(array.as_binary_view().into_iter())),
        _ => Err(ArrowError::InvalidArgumentError(format!(
            "Input data type not supported in variant_from_json: {}",
            array.data_type()
        ))),
    }
}

fn make_repeated_dict_array(scalar: Scalar<BinaryArray>, length: usize) -> ArrayRef {
    let dict_keys = std::iter::repeat(0_i8).take(length).collect::<Vec<_>>();
    let metadata =
        DictionaryArray::new(dict_keys.into(), Arc::new(scalar.into_inner()) as ArrayRef);
    Arc::new(metadata)
}

fn values_from_json(
    jsons: &[jiter::JsonValue],
    null_count: usize,
    null_buffer: Option<&NullBuffer>,
    key_map: &MetadataRef,
) -> Result<BinaryArray, ArrowError> {
    let mut builder = BinaryBuilder::with_capacity(
        jsons.len(),
        jsons.len() - null_count, // For now, just one byte per item that isn't null.
    );
    // TODO: Instead of using a temporary buffer, we could use the builder's buffer.
    let mut buffer = Vec::new();
    for (i, json) in jsons.iter().enumerate() {
        if null_buffer.map(|b| b.is_valid(i)).unwrap_or(true) {
            convert_value(json, &mut buffer, key_map)?;
            builder.append_value(&buffer);
            buffer.clear();
        } else {
            builder.append_null();
        }
    }

    Ok(builder.finish())
}

fn convert_value(
    json: &jiter::JsonValue,
    buffer: &mut Vec<u8>,
    metadata: &MetadataRef,
) -> Result<(), ArrowError> {
    match json {
        jiter::JsonValue::Null => write::write_null(buffer),
        jiter::JsonValue::Bool(true) => write::write_bool(buffer, true),
        jiter::JsonValue::Bool(false) => write::write_bool(buffer, false),
        jiter::JsonValue::Int(value) => write::write_i64(buffer, *value),
        jiter::JsonValue::Float(value) => write::write_f64(buffer, *value),
        jiter::JsonValue::BigInt(value) => {
            let value: i128 = i128::try_from(value).map_err(|_| {
                ArrowError::ComputeError(format!("Could not fit value {} into an i128", value))
            })?;
            write::write_decimal(buffer, value, 0)
        }
        jiter::JsonValue::Str(value) => write::write_string(buffer, value),
        jiter::JsonValue::Array(array) => {
            let mut array_builder = ArrayBuilder::new(buffer, array.len());
            let mut tmp_buffer = Vec::new();
            for value in array.iter() {
                convert_value(value, &mut tmp_buffer, metadata)?;
                array_builder.append_value(&tmp_buffer);
                tmp_buffer.clear();
            }
            array_builder.finish();
        }
        jiter::JsonValue::Object(object) => {
            let mut object_builder = ObjectBuilder::with_capacity(buffer, metadata, object.len());

            let mut tmp_buffer = Vec::new();
            for (key, value) in object.iter() {
                convert_value(value, &mut tmp_buffer, metadata)?;
                object_builder
                    .append_value(key, &tmp_buffer)
                    .map_err(ArrowError::ComputeError)?;
                tmp_buffer.clear();
            }

            object_builder.finish();
        }
    }
    Ok(())
}
