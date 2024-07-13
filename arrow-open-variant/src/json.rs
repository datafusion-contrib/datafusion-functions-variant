//! Parse JSON data into variant data.

use std::borrow::Cow;
use std::{collections::BTreeSet, sync::Arc};

use arrow_array::builder::BinaryBuilder;
use arrow_array::{
    cast::AsArray, Array, ArrayRef, BinaryArray, DictionaryArray, Scalar, StructArray,
};
use arrow_buffer::NullBuffer;
use arrow_schema::{ArrowError, DataType, Field};
use jiter::JsonValue;
use open_variant::metadata::{build_metadata, MetadataRef};
use open_variant::values::write::{self, ArrayBuilder, ObjectBuilder};

pub fn variant_from_json(array: &dyn Array) -> Result<ArrayRef, ArrowError> {
    // TODO: there's probably an optimal implementation that uses jiter, but that's
    // more complex to implement.

    // First, use jitter to parse the JSON string into a JSON object

    // Create a generic iterator so we don't have to monomorphize over every
    // string and binary array type.
    let bytes_iter = bytes_iter_from_array(array)?;
    let jsons: Vec<JsonValue<'_>> = bytes_iter
        .map(|bytes| match bytes {
            Some(bytes) => jiter::JsonValue::parse(bytes, true)
                .map_err(|e| ArrowError::ComputeError(format!("Failed to parse JSON: {}", e))),
            None => Ok(jiter::JsonValue::Null),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let jsons_ref = jsons.as_slice();

    // Next, instantiate collector for the dictionary. Then collect the values
    // for this dictionary.
    // For now we just collect object keys.
    // TODO: also support collecting common strings from values.
    let strings = collect_all_keys(jsons_ref)?;

    let metadata = build_metadata(strings.iter().map(|x| x.as_ref()));
    let metadata = BinaryArray::new_scalar(metadata);
    let metadata = make_repeated_dict_array(metadata, array.len());
    let metadata_ref = metadata
        .as_any_dictionary()
        .values()
        .as_binary::<i32>()
        .value(0);
    let metadata_ref = MetadataRef::new(metadata_ref);

    let data: BinaryArray =
        values_from_json(jsons_ref, array.null_count(), array.nulls(), &metadata_ref)?;
    // Finally, create the StructArray
    let fields = vec![
        Field::new(
            "metadata",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Binary)),
            false,
        ),
        Field::new("values", DataType::Binary, true),
    ];
    let null_buffer = data.nulls().cloned();
    Ok(Arc::new(StructArray::new(
        fields.into(),
        vec![metadata, Arc::new(data) as ArrayRef],
        null_buffer,
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

fn collect_all_keys<'a>(jsons: &[JsonValue<'a>]) -> Result<BTreeSet<Cow<'a, str>>, ArrowError> {
    let mut seen = BTreeSet::new();
    let mut stack = Vec::new();

    let is_nested = |json: &JsonValue| matches!(json, JsonValue::Object(_) | JsonValue::Array(_));
    for json in jsons {
        match json {
            JsonValue::Object(object) => {
                for (key, value) in object.iter() {
                    seen.insert(key.clone());
                    if is_nested(value) {
                        stack.push(value);
                    }
                }
            }
            JsonValue::Array(array) => {
                for value in array.iter() {
                    if is_nested(value) {
                        stack.push(value);
                    }
                }
            }
            _ => {}
        }
    }

    while let Some(json) = stack.pop() {
        match json {
            JsonValue::Object(object) => {
                for (key, value) in object.iter() {
                    seen.insert(key.clone());
                    if is_nested(value) {
                        stack.push(value);
                    }
                }
            }
            JsonValue::Array(array) => {
                for value in array.iter() {
                    if is_nested(value) {
                        stack.push(value);
                    }
                }
            }
            _ => {}
        }
    }

    Ok(seen)
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
            if buffer == [0] {
                // Special case for nulls, which are represented as "0" in the variant format.
                builder.append_null();
            } else {
                builder.append_value(&buffer);
            }
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

#[cfg(test)]
mod tests {
    use arrow_array::{
        types::Int8Type, BinaryViewArray, Int8Array, LargeStringArray, StringArray, StringViewArray,
    };
    use open_variant::values::{BasicType, PrimitiveTypeId, VariantRef};

    use super::*;

    fn check_parsing(jsons: &[&str]) -> ArrayRef {
        let string_array = StringArray::from_iter_values(jsons);
        let variant_array = variant_from_json(&string_array).unwrap();
        let expected_type = DataType::Struct(
            vec![
                Field::new(
                    "metadata",
                    DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Binary)),
                    false,
                ),
                Field::new("values", DataType::Binary, true),
            ]
            .into(),
        );
        assert_eq!(variant_array.data_type(), &expected_type);
        variant_array
    }

    #[test]
    fn test_nulls() {
        // Top-level nulls are represented as normal Arrow nulls.
        let output = check_parsing(&["null", "null", "null"]);
        assert_eq!(output.null_count(), 3);

        let output = check_parsing(&["null", "true", "null"]);
        assert_eq!(output.null_count(), 2);
        assert!(!output.is_null(1));

        // Nested nulls are of null data type.
        let output = check_parsing(&[r#"{"x": null}"#]);
        assert_eq!(output.null_count(), 0);
        let values = output.as_struct().column(1).as_binary::<i32>();
        let variant = VariantRef::try_new(values.value(0)).unwrap();
        assert_eq!(variant.basic_type(), BasicType::Object);
        let variant = variant.field(0).unwrap().unwrap();
        assert_eq!(variant.basic_type(), BasicType::Primitive);
        assert_eq!(variant.primitive_type_id(), PrimitiveTypeId::Null);
    }

    #[test]
    fn test_boolean() {
        let output = check_parsing(&["true", "false"]);
        let values = output.as_struct().column(1).as_binary::<i32>();
        let variant = VariantRef::try_new(values.value(0)).unwrap();
        assert_eq!(variant.basic_type(), BasicType::Primitive);
        assert_eq!(variant.primitive_type_id(), PrimitiveTypeId::BoolTrue);
        assert!(variant.get_bool());

        let variant = VariantRef::try_new(values.value(1)).unwrap();
        assert_eq!(variant.basic_type(), BasicType::Primitive);
        assert_eq!(variant.primitive_type_id(), PrimitiveTypeId::BoolFalse);
        assert!(!variant.get_bool());
    }

    #[test]
    fn test_numbers() {
        let output = check_parsing(&["-42"]);
        let values = output.as_struct().column(1).as_binary::<i32>();
        let variant = VariantRef::try_new(values.value(0)).unwrap();
        assert_eq!(variant.basic_type(), BasicType::Primitive);
        assert_eq!(variant.primitive_type_id(), PrimitiveTypeId::Int64);
        assert_eq!(variant.get_i64(), -42);
    }

    #[test]
    fn test_big_integers() {
        let output = check_parsing(&[&i128::MAX.to_string()]);
        let values = output.as_struct().column(1).as_binary::<i32>();
        let variant = VariantRef::try_new(values.value(0)).unwrap();
        assert_eq!(variant.basic_type(), BasicType::Primitive);
        assert_eq!(variant.primitive_type_id(), PrimitiveTypeId::Decimal16);
        assert_eq!(variant.get_i128(), i128::MAX);
    }

    #[test]
    fn test_floats() {
        let output = check_parsing(&["45.454545"]);
        let values = output.as_struct().column(1).as_binary::<i32>();
        let variant = VariantRef::try_new(values.value(0)).unwrap();
        assert_eq!(variant.basic_type(), BasicType::Primitive);
        assert_eq!(variant.primitive_type_id(), PrimitiveTypeId::Float64);
        assert_eq!(variant.get_f64(), 45.454545);
    }

    #[test]
    fn test_strings() {
        let output = check_parsing(&["\"some string\""]);
        let values = output.as_struct().column(1).as_binary::<i32>();
        let variant = VariantRef::try_new(values.value(0)).unwrap();
        assert_eq!(variant.basic_type(), BasicType::Primitive);
        assert_eq!(variant.primitive_type_id(), PrimitiveTypeId::String);
        assert_eq!(variant.get_string(), "some string");
    }

    fn get_field<'a>(
        meta_ref: &'a MetadataRef<'a>,
        variant: &'a VariantRef<'a>,
        key: &str,
    ) -> VariantRef<'a> {
        let field_id = meta_ref.find_string(key).unwrap();
        variant.field(field_id).unwrap().unwrap()
    }

    #[test]
    fn test_objects() {
        let output = check_parsing(&[
            r#"{"a": 1, "b": 2, "c": 3}"#,
            r#"{"b": 2, "c": 3, "a": 1}"#, // Different order
            r#"{"a": 1, "b": 2, "c": 3, "d": {"e": 4}}"#, // Nested object
        ]);
        let metadata = output
            .as_struct()
            .column(0)
            .as_dictionary::<Int8Type>()
            .values()
            .as_binary::<i32>()
            .value(0);
        let metadata_ref = MetadataRef::new(metadata);
        let values = output.as_struct().column(1).as_binary::<i32>();

        let variant = VariantRef::try_new(values.value(0)).unwrap();
        assert_eq!(variant.basic_type(), BasicType::Object);

        assert_eq!(get_field(&metadata_ref, &variant, "a").get_i64(), 1);
        assert_eq!(get_field(&metadata_ref, &variant, "b").get_i64(), 2);
        assert_eq!(get_field(&metadata_ref, &variant, "c").get_i64(), 3);

        let variant = VariantRef::try_new(values.value(1)).unwrap();
        assert_eq!(variant.basic_type(), BasicType::Object);
        assert_eq!(get_field(&metadata_ref, &variant, "a").get_i64(), 1);
        assert_eq!(get_field(&metadata_ref, &variant, "b").get_i64(), 2);
        assert_eq!(get_field(&metadata_ref, &variant, "c").get_i64(), 3);

        let variant = VariantRef::try_new(values.value(2)).unwrap();
        assert_eq!(variant.basic_type(), BasicType::Object);
        assert_eq!(get_field(&metadata_ref, &variant, "a").get_i64(), 1);
        assert_eq!(get_field(&metadata_ref, &variant, "b").get_i64(), 2);
        assert_eq!(get_field(&metadata_ref, &variant, "c").get_i64(), 3);
        let nested = get_field(&metadata_ref, &variant, "d");
        assert_eq!(get_field(&metadata_ref, &nested, "e").get_i64(), 4);
    }

    fn get_element<'a>(variant: &'a VariantRef<'a>, i: usize) -> VariantRef<'a> {
        variant.field(i).unwrap().unwrap()
    }

    #[test]
    fn test_arrays() {
        // Arrays of different types
        // Arrays with objects
        // Arrays with arrays
        let output = check_parsing(&[
            "[1, \"b\", 3.0]",
            r#"["a", {"b": 2}, [3, 4]]"#, // Nested object and array
            r#"[[3, 4, {"c": 5}]]"#,
            r#"[{"d": [6, 7]}]"#,
        ]);

        let metadata = output
            .as_struct()
            .column(0)
            .as_dictionary::<Int8Type>()
            .values()
            .as_binary::<i32>()
            .value(0);
        let metadata_ref = MetadataRef::new(metadata);

        let values = output.as_struct().column(1).as_binary::<i32>();

        let variant = VariantRef::try_new(values.value(0)).unwrap();
        assert_eq!(variant.basic_type(), BasicType::Array);
        assert_eq!(get_element(&variant, 0).get_i64(), 1);
        assert_eq!(get_element(&variant, 1).get_string(), "b");
        assert_eq!(get_element(&variant, 2).get_f64(), 3.0);

        let variant = VariantRef::try_new(values.value(1)).unwrap();
        assert_eq!(variant.basic_type(), BasicType::Array);
        assert_eq!(get_element(&variant, 0).get_string(), "a");
        let nested = get_element(&variant, 1);
        assert_eq!(nested.basic_type(), BasicType::Object);
        assert_eq!(get_field(&metadata_ref, &nested, "b").get_i64(), 2);
        let nested = get_element(&variant, 2);
        assert_eq!(nested.basic_type(), BasicType::Array);
        assert_eq!(get_element(&nested, 0).get_i64(), 3);
        assert_eq!(get_element(&nested, 1).get_i64(), 4);

        let variant = VariantRef::try_new(values.value(2)).unwrap();
        assert_eq!(variant.basic_type(), BasicType::Array);
        let nested = get_element(&variant, 0);
        assert_eq!(nested.basic_type(), BasicType::Array);
        assert_eq!(get_element(&nested, 0).get_i64(), 3);
        assert_eq!(get_element(&nested, 1).get_i64(), 4);
        let nested = get_element(&nested, 2);
        assert_eq!(nested.basic_type(), BasicType::Object);
        assert_eq!(get_field(&metadata_ref, &nested, "c").get_i64(), 5);

        let variant = VariantRef::try_new(values.value(3)).unwrap();
        assert_eq!(variant.basic_type(), BasicType::Array);
        let nested = get_element(&variant, 0);
        assert_eq!(nested.basic_type(), BasicType::Object);
        let nested = get_field(&metadata_ref, &nested, "d");
        assert_eq!(nested.basic_type(), BasicType::Array);
        assert_eq!(get_element(&nested, 0).get_i64(), 6);
        assert_eq!(get_element(&nested, 1).get_i64(), 7);
    }

    #[test]
    fn test_types() {
        // Accepts all string and binary types
        let values = &["\"x\"", "1"];
        let arrays = [
            Arc::new(StringArray::from_iter_values(values)) as ArrayRef,
            Arc::new(LargeStringArray::from_iter_values(values)) as ArrayRef,
            Arc::new(StringViewArray::from_iter_values(values)) as ArrayRef,
            Arc::new(BinaryArray::from_iter_values(values)) as ArrayRef,
            Arc::new(LargeStringArray::from_iter_values(values)) as ArrayRef,
            Arc::new(BinaryViewArray::from_iter_values(values)) as ArrayRef,
        ];

        for array in &arrays {
            let output = variant_from_json(array);
            assert!(
                output.is_ok(),
                "Failed for {:?} due to {}",
                array.data_type(),
                output.unwrap_err()
            );
            let output = output.unwrap();
            assert_eq!(
                output.data_type(),
                &DataType::Struct(
                    vec![
                        Field::new(
                            "metadata",
                            DataType::Dictionary(
                                Box::new(DataType::Int8),
                                Box::new(DataType::Binary)
                            ),
                            false,
                        ),
                        Field::new("values", DataType::Binary, true),
                    ]
                    .into(),
                )
            );
        }
    }

    #[test]
    fn test_validates_datatype() {
        let wrong_array = Arc::new(Int8Array::from(vec![1, 2, 3])) as ArrayRef;
        let output = variant_from_json(&wrong_array);
        assert!(output.is_err());
        assert!(
            matches!(&output, Err(ArrowError::InvalidArgumentError(message))
            if message.contains("Input data type not supported in variant_from_json: Int8")),
            "Unexpected error: {:?}",
            output
        );
    }

    #[test]
    fn test_parsing_error() {
        // Errors if fails to parse any value.
        // TODO: Should we have other error modes, such that invalid JSON could be
        // made null or output some error value?
        let values = &[r#"{"a": "#];
        let array = Arc::new(StringArray::from_iter_values(values)) as ArrayRef;
        let output = variant_from_json(&array);
        assert!(output.is_err());
        assert!(matches!(output, Err(ArrowError::ComputeError(message))
            if message.contains("Failed to parse JSON")));
    }
}
