# open-variant-rs

This is a Rust implementation of the Open Variant format. This provides a data
structure for semi-structured data (like JSON) that is optimized for OLAP queries.

Variants can be values of a variety of types, including strings, integers, decimals,
objects, and arrays.

Data is written in two parts: a metadata section and a data section. The metadata
section is meant to be shared across many data records. It holds the version of 
the format as well as a list of keys. The data section holds the serialized
variant data, with all keys replaced by their index in the metadata section.

## Example

```rust
// Writing an object
use open_variant::metadata::build_metadata;
use open_variant::variant::write::{ObjectBuilder, write_i64, write_string};
use open_variant::metadata::MetadataRef;

// Provide all keys up front.
let metadata: Vec<u8> = build_metadata(["price", "quantity", "product"].into_iter());
let metadata_ref = MetadataRef::new(&metadata);

// Now, we can build the object:
// { "product": "apple", "price": 1.23, "quantity": 4 }
let mut data_buffer: Vec<u8> = Vec::new();
let mut object = ObjectBuilder::with_capacity(&mut data_buffer, &metadata_ref, 3);
object.append_string("product", "apple");
object.append_decimal("price", 123, 2); // value, scale
object.append_i64("quantity", 4);
object.finish();

// Reading the variant
use open_variant::variant::VariantRef;

let variant_ref = VariantRef(&data_buffer);

// Get the field id for the "product" key. This can be done once and
// reused for all records that share the same metadata buffer.
let field_id = metadata_ref.find_string("product").unwrap();
let product = variant_ref.get_object_value(field_id).unwrap().get_string();
assert_eq!(product, "apple");
```
