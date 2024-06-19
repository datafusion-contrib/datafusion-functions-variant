# Open Variant Rust

This is a Rust implementation of the Open Variant format. This provides a data
structure for semi-structured data (like JSON) that is optimized for OLAP queries.

This is based on the specification and Java implementation hosted in the Apache
Spark project. The specification can be found
[here](https://github.com/apache/spark/tree/master/common/variant).

There are three libraries:

1. [open-variant](./open-variant/): The core library that provides the data structure.
2. (TODO) `arrow-open-variant`: A library to use variant data as an extension type in
    Apache Arrow.
3. (TODO) `datafusion-functions-variant`: A library that provides functions to work
    with variant data in DataFusion.

