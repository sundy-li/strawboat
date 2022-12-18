use std::fs::File;

use arrow::array::{FixedSizeListArray, ListArray, StructArray, Utf8Array};
use arrow::datatypes::DataType;
use arrow::{
    array::{Array, Int32Array},
    chunk::Chunk,
    datatypes::{Field, Schema},
    error::Result,
    io::parquet::write::{
        transverse, CompressionOptions, Encoding, FileWriter, RowGroupIterator, Version,
        WriteOptions,
    },
};
use serde_json::error::Category::Data;

fn write_chunk(path: &str, schema: Schema, chunk: Chunk<Box<dyn Array>>) -> Result<()> {
    let options = WriteOptions {
        write_statistics: true,
        compression: CompressionOptions::Uncompressed,
        version: Version::V2,
        data_pagesize_limit: None,
    };

    let iter = vec![Ok(chunk)];

    let encodings = schema
        .fields
        .iter()
        .map(|f| transverse(&f.data_type, |_| Encoding::Plain))
        .collect();

    let row_groups = RowGroupIterator::try_new(iter.into_iter(), &schema, options, encodings)?;

    // Create a new empty file
    let file = File::create(path)?;

    let mut writer = FileWriter::try_new(file, schema, options)?;

    for group in row_groups {
        writer.write(group?)?;
    }
    let _size = writer.end(None)?;
    Ok(())
}

fn main() -> Result<()> {
    // mock int32 array
    let int32_array = Int32Array::from(&[Some(0), Some(1), None]);
    let int32_field = Field::new("c1", int32_array.data_type().clone(), true);

    // mock struct array
    let s1 = [Some("a"), Some("bc"), None];
    let s2 = [Some(1), Some(2), None];
    let dt = DataType::Struct(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, true),
    ]);
    let struct_array = StructArray::try_new(
        dt,
        vec![
            Utf8Array::<i32>::from(s1).boxed(),
            Int32Array::from(s2).boxed(),
        ],
        None,
    )?;
    let struct_field = Field::new("c2", struct_array.data_type().clone(), true);

    // mock list array
    let l1 = Int32Array::from(&[
        Some(0),
        Some(1),
        None,
        Some(2),
        Some(3),
        None,
        Some(4),
        Some(5),
        None,
    ]);
    let list_array = ListArray::try_new(
        DataType::List(Box::new(Field::new("item", l1.data_type().clone(), false))),
        vec![0, 3, 5, 9].into(),
        l1.boxed(),
        None,
    )?;
    let list_field = Field::new("c3", list_array.data_type().clone(), true);

    let schema = Schema::from(vec![int32_field, struct_field, list_field]);
    let chunk = Chunk::new(vec![
        int32_array.boxed(),
        struct_array.boxed(),
        list_array.boxed(),
    ]);

    write_chunk("/tmp/input.parquet", schema, chunk)
}
