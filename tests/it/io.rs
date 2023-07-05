// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use arrow::{
    array::{
        Array, BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
        Int64Array, Int8Array, ListArray, MapArray, PrimitiveArray, StructArray, UInt16Array,
        UInt32Array, UInt64Array, UInt8Array, Utf8Array,
    },
    bitmap::{Bitmap, MutableBitmap},
    chunk::Chunk,
    compute,
    datatypes::{DataType, Field, Schema},
    io::parquet::{
        read::{n_columns, ColumnDescriptor},
        write::to_parquet_schema,
    },
    offset::OffsetsBuffer,
};
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::{
    collections::HashMap,
    io::{BufRead, BufReader},
};
use strawboat::{
    read::{
        batch_read::batch_read_array,
        deserialize::column_iter_to_arrays,
        reader::{is_primitive, NativeReader},
    },
    write::{NativeWriter, WriteOptions},
    ColumnMeta, Compression, PageMeta,
};

const WRITE_PAGE: usize = 128;

#[test]
fn test_basic() {
    let chunk = Chunk::new(vec![
        Box::new(BooleanArray::from_slice([
            true, true, true, false, false, false,
        ])) as _,
        Box::new(UInt8Array::from_vec(vec![1, 2, 3, 4, 5, 6])) as _,
        Box::new(UInt16Array::from_vec(vec![1, 2, 3, 4, 5, 6])) as _,
        Box::new(UInt32Array::from_vec(vec![1, 2, 3, 4, 5, 6])) as _,
        Box::new(UInt64Array::from_vec(vec![1, 2, 3, 4, 5, 6])) as _,
        Box::new(Int8Array::from_vec(vec![1, 2, 3, 4, 5, 6])) as _,
        Box::new(Int16Array::from_vec(vec![1, 2, 3, 4, 5, 6])) as _,
        Box::new(Int32Array::from_vec(vec![1, 2, 3, 4, 5, 6])) as _,
        Box::new(Int64Array::from_vec(vec![1, 2, 3, 4, 5, 6])) as _,
        Box::new(Float32Array::from_vec(vec![1.1, 2.2, 3.3, 4.4, 5.5, 6.6])) as _,
        Box::new(Float64Array::from_vec(vec![1.1, 2.2, 3.3, 4.4, 5.5, 6.6])) as _,
        Box::new(Utf8Array::<i32>::from_iter_values(
            ["1.1", "2.2", "3.3", "4.4", "5.5", "6.6"].iter(),
        )) as _,
        Box::new(BinaryArray::<i64>::from_iter_values(
            ["1.1", "2.2", "3.3", "4.4", "5.5", "6.6"].iter(),
        )) as _,
    ]);
    test_write_read(chunk);
}

#[test]
fn test_random_nonull() {
    let size: usize = 1000;
    let chunk = Chunk::new(vec![
        Box::new(create_random_bool(size, 0.0)) as _,
        Box::new(create_random_index(size, 0.0)) as _,
        Box::new(create_random_string(size, 0.0)) as _,
    ]);
    test_write_read(chunk);
}

#[test]
fn test_random() {
    let size = 1000;
    let chunk = Chunk::new(vec![
        Box::new(create_random_bool(size, 0.1)) as _,
        Box::new(create_random_index(size, 0.1)) as _,
        Box::new(create_random_index(size, 0.2)) as _,
        Box::new(create_random_index(size, 0.3)) as _,
        Box::new(create_random_index(size, 0.4)) as _,
        Box::new(create_random_index(size, 0.5)) as _,
        Box::new(create_random_string(size, 0.4)) as _,
    ]);
    test_write_read(chunk);
}

#[test]
fn test_struct() {
    let struct_array = create_struct(1000, 0.2);
    let chunk = Chunk::new(vec![Box::new(struct_array) as _]);
    test_write_read(chunk);
}

#[test]
fn test_list() {
    let list_array = create_list(1000, 0.2);
    let chunk = Chunk::new(vec![Box::new(list_array) as _]);
    test_write_read(chunk);
}

#[test]
fn test_map() {
    let map_array = create_map(1000, 0.2);
    let chunk = Chunk::new(vec![Box::new(map_array) as _]);
    test_write_read(chunk);
}

#[test]
fn test_list_list() {
    let l1 = create_list(2000, 0.2);

    let mut offsets = vec![];
    for i in (0..=1000).step_by(2) {
        offsets.push(i);
    }
    let list_array = ListArray::try_new(
        DataType::List(Box::new(Field::new("item", l1.data_type().clone(), true))),
        OffsetsBuffer::try_from(offsets).unwrap(),
        l1.boxed(),
        None,
    )
    .unwrap();

    let chunk = Chunk::new(vec![Box::new(list_array) as _]);
    test_write_read(chunk);
}

#[test]
fn test_list_struct() {
    let s1 = create_struct(2000, 0.2);

    let mut offsets = vec![];
    for i in (0..=1000).step_by(2) {
        offsets.push(i);
    }
    let list_array = ListArray::try_new(
        DataType::List(Box::new(Field::new("item", s1.data_type().clone(), true))),
        OffsetsBuffer::try_from(offsets).unwrap(),
        s1.boxed(),
        None,
    )
    .unwrap();

    let chunk = Chunk::new(vec![Box::new(list_array) as _]);
    test_write_read(chunk);
}

#[test]
fn test_list_map() {
    let m1 = create_map(2000, 0.2);

    let mut offsets = vec![];
    for i in (0..=1000).step_by(2) {
        offsets.push(i);
    }
    let list_array = ListArray::try_new(
        DataType::List(Box::new(Field::new("item", m1.data_type().clone(), true))),
        OffsetsBuffer::try_from(offsets).unwrap(),
        m1.boxed(),
        None,
    )
    .unwrap();

    let chunk = Chunk::new(vec![Box::new(list_array) as _]);
    test_write_read(chunk);
}

#[test]
fn test_struct_list() {
    let size = 1000;
    let null_density = 0.2;
    let dt = DataType::Struct(vec![
        Field::new("name", DataType::LargeBinary, true),
        Field::new(
            "age",
            DataType::List(Box::new(Field::new("item", DataType::Int32, true))),
            true,
        ),
    ]);
    let struct_array = StructArray::try_new(
        dt,
        vec![
            Box::new(create_random_string(size, null_density)) as _,
            Box::new(create_list(size, null_density)) as _,
        ],
        None,
    )
    .unwrap();
    let chunk = Chunk::new(vec![Box::new(struct_array) as _]);
    test_write_read(chunk);
}

fn create_list(size: usize, null_density: f32) -> ListArray<i32> {
    let (offsets, bitmap) = create_random_offsets(size, 0.1);
    let length = *offsets.last().unwrap() as usize;
    let l1 = create_random_index(length, null_density);

    ListArray::try_new(
        DataType::List(Box::new(Field::new("item", l1.data_type().clone(), true))),
        OffsetsBuffer::try_from(offsets).unwrap(),
        l1.boxed(),
        bitmap,
    )
    .unwrap()
}

fn create_map(size: usize, null_density: f32) -> MapArray {
    let (offsets, bitmap) = create_random_offsets(size, 0.1);
    let length = *offsets.last().unwrap() as usize;
    let dt = DataType::Struct(vec![
        Field::new("key", DataType::Int32, false),
        Field::new("value", DataType::LargeBinary, true),
    ]);
    let struct_array = StructArray::try_new(
        dt,
        vec![
            Box::new(create_random_index(length, 0.0)) as _,
            Box::new(create_random_string(length, null_density)) as _,
        ],
        None,
    )
    .unwrap();

    MapArray::try_new(
        DataType::Map(
            Box::new(Field::new(
                "entries",
                struct_array.data_type().clone(),
                false,
            )),
            false,
        ),
        OffsetsBuffer::try_from(offsets).unwrap(),
        struct_array.boxed(),
        bitmap,
    )
    .unwrap()
}

fn create_struct(size: usize, null_density: f32) -> StructArray {
    let dt = DataType::Struct(vec![
        Field::new("name", DataType::LargeBinary, true),
        Field::new("age", DataType::Int32, true),
    ]);
    StructArray::try_new(
        dt,
        vec![
            Box::new(create_random_string(size, null_density)) as _,
            Box::new(create_random_index(size, null_density)) as _,
        ],
        None,
    )
    .unwrap()
}

fn create_random_bool(size: usize, null_density: f32) -> BooleanArray {
    let mut rng = StdRng::seed_from_u64(42);
    (0..size)
        .map(|_| {
            if rng.gen::<f32>() > null_density {
                let value = rng.gen::<bool>();
                Some(value)
            } else {
                None
            }
        })
        .collect::<BooleanArray>()
}

fn create_random_index(size: usize, null_density: f32) -> PrimitiveArray<i32> {
    let mut rng = StdRng::seed_from_u64(42);
    (0..size)
        .map(|_| {
            if rng.gen::<f32>() > null_density {
                let value = rng.gen_range::<i32, _>(0i32..size as i32);
                Some(value)
            } else {
                None
            }
        })
        .collect::<PrimitiveArray<i32>>()
}

fn create_random_string(size: usize, null_density: f32) -> BinaryArray<i64> {
    let mut rng = StdRng::seed_from_u64(42);
    (0..size)
        .map(|_| {
            if rng.gen::<f32>() > null_density {
                let value = rng.gen_range::<i32, _>(0i32..size as i32);
                Some(format!("{value}"))
            } else {
                None
            }
        })
        .collect::<BinaryArray<i64>>()
}

fn create_random_offsets(size: usize, null_density: f32) -> (Vec<i32>, Option<Bitmap>) {
    let mut offsets = Vec::with_capacity(size + 1);
    offsets.push(0i32);
    let mut builder = MutableBitmap::with_capacity(size);
    let mut rng = StdRng::seed_from_u64(42);
    for _ in 0..size {
        if rng.gen::<f32>() > null_density {
            let offset = rng.gen_range::<i32, _>(0i32..3i32);
            offsets.push(*offsets.last().unwrap() + offset);
            builder.push(true);
        } else {
            offsets.push(*offsets.last().unwrap());
            builder.push(false);
        }
    }
    (offsets, builder.into())
}

fn test_write_read(chunk: Chunk<Box<dyn Array>>) {
    let compressions = vec![
        Compression::LZ4,
        Compression::ZSTD,
        Compression::SNAPPY,
        Compression::None,
    ];

    for compression in compressions {
        test_write_read_with_options(
            chunk.clone(),
            WriteOptions {
                default_compression: compression,
                max_page_size: Some(WRITE_PAGE),
                column_compressions: Default::default(),
            },
        );
    }

    // test column compression
    for compression in vec![Compression::RLE, Compression::Dict] {
        let mut column_compressions = HashMap::new();
        let compressor = compression.create_compressor();
        for (id, array) in chunk.arrays().iter().enumerate() {
            if compressor.support_datatype(array.data_type()) {
                column_compressions.insert(id, compression);
            }
        }
        test_write_read_with_options(
            chunk.clone(),
            WriteOptions {
                default_compression: Compression::LZ4,
                max_page_size: Some(WRITE_PAGE),
                column_compressions,
            },
        );
    }
}

fn test_write_read_with_options(chunk: Chunk<Box<dyn Array>>, options: WriteOptions) {
    let mut bytes = Vec::new();
    let fields: Vec<Field> = chunk
        .iter()
        .map(|array| {
            Field::new(
                "name",
                array.data_type().clone(),
                array.validity().is_some(),
            )
        })
        .collect();

    let schema = Schema::from(fields);
    let mut writer = NativeWriter::new(&mut bytes, schema.clone(), options);

    writer.start().unwrap();
    writer.write(&chunk).unwrap();
    writer.finish().unwrap();

    let mut batch_metas = writer.metas.clone();
    let mut metas = writer.metas.clone();
    let schema_descriptor = to_parquet_schema(&schema).unwrap();
    let mut leaves = schema_descriptor.columns().to_vec();
    let mut results = Vec::with_capacity(schema.fields.len());
    for field in schema.fields.iter() {
        let n = n_columns(&field.data_type);

        let curr_metas: Vec<ColumnMeta> = metas.drain(..n).collect();
        let curr_leaves: Vec<ColumnDescriptor> = leaves.drain(..n).collect();

        let mut native_readers = Vec::with_capacity(n);
        for curr_meta in curr_metas.iter() {
            let mut range_bytes = std::io::Cursor::new(bytes.clone());
            range_bytes.consume(curr_meta.offset as usize);

            let native_reader = NativeReader::new(range_bytes, curr_meta.pages.clone(), vec![]);
            native_readers.push(native_reader);
        }
        let is_nested = !is_primitive(field.data_type());

        let mut array_iter =
            column_iter_to_arrays(native_readers, curr_leaves, field.clone(), is_nested).unwrap();

        let mut arrays = vec![];
        for array in array_iter.by_ref() {
            arrays.push(array.unwrap().to_boxed());
        }
        let arrays: Vec<&dyn Array> = arrays.iter().map(|v| v.as_ref()).collect();
        let result = compute::concatenate::concatenate(&arrays).unwrap();
        results.push(result);
    }
    let result_chunk = Chunk::new(results);

    assert_eq!(chunk, result_chunk);

    // test batch read
    let schema_descriptor = to_parquet_schema(&schema).unwrap();
    let mut leaves = schema_descriptor.columns().to_vec();
    let mut batch_results = Vec::with_capacity(schema.fields.len());
    for field in schema.fields.iter() {
        let n = n_columns(&field.data_type);

        let curr_metas: Vec<ColumnMeta> = batch_metas.drain(..n).collect();
        let curr_leaves: Vec<ColumnDescriptor> = leaves.drain(..n).collect();

        let mut pages: Vec<Vec<PageMeta>> = Vec::with_capacity(n);
        let mut readers = Vec::with_capacity(n);
        for curr_meta in curr_metas.iter() {
            pages.push(curr_meta.pages.clone());
            let mut reader = std::io::Cursor::new(bytes.clone());
            reader.consume(curr_meta.offset as usize);

            let buffer_size = curr_meta.total_len().min(8192) as usize;
            let reader = BufReader::with_capacity(buffer_size, reader);

            readers.push(reader);
        }
        let is_nested = !is_primitive(field.data_type());
        let batch_result =
            batch_read_array(readers, curr_leaves, field.clone(), is_nested, pages).unwrap();
        batch_results.push(batch_result);
    }
    let batch_result_chunk = Chunk::new(batch_results);

    assert_eq!(chunk, batch_result_chunk);
}
