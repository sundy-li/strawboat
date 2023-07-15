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

use std::fs::File;
use std::io::{BufReader, Seek};
use std::time::Instant;

use arrow::{
    array::Array,
    chunk::Chunk,
    compute,
    error::Result,
    io::parquet::{
        read::{n_columns, ColumnDescriptor},
        write::to_parquet_schema,
    },
};
use strawboat::{
    read::{
        deserialize::column_iter_to_arrays,
        reader::{infer_schema, is_primitive, read_meta, NativeReader},
    },
    ColumnMeta,
};

/// Simplest way: read all record batches from the file. This can be used e.g. for random access.
// cargo run --example strawboat_read --release /tmp/input.str
fn main() -> Result<()> {
    use std::env;
    let args: Vec<String> = env::args().collect();

    let file_path = &args[1];

    let t = Instant::now();
    {
        let mut reader = File::open(file_path).unwrap();
        // we can read its metadata:
        // and infer a [`Schema`] from the `metadata`.
        let schema = infer_schema(&mut reader).unwrap();

        let mut metas: Vec<ColumnMeta> = read_meta(&mut reader)?;
        let schema_descriptor = to_parquet_schema(&schema)?;
        let mut leaves = schema_descriptor.columns().to_vec();

        let mut array_iters = vec![];
        for field in schema.fields.iter() {
            let n = n_columns(&field.data_type);

            let curr_metas: Vec<ColumnMeta> = metas.drain(..n).collect();
            let curr_leaves: Vec<ColumnDescriptor> = leaves.drain(..n).collect();

            let mut native_readers = Vec::with_capacity(n);
            for curr_meta in curr_metas.iter() {
                let mut reader = File::open(file_path).unwrap();
                reader
                    .seek(std::io::SeekFrom::Start(curr_meta.offset))
                    .unwrap();
                let buffer_size = curr_meta.total_len().min(8192) as usize;
                let reader = BufReader::with_capacity(buffer_size, reader);

                let native_reader = NativeReader::new(reader, curr_meta.pages.clone(), vec![]);
                native_readers.push(native_reader);
            }
            let is_nested = !is_primitive(field.data_type());

            let array_iter =
                column_iter_to_arrays(native_readers, curr_leaves, field.clone(), is_nested)?;
            array_iters.push(array_iter);
        }

        let mut results = Vec::new();
        for array_iter in array_iters.iter_mut() {
            let mut arrays = vec![];
            for array in array_iter.by_ref() {
                arrays.push(array?.to_boxed());
            }
            let arrays: Vec<&dyn Array> = arrays.iter().map(|v| v.as_ref()).collect();
            let result = compute::concatenate::concatenate(&arrays).unwrap();
            results.push(result);
        }

        let chunk = Chunk::new(results);
        println!("chunk={chunk:?}");
        println!("READ -> {:?} rows", chunk.len());
    }
    println!("cost {:?} ms", t.elapsed().as_millis());
    Ok(())
}
