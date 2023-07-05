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

use criterion::{criterion_group, criterion_main, Criterion};

use arrow::array::{clone, Array};
use arrow::chunk::Chunk;
use arrow::datatypes::{Field, Schema};
use arrow::error::Result;
use arrow::util::bench_util::{create_boolean_array, create_primitive_array, create_string_array};

use strawboat::{write, Compression};

type ChunkBox = Chunk<Box<dyn Array>>;

fn write(array: &dyn Array) -> Result<()> {
    let schema = Schema::from(vec![Field::new("c1", array.data_type().clone(), true)]);
    let columns: ChunkBox = Chunk::new(vec![clone(array)]);

    let options = write::WriteOptions {
        default_compression: Compression::LZ4,
        max_page_size: Some(8192),
        column_compressions: Default::default(),
    };

    let file = vec![];
    let mut writer = write::NativeWriter::new(file, schema, options);

    writer.start()?;
    writer.write(&columns)?;
    writer.finish()?;

    Ok(())
}

fn add_benchmark(c: &mut Criterion) {
    (0..=10).step_by(2).for_each(|i| {
        let array = &create_boolean_array(1024 * 2usize.pow(i), 0.1, 0.5);
        let a = format!("write bool 2^{}", 10 + i);
        c.bench_function(&a, |b| b.iter(|| write(array).unwrap()));
    });
    (0..=10).step_by(2).for_each(|i| {
        let array = &create_string_array::<i32>(1024 * 2usize.pow(i), 4, 0.1, 42);
        let a = format!("write utf8 2^{}", 10 + i);
        c.bench_function(&a, |b| b.iter(|| write(array).unwrap()));
    });
    (0..=10).step_by(2).for_each(|i| {
        let array = &create_primitive_array::<i64>(1024 * 2usize.pow(i), 0.0);
        let a = format!("write i64 2^{}", 10 + i);
        c.bench_function(&a, |b| b.iter(|| write(array).unwrap()));
    });
}

criterion_group!(benches, add_benchmark);
criterion_main!(benches);
