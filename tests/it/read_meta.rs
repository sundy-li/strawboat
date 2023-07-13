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

use std::collections::HashMap;

use arrow::{
    datatypes::{Field, Schema},
    error::Result,
};
use strawboat::{
    read::reader::{read_meta, read_meta_async},
    write::{NativeWriter, WriteOptions},
    ColumnMeta, Compression,
};

use crate::io::{new_test_chunk, WRITE_PAGE};

fn write_data(dest: &mut Vec<u8>) -> Vec<ColumnMeta> {
    let chunk = new_test_chunk();
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

    let mut writer = NativeWriter::new(
        dest,
        Schema::from(fields),
        WriteOptions {
            default_compression: Compression::LZ4,
            max_page_size: Some(WRITE_PAGE),
            column_compressions: HashMap::new(),
        },
    );

    writer.start().unwrap();
    writer.write(&chunk).unwrap();
    writer.finish().unwrap();

    writer.metas
}

#[test]
fn test_read_meta() -> Result<()> {
    let mut buf = Vec::new();
    let expected_meta = write_data(&mut buf);

    let mut reader = std::io::Cursor::new(buf);
    let meta = read_meta(&mut reader)?;

    assert_eq!(expected_meta, meta);

    Ok(())
}

#[test]
fn test_read_meta_async() -> Result<()> {
    async_std::task::block_on(test_read_meta_async_impl())
}

async fn test_read_meta_async_impl() -> Result<()> {
    let mut buf: Vec<u8> = Vec::new();
    let expected_meta = write_data(&mut buf);
    let len = buf.len();

    let mut reader = async_std::io::Cursor::new(buf);

    {
        // Without `total_len`.
        let meta = read_meta_async(&mut reader, None).await?;
        assert_eq!(expected_meta, meta);
    }

    {
        // With `total_len`.
        let meta = read_meta_async(&mut reader, Some(len)).await?;
        assert_eq!(expected_meta, meta);
    }
    Ok(())
}
