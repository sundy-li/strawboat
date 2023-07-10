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

use std::io::Write;

use arrow::bitmap::Bitmap;
use arrow::buffer::Buffer;
use arrow::error::Result;
use arrow::types::Offset;

use crate::Compression;

pub(crate) fn write_binary<O: Offset, W: Write>(
    w: &mut W,
    offsets: &Buffer<O>,
    values: &Buffer<u8>,
    validity: Option<&Bitmap>,
    compression: Compression,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    todo!()
}
