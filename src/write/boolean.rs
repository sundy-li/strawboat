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
use arrow::error::Result;

use crate::Compression;

pub(crate) fn write_bitmap<W: Write>(
    w: &mut W,
    bitmap: &Bitmap,
    compression: Compression,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    let codec: u8 = compression.into();
    w.write_all(&codec.to_le_bytes())?;
    scratch.clear();

    let (slice, slice_offset, _) = bitmap.as_slice();
    let compressor = compression.create_compressor();
    let compressed_size = if compressor.raw_mode() {
        let bitmap = if slice_offset != 0 {
            // case where we can't slice the bitmap as the offsets are not multiple of 8
            Bitmap::from_trusted_len_iter(bitmap.iter())
        } else {
            bitmap.clone()
        };

        let (slice, _, _) = bitmap.as_slice();
        compressor.compress(slice, scratch)
    } else {
        compressor.compress_bitmap(bitmap, scratch)
    }?;

    //compressed size
    w.write_all(&(compressed_size as u32).to_le_bytes())?;
    //uncompressed size
    w.write_all(&(slice.len() as u32).to_le_bytes())?;
    w.write_all(&scratch[..compressed_size])?;

    Ok(())
}
