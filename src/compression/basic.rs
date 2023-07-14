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

use arrow::error::{Error, Result};

use super::Compression;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CommonCompression {
    None,
    LZ4,
    ZSTD,
    SNAPPY,
}

impl Default for CommonCompression {
    fn default() -> Self {
        Self::None
    }
}

impl TryFrom<&Compression> for CommonCompression {
    type Error = Error;

    fn try_from(value: &Compression) -> Result<Self> {
        match value {
            Compression::None => Ok(CommonCompression::None),
            Compression::LZ4 => Ok(CommonCompression::LZ4),
            Compression::ZSTD => Ok(CommonCompression::ZSTD),
            Compression::SNAPPY => Ok(CommonCompression::SNAPPY),
            other => Err(Error::OutOfSpec(format!(
                "Unknown compression codec {other:?}",
            ))),
        }
    }
}

impl CommonCompression {
    pub fn to_compression(&self) -> Compression {
        match self {
            Self::None => Compression::None,
            Self::LZ4 => Compression::LZ4,
            Self::ZSTD => Compression::ZSTD,
            Self::SNAPPY => Compression::SNAPPY,
        }
    }

    pub fn decompress(&self, input: &[u8], out_slice: &mut [u8]) -> Result<()> {
        match self {
            Self::LZ4 => decompress_lz4(input, out_slice),
            Self::ZSTD => decompress_zstd(input, out_slice),
            Self::SNAPPY => decompress_snappy(input, out_slice),
            Self::None => {
                out_slice.copy_from_slice(input);
                Ok(())
            }
        }
    }

    pub fn compress(&self, input_buf: &[u8], output_buf: &mut Vec<u8>) -> Result<usize> {
        match self {
            Self::LZ4 => compress_lz4(input_buf, output_buf),
            Self::ZSTD => compress_zstd(input_buf, output_buf),
            Self::SNAPPY => compress_snappy(input_buf, output_buf),
            Self::None => {
                output_buf.extend_from_slice(input_buf);
                Ok(input_buf.len())
            }
        }
    }
}

pub fn decompress_lz4(input_buf: &[u8], output_buf: &mut [u8]) -> Result<()> {
    lz4::block::decompress_to_buffer(input_buf, Some(output_buf.len() as i32), output_buf)
        .map(|_| {})
        .map_err(|e| e.into())
}

pub fn decompress_zstd(input_buf: &[u8], output_buf: &mut [u8]) -> Result<()> {
    zstd::bulk::decompress_to_buffer(input_buf, output_buf)
        .map(|_| {})
        .map_err(|e| e.into())
}

pub fn decompress_snappy(input_buf: &[u8], output_buf: &mut [u8]) -> Result<()> {
    snap::raw::Decoder::new()
        .decompress(input_buf, output_buf)
        .map(|_| {})
        .map_err(|e| {
            arrow::error::Error::External("decompress snappy faild".to_owned(), Box::new(e))
        })
}

pub fn compress_lz4(input_buf: &[u8], output_buf: &mut Vec<u8>) -> Result<usize> {
    let bound = lz4::block::compress_bound(input_buf.len())?;
    let len = output_buf.len();
    output_buf.reserve(bound);

    let s = unsafe {
        core::slice::from_raw_parts_mut(output_buf.as_mut_ptr().offset(len as isize), bound)
    };

    let size = lz4::block::compress_to_buffer(input_buf, None, false, s)
        .map_err(|e| arrow::error::Error::External("Compress lz4 faild".to_owned(), Box::new(e)))?;

    unsafe { output_buf.set_len(size + len) };
    Ok(size)
}

pub fn compress_zstd(input_buf: &[u8], output_buf: &mut Vec<u8>) -> Result<usize> {
    let bound = zstd::zstd_safe::compress_bound(input_buf.len());
    let len = output_buf.len();
    output_buf.reserve(bound);

    let s = unsafe {
        core::slice::from_raw_parts_mut(output_buf.as_mut_ptr().offset(len as isize), bound)
    };

    let size = zstd::bulk::compress_to_buffer(input_buf, s, 0).map_err(|e| {
        arrow::error::Error::External("Compress zstd faild".to_owned(), Box::new(e))
    })?;

    unsafe { output_buf.set_len(size + len) };
    Ok(size)
}

pub fn compress_snappy(input_buf: &[u8], output_buf: &mut Vec<u8>) -> Result<usize> {
    let bound = snap::raw::max_compress_len(input_buf.len());
    let len = output_buf.len();

    output_buf.reserve(bound);
    let s = unsafe {
        core::slice::from_raw_parts_mut(output_buf.as_mut_ptr().offset(len as isize), bound)
    };

    let size = snap::raw::Encoder::new()
        .compress(input_buf, s)
        .map_err(|e| {
            arrow::error::Error::External("Compress snappy faild".to_owned(), Box::new(e))
        })?;

    unsafe { output_buf.set_len(size + len) };
    Ok(size)
}
