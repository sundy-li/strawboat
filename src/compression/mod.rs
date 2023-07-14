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

mod basic;

pub mod binary;
pub mod boolean;
pub mod integer;

use arrow::{bitmap::Bitmap, error::Result};

pub use basic::CommonCompression;

// use self::dict::Dict;

/// Compression codec
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Compression {
    None,
    LZ4,
    ZSTD,
    SNAPPY,

    // start from 10 for none common compression
    RLE,
    Dict,
}

impl Default for Compression {
    fn default() -> Self {
        Self::None
    }
}

impl Compression {
    pub fn is_none(&self) -> bool {
        matches!(self, Compression::None)
    }

    pub fn from_codec(t: u8) -> Result<Self> {
        match t {
            0 => Ok(Compression::None),
            1 => Ok(Compression::LZ4),
            2 => Ok(Compression::ZSTD),
            3 => Ok(Compression::SNAPPY),
            10 => Ok(Compression::RLE),
            11 => Ok(Compression::Dict),
            other => Err(arrow::error::Error::OutOfSpec(format!(
                "Unknown compression codec {other}",
            ))),
        }
    }

    pub fn raw_mode(&self) -> bool {
        matches!(
            self,
            Compression::None | Compression::LZ4 | Compression::ZSTD | Compression::SNAPPY
        )
    }
}

impl From<Compression> for u8 {
    fn from(value: Compression) -> Self {
        match value {
            Compression::None => 0,
            Compression::LZ4 => 1,
            Compression::ZSTD => 2,
            Compression::SNAPPY => 3,
            Compression::RLE => 10,
            Compression::Dict => 11,
        }
    }
}

#[inline]
pub(crate) fn is_valid(validity: &Option<&Bitmap>, i: usize) -> bool {
    match validity {
        Some(v) => v.get_bit(i),
        None => true,
    }
}

#[inline]
pub(crate) fn get_bits_needed(input: u64) -> u32 {
    u64::BITS - input.leading_zeros()
}

#[cfg(test)]
mod tests {}
