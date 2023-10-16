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

use arrow::array::BooleanArray;
use arrow::bitmap::MutableBitmap;

use arrow::error::Result;

use byteorder::{LittleEndian, ReadBytesExt};

use crate::compression::{integer::RLE, SAMPLE_SIZE};
use crate::compression::{Compression, SAMPLE_COUNT};

use super::{compress_sample_ratio, BooleanCompression, BooleanStats};

impl BooleanCompression for RLE {
    fn compress(&self, array: &BooleanArray, output: &mut Vec<u8>) -> Result<usize> {
        let size = output.len();
        self.compress_integer(
            output,
            array.values().iter().map(|v| v as u8),
            array.validity(),
        )?;
        Ok(output.len() - size)
    }

    fn decompress(&self, mut input: &[u8], length: usize, array: &mut MutableBitmap) -> Result<()> {
        let mut num_values = 0;
        while !input.is_empty() {
            let len: u32 = input.read_u32::<LittleEndian>()?;
            let t = input.read_u8()? != 0;
            for _ in 0..len {
                array.push(t);
            }
            num_values += len as usize;
            if num_values >= length {
                break;
            }
        }
        Ok(())
    }

    fn to_compression(&self) -> Compression {
        Compression::Rle
    }

    fn compress_ratio(&self, stats: &BooleanStats) -> f64 {
        compress_sample_ratio(self, stats, SAMPLE_COUNT, SAMPLE_SIZE)
    }
}
