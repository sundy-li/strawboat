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

use arrow::array::PrimitiveArray;

use arrow::error::Error;
use arrow::error::Result;
use byteorder::{LittleEndian, ReadBytesExt};

use crate::compression::get_bits_needed;
use crate::compression::integer::Dict;
use crate::compression::integer::DictEncoder;
use crate::compression::integer::RawNative;
use crate::compression::Compression;
use crate::general_err;
use crate::util::bit_pack::need_bytes;
use crate::write::WriteOptions;

use super::traits::DoubleType;
use super::DoubleCompression;
use super::DoubleStats;

impl<T: DoubleType> DoubleCompression<T> for Dict {
    fn compress(
        &self,
        array: &PrimitiveArray<T>,
        _stats: &DoubleStats<T>,
        _write_options: &WriteOptions,
        output_buf: &mut Vec<u8>,
    ) -> Result<usize> {
        let start = output_buf.len();
        let mut encoder = DictEncoder::with_capacity(array.len());
        for val in array.values().iter() {
            encoder.push(&RawNative { inner: *val });
        }

        let sets = encoder.get_sets();
        output_buf.extend_from_slice(&(sets.len() as u32).to_le_bytes());
        // data page use plain encoding
        for val in sets.iter() {
            let bs = val.inner.to_le_bytes();
            output_buf.extend_from_slice(bs.as_ref());
        }
        // dict data use custom encoding
        encoder.compress_indices(output_buf);

        Ok(output_buf.len() - start)
    }

    fn decompress(&self, mut input: &[u8], length: usize, output: &mut Vec<T>) -> Result<()> {
        let unique_num = input.read_u32::<LittleEndian>()? as usize;
        let data_size = unique_num as usize * std::mem::size_of::<T>();
        if input.len() < data_size {
            return Err(general_err!(
                "Invalid data size: {} less than {}",
                input.len(),
                data_size
            ));
        }

        let data: Vec<T> = input[0..data_size]
            .chunks(std::mem::size_of::<T>())
            .map(|chunk| match <T::Bytes>::try_from(chunk) {
                Ok(bs) => T::from_le_bytes(bs),
                Err(_e) => {
                    unreachable!()
                }
            })
            .collect();

        let indices =
            DictEncoder::<u32>::decompress_indices(&input[data_size..], length, unique_num);
        output.reserve(length);
        // TODO: optimize with simd gather
        for i in indices.iter() {
            output.push(data[*i as usize]);
        }
        Ok(())
    }

    fn to_compression(&self) -> Compression {
        Compression::Dict
    }

    fn compress_ratio(&self, stats: &super::DoubleStats<T>) -> f64 {
        #[cfg(debug_assertions)]
        {
            if option_env!("STRAWBOAT_DICT_COMPRESSION") == Some("1") {
                return f64::MAX;
            }
        }

        let after_size = stats.unique_count * std::mem::size_of::<T>()
            + need_bytes(
                stats.tuple_count,
                get_bits_needed(stats.unique_count as u64 - 1) as u8,
            );
        stats.total_bytes as f64 / after_size as f64
    }
}
