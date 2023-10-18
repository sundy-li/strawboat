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

use arrow::error::Result;

use crate::{
    compression::{get_bits_needed, Compression},
    util::bit_pack::{
        align, block_need_bytes, need_bytes, pack32, pack64, unpack32, unpack64, BITPACK_BLOCK_SIZE,
    },
    write::WriteOptions,
};

use super::{IntegerCompression, IntegerStats, IntegerType};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Bitpacking {}

impl<T: IntegerType> IntegerCompression<T> for Bitpacking {
    fn compress(
        &self,
        array: &PrimitiveArray<T>,
        stats: &IntegerStats<T>,
        _write_options: &WriteOptions,
        output: &mut Vec<u8>,
    ) -> Result<usize> {
        let start = output.len();
        let width = get_bits_needed(stats.max.as_i64() as u64);
        let aligned_len = align(array.len(), BITPACK_BLOCK_SIZE);
        let bytes_needed = need_bytes(aligned_len, width as u8);
        output.resize(start + bytes_needed + 1, 0);
        output[start] = width as u8;
        let output_slice = &mut output[start + 1..];
        for (i_block, o_block) in array
            .values()
            .chunks_exact(BITPACK_BLOCK_SIZE)
            .zip(output_slice.chunks_mut(block_need_bytes(width as u8)))
        {
            match std::mem::size_of::<T>() {
                4 => {
                    let i_block: &[u32] = bytemuck::cast_slice(i_block);
                    pack32(i_block.try_into().unwrap(), o_block, width as usize);
                }
                8 => {
                    let i_block: &[u64] = bytemuck::cast_slice(i_block);
                    pack64(i_block.try_into().unwrap(), o_block, width as usize);
                }
                _ => unreachable!(),
            }
        }
        let remain = array.len() % BITPACK_BLOCK_SIZE;
        if remain != 0 {
            let mut i_block = array.values()[array.len() - remain..].to_vec();
            i_block.resize(BITPACK_BLOCK_SIZE, T::default());
            let o_block = &mut output_slice[bytes_needed - block_need_bytes(width as u8)..];
            match std::mem::size_of::<T>() {
                4 => {
                    let i_block: &[u32] = bytemuck::cast_slice(&i_block);
                    pack32(i_block.try_into().unwrap(), o_block, width as usize);
                }
                8 => {
                    let i_block: &[u64] = bytemuck::cast_slice(&i_block);
                    pack64(i_block.try_into().unwrap(), o_block, width as usize);
                }
                _ => unreachable!(),
            }
        }
        Ok(output.len() - start)
    }

    fn decompress(&self, input: &[u8], length: usize, output: &mut Vec<T>) -> Result<()> {
        let start = output.len();
        let width = input[0];
        output.resize(start + align(length, BITPACK_BLOCK_SIZE), T::default());
        let output_slice = &mut output[start..];
        let input = &input[1..];
        for (o_block, i_block) in output_slice
            .chunks_mut(BITPACK_BLOCK_SIZE)
            .zip(input.chunks(block_need_bytes(width)))
        {
            match std::mem::size_of::<T>() {
                4 => {
                    let o_block: &mut [u32] = bytemuck::cast_slice_mut(o_block);
                    unpack32(i_block, o_block, width as usize);
                }
                8 => {
                    let o_block: &mut [u64] = bytemuck::cast_slice_mut(o_block);
                    unpack64(i_block, o_block, width as usize);
                }
                _ => unreachable!(),
            }
        }
        output.truncate(start + length);
        Ok(())
    }

    fn to_compression(&self) -> Compression {
        Compression::Bitpacking
    }

    fn compress_ratio(&self, stats: &IntegerStats<T>) -> f64 {
        if stats.min.as_i64() < 0
            || (std::mem::size_of::<T>() != 4 && std::mem::size_of::<T>() != 8)
        {
            return 0.0f64;
        }
        let width = get_bits_needed(stats.max.as_i64() as u64);
        (std::mem::size_of::<T>() * 8) as f64 / width as f64
    }
}
