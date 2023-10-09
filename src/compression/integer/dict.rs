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
use arrow::types::NativeType;
use byteorder::{LittleEndian, ReadBytesExt};
use std::hash::Hash;

use super::IntegerCompression;
use super::IntegerStats;
use super::IntegerType;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Dict {}
//TODO: reduce code duplication with src/compression/double/dict.rs
impl<T: IntegerType> IntegerCompression<T> for Dict {
    fn compress(
        &self,
        array: &PrimitiveArray<T>,
        _stats: &IntegerStats<T>,
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

    fn compress_ratio(&self, stats: &super::IntegerStats<T>) -> f64 {
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

/// Dictionary encoder.
/// The dictionary encoding builds a dictionary of values encountered in a given column.
/// The dictionary page is written first, before the data pages of the column chunk.
///
/// Dictionary page format: the entries in the dictionary - in dictionary order -
/// using the plain encoding.
///
/// Data page format: the bit width used to encode the entry ids stored as 1 byte
/// (max bit width = 32), followed by the values encoded using RLE/Bit packed described
/// above (with the given bit width).
pub struct DictEncoder<T: AsBytes> {
    interner: DictMap<T>,
    indices: Vec<u32>,
}

impl<T> DictEncoder<T>
where
    T: AsBytes + PartialEq + Clone,
{
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            interner: DictMap::new(),
            indices: Vec::with_capacity(capacity),
        }
    }

    #[cfg(test)]
    pub fn new(indices: Vec<u32>, sets: Vec<T>) -> Self {
        Self {
            interner: DictMap {
                state: Default::default(),
                dedup: HashMap::with_capacity_and_hasher(DEFAULT_DEDUP_CAPACITY, ()),
                sets,
            },
            indices,
        }
    }

    pub fn push(&mut self, value: &T) {
        let key = self.interner.entry_key(value);
        self.indices.push(key);
    }

    pub fn push_last_index(&mut self) {
        self.indices.push(self.indices.last().cloned().unwrap());
    }

    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }

    pub fn get_sets(&self) -> &[T] {
        &self.interner.sets
    }
    
    pub fn compress_indices(&self, output: &mut Vec<u8>) {
        let len = output.len();
        let width = get_bits_needed(self.interner.sets.len() as u64 - 1);
        let bytes_needed = need_bytes(self.indices.len(), width as u8);
        output.resize(len + bytes_needed, 0); //TODO:can be uninitialized
        let output = &mut output[len..];
        for (i_block, o_block) in self
            .indices
            .chunks(BITPACK_BLOCK_SIZE)
            .zip(output.chunks_mut(block_need_bytes(width as u8)))
        {
            pack32(i_block.try_into().unwrap(), o_block, width as usize);
        }
    }

    pub fn decompress_indices(input: &[u8], length: usize, unique_num: usize) -> Vec<u32> {
        let width = get_bits_needed(unique_num as u64 - 1);
        let mut indices = vec![0u32; length]; //TODO:can be uninitialized
        for (o_block, i_block) in indices
            .chunks_mut(BITPACK_BLOCK_SIZE)
            .zip(input.chunks(block_need_bytes(width as u8)))
        {
            unpack32(i_block, o_block, width as usize);
        }
        indices
    }
}

use hashbrown::hash_map::RawEntryMut;
use hashbrown::HashMap;

use crate::compression::{get_bits_needed, Compression};

use crate::general_err;
use crate::util::bit_pack::block_need_bytes;
use crate::util::bit_pack::need_bytes;
use crate::util::bit_pack::pack32;
use crate::util::bit_pack::unpack32;
use crate::util::bit_pack::BITPACK_BLOCK_SIZE;
use crate::util::AsBytes;
use crate::write::WriteOptions;

const DEFAULT_DEDUP_CAPACITY: usize = 4096;

#[derive(Debug, Default)]
pub struct DictMap<T: AsBytes> {
    state: ahash::RandomState,
    dedup: HashMap<u32, (), ()>,
    sets: Vec<T>,
}

impl<T> DictMap<T>
where
    T: AsBytes + PartialEq + Clone,
{
    pub fn new() -> Self {
        Self {
            state: Default::default(),
            dedup: HashMap::with_capacity_and_hasher(DEFAULT_DEDUP_CAPACITY, ()),
            sets: vec![],
        }
    }

    pub fn entry_key(&mut self, value: &T) -> u32 {
        let hash = self.state.hash_one(value.as_bytes());

        let entry = self
            .dedup
            .raw_entry_mut()
            .from_hash(hash, |index| value == &self.sets[*index as usize]);

        match entry {
            RawEntryMut::Occupied(entry) => *entry.into_key(),
            RawEntryMut::Vacant(entry) => {
                let key = self.sets.len() as u32;
                self.sets.push(value.clone());
                *entry
                    .insert_with_hasher(hash, key, (), |key| {
                        self.state.hash_one(self.sets[*key as usize].as_bytes())
                    })
                    .0
            }
        }
    }
}

#[repr(C)]
#[derive(Clone, PartialEq)]
pub struct RawNative<T: NativeType> {
    pub(crate) inner: T,
}

impl<T: NativeType> AsBytes for RawNative<T> {
    fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self as *const RawNative<T> as *const u8,
                std::mem::size_of::<T>(),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_compress_indices() {
        let indices = (0u32..256).collect::<Vec<_>>();
        let indices = indices
            .iter()
            .cycle()
            .take(2 * 256)
            .cloned()
            .collect::<Vec<_>>();
        let sets = vec![0; 256];
        let unique_num = sets.len();
        let encoder = DictEncoder::<u32>::new(indices.clone(), sets);
        let mut compressed = vec![];
        encoder.compress_indices(&mut compressed);
        let decompressed =
            DictEncoder::<u32>::decompress_indices(&compressed, indices.len(), unique_num);
        assert_eq!(indices, decompressed);
    }
}
