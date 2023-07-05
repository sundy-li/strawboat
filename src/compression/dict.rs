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

use arrow::bitmap::Bitmap;
use arrow::error::Error;
use std::io::BufRead;

use arrow::array::PrimitiveArray;
use arrow::buffer::Buffer;

use arrow::datatypes::{DataType, PhysicalType};
use arrow::error::Result;
use arrow::types::{NativeType, Offset};
use byteorder::{LittleEndian, ReadBytesExt};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Dict {}

impl Dict {
    pub fn compress_primitive_array<T: NativeType>(
        &self,
        array: &PrimitiveArray<T>,
        output_buf: &mut Vec<u8>,
    ) -> Result<usize> {
        let start = output_buf.len();
        let mut encoder = DictEncoder::with_capacity(array.len());
        for val in array.values().iter() {
            encoder.push(&RawNative { inner: *val });
        }
        let indices = encoder.get_indices();
        // dict data use RLE encoding
        let rle = RLE {};
        rle.encode_native(output_buf, indices.iter().cloned(), None)?;
        let sets = encoder.get_sets();
        // data page use plain encoding
        for val in sets.iter() {
            let bs = val.inner.to_le_bytes();
            output_buf.extend_from_slice(bs.as_ref());
        }

        Ok(output_buf.len() - start)
    }

    pub fn decompress_primitive_array<T: NativeType>(
        &self,
        input: &[u8],
        length: usize,
        array: &mut Vec<T>,
    ) -> Result<()> {
        let rle = RLE {};
        let mut indices: Vec<u32> = Vec::with_capacity(length);

        let input = rle.decode_native(input, length, &mut indices)?;

        let data: Vec<T> = input
            .chunks(std::mem::size_of::<T>())
            .map(|chunk| match <T::Bytes>::try_from(chunk) {
                Ok(bs) => T::from_le_bytes(bs),
                Err(_) => unreachable!(),
            })
            .collect();

        for i in indices.iter() {
            array.push(data[*i as usize]);
        }
        Ok(())
    }

    pub fn compress_binary_array<O: Offset>(
        &self,
        offsets: &Buffer<O>,
        values: &Buffer<u8>,
        validity: Option<&Bitmap>,
        output_buf: &mut Vec<u8>,
    ) -> Result<usize> {
        let start = output_buf.len();
        let mut encoder = DictEncoder::with_capacity(offsets.len() - 1);

        for (i, range) in offsets.windows(2).enumerate() {
            if !is_valid(&validity, i) && !encoder.get_indices().is_empty() {
                encoder.push_index();
            } else {
                let data = values.clone().sliced(
                    range[0].to_usize(),
                    range[1].to_usize() - range[0].to_usize(),
                );
                encoder.push(&data);
            }
        }

        let indices = encoder.get_indices();
        // dict data use RLE encoding
        let rle = RLE {};
        rle.encode_native(output_buf, indices.iter().cloned(), None)?;
        let sets = encoder.get_sets();
        // data page use plain encoding
        for val in sets.iter() {
            let bs = val.as_bytes();
            output_buf.extend_from_slice(&(bs.len() as u64).to_le_bytes());
            output_buf.extend_from_slice(bs.as_ref());
        }

        Ok(output_buf.len() - start)
    }

    pub fn decompress_binary_array<O: Offset>(
        &self,
        input: &[u8],
        length: usize,
        offsets: &mut Vec<O>,
        values: &mut Vec<u8>,
    ) -> Result<()> {
        let rle = RLE {};
        let mut indices: Vec<u32> = Vec::with_capacity(length);
        let mut input = rle.decode_native(input, length, &mut indices)?;

        let mut data: Vec<u8> = vec![];
        let mut data_offsets = vec![0];

        let mut last_offset = 0;
        for _ in 0..=*indices.iter().max().unwrap() {
            let len = input.read_u64::<LittleEndian>()? as usize;
            if input.len() < len {
                return Err(general_err!("data size is less than {}", len));
            }
            last_offset += len;
            data_offsets.push(last_offset);
            data.extend_from_slice(&input[..len]);
            input.consume(len);
        }

        last_offset = if offsets.is_empty() {
            offsets.push(O::default());
            0
        } else {
            offsets.last().unwrap().to_usize()
        };

        for i in indices.iter() {
            let off = data_offsets[*i as usize];
            let end = data_offsets[(*i + 1) as usize];

            values.extend_from_slice(&data[off..end]);

            last_offset += end - off;
            offsets.push(O::from_usize(last_offset).unwrap());
        }
        Ok(())
    }

    pub fn raw_mode(&self) -> bool {
        false
    }

    pub fn support_datatype(&self, data_type: &DataType) -> bool {
        let t = data_type.to_physical_type();
        matches!(
            t,
            PhysicalType::Primitive(_)
                | PhysicalType::Binary
                | PhysicalType::LargeBinary
                | PhysicalType::Utf8
                | PhysicalType::LargeUtf8
        )
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

    pub fn push(&mut self, value: &T) {
        let key = self.interner.entry_key(value);
        self.indices.push(key);
    }

    pub fn push_index(&mut self) {
        match self.indices.last() {
            Some(i) => self.indices.push(*i),
            None => self.indices.push(0),
        }
    }

    pub fn get_sets(&self) -> &[T] {
        &self.interner.sets
    }

    pub fn get_indices(&self) -> &[u32] {
        &self.indices
    }
}

use hashbrown::hash_map::RawEntryMut;
use hashbrown::HashMap;

use crate::general_err;

use super::is_valid;
use super::rle::RLE;

const DEFAULT_DEDUP_CAPACITY: usize = 4096;

#[derive(Debug, Default)]
pub struct DictMap<T: AsBytes> {
    state: ahash::RandomState,
    dedup: HashMap<u32, (), ()>,
    sets: Vec<T>,
}

pub trait AsBytes {
    /// Returns slice of bytes for this data type.
    fn as_bytes(&self) -> &[u8];
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

impl AsBytes for [u8] {
    fn as_bytes(&self) -> &[u8] {
        self
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

impl AsBytes for Buffer<u8> {
    fn as_bytes(&self) -> &[u8] {
        self.as_slice()
    }
}

impl AsBytes for bool {
    fn as_bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self as *const bool as *const u8, 1) }
    }
}

impl AsBytes for Vec<u8> {
    fn as_bytes(&self) -> &[u8] {
        self.as_slice()
    }
}

impl<'a> AsBytes for &'a str {
    fn as_bytes(&self) -> &[u8] {
        (self as &str).as_bytes()
    }
}

impl AsBytes for str {
    fn as_bytes(&self) -> &[u8] {
        (self as &str).as_bytes()
    }
}

#[cfg(test)]
mod tests {}
