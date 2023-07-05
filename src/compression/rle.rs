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

use std::io::{Read, Write};

use arrow::array::PrimitiveArray;
use arrow::bitmap::{Bitmap, MutableBitmap};
use arrow::datatypes::{DataType, PhysicalType};
use arrow::error::Result;
use arrow::types::NativeType;
use byteorder::{LittleEndian, ReadBytesExt};

use super::is_valid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RLE {}

impl RLE {
    pub fn encode_native<T: NativeType, W: Write>(
        &self,
        w: &mut W,
        values: impl IntoIterator<Item = T>,
        validity: Option<&Bitmap>,
    ) -> Result<()> {
        // help me generate RLE encode algorithm
        let mut seen_count: u32 = 0;
        let mut last_value = T::default();
        let mut all_null = true;

        for (i, item) in values.into_iter().enumerate() {
            if is_valid(&validity, i) {
                if all_null {
                    all_null = false;
                    last_value = item;

                    seen_count += 1;
                } else {
                    if last_value != item {
                        // flush  u32 cnt , value
                        w.write_all(&seen_count.to_le_bytes())?;
                        w.write_all(last_value.to_le_bytes().as_ref())?;

                        last_value = item;
                        seen_count = 1;
                    } else {
                        seen_count += 1;
                    }
                }
            } else {
                // NULL value: we merely increment the seen_count
                seen_count += 1;
            }
        }

        if seen_count != 0 {
            w.write_all(&seen_count.to_le_bytes())?;
            w.write_all(last_value.to_le_bytes().as_ref())?;
        }

        Ok(())
    }

    pub fn decode_native<'a, T: NativeType>(
        &self,
        mut input: &'a [u8],
        length: usize,
        array: &mut Vec<T>,
    ) -> Result<&'a [u8]> {
        let mut bs = vec![0u8; std::mem::size_of::<T>()];
        let mut num_values = 0;
        loop {
            let len = input.read_u32::<LittleEndian>()?;
            input.read_exact(&mut bs)?;

            let a: T::Bytes = match bs.as_slice().try_into() {
                Ok(a) => a,
                Err(_) => unreachable!(),
            };
            let t = T::from_le_bytes(a);
            for _ in 0..len {
                array.push(t);
            }

            num_values += len as usize;
            if num_values >= length {
                break;
            }
        }
        Ok(input)
    }
}

impl RLE {
    pub fn compress_primitive_array<T: NativeType>(
        &self,
        array: &PrimitiveArray<T>,
        output_buf: &mut Vec<u8>,
    ) -> Result<usize> {
        let size = output_buf.len();
        self.encode_native(output_buf, array.values().clone(), array.validity())?;
        Ok(output_buf.len() - size)
    }

    pub fn compress_bitmap(&self, array: &Bitmap, output_buf: &mut Vec<u8>) -> Result<usize> {
        let size = output_buf.len();
        self.encode_native(output_buf, array.iter().map(|v| v as u8), None)?;
        Ok(output_buf.len() - size)
    }

    pub fn decompress_primitive_array<T: NativeType>(
        &self,
        input: &[u8],
        length: usize,
        array: &mut Vec<T>,
    ) -> Result<()> {
        let _ = self.decode_native(input, length, array)?;
        Ok(())
    }

    pub fn decompress_bitmap(&self, mut input: &[u8], array: &mut MutableBitmap) -> Result<()> {
        while !input.is_empty() {
            let len: u32 = input.read_u32::<LittleEndian>()?;
            let t = input.read_u8()? != 0;
            for _ in 0..len {
                array.push(t);
            }
        }
        Ok(())
    }

    /// if raw_mode is true, we use following methods to apply compression and decompression
    pub fn raw_mode(&self) -> bool {
        false
    }

    /// if raw_mode is true, we use following methods to apply compression and decompression
    pub fn support_datatype(&self, data_type: &DataType) -> bool {
        let t = data_type.to_physical_type();
        matches!(t, PhysicalType::Primitive(_) | PhysicalType::Boolean)
    }
}
