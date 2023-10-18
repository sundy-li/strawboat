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

use crate::{compression::Compression, util::AsBytes, write::WriteOptions};

use super::{
    compress_integer, decompress_integer, IntegerCompression, IntegerStats, IntegerType, RawNative,
};
use arrow::error::Result;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
/// Delta is a data preparation codec, used for better compression of sorted data.
pub struct Delta;

impl<T: IntegerType> IntegerCompression<T> for Delta {
    fn compress(
        &self,
        array: &PrimitiveArray<T>,
        _stats: &IntegerStats<T>,
        write_options: &WriteOptions,
        output: &mut Vec<u8>,
    ) -> Result<usize> {
        assert!(array.len() > 0);
        let start = output.len();
        let mut delta = vec![];
        delta.reserve(std::mem::size_of::<T>() * (array.len() - 1));
        output.extend_from_slice(
            RawNative {
                inner: array.value(0),
            }
            .as_bytes(),
        );
        for i in 1..array.len() {
            delta.push(array.value(i).sub(&array.value(i - 1)));
        }
        let delta = PrimitiveArray::from_vec(delta);
        // Delta doesn't make data smaller, must be used along with other codecs
        // Note that we don't need to forbid delta here
        compress_integer(&delta, write_options.clone(), output)?;
        Ok(output.len() - start)
    }

    fn decompress(&self, mut input: &[u8], length: usize, output: &mut Vec<T>) -> Result<()> {
        let start = output.len();
        output.reserve(length);
        let first_value = match <T::Bytes>::try_from(&input[0..std::mem::size_of::<T>()]) {
            Ok(bytes) => T::from_le_bytes(bytes),
            Err(_) => unreachable!(),
        };
        output.push(first_value);
        input = &input[std::mem::size_of::<T>()..];
        let mut delta: Vec<T> = vec![];
        decompress_integer(&mut input, length - 1, &mut delta, &mut vec![])?;
        for i in 0..delta.len() {
            output.push(output[i + start].add(&delta[i]));
        }
        Ok(())
    }

    fn to_compression(&self) -> Compression {
        Compression::Delta
    }

    fn compress_ratio(&self, stats: &IntegerStats<T>) -> f64 {
        if std::mem::size_of::<T>() != 32 && stats.is_sorted && stats.tuple_count > 1 {
            f64::MAX
        } else {
            0.0
        }
    }
}
