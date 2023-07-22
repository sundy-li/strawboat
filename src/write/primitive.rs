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

use arrow::array::Array;
use arrow::error::Result;
use arrow::types::i256;
use arrow::{array::PrimitiveArray, types::NativeType};

use crate::compression::double::compress_double;
use crate::compression::integer::compress_integer;

use super::WriteOptions;

pub(crate) fn write_primitive<T: NativeType, W: Write>(
    w: &mut W,
    array: &PrimitiveArray<T>,
    write_options: WriteOptions,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    scratch.clear();
    // compress_integer(array, write_options, scratch)?;
    match T::PRIMITIVE {
        arrow::types::PrimitiveType::Int8 => {
            let array: &PrimitiveArray<i8> = array.as_any().downcast_ref().unwrap();
            compress_integer(array, write_options, scratch)?;
        }
        arrow::types::PrimitiveType::Int16 => {
            let array: &PrimitiveArray<i16> = array.as_any().downcast_ref().unwrap();
            compress_integer(array, write_options, scratch)?;
        }
        arrow::types::PrimitiveType::Int32 => {
            let array: &PrimitiveArray<i32> = array.as_any().downcast_ref().unwrap();
            compress_integer(array, write_options, scratch)?;
        }
        arrow::types::PrimitiveType::Int64 => {
            let array: &PrimitiveArray<i64> = array.as_any().downcast_ref().unwrap();
            compress_integer(array, write_options, scratch)?;
        }
        arrow::types::PrimitiveType::UInt8 => {
            let array: &PrimitiveArray<u8> = array.as_any().downcast_ref().unwrap();
            compress_integer(array, write_options, scratch)?;
        }
        arrow::types::PrimitiveType::UInt16 => {
            let array: &PrimitiveArray<u16> = array.as_any().downcast_ref().unwrap();
            compress_integer(array, write_options, scratch)?;
        }
        arrow::types::PrimitiveType::UInt32 => {
            let array: &PrimitiveArray<u32> = array.as_any().downcast_ref().unwrap();
            compress_integer(array, write_options, scratch)?;
        }
        arrow::types::PrimitiveType::UInt64 => {
            let array: &PrimitiveArray<u64> = array.as_any().downcast_ref().unwrap();
            compress_integer(array, write_options, scratch)?;
        }
        arrow::types::PrimitiveType::Int128 => {
            let array: &PrimitiveArray<i128> = array.as_any().downcast_ref().unwrap();
            compress_integer(array, write_options, scratch)?;
        }
        arrow::types::PrimitiveType::Int256 => {
            let array: &PrimitiveArray<i256> = array.as_any().downcast_ref().unwrap();
            compress_integer(array, write_options, scratch)?;
        }
        arrow::types::PrimitiveType::Float32 => {
            let array: &PrimitiveArray<f32> = array.as_any().downcast_ref().unwrap();

            compress_double(array, write_options, scratch)?;
        }
        arrow::types::PrimitiveType::Float64 => {
            let array: &PrimitiveArray<f64> = array.as_any().downcast_ref().unwrap();

            compress_double(array, write_options, scratch)?;
        }

        arrow::types::PrimitiveType::Float16 => unimplemented!(),
        arrow::types::PrimitiveType::DaysMs => unimplemented!(),
        arrow::types::PrimitiveType::MonthDayNano => unimplemented!(),
    }
    w.write_all(scratch.as_slice())?;
    Ok(())
}
