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

//! Data types that connect Parquet physical types with their Rust-specific
//! representations.
use arrow::datatypes::DataType;
use arrow::error::{Error, Result};
use bytes::Bytes;
use std::cmp::Ordering;
use std::fmt;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::str::from_utf8;

use crate::util::{bit_util::FromBytes, memory::ByteBufferPtr};

/// Rust representation for BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY Parquet physical types.
/// Value is backed by a byte buffer.
#[derive(Clone, Default)]
pub struct ByteArray {
    data: Option<ByteBufferPtr>,
}

// Special case Debug that prints out byte arrays that are valid utf8 as &str's
impl std::fmt::Debug for ByteArray {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug_struct = f.debug_struct("ByteArray");
        match self.as_utf8() {
            Ok(s) => debug_struct.field("data", &s),
            Err(_) => debug_struct.field("data", &self.data),
        };
        debug_struct.finish()
    }
}

impl PartialOrd for ByteArray {
    fn partial_cmp(&self, other: &ByteArray) -> Option<Ordering> {
        // sort nulls first (consistent with PartialCmp on Option)
        //
        // Since ByteBuffer doesn't implement PartialOrd, so can't
        // derive an implementation
        match (&self.data, &other.data) {
            (None, None) => Some(Ordering::Equal),
            (None, Some(_)) => Some(Ordering::Less),
            (Some(_), None) => Some(Ordering::Greater),
            (Some(self_data), Some(other_data)) => {
                // compare slices directly
                self_data.data().partial_cmp(other_data.data())
            }
        }
    }
}

impl ByteArray {
    /// Creates new byte array with no data set.
    #[inline]
    pub fn new() -> Self {
        ByteArray { data: None }
    }

    /// Gets length of the underlying byte buffer.
    #[inline]
    pub fn len(&self) -> usize {
        assert!(self.data.is_some());
        self.data.as_ref().unwrap().len()
    }

    /// Checks if the underlying buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns slice of data.
    #[inline]
    pub fn data(&self) -> &[u8] {
        self.data
            .as_ref()
            .expect("set_data should have been called")
            .as_ref()
    }

    /// Set data from another byte buffer.
    #[inline]
    pub fn set_data(&mut self, data: ByteBufferPtr) {
        self.data = Some(data);
    }

    /// Returns `ByteArray` instance with slice of values for a data.
    #[inline]
    pub fn slice(&self, start: usize, len: usize) -> Self {
        Self::from(
            self.data
                .as_ref()
                .expect("set_data should have been called")
                .range(start, len),
        )
    }

    pub fn as_utf8(&self) -> Result<&str> {
        self.data
            .as_ref()
            .map(|ptr| ptr.as_ref())
            .ok_or_else(|| general_err!("Can't convert empty byte array to utf8"))
            .and_then(|bytes| from_utf8(bytes).map_err(|e| e.into()))
    }
}

impl From<Vec<u8>> for ByteArray {
    fn from(buf: Vec<u8>) -> ByteArray {
        Self {
            data: Some(ByteBufferPtr::new(buf)),
        }
    }
}

impl<'a> From<&'a [u8]> for ByteArray {
    fn from(b: &'a [u8]) -> ByteArray {
        let mut v = Vec::new();
        v.extend_from_slice(b);
        Self {
            data: Some(ByteBufferPtr::new(v)),
        }
    }
}

impl<'a> From<&'a str> for ByteArray {
    fn from(s: &'a str) -> ByteArray {
        let mut v = Vec::new();
        v.extend_from_slice(s.as_bytes());
        Self {
            data: Some(ByteBufferPtr::new(v)),
        }
    }
}

impl From<ByteBufferPtr> for ByteArray {
    fn from(ptr: ByteBufferPtr) -> ByteArray {
        Self { data: Some(ptr) }
    }
}

impl From<Bytes> for ByteArray {
    fn from(value: Bytes) -> Self {
        ByteBufferPtr::from(value).into()
    }
}

impl PartialEq for ByteArray {
    fn eq(&self, other: &ByteArray) -> bool {
        match (&self.data, &other.data) {
            (Some(d1), Some(d2)) => d1.as_ref() == d2.as_ref(),
            (None, None) => true,
            _ => false,
        }
    }
}

impl fmt::Display for ByteArray {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self.data())
    }
}

/// Converts an instance of data type to a slice of bytes as `u8`.
pub trait AsBytes {
    /// Returns slice of bytes for this data type.
    fn as_bytes(&self) -> &[u8];
}

/// Converts an slice of a data type to a slice of bytes.
pub trait SliceAsBytes: Sized {
    /// Returns slice of bytes for a slice of this data type.
    fn slice_as_bytes(self_: &[Self]) -> &[u8];
    /// Return the internal representation as a mutable slice
    ///
    /// # Safety
    /// If modified you are _required_ to ensure the internal representation
    /// is valid and correct for the actual raw data
    unsafe fn slice_as_bytes_mut(self_: &mut [Self]) -> &mut [u8];
}

impl AsBytes for [u8] {
    fn as_bytes(&self) -> &[u8] {
        self
    }
}

macro_rules! gen_as_bytes {
    ($source_ty:ident) => {
        impl AsBytes for $source_ty {
            #[allow(clippy::size_of_in_element_count)]
            fn as_bytes(&self) -> &[u8] {
                unsafe {
                    std::slice::from_raw_parts(
                        self as *const $source_ty as *const u8,
                        std::mem::size_of::<$source_ty>(),
                    )
                }
            }
        }

        impl SliceAsBytes for $source_ty {
            #[inline]
            #[allow(clippy::size_of_in_element_count)]
            fn slice_as_bytes(self_: &[Self]) -> &[u8] {
                unsafe {
                    std::slice::from_raw_parts(
                        self_.as_ptr() as *const u8,
                        std::mem::size_of::<$source_ty>() * self_.len(),
                    )
                }
            }

            #[inline]
            #[allow(clippy::size_of_in_element_count)]
            unsafe fn slice_as_bytes_mut(self_: &mut [Self]) -> &mut [u8] {
                std::slice::from_raw_parts_mut(
                    self_.as_mut_ptr() as *mut u8,
                    std::mem::size_of::<$source_ty>() * self_.len(),
                )
            }
        }
    };
}

gen_as_bytes!(i8);
gen_as_bytes!(i16);
gen_as_bytes!(i32);
gen_as_bytes!(i64);
gen_as_bytes!(u8);
gen_as_bytes!(u16);
gen_as_bytes!(u32);
gen_as_bytes!(u64);
gen_as_bytes!(f32);
gen_as_bytes!(f64);

macro_rules! unimplemented_slice_as_bytes {
    ($ty: ty) => {
        impl SliceAsBytes for $ty {
            fn slice_as_bytes(_self: &[Self]) -> &[u8] {
                unimplemented!()
            }

            unsafe fn slice_as_bytes_mut(_self: &mut [Self]) -> &mut [u8] {
                unimplemented!()
            }
        }
    };
}

// TODO - Can Int96 and bool be implemented in these terms?
unimplemented_slice_as_bytes!(bool);
unimplemented_slice_as_bytes!(ByteArray);

impl AsBytes for bool {
    fn as_bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self as *const bool as *const u8, 1) }
    }
}

impl AsBytes for ByteArray {
    fn as_bytes(&self) -> &[u8] {
        self.data()
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

pub(crate) mod private {
    use crate::util::bit_util::{read_num_bytes, BitReader, BitWriter};
    use crate::util::memory::ByteBufferPtr;
    use arrow::error::Error;
    use arrow::error::Result;

    use std::convert::TryInto;

    use super::SliceAsBytes;

    /// Sealed trait to start to remove specialisation from implementations
    ///
    /// This is done to force the associated value type to be unimplementable outside of this
    /// crate, and thus hint to the type system (and end user) traits are public for the contract
    /// and not for extension.
    pub trait PhysicalType:
        PartialEq
        + std::fmt::Debug
        + std::fmt::Display
        + Default
        + Clone
        + super::AsBytes
        + super::FromBytes
        + SliceAsBytes
        + PartialOrd
        + Send
    {
        /// Encode the value directly from a higher level encoder
        fn encode<W: std::io::Write>(
            values: &[Self],
            writer: &mut W,
            bit_writer: &mut BitWriter,
        ) -> Result<()>;

        /// Return the encoded size for a type
        fn dict_encoding_size(&self) -> (usize, usize) {
            (std::mem::size_of::<Self>(), 1)
        }

        /// Return the value as i64 if possible
        ///
        /// This is essentially the same as `std::convert::TryInto<i64>` but can
        /// implemented for `f32` and `f64`, types that would fail orphan rules
        fn as_i64(&self) -> Result<i64> {
            Err(general_err!("Type cannot be converted to i64"))
        }

        /// Return the value as u64 if possible
        ///
        /// This is essentially the same as `std::convert::TryInto<u64>` but can
        /// implemented for `f32` and `f64`, types that would fail orphan rules
        fn as_u64(&self) -> Result<u64> {
            self.as_i64()
                .map_err(|_| general_err!("Type cannot be converted to u64"))
                .map(|x| x as u64)
        }

        /// Return the value as an Any to allow for downcasts without transmutation
        fn as_any(&self) -> &dyn std::any::Any;

        /// Return the value as an mutable Any to allow for downcasts without transmutation
        fn as_mut_any(&mut self) -> &mut dyn std::any::Any;
    }

    impl PhysicalType for bool {
        #[inline]
        fn encode<W: std::io::Write>(
            values: &[Self],
            _: &mut W,
            bit_writer: &mut BitWriter,
        ) -> Result<()> {
            for value in values {
                bit_writer.put_value(*value as u64, 1)
            }
            Ok(())
        }

        #[inline]
        fn as_i64(&self) -> Result<i64> {
            Ok(*self as i64)
        }

        #[inline]
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        #[inline]
        fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
            self
        }
    }

    macro_rules! impl_from_raw {
        ($ty: ty, $self: ident => $as_i64: block) => {
            impl PhysicalType for $ty {

                #[inline]
                fn encode<W: std::io::Write>(values: &[Self], writer: &mut W, _: &mut BitWriter) -> Result<()> {
                    let raw = unsafe {
                        std::slice::from_raw_parts(
                            values.as_ptr() as *const u8,
                            std::mem::size_of::<$ty>() * values.len(),
                        )
                    };
                    writer.write_all(raw)?;

                    Ok(())
                }


                #[inline]
                fn as_i64(&$self) -> Result<i64> {
                    $as_i64
                }

                #[inline]
                fn as_any(&self) -> &dyn std::any::Any {
                    self
                }

                #[inline]
                fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
                    self
                }
            }
        }
    }

    impl_from_raw!(i32, self => { Ok(*self as i64) });
    impl_from_raw!(i64, self => { Ok(*self) });
    impl_from_raw!(f32, self => { Err(general_err!("Type cannot be converted to i64")) });
    impl_from_raw!(f64,  self => { Err(general_err!("Type cannot be converted to i64")) });

    impl PhysicalType for super::ByteArray {
        #[inline]
        fn encode<W: std::io::Write>(
            values: &[Self],
            writer: &mut W,
            _: &mut BitWriter,
        ) -> Result<()> {
            for value in values {
                let len: u32 = value.len().try_into().unwrap();
                writer.write_all(&len.to_ne_bytes())?;
                let raw = value.data();
                writer.write_all(raw)?;
            }
            Ok(())
        }

        #[inline]
        fn dict_encoding_size(&self) -> (usize, usize) {
            (std::mem::size_of::<u32>(), self.len())
        }

        #[inline]
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        #[inline]
        fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
            self
        }
    }
}
/// Contains the Parquet physical type information as well as the Rust primitive type
/// presentation.
pub trait ValueType: 'static + Send {
    type T: private::PhysicalType;

    fn get_data_type() -> DataType;

    /// Returns size in bytes for Rust representation of the physical type.
    fn get_type_size() -> usize;
}

// Workaround bug in specialization
pub trait SliceAsBytesDataType: ValueType
where
    Self::T: SliceAsBytes,
{
}

impl<T> SliceAsBytesDataType for T
where
    T: ValueType,
    <T as ValueType>::T: SliceAsBytes,
{
}

macro_rules! make_type {
    ($name:ident, $data_type: expr, $native_ty:ty, $size:expr) => {
        #[derive(Clone)]
        pub struct $name {}

        impl ValueType for $name {
            type T = $native_ty;

            fn get_data_type() -> DataType {
                $data_type
            }

            fn get_type_size() -> usize {
                $size
            }
        }
    };
}

// Generate struct definitions for all physical types

make_type!(BoolType, DataType::Boolean, bool, 1);
make_type!(Int32Type, DataType::Int32, i32, 4);
make_type!(Int64Type, DataType::Int64, i64, 8);
make_type!(FloatType, DataType::Float32, f32, 4);
make_type!(DoubleType, DataType::Float64, f64, 8);
make_type!(
    ByteArrayType,
    DataType::Binary,
    ByteArray,
    mem::size_of::<ByteArray>()
);

impl AsRef<[u8]> for ByteArray {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_byte_array_from() {
        assert_eq!(
            ByteArray::from(vec![b'A', b'B', b'C']).data(),
            &[b'A', b'B', b'C']
        );
        assert_eq!(ByteArray::from("ABC").data(), &[b'A', b'B', b'C']);
        assert_eq!(
            ByteArray::from(ByteBufferPtr::new(vec![1u8, 2u8, 3u8, 4u8, 5u8])).data(),
            &[1u8, 2u8, 3u8, 4u8, 5u8]
        );
        let buf = vec![6u8, 7u8, 8u8, 9u8, 10u8];
        assert_eq!(ByteArray::from(buf).data(), &[6u8, 7u8, 8u8, 9u8, 10u8]);
    }

    #[test]
    fn test_byte_array_ord() {
        let ba1 = ByteArray::from(vec![1, 2, 3]);
        let ba11 = ByteArray::from(vec![1, 2, 3]);
        let ba2 = ByteArray::from(vec![3, 4]);
        let ba3 = ByteArray::from(vec![1, 2, 4]);
        let ba4 = ByteArray::from(vec![]);
        let ba5 = ByteArray::from(vec![2, 2, 3]);

        assert!(ba1 < ba2);
        assert!(ba3 > ba1);
        assert!(ba1 > ba4);
        assert_eq!(ba1, ba11);
        assert!(ba5 > ba1);
    }
}
