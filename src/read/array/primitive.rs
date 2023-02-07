use std::io::Cursor;
use std::marker::PhantomData;

use crate::read::{read_basic::*, BufReader, PageIterator};
use arrow::datatypes::DataType;
use arrow::error::Result;
use arrow::io::parquet::read::{InitNested, NestedState};
use arrow::{array::PrimitiveArray, types::NativeType};
use parquet2::metadata::ColumnDescriptor;
use std::convert::TryInto;

pub struct PrimitiveIter<I, T>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
    T: NativeType,
{
    iter: I,
    is_nullable: bool,
    data_type: DataType,
    scratch: Vec<u8>,
    _phantom: PhantomData<T>,
}

impl<I, T> PrimitiveIter<I, T>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
    T: NativeType,
{
    pub fn new(iter: I, is_nullable: bool, data_type: DataType) -> Self {
        Self {
            iter,
            is_nullable,
            data_type,
            scratch: vec![],
            _phantom: PhantomData,
        }
    }
}

impl<I, T> Iterator for PrimitiveIter<I, T>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
    T: NativeType,
    Vec<u8>: TryInto<T::Bytes>,
{
    type Item = Result<PrimitiveArray<T>>;

    fn next(&mut self) -> Option<Self::Item> {
        let (num_values, buffer) = match self.iter.next() {
            Some(Ok((num_values, buffer))) => (num_values, buffer),
            Some(Err(err)) => {
                return Some(Result::Err(err));
            }
            None => {
                return None;
            }
        };

        let length = num_values as usize;
        let mut reader = BufReader::with_capacity(buffer.len(), Cursor::new(buffer));
        let validity = if self.is_nullable {
            match read_validity(&mut reader, length) {
                Ok(validity) => validity,
                Err(err) => {
                    return Some(Result::Err(err));
                }
            }
        } else {
            None
        };
        let values = match read_buffer(&mut reader, length, &mut self.scratch) {
            Ok(values) => values,
            Err(err) => {
                return Some(Result::Err(err));
            }
        };
        let mut buffer = reader.into_inner().into_inner();
        self.iter.swap_buffer(&mut buffer);

        Some(PrimitiveArray::<T>::try_new(
            self.data_type.clone(),
            values,
            validity,
        ))
    }
}

#[derive(Debug)]
pub struct PrimitiveNestedIter<I, T>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
    T: NativeType,
{
    iter: I,
    data_type: DataType,
    leaf: ColumnDescriptor,
    init: Vec<InitNested>,
    scratch: Vec<u8>,
    _phantom: PhantomData<T>,
}

impl<I, T> PrimitiveNestedIter<I, T>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
    T: NativeType,
{
    pub fn new(
        iter: I,
        data_type: DataType,
        leaf: ColumnDescriptor,
        init: Vec<InitNested>,
    ) -> Self {
        Self {
            iter,
            data_type,
            leaf,
            init,
            scratch: vec![],
            _phantom: PhantomData,
        }
    }
}

impl<I, T> Iterator for PrimitiveNestedIter<I, T>
where
    I: Iterator<Item = Result<(u64, Vec<u8>)>> + PageIterator + Send + Sync,
    T: NativeType,
    Vec<u8>: TryInto<T::Bytes>,
{
    type Item = Result<(NestedState, PrimitiveArray<T>)>;

    fn next(&mut self) -> Option<Self::Item> {
        let (num_values, buffer) = match self.iter.next() {
            Some(Ok((num_values, buffer))) => (num_values, buffer),
            Some(Err(err)) => {
                return Some(Result::Err(err));
            }
            None => {
                return None;
            }
        };

        let length = num_values as usize;
        let mut reader = BufReader::with_capacity(buffer.len(), Cursor::new(buffer));
        let (nested, validity) =
            match read_validity_nested(&mut reader, length, &self.leaf, self.init.clone()) {
                Ok((nested, validity)) => (nested, validity),
                Err(err) => {
                    return Some(Result::Err(err));
                }
            };
        let values = match read_buffer(&mut reader, length, &mut self.scratch) {
            Ok(values) => values,
            Err(err) => {
                return Some(Result::Err(err));
            }
        };
        let mut buffer = reader.into_inner().into_inner();
        self.iter.swap_buffer(&mut buffer);

        let array = match PrimitiveArray::<T>::try_new(self.data_type.clone(), values, validity) {
            Ok(array) => array,
            Err(err) => {
                return Some(Result::Err(err));
            }
        };

        Some(Ok((nested, array)))
    }
}
