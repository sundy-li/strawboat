use crate::read::{read_basic::*, NativeReadBuf};
use arrow::datatypes::DataType;
use arrow::error::Result;
use arrow::io::parquet::read::{InitNested, NestedState};
use arrow::{array::PrimitiveArray, types::NativeType};
use parquet2::metadata::ColumnDescriptor;
use std::convert::TryInto;

pub fn read_primitive<T: NativeType, R: NativeReadBuf>(
    reader: &mut R,
    data_type: DataType,
    length: usize,
    scratch: &mut Vec<u8>,
) -> Result<PrimitiveArray<T>>
where
    Vec<u8>: TryInto<T::Bytes>,
{
    let validity = read_validity(reader, length, scratch)?;

    let values = read_buffer(reader, length, scratch)?;
    PrimitiveArray::<T>::try_new(data_type, values, validity)
}

pub fn read_primitive_nested<T: NativeType, R: NativeReadBuf>(
    reader: &mut R,
    data_type: DataType,
    leaf: &ColumnDescriptor,
    init: Vec<InitNested>,
    length: usize,
    scratch: &mut Vec<u8>,
) -> Result<(NestedState, PrimitiveArray<T>)>
where
    Vec<u8>: TryInto<T::Bytes>,
{
    let (mut nested, validity) = read_validity_nested(reader, length, leaf, init, scratch)?;
    nested.nested.pop();

    let values = read_buffer(reader, length, scratch)?;
    let array = PrimitiveArray::<T>::try_new(data_type, values, validity)?;

    Ok((nested, array))
}
