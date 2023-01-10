use crate::read::{read_basic::*, QuiverReadBuf};
use arrow::datatypes::DataType;
use arrow::error::Result;
use arrow::{array::PrimitiveArray, types::NativeType};
use std::convert::TryInto;

#[allow(clippy::too_many_arguments)]
pub fn read_primitive<T: NativeType, R: QuiverReadBuf>(
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
