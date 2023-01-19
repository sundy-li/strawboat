use crate::read::NativeReadBuf;
use arrow::array::BooleanArray;
use arrow::datatypes::DataType;
use arrow::error::Result;
use arrow::io::parquet::read::{InitNested, NestedState};
use parquet2::metadata::ColumnDescriptor;

use super::super::read_basic::*;

pub fn read_boolean<R: NativeReadBuf>(
    reader: &mut R,
    data_type: DataType,
    length: usize,
    scratch: &mut Vec<u8>,
) -> Result<BooleanArray> {
    let validity = read_validity(reader, length, scratch)?;
    let values = read_bitmap(reader, length, scratch)?;
    BooleanArray::try_new(data_type, values, validity)
}

pub fn read_boolean_nested<R: NativeReadBuf>(
    reader: &mut R,
    data_type: DataType,
    leaf: &ColumnDescriptor,
    init: Vec<InitNested>,
    length: usize,
    scratch: &mut Vec<u8>,
) -> Result<(NestedState, BooleanArray)> {
    let (mut nested, validity) = read_validity_nested(reader, length, leaf, init, scratch)?;
    nested.nested.pop();

    let values = read_bitmap(reader, length, scratch)?;
    let array = BooleanArray::try_new(data_type, values, validity)?;

    Ok((nested, array))
}
