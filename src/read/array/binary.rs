use crate::read::NativeReadBuf;
use arrow::array::BinaryArray;
use arrow::buffer::Buffer;
use arrow::datatypes::DataType;
use arrow::error::Result;
use arrow::io::parquet::read::{InitNested, NestedState};
use arrow::offset::OffsetsBuffer;
use arrow::types::Offset;
use parquet2::metadata::ColumnDescriptor;

use super::super::read_basic::*;

pub fn read_binary<O: Offset, R: NativeReadBuf>(
    reader: &mut R,
    is_nullable: bool,
    data_type: DataType,
    length: usize,
    scratch: &mut Vec<u8>,
) -> Result<BinaryArray<O>> {
    let validity = if is_nullable {
        read_validity(reader, length)?
    } else {
        None
    };

    let offsets: Buffer<O> = read_buffer(reader, 1 + length, scratch)?;
    let last_offset = offsets.last().unwrap().to_usize();
    let values = read_buffer(reader, last_offset, scratch)?;

    BinaryArray::<O>::try_new(
        data_type,
        unsafe { OffsetsBuffer::new_unchecked(offsets) },
        values,
        validity,
    )
}

pub fn read_binary_nested<O: Offset, R: NativeReadBuf>(
    reader: &mut R,
    data_type: DataType,
    leaf: &ColumnDescriptor,
    init: Vec<InitNested>,
    length: usize,
    scratch: &mut Vec<u8>,
) -> Result<(NestedState, BinaryArray<O>)> {
    let (mut nested, validity) = read_validity_nested(reader, length, leaf, init)?;
    nested.nested.pop();

    let offsets: Buffer<O> = read_buffer(reader, 1 + length, scratch)?;
    let last_offset = offsets.last().unwrap().to_usize();
    let values = read_buffer(reader, last_offset, scratch)?;

    let array = BinaryArray::<O>::try_new(
        data_type,
        unsafe { OffsetsBuffer::new_unchecked(offsets) },
        values,
        validity,
    )?;
    Ok((nested, array))
}
