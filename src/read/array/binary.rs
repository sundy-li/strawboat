use crate::read::PaReadBuf;
use arrow::array::BinaryArray;
use arrow::buffer::Buffer;
use arrow::datatypes::DataType;
use arrow::error::Result;
use arrow::offset::OffsetsBuffer;
use arrow::types::Offset;

use super::super::read_basic::*;

#[allow(clippy::too_many_arguments)]
pub fn read_binary<O: Offset, R: PaReadBuf>(
    reader: &mut R,
    data_type: DataType,

    length: usize,
    scratch: &mut Vec<u8>,
) -> Result<BinaryArray<O>> {
    let validity = read_validity(reader, length, scratch)?;

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
