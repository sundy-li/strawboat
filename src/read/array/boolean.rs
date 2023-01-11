use crate::read::NativeReadBuf;
use arrow::array::BooleanArray;
use arrow::datatypes::DataType;
use arrow::error::Result;

use super::super::read_basic::*;

#[allow(clippy::too_many_arguments)]
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
