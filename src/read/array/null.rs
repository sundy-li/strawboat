use arrow::{array::NullArray, datatypes::DataType, error::Result};

pub fn read_null(data_type: DataType, length: usize) -> Result<NullArray> {
    NullArray::try_new(data_type, length)
}
