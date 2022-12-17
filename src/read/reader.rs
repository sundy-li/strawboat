use super::PaReadBuf;
use super::{deserialize, read_basic::read_u32, Compression};
use arrow::error::Result;
use arrow::{array::Array, datatypes::DataType};

pub struct PaReader<R: PaReadBuf> {
    reader: R,
    data_type: DataType,
    is_little_endian: bool,
    compression: Option<Compression>,

    current_values: usize,
    num_values: usize,
    scratch: Vec<u8>,
}

impl<R: PaReadBuf> PaReader<R> {
    pub fn new(
        reader: R,
        data_type: DataType,
        is_little_endian: bool,
        compression: Option<Compression>,

        num_values: usize,
        scratch: Vec<u8>,
    ) -> Self {
        Self {
            reader,
            data_type,
            is_little_endian,
            compression,
            current_values: 0,
            num_values,
            scratch,
        }
    }

    // must call after has_next
    pub fn next_array(&mut self) -> Result<Box<dyn Array>> {
        let num_values = read_u32(&mut self.reader)? as usize;
        let result = deserialize::read(
            &mut self.reader,
            self.data_type.clone(),
            self.is_little_endian,
            self.compression.clone(),
            num_values,
            &mut self.scratch,
        )?;
        self.current_values += num_values as usize;
        Ok(result)
    }

    pub fn has_next(&self) -> bool {
        self.current_values < self.num_values
    }
}
