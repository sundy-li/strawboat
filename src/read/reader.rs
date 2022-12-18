use super::PaReadBuf;
use super::{deserialize, read_basic::read_u32, read_basic::read_u64};
use crate::{ColumnMeta, Compression};
use arrow::datatypes::Schema;
use arrow::error::Result;
use arrow::io::ipc::read::deserialize_schema;
use arrow::{array::Array, datatypes::DataType};
use std::io::{Read, Seek, SeekFrom};

pub struct PaReader<R: PaReadBuf> {
    reader: R,
    data_type: DataType,
    is_little_endian: bool,
    compression: Compression,

    current_values: usize,
    num_values: usize,
    scratch: Vec<u8>,
}

impl<R: PaReadBuf> PaReader<R> {
    pub fn new(
        reader: R,
        data_type: DataType,
        is_little_endian: bool,
        compression: Compression,

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

pub fn read_meta<Reader: Read + Seek>(reader: &mut Reader) -> Result<Vec<ColumnMeta>> {
    // ARROW_MAGIC(6 bytes) + EOS(8 bytes) + num_cols(4 bytes) = 18 bytes
    reader.seek(SeekFrom::End(-18))?;
    let num_cols = read_u32(reader)? as usize;
    // 24 bytes per column meta
    let meta_size = num_cols * 24;
    reader.seek(SeekFrom::End(-22 - meta_size as i64))?;
    let mut metas = Vec::with_capacity(num_cols);
    let mut buf = vec![0u8; meta_size];
    reader.read_exact(&mut buf)?;
    for i in 0..num_cols {
        let start = i * 24;
        let offset = u64::from_le_bytes(<[u8; 8]>::try_from(&buf[start..start + 8]).expect(""));
        let length =
            u64::from_le_bytes(<[u8; 8]>::try_from(&buf[start + 8..start + 16]).expect(""));
        let num_values =
            u64::from_le_bytes(<[u8; 8]>::try_from(&buf[start + 16..start + 24]).expect(""));
        metas.push(ColumnMeta {
            offset,
            length,
            num_values,
        })
    }
    Ok(metas)
}

pub fn infer_schema<Reader: Read + Seek>(reader: &mut Reader) -> Result<Schema> {
    // ARROW_MAGIC(6 bytes) + EOS(8 bytes) + num_cols(4 bytes) + schema_size(4bytes) = 22 bytes
    reader.seek(SeekFrom::End(-22))?;
    let schema_size = read_u32(reader)? as usize;
    let column_meta_size = read_u32(reader)? as usize;
    reader.seek(SeekFrom::Current(
        (-(column_meta_size as i64) * 24 - (schema_size as i64) - 8) as i64,
    ))?;
    let mut schema_bytes = vec![0u8; schema_size];
    reader.read_exact(&mut schema_bytes)?;
    let (schema, _) = deserialize_schema(&schema_bytes).expect("deserialize schema error");
    return Ok(schema);
}
