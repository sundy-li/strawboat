use crate::{ColumnMeta, PageMeta};

use super::read_basic::read_u64;
use super::PaReadBuf;
use super::{deserialize, read_basic::read_u32};
use arrow::datatypes::Schema;
use arrow::error::Result;
use arrow::io::ipc::read::deserialize_schema;
use arrow::{array::Array, datatypes::DataType};
use std::io::{Read, Seek, SeekFrom};

pub struct PaReader<R: PaReadBuf> {
    reader: R,
    data_type: DataType,
    current_page: usize,
    page_metas: Vec<PageMeta>,
    scratch: Vec<u8>,
}

impl<R: PaReadBuf> PaReader<R> {
    pub fn new(reader: R, data_type: DataType, page_metas: Vec<PageMeta>, scratch: Vec<u8>) -> Self {
        Self {
            reader,
            data_type,
            current_page: 0,
            page_metas,
            scratch,
        }
    }

    // must call after has_next
    pub fn next_array(&mut self) -> Result<Box<dyn Array>> {
        let page = &self.page_metas[self.current_page];
        let result = deserialize::read(
            &mut self.reader,
            self.data_type.clone(),
            page.num_values as usize,
            &mut self.scratch,
        )?;
        self.current_page += 1;
        Ok(result)
    }

    pub fn has_next(&self) -> bool {
        self.current_page < self.page_metas.len()
    }
}

pub fn read_meta<Reader: Read + Seek>(reader: &mut Reader) -> Result<Vec<ColumnMeta>> {
    // ARROW_MAGIC(6 bytes) + EOS(8 bytes) + meta_size(4 bytes) = 18 bytes
    reader.seek(SeekFrom::End(-18))?;
    let meta_size = read_u32(reader)? as usize;
    reader.seek(SeekFrom::End(-22 - meta_size as i64))?;

    let mut buf = vec![0u8; meta_size];
    reader.read_exact(&mut buf)?;

    let mut buf_reader = std::io::Cursor::new(buf);
        let meta_len = read_u64(&mut buf_reader)?;
        let mut metas = Vec::with_capacity(meta_len as usize);
        for _i in 0..meta_len {
            let offset = read_u64(&mut buf_reader)?;
            let page_num = read_u64(&mut buf_reader)?;
            let mut pages = Vec::with_capacity(page_num as usize);
            for _p in 0..page_num {
                let length = read_u64(&mut buf_reader)?;
                let num_values = read_u64(&mut buf_reader)?;
                pages.push(PageMeta { length, num_values });
            }
            metas.push(ColumnMeta { offset, pages })
        }
    Ok(metas)
}

pub fn infer_schema<Reader: Read + Seek>(reader: &mut Reader) -> Result<Schema> {
    // ARROW_MAGIC(6 bytes) + EOS(8 bytes) + meta_size(4 bytes) + schema_size(4bytes) = 22 bytes
    reader.seek(SeekFrom::End(-22))?;
    let schema_size = read_u32(reader)? as usize;
    let column_meta_size = read_u32(reader)? as usize;

    reader.seek(SeekFrom::Current(
        (-(column_meta_size as i64) - (schema_size as i64) - 8) as i64,
    ))?;
    let mut schema_bytes = vec![0u8; schema_size];
    reader.read_exact(&mut schema_bytes)?;
    let (schema, _) = deserialize_schema(&schema_bytes).expect("deserialize schema error");
    return Ok(schema);
}
