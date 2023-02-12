use crate::{ColumnMeta, PageMeta};

use super::{
    read_basic::{read_u32, read_u64},
    NativeReadBuf, PageIterator,
};
use arrow::bitmap::Bitmap;
use arrow::datatypes::{DataType, PhysicalType, Schema};
use arrow::error::Result;
use arrow::io::ipc::read::deserialize_schema;
use std::io::{Read, Seek, SeekFrom};

pub fn is_primitive(data_type: &DataType) -> bool {
    matches!(
        data_type.to_physical_type(),
        PhysicalType::Primitive(_)
            | PhysicalType::Null
            | PhysicalType::Boolean
            | PhysicalType::Utf8
            | PhysicalType::LargeUtf8
            | PhysicalType::Binary
            | PhysicalType::LargeBinary
            | PhysicalType::FixedSizeBinary
            | PhysicalType::Dictionary(_)
    )
}

#[derive(Debug)]
pub struct NativeReader<R: NativeReadBuf> {
    page_reader: R,
    page_metas: Vec<PageMeta>,
    current_page: usize,
    scratch: Vec<u8>,
    skip_pages: Option<Bitmap>,
}

impl<R: NativeReadBuf> NativeReader<R> {
    pub fn new(page_reader: R, page_metas: Vec<PageMeta>, scratch: Vec<u8>) -> Self {
        Self {
            page_reader,
            page_metas,
            current_page: 0,
            scratch,
            skip_pages: None,
        }
    }

    pub fn has_next(&self) -> bool {
        self.current_page < self.page_metas.len()
    }

    pub fn current_page(&self) -> usize {
        self.current_page
    }

    pub fn set_skip_pages(&mut self, skip_pages: Bitmap) {
        assert_eq!(self.page_metas.len(), skip_pages.len());
        self.skip_pages = Some(skip_pages)
    }
}

impl<R: NativeReadBuf> PageIterator for NativeReader<R> {
    fn swap_buffer(&mut self, scratch: &mut Vec<u8>) {
        std::mem::swap(&mut self.scratch, scratch)
    }
}

impl<R: NativeReadBuf + std::io::Seek> Iterator for NativeReader<R> {
    type Item = Result<(u64, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.current_page < self.page_metas.len() {
            if let Some(skip_pages) = &self.skip_pages {
                let is_skip = unsafe { skip_pages.get_bit_unchecked(self.current_page) };
                if is_skip {
                    if let Some(err) = self.skip_page().err() {
                        return Some(Result::Err(err));
                    }
                    continue;
                }
            }
            break;
        }
        if self.current_page == self.page_metas.len() {
            return None;
        }
        let mut buffer = std::mem::take(&mut self.scratch);
        let page_meta = &self.page_metas[self.current_page];
        buffer.resize(page_meta.length as usize, 0);
        if let Some(err) = self.page_reader.read_exact(&mut buffer).err() {
            return Some(Result::Err(err.into()));
        }
        self.current_page += 1;
        Some(Ok((page_meta.num_values, buffer)))
    }
}

impl<R: NativeReadBuf + std::io::Seek> NativeReader<R> {
    pub fn skip_page(&mut self) -> Result<()> {
        if self.current_page == self.page_metas.len() {
            return Ok(());
        }
        let page_meta = &self.page_metas[self.current_page];
        self.page_reader
            .seek(SeekFrom::Current(page_meta.length as i64))?;
        self.current_page += 1;
        Ok(())
    }
}

pub fn read_meta<Reader: Read + Seek>(reader: &mut Reader) -> Result<Vec<ColumnMeta>> {
    // EOS(8 bytes) + meta_size(4 bytes) = 12 bytes
    reader.seek(SeekFrom::End(-12))?;
    let mut buf = vec![0u8; 4];
    let meta_size = read_u32(reader, buf.as_mut_slice())? as usize;
    reader.seek(SeekFrom::End(-16 - meta_size as i64))?;

    let mut meta_buf = vec![0u8; meta_size];
    reader.read_exact(&mut meta_buf)?;

    let mut buf_reader = std::io::Cursor::new(meta_buf);
    let mut buf = vec![0u8; 8];
    let meta_len = read_u64(&mut buf_reader, buf.as_mut_slice())?;
    let mut metas = Vec::with_capacity(meta_len as usize);
    for _i in 0..meta_len {
        let offset = read_u64(&mut buf_reader, buf.as_mut_slice())?;
        let page_num = read_u64(&mut buf_reader, buf.as_mut_slice())?;
        let mut pages = Vec::with_capacity(page_num as usize);
        for _p in 0..page_num {
            let length = read_u64(&mut buf_reader, buf.as_mut_slice())?;
            let num_values = read_u64(&mut buf_reader, buf.as_mut_slice())?;

            pages.push(PageMeta { length, num_values });
        }
        metas.push(ColumnMeta { offset, pages })
    }
    Ok(metas)
}

pub fn infer_schema<Reader: Read + Seek>(reader: &mut Reader) -> Result<Schema> {
    // EOS(8 bytes) + meta_size(4 bytes) + schema_size(4bytes) = 16 bytes
    reader.seek(SeekFrom::End(-16))?;
    let mut buf = vec![0u8; 4];
    let schema_size = read_u32(reader, buf.as_mut_slice())? as usize;
    let column_meta_size = read_u32(reader, buf.as_mut_slice())? as usize;

    reader.seek(SeekFrom::Current(
        -(column_meta_size as i64) - (schema_size as i64) - 8,
    ))?;
    let mut schema_bytes = vec![0u8; schema_size];
    reader.read_exact(&mut schema_bytes)?;
    let (schema, _) = deserialize_schema(&schema_bytes).expect("deserialize schema error");
    Ok(schema)
}
