use crate::{ColumnMeta, PageMeta};

use super::{
    deserialize,
    read_basic::{read_u32, read_u64},
    NativeReadBuf,
};
use arrow::array::Array;
use arrow::datatypes::{DataType, Field, PhysicalType, Schema};
use arrow::error::Result;
use arrow::io::ipc::read::deserialize_schema;
use std::io::{Read, Seek, SeekFrom};

use arrow::io::parquet::read::ColumnDescriptor;

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

pub struct NativeReader<R: NativeReadBuf> {
    page_readers: Vec<R>,
    field: Field,
    is_nested: bool,
    leaves: Vec<ColumnDescriptor>,
    column_metas: Vec<ColumnMeta>,
    current_page: usize,
    scratchs: Vec<Vec<u8>>,
}

impl<R: NativeReadBuf> NativeReader<R> {
    pub fn new(
        page_readers: Vec<R>,
        field: Field,
        is_nested: bool,
        leaves: Vec<ColumnDescriptor>,
        column_metas: Vec<ColumnMeta>,
        scratchs: Vec<Vec<u8>>,
    ) -> Self {
        Self {
            page_readers,
            field,
            is_nested,
            leaves,
            column_metas,
            current_page: 0,
            scratchs,
        }
    }

    /// must call after has_next
    pub fn next_array(&mut self) -> Result<Box<dyn Array>> {
        let result = if self.is_nested {
            let page_meta = &self.column_metas[0].pages[self.current_page].clone();

            deserialize::read_simple(
                &mut self.page_readers[0],
                self.field.clone(),
                page_meta.num_values as usize,
                &mut self.scratchs[0],
            )?
        } else {
            let page_metas = &self
                .column_metas
                .iter()
                .map(|meta| meta.pages[self.current_page].clone())
                .collect::<Vec<_>>();

            let (_, array) = deserialize::read_nested(
                &mut self.page_readers,
                self.field.clone(),
                &mut self.leaves,
                page_metas.clone(),
                vec![],
                &mut self.scratchs,
            )?;
            array
        };
        self.current_page += 1;

        Ok(result)
    }

    pub fn has_next(&self) -> bool {
        self.current_page < self.column_metas[0].pages.len()
    }

    pub fn current_page(&self) -> usize {
        self.current_page
    }
}

impl<R: NativeReadBuf + std::io::Seek> NativeReader<R> {
    pub fn skip_page(&mut self) -> Result<()> {
        for (i, column_meta) in self.column_metas.iter().enumerate() {
            let page_meta = &column_meta.pages[self.current_page];
            self.page_readers[i].seek(SeekFrom::Current(page_meta.length as i64))?;
        }
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
