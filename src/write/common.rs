use std::io::Write;

use arrow::array::*;
use arrow::chunk::Chunk;

use crate::ColumnMeta;
use crate::Compression;
use crate::PageMeta;
use arrow::error::Result;

use super::{write, NativeWriter};

/// Options declaring the behaviour of writing to IPC
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct WriteOptions {
    /// Whether the buffers should be compressed and which codec to use.
    /// Note: to use compression the crate must be compiled with feature `io_ipc_compression`.
    pub compression: Compression,
    pub max_page_size: Option<usize>,
}

impl<W: Write> NativeWriter<W> {
    pub fn encode_chunk(&mut self, chunk: &Chunk<Box<dyn Array>>) -> Result<()> {
        let page_size = self
            .options
            .max_page_size
            .unwrap_or(chunk.len())
            .min(chunk.len());

        for array in chunk.arrays() {
            let start = self.writer.offset;
            let mut page_metas = Vec::with_capacity(array.len() / page_size);

            for offset in (0..array.len()).step_by(page_size) {
                let page_start = self.writer.offset;
                let num_values = if offset + page_size >= array.len() {
                    array.len() - offset
                } else {
                    page_size
                };
                let sub_array = array.slice(offset, num_values);
                self.write_array(sub_array.as_ref())?;

                let page_end = self.writer.offset;

                page_metas.push(PageMeta {
                    length: (page_end - page_start) as u64,
                    num_values: num_values as u64,
                });
            }

            self.metas.push(ColumnMeta {
                offset: start,
                pages: page_metas,
            })
        }
        Ok(())
    }

    pub fn write_array(&mut self, array: &dyn Array) -> Result<()> {
        write(
            &mut self.writer,
            array,
            self.options.compression,
            &mut self.scratch,
        )
    }
}
