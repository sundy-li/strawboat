use std::io::Write;

use arrow::array::*;
use arrow::chunk::Chunk;

use crate::ColumnMeta;
use crate::Compression;
use arrow::error::Result;

use super::{write, PaWriter};

/// Options declaring the behaviour of writing to IPC
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct WriteOptions {
    /// Whether the buffers should be compressed and which codec to use.
    /// Note: to use compression the crate must be compiled with feature `io_ipc_compression`.
    pub compression: Compression,
    pub max_page_size: Option<usize>,
}

impl<W: Write> PaWriter<W> {
    pub fn encode_chunk(&mut self, chunk: &Chunk<Box<dyn Array>>) -> Result<()> {
        let page_size = self
            .options
            .max_page_size
            .unwrap_or(chunk.len())
            .min(chunk.len());
        for array in chunk.arrays() {
            let start = self.writer.offset;

            for offset in (0..array.len()).step_by(page_size) {
                let length = if offset + page_size >= array.len() {
                    array.len() - offset
                } else {
                    page_size
                };
                let sub_array = array.slice(offset, length);
                self.write_array(sub_array.as_ref())?;
            }

            let end = self.writer.offset;
            self.add_meta(start, end - start, array.as_ref().len() as u64);
        }
        Ok(())
    }

    pub fn write_array(&mut self, array: &dyn Array) -> Result<()> {
        self.writer.write_all(&(array.len() as u32).to_le_bytes())?;
        write(
            &mut self.writer,
            array,
            self.options.compression,
            &mut self.scratch,
        )
    }

    pub fn add_meta(&mut self, start: u64, length: u64, num_values: u64) {
        let meta = ColumnMeta {
            offset: start,
            length,
            num_values,
        };
        self.metas.push(meta);
    }
}
