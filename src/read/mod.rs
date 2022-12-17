//! APIs to read Arrow's IPC format.
//!
//! The two important structs here are the [`FileReader`](reader::FileReader),
//! which provides arbitrary access to any of its messages, and the
//! [`StreamReader`](stream::StreamReader), which only supports reading
//! data in the order it was written in.

mod array;
pub mod deserialize;
mod read_basic;
use std::io::BufReader;
pub mod reader;
// pub(crate) mod file;

// mod read_basic;
// mod common;

/// Compression codec
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Compression {
    /// LZ4 (framed)
    LZ4,
    /// ZSTD
    ZSTD,
}

pub trait PaReadBuf: std::io::BufRead {
    fn buffer_bytes(&self) -> &[u8];
}

impl<R: std::io::Read> PaReadBuf for BufReader<R> {
    fn buffer_bytes(&self) -> &[u8] {
        self.buffer()
    }
}

impl<T: AsRef<[u8]>> PaReadBuf for std::io::Cursor<T> {
    fn buffer_bytes(&self) -> &[u8] {
        let len = self.position().min(self.get_ref().as_ref().len() as u64);
        &self.get_ref().as_ref()[(len as usize)..]
    }
}

impl<B: PaReadBuf + ?Sized> PaReadBuf for Box<B> {
    fn buffer_bytes(&self) -> &[u8] {
        (**self).buffer_bytes()
    }
}
