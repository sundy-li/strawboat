use arrow::error::{Error, Result};

use crate::Compression;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CommonCompression {
    None,
    LZ4,
    ZSTD,
    SNAPPY,
}

impl TryFrom<&Compression> for CommonCompression {
    type Error = Error;

    fn try_from(value: &Compression) -> Result<Self> {
        match value {
            Compression::None => Ok(CommonCompression::None),
            Compression::LZ4 => Ok(CommonCompression::LZ4),
            Compression::ZSTD => Ok(CommonCompression::ZSTD),
            Compression::SNAPPY => Ok(CommonCompression::SNAPPY),
            other => Err(Error::OutOfSpec(format!(
                "Unknown compression codec {other:?}",
            ))),
        }
    }
}

impl CommonCompression {
    pub fn decompress(&self, input: &[u8], out_slice: &mut [u8]) -> Result<()> {
        match self {
            Self::LZ4 => decompress_lz4(input, out_slice),
            Self::ZSTD => decompress_zstd(input, out_slice),
            Self::SNAPPY => decompress_snappy(input, out_slice),
            Self::None => {
                out_slice.copy_from_slice(input);
                Ok(())
            }
        }
    }

    pub fn compress(&self, input_buf: &[u8], output_buf: &mut Vec<u8>) -> Result<usize> {
        match self {
            Self::LZ4 => compress_lz4(input_buf, output_buf),
            Self::ZSTD => compress_zstd(input_buf, output_buf),
            Self::SNAPPY => compress_snappy(input_buf, output_buf),
            Self::None => {
                output_buf.extend_from_slice(input_buf);
                Ok(input_buf.len())
            }
        }
    }
}

pub fn decompress_lz4(input_buf: &[u8], output_buf: &mut [u8]) -> Result<()> {
    lz4::block::decompress_to_buffer(input_buf, Some(output_buf.len() as i32), output_buf)
        .map(|_| {})
        .map_err(|e| e.into())
}

pub fn decompress_zstd(input_buf: &[u8], output_buf: &mut [u8]) -> Result<()> {
    zstd::bulk::decompress_to_buffer(input_buf, output_buf)
        .map(|_| {})
        .map_err(|e| e.into())
}

pub fn decompress_snappy(input_buf: &[u8], output_buf: &mut [u8]) -> Result<()> {
    snap::raw::Decoder::new()
        .decompress(input_buf, output_buf)
        .map(|_| {})
        .map_err(|e| {
            arrow::error::Error::External("decompress snappy faild".to_owned(), Box::new(e))
        })
}

pub fn compress_lz4(input_buf: &[u8], output_buf: &mut Vec<u8>) -> Result<usize> {
    let bound = lz4::block::compress_bound(input_buf.len())?;
    output_buf.resize(bound, 0);
    lz4::block::compress_to_buffer(input_buf, None, false, output_buf.as_mut_slice())
        .map_err(|e| e.into())
}

pub fn compress_zstd(input_buf: &[u8], output_buf: &mut Vec<u8>) -> Result<usize> {
    let bound = zstd::zstd_safe::compress_bound(input_buf.len());
    output_buf.resize(bound, 0);
    zstd::bulk::compress_to_buffer(input_buf, output_buf.as_mut_slice(), 0).map_err(|e| e.into())
}

pub fn compress_snappy(input_buf: &[u8], output_buf: &mut Vec<u8>) -> Result<usize> {
    let bound = snap::raw::max_compress_len(input_buf.len());
    output_buf.resize(bound, 0);
    snap::raw::Encoder::new()
        .compress(input_buf, output_buf)
        .map_err(|e| {
            arrow::error::Error::External("decompress snappy faild".to_owned(), Box::new(e))
        })
}
