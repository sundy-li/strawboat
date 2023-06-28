// mod bitpack;

mod basic;
mod rle;
// mod rle_0;

use arrow::{
    array::PrimitiveArray,
    bitmap::{Bitmap, MutableBitmap},
    datatypes::DataType,
    error::Result,
    types::{NativeType, Offset},
};

use crate::compression::basic::CommonCompression;

use self::rle::RLE;

/// Compression codec
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Compression {
    None,
    LZ4,
    ZSTD,
    SNAPPY,

    // start from 10 for none common compression
    RLE,
}

impl Default for Compression {
    fn default() -> Self {
        Self::None
    }
}

impl Compression {
    pub fn is_none(&self) -> bool {
        matches!(self, Compression::None)
    }

    pub fn from_codec(t: u8) -> Result<Self> {
        match t {
            0 => Ok(Compression::None),
            1 => Ok(Compression::LZ4),
            2 => Ok(Compression::ZSTD),
            3 => Ok(Compression::SNAPPY),
            10 => Ok(Compression::RLE),
            other => Err(arrow::error::Error::OutOfSpec(format!(
                "Unknown compression codec {other}",
            ))),
        }
    }

    pub fn raw_mode(&self) -> bool {
        matches!(
            self,
            Compression::None | Compression::LZ4 | Compression::ZSTD | Compression::SNAPPY
        )
    }

    pub fn create_compressor(&self) -> Compressor {
        if let Ok(c) = CommonCompression::try_from(self) {
            return Compressor::Basic(c);
        }
        match self {
            Compression::RLE => Compressor::RLE(RLE {}),
            _ => unreachable!(),
        }
    }
}

impl From<Compression> for u8 {
    fn from(value: Compression) -> Self {
        match value {
            Compression::None => 0,
            Compression::LZ4 => 1,
            Compression::ZSTD => 2,
            Compression::SNAPPY => 3,
            Compression::RLE => 10,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Compressor {
    Basic(CommonCompression),
    RLE(RLE),
}

impl Compressor {
    pub fn compress_primitive_array<T: NativeType>(
        &self,
        array: &PrimitiveArray<T>,
        output_buf: &mut Vec<u8>,
    ) -> Result<usize> {
        match self {
            Compressor::Basic(_) => unreachable!(),
            Compressor::RLE(rle) => rle.compress_primitive_array(array, output_buf),
        }
    }

    pub fn compress_binary_array<O: Offset>(
        &self,
        _offsets: &[O],
        _values: &[u8],
        _output_buf: &mut Vec<u8>,
    ) -> Result<usize> {
        match self {
            Compressor::Basic(_) => unreachable!(),
            Compressor::RLE(_) => unreachable!(),
        }
    }

    pub fn compress_bitmap(&self, array: &Bitmap, output_buf: &mut Vec<u8>) -> Result<usize> {
        match self {
            Compressor::Basic(_) => unreachable!(),
            Compressor::RLE(rle) => rle.compress_bitmap(array, output_buf),
        }
    }

    pub fn decompress_primitive_array<T: NativeType>(
        &self,
        input: &[u8],
        array: &mut Vec<T>,
    ) -> Result<()> {
        match self {
            Compressor::Basic(_) => unreachable!(),
            Compressor::RLE(rle) => rle.decompress_primitive_array(input, array),
        }
    }

    pub fn decompress_binary_array<O: Offset>(
        &self,
        _input: &[u8],
        _offsets: &mut Vec<O>,
        _values: &mut Vec<u8>,
    ) -> Result<()> {
        match self {
            Compressor::Basic(_) => unreachable!(),
            Compressor::RLE(_) => unreachable!(),
        }
    }

    pub fn decompress_bitmap(&self, input: &[u8], array: &mut MutableBitmap) -> Result<()> {
        match self {
            Compressor::Basic(_) => unreachable!(),
            Compressor::RLE(rle) => rle.decompress_bitmap(input, array),
        }
    }

    /// if raw_mode is true, we use following methods to apply compression and decompression
    pub fn raw_mode(&self) -> bool {
        match self {
            Compressor::Basic(_) => true,
            Compressor::RLE(_) => false,
        }
    }

    /// if raw_mode is true, we use following methods to apply compression and decompression
    pub fn support_datatype(&self, data_type: &DataType) -> bool {
        match self {
            Compressor::Basic(_c) => true,
            Compressor::RLE(rle) => rle.support_datatype(data_type),
        }
    }

    pub fn decompress(&self, input: &[u8], out_slice: &mut [u8]) -> Result<()> {
        match self {
            Compressor::Basic(c) => c.decompress(input, out_slice),
            Compressor::RLE(_) => unreachable!(),
        }
    }

    pub fn compress(&self, input_buf: &[u8], output_buf: &mut Vec<u8>) -> Result<usize> {
        match self {
            Compressor::Basic(c) => c.compress(input_buf, output_buf),
            Compressor::RLE(_) => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {}
