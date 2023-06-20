// mod bitpack;

mod basic;
mod rle;
// mod rle_0;

use arrow::{
    array::{PrimitiveArray},
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
        todo!("")
    }
}

impl From<Compression> for u8 {
    fn from(value: Compression) -> Self {
        match value {
            Compression::None => 0,
            Compression::LZ4 => 1,
            Compression::ZSTD => 2,
            Compression::SNAPPY => 3,
            Compression::RLE => 4,
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
        _array: &PrimitiveArray<T>,
        _output_buf: &mut Vec<u8>,
    ) -> Result<usize> {
        match self {
            Compressor::Basic(_) => unreachable!(),
            Compressor::RLE(_) => todo!(),
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
            Compressor::RLE(_) => todo!(),
        }
    }

    pub fn compress_bitmap(&self, _array: &Bitmap, _output_buf: &mut Vec<u8>) -> Result<usize> {
        match self {
            Compressor::Basic(_) => unreachable!(),
            Compressor::RLE(_) => todo!(),
        }
    }

    pub fn decompress_primitive_array<T: NativeType>(
        &self,
        _input: &[u8],
        _array: &mut Vec<T>,
    ) -> Result<()> {
        match self {
            Compressor::Basic(_) => unreachable!(),
            Compressor::RLE(_) => todo!(),
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
            Compressor::RLE(_) => todo!(),
        }
    }

    pub fn decompress_bitmap<T: NativeType>(
        &self,
        _input: &[u8],
        _array: &mut MutableBitmap,
    ) -> Result<()> {
        match self {
            Compressor::Basic(_) => unreachable!(),
            Compressor::RLE(_) => todo!(),
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
    pub fn support_datatype(&self, _data_type: &DataType) -> bool {
        match self {
            Compressor::Basic(_c) => true,
            Compressor::RLE(_) => true,
        }
    }

    pub fn decompress(&self, input: &[u8], out_slice: &mut [u8]) -> Result<()> {
        match self {
            Compressor::Basic(c) => c.decompress(input, out_slice),
            Compressor::RLE(_) => unimplemented!(),
        }
    }

    pub fn compress(&self, input_buf: &[u8], output_buf: &mut Vec<u8>) -> Result<usize> {
        match self {
            Compressor::Basic(c) => c.compress(input_buf, output_buf),
            Compressor::RLE(_) => unimplemented!(),
        }
    }
}

#[cfg(test)]
mod tests {}
