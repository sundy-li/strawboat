// mod bitpack;

mod basic;
mod dict;
mod rle;

use arrow::{
    array::PrimitiveArray,
    bitmap::{Bitmap, MutableBitmap},
    buffer::Buffer,
    datatypes::DataType,
    error::Result,
    types::{NativeType, Offset},
};

use crate::compression::basic::CommonCompression;

use self::{dict::Dict, rle::RLE};

/// Compression codec
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Compression {
    None,
    LZ4,
    ZSTD,
    SNAPPY,

    // start from 10 for none common compression
    RLE,
    Dict,
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
            11 => Ok(Compression::Dict),
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
            Compression::Dict => Compressor::Dict(Dict {}),
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
            Compression::Dict => 11,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Compressor {
    Basic(CommonCompression),
    RLE(RLE),
    Dict(Dict),
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
            Compressor::Dict(dict) => dict.compress_primitive_array(array, output_buf),
        }
    }

    pub fn compress_binary_array<O: Offset>(
        &self,
        offsets: &Buffer<O>,
        values: &Buffer<u8>,
        output_buf: &mut Vec<u8>,
    ) -> Result<usize> {
        match self {
            Compressor::Basic(_) => unreachable!(),
            Compressor::RLE(_) => unreachable!(),
            Compressor::Dict(dict) => dict.compress_binary_array(offsets, values, output_buf),
        }
    }

    pub fn compress_bitmap(&self, array: &Bitmap, output_buf: &mut Vec<u8>) -> Result<usize> {
        match self {
            Compressor::Basic(_) | Compressor::Dict(_) => unreachable!(),
            Compressor::RLE(rle) => rle.compress_bitmap(array, output_buf),
        }
    }

    pub fn decompress_primitive_array<T: NativeType>(
        &self,
        input: &[u8],
        length: usize,
        array: &mut Vec<T>,
    ) -> Result<()> {
        match self {
            Compressor::Basic(_) => unreachable!(),
            Compressor::RLE(rle) => rle.decompress_primitive_array(input, length, array),
            Compressor::Dict(dict) => dict.decompress_primitive_array(input, length, array),
        }
    }

    pub fn decompress_binary_array<O: Offset>(
        &self,
        input: &[u8],
        length: usize,
        offsets: &mut Vec<O>,
        values: &mut Vec<u8>,
    ) -> Result<()> {
        match self {
            Compressor::Basic(_) => unreachable!(),
            Compressor::RLE(_) => unreachable!(),
            Compressor::Dict(dict) => dict.decompress_binary_array(input, length, offsets, values),
        }
    }

    pub fn decompress_bitmap(&self, input: &[u8], array: &mut MutableBitmap) -> Result<()> {
        match self {
            Compressor::Basic(_) | Compressor::Dict(_) => unreachable!(),
            Compressor::RLE(rle) => rle.decompress_bitmap(input, array),
        }
    }

    /// if raw_mode is true, we use following methods to apply compression and decompression
    pub fn raw_mode(&self) -> bool {
        matches!(self, Compressor::Basic(_))
    }

    /// if raw_mode is true, we use following methods to apply compression and decompression
    pub fn support_datatype(&self, data_type: &DataType) -> bool {
        match self {
            Compressor::Basic(_c) => true,
            Compressor::RLE(rle) => rle.support_datatype(data_type),
            Compressor::Dict(dict) => dict.support_datatype(data_type),
        }
    }

    pub fn decompress(&self, input: &[u8], out_slice: &mut [u8]) -> Result<()> {
        match self {
            Compressor::Basic(c) => c.decompress(input, out_slice),
            _ => unreachable!(),
        }
    }

    pub fn compress(&self, input_buf: &[u8], output_buf: &mut Vec<u8>) -> Result<usize> {
        match self {
            Compressor::Basic(c) => c.compress(input_buf, output_buf),
            _ => unreachable!(),
        }
    }
}

#[inline]
pub(crate) fn is_valid(validity: &Option<&Bitmap>, i: usize) -> bool {
    match validity {
        Some(ref v) => v.get_bit(i),
        None => true,
    }
}

#[cfg(test)]
mod tests {}
