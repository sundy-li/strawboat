use std::convert::TryInto;
use std::io::Read;

use arrow::buffer::Buffer;
use arrow::error::Result;

use arrow::{bitmap::Bitmap, types::NativeType};

use super::super::endianess::is_native_little_endian;
use super::NativeReadBuf;
use crate::{compression, Compression};

fn read_swapped<T: NativeType, R: NativeReadBuf>(
    reader: &mut R,
    length: usize,
    buffer: &mut Vec<T>,
) -> Result<()> {
    // slow case where we must reverse bits
    let mut slice = vec![0u8; length * std::mem::size_of::<T>()];
    reader.read_exact(&mut slice)?;

    let chunks = slice.chunks_exact(std::mem::size_of::<T>());
    // machine is little endian, file is big endian
    buffer
        .as_mut_slice()
        .iter_mut()
        .zip(chunks)
        .try_for_each(|(slot, chunk)| {
            let a: T::Bytes = match chunk.try_into() {
                Ok(a) => a,
                Err(_) => unreachable!(),
            };
            *slot = T::from_le_bytes(a);
            Result::Ok(())
        })?;
    Ok(())
}

fn read_uncompressed_buffer<T: NativeType, R: NativeReadBuf>(
    reader: &mut R,
    length: usize,
) -> Result<Vec<T>> {
    // it is undefined behavior to call read_exact on un-initialized, https://doc.rust-lang.org/std/io/trait.Read.html#tymethod.read
    // see also https://github.com/MaikKlein/ash/issues/354#issue-781730580
    let mut buffer = vec![T::default(); length];

    if is_native_little_endian() {
        // fast case where we can just copy the contents
        let slice = bytemuck::cast_slice_mut(&mut buffer);
        reader.read_exact(slice)?;
    } else {
        read_swapped(reader, length, &mut buffer)?;
    }
    Ok(buffer)
}

pub fn read_buffer<T: NativeType, R: NativeReadBuf>(
    reader: &mut R,
    length: usize,
    scratch: &mut Vec<u8>,
) -> Result<Buffer<T>> {
    let compression = Compression::from_codec(read_u8(reader)?)?;
    let compressed_size = read_u32(reader)? as usize;
    let uncompressed_size = read_u32(reader)? as usize;

    if compression.is_none() {
        return Ok(read_uncompressed_buffer(reader, length)?.into());
    }
    let mut buffer = vec![T::default(); length];
    let out_slice = bytemuck::cast_slice_mut(&mut buffer);

    assert_eq!(uncompressed_size, out_slice.len());

    // already fit in buffer
    let mut use_inner = false;
    let input = if reader.buffer_bytes().len() > compressed_size {
        use_inner = true;
        reader.buffer_bytes()
    } else {
        scratch.resize(compressed_size, 0);
        reader.read_exact(scratch.as_mut_slice())?;
        scratch.as_slice()
    };

    match compression {
        Compression::LZ4 => {
            compression::decompress_lz4(&input[..compressed_size], out_slice)?;
        }
        Compression::ZSTD => {
            compression::decompress_zstd(&input[..compressed_size], out_slice)?;
        }
        Compression::SNAPPY => {
            compression::decompress_snappy(&input[..compressed_size], out_slice)?;
        }
        Compression::None => unreachable!(),
    }

    if use_inner {
        reader.consume(compressed_size);
    }

    Ok(buffer.into())
}

pub fn read_bitmap<R: NativeReadBuf>(
    reader: &mut R,
    length: usize,
    scratch: &mut Vec<u8>,
) -> Result<Bitmap> {
    let bytes = (length + 7) / 8;
    let mut buffer = vec![0u8; bytes];

    let compression = Compression::from_codec(read_u8(reader)?)?;
    let compressed_size = read_u32(reader)? as usize;
    let uncompressed_size = read_u32(reader)? as usize;

    assert_eq!(uncompressed_size, bytes);

    if compression.is_none() {
        reader
            .by_ref()
            .take(bytes as u64)
            .read_exact(buffer.as_mut_slice())?;
        return Bitmap::try_new(buffer, length);
    }

    // already fit in buffer
    let mut use_inner = false;
    let input = if reader.buffer_bytes().len() > compressed_size as usize {
        use_inner = true;
        reader.buffer_bytes()
    } else {
        scratch.resize(compressed_size, 0);
        reader.read_exact(scratch.as_mut_slice())?;
        scratch.as_slice()
    };

    match compression {
        Compression::LZ4 => {
            compression::decompress_lz4(&input[..compressed_size], &mut buffer)?;
        }
        Compression::ZSTD => {
            compression::decompress_zstd(&input[..compressed_size], &mut buffer)?;
        }
        Compression::SNAPPY => {
            compression::decompress_snappy(&input[..compressed_size], &mut buffer)?;
        }
        Compression::None => unreachable!(),
    }

    if use_inner {
        reader.consume(compressed_size);
    }

    Bitmap::try_new(buffer, length)
}

#[allow(clippy::too_many_arguments)]
pub fn read_validity<R: NativeReadBuf>(
    reader: &mut R,
    length: usize,
    scratch: &mut Vec<u8>,
) -> Result<Option<Bitmap>> {
    let has_null = read_u8(reader)?;
    if has_null > 0 {
        Ok(Some(read_bitmap(reader, length, scratch)?))
    } else {
        Ok(None)
    }
}

pub fn read_u8<R: Read>(r: &mut R) -> Result<u8> {
    let mut buf = [0; 1];
    r.read_exact(&mut buf)?;
    Ok(buf[0])
}

pub fn read_u32<R: Read>(r: &mut R) -> Result<u32> {
    let mut buf = [0; 4];
    r.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

pub fn read_u64<R: Read>(r: &mut R) -> Result<u64> {
    let mut buf = [0; 8];
    r.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
}
