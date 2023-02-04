use std::convert::TryInto;
use std::io::Read;

use super::super::endianess::is_native_little_endian;
use super::NativeReadBuf;
use crate::{compression, Compression};

use arrow::{
    bitmap::{Bitmap, MutableBitmap},
    buffer::Buffer,
    error::Result,
    io::parquet::read::{init_nested, InitNested, NestedState},
    types::NativeType,
};

use parquet2::{
    encoding::hybrid_rle::{BitmapIter, Decoder, HybridEncoded, HybridRleDecoder},
    metadata::ColumnDescriptor,
    read::levels::get_bit_width,
};

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
    let mut buf = vec![0u8; 1];
    let compression = Compression::from_codec(read_u8(reader, buf.as_mut_slice())?)?;
    let mut buf = vec![0u8; 4];
    let compressed_size = read_u32(reader, buf.as_mut_slice())? as usize;
    let uncompressed_size = read_u32(reader, buf.as_mut_slice())? as usize;

    if compression.is_none() {
        return Ok(read_uncompressed_buffer(reader, length)?.into());
    }
    let mut buffer = vec![T::default(); length];
    let out_slice = bytemuck::cast_slice_mut(&mut buffer);

    assert_eq!(uncompressed_size, out_slice.len());

    // already fit in buffer
    let mut use_inner = false;
    let input = if reader.buffer_bytes().len() >= compressed_size {
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
    let mut buf = vec![0u8; 1];
    let compression = Compression::from_codec(read_u8(reader, buf.as_mut_slice())?)?;
    let mut buf = vec![0u8; 4];
    let compressed_size = read_u32(reader, buf.as_mut_slice())? as usize;
    let uncompressed_size = read_u32(reader, buf.as_mut_slice())? as usize;

    let bytes = (length + 7) / 8;
    assert_eq!(uncompressed_size, bytes);
    let mut buffer = vec![0u8; bytes];

    if compression.is_none() {
        reader
            .by_ref()
            .take(bytes as u64)
            .read_exact(buffer.as_mut_slice())?;
        return Bitmap::try_new(buffer, length);
    }

    // already fit in buffer
    let mut use_inner = false;
    let input = if reader.buffer_bytes().len() >= compressed_size {
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

pub fn read_validity<R: NativeReadBuf>(reader: &mut R, length: usize) -> Result<Option<Bitmap>> {
    let mut buf = vec![0u8; 4];
    let def_levels_len = read_u32(reader, buf.as_mut_slice())?;
    if def_levels_len == 0 {
        return Ok(None);
    }
    let mut def_levels = vec![0u8; def_levels_len as usize];
    reader.read_exact(def_levels.as_mut_slice())?;

    let mut builder = MutableBitmap::with_capacity(length);
    let decoder = Decoder::new(def_levels.as_slice(), 1);
    for encoded in decoder {
        let encoded = encoded.unwrap();
        match encoded {
            HybridEncoded::Bitpacked(r) => {
                let bitmap_iter = BitmapIter::new(r, 0, length);
                for v in bitmap_iter {
                    unsafe { builder.push_unchecked(v) };
                }
            }
            HybridEncoded::Rle(_, _) => unreachable!(),
        }
    }
    Ok(Some(std::mem::take(&mut builder).into()))
}

pub fn read_validity_nested<R: NativeReadBuf>(
    reader: &mut R,
    length: usize,
    leaf: &ColumnDescriptor,
    init: Vec<InitNested>,
) -> Result<(NestedState, Option<Bitmap>)> {
    // If the Array is a List, additional is the length of offsets,
    // otherwise additional is equal to length.
    let mut buf = vec![0u8; 4];
    let additional = read_u32(reader, buf.as_mut_slice())?;
    let rep_levels_len = read_u32(reader, buf.as_mut_slice())?;
    let def_levels_len = read_u32(reader, buf.as_mut_slice())?;
    let max_rep_level = leaf.descriptor.max_rep_level;
    let max_def_level = leaf.descriptor.max_def_level;

    let mut rep_levels = vec![0u8; rep_levels_len as usize];
    reader.read_exact(rep_levels.as_mut_slice())?;
    let mut def_levels = vec![0u8; def_levels_len as usize];
    reader.read_exact(def_levels.as_mut_slice())?;

    let reps = HybridRleDecoder::try_new(&rep_levels, get_bit_width(max_rep_level), length)?;
    let defs = HybridRleDecoder::try_new(&def_levels, get_bit_width(max_def_level), length)?;
    let mut page_iter = reps.zip(defs).peekable();

    let mut nested = init_nested(&init, length);

    // The following code is copied from arrow2 `extend_offsets2` function.
    // https://github.com/jorgecarleitao/arrow2/blob/main/src/io/parquet/read/deserialize/nested_utils.rs#L403
    // The main purpose of this code is to caculate the `NestedState` and `Bitmap`
    // of the nested information by decode `rep_levels` and `def_levels`.
    let max_depth = nested.nested.len();

    let mut cum_sum = vec![0u32; max_depth + 1];
    for (i, nest) in nested.nested.iter().enumerate() {
        let delta = nest.is_nullable() as u32 + nest.is_repeated() as u32;
        cum_sum[i + 1] = cum_sum[i] + delta;
    }

    let mut cum_rep = vec![0u32; max_depth + 1];
    for (i, nest) in nested.nested.iter().enumerate() {
        let delta = nest.is_repeated() as u32;
        cum_rep[i + 1] = cum_rep[i] + delta;
    }

    let mut is_nullable = false;
    let mut builder = MutableBitmap::with_capacity(length);

    let mut rows = 0;
    while let Some((rep, def)) = page_iter.next() {
        let rep = rep?;
        let def = def?;
        if rep == 0 {
            rows += 1;
        }

        let mut is_required = false;
        for depth in 0..max_depth {
            let right_level = rep <= cum_rep[depth] && def >= cum_sum[depth];
            if is_required || right_level {
                let length = nested
                    .nested
                    .get(depth + 1)
                    .map(|x| x.len() as i64)
                    // the last depth is the leaf, which is always increased by 1
                    .unwrap_or(1);

                let nest = &mut nested.nested[depth];

                let is_valid = nest.is_nullable() && def > cum_sum[depth];
                nest.push(length, is_valid);
                if nest.is_required() && !is_valid {
                    is_required = true;
                } else {
                    is_required = false
                };

                if depth == max_depth - 1 {
                    // the leaf / primitive
                    is_nullable = nest.is_nullable();
                    if is_nullable {
                        let is_valid = (def != cum_sum[depth]) || !nest.is_nullable();
                        if right_level && is_valid {
                            unsafe { builder.push_unchecked(true) };
                        } else {
                            unsafe { builder.push_unchecked(false) };
                        }
                    }
                }
            }
        }

        let next_rep = *page_iter
            .peek()
            .map(|x| x.0.as_ref())
            .transpose()
            .unwrap() // todo: fix this
            .unwrap_or(&0);

        if next_rep == 0 && rows == additional {
            break;
        }
    }

    let validity = if is_nullable {
        Some(std::mem::take(&mut builder).into())
    } else {
        None
    };

    Ok((nested, validity))
}

#[inline(always)]
pub fn read_u8<R: Read>(r: &mut R, buf: &mut [u8]) -> Result<u8> {
    r.read_exact(buf)?;
    Ok(buf[0])
}

#[inline(always)]
pub fn read_u32<R: Read>(r: &mut R, buf: &mut [u8]) -> Result<u32> {
    r.read_exact(buf)?;
    Ok(u32::from_le_bytes(buf.try_into().unwrap()))
}

#[inline(always)]
pub fn read_u64<R: Read>(r: &mut R, buf: &mut [u8]) -> Result<u64> {
    r.read_exact(buf)?;
    Ok(u64::from_le_bytes(buf.try_into().unwrap()))
}
