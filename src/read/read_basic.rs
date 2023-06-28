use std::convert::TryInto;
use std::io::Read;

use crate::compression::Compressor;
use crate::Compression;

use super::NativeReadBuf;

use arrow::{
    bitmap::{Bitmap, MutableBitmap},
    error::Result,
    io::parquet::read::{init_nested, InitNested, NestedState},
    types::NativeType,
};

use parquet2::{
    encoding::hybrid_rle::{BitmapIter, Decoder, HybridEncoded, HybridRleDecoder},
    metadata::ColumnDescriptor,
    read::levels::get_bit_width,
};

pub fn read_raw_slice<R: NativeReadBuf>(
    reader: &mut R,
    compressor: &Compressor,
    compressed_size: usize,
    scratch: &mut Vec<u8>,
    out_slice: &mut [u8],
) -> Result<()> {
    // already fit in buffer
    let mut use_inner = false;
    reader.fill_buf()?;

    let input = if reader.buffer_bytes().len() >= compressed_size {
        use_inner = true;
        reader.buffer_bytes()
    } else {
        scratch.resize(compressed_size, 0);
        reader.read_exact(scratch.as_mut_slice())?;
        scratch.as_slice()
    };

    compressor.decompress(&input[..compressed_size], out_slice)?;
    if use_inner {
        reader.consume(compressed_size);
    }
    Ok(())
}

pub fn read_buffer<T: NativeType, R: NativeReadBuf>(
    reader: &mut R,
    length: usize,
    scratch: &mut Vec<u8>,
    out_buf: &mut Vec<T>,
) -> Result<()> {
    let mut buf = vec![0u8; 1];
    let compression = Compression::from_codec(read_u8(reader, buf.as_mut_slice())?)?;
    let mut buf = vec![0u8; 4];
    let compressed_size = read_u32(reader, buf.as_mut_slice())? as usize;
    let uncompressed_size = read_u32(reader, buf.as_mut_slice())? as usize;

    let compressor = compression.create_compressor();

    if compressor.raw_mode() {
        out_buf.reserve(length);
        // Note: it's more efficient to create a buffer with uninitialized memory if we know the length
        let byte_size = length * core::mem::size_of::<T>();
        let out_slice = unsafe {
            core::slice::from_raw_parts_mut(
                out_buf.as_mut_ptr().add(out_buf.len()) as *mut u8,
                byte_size,
            )
        };
        debug_assert!(out_slice.len() >= uncompressed_size);

        read_raw_slice(reader, &compressor, compressed_size, scratch, out_slice)?;
        unsafe { out_buf.set_len(out_buf.len() + length) };
    } else {
        // already fit in buffer
        let mut use_inner = false;
        reader.fill_buf()?;

        let input = if reader.buffer_bytes().len() >= compressed_size {
            use_inner = true;
            reader.buffer_bytes()
        } else {
            scratch.resize(compressed_size, 0);
            reader.read_exact(scratch.as_mut_slice())?;
            scratch.as_slice()
        };

        compressor.decompress_primitive_array(&input[..compressed_size], out_buf)?;
        if use_inner {
            reader.consume(compressed_size);
        }
    }
    Ok(())
}

pub fn read_validity<R: NativeReadBuf>(
    reader: &mut R,
    length: usize,
    builder: &mut MutableBitmap,
) -> Result<()> {
    let mut buf = vec![0u8; 4];
    let def_levels_len = read_u32(reader, buf.as_mut_slice())?;
    if def_levels_len == 0 {
        return Ok(());
    }
    let mut def_levels = vec![0u8; def_levels_len as usize];
    reader.read_exact(def_levels.as_mut_slice())?;

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
    Ok(())
}

pub fn read_validity_nested<R: NativeReadBuf>(
    reader: &mut R,
    num_values: usize,
    leaf: &ColumnDescriptor,
    init: Vec<InitNested>,
) -> Result<(NestedState, Option<Bitmap>)> {
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

    let reps = HybridRleDecoder::try_new(&rep_levels, get_bit_width(max_rep_level), num_values)?;
    let defs = HybridRleDecoder::try_new(&def_levels, get_bit_width(max_def_level), num_values)?;
    let mut page_iter = reps.zip(defs).peekable();

    let mut nested = init_nested(&init, num_values);

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
    let mut builder = MutableBitmap::with_capacity(num_values);

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
