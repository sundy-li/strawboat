#![allow(clippy::ptr_arg)]
use std::io::Write;

// false positive in clippy, see https://github.com/rust-lang/rust-clippy/issues/8463
use arrow::error::Result;

use arrow::types::Offset;
use arrow::{
    array::*, bitmap::Bitmap, datatypes::PhysicalType, trusted_len::TrustedLen, types::NativeType,
};

use crate::with_match_primitive_type;

use super::super::compression;
use super::super::endianess::is_native_little_endian;
use super::common::Compression;

fn write_primitive<T: NativeType, W: Write>(
    w: &mut W,
    array: &PrimitiveArray<T>,
    is_little_endian: bool,
    compression: Option<Compression>,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    write_validity(w, array.validity(), compression, scratch)?;
    write_buffer(w, array.values(), is_little_endian, compression, scratch)
}

fn write_boolean<W: Write>(
    w: &mut W,
    array: &BooleanArray,
    _: bool,
    compression: Option<Compression>,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    write_validity(w, array.validity(), compression, scratch)?;
    write_bitmap(w, array.values(), compression, scratch)
}

#[allow(clippy::too_many_arguments)]
fn write_generic_binary<O: Offset, W: Write>(
    w: &mut W,
    validity: Option<&Bitmap>,
    offsets: &[O],
    values: &[u8],
    is_little_endian: bool,
    compression: Option<Compression>,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    write_validity(w, validity, compression, scratch)?;

    let first = *offsets.first().unwrap();
    let last = *offsets.last().unwrap();

    if first == O::default() {
        write_buffer(w, offsets, is_little_endian, compression, scratch)?;
    } else {
        write_buffer_from_iter(
            w,
            offsets.iter().map(|x| *x - first),
            is_little_endian,
            compression,
            scratch,
        )?;
    }

    write_buffer(
        w,
        &values[first.to_usize()..last.to_usize()],
        is_little_endian,
        compression,
        scratch,
    )
    // write_bytes(
    //     w,
    //     &values[first.to_usize()..last.to_usize()],
    //     compression,
    //     scratch,
    // )
}

fn write_binary<O: Offset, W: Write>(
    w: &mut W,
    array: &BinaryArray<O>,
    is_little_endian: bool,
    compression: Option<Compression>,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    write_generic_binary(
        w,
        array.validity(),
        array.offsets().as_slice(),
        array.values(),
        is_little_endian,
        compression,
        scratch,
    )
}

fn write_utf8<O: Offset, W: Write>(
    w: &mut W,
    array: &Utf8Array<O>,
    is_little_endian: bool,
    compression: Option<Compression>,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    write_generic_binary(
        w,
        array.validity(),
        array.offsets().as_slice(),
        array.values(),
        is_little_endian,
        compression,
        scratch,
    )
}

/// Writes an [`Array`] to `arrow_data`
pub fn write<W: Write>(
    w: &mut W,
    array: &dyn Array,
    is_little_endian: bool,
    compression: Option<Compression>,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    use PhysicalType::*;
    match array.data_type().to_physical_type() {
        Null => Ok(()),
        Boolean => write_boolean::<W>(
            w,
            array.as_any().downcast_ref().unwrap(),
            is_little_endian,
            compression,
            scratch,
        ),
        Primitive(primitive) => with_match_primitive_type!(primitive, |$T| {
            let array = array.as_any().downcast_ref().unwrap();
            write_primitive::<$T, W>(w, array, is_little_endian, compression, scratch)
        }),
        Binary => write_binary::<i32, W>(
            w,
            array.as_any().downcast_ref().unwrap(),
            is_little_endian,
            compression,
            scratch,
        ),
        LargeBinary => write_binary::<i64, W>(
            w,
            array.as_any().downcast_ref().unwrap(),
            is_little_endian,
            compression,
            scratch,
        ),
        // FixedSizeBinary => write_fixed_size_binary(
        //     array.as_any().downcast_ref().unwrap(),
        //     buffers,
        //     arrow_data,
        //     offset,
        //     is_little_endian,
        //     compression,
        // ),
        Utf8 => write_utf8::<i32, W>(
            w,
            array.as_any().downcast_ref().unwrap(),
            is_little_endian,
            compression,
            scratch,
        ),
        LargeUtf8 => write_utf8::<i64, W>(
            w,
            array.as_any().downcast_ref().unwrap(),
            is_little_endian,
            compression,
            scratch,
        ),
        // List => write_list::<i32>(
        //     array.as_any().downcast_ref().unwrap(),
        //     buffers,
        //     arrow_data,
        //     nodes,
        //     offset,
        //     is_little_endian,
        //     compression,
        // ),
        // LargeList => write_list::<i64>(
        //     array.as_any().downcast_ref().unwrap(),
        //     buffers,
        //     arrow_data,
        //     nodes,
        //     offset,
        //     is_little_endian,
        //     compression,
        // ),
        FixedSizeList => unimplemented!(),
        Struct => unimplemented!(),
        Dictionary(_key_type) => unimplemented!(),
        Union => unimplemented!(),
        Map => unimplemented!(),
        _ => todo!(),
    }
}

/// writes `bytes` to `arrow_data` updating `buffers` and `offset` and guaranteeing a 8 byte boundary.
fn write_bytes<W: Write>(
    w: &mut W,
    bytes: &[u8],
    compression: Option<Compression>,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    if let Some(compression) = compression {
        let compressed_size = match compression {
            Compression::LZ4 => compression::compress_lz4(bytes, scratch)?,
            Compression::ZSTD => compression::compress_zstd(bytes, scratch)?,
        };

        //compressed size
        w.write_all(&(compressed_size as u32).to_le_bytes())?;
        //uncompressed size
        w.write_all(&(bytes.len() as u32).to_le_bytes())?;
        w.write_all(&scratch[..compressed_size])?;
    } else {
        w.write(bytes)?;
    };
    Ok(())
}

fn write_validity<W: Write>(
    w: &mut W,
    bitmap: Option<&Bitmap>,
    compression: Option<Compression>,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    match bitmap {
        Some(bitmap) => {
            w.write_all(&[1u8])?;
            let (slice, slice_offset, _) = bitmap.as_slice();
            if slice_offset != 0 {
                // case where we can't slice the bitmap as the offsets are not multiple of 8
                let bytes = Bitmap::from_trusted_len_iter(bitmap.iter());
                let (slice, _, _) = bytes.as_slice();
                write_bytes(w, slice, compression, scratch)
            } else {
                write_bytes(w, slice, compression, scratch)
            }
        }
        None => w.write_all(&[0u8]).map_err(|e| e.into()),
    }
}

fn write_bitmap<W: Write>(
    w: &mut W,
    bitmap: &Bitmap,
    compression: Option<Compression>,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    let (slice, slice_offset, _) = bitmap.as_slice();
    if slice_offset != 0 {
        // case where we can't slice the bitmap as the offsets are not multiple of 8
        let bytes = Bitmap::from_trusted_len_iter(bitmap.iter());
        let (slice, _, _) = bytes.as_slice();
        write_bytes(w, slice, compression, scratch)
    } else {
        write_bytes(w, slice, compression, scratch)
    }
}

/// writes `bytes` to `arrow_data` updating `buffers` and `offset` and guaranteeing a 8 byte boundary.
fn write_buffer<T: NativeType, W: Write>(
    w: &mut W,
    buffer: &[T],
    is_little_endian: bool,
    compression: Option<Compression>,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    if let Some(compression) = compression {
        _write_compressed_buffer(w, buffer, is_little_endian, compression, scratch)
    } else {
        _write_buffer(w, buffer, is_little_endian)
    }
}

#[inline]
fn _write_buffer_from_iter<T: NativeType, I: TrustedLen<Item = T>, W: Write>(
    w: &mut W,
    buffer: I,
    is_little_endian: bool,
) -> Result<()> {
    let _len = buffer.size_hint().0;
    if is_little_endian {
        buffer
            .map(|x| T::to_le_bytes(&x))
            .for_each(|x| w.write_all(x.as_ref()).unwrap());
    } else {
        buffer
            .map(|x| T::to_be_bytes(&x))
            .for_each(|x| w.write_all(x.as_ref()).unwrap());
    }
    Ok(())
}

#[inline]
fn _write_compressed_buffer_from_iter<T: NativeType, I: TrustedLen<Item = T>, W: Write>(
    w: &mut W,
    buffer: I,
    is_little_endian: bool,
    compression: Compression,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    let len = buffer.size_hint().0;
    let mut swapped = Vec::with_capacity(len * std::mem::size_of::<T>());
    if is_little_endian {
        buffer
            .map(|x| T::to_le_bytes(&x))
            .for_each(|x| swapped.extend_from_slice(x.as_ref()));
    } else {
        buffer
            .map(|x| T::to_be_bytes(&x))
            .for_each(|x| swapped.extend_from_slice(x.as_ref()))
    };

    let compressed_size = match compression {
        Compression::LZ4 => compression::compress_lz4(&swapped, scratch)?,
        Compression::ZSTD => compression::compress_zstd(&swapped, scratch)?,
    };
    //compressed size
    w.write_all(&(compressed_size as u32).to_le_bytes())?;
    //uncompressed size
    w.write_all(&(swapped.len() as u32).to_le_bytes())?;
    w.write_all(&scratch[0..compressed_size])?;

    Ok(())
}

fn _write_buffer<T: NativeType, W: Write>(
    w: &mut W,
    buffer: &[T],
    is_little_endian: bool,
) -> Result<()> {
    if is_little_endian == is_native_little_endian() {
        // in native endianess we can use the bytes directly.
        let buffer = bytemuck::cast_slice(buffer);
        w.write_all(buffer).map_err(|e| e.into())
    } else {
        _write_buffer_from_iter(w, buffer.iter().copied(), is_little_endian)
    }
}

fn _write_compressed_buffer<T: NativeType, W: Write>(
    w: &mut W,
    buffer: &[T],
    is_little_endian: bool,
    compression: Compression,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    if is_little_endian == is_native_little_endian() {
        let bytes = bytemuck::cast_slice(buffer);

        let compressed_size = match compression {
            Compression::LZ4 => compression::compress_lz4(bytes, scratch)?,
            Compression::ZSTD => compression::compress_zstd(bytes, scratch)?,
        };

        //compressed size
        w.write_all(&(compressed_size as u32).to_le_bytes())?;

        //uncompressed size
        w.write_all(&(bytes.len() as u32).to_le_bytes())?;
        w.write_all(&scratch[0..compressed_size])?;
        Ok(())
    } else {
        todo!("unsupport bigendian for now")
    }
}

/// writes `bytes` to `arrow_data` updating `buffers` and `offset` and guaranteeing a 8 byte boundary.
#[inline]
fn write_buffer_from_iter<T: NativeType, I: TrustedLen<Item = T>, W: Write>(
    w: &mut W,
    buffer: I,
    is_little_endian: bool,
    compression: Option<Compression>,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    if let Some(compression) = compression {
        _write_compressed_buffer_from_iter(w, buffer, is_little_endian, compression, scratch)?;
    } else {
        _write_buffer_from_iter(w, buffer, is_little_endian)?;
    }
    Ok(())
}
