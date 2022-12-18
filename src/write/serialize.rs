#![allow(clippy::ptr_arg)]

use std::io::Write;

// false positive in clippy, see https://github.com/rust-lang/rust-clippy/issues/8463
use arrow::error::Result;

use arrow::buffer::Buffer;
use arrow::datatypes::DataType;
use arrow::types::Offset;
use arrow::{
    array::*, bitmap::Bitmap, datatypes::PhysicalType, trusted_len::TrustedLen, types::NativeType,
};

use crate::with_match_primitive_type;

use super::super::endianess::is_native_little_endian;
use crate::compression;
use crate::Compression;

fn write_primitive<T: NativeType, W: Write>(
    w: &mut W,
    array: &PrimitiveArray<T>,
    is_little_endian: bool,
    compression: Compression,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    write_validity(w, array.validity(), compression, scratch)?;
    write_buffer(w, array.values(), is_little_endian, compression, scratch)
}

fn write_boolean<W: Write>(
    w: &mut W,
    array: &BooleanArray,
    _: bool,
    compression: Compression,
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
    compression: Compression,
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
    compression: Compression,
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
    compression: Compression,
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
    compression: Compression,
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
        Struct => {
            let _ = if let DataType::Struct(children) = array.data_type().to_logical_type() {
                children
            } else {
                unreachable!()
            };
            let struct_array: &StructArray = array.as_any().downcast_ref().unwrap();
            for sub_array in struct_array.values() {
                write(
                    w,
                    sub_array.as_ref(),
                    is_little_endian,
                    compression,
                    scratch,
                )?;
            }
            Ok(())
        }
        List => {
            // dbg!(array);
            let list_array: &ListArray<i32> = array.as_any().downcast_ref().unwrap();
            let offset = list_array.offsets().to_owned();
            // write offset num
            write_primitive::<i32, W>(
                w,
                &Int32Array::new(
                    DataType::Int32,
                    Buffer::from(vec![offset.len() as i32 + 1]),
                    None,
                ),
                is_little_endian,
                compression,
                scratch,
            )?;
            // write offset
            write_primitive::<i32, W>(
                w,
                &Int32Array::new(DataType::Int32, offset.buffer().to_owned(), None),
                is_little_endian,
                compression,
                scratch,
            )?;
            // write values
            write(
                w,
                list_array.values().as_ref(),
                is_little_endian,
                compression,
                scratch,
            )?;
            Ok(())
        }
        FixedSizeList => unimplemented!(),
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
    compression: Compression,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    let compressed_size = match compression {
        Compression::None => {
            //compressed size
            w.write_all(&(bytes.len() as u32).to_le_bytes())?;
            //uncompressed size
            w.write_all(&(bytes.len() as u32).to_le_bytes())?;
            w.write(bytes)?;
            return Ok(());
        }
        Compression::LZ4 => compression::compress_lz4(bytes, scratch)?,
        Compression::ZSTD => compression::compress_zstd(bytes, scratch)?,
    };

    //compressed size
    w.write_all(&(compressed_size as u32).to_le_bytes())?;
    //uncompressed size
    w.write_all(&(bytes.len() as u32).to_le_bytes())?;
    w.write_all(&scratch[..compressed_size])?;

    Ok(())
}

fn write_validity<W: Write>(
    w: &mut W,
    bitmap: Option<&Bitmap>,
    compression: Compression,
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
    compression: Compression,
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
    compression: Compression,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    if is_little_endian == is_native_little_endian() {
        let bytes = bytemuck::cast_slice(buffer);
        let compressed_size = match compression {
            Compression::None => {
                //compressed size
                w.write_all(&(bytes.len() as u32).to_le_bytes())?;
                //uncompressed size
                w.write_all(&(bytes.len() as u32).to_le_bytes())?;
                w.write(bytes)?;
                return Ok(());
            }
            Compression::LZ4 => compression::compress_lz4(bytes, scratch)?,
            Compression::ZSTD => compression::compress_zstd(bytes, scratch)?,
        };

        let codec = u8::from(compression);
        w.write_all(&codec.to_le_bytes());
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

/// writes `bytes` to `arrow_data` updating `buffers` and `offset` and guaranteeing a 8 byte boundary.
#[inline]
fn write_buffer_from_iter<T: NativeType, I: TrustedLen<Item = T>, W: Write>(
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
        Compression::None => {
            //compressed size
            w.write_all(&(swapped.len() as u32).to_le_bytes())?;
            //uncompressed size
            w.write_all(&(swapped.len() as u32).to_le_bytes())?;
            w.write(swapped.as_slice())?;
            return Ok(());
        }
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
