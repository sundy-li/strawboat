use std::io::Write;

use arrow::{
    array::*,
    bitmap::Bitmap,
    datatypes::PhysicalType,
    error::Result,
    io::parquet::write::{slice_nested_leaf, write_def_levels, write_rep_and_def, Nested, Version},
    trusted_len::TrustedLen,
    types::{NativeType, Offset},
};
use parquet2::schema::{
    types::{FieldInfo, PrimitiveType},
    Repetition,
};

use crate::compression;
use crate::with_match_primitive_type;
use crate::Compression;

pub fn write<W: Write>(
    w: &mut W,
    array: &dyn Array,
    nested: &[Nested],
    type_: PrimitiveType,
    length: usize,
    compression: Compression,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    if nested.len() == 1 {
        return write_simple(w, array, type_, compression, scratch);
    }
    write_nested(w, array, nested, length, compression, scratch)
}

/// Writes an [`Array`] to `arrow_data`
pub fn write_simple<W: Write>(
    w: &mut W,
    array: &dyn Array,
    type_: PrimitiveType,
    compression: Compression,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    let is_optional = is_nullable(&type_.field_info);

    use PhysicalType::*;
    match array.data_type().to_physical_type() {
        Null => {}
        Boolean => {
            let array: &BooleanArray = array.as_any().downcast_ref().unwrap();
            write_validity::<W>(w, is_optional, array.validity(), array.len(), scratch)?;
            write_boolean::<W>(w, array, compression, scratch)?;
        }
        Primitive(primitive) => with_match_primitive_type!(primitive, |$T| {
            let array: &PrimitiveArray<$T> = array.as_any().downcast_ref().unwrap();
            write_validity::<W>(
                w,
                is_optional,
                array.validity(),
                array.len(),
                scratch,
            )?;
            write_primitive::<$T, W>(w, array, compression, scratch)?;
        }),
        Binary => {
            let array: &BinaryArray<i32> = array.as_any().downcast_ref().unwrap();
            write_validity::<W>(w, is_optional, array.validity(), array.len(), scratch)?;
            write_binary::<i32, W>(w, array, compression, scratch)?;
        }
        LargeBinary => {
            let array: &BinaryArray<i64> = array.as_any().downcast_ref().unwrap();
            write_validity::<W>(w, is_optional, array.validity(), array.len(), scratch)?;

            write_binary::<i64, W>(w, array, compression, scratch)?;
        }
        Utf8 => {
            let array: &Utf8Array<i32> = array.as_any().downcast_ref().unwrap();
            write_validity::<W>(w, is_optional, array.validity(), array.len(), scratch)?;

            write_utf8::<i32, W>(w, array, compression, scratch)?;
        }
        LargeUtf8 => {
            let array: &Utf8Array<i64> = array.as_any().downcast_ref().unwrap();
            write_validity::<W>(w, is_optional, array.validity(), array.len(), scratch)?;

            write_utf8::<i64, W>(w, array, compression, scratch)?;
        }
        Struct => unreachable!(),
        List => unreachable!(),
        FixedSizeList => unreachable!(),
        Dictionary(_key_type) => unreachable!(),
        Union => unreachable!(),
        Map => unreachable!(),
        _ => todo!(),
    }

    Ok(())
}

/// Writes a nested [`Array`] to `arrow_data`
pub fn write_nested<W: Write>(
    w: &mut W,
    array: &dyn Array,
    nested: &[Nested],
    length: usize,
    compression: Compression,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    // we slice the leaf by the offsets as dremel only computes lengths and thus
    // does NOT take the starting offset into account.
    // By slicing the leaf array we also don't write too many values.
    let (start, len) = slice_nested_leaf(nested);
    // write rep_levels and def_levels
    write_nested_validity::<W>(w, nested, length, start, scratch)?;

    let array = array.slice(start, len);

    use PhysicalType::*;
    match array.data_type().to_physical_type() {
        Null => {}
        Boolean => write_boolean::<W>(
            w,
            array.as_any().downcast_ref().unwrap(),
            compression,
            scratch,
        )?,
        Primitive(primitive) => with_match_primitive_type!(primitive, |$T| {
            let array = array.as_any().downcast_ref().unwrap();
            write_primitive::<$T, W>(w, array, compression, scratch)?;
        }),
        Binary => write_binary::<i32, W>(
            w,
            array.as_any().downcast_ref().unwrap(),
            compression,
            scratch,
        )?,
        LargeBinary => write_binary::<i64, W>(
            w,
            array.as_any().downcast_ref().unwrap(),
            compression,
            scratch,
        )?,
        Utf8 => write_utf8::<i32, W>(
            w,
            array.as_any().downcast_ref().unwrap(),
            compression,
            scratch,
        )?,
        LargeUtf8 => write_utf8::<i64, W>(
            w,
            array.as_any().downcast_ref().unwrap(),
            compression,
            scratch,
        )?,
        Struct => unreachable!(),
        List => unreachable!(),
        FixedSizeList => unreachable!(),
        Dictionary(_key_type) => unreachable!(),
        Union => unreachable!(),
        Map => unreachable!(),
        _ => todo!(),
    }

    Ok(())
}

fn write_validity<W: Write>(
    w: &mut W,
    is_optional: bool,
    validity: Option<&Bitmap>,
    length: usize,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    scratch.clear();

    write_def_levels(scratch, is_optional, validity, length, Version::V2)?;
    let rep_levels_len = 0;
    let def_levels_len = scratch.len();

    w.write_all(&(length as u32).to_le_bytes())?;
    w.write_all(&(rep_levels_len as u32).to_le_bytes())?;
    w.write_all(&(def_levels_len as u32).to_le_bytes())?;
    w.write_all(&scratch[..def_levels_len])?;

    Ok(())
}

fn write_nested_validity<W: Write>(
    w: &mut W,
    nested: &[Nested],
    length: usize,
    start: usize,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    scratch.clear();

    let (rep_levels_len, def_levels_len) = write_rep_and_def(Version::V2, nested, scratch, start)?;

    w.write_all(&(length as u32).to_le_bytes())?;
    w.write_all(&(rep_levels_len as u32).to_le_bytes())?;
    w.write_all(&(def_levels_len as u32).to_le_bytes())?;
    w.write_all(&scratch[..scratch.len()])?;

    Ok(())
}

fn write_primitive<T: NativeType, W: Write>(
    w: &mut W,
    array: &PrimitiveArray<T>,
    compression: Compression,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    write_buffer(w, array.values(), compression, scratch)
}

fn write_boolean<W: Write>(
    w: &mut W,
    array: &BooleanArray,
    compression: Compression,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    write_bitmap(w, array.values(), compression, scratch)
}

fn write_generic_binary<O: Offset, W: Write>(
    w: &mut W,
    offsets: &[O],
    values: &[u8],
    compression: Compression,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    let first = *offsets.first().unwrap();
    let last = *offsets.last().unwrap();

    if first == O::default() {
        write_buffer(w, offsets, compression, scratch)?;
    } else {
        write_buffer_from_iter(w, offsets.iter().map(|x| *x - first), compression, scratch)?;
    }

    write_buffer(
        w,
        &values[first.to_usize()..last.to_usize()],
        compression,
        scratch,
    )
}

fn write_binary<O: Offset, W: Write>(
    w: &mut W,
    array: &BinaryArray<O>,
    compression: Compression,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    write_generic_binary(
        w,
        array.offsets().as_slice(),
        array.values(),
        compression,
        scratch,
    )
}

fn write_utf8<O: Offset, W: Write>(
    w: &mut W,
    array: &Utf8Array<O>,
    compression: Compression,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    write_generic_binary(
        w,
        array.offsets().as_slice(),
        array.values(),
        compression,
        scratch,
    )
}

/// writes `bytes` to `arrow_data` updating `buffers` and `offset` and guaranteeing a 8 byte boundary.
fn write_bytes<W: Write>(
    w: &mut W,
    bytes: &[u8],
    compression: Compression,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    let codec: u8 = compression.into();
    w.write_all(&codec.to_le_bytes())?;

    let compressed_size = match compression {
        Compression::None => {
            //compressed size
            w.write_all(&(bytes.len() as u32).to_le_bytes())?;
            //uncompressed size
            w.write_all(&(bytes.len() as u32).to_le_bytes())?;
            w.write_all(bytes)?;
            return Ok(());
        }
        Compression::LZ4 => compression::compress_lz4(bytes, scratch)?,
        Compression::ZSTD => compression::compress_zstd(bytes, scratch)?,
        Compression::SNAPPY => compression::compress_snappy(bytes, scratch)?,
    };

    //compressed size
    w.write_all(&(compressed_size as u32).to_le_bytes())?;
    //uncompressed size
    w.write_all(&(bytes.len() as u32).to_le_bytes())?;
    w.write_all(&scratch[..compressed_size])?;

    Ok(())
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
    compression: Compression,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    let codec = u8::from(compression);
    w.write_all(&codec.to_le_bytes())?;
    let bytes = bytemuck::cast_slice(buffer);
    let compressed_size = match compression {
        Compression::None => {
            //compressed size
            w.write_all(&(bytes.len() as u32).to_le_bytes())?;
            //uncompressed size
            w.write_all(&(bytes.len() as u32).to_le_bytes())?;
            w.write_all(bytes)?;
            return Ok(());
        }
        Compression::LZ4 => compression::compress_lz4(bytes, scratch)?,
        Compression::ZSTD => compression::compress_zstd(bytes, scratch)?,
        Compression::SNAPPY => compression::compress_snappy(bytes, scratch)?,
    };

    //compressed size
    w.write_all(&(compressed_size as u32).to_le_bytes())?;

    //uncompressed size
    w.write_all(&(bytes.len() as u32).to_le_bytes())?;
    w.write_all(&scratch[0..compressed_size])?;
    Ok(())
}

/// writes `bytes` to `arrow_data` updating `buffers` and `offset` and guaranteeing a 8 byte boundary.
#[inline]
fn write_buffer_from_iter<T: NativeType, I: TrustedLen<Item = T>, W: Write>(
    w: &mut W,
    buffer: I,
    compression: Compression,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    let len = buffer.size_hint().0;
    let mut swapped = Vec::with_capacity(len * std::mem::size_of::<T>());
    buffer
        .map(|x| T::to_le_bytes(&x))
        .for_each(|x| swapped.extend_from_slice(x.as_ref()));

    let codec = u8::from(compression);
    w.write_all(&codec.to_le_bytes())?;

    let compressed_size = match compression {
        Compression::None => {
            //compressed size
            w.write_all(&(swapped.len() as u32).to_le_bytes())?;
            //uncompressed size
            w.write_all(&(swapped.len() as u32).to_le_bytes())?;
            w.write_all(swapped.as_slice())?;
            return Ok(());
        }
        Compression::LZ4 => compression::compress_lz4(&swapped, scratch)?,
        Compression::ZSTD => compression::compress_zstd(&swapped, scratch)?,
        Compression::SNAPPY => compression::compress_snappy(&swapped, scratch)?,
    };

    //compressed size
    w.write_all(&(compressed_size as u32).to_le_bytes())?;
    //uncompressed size
    w.write_all(&(swapped.len() as u32).to_le_bytes())?;
    w.write_all(&scratch[0..compressed_size])?;

    Ok(())
}

fn is_nullable(field_info: &FieldInfo) -> bool {
    match field_info.repetition {
        Repetition::Optional => true,
        Repetition::Repeated => true,
        Repetition::Required => false,
    }
}
