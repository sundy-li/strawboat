use std::io::Write;

use arrow::{
    array::*,
    bitmap::Bitmap,
    datatypes::PhysicalType,
    error::Result,
    io::parquet::write::{write_def_levels, write_rep_and_def, Nested, Version},
    trusted_len::TrustedLen,
    types::NativeType,
};
use parquet2::schema::{
    types::{FieldInfo, PrimitiveType},
    Repetition,
};

use super::{boolean::write_bitmap, primitive::write_primitive};
use crate::Compression;
use crate::{with_match_primitive_type, write::binary::write_binary};

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
    use PhysicalType::*;

    let is_optional = is_nullable(&type_.field_info);
    match array.data_type().to_physical_type() {
        Null => {}
        Boolean => {
            let array: &BooleanArray = array.as_any().downcast_ref().unwrap();
            if is_optional {
                write_validity::<W>(w, is_optional, array.validity(), array.len(), scratch)?;
            }
            write_bitmap::<W>(w, array.values(), compression, scratch)?
        }
        Primitive(primitive) => with_match_primitive_type!(primitive, |$T| {
            let array: &PrimitiveArray<$T> = array.as_any().downcast_ref().unwrap();
            if is_optional {
                write_validity::<W>(w, is_optional, array.validity(), array.len(), scratch)?;
            }
            write_primitive::<$T, W>(w, array, compression, scratch)?;
        }),
        Binary => {
            let array: &BinaryArray<i32> = array.as_any().downcast_ref().unwrap();
            if is_optional {
                write_validity::<W>(w, is_optional, array.validity(), array.len(), scratch)?;
            }
            write_binary::<i32, W>(
                w,
                array.offsets().buffer(),
                array.values(),
                array.validity(),
                compression,
                scratch,
            )?;
        }
        LargeBinary => {
            let array: &BinaryArray<i64> = array.as_any().downcast_ref().unwrap();
            if is_optional {
                write_validity::<W>(w, is_optional, array.validity(), array.len(), scratch)?;
            }
            write_binary::<i64, W>(
                w,
                array.offsets().buffer(),
                array.values(),
                array.validity(),
                compression,
                scratch,
            )?;
        }
        Utf8 => {
            let array: &Utf8Array<i32> = array.as_any().downcast_ref().unwrap();
            if is_optional {
                write_validity::<W>(w, is_optional, array.validity(), array.len(), scratch)?;
            }
            write_binary::<i32, W>(
                w,
                array.offsets().buffer(),
                array.values(),
                array.validity(),
                compression,
                scratch,
            )?;
        }
        LargeUtf8 => {
            let array: &Utf8Array<i64> = array.as_any().downcast_ref().unwrap();
            if is_optional {
                write_validity::<W>(w, is_optional, array.validity(), array.len(), scratch)?;
            }
            write_binary::<i64, W>(
                w,
                array.offsets().buffer(),
                array.values(),
                array.validity(),
                compression,
                scratch,
            )?;
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
    write_nested_validity::<W>(w, nested, length, scratch)?;

    scratch.clear();

    use PhysicalType::*;
    match array.data_type().to_physical_type() {
        Null => {}
        Boolean => {
            let array: &BooleanArray = array.as_any().downcast_ref().unwrap();
            write_bitmap::<W>(w, array.values(), compression, scratch)?
        }
        Primitive(primitive) => with_match_primitive_type!(primitive, |$T| {
            let array = array.as_any().downcast_ref().unwrap();
            write_primitive::<$T, W>(w, array, compression, scratch)?;
        }),
        Binary => {
            let binary_array: &BinaryArray<i32> = array.as_any().downcast_ref().unwrap();
            write_binary::<i32, W>(
                w,
                binary_array.offsets().buffer(),
                binary_array.values(),
                array.validity(),
                compression,
                scratch,
            )?;
        }
        LargeBinary => {
            let binary_array: &BinaryArray<i64> = array.as_any().downcast_ref().unwrap();
            write_binary::<i64, W>(
                w,
                binary_array.offsets().buffer(),
                binary_array.values(),
                array.validity(),
                compression,
                scratch,
            )?;
        }
        Utf8 => {
            let binary_array: &Utf8Array<i32> = array.as_any().downcast_ref().unwrap();
            write_binary::<i32, W>(
                w,
                binary_array.offsets().buffer(),
                binary_array.values(),
                array.validity(),
                compression,
                scratch,
            )?;
        }
        LargeUtf8 => {
            let binary_array: &Utf8Array<i64> = array.as_any().downcast_ref().unwrap();
            write_binary::<i64, W>(
                w,
                binary_array.offsets().buffer(),
                binary_array.values(),
                array.validity(),
                compression,
                scratch,
            )?;
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

fn write_validity<W: Write>(
    w: &mut W,
    is_optional: bool,
    validity: Option<&Bitmap>,
    length: usize,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    scratch.clear();

    write_def_levels(scratch, is_optional, validity, length, Version::V2)?;
    let def_levels_len = scratch.len();
    w.write_all(&(def_levels_len as u32).to_le_bytes())?;
    w.write_all(&scratch[..def_levels_len])?;

    Ok(())
}

fn write_nested_validity<W: Write>(
    w: &mut W,
    nested: &[Nested],
    length: usize,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    scratch.clear();

    let (rep_levels_len, def_levels_len) = write_rep_and_def(Version::V2, nested, scratch)?;
    w.write_all(&(length as u32).to_le_bytes())?;
    w.write_all(&(rep_levels_len as u32).to_le_bytes())?;
    w.write_all(&(def_levels_len as u32).to_le_bytes())?;
    w.write_all(&scratch[..scratch.len()])?;

    Ok(())
}

/// writes `bytes` to `arrow_data` updating `buffers` and `offset` and guaranteeing a 8 byte boundary.
pub fn write_buffer<T: NativeType, W: Write>(
    w: &mut W,
    buffer: &[T],
    compression: Compression,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    let codec = u8::from(compression);
    w.write_all(&codec.to_le_bytes())?;
    let bytes = bytemuck::cast_slice(buffer);

    scratch.clear();

    let compressor = compression.create_compressor();
    let compressed_size = compressor.compress(bytes, scratch)?;
    //compressed size
    w.write_all(&(compressed_size as u32).to_le_bytes())?;

    //uncompressed size
    w.write_all(&(bytes.len() as u32).to_le_bytes())?;
    w.write_all(&scratch[0..compressed_size])?;
    Ok(())
}

/// writes `bytes` to `arrow_data` updating `buffers` and `offset` and guaranteeing a 8 byte boundary.
#[inline]
pub fn write_buffer_from_iter<T: NativeType, I: TrustedLen<Item = T>, W: Write>(
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

    scratch.clear();

    let compressor = compression.create_compressor();
    let compressed_size = compressor.compress(&swapped, scratch)?;

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
