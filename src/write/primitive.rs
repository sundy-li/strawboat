use std::io::Write;

use arrow::error::Result;
use arrow::{array::PrimitiveArray, types::NativeType};

use crate::Compression;

pub(crate) fn write_primitive<T: NativeType, W: Write>(
    w: &mut W,
    array: &PrimitiveArray<T>,
    compression: Compression,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    let codec = u8::from(compression);
    w.write_all(&codec.to_le_bytes())?;
    let bytes = bytemuck::cast_slice(array.values());

    scratch.clear();
    let compressor = compression.create_compressor();

    let compressed_size = if compressor.raw_mode() {
        compressor.compress(bytes, scratch)
    } else {
        compressor.compress_primitive_array(array, scratch)
    }?;

    //compressed size
    w.write_all(&(compressed_size as u32).to_le_bytes())?;
    //uncompressed size
    w.write_all(&(bytes.len() as u32).to_le_bytes())?;
    w.write_all(&scratch[0..compressed_size])?;
    Ok(())
}
