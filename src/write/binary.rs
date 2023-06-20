use std::io::Write;

use arrow::error::Result;
use arrow::types::Offset;

use crate::Compression;

use super::serialize::write_buffer_from_iter;
use super::write_buffer;

pub(crate) fn write_raw_binary<O: Offset, W: Write>(
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

pub(crate) fn write_binary<O: Offset, W: Write>(
    w: &mut W,
    offsets: &[O],
    values: &[u8],
    compression: Compression,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    let _codec = u8::from(compression);
    scratch.clear();

    let compressor = compression.create_compressor();

    // Write an extra codec to indicate the whole compression method
    let codec = u8::from(compression);
    w.write_all(&codec.to_le_bytes())?;

    if compressor.raw_mode() {
        write_raw_binary(w, offsets, values, compression, scratch)
    } else {
        let codec = u8::from(compression);
        w.write_all(&codec.to_le_bytes())?;
        let compressed_size = compressor.compress_binary_array(offsets, values, scratch)?;
        //compressed size
        w.write_all(&(compressed_size as u32).to_le_bytes())?;
        //uncompressed size
        w.write_all(&(values.len() as u32).to_le_bytes())?;
        w.write_all(&scratch[0..compressed_size])?;
        Ok(())
    }
}
