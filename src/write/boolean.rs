use std::io::Write;

use arrow::bitmap::Bitmap;
use arrow::error::Result;

use crate::Compression;

pub(crate) fn write_bitmap<W: Write>(
    w: &mut W,
    bitmap: &Bitmap,
    compression: Compression,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    let codec: u8 = compression.into();
    w.write_all(&codec.to_le_bytes())?;
    scratch.clear();

    let (slice, slice_offset, _) = bitmap.as_slice();
    let compressor = compression.create_compressor();
    let compressed_size = if compressor.raw_mode() {
        let bitmap = if slice_offset != 0 {
            // case where we can't slice the bitmap as the offsets are not multiple of 8
            Bitmap::from_trusted_len_iter(bitmap.iter())
        } else {
            bitmap.clone()
        };

        let (slice, _, _) = bitmap.as_slice();
        compressor.compress(slice, scratch)
    } else {
        compressor.compress_bitmap(bitmap, scratch)
    }?;

    //compressed size
    w.write_all(&(compressed_size as u32).to_le_bytes())?;
    //uncompressed size
    w.write_all(&(slice.len() as u32).to_le_bytes())?;
    w.write_all(&scratch[..compressed_size])?;

    Ok(())
}
