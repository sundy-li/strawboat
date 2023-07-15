use crate::{
    compression::Compression, read::read_basic::read_compress_header, write::WriteOptions,
    CommonCompression,
};
use arrow::{array::PrimitiveArray, error::Result, types::NativeType};

use crate::read::NativeReadBuf;

pub fn compress_primitive<T: NativeType>(
    array: &PrimitiveArray<T>,
    write_options: WriteOptions,
    buf: &mut Vec<u8>,
) -> Result<()> {
    // choose compressor
    let c = write_options.default_compression;
    let codec = u8::from(c.to_compression());
    buf.extend_from_slice(&codec.to_le_bytes());
    let pos = buf.len();
    buf.extend_from_slice(&[0u8; 8]);

    let input_buf = bytemuck::cast_slice(array.values());
    let compressed_size = c.compress(input_buf, buf)?;
    buf[pos..pos + 4].copy_from_slice(&(compressed_size as u32).to_le_bytes());
    buf[pos + 4..pos + 8]
        .copy_from_slice(&((array.len() * std::mem::size_of::<T>()) as u32).to_le_bytes());
    Ok(())
}

pub fn decompress_primitive<T: NativeType, R: NativeReadBuf>(
    reader: &mut R,
    length: usize,
    output: &mut Vec<T>,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    let (codec, compressed_size, _uncompressed_size) = read_compress_header(reader)?;
    let compression = Compression::from_codec(codec)?;
    let c = CommonCompression::try_from(&compression)?;

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

    output.reserve(length);
    let out_slice = unsafe {
        core::slice::from_raw_parts_mut(
            output.as_mut_ptr().add(output.len()) as *mut u8,
            length * std::mem::size_of::<T>(),
        )
    };
    c.decompress(&input[..compressed_size], out_slice)?;
    unsafe { output.set_len(output.len() + length) };

    if use_inner {
        reader.consume(compressed_size);
    }
    Ok(())
}
