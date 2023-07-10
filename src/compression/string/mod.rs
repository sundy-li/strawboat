use arrow::{
    array::BinaryArray,
    bitmap::Bitmap,
    error::Result,
    types::{NativeType, Offset},
};

use crate::{
    read::{
        read_basic::{read_u32, read_u8},
        NativeReadBuf,
    },
    Compression,
};

use super::basic::CommonCompression;

pub fn encoding_binary<O: Offset>(array: &BinaryArray<O>, buf: &mut Vec<u8>) -> Result<()> {
    // choose compressor
    let compression = Compression::None;
    let codec = u8::from(compression);
    buf.extend_from_slice(&codec.to_le_bytes());
    let pos = buf.len();
    buf.extend_from_slice(&[0u8; 8]);

    let compressor = choose_compressor(array, &compression);
    let compressed_size = match compressor {
        BinaryEncoder::Basic(c) => {
            let input_buf = bytemuck::cast_slice(array.values());
            c.compress(input_buf, buf)
        }
        BinaryEncoder::Encoder(c) => c.compress(array, buf),
    }?;

    // TODO

    Ok(())
}

pub fn decode_binary<O: Offset, R: NativeReadBuf>(
    reader: &mut R,
    length: usize,
    output: &mut Vec<O>,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    let mut buf = vec![0u8; 4];
    let compression = Compression::from_codec(read_u8(reader, buf.as_mut_slice())?)?;
    let compressed_size = read_u32(reader, buf.as_mut_slice())? as usize;
    let uncompressed_size = read_u32(reader, buf.as_mut_slice())? as usize;
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

    if let Ok(c) = CommonCompression::try_from(&compression) {
        let out_slice = unsafe {
            core::slice::from_raw_parts_mut(
                output.as_mut_ptr().add(output.len()) as *mut u8,
                length * std::mem::size_of::<O>(),
            )
        };
        c.decompress(&input[..compressed_size], out_slice)?;
    } else {
        // choose
        todo!()
    }

    if use_inner {
        reader.consume(compressed_size);
    }
    Ok(())
}

pub trait BinaryCompression<O: Offset> {
    fn compress(&self, array: &BinaryArray<O>, output: &mut Vec<u8>) -> Result<usize>;
    fn decompress(&self, input: &[u8], length: usize, output: &mut Vec<O>) -> Result<()>;
}

enum BinaryEncoder<O: Offset> {
    Basic(CommonCompression),
    Encoder(Box<dyn BinaryCompression<O>>),
}

fn choose_compressor<O: Offset>(
    value: &BinaryArray<O>,
    compression: &Compression,
) -> BinaryEncoder<O> {
    // todo
    BinaryEncoder::Basic(CommonCompression::LZ4)
}

#[inline]
pub(crate) fn is_valid(validity: &Option<&Bitmap>, i: usize) -> bool {
    match validity {
        Some(v) => v.get_bit(i),
        None => true,
    }
}
