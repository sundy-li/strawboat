mod rle;

use arrow::{array::PrimitiveArray, bitmap::Bitmap, error::Result, types::NativeType};

use crate::{
    read::{
        read_basic::{read_u32, read_u8},
        NativeReadBuf,
    },
    Compression,
};

use self::rle::RLE;

use super::basic::CommonCompression;

pub fn encode_native<T: NativeType>(array: &PrimitiveArray<T>, buf: &mut Vec<u8>) -> Result<()> {
    // choose compressor
    let compression = Compression::None;
    let codec = u8::from(compression);
    buf.extend_from_slice(&codec.to_le_bytes());
    let pos = buf.len();
    buf.extend_from_slice(&[0u8; 8]);

    let compressor = choose_compressor(array, &compression);
    let compressed_size = match compressor {
        IntEncoder::Basic(c) => {
            let input_buf = bytemuck::cast_slice(array.values());
            c.compress(input_buf, buf)
        }
        IntEncoder::Encoder(c) => c.compress(array, buf),
    }?;
    buf[pos..].copy_from_slice(&(compressed_size as u64).to_le_bytes());
    buf[pos + 4..].copy_from_slice(&(array.len() * std::mem::size_of::<T>()).to_le_bytes());
    Ok(())
}

pub fn decode_native<T: NativeType, R: NativeReadBuf>(
    reader: &mut R,
    length: usize,
    output: &mut Vec<T>,
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
                length * std::mem::size_of::<T>(),
            )
        };
        c.decompress(&input[..compressed_size], out_slice)?;
    } else {
        // choose
        let encoder = Box::new(RLE {});
        encoder.decompress(input, length, output)?;
    }

    if use_inner {
        reader.consume(compressed_size);
    }
    Ok(())
}

pub trait IntegerCompression<T: NativeType> {
    fn compress(&self, array: &PrimitiveArray<T>, output: &mut Vec<u8>) -> Result<usize>;
    fn decompress(&self, input: &[u8], length: usize, output: &mut Vec<T>) -> Result<()>;
}

enum IntEncoder<T: NativeType> {
    Basic(CommonCompression),
    Encoder(Box<dyn IntegerCompression<T>>),
}

fn choose_compressor<T: NativeType>(
    value: &PrimitiveArray<T>,
    compression: &Compression,
) -> IntEncoder<T> {
    // todo
    IntEncoder::Encoder(Box::new(RLE {}))
}

#[inline]
pub(crate) fn is_valid(validity: &Option<&Bitmap>, i: usize) -> bool {
    match validity {
        Some(v) => v.get_bit(i),
        None => true,
    }
}
