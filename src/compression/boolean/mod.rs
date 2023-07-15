mod one_value;
mod rle;

use arrow::{
    array::BooleanArray,
    bitmap::{Bitmap, MutableBitmap},
    error::{Error, Result},
};
use rand::{seq::IteratorRandom, thread_rng};

use crate::{
    read::{read_basic::read_compress_header, NativeReadBuf},
    write::WriteOptions,
};

use super::{
    basic::CommonCompression,
    integer::{OneValue, RLE},
    Compression,
};

pub fn encode_bitmap(
    array: &BooleanArray,
    buf: &mut Vec<u8>,
    write_options: WriteOptions,
) -> Result<()> {
    // choose compressor
    let stats = gen_stats(array);
    let compressor = choose_compressor(array, &stats, &write_options);

    log::info!(
        "choose boolean compression : {:?}",
        compressor.to_compression()
    );

    let codec = u8::from(compressor.to_compression());
    buf.extend_from_slice(&codec.to_le_bytes());
    let pos = buf.len();
    buf.extend_from_slice(&[0u8; 8]);

    let compressed_size = match compressor {
        BitmapEncoder::Basic(c) => {
            let bitmap = array.values();
            let (_, slice_offset, _) = bitmap.as_slice();

            let bitmap = if slice_offset != 0 {
                // case where we can't slice the bitmap as the offsets are not multiple of 8
                Bitmap::from_trusted_len_iter(bitmap.iter())
            } else {
                bitmap.clone()
            };
            let (slice, _, _) = bitmap.as_slice();
            c.compress(slice, buf)
        }
        BitmapEncoder::Encoder(c) => c.compress(array, buf),
    }?;
    buf[pos..pos + 4].copy_from_slice(&(compressed_size as u32).to_le_bytes());
    buf[pos + 4..pos + 8].copy_from_slice(&(array.len() as u32).to_le_bytes());
    Ok(())
}

pub fn decode_bitmap<R: NativeReadBuf>(
    reader: &mut R,
    length: usize,
    output: &mut MutableBitmap,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    let (codec, compressed_size, _uncompressed_size) = read_compress_header(reader)?;
    let compression = Compression::from_codec(codec)?;

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

    let compressor = BitmapEncoder::from_compression(compression)?;
    match compressor {
        BitmapEncoder::Basic(c) => {
            let bytes = (length + 7) / 8;
            let mut buffer = vec![0u8; bytes];
            c.decompress(&input[..compressed_size], &mut buffer)?;
            output.extend_from_slice(buffer.as_slice(), 0, length);
        }
        BitmapEncoder::Encoder(c) => {
            c.decompress(input, length, output)?;
        }
    }

    if use_inner {
        reader.consume(compressed_size);
    }
    Ok(())
}

pub trait BooleanCompression {
    fn compress(&self, array: &BooleanArray, output: &mut Vec<u8>) -> Result<usize>;
    fn decompress(&self, input: &[u8], length: usize, output: &mut MutableBitmap) -> Result<()>;
    fn to_compression(&self) -> Compression;

    fn compress_ratio(&self, stats: &BooleanStats) -> f64;
}

enum BitmapEncoder {
    Basic(CommonCompression),
    Encoder(Box<dyn BooleanCompression>),
}

impl BitmapEncoder {
    fn to_compression(&self) -> Compression {
        match self {
            Self::Basic(c) => c.to_compression(),
            Self::Encoder(c) => c.to_compression(),
        }
    }

    fn from_compression(compression: Compression) -> Result<Self> {
        if let Ok(c) = CommonCompression::try_from(&compression) {
            return Ok(Self::Basic(c));
        }
        match compression {
            Compression::RLE => Ok(Self::Encoder(Box::new(RLE {}))),
            Compression::OneValue => Ok(Self::Encoder(Box::new(OneValue {}))),
            other => Err(Error::OutOfSpec(format!(
                "Unknown compression codec {other:?}",
            ))),
        }
    }
}

#[allow(dead_code)]
pub struct BooleanStats {
    pub src: BooleanArray,
    pub total_bytes: usize,
    pub rows: usize,
    pub null_count: usize,
    pub false_count: usize,
    pub true_count: usize,
    pub average_run_length: f64,
}

fn gen_stats(array: &BooleanArray) -> BooleanStats {
    let mut null_count = 0;
    let mut false_count = 0;
    let mut true_count = 0;

    let mut is_init_value_initialized = false;
    let mut last_value = false;
    let mut run_count = 0;

    for v in array.iter() {
        if !is_init_value_initialized {
            is_init_value_initialized = true;
            last_value = v.unwrap_or_default();
        }

        match v {
            Some(v) => {
                if v {
                    true_count += 1;
                } else {
                    false_count += 1;
                }

                if last_value != v {
                    run_count += 1;
                    last_value = v;
                }
            }
            None => null_count += 1,
        }
    }

    BooleanStats {
        src: array.clone(),
        rows: array.len(),
        total_bytes: array.values().len() / 8,
        null_count,
        false_count,
        true_count,
        average_run_length: array.len() as f64 / 8.0f64 / run_count as f64,
    }
}

fn choose_compressor(
    _array: &BooleanArray,
    stats: &BooleanStats,
    write_options: &WriteOptions,
) -> BitmapEncoder {
    let basic = BitmapEncoder::Basic(write_options.default_compression);
    if let Some(ratio) = write_options.default_compress_ratio {
        let mut max_ratio = ratio as f64;
        let mut result = basic;

        let encoders: Vec<Box<dyn BooleanCompression>> =
            vec![Box::new(OneValue {}) as _, Box::new(RLE {}) as _];

        for encoder in encoders {
            if write_options
                .forbidden_compressions
                .contains(&encoder.to_compression())
            {
                continue;
            }

            let r = encoder.compress_ratio(stats);
            if r > max_ratio {
                max_ratio = r;
                result = BitmapEncoder::Encoder(encoder);
            }
        }
        result
    } else {
        basic
    }
}

fn compress_sample_ratio<C: BooleanCompression>(
    c: &C,
    array: &BooleanArray,
    sample_size: usize,
) -> f64 {
    let mut rng = thread_rng();
    let sample_array = array.iter().choose_multiple(&mut rng, sample_size);
    let sample_array = BooleanArray::from(sample_array);

    let stats = gen_stats(&sample_array);
    let size = c
        .compress(&sample_array, &mut vec![])
        .unwrap_or(stats.total_bytes);

    stats.total_bytes as f64 / size as f64
}
