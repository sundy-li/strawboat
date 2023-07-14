mod dict;
mod rle;

use std::{collections::HashMap, hash::Hash};

use arrow::{
    array::{Array, PrimitiveArray},
    error::{Error, Result},
    types::NativeType,
};

use crate::{
    read::{read_basic::read_compress_header, NativeReadBuf},
    write::WriteOptions,
};

pub use self::dict::AsBytes;
pub use self::dict::Dict;
pub use self::dict::DictEncoder;
pub use self::rle::RLE;

use super::{basic::CommonCompression, is_valid, Compression};

pub fn compress_native_fallback<T: NativeType>(
    array: &PrimitiveArray<T>,
    write_options: WriteOptions,
    buf: &mut Vec<u8>,
) -> Result<()> {
    // choose compressor
    let compressor = IntCompressor::Basic(write_options.default_compression);

    let codec = u8::from(compressor.to_compression());
    buf.extend_from_slice(&codec.to_le_bytes());
    let pos = buf.len();
    buf.extend_from_slice(&[0u8; 8]);

    let compressed_size = match compressor {
        IntCompressor::Basic(c) => {
            let input_buf = bytemuck::cast_slice(array.values());
            c.compress(input_buf, buf)
        }
        IntCompressor::Extend(c) => c.compress(array, &write_options, buf),
    }?;
    buf[pos..pos + 4].copy_from_slice(&(compressed_size as u32).to_le_bytes());
    buf[pos + 4..pos + 8]
        .copy_from_slice(&((array.len() * std::mem::size_of::<T>()) as u32).to_le_bytes());
    Ok(())
}

pub fn compress_native<T: NativeType + PartialOrd + Eq + Hash>(
    array: &PrimitiveArray<T>,
    write_options: WriteOptions,
    buf: &mut Vec<u8>,
) -> Result<()> {
    // choose compressor
    let stats = gen_stats(array);
    let compressor = choose_compressor(array, &stats, &write_options);

    log::info!(
        "choose integer compression : {:?}",
        compressor.to_compression()
    );

    let codec = u8::from(compressor.to_compression());
    buf.extend_from_slice(&codec.to_le_bytes());
    let pos = buf.len();
    buf.extend_from_slice(&[0u8; 8]);

    let compressed_size = match compressor {
        IntCompressor::Basic(c) => {
            let input_buf = bytemuck::cast_slice(array.values());
            c.compress(input_buf, buf)
        }
        IntCompressor::Extend(c) => c.compress(array, &write_options, buf),
    }?;
    buf[pos..pos + 4].copy_from_slice(&(compressed_size as u32).to_le_bytes());
    buf[pos + 4..pos + 8]
        .copy_from_slice(&((array.len() * std::mem::size_of::<T>()) as u32).to_le_bytes());
    Ok(())
}

pub fn decompress_native<T: NativeType, R: NativeReadBuf>(
    reader: &mut R,
    length: usize,
    output: &mut Vec<T>,
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

    let compressor = IntCompressor::<T>::from_compression(compression)?;

    match compressor {
        IntCompressor::Basic(c) => {
            output.reserve(length);
            let out_slice = unsafe {
                core::slice::from_raw_parts_mut(
                    output.as_mut_ptr().add(output.len()) as *mut u8,
                    length * std::mem::size_of::<T>(),
                )
            };
            c.decompress(&input[..compressed_size], out_slice)?;
            unsafe { output.set_len(output.len() + length) };
        }
        IntCompressor::Extend(c) => {
            c.decompress(input, length, output)?;
        }
    }

    if use_inner {
        reader.consume(compressed_size);
    }
    Ok(())
}

pub trait IntegerCompression<T: NativeType> {
    fn compress(
        &self,
        array: &PrimitiveArray<T>,
        write_options: &WriteOptions,
        output: &mut Vec<u8>,
    ) -> Result<usize>;
    fn decompress(&self, input: &[u8], length: usize, output: &mut Vec<T>) -> Result<()>;

    fn to_compression(&self) -> Compression;
    fn compress_ratio(&self, stats: &IntegerStats<T>) -> f64;
}

enum IntCompressor<T: NativeType> {
    Basic(CommonCompression),
    Extend(Box<dyn IntegerCompression<T>>),
}

impl<T: NativeType> IntCompressor<T> {
    fn to_compression(&self) -> Compression {
        match self {
            Self::Basic(c) => c.to_compression(),
            Self::Extend(c) => c.to_compression(),
        }
    }

    fn from_compression(compression: Compression) -> Result<Self> {
        if let Ok(c) = CommonCompression::try_from(&compression) {
            return Ok(Self::Basic(c));
        }
        match compression {
            Compression::RLE => Ok(Self::Extend(Box::new(RLE {}))),
            Compression::Dict => Ok(Self::Extend(Box::new(Dict {}))),
            other => Err(Error::OutOfSpec(format!(
                "Unknown compression codec {other:?}",
            ))),
        }
    }
}

#[allow(dead_code)]
pub struct IntegerStats<T: NativeType> {
    pub tuple_count: usize,
    pub total_size: usize,
    pub null_count: usize,
    pub average_run_length: f64,
    pub is_sorted: bool,
    pub min: T,
    pub max: T,
    pub distinct_values: HashMap<T, usize>,
    pub unique_count: usize,
    pub set_count: usize,
}

fn gen_stats<T: NativeType + PartialOrd + Eq + Hash>(array: &PrimitiveArray<T>) -> IntegerStats<T> {
    let mut stats = IntegerStats::<T> {
        tuple_count: array.len(),
        total_size: array.len() * std::mem::size_of::<T>(),
        null_count: array.null_count(),
        average_run_length: 0.0,
        is_sorted: true,
        min: T::default(),
        max: T::default(),
        distinct_values: HashMap::new(),
        unique_count: 0,
        set_count: array.len() - array.null_count(),
    };

    let _is_init_value_initialized = false;
    let mut last_value = T::default();
    let mut run_count = 0;

    let validity = array.validity();
    for (i, current_value) in array.values().iter().cloned().enumerate() {
        if is_valid(&validity, i) {
            if current_value < last_value {
                stats.is_sorted = false;
            }

            if last_value != current_value {
                run_count += 1;
                last_value = current_value;
            }
        }

        *stats.distinct_values.entry(current_value).or_insert(0) += 1;

        if current_value > stats.max {
            stats.max = current_value;
        } else if current_value < stats.min {
            stats.min = current_value;
        }
    }
    stats.unique_count = stats.distinct_values.len();
    stats.average_run_length = array.len() as f64 / run_count as f64;

    stats
}

fn choose_compressor<T: NativeType>(
    _value: &PrimitiveArray<T>,
    stats: &IntegerStats<T>,
    write_options: &WriteOptions,
) -> IntCompressor<T> {
    let basic = IntCompressor::Basic(write_options.default_compression);
    if let Some(ratio) = write_options.default_compress_ratio {
        let mut max_ratio = ratio as f64;
        let mut result = basic;
        let compressors: Vec<Box<dyn IntegerCompression<T>>> =
            vec![Box::new(RLE {}) as _, Box::new(Dict {}) as _];
        for encoder in compressors {
            if write_options
                .forbidden_compressions
                .contains(&encoder.to_compression())
            {
                continue;
            }
            let r = encoder.compress_ratio(stats);
            if r > max_ratio {
                max_ratio = r;
                result = IntCompressor::Extend(encoder);
            }
        }
        result
    } else {
        basic
    }
}
