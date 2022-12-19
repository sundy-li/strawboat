use arrow::error::Result;

/// Compression codec
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Compression {
    None,
    /// LZ4 (framed)
    LZ4,
    /// ZSTD
    ZSTD,
}

impl Default for Compression {
    fn default() -> Self {
        Self::None
    }
}

impl Compression {
    pub fn is_none(&self) -> bool {
        matches!(self, Compression::None)
    }

    pub fn from_codec(t: u8) -> Result<Self> {
        match t {
            0 => Ok(Compression::None),
            1 => Ok(Compression::LZ4),
            2 => Ok(Compression::ZSTD),
            other => Err(arrow::error::Error::OutOfSpec(format!(
                "Unknown compression codec {}",
                other
            ))),
        }
    }
}

impl From<Compression> for u8 {
    fn from(value: Compression) -> Self {
        match value {
            Compression::None => 0,
            Compression::LZ4 => 1,
            Compression::ZSTD => 2,
        }
    }
}

pub fn decompress_lz4(input_buf: &[u8], output_buf: &mut [u8]) -> Result<()> {
    lz4::block::decompress_to_buffer(input_buf, Some(output_buf.len() as i32), output_buf)
        .map(|_| {})
        .map_err(|e| e.into())
}

pub fn decompress_zstd(input_buf: &[u8], output_buf: &mut [u8]) -> Result<()> {
    zstd::bulk::decompress_to_buffer(input_buf, output_buf)
        .map(|_| {})
        .map_err(|e| e.into())
}

pub fn compress_lz4(input_buf: &[u8], output_buf: &mut Vec<u8>) -> Result<usize> {
    let bound = lz4::block::compress_bound(input_buf.len())?;
    output_buf.resize(bound, 0);
    lz4::block::compress_to_buffer(input_buf, None, false, output_buf.as_mut_slice())
        .map_err(|e| e.into())
}

pub fn compress_zstd(input_buf: &[u8], output_buf: &mut Vec<u8>) -> Result<usize> {
    let bound = zstd::zstd_safe::compress_bound(input_buf.len());
    output_buf.resize(bound, 0);
    zstd::bulk::compress_to_buffer(input_buf, output_buf.as_mut_slice(), 0).map_err(|e| e.into())
}

#[cfg(test)]
mod tests {}
