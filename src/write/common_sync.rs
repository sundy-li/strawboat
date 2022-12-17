use std::io::Write;

use arrow::error::Result;

use super::super::CONTINUATION_MARKER;

/// Write a record batch to the writer, writing the message size before the message
/// if the record batch is being written to a stream
pub fn write_continuation<W: Write>(writer: &mut W, total_len: i32) -> Result<usize> {
    writer.write_all(&CONTINUATION_MARKER)?;
    writer.write_all(&total_len.to_le_bytes()[..])?;
    Ok(8)
}
