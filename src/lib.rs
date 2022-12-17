#[allow(unused_must_use)]
mod compression;
mod endianess;

// pub mod read;
pub mod read;
pub mod write;

#[macro_use]
mod util;

const ARROW_MAGIC: [u8; 6] = [b'A', b'R', b'R', b'O', b'W', b'1'];
pub(crate) const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];

#[derive(
    Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize,
)]
pub struct ColumnMeta {
    pub offset: u64,
    pub length: u64,
    pub num_values: u64,
}
