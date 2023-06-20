//! APIs to write to Arrow's IPC format.
pub(crate) mod binary;
pub(crate) mod boolean;
pub(crate) mod common;
pub(crate) mod primitive;
mod serialize;
pub(crate) mod writer;

pub use common::WriteOptions;
pub use serialize::write;
pub use serialize::write_buffer;
pub use writer::NativeWriter;
