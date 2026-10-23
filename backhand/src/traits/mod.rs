//! Shared traits for v3 and v4 SquashFS implementations

pub mod block_reader;
pub mod compression;
pub mod decompression;
pub mod error;
pub mod file_reader;
pub mod filesystem;
pub mod squashfs;
pub mod types;

pub use compression::CompressionAction;
pub use error::BackhandError;
pub use filesystem::FilesystemReaderTrait;
pub use squashfs::{GenericSquashfs, SquashfsVersion};
pub use types::Compressor;
