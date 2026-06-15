//! Reference sinks.

pub mod binary;
pub mod counting;
#[cfg(feature = "hdf5")]
pub mod hdf5;

pub use binary::BinaryFrameSink;
pub use counting::CountingSink;
#[cfg(feature = "hdf5")]
pub use hdf5::Hdf5FrameSink;
