//! Reference sinks.

pub mod binary;
pub mod counting;

pub use binary::BinaryFrameSink;
pub use counting::CountingSink;
