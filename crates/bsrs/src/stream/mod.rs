//! bsrs-stream — `FramePipe` + reference sources/sinks.

#![deny(missing_docs)]

pub mod pipe;
pub mod sinks;
pub mod sources;

pub use pipe::{FramePipe, FramePipeBuilder};
