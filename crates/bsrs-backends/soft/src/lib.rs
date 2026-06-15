//! Soft (in-memory) backend — for tests, demos, and simulators.
//!
//! Mirrors `ophyd_async/core/_soft_signal_backend.py`. Stores a value in a
//! tokio `watch` channel; `connect`/`get_*`/`put` are local operations.

#![deny(missing_docs)]

pub mod detector;
pub mod motor;
pub mod signal;

pub use detector::{SoftDetector, SoftDetectorControl, SoftDetectorWriter};
pub use motor::SoftMotor;
pub use signal::SoftSignalBackend;
