//! cirrus-devices — Signal, StandardDetector, and helpers for building Device
//! trees on top of `SignalBackend`.

#![deny(missing_docs)]

pub mod detector;
pub mod signal;

pub use detector::{StandardDetector, TriggerInfo};
pub use signal::{Signal, SignalConfig, SignalKind};

/// Re-export of the `#[derive(Device)]` proc-macro.
pub use cirrus_derive::Device;
