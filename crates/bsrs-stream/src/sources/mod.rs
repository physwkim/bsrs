//! Reference sources.

pub mod vec;
pub use vec::VecFrameSource;

#[cfg(feature = "pva")]
pub mod pva_mon;
#[cfg(feature = "pva")]
pub use pva_mon::PvaMonitorSource;
