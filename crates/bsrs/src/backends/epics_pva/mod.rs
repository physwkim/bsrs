//! EPICS PV Access backend for bsrs.
//!
//! Like the CA backend, this is feature-gated. The `pva` feature (on by
//! default) wires up `epics-pva-rs::PvaClient`; `--no-default-features`
//! falls back to a stub that errors on call.

#![deny(missing_docs)]

#[cfg(not(feature = "pva"))]
mod stub;
#[cfg(not(feature = "pva"))]
pub use stub::EpicsPvaBackend;

#[cfg(feature = "pva")]
mod real;
#[cfg(feature = "pva")]
pub use real::{pva_context, EpicsPvaBackend};
