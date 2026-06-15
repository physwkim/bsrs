//! EPICS PV Access backend for bsrs.
//!
//! Like the CA backend, this crate is feature-gated. Default build = stub;
//! `--features real` wires up `epics-pva-rs::PvaClient`.

#![deny(missing_docs)]

#[cfg(not(feature = "real"))]
mod stub;
#[cfg(not(feature = "real"))]
pub use stub::EpicsPvaBackend;

#[cfg(feature = "real")]
mod real;
#[cfg(feature = "real")]
pub use real::{pva_context, EpicsPvaBackend};
