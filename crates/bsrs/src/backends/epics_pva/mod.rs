//! EPICS PV Access backend for bsrs.
//!
//! Like the CA backend, this crate is feature-gated. Default build = stub;
//! `--features real` wires up `epics-pva-rs::PvaClient`.

#![deny(missing_docs)]

#[cfg(not(feature = "pva"))]
mod stub;
#[cfg(not(feature = "pva"))]
pub use stub::EpicsPvaBackend;

#[cfg(feature = "pva")]
mod real;
#[cfg(feature = "pva")]
pub use real::{pva_context, EpicsPvaBackend};
