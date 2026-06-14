//! EPICS Channel Access backend for cirrus.
//!
//! Two builds:
//!
//! - **default** (no features): exposes [`EpicsCaBackend`] as a stub whose
//!   methods all return `Backend("epics-ca disabled — enable feature `real`")`.
//!   Lets the rest of the workspace compile without dragging in `epics-ca-rs`.
//! - **`real`** feature: wires up `epics_ca_rs::CaClient` + `CaChannel`,
//!   with a sharded process-singleton client registry (rule **K3**) and
//!   in-flight de-dup via `pending: Notify` (rule **K4**). Subscription tokens
//!   propagate `Drop` to `MonitorHandle::drop`, satisfying rule **K2**.

#![deny(missing_docs)]

#[cfg(not(feature = "real"))]
mod stub;
#[cfg(not(feature = "real"))]
pub use stub::EpicsCaBackend;

#[cfg(feature = "real")]
mod real;
#[cfg(feature = "real")]
pub use real::{ca_context, CaEnumBackend, EpicsCaBackend};
