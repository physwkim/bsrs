//! EPICS Channel Access backend for bsrs.
//!
//! Two builds:
//!
//! - **`ca`** feature (on by default): wires up `epics_ca_rs::CaClient` +
//!   `CaChannel`, with a sharded process-singleton client registry (rule
//!   **K3**) and in-flight de-dup via `pending: Notify` (rule **K4**).
//!   Subscription tokens propagate `Drop` to `MonitorHandle::drop`,
//!   satisfying rule **K2**.
//! - **`--no-default-features`**: exposes [`EpicsCaBackend`] as a stub whose
//!   methods all return the `epics-ca backend disabled` error, letting the
//!   rest of the workspace compile without dragging in `epics-ca-rs`.

#![deny(missing_docs)]

#[cfg(not(feature = "ca"))]
mod stub;
#[cfg(not(feature = "ca"))]
pub use stub::EpicsCaBackend;

#[cfg(feature = "ca")]
mod real;
#[cfg(feature = "ca")]
pub use real::{ca_context, CaEnumBackend, EpicsCaBackend};
