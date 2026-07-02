//! Signal backends: in-memory `soft`, `mock`, and EPICS `epics_ca` / `epics_pva`.
//!
//! The EPICS backends self-gate: without the `ca` / `pva` feature they compile
//! as stubs (every method returns a `Backend("… disabled")` error) so the crate
//! builds on hosts without the EPICS-rs stack; with the feature they link
//! `epics-ca-rs` / `epics-pva-rs` and expose the real backend.

#![deny(missing_docs)]

pub mod mock;
pub mod soft;

pub mod epics_ca;
pub mod epics_pva;
