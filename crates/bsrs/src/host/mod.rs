//! bsrs host runtime — reusable components for embedding a bsrs
//! `RunEngine` + queue server into a process.
//!
//! Split out of `bsrs-cli` (which is now a thin binary on top) so
//! downstream binaries — e.g. a beamline-specific `qs-manager` that
//! registers composite ophyd-style devices — can reuse the same Lua
//! bridge and CA/PVA device factories without depending on the CLI
//! crate.
//!
//! ## Surface
//!
//! - [`lua_env::build_lua`] — construct an `mlua::Lua` state with
//!   bsrs types (`LuaRunEngine`, `LuaDevice`, plan factories, bp/bps
//!   namespaces, …) pre-registered.
//! - [`manager_lua::ManagerLuaState`] — implements
//!   `crate::qs::LuaEvaluator`; auto-publishes every device in the
//!   `Registry` as a Lua global.
//! - [`ca_devices::CaMotor`] / [`ca_devices::CaDetector`] — CA-backed
//!   ophyd-equivalent device handles (feature `ca`).
//! - [`pva_devices::PvaMotor`] / [`pva_devices::PvaDetector`] — same
//!   for PV Access (feature `pva`).
//! - [`checkpoint_store::JsonlCheckpointStore`] — JSONL-backed
//!   `CheckpointHook` for crash-recovery audit.
//! - [`lua_tiled`] — `tiled.*` Lua bindings (feature `tiled`).
//! - [`ca_suspender`] — CA-PV-backed `SuspendThreshold` / boolean
//!   suspender installers (feature `ca`).

#[cfg(feature = "ca")]
pub mod areadetector;
#[cfg(feature = "ca")]
pub mod ca_devices;
#[cfg(feature = "ca")]
pub mod ca_suspender;
pub mod checkpoint_store;
pub mod lua_env;
#[cfg(feature = "tiled")]
pub mod lua_tiled;
pub mod manager_lua;
#[cfg(feature = "pva")]
pub mod pva_devices;
