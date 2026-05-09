//! cirrus-qs — queueserver-compatible 0MQ JSON-RPC daemon.
//!
//! Exposes the bluesky-queueserver external API (the JSON-RPC methods that
//! `qserver` CLI / `bluesky-httpserver` use) over a 0MQ REP socket. Internally
//! drives [`cirrus_engine::RunEngine`] for plan execution.
//!
//! This is a **standalone replacement for the queueserver manager+worker pair**
//! when you want a pure-Rust orchestration stack. Operations clients (qserver
//! CLI, web UI) connect at the same `tcp://*:60615` endpoint and speak the
//! same protocol.
//!
//! ## What's implemented (39 / 50 methods)
//!
//! - 0MQ REP server (control plane).
//! - Plan queue with history (FIFO + archived completed items).
//! - State machine: `idle / executing_queue / paused / aborting`.
//! - Plan / device registry — Rust-native, no Python.
//! - Document broadcast via cirrus-callbacks `ZmqDocumentSink` (separate
//!   PUB socket).
//! - All queue ops: add / add_batch / update / get / remove /
//!   remove_batch / move / move_batch / execute / clear /
//!   start / stop / stop_cancel / autostart / mode_set
//! - History: history_get / history_clear
//! - Environment: open / close / destroy / update
//! - RE control: pause / resume / abort / halt / stop / runs / metadata
//! - Listings: plans_allowed / plans_existing / devices_allowed /
//!   devices_existing
//! - Lock manager: lock / lock_info / unlock (subsystem-aware key check)
//! - status response includes the full bluesky shape
//!   (`re_state`, `worker_environment_*`, `queue_*_uid`, `lock_info_uid`,
//!   etc.)
//!
//! ## What returns NOT_IMPLEMENTED (registered, but stub-only)
//!
//! These methods are declared in the dispatch table so clients see a
//! defined error code instead of `METHOD_NOT_FOUND`. They are
//! bluesky-queueserver-specific and don't translate to cirrus's
//! single-binary, no-IPython, no-permission-ACL model:
//!
//! - `permissions_reload`, `permissions_get`, `permissions_set`
//! - `script_upload`, `function_execute`
//! - `task_result`, `task_status`
//! - `kernel_interrupt`
//! - `manager_stop`, `manager_kill`, `manager_test`
//!
//! ## Example
//!
//! ```ignore
//! use cirrus_qs::{Server, Registry};
//! use cirrus_backend_soft::SoftDetector;
//! use std::sync::Arc;
//!
//! # async fn run() -> cirrus_core::error::Result<()> {
//! let det = SoftDetector::new("det1");
//! let mut reg = Registry::new();
//! reg.register_readable("det1", det as Arc<dyn cirrus_core::msg::ReadableObj>);
//! reg.register_plan_count("count");
//!
//! let server = Server::builder()
//!     .control_address("tcp://*:60615")
//!     .document_address("tcp://*:60625")
//!     .registry(reg)
//!     .build()?;
//! server.run_async().await?;
//! # Ok(())
//! # }
//! ```

#![deny(missing_docs)]

mod dispatch;
mod methods;
#[cfg(feature = "metrics")]
mod metrics;
mod queue;
mod registry;
mod server;
mod state;
mod transport;

pub use methods::{JsonRpcError, RpcRequest, RpcResponse};
pub use queue::{PlanQueue, QueuedItem};
pub use registry::{PlanFactory, Registry};
pub use server::{Server, ServerBuilder, ServerShutdown};
pub use state::{EState, EngineState, LockInfo};
