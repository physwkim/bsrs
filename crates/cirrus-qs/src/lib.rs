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
//! ## What's implemented
//!
//! - 0MQ REP server (control plane).
//! - Plan queue (FIFO).
//! - State machine: `idle / executing_queue / paused / aborting`.
//! - Plan / device registry — Rust-native, no Python.
//! - Document broadcast via cirrus-callbacks `ZmqDocumentSink` (separate
//!   PUB socket).
//! - 10 most-used RPC methods (see `methods.rs`).
//!
//! ## What's deferred
//!
//! - IPython kernel mode.
//! - `script_upload` / `function_execute`.
//! - Lock manager / permissions / user groups.
//! - Plan history persistence.
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

mod methods;
mod queue;
mod registry;
mod server;
mod state;
mod transport;

pub use methods::{JsonRpcError, RpcRequest, RpcResponse};
pub use queue::{PlanQueue, QueuedItem};
pub use registry::{PlanFactory, Registry};
pub use server::{Server, ServerBuilder, ServerShutdown};
pub use state::{EState, EngineState};
