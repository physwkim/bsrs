//! Prometheus metrics endpoint for bsrs-qs.
//!
//! Behind the `metrics` Cargo feature. When enabled, the server can
//! be configured to start a `/metrics` HTTP listener via
//! [`Server::metrics_address`](crate::qs::Server::metrics_address) (or
//! [`ServerBuilder::metrics_address`]). The metrics are emitted in
//! the standard Prometheus text format.
//!
//! # Recorded metrics
//!
//! - `bsrs_qs_rpc_calls_total{method=...}` (counter)
//! - `bsrs_qs_rpc_errors_total{method=...}` (counter)
//! - `bsrs_qs_queue_depth` (gauge — current queue size)
//! - `bsrs_qs_runs_total{exit_status=...}` (counter — incremented
//!   on each engine `RunResult` returned through `queue_item_execute`
//!   / queue worker)
//! - `bsrs_qs_documents_total{name=...}` (counter — bumped by the
//!   document broadcast path; useful sanity check that the engine is
//!   emitting expected doc kinds)
//!
//! When the feature is disabled, all hooks are no-op `inline` macros
//! so bsrs-qs without `--features metrics` is unchanged.

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use metrics_exporter_prometheus::PrometheusBuilder;

use crate::callbacks::document_name;
use crate::core::error::Result;
use crate::engine::DocumentSink;
use crate::event_model::Document;

/// Initialize the Prometheus exporter on the given socket address.
/// Idempotent; second call is a no-op (PrometheusBuilder installs a
/// global recorder).
pub fn install(addr: SocketAddr) -> Result<(), String> {
    PrometheusBuilder::new()
        .with_http_listener(addr)
        .install()
        .map_err(|e| format!("metrics exporter install: {e}"))
}

/// Record an RPC call.
pub fn rpc_call(method: &str) {
    metrics::counter!("bsrs_qs_rpc_calls_total", "method" => method.to_string()).increment(1);
}

/// Record an RPC error response.
pub fn rpc_error(method: &str) {
    metrics::counter!("bsrs_qs_rpc_errors_total", "method" => method.to_string()).increment(1);
}

/// Update the queue-depth gauge.
pub fn queue_depth(depth: usize) {
    metrics::gauge!("bsrs_qs_queue_depth").set(depth as f64);
}

/// Record one finished run with its exit status.
pub fn run_finished(exit_status: &str) {
    metrics::counter!(
        "bsrs_qs_runs_total",
        "exit_status" => exit_status.to_string()
    )
    .increment(1);
}

/// Record one document by kind.
pub fn document(name: &str) {
    metrics::counter!("bsrs_qs_documents_total", "name" => name.to_string()).increment(1);
}

/// `DocumentSink` wrapper that counts each document by kind
/// (`bsrs_qs_documents_total{name=...}`) before forwarding to the real
/// broadcast sink. The server wraps its document sink with this at
/// build time, so every document on the broadcast path is counted at
/// exactly one place.
pub struct CountingSink {
    inner: Arc<dyn DocumentSink>,
}

impl CountingSink {
    pub fn new(inner: Arc<dyn DocumentSink>) -> Self {
        CountingSink { inner }
    }
}

#[async_trait]
impl DocumentSink for CountingSink {
    async fn dispatch(&self, doc: &Document) -> Result<()> {
        document(document_name(doc));
        self.inner.dispatch(doc).await
    }
}
