//! `Server` — owns the engine, queue, registry, and the REP socket.

use std::sync::Arc;
use std::sync::Mutex as StdMutex;

use cirrus_callbacks::ZmqDocumentSink;
use cirrus_core::error::{CirrusError, Result};
use cirrus_core::msg::RunMetadata;
use cirrus_engine::{DocumentSink, RunEngine};
use tokio::sync::Mutex;
use tokio::task::AbortHandle;

use crate::dispatch::dispatch;
use crate::queue::PlanQueue;
use crate::registry::Registry;
use crate::state::{EState, EngineState};
use crate::transport::ReqRepSocket;

/// Server builder. Construct and `build()` to commit (rule **K9** — no
/// background tasks until `run_async` / `run_blocking`).
pub struct ServerBuilder {
    control_address: String,
    document_address: Option<String>,
    registry: Option<Registry>,
    /// Optional Prometheus `/metrics` HTTP listener address (e.g.
    /// `127.0.0.1:9090`). Only honored when the `metrics` feature
    /// is built.
    metrics_address: Option<String>,
}

impl Default for ServerBuilder {
    fn default() -> Self {
        Self {
            control_address: "tcp://*:60615".into(),
            document_address: Some("tcp://*:60625".into()),
            registry: None,
            metrics_address: None,
        }
    }
}

impl ServerBuilder {
    /// Override the control REP address.
    pub fn control_address(mut self, addr: impl Into<String>) -> Self {
        self.control_address = addr.into();
        self
    }
    /// Override (or disable, via `None`) the Document PUB address.
    pub fn document_address(mut self, addr: impl Into<String>) -> Self {
        self.document_address = Some(addr.into());
        self
    }
    /// Set the registered plans + devices.
    pub fn registry(mut self, r: Registry) -> Self {
        self.registry = Some(r);
        self
    }
    /// Configure the Prometheus `/metrics` listener address (e.g.
    /// `127.0.0.1:9090`). Requires the `metrics` Cargo feature.
    /// Without the feature this is a no-op.
    pub fn metrics_address(mut self, addr: impl Into<String>) -> Self {
        self.metrics_address = Some(addr.into());
        self
    }
    /// Commit. Binds the REP / PUB sockets but does not yet start serving.
    pub fn build(self) -> Result<Server> {
        let registry = self
            .registry
            .ok_or_else(|| CirrusError::State("Server requires a Registry".into()))?;
        let socket = ReqRepSocket::bind(&self.control_address)?;
        let document_sink: Option<Arc<dyn DocumentSink>> = self
            .document_address
            .as_ref()
            .map(|a| -> Result<Arc<dyn DocumentSink>> {
                Ok(Arc::new(ZmqDocumentSink::bind(a)?) as Arc<dyn DocumentSink>)
            })
            .transpose()?;
        // Install the Prometheus exporter if a metrics_address was
        // configured AND the feature is built. Idempotent: once
        // installed, subsequent ServerBuilder builds with the same
        // address are no-ops (the recorder is global per-process).
        #[cfg(feature = "metrics")]
        if let Some(addr) = self.metrics_address.as_deref() {
            let parsed: std::net::SocketAddr = addr
                .parse()
                .map_err(|e| CirrusError::State(format!("metrics_address parse: {e}")))?;
            if let Err(e) = crate::metrics::install(parsed) {
                tracing::warn!("metrics endpoint not installed: {e}");
            } else {
                tracing::info!("metrics: Prometheus /metrics on http://{parsed}");
            }
        }
        #[cfg(not(feature = "metrics"))]
        if self.metrics_address.is_some() {
            tracing::warn!(
                "metrics_address set but cirrus-qs was built without --features metrics; ignoring"
            );
        }
        Ok(Server {
            socket,
            document_sink,
            registry: Arc::new(registry),
            queue: Arc::new(StdMutex::new(PlanQueue::new())),
            state: Arc::new(StdMutex::new(EngineState::initial())),
            engine: Arc::new(Mutex::new(None)),
            queue_task: Arc::new(StdMutex::new(None)),
        })
    }
}

/// The cirrus-qs server.
pub struct Server {
    pub(crate) socket: ReqRepSocket,
    document_sink: Option<Arc<dyn DocumentSink>>,
    registry: Arc<Registry>,
    queue: Arc<StdMutex<PlanQueue>>,
    state: Arc<StdMutex<EngineState>>,
    engine: Arc<Mutex<Option<Arc<RunEngine>>>>,
    /// AbortHandle for the currently-running `execute_queue_loop`, if any.
    /// Stored so [`ServerShutdown::shutdown`] can stop the worker mid-plan
    /// (rule **K1**: spawned task must terminate when its owner drops).
    queue_task: Arc<StdMutex<Option<AbortHandle>>>,
}

impl Server {
    /// Builder.
    pub fn builder() -> ServerBuilder {
        ServerBuilder::default()
    }

    /// Async entry point. The REP-socket loop runs on a dedicated blocking
    /// thread (libzmq REP is sync in the `zmq` crate). Plan execution
    /// happens on the cirrus runtime.
    pub async fn run_async(&self) -> Result<()> {
        let socket = self.socket.clone();
        let registry = self.registry.clone();
        let queue = self.queue.clone();
        let state = self.state.clone();
        let engine = self.engine.clone();
        let document_sink = self.document_sink.clone();
        let queue_task = self.queue_task.clone();

        let join = tokio::task::spawn_blocking(move || {
            let rt = tokio::runtime::Handle::current();
            rep_loop(
                rt,
                socket,
                registry,
                queue,
                state,
                engine,
                document_sink,
                queue_task,
            )
        });
        join.await
            .map_err(|e| CirrusError::Backend(format!("rep loop join: {e}")))?
    }

    /// Sync entry point.
    pub fn run_blocking(self) -> Result<()> {
        cirrus_core::runtime::block_on(self.run_async())
    }

    /// Engine getter (test only).
    #[doc(hidden)]
    pub fn engine_arc(&self) -> Arc<Mutex<Option<Arc<RunEngine>>>> {
        self.engine.clone()
    }

    /// State getter (test only).
    #[doc(hidden)]
    pub fn state_arc(&self) -> Arc<StdMutex<EngineState>> {
        self.state.clone()
    }

    /// Get a `ServerShutdown` handle. Calling it signals the REP loop to
    /// exit at its next iteration (within ~200 ms) and aborts any
    /// in-flight queue execution task.
    pub fn shutdown_handle(&self) -> ServerShutdown {
        ServerShutdown {
            socket: self.socket.clone(),
            queue_task: self.queue_task.clone(),
        }
    }
}

/// Lightweight handle returned by [`Server::shutdown_handle`].
#[derive(Clone)]
pub struct ServerShutdown {
    socket: ReqRepSocket,
    queue_task: Arc<StdMutex<Option<AbortHandle>>>,
}

impl ServerShutdown {
    /// Signal the server to exit. The REP loop ends within ~200 ms, and
    /// any in-flight queue execution task is aborted (rule **K1**).
    pub fn shutdown(&self) {
        self.socket.shutdown();
        if let Some(h) = self.queue_task.lock().unwrap().take() {
            h.abort();
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn rep_loop(
    rt: tokio::runtime::Handle,
    socket: ReqRepSocket,
    registry: Arc<Registry>,
    queue: Arc<StdMutex<PlanQueue>>,
    state: Arc<StdMutex<EngineState>>,
    engine: Arc<Mutex<Option<Arc<RunEngine>>>>,
    document_sink: Option<Arc<dyn DocumentSink>>,
    queue_task: Arc<StdMutex<Option<AbortHandle>>>,
) -> Result<()> {
    while !socket.is_shutdown() {
        let req = match socket.try_recv() {
            Ok(Some(r)) => r,
            Ok(None) => continue, // recv timeout, poll shutdown again
            Err(_) => continue,   // parse error already responded
        };
        let resp = dispatch(
            &rt,
            &req,
            registry.clone(),
            queue.clone(),
            state.clone(),
            engine.clone(),
            document_sink.clone(),
            queue_task.clone(),
        );
        if let Err(e) = socket.send(&resp) {
            tracing::warn!(target: "cirrus-qs", "rep_loop: send error: {e}");
        }
    }
    // Loop exited (shutdown). Make absolutely sure the queue worker is gone.
    if let Some(h) = queue_task.lock().unwrap().take() {
        h.abort();
    }
    Ok(())
}

pub(crate) async fn execute_queue_loop(
    re: Arc<RunEngine>,
    registry: Arc<Registry>,
    queue: Arc<StdMutex<PlanQueue>>,
    state: Arc<StdMutex<EngineState>>,
    task_slot: Arc<StdMutex<Option<AbortHandle>>>,
) {
    // Always clear the slot when we exit, so the slot reflects "no live
    // worker" and a future shutdown does not abort an unrelated handle.
    let _slot_guard = ClearOnDrop(task_slot.clone());
    loop {
        // Honor queue_stop_pending: drain to idle without running the next item.
        if state.lock().unwrap().queue_stop_pending {
            let mut s = state.lock().unwrap();
            s.queue_stop_pending = false;
            s.state = Some(EState::Idle);
            return;
        }
        let item = queue.lock().unwrap().pop_front();
        let item = match item {
            Some(it) => it,
            None => {
                state.lock().unwrap().state = Some(EState::Idle);
                return;
            }
        };
        {
            let mut s = state.lock().unwrap();
            s.state = Some(EState::ExecutingQueue);
            s.current_plan_name = Some(item.name.clone());
        }
        let factory = match registry.plan(&item.name) {
            Some(f) => f.clone(),
            None => {
                tracing::error!("queue: unknown plan {}", item.name);
                let mut s = state.lock().unwrap();
                s.plans_failed += 1;
                let archived = item.clone().with_result(serde_json::json!({
                    "exit_status": "fail",
                    "reason": "unknown plan",
                }));
                drop(s);
                queue.lock().unwrap().push_history(archived);
                continue;
            }
        };
        let plan = match factory(&registry, &item.args) {
            Ok(p) => p,
            Err(e) => {
                tracing::error!("queue: plan {} build failed: {e}", item.name);
                let mut s = state.lock().unwrap();
                s.plans_failed += 1;
                let archived = item.clone().with_result(serde_json::json!({
                    "exit_status": "fail",
                    "reason": format!("plan build failed: {e}"),
                }));
                drop(s);
                queue.lock().unwrap().push_history(archived);
                continue;
            }
        };
        let _ = item.meta;
        let _meta = RunMetadata {
            scan_id: None,
            plan_name: Some(item.name.clone()),
            extra: Default::default(),
        };
        let run_result = re.run_async(plan).await;
        let exit_status = match &run_result {
            Ok(r) => r.exit_status.clone(),
            Err(_) => "fail".to_string(),
        };
        let run_uid = run_result.as_ref().ok().and_then(|r| r.run_uid.clone());
        // Bookkeeping after the run.
        {
            let mut s = state.lock().unwrap();
            s.plans_run += 1;
            s.current_run_uid = run_uid.clone();
            s.current_plan_name = None;
            if let Some(uid) = &run_uid {
                s.re_runs.push(uid.clone());
                if s.re_runs.len() > 64 {
                    let drop_n = s.re_runs.len() - 64;
                    s.re_runs.drain(0..drop_n);
                }
            }
            if exit_status == "abort" || exit_status == "fail" || exit_status == "halt" {
                s.plans_failed += 1;
            }
        }
        // Archive the item with its result.
        let archived = item.clone().with_result(serde_json::json!({
            "exit_status": exit_status,
            "run_uid": run_uid,
        }));
        queue.lock().unwrap().push_history(archived);
        // Loop mode: re-enqueue at the back (bluesky's "loop" plan_queue_mode).
        if state
            .lock()
            .unwrap()
            .queue_mode
            .get("loop")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            queue.lock().unwrap().push_back(item);
        }
        // On non-success, idle out (matches bluesky behaviour: queue_start
        // halts on error).
        if exit_status != "success" {
            state.lock().unwrap().state = Some(EState::Idle);
            return;
        }
    }
}

/// RAII guard: clears the queue-task slot on drop so a future
/// `ServerShutdown::shutdown` doesn't abort an already-finished handle.
struct ClearOnDrop(Arc<StdMutex<Option<AbortHandle>>>);

impl Drop for ClearOnDrop {
    fn drop(&mut self) {
        *self.0.lock().unwrap() = None;
    }
}
