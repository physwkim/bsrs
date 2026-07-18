//! `Server` — owns the engine, queue, registry, and the REP socket.

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;

use crate::callbacks::ZmqDocumentSink;
use crate::core::error::{BsrsError, Result};
use crate::engine::{DocumentSink, RunEngine, RunOptions};
use tokio::sync::Mutex;

use crate::engine::CheckpointHook;
use crate::qs::dispatch::dispatch;
use crate::qs::lua_eval::LuaEvaluator;
use crate::qs::permissions::Permissions;
use crate::qs::queue::PlanQueue;
use crate::qs::registry::Registry;
use crate::qs::state::{EState, EngineState};
use crate::qs::task_slot::{QueueTaskSlot, SlotClaim};
use crate::qs::tasks::TaskTracker;
use crate::qs::transport::ReqRepSocket;

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
    /// Optional permissions.toml path. Without this, the server runs
    /// permissive (any caller is `default_group = primary` and
    /// `primary` allows everything).
    permissions_path: Option<std::path::PathBuf>,
    /// Optional Lua evaluator. Without this, the `lua_eval` RPC
    /// returns `NOT_IMPLEMENTED`.
    lua_evaluator: Option<Arc<dyn LuaEvaluator>>,
    /// Optional pre-allocated engine slot. The daemon-side Lua
    /// bridge needs to share the slot with the server so it can
    /// resolve `RE` lazily; supplying it here avoids constructing
    /// two slots that fight over the same engine identity.
    engine_slot: Option<Arc<Mutex<Option<Arc<RunEngine>>>>>,
    /// Optional `CheckpointHook` installed on the engine the moment
    /// `environment_open` creates it. Avoids the "watcher race"
    /// where a polling task tries to install a hook after the engine
    /// is born — a fast plan can run to completion before the
    /// watcher catches up.
    checkpoint_hook: Option<CheckpointHook>,
    /// Explicit CURVE private key (Z85, 40 chars). Takes precedence over
    /// the `QSERVER_ZMQ_PRIVATE_KEY` env var. If neither is set, CURVE
    /// is disabled (plaintext). Mirrors the reference's config mechanism
    /// (comms.py / manager.py).
    curve_private_key: Option<String>,
}

impl Default for ServerBuilder {
    fn default() -> Self {
        Self {
            control_address: "tcp://*:60615".into(),
            document_address: Some("tcp://*:60625".into()),
            registry: None,
            metrics_address: None,
            permissions_path: None,
            lua_evaluator: None,
            engine_slot: None,
            checkpoint_hook: None,
            curve_private_key: None,
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
    /// Do not bind a Document PUB socket. The server's document sink is used
    /// only when `environment_open` creates the engine; a caller that
    /// *pre-opens* the engine (seeding `engine_slot` itself, with its own
    /// document sink already attached) must call this so the server does not
    /// contend for the same PUB address. Used by the fused console.
    pub fn without_document_socket(mut self) -> Self {
        self.document_address = None;
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
    /// Load RBAC policy from a TOML file. The dispatcher consults the
    /// loaded policy on every request and returns
    /// `codes::NOT_AUTHORIZED` for denied calls. Without this, the
    /// server runs permissive — every method is allowed for the
    /// `default_group = primary`.
    pub fn permissions_path(mut self, path: impl Into<std::path::PathBuf>) -> Self {
        self.permissions_path = Some(path.into());
        self
    }
    /// Provide a [`LuaEvaluator`] for the `lua_eval` RPC. Without
    /// this, `lua_eval` returns `NOT_IMPLEMENTED`. The evaluator
    /// shares state across calls (typical impls hold one mlua state
    /// behind a mutex; see the bsrs-cli `manager` module).
    pub fn lua_evaluator(mut self, ev: Arc<dyn LuaEvaluator>) -> Self {
        self.lua_evaluator = Some(ev);
        self
    }
    /// Override the engine slot. Lets a daemon-side bridge share the
    /// same `Arc<Mutex<Option<Arc<RunEngine>>>>` with the server, so
    /// when `environment_open` populates it the bridge sees the same
    /// engine. If unset, the server builds a fresh empty slot.
    pub fn engine_slot(mut self, slot: Arc<Mutex<Option<Arc<RunEngine>>>>) -> Self {
        self.engine_slot = Some(slot);
        self
    }
    /// Install a `CheckpointHook` on the engine the moment
    /// `environment_open` creates it. The hook is invoked
    /// synchronously on every `Msg::Checkpoint`; it must be quick.
    /// Used by the daemon's crash-recovery JSONL store.
    pub fn checkpoint_hook(mut self, hook: CheckpointHook) -> Self {
        self.checkpoint_hook = Some(hook);
        self
    }
    /// Enable ZMQ CURVE encryption using the given Z85-encoded private key
    /// (40 characters). Overrides the `QSERVER_ZMQ_PRIVATE_KEY` env var.
    ///
    /// Mirrors the reference: `comms.py` / `manager.py` accept the private
    /// key as a config value that is applied to the REP socket before bind.
    /// Without this (and without the env var), the socket accepts plain-text
    /// connections.
    pub fn curve_private_key(mut self, key: impl Into<String>) -> Self {
        self.curve_private_key = Some(key.into());
        self
    }
    /// Commit. Binds the REP / PUB sockets but does not yet start serving.
    pub fn build(self) -> Result<Server> {
        let registry = self
            .registry
            .ok_or_else(|| BsrsError::State("Server requires a Registry".into()))?;
        // Resolve CURVE private key: explicit field first, then env var
        // (mirrors comms.py / manager.py config resolution).
        let curve_key = self.curve_private_key.or_else(|| {
            std::env::var("QSERVER_ZMQ_PRIVATE_KEY")
                .ok()
                .filter(|s| !s.is_empty())
        });
        let socket = ReqRepSocket::bind(&self.control_address, curve_key.as_deref())?;
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
                .map_err(|e| BsrsError::State(format!("metrics_address parse: {e}")))?;
            if let Err(e) = crate::qs::metrics::install(parsed) {
                tracing::warn!("metrics endpoint not installed: {e}");
            } else {
                tracing::info!("metrics: Prometheus /metrics on http://{parsed}");
            }
        }
        #[cfg(not(feature = "metrics"))]
        if self.metrics_address.is_some() {
            tracing::warn!(
                "metrics_address set but bsrs-qs was built without --features metrics; ignoring"
            );
        }
        let permissions = match self.permissions_path.as_deref() {
            Some(p) => Arc::new(
                Permissions::load_from_file(p)
                    .map_err(|e| BsrsError::State(format!("permissions: {e}")))?,
            ),
            None => Arc::new(Permissions::permissive()),
        };
        let engine = self
            .engine_slot
            .unwrap_or_else(|| Arc::new(Mutex::new(None)));
        Ok(Server {
            socket,
            document_sink,
            registry: Arc::new(registry),
            queue: Arc::new(StdMutex::new(PlanQueue::new())),
            state: Arc::new(StdMutex::new(EngineState::initial())),
            engine,
            queue_task: Arc::new(QueueTaskSlot::new()),
            permissions,
            lua_evaluator: self.lua_evaluator,
            task_tracker: Arc::new(TaskTracker::new()),
            checkpoint_hook: self.checkpoint_hook,
        })
    }
}

/// The bsrs-qs server.
pub struct Server {
    pub(crate) socket: ReqRepSocket,
    document_sink: Option<Arc<dyn DocumentSink>>,
    registry: Arc<Registry>,
    queue: Arc<StdMutex<PlanQueue>>,
    state: Arc<StdMutex<EngineState>>,
    engine: Arc<Mutex<Option<Arc<RunEngine>>>>,
    /// Claim slot for the currently-running queue worker
    /// (`execute_queue_loop` / `execute_single_item`), if any. Stored so
    /// [`ServerShutdown::shutdown`] can stop the worker mid-plan (rule
    /// **K1**: spawned task must terminate when its owner drops).
    queue_task: Arc<QueueTaskSlot>,
    permissions: Arc<Permissions>,
    lua_evaluator: Option<Arc<dyn LuaEvaluator>>,
    task_tracker: Arc<TaskTracker>,
    checkpoint_hook: Option<CheckpointHook>,
}

impl Server {
    /// Builder.
    pub fn builder() -> ServerBuilder {
        ServerBuilder::default()
    }

    /// Async entry point. The REP-socket loop runs on a dedicated blocking
    /// thread (libzmq REP is sync in the `zmq` crate). Plan execution
    /// happens on the bsrs runtime.
    pub async fn run_async(&self) -> Result<()> {
        let socket = self.socket.clone();
        let registry = self.registry.clone();
        let queue = self.queue.clone();
        let state = self.state.clone();
        let engine = self.engine.clone();
        let document_sink = self.document_sink.clone();
        let queue_task = self.queue_task.clone();
        let permissions = self.permissions.clone();
        let lua_evaluator = self.lua_evaluator.clone();
        let task_tracker = self.task_tracker.clone();
        let checkpoint_hook = self.checkpoint_hook.clone();

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
                permissions,
                lua_evaluator,
                task_tracker,
                checkpoint_hook,
            )
        });
        join.await
            .map_err(|e| BsrsError::Backend(format!("rep loop join: {e}")))?
    }

    /// Sync entry point.
    pub fn run_blocking(self) -> Result<()> {
        crate::core::runtime::block_on(self.run_async())
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

    /// The resolved control (REP) endpoint this server bound to. For a
    /// wildcard bind (`tcp://127.0.0.1:*`) this is the concrete OS-assigned
    /// address.
    pub fn control_endpoint(&self) -> &str {
        self.socket.endpoint()
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
    queue_task: Arc<QueueTaskSlot>,
}

impl ServerShutdown {
    /// Signal the server to exit. The REP loop ends within ~200 ms, and
    /// any in-flight queue execution task is aborted (rule **K1**).
    pub fn shutdown(&self) {
        self.socket.shutdown();
        if let Some(h) = self.queue_task.take() {
            h.abort();
        }
    }

    /// The resolved control (REP) endpoint the server bound to.
    pub fn control_endpoint(&self) -> &str {
        self.socket.endpoint()
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
    queue_task: Arc<QueueTaskSlot>,
    permissions: Arc<Permissions>,
    lua_evaluator: Option<Arc<dyn LuaEvaluator>>,
    task_tracker: Arc<TaskTracker>,
    checkpoint_hook: Option<CheckpointHook>,
) -> Result<()> {
    let stop_requested = Arc::new(AtomicBool::new(false));
    while !socket.is_shutdown() {
        let (req, encoding) = match socket.try_recv() {
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
            permissions.clone(),
            lua_evaluator.clone(),
            task_tracker.clone(),
            checkpoint_hook.clone(),
            stop_requested.clone(),
        );
        if let Err(e) = socket.send(&resp, encoding) {
            tracing::warn!(target: "bsrs-qs", "rep_loop: send error: {e}");
        }
        if stop_requested.load(std::sync::atomic::Ordering::SeqCst) {
            socket.shutdown();
            break;
        }
    }
    // Loop exited (shutdown). Make absolutely sure the queue worker is gone.
    if let Some(h) = queue_task.take() {
        h.abort();
    }
    Ok(())
}

/// Return the manager to idle when the queue worker exits.
/// `disable_autostart` — true on every failure/stop exit (ref:
/// manager.py:852,1080: a failed/interrupted plan and queue_stop
/// deactivate autostart); false only on a normal drain to an empty
/// queue, which keeps autostart armed (manager.py:1085).
fn idle_out(state: &Arc<StdMutex<EngineState>>, disable_autostart: bool) {
    let mut s = state.lock().unwrap();
    s.state = Some(EState::Idle);
    s.pause_pending = false;
    s.current_plan_name = None;
    if disable_autostart {
        s.queue_autostart_enabled = false;
    }
}

pub(crate) async fn execute_queue_loop(
    re: Arc<RunEngine>,
    registry: Arc<Registry>,
    queue: Arc<StdMutex<PlanQueue>>,
    state: Arc<StdMutex<EngineState>>,
    claim: SlotClaim,
) {
    // Hold the slot claim for the worker's whole life; its drop (normal
    // exit or abort) releases the slot — generation-checked, so a stale
    // worker can never clear a successor's claim.
    let _claim = claim;
    loop {
        // Honor queue_stop_pending: drain to idle without running the next item.
        if state.lock().unwrap().queue_stop_pending {
            state.lock().unwrap().queue_stop_pending = false;
            idle_out(&state, true);
            return;
        }
        let item = queue.lock().unwrap().pop_front();
        let item = match item {
            Some(it) => it,
            None => {
                idle_out(&state, false);
                return;
            }
        };
        // Handle instruction items (ref: manager.py:1154-1165).
        if item.item_type == "instruction" {
            if item.name == "queue_stop" {
                let archived = item.with_result(serde_json::json!({"exit_status": "completed"}));
                queue.lock().unwrap().push_history(archived);
                state.lock().unwrap().queue_stop_pending = true;
                continue; // next iteration checks queue_stop_pending and exits
            } else {
                // Uniform rule: any item that cannot start stops the queue.
                tracing::error!("queue: unknown instruction: {}", item.name);
                let reason = format!("unknown instruction: {}", item.name);
                let archived = item.with_result(serde_json::json!({
                    "exit_status": "fail",
                    "reason": reason,
                }));
                queue.lock().unwrap().push_history(archived);
                idle_out(&state, true);
                return;
            }
        }

        let factory = match registry.plan(&item.name) {
            Some(f) => f.clone(),
            None => {
                // An item that fails to start stops the queue like a failed
                // run (ref: manager.py:847-852 — "failed" → idle + autostart
                // disable), instead of silently skipping to the next item.
                tracing::error!("queue: unknown plan {}", item.name);
                state.lock().unwrap().plans_failed += 1;
                let archived = item.clone().with_result(serde_json::json!({
                    "exit_status": "fail",
                    "reason": "unknown plan",
                }));
                queue.lock().unwrap().push_history(archived);
                idle_out(&state, true);
                return;
            }
        };
        let plan = match factory(&registry, &item.args) {
            Ok(p) => p,
            Err(e) => {
                // Same rule as the unknown-plan arm above.
                tracing::error!("queue: plan {} build failed: {e}", item.name);
                state.lock().unwrap().plans_failed += 1;
                let archived = item.clone().with_result(serde_json::json!({
                    "exit_status": "fail",
                    "reason": format!("plan build failed: {e}"),
                }));
                queue.lock().unwrap().push_history(archived);
                idle_out(&state, true);
                return;
            }
        };
        let exit_status = run_plan_item(&re, &queue, &state, &item, plan).await;
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
        // halts on error) and deactivate autostart.
        if exit_status != "success" {
            idle_out(&state, true);
            return;
        }
    }
}

/// Run one plan item through the worker machinery: manager state
/// (`ExecutingQueue` + running-item fields), the run itself, run bookkeeping
/// (`plans_run` / `plans_failed` / `re_runs`) and the history archive.
/// Shared by the queue worker and `queue_item_execute` so an immediately
/// executed item is accounted exactly like a queued one. Returns the exit
/// status.
async fn run_plan_item(
    re: &Arc<RunEngine>,
    queue: &Arc<StdMutex<PlanQueue>>,
    state: &Arc<StdMutex<EngineState>>,
    item: &crate::qs::queue::QueuedItem,
    plan: crate::core::plan::Plan,
) -> String {
    {
        let mut s = state.lock().unwrap();
        s.state = Some(EState::ExecutingQueue);
        s.current_plan_name = Some(item.name.clone());
    }
    // Forward the queue item's submitter metadata into the run as per-call
    // md (bluesky's `_metadata_per_call`, the highest-precedence merge
    // layer), so keys the submitter attached land in RunStart. A non-object
    // `meta` (or JSON null) contributes nothing. The plan supplies its own
    // plan_name/plan_args at OpenRun, so md carries only the submitter keys.
    let md: std::collections::HashMap<String, serde_json::Value> = item
        .meta
        .as_object()
        .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        .unwrap_or_default();
    let opts = RunOptions {
        md,
        subs: Vec::new(),
    };
    let run_result = re.run_async_with(plan, opts).await;
    let exit_status = match &run_result {
        Ok(r) => r.exit_status.clone(),
        Err(_) => "fail".to_string(),
    };
    let run_uid = run_result
        .as_ref()
        .ok()
        .and_then(|r| r.run_uids.last().cloned());
    // Bookkeeping after the run.
    {
        let mut s = state.lock().unwrap();
        s.plans_run += 1;
        s.current_run_uid = run_uid.clone();
        s.current_plan_name = None;
        if let Some(uid) = &run_uid {
            // Mark any prior entry for this uid as closed (shouldn't happen, but safe).
            for entry in s.re_runs.iter_mut() {
                if &entry.0 == uid {
                    entry.1 = false;
                }
            }
            s.re_runs.push((uid.clone(), false));
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
    exit_status
}

/// Run a single item outside the queue (`queue_item_execute`, ref:
/// manager.py:2744 `_queue_item_execute_handler`): the item is never
/// queued, loop mode does not re-enqueue it, and the queue is NOT started
/// afterwards — the manager returns to idle. Results are archived to the
/// plan history exactly like a queued item.
pub(crate) async fn execute_single_item(
    re: Arc<RunEngine>,
    queue: Arc<StdMutex<PlanQueue>>,
    state: Arc<StdMutex<EngineState>>,
    claim: SlotClaim,
    item: crate::qs::queue::QueuedItem,
    plan: crate::core::plan::Plan,
) {
    let _claim = claim;
    let exit_status = run_plan_item(&re, &queue, &state, &item, plan).await;
    // A failed/interrupted item deactivates autostart, same as a queued
    // plan (ref: manager.py:852 — the plan-state path is shared between
    // queued and immediate execution).
    idle_out(&state, exit_status != "success");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::msg::Msg;
    use crate::core::plan::plan_box;
    use crate::event_model::Document;
    use crate::qs::queue::QueuedItem;
    use crate::qs::registry::PlanFactory;
    use serde_json::json;
    use std::collections::HashMap;

    // PLAN-25 (part 2): a queued plan's submitter `meta` is forwarded into the
    // run as per-call md (bluesky's `_metadata_per_call`), so its keys land in
    // the RunStart document. Regression for the previously-dropped `item.meta`
    // (the queue loop ran the plan with no md).
    #[tokio::test]
    async fn queue_item_meta_reaches_runstart() {
        let re = Arc::new(RunEngine::new(vec![]));
        let seen: Arc<StdMutex<Vec<HashMap<String, serde_json::Value>>>> =
            Arc::new(StdMutex::new(Vec::new()));
        let seen_c = seen.clone();
        re.subscribe(Arc::new(move |d: &Document| {
            if let Document::Start(s) = d {
                seen_c.lock().unwrap().push(s.extra.clone());
            }
        }));

        let mut reg = Registry::new();
        let factory: PlanFactory = Arc::new(|_reg, _args| {
            Ok(plan_box(async_stream::stream! {
                yield Msg::OpenRun(Default::default());
                yield Msg::CloseRun { exit_status: "success".into(), reason: None };
            }))
        });
        reg.register_plan("noop", factory);
        let registry = Arc::new(reg);

        let queue = Arc::new(StdMutex::new(PlanQueue::new()));
        let mut item = QueuedItem::plan("noop", json!({ "args": [] }));
        // Keys chosen to avoid RunStart's typed fields (sample/group/owner/…),
        // so they surface in `extra`.
        item.meta = json!({ "purpose": "alignment", "operator": "sang" });
        queue.lock().unwrap().push_back(item);

        let state = Arc::new(StdMutex::new(EngineState::default()));
        let slot = Arc::new(QueueTaskSlot::new());
        let claim = slot.claim().expect("fresh slot");
        execute_queue_loop(re.clone(), registry, queue, state, claim).await;
        assert!(!slot.is_active(), "worker exit must release the slot");

        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1, "exactly one run opened");
        assert_eq!(seen[0].get("purpose"), Some(&json!("alignment")));
        assert_eq!(seen[0].get("operator"), Some(&json!("sang")));
    }
}
