//! `Server` — owns the engine, queue, registry, and the REP socket.

use std::sync::Arc;
use std::sync::Mutex as StdMutex;

use cirrus_callbacks::ZmqDocumentSink;
use cirrus_core::error::{CirrusError, Result};
use cirrus_core::msg::RunMetadata;
use cirrus_engine::{DocumentSink, RunEngine};
use serde_json::{json, Value};
use tokio::sync::Mutex;

use crate::methods::{codes, RpcRequest, RpcResponse};
use crate::queue::{PlanQueue, QueuedItem};
use crate::registry::Registry;
use crate::state::{EState, EngineState};
use crate::transport::ReqRepSocket;

/// Server builder. Construct and `build()` to commit (rule **K9** — no
/// background tasks until `run_async` / `run_blocking`).
pub struct ServerBuilder {
    control_address: String,
    document_address: Option<String>,
    registry: Option<Registry>,
}

impl Default for ServerBuilder {
    fn default() -> Self {
        Self {
            control_address: "tcp://*:60615".into(),
            document_address: Some("tcp://*:60625".into()),
            registry: None,
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
        Ok(Server {
            socket,
            document_sink,
            registry: Arc::new(registry),
            queue: Arc::new(StdMutex::new(PlanQueue::new())),
            state: Arc::new(StdMutex::new(EngineState::initial())),
            engine: Arc::new(Mutex::new(None)),
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

        let join = tokio::task::spawn_blocking(move || {
            let rt = tokio::runtime::Handle::current();
            rep_loop(rt, socket, registry, queue, state, engine, document_sink)
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
    /// exit at its next iteration (within ~200 ms).
    pub fn shutdown_handle(&self) -> ServerShutdown {
        ServerShutdown {
            socket: self.socket.clone(),
        }
    }
}

/// Lightweight handle returned by [`Server::shutdown_handle`].
#[derive(Clone)]
pub struct ServerShutdown {
    socket: ReqRepSocket,
}

impl ServerShutdown {
    /// Signal the server to exit.
    pub fn shutdown(&self) {
        self.socket.shutdown();
    }
}

fn rep_loop(
    rt: tokio::runtime::Handle,
    socket: ReqRepSocket,
    registry: Arc<Registry>,
    queue: Arc<StdMutex<PlanQueue>>,
    state: Arc<StdMutex<EngineState>>,
    engine: Arc<Mutex<Option<Arc<RunEngine>>>>,
    document_sink: Option<Arc<dyn DocumentSink>>,
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
        );
        if let Err(e) = socket.send(&resp) {
            tracing::warn!(target: "cirrus-qs", "rep_loop: send error: {e}");
        }
    }
    Ok(())
}

fn dispatch(
    rt: &tokio::runtime::Handle,
    req: &RpcRequest,
    registry: Arc<Registry>,
    queue: Arc<StdMutex<PlanQueue>>,
    state: Arc<StdMutex<EngineState>>,
    engine: Arc<Mutex<Option<Arc<RunEngine>>>>,
    document_sink: Option<Arc<dyn DocumentSink>>,
) -> RpcResponse {
    let id = req.id.clone();
    match req.method.as_str() {
        "ping" => RpcResponse::ok(id, json!({"success": true, "msg": "pong"})),

        "status" => {
            let mut st = state.lock().unwrap().clone();
            st.queue_len = queue.lock().unwrap().len();
            RpcResponse::ok(
                id,
                json!({
                    "success": true,
                    "msg": "",
                    "manager_state": st.state.map(|s| s.as_str()).unwrap_or("environment_closed"),
                    "items_in_queue": st.queue_len,
                    "running_item_uid": st.current_run_uid,
                    "running_item_name": st.current_plan_name,
                    "plans_run": st.plans_run,
                    "plans_failed": st.plans_failed,
                }),
            )
        }

        "environment_open" => {
            let mut e = rt.block_on(engine.lock());
            if e.is_some() {
                return RpcResponse::err(id, codes::QSERVER, "environment already open");
            }
            let sinks: Vec<Arc<dyn DocumentSink>> = document_sink.iter().cloned().collect();
            *e = Some(Arc::new(RunEngine::new(sinks)));
            state.lock().unwrap().state = Some(EState::Idle);
            RpcResponse::ok(id, json!({"success": true, "msg": ""}))
        }

        "environment_close" => {
            let mut e = rt.block_on(engine.lock());
            if e.is_none() {
                return RpcResponse::err(id, codes::QSERVER, "no environment");
            }
            *e = None;
            state.lock().unwrap().state = Some(EState::EnvironmentClosed);
            RpcResponse::ok(id, json!({"success": true, "msg": ""}))
        }

        "queue_item_add" => {
            let item = match req.params.get("item") {
                Some(it) => it.clone(),
                None => return RpcResponse::err(id, codes::INVALID_PARAMS, "missing 'item'"),
            };
            let name = match item.get("name").and_then(|v| v.as_str()) {
                Some(n) => n.to_string(),
                None => return RpcResponse::err(id, codes::INVALID_PARAMS, "item.name required"),
            };
            if registry.plan(&name).is_none() {
                return RpcResponse::err(id, codes::QSERVER, format!("unknown plan: {name}"));
            }
            let queued = QueuedItem::plan(name, item);
            let item_uid = queued.item_uid.clone();
            queue.lock().unwrap().push_back(queued);
            RpcResponse::ok(
                id,
                json!({"success": true, "msg": "", "qsize": queue.lock().unwrap().len(), "item_uid": item_uid}),
            )
        }

        "queue_item_remove" => {
            let uid = match req.params.get("uid").and_then(|v| v.as_str()) {
                Some(u) => u.to_string(),
                None => return RpcResponse::err(id, codes::INVALID_PARAMS, "uid required"),
            };
            let removed = queue.lock().unwrap().remove_by_uid(&uid);
            match removed {
                Some(i) => RpcResponse::ok(
                    id,
                    json!({"success": true, "msg": "", "item": serde_json::to_value(&i).unwrap()}),
                ),
                None => RpcResponse::err(id, codes::QSERVER, format!("uid not found: {uid}")),
            }
        }

        "queue_get" => {
            let snap = queue.lock().unwrap().snapshot();
            RpcResponse::ok(
                id,
                json!({"success": true, "msg": "", "items": snap, "running_item": Value::Null}),
            )
        }

        "queue_start" => {
            let e_guard = rt.block_on(engine.lock());
            let re = match e_guard.as_ref() {
                Some(r) => r.clone(),
                None => return RpcResponse::err(id, codes::QSERVER, "environment not open"),
            };
            drop(e_guard);
            let cur_state = state.lock().unwrap().state;
            if cur_state != Some(EState::Idle) {
                return RpcResponse::err(
                    id,
                    codes::QSERVER,
                    format!("cannot start in state {:?}", cur_state),
                );
            }
            let registry = registry.clone();
            let queue = queue.clone();
            let state = state.clone();
            tokio::spawn(execute_queue_loop(re, registry, queue, state));
            RpcResponse::ok(id, json!({"success": true, "msg": ""}))
        }

        "re_pause" => {
            let e_guard = rt.block_on(engine.lock());
            if let Some(re) = e_guard.as_ref() {
                let defer = req
                    .params
                    .get("option")
                    .and_then(|v| v.as_str())
                    .map(|s| s == "deferred")
                    .unwrap_or(false);
                re.pause(defer);
                RpcResponse::ok(id, json!({"success": true, "msg": ""}))
            } else {
                RpcResponse::err(id, codes::QSERVER, "no environment")
            }
        }

        "re_resume" => {
            let e_guard = rt.block_on(engine.lock());
            if let Some(re) = e_guard.as_ref() {
                re.resume();
                RpcResponse::ok(id, json!({"success": true, "msg": ""}))
            } else {
                RpcResponse::err(id, codes::QSERVER, "no environment")
            }
        }

        "re_abort" => {
            let e_guard = rt.block_on(engine.lock());
            if let Some(re) = e_guard.as_ref() {
                re.abort("user abort");
                state.lock().unwrap().state = Some(EState::Aborting);
                RpcResponse::ok(id, json!({"success": true, "msg": ""}))
            } else {
                RpcResponse::err(id, codes::QSERVER, "no environment")
            }
        }

        "re_halt" => {
            let e_guard = rt.block_on(engine.lock());
            if let Some(re) = e_guard.as_ref() {
                re.halt("user halt");
                state.lock().unwrap().state = Some(EState::Aborting);
                RpcResponse::ok(id, json!({"success": true, "msg": ""}))
            } else {
                RpcResponse::err(id, codes::QSERVER, "no environment")
            }
        }

        "plans_allowed" => RpcResponse::ok(
            id,
            json!({
                "success": true,
                "msg": "",
                "plans_allowed": registry.plan_names(),
                "plans_allowed_uid": "static",
            }),
        ),

        "devices_allowed" => RpcResponse::ok(
            id,
            json!({
                "success": true,
                "msg": "",
                "devices_allowed": registry.device_names(),
                "devices_allowed_uid": "static",
            }),
        ),

        m => RpcResponse::err(id, codes::METHOD_NOT_FOUND, format!("unknown method: {m}")),
    }
}

async fn execute_queue_loop(
    re: Arc<RunEngine>,
    registry: Arc<Registry>,
    queue: Arc<StdMutex<PlanQueue>>,
    state: Arc<StdMutex<EngineState>>,
) {
    loop {
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
                state.lock().unwrap().plans_failed += 1;
                continue;
            }
        };
        let plan = match factory(&registry, &item.args) {
            Ok(p) => p,
            Err(e) => {
                tracing::error!("queue: plan {} build failed: {e}", item.name);
                state.lock().unwrap().plans_failed += 1;
                continue;
            }
        };
        // Wrap the plan with an OpenRun envelope using the queued item meta.
        let _ = item.meta;
        let _meta = RunMetadata {
            scan_id: None,
            plan_name: Some(item.name.clone()),
            extra: Default::default(),
        };
        match re.run_async(plan).await {
            Ok(result) => {
                let mut s = state.lock().unwrap();
                s.plans_run += 1;
                s.current_run_uid = result.run_uid;
                s.current_plan_name = None;
                if result.exit_status == "abort" || result.exit_status == "fail" {
                    s.plans_failed += 1;
                    s.state = Some(EState::Idle);
                    return;
                }
            }
            Err(e) => {
                tracing::error!("plan {} failed: {e}", item.name);
                let mut s = state.lock().unwrap();
                s.plans_failed += 1;
                s.state = Some(EState::Idle);
                return;
            }
        }
    }
}
