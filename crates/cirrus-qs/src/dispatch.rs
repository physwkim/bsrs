//! JSON-RPC dispatch table. Mirrors bluesky-queueserver's
//! `_zmq_execute` (manager.py:3697) — every public method name is
//! registered here so clients see a uniform "method known / unknown"
//! distinction instead of hitting the catch-all `METHOD_NOT_FOUND`.
//!
//! Methods that don't map to cirrus's single-binary, no-IPython,
//! no-permissions model return `codes::NOT_IMPLEMENTED` with a clear
//! reason string.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;

use cirrus_engine::{DocumentSink, RunEngine};
use serde_json::{json, Value};
use tokio::sync::Mutex;
use tokio::task::AbortHandle;

use crate::methods::{codes, RpcRequest, RpcResponse};
use crate::queue::{PlanQueue, QueuedItem};
use crate::registry::Registry;
use crate::state::{EState, EngineState};

/// Top-level dispatch entry. Returns the JSON-RPC response shape.
#[allow(clippy::too_many_arguments)]
pub(crate) fn dispatch(
    rt: &tokio::runtime::Handle,
    req: &RpcRequest,
    registry: Arc<Registry>,
    queue: Arc<StdMutex<PlanQueue>>,
    state: Arc<StdMutex<EngineState>>,
    engine: Arc<Mutex<Option<Arc<RunEngine>>>>,
    document_sink: Option<Arc<dyn DocumentSink>>,
    queue_task: Arc<StdMutex<Option<AbortHandle>>>,
) -> RpcResponse {
    let id = req.id.clone();
    let m = req.method.as_str();

    // Lock check: any method that mutates queue / environment is gated
    // by lock state (mirrors bluesky's lock semantics).
    if !lock_check(m, &state, &req.params) {
        return RpcResponse::err(
            id,
            codes::QSERVER,
            "operation rejected: subsystem is locked (use `unlock` with the matching key)",
        );
    }

    match m {
        // -- info ---------------------------------------------------------
        "ping" => RpcResponse::ok(id, json!({"success": true, "msg": "pong"})),
        "status" => status_response(id, &state, &queue, &engine, rt),
        "config_get" => RpcResponse::ok(
            id,
            json!({
                "success": true,
                "msg": "",
                "config": {
                    "implementation": "cirrus-qs",
                    "runtime": "rust",
                    "version": env!("CARGO_PKG_VERSION"),
                    "wire_protocol": "bluesky-queueserver-compatible (subset)",
                },
            }),
        ),

        // -- plans / devices listing -------------------------------------
        "plans_allowed" => RpcResponse::ok(
            id,
            json!({
                "success": true,
                "msg": "",
                "plans_allowed": registry.plan_names(),
                "plans_allowed_uid": "static",
            }),
        ),
        "plans_existing" => RpcResponse::ok(
            id,
            json!({
                "success": true,
                "msg": "",
                "plans_existing": registry.plan_names(),
                "plans_existing_uid": "static",
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
        "devices_existing" => RpcResponse::ok(
            id,
            json!({
                "success": true,
                "msg": "",
                "devices_existing": registry.device_names(),
                "devices_existing_uid": "static",
            }),
        ),

        // -- environment --------------------------------------------------
        "environment_open" => env_open(id, document_sink, &state, &engine, rt),
        "environment_close" => env_close(id, &state, &engine, rt),
        "environment_destroy" => env_close(id, &state, &engine, rt), // forced close
        "environment_update" => RpcResponse::ok(id, json!({"success": true, "msg": ""})),

        // -- queue contents -----------------------------------------------
        "queue_get" => queue_get(id, &queue, &state),
        "queue_clear" => {
            queue.lock().unwrap().clear();
            RpcResponse::ok(id, json!({"success": true, "msg": ""}))
        }
        "queue_item_add" => queue_item_add(id, &registry, &queue, &req.params),
        "queue_item_add_batch" => queue_item_add_batch(id, &registry, &queue, &req.params),
        "queue_item_update" => queue_item_update(id, &queue, &req.params),
        "queue_item_get" => queue_item_get(id, &queue, &req.params),
        "queue_item_remove" => queue_item_remove(id, &queue, &req.params),
        "queue_item_remove_batch" => queue_item_remove_batch(id, &queue, &req.params),
        "queue_item_move" => queue_item_move(id, &queue, &req.params),
        "queue_item_move_batch" => queue_item_move_batch(id, &queue, &req.params),
        "queue_item_execute" => queue_item_execute(id, &registry, &engine, rt, &req.params),

        // -- queue execution ----------------------------------------------
        "queue_start" => queue_start(id, &registry, &queue, &state, &engine, rt, &queue_task),
        "queue_stop" => {
            state.lock().unwrap().queue_stop_pending = true;
            RpcResponse::ok(id, json!({"success": true, "msg": ""}))
        }
        "queue_stop_cancel" => {
            state.lock().unwrap().queue_stop_pending = false;
            RpcResponse::ok(id, json!({"success": true, "msg": ""}))
        }
        "queue_autostart" => {
            let enable = req
                .params
                .get("enable")
                .and_then(|v| v.as_bool())
                .unwrap_or_else(|| {
                    req.params
                        .get("option")
                        .and_then(|v| v.as_str())
                        .map(|s| s == "enable")
                        .unwrap_or(false)
                });
            state.lock().unwrap().queue_autostart_enabled = enable;
            RpcResponse::ok(id, json!({"success": true, "msg": ""}))
        }
        "queue_mode_set" => {
            let mode = match req.params.get("mode") {
                Some(Value::Object(m)) => m.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
                _ => return RpcResponse::err(id, codes::INVALID_PARAMS, "missing 'mode' object"),
            };
            state.lock().unwrap().queue_mode = mode;
            RpcResponse::ok(id, json!({"success": true, "msg": ""}))
        }

        // -- history ------------------------------------------------------
        "history_get" => {
            let q = queue.lock().unwrap();
            RpcResponse::ok(
                id,
                json!({
                    "success": true,
                    "msg": "",
                    "items": q.history_snapshot(),
                    "plan_history_uid": q.history_uid(),
                }),
            )
        }
        "history_clear" => {
            queue.lock().unwrap().clear_history();
            RpcResponse::ok(id, json!({"success": true, "msg": ""}))
        }

        // -- RunEngine control --------------------------------------------
        "re_pause" => re_pause(id, &engine, rt, &req.params),
        "re_resume" => re_with(id, &engine, rt, |re| re.resume()),
        "re_abort" => re_with(id, &engine, rt, |re| {
            re.abort("user abort");
            // ExecutingQueue → Aborting, but the worker loop will idle out
            // when run_async returns.
        }),
        "re_halt" => re_with(id, &engine, rt, |re| re.halt("user halt")),
        "re_stop" => re_with(id, &engine, rt, |re| re.stop()),
        "re_runs" => re_runs(id, &state),
        "re_metadata" => re_metadata(id, &engine, rt, &req.params),

        // -- locks --------------------------------------------------------
        "lock" => lock_apply(id, &state, &req.params),
        "lock_info" => RpcResponse::ok(
            id,
            json!({
                "success": true,
                "msg": "",
                "lock_info": serde_json::to_value(&state.lock().unwrap().lock).unwrap(),
                "lock_info_uid": state.lock().unwrap().lock.uid.clone(),
            }),
        ),
        "unlock" => lock_release(id, &state, &req.params),

        // -- bluesky-queueserver wire compat: many clients always
        //    call these even when the server side does the work
        //    synchronously. Return a "completed / no-op" shape so
        //    naive clients don't error.
        "task_status" => RpcResponse::ok(
            id,
            json!({
                "success": true,
                "msg": "",
                "status": "completed",
                "task_uid": req.params.get("task_uid").cloned().unwrap_or(Value::Null),
            }),
        ),
        "task_result" => RpcResponse::ok(
            id,
            json!({
                "success": true,
                "msg": "",
                "status": "completed",
                "result": {
                    "return_value": null,
                    "traceback": "",
                    "msg": "",
                    "success": true,
                    "task_uid": req.params.get("task_uid").cloned().unwrap_or(Value::Null),
                },
            }),
        ),
        "manager_test" => RpcResponse::ok(
            id,
            json!({"success": true, "msg": ""}),
        ),
        "permissions_get" => RpcResponse::ok(
            id,
            json!({
                "success": true,
                "msg": "",
                // cirrus-qs has no RBAC layer yet — return the
                // permissive default that allows everything.
                "user_group_permissions": {
                    "user_groups": {
                        "root": {"allowed_plans": [".*"], "allowed_devices": [".*"]},
                        "primary": {"allowed_plans": [".*"], "allowed_devices": [".*"]},
                    },
                },
                "user_group_permissions_uid": "static",
            }),
        ),

        // -- not-implemented stubs (registered so clients see the method
        //    name but get a defined error). --------------------------------
        "permissions_reload"
        | "permissions_set"
        | "script_upload"
        | "function_execute"
        | "kernel_interrupt"
        | "manager_stop"
        | "manager_kill" => RpcResponse::err(
            id,
            codes::NOT_IMPLEMENTED,
            format!(
                "method '{m}' is registered but not implemented in cirrus-qs (bluesky-queueserver-only feature)"
            ),
        ),

        // Unknown.
        other => RpcResponse::err(id, codes::METHOD_NOT_FOUND, format!("unknown method: {other}")),
    }
}

// -- helpers ----------------------------------------------------------------

fn lock_check(method: &str, state: &Arc<StdMutex<EngineState>>, params: &Value) -> bool {
    // Methods that don't mutate state are always allowed.
    let always_allowed = matches!(
        method,
        "ping"
            | "status"
            | "config_get"
            | "queue_get"
            | "history_get"
            | "lock_info"
            | "plans_allowed"
            | "plans_existing"
            | "devices_allowed"
            | "devices_existing"
            | "re_runs"
            | "re_metadata"
            | "task_status"
            | "task_result"
            | "manager_test"
    );
    if always_allowed {
        return true;
    }
    let st = state.lock().unwrap();
    if !st.lock.is_locked() {
        return true;
    }
    // If lock is held, the request must include the matching lock_key.
    let key = params.get("lock_key").and_then(|v| v.as_str());
    let supplied_hash = key.map(hash_key);
    // The unlock method MUST hash-match.
    if method == "unlock" {
        return supplied_hash == st.lock.key_hash;
    }
    // Subsystem-aware: only block ops that touch the locked subsystem.
    let env_method = matches!(
        method,
        "environment_open" | "environment_close" | "environment_destroy" | "environment_update"
    );
    let queue_method =
        method.starts_with("queue_") || method.starts_with("history_") || method.starts_with("re_");
    let blocked = (st.lock.environment && env_method) || (st.lock.queue && queue_method);
    if !blocked {
        return true;
    }
    supplied_hash == st.lock.key_hash
}

fn hash_key(k: &str) -> u64 {
    let mut h = DefaultHasher::new();
    k.hash(&mut h);
    h.finish()
}

fn status_response(
    id: Option<Value>,
    state: &Arc<StdMutex<EngineState>>,
    queue: &Arc<StdMutex<PlanQueue>>,
    engine: &Arc<Mutex<Option<Arc<RunEngine>>>>,
    rt: &tokio::runtime::Handle,
) -> RpcResponse {
    let q = queue.lock().unwrap();
    let st = state.lock().unwrap().clone();
    let env_exists = rt.block_on(engine.lock()).is_some();
    let re_state = if env_exists {
        st.state.map(|s| s.as_str()).unwrap_or("idle").to_string()
    } else {
        "null".to_string()
    };
    RpcResponse::ok(
        id,
        json!({
            "success": true,
            "msg": "",
            "manager_state": st.state.map(|s| s.as_str()).unwrap_or("environment_closed"),
            "manager_version": env!("CARGO_PKG_VERSION"),
            "msg_recv": "",
            "items_in_queue": q.len(),
            "items_in_history": q.history_size(),
            "running_item_uid": st.current_run_uid,
            "running_item_name": st.current_plan_name,
            "plans_run": st.plans_run,
            "plans_failed": st.plans_failed,
            "re_state": re_state,
            "worker_environment_exists": env_exists,
            "worker_environment_state": if env_exists { "idle" } else { "closed" },
            "queue_stop_pending": st.queue_stop_pending,
            "queue_autostart_enabled": st.queue_autostart_enabled,
            "plan_queue_mode": st.queue_mode,
            "plan_queue_uid": q.queue_uid(),
            "plan_history_uid": q.history_uid(),
            "lock_info_uid": st.lock.uid,
            "lock": {
                "environment": st.lock.environment,
                "queue": st.lock.queue,
            },
            // Many bluesky clients also read these — present them as
            // static so allowed/existing UID changes never trigger
            // cache invalidation noise.
            "devices_allowed_uid": "static",
            "plans_allowed_uid": "static",
            "devices_existing_uid": "static",
            "plans_existing_uid": "static",
            "task_results_uid": "static",
            "run_list_uid": "static",
        }),
    )
}

fn env_open(
    id: Option<Value>,
    document_sink: Option<Arc<dyn DocumentSink>>,
    state: &Arc<StdMutex<EngineState>>,
    engine: &Arc<Mutex<Option<Arc<RunEngine>>>>,
    rt: &tokio::runtime::Handle,
) -> RpcResponse {
    let mut e = rt.block_on(engine.lock());
    if e.is_some() {
        return RpcResponse::err(id, codes::QSERVER, "environment already open");
    }
    let sinks: Vec<Arc<dyn DocumentSink>> = document_sink.iter().cloned().collect();
    *e = Some(Arc::new(RunEngine::new(sinks)));
    state.lock().unwrap().state = Some(EState::Idle);
    RpcResponse::ok(id, json!({"success": true, "msg": ""}))
}

fn env_close(
    id: Option<Value>,
    state: &Arc<StdMutex<EngineState>>,
    engine: &Arc<Mutex<Option<Arc<RunEngine>>>>,
    rt: &tokio::runtime::Handle,
) -> RpcResponse {
    let mut e = rt.block_on(engine.lock());
    if e.is_none() {
        return RpcResponse::err(id, codes::QSERVER, "no environment");
    }
    *e = None;
    state.lock().unwrap().state = Some(EState::EnvironmentClosed);
    RpcResponse::ok(id, json!({"success": true, "msg": ""}))
}

fn queue_get(
    id: Option<Value>,
    queue: &Arc<StdMutex<PlanQueue>>,
    state: &Arc<StdMutex<EngineState>>,
) -> RpcResponse {
    let q = queue.lock().unwrap();
    let st = state.lock().unwrap();
    let running = if let Some(name) = &st.current_plan_name {
        json!({
            "name": name,
            "item_uid": st.current_run_uid.clone().unwrap_or_default(),
        })
    } else {
        Value::Null
    };
    RpcResponse::ok(
        id,
        json!({
            "success": true,
            "msg": "",
            "items": q.snapshot(),
            "running_item": running,
            "plan_queue_uid": q.queue_uid(),
        }),
    )
}

fn queue_item_add(
    id: Option<Value>,
    registry: &Arc<Registry>,
    queue: &Arc<StdMutex<PlanQueue>>,
    params: &Value,
) -> RpcResponse {
    let item = match params.get("item") {
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
    let mut q = queue.lock().unwrap();
    q.push_back(queued);
    RpcResponse::ok(
        id,
        json!({
            "success": true,
            "msg": "",
            "qsize": q.len(),
            "item_uid": item_uid,
            "plan_queue_uid": q.queue_uid(),
        }),
    )
}

fn queue_item_add_batch(
    id: Option<Value>,
    registry: &Arc<Registry>,
    queue: &Arc<StdMutex<PlanQueue>>,
    params: &Value,
) -> RpcResponse {
    let items = match params.get("items").and_then(|v| v.as_array()) {
        Some(a) => a.clone(),
        None => return RpcResponse::err(id, codes::INVALID_PARAMS, "missing 'items' array"),
    };
    let mut added = Vec::new();
    let mut errors = Vec::new();
    for item in items {
        let name = item
            .get("name")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        match name {
            Some(n) if registry.plan(&n).is_some() => {
                let qi = QueuedItem::plan(n, item);
                let uid = qi.item_uid.clone();
                queue.lock().unwrap().push_back(qi);
                added.push(uid);
            }
            Some(n) => errors.push(format!("unknown plan: {n}")),
            None => errors.push("item.name required".to_string()),
        }
    }
    let q = queue.lock().unwrap();
    RpcResponse::ok(
        id,
        json!({
            "success": errors.is_empty(),
            "msg": if errors.is_empty() { "".into() } else { errors.join("; ") },
            "qsize": q.len(),
            "items_added": added,
            "plan_queue_uid": q.queue_uid(),
        }),
    )
}

fn queue_item_update(
    id: Option<Value>,
    queue: &Arc<StdMutex<PlanQueue>>,
    params: &Value,
) -> RpcResponse {
    let item = match params.get("item") {
        Some(i) => i.clone(),
        None => return RpcResponse::err(id, codes::INVALID_PARAMS, "missing 'item'"),
    };
    let uid = match item.get("item_uid").and_then(|v| v.as_str()) {
        Some(u) => u.to_string(),
        None => return RpcResponse::err(id, codes::INVALID_PARAMS, "item.item_uid required"),
    };
    let name = item
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let mut q = queue.lock().unwrap();
    let new_item = QueuedItem::plan(name, item);
    match q.update(&uid, new_item) {
        Some(updated) => RpcResponse::ok(
            id,
            json!({
                "success": true,
                "msg": "",
                "item": serde_json::to_value(&updated).unwrap(),
                "plan_queue_uid": q.queue_uid(),
            }),
        ),
        None => RpcResponse::err(id, codes::QSERVER, format!("uid not found: {uid}")),
    }
}

fn queue_item_get(
    id: Option<Value>,
    queue: &Arc<StdMutex<PlanQueue>>,
    params: &Value,
) -> RpcResponse {
    let q = queue.lock().unwrap();
    if let Some(uid) = params.get("uid").and_then(|v| v.as_str()) {
        return match q.get_by_uid(uid) {
            Some(it) => RpcResponse::ok(
                id,
                json!({"success": true, "msg": "", "item": serde_json::to_value(it).unwrap()}),
            ),
            None => RpcResponse::err(id, codes::QSERVER, format!("uid not found: {uid}")),
        };
    }
    if let Some(pos) = params.get("pos") {
        let snap = q.snapshot();
        let idx_opt = match pos {
            Value::String(s) if s == "front" => snap.first().cloned(),
            Value::String(s) if s == "back" => snap.last().cloned(),
            Value::Number(n) => n.as_u64().and_then(|i| snap.get(i as usize).cloned()),
            _ => None,
        };
        return match idx_opt {
            Some(it) => RpcResponse::ok(
                id,
                json!({"success": true, "msg": "", "item": serde_json::to_value(it).unwrap()}),
            ),
            None => RpcResponse::err(id, codes::QSERVER, format!("pos not found: {pos}")),
        };
    }
    RpcResponse::err(id, codes::INVALID_PARAMS, "specify 'uid' or 'pos'")
}

fn queue_item_remove(
    id: Option<Value>,
    queue: &Arc<StdMutex<PlanQueue>>,
    params: &Value,
) -> RpcResponse {
    let uid = match params.get("uid").and_then(|v| v.as_str()) {
        Some(u) => u.to_string(),
        None => return RpcResponse::err(id, codes::INVALID_PARAMS, "uid required"),
    };
    let mut q = queue.lock().unwrap();
    match q.remove_by_uid(&uid) {
        Some(it) => RpcResponse::ok(
            id,
            json!({
                "success": true,
                "msg": "",
                "item": serde_json::to_value(&it).unwrap(),
                "qsize": q.len(),
                "plan_queue_uid": q.queue_uid(),
            }),
        ),
        None => RpcResponse::err(id, codes::QSERVER, format!("uid not found: {uid}")),
    }
}

fn queue_item_remove_batch(
    id: Option<Value>,
    queue: &Arc<StdMutex<PlanQueue>>,
    params: &Value,
) -> RpcResponse {
    let uids = match params.get("uids").and_then(|v| v.as_array()) {
        Some(a) => a
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect::<Vec<_>>(),
        None => return RpcResponse::err(id, codes::INVALID_PARAMS, "missing 'uids' array"),
    };
    let mut removed = Vec::new();
    {
        let mut q = queue.lock().unwrap();
        for uid in &uids {
            if let Some(it) = q.remove_by_uid(uid) {
                removed.push(it);
            }
        }
    }
    let q = queue.lock().unwrap();
    RpcResponse::ok(
        id,
        json!({
            "success": true,
            "msg": "",
            "items_removed": removed,
            "qsize": q.len(),
            "plan_queue_uid": q.queue_uid(),
        }),
    )
}

fn queue_item_move(
    id: Option<Value>,
    queue: &Arc<StdMutex<PlanQueue>>,
    params: &Value,
) -> RpcResponse {
    let uid = match params.get("uid").and_then(|v| v.as_str()) {
        Some(u) => u.to_string(),
        None => return RpcResponse::err(id, codes::INVALID_PARAMS, "uid required"),
    };
    let dest = resolve_pos(params.get("pos_dest"), queue);
    let mut q = queue.lock().unwrap();
    match q.move_to(&uid, dest) {
        Some(it) => RpcResponse::ok(
            id,
            json!({
                "success": true,
                "msg": "",
                "item": serde_json::to_value(&it).unwrap(),
                "plan_queue_uid": q.queue_uid(),
            }),
        ),
        None => RpcResponse::err(id, codes::QSERVER, format!("uid not found: {uid}")),
    }
}

fn queue_item_move_batch(
    id: Option<Value>,
    queue: &Arc<StdMutex<PlanQueue>>,
    params: &Value,
) -> RpcResponse {
    let uids = match params.get("uids").and_then(|v| v.as_array()) {
        Some(a) => a
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect::<Vec<_>>(),
        None => return RpcResponse::err(id, codes::INVALID_PARAMS, "missing 'uids' array"),
    };
    let dest = resolve_pos(params.get("pos_dest"), queue);
    let mut moved = Vec::new();
    {
        let mut q = queue.lock().unwrap();
        for (i, uid) in uids.iter().enumerate() {
            if let Some(it) = q.move_to(uid, dest + i) {
                moved.push(it);
            }
        }
    }
    let q = queue.lock().unwrap();
    RpcResponse::ok(
        id,
        json!({
            "success": true,
            "msg": "",
            "items_moved": moved,
            "plan_queue_uid": q.queue_uid(),
        }),
    )
}

fn resolve_pos(p: Option<&Value>, queue: &Arc<StdMutex<PlanQueue>>) -> usize {
    match p {
        Some(Value::String(s)) if s == "front" => 0,
        Some(Value::String(s)) if s == "back" => queue.lock().unwrap().len(),
        Some(Value::Number(n)) => n.as_u64().unwrap_or(0) as usize,
        _ => queue.lock().unwrap().len(),
    }
}

fn queue_item_execute(
    id: Option<Value>,
    registry: &Arc<Registry>,
    engine: &Arc<Mutex<Option<Arc<RunEngine>>>>,
    rt: &tokio::runtime::Handle,
    params: &Value,
) -> RpcResponse {
    let item = match params.get("item") {
        Some(i) => i.clone(),
        None => return RpcResponse::err(id, codes::INVALID_PARAMS, "missing 'item'"),
    };
    let name = match item.get("name").and_then(|v| v.as_str()) {
        Some(n) => n.to_string(),
        None => return RpcResponse::err(id, codes::INVALID_PARAMS, "item.name required"),
    };
    let factory = match registry.plan(&name) {
        Some(f) => f.clone(),
        None => return RpcResponse::err(id, codes::QSERVER, format!("unknown plan: {name}")),
    };
    let plan = match factory(registry, &item) {
        Ok(p) => p,
        Err(e) => return RpcResponse::err(id, codes::QSERVER, format!("plan build failed: {e}")),
    };
    let e_guard = rt.block_on(engine.lock());
    let re = match e_guard.as_ref() {
        Some(r) => r.clone(),
        None => return RpcResponse::err(id, codes::QSERVER, "environment not open"),
    };
    drop(e_guard);
    let result = rt.block_on(re.run_async(plan));
    match result {
        Ok(r) => RpcResponse::ok(
            id,
            json!({
                "success": r.exit_status == "success",
                "msg": "",
                "exit_status": r.exit_status,
                "run_uid": r.run_uid,
            }),
        ),
        Err(e) => RpcResponse::err(id, codes::QSERVER, format!("run failed: {e}")),
    }
}

#[allow(clippy::too_many_arguments)]
fn queue_start(
    id: Option<Value>,
    registry: &Arc<Registry>,
    queue: &Arc<StdMutex<PlanQueue>>,
    state: &Arc<StdMutex<EngineState>>,
    engine: &Arc<Mutex<Option<Arc<RunEngine>>>>,
    rt: &tokio::runtime::Handle,
    queue_task: &Arc<StdMutex<Option<AbortHandle>>>,
) -> RpcResponse {
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
            format!("cannot start in state {cur_state:?}"),
        );
    }
    let registry = registry.clone();
    let queue = queue.clone();
    let state = state.clone();
    let task_slot = queue_task.clone();
    let join = tokio::spawn(crate::server::execute_queue_loop(
        re,
        registry,
        queue,
        state,
        task_slot.clone(),
    ));
    *task_slot.lock().unwrap() = Some(join.abort_handle());
    RpcResponse::ok(id, json!({"success": true, "msg": ""}))
}

fn re_pause(
    id: Option<Value>,
    engine: &Arc<Mutex<Option<Arc<RunEngine>>>>,
    rt: &tokio::runtime::Handle,
    params: &Value,
) -> RpcResponse {
    let e_guard = rt.block_on(engine.lock());
    if let Some(re) = e_guard.as_ref() {
        let defer = params
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

fn re_with(
    id: Option<Value>,
    engine: &Arc<Mutex<Option<Arc<RunEngine>>>>,
    rt: &tokio::runtime::Handle,
    f: impl FnOnce(&Arc<RunEngine>),
) -> RpcResponse {
    let e_guard = rt.block_on(engine.lock());
    if let Some(re) = e_guard.as_ref() {
        f(re);
        RpcResponse::ok(id, json!({"success": true, "msg": ""}))
    } else {
        RpcResponse::err(id, codes::QSERVER, "no environment")
    }
}

fn re_runs(id: Option<Value>, state: &Arc<StdMutex<EngineState>>) -> RpcResponse {
    let st = state.lock().unwrap();
    let runs: Vec<Value> = st
        .re_runs
        .iter()
        .map(|uid| json!({"uid": uid, "is_open": false}))
        .collect();
    RpcResponse::ok(
        id,
        json!({"success": true, "msg": "", "run_list": runs, "run_list_uid": "static"}),
    )
}

fn re_metadata(
    id: Option<Value>,
    engine: &Arc<Mutex<Option<Arc<RunEngine>>>>,
    rt: &tokio::runtime::Handle,
    params: &Value,
) -> RpcResponse {
    let e_guard = rt.block_on(engine.lock());
    let re = match e_guard.as_ref() {
        Some(r) => r.clone(),
        None => return RpcResponse::err(id, codes::QSERVER, "no environment"),
    };
    drop(e_guard);
    if let Some(md_in) = params.get("metadata").and_then(|v| v.as_object()) {
        let merged: std::collections::HashMap<String, Value> =
            md_in.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        re.md_replace(merged);
    }
    RpcResponse::ok(
        id,
        json!({
            "success": true,
            "msg": "",
            "metadata": re.md(),
        }),
    )
}

fn lock_apply(
    id: Option<Value>,
    state: &Arc<StdMutex<EngineState>>,
    params: &Value,
) -> RpcResponse {
    let key = match params.get("lock_key").and_then(|v| v.as_str()) {
        Some(k) => k,
        None => return RpcResponse::err(id, codes::INVALID_PARAMS, "missing 'lock_key'"),
    };
    let env = params
        .get("environment")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let queue = params
        .get("queue")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if !env && !queue {
        return RpcResponse::err(
            id,
            codes::INVALID_PARAMS,
            "must lock at least one of `environment` / `queue`",
        );
    }
    let user = params
        .get("user")
        .and_then(|v| v.as_str())
        .map(String::from);
    let note = params
        .get("note")
        .and_then(|v| v.as_str())
        .map(String::from);
    {
        let mut st = state.lock().unwrap();
        if st.lock.is_locked() && st.lock.key_hash != Some(hash_key(key)) {
            return RpcResponse::err(id, codes::QSERVER, "subsystem already locked");
        }
        st.lock.lock(env, queue, user, note, hash_key(key));
    }
    let st = state.lock().unwrap();
    RpcResponse::ok(
        id,
        json!({
            "success": true,
            "msg": "",
            "lock_info": serde_json::to_value(&st.lock).unwrap(),
            "lock_info_uid": st.lock.uid.clone(),
        }),
    )
}

fn lock_release(
    id: Option<Value>,
    state: &Arc<StdMutex<EngineState>>,
    params: &Value,
) -> RpcResponse {
    let key = match params.get("lock_key").and_then(|v| v.as_str()) {
        Some(k) => k,
        None => return RpcResponse::err(id, codes::INVALID_PARAMS, "missing 'lock_key'"),
    };
    let mut st = state.lock().unwrap();
    if st.lock.is_locked() && st.lock.key_hash != Some(hash_key(key)) {
        return RpcResponse::err(id, codes::QSERVER, "lock_key does not match");
    }
    st.lock.clear();
    RpcResponse::ok(
        id,
        json!({
            "success": true,
            "msg": "",
            "lock_info": serde_json::to_value(&st.lock).unwrap(),
            "lock_info_uid": st.lock.uid.clone(),
        }),
    )
}
