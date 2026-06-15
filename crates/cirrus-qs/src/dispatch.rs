//! bluesky-queueserver protocol dispatch table. Mirrors
//! `_zmq_execute` (manager.py:3697) — every public method name is
//! registered here so clients see a uniform "method known / unknown"
//! distinction instead of hitting the catch-all unknown-method response.
//!
//! Methods that don't map to cirrus's single-binary, no-IPython,
//! no-permissions model return a flat error with a clear reason string.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;

use cirrus_engine::{CheckpointHook, DocumentSink, RunEngine};
use serde_json::{json, Value};
use tokio::sync::Mutex;
use tokio::task::AbortHandle;

use crate::lua_eval::LuaEvaluator;
use crate::methods::{err, QsRequest};
use crate::permissions::Permissions;
use crate::queue::{PlanQueue, QueuedItem};
use crate::registry::Registry;
use crate::state::{EState, EngineState};
use crate::tasks::TaskTracker;

/// Top-level dispatch entry. Returns a flat bluesky-queueserver response dict.
#[allow(clippy::too_many_arguments)]
pub(crate) fn dispatch(
    rt: &tokio::runtime::Handle,
    req: &QsRequest,
    registry: Arc<Registry>,
    queue: Arc<StdMutex<PlanQueue>>,
    state: Arc<StdMutex<EngineState>>,
    engine: Arc<Mutex<Option<Arc<RunEngine>>>>,
    document_sink: Option<Arc<dyn DocumentSink>>,
    queue_task: Arc<StdMutex<Option<AbortHandle>>>,
    permissions: Arc<Permissions>,
    lua_evaluator: Option<Arc<dyn LuaEvaluator>>,
    task_tracker: Arc<TaskTracker>,
    checkpoint_hook: Option<CheckpointHook>,
) -> Value {
    let m = req.method.as_str();

    #[cfg(feature = "metrics")]
    crate::metrics::rpc_call(m);

    // RBAC gate: classify the method and check the caller's group.
    let group = permissions.resolve_group(&req.params);
    if let Err(reason) = permissions.check(m, &req.params, &group) {
        #[cfg(feature = "metrics")]
        crate::metrics::rpc_error(m);
        return err(reason);
    }

    // Lock check: any method that mutates queue / environment is gated
    // by lock state (mirrors bluesky's lock semantics).
    if !lock_check(m, &state, &req.params) {
        return err("operation rejected: subsystem is locked (use `unlock` with the matching key)");
    }

    match m {
        // -- info ---------------------------------------------------------
        // ping returns the full status dict (ref: manager.py:1888 calls _status_handler).
        "ping" => status_response(&state, &queue, &engine, rt),
        "status" => status_response(&state, &queue, &engine, rt),
        "config_get" => json!({
            "success": true,
            "msg": "",
            "config": {
                "implementation": "cirrus-qs",
                "runtime": "rust",
                "version": env!("CARGO_PKG_VERSION"),
                "wire_protocol": "bluesky-queueserver-compatible (subset)",
                "ip_connect_info": {},
            },
        }),

        // -- plans / devices listing -------------------------------------
        "plans_allowed" => json!({
            "success": true,
            "msg": "",
            "plans_allowed": registry.plan_names(),
            "plans_allowed_uid": "static",
        }),
        "plans_existing" => json!({
            "success": true,
            "msg": "",
            "plans_existing": registry.plan_names(),
            "plans_existing_uid": "static",
        }),
        "devices_allowed" => json!({
            "success": true,
            "msg": "",
            "devices_allowed": registry.device_names(),
            "devices_allowed_uid": "static",
        }),
        "devices_existing" => json!({
            "success": true,
            "msg": "",
            "devices_existing": registry.device_names(),
            "devices_existing_uid": "static",
        }),
        "device_inspect" => {
            let name = match req.params.get("name").and_then(|v| v.as_str()) {
                Some(s) => s,
                None => {
                    return err("device_inspect: missing string param 'name'");
                }
            };
            match registry.inspect_device(name) {
                Some(state) => json!({"success": true, "msg": "", "name": name, "state": state}),
                None => err(format!("device_inspect: no device named {name:?}")),
            }
        }

        // -- environment --------------------------------------------------
        "environment_open" => {
            env_open(document_sink, &state, &engine, rt, checkpoint_hook.as_ref())
        }
        "environment_close" => env_close(&state, &engine, rt),
        "environment_destroy" => env_close(&state, &engine, rt), // forced close
        "environment_update" => json!({"success": true, "msg": ""}),

        // -- queue contents -----------------------------------------------
        "queue_get" => queue_get(&queue, &state),
        "queue_clear" => {
            queue.lock().unwrap().clear();
            json!({"success": true, "msg": ""})
        }
        "queue_item_add" => queue_item_add(&registry, &queue, &req.params),
        "queue_item_add_batch" => queue_item_add_batch(&registry, &queue, &req.params),
        "queue_item_update" => queue_item_update(&queue, &req.params),
        "queue_item_get" => queue_item_get(&queue, &req.params),
        "queue_item_remove" => queue_item_remove(&queue, &req.params),
        "queue_item_remove_batch" => queue_item_remove_batch(&queue, &req.params),
        "queue_item_move" => queue_item_move(&queue, &req.params),
        "queue_item_move_batch" => queue_item_move_batch(&queue, &req.params),
        "queue_item_execute" => queue_item_execute(&registry, &engine, rt, &req.params),

        // -- queue execution ----------------------------------------------
        "queue_start" => queue_start(&registry, &queue, &state, &engine, rt, &queue_task),
        "queue_stop" => {
            state.lock().unwrap().queue_stop_pending = true;
            json!({"success": true, "msg": ""})
        }
        "queue_stop_cancel" => {
            state.lock().unwrap().queue_stop_pending = false;
            json!({"success": true, "msg": ""})
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
            json!({"success": true, "msg": ""})
        }
        "queue_mode_set" => {
            let mode = match req.params.get("mode") {
                Some(Value::Object(m)) => m.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
                _ => return err("missing 'mode' object"),
            };
            state.lock().unwrap().queue_mode = mode;
            json!({"success": true, "msg": ""})
        }

        // -- history ------------------------------------------------------
        "history_get" => {
            let q = queue.lock().unwrap();
            json!({
                "success": true,
                "msg": "",
                "items": q.history_snapshot(),
                "plan_history_uid": q.history_uid(),
            })
        }
        "history_clear" => {
            queue.lock().unwrap().clear_history();
            json!({"success": true, "msg": ""})
        }

        // -- RunEngine control --------------------------------------------
        "re_pause" => re_pause(&engine, rt, &req.params),
        "re_resume" => re_with(&engine, rt, |re| re.resume()),
        "re_abort" => re_with(&engine, rt, |re| {
            re.abort("user abort");
        }),
        "re_halt" => re_with(&engine, rt, |re| re.halt("user halt")),
        "re_stop" => re_with(&engine, rt, |re| re.stop()),
        "re_runs" => re_runs(&state),
        "re_metadata" => re_metadata(&engine, rt, &req.params),

        // -- locks --------------------------------------------------------
        "lock" => lock_apply(&state, &req.params),
        "lock_info" => {
            let st = state.lock().unwrap();
            json!({
                "success": true,
                "msg": "",
                "lock_info": serde_json::to_value(&st.lock).unwrap(),
                "lock_info_uid": st.lock.uid.clone(),
            })
        }
        "unlock" => lock_release(&state, &req.params),

        // -- bluesky-queueserver wire compat: many clients always
        //    call these even when the server side does the work
        //    synchronously. Return a "completed / no-op" shape so
        //    naive clients don't error.
        "task_status" => {
            let uid = req
                .params
                .get("task_uid")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            // Per-task RBAC: if the originating method was Admin
            // class, only admin callers may poll the result.
            if let Err(reason) = check_task_access(uid, &group, &task_tracker, &permissions) {
                return err(reason);
            }
            let status = task_tracker.status(uid).unwrap_or("completed");
            json!({
                "success": true,
                "msg": "",
                "status": status,
                "task_uid": req.params.get("task_uid").cloned().unwrap_or(Value::Null),
            })
        }
        "task_result" => {
            let uid = req
                .params
                .get("task_uid")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            if let Err(reason) = check_task_access(uid, &group, &task_tracker, &permissions) {
                return err(reason);
            }
            let status = task_tracker.status(uid).unwrap_or("completed");
            let (success, return_value, traceback) = match task_tracker.result(uid) {
                Some(r) => (
                    r.is_success(),
                    r.return_value.map(Value::String).unwrap_or(Value::Null),
                    r.error.unwrap_or_default(),
                ),
                None => (true, Value::Null, String::new()),
            };
            let stdout = task_tracker
                .result(uid)
                .map(|r| r.stdout)
                .unwrap_or_default();
            json!({
                "success": true,
                "msg": "",
                "status": status,
                "result": {
                    "return_value": return_value,
                    "traceback": traceback,
                    "stdout": stdout,
                    "msg": "",
                    "success": success,
                    "task_uid": uid,
                },
            })
        }
        "lua_eval" => {
            let src = match req.params.get("source").and_then(|v| v.as_str()) {
                Some(s) => s.to_string(),
                None => {
                    return err("lua_eval: missing string param 'source'");
                }
            };
            // Sanity-bound the input. A malicious or buggy client
            // sending tens of MB of Lua source would otherwise pin
            // daemon memory through the parse + spawn path.
            const MAX_LUA_EVAL_SOURCE: usize = 1 << 20; // 1 MiB
            if src.len() > MAX_LUA_EVAL_SOURCE {
                return err(format!(
                    "lua_eval: source too large ({} bytes, max {} bytes)",
                    src.len(),
                    MAX_LUA_EVAL_SOURCE
                ));
            }
            let ev = match lua_evaluator.clone() {
                Some(e) => e,
                None => {
                    return err("lua_eval: this cirrus-qs build has no Lua evaluator wired \
                         (use `cirrus qs-manager` rather than a custom build)");
                }
            };
            let task_uid = uuid::Uuid::new_v4().to_string();
            task_tracker.start(&task_uid, "lua_eval");
            let tracker = task_tracker.clone();
            let uid_for_task = task_uid.clone();
            rt.spawn(async move {
                // Catch panics from the eval future so a fault
                // (mlua bug, OOM, etc.) doesn't leave the task
                // stuck in `Running` forever.
                use futures::FutureExt;
                let result = match std::panic::AssertUnwindSafe(ev.eval(&src))
                    .catch_unwind()
                    .await
                {
                    Ok(r) => r,
                    Err(p) => {
                        let msg = panic_payload_message(p);
                        crate::tasks::EvalResult {
                            stdout: String::new(),
                            return_value: None,
                            error: Some(format!("lua_eval panicked: {msg}")),
                        }
                    }
                };
                tracker.complete(&uid_for_task, result);
            });
            json!({"success": true, "msg": "", "task_uid": task_uid})
        }
        "manager_test" => json!({"success": true, "msg": ""}),
        "permissions_get" => json!({
            "success": true,
            "msg": "",
            "user_group_permissions": permissions.snapshot_for_get(),
            "user_group_permissions_uid": permissions_uid(&permissions),
        }),
        "permissions_reload" => match permissions.reload() {
            Ok(()) => json!({"success": true, "msg": "permissions reloaded"}),
            Err(e) => err(format!("permissions_reload: {e}")),
        },

        // -- not-implemented stubs (registered so clients see the method
        //    name but get a defined error). --------------------------------
        "permissions_set" | "script_upload" | "function_execute" | "kernel_interrupt"
        | "manager_stop" | "manager_kill" => err(format!(
            "method '{m}' is registered but not implemented in cirrus-qs \
             (bluesky-queueserver-only feature)"
        )),

        // Unknown.
        other => err(format!("unknown method: {other}")),
    }
}

// -- helpers ----------------------------------------------------------------

fn panic_payload_message(p: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = p.downcast_ref::<&'static str>() {
        return s.to_string();
    }
    if let Some(s) = p.downcast_ref::<String>() {
        return s.clone();
    }
    "<no message>".to_string()
}

/// Per-task RBAC gate for `task_status` / `task_result`.
fn check_task_access(
    uid: &str,
    caller_group: &str,
    tracker: &Arc<TaskTracker>,
    permissions: &Arc<Permissions>,
) -> Result<(), String> {
    let Some(source) = tracker.source_method(uid) else {
        return Ok(());
    };
    if classify_local(&source) == crate::permissions::MethodClass::Admin
        && !permissions.is_admin(caller_group)
    {
        return Err(format!(
            "RBAC: task {uid:?} originated from admin-class method '{source}'; \
             non-admin caller cannot poll its status / result"
        ));
    }
    Ok(())
}

fn classify_local(method: &str) -> crate::permissions::MethodClass {
    crate::permissions::classify(method)
}

fn permissions_uid(p: &Permissions) -> String {
    let snap = p.snapshot_for_get();
    let body = serde_json::to_string(&snap).unwrap_or_default();
    let mut h = DefaultHasher::new();
    body.hash(&mut h);
    format!("{:016x}", h.finish())
}

fn lock_check(method: &str, state: &Arc<StdMutex<EngineState>>, params: &Value) -> bool {
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
    let key = params.get("lock_key").and_then(|v| v.as_str());
    let supplied_hash = key.map(hash_key);
    if method == "unlock" {
        return supplied_hash == st.lock.key_hash;
    }
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
    state: &Arc<StdMutex<EngineState>>,
    queue: &Arc<StdMutex<PlanQueue>>,
    engine: &Arc<Mutex<Option<Arc<RunEngine>>>>,
    rt: &tokio::runtime::Handle,
) -> Value {
    let q = queue.lock().unwrap();
    let st = state.lock().unwrap().clone();
    let env_exists = rt.block_on(engine.lock()).is_some();
    let re_state = if env_exists {
        st.state.map(|s| s.as_str()).unwrap_or("idle").to_string()
    } else {
        "null".to_string()
    };
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
        "devices_allowed_uid": "static",
        "plans_allowed_uid": "static",
        "devices_existing_uid": "static",
        "plans_existing_uid": "static",
        "task_results_uid": "static",
        "run_list_uid": "static",
    })
}

fn env_open(
    document_sink: Option<Arc<dyn DocumentSink>>,
    state: &Arc<StdMutex<EngineState>>,
    engine: &Arc<Mutex<Option<Arc<RunEngine>>>>,
    rt: &tokio::runtime::Handle,
    checkpoint_hook: Option<&CheckpointHook>,
) -> Value {
    let mut e = rt.block_on(engine.lock());
    if e.is_some() {
        return err("environment already open");
    }
    let sinks: Vec<Arc<dyn DocumentSink>> = document_sink.iter().cloned().collect();
    let re = Arc::new(RunEngine::new(sinks));
    if let Some(hook) = checkpoint_hook {
        re.set_checkpoint_hook(hook.clone());
    }
    *e = Some(re);
    state.lock().unwrap().state = Some(EState::Idle);
    json!({"success": true, "msg": ""})
}

fn env_close(
    state: &Arc<StdMutex<EngineState>>,
    engine: &Arc<Mutex<Option<Arc<RunEngine>>>>,
    rt: &tokio::runtime::Handle,
) -> Value {
    let mut e = rt.block_on(engine.lock());
    if e.is_none() {
        return err("no environment");
    }
    *e = None;
    state.lock().unwrap().state = Some(EState::EnvironmentClosed);
    json!({"success": true, "msg": ""})
}

fn queue_get(queue: &Arc<StdMutex<PlanQueue>>, state: &Arc<StdMutex<EngineState>>) -> Value {
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
    json!({
        "success": true,
        "msg": "",
        "items": q.snapshot(),
        "running_item": running,
        "plan_queue_uid": q.queue_uid(),
    })
}

fn queue_item_add(
    registry: &Arc<Registry>,
    queue: &Arc<StdMutex<PlanQueue>>,
    params: &Value,
) -> Value {
    let item = match params.get("item") {
        Some(it) => it.clone(),
        None => return err("missing 'item'"),
    };
    let name = match item.get("name").and_then(|v| v.as_str()) {
        Some(n) => n.to_string(),
        None => return err("item.name required"),
    };
    if registry.plan(&name).is_none() {
        return err(format!("unknown plan: {name}"));
    }
    let queued = QueuedItem::plan(name, item);
    let queued_val = serde_json::to_value(&queued).unwrap();
    let mut q = queue.lock().unwrap();
    q.push_back(queued);
    json!({
        "success": true,
        "msg": "",
        "qsize": q.len(),
        "item": queued_val,
        "plan_queue_uid": q.queue_uid(),
    })
}

fn queue_item_add_batch(
    registry: &Arc<Registry>,
    queue: &Arc<StdMutex<PlanQueue>>,
    params: &Value,
) -> Value {
    let items = match params.get("items").and_then(|v| v.as_array()) {
        Some(a) => a.clone(),
        None => return err("missing 'items' array"),
    };
    let mut added_items: Vec<Value> = Vec::new();
    let mut results: Vec<Value> = Vec::new();
    let mut had_error = false;
    for item in items {
        let name = item
            .get("name")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        match name {
            Some(n) if registry.plan(&n).is_some() => {
                let qi = QueuedItem::plan(n, item);
                let qi_val = serde_json::to_value(&qi).unwrap();
                queue.lock().unwrap().push_back(qi);
                added_items.push(qi_val);
                results.push(json!({"success": true, "msg": ""}));
            }
            Some(n) => {
                let msg = format!("unknown plan: {n}");
                results.push(json!({"success": false, "msg": msg}));
                had_error = true;
            }
            None => {
                results.push(json!({"success": false, "msg": "item.name required"}));
                had_error = true;
            }
        }
    }
    let q = queue.lock().unwrap();
    json!({
        "success": !had_error,
        "msg": if had_error { "one or more items failed" } else { "" },
        "qsize": q.len(),
        "items": added_items,
        "results": results,
        "plan_queue_uid": q.queue_uid(),
    })
}

fn queue_item_update(queue: &Arc<StdMutex<PlanQueue>>, params: &Value) -> Value {
    let item = match params.get("item") {
        Some(i) => i.clone(),
        None => return err("missing 'item'"),
    };
    let uid = match item.get("item_uid").and_then(|v| v.as_str()) {
        Some(u) => u.to_string(),
        None => return err("item.item_uid required"),
    };
    let name = item
        .get("name")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let mut q = queue.lock().unwrap();
    let new_item = QueuedItem::plan(name, item);
    match q.update(&uid, new_item) {
        Some(updated) => json!({
            "success": true,
            "msg": "",
            "item": serde_json::to_value(&updated).unwrap(),
            "plan_queue_uid": q.queue_uid(),
        }),
        None => err(format!("uid not found: {uid}")),
    }
}

fn queue_item_get(queue: &Arc<StdMutex<PlanQueue>>, params: &Value) -> Value {
    let q = queue.lock().unwrap();
    if let Some(uid) = params.get("uid").and_then(|v| v.as_str()) {
        return match q.get_by_uid(uid) {
            Some(it) => {
                json!({"success": true, "msg": "", "item": serde_json::to_value(it).unwrap()})
            }
            None => err(format!("uid not found: {uid}")),
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
            Some(it) => {
                json!({"success": true, "msg": "", "item": serde_json::to_value(it).unwrap()})
            }
            None => err(format!("pos not found: {pos}")),
        };
    }
    err("specify 'uid' or 'pos'")
}

fn queue_item_remove(queue: &Arc<StdMutex<PlanQueue>>, params: &Value) -> Value {
    let uid = match params.get("uid").and_then(|v| v.as_str()) {
        Some(u) => u.to_string(),
        None => return err("uid required"),
    };
    let mut q = queue.lock().unwrap();
    match q.remove_by_uid(&uid) {
        Some(it) => json!({
            "success": true,
            "msg": "",
            "item": serde_json::to_value(&it).unwrap(),
            "qsize": q.len(),
            "plan_queue_uid": q.queue_uid(),
        }),
        None => err(format!("uid not found: {uid}")),
    }
}

fn queue_item_remove_batch(queue: &Arc<StdMutex<PlanQueue>>, params: &Value) -> Value {
    let uids = match params.get("uids").and_then(|v| v.as_array()) {
        Some(a) => a
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect::<Vec<_>>(),
        None => return err("missing 'uids' array"),
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
    json!({
        "success": true,
        "msg": "",
        "items_removed": removed,
        "qsize": q.len(),
        "plan_queue_uid": q.queue_uid(),
    })
}

fn queue_item_move(queue: &Arc<StdMutex<PlanQueue>>, params: &Value) -> Value {
    let uid = match params.get("uid").and_then(|v| v.as_str()) {
        Some(u) => u.to_string(),
        None => return err("uid required"),
    };
    let dest = resolve_pos(params.get("pos_dest"), queue);
    let mut q = queue.lock().unwrap();
    match q.move_to(&uid, dest) {
        Some(it) => json!({
            "success": true,
            "msg": "",
            "item": serde_json::to_value(&it).unwrap(),
            "plan_queue_uid": q.queue_uid(),
        }),
        None => err(format!("uid not found: {uid}")),
    }
}

fn queue_item_move_batch(queue: &Arc<StdMutex<PlanQueue>>, params: &Value) -> Value {
    let uids = match params.get("uids").and_then(|v| v.as_array()) {
        Some(a) => a
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect::<Vec<_>>(),
        None => return err("missing 'uids' array"),
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
    json!({
        "success": true,
        "msg": "",
        "items_moved": moved,
        "plan_queue_uid": q.queue_uid(),
    })
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
    registry: &Arc<Registry>,
    engine: &Arc<Mutex<Option<Arc<RunEngine>>>>,
    rt: &tokio::runtime::Handle,
    params: &Value,
) -> Value {
    let item = match params.get("item") {
        Some(i) => i.clone(),
        None => return err("missing 'item'"),
    };
    let name = match item.get("name").and_then(|v| v.as_str()) {
        Some(n) => n.to_string(),
        None => return err("item.name required"),
    };
    let factory = match registry.plan(&name) {
        Some(f) => f.clone(),
        None => return err(format!("unknown plan: {name}")),
    };
    let plan = match factory(registry, &item) {
        Ok(p) => p,
        Err(e) => return err(format!("plan build failed: {e}")),
    };
    let e_guard = rt.block_on(engine.lock());
    let re = match e_guard.as_ref() {
        Some(r) => r.clone(),
        None => return err("environment not open"),
    };
    drop(e_guard);
    let result = rt.block_on(re.run_async(plan));
    match result {
        Ok(r) => json!({
            "success": r.exit_status == "success",
            "msg": "",
            "exit_status": r.exit_status,
            "run_uid": r.run_uid,
        }),
        Err(e) => err(format!("run failed: {e}")),
    }
}

#[allow(clippy::too_many_arguments)]
fn queue_start(
    registry: &Arc<Registry>,
    queue: &Arc<StdMutex<PlanQueue>>,
    state: &Arc<StdMutex<EngineState>>,
    engine: &Arc<Mutex<Option<Arc<RunEngine>>>>,
    rt: &tokio::runtime::Handle,
    queue_task: &Arc<StdMutex<Option<AbortHandle>>>,
) -> Value {
    let e_guard = rt.block_on(engine.lock());
    let re = match e_guard.as_ref() {
        Some(r) => r.clone(),
        None => return err("environment not open"),
    };
    drop(e_guard);
    let cur_state = state.lock().unwrap().state;
    if cur_state != Some(EState::Idle) {
        return err(format!("cannot start in state {cur_state:?}"));
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
    json!({"success": true, "msg": ""})
}

fn re_pause(
    engine: &Arc<Mutex<Option<Arc<RunEngine>>>>,
    rt: &tokio::runtime::Handle,
    params: &Value,
) -> Value {
    let e_guard = rt.block_on(engine.lock());
    if let Some(re) = e_guard.as_ref() {
        let defer = params
            .get("option")
            .and_then(|v| v.as_str())
            .map(|s| s == "deferred")
            .unwrap_or(false);
        re.pause(defer);
        json!({"success": true, "msg": ""})
    } else {
        err("no environment")
    }
}

fn re_with(
    engine: &Arc<Mutex<Option<Arc<RunEngine>>>>,
    rt: &tokio::runtime::Handle,
    f: impl FnOnce(&Arc<RunEngine>),
) -> Value {
    let e_guard = rt.block_on(engine.lock());
    if let Some(re) = e_guard.as_ref() {
        f(re);
        json!({"success": true, "msg": ""})
    } else {
        err("no environment")
    }
}

fn re_runs(state: &Arc<StdMutex<EngineState>>) -> Value {
    let st = state.lock().unwrap();
    let runs: Vec<Value> = st
        .re_runs
        .iter()
        .map(|uid| json!({"uid": uid, "is_open": false}))
        .collect();
    json!({"success": true, "msg": "", "run_list": runs, "run_list_uid": "static"})
}

fn re_metadata(
    engine: &Arc<Mutex<Option<Arc<RunEngine>>>>,
    rt: &tokio::runtime::Handle,
    params: &Value,
) -> Value {
    let e_guard = rt.block_on(engine.lock());
    let re = match e_guard.as_ref() {
        Some(r) => r.clone(),
        None => return err("no environment"),
    };
    drop(e_guard);
    if let Some(md_in) = params.get("metadata").and_then(|v| v.as_object()) {
        let merged: std::collections::HashMap<String, Value> =
            md_in.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        re.md_replace(merged);
    }
    json!({
        "success": true,
        "msg": "",
        "re_metadata": re.md(),
    })
}

fn lock_apply(state: &Arc<StdMutex<EngineState>>, params: &Value) -> Value {
    let key = match params.get("lock_key").and_then(|v| v.as_str()) {
        Some(k) => k,
        None => return err("missing 'lock_key'"),
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
        return err("must lock at least one of `environment` / `queue`");
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
            return err("subsystem already locked");
        }
        st.lock.lock(env, queue, user, note, hash_key(key));
    }
    let st = state.lock().unwrap();
    json!({
        "success": true,
        "msg": "",
        "lock_info": serde_json::to_value(&st.lock).unwrap(),
        "lock_info_uid": st.lock.uid.clone(),
    })
}

fn lock_release(state: &Arc<StdMutex<EngineState>>, params: &Value) -> Value {
    let key = match params.get("lock_key").and_then(|v| v.as_str()) {
        Some(k) => k,
        None => return err("missing 'lock_key'"),
    };
    let mut st = state.lock().unwrap();
    if st.lock.is_locked() && st.lock.key_hash != Some(hash_key(key)) {
        return err("lock_key does not match");
    }
    st.lock.clear();
    json!({
        "success": true,
        "msg": "",
        "lock_info": serde_json::to_value(&st.lock).unwrap(),
        "lock_info_uid": st.lock.uid.clone(),
    })
}
