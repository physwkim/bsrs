//! bluesky-queueserver protocol dispatch table. Mirrors
//! `_zmq_execute` (manager.py:3697) — every public method name is
//! registered here so clients see a uniform "method known / unknown"
//! distinction instead of hitting the catch-all unknown-method response.
//!
//! Methods that don't map to bsrs's single-binary, no-IPython,
//! no-permissions model return a flat error with a clear reason string.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;

use crate::engine::{CheckpointHook, DocumentSink, RunEngine};
use serde_json::{json, Value};
use tokio::sync::Mutex;
use tokio::task::AbortHandle;

use crate::qs::lua_eval::LuaEvaluator;
use crate::qs::methods::{err, QsRequest};
use crate::qs::permissions::Permissions;
use crate::qs::queue::{PlanQueue, QueuedItem};
use crate::qs::registry::Registry;
use crate::qs::state::{EState, EngineState};
use crate::qs::tasks::TaskTracker;

/// Top-level dispatch entry. Returns a flat bluesky-queueserver response dict.
/// When `manager_stop` is called, `stop_requested` is set to `true`; the
/// caller (rep_loop) must check this after sending the response and exit.
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
    stop_requested: Arc<AtomicBool>,
) -> Value {
    let m = req.method.as_str();

    #[cfg(feature = "metrics")]
    crate::qs::metrics::rpc_call(m);

    // RBAC gate: classify the method and check the caller's group.
    let group = permissions.resolve_group(&req.params);
    if let Err(reason) = permissions.check(m, &req.params, &group) {
        #[cfg(feature = "metrics")]
        crate::qs::metrics::rpc_error(m);
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
                "implementation": "bsrs-qs",
                "runtime": "rust",
                "version": env!("CARGO_PKG_VERSION"),
                "wire_protocol": "bluesky-queueserver-compatible (subset)",
                "ip_connect_info": {},
            },
        }),

        // -- plans / devices listing -------------------------------------
        // Return rich dicts matching bluesky's wire shape:
        //   {plan_name: {name, description, parameters, module}}
        // (ref: manager.py:_plans_allowed_handler,_plans_existing_handler).
        // plans_allowed filters by the caller's group (QS-14); tolerate
        // absent user_group — caller group already resolved from api_key.
        "plans_allowed" => {
            let all_names = registry.plan_names();
            let allowed: Vec<String> = permissions
                .filter_plans_for_group(&group, &all_names)
                .into_iter()
                .cloned()
                .collect();
            let dict = registry.plans_dict_subset(&allowed);
            json!({
                "success": true,
                "msg": "",
                "plans_allowed": dict,
                "plans_allowed_uid": "static",
            })
        }
        "plans_existing" => json!({
            "success": true,
            "msg": "",
            "plans_existing": registry.plan_dict(),
            "plans_existing_uid": "static",
        }),
        "devices_allowed" => {
            let all_names = registry.device_names();
            let allowed: Vec<String> = permissions
                .filter_devices_for_group(&group, &all_names)
                .into_iter()
                .cloned()
                .collect();
            let dict = registry.devices_dict_subset(&allowed);
            json!({
                "success": true,
                "msg": "",
                "devices_allowed": dict,
                "devices_allowed_uid": "static",
            })
        }
        "devices_existing" => json!({
            "success": true,
            "msg": "",
            "devices_existing": registry.device_dict(),
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
        "environment_destroy" => env_destroy(&state, &engine, &queue_task, rt),
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
            // Strict: a typo'd request must not silently disable autostart.
            let enable = match (req.params.get("enable"), req.params.get("option")) {
                (Some(Value::Bool(b)), _) => *b,
                (None, Some(Value::String(s))) if s == "enable" => true,
                (None, Some(Value::String(s))) if s == "disable" => false,
                _ => {
                    return err("queue_autostart: required param 'enable' (bool) \
                         or 'option' (\"enable\"/\"disable\")")
                }
            };
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
        "re_pause" => re_pause(&state, &engine, rt, &req.params),
        "re_resume" => {
            state.lock().unwrap().pause_pending = false;
            re_with(&engine, rt, |re| re.resume())
        }
        "re_abort" => {
            state.lock().unwrap().pause_pending = false;
            re_with(&engine, rt, |re| {
                re.abort("user abort");
            })
        }
        "re_halt" => {
            state.lock().unwrap().pause_pending = false;
            re_with(&engine, rt, |re| re.halt("user halt"))
        }
        "re_stop" => re_with(&engine, rt, |re| re.stop()),
        "re_runs" => re_runs(&state, &req.params),
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
                    return err("lua_eval: this bsrs-qs build has no Lua evaluator wired \
                         (use `bsrs qs-manager` rather than a custom build)");
                }
            };
            let task_uid = uuid::Uuid::new_v4().to_string();
            task_tracker.start(&task_uid, "lua_eval");
            let tracker = task_tracker.clone();
            let uid_for_task = task_uid.clone();
            let state_for_task = state.clone();
            state.lock().unwrap().worker_background_tasks += 1;
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
                        crate::qs::tasks::EvalResult {
                            stdout: String::new(),
                            return_value: None,
                            error: Some(format!("lua_eval panicked: {msg}")),
                        }
                    }
                };
                tracker.complete(&uid_for_task, result);
                let mut st = state_for_task.lock().unwrap();
                st.worker_background_tasks = st.worker_background_tasks.saturating_sub(1);
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

        // -- manager stop -------------------------------------------------
        "manager_stop" => {
            // Abort any in-flight queue task, then signal the rep loop to exit.
            if let Some(h) = queue_task.lock().unwrap().take() {
                h.abort();
            }
            stop_requested.store(true, std::sync::atomic::Ordering::SeqCst);
            json!({"success": true, "msg": ""})
        }

        // -- function_execute (ref: manager.py:2992) ----------------------
        "function_execute" => {
            function_execute(rt, &req.params, &lua_evaluator, &task_tracker, &state)
        }

        // -- not-implemented stubs (registered so clients see the method
        //    name but get a defined error). --------------------------------
        "permissions_set" | "script_upload" | "kernel_interrupt" | "manager_kill" => err(format!(
            "method '{m}' is registered but not implemented in bsrs-qs \
                 (bluesky-queueserver-only feature)"
        )),

        // Unknown.
        other => err(format!("unknown method: {other}")),
    }
}

// -- helpers ----------------------------------------------------------------

/// Synthesize a Lua call `name(arg1, arg2, ..., {kwarg1=v1, ...})`.
/// Positional args from `args` array come first; keyword args from
/// `kwargs` object are passed as a trailing Lua table.
fn lua_source_for_function(name: &str, args: &Value, kwargs: &Value) -> String {
    let mut parts: Vec<String> = Vec::new();
    if let Some(arr) = args.as_array() {
        for v in arr {
            parts.push(serde_json::to_string(v).unwrap_or_else(|_| "nil".into()));
        }
    }
    if let Some(obj) = kwargs.as_object() {
        if !obj.is_empty() {
            let pairs: Vec<String> = obj
                .iter()
                .map(|(k, v)| {
                    format!(
                        "{}={}",
                        k,
                        serde_json::to_string(v).unwrap_or_else(|_| "nil".into())
                    )
                })
                .collect();
            parts.push(format!("{{{}}}", pairs.join(",")));
        }
    }
    format!("return {}({})", name, parts.join(","))
}

#[allow(clippy::too_many_arguments)]
fn function_execute(
    rt: &tokio::runtime::Handle,
    params: &Value,
    lua_evaluator: &Option<Arc<dyn LuaEvaluator>>,
    task_tracker: &Arc<TaskTracker>,
    state: &Arc<StdMutex<EngineState>>,
) -> Value {
    let item = match params.get("item") {
        Some(i) => i.clone(),
        None => return err("function_execute: missing 'item'"),
    };
    // item_type must be "function" (ref: manager.py:3021).
    let item_type = item
        .get("item_type")
        .and_then(|v| v.as_str())
        .unwrap_or("function");
    if item_type != "function" {
        return err(format!(
            "function_execute: item_type must be 'function', got {item_type:?}"
        ));
    }
    let name = match item.get("name").and_then(|v| v.as_str()) {
        Some(n) => n.to_string(),
        None => return err("function_execute: item.name required"),
    };
    let args = item.get("args").cloned().unwrap_or(Value::Array(vec![]));
    let kwargs = item
        .get("kwargs")
        .cloned()
        .unwrap_or(Value::Object(Default::default()));
    let run_in_background = params
        .get("run_in_background")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let _ = run_in_background; // background vs foreground distinction not yet tracked

    let ev = match lua_evaluator.clone() {
        Some(e) => e,
        None => {
            return err("function_execute: no Lua evaluator wired \
                 (use `bsrs qs-manager` rather than a custom build)");
        }
    };

    // Generate a unique item_uid and stamp it into the returned item dict.
    let item_uid = uuid::Uuid::new_v4().to_string();
    let mut returned_item = item.clone();
    if let Some(obj) = returned_item.as_object_mut() {
        obj.insert("item_uid".to_string(), Value::String(item_uid.clone()));
        obj.insert(
            "item_type".to_string(),
            Value::String("function".to_string()),
        );
    }

    let src = lua_source_for_function(&name, &args, &kwargs);
    let task_uid = uuid::Uuid::new_v4().to_string();
    task_tracker.start(&task_uid, "function_execute");
    let tracker = task_tracker.clone();
    let uid_for_task = task_uid.clone();
    let state_for_task = state.clone();
    state.lock().unwrap().worker_background_tasks += 1;
    rt.spawn(async move {
        use futures::FutureExt;
        let result = match std::panic::AssertUnwindSafe(ev.eval(&src))
            .catch_unwind()
            .await
        {
            Ok(r) => r,
            Err(p) => {
                let msg = if let Some(s) = p.downcast_ref::<&'static str>() {
                    s.to_string()
                } else if let Some(s) = p.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "<no message>".to_string()
                };
                crate::qs::tasks::EvalResult {
                    stdout: String::new(),
                    return_value: None,
                    error: Some(format!("function_execute panicked: {msg}")),
                }
            }
        };
        tracker.complete(&uid_for_task, result);
        let mut st = state_for_task.lock().unwrap();
        st.worker_background_tasks = st.worker_background_tasks.saturating_sub(1);
    });

    json!({
        "success": true,
        "msg": "",
        "item": returned_item,
        "task_uid": task_uid,
    })
}

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
    if classify_local(&source) == crate::qs::permissions::MethodClass::Admin
        && !permissions.is_admin(caller_group)
    {
        return Err(format!(
            "RBAC: task {uid:?} originated from admin-class method '{source}'; \
             non-admin caller cannot poll its status / result"
        ));
    }
    Ok(())
}

fn classify_local(method: &str) -> crate::qs::permissions::MethodClass {
    crate::qs::permissions::classify(method)
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
    let (env_exists, engine_paused) = {
        let e_guard = rt.block_on(engine.lock());
        let paused = e_guard
            .as_ref()
            .is_some_and(|re| re.state() == crate::engine::EngineRunState::Paused);
        (e_guard.is_some(), paused)
    };
    // The engine owns pause truth: the manager-side `EState` stays
    // `ExecutingQueue` while `run_async` is parked at the pause gate, so
    // "paused" is derived live from the engine instead of duplicating the
    // transition into manager state.
    let paused_mid_run = engine_paused && st.state == Some(EState::ExecutingQueue);
    let manager_state = if paused_mid_run {
        EState::Paused.as_str()
    } else {
        st.state.map(|s| s.as_str()).unwrap_or("environment_closed")
    };
    let re_state = if paused_mid_run {
        EState::Paused.as_str().to_string()
    } else if env_exists {
        st.state.map(|s| s.as_str()).unwrap_or("idle").to_string()
    } else {
        "null".to_string()
    };
    json!({
        "success": true,
        "msg": "",
        "status_uid": uuid::Uuid::new_v4().to_string(),
        "time": crate::qs::state::now_iso8601(),
        "manager_state": manager_state,
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
        // A landed pause is no longer pending.
        "pause_pending": st.pause_pending && !paused_mid_run,
        "worker_background_tasks": st.worker_background_tasks,
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
    // Set transitional state (ref: manager.py:511 MState.CREATING_ENVIRONMENT).
    state.lock().unwrap().state = Some(EState::CreatingEnvironment);
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
    // Set transitional state (ref: manager.py:591 MState.CLOSING_ENVIRONMENT).
    state.lock().unwrap().state = Some(EState::ClosingEnvironment);
    *e = None;
    state.lock().unwrap().state = Some(EState::EnvironmentClosed);
    json!({"success": true, "msg": ""})
}

fn env_destroy(
    state: &Arc<StdMutex<EngineState>>,
    engine: &Arc<Mutex<Option<Arc<RunEngine>>>>,
    queue_task: &Arc<StdMutex<Option<AbortHandle>>>,
    rt: &tokio::runtime::Handle,
) -> Value {
    // Force-abort any running queue task before dropping the engine.
    if let Some(h) = queue_task.lock().unwrap().take() {
        h.abort();
    }
    // Set transitional state (ref: manager.py:660 MState.DESTROYING_ENVIRONMENT).
    state.lock().unwrap().state = Some(EState::DestroyingEnvironment);
    env_close_inner(state, engine, rt)
}

fn env_close_inner(
    state: &Arc<StdMutex<EngineState>>,
    engine: &Arc<Mutex<Option<Arc<RunEngine>>>>,
    rt: &tokio::runtime::Handle,
) -> Value {
    let mut e = rt.block_on(engine.lock());
    if e.is_none() {
        state.lock().unwrap().state = Some(EState::EnvironmentClosed);
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

/// Insert `queued` into `q` at the position requested by `pos` /
/// `before_uid` / `after_uid` (mirrors `plan_queue_ops.py::add_item_to_queue`).
/// Exactly one mode is honored: `before_uid`, else `after_uid`, else `pos`
/// (string `"front"`/`"back"` or a signed integer index with the reference's
/// negative-index / clamp arithmetic). Returns `Err(msg)` — **without mutating
/// `q`** — when a referenced uid is absent or `pos` is malformed. Shared by
/// single- and batch-add so both resolve position identically.
fn insert_positioned(
    q: &mut PlanQueue,
    queued: QueuedItem,
    pos_val: Option<&Value>,
    before_uid: Option<&str>,
    after_uid: Option<&str>,
) -> Result<(), String> {
    let qsize = q.len() as i64;
    if let Some(buid) = before_uid {
        if !q.insert_before_uid(buid, queued) {
            return Err(format!("before_uid not found: {buid}"));
        }
    } else if let Some(auid) = after_uid {
        if !q.insert_after_uid(auid, queued) {
            return Err(format!("after_uid not found: {auid}"));
        }
    } else {
        match pos_val {
            None => q.push_back(queued),
            Some(Value::String(s)) if s == "back" => q.push_back(queued),
            Some(Value::String(s)) if s == "front" => q.insert_at(0, queued),
            Some(Value::String(s)) => return Err(format!("invalid pos string: {s}")),
            Some(Value::Number(n)) => {
                let i = match n.as_i64() {
                    Some(v) => v,
                    None => return Err("pos must be an integer".to_string()),
                };
                if i == 0 || i < -qsize {
                    q.insert_at(0, queued);
                } else if i == -1 || i >= qsize {
                    q.push_back(queued);
                } else {
                    // pos_reference: positive → direct index; negative → qsize + pos + 1
                    let ref_idx = if i > 0 {
                        i as usize
                    } else {
                        (qsize + i + 1) as usize
                    };
                    q.insert_at(ref_idx, queued);
                }
            }
            Some(other) => return Err(format!("invalid pos value: {other}")),
        }
    }
    Ok(())
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
    let item_type = item
        .get("item_type")
        .and_then(|v| v.as_str())
        .unwrap_or("plan");
    let name = match item.get("name").and_then(|v| v.as_str()) {
        Some(n) => n.to_string(),
        None => return err("item.name required"),
    };

    // Positional-insertion params (ref: plan_queue_ops.py:900-968).
    let pos_val = params.get("pos");
    let before_uid_str = params.get("before_uid").and_then(|v| v.as_str());
    let after_uid_str = params.get("after_uid").and_then(|v| v.as_str());

    if pos_val.is_some() && (before_uid_str.is_some() || after_uid_str.is_some()) {
        return err("ambiguous: cannot specify both 'pos' and 'before_uid'/'after_uid'");
    }
    if before_uid_str.is_some() && after_uid_str.is_some() {
        return err("ambiguous: cannot specify both 'before_uid' and 'after_uid'");
    }

    // Build queue item based on item_type (ref: manager.py:_prepare_item).
    let mut queued = match item_type {
        "plan" => {
            if registry.plan(&name).is_none() {
                return err(format!("unknown plan: {name}"));
            }
            QueuedItem::plan(name, item)
        }
        "instruction" => {
            if name != "queue_stop" {
                return err(format!(
                    "unsupported instruction: {name:?} (only 'queue_stop' is supported)"
                ));
            }
            QueuedItem::instruction(name)
        }
        other => return err(format!("unsupported item_type: {other:?}")),
    };
    queued.user = params
        .get("user")
        .and_then(|v| v.as_str())
        .map(String::from);
    queued.user_group = params
        .get("user_group")
        .and_then(|v| v.as_str())
        .map(String::from);
    let queued_val = serde_json::to_value(&queued).unwrap();

    let mut q = queue.lock().unwrap();
    if let Err(msg) = insert_positioned(&mut q, queued, pos_val, before_uid_str, after_uid_str) {
        return err(msg);
    }
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
    let batch_user = params
        .get("user")
        .and_then(|v| v.as_str())
        .map(String::from);
    let batch_user_group = params
        .get("user_group")
        .and_then(|v| v.as_str())
        .map(String::from);

    // Batch position — applies to the FIRST item; the rest follow it as a
    // contiguous block (ref: plan_queue_ops.py::_add_batch_to_queue). Same
    // ambiguity rules as single-add.
    let pos_val = params.get("pos");
    let before_uid = params.get("before_uid").and_then(|v| v.as_str());
    let after_uid = params.get("after_uid").and_then(|v| v.as_str());
    if pos_val.is_some() && (before_uid.is_some() || after_uid.is_some()) {
        return err("ambiguous: cannot specify both 'pos' and 'before_uid'/'after_uid'");
    }
    if before_uid.is_some() && after_uid.is_some() {
        return err("ambiguous: cannot specify both 'before_uid' and 'after_uid'");
    }

    // Phase 1 — validate and build every item before touching the queue. The
    // batch is atomic: if any item is rejected, nothing is added (ref: the
    // add-then-undo loop in `_add_batch_to_queue`; here up-front validation
    // makes the same guarantee without a partial insert to undo).
    let mut prepared: Vec<QueuedItem> = Vec::with_capacity(items.len());
    let mut results: Vec<Value> = Vec::with_capacity(items.len());
    let mut had_error = false;
    for item in &items {
        let name = item
            .get("name")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        match name {
            Some(n) if registry.plan(&n).is_some() => {
                let mut qi = QueuedItem::plan(n, item.clone());
                qi.user = batch_user.clone();
                qi.user_group = batch_user_group.clone();
                prepared.push(qi);
                results.push(json!({"success": true, "msg": ""}));
            }
            Some(n) => {
                results.push(json!({"success": false, "msg": format!("unknown plan: {n}")}));
                had_error = true;
            }
            None => {
                results.push(json!({"success": false, "msg": "item.name required"}));
                had_error = true;
            }
        }
    }

    let mut q = queue.lock().unwrap();

    // Atomic rejection — queue untouched; return the submitted items unchanged
    // (ref: `items_added = items` on failure).
    if had_error {
        return json!({
            "success": false,
            "msg": "batch rejected: one or more items failed validation",
            "qsize": q.len(),
            "items": items,
            "results": results,
            "plan_queue_uid": q.queue_uid(),
        });
    }

    // Phase 2 — insert the block. First item at the batch position; each
    // subsequent item immediately after the previous, preserving order.
    let mut added_items: Vec<Value> = Vec::with_capacity(prepared.len());
    let mut prev_uid: Option<String> = None;
    for qi in prepared {
        let uid = qi.item_uid.clone();
        let qi_val = serde_json::to_value(&qi).unwrap();
        let placed = match &prev_uid {
            None => insert_positioned(&mut q, qi, pos_val, before_uid, after_uid),
            Some(p) => insert_positioned(&mut q, qi, None, None, Some(p.as_str())),
        };
        if let Err(msg) = placed {
            // Reachable only for the first item (a bad batch before_uid/after_uid),
            // and it fails before any insert — so the queue is still untouched.
            // Every later item inserts after the previous (which exists), so it
            // cannot fail. Atomicity therefore holds without an undo path.
            return err(msg);
        }
        added_items.push(qi_val);
        prev_uid = Some(uid);
    }

    json!({
        "success": true,
        "msg": "",
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
    // replace=true: generate a new UID for the replacement item (manager.py:2552).
    let replace = params
        .get("replace")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let mut q = queue.lock().unwrap();
    let new_item = QueuedItem::plan(name, item);
    let result = if replace {
        q.replace_at_uid(&uid, new_item)
    } else {
        q.update(&uid, new_item)
    };
    match result {
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
    let mut q = queue.lock().unwrap();
    let result = if let Some(ref_uid) = params.get("before_uid").and_then(|v| v.as_str()) {
        q.move_before_uid(&uid, ref_uid)
    } else if let Some(ref_uid) = params.get("after_uid").and_then(|v| v.as_str()) {
        q.move_after_uid(&uid, ref_uid)
    } else {
        drop(q);
        let dest = resolve_pos(params.get("pos_dest"), queue);
        let mut q = queue.lock().unwrap();
        let r = q.move_to(&uid, dest);
        return match r {
            Some(it) => json!({
                "success": true,
                "msg": "",
                "item": serde_json::to_value(&it).unwrap(),
                "plan_queue_uid": q.queue_uid(),
            }),
            None => err(format!("uid not found: {uid}")),
        };
    };
    match result {
        Some(it) => json!({
            "success": true,
            "msg": "",
            "item": serde_json::to_value(&it).unwrap(),
            "plan_queue_uid": q.queue_uid(),
        }),
        None => err(format!("uid or reference uid not found: {uid}")),
    }
}

fn queue_item_move_batch(queue: &Arc<StdMutex<PlanQueue>>, params: &Value) -> Value {
    let uids: Vec<String> = match params.get("uids").and_then(|v| v.as_array()) {
        Some(a) => a
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect(),
        None => return err("missing 'uids' array"),
    };

    // Destination — exactly one of pos_dest / before_uid / after_uid (JSON null
    // counts as unspecified). Mirrors `plan_queue_ops.py::_move_batch`.
    let pos_dest = params.get("pos_dest").filter(|v| !v.is_null());
    let before_uid = params.get("before_uid").and_then(|v| v.as_str());
    let after_uid = params.get("after_uid").and_then(|v| v.as_str());
    let n_dest = pos_dest.is_some() as u8 + before_uid.is_some() as u8 + after_uid.is_some() as u8;
    if n_dest < 1 {
        return err("destination not specified: use 'pos_dest', 'before_uid', or 'after_uid'");
    }
    if n_dest > 1 {
        return err("ambiguous: only one of 'pos_dest', 'before_uid', 'after_uid'");
    }
    let reorder = params
        .get("reorder")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let mut q = queue.lock().unwrap();

    // Atomic pre-validation — on any failure nothing is moved (ref: _move_batch).
    // uids unique.
    let mut seen = std::collections::HashSet::new();
    for u in &uids {
        if !seen.insert(u.as_str()) {
            return err(format!("duplicate uid in batch: {u}"));
        }
    }
    // Every uid exists in the queue.
    let missing: Vec<&str> = uids
        .iter()
        .filter(|u| q.get_by_uid(u).is_none())
        .map(String::as_str)
        .collect();
    if !missing.is_empty() {
        return err(format!("uids not in queue: {}", missing.join(", ")));
    }
    // The reference uid must exist and must NOT be a member of the batch.
    if let Some(b) = before_uid {
        if uids.iter().any(|u| u == b) {
            return err(format!("before_uid is in the batch: {b}"));
        }
        if q.get_by_uid(b).is_none() {
            return err(format!("before_uid not found: {b}"));
        }
    }
    if let Some(a) = after_uid {
        if uids.iter().any(|u| u == a) {
            return err(format!("after_uid is in the batch: {a}"));
        }
        if q.get_by_uid(a).is_none() {
            return err(format!("after_uid not found: {a}"));
        }
    }

    // reorder=false → move in the order given in `uids`; reorder=true → move in
    // the items' original queue order (sort uids by current index).
    let ordered: Vec<String> = if reorder {
        let mut with_idx: Vec<(usize, String)> = uids
            .iter()
            .map(|u| (q.index_of_uid(u).unwrap(), u.clone()))
            .collect();
        with_idx.sort_by_key(|(i, _)| *i);
        with_idx.into_iter().map(|(_, u)| u).collect()
    } else {
        uids.clone()
    };

    // Block move — first item to the destination, each subsequent item
    // immediately after the previous, preserving `ordered`.
    let mut moved: Vec<Value> = Vec::with_capacity(ordered.len());
    let mut prev_uid: Option<String> = None;
    for uid in &ordered {
        let placed = match &prev_uid {
            None => {
                if let Some(b) = before_uid {
                    q.move_before_uid(uid, b)
                } else if let Some(a) = after_uid {
                    q.move_after_uid(uid, a)
                } else {
                    let dest = resolve_pos_q(pos_dest, &q);
                    q.move_to(uid, dest)
                }
            }
            Some(p) => q.move_after_uid(uid, p.as_str()),
        };
        match placed {
            Some(it) => moved.push(serde_json::to_value(&it).unwrap()),
            // Unreachable after validation (uid + ref both proven present).
            None => return err(format!("move failed for uid: {uid}")),
        }
        prev_uid = Some(uid.clone());
    }

    json!({
        "success": true,
        "msg": "",
        "items_moved": moved,
        "qsize": q.len(),
        "plan_queue_uid": q.queue_uid(),
    })
}

fn resolve_pos(p: Option<&Value>, queue: &Arc<StdMutex<PlanQueue>>) -> usize {
    resolve_pos_q(p, &queue.lock().unwrap())
}

/// Lock-free `pos_dest` resolution against an already-held queue. Callers that
/// hold the queue lock (e.g. `queue_item_move_batch`) must use this — the
/// lock-taking [`resolve_pos`] would deadlock.
fn resolve_pos_q(p: Option<&Value>, q: &PlanQueue) -> usize {
    match p {
        Some(Value::String(s)) if s == "front" => 0,
        Some(Value::String(s)) if s == "back" => q.len(),
        Some(Value::Number(n)) => n.as_u64().unwrap_or(0) as usize,
        _ => q.len(),
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
            // Preserve the single-valued `run_uid` wire field (the most recently
            // opened run); RunResult now carries all opened UIDs in `run_uids`.
            "run_uid": r.run_uids.last(),
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
    let join = tokio::spawn(crate::qs::server::execute_queue_loop(
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
    state: &Arc<StdMutex<EngineState>>,
    engine: &Arc<Mutex<Option<Arc<RunEngine>>>>,
    rt: &tokio::runtime::Handle,
    params: &Value,
) -> Value {
    let e_guard = rt.block_on(engine.lock());
    if let Some(re) = e_guard.as_ref() {
        // Pausing an idle engine would latch is_paused/deferred_pause with
        // nothing to land on (bluesky-queueserver also rejects re_pause
        // unless a plan is executing).
        if re.state() == crate::engine::EngineRunState::Idle {
            return err("re_pause: no plan is running");
        }
        let defer = params
            .get("option")
            .and_then(|v| v.as_str())
            .map(|s| s == "deferred")
            .unwrap_or(false);
        re.pause(defer);
        state.lock().unwrap().pause_pending = true;
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

fn re_runs(state: &Arc<StdMutex<EngineState>>, params: &Value) -> Value {
    let st = state.lock().unwrap();
    let option = params
        .get("option")
        .and_then(|v| v.as_str())
        .unwrap_or("active");
    let runs: Vec<Value> = st
        .re_runs
        .iter()
        .filter(|(_, is_open)| match option {
            "open" => *is_open,
            "closed" => !is_open,
            _ => true, // "active" = all
        })
        .map(|(uid, is_open)| json!({"uid": uid, "is_open": is_open}))
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
