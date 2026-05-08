//! Lua environment for the cirrus REPL. Wraps cirrus types and plan
//! factories as `mlua::UserData` and globals.
//!
//! ## Concurrency note for Lua callbacks
//!
//! mlua holds a reentrant mutex while a callback is in flight. The
//! REPL thread acquires it during `RE:run` and keeps it parked during
//! `block_on`. A worker thread (monitor pump, suspender watcher,
//! `RE:suspend_until_seconds` auto-resume) that tries to acquire the
//! mutex would block forever → deadlock if the engine awaits the
//! worker's progress.
//!
//! `RE:subscribe` and `msg.subscribe` solve this with thread-aware
//! routing in [`make_lua_subscriber_cb`]: same-thread callbacks fire
//! synchronously (reentrant lock OK); other-thread callbacks push
//! into a per-subscriber buffer and are replayed on the REPL thread
//! after `RE:run`'s `block_on` returns (see
//! [`drain_lua_subscriber_buffers`]). This means worker-emitted docs
//! are still delivered to Lua subscribers, just batched to run end —
//! sufficient for the prototype/debug workflow.
//!
//! The other Lua callbacks ([`RE:set_input_handler`],
//! [`RE:set_md_validator`], [`RE:set_md_normalizer`],
//! [`RE:set_scan_id_source`], [`RE:set_before_plan`],
//! [`RE:set_after_plan`], [`RE:register_command`]) are still subject
//! to the original constraint: they MUST fire on the REPL thread.
//! That holds in practice because the engine invokes them inline
//! during `run_async` (driven by the REPL's `block_on`), never from
//! a spawned task. A future contributor who routes any of those
//! through `tokio::spawn` would break that assumption.

use std::sync::Arc;

use cirrus_backend_soft::{SoftDetector, SoftMotor};
use cirrus_core::msg::{
    CollectableObj, ConfigurableObj, ConfigureArgs, FlyableObj, LocatableObj, MonitorableObj,
    MovableObj, Msg, PausableObj, PreparableObj, ReadableObj, RunMetadata, StageableObj,
    StoppableObj, SubscribeCallback, TriggerableObj,
};
use cirrus_core::plan::{plan_box, Plan};
use cirrus_engine::{DocumentSink, RunEngine};
use mlua::{Lua, ThreadStatus, UserData, UserDataMethods, Value as LuaValue, Variadic};
use tokio::sync::Mutex as TMutex;

/// Holder for an opaque cirrus device. Wraps the trait-object Arc and
/// remembers the device name so Lua-side `tostring` is informative.
#[derive(Clone)]
pub struct LuaDevice {
    pub name: String,
    pub readable: Option<Arc<dyn ReadableObj>>,
    pub movable: Option<Arc<dyn MovableObj>>,
    pub locatable: Option<Arc<dyn LocatableObj>>,
    pub stoppable: Option<Arc<dyn StoppableObj>>,
    pub triggerable: Option<Arc<dyn TriggerableObj>>,
    pub stageable: Option<Arc<dyn StageableObj>>,
    pub monitorable: Option<Arc<dyn MonitorableObj>>,
    pub flyable: Option<Arc<dyn FlyableObj>>,
    pub preparable: Option<Arc<dyn PreparableObj>>,
    pub configurable: Option<Arc<dyn ConfigurableObj>>,
    pub collectable: Option<Arc<dyn CollectableObj>>,
    pub pausable: Option<Arc<dyn PausableObj>>,
}

impl UserData for LuaDevice {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_method("name", |_, dev, ()| Ok(dev.name.clone()));
        methods.add_meta_method("__tostring", |_, dev, ()| {
            let mut roles = Vec::new();
            if dev.readable.is_some() {
                roles.push("readable");
            }
            if dev.movable.is_some() {
                roles.push("movable");
            }
            if dev.locatable.is_some() {
                roles.push("locatable");
            }
            if dev.stoppable.is_some() {
                roles.push("stoppable");
            }
            if dev.triggerable.is_some() {
                roles.push("triggerable");
            }
            if dev.stageable.is_some() {
                roles.push("stageable");
            }
            if dev.monitorable.is_some() {
                roles.push("monitorable");
            }
            if dev.flyable.is_some() {
                roles.push("flyable");
            }
            Ok(format!("Device({}, [{}])", dev.name, roles.join(",")))
        });

        // ---- bluesky-style short-name methods (mirrors cirrus-core::ext) ----

        // motor:position()  ->  number      LocatableExt::position
        methods.add_method("position", |_, dev, ()| {
            let lo = dev.locatable.clone().ok_or_else(|| {
                mlua::Error::RuntimeError(format!("{} is not locatable", dev.name))
            })?;
            cirrus_core::runtime::cirrus_runtime()
                .block_on(lo.locate_dyn())
                .map(|l| l.readback)
                .map_err(|e| mlua::Error::RuntimeError(format!("position: {e}")))
        });
        // motor:target()    ->  number
        methods.add_method("target", |_, dev, ()| {
            let lo = dev.locatable.clone().ok_or_else(|| {
                mlua::Error::RuntimeError(format!("{} is not locatable", dev.name))
            })?;
            cirrus_core::runtime::cirrus_runtime()
                .block_on(lo.locate_dyn())
                .map(|l| l.setpoint)
                .map_err(|e| mlua::Error::RuntimeError(format!("target: {e}")))
        });
        // motor:locate()    ->  {setpoint=, readback=}
        methods.add_method("locate", |lua, dev, ()| {
            let lo = dev.locatable.clone().ok_or_else(|| {
                mlua::Error::RuntimeError(format!("{} is not locatable", dev.name))
            })?;
            let l = cirrus_core::runtime::cirrus_runtime()
                .block_on(lo.locate_dyn())
                .map_err(|e| mlua::Error::RuntimeError(format!("locate: {e}")))?;
            let t = lua.create_table()?;
            t.set("setpoint", l.setpoint)?;
            t.set("readback", l.readback)?;
            Ok(t)
        });
        // det:read()        ->  {field={value=, timestamp=, ...}, ...}
        methods.add_method("read", |lua, dev, ()| {
            let r = dev.readable.clone().ok_or_else(|| {
                mlua::Error::RuntimeError(format!("{} is not readable", dev.name))
            })?;
            let data = cirrus_core::runtime::cirrus_runtime()
                .block_on(r.read_dyn())
                .map_err(|e| mlua::Error::RuntimeError(format!("read: {e}")))?;
            let t = lua.create_table()?;
            for (k, v) in data {
                let inner = lua.create_table()?;
                inner.set("value", json_to_lua(lua, &v.value))?;
                inner.set("timestamp", v.timestamp)?;
                if let Some(s) = v.alarm_severity {
                    inner.set("alarm_severity", s as i64)?;
                }
                if let Some(m) = v.message {
                    inner.set("message", m)?;
                }
                t.set(k, inner)?;
            }
            Ok(t)
        });
        // det:describe()    ->  {field={source=, dtype=, ...}, ...}
        methods.add_method("describe", |lua, dev, ()| {
            let r = dev.readable.clone().ok_or_else(|| {
                mlua::Error::RuntimeError(format!("{} is not readable", dev.name))
            })?;
            let dks = cirrus_core::runtime::cirrus_runtime()
                .block_on(r.describe_dyn())
                .map_err(|e| mlua::Error::RuntimeError(format!("describe: {e}")))?;
            let t = lua.create_table()?;
            for (k, dk) in dks {
                let inner = lua.create_table()?;
                inner.set("source", dk.source)?;
                inner.set("dtype", format!("{:?}", dk.dtype))?;
                inner.set(
                    "shape",
                    dk.shape
                        .iter()
                        .filter_map(|s| s.map(|n| n as i64))
                        .collect::<Vec<i64>>(),
                )?;
                if let Some(u) = dk.units {
                    inner.set("units", u)?;
                }
                if let Some(p) = dk.precision {
                    inner.set("precision", p)?;
                }
                t.set(k, inner)?;
            }
            Ok(t)
        });
        // motor:set(v)      ->  Status userdata (call :wait() for completion)
        methods.add_method("set", |_, dev, v: f64| {
            let mv = dev
                .movable
                .clone()
                .ok_or_else(|| mlua::Error::RuntimeError(format!("{} is not movable", dev.name)))?;
            let status = cirrus_core::runtime::cirrus_runtime().block_on(mv.set_dyn(v));
            Ok(LuaStatus {
                inner: TMutex::new(Some(status)),
                label: format!("set({}={v})", dev.name),
            })
        });
        // motor:move_to(v)  ->  nil  (set + wait for completion)
        methods.add_method("move_to", |_, dev, v: f64| {
            let mv = dev
                .movable
                .clone()
                .ok_or_else(|| mlua::Error::RuntimeError(format!("{} is not movable", dev.name)))?;
            cirrus_core::runtime::cirrus_runtime()
                .block_on(async move {
                    let s = mv.set_dyn(v).await;
                    s.await
                })
                .map_err(|e| mlua::Error::RuntimeError(format!("move_to: {e:?}")))?;
            Ok(())
        });
        // det:trigger()     ->  Status userdata
        methods.add_method("trigger", |_, dev, ()| {
            let t = dev.triggerable.clone().ok_or_else(|| {
                mlua::Error::RuntimeError(format!("{} is not triggerable", dev.name))
            })?;
            let status = cirrus_core::runtime::cirrus_runtime().block_on(t.trigger_dyn());
            Ok(LuaStatus {
                inner: TMutex::new(Some(status)),
                label: format!("trigger({})", dev.name),
            })
        });
        // motor:stop()      ->  nil
        methods.add_method("stop", |_, dev, ()| {
            let s = dev.stoppable.clone().ok_or_else(|| {
                mlua::Error::RuntimeError(format!("{} is not stoppable", dev.name))
            })?;
            cirrus_core::runtime::cirrus_runtime()
                .block_on(s.stop_dyn(true))
                .map_err(|e| mlua::Error::RuntimeError(format!("stop: {e}")))?;
            Ok(())
        });
        // motor:stop_emergency() -> nil
        methods.add_method("stop_emergency", |_, dev, ()| {
            let s = dev.stoppable.clone().ok_or_else(|| {
                mlua::Error::RuntimeError(format!("{} is not stoppable", dev.name))
            })?;
            cirrus_core::runtime::cirrus_runtime()
                .block_on(s.stop_dyn(false))
                .map_err(|e| mlua::Error::RuntimeError(format!("stop_emergency: {e}")))?;
            Ok(())
        });
        // dev:stage() / dev:unstage() -> nil
        methods.add_method("stage", |_, dev, ()| {
            let s = dev.stageable.clone().ok_or_else(|| {
                mlua::Error::RuntimeError(format!("{} is not stageable", dev.name))
            })?;
            cirrus_core::runtime::cirrus_runtime()
                .block_on(s.stage_dyn())
                .map_err(|e| mlua::Error::RuntimeError(format!("stage: {e}")))?;
            Ok(())
        });
        methods.add_method("unstage", |_, dev, ()| {
            let s = dev.stageable.clone().ok_or_else(|| {
                mlua::Error::RuntimeError(format!("{} is not stageable", dev.name))
            })?;
            cirrus_core::runtime::cirrus_runtime()
                .block_on(s.unstage_dyn())
                .map_err(|e| mlua::Error::RuntimeError(format!("unstage: {e}")))?;
            Ok(())
        });
        // flyer:kickoff() / :complete() -> Status
        methods.add_method("kickoff", |_, dev, ()| {
            let f = dev
                .flyable
                .clone()
                .ok_or_else(|| mlua::Error::RuntimeError(format!("{} is not flyable", dev.name)))?;
            let status = cirrus_core::runtime::cirrus_runtime().block_on(f.kickoff_dyn());
            Ok(LuaStatus {
                inner: TMutex::new(Some(status)),
                label: format!("kickoff({})", dev.name),
            })
        });
        methods.add_method("complete", |_, dev, ()| {
            let f = dev
                .flyable
                .clone()
                .ok_or_else(|| mlua::Error::RuntimeError(format!("{} is not flyable", dev.name)))?;
            let status = cirrus_core::runtime::cirrus_runtime().block_on(f.complete_dyn());
            Ok(LuaStatus {
                inner: TMutex::new(Some(status)),
                label: format!("complete({})", dev.name),
            })
        });
        // pause_count / resume_count — only meaningful for
        // soft_pausable test devices. Reads are routed through the
        // PAUSE_COUNTERS side-channel keyed by device name.
        methods.add_method("pause_count", |_, dev, ()| {
            Ok(PAUSE_COUNTERS
                .lock()
                .unwrap()
                .get(&dev.name)
                .map(|c| c.paused.load(std::sync::atomic::Ordering::SeqCst))
                .unwrap_or(0))
        });
        methods.add_method("resume_count", |_, dev, ()| {
            Ok(PAUSE_COUNTERS
                .lock()
                .unwrap()
                .get(&dev.name)
                .map(|c| c.resumed.load(std::sync::atomic::Ordering::SeqCst))
                .unwrap_or(0))
        });
    }
}

/// Side-channel registry of test-only `LuaPausableCounter` instances,
/// keyed by device name, so `LuaDevice:pause_count()` /
/// `:resume_count()` can read the underlying atomic counters without
/// trait-object downcasting.
static PAUSE_COUNTERS: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<String, Arc<LuaPausableCounter>>>,
> = std::sync::LazyLock::new(|| std::sync::Mutex::new(std::collections::HashMap::new()));

/// Lua-side `Status` handle. Wraps a single-use `cirrus_core::Status` so
/// users can `s:wait()` to block on completion. Returned by
/// `motor:set(v)`, `det:trigger()`, `flyer:kickoff()`, `flyer:complete()`.
pub struct LuaStatus {
    inner: TMutex<Option<cirrus_core::status::Status>>,
    label: String,
}

impl UserData for LuaStatus {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_meta_method("__tostring", |_, s, ()| Ok(format!("Status({})", s.label)));
        // s:wait() — block until the operation completes. Returns nil on
        // success; raises a Lua error on failure.
        methods.add_method("wait", |_, s, ()| {
            let st = s
                .inner
                .blocking_lock()
                .take()
                .ok_or_else(|| mlua::Error::RuntimeError("Status already awaited".into()))?;
            cirrus_core::runtime::cirrus_runtime()
                .block_on(st)
                .map_err(|e| mlua::Error::RuntimeError(format!("status: {e:?}")))?;
            Ok(())
        });
        // s:done() — non-blocking: true if no longer pending. Currently
        // Status is single-use; once `wait` consumes it, done()=true.
        methods.add_method("done", |_, s, ()| Ok(s.inner.blocking_lock().is_none()));
    }
}

/// Holder for a `Plan`. Two flavors:
///
/// - `Prebuilt`: a finished `Plan` stream, used by the canned plan
///   factories (`count`, `scan`, …). Single-use; consumed on `RE:run`.
/// - `Coroutine`: a `mlua::Thread` plus its initial-resume args.
///   Bound to a specific `Arc<RunEngine>` only at `RE:run` time so the
///   bridge can call back into engine state (for `coroutine.yield`'s
///   return value).
pub struct LuaPlan {
    pub label: String,
    pub kind: TMutex<Option<LuaPlanKind>>,
}

/// Internal: which flavor of plan this `LuaPlan` carries.
pub enum LuaPlanKind {
    /// Prebuilt finished stream (count/scan/mvr/sleep/null).
    Prebuilt(Plan),
    /// Lua coroutine + initial args. Built into a Plan at `RE:run` so
    /// the bridge can use the engine reference.
    Coroutine {
        lua: Lua,
        thread: mlua::Thread,
        args: Vec<LuaValue>,
    },
}

impl UserData for LuaPlan {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_meta_method("__tostring", |_, p, ()| Ok(format!("Plan({})", p.label)));
        methods.add_method("label", |_, p, ()| Ok(p.label.clone()));
    }
}

/// Single `Msg` value, yielded from a Lua coroutine. Constructed via
/// `msg.*` factories.
#[derive(Clone)]
pub struct LuaMsg(pub Msg);

impl UserData for LuaMsg {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_meta_method("__tostring", |_, m, ()| Ok(format!("Msg({:?})", m.0)));
    }
}

/// Tiny in-process `PausableObj` impl for verifying
/// `RE:register_pausable` plumbing from Lua tests. Counts pause/resume
/// hook invocations atomically.
pub struct LuaPausableCounter {
    name: String,
    paused: std::sync::atomic::AtomicU64,
    resumed: std::sync::atomic::AtomicU64,
}

impl LuaPausableCounter {
    fn new(name: String) -> Self {
        Self {
            name,
            paused: std::sync::atomic::AtomicU64::new(0),
            resumed: std::sync::atomic::AtomicU64::new(0),
        }
    }
}

impl cirrus_core::msg::NamedObj for LuaPausableCounter {
    fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait::async_trait]
impl cirrus_core::msg::PausableObj for LuaPausableCounter {
    async fn pause_dyn(&self) -> Result<(), cirrus_core::error::CirrusError> {
        self.paused
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
    async fn resume_dyn(&self) -> Result<(), cirrus_core::error::CirrusError> {
        self.resumed
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}

/// `RunEngine` wrapper exposed as the `RE` global.
#[derive(Clone)]
pub struct LuaRunEngine {
    pub re: Arc<RunEngine>,
}

impl UserData for LuaRunEngine {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_method("run", |_, this, plan: mlua::AnyUserData| {
            let plan_ud = plan
                .borrow_mut::<LuaPlan>()
                .map_err(mlua::Error::external)?;
            let kind = plan_ud.kind.blocking_lock().take().ok_or_else(|| {
                mlua::Error::RuntimeError("plan was already consumed (Plans are single-use)".into())
            })?;
            let re = this.re.clone();
            let plan = match kind {
                LuaPlanKind::Prebuilt(p) => p,
                LuaPlanKind::Coroutine { lua, thread, args } => {
                    coroutine_to_plan(lua, thread, args, re.clone())
                }
            };
            // Drive the plan to completion on cirrus's own runtime. Lua
            // callbacks run from a sync REPL thread (see main.rs), so
            // `block_on` here is safe.
            let result = cirrus_core::runtime::cirrus_runtime()
                .block_on(re.run_async(plan))
                .map_err(|e| mlua::Error::RuntimeError(format!("plan failed: {e}")))?;
            // Replay any buffered Lua subscriber callbacks emitted
            // from worker threads (monitor pumps, suspender watchers)
            // — running on the REPL thread now that block_on has
            // returned, so mlua's reentrant lock won't deadlock.
            drain_lua_subscriber_buffers();
            Ok(format!(
                "exit_status={} run_uid={}",
                result.exit_status,
                result.run_uid.unwrap_or_else(|| "—".into())
            ))
        });
        methods.add_method("pause", |_, this, deferred: Option<bool>| {
            this.re.pause(deferred.unwrap_or(false));
            Ok(())
        });
        methods.add_method("resume", |_, this, ()| {
            this.re.resume();
            Ok(())
        });
        methods.add_method("abort", |_, this, reason: Option<String>| {
            this.re.abort(reason.unwrap_or_else(|| "user abort".into()));
            Ok(())
        });
        methods.add_method("halt", |_, this, ()| {
            this.re.halt("user halt");
            Ok(())
        });
        methods.add_method("stop", |_, this, ()| {
            this.re.stop();
            Ok(())
        });
        methods.add_method("state", |_, this, ()| Ok(format!("{:?}", this.re.state())));
        methods.add_method("md_get", |_, this, ()| {
            let md = this.re.md();
            let json = serde_json::Value::Object(md.into_iter().collect());
            Ok(serde_json::to_string_pretty(&json).unwrap_or_default())
        });
        methods.add_method("md_set", |_, this, (k, v): (String, LuaValue)| {
            let json = lua_value_to_json(&v).map_err(mlua::Error::external)?;
            this.re.md_set(k, json);
            Ok(())
        });
        methods.add_method("md_remove", |_, this, k: String| {
            this.re.md_remove(&k);
            Ok(())
        });
        methods.add_method("md_replace", |_, this, t: mlua::Table| {
            let mut md = std::collections::HashMap::new();
            for pair in t.pairs::<String, LuaValue>().flatten() {
                md.insert(pair.0, lua_value_to_json(&pair.1)?);
            }
            this.re.md_replace(md);
            Ok(())
        });
        methods.add_method("is_paused", |_, this, ()| Ok(this.re.is_paused()));
        methods.add_method("current_run_uid", |_, this, ()| {
            // current_run_uid is async; block on cirrus runtime — safe
            // from Lua REPL thread.
            let uid = cirrus_core::runtime::cirrus_runtime().block_on(this.re.current_run_uid());
            Ok(uid)
        });
        methods.add_method("set_loop_timeout", |_, this, secs: Option<f64>| {
            this.re
                .set_loop_timeout(secs.map(std::time::Duration::from_secs_f64));
            Ok(())
        });
        methods.add_method("request_pause", |_, this, defer: Option<bool>| {
            this.re.request_pause(defer.unwrap_or(false));
            Ok(())
        });
        methods.add_method("request_suspend", |_, this, reason: Option<String>| {
            this.re
                .request_suspend(reason.unwrap_or_else(|| "user-suspend".into()));
            Ok(())
        });
        methods.add_method("set_record_interruptions", |_, this, on: bool| {
            this.re.set_record_interruptions(on);
            Ok(())
        });
        methods.add_method("record_interruptions_enabled", |_, this, ()| {
            Ok(this.re.record_interruptions_enabled())
        });
        methods.add_method("install_signal_handler", |_, this, ()| {
            this.re.install_signal_handler();
            Ok(())
        });
        methods.add_method("next_suspender_id", |_, this, ()| {
            Ok(this.re.next_suspender_id())
        });
        methods.add_method("clear_preprocessors", |_, this, ()| {
            this.re.clear_preprocessors();
            Ok(())
        });
        methods.add_method("unsubscribe", |_, this, id: u64| {
            this.re.unsubscribe(id);
            Ok(())
        });
        methods.add_method("unregister_command", |_, this, name: String| {
            if name == BRIDGE_ERROR_CMD {
                return Err(mlua::Error::RuntimeError(format!(
                    "{name:?} is reserved by the cirrus Lua bridge for error \
                     propagation; unregistering it would silently swallow Lua \
                     coroutine errors"
                )));
            }
            this.re.unregister_command(&name);
            Ok(())
        });
        // take_msg_result returns the most recent MsgResult side-channel
        // value as a Lua-friendly value. Returns nil when MsgResult::None.
        methods.add_method("take_msg_result", |lua, this, ()| {
            Ok(msg_result_to_lua(lua, this.re.take_msg_result()))
        });

        // -- callback-heavy methods --------------------------------------

        // subscribe(callback, [name]) -> id. callback signature:
        //   function(name, body_json_string) ... end
        // Optional `name` filters by document type ("start" / "stop" /
        // "event" / "descriptor" / "resource" / "datum" / "event_page" /
        // "datum_page" / "stream_resource" / "stream_datum"). Pass
        // `"all"` or omit to match every document. Mirrors bluesky's
        // `RE.subscribe(func, name="all")`.
        //
        // Worker-thread emissions (monitor pumps, suspender watchers)
        // are routed through a buffer and replayed on the REPL thread
        // after `RE:run` returns — see [`make_lua_subscriber_cb`].
        methods.add_method(
            "subscribe",
            |_, this, (cb, name): (mlua::Function, Option<String>)| {
                let dcb = make_lua_subscriber_cb(cb, name);
                Ok(this.re.subscribe(dcb))
            },
        );

        // register_command(name, callback). callback signature:
        //   function(payload_string) ... end
        // The payload is downcast-cloned into a string when possible
        // (callers typically pass String/JSON). Non-string payloads
        // surface as the Debug-formatted Rust value.
        methods.add_method(
            "register_command",
            |_, this, (name, cb): (String, mlua::Function)| {
                if name == BRIDGE_ERROR_CMD {
                    return Err(mlua::Error::RuntimeError(format!(
                        "{name:?} is reserved by the cirrus Lua bridge; \
                         overriding it would silently swallow Lua coroutine errors"
                    )));
                }
                let f = cb;
                let handler: cirrus_engine::CustomCommandHandler =
                    Arc::new(move |payload: &(dyn std::any::Any + Send + Sync)| {
                        let f = f.clone();
                        let txt = if let Some(s) = payload.downcast_ref::<String>() {
                            s.clone()
                        } else if let Some(s) = payload.downcast_ref::<&str>() {
                            (*s).to_string()
                        } else if let Some(j) = payload.downcast_ref::<serde_json::Value>() {
                            j.to_string()
                        } else {
                            "<opaque>".to_string()
                        };
                        Box::pin(async move {
                            f.call::<()>(txt).map_err(|e| {
                                cirrus_core::error::CirrusError::Plan(format!(
                                    "Lua command handler: {e}"
                                ))
                            })
                        })
                    });
                this.re.register_command(name, handler);
                Ok(())
            },
        );

        // set_md_validator(callback). callback receives a table; returns
        // nil for OK or a string error message to reject the run.
        methods.add_method(
            "set_md_validator",
            |lua, this, cb: Option<mlua::Function>| {
                match cb {
                    None => {
                        this.re.set_md_validator(None);
                    }
                    Some(f) => {
                        let lua = lua.clone();
                        let v: cirrus_engine::MdValidator = Arc::new(move |md| {
                            let table = json_md_to_lua_table(&lua, md.clone())?;
                            match f.call::<Option<String>>(table) {
                                Ok(None) => Ok(()),
                                Ok(Some(msg)) => Err(cirrus_core::error::CirrusError::Plan(msg)),
                                Err(e) => Err(cirrus_core::error::CirrusError::Plan(format!(
                                    "Lua md_validator: {e}"
                                ))),
                            }
                        });
                        this.re.set_md_validator(Some(v));
                    }
                }
                Ok(())
            },
        );

        // set_md_normalizer(callback). callback receives a table, returns
        // a (possibly new) table.
        methods.add_method(
            "set_md_normalizer",
            |lua, this, cb: Option<mlua::Function>| {
                match cb {
                    None => this.re.set_md_normalizer(None),
                    Some(f) => {
                        let lua = lua.clone();
                        let n: cirrus_engine::MdNormalizer = Arc::new(move |md| {
                            let table = json_md_to_lua_table(&lua, md.clone())?;
                            match f.call::<mlua::Table>(table) {
                                Ok(t) => lua_table_to_json_md(&t).map_err(|e| {
                                    cirrus_core::error::CirrusError::Plan(format!(
                                        "Lua md_normalizer: {e}"
                                    ))
                                }),
                                Err(e) => Err(cirrus_core::error::CirrusError::Plan(format!(
                                    "Lua md_normalizer: {e}"
                                ))),
                            }
                        });
                        this.re.set_md_normalizer(Some(n));
                    }
                }
                Ok(())
            },
        );

        // set_scan_id_source(callback). callback receives a table, returns
        // an integer.
        methods.add_method(
            "set_scan_id_source",
            |lua, this, cb: Option<mlua::Function>| {
                match cb {
                    None => this.re.set_scan_id_source(None),
                    Some(f) => {
                        let lua = lua.clone();
                        let s: cirrus_engine::ScanIdSource = Arc::new(move |md| {
                            let table = json_md_to_lua_table(&lua, md.clone())?;
                            f.call::<u64>(table).map_err(|e| {
                                cirrus_core::error::CirrusError::Plan(format!(
                                    "Lua scan_id_source: {e}"
                                ))
                            })
                        });
                        this.re.set_scan_id_source(Some(s));
                    }
                }
                Ok(())
            },
        );

        // set_before_plan(callback) — synchronous Lua function, no args.
        methods.add_method("set_before_plan", |_, this, cb: Option<mlua::Function>| {
            match cb {
                None => this.re.set_before_plan(None),
                Some(f) => {
                    let h: cirrus_engine::PlanHook = Arc::new(move || {
                        let _ = f.call::<()>(());
                    });
                    this.re.set_before_plan(Some(h));
                }
            }
            Ok(())
        });
        methods.add_method("set_after_plan", |_, this, cb: Option<mlua::Function>| {
            match cb {
                None => this.re.set_after_plan(None),
                Some(f) => {
                    let h: cirrus_engine::PlanHook = Arc::new(move || {
                        let _ = f.call::<()>(());
                    });
                    this.re.set_after_plan(Some(h));
                }
            }
            Ok(())
        });

        // set_input_handler(callback) — callback(prompt_string) -> string.
        methods.add_method(
            "set_input_handler",
            |_, this, cb: Option<mlua::Function>| {
                match cb {
                    None => this.re.set_input_handler(None),
                    Some(f) => {
                        let h: cirrus_engine::InputHandler = Arc::new(move |prompt| {
                            let f = f.clone();
                            Box::pin(async move {
                                f.call::<String>(prompt).map_err(|e| {
                                    cirrus_core::error::CirrusError::Plan(format!(
                                        "Lua input handler: {e}"
                                    ))
                                })
                            })
                        });
                        this.re.set_input_handler(Some(h));
                    }
                }
                Ok(())
            },
        );

        // register_pausable(device) — device must have the pausable role.
        methods.add_method("register_pausable", |_, this, dev: mlua::AnyUserData| {
            let d = dev.borrow::<LuaDevice>()?;
            let p = d
                .pausable
                .clone()
                .ok_or_else(|| mlua::Error::RuntimeError(format!("{} is not pausable", d.name)))?;
            cirrus_core::runtime::cirrus_runtime().block_on(this.re.register_pausable(p));
            Ok(())
        });
        // unregister_pausable(arg) — accept either a device userdata
        // (in which case its name is used) or a string. Mirrors the
        // register_pausable(device) shape so users don't have to
        // remember the lookup key.
        methods.add_method("unregister_pausable", |_, this, arg: LuaValue| {
            let name = match arg {
                LuaValue::String(s) => s.to_str()?.to_string(),
                LuaValue::UserData(ud) => {
                    let d = ud.borrow::<LuaDevice>().map_err(|_| {
                        mlua::Error::RuntimeError(
                            "unregister_pausable: argument must be a device or a name string"
                                .into(),
                        )
                    })?;
                    d.name.clone()
                }
                _ => {
                    return Err(mlua::Error::RuntimeError(
                        "unregister_pausable: argument must be a device or a name string".into(),
                    ))
                }
            };
            cirrus_core::runtime::cirrus_runtime().block_on(this.re.unregister_pausable(&name));
            Ok(())
        });

        // suspend_until_seconds(secs, [justification]) — convenience for
        // tests/debug. The full `suspend_until(BoxFuture)` API isn't
        // expressible from Lua, so we expose the common "pause then
        // auto-resume after N seconds" form.
        methods.add_method(
            "suspend_until_seconds",
            |_, this, (secs, just): (f64, Option<String>)| {
                let fut = Box::pin(async move {
                    tokio::time::sleep(std::time::Duration::from_secs_f64(secs)).await;
                });
                this.re.suspend_until_with(fut, just);
                Ok(())
            },
        );

        // run_async_with(plan, opts) where opts = {md = {...}, subs = {f1, f2}}.
        methods.add_method(
            "run_async_with",
            |_, this, (plan, opts): (mlua::AnyUserData, mlua::Table)| {
                let plan_ud = plan
                    .borrow_mut::<LuaPlan>()
                    .map_err(mlua::Error::external)?;
                let kind = plan_ud.kind.blocking_lock().take().ok_or_else(|| {
                    mlua::Error::RuntimeError(
                        "plan was already consumed (Plans are single-use)".into(),
                    )
                })?;
                let re = this.re.clone();
                let plan = match kind {
                    LuaPlanKind::Prebuilt(p) => p,
                    LuaPlanKind::Coroutine { lua, thread, args } => {
                        coroutine_to_plan(lua, thread, args, re.clone())
                    }
                };
                let mut md = std::collections::HashMap::new();
                // Validate opts.md type up-front so a typo (e.g.
                // `md = 42`) surfaces as a clear error instead of
                // being silently dropped.
                match opts.get::<LuaValue>("md") {
                    Ok(LuaValue::Nil) => {}
                    Ok(LuaValue::Table(t)) => {
                        for pair in t.pairs::<String, LuaValue>().flatten() {
                            md.insert(pair.0, lua_value_to_json(&pair.1)?);
                        }
                    }
                    Ok(_) => {
                        return Err(mlua::Error::RuntimeError(
                            "run_async_with: opts.md must be a table".into(),
                        ));
                    }
                    Err(e) => return Err(e),
                }
                let mut subs: Vec<cirrus_engine::DocumentCallback> = Vec::new();
                match opts.get::<LuaValue>("subs") {
                    Ok(LuaValue::Nil) => {}
                    Ok(LuaValue::Table(t)) => {
                        for v in t.sequence_values::<mlua::Function>().flatten() {
                            let f = v;
                            let dcb: cirrus_engine::DocumentCallback =
                                Arc::new(move |doc: &cirrus_event_model::Document| {
                                    let (name, body) = document_to_name_body(doc);
                                    if let Err(e) = f.call::<()>((name, body.to_string())) {
                                        tracing::warn!(
                                            "run_async_with subs Lua callback error: {e}"
                                        );
                                    }
                                });
                            subs.push(dcb);
                        }
                    }
                    Ok(_) => {
                        return Err(mlua::Error::RuntimeError(
                            "run_async_with: opts.subs must be a sequence of functions".into(),
                        ));
                    }
                    Err(e) => return Err(e),
                }
                let opts = cirrus_engine::RunOptions { md, subs };
                let result = cirrus_core::runtime::cirrus_runtime()
                    .block_on(re.run_async_with(plan, opts))
                    .map_err(|e| mlua::Error::RuntimeError(format!("plan failed: {e}")))?;
                drain_lua_subscriber_buffers();
                Ok(format!(
                    "exit_status={} run_uid={}",
                    result.exit_status,
                    result.run_uid.unwrap_or_else(|| "—".into())
                ))
            },
        );
    }
}

/// Build a fresh Lua state with cirrus globals registered.
/// Custom-command name used by [`coroutine_to_plan`] to surface a Lua
/// coroutine error as a run failure. The handler downcasts the
/// payload (a `String`) and returns `Err(CirrusError::Plan(msg))` so
/// the engine marks the run `exit_status="fail"`.
const BRIDGE_ERROR_CMD: &str = "_cirrus_lua_bridge_error";

/// REPL thread id, captured by [`build_lua`]. Lua subscribers compare
/// `current().id()` against this to decide whether to call Lua
/// synchronously (same thread → reentrant lock OK) or buffer for
/// post-`RE:run` drain (different thread → deadlock avoidance).
static REPL_THREAD_ID: std::sync::OnceLock<std::thread::ThreadId> = std::sync::OnceLock::new();

/// Per-subscriber state: the Lua callback, an optional document-name
/// filter, and a worker-thread buffer drained after `RE:run` returns.
struct LuaSubscriberInner {
    lua_fn: mlua::Function,
    /// Document-name filter. `None` or `"all"` matches all docs;
    /// otherwise only docs whose name equals this string fire.
    filter: Option<String>,
    /// Buffer for (name, body_json) pairs pushed by worker threads.
    /// Drained after `RE:run` returns.
    buffer: std::sync::Mutex<Vec<(&'static str, String)>>,
}

/// Global registry of Lua subscribers needing post-`RE:run` drain.
/// Entries persist across runs (subscribers added via `RE:subscribe`
/// are persistent); per-run subscribers added via `Msg::Subscribe`
/// are removed by the engine's `temp_subscribers` cleanup, but their
/// buffer entries here remain only while their `Arc` is alive.
static SUBSCRIBER_BUFFERS: std::sync::LazyLock<std::sync::Mutex<Vec<Arc<LuaSubscriberInner>>>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(Vec::new()));

/// Build a `DocumentCallback` for a Lua subscriber. Routes calls based
/// on the current thread:
/// - same as REPL: call the Lua fn synchronously (no deadlock — same
///   thread re-enters mlua's reentrant mutex)
/// - different thread (worker — monitor pumps, suspend tasks): push
///   `(name, body_json)` into the per-subscriber buffer; the next
///   `drain_lua_subscriber_buffers()` (run after `RE:run`'s
///   `block_on` returns) replays the buffered entries on the REPL
///   thread.
fn make_lua_subscriber_cb(
    f: mlua::Function,
    filter: Option<String>,
) -> cirrus_engine::DocumentCallback {
    let inner = Arc::new(LuaSubscriberInner {
        lua_fn: f,
        filter,
        buffer: std::sync::Mutex::new(Vec::new()),
    });
    SUBSCRIBER_BUFFERS.lock().unwrap().push(inner.clone());
    Arc::new(move |doc: &cirrus_event_model::Document| {
        let (name, body) = document_to_name_body(doc);
        if let Some(ref f) = inner.filter {
            if f != name && f != "all" {
                return;
            }
        }
        let on_repl = REPL_THREAD_ID
            .get()
            .copied()
            .map(|id| std::thread::current().id() == id)
            .unwrap_or(false);
        if on_repl {
            // Same thread as REPL — mlua reentrant lock allows the
            // call. Errors are warned (not propagated; subscribers
            // shouldn't fail the run).
            if let Err(e) = inner.lua_fn.call::<()>((name, body.to_string())) {
                tracing::warn!("Lua subscriber callback error: {e}");
            }
        } else {
            inner.buffer.lock().unwrap().push((name, body.to_string()));
        }
    })
}

/// Drain every Lua subscriber's buffered (name, body) entries by
/// calling its Lua fn on the current (REPL) thread. Cleans up dead
/// registry entries (those whose only owner is the registry itself).
fn drain_lua_subscriber_buffers() {
    let snapshot: Vec<_> = SUBSCRIBER_BUFFERS.lock().unwrap().iter().cloned().collect();
    for inner in snapshot {
        let entries: Vec<(&'static str, String)> =
            std::mem::take(&mut *inner.buffer.lock().unwrap());
        for (name, body) in entries {
            if let Err(e) = inner.lua_fn.call::<()>((name, body)) {
                tracing::warn!("Lua subscriber drain callback error: {e}");
            }
        }
    }
    // Reap entries the engine has unsubscribed (Arc strong = 1, only
    // the registry holds it).
    SUBSCRIBER_BUFFERS
        .lock()
        .unwrap()
        .retain(|a| Arc::strong_count(a) > 1);
}

pub fn build_lua(re: Arc<RunEngine>) -> mlua::Result<Lua> {
    // Capture the calling thread as the "REPL thread" — the only
    // thread that's safe to invoke Lua callbacks on while a run is
    // in progress (mlua reentrant lock). Worker threads buffer.
    let _ = REPL_THREAD_ID.set(std::thread::current().id());

    let lua = Lua::new();

    // RE global.
    lua.globals().set("RE", LuaRunEngine { re: re.clone() })?;

    // Install the bridge-error command. Yielded by coroutine_to_plan
    // when the Lua coroutine raises; the engine processes it as a
    // fail-Msg, propagating the failure into RE:run's RunResult.
    let bridge_handler: cirrus_engine::CustomCommandHandler =
        Arc::new(|payload: &(dyn std::any::Any + Send + Sync)| {
            let msg = payload
                .downcast_ref::<String>()
                .cloned()
                .unwrap_or_else(|| "Lua coroutine error (no detail)".into());
            Box::pin(async move { Err(cirrus_core::error::CirrusError::Plan(msg)) })
        });
    re.register_command(BRIDGE_ERROR_CMD, bridge_handler);

    // Device factories.
    let f = lua.create_function(|_, name: String| {
        let det = SoftDetector::new(&name);
        Ok(LuaDevice {
            name,
            readable: Some(det as Arc<dyn ReadableObj>),
            movable: None,
            locatable: None,
            stoppable: None,
            triggerable: None,
            stageable: None,
            monitorable: None,
            flyable: None,
            preparable: None,
            configurable: None,
            collectable: None,
            pausable: None,
        })
    })?;
    lua.globals().set("soft_detector", f)?;

    let f = lua.create_function(|_, (name, init): (String, Option<f64>)| {
        let motor = Arc::new(SoftMotor::new(&name, Some(init.unwrap_or(0.0))));
        Ok(LuaDevice {
            name,
            readable: Some(motor.clone() as Arc<dyn ReadableObj>),
            movable: Some(motor.clone() as Arc<dyn MovableObj>),
            locatable: Some(motor.clone() as Arc<dyn LocatableObj>),
            stoppable: Some(motor.clone() as Arc<dyn StoppableObj>),
            triggerable: None,
            stageable: None,
            monitorable: None,
            flyable: None,
            preparable: None,
            configurable: None,
            collectable: None,
            pausable: None,
        })
    })?;
    lua.globals().set("soft_motor", f)?;

    // Pausable test device — counts pause/resume hook invocations into
    // an internal AtomicU64 pair, exposed to Lua via :pause_count() /
    // :resume_count(). Only useful for validating register_pausable
    // plumbing; not a production device.
    let f = lua.create_function(|_, name: String| {
        let counter = Arc::new(LuaPausableCounter::new(name.clone()));
        PAUSE_COUNTERS
            .lock()
            .unwrap()
            .insert(name.clone(), counter.clone());
        Ok(LuaDevice {
            name,
            readable: None,
            movable: None,
            locatable: None,
            stoppable: None,
            triggerable: None,
            stageable: None,
            monitorable: None,
            flyable: None,
            preparable: None,
            configurable: None,
            collectable: None,
            pausable: Some(counter as Arc<dyn PausableObj>),
        })
    })?;
    lua.globals().set("soft_pausable", f)?;

    // Plan factories. Each returns a `LuaPlan` userdata.
    register_plan_factories(&lua)?;

    Ok(lua)
}

fn register_plan_factories(lua: &Lua) -> mlua::Result<()> {
    // count(detectors_table, num) -> Plan
    let f = lua.create_function(|_, (dets, num): (mlua::Table, usize)| {
        let detectors = dets_table_to_readables(&dets)?;
        let plan = cirrus_plans::count(detectors, num);
        Ok(LuaPlan {
            label: format!("count(n={})", num),
            kind: TMutex::new(Some(LuaPlanKind::Prebuilt(plan))),
        })
    })?;
    lua.globals().set("count", f)?;

    // scan(detectors, motor, start, stop, num) -> Plan
    let f = lua.create_function(
        |_, (dets, motor, start, stop, num): (mlua::Table, mlua::AnyUserData, f64, f64, usize)| {
            let detectors = dets_table_to_readables(&dets)?;
            let m = motor.borrow::<LuaDevice>()?;
            let movable = m
                .movable
                .clone()
                .ok_or_else(|| mlua::Error::RuntimeError(format!("{} is not movable", m.name)))?;
            let readable = m
                .readable
                .clone()
                .ok_or_else(|| mlua::Error::RuntimeError(format!("{} is not readable", m.name)))?;
            let plan = cirrus_plans::scan(detectors, movable, readable, start, stop, num);
            Ok(LuaPlan {
                label: format!("scan(n={})", num),
                kind: TMutex::new(Some(LuaPlanKind::Prebuilt(plan))),
            })
        },
    )?;
    lua.globals().set("scan", f)?;

    // mvr(motor, delta) -> Plan
    let f = lua.create_function(|_, (motor, delta): (mlua::AnyUserData, f64)| {
        let m = motor.borrow::<LuaDevice>()?;
        let loc = m
            .locatable
            .clone()
            .ok_or_else(|| mlua::Error::RuntimeError(format!("{} is not locatable", m.name)))?;
        let plan = cirrus_plans::stubs::mvr(loc, delta);
        Ok(LuaPlan {
            label: format!("mvr({}, {})", m.name, delta),
            kind: TMutex::new(Some(LuaPlanKind::Prebuilt(plan))),
        })
    })?;
    lua.globals().set("mvr", f)?;

    // sleep(seconds) -> Plan
    let f = lua.create_function(|_, secs: f64| {
        let plan = cirrus_plans::stubs::sleep(std::time::Duration::from_secs_f64(secs));
        Ok(LuaPlan {
            label: format!("sleep({secs}s)"),
            kind: TMutex::new(Some(LuaPlanKind::Prebuilt(plan))),
        })
    })?;
    lua.globals().set("sleep", f)?;

    // null() -> Plan (no-op, useful for testing)
    let f = lua.create_function(|_, ()| {
        let plan = cirrus_plans::stubs::null();
        Ok(LuaPlan {
            label: "null".into(),
            kind: TMutex::new(Some(LuaPlanKind::Prebuilt(plan))),
        })
    })?;
    lua.globals().set("null", f)?;

    // print(...) — convenient print, joins args with spaces.
    let f = lua.create_function(|_, args: Variadic<LuaValue>| {
        let parts: Vec<String> = args.iter().map(lua_value_repr).collect();
        println!("{}", parts.join(" "));
        Ok(())
    })?;
    lua.globals().set("print", f)?;

    // msg.* — Msg constructors for use INSIDE coroutine plans.
    register_msg_namespace(lua)?;

    // bp.* / bps.* / bpt.* / bpp.* namespaces — full bluesky surface.
    register_bluesky_namespaces(lua)?;

    // plan(fn, ...) — defer Plan construction until RE:run so the
    // bridge can capture the engine reference (needed to surface return
    // values back to the coroutine).
    let f = lua.create_function(|lua, args: Variadic<LuaValue>| {
        let mut iter = args.into_iter();
        let fn_value = iter
            .next()
            .ok_or_else(|| mlua::Error::RuntimeError("plan(fn, ...) requires a function".into()))?;
        let fn_obj = match fn_value {
            LuaValue::Function(f) => f,
            other => {
                return Err(mlua::Error::RuntimeError(format!(
                    "plan(fn, ...) expected a function, got {other:?}"
                )));
            }
        };
        let rest: Vec<LuaValue> = iter.collect();
        let thread = lua.create_thread(fn_obj)?;
        Ok(LuaPlan {
            label: "coroutine".into(),
            kind: TMutex::new(Some(LuaPlanKind::Coroutine {
                lua: lua.clone(),
                thread,
                args: rest,
            })),
        })
    })?;
    lua.globals().set("plan", f)?;

    Ok(())
}

fn dets_table_to_readables(t: &mlua::Table) -> mlua::Result<Vec<Arc<dyn ReadableObj>>> {
    let mut out = Vec::new();
    for pair in t.clone().sequence_values::<mlua::AnyUserData>() {
        let ud = pair?;
        let dev = ud.borrow::<LuaDevice>()?;
        let r = dev
            .readable
            .clone()
            .ok_or_else(|| mlua::Error::RuntimeError(format!("{} is not readable", dev.name)))?;
        out.push(r);
    }
    Ok(out)
}

fn lua_value_repr(v: &LuaValue) -> String {
    match v {
        LuaValue::Nil => "nil".into(),
        LuaValue::Boolean(b) => b.to_string(),
        LuaValue::Integer(i) => i.to_string(),
        LuaValue::Number(n) => n.to_string(),
        LuaValue::String(s) => s
            .to_str()
            .map(|c| c.to_string())
            .unwrap_or_else(|_| String::new()),
        LuaValue::Table(t) => {
            let mut parts = Vec::new();
            for pair in t.clone().pairs::<LuaValue, LuaValue>().flatten() {
                parts.push(format!(
                    "{}={}",
                    lua_value_repr(&pair.0),
                    lua_value_repr(&pair.1)
                ));
            }
            format!("{{{}}}", parts.join(","))
        }
        LuaValue::UserData(_) => "<userdata>".into(),
        other => format!("{other:?}"),
    }
}

fn lua_value_to_json(v: &LuaValue) -> mlua::Result<serde_json::Value> {
    Ok(match v {
        LuaValue::Nil => serde_json::Value::Null,
        LuaValue::Boolean(b) => serde_json::Value::Bool(*b),
        LuaValue::Integer(i) => serde_json::Value::from(*i),
        LuaValue::Number(n) => serde_json::Value::from(*n),
        LuaValue::String(s) => serde_json::Value::String(s.to_str()?.to_string()),
        LuaValue::Table(t) => {
            // If table is a sequence (1..n), encode as array; else object.
            let len = t.len()?;
            if len > 0 {
                let mut arr = Vec::with_capacity(len as usize);
                for i in 1..=len {
                    let v: LuaValue = t.get(i)?;
                    arr.push(lua_value_to_json(&v)?);
                }
                serde_json::Value::Array(arr)
            } else {
                let mut obj = serde_json::Map::new();
                for pair in t.clone().pairs::<String, LuaValue>().flatten() {
                    obj.insert(pair.0, lua_value_to_json(&pair.1)?);
                }
                serde_json::Value::Object(obj)
            }
        }
        _ => serde_json::Value::String(format!("{v:?}")),
    })
}

/// Build a minimal `DataKey` from a Lua table:
/// `{source = "...", dtype = "number"|"string"|"boolean"|"integer"|"array",
///   shape = {1, 2, 3}, units = "...", precision = 3}`.
fn lua_table_to_data_key(t: &mlua::Table) -> mlua::Result<cirrus_event_model::DataKey> {
    let source: String = t.get("source").unwrap_or_else(|_| "lua".into());
    let dtype_str: String = t.get("dtype").unwrap_or_else(|_| "number".into());
    let dtype = match dtype_str.as_str() {
        "number" => cirrus_event_model::Dtype::Number,
        "string" => cirrus_event_model::Dtype::String,
        "boolean" => cirrus_event_model::Dtype::Boolean,
        "integer" => cirrus_event_model::Dtype::Integer,
        "array" => cirrus_event_model::Dtype::Array,
        other => {
            return Err(mlua::Error::RuntimeError(format!(
                "unknown dtype {other:?} (expected number/string/boolean/integer/array)"
            )))
        }
    };
    let shape: Vec<Option<u64>> = if let Ok(s) = t.get::<mlua::Table>("shape") {
        let mut v = Vec::new();
        for x in s.sequence_values::<i64>().flatten() {
            v.push(if x < 0 { None } else { Some(x as u64) });
        }
        v
    } else {
        Vec::new()
    };
    Ok(cirrus_event_model::DataKey {
        source,
        dtype,
        shape,
        dtype_numpy: t.get::<String>("dtype_numpy").ok(),
        external: None,
        units: t.get::<String>("units").ok(),
        precision: t.get::<i64>("precision").ok(),
        object_name: t.get::<String>("object_name").ok(),
        dims: None,
        limits: None,
    })
}

/// Required-field hints for each publishable Document kind. Used to
/// generate friendly error messages when the Lua-supplied `body` table
/// is missing fields, before serde gives a cryptic message.
///
/// Lists must match `cirrus-event-model` structs: only fields without
/// `#[serde(default)]` (and that aren't `Option<T>` with a default) go
/// here. Verified against `crates/cirrus-event-model/src/documents.rs`.
fn publish_required_fields(kind: &str) -> Option<&'static [&'static str]> {
    match kind {
        // Resource: resource_kwargs has #[serde(default)]; run_start
        // is Option + default. Required = the five strings.
        "resource" => Some(&["uid", "spec", "root", "resource_path", "path_semantics"]),
        // Datum: datum_kwargs has #[serde(default)].
        "datum" => Some(&["datum_id", "resource"]),
        // StreamResource: parameters has #[serde(default)]; run_start
        // is Option + default.
        "stream_resource" => Some(&["uid", "data_key", "mimetype", "uri"]),
        // StreamDatum: all required.
        "stream_datum" => Some(&[
            "uid",
            "stream_resource",
            "descriptor",
            "indices",
            "seq_nums",
        ]),
        // EventPage: all required. Note: there is NO `filled` field
        // on cirrus's EventPage.
        "event_page" => Some(&["uid", "descriptor", "time", "seq_num", "data", "timestamps"]),
        // DatumPage: datum_kwargs has #[serde(default)].
        "datum_page" => Some(&["datum_id", "resource"]),
        _ => None,
    }
}

/// Reconstruct a `Document` variant from a Lua-supplied JSON body. Only
/// the variants useful for `Msg::Publish` from Lua (Resource, Datum,
/// StreamResource, StreamDatum, EventPage, DatumPage) are supported.
/// Pre-validates required fields so the user gets an actionable error
/// instead of serde's cryptic deserialization failure.
fn lua_publish_to_document(
    kind: &str,
    body: serde_json::Value,
) -> Result<cirrus_event_model::Document, String> {
    use cirrus_event_model::Document as D;
    let required = publish_required_fields(kind).ok_or_else(|| {
        format!(
            "unsupported publish kind {kind:?} (use one of: resource, datum, \
             stream_resource, stream_datum, event_page, datum_page)"
        )
    })?;
    if let Some(obj) = body.as_object() {
        let missing: Vec<&str> = required
            .iter()
            .filter(|k| !obj.contains_key(**k))
            .copied()
            .collect();
        if !missing.is_empty() {
            return Err(format!(
                "publish kind={kind:?} body missing required fields: {missing:?} \
                 (required: {required:?})"
            ));
        }
    } else {
        return Err(format!(
            "publish body must be a Lua table (got {kind:?} body of non-object type)"
        ));
    }
    let parsed = match kind {
        "resource" => D::Resource(serde_json::from_value(body).map_err(|e| e.to_string())?),
        "datum" => D::Datum(serde_json::from_value(body).map_err(|e| e.to_string())?),
        "stream_resource" => {
            D::StreamResource(serde_json::from_value(body).map_err(|e| e.to_string())?)
        }
        "stream_datum" => D::StreamDatum(serde_json::from_value(body).map_err(|e| e.to_string())?),
        "event_page" => D::EventPage(serde_json::from_value(body).map_err(|e| e.to_string())?),
        "datum_page" => D::DatumPage(serde_json::from_value(body).map_err(|e| e.to_string())?),
        _ => unreachable!("kind already validated by publish_required_fields"),
    };
    Ok(parsed)
}

/// Helper for `msg.subscribe`: turn a Document into `(name, json_body)`.
fn document_to_name_body(d: &cirrus_event_model::Document) -> (&'static str, serde_json::Value) {
    use cirrus_event_model::Document::*;
    match d {
        Start(s) => (
            "start",
            serde_json::to_value(s).unwrap_or(serde_json::Value::Null),
        ),
        Descriptor(d) => (
            "descriptor",
            serde_json::to_value(d).unwrap_or(serde_json::Value::Null),
        ),
        Event(e) => (
            "event",
            serde_json::to_value(e).unwrap_or(serde_json::Value::Null),
        ),
        EventPage(e) => (
            "event_page",
            serde_json::to_value(e).unwrap_or(serde_json::Value::Null),
        ),
        Resource(r) => (
            "resource",
            serde_json::to_value(r).unwrap_or(serde_json::Value::Null),
        ),
        Datum(d) => (
            "datum",
            serde_json::to_value(d).unwrap_or(serde_json::Value::Null),
        ),
        DatumPage(d) => (
            "datum_page",
            serde_json::to_value(d).unwrap_or(serde_json::Value::Null),
        ),
        StreamResource(r) => (
            "stream_resource",
            serde_json::to_value(r).unwrap_or(serde_json::Value::Null),
        ),
        StreamDatum(d) => (
            "stream_datum",
            serde_json::to_value(d).unwrap_or(serde_json::Value::Null),
        ),
        Stop(s) => (
            "stop",
            serde_json::to_value(s).unwrap_or(serde_json::Value::Null),
        ),
    }
}

/// Convert a `HashMap<String, serde_json::Value>` md dict into a fresh
/// Lua table for callback hand-off, using a captured Lua handle.
fn json_md_to_lua_table(
    lua: &Lua,
    md: std::collections::HashMap<String, serde_json::Value>,
) -> cirrus_core::error::Result<mlua::Table> {
    let table = lua
        .create_table()
        .map_err(|e| cirrus_core::error::CirrusError::Plan(format!("Lua table create: {e}")))?;
    for (k, v) in md {
        let lv = json_to_lua_value(lua, &v)
            .map_err(|e| cirrus_core::error::CirrusError::Plan(format!("Lua convert: {e}")))?;
        table
            .set(k, lv)
            .map_err(|e| cirrus_core::error::CirrusError::Plan(format!("Lua set: {e}")))?;
    }
    Ok(table)
}

/// Convert a Lua table back into a md HashMap.
fn lua_table_to_json_md(
    t: &mlua::Table,
) -> mlua::Result<std::collections::HashMap<String, serde_json::Value>> {
    let mut out = std::collections::HashMap::new();
    for pair in t.pairs::<String, LuaValue>().flatten() {
        out.insert(pair.0, lua_value_to_json(&pair.1)?);
    }
    Ok(out)
}

/// JSON Value -> Lua Value (used for md callbacks). Nested objects
/// become tables, arrays become 1-indexed sequence tables.
fn json_to_lua_value(lua: &Lua, v: &serde_json::Value) -> mlua::Result<LuaValue> {
    Ok(match v {
        serde_json::Value::Null => LuaValue::Nil,
        serde_json::Value::Bool(b) => LuaValue::Boolean(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                LuaValue::Integer(i)
            } else if let Some(f) = n.as_f64() {
                LuaValue::Number(f)
            } else {
                LuaValue::Nil
            }
        }
        serde_json::Value::String(s) => LuaValue::String(lua.create_string(s)?),
        serde_json::Value::Array(a) => {
            let t = lua.create_table()?;
            for (i, x) in a.iter().enumerate() {
                t.set(i + 1, json_to_lua_value(lua, x)?)?;
            }
            LuaValue::Table(t)
        }
        serde_json::Value::Object(o) => {
            let t = lua.create_table()?;
            for (k, x) in o {
                t.set(k.clone(), json_to_lua_value(lua, x)?)?;
            }
            LuaValue::Table(t)
        }
    })
}

/// `_used` is here to silence unused-imports without exposing the full API.
#[allow(dead_code)]
pub fn _used(_d: Arc<dyn DocumentSink>) {}

// ---------------------------------------------------------------------------
// Msg constructors — used inside `plan(fn, ...)` coroutines via `coroutine.yield`.
// ---------------------------------------------------------------------------

fn register_msg_namespace(lua: &Lua) -> mlua::Result<()> {
    let msg = lua.create_table()?;

    // open_run({plan_name=, scan_id=, ...}) -> Msg::OpenRun
    msg.set(
        "open_run",
        lua.create_function(|_, meta: Option<mlua::Table>| {
            let mut m = RunMetadata::default();
            if let Some(t) = meta {
                if let Ok(s) = t.get::<String>("plan_name") {
                    m.plan_name = Some(s);
                }
                if let Ok(n) = t.get::<u64>("scan_id") {
                    m.scan_id = Some(n);
                }
                for pair in t.pairs::<String, LuaValue>().flatten() {
                    if pair.0 != "plan_name" && pair.0 != "scan_id" {
                        m.extra.insert(pair.0, lua_value_to_json(&pair.1)?);
                    }
                }
            }
            Ok(LuaMsg(Msg::OpenRun(m)))
        })?,
    )?;
    // close_run([exit_status, [reason]]) -> Msg::CloseRun
    msg.set(
        "close_run",
        lua.create_function(|_, (es, rs): (Option<String>, Option<String>)| {
            Ok(LuaMsg(Msg::CloseRun {
                exit_status: es.unwrap_or_else(|| "success".into()),
                reason: rs,
            }))
        })?,
    )?;
    // create([stream]) -> Msg::Create
    msg.set(
        "create",
        lua.create_function(|_, name: Option<String>| {
            Ok(LuaMsg(Msg::Create {
                stream_name: name.unwrap_or_else(|| "primary".into()),
            }))
        })?,
    )?;
    msg.set("save", lua.create_function(|_, ()| Ok(LuaMsg(Msg::Save)))?)?;
    msg.set("drop", lua.create_function(|_, ()| Ok(LuaMsg(Msg::Drop)))?)?;
    // read(device) -> Msg::Read
    msg.set(
        "read",
        lua.create_function(|_, dev: mlua::AnyUserData| {
            let d = dev.borrow::<LuaDevice>()?;
            let r = d
                .readable
                .clone()
                .ok_or_else(|| mlua::Error::RuntimeError(format!("{} is not readable", d.name)))?;
            Ok(LuaMsg(Msg::Read(r)))
        })?,
    )?;
    // set(device, value, [group]) -> Msg::Set
    // If `group` is omitted, the bridge auto-generates a unique id so
    // the coroutine receives a wait-able group string back from yield.
    msg.set(
        "set",
        lua.create_function(
            |_, (dev, val, group): (mlua::AnyUserData, f64, Option<String>)| {
                let d = dev.borrow::<LuaDevice>()?;
                let mv = d.movable.clone().ok_or_else(|| {
                    mlua::Error::RuntimeError(format!("{} is not movable", d.name))
                })?;
                Ok(LuaMsg(Msg::Set {
                    obj: mv,
                    value: val,
                    group: Some(group.unwrap_or_else(auto_group)),
                }))
            },
        )?,
    )?;
    // trigger(device, [group])
    msg.set(
        "trigger",
        lua.create_function(|_, (dev, group): (mlua::AnyUserData, Option<String>)| {
            let d = dev.borrow::<LuaDevice>()?;
            let t = d.triggerable.clone().ok_or_else(|| {
                mlua::Error::RuntimeError(format!("{} is not triggerable", d.name))
            })?;
            Ok(LuaMsg(Msg::Trigger {
                obj: t,
                group: Some(group.unwrap_or_else(auto_group)),
            }))
        })?,
    )?;
    // wait(group, [timeout_secs], [error_on_timeout])
    msg.set(
        "wait",
        lua.create_function(
            |_, (group, timeout_secs, err): (String, Option<f64>, Option<bool>)| {
                Ok(LuaMsg(Msg::Wait {
                    group,
                    timeout: timeout_secs.map(std::time::Duration::from_secs_f64),
                    error_on_timeout: err.unwrap_or(true),
                }))
            },
        )?,
    )?;
    // sleep(seconds) — inside-coroutine sleep Msg.
    msg.set(
        "sleep",
        lua.create_function(|_, secs: f64| {
            Ok(LuaMsg(Msg::Sleep(std::time::Duration::from_secs_f64(secs))))
        })?,
    )?;
    msg.set(
        "checkpoint",
        lua.create_function(|_, ()| Ok(LuaMsg(Msg::Checkpoint)))?,
    )?;
    msg.set(
        "clear_checkpoint",
        lua.create_function(|_, ()| Ok(LuaMsg(Msg::ClearCheckpoint)))?,
    )?;
    msg.set(
        "rewindable",
        lua.create_function(|_, b: bool| Ok(LuaMsg(Msg::Rewindable(b))))?,
    )?;
    msg.set(
        "pause",
        lua.create_function(|_, defer: Option<bool>| {
            Ok(LuaMsg(Msg::Pause {
                defer: defer.unwrap_or(false),
            }))
        })?,
    )?;
    msg.set(
        "resume",
        lua.create_function(|_, ()| Ok(LuaMsg(Msg::Resume)))?,
    )?;
    msg.set("null", lua.create_function(|_, ()| Ok(LuaMsg(Msg::Null)))?)?;
    // stage / unstage
    msg.set(
        "stage",
        lua.create_function(|_, dev: mlua::AnyUserData| {
            let d = dev.borrow::<LuaDevice>()?;
            let s = d
                .stageable
                .clone()
                .ok_or_else(|| mlua::Error::RuntimeError(format!("{} is not stageable", d.name)))?;
            Ok(LuaMsg(Msg::Stage(s)))
        })?,
    )?;
    msg.set(
        "unstage",
        lua.create_function(|_, dev: mlua::AnyUserData| {
            let d = dev.borrow::<LuaDevice>()?;
            let s = d
                .stageable
                .clone()
                .ok_or_else(|| mlua::Error::RuntimeError(format!("{} is not stageable", d.name)))?;
            Ok(LuaMsg(Msg::Unstage(s)))
        })?,
    )?;
    // stop_dev(device, [success])
    msg.set(
        "stop_dev",
        lua.create_function(|_, (dev, success): (mlua::AnyUserData, Option<bool>)| {
            let d = dev.borrow::<LuaDevice>()?;
            let s = d
                .stoppable
                .clone()
                .ok_or_else(|| mlua::Error::RuntimeError(format!("{} is not stoppable", d.name)))?;
            Ok(LuaMsg(Msg::Stop {
                obj: s,
                success: success.unwrap_or(true),
            }))
        })?,
    )?;
    // monitor(device, [stream_name])
    msg.set(
        "monitor",
        lua.create_function(|_, (dev, name): (mlua::AnyUserData, Option<String>)| {
            let d = dev.borrow::<LuaDevice>()?;
            let m = d.monitorable.clone().ok_or_else(|| {
                mlua::Error::RuntimeError(format!("{} is not monitorable", d.name))
            })?;
            Ok(LuaMsg(Msg::Monitor { obj: m, name }))
        })?,
    )?;
    msg.set(
        "unmonitor",
        lua.create_function(|_, dev: mlua::AnyUserData| {
            let d = dev.borrow::<LuaDevice>()?;
            let m = d.monitorable.clone().ok_or_else(|| {
                mlua::Error::RuntimeError(format!("{} is not monitorable", d.name))
            })?;
            Ok(LuaMsg(Msg::Unmonitor(m)))
        })?,
    )?;
    // locate(motor) -> Msg::Locate. Coroutine receives a {setpoint, readback}
    // table on the next resume.
    msg.set(
        "locate",
        lua.create_function(|_, dev: mlua::AnyUserData| {
            let d = dev.borrow::<LuaDevice>()?;
            let l = d
                .locatable
                .clone()
                .ok_or_else(|| mlua::Error::RuntimeError(format!("{} is not locatable", d.name)))?;
            Ok(LuaMsg(Msg::Locate(l)))
        })?,
    )?;
    // kickoff(device, [group]) — auto-group if absent.
    msg.set(
        "kickoff",
        lua.create_function(|_, (dev, group): (mlua::AnyUserData, Option<String>)| {
            let d = dev.borrow::<LuaDevice>()?;
            let f = d
                .flyable
                .clone()
                .ok_or_else(|| mlua::Error::RuntimeError(format!("{} is not flyable", d.name)))?;
            Ok(LuaMsg(Msg::Kickoff {
                obj: f,
                group: Some(group.unwrap_or_else(auto_group)),
            }))
        })?,
    )?;
    // complete(device, [group])
    msg.set(
        "complete",
        lua.create_function(|_, (dev, group): (mlua::AnyUserData, Option<String>)| {
            let d = dev.borrow::<LuaDevice>()?;
            let f = d
                .flyable
                .clone()
                .ok_or_else(|| mlua::Error::RuntimeError(format!("{} is not flyable", d.name)))?;
            Ok(LuaMsg(Msg::Complete {
                obj: f,
                group: Some(group.unwrap_or_else(auto_group)),
            }))
        })?,
    )?;
    // prepare(device, value, [group]) — value is any Lua value (number,
    // string, table). Auto-group if not supplied.
    msg.set(
        "prepare",
        lua.create_function(
            |_, (dev, value, group): (mlua::AnyUserData, LuaValue, Option<String>)| {
                let d = dev.borrow::<LuaDevice>()?;
                let p = d.preparable.clone().ok_or_else(|| {
                    mlua::Error::RuntimeError(format!("{} is not preparable", d.name))
                })?;
                Ok(LuaMsg(Msg::Prepare {
                    obj: p,
                    value: lua_value_to_json(&value)?,
                    group: Some(group.unwrap_or_else(auto_group)),
                }))
            },
        )?,
    )?;
    // wait_for(factories_table, [timeout_secs]) — factories is a Lua
    // sequence of functions. Semantics IS NOT bluesky's "produce a
    // future to await" — Lua has no native async. Each factory runs
    // *synchronously* on the engine task thread when the WaitFor Msg
    // is processed; its return value drives the surrogate future:
    //   - return nil  → factory's future resolves Ok(())
    //   - return "<err string>" → resolves Err(CirrusError::Plan(s))
    //   - raise lua error → resolves Err with the formatted error
    // Run order: factories execute in sequence, NOT in parallel.
    // Sufficient for test/debug; for true async waits, port to Rust.
    msg.set(
        "wait_for",
        lua.create_function(|_, (factories, timeout_secs): (mlua::Table, Option<f64>)| {
            let mut fs: Vec<
                Arc<
                    dyn Fn() -> futures::future::BoxFuture<'static, cirrus_core::error::Result<()>>
                        + Send
                        + Sync,
                >,
            > = Vec::new();
            for v in factories.sequence_values::<LuaValue>().flatten() {
                if let LuaValue::Function(f) = v {
                    let owned = f.clone();
                    let f_arc: Arc<
                        dyn Fn() -> futures::future::BoxFuture<
                                'static,
                                cirrus_core::error::Result<()>,
                            > + Send
                            + Sync,
                    > = Arc::new(move || {
                        // Call the Lua factory synchronously each
                        // time the Msg is processed. Lua function is
                        // !Send across threads, so we use blocking
                        // call via mlua's typical pattern — accept
                        // the constraint that wait_for from Lua is
                        // limited to non-blocking factories.
                        let res: mlua::Result<Option<String>> = owned.call(());
                        Box::pin(async move {
                            match res {
                                Ok(None) => Ok(()),
                                Ok(Some(err)) => Err(cirrus_core::error::CirrusError::Plan(err)),
                                Err(e) => Err(cirrus_core::error::CirrusError::Plan(format!(
                                    "wait_for factory: {e}"
                                ))),
                            }
                        })
                    });
                    fs.push(f_arc);
                }
            }
            Ok(LuaMsg(Msg::WaitFor {
                factories: fs,
                timeout: timeout_secs.map(std::time::Duration::from_secs_f64),
            }))
        })?,
    )?;
    // input(prompt) — Msg::Input { prompt }
    msg.set(
        "input",
        lua.create_function(|_, prompt: Option<String>| {
            Ok(LuaMsg(Msg::Input {
                prompt: prompt.unwrap_or_default(),
            }))
        })?,
    )?;
    // re_class()
    msg.set(
        "re_class",
        lua.create_function(|_, ()| Ok(LuaMsg(Msg::ReClass)))?,
    )?;
    // configure(device, args_table) — args_table is {key=value, ...}
    msg.set(
        "configure",
        lua.create_function(|_, (dev, args): (mlua::AnyUserData, mlua::Table)| {
            let d = dev.borrow::<LuaDevice>()?;
            let c = d.configurable.clone().ok_or_else(|| {
                mlua::Error::RuntimeError(format!("{} is not configurable", d.name))
            })?;
            let mut values = std::collections::HashMap::new();
            for pair in args.pairs::<String, LuaValue>().flatten() {
                values.insert(pair.0, lua_value_to_json(&pair.1)?);
            }
            Ok(LuaMsg(Msg::Configure {
                obj: c,
                args: ConfigureArgs { values },
            }))
        })?,
    )?;
    // declare_stream(name, data_keys_table) — data_keys is
    // {field = {source=, dtype="number"|"string"|..., shape={...}}, ...}
    msg.set(
        "declare_stream",
        lua.create_function(|_, (stream_name, keys_t): (String, mlua::Table)| {
            let mut data_keys = std::collections::HashMap::new();
            for pair in keys_t.pairs::<String, mlua::Table>().flatten() {
                let dk = lua_table_to_data_key(&pair.1)?;
                data_keys.insert(pair.0, dk);
            }
            Ok(LuaMsg(Msg::DeclareStream {
                stream_name,
                data_keys,
            }))
        })?,
    )?;
    // collect(device, [stream_name])
    msg.set(
        "collect",
        lua.create_function(|_, (dev, name): (mlua::AnyUserData, Option<String>)| {
            let d = dev.borrow::<LuaDevice>()?;
            let c = d.collectable.clone().ok_or_else(|| {
                mlua::Error::RuntimeError(format!("{} is not collectable", d.name))
            })?;
            Ok(LuaMsg(Msg::Collect {
                obj: c,
                stream_name: name,
            }))
        })?,
    )?;
    // publish(doc_table) — only minimal Document variants are
    // constructible from Lua: Resource, Datum, StreamResource,
    // StreamDatum. Take a `kind` field plus the variant payload as a
    // Lua table; serialize via serde_json round-trip.
    msg.set(
        "publish",
        lua.create_function(|_, t: mlua::Table| {
            let kind: String = t.get("kind")?;
            let body: LuaValue = t.get("body")?;
            let body_json = lua_value_to_json(&body)?;
            let doc = lua_publish_to_document(&kind, body_json)
                .map_err(|e| mlua::Error::RuntimeError(format!("publish: {e}")))?;
            Ok(LuaMsg(Msg::Publish(Box::new(doc))))
        })?,
    )?;
    // subscribe(callback, [name]) — same shape as `RE:subscribe`. The
    // subscription id lands in `MsgResult::SubscriptionId` on yield;
    // the subscription is auto-removed at run end (temp_subscribers).
    // Worker-thread emissions are buffered and replayed after RE:run.
    msg.set(
        "subscribe",
        lua.create_function(|_, (cb, name): (mlua::Function, Option<String>)| {
            let cb_arc: SubscribeCallback = make_lua_subscriber_cb(cb, name);
            Ok(LuaMsg(Msg::Subscribe(cb_arc)))
        })?,
    )?;
    // unsubscribe(id)
    msg.set(
        "unsubscribe",
        lua.create_function(|_, id: u64| Ok(LuaMsg(Msg::Unsubscribe(id))))?,
    )?;
    // register_pausable(device)
    msg.set(
        "register_pausable",
        lua.create_function(|_, dev: mlua::AnyUserData| {
            let d = dev.borrow::<LuaDevice>()?;
            let p = d
                .pausable
                .clone()
                .ok_or_else(|| mlua::Error::RuntimeError(format!("{} is not pausable", d.name)))?;
            Ok(LuaMsg(Msg::RegisterPausable(p)))
        })?,
    )?;
    msg.set(
        "unregister_pausable",
        lua.create_function(|_, dev: mlua::AnyUserData| {
            let d = dev.borrow::<LuaDevice>()?;
            let p = d
                .pausable
                .clone()
                .ok_or_else(|| mlua::Error::RuntimeError(format!("{} is not pausable", d.name)))?;
            Ok(LuaMsg(Msg::UnregisterPausable(p)))
        })?,
    )?;
    // install_suspender — not exposable from Lua without a Lua-defined
    // Suspender impl (Send/Sync trait object). Document the gap; keep
    // remove_suspender(id) only.
    msg.set(
        "remove_suspender",
        lua.create_function(|_, id: u64| Ok(LuaMsg(Msg::RemoveSuspender { id })))?,
    )?;
    lua.globals().set("msg", msg)?;
    Ok(())
}

/// Allocate a unique wait-group id for an auto-grouped Set / Trigger /
/// Kickoff / Complete. Returned to the coroutine via the bridge as the
/// yield's return value, so plans can `coroutine.yield(msg.wait(s))`.
fn auto_group() -> String {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    format!("auto-{n}")
}

// ---------------------------------------------------------------------------
// Lua coroutine ↔ Plan bridge.
//
// Each call to the resulting Plan's `next()` resumes the coroutine
// once. Yielded `LuaMsg` values become `Msg`s; non-`LuaMsg` yields are
// logged and skipped. When the coroutine returns, the Plan ends.
//
// `mlua::Thread` is `Send` because we built the Lua state with the
// `send` feature; no extra synchronization needed inside the stream.
// ---------------------------------------------------------------------------
fn coroutine_to_plan(
    lua: Lua,
    thread: mlua::Thread,
    args: Vec<LuaValue>,
    re: Arc<cirrus_engine::RunEngine>,
) -> Plan {
    plan_box(async_stream::stream! {
        // Drain any stale result from a previous run.
        let _ = re.take_msg_result();

        let mut started = false;
        let mut have_pending_result = false;

        loop {
            let resume_args: Variadic<LuaValue> = if !started {
                started = true;
                Variadic::from_iter(args.iter().cloned())
            } else if have_pending_result {
                have_pending_result = false;
                let result = re.take_msg_result();
                let v = msg_result_to_lua(&lua, result);
                Variadic::from_iter(std::iter::once(v))
            } else {
                Variadic::new()
            };

            let resume_result: mlua::Result<LuaValue> = thread.resume(resume_args);
            match resume_result {
                Ok(v) => {
                    let still_running = thread.status() == ThreadStatus::Resumable;
                    if let LuaValue::UserData(ud) = &v {
                        if let Ok(m) = ud.borrow::<LuaMsg>() {
                            yield m.0.clone();
                            have_pending_result = true;
                            if !still_running {
                                break;
                            }
                            continue;
                        }
                    }
                    if !still_running {
                        break;
                    }
                    tracing::warn!("coroutine plan: yielded a non-Msg value, skipping");
                }
                Err(e) => {
                    let msg = format!("Lua coroutine error: {e}");
                    eprintln!("coroutine plan error: {e}");
                    tracing::error!("coroutine plan: lua error: {e}");
                    // Surface as a run failure: yield the bridge-error
                    // Custom Msg whose handler returns Err. The engine
                    // marks the run exit_status="fail" and RE:run
                    // bubbles that to the caller's stdout.
                    yield Msg::Custom {
                        name: BRIDGE_ERROR_CMD,
                        payload: Box::new(msg),
                    };
                    break;
                }
            }
        }
    })
}

fn msg_result_to_lua(lua: &Lua, r: cirrus_engine::MsgResult) -> LuaValue {
    use cirrus_engine::MsgResult;
    match r {
        MsgResult::None => LuaValue::Nil,
        MsgResult::OpenRun { uid } => lua
            .create_string(&uid)
            .map(LuaValue::String)
            .unwrap_or(LuaValue::Nil),
        MsgResult::Status { group } => lua
            .create_string(&group)
            .map(LuaValue::String)
            .unwrap_or(LuaValue::Nil),
        MsgResult::CloseRun { exit_status } => lua
            .create_string(&exit_status)
            .map(LuaValue::String)
            .unwrap_or(LuaValue::Nil),
        MsgResult::Reading { data } => {
            // Build a Lua table {field = {value=, timestamp=, ...}, ...}
            let t = match lua.create_table() {
                Ok(t) => t,
                Err(_) => return LuaValue::Nil,
            };
            for (k, v) in data {
                let inner = match lua.create_table() {
                    Ok(i) => i,
                    Err(_) => continue,
                };
                let _ = inner.set("value", json_to_lua(lua, &v.value));
                let _ = inner.set("timestamp", v.timestamp);
                if let Some(s) = v.alarm_severity {
                    let _ = inner.set("alarm_severity", s as i64);
                }
                if let Some(m) = v.message {
                    let _ = inner.set("message", m);
                }
                let _ = t.set(k, inner);
            }
            LuaValue::Table(t)
        }
        MsgResult::Location { setpoint, readback } => {
            let t = match lua.create_table() {
                Ok(t) => t,
                Err(_) => return LuaValue::Nil,
            };
            let _ = t.set("setpoint", setpoint);
            let _ = t.set("readback", readback);
            LuaValue::Table(t)
        }
        MsgResult::Input { text } => lua
            .create_string(&text)
            .map(LuaValue::String)
            .unwrap_or(LuaValue::Nil),
        MsgResult::EngineClass { name } => lua
            .create_string(name)
            .map(LuaValue::String)
            .unwrap_or(LuaValue::Nil),
        MsgResult::SubscriptionId { id } => LuaValue::Integer(id as i64),
    }
}

// ---------------------------------------------------------------------------
// bp / bps / bpt / bpp namespaces — full bluesky-compatible Lua surface.
// ---------------------------------------------------------------------------

fn take_inner_plan(ud: &mlua::AnyUserData) -> mlua::Result<Plan> {
    let lp = ud.borrow::<LuaPlan>()?;
    let kind = lp
        .kind
        .blocking_lock()
        .take()
        .ok_or_else(|| mlua::Error::RuntimeError("plan was already consumed".into()))?;
    match kind {
        LuaPlanKind::Prebuilt(p) => Ok(p),
        LuaPlanKind::Coroutine { .. } => Err(mlua::Error::RuntimeError(
            "coroutine plans (built via plan()) can't be wrapped by bpp.* — \
             pass a prebuilt plan (count/scan/...) instead"
                .into(),
        )),
    }
}

fn wrap_prebuilt(label: impl Into<String>, plan: Plan) -> LuaPlan {
    LuaPlan {
        label: label.into(),
        kind: TMutex::new(Some(LuaPlanKind::Prebuilt(plan))),
    }
}

fn dets(t: &mlua::Table) -> mlua::Result<Vec<Arc<dyn ReadableObj>>> {
    dets_table_to_readables(t)
}

fn devs_of(t: &mlua::Table, role: &'static str) -> mlua::Result<Vec<Arc<dyn StageableObj>>> {
    let mut out = Vec::new();
    for v in t.clone().sequence_values::<mlua::AnyUserData>() {
        let ud = v?;
        let d = ud.borrow::<LuaDevice>()?;
        let s = d
            .stageable
            .clone()
            .ok_or_else(|| mlua::Error::RuntimeError(format!("{} is not {}", d.name, role)))?;
        out.push(s);
    }
    Ok(out)
}

fn motors_of(t: &mlua::Table) -> mlua::Result<Vec<Arc<dyn LocatableObj>>> {
    let mut out = Vec::new();
    for v in t.clone().sequence_values::<mlua::AnyUserData>() {
        let ud = v?;
        let d = ud.borrow::<LuaDevice>()?;
        let l = d
            .locatable
            .clone()
            .ok_or_else(|| mlua::Error::RuntimeError(format!("{} is not locatable", d.name)))?;
        out.push(l);
    }
    Ok(out)
}

fn monitors_of(t: &mlua::Table) -> mlua::Result<Vec<Arc<dyn MonitorableObj>>> {
    let mut out = Vec::new();
    for v in t.clone().sequence_values::<mlua::AnyUserData>() {
        let ud = v?;
        let d = ud.borrow::<LuaDevice>()?;
        let m = d
            .monitorable
            .clone()
            .ok_or_else(|| mlua::Error::RuntimeError(format!("{} is not monitorable", d.name)))?;
        out.push(m);
    }
    Ok(out)
}

type MotorMR = (Arc<dyn MovableObj>, Arc<dyn ReadableObj>, String);

fn motor_movable_readable(ud: &mlua::AnyUserData) -> mlua::Result<MotorMR> {
    let d = ud.borrow::<LuaDevice>()?;
    let mv = d
        .movable
        .clone()
        .ok_or_else(|| mlua::Error::RuntimeError(format!("{} is not movable", d.name)))?;
    let rd = d
        .readable
        .clone()
        .ok_or_else(|| mlua::Error::RuntimeError(format!("{} is not readable", d.name)))?;
    Ok((mv, rd, d.name.clone()))
}

fn vec_f64(t: &mlua::Table) -> mlua::Result<Vec<f64>> {
    let mut out = Vec::new();
    for v in t.clone().sequence_values::<f64>() {
        out.push(v?);
    }
    Ok(out)
}

fn nested_vec_f64(t: &mlua::Table) -> mlua::Result<Vec<Vec<f64>>> {
    let mut out = Vec::new();
    for v in t.clone().sequence_values::<mlua::Table>() {
        out.push(vec_f64(&v?)?);
    }
    Ok(out)
}

fn lua_table_to_metadata(t: Option<mlua::Table>) -> mlua::Result<cirrus_core::msg::RunMetadata> {
    let mut m = cirrus_core::msg::RunMetadata::default();
    let Some(t) = t else { return Ok(m) };
    if let Ok(s) = t.get::<String>("plan_name") {
        m.plan_name = Some(s);
    }
    if let Ok(n) = t.get::<u64>("scan_id") {
        m.scan_id = Some(n);
    }
    for pair in t.pairs::<String, LuaValue>().flatten() {
        if pair.0 != "plan_name" && pair.0 != "scan_id" {
            m.extra.insert(pair.0, lua_value_to_json(&pair.1)?);
        }
    }
    Ok(m)
}

fn register_bluesky_namespaces(lua: &Lua) -> mlua::Result<()> {
    register_bp(lua)?;
    register_bps(lua)?;
    register_bpt(lua)?;
    register_bpp(lua)?;
    Ok(())
}

fn register_bp(lua: &Lua) -> mlua::Result<()> {
    let bp = lua.create_table()?;

    bp.set(
        "count",
        lua.create_function(|_, (dt, num): (mlua::Table, usize)| {
            Ok(wrap_prebuilt(
                format!("count(n={num})"),
                cirrus_plans::count(dets(&dt)?, num),
            ))
        })?,
    )?;
    bp.set(
        "count_with_trigger",
        lua.create_function(
            |_, (dets_t, trigs_t, num): (mlua::Table, mlua::Table, usize)| {
                let dets = dets_table_to_readables(&dets_t)?;
                let mut trigs: Vec<Arc<dyn TriggerableObj>> = Vec::new();
                for v in trigs_t.clone().sequence_values::<mlua::AnyUserData>() {
                    let ud = v?;
                    let d = ud.borrow::<LuaDevice>()?;
                    let t = d.triggerable.clone().ok_or_else(|| {
                        mlua::Error::RuntimeError(format!("{} is not triggerable", d.name))
                    })?;
                    trigs.push(t);
                }
                Ok(wrap_prebuilt(
                    format!("count_with_trigger(n={num})"),
                    cirrus_plans::count_with_trigger(dets, trigs, num),
                ))
            },
        )?,
    )?;
    bp.set(
        "scan",
        lua.create_function(
            |_, (dt, mu, start, stop, num): (mlua::Table, mlua::AnyUserData, f64, f64, usize)| {
                let (mv, rd, _) = motor_movable_readable(&mu)?;
                Ok(wrap_prebuilt(
                    format!("scan(n={num})"),
                    cirrus_plans::scan(dets(&dt)?, mv, rd, start, stop, num),
                ))
            },
        )?,
    )?;
    bp.set(
        "list_scan",
        lua.create_function(
            |_, (dt, mu, points_t): (mlua::Table, mlua::AnyUserData, mlua::Table)| {
                let (mv, rd, _) = motor_movable_readable(&mu)?;
                Ok(wrap_prebuilt(
                    "list_scan",
                    cirrus_plans::list_scan(dets(&dt)?, mv, rd, vec_f64(&points_t)?),
                ))
            },
        )?,
    )?;
    bp.set(
        "rel_scan",
        lua.create_function(
            |_, (dt, mu, start, stop, num): (mlua::Table, mlua::AnyUserData, f64, f64, usize)| {
                let d = mu.borrow::<LuaDevice>()?;
                let mv = d.movable.clone().ok_or_else(|| {
                    mlua::Error::RuntimeError(format!("{} is not movable", d.name))
                })?;
                let rd = d.readable.clone().ok_or_else(|| {
                    mlua::Error::RuntimeError(format!("{} is not readable", d.name))
                })?;
                let lo = d.locatable.clone().ok_or_else(|| {
                    mlua::Error::RuntimeError(format!("{} is not locatable", d.name))
                })?;
                // Fetch current readback so rel_scan can compute the
                // absolute window. Lua callbacks run from the sync REPL
                // thread, so block_on is safe here.
                let current = cirrus_core::runtime::cirrus_runtime()
                    .block_on(lo.locate_dyn())
                    .map(|l| l.readback)
                    .unwrap_or(0.0);
                Ok(wrap_prebuilt(
                    format!("rel_scan(n={num})"),
                    cirrus_plans::rel_scan(dets(&dt)?, mv, rd, current, start, stop, num),
                ))
            },
        )?,
    )?;
    bp.set(
        "rel_list_scan",
        lua.create_function(
            |_, (dt, mu, points_t): (mlua::Table, mlua::AnyUserData, mlua::Table)| {
                let d = mu.borrow::<LuaDevice>()?;
                let lo = d.locatable.clone().ok_or_else(|| {
                    mlua::Error::RuntimeError(format!("{} is not locatable", d.name))
                })?;
                let rd = d.readable.clone().ok_or_else(|| {
                    mlua::Error::RuntimeError(format!("{} is not readable", d.name))
                })?;
                Ok(wrap_prebuilt(
                    "rel_list_scan",
                    cirrus_plans::rel_list_scan(dets(&dt)?, lo, rd, vec_f64(&points_t)?),
                ))
            },
        )?,
    )?;
    // grid_scan(detectors, axes_table) where axes_table is a list of
    // tables: { {motor=, start=, stop=, num=}, ... }. The motor must be
    // both Movable and Readable.
    // grid_scan: 2D only in cirrus-plans. Pass {axes={a1, a2}} where each
    // axis is {motor=, start=, stop=, num=}.
    bp.set(
        "grid_scan",
        lua.create_function(|_, (dt, axes_t): (mlua::Table, mlua::Table)| {
            let dets = dets_table_to_readables(&dt)?;
            let (a1, a2) = pair_grid_axes(&axes_t)?;
            Ok(wrap_prebuilt(
                "grid_scan",
                cirrus_plans::grid_scan(
                    dets, a1.0, a1.1, a1.2, a1.3, a1.4, a2.0, a2.1, a2.2, a2.3, a2.4,
                ),
            ))
        })?,
    )?;
    bp.set(
        "rel_grid_scan",
        lua.create_function(|_, (dt, axes_t): (mlua::Table, mlua::Table)| {
            let dets = dets_table_to_readables(&dt)?;
            let (a1, a2) = pair_grid_rel_axes(&axes_t)?;
            Ok(wrap_prebuilt(
                "rel_grid_scan",
                cirrus_plans::rel_grid_scan(
                    dets, a1.0, a1.1, a1.2, a1.3, a1.4, a2.0, a2.1, a2.2, a2.3, a2.4,
                ),
            ))
        })?,
    )?;
    bp.set(
        "list_grid_scan",
        lua.create_function(|_, (dt, axes_t): (mlua::Table, mlua::Table)| {
            let dets = dets_table_to_readables(&dt)?;
            let axes = axes_table_to_list_grid_axes(&axes_t)?;
            Ok(wrap_prebuilt(
                "list_grid_scan",
                cirrus_plans::list_grid_scan(dets, axes),
            ))
        })?,
    )?;
    bp.set(
        "inner_product_scan",
        lua.create_function(|_, (dt, num, axes_t): (mlua::Table, usize, mlua::Table)| {
            let dets = dets_table_to_readables(&dt)?;
            let axes = axes_table_to_inner_product(&axes_t)?;
            Ok(wrap_prebuilt(
                format!("inner_product_scan(n={num})"),
                cirrus_plans::inner_product_scan(dets, num, axes),
            ))
        })?,
    )?;
    bp.set(
        "scan_nd",
        lua.create_function(
            |_, (dt, axes_t, points_t): (mlua::Table, mlua::Table, mlua::Table)| {
                let dets = dets_table_to_readables(&dt)?;
                let axes = axes_table_to_scan_nd(&axes_t)?;
                let points = nested_vec_f64(&points_t)?;
                Ok(wrap_prebuilt(
                    format!("scan_nd(n={})", points.len()),
                    cirrus_plans::scan_nd(dets, axes, points),
                ))
            },
        )?,
    )?;
    // spiral / spiral_square / spiral_fermat — same arg shape.
    bp.set(
        "spiral",
        lua.create_function(
            |_,
             (dt, xm, ym, x_start, y_start, x_range, y_range, dr, nth): (
                mlua::Table,
                mlua::AnyUserData,
                mlua::AnyUserData,
                f64,
                f64,
                f64,
                f64,
                f64,
                usize,
            )| {
                let (xmv, xrd, _) = motor_movable_readable(&xm)?;
                let (ymv, yrd, _) = motor_movable_readable(&ym)?;
                Ok(wrap_prebuilt(
                    "spiral",
                    cirrus_plans::spiral(
                        dets(&dt)?,
                        xmv,
                        xrd,
                        ymv,
                        yrd,
                        x_start,
                        y_start,
                        x_range,
                        y_range,
                        dr,
                        nth,
                    ),
                ))
            },
        )?,
    )?;
    bp.set(
        "spiral_square",
        lua.create_function(
            |_,
             (dt, xm, ym, xc, yc, xr, yr, xn, yn): (
                mlua::Table,
                mlua::AnyUserData,
                mlua::AnyUserData,
                f64,
                f64,
                f64,
                f64,
                usize,
                usize,
            )| {
                let (xmv, xrd, _) = motor_movable_readable(&xm)?;
                let (ymv, yrd, _) = motor_movable_readable(&ym)?;
                Ok(wrap_prebuilt(
                    "spiral_square",
                    cirrus_plans::spiral_square(
                        dets(&dt)?,
                        xmv,
                        xrd,
                        ymv,
                        yrd,
                        xc,
                        yc,
                        xr,
                        yr,
                        xn,
                        yn,
                    ),
                ))
            },
        )?,
    )?;
    bp.set(
        "spiral_fermat",
        lua.create_function(
            |_,
             (dt, xm, ym, x_start, y_start, x_range, y_range, dr, factor): (
                mlua::Table,
                mlua::AnyUserData,
                mlua::AnyUserData,
                f64,
                f64,
                f64,
                f64,
                f64,
                f64,
            )| {
                let (xmv, xrd, _) = motor_movable_readable(&xm)?;
                let (ymv, yrd, _) = motor_movable_readable(&ym)?;
                Ok(wrap_prebuilt(
                    "spiral_fermat",
                    cirrus_plans::spiral_fermat(
                        dets(&dt)?,
                        xmv,
                        xrd,
                        ymv,
                        yrd,
                        x_start,
                        y_start,
                        x_range,
                        y_range,
                        dr,
                        factor,
                    ),
                ))
            },
        )?,
    )?;
    bp.set(
        "ramp_plan",
        lua.create_function(
            |_, (go_plan, dt, period_secs, samples): (mlua::AnyUserData, mlua::Table, f64, usize)| {
                let go = take_inner_plan(&go_plan)?;
                Ok(wrap_prebuilt(
                    format!("ramp_plan(n={samples})"),
                    cirrus_plans::ramp_plan(
                        go,
                        dets(&dt)?,
                        std::time::Duration::from_secs_f64(period_secs),
                        samples,
                    ),
                ))
            },
        )?,
    )?;
    bp.set(
        "log_scan",
        lua.create_function(
            |_, (dt, mu, start, stop, num): (mlua::Table, mlua::AnyUserData, f64, f64, usize)| {
                let (mv, rd, _) = motor_movable_readable(&mu)?;
                Ok(wrap_prebuilt(
                    format!("log_scan(n={num})"),
                    cirrus_plans::log_scan(dets(&dt)?, mv, rd, start, stop, num),
                ))
            },
        )?,
    )?;
    // bp.fly requires a CollectableObj wrapper, which isn't on LuaDevice
    // yet. Soft devices don't impl Flyable/Collectable, so even with the
    // wrapper there'd be nothing to test against. Stub for future EPICS
    // flyer / collector support.
    bp.set(
        "fly",
        lua.create_function(|_, _: mlua::Variadic<LuaValue>| -> mlua::Result<LuaPlan> {
            Err(mlua::Error::RuntimeError(
                "bp.fly is not yet wired through Lua (needs CollectableObj wrapper). \
                 Use a coroutine plan with msg.kickoff/complete/collect instead."
                    .into(),
            ))
        })?,
    )?;

    lua.globals().set("bp", bp)?;
    Ok(())
}

type GridAxisAbs = (Arc<dyn MovableObj>, Arc<dyn ReadableObj>, f64, f64, usize);

type GridAxisRel = (Arc<dyn LocatableObj>, Arc<dyn ReadableObj>, f64, f64, usize);

fn pair_grid_axes(t: &mlua::Table) -> mlua::Result<(GridAxisAbs, GridAxisAbs)> {
    let row1: mlua::Table = t.get(1)?;
    let row2: mlua::Table = t.get(2)?;
    Ok((row_to_grid_axis_abs(&row1)?, row_to_grid_axis_abs(&row2)?))
}

fn pair_grid_rel_axes(t: &mlua::Table) -> mlua::Result<(GridAxisRel, GridAxisRel)> {
    let row1: mlua::Table = t.get(1)?;
    let row2: mlua::Table = t.get(2)?;
    Ok((row_to_grid_axis_rel(&row1)?, row_to_grid_axis_rel(&row2)?))
}

fn row_to_grid_axis_abs(row: &mlua::Table) -> mlua::Result<GridAxisAbs> {
    let mu: mlua::AnyUserData = row.get("motor")?;
    let (mv, rd, _) = motor_movable_readable(&mu)?;
    Ok((mv, rd, row.get("start")?, row.get("stop")?, row.get("num")?))
}

fn row_to_grid_axis_rel(row: &mlua::Table) -> mlua::Result<GridAxisRel> {
    let mu: mlua::AnyUserData = row.get("motor")?;
    let d = mu.borrow::<LuaDevice>()?;
    let lo = d
        .locatable
        .clone()
        .ok_or_else(|| mlua::Error::RuntimeError(format!("{} is not locatable", d.name)))?;
    let rd = d
        .readable
        .clone()
        .ok_or_else(|| mlua::Error::RuntimeError(format!("{} is not readable", d.name)))?;
    Ok((lo, rd, row.get("start")?, row.get("stop")?, row.get("num")?))
}

fn axes_table_to_list_grid_axes(t: &mlua::Table) -> mlua::Result<Vec<cirrus_plans::ListGridAxis>> {
    let mut out = Vec::new();
    for v in t.clone().sequence_values::<mlua::Table>() {
        let row = v?;
        let mu: mlua::AnyUserData = row.get("motor")?;
        let (mv, rd, _) = motor_movable_readable(&mu)?;
        let pts_t: mlua::Table = row.get("points")?;
        out.push((mv, rd, vec_f64(&pts_t)?));
    }
    Ok(out)
}

type InnerProductAxis = (Arc<dyn MovableObj>, Arc<dyn ReadableObj>, f64, f64);

fn axes_table_to_inner_product(t: &mlua::Table) -> mlua::Result<Vec<InnerProductAxis>> {
    let mut out = Vec::new();
    for v in t.clone().sequence_values::<mlua::Table>() {
        let row = v?;
        let mu: mlua::AnyUserData = row.get("motor")?;
        let (mv, rd, _) = motor_movable_readable(&mu)?;
        let start: f64 = row.get("start")?;
        let stop: f64 = row.get("stop")?;
        out.push((mv, rd, start, stop));
    }
    Ok(out)
}

type TrigReadVecs = (Vec<Arc<dyn TriggerableObj>>, Vec<Arc<dyn ReadableObj>>);

fn split_trig_read(t: &mlua::Table) -> mlua::Result<TrigReadVecs> {
    let mut trigs = Vec::new();
    let mut reads = Vec::new();
    for v in t.clone().sequence_values::<mlua::AnyUserData>() {
        let ud = v?;
        let d = ud.borrow::<LuaDevice>()?;
        let t = d
            .triggerable
            .clone()
            .ok_or_else(|| mlua::Error::RuntimeError(format!("{} is not triggerable", d.name)))?;
        let r = d
            .readable
            .clone()
            .ok_or_else(|| mlua::Error::RuntimeError(format!("{} is not readable", d.name)))?;
        trigs.push(t);
        reads.push(r);
    }
    Ok((trigs, reads))
}

type MotorPair = (Arc<dyn MovableObj>, Arc<dyn ReadableObj>);

fn axes_table_to_scan_nd(t: &mlua::Table) -> mlua::Result<Vec<MotorPair>> {
    let mut out = Vec::new();
    for v in t.clone().sequence_values::<mlua::AnyUserData>() {
        let ud = v?;
        let (mv, rd, _) = motor_movable_readable(&ud)?;
        out.push((mv, rd));
    }
    Ok(out)
}

fn register_bps(lua: &Lua) -> mlua::Result<()> {
    let bps = lua.create_table()?;
    use cirrus_plans::stubs;

    bps.set(
        "open_run",
        lua.create_function(|_, md: Option<mlua::Table>| {
            let m = lua_table_to_metadata(md)?;
            Ok(wrap_prebuilt("open_run", stubs::open_run(m)))
        })?,
    )?;
    bps.set(
        "close_run",
        lua.create_function(|_, (es, rs): (Option<String>, Option<String>)| {
            Ok(wrap_prebuilt(
                "close_run",
                stubs::close_run(es.unwrap_or_else(|| "success".into()), rs),
            ))
        })?,
    )?;
    bps.set(
        "create",
        lua.create_function(|_, name: Option<String>| {
            Ok(wrap_prebuilt(
                "create",
                stubs::create(name.unwrap_or_else(|| "primary".into())),
            ))
        })?,
    )?;
    bps.set(
        "save",
        lua.create_function(|_, ()| Ok(wrap_prebuilt("save", stubs::save())))?,
    )?;
    bps.set(
        "drop",
        lua.create_function(|_, ()| Ok(wrap_prebuilt("drop", stubs::drop_bundle())))?,
    )?;
    bps.set(
        "read",
        lua.create_function(|_, dev: mlua::AnyUserData| {
            let d = dev.borrow::<LuaDevice>()?;
            let r = d
                .readable
                .clone()
                .ok_or_else(|| mlua::Error::RuntimeError(format!("{} is not readable", d.name)))?;
            Ok(wrap_prebuilt("read", stubs::read(r)))
        })?,
    )?;
    bps.set(
        "null",
        lua.create_function(|_, ()| Ok(wrap_prebuilt("null", stubs::null())))?,
    )?;
    bps.set(
        "abs_set",
        lua.create_function(
            |_, (mu, v, group): (mlua::AnyUserData, f64, Option<String>)| {
                let d = mu.borrow::<LuaDevice>()?;
                let mv = d.movable.clone().ok_or_else(|| {
                    mlua::Error::RuntimeError(format!("{} is not movable", d.name))
                })?;
                Ok(wrap_prebuilt("abs_set", stubs::abs_set(mv, v, group)))
            },
        )?,
    )?;
    bps.set(
        "mv",
        lua.create_function(|_, (mu, v): (mlua::AnyUserData, f64)| {
            let d = mu.borrow::<LuaDevice>()?;
            let mv = d
                .movable
                .clone()
                .ok_or_else(|| mlua::Error::RuntimeError(format!("{} is not movable", d.name)))?;
            Ok(wrap_prebuilt("mv", stubs::mv(mv, v)))
        })?,
    )?;
    bps.set(
        "mvr",
        lua.create_function(|_, (mu, delta): (mlua::AnyUserData, f64)| {
            let d = mu.borrow::<LuaDevice>()?;
            let lo = d
                .locatable
                .clone()
                .ok_or_else(|| mlua::Error::RuntimeError(format!("{} is not locatable", d.name)))?;
            Ok(wrap_prebuilt("mvr", stubs::mvr(lo, delta)))
        })?,
    )?;
    bps.set(
        "trigger",
        lua.create_function(|_, (dev, group): (mlua::AnyUserData, Option<String>)| {
            let d = dev.borrow::<LuaDevice>()?;
            let t = d.triggerable.clone().ok_or_else(|| {
                mlua::Error::RuntimeError(format!("{} is not triggerable", d.name))
            })?;
            Ok(wrap_prebuilt("trigger", stubs::trigger(t, group)))
        })?,
    )?;
    bps.set(
        "stop_dev",
        lua.create_function(|_, dev: mlua::AnyUserData| {
            let d = dev.borrow::<LuaDevice>()?;
            let s = d
                .stoppable
                .clone()
                .ok_or_else(|| mlua::Error::RuntimeError(format!("{} is not stoppable", d.name)))?;
            Ok(wrap_prebuilt("stop", stubs::stop(s)))
        })?,
    )?;
    bps.set(
        "sleep",
        lua.create_function(|_, secs: f64| {
            Ok(wrap_prebuilt(
                "sleep",
                stubs::sleep(std::time::Duration::from_secs_f64(secs)),
            ))
        })?,
    )?;
    bps.set(
        "wait",
        lua.create_function(|_, (group, timeout): (String, Option<f64>)| {
            Ok(wrap_prebuilt(
                "wait",
                stubs::wait(group, timeout.map(std::time::Duration::from_secs_f64)),
            ))
        })?,
    )?;
    bps.set(
        "checkpoint",
        lua.create_function(|_, ()| Ok(wrap_prebuilt("checkpoint", stubs::checkpoint())))?,
    )?;
    bps.set(
        "clear_checkpoint",
        lua.create_function(|_, ()| {
            Ok(wrap_prebuilt("clear_checkpoint", stubs::clear_checkpoint()))
        })?,
    )?;
    bps.set(
        "pause",
        lua.create_function(|_, ()| Ok(wrap_prebuilt("pause", stubs::pause())))?,
    )?;
    bps.set(
        "deferred_pause",
        lua.create_function(|_, ()| Ok(wrap_prebuilt("deferred_pause", stubs::deferred_pause())))?,
    )?;
    bps.set(
        "resume",
        lua.create_function(|_, ()| Ok(wrap_prebuilt("resume", stubs::resume())))?,
    )?;
    bps.set(
        "kickoff",
        lua.create_function(|_, (dev, group): (mlua::AnyUserData, Option<String>)| {
            let d = dev.borrow::<LuaDevice>()?;
            let f = d
                .flyable
                .clone()
                .ok_or_else(|| mlua::Error::RuntimeError(format!("{} is not flyable", d.name)))?;
            Ok(wrap_prebuilt("kickoff", stubs::kickoff(f, group)))
        })?,
    )?;
    bps.set(
        "complete",
        lua.create_function(|_, (dev, group): (mlua::AnyUserData, Option<String>)| {
            let d = dev.borrow::<LuaDevice>()?;
            let f = d
                .flyable
                .clone()
                .ok_or_else(|| mlua::Error::RuntimeError(format!("{} is not flyable", d.name)))?;
            Ok(wrap_prebuilt("complete", stubs::complete(f, group)))
        })?,
    )?;
    bps.set(
        "stage",
        lua.create_function(|_, dev: mlua::AnyUserData| {
            let d = dev.borrow::<LuaDevice>()?;
            let s = d
                .stageable
                .clone()
                .ok_or_else(|| mlua::Error::RuntimeError(format!("{} is not stageable", d.name)))?;
            Ok(wrap_prebuilt("stage", stubs::stage(s)))
        })?,
    )?;
    bps.set(
        "unstage",
        lua.create_function(|_, dev: mlua::AnyUserData| {
            let d = dev.borrow::<LuaDevice>()?;
            let s = d
                .stageable
                .clone()
                .ok_or_else(|| mlua::Error::RuntimeError(format!("{} is not stageable", d.name)))?;
            Ok(wrap_prebuilt("unstage", stubs::unstage(s)))
        })?,
    )?;
    bps.set(
        "stage_all",
        lua.create_function(|_, t: mlua::Table| {
            Ok(wrap_prebuilt(
                "stage_all",
                stubs::stage_all(devs_of(&t, "stageable")?),
            ))
        })?,
    )?;
    bps.set(
        "unstage_all",
        lua.create_function(|_, t: mlua::Table| {
            Ok(wrap_prebuilt(
                "unstage_all",
                stubs::unstage_all(devs_of(&t, "stageable")?),
            ))
        })?,
    )?;
    bps.set(
        "monitor",
        lua.create_function(|_, (dev, name): (mlua::AnyUserData, Option<String>)| {
            let d = dev.borrow::<LuaDevice>()?;
            let m = d.monitorable.clone().ok_or_else(|| {
                mlua::Error::RuntimeError(format!("{} is not monitorable", d.name))
            })?;
            Ok(wrap_prebuilt("monitor", stubs::monitor(m, name)))
        })?,
    )?;
    bps.set(
        "unmonitor",
        lua.create_function(|_, dev: mlua::AnyUserData| {
            let d = dev.borrow::<LuaDevice>()?;
            let m = d.monitorable.clone().ok_or_else(|| {
                mlua::Error::RuntimeError(format!("{} is not monitorable", d.name))
            })?;
            Ok(wrap_prebuilt("unmonitor", stubs::unmonitor(m)))
        })?,
    )?;
    // bps.trigger_and_read / one_shot: each device must be both
    // Triggerable AND Readable. Soft devices currently aren't
    // Triggerable; this path is for EPICS-backed detectors.
    bps.set(
        "trigger_and_read",
        lua.create_function(|_, (dt, name): (mlua::Table, Option<String>)| {
            let (trigs, reads) = split_trig_read(&dt)?;
            let plan =
                stubs::trigger_and_read(trigs, reads, name.unwrap_or_else(|| "primary".into()));
            Ok(wrap_prebuilt("trigger_and_read", plan))
        })?,
    )?;
    bps.set(
        "one_shot",
        lua.create_function(|_, dt: mlua::Table| {
            let (trigs, reads) = split_trig_read(&dt)?;
            Ok(wrap_prebuilt("one_shot", stubs::one_shot(trigs, reads)))
        })?,
    )?;
    bps.set(
        "repeater",
        lua.create_function(|_, (n, f): (usize, mlua::Function)| {
            let mut plans = Vec::with_capacity(n);
            for i in 0..n {
                let lp_ud: mlua::AnyUserData = f.call(i)?;
                let p = take_inner_plan(&lp_ud)?;
                plans.push(p);
            }
            Ok(wrap_prebuilt(
                format!("repeater(n={n})"),
                cirrus_plans::preprocessors::pchain(plans),
            ))
        })?,
    )?;

    lua.globals().set("bps", bps)?;
    Ok(())
}

fn register_bpt(lua: &Lua) -> mlua::Result<()> {
    let bpt = lua.create_table()?;
    use cirrus_plans::patterns;

    bpt.set(
        "inner_product",
        lua.create_function(|lua, (num, axes_t): (usize, mlua::Table)| {
            let mut axes = Vec::new();
            for v in axes_t.clone().sequence_values::<mlua::Table>() {
                let row = v?;
                let s: f64 = row.get(1)?;
                let e: f64 = row.get(2)?;
                axes.push((s, e));
            }
            let pts = patterns::inner_product(num, &axes);
            nested_f64_to_lua_table(lua, &pts)
        })?,
    )?;
    bpt.set(
        "outer_product",
        lua.create_function(|lua, axes_t: mlua::Table| {
            let mut axes = Vec::new();
            for v in axes_t.clone().sequence_values::<mlua::Table>() {
                let row = v?;
                let s: f64 = row.get(1)?;
                let e: f64 = row.get(2)?;
                let n: usize = row.get(3)?;
                axes.push((s, e, n));
            }
            nested_f64_to_lua_table(lua, &patterns::outer_product(&axes))
        })?,
    )?;
    bpt.set(
        "inner_list_product",
        lua.create_function(|lua, axes_t: mlua::Table| {
            let axes = nested_vec_f64(&axes_t)?;
            nested_f64_to_lua_table(lua, &patterns::inner_list_product(&axes))
        })?,
    )?;
    bpt.set(
        "outer_list_product",
        lua.create_function(|lua, axes_t: mlua::Table| {
            let axes = nested_vec_f64(&axes_t)?;
            nested_f64_to_lua_table(lua, &patterns::outer_list_product(&axes))
        })?,
    )?;
    bpt.set(
        "spiral",
        lua.create_function(
            |lua, (xs, ys, xr, yr, dr, nth): (f64, f64, f64, f64, f64, usize)| {
                let pts = patterns::spiral(xs, ys, xr, yr, dr, nth);
                pairs_to_lua_table(lua, &pts)
            },
        )?,
    )?;
    bpt.set(
        "spiral_square",
        lua.create_function(
            |lua, (xc, yc, xr, yr, xn, yn): (f64, f64, f64, f64, usize, usize)| {
                let pts = patterns::spiral_square_pattern(xc, yc, xr, yr, xn, yn);
                pairs_to_lua_table(lua, &pts)
            },
        )?,
    )?;
    bpt.set(
        "spiral_fermat",
        lua.create_function(
            |lua, (xs, ys, xr, yr, dr, factor): (f64, f64, f64, f64, f64, f64)| {
                let pts = patterns::spiral_fermat_pattern(xs, ys, xr, yr, dr, factor);
                pairs_to_lua_table(lua, &pts)
            },
        )?,
    )?;

    lua.globals().set("bpt", bpt)?;
    Ok(())
}

fn nested_f64_to_lua_table(lua: &Lua, v: &[Vec<f64>]) -> mlua::Result<mlua::Table> {
    let outer = lua.create_table()?;
    for (i, row) in v.iter().enumerate() {
        let inner = lua.create_table()?;
        for (j, x) in row.iter().enumerate() {
            inner.set(j + 1, *x)?;
        }
        outer.set(i + 1, inner)?;
    }
    Ok(outer)
}

fn pairs_to_lua_table(lua: &Lua, v: &[(f64, f64)]) -> mlua::Result<mlua::Table> {
    let outer = lua.create_table()?;
    for (i, (x, y)) in v.iter().enumerate() {
        let inner = lua.create_table()?;
        inner.set(1, *x)?;
        inner.set(2, *y)?;
        outer.set(i + 1, inner)?;
    }
    Ok(outer)
}

fn register_bpp(lua: &Lua) -> mlua::Result<()> {
    let bpp = lua.create_table()?;
    use cirrus_plans::preprocessors as pp;

    bpp.set(
        "run_wrapper",
        lua.create_function(
            |_, (plan_ud, md): (mlua::AnyUserData, Option<mlua::Table>)| {
                let inner = take_inner_plan(&plan_ud)?;
                let m = lua_table_to_metadata(md)?;
                Ok(wrap_prebuilt("run_wrapper", pp::run_wrapper(inner, m)))
            },
        )?,
    )?;
    bpp.set(
        "inject_md",
        lua.create_function(|_, (plan_ud, md_t): (mlua::AnyUserData, mlua::Table)| {
            let inner = take_inner_plan(&plan_ud)?;
            let mut extra = std::collections::HashMap::new();
            for pair in md_t.pairs::<String, LuaValue>().flatten() {
                extra.insert(pair.0, lua_value_to_json(&pair.1)?);
            }
            Ok(wrap_prebuilt(
                "inject_md",
                pp::inject_md_wrapper(inner, extra),
            ))
        })?,
    )?;
    bpp.set(
        "rewindable",
        lua.create_function(|_, (plan_ud, on): (mlua::AnyUserData, bool)| {
            let inner = take_inner_plan(&plan_ud)?;
            Ok(wrap_prebuilt(
                "rewindable_wrapper",
                pp::rewindable_wrapper(inner, on),
            ))
        })?,
    )?;
    bpp.set(
        "monitor_during",
        lua.create_function(
            |_, (plan_ud, signals_t): (mlua::AnyUserData, mlua::Table)| {
                let inner = take_inner_plan(&plan_ud)?;
                Ok(wrap_prebuilt(
                    "monitor_during_wrapper",
                    pp::monitor_during_wrapper(inner, monitors_of(&signals_t)?),
                ))
            },
        )?,
    )?;
    bpp.set(
        "stage_wrapper",
        lua.create_function(|_, (plan_ud, devs_t): (mlua::AnyUserData, mlua::Table)| {
            let inner = take_inner_plan(&plan_ud)?;
            Ok(wrap_prebuilt(
                "stage_wrapper",
                pp::stage_wrapper(inner, devs_of(&devs_t, "stageable")?),
            ))
        })?,
    )?;
    bpp.set(
        "baseline_wrapper",
        lua.create_function(
            |_, (plan_ud, devs_t, name): (mlua::AnyUserData, mlua::Table, Option<String>)| {
                let inner = take_inner_plan(&plan_ud)?;
                let dets = dets_table_to_readables(&devs_t)?;
                Ok(wrap_prebuilt(
                    "baseline_wrapper",
                    pp::baseline_wrapper(inner, dets, name.unwrap_or_else(|| "baseline".into())),
                ))
            },
        )?,
    )?;
    bpp.set(
        "finalize_wrapper",
        lua.create_function(
            |_, (plan_ud, fin_ud): (mlua::AnyUserData, mlua::AnyUserData)| {
                let inner = take_inner_plan(&plan_ud)?;
                let fin = take_inner_plan(&fin_ud)?;
                Ok(wrap_prebuilt(
                    "finalize_wrapper",
                    pp::finalize_wrapper(inner, fin),
                ))
            },
        )?,
    )?;
    bpp.set(
        "subs_wrapper",
        lua.create_function(|_, plan_ud: mlua::AnyUserData| {
            let inner = take_inner_plan(&plan_ud)?;
            Ok(wrap_prebuilt("subs_wrapper", pp::subs_wrapper(inner, ())))
        })?,
    )?;
    bpp.set(
        "relative_set",
        lua.create_function(|_, (plan_ud, motors_t): (mlua::AnyUserData, mlua::Table)| {
            let inner = take_inner_plan(&plan_ud)?;
            Ok(wrap_prebuilt(
                "relative_set_wrapper",
                pp::relative_set_wrapper(inner, motors_of(&motors_t)?),
            ))
        })?,
    )?;
    bpp.set(
        "reset_positions",
        lua.create_function(|_, (plan_ud, motors_t): (mlua::AnyUserData, mlua::Table)| {
            let inner = take_inner_plan(&plan_ud)?;
            Ok(wrap_prebuilt(
                "reset_positions_wrapper",
                pp::reset_positions_wrapper(inner, motors_of(&motors_t)?),
            ))
        })?,
    )?;
    bpp.set(
        "print_summary",
        lua.create_function(|_, plan_ud: mlua::AnyUserData| {
            let inner = take_inner_plan(&plan_ud)?;
            Ok(wrap_prebuilt(
                "print_summary_wrapper",
                pp::print_summary_wrapper(inner),
            ))
        })?,
    )?;
    bpp.set(
        "contingency",
        lua.create_function(
            |_, (plan_ud, fin_ud): (mlua::AnyUserData, mlua::AnyUserData)| {
                let inner = take_inner_plan(&plan_ud)?;
                let fin = take_inner_plan(&fin_ud)?;
                Ok(wrap_prebuilt(
                    "contingency_wrapper",
                    pp::contingency_wrapper(inner, fin),
                ))
            },
        )?,
    )?;
    bpp.set(
        "pchain",
        lua.create_function(|_, plans_t: mlua::Table| {
            let mut plans = Vec::new();
            for v in plans_t.clone().sequence_values::<mlua::AnyUserData>() {
                plans.push(take_inner_plan(&v?)?);
            }
            Ok(wrap_prebuilt("pchain", pp::pchain(plans)))
        })?,
    )?;
    // msg_mutator and plan_mutator: take a Lua function. Bridges Msg
    // userdata across the call boundary.
    bpp.set(
        "msg_mutator",
        lua.create_function(|lua, (plan_ud, f): (mlua::AnyUserData, mlua::Function)| {
            let inner = take_inner_plan(&plan_ud)?;
            let lua_clone = lua.clone();
            let mutated = pp::msg_mutator(inner, move |m: Msg| {
                let ud = match lua_clone.create_userdata(LuaMsg(m.clone())) {
                    Ok(u) => u,
                    Err(_) => return m,
                };
                let result: mlua::Result<mlua::AnyUserData> = f.call(ud);
                match result {
                    Ok(out) => match out.borrow::<LuaMsg>() {
                        Ok(om) => om.0.clone(),
                        Err(_) => m,
                    },
                    Err(e) => {
                        eprintln!("bpp.msg_mutator error: {e}");
                        m
                    }
                }
            });
            Ok(wrap_prebuilt("msg_mutator", mutated))
        })?,
    )?;

    lua.globals().set("bpp", bpp)?;
    Ok(())
}

fn json_to_lua(lua: &Lua, v: &serde_json::Value) -> LuaValue {
    use serde_json::Value as J;
    match v {
        J::Null => LuaValue::Nil,
        J::Bool(b) => LuaValue::Boolean(*b),
        J::Number(n) => {
            if let Some(i) = n.as_i64() {
                LuaValue::Integer(i)
            } else if let Some(f) = n.as_f64() {
                LuaValue::Number(f)
            } else {
                LuaValue::Nil
            }
        }
        J::String(s) => lua
            .create_string(s)
            .map(LuaValue::String)
            .unwrap_or(LuaValue::Nil),
        J::Array(a) => {
            let t = match lua.create_table() {
                Ok(t) => t,
                Err(_) => return LuaValue::Nil,
            };
            for (i, x) in a.iter().enumerate() {
                let _ = t.set(i + 1, json_to_lua(lua, x));
            }
            LuaValue::Table(t)
        }
        J::Object(o) => {
            let t = match lua.create_table() {
                Ok(t) => t,
                Err(_) => return LuaValue::Nil,
            };
            for (k, x) in o {
                let _ = t.set(k.clone(), json_to_lua(lua, x));
            }
            LuaValue::Table(t)
        }
    }
}
