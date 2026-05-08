//! Lua environment for the cirrus REPL. Wraps cirrus types and plan
//! factories as `mlua::UserData` and globals.

use std::sync::Arc;

use cirrus_backend_soft::{SoftDetector, SoftMotor};
use cirrus_core::msg::{
    FlyableObj, LocatableObj, MonitorableObj, MovableObj, Msg, ReadableObj, RunMetadata,
    StageableObj, StoppableObj, TriggerableObj,
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
    }
}

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
    }
}

/// Build a fresh Lua state with cirrus globals registered.
pub fn build_lua(re: Arc<RunEngine>) -> mlua::Result<Lua> {
    let lua = Lua::new();

    // RE global.
    lua.globals().set("RE", LuaRunEngine { re: re.clone() })?;

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
        })
    })?;
    lua.globals().set("soft_motor", f)?;

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
                    eprintln!("coroutine plan error: {e}");
                    tracing::error!("coroutine plan: lua error: {e}");
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
