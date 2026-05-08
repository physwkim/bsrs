//! Lua environment for the cirrus REPL. Wraps cirrus types and plan
//! factories as `mlua::UserData` and globals.

use std::sync::Arc;

use cirrus_backend_soft::{SoftDetector, SoftMotor};
use cirrus_core::msg::{
    FlyableObj, LocatableObj, MovableObj, Msg, MonitorableObj, ReadableObj, RunMetadata,
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
            Ok(format!("Device({}, [{}])", dev.name, roles.join(",")))
        });
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
            let plan_ud = plan.borrow_mut::<LuaPlan>().map_err(mlua::Error::external)?;
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
        methods.add_method("state", |_, this, ()| {
            Ok(format!("{:?}", this.re.state()))
        });
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
        |_,
         (dets, motor, start, stop, num): (
            mlua::Table,
            mlua::AnyUserData,
            f64,
            f64,
            usize,
        )| {
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

    // plan(fn, ...) — defer Plan construction until RE:run so the
    // bridge can capture the engine reference (needed to surface return
    // values back to the coroutine).
    let f = lua.create_function(
        |lua, args: Variadic<LuaValue>| {
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
        },
    )?;
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
                parts.push(format!("{}={}", lua_value_repr(&pair.0), lua_value_repr(&pair.1)));
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
                let mv = d
                    .movable
                    .clone()
                    .ok_or_else(|| mlua::Error::RuntimeError(format!("{} is not movable", d.name)))?;
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
            let t = d
                .triggerable
                .clone()
                .ok_or_else(|| mlua::Error::RuntimeError(format!("{} is not triggerable", d.name)))?;
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
            let m = d
                .monitorable
                .clone()
                .ok_or_else(|| mlua::Error::RuntimeError(format!("{} is not monitorable", d.name)))?;
            Ok(LuaMsg(Msg::Monitor { obj: m, name }))
        })?,
    )?;
    msg.set(
        "unmonitor",
        lua.create_function(|_, dev: mlua::AnyUserData| {
            let d = dev.borrow::<LuaDevice>()?;
            let m = d
                .monitorable
                .clone()
                .ok_or_else(|| mlua::Error::RuntimeError(format!("{} is not monitorable", d.name)))?;
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
    }
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
