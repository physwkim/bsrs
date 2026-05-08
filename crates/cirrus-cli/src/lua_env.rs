//! Lua environment for the cirrus REPL. Wraps cirrus types and plan
//! factories as `mlua::UserData` and globals.

use std::sync::Arc;

use cirrus_backend_soft::{SoftDetector, SoftMotor};
use cirrus_core::msg::{LocatableObj, MovableObj, ReadableObj, StoppableObj};
use cirrus_core::plan::Plan;
use cirrus_engine::{DocumentSink, RunEngine};
use mlua::{Lua, UserData, UserDataMethods, Value as LuaValue, Variadic};
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

/// Holder for a built `Plan`. Plans are single-use streams, so the
/// userdata stores `Option<Plan>` and is taken on `RE:run`.
pub struct LuaPlan {
    pub label: String,
    pub plan: TMutex<Option<Plan>>,
}

impl UserData for LuaPlan {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_meta_method("__tostring", |_, p, ()| Ok(format!("Plan({})", p.label)));
        methods.add_method("label", |_, p, ()| Ok(p.label.clone()));
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
            let plan = plan_ud.plan.blocking_lock().take().ok_or_else(|| {
                mlua::Error::RuntimeError("plan was already consumed (Plans are single-use)".into())
            })?;
            let re = this.re.clone();
            // Lua callbacks run on the same tokio worker that drives the
            // REPL. Use block_in_place so block_on can wait without
            // deadlocking that worker.
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
            stoppable: None,
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
            plan: TMutex::new(Some(plan)),
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
                plan: TMutex::new(Some(plan)),
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
            plan: TMutex::new(Some(plan)),
        })
    })?;
    lua.globals().set("mvr", f)?;

    // sleep(seconds) -> Plan
    let f = lua.create_function(|_, secs: f64| {
        let plan = cirrus_plans::stubs::sleep(std::time::Duration::from_secs_f64(secs));
        Ok(LuaPlan {
            label: format!("sleep({secs}s)"),
            plan: TMutex::new(Some(plan)),
        })
    })?;
    lua.globals().set("sleep", f)?;

    // null() -> Plan (no-op, useful for testing)
    let f = lua.create_function(|_, ()| {
        let plan = cirrus_plans::stubs::null();
        Ok(LuaPlan {
            label: "null".into(),
            plan: TMutex::new(Some(plan)),
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
