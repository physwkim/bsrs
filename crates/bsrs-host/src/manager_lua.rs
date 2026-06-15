//! Daemon-side Lua bridge — pre-populates an mlua state with the
//! registry's devices and a `RE` global pointing at the daemon's
//! engine, then implements [`bsrs_qs::LuaEvaluator`] so the
//! `lua_eval` RPC can drive it.
//!
//! ## Concurrency
//!
//! One mlua state, shared across all RPC calls (single beamline =
//! single operator pattern). Acquired through a `tokio::sync::Mutex`
//! so concurrent `lua_eval` requests serialize. Plan execution
//! (`RE:run`) runs the engine on the same tokio runtime; the REP
//! socket loop is *not* blocked because `lua_eval` itself returns a
//! task_uid immediately.
//!
//! ## Lifecycle
//!
//! The Lua state is built lazily on the first `eval()` call. We
//! consult `engine_slot` then; if no engine is open, the eval fails
//! with a clear message. Once built, the Lua state persists for the
//! lifetime of the daemon process — globals set by previous evals
//! survive across calls.
//!
//! ## Aborting in-flight evals
//!
//! - **Plans** (`RE:run(...)` from Lua): use the `re_abort` JSON-RPC.
//!   Goes through `bsrs qs re abort` from another terminal. The
//!   engine's abort flag propagates through the `bsrs_runtime`
//!   `block_on` inside Lua at the next checkpoint; the lua_eval task
//!   then transitions to `completed` with `exit_status` other than
//!   `success`. Verified by
//!   `re_abort_cancels_in_flight_lua_eval_plan` in
//!   `bsrs-cli/tests/cli_round_trip.rs`.
//! - **Pure Lua loops** (`while true do ... end` with no Msg yields):
//!   not abortable from the daemon side. The blocking thread is
//!   stuck in mlua. Restart the daemon to recover. Future work could
//!   wire `mlua::HookTriggers` against an atomic abort flag, but that
//!   adds latency to every Lua instruction and is deferred.

use std::sync::{Arc, Mutex as StdMutex};

use async_trait::async_trait;
use bsrs_engine::RunEngine;
use bsrs_qs::{EvalResult, LuaEvaluator, LuaExposedEntry, Registry};
use mlua::{Lua, ObjectLike, Table, Value as LuaValue, Variadic};

use tokio::sync::Mutex as TMutex;

use crate::lua_env::{build_lua, LuaDevice};

/// Daemon-side Lua bridge.
pub struct ManagerLuaState {
    /// Built lazily on first eval (needs an open engine). Held
    /// behind a `std::sync::Mutex` (not tokio's) so we can move the
    /// `Arc` into a `spawn_blocking` task — eval is sync work and
    /// often calls back into `bsrs_runtime().block_on(...)`,
    /// which deadlocks / panics from a tokio async context.
    lua: Arc<StdMutex<Option<Lua>>>,
    /// Engine slot — populated by `environment_open`. We snapshot
    /// the Arc on lazy build.
    engine_slot: Arc<TMutex<Option<Arc<RunEngine>>>>,
    /// Read-only registry view; devices published as Lua globals.
    registry: Arc<Registry>,
}

impl ManagerLuaState {
    /// Build a state bound to the given engine slot + registry.
    /// Building does no I/O; the actual mlua state is constructed on
    /// the first `eval()` call (when the engine is guaranteed open).
    pub fn new(engine_slot: Arc<TMutex<Option<Arc<RunEngine>>>>, registry: Arc<Registry>) -> Self {
        Self {
            lua: Arc::new(StdMutex::new(None)),
            engine_slot,
            registry,
        }
    }

    fn build_state(re: Arc<RunEngine>, registry: &Registry) -> mlua::Result<Lua> {
        let lua = build_lua(re)?;
        // Publish each registered device as a Lua global with its
        // declared name. Walk the role tables; a device that appears
        // under multiple roles (motor: readable + movable) carries
        // both. Roles the registry doesn't currently track
        // (locatable, stoppable, monitorable, ...) are left None —
        // those calls error from Lua.
        for name in registry.device_names() {
            let dev = LuaDevice {
                name: name.clone(),
                readable: registry.readable(&name).cloned(),
                movable: registry.movable(&name).cloned(),
                locatable: registry.locatable(&name).cloned(),
                stoppable: None,
                triggerable: registry.triggerable(&name).cloned(),
                stageable: registry.stageable(&name).cloned(),
                monitorable: None,
                flyable: registry.flyable(&name).cloned(),
                preparable: None,
                configurable: None,
                collectable: registry.collectable(&name).cloned(),
                pausable: None,
            };
            // If the registry has #[lua_methods] for this name, wrap
            // the userdata in a Lua table that adds the custom
            // methods and falls back to the userdata for built-ins.
            if let Some(entry) = registry.lua_exposed(&name) {
                let proxy = make_method_proxy(&lua, dev, entry)?;
                lua.globals().set(name.as_str(), proxy)?;
            } else {
                lua.globals().set(name.as_str(), dev)?;
            }
        }
        Ok(lua)
    }
}

/// Acquire a poison-resistant lock on the shared Lua state. If a
/// prior `eval()` panicked while holding the mutex (e.g. an mlua
/// bug), the standard `lock().unwrap()` would propagate the panic
/// to every subsequent caller. Recover via `into_inner()` — Lua's
/// internal state is robust enough to keep using even after a
/// caller-side panic, and the alternative (denying all future
/// evals) is worse for an attached debug session.
fn lock_recover<T>(m: &StdMutex<T>) -> std::sync::MutexGuard<'_, T> {
    match m.lock() {
        Ok(g) => g,
        Err(poisoned) => {
            tracing::warn!("lua_eval: shared Lua state mutex was poisoned; recovering");
            poisoned.into_inner()
        }
    }
}

#[async_trait]
impl LuaEvaluator for ManagerLuaState {
    async fn eval(&self, source: &str) -> EvalResult {
        // Lazy-build the Lua state if it doesn't exist yet. We need
        // to do this on the async side because we lock the engine
        // slot (a tokio mutex).
        if lock_recover(&self.lua).is_none() {
            let re = match self.engine_slot.lock().await.as_ref() {
                Some(e) => e.clone(),
                None => {
                    return EvalResult {
                        stdout: String::new(),
                        return_value: None,
                        error: Some(
                            "lua_eval: environment not open. Call \
                             environment_open first."
                                .into(),
                        ),
                    };
                }
            };
            match Self::build_state(re, &self.registry) {
                Ok(l) => *lock_recover(&self.lua) = Some(l),
                Err(e) => {
                    return EvalResult {
                        stdout: String::new(),
                        return_value: None,
                        error: Some(format!("lua_eval: build state: {e}")),
                    };
                }
            }
        }

        // Run the sync eval on a blocking thread. Lua callbacks may
        // call `bsrs_runtime().block_on(...)` (RE:run, motor:set,
        // etc.) which would deadlock if executed on a tokio worker.
        let lua = self.lua.clone();
        let src = source.to_string();
        tokio::task::spawn_blocking(move || {
            let mut g = lock_recover(&lua);
            let lua_ref = match g.as_mut() {
                Some(l) => l,
                None => {
                    // Should not happen — the lazy-init above ran in
                    // the async path. If we somehow get here, surface
                    // a clean error rather than panicking.
                    return EvalResult {
                        stdout: String::new(),
                        return_value: None,
                        error: Some("lua_eval: state vanished between init and eval".into()),
                    };
                }
            };
            eval_in(lua_ref, &src)
        })
        .await
        .unwrap_or_else(|e| EvalResult {
            stdout: String::new(),
            return_value: None,
            error: Some(format!("lua_eval task join: {e}")),
        })
    }
}

/// Capture stdout + return value of evaluating `source` in `lua`.
/// Tries `return <source>` first (so bare expressions surface a
/// return_value); falls back to `source` as a statement chunk.
fn eval_in(lua: &Lua, source: &str) -> EvalResult {
    use mlua::Function;
    use std::sync::Mutex as StdMutex;

    let captured: Arc<StdMutex<Vec<String>>> = Arc::new(StdMutex::new(Vec::new()));
    let cap_for_fn = captured.clone();
    let saved_print = lua.globals().get::<Function>("print").ok();
    let new_print = match lua.create_function(move |_, args: mlua::Variadic<mlua::Value>| {
        let mut parts = Vec::with_capacity(args.len());
        for v in args.iter() {
            parts.push(value_to_string(v));
        }
        // Print captures: poison-recover so a prior panic doesn't
        // wedge subsequent evals.
        match cap_for_fn.lock() {
            Ok(mut g) => g.push(parts.join("\t")),
            Err(p) => p.into_inner().push(parts.join("\t")),
        }
        Ok(())
    }) {
        Ok(f) => f,
        Err(e) => {
            return EvalResult {
                stdout: String::new(),
                return_value: None,
                error: Some(format!("create capture print: {e}")),
            };
        }
    };
    if let Err(e) = lua.globals().set("print", new_print) {
        return EvalResult {
            stdout: String::new(),
            return_value: None,
            error: Some(format!("install capture print: {e}")),
        };
    }

    let outcome = run_chunk(lua, source);

    if let Some(p) = saved_print {
        let _ = lua.globals().set("print", p);
    }

    let stdout = match captured.lock() {
        Ok(g) => g.join("\n"),
        Err(p) => p.into_inner().join("\n"),
    };
    match outcome {
        Ok(rv) => EvalResult {
            stdout,
            return_value: rv,
            error: None,
        },
        Err(e) => EvalResult {
            stdout,
            return_value: None,
            error: Some(e),
        },
    }
}

fn run_chunk(lua: &Lua, source: &str) -> Result<Option<String>, String> {
    // First, try `return <source>` so bare expressions yield a value.
    let as_expr = format!("return {source}");
    if let Ok(v) = lua.load(&as_expr).eval::<mlua::Value>() {
        return Ok(Some(value_to_string(&v)));
    }
    // Fall back to running source as a chunk. Use call() so an
    // explicit `return` statement inside the chunk still propagates.
    match lua.load(source).call::<mlua::MultiValue>(()) {
        Ok(mv) => {
            let mut iter = mv.into_iter();
            match iter.next() {
                Some(v) => Ok(Some(value_to_string(&v))),
                None => Ok(None),
            }
        }
        Err(e) => Err(format!("{e}")),
    }
}

/// Wrap a `LuaDevice` userdata in a Lua table that adds each
/// `#[lua_methods]`-exposed method. The `__index` metamethod
/// delegates unknown keys to the underlying userdata so the standard
/// methods (`:read`, `:set`, `:inspect`, ...) keep working.
fn make_method_proxy(lua: &Lua, dev: LuaDevice, entry: &LuaExposedEntry) -> mlua::Result<Table> {
    let t = lua.create_table()?;
    let dev_ud = lua.create_userdata(dev)?;
    t.set("_device", dev_ud.clone())?;

    // Each #[lua_method] becomes a Lua function on the table. The
    // closure captures the device Arc (downcasted via Any in the
    // dispatch fn) and the static method entry.
    for method in entry.methods.iter().copied() {
        let device_arc = entry.device.clone();
        let f = lua.create_function(move |lua, args: Variadic<LuaValue>| {
            // First arg is the table itself (from `dev:method(...)`);
            // skip it. Remaining args are the user-supplied params.
            let mut json_args: Vec<serde_json::Value> = Vec::with_capacity(args.len());
            for v in args.iter().skip(1) {
                json_args.push(lua_value_to_json(v)?);
            }
            let r =
                (method.dispatch)(&*device_arc, &json_args).map_err(mlua::Error::RuntimeError)?;
            json_to_lua_value(lua, &r)
        })?;
        t.set(method.name, f)?;
    }

    // Metatable: missing keys delegate to the userdata, with self
    // re-bound to the userdata so existing add_method handlers see
    // the correct receiver. Wrappers are cached on the proxy table
    // (via raw_set) so a hot loop like `for i=1,1e6 do dx:read() end`
    // doesn't allocate a fresh closure per iteration.
    let meta = lua.create_table()?;
    let dev_ud_for_meta = dev_ud.clone();
    let __index = lua.create_function(move |lua, (proxy, key): (Table, mlua::String)| {
        let key_str = key.to_str()?.to_string();
        // Cache hit: a previous lookup already stored a wrapper.
        let cached: mlua::Value = proxy.raw_get(&*key_str)?;
        if !matches!(cached, mlua::Value::Nil) {
            return Ok(cached);
        }
        // Cache miss: look up on the userdata.
        let val: mlua::Value = dev_ud_for_meta.get(&*key_str).unwrap_or(mlua::Value::Nil);
        if let mlua::Value::Function(f) = val {
            let dev_for_call = dev_ud_for_meta.clone();
            let wrapped = lua.create_function(
                move |_lua, args: Variadic<mlua::Value>| -> mlua::Result<Variadic<mlua::Value>> {
                    let mut new_args: Vec<mlua::Value> = Vec::with_capacity(args.len());
                    new_args.push(mlua::Value::UserData(dev_for_call.clone()));
                    for a in args.into_iter().skip(1) {
                        new_args.push(a);
                    }
                    f.call::<Variadic<mlua::Value>>(Variadic::from_iter(new_args))
                },
            )?;
            // Memoize on the proxy table so subsequent lookups hit
            // direct table indexing (no __index call). raw_set
            // bypasses __newindex.
            proxy.raw_set(&*key_str, wrapped.clone())?;
            Ok(mlua::Value::Function(wrapped))
        } else {
            Ok(val)
        }
    })?;
    meta.set("__index", __index)?;
    t.set_metatable(Some(meta))?;
    Ok(t)
}

fn lua_value_to_json(v: &LuaValue) -> mlua::Result<serde_json::Value> {
    Ok(match v {
        LuaValue::Nil => serde_json::Value::Null,
        LuaValue::Boolean(b) => serde_json::Value::Bool(*b),
        LuaValue::Integer(i) => serde_json::Value::Number((*i).into()),
        LuaValue::Number(n) => serde_json::Number::from_f64(*n)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        LuaValue::String(s) => {
            serde_json::Value::String(s.to_str().map(|s| s.to_string()).unwrap_or_default())
        }
        other => {
            return Err(mlua::Error::RuntimeError(format!(
                "lua_method: unsupported arg type: {other:?}"
            )))
        }
    })
}

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
        serde_json::Value::Array(arr) => {
            let t = lua.create_table()?;
            for (i, x) in arr.iter().enumerate() {
                t.set(i + 1, json_to_lua_value(lua, x)?)?;
            }
            LuaValue::Table(t)
        }
        serde_json::Value::Object(map) => {
            let t = lua.create_table()?;
            for (k, x) in map {
                t.set(k.as_str(), json_to_lua_value(lua, x)?)?;
            }
            LuaValue::Table(t)
        }
    })
}

fn value_to_string(v: &mlua::Value) -> String {
    match v {
        mlua::Value::Nil => "nil".to_string(),
        mlua::Value::Boolean(b) => b.to_string(),
        mlua::Value::Integer(i) => i.to_string(),
        mlua::Value::Number(n) => n.to_string(),
        mlua::Value::String(s) => s.to_str().map(|s| s.to_string()).unwrap_or_default(),
        other => format!("{other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bsrs_engine::RunEngine;

    fn fresh_state() -> (Arc<TMutex<Option<Arc<RunEngine>>>>, ManagerLuaState) {
        let engine_slot = Arc::new(TMutex::new(None));
        let registry = Arc::new(Registry::new());
        let state = ManagerLuaState::new(engine_slot.clone(), registry);
        (engine_slot, state)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn eval_without_environment_errors() {
        let (_slot, state) = fresh_state();
        let r = state.eval("1 + 1").await;
        assert!(r.error.is_some());
        assert!(r.error.as_deref().unwrap().contains("environment not open"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn eval_expression_returns_value() {
        let (slot, state) = fresh_state();
        *slot.lock().await = Some(Arc::new(RunEngine::new(vec![])));
        let r = state.eval("1 + 2").await;
        assert!(r.error.is_none(), "{r:?}");
        assert_eq!(r.return_value.as_deref(), Some("3"));
        assert_eq!(r.stdout, "");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn eval_print_captures_stdout() {
        let (slot, state) = fresh_state();
        *slot.lock().await = Some(Arc::new(RunEngine::new(vec![])));
        let r = state.eval("print(\"hello\"); print(42)").await;
        assert!(r.error.is_none(), "{r:?}");
        assert!(r.return_value.is_none());
        assert_eq!(r.stdout, "hello\n42");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn eval_globals_persist_between_calls() {
        let (slot, state) = fresh_state();
        *slot.lock().await = Some(Arc::new(RunEngine::new(vec![])));
        let _ = state.eval("x = 41").await;
        let r = state.eval("x + 1").await;
        assert_eq!(r.return_value.as_deref(), Some("42"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn eval_lua_error_surfaces() {
        let (slot, state) = fresh_state();
        *slot.lock().await = Some(Arc::new(RunEngine::new(vec![])));
        let r = state.eval("error('boom')").await;
        assert!(r.error.is_some());
        assert!(r.error.as_deref().unwrap().contains("boom"));
    }

    // -- #[lua_methods] proc-macro round-trip -------------------------------

    use std::sync::atomic::{AtomicU64, Ordering};

    pub struct Diffractometer {
        name: String,
        h: AtomicU64,
        k: AtomicU64,
        l: AtomicU64,
    }

    impl bsrs_core::msg::NamedObj for Diffractometer {
        fn name(&self) -> &str {
            &self.name
        }
    }

    #[bsrs_derive::lua_methods]
    impl Diffractometer {
        #[lua_method]
        pub fn set_orientation(&self, h: f64, k: f64, l: f64) -> Result<(), String> {
            self.h.store(h.to_bits(), Ordering::SeqCst);
            self.k.store(k.to_bits(), Ordering::SeqCst);
            self.l.store(l.to_bits(), Ordering::SeqCst);
            Ok(())
        }
        #[lua_method]
        pub fn current_hkl(&self) -> (f64, f64, f64) {
            (
                f64::from_bits(self.h.load(Ordering::SeqCst)),
                f64::from_bits(self.k.load(Ordering::SeqCst)),
                f64::from_bits(self.l.load(Ordering::SeqCst)),
            )
        }
        #[lua_method]
        pub fn label(&self) -> String {
            self.name.clone()
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lua_methods_proc_macro_round_trip() {
        // Build a registry with the custom device + its lua methods.
        let dx = Arc::new(Diffractometer {
            name: "dx".into(),
            h: AtomicU64::new(0_f64.to_bits()),
            k: AtomicU64::new(0_f64.to_bits()),
            l: AtomicU64::new(0_f64.to_bits()),
        });
        let mut reg = Registry::new();
        // Register a NamedObj-only role so device_names() includes it.
        // We piggy-back on the readables map via a dummy ReadableObj
        // would be heavy; just call register_lua_methods and add a
        // synthetic readable via a no-op shim.
        // Simpler: just call register_lua_methods, but then the loop in
        // build_state walks registry.device_names() (union of role
        // tables) and won't see "dx". Add a lua-only registration
        // helper here.
        reg.register_lua_methods("dx", dx.clone());
        // device_names() doesn't index lua_exposed; add a stub
        // readable so the device is published. Production code
        // registers concrete role traits anyway.
        struct StubReadable;
        impl bsrs_core::msg::NamedObj for StubReadable {
            fn name(&self) -> &str {
                "dx"
            }
        }
        #[async_trait::async_trait]
        impl bsrs_core::msg::ReadableObj for StubReadable {
            async fn read_dyn(
                &self,
            ) -> bsrs_core::error::Result<
                std::collections::HashMap<String, bsrs_core::reading::ReadingValue>,
            > {
                Ok(std::collections::HashMap::new())
            }
            async fn describe_dyn(
                &self,
            ) -> bsrs_core::error::Result<
                std::collections::HashMap<String, bsrs_event_model::DataKey>,
            > {
                Ok(std::collections::HashMap::new())
            }
        }
        reg.register_readable(
            "dx",
            Arc::new(StubReadable) as Arc<dyn bsrs_core::msg::ReadableObj>,
        );

        let engine_slot = Arc::new(TMutex::new(Some(Arc::new(RunEngine::new(vec![])))));
        let state = ManagerLuaState::new(engine_slot.clone(), Arc::new(reg));

        // Custom method round-trip.
        let r = state.eval("dx:set_orientation(1.5, 2.5, 3.5)").await;
        assert!(r.error.is_none(), "{r:?}");

        let r = state.eval("dx:current_hkl()").await;
        assert!(r.error.is_none(), "{r:?}");
        // Tuple return → JSON array → Lua table; render as an array.
        // value_to_string falls through to Debug for Table; the test
        // just checks it succeeded — we'll parse parts via a follow-up.
        let r = state
            .eval("local h,k,l = table.unpack(dx:current_hkl()); return h")
            .await;
        assert_eq!(r.return_value.as_deref(), Some("1.5"), "{r:?}");
        let r = state
            .eval("local h,k,l = table.unpack(dx:current_hkl()); return l")
            .await;
        assert_eq!(r.return_value.as_deref(), Some("3.5"), "{r:?}");

        // Standard userdata methods still work via __index fallback
        // (read_dyn is a no-op stub here, but the dispatch path is
        // exercised — name() returns the device name).
        let r = state.eval("dx:name()").await;
        assert_eq!(r.return_value.as_deref(), Some("dx"), "{r:?}");

        // Custom method with String return.
        let r = state.eval("dx:label()").await;
        assert_eq!(r.return_value.as_deref(), Some("dx"));

        // Arity error surfaces as a clean Lua error.
        let r = state.eval("dx:set_orientation(1.0)").await;
        assert!(r.error.is_some());
        assert!(r.error.as_deref().unwrap().contains("expected 3 args"));
    }
}
