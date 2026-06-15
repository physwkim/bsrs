//! Plans for cirrus — equivalents of `bluesky.plans` and `bluesky.plan_stubs`.

#![deny(missing_docs)]

pub mod patterns;
pub mod preprocessors;

use cirrus_core::msg::{
    AwaitableFactory, CollectableObj, ConfigurableObj, ConfigureArgs, FlyableObj, LocatableObj,
    MonitorableObj, MovableObj, Msg, PreparableObj, ReadableObj, RunMetadata, StageableObj,
    StoppableObj, TriggerableObj,
};
use cirrus_core::plan::{plan_box, Plan};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Mint a process-unique synchronization-group name carrying a human-readable
/// `label` prefix — cirrus's port of bluesky's `short_uid(label)`
/// (`utils/__init__.py`). A stub that lets the caller supply a sync group but
/// falls back to a default when none is given uses this for the fallback, so
/// the default can never collide with a user-chosen group of the same name.
///
/// bluesky appends a uuid4 fragment for this isolation; cirrus appends a
/// monotonic process-global counter, which is equally unique within a process
/// and needs no extra dependency. The `label-N` shape stays readable in
/// message dumps and tests (match it with `starts_with(label)`).
fn short_uid(label: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    format!("{label}-{}", COUNTER.fetch_add(1, Ordering::Relaxed))
}

// ===========================================================================
//  plan_stubs (single-Msg / small composites; mirrors bluesky.plan_stubs)
// ===========================================================================

/// `bluesky.plan_stubs` equivalents — single- or few-`Msg` helpers that are
/// the building blocks of compound plans.
pub mod stubs {
    use super::*;

    /// Remove redundant (identical) entries from a device list, preserving
    /// first-appearance order. cirrus's port of bluesky's `separate_devices`
    /// (utils/__init__.py:773) for a flat device model: bluesky filters out any
    /// device that has another listed device as an ancestor, and since
    /// `ancestry(obj)` starts with `obj` itself, an exact duplicate is dropped
    /// (`[A, A] -> [A]`). cirrus has no device parent/child hierarchy, so the
    /// only redundancy is an exact duplicate — deduplicated here by `Arc`
    /// identity. Two *distinct* objects that happen to share a name are NOT
    /// merged (bluesky keeps both); they remain a genuine data-key collision
    /// the bundler rejects.
    fn separate_devices<T: ?Sized>(devices: Vec<Arc<T>>) -> Vec<Arc<T>> {
        let mut out: Vec<Arc<T>> = Vec::with_capacity(devices.len());
        for d in devices {
            if !out.iter().any(|e| Arc::ptr_eq(e, &d)) {
                out.push(d);
            }
        }
        out
    }

    /// `open_run(md)` — emit `Msg::OpenRun(md)`.
    pub fn open_run(md: RunMetadata) -> Plan {
        plan_box(async_stream::stream! {
            yield Msg::OpenRun(md);
        })
    }

    /// `close_run(exit_status, reason)` — emit `Msg::CloseRun`.
    pub fn close_run(exit_status: impl Into<String>, reason: Option<String>) -> Plan {
        let exit_status = exit_status.into();
        plan_box(async_stream::stream! {
            yield Msg::CloseRun { exit_status, reason };
        })
    }

    /// `create(stream_name)` — open a new event bundle.
    pub fn create(stream_name: impl Into<String>) -> Plan {
        let stream_name = stream_name.into();
        plan_box(async_stream::stream! {
            yield Msg::Create { stream_name };
        })
    }

    /// `save()` — flush the open bundle as Event documents.
    pub fn save() -> Plan {
        plan_box(async_stream::stream! {
            yield Msg::Save;
        })
    }

    /// `drop()` — discard the open bundle.
    pub fn drop_bundle() -> Plan {
        plan_box(async_stream::stream! {
            yield Msg::Drop;
        })
    }

    /// `declare_stream(name, data_keys)` — pre-declare a stream descriptor.
    pub fn declare_stream(
        stream_name: impl Into<String>,
        data_keys: std::collections::HashMap<String, cirrus_event_model::DataKey>,
    ) -> Plan {
        let stream_name = stream_name.into();
        plan_box(async_stream::stream! {
            yield Msg::DeclareStream { stream_name, data_keys };
        })
    }

    /// `read(obj)` — read all signals on `obj` into the open bundle.
    pub fn read(obj: Arc<dyn ReadableObj>) -> Plan {
        plan_box(async_stream::stream! {
            yield Msg::Read(obj);
        })
    }

    /// `null()` — no-op message.
    pub fn null() -> Plan {
        plan_box(async_stream::stream! {
            yield Msg::Null;
        })
    }

    /// `abs_set(motor, value, group)` — emit `Msg::Set` without waiting.
    pub fn abs_set(motor: Arc<dyn MovableObj>, value: f64, group: Option<String>) -> Plan {
        plan_box(async_stream::stream! {
            yield Msg::Set { obj: motor, value, group };
        })
    }

    /// `mv(motor, value)` — set + wait. Same group lifetime.
    pub fn mv(motor: Arc<dyn MovableObj>, value: f64) -> Plan {
        plan_box(async_stream::stream! {
            yield Msg::Set { obj: motor, value, group: Some("mv".into()) };
            yield Msg::Wait { group: "mv".into(), error_on_timeout: true, timeout: None };
        })
    }

    /// `mvr(motor, delta)` — relative move. The plan reads the current
    /// setpoint (commanded position) via `LocatableObj::locate_dyn` *inside*
    /// the generator, adds `delta`, then yields `Set`+`Wait` for the absolute
    /// target. Bases on the setpoint, not the readback, matching bluesky's
    /// `relative_set_wrapper` (`__read_and_stash_a_motor`).
    /// Motor must implement `LocatableObj` (which extends `MovableObj`).
    pub fn mvr(motor: Arc<dyn LocatableObj>, delta: f64) -> Plan {
        plan_box(async_stream::stream! {
            let loc = match motor.locate_dyn().await {
                Ok(l) => l,
                Err(e) => {
                    // Fail the run cleanly via Msg::Fail rather than
                    // panicking the plan task. The engine's Fail
                    // handler closes the run with exit_status="fail".
                    yield Msg::Fail(format!("mvr({}): locate_dyn failed: {e}", motor.name()));
                    return;
                }
            };
            let target = loc.setpoint + delta;
            let movable: Arc<dyn MovableObj> = motor;
            yield Msg::Set { obj: movable, value: target, group: Some("mv".into()) };
            yield Msg::Wait { group: "mv".into(), error_on_timeout: true, timeout: None };
        })
    }

    /// `rel_set(motor, value, group)` — set relative to the motor's current
    /// setpoint (commanded position), WITHOUT waiting (bluesky
    /// `plan_stubs.rel_set`, default `wait=False`). Reads the setpoint via
    /// `LocatableObj::locate_dyn`, adds `value`, and yields a single `Msg::Set`
    /// to that absolute target under the caller's `group`.
    ///
    /// Differs from `mvr` only by omitting the trailing `Msg::Wait`. Like
    /// `mvr` — and unlike bluesky's `relative_set_wrapper` composition, which
    /// would silently fall back to a zero offset — a `locate_dyn` failure
    /// fails the run via `Msg::Fail` rather than degrading a single explicit
    /// set into an absolute move.
    pub fn rel_set(motor: Arc<dyn LocatableObj>, value: f64, group: Option<String>) -> Plan {
        plan_box(async_stream::stream! {
            let loc = match motor.locate_dyn().await {
                Ok(l) => l,
                Err(e) => {
                    yield Msg::Fail(format!("rel_set({}): locate_dyn failed: {e}", motor.name()));
                    return;
                }
            };
            let target = loc.setpoint + value;
            let movable: Arc<dyn MovableObj> = motor;
            yield Msg::Set { obj: movable, value: target, group };
        })
    }

    /// `trigger(obj, group)`.
    pub fn trigger(obj: Arc<dyn TriggerableObj>, group: Option<String>) -> Plan {
        plan_box(async_stream::stream! {
            yield Msg::Trigger { obj, group };
        })
    }

    /// `stop(obj)` — yield `Msg::Stop` so the engine calls
    /// `StoppableObj::stop_dyn(success=true)` on the device.
    pub fn stop(obj: Arc<dyn StoppableObj>) -> Plan {
        plan_box(async_stream::stream! {
            yield Msg::Stop { obj, success: true };
        })
    }

    /// Like `stop` but signals an emergency stop (`success=false`).
    pub fn stop_emergency(obj: Arc<dyn StoppableObj>) -> Plan {
        plan_box(async_stream::stream! {
            yield Msg::Stop { obj, success: false };
        })
    }

    /// `sleep(d)`.
    pub fn sleep(d: Duration) -> Plan {
        plan_box(async_stream::stream! {
            yield Msg::Sleep(d);
        })
    }

    /// `wait(group, timeout)`.
    pub fn wait(group: impl Into<String>, timeout: Option<Duration>) -> Plan {
        let group = group.into();
        plan_box(async_stream::stream! {
            yield Msg::Wait { group, error_on_timeout: true, timeout };
        })
    }

    /// `wait_for(factories, timeout)` — emit `Msg::WaitFor`. The cirrus
    /// equivalent of bluesky's `wait_for`: each factory produces a fresh future
    /// and the engine starts them all up front, awaiting them *concurrently*
    /// (bluesky's `[ensure_future(f()) for f in futs]` + `asyncio.wait`). An
    /// optional `timeout` bounds the single concurrent wait, after which the
    /// engine returns `CirrusError::Timeout`. Unlike [`wait`], which waits on a
    /// status group, this waits on arbitrary awaitables supplied by the plan.
    pub fn wait_for(factories: Vec<AwaitableFactory>, timeout: Option<Duration>) -> Plan {
        plan_box(async_stream::stream! {
            yield Msg::WaitFor { factories, timeout };
        })
    }

    /// `checkpoint()`.
    pub fn checkpoint() -> Plan {
        plan_box(async_stream::stream! {
            yield Msg::Checkpoint;
        })
    }

    /// `clear_checkpoint()`.
    pub fn clear_checkpoint() -> Plan {
        plan_box(async_stream::stream! {
            yield Msg::ClearCheckpoint;
        })
    }

    /// `pause()` — request immediate pause.
    pub fn pause() -> Plan {
        plan_box(async_stream::stream! {
            yield Msg::Pause { defer: false };
        })
    }

    /// `deferred_pause()` — pause at next checkpoint.
    pub fn deferred_pause() -> Plan {
        plan_box(async_stream::stream! {
            yield Msg::Pause { defer: true };
        })
    }

    /// `resume()` — opposite of pause (typically issued by external control,
    /// not by plans, but provided for parity).
    pub fn resume() -> Plan {
        plan_box(async_stream::stream! {
            yield Msg::Resume;
        })
    }

    /// `prepare(obj, value, group, wait)` — emit `Msg::Prepare` to set up a
    /// `Preparable` device (flyer, detector) for a step or fly scan. Mirrors
    /// bluesky `plan_stubs.prepare`: the resulting `Status` joins `group`, and
    /// when `wait` is true the plan blocks on that group before continuing.
    ///
    /// bluesky mints a fresh uuid for `group` when none is given so the Status
    /// can always be waited on; cirrus-plans carries no uuid dependency, so a
    /// requested wait without an explicit group falls back to the literal
    /// `"prepare"` (as [`kickoff_all`]/[`complete_all`] do). Without a wait the
    /// caller's `group` passes through untouched (may be `None`).
    pub fn prepare(
        obj: Arc<dyn PreparableObj>,
        value: serde_json::Value,
        group: Option<String>,
        wait: bool,
    ) -> Plan {
        plan_box(async_stream::stream! {
            if wait {
                let group = group.unwrap_or_else(|| short_uid("prepare"));
                yield Msg::Prepare { obj, value, group: Some(group.clone()) };
                yield Msg::Wait { group, error_on_timeout: true, timeout: None };
            } else {
                yield Msg::Prepare { obj, value, group };
            }
        })
    }

    /// `kickoff(flyer, group)`.
    pub fn kickoff(flyer: Arc<dyn FlyableObj>, group: Option<String>) -> Plan {
        plan_box(async_stream::stream! {
            yield Msg::Kickoff { obj: flyer, group };
        })
    }

    /// `complete(flyer, group)`.
    pub fn complete(flyer: Arc<dyn FlyableObj>, group: Option<String>) -> Plan {
        plan_box(async_stream::stream! {
            yield Msg::Complete { obj: flyer, group };
        })
    }

    /// `kickoff_all(flyers, group, wait)` — kickoff every flyer under one
    /// shared group, then optionally `Msg::Wait` on that group. Mirrors
    /// bluesky `plan_stubs.kickoff_all`, where `wait` defaults to **true**.
    ///
    /// `group` of `None` mints a process-unique default via [`short_uid`]
    /// (`"kickoff_all-N"`), mirroring bluesky minting a fresh uuid here so the
    /// default cannot collide with a user group — pass an explicit group to
    /// share/await a known one across concurrent kickoffs.
    pub fn kickoff_all(
        flyers: Vec<Arc<dyn FlyableObj>>,
        group: Option<String>,
        wait: bool,
    ) -> Plan {
        let group = group.unwrap_or_else(|| short_uid("kickoff_all"));
        plan_box(async_stream::stream! {
            for f in flyers {
                yield Msg::Kickoff { obj: f, group: Some(group.clone()) };
            }
            if wait {
                yield Msg::Wait { group, error_on_timeout: true, timeout: None };
            }
        })
    }

    /// `complete_all(flyers, group, wait)` — tell every flyer to stop
    /// collecting under one shared group, then optionally `Msg::Wait` on it.
    /// Mirrors bluesky `plan_stubs.complete_all`, where `wait` defaults to
    /// **false** (note: opposite of [`kickoff_all`]).
    ///
    /// `group` of `None` mints a process-unique default via [`short_uid`]
    /// (`"complete_all-N"`); pass an explicit group when a later `wait` must
    /// name it (the `wait=false` default leaves the group outstanding).
    pub fn complete_all(
        flyers: Vec<Arc<dyn FlyableObj>>,
        group: Option<String>,
        wait: bool,
    ) -> Plan {
        let group = group.unwrap_or_else(|| short_uid("complete_all"));
        plan_box(async_stream::stream! {
            for f in flyers {
                yield Msg::Complete { obj: f, group: Some(group.clone()) };
            }
            if wait {
                yield Msg::Wait { group, error_on_timeout: true, timeout: None };
            }
        })
    }

    /// `collect(obj, stream_name)`.
    pub fn collect(obj: Arc<dyn CollectableObj>, stream_name: Option<String>) -> Plan {
        plan_box(async_stream::stream! {
            yield Msg::Collect { obj, stream_name };
        })
    }

    /// `stage(obj)`.
    pub fn stage(obj: Arc<dyn StageableObj>) -> Plan {
        plan_box(async_stream::stream! {
            yield Msg::Stage(obj);
        })
    }

    /// `unstage(obj)`.
    pub fn unstage(obj: Arc<dyn StageableObj>) -> Plan {
        plan_box(async_stream::stream! {
            yield Msg::Unstage(obj);
        })
    }

    /// `stage_all(objs)` — stage each in order.
    pub fn stage_all(objs: Vec<Arc<dyn StageableObj>>) -> Plan {
        plan_box(async_stream::stream! {
            for o in objs { yield Msg::Stage(o); }
        })
    }

    /// `unstage_all(objs)` — unstage each in *reverse* order (LIFO).
    pub fn unstage_all(objs: Vec<Arc<dyn StageableObj>>) -> Plan {
        plan_box(async_stream::stream! {
            for o in objs.into_iter().rev() { yield Msg::Unstage(o); }
        })
    }

    /// `configure(obj, args)`.
    pub fn configure(obj: Arc<dyn ConfigurableObj>, args: ConfigureArgs) -> Plan {
        plan_box(async_stream::stream! {
            yield Msg::Configure { obj, args };
        })
    }

    /// `monitor(obj, name)`.
    pub fn monitor(obj: Arc<dyn MonitorableObj>, name: Option<String>) -> Plan {
        plan_box(async_stream::stream! {
            yield Msg::Monitor { obj, name };
        })
    }

    /// `unmonitor(obj)`.
    pub fn unmonitor(obj: Arc<dyn MonitorableObj>) -> Plan {
        plan_box(async_stream::stream! {
            yield Msg::Unmonitor(obj);
        })
    }

    /// `trigger_and_read(devices, name)` — bluesky's most common building
    /// block. Trigger every device, wait, then create + read each + save.
    pub fn trigger_and_read(
        triggerables: Vec<Arc<dyn TriggerableObj>>,
        readables: Vec<Arc<dyn ReadableObj>>,
        name: impl Into<String>,
    ) -> Plan {
        let name = name.into();
        // Drop redundant entries before bundling, mirroring bluesky's
        // `separate_devices(devices)` call at the head of `trigger_and_read`
        // (plan_stubs.py:1450). Without it, the same readable passed twice emits
        // two `Read`s that collide on their shared data keys and abort the run
        // (the bundler rejects colliding field names); bluesky reads it once.
        let triggerables = separate_devices(triggerables);
        let readables = separate_devices(readables);
        plan_box(async_stream::stream! {
            // Skip the trigger/wait pair when nothing is triggerable, mirroring
            // bluesky's `no_wait` guard (plan_stubs.py:1455-1462): a Wait on a
            // group that received no Trigger is a spurious message.
            if !triggerables.is_empty() {
                for t in &triggerables {
                    yield Msg::Trigger { obj: t.clone(), group: Some("trig".into()) };
                }
                yield Msg::Wait { group: "trig".into(), error_on_timeout: true, timeout: None };
            }
            yield Msg::Create { stream_name: name };
            for r in &readables {
                yield Msg::Read(r.clone());
            }
            yield Msg::Save;
        })
    }

    /// `one_shot(detectors)` — trigger-and-read all detectors once into the
    /// `primary` stream. Detectors must impl both `TriggerableObj` and
    /// `ReadableObj`. Provide them as separate Vecs.
    pub fn one_shot(
        triggerables: Vec<Arc<dyn TriggerableObj>>,
        readables: Vec<Arc<dyn ReadableObj>>,
    ) -> Plan {
        plan_box(async_stream::stream! {
            // Each shot is a rewind boundary: emit a Checkpoint before the
            // acquisition so a pause/resume mid-count re-does only the current
            // shot, not the whole run. Mirrors bluesky's `one_shot`
            // (plan_stubs.py:1622: `yield Msg("checkpoint")` before
            // `trigger_and_read`).
            yield Msg::Checkpoint;
            let mut inner = trigger_and_read(triggerables, readables, "primary");
            while let Some(item) = futures::StreamExt::next(&mut inner).await {
                if let cirrus_core::plan::PlanItem::Bare(m) = item {
                    yield m;
                }
            }
        })
    }

    /// `repeater(n, plan)` — run `plan` `n` times. Each call to `plan_fn`
    /// builds a fresh Plan (so it can yield more than once).
    pub fn repeater<F>(n: usize, mut plan_fn: F) -> Plan
    where
        F: FnMut() -> Plan + Send + 'static,
    {
        plan_box(async_stream::stream! {
            for _ in 0..n {
                let mut p = plan_fn();
                while let Some(item) = futures::StreamExt::next(&mut p).await {
                    if let cirrus_core::plan::PlanItem::Bare(m) = item {
                        yield m;
                    }
                }
            }
        })
    }

    /// `repeat(plan_fn, num, delay)` — repeat `plan_fn` `num` times, emitting a
    /// `Msg::Checkpoint` *before* each repetition and a time-compensated
    /// `Msg::Sleep` *after* each when `delay > 0`. Mirrors bluesky
    /// `plan_stubs.repeat`; distinct from [`repeater`], which only chains
    /// copies with no checkpoint or delay. Intended for users who want the
    /// control-flow shape of `count` without reimplementing it.
    ///
    /// `delay` is a *target cadence*: the emitted sleep is `delay` minus the
    /// wall-clock time that iteration's own messages took to process, so a
    /// slow plan shortens the sleep and never lengthens it; a plan that
    /// already overran `delay` emits no sleep. Matching bluesky's scalar-delay
    /// control flow, a sleep is emitted after *every* repetition (including the
    /// last) whenever `delay > 0`.
    ///
    /// `num = None` repeats forever (until the run is aborted).
    pub fn repeat<F>(mut plan_fn: F, num: Option<usize>, delay: Duration) -> Plan
    where
        F: FnMut() -> Plan + Send + 'static,
    {
        plan_box(async_stream::stream! {
            let mut i: usize = 0;
            loop {
                if let Some(n) = num {
                    if i >= n {
                        break;
                    }
                }
                // Captured before the checkpoint; the stream stays suspended at
                // each `yield` until the engine polls again, so `elapsed`
                // includes the engine's processing of this iteration's messages
                // (matching bluesky's `now = time.time()` span).
                let start = std::time::Instant::now();
                yield Msg::Checkpoint;
                let mut p = plan_fn();
                while let Some(item) = futures::StreamExt::next(&mut p).await {
                    if let cirrus_core::plan::PlanItem::Bare(m) = item {
                        yield m;
                    }
                }
                if !delay.is_zero() {
                    let elapsed = start.elapsed();
                    if delay > elapsed {
                        yield Msg::Sleep(delay - elapsed);
                    }
                }
                i += 1;
            }
        })
    }
}

// ===========================================================================
//  plans (compound; mirrors bluesky.plans)
// ===========================================================================

/// `count(detectors, num)` — read each detector `num` times.
pub fn count(detectors: Vec<Arc<dyn ReadableObj>>, num: usize) -> Plan {
    plan_box(async_stream::stream! {
        yield Msg::OpenRun(RunMetadata {
            plan_name: Some("count".into()),
            ..Default::default()
        });
        for _ in 0..num {
            // Per-shot rewind boundary (bluesky count == repeat(one_shot),
            // both of which emit a Checkpoint per shot: plan_stubs.py:1808,
            // :1622). Without it a pause/resume rewinds the whole run.
            yield Msg::Checkpoint;
            yield Msg::Create { stream_name: "primary".into() };
            for d in &detectors {
                yield Msg::Read(d.clone());
            }
            yield Msg::Save;
        }
        yield Msg::CloseRun {
            exit_status: "success".into(),
            reason: None,
        };
    })
}

/// `count_with_trigger(detectors, num)` — trigger then read each iteration.
pub fn count_with_trigger(
    detectors: Vec<Arc<dyn ReadableObj>>,
    triggerables: Vec<Arc<dyn TriggerableObj>>,
    num: usize,
) -> Plan {
    plan_box(async_stream::stream! {
        yield Msg::OpenRun(RunMetadata {
            plan_name: Some("count_with_trigger".into()),
            ..Default::default()
        });
        for _ in 0..num {
            // Per-shot rewind boundary (bluesky count == repeat(one_shot):
            // plan_stubs.py:1808, :1622). Without it a pause/resume rewinds
            // the whole run instead of re-doing only the current shot.
            yield Msg::Checkpoint;
            // Skip the trigger/wait pair when nothing is triggerable, mirroring
            // bluesky's `no_wait` guard (plan_stubs.py:1455-1462): a Wait on a
            // group that received no Trigger is a spurious message.
            if !triggerables.is_empty() {
                for t in &triggerables {
                    yield Msg::Trigger { obj: t.clone(), group: Some("trigger".into()) };
                }
                yield Msg::Wait {
                    group: "trigger".into(),
                    error_on_timeout: true,
                    timeout: None,
                };
            }
            yield Msg::Create { stream_name: "primary".into() };
            for d in &detectors {
                yield Msg::Read(d.clone());
            }
            yield Msg::Save;
        }
        yield Msg::CloseRun {
            exit_status: "success".into(),
            reason: None,
        };
    })
}

/// 1-D step `scan` from `start` to `stop` (inclusive) in `num` steps.
pub fn scan(
    detectors: Vec<Arc<dyn ReadableObj>>,
    motor: Arc<dyn MovableObj>,
    motor_reader: Arc<dyn ReadableObj>,
    start: f64,
    stop: f64,
    num: usize,
) -> Plan {
    let step = if num > 1 {
        (stop - start) / (num as f64 - 1.0)
    } else {
        0.0
    };
    plan_box(async_stream::stream! {
        yield Msg::OpenRun(RunMetadata {
            plan_name: Some("scan".into()),
            ..Default::default()
        });
        for i in 0..num {
            // Per-step rewind boundary (bluesky one_1d_step.move(): a
            // Checkpoint before the set, plan_stubs.py:1669). Without it a
            // pause/resume rewinds the whole run instead of the current step.
            yield Msg::Checkpoint;
            let pos = start + step * (i as f64);
            yield Msg::Set {
                obj: motor.clone(),
                value: pos,
                group: Some("set".into()),
            };
            yield Msg::Wait {
                group: "set".into(),
                error_on_timeout: true,
                timeout: None,
            };
            yield Msg::Create { stream_name: "primary".into() };
            yield Msg::Read(motor_reader.clone());
            for d in &detectors {
                yield Msg::Read(d.clone());
            }
            yield Msg::Save;
        }
        yield Msg::CloseRun {
            exit_status: "success".into(),
            reason: None,
        };
    })
}

/// `list_scan(detectors, motor, points)` — visit each position in `points`,
/// reading detectors at each.
pub fn list_scan(
    detectors: Vec<Arc<dyn ReadableObj>>,
    motor: Arc<dyn MovableObj>,
    motor_reader: Arc<dyn ReadableObj>,
    points: Vec<f64>,
) -> Plan {
    plan_box(async_stream::stream! {
        yield Msg::OpenRun(RunMetadata {
            plan_name: Some("list_scan".into()),
            ..Default::default()
        });
        for pos in points {
            // Per-step rewind boundary (bluesky one_1d_step.move():
            // plan_stubs.py:1669).
            yield Msg::Checkpoint;
            yield Msg::Set {
                obj: motor.clone(),
                value: pos,
                group: Some("set".into()),
            };
            yield Msg::Wait {
                group: "set".into(),
                error_on_timeout: true,
                timeout: None,
            };
            yield Msg::Create { stream_name: "primary".into() };
            yield Msg::Read(motor_reader.clone());
            for d in &detectors {
                yield Msg::Read(d.clone());
            }
            yield Msg::Save;
        }
        yield Msg::CloseRun {
            exit_status: "success".into(),
            reason: None,
        };
    })
}

/// `rel_scan(detectors, motor, start, stop, num)` — like `scan` but
/// `start`/`stop` are relative to the motor's current position. Caller
/// supplies `current` (read off the motor before invoking).
///
/// After the scan's run closes, the motor is returned to `current`,
/// mirroring bluesky's `reset_positions_decorator` on `rel_scan`
/// (`plans.py:1591`). Like cirrus's other plan-level brackets, the reset
/// runs on normal completion, not after an engine-side abort.
pub fn rel_scan(
    detectors: Vec<Arc<dyn ReadableObj>>,
    motor: Arc<dyn MovableObj>,
    motor_reader: Arc<dyn ReadableObj>,
    current: f64,
    start: f64,
    stop: f64,
    num: usize,
) -> Plan {
    let reset_motor = motor.clone();
    let inner = scan(
        detectors,
        motor,
        motor_reader,
        current + start,
        current + stop,
        num,
    );
    plan_box(async_stream::stream! {
        let mut inner = inner;
        while let Some(item) = futures::StreamExt::next(&mut inner).await {
            if let cirrus_core::plan::PlanItem::Bare(m) = item {
                yield m;
            }
        }
        // `current` is the readback the caller snapshotted before the scan;
        // return the motor there so a relative scan leaves no net motion.
        yield Msg::Set { obj: reset_motor, value: current, group: Some("reset".into()) };
        yield Msg::Wait { group: "reset".into(), error_on_timeout: true, timeout: None };
    })
}

/// `grid_scan(dets, m1, s1, e1, n1, m2, s2, e2, n2)` — 2-D rectilinear scan.
/// `m1` is the slow axis (outer loop), `m2` is the fast axis (inner loop).
/// Every grid point the detectors are read once into `primary`.
#[allow(clippy::too_many_arguments)]
pub fn grid_scan(
    detectors: Vec<Arc<dyn ReadableObj>>,
    motor1: Arc<dyn MovableObj>,
    motor1_reader: Arc<dyn ReadableObj>,
    s1: f64,
    e1: f64,
    n1: usize,
    motor2: Arc<dyn MovableObj>,
    motor2_reader: Arc<dyn ReadableObj>,
    s2: f64,
    e2: f64,
    n2: usize,
) -> Plan {
    let step1 = if n1 > 1 {
        (e1 - s1) / (n1 as f64 - 1.0)
    } else {
        0.0
    };
    let step2 = if n2 > 1 {
        (e2 - s2) / (n2 as f64 - 1.0)
    } else {
        0.0
    };
    plan_box(async_stream::stream! {
        yield Msg::OpenRun(RunMetadata {
            plan_name: Some("grid_scan".into()),
            ..Default::default()
        });
        for i in 0..n1 {
            // Row-change rewind boundary: a pause during the slow-axis move
            // rewinds here, re-driving motor1 (bluesky move_per_step emits a
            // Checkpoint before the step's moves, plan_stubs.py:1695).
            yield Msg::Checkpoint;
            let p1 = s1 + step1 * (i as f64);
            yield Msg::Set {
                obj: motor1.clone(),
                value: p1,
                group: Some("set1".into()),
            };
            yield Msg::Wait {
                group: "set1".into(),
                error_on_timeout: true,
                timeout: None,
            };
            for j in 0..n2 {
                // Per-point rewind boundary for the fast axis (motor1 is
                // already settled at p1 above). Mirrors one Checkpoint per
                // grid point (bluesky move_per_step, plan_stubs.py:1695).
                yield Msg::Checkpoint;
                let p2 = s2 + step2 * (j as f64);
                yield Msg::Set {
                    obj: motor2.clone(),
                    value: p2,
                    group: Some("set2".into()),
                };
                yield Msg::Wait {
                    group: "set2".into(),
                    error_on_timeout: true,
                    timeout: None,
                };
                yield Msg::Create { stream_name: "primary".into() };
                yield Msg::Read(motor1_reader.clone());
                yield Msg::Read(motor2_reader.clone());
                for d in &detectors {
                    yield Msg::Read(d.clone());
                }
                yield Msg::Save;
            }
        }
        yield Msg::CloseRun {
            exit_status: "success".into(),
            reason: None,
        };
    })
}

// ---------------------------------------------------------------------------
// Multi-axis & list-grid plans (mirrors bluesky.plans).
// ---------------------------------------------------------------------------

/// One axis of a multi-motor scan: `(motor, motor_reader, start, stop)`.
pub type ScanAxis = (Arc<dyn MovableObj>, Arc<dyn ReadableObj>, f64, f64);

/// One axis of a list-grid scan: `(motor, motor_reader, points)`.
pub type ListGridAxis = (Arc<dyn MovableObj>, Arc<dyn ReadableObj>, Vec<f64>);

/// One axis of a *relative* list-grid scan: `(motor, motor_reader, points)`,
/// where `points` are offsets from the motor's current setpoint and the
/// motor must be `LocatableObj` so that the setpoint can be snapshotted.
pub type RelListGridAxis = (Arc<dyn LocatableObj>, Arc<dyn ReadableObj>, Vec<f64>);

/// `inner_product_scan(dets, num, [(motor1, s1, e1), ...])` — all motors move
/// together (linspaced) for `num` points. Mirrors bluesky's
/// `inner_product_scan` for the typical positional-only argument shape.
pub fn inner_product_scan(
    detectors: Vec<Arc<dyn ReadableObj>>,
    num: usize,
    axes: Vec<ScanAxis>,
) -> Plan {
    let bounds: Vec<(f64, f64)> = axes.iter().map(|(_, _, s, e)| (*s, *e)).collect();
    let pts = patterns::inner_product(num, &bounds);
    plan_box(async_stream::stream! {
        yield Msg::OpenRun(RunMetadata {
            plan_name: Some("inner_product_scan".into()),
            ..Default::default()
        });
        for row in pts {
            // Per-step rewind boundary before this point's moves (bluesky
            // move_per_step, plan_stubs.py:1695).
            yield Msg::Checkpoint;
            for (i, val) in row.iter().enumerate() {
                yield Msg::Set {
                    obj: axes[i].0.clone(),
                    value: *val,
                    group: Some("set".into()),
                };
            }
            yield Msg::Wait { group: "set".into(), error_on_timeout: true, timeout: None };
            yield Msg::Create { stream_name: "primary".into() };
            for (_, mr, _, _) in &axes {
                yield Msg::Read(mr.clone());
            }
            for d in &detectors {
                yield Msg::Read(d.clone());
            }
            yield Msg::Save;
        }
        yield Msg::CloseRun { exit_status: "success".into(), reason: None };
    })
}

/// `x2x_scan(dets, motor1, m1_reader, motor2, m2_reader, start, stop, num)` —
/// coupled 2:1 *relative* inner-product scan (bluesky `plans.x2x_scan`,
/// a generalised theta-2theta). `motor1` sweeps `start..stop` relative to its
/// current setpoint while `motor2` sweeps `start/2..stop/2` relative to its
/// own; the two move together each step. Built from [`inner_product_scan`]
/// run through `relative_set_wrapper`.
///
/// As with cirrus's other `rel_*` scans, the motors are not returned to their
/// starting positions afterward (offset-only; bluesky's
/// `relative_inner_product_scan` also applies `reset_positions_decorator`).
#[allow(clippy::too_many_arguments)]
pub fn x2x_scan(
    detectors: Vec<Arc<dyn ReadableObj>>,
    motor1: Arc<dyn LocatableObj>,
    motor1_reader: Arc<dyn ReadableObj>,
    motor2: Arc<dyn LocatableObj>,
    motor2_reader: Arc<dyn ReadableObj>,
    start: f64,
    stop: f64,
    num: usize,
) -> Plan {
    let m1: Arc<dyn MovableObj> = motor1.clone();
    let m2: Arc<dyn MovableObj> = motor2.clone();
    let inner = inner_product_scan(
        detectors,
        num,
        vec![
            (m1, motor1_reader, start, stop),
            (m2, motor2_reader, start / 2.0, stop / 2.0),
        ],
    );
    preprocessors::relative_set_wrapper(inner, vec![motor1, motor2])
}

/// `scan_nd(dets, motors, points)` — visit each row of `points` (shape
/// `[N, len(motors)]`). Stripped-down `scan_nd`; bluesky's full version
/// accepts `cycler` objects, this one takes the pre-computed list.
pub fn scan_nd(
    detectors: Vec<Arc<dyn ReadableObj>>,
    motors: Vec<(Arc<dyn MovableObj>, Arc<dyn ReadableObj>)>,
    points: Vec<Vec<f64>>,
) -> Plan {
    plan_box(async_stream::stream! {
        yield Msg::OpenRun(RunMetadata {
            plan_name: Some("scan_nd".into()),
            ..Default::default()
        });
        // Per-motor last-set position — cirrus's port of bluesky's
        // move_per_step `pos_cache` (plan_stubs.py:1688-1702). A motor whose
        // target equals its last-set value is NOT re-commanded this point:
        // in an N-D grid the slow axes stay constant across a row's inner
        // points, so without this they would receive a spurious "move to where
        // you already are" Set + settle every point. `None` until a motor is
        // first set, so every motor moves on the first point (bluesky seeds
        // pos_cache with a None default, so the first `pos == None` is False).
        // Exact equality mirrors bluesky's `pos == pos_cache[motor]`: grid
        // points recur exactly, so an epsilon is unwanted (it would skip a
        // genuine small move).
        let mut pos_cache: Vec<Option<f64>> = vec![None; motors.len()];
        for row in points {
            // Per-step rewind boundary before this point's moves (bluesky
            // move_per_step, plan_stubs.py:1695).
            yield Msg::Checkpoint;
            for (i, v) in row.iter().enumerate() {
                if i >= motors.len() { break; }
                if pos_cache[i] == Some(*v) {
                    // This step does not move motor i (bluesky move_per_step
                    // `if pos == pos_cache[motor]: continue`, plan_stubs.py:1698).
                    continue;
                }
                yield Msg::Set {
                    obj: motors[i].0.clone(),
                    value: *v,
                    group: Some("set".into()),
                };
                pos_cache[i] = Some(*v);
            }
            // Yielded unconditionally, matching bluesky (the wait on an empty
            // `set` group is a no-op). Real grid points always move the fast
            // axis, so the group is non-empty in practice.
            yield Msg::Wait { group: "set".into(), error_on_timeout: true, timeout: None };
            yield Msg::Create { stream_name: "primary".into() };
            for (_, mr) in &motors {
                yield Msg::Read(mr.clone());
            }
            for d in &detectors {
                yield Msg::Read(d.clone());
            }
            yield Msg::Save;
        }
        yield Msg::CloseRun { exit_status: "success".into(), reason: None };
    })
}

/// `list_grid_scan(dets, [(motor, [points...]), ...])` — N-D grid where
/// each axis traces a user-supplied list of positions.
pub fn list_grid_scan(detectors: Vec<Arc<dyn ReadableObj>>, axes: Vec<ListGridAxis>) -> Plan {
    let lists: Vec<Vec<f64>> = axes.iter().map(|(_, _, l)| l.clone()).collect();
    let pts = patterns::outer_list_product(&lists);
    let motors: Vec<(Arc<dyn MovableObj>, Arc<dyn ReadableObj>)> =
        axes.into_iter().map(|(m, r, _)| (m, r)).collect();
    scan_nd(detectors, motors, pts)
}

/// `rel_list_grid_scan(dets, axes)` — relative variant of [`list_grid_scan`]
/// (bluesky `plans.rel_list_grid_scan`). Each axis's positions are offset by
/// that axis motor's current setpoint, snapshotted once per motor via
/// `LocatableObj::locate_dyn`.
///
/// As in bluesky, each axis motor is returned to its starting position after
/// the scan (`reset_positions_decorator`). Like [`list_grid_scan`], snaking is
/// not applied — each axis traces a plain outer-product trajectory.
pub fn rel_list_grid_scan(
    detectors: Vec<Arc<dyn ReadableObj>>,
    axes: Vec<RelListGridAxis>,
) -> Plan {
    let reset_motors: Vec<Arc<dyn LocatableObj>> = axes.iter().map(|(m, _, _)| m.clone()).collect();
    let inner = plan_box(async_stream::stream! {
        let mut abs_axes: Vec<ListGridAxis> = Vec::with_capacity(axes.len());
        for (motor, reader, points) in axes {
            let bias = motor.locate_dyn().await.map(|l| l.setpoint).unwrap_or(0.0);
            let abs_points: Vec<f64> = points.iter().map(|p| *p + bias).collect();
            let mv: Arc<dyn MovableObj> = motor;
            abs_axes.push((mv, reader, abs_points));
        }
        let mut inner = list_grid_scan(detectors, abs_axes);
        while let Some(item) = futures::StreamExt::next(&mut inner).await {
            if let cirrus_core::plan::PlanItem::Bare(m) = item {
                yield m;
            }
        }
    });
    preprocessors::reset_positions_wrapper(inner, reset_motors)
}

/// `spiral_square(dets, x_motor, y_motor, x_center, y_center, x_range,
/// y_range, x_num, y_num)` — visits an `x_num × y_num` grid in spiral
/// order outward from the center.
#[allow(clippy::too_many_arguments)]
pub fn spiral_square(
    detectors: Vec<Arc<dyn ReadableObj>>,
    x_motor: Arc<dyn MovableObj>,
    x_reader: Arc<dyn ReadableObj>,
    y_motor: Arc<dyn MovableObj>,
    y_reader: Arc<dyn ReadableObj>,
    x_center: f64,
    y_center: f64,
    x_range: f64,
    y_range: f64,
    x_num: usize,
    y_num: usize,
) -> Plan {
    let pts = patterns::spiral_square_pattern(x_center, y_center, x_range, y_range, x_num, y_num);
    plan_box(async_stream::stream! {
        yield Msg::OpenRun(RunMetadata {
            plan_name: Some("spiral_square".into()),
            ..Default::default()
        });
        for (x, y) in pts {
            // Per-point rewind boundary (bluesky move_per_step, :1695).
            yield Msg::Checkpoint;
            yield Msg::Set { obj: x_motor.clone(), value: x, group: Some("set".into()) };
            yield Msg::Set { obj: y_motor.clone(), value: y, group: Some("set".into()) };
            yield Msg::Wait { group: "set".into(), error_on_timeout: true, timeout: None };
            yield Msg::Create { stream_name: "primary".into() };
            yield Msg::Read(x_reader.clone());
            yield Msg::Read(y_reader.clone());
            for d in &detectors {
                yield Msg::Read(d.clone());
            }
            yield Msg::Save;
        }
        yield Msg::CloseRun { exit_status: "success".into(), reason: None };
    })
}

/// `spiral(dets, x_motor, y_motor, x_start, y_start, x_range, y_range, dr,
/// nth)` — Archimedean spiral through `(x, y)` until the spiral exits the
/// bounding rect. `dr` is radial increment / turn; `nth` is points / turn.
#[allow(clippy::too_many_arguments)]
pub fn spiral(
    detectors: Vec<Arc<dyn ReadableObj>>,
    x_motor: Arc<dyn MovableObj>,
    x_reader: Arc<dyn ReadableObj>,
    y_motor: Arc<dyn MovableObj>,
    y_reader: Arc<dyn ReadableObj>,
    x_start: f64,
    y_start: f64,
    x_range: f64,
    y_range: f64,
    dr: f64,
    nth: usize,
) -> Plan {
    let pts = patterns::spiral(x_start, y_start, x_range, y_range, dr, nth);
    plan_box(async_stream::stream! {
        yield Msg::OpenRun(RunMetadata {
            plan_name: Some("spiral".into()),
            ..Default::default()
        });
        for (x, y) in pts {
            // Per-point rewind boundary (bluesky move_per_step, :1695).
            yield Msg::Checkpoint;
            yield Msg::Set { obj: x_motor.clone(), value: x, group: Some("set".into()) };
            yield Msg::Set { obj: y_motor.clone(), value: y, group: Some("set".into()) };
            yield Msg::Wait { group: "set".into(), error_on_timeout: true, timeout: None };
            yield Msg::Create { stream_name: "primary".into() };
            yield Msg::Read(x_reader.clone());
            yield Msg::Read(y_reader.clone());
            for d in &detectors {
                yield Msg::Read(d.clone());
            }
            yield Msg::Save;
        }
        yield Msg::CloseRun { exit_status: "success".into(), reason: None };
    })
}

/// `ramp_plan(go_plan, monitor_signal, take_pre_data_count, period)` —
/// kicks off `go_plan` (a *sub-plan* that initiates a monotonic ramp,
/// e.g. `mv(temperature, 300)`), then samples `detectors` every `period`
/// while waiting for the ramp to land. Simplified vs bluesky's full
/// version — no wait_for_motor_done branch; caller must interrupt.
pub fn ramp_plan(
    go_plan: Plan,
    detectors: Vec<Arc<dyn ReadableObj>>,
    period: std::time::Duration,
    samples: usize,
) -> Plan {
    plan_box(async_stream::stream! {
        yield Msg::OpenRun(RunMetadata {
            plan_name: Some("ramp_plan".into()),
            ..Default::default()
        });
        // Kick off the ramp (do not wait — go_plan should issue Set
        // without a Wait if it wants asynchronous progress).
        let mut go = go_plan;
        while let Some(item) = futures::StreamExt::next(&mut go).await {
            if let cirrus_core::plan::PlanItem::Bare(m) = item {
                yield m;
            }
        }
        for _ in 0..samples {
            yield Msg::Sleep(period);
            yield Msg::Create { stream_name: "primary".into() };
            for d in &detectors {
                yield Msg::Read(d.clone());
            }
            yield Msg::Save;
        }
        yield Msg::CloseRun { exit_status: "success".into(), reason: None };
    })
}

/// `rel_list_scan` — relative variant of `list_scan`. Reads each motor's
/// setpoint once at the start of the plan and offsets the supplied points.
pub fn rel_list_scan(
    detectors: Vec<Arc<dyn ReadableObj>>,
    motor: Arc<dyn LocatableObj>,
    motor_reader: Arc<dyn ReadableObj>,
    points: Vec<f64>,
) -> Plan {
    let reset_motor = motor.clone();
    let inner = plan_box(async_stream::stream! {
        let bias = motor.locate_dyn().await
            .map(|l| l.setpoint)
            .unwrap_or(0.0);
        let abs_points: Vec<f64> = points.iter().map(|p| *p + bias).collect();
        let mv: Arc<dyn MovableObj> = motor;
        let mut inner = list_scan(detectors, mv, motor_reader, abs_points);
        while let Some(item) = futures::StreamExt::next(&mut inner).await {
            if let cirrus_core::plan::PlanItem::Bare(m) = item {
                yield m;
            }
        }
    });
    preprocessors::reset_positions_wrapper(inner, vec![reset_motor])
}

/// `rel_grid_scan` — relative variant of `grid_scan`. Both motors are
/// `LocatableObj` so we can snapshot starting positions. As in bluesky, both
/// motors are returned to those positions after the scan
/// (`reset_positions_decorator`).
#[allow(clippy::too_many_arguments)]
pub fn rel_grid_scan(
    detectors: Vec<Arc<dyn ReadableObj>>,
    motor1: Arc<dyn LocatableObj>,
    motor1_reader: Arc<dyn ReadableObj>,
    s1: f64,
    e1: f64,
    n1: usize,
    motor2: Arc<dyn LocatableObj>,
    motor2_reader: Arc<dyn ReadableObj>,
    s2: f64,
    e2: f64,
    n2: usize,
) -> Plan {
    let reset_motors: Vec<Arc<dyn LocatableObj>> = vec![motor1.clone(), motor2.clone()];
    let inner = plan_box(async_stream::stream! {
        let b1 = motor1.locate_dyn().await.map(|l| l.setpoint).unwrap_or(0.0);
        let b2 = motor2.locate_dyn().await.map(|l| l.setpoint).unwrap_or(0.0);
        let m1mv: Arc<dyn MovableObj> = motor1;
        let m2mv: Arc<dyn MovableObj> = motor2;
        let mut inner = grid_scan(
            detectors,
            m1mv, motor1_reader,
            s1 + b1, e1 + b1, n1,
            m2mv, motor2_reader,
            s2 + b2, e2 + b2, n2,
        );
        while let Some(item) = futures::StreamExt::next(&mut inner).await {
            if let cirrus_core::plan::PlanItem::Bare(m) = item {
                yield m;
            }
        }
    });
    preprocessors::reset_positions_wrapper(inner, reset_motors)
}

/// `log_scan(detectors, motor, motor_readback, start, stop, num)` —
/// 1-D scan with logarithmically-spaced points (`start` and `stop`
/// must be the same sign and non-zero). Calls `list_scan` internally.
pub fn log_scan(
    detectors: Vec<Arc<dyn ReadableObj>>,
    motor: Arc<dyn MovableObj>,
    motor_readback: Arc<dyn ReadableObj>,
    start: f64,
    stop: f64,
    num: usize,
) -> Plan {
    if num == 0 || start == 0.0 || stop == 0.0 || start.signum() != stop.signum() {
        return stubs::null();
    }
    let log_start = start.abs().ln();
    let log_stop = stop.abs().ln();
    let sign = start.signum();
    let points: Vec<f64> = (0..num)
        .map(|i| {
            let t = if num > 1 {
                i as f64 / (num as f64 - 1.0)
            } else {
                0.0
            };
            sign * (log_start + (log_stop - log_start) * t).exp()
        })
        .collect();
    list_scan(detectors, motor, motor_readback, points)
}

/// `rel_log_scan(detectors, motor, motor_readback, start, stop, num)` —
/// relative variant of [`log_scan`]: the log-spaced targets are offset by the
/// motor's current setpoint, snapshotted once via `LocatableObj::locate_dyn`
/// (bluesky `plans.rel_log_scan`, `relative_set_decorator`).
///
/// As in bluesky, the motor is returned to its starting position after the
/// scan (`reset_positions_decorator` over `relative_set_decorator`).
pub fn rel_log_scan(
    detectors: Vec<Arc<dyn ReadableObj>>,
    motor: Arc<dyn LocatableObj>,
    motor_readback: Arc<dyn ReadableObj>,
    start: f64,
    stop: f64,
    num: usize,
) -> Plan {
    let mv: Arc<dyn MovableObj> = motor.clone();
    let inner = log_scan(detectors, mv, motor_readback, start, stop, num);
    let rel = preprocessors::relative_set_wrapper(inner, vec![motor.clone()]);
    preprocessors::reset_positions_wrapper(rel, vec![motor])
}

/// `spiral_fermat(detectors, x_motor, x_reader, y_motor, y_reader,
/// x_start, y_start, x_range, y_range, dr, factor)` —
/// Fermat (sunflower) spiral via golden-angle increments. See
/// `patterns::spiral_fermat_pattern`.
#[allow(clippy::too_many_arguments)]
pub fn spiral_fermat(
    detectors: Vec<Arc<dyn ReadableObj>>,
    x_motor: Arc<dyn MovableObj>,
    x_reader: Arc<dyn ReadableObj>,
    y_motor: Arc<dyn MovableObj>,
    y_reader: Arc<dyn ReadableObj>,
    x_start: f64,
    y_start: f64,
    x_range: f64,
    y_range: f64,
    dr: f64,
    factor: f64,
) -> Plan {
    let pts = patterns::spiral_fermat_pattern(x_start, y_start, x_range, y_range, dr, factor);
    plan_box(async_stream::stream! {
        yield Msg::OpenRun(RunMetadata {
            plan_name: Some("spiral_fermat".into()),
            ..Default::default()
        });
        for (x, y) in pts {
            // Per-point rewind boundary (bluesky move_per_step, :1695).
            yield Msg::Checkpoint;
            yield Msg::Set { obj: x_motor.clone(), value: x, group: Some("set".into()) };
            yield Msg::Set { obj: y_motor.clone(), value: y, group: Some("set".into()) };
            yield Msg::Wait { group: "set".into(), error_on_timeout: true, timeout: None };
            yield Msg::Create { stream_name: "primary".into() };
            yield Msg::Read(x_reader.clone());
            yield Msg::Read(y_reader.clone());
            for d in &detectors {
                yield Msg::Read(d.clone());
            }
            yield Msg::Save;
        }
        yield Msg::CloseRun { exit_status: "success".into(), reason: None };
    })
}

/// `rel_spiral(...)` — relative variant of [`spiral`]: the spiral is drawn
/// around the motors' current setpoints instead of an absolute centre.
/// Both axis motors are `LocatableObj` so the offsets can be snapshotted
/// once via `relative_set_wrapper` (bluesky `plans.rel_spiral`).
///
/// As in bluesky, both motors are returned to their start positions after the
/// scan (`reset_positions_decorator` over `relative_set_decorator`).
#[allow(clippy::too_many_arguments)]
pub fn rel_spiral(
    detectors: Vec<Arc<dyn ReadableObj>>,
    x_motor: Arc<dyn LocatableObj>,
    x_reader: Arc<dyn ReadableObj>,
    y_motor: Arc<dyn LocatableObj>,
    y_reader: Arc<dyn ReadableObj>,
    x_start: f64,
    y_start: f64,
    x_range: f64,
    y_range: f64,
    dr: f64,
    nth: usize,
) -> Plan {
    let xm: Arc<dyn MovableObj> = x_motor.clone();
    let ym: Arc<dyn MovableObj> = y_motor.clone();
    let inner = spiral(
        detectors, xm, x_reader, ym, y_reader, x_start, y_start, x_range, y_range, dr, nth,
    );
    let rel = preprocessors::relative_set_wrapper(inner, vec![x_motor.clone(), y_motor.clone()]);
    preprocessors::reset_positions_wrapper(rel, vec![x_motor, y_motor])
}

/// `rel_spiral_square(...)` — relative variant of [`spiral_square`]; the
/// square raster spiral is centred on the motors' current setpoints
/// (bluesky `plans.rel_spiral_square`). Returns the motors to start, see
/// [`rel_spiral`].
#[allow(clippy::too_many_arguments)]
pub fn rel_spiral_square(
    detectors: Vec<Arc<dyn ReadableObj>>,
    x_motor: Arc<dyn LocatableObj>,
    x_reader: Arc<dyn ReadableObj>,
    y_motor: Arc<dyn LocatableObj>,
    y_reader: Arc<dyn ReadableObj>,
    x_center: f64,
    y_center: f64,
    x_range: f64,
    y_range: f64,
    x_num: usize,
    y_num: usize,
) -> Plan {
    let xm: Arc<dyn MovableObj> = x_motor.clone();
    let ym: Arc<dyn MovableObj> = y_motor.clone();
    let inner = spiral_square(
        detectors, xm, x_reader, ym, y_reader, x_center, y_center, x_range, y_range, x_num, y_num,
    );
    let rel = preprocessors::relative_set_wrapper(inner, vec![x_motor.clone(), y_motor.clone()]);
    preprocessors::reset_positions_wrapper(rel, vec![x_motor, y_motor])
}

/// `rel_spiral_fermat(...)` — relative variant of [`spiral_fermat`]; the
/// Fermat (sunflower) spiral is centred on the motors' current setpoints
/// (bluesky `plans.rel_spiral_fermat`). Returns the motors to start, see
/// [`rel_spiral`].
#[allow(clippy::too_many_arguments)]
pub fn rel_spiral_fermat(
    detectors: Vec<Arc<dyn ReadableObj>>,
    x_motor: Arc<dyn LocatableObj>,
    x_reader: Arc<dyn ReadableObj>,
    y_motor: Arc<dyn LocatableObj>,
    y_reader: Arc<dyn ReadableObj>,
    x_start: f64,
    y_start: f64,
    x_range: f64,
    y_range: f64,
    dr: f64,
    factor: f64,
) -> Plan {
    let xm: Arc<dyn MovableObj> = x_motor.clone();
    let ym: Arc<dyn MovableObj> = y_motor.clone();
    let inner = spiral_fermat(
        detectors, xm, x_reader, ym, y_reader, x_start, y_start, x_range, y_range, dr, factor,
    );
    let rel = preprocessors::relative_set_wrapper(inner, vec![x_motor.clone(), y_motor.clone()]);
    preprocessors::reset_positions_wrapper(rel, vec![x_motor, y_motor])
}

/// `fly(flyer, dets)` — kickoff, collect while completing, unstage.
pub fn fly(
    flyer: Arc<dyn FlyableObj>,
    collectable: Arc<dyn CollectableObj>,
    stageables: Vec<Arc<dyn StageableObj>>,
) -> Plan {
    plan_box(async_stream::stream! {
        yield Msg::OpenRun(RunMetadata {
            plan_name: Some("fly".into()),
            ..Default::default()
        });
        for s in &stageables {
            yield Msg::Stage(s.clone());
        }
        yield Msg::Kickoff { obj: flyer.clone(), group: Some("kick".into()) };
        yield Msg::Wait {
            group: "kick".into(),
            error_on_timeout: true,
            timeout: None,
        };
        yield Msg::Complete { obj: flyer.clone(), group: Some("done".into()) };
        yield Msg::Wait {
            group: "done".into(),
            error_on_timeout: true,
            timeout: None,
        };
        yield Msg::Collect {
            obj: collectable.clone(),
            stream_name: None,
        };
        for s in &stageables {
            yield Msg::Unstage(s.clone());
        }
        yield Msg::CloseRun {
            exit_status: "success".into(),
            reason: None,
        };
    })
}

/// `adaptive_scan(detectors, signal_field, motor, motor_reader, start,
/// stop, min_step, max_step, target_delta, backstep)` — adaptive
/// step-sized 1-D scan. Mirrors bluesky's `adaptive_scan`.
///
/// At each step, reads `signal_field` from the first detector's
/// reading. Compares delta to the previous reading:
/// - If `|delta|` exceeds `target_delta * 1.5`, the next step
///   shrinks (toward `min_step`) and optionally back-steps (when
///   `backstep=true`) to capture the missed transition.
/// - If `|delta|` is well below `target_delta * 0.5`, the next step
///   doubles (toward `max_step`).
///
/// Useful for scanning across a peak / edge where uniform-step
/// density would either miss the feature or oversample the flat
/// regions.
#[allow(clippy::too_many_arguments)]
pub fn adaptive_scan(
    detectors: Vec<Arc<dyn ReadableObj>>,
    signal_field: impl Into<String>,
    motor: Arc<dyn MovableObj>,
    motor_reader: Arc<dyn ReadableObj>,
    start: f64,
    stop: f64,
    min_step: f64,
    max_step: f64,
    target_delta: f64,
    backstep: bool,
) -> Plan {
    let signal_field = signal_field.into();
    let mid_step = (min_step + max_step) * 0.5;
    plan_box(async_stream::stream! {
        yield Msg::OpenRun(RunMetadata {
            plan_name: Some("adaptive_scan".into()),
            ..Default::default()
        });
        let direction = if stop >= start { 1.0_f64 } else { -1.0 };
        let mut pos = start;
        let mut prev_signal: Option<f64> = None;
        let mut step = mid_step.max(min_step.min(max_step));
        let max_iters = 10_000_usize;
        let mut iter = 0_usize;
        loop {
            iter += 1;
            if iter > max_iters {
                break;
            }
            if (direction > 0.0 && pos > stop) || (direction < 0.0 && pos < stop) {
                break;
            }
            // Per-step rewind boundary (bluesky one_1d_step.move(): :1669).
            // After the break checks, so a terminal iteration emits none.
            yield Msg::Checkpoint;
            yield Msg::Set { obj: motor.clone(), value: pos, group: Some("set".into()) };
            yield Msg::Wait {
                group: "set".into(),
                error_on_timeout: true,
                timeout: None,
            };
            yield Msg::Create { stream_name: "primary".into() };
            yield Msg::Read(motor_reader.clone());
            for d in &detectors {
                yield Msg::Read(d.clone());
            }
            yield Msg::Save;
            // Best-effort signal sample for adaptation. We don't
            // have direct access to the value yielded into the
            // bundler from this side of the plan stream, so we
            // re-read the first detector. For soft / fast-read
            // detectors this is cheap; for slow ones consider a
            // separate signal channel.
            let now_signal: Option<f64> = if let Some(d) = detectors.first() {
                if let Ok(map) = d.read_dyn().await {
                    map.get(&signal_field).and_then(|rv| rv.value.as_f64())
                } else { None }
            } else { None };
            let next_step = match (prev_signal, now_signal) {
                (Some(p), Some(n)) => {
                    let abs_delta = (n - p).abs();
                    if abs_delta > target_delta * 1.5 {
                        let new_step = (step * 0.5).max(min_step);
                        if backstep && new_step < step {
                            pos -= step * direction;
                        }
                        new_step
                    } else if abs_delta < target_delta * 0.5 {
                        (step * 2.0).min(max_step)
                    } else {
                        step
                    }
                }
                _ => step,
            };
            prev_signal = now_signal.or(prev_signal);
            step = next_step.clamp(min_step, max_step);
            pos += step * direction;
        }
        yield Msg::CloseRun {
            exit_status: "success".into(),
            reason: None,
        };
    })
}

/// `tune_centroid(detectors, signal_field, motor, motor_reader, start,
/// stop, num)` — a uniform scan that finds the centroid of
/// `signal_field` across the detector readings, then sets `motor`
/// to that centroid. Mirrors a simplified bluesky `tune_centroid`.
///
/// Centroid = `Σ(pos_i * sig_i) / Σ(sig_i)`. If all signals are zero
/// (or non-numeric), the motor stops at the last scan position.
#[allow(clippy::too_many_arguments)]
pub fn tune_centroid(
    detectors: Vec<Arc<dyn ReadableObj>>,
    signal_field: impl Into<String>,
    motor: Arc<dyn MovableObj>,
    motor_reader: Arc<dyn ReadableObj>,
    start: f64,
    stop: f64,
    num: usize,
) -> Plan {
    let signal_field = signal_field.into();
    let step = if num > 1 {
        (stop - start) / (num as f64 - 1.0)
    } else {
        0.0
    };
    plan_box(async_stream::stream! {
        yield Msg::OpenRun(RunMetadata {
            plan_name: Some("tune_centroid".into()),
            ..Default::default()
        });
        let mut sum_xy = 0.0_f64;
        let mut sum_y = 0.0_f64;
        let mut last_pos = start;
        for i in 0..num {
            // Per-step rewind boundary (bluesky one_1d_step.move(): :1669).
            yield Msg::Checkpoint;
            let pos = start + step * (i as f64);
            last_pos = pos;
            yield Msg::Set { obj: motor.clone(), value: pos, group: Some("set".into()) };
            yield Msg::Wait {
                group: "set".into(),
                error_on_timeout: true,
                timeout: None,
            };
            yield Msg::Create { stream_name: "primary".into() };
            yield Msg::Read(motor_reader.clone());
            for d in &detectors {
                yield Msg::Read(d.clone());
            }
            yield Msg::Save;
            if let Some(d) = detectors.first() {
                if let Ok(map) = d.read_dyn().await {
                    if let Some(y) = map.get(&signal_field).and_then(|rv| rv.value.as_f64()) {
                        sum_xy += pos * y;
                        sum_y += y;
                    }
                }
            }
        }
        let target = if sum_y.abs() > f64::EPSILON {
            sum_xy / sum_y
        } else {
            last_pos
        };
        yield Msg::Set { obj: motor.clone(), value: target, group: Some("center".into()) };
        yield Msg::Wait {
            group: "center".into(),
            error_on_timeout: true,
            timeout: None,
        };
        yield Msg::CloseRun {
            exit_status: "success".into(),
            reason: None,
        };
    })
}

/// `rel_adaptive_scan(...)` — relative variant of [`adaptive_scan`].
/// Reads the motor's current setpoint once at start, adds the
/// supplied `start`/`stop` offsets, and runs `adaptive_scan` over
/// that absolute range. As in bluesky, the motor is returned to its
/// starting position after the scan (`reset_positions_decorator`).
#[allow(clippy::too_many_arguments)]
pub fn rel_adaptive_scan(
    detectors: Vec<Arc<dyn ReadableObj>>,
    signal_field: impl Into<String>,
    motor: Arc<dyn LocatableObj>,
    motor_reader: Arc<dyn ReadableObj>,
    start_offset: f64,
    stop_offset: f64,
    min_step: f64,
    max_step: f64,
    target_delta: f64,
    backstep: bool,
) -> Plan {
    let signal_field = signal_field.into();
    let reset_motor = motor.clone();
    let inner = plan_box(async_stream::stream! {
        let center = match motor.locate_dyn().await {
            Ok(loc) => loc.setpoint,
            Err(e) => {
                yield Msg::Fail(format!(
                    "rel_adaptive_scan({}): locate_dyn failed: {e}",
                    motor.name()
                ));
                return;
            }
        };
        let abs_start = center + start_offset;
        let abs_stop = center + stop_offset;
        let movable: Arc<dyn MovableObj> = motor;
        let mut inner = adaptive_scan(
            detectors,
            signal_field,
            movable,
            motor_reader,
            abs_start,
            abs_stop,
            min_step,
            max_step,
            target_delta,
            backstep,
        );
        use futures::StreamExt;
        while let Some(item) = inner.next().await {
            if let cirrus_core::plan::PlanItem::Bare(m) = item {
                yield m;
            }
        }
    });
    preprocessors::reset_positions_wrapper(inner, vec![reset_motor])
}

#[cfg(test)]
mod tests {
    use super::*;
    use cirrus_core::plan::{Plan, PlanItem};
    use cirrus_core::status::Status;
    use futures::StreamExt;

    /// Minimal flyer for stub-stream tests. `kickoff_dyn`/`complete_dyn`
    /// are never called by `drain` (only the engine invokes them), so the
    /// returned `Status::done()` is just a stand-in.
    struct FakeFlyer(String);

    impl cirrus_core::msg::NamedObj for FakeFlyer {
        fn name(&self) -> &str {
            &self.0
        }
    }

    #[async_trait::async_trait]
    impl FlyableObj for FakeFlyer {
        async fn kickoff_dyn(&self) -> Status {
            Status::done()
        }
        async fn complete_dyn(&self) -> Status {
            Status::done()
        }
    }

    /// Minimal preparable for `prepare` stub-stream tests. `prepare_dyn` is
    /// never called by `drain` (only the engine invokes it).
    struct FakePreparable(String);

    impl cirrus_core::msg::NamedObj for FakePreparable {
        fn name(&self) -> &str {
            &self.0
        }
    }

    #[async_trait::async_trait]
    impl PreparableObj for FakePreparable {
        async fn prepare_dyn(&self, _value: serde_json::Value) -> Status {
            Status::done()
        }
    }

    async fn drain(mut plan: Plan) -> Vec<Msg> {
        let mut out = Vec::new();
        while let Some(item) = plan.next().await {
            if let PlanItem::Bare(m) = item {
                out.push(m);
            }
        }
        out
    }

    fn flyers(n: usize) -> Vec<Arc<dyn FlyableObj>> {
        (0..n)
            .map(|i| Arc::new(FakeFlyer(format!("fly{i}"))) as Arc<dyn FlyableObj>)
            .collect()
    }

    fn kickoff_group(m: &Msg) -> Option<&str> {
        match m {
            Msg::Kickoff { group, .. } => group.as_deref(),
            _ => None,
        }
    }

    fn complete_group(m: &Msg) -> Option<&str> {
        match m {
            Msg::Complete { group, .. } => group.as_deref(),
            _ => None,
        }
    }

    use cirrus_core::msg::{DynLocation, MovableObj, NamedObj, ReadableObj};
    use cirrus_core::reading::ReadingValue;
    use std::collections::HashMap;

    /// Locatable motor whose `locate_dyn` reports a fixed readback (`bias`).
    /// `set_dyn` is never invoked by `drain`.
    struct FakeMotor {
        name: String,
        bias: f64,
    }

    impl NamedObj for FakeMotor {
        fn name(&self) -> &str {
            &self.name
        }
    }

    #[async_trait::async_trait]
    impl MovableObj for FakeMotor {
        async fn set_dyn(&self, _value: f64) -> Status {
            Status::done()
        }
    }

    #[async_trait::async_trait]
    impl cirrus_core::msg::LocatableObj for FakeMotor {
        async fn locate_dyn(&self) -> Result<DynLocation, cirrus_core::error::CirrusError> {
            Ok(DynLocation {
                setpoint: self.bias,
                readback: self.bias,
            })
        }
    }

    /// Locatable motor that counts `locate_dyn` calls, so a test can assert a
    /// listed-but-unmoved motor is never located by a lazy-capture wrapper.
    struct CountingMotor {
        name: String,
        setpoint: f64,
        locates: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl NamedObj for CountingMotor {
        fn name(&self) -> &str {
            &self.name
        }
    }

    #[async_trait::async_trait]
    impl MovableObj for CountingMotor {
        async fn set_dyn(&self, _value: f64) -> Status {
            Status::done()
        }
    }

    #[async_trait::async_trait]
    impl cirrus_core::msg::LocatableObj for CountingMotor {
        async fn locate_dyn(&self) -> Result<DynLocation, cirrus_core::error::CirrusError> {
            self.locates
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(DynLocation {
                setpoint: self.setpoint,
                readback: self.setpoint,
            })
        }
    }

    /// Readable carried only inside `Msg::Read`; `read_dyn`/`describe_dyn`
    /// are never called by `drain`.
    struct FakeReadable(String);

    impl NamedObj for FakeReadable {
        fn name(&self) -> &str {
            &self.0
        }
    }

    #[async_trait::async_trait]
    impl ReadableObj for FakeReadable {
        async fn read_dyn(
            &self,
        ) -> Result<HashMap<String, ReadingValue>, cirrus_core::error::CirrusError> {
            Ok(HashMap::new())
        }
        async fn describe_dyn(
            &self,
        ) -> Result<HashMap<String, cirrus_event_model::DataKey>, cirrus_core::error::CirrusError>
        {
            Ok(HashMap::new())
        }
    }

    /// Triggerable carried only inside `Msg::Trigger`; `trigger_dyn` is never
    /// called by `drain`.
    struct FakeTriggerable(String);

    impl NamedObj for FakeTriggerable {
        fn name(&self) -> &str {
            &self.0
        }
    }

    #[async_trait::async_trait]
    impl TriggerableObj for FakeTriggerable {
        async fn trigger_dyn(&self) -> Status {
            Status::done()
        }
    }

    // Empty triggerables → no Trigger and no Wait (bluesky no_wait guard),
    // but a Create/Read/Save event is still produced.
    #[tokio::test]
    async fn trigger_and_read_skips_wait_when_no_triggerables() {
        let r = Arc::new(FakeReadable("det".into())) as Arc<dyn ReadableObj>;
        let msgs = drain(stubs::trigger_and_read(vec![], vec![r], "primary")).await;
        assert!(!msgs.iter().any(|m| matches!(m, Msg::Trigger { .. })));
        assert!(!msgs.iter().any(|m| matches!(m, Msg::Wait { .. })));
        assert!(matches!(msgs.first(), Some(Msg::Create { .. })));
        assert!(matches!(msgs.last(), Some(Msg::Save)));
    }

    // A triggerable present → Trigger then Wait{trig} precede Create.
    #[tokio::test]
    async fn trigger_and_read_emits_trigger_then_wait_when_triggerable() {
        let t = Arc::new(FakeTriggerable("det".into())) as Arc<dyn TriggerableObj>;
        let r = Arc::new(FakeReadable("det".into())) as Arc<dyn ReadableObj>;
        let msgs = drain(stubs::trigger_and_read(vec![t], vec![r], "primary")).await;
        assert!(matches!(msgs.first(), Some(Msg::Trigger { .. })));
        let wait_pos = msgs
            .iter()
            .position(|m| matches!(m, Msg::Wait { group, .. } if group == "trig"))
            .expect("Wait{trig} present");
        let create_pos = msgs
            .iter()
            .position(|m| matches!(m, Msg::Create { .. }))
            .expect("Create present");
        assert!(wait_pos < create_pos, "Wait must precede Create");
    }

    // The same device passed twice is read/triggered once, mirroring bluesky's
    // separate_devices() at the head of trigger_and_read. Without dedup the two
    // Reads share data keys and the bundler aborts the run on the collision.
    #[tokio::test]
    async fn trigger_and_read_dedups_repeated_devices() {
        let t = Arc::new(FakeTriggerable("det".into())) as Arc<dyn TriggerableObj>;
        let r = Arc::new(FakeReadable("det".into())) as Arc<dyn ReadableObj>;
        // Same Arc handed in twice in each list.
        let msgs = drain(stubs::trigger_and_read(
            vec![t.clone(), t.clone()],
            vec![r.clone(), r.clone()],
            "primary",
        ))
        .await;
        assert_eq!(
            msgs.iter()
                .filter(|m| matches!(m, Msg::Trigger { .. }))
                .count(),
            1,
            "a device listed twice must be triggered once"
        );
        assert_eq!(
            msgs.iter().filter(|m| matches!(m, Msg::Read(_))).count(),
            1,
            "a device listed twice must be read once"
        );
        // Exactly one Wait{trig} and one Create/Save still bracket the event.
        assert_eq!(
            msgs.iter()
                .filter(|m| matches!(m, Msg::Wait { .. }))
                .count(),
            1
        );
    }

    // Empty triggerables across iterations → zero Wait, one Save per iteration.
    #[tokio::test]
    async fn count_with_trigger_skips_wait_each_iteration_when_no_triggerables() {
        let d = Arc::new(FakeReadable("det".into())) as Arc<dyn ReadableObj>;
        let msgs = drain(count_with_trigger(vec![d], vec![], 2)).await;
        assert_eq!(
            msgs.iter()
                .filter(|m| matches!(m, Msg::Wait { .. }))
                .count(),
            0
        );
        assert!(!msgs.iter().any(|m| matches!(m, Msg::Trigger { .. })));
        assert_eq!(
            msgs.iter().filter(|m| matches!(m, Msg::Save)).count(),
            2,
            "one Save per iteration"
        );
    }

    // A triggerable present → one Trigger and one Wait{trigger} per iteration.
    #[tokio::test]
    async fn count_with_trigger_emits_wait_each_iteration_when_triggerable() {
        let t = Arc::new(FakeTriggerable("det".into())) as Arc<dyn TriggerableObj>;
        let d = Arc::new(FakeReadable("det".into())) as Arc<dyn ReadableObj>;
        let msgs = drain(count_with_trigger(vec![d], vec![t], 2)).await;
        assert_eq!(
            msgs.iter()
                .filter(|m| matches!(m, Msg::Wait { group, .. } if group == "trigger"))
                .count(),
            2
        );
        assert_eq!(
            msgs.iter()
                .filter(|m| matches!(m, Msg::Trigger { .. }))
                .count(),
            2
        );
    }

    // Each count shot is a rewind boundary: a Checkpoint precedes every
    // Create (bluesky count == repeat(one_shot), both emit a per-shot
    // checkpoint; plan_stubs.py:1808, :1622).
    #[tokio::test]
    async fn count_checkpoints_before_each_shot() {
        let d = Arc::new(FakeReadable("det".into())) as Arc<dyn ReadableObj>;
        let msgs = drain(count(vec![d], 3)).await;
        assert_eq!(
            msgs.iter().filter(|m| matches!(m, Msg::Checkpoint)).count(),
            3,
            "one Checkpoint per shot"
        );
        for (idx, m) in msgs.iter().enumerate() {
            if matches!(m, Msg::Create { .. }) {
                assert!(
                    idx > 0 && matches!(msgs[idx - 1], Msg::Checkpoint),
                    "Create at {idx} not immediately preceded by Checkpoint"
                );
            }
        }
    }

    // count_with_trigger opens each shot with a Checkpoint, before the
    // (optional) trigger and the Create.
    #[tokio::test]
    async fn count_with_trigger_checkpoints_each_shot() {
        let t = Arc::new(FakeTriggerable("det".into())) as Arc<dyn TriggerableObj>;
        let d = Arc::new(FakeReadable("det".into())) as Arc<dyn ReadableObj>;
        let msgs = drain(count_with_trigger(vec![d], vec![t], 2)).await;
        assert_eq!(
            msgs.iter().filter(|m| matches!(m, Msg::Checkpoint)).count(),
            2,
            "one Checkpoint per shot"
        );
        // The first per-shot message is the Checkpoint, ahead of the Trigger.
        let first_cp = msgs.iter().position(|m| matches!(m, Msg::Checkpoint));
        let first_trig = msgs.iter().position(|m| matches!(m, Msg::Trigger { .. }));
        assert!(
            matches!((first_cp, first_trig), (Some(c), Some(t)) if c < t),
            "Checkpoint must precede the shot's Trigger"
        );
    }

    // Standalone one_shot is a single checkpointed acquisition (bluesky
    // one_shot, plan_stubs.py:1621-1623).
    #[tokio::test]
    async fn one_shot_checkpoints_before_acquisition() {
        let d = Arc::new(FakeReadable("det".into())) as Arc<dyn ReadableObj>;
        let msgs = drain(stubs::one_shot(vec![], vec![d])).await;
        assert!(
            matches!(msgs.first(), Some(Msg::Checkpoint)),
            "one_shot must open with a Checkpoint, got {:?}",
            msgs.first()
        );
        assert_eq!(
            msgs.iter().filter(|m| matches!(m, Msg::Checkpoint)).count(),
            1
        );
    }

    fn set_values(msgs: &[Msg]) -> Vec<f64> {
        msgs.iter()
            .filter_map(|m| match m {
                Msg::Set { value, .. } => Some(*value),
                _ => None,
            })
            .collect()
    }

    #[tokio::test]
    async fn rel_log_scan_offsets_log_spaced_targets_by_current_readback() {
        // log_scan(1, 100, 3) → log-spaced points [1, 10, 100]; with a current
        // readback of 10 every target shifts by +10 → [11, 20, 110].
        let motor = Arc::new(FakeMotor {
            name: "m".into(),
            bias: 10.0,
        }) as Arc<dyn cirrus_core::msg::LocatableObj>;
        let reader = Arc::new(FakeReadable("m_rbv".into())) as Arc<dyn ReadableObj>;
        let plan = rel_log_scan(vec![], motor, reader, 1.0, 100.0, 3);
        let msgs = drain(plan).await;
        let vals = scan_set_values(&msgs);
        assert_eq!(vals.len(), 3, "expected 3 scan Set targets, got {vals:?}");
        for (got, want) in vals.iter().zip([11.0, 20.0, 110.0]) {
            assert!(
                (got - want).abs() < 1e-9,
                "Set target {got} != expected {want} (bias-offset log point)"
            );
        }
        // After the scan the motor returns to its starting readback (10).
        assert_eq!(
            named_reset_sets(&msgs),
            vec![("m".to_string(), 10.0)],
            "rel_log_scan must reset the motor to start"
        );
    }

    #[tokio::test]
    async fn rel_log_scan_zero_bias_matches_absolute_log_scan() {
        // With a current readback of 0 the relative scan reduces to log_scan.
        let motor = Arc::new(FakeMotor {
            name: "m".into(),
            bias: 0.0,
        }) as Arc<dyn cirrus_core::msg::LocatableObj>;
        let reader = Arc::new(FakeReadable("m_rbv".into())) as Arc<dyn ReadableObj>;
        let plan = rel_log_scan(vec![], motor, reader, 1.0, 100.0, 3);
        let vals = set_values(&drain(plan).await);
        for (got, want) in vals.iter().zip([1.0, 10.0, 100.0]) {
            assert!((got - want).abs() < 1e-9, "Set target {got} != {want}");
        }
    }

    fn named_set_values(msgs: &[Msg]) -> Vec<(String, f64)> {
        msgs.iter()
            .filter_map(|m| match m {
                Msg::Set { obj, value, .. } => Some((obj.name().to_string(), *value)),
                _ => None,
            })
            .collect()
    }

    /// Set targets from the scan body only (excludes the `reset` epilogue that
    /// returns relative-scan motors to their starting positions).
    fn scan_set_values(msgs: &[Msg]) -> Vec<f64> {
        msgs.iter()
            .filter_map(|m| match m {
                Msg::Set { value, group, .. } if group.as_deref() != Some("reset") => Some(*value),
                _ => None,
            })
            .collect()
    }

    /// Named Set targets from the scan body only (excludes the `reset` epilogue).
    fn named_scan_sets(msgs: &[Msg]) -> Vec<(String, f64)> {
        msgs.iter()
            .filter_map(|m| match m {
                Msg::Set { obj, value, group } if group.as_deref() != Some("reset") => {
                    Some((obj.name().to_string(), *value))
                }
                _ => None,
            })
            .collect()
    }

    /// Named Set targets from the `reset` epilogue only — the moves that return
    /// each motor to its starting readback after a relative scan.
    fn named_reset_sets(msgs: &[Msg]) -> Vec<(String, f64)> {
        msgs.iter()
            .filter_map(|m| match m {
                Msg::Set { obj, value, group } if group.as_deref() == Some("reset") => {
                    Some((obj.name().to_string(), *value))
                }
                _ => None,
            })
            .collect()
    }

    fn motor_xy(
        bx: f64,
        by: f64,
    ) -> (
        Arc<dyn cirrus_core::msg::LocatableObj>,
        Arc<dyn cirrus_core::msg::LocatableObj>,
    ) {
        (
            Arc::new(FakeMotor {
                name: "x".into(),
                bias: bx,
            }) as Arc<dyn cirrus_core::msg::LocatableObj>,
            Arc::new(FakeMotor {
                name: "y".into(),
                bias: by,
            }) as Arc<dyn cirrus_core::msg::LocatableObj>,
        )
    }

    fn abs_motor_xy() -> (Arc<dyn MovableObj>, Arc<dyn MovableObj>) {
        (
            Arc::new(FakeMotor {
                name: "x".into(),
                bias: 0.0,
            }) as Arc<dyn MovableObj>,
            Arc::new(FakeMotor {
                name: "y".into(),
                bias: 0.0,
            }) as Arc<dyn MovableObj>,
        )
    }

    fn rdr(n: &str) -> Arc<dyn ReadableObj> {
        Arc::new(FakeReadable(n.into())) as Arc<dyn ReadableObj>
    }

    /// Drain `abs` and `rel` (the same 2-D scan, absolute vs relative with
    /// readbacks `bx`/`by`) and assert each Set target shifts by the matching
    /// motor's readback: x-targets by `bx`, y-targets by `by`.
    async fn assert_xy_relative_offsets(abs: Plan, rel: Plan, bx: f64, by: f64) {
        let abs_sets = named_scan_sets(&drain(abs).await);
        let rel_msgs = drain(rel).await;
        let rel_sets = named_scan_sets(&rel_msgs);
        assert_eq!(abs_sets.len(), rel_sets.len(), "Set count must match");
        assert!(!abs_sets.is_empty(), "plan produced no Set targets");
        for ((an, av), (rn, rv)) in abs_sets.iter().zip(&rel_sets) {
            assert_eq!(an, rn, "motor order must match");
            let bias = if an == "x" { bx } else { by };
            assert!(
                (rv - (av + bias)).abs() < 1e-9,
                "{rn}: relative {rv} != absolute {av} + bias {bias}"
            );
        }
        // After the scan both motors return to their starting readbacks.
        assert_eq!(
            named_reset_sets(&rel_msgs),
            vec![("x".to_string(), bx), ("y".to_string(), by)],
            "rel scan must reset both motors to start"
        );
    }

    #[tokio::test]
    async fn rel_spiral_centres_pattern_on_current_readbacks() {
        let (axm, aym) = abs_motor_xy();
        let abs = spiral(
            vec![],
            axm,
            rdr("xr"),
            aym,
            rdr("yr"),
            0.0,
            0.0,
            2.0,
            2.0,
            0.5,
            8,
        );
        let (xm, ym) = motor_xy(5.0, 7.0);
        let rel = rel_spiral(
            vec![],
            xm,
            rdr("xr"),
            ym,
            rdr("yr"),
            0.0,
            0.0,
            2.0,
            2.0,
            0.5,
            8,
        );
        assert_xy_relative_offsets(abs, rel, 5.0, 7.0).await;
    }

    #[tokio::test]
    async fn rel_spiral_square_centres_pattern_on_current_readbacks() {
        let (axm, aym) = abs_motor_xy();
        let abs = spiral_square(
            vec![],
            axm,
            rdr("xr"),
            aym,
            rdr("yr"),
            0.0,
            0.0,
            2.0,
            2.0,
            3,
            3,
        );
        let (xm, ym) = motor_xy(5.0, 7.0);
        let rel = rel_spiral_square(
            vec![],
            xm,
            rdr("xr"),
            ym,
            rdr("yr"),
            0.0,
            0.0,
            2.0,
            2.0,
            3,
            3,
        );
        assert_xy_relative_offsets(abs, rel, 5.0, 7.0).await;
    }

    #[tokio::test]
    async fn rel_spiral_fermat_centres_pattern_on_current_readbacks() {
        let (axm, aym) = abs_motor_xy();
        let abs = spiral_fermat(
            vec![],
            axm,
            rdr("xr"),
            aym,
            rdr("yr"),
            0.0,
            0.0,
            2.0,
            2.0,
            0.5,
            1.0,
        );
        let (xm, ym) = motor_xy(5.0, 7.0);
        let rel = rel_spiral_fermat(
            vec![],
            xm,
            rdr("xr"),
            ym,
            rdr("yr"),
            0.0,
            0.0,
            2.0,
            2.0,
            0.5,
            1.0,
        );
        assert_xy_relative_offsets(abs, rel, 5.0, 7.0).await;
    }

    #[tokio::test]
    async fn rel_list_grid_scan_offsets_each_axis_by_its_readback() {
        // x list [1,2] with readback 10 → [11,12]; y list [5] with readback
        // 20 → [25]. The outer product visits (11,25) then (12,25). y is the
        // slow axis and holds 25 across both points, so it is Set once and
        // skipped on the second point — bluesky's move_per_step pos_cache
        // (plan_stubs.py:1698). The unchanged motor is not re-commanded.
        let (xm, ym) = motor_xy(10.0, 20.0);
        let axes: Vec<RelListGridAxis> =
            vec![(xm, rdr("xr"), vec![1.0, 2.0]), (ym, rdr("yr"), vec![5.0])];
        let msgs = drain(rel_list_grid_scan(vec![], axes)).await;
        let sets = named_scan_sets(&msgs);
        let expected = [("x", 11.0), ("y", 25.0), ("x", 12.0)];
        assert_eq!(sets.len(), expected.len(), "got {sets:?}");
        for ((gn, gv), (en, ev)) in sets.iter().zip(expected) {
            assert_eq!(gn, en, "motor order");
            assert!((gv - ev).abs() < 1e-9, "{gn}: {gv} != {ev}");
        }
        // Each axis returns to its starting readback after the scan.
        assert_eq!(
            named_reset_sets(&msgs),
            vec![("x".to_string(), 10.0), ("y".to_string(), 20.0)],
            "rel_list_grid_scan must reset every axis to start"
        );
    }

    #[tokio::test]
    async fn rel_scan_returns_motor_to_supplied_current() {
        // current 10, offsets -2..2 over 3 points → absolute targets [8, 10, 12];
        // the reset epilogue then returns the motor to `current` (10) so the
        // relative scan leaves no net motion.
        let motor = Arc::new(FakeMotor {
            name: "m".into(),
            bias: 0.0,
        }) as Arc<dyn MovableObj>;
        let msgs = drain(rel_scan(vec![], motor, rdr("m_rbv"), 10.0, -2.0, 2.0, 3)).await;
        let vals = scan_set_values(&msgs);
        assert_eq!(vals.len(), 3, "expected 3 scan Set targets, got {vals:?}");
        for (got, want) in vals.iter().zip([8.0, 10.0, 12.0]) {
            assert!((got - want).abs() < 1e-9, "Set target {got} != {want}");
        }
        assert_eq!(
            named_reset_sets(&msgs),
            vec![("m".to_string(), 10.0)],
            "rel_scan must return the motor to the supplied current"
        );
    }

    #[tokio::test]
    async fn reset_positions_only_resets_motors_the_plan_moved() {
        // Two eligible motors; the inner plan moves only `moved`. bluesky stashes
        // a motor's reset position lazily at its first `set` (OrderedDict via
        // insert_reads, preprocessors.py:1177-1189), so a listed-but-unmoved
        // motor is never restored. Eager capture at wrapper entry wrongly reset
        // `unmoved` to its start position as well.
        let moved = Arc::new(FakeMotor {
            name: "moved".into(),
            bias: 5.0,
        }) as Arc<dyn cirrus_core::msg::LocatableObj>;
        let unmoved = Arc::new(FakeMotor {
            name: "unmoved".into(),
            bias: 3.0,
        }) as Arc<dyn cirrus_core::msg::LocatableObj>;
        let moved_mv: Arc<dyn MovableObj> = moved.clone();
        let inner = plan_box(async_stream::stream! {
            yield Msg::Set { obj: moved_mv, value: 9.0, group: None };
        });
        let msgs = drain(preprocessors::reset_positions_wrapper(
            inner,
            vec![moved, unmoved],
        ))
        .await;
        assert_eq!(
            named_reset_sets(&msgs),
            vec![("moved".to_string(), 5.0)],
            "reset must restore only the moved motor, not the untouched one"
        );
    }

    #[tokio::test]
    async fn relative_set_locates_only_motors_the_plan_moves() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        // bluesky inserts __read_and_stash_a_motor lazily at the first `set` per
        // motor (preprocessors.py:1136-1148), so a listed motor the plan never
        // moves is never located, and the moved motor's base is captured at that
        // first set. Eager snapshot at wrapper entry located *every* listed motor.
        let moved_locates = Arc::new(AtomicUsize::new(0));
        let unmoved_locates = Arc::new(AtomicUsize::new(0));
        let moved = Arc::new(CountingMotor {
            name: "moved".into(),
            setpoint: 5.0,
            locates: moved_locates.clone(),
        });
        let unmoved = Arc::new(CountingMotor {
            name: "unmoved".into(),
            setpoint: 3.0,
            locates: unmoved_locates.clone(),
        });
        let moved_mv: Arc<dyn MovableObj> = moved.clone();
        let inner = plan_box(async_stream::stream! {
            yield Msg::Set { obj: moved_mv, value: 2.0, group: None };
        });
        let msgs = drain(preprocessors::relative_set_wrapper(
            inner,
            vec![
                moved as Arc<dyn cirrus_core::msg::LocatableObj>,
                unmoved as Arc<dyn cirrus_core::msg::LocatableObj>,
            ],
        ))
        .await;
        assert_eq!(
            moved_locates.load(Ordering::SeqCst),
            1,
            "the moved motor must be located exactly once, at its first set"
        );
        assert_eq!(
            unmoved_locates.load(Ordering::SeqCst),
            0,
            "a listed motor the plan never moves must never be located"
        );
        // The moved set is biased by the lazily-captured base: 2.0 + 5.0 = 7.0.
        assert_eq!(
            set_values(&msgs),
            vec![7.0],
            "the moved set must be biased by its lazily-captured base"
        );
    }

    // Each scan step is a rewind boundary: a Checkpoint precedes every
    // step Set (bluesky one_1d_step.move(), plan_stubs.py:1669).
    #[tokio::test]
    async fn scan_checkpoints_before_each_step() {
        let motor = Arc::new(FakeMotor {
            name: "m".into(),
            bias: 0.0,
        }) as Arc<dyn MovableObj>;
        let msgs = drain(scan(vec![], motor, rdr("m_rbv"), 0.0, 10.0, 3)).await;
        assert_eq!(
            msgs.iter().filter(|m| matches!(m, Msg::Checkpoint)).count(),
            3,
            "one Checkpoint per step"
        );
        for (idx, m) in msgs.iter().enumerate() {
            if matches!(m, Msg::Set { group, .. } if group.as_deref() == Some("set")) {
                assert!(
                    idx > 0 && matches!(msgs[idx - 1], Msg::Checkpoint),
                    "step Set at {idx} not immediately preceded by Checkpoint"
                );
            }
        }
    }

    // A delegating rel_ plan inherits the base plan's per-step checkpoints.
    #[tokio::test]
    async fn rel_scan_inherits_step_checkpoints() {
        let motor = Arc::new(FakeMotor {
            name: "m".into(),
            bias: 0.0,
        }) as Arc<dyn MovableObj>;
        let msgs = drain(rel_scan(vec![], motor, rdr("m_rbv"), 10.0, -2.0, 2.0, 3)).await;
        assert_eq!(
            msgs.iter().filter(|m| matches!(m, Msg::Checkpoint)).count(),
            3,
            "rel_scan delegates to scan, inheriting its per-step checkpoints"
        );
    }

    // A 2x2 grid emits one Checkpoint per slow-axis row plus one per point.
    #[tokio::test]
    async fn grid_scan_checkpoints_rows_and_points() {
        let m1 = Arc::new(FakeMotor {
            name: "m1".into(),
            bias: 0.0,
        }) as Arc<dyn MovableObj>;
        let m2 = Arc::new(FakeMotor {
            name: "m2".into(),
            bias: 0.0,
        }) as Arc<dyn MovableObj>;
        let msgs = drain(grid_scan(
            vec![],
            m1,
            rdr("m1r"),
            0.0,
            1.0,
            2,
            m2,
            rdr("m2r"),
            0.0,
            1.0,
            2,
        ))
        .await;
        assert_eq!(
            msgs.iter().filter(|m| matches!(m, Msg::Checkpoint)).count(),
            6,
            "grid_scan: one Checkpoint per row (2) + one per point (4)"
        );
    }

    #[tokio::test]
    async fn scan_nd_skips_resetting_an_unchanged_motor() {
        // bluesky's move_per_step pos_cache (plan_stubs.py:1698) skips a motor
        // whose target equals its last-set position. In an N-D grid the slow
        // axis stays constant across a row's inner points, so it must be Set
        // once, not re-commanded every point. Two points: m0 (slow) stays at
        // 0.0, m1 (fast) moves 0.0 -> 1.0.
        let m0 = Arc::new(FakeMotor {
            name: "m0".into(),
            bias: 0.0,
        }) as Arc<dyn MovableObj>;
        let m1 = Arc::new(FakeMotor {
            name: "m1".into(),
            bias: 0.0,
        }) as Arc<dyn MovableObj>;
        let motors = vec![(m0, rdr("m0r")), (m1, rdr("m1r"))];
        let points = vec![vec![0.0, 0.0], vec![0.0, 1.0]];
        let msgs = drain(scan_nd(vec![], motors, points)).await;
        let sets_for = |name: &str| {
            msgs.iter()
                .filter(|m| matches!(m, Msg::Set { obj, .. } if obj.name() == name))
                .count()
        };
        assert_eq!(
            sets_for("m0"),
            1,
            "the unchanged slow motor m0 is Set once, not re-commanded each point"
        );
        assert_eq!(
            sets_for("m1"),
            2,
            "the moving fast motor m1 is Set on both points"
        );
    }

    #[tokio::test]
    async fn rel_set_offsets_by_readback_and_does_not_wait() {
        let motor = Arc::new(FakeMotor {
            name: "m".into(),
            bias: 100.0,
        }) as Arc<dyn cirrus_core::msg::LocatableObj>;
        let msgs = drain(stubs::rel_set(motor, 5.0, Some("g".into()))).await;
        // Exactly one Set, offset by the readback (100 + 5), no trailing Wait.
        assert_eq!(msgs.len(), 1, "expected a single Set, got {msgs:?}");
        match &msgs[0] {
            Msg::Set { obj, value, group } => {
                assert_eq!(obj.name(), "m");
                assert!((value - 105.0).abs() < 1e-9, "target {value} != 105");
                assert_eq!(group.as_deref(), Some("g"));
            }
            other => panic!("expected Msg::Set, got {other:?}"),
        }
        assert!(
            !msgs.iter().any(|m| matches!(m, Msg::Wait { .. })),
            "rel_set must not emit Wait (unlike mvr)"
        );
    }

    #[tokio::test]
    async fn relative_moves_base_on_setpoint_not_readback() {
        // Motor whose commanded setpoint (5.0) differs from its actual readback
        // (4.0) — the only case where the two relative bases diverge. bluesky
        // stashes location["setpoint"] for a Locatable
        // (__read_and_stash_a_motor), so a +2 relative move targets 7.0, not
        // 6.0.
        struct SplitMotor;
        impl NamedObj for SplitMotor {
            fn name(&self) -> &str {
                "split"
            }
        }
        #[async_trait::async_trait]
        impl MovableObj for SplitMotor {
            async fn set_dyn(&self, _value: f64) -> Status {
                Status::done()
            }
        }
        #[async_trait::async_trait]
        impl cirrus_core::msg::LocatableObj for SplitMotor {
            async fn locate_dyn(&self) -> Result<DynLocation, cirrus_core::error::CirrusError> {
                Ok(DynLocation {
                    setpoint: 5.0,
                    readback: 4.0,
                })
            }
        }

        fn set_value(msgs: &[Msg]) -> f64 {
            msgs.iter()
                .find_map(|m| match m {
                    Msg::Set { value, .. } => Some(*value),
                    _ => None,
                })
                .expect("a Set message")
        }

        let motor: Arc<dyn cirrus_core::msg::LocatableObj> = Arc::new(SplitMotor);

        // rel_set(+2): 5.0 (setpoint) + 2 = 7.0, not 4.0 (readback) + 2 = 6.0.
        let msgs = drain(stubs::rel_set(motor.clone(), 2.0, None)).await;
        assert!(
            (set_value(&msgs) - 7.0).abs() < 1e-9,
            "rel_set must base on setpoint 5.0 (→7.0), not readback 4.0 (→6.0): got {}",
            set_value(&msgs)
        );

        // mvr(+2): same setpoint base.
        let msgs = drain(stubs::mvr(motor.clone(), 2.0)).await;
        assert!(
            (set_value(&msgs) - 7.0).abs() < 1e-9,
            "mvr must base on setpoint (→7.0): got {}",
            set_value(&msgs)
        );

        // relative_set_wrapper rewrites an inner Set(+2) to setpoint + 2 = 7.0.
        let mv: Arc<dyn MovableObj> = Arc::new(SplitMotor);
        let inner = plan_box(async_stream::stream! {
            yield Msg::Set { obj: mv, value: 2.0, group: None };
        });
        let msgs = drain(preprocessors::relative_set_wrapper(inner, vec![motor])).await;
        assert!(
            (set_value(&msgs) - 7.0).abs() < 1e-9,
            "relative_set_wrapper must base on setpoint (→7.0): got {}",
            set_value(&msgs)
        );
    }

    #[tokio::test]
    async fn x2x_scan_couples_motors_2to1_relative_to_readbacks() {
        // motor1 sweeps 0→4 (readback 10); motor2 sweeps the half range 0→2
        // (readback 100). inner_product(3) → m1 [0,2,4], m2 [0,1,2].
        let (m1, m2) = motor_xy(10.0, 100.0);
        let plan = x2x_scan(vec![], m1, rdr("xr"), m2, rdr("yr"), 0.0, 4.0, 3);
        let sets = named_set_values(&drain(plan).await);
        let expected = [
            ("x", 10.0),
            ("y", 100.0),
            ("x", 12.0),
            ("y", 101.0),
            ("x", 14.0),
            ("y", 102.0),
        ];
        assert_eq!(sets.len(), expected.len(), "got {sets:?}");
        for ((gn, gv), (en, ev)) in sets.iter().zip(expected) {
            assert_eq!(gn, en, "motor order");
            assert!((gv - ev).abs() < 1e-9, "{gn}: {gv} != {ev}");
        }
    }

    #[tokio::test]
    async fn kickoff_all_kicks_each_then_waits_shared_group() {
        let msgs = drain(stubs::kickoff_all(flyers(3), None, true)).await;
        // 3 Kickoff + 1 Wait, all sharing one process-unique default group.
        assert_eq!(msgs.len(), 4);
        let g = kickoff_group(&msgs[0]).expect("kickoff group").to_string();
        assert!(
            g.starts_with("kickoff_all-"),
            "default group must be short_uid-minted, got {g:?}"
        );
        for m in &msgs[..3] {
            assert_eq!(kickoff_group(m), Some(g.as_str()));
        }
        assert!(matches!(
            &msgs[3],
            Msg::Wait { group, error_on_timeout: true, timeout: None } if *group == g
        ));
    }

    // Two None-group calls to the same stub must mint DIFFERENT default sync
    // groups, so a stub's internal default can never collide with a user group
    // — or with another invocation's. Mirrors bluesky's per-call short_uid; the
    // fixed-literal fallback this replaced returned the same name every time.
    #[tokio::test]
    async fn default_sync_groups_are_unique_per_invocation() {
        let a = drain(stubs::kickoff_all(flyers(1), None, true)).await;
        let b = drain(stubs::kickoff_all(flyers(1), None, true)).await;
        let ga = kickoff_group(&a[0]).expect("group a").to_string();
        let gb = kickoff_group(&b[0]).expect("group b").to_string();
        assert!(ga.starts_with("kickoff_all-") && gb.starts_with("kickoff_all-"));
        assert_ne!(ga, gb, "each invocation must mint a distinct default group");
    }

    #[tokio::test]
    async fn kickoff_all_no_wait_omits_wait_and_honors_group() {
        let msgs = drain(stubs::kickoff_all(flyers(2), Some("g".into()), false)).await;
        assert_eq!(msgs.len(), 2);
        assert!(msgs.iter().all(|m| kickoff_group(m) == Some("g")));
        assert!(!msgs.iter().any(|m| matches!(m, Msg::Wait { .. })));
    }

    #[tokio::test]
    async fn complete_all_completes_each_then_waits_when_requested() {
        let msgs = drain(stubs::complete_all(flyers(2), None, true)).await;
        assert_eq!(msgs.len(), 3);
        let g = complete_group(&msgs[0])
            .expect("complete group")
            .to_string();
        assert!(
            g.starts_with("complete_all-"),
            "default group must be short_uid-minted, got {g:?}"
        );
        for m in &msgs[..2] {
            assert_eq!(complete_group(m), Some(g.as_str()));
        }
        assert!(matches!(
            &msgs[2],
            Msg::Wait { group, .. } if *group == g
        ));
    }

    #[tokio::test]
    async fn complete_all_default_no_wait_emits_only_completes() {
        // bluesky's complete_all defaults wait=false; this exercises that path.
        let msgs = drain(stubs::complete_all(flyers(2), None, false)).await;
        assert_eq!(msgs.len(), 2);
        let g = complete_group(&msgs[0])
            .expect("complete group")
            .to_string();
        assert!(g.starts_with("complete_all-"), "got {g:?}");
        assert!(msgs.iter().all(|m| complete_group(m) == Some(g.as_str())));
        assert!(!msgs.iter().any(|m| matches!(m, Msg::Wait { .. })));
    }

    fn preparable(name: &str) -> Arc<dyn PreparableObj> {
        Arc::new(FakePreparable(name.into())) as Arc<dyn PreparableObj>
    }

    // wait=true: Prepare carries the value and a wait-group, followed by a Wait
    // on that same group. group=None mints a process-unique "prepare-N" default
    // via short_uid, and an explicit group passes through to both messages.
    #[tokio::test]
    async fn prepare_with_wait_emits_prepare_then_wait_on_same_group() {
        // Default group when none is given.
        let val = serde_json::json!({"trigger": "internal"});
        let msgs = drain(stubs::prepare(preparable("det"), val.clone(), None, true)).await;
        assert_eq!(msgs.len(), 2);
        let g = match &msgs[0] {
            Msg::Prepare { value, group, .. } => {
                assert_eq!(value, &val, "value must thread through unchanged");
                let g = group.clone().expect("default group minted");
                assert!(
                    g.starts_with("prepare-"),
                    "default group must be short_uid-minted, got {g:?}"
                );
                g
            }
            other => panic!("first msg not Prepare: {other:?}"),
        };
        assert!(matches!(
            &msgs[1],
            Msg::Wait { group, error_on_timeout: true, timeout: None } if *group == g
        ));

        // Explicit group reaches both the Prepare and its Wait.
        let msgs = drain(stubs::prepare(
            preparable("det"),
            val,
            Some("g".into()),
            true,
        ))
        .await;
        assert!(matches!(&msgs[0], Msg::Prepare { group, .. } if group.as_deref() == Some("g")));
        assert!(matches!(&msgs[1], Msg::Wait { group, .. } if group == "g"));
    }

    // wait=false: only the Prepare is emitted, no Wait, and the caller's group
    // passes through verbatim — including None (no fallback minted).
    #[tokio::test]
    async fn prepare_no_wait_emits_only_prepare_and_preserves_group() {
        let val = serde_json::json!(null);
        let msgs = drain(stubs::prepare(
            preparable("det"),
            val.clone(),
            Some("g".into()),
            false,
        ))
        .await;
        assert_eq!(msgs.len(), 1);
        assert!(matches!(&msgs[0], Msg::Prepare { group, .. } if group.as_deref() == Some("g")));
        assert!(!msgs.iter().any(|m| matches!(m, Msg::Wait { .. })));

        // None group is preserved (not defaulted) when not waiting.
        let msgs = drain(stubs::prepare(preparable("det"), val, None, false)).await;
        assert_eq!(msgs.len(), 1);
        assert!(matches!(&msgs[0], Msg::Prepare { group: None, .. }));
    }

    // wait_for emits a single Msg::WaitFor carrying the supplied factories and
    // timeout verbatim; each factory must remain callable (produces a future).
    #[tokio::test]
    async fn wait_for_emits_single_msg_with_factories_and_timeout() {
        let f0: AwaitableFactory = Arc::new(|| Box::pin(async { Ok(()) }));
        let f1: AwaitableFactory = Arc::new(|| Box::pin(async { Ok(()) }));
        let msgs = drain(stubs::wait_for(vec![f0, f1], Some(Duration::from_secs(2)))).await;
        assert_eq!(msgs.len(), 1);
        match &msgs[0] {
            Msg::WaitFor { factories, timeout } => {
                assert_eq!(factories.len(), 2);
                assert_eq!(*timeout, Some(Duration::from_secs(2)));
                // The factory is invocable and yields a completing future.
                factories[0]().await.unwrap();
            }
            other => panic!("expected Msg::WaitFor, got {other:?}"),
        }
    }

    // No timeout passes through as None (indefinite wait).
    #[tokio::test]
    async fn wait_for_preserves_none_timeout() {
        let f0: AwaitableFactory = Arc::new(|| Box::pin(async { Ok(()) }));
        let msgs = drain(stubs::wait_for(vec![f0], None)).await;
        assert_eq!(msgs.len(), 1);
        assert!(matches!(
            &msgs[0],
            Msg::WaitFor { factories, timeout: None } if factories.len() == 1
        ));
    }

    // delay > 0: a Checkpoint precedes each repetition's messages and a
    // time-compensated Sleep follows each (including the last, per bluesky's
    // scalar-delay flow). The compensated sleep never exceeds the target delay.
    #[tokio::test]
    async fn repeat_checkpoints_each_iteration_and_sleeps_when_delay_positive() {
        let delay = Duration::from_millis(100);
        // Inner plan yields exactly one Msg::Null per repetition.
        let msgs = drain(stubs::repeat(stubs::null, Some(3), delay)).await;

        let checkpoints = msgs.iter().filter(|m| matches!(m, Msg::Checkpoint)).count();
        let nulls = msgs.iter().filter(|m| matches!(m, Msg::Null)).count();
        let sleeps: Vec<Duration> = msgs
            .iter()
            .filter_map(|m| match m {
                Msg::Sleep(d) => Some(*d),
                _ => None,
            })
            .collect();
        assert_eq!(checkpoints, 3);
        assert_eq!(nulls, 3);
        assert_eq!(
            sleeps.len(),
            3,
            "scalar delay sleeps after every repetition"
        );
        for d in &sleeps {
            assert!(
                *d <= delay,
                "compensated sleep never exceeds the target cadence"
            );
            assert!(
                *d > Duration::ZERO,
                "a fast no-op plan leaves nearly the full delay to sleep"
            );
        }
        // Every Null repetition is immediately preceded by its Checkpoint.
        for (idx, m) in msgs.iter().enumerate() {
            if matches!(m, Msg::Null) {
                assert!(
                    idx > 0 && matches!(msgs[idx - 1], Msg::Checkpoint),
                    "Null at index {idx} not immediately preceded by Checkpoint"
                );
            }
        }
    }

    // delay == 0: checkpoints still bracket each repetition, but no Sleep.
    #[tokio::test]
    async fn repeat_zero_delay_emits_no_sleep() {
        let msgs = drain(stubs::repeat(stubs::null, Some(2), Duration::ZERO)).await;
        // Exact sequence: Checkpoint, Null, Checkpoint, Null.
        assert_eq!(msgs.len(), 4);
        assert_eq!(
            msgs.iter().filter(|m| matches!(m, Msg::Checkpoint)).count(),
            2
        );
        assert_eq!(msgs.iter().filter(|m| matches!(m, Msg::Null)).count(), 2);
        assert!(
            !msgs.iter().any(|m| matches!(m, Msg::Sleep(_))),
            "delay=0 emits no Sleep"
        );
    }

    // num == 0: zero repetitions, nothing emitted (not even a checkpoint).
    #[tokio::test]
    async fn repeat_num_zero_yields_nothing() {
        let msgs = drain(stubs::repeat(
            stubs::null,
            Some(0),
            Duration::from_millis(10),
        ))
        .await;
        assert!(msgs.is_empty(), "num=0 runs no iterations");
    }
}
