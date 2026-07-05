//! Plans for bsrs — equivalents of `bluesky.plans` and `bluesky.plan_stubs`.

#![deny(missing_docs)]

pub mod patterns;
pub mod preprocessors;

use crate::core::msg::{
    AwaitableFactory, CollectableObj, ConfigurableObj, ConfigureArgs, FlyableObj, LocatableObj,
    MonitorableObj, MovableObj, Msg, MsgResult, PreparableObj, ReadableObj, RunMetadata,
    StageableObj, StoppableObj, TriggerableObj,
};
use crate::core::plan::{plan_box, plan_items, respond, Plan, PlanItem};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Mint a process-unique synchronization-group name carrying a human-readable
/// `label` prefix — bsrs's port of bluesky's `short_uid(label)`
/// (`utils/__init__.py`). A stub that lets the caller supply a sync group but
/// falls back to a default when none is given uses this for the fallback, so
/// the default can never collide with a user-chosen group of the same name.
///
/// bluesky appends a uuid4 fragment for this isolation; bsrs appends a
/// monotonic process-global counter, which is equally unique within a process
/// and needs no extra dependency. The `label-N` shape stays readable in
/// message dumps and tests (match it with `starts_with(label)`).
fn short_uid(label: &str) -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    format!("{label}-{}", COUNTER.fetch_add(1, Ordering::Relaxed))
}

/// How a scan's motors map onto the independent axes reported in the
/// `dimensions` hint. Coupled motors (inner-product) move together and form a
/// single combined axis; grid (outer-product) motors are independent axes, one
/// per motor; a `Time` series (`count`) has no motor and reports the implicit
/// `time` axis. Mirrors the split between bluesky's `derive_default_hints`
/// (coupled, plans.py:58-63) and the per-motor `motor_hints` in the
/// outer-product plans (plans.py:350).
#[derive(Clone, Copy, PartialEq, Eq)]
enum AxisGrouping {
    /// One combined axis over all motors' hinted fields (inner product).
    Coupled,
    /// One axis per motor (outer product / grid).
    Grid,
    /// No motor; the implicit `time` axis (`count`).
    Time,
}

/// A motor reader's hinted axis fields — its `hint_fields()`, or its own name
/// when it declares none. Ports bluesky `motor.hints["fields"]`, which defaults
/// to `[motor.name]`.
fn motor_hint_fields(reader: &Arc<dyn ReadableObj>) -> Vec<String> {
    reader
        .hint_fields()
        .unwrap_or_else(|| vec![reader.name().to_string()])
}

/// Assemble the RunStart metadata bluesky's scan-family plans inject so a
/// consumer (BEC / LiveTable / LiveFit) can label axes and size the scan:
/// the device-name lists (`detectors`, `motors`), the point counts
/// (`num_points`, `num_intervals`), and the `dimensions` hint grouped per
/// [`AxisGrouping`]. Rides the same `RunMetadata::extra` -> `RunStart` path as
/// `plan_name`/`scan_id`, so every key lands as a top-level RunStart field.
/// Ports the `_md` dicts in bluesky/plans.py (count 104-116, outer_product
/// 336-352) plus `derive_default_hints` (plans.py:58-63).
fn scan_run_md(
    plan_name: &str,
    detectors: &[Arc<dyn ReadableObj>],
    motors: &[Arc<dyn ReadableObj>],
    num_points: Option<usize>,
    grouping: AxisGrouping,
) -> RunMetadata {
    use crate::event_model::{DimensionItem, Hints};
    use serde_json::Value;

    let mut extra: std::collections::HashMap<String, Value> = std::collections::HashMap::new();
    extra.insert(
        "detectors".into(),
        Value::from(
            detectors
                .iter()
                .map(|d| d.name().to_string())
                .collect::<Vec<_>>(),
        ),
    );
    if !motors.is_empty() {
        extra.insert(
            "motors".into(),
            Value::from(
                motors
                    .iter()
                    .map(|m| m.name().to_string())
                    .collect::<Vec<_>>(),
            ),
        );
    }
    if let Some(n) = num_points {
        extra.insert("num_points".into(), Value::from(n));
        // bluesky: `num_intervals = num - 1` (plans.py:106).
        extra.insert("num_intervals".into(), Value::from(n.saturating_sub(1)));
    }
    // One `[fields, "primary"]` entry per independent axis.
    let axes: Vec<Vec<String>> = match grouping {
        AxisGrouping::Time => vec![vec!["time".to_string()]],
        AxisGrouping::Coupled => vec![motors.iter().flat_map(motor_hint_fields).collect()],
        AxisGrouping::Grid => motors.iter().map(motor_hint_fields).collect(),
    };
    let dimensions: Vec<Vec<DimensionItem>> = axes
        .into_iter()
        .filter(|fields| !fields.is_empty())
        .map(|fields| {
            vec![
                DimensionItem::Fields(fields),
                DimensionItem::Name("primary".to_string()),
            ]
        })
        .collect();
    let hints = Hints {
        dimensions: if dimensions.is_empty() {
            None
        } else {
            Some(dimensions)
        },
    };
    // `serde_json::to_value(Hints)` cannot fail (no maps with non-string keys,
    // no non-finite floats); unwrap is a real invariant, not a silenced error.
    extra.insert(
        "hints".into(),
        serde_json::to_value(hints).expect("Hints serializes to a JSON object"),
    );
    RunMetadata {
        plan_name: Some(plan_name.to_string()),
        extra,
        ..Default::default()
    }
}

// ===========================================================================
//  plan_stubs (single-Msg / small composites; mirrors bluesky.plan_stubs)
// ===========================================================================

/// `bluesky.plan_stubs` equivalents — single- or few-`Msg` helpers that are
/// the building blocks of compound plans.
pub mod stubs {
    use super::*;

    /// Remove redundant (identical) entries from a device list, preserving
    /// first-appearance order. bsrs's port of bluesky's `separate_devices`
    /// (utils/__init__.py:773) for a flat device model: bluesky filters out any
    /// device that has another listed device as an ancestor, and since
    /// `ancestry(obj)` starts with `obj` itself, an exact duplicate is dropped
    /// (`[A, A] -> [A]`). bsrs has no device parent/child hierarchy, so the
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
        data_keys: std::collections::HashMap<String, crate::event_model::DataKey>,
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

    /// `mv_many(moves)` — bluesky's variadic `mv(*args)` (plan_stubs.py:357):
    /// fire every `(motor, target)` into ONE shared group, then wait once, so
    /// the motors move in parallel behind a single barrier. Daily beamline
    /// pattern (e.g. sample position + detector distance together). The
    /// single-motor [`mv`] is the one-element case.
    pub fn mv_many(moves: Vec<(Arc<dyn MovableObj>, f64)>) -> Plan {
        plan_box(async_stream::stream! {
            for (motor, value) in &moves {
                yield Msg::Set { obj: motor.clone(), value: *value, group: Some("mv".into()) };
            }
            // One wait for the whole group — parallel motion, single barrier.
            yield Msg::Wait { group: "mv".into(), error_on_timeout: true, timeout: None };
        })
    }

    /// `mv(motor, value)` — set + wait. The single-motor case of [`mv_many`].
    pub fn mv(motor: Arc<dyn MovableObj>, value: f64) -> Plan {
        mv_many(vec![(motor, value)])
    }

    /// `mvr_many(moves)` — relative multi-motor move: each motor's absolute
    /// target is its own current setpoint (via `LocatableObj::locate_dyn`) plus
    /// its `delta`. Every target is resolved FIRST (all reads before any
    /// motion), then all `Set`s fire into one shared group and a single `Wait`
    /// follows — so a `locate_dyn` failure on any motor aborts the run via
    /// `Msg::Fail` before a single motor starts moving. Bases on the setpoint,
    /// not the readback, matching bluesky's `relative_set_wrapper`
    /// (`__read_and_stash_a_motor`). The single-motor [`mvr`] is the
    /// one-element case.
    pub fn mvr_many(moves: Vec<(Arc<dyn LocatableObj>, f64)>) -> Plan {
        plan_box(async_stream::stream! {
            // Read every setpoint before yielding any Set, so a locate failure
            // fails the run before any motion begins.
            let mut targets: Vec<(Arc<dyn MovableObj>, f64)> = Vec::with_capacity(moves.len());
            for (motor, delta) in moves {
                let loc = match motor.locate_dyn().await {
                    Ok(l) => l,
                    Err(e) => {
                        // Fail the run cleanly via Msg::Fail rather than
                        // panicking the plan task. The engine's Fail handler
                        // closes the run with exit_status="fail".
                        yield Msg::Fail(format!("mvr({}): locate_dyn failed: {e}", motor.name()));
                        return;
                    }
                };
                targets.push((motor as Arc<dyn MovableObj>, loc.setpoint + delta));
            }
            for (motor, value) in &targets {
                yield Msg::Set { obj: motor.clone(), value: *value, group: Some("mv".into()) };
            }
            yield Msg::Wait { group: "mv".into(), error_on_timeout: true, timeout: None };
        })
    }

    /// `mvr(motor, delta)` — relative move. The single-motor case of
    /// [`mvr_many`]. Motor must implement `LocatableObj` (which extends
    /// `MovableObj`).
    pub fn mvr(motor: Arc<dyn LocatableObj>, delta: f64) -> Plan {
        mvr_many(vec![(motor, delta)])
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

    /// `wait_for(factories, timeout)` — emit `Msg::WaitFor`. The bsrs
    /// equivalent of bluesky's `wait_for`: each factory produces a fresh future
    /// and the engine starts them all up front, awaiting them *concurrently*
    /// (bluesky's `[ensure_future(f()) for f in futs]` + `asyncio.wait`). An
    /// optional `timeout` bounds the single concurrent wait, after which the
    /// engine returns `BsrsError::Timeout`. Unlike [`wait`], which waits on a
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
    /// can always be waited on; bsrs-plans carries no uuid dependency, so a
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

    /// `collect_while_completing(flyers, dets, flush_period, stream_name)`.
    ///
    /// Kicks off `complete` on every flyer without waiting, then repeatedly
    /// `Wait`s on the group for up to `flush_period` (move-on:
    /// `error_on_timeout = false`) and `collect`s every detector once, until the
    /// group reports done. With `flush_period = None` the `Wait` blocks until the
    /// flyers finish, yielding a single terminal collect; with `Some(period)` the
    /// detectors are flushed each period while the flyers run. Mirrors bluesky's
    /// `collect_while_completing` (plan_stubs.py). bluesky's `watch` groups are
    /// consumer-side progress reporting and have no bsrs equivalent, so they are
    /// omitted.
    ///
    /// This is the canonical consumer of the plan↔engine response channel: each
    /// loop turn yields a [`respond`]-carrying `Wait` and awaits the engine's
    /// [`MsgResult::WaitComplete`] to decide whether to iterate again.
    pub fn collect_while_completing(
        flyers: Vec<Arc<dyn FlyableObj>>,
        dets: Vec<Arc<dyn CollectableObj>>,
        flush_period: Option<Duration>,
        stream_name: Option<String>,
    ) -> Plan {
        let group = short_uid("complete");
        plan_items(async_stream::stream! {
            // complete_all(flyers, group, wait=false): kick every flyer off
            // against the shared group, do not block here.
            for f in flyers {
                yield PlanItem::from(Msg::Complete { obj: f, group: Some(group.clone()) });
            }
            loop {
                let (item, rx) = respond(Msg::Wait {
                    group: group.clone(),
                    error_on_timeout: false,
                    timeout: flush_period,
                });
                yield item;
                let done = match rx.await {
                    Ok(MsgResult::WaitComplete { done }) => done,
                    // Sender dropped (the engine failed the `Wait` and tore the
                    // run down) or an unexpected result: stop instead of looping
                    // forever. No further collect — the engine is done with us.
                    _ => break,
                };
                for d in &dets {
                    yield PlanItem::from(Msg::Collect {
                        obj: d.clone(),
                        stream_name: stream_name.clone(),
                    });
                }
                if done {
                    break;
                }
            }
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

    /// `broadcast_msg(objs, make)` — fan one command across many objects,
    /// yielding `make(obj)` for each. bsrs's typed analog of bluesky's
    /// `broadcast_msg(command, objs, *args, **kwargs)` (plan_stubs.py:1489),
    /// which builds `Msg(command, obj, ...)` per object.
    ///
    /// bsrs's `Msg` is a typed enum, not a `(command_str, obj)` pair, so the
    /// caller supplies the per-object message builder rather than a command
    /// string. The fixed-command fans above (`stage_all`, `unstage_all`,
    /// `kickoff_all`, `complete_all`) are specializations of this shape — and
    /// stage/unstage are exactly what bluesky uses `broadcast_msg` for
    /// (`cntx.py:169,173`). bluesky also collects each message's engine return
    /// value; a bsrs `Plan` yields into the engine with no caller-visible
    /// return, so only the fan-out is ported (use [`respond`] when a per-object
    /// result is actually needed).
    pub fn broadcast_msg<T>(
        objs: Vec<Arc<T>>,
        make: impl Fn(Arc<T>) -> Msg + Send + 'static,
    ) -> Plan
    where
        T: ?Sized + Send + Sync + 'static,
    {
        plan_box(async_stream::stream! {
            for o in objs {
                yield make(o);
            }
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
            // bluesky wraps the reads in a contingency: on a read exception the
            // open bundle is `drop`ped (not `save`d) and the error re-raised;
            // on success the bundle is `save`d (plan_stubs.py:1466-1481). Port
            // that so a mid-bundle read failure discards the partial bundle
            // through the sanctioned Drop path instead of relying on the run-end
            // bundler teardown, and so a future caller that catches the failure
            // never inherits a half-open bundle.
            let read_plan = {
                let readables = readables.clone();
                plan_box(async_stream::stream! {
                    for r in &readables {
                        yield Msg::Read(r.clone());
                    }
                })
            };
            let guarded = preprocessors::contingency_wrapper(
                read_plan,
                Some(drop_bundle()),
                Some(save()),
                None,
                true,
            );
            let mut guarded = guarded;
            while let Some(item) = futures::StreamExt::next(&mut guarded).await {
                // Internal Bare-only sub-plan: no `Respond` item to preserve.
                let (PlanItem::Bare(m) | PlanItem::Respond(m, _)) = item;
                yield m;
            }
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
                // Internal Bare-only sub-plan: no `Respond` item to preserve.
                let (PlanItem::Bare(m) | PlanItem::Respond(m, _)) = item;
                yield m;
            }
        })
    }

    /// `repeater(n, plan)` — run `plan` `n` times. Each call to `plan_fn`
    /// builds a fresh Plan (so it can yield more than once).
    pub fn repeater<F>(n: usize, mut plan_fn: F) -> Plan
    where
        F: FnMut() -> Plan + Send + 'static,
    {
        plan_items(async_stream::stream! {
            for _ in 0..n {
                let mut p = plan_fn();
                while let Some(item) = futures::StreamExt::next(&mut p).await {
                    // User-supplied plan: preserve whole items so a `Respond`
                    // (e.g. `repeater(n, || collect_while_completing(...))`) keeps
                    // its response channel across repetitions.
                    yield item;
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
        plan_items(async_stream::stream! {
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
                yield PlanItem::from(Msg::Checkpoint);
                let mut p = plan_fn();
                while let Some(item) = futures::StreamExt::next(&mut p).await {
                    // User-supplied plan: preserve whole items so a `Respond`
                    // survives repetition (see `repeater`).
                    yield item;
                }
                if !delay.is_zero() {
                    let elapsed = start.elapsed();
                    if delay > elapsed {
                        yield PlanItem::from(Msg::Sleep(delay - elapsed));
                    }
                }
                i += 1;
            }
        })
    }

    /// `move_per_step(motors)` — the move half of one step: a `Checkpoint`, a
    /// `Set` for each motor that carries a target (`Some`), then a single `Wait`
    /// on the shared `"set"` group. A motor with a `None` target is already at
    /// position and is not re-commanded (bluesky `move_per_step`'s
    /// skip-unchanged, plan_stubs.py:1678). The `Wait` is emitted
    /// unconditionally, matching bluesky — a wait on an empty group is a no-op.
    /// A building block for custom [`PerStep`](super::PerStep) hooks.
    pub fn move_per_step(motors: Vec<super::StepMotor>) -> Plan {
        plan_box(async_stream::stream! {
            yield Msg::Checkpoint;
            for (m, _, target) in &motors {
                if let Some(pos) = target {
                    yield Msg::Set { obj: m.clone(), value: *pos, group: Some("set".into()) };
                }
            }
            yield Msg::Wait { group: "set".into(), error_on_timeout: true, timeout: None };
        })
    }

    /// `one_nd_step(detectors, motors)` — the default [`PerStep`](super::PerStep):
    /// [`move_per_step`] then read the motor readers and detectors into
    /// `"primary"` as a `Create → Read* → Save` bundle. bsrs's port of bluesky's
    /// `one_nd_step` (plan_stubs.py:1707); reads without triggering (bsrs step
    /// scans take plain `Readable` detectors, not `Triggerable`).
    pub fn one_nd_step(
        detectors: Vec<Arc<dyn ReadableObj>>,
        motors: Vec<super::StepMotor>,
    ) -> Plan {
        plan_box(async_stream::stream! {
            let mut mv = move_per_step(motors.clone());
            while let Some(item) = futures::StreamExt::next(&mut mv).await {
                // Internal Bare-only sub-plan: no `Respond` item to preserve.
                let (PlanItem::Bare(m) | PlanItem::Respond(m, _)) = item;
                yield m;
            }
            yield Msg::Create { stream_name: "primary".into() };
            for (_, reader, _) in &motors {
                yield Msg::Read(reader.clone());
            }
            for d in &detectors {
                yield Msg::Read(d.clone());
            }
            yield Msg::Save;
        })
    }

    /// `read_shot(detectors)` — the default read-only [`PerShot`](super::PerShot)
    /// for [`count`](super::count): a `Checkpoint` then read the detectors into
    /// `"primary"` as a `Create → Read* → Save` bundle, without triggering. This
    /// is the plain-read analogue of [`one_shot`], which triggers via
    /// `trigger_and_read`; `count` takes plain `Readable` detectors, so its
    /// default does not trigger.
    pub fn read_shot(detectors: Vec<Arc<dyn ReadableObj>>) -> Plan {
        plan_box(async_stream::stream! {
            yield Msg::Checkpoint;
            yield Msg::Create { stream_name: "primary".into() };
            for d in &detectors {
                yield Msg::Read(d.clone());
            }
            yield Msg::Save;
        })
    }
}

// ===========================================================================
//  plans (compound; mirrors bluesky.plans)
// ===========================================================================

/// One motor's role in a scan step: its handle, its reader, and — if it should
/// be commanded this step — the target position. A `None` target means the motor
/// is already at position and must not be re-commanded (bluesky `move_per_step`'s
/// skip-unchanged); the step still reads it. Used by [`PerStep`] hooks.
pub type StepMotor = (Arc<dyn MovableObj>, Arc<dyn ReadableObj>, Option<f64>);

/// Per-step hook for step scans: given the detectors and this step's motors,
/// yield the Msgs for one inner-loop step (move + read). bsrs's analogue of
/// bluesky's `per_step`; the default is [`stubs::one_nd_step`]. A custom hook
/// replaces the entire move-and-read step, so it owns its own `Checkpoint`,
/// moves, and reads.
pub type PerStep = Arc<dyn Fn(Vec<Arc<dyn ReadableObj>>, Vec<StepMotor>) -> Plan + Send + Sync>;

/// Per-shot hook for `count`: given the detectors, yield the Msgs for one shot.
/// bsrs's analogue of bluesky's `per_shot`; the default is [`stubs::read_shot`].
pub type PerShot = Arc<dyn Fn(Vec<Arc<dyn ReadableObj>>) -> Plan + Send + Sync>;

/// Inter-shot delay for [`count_ext`]. Ports bluesky's `count` `delay`
/// (`ScalarOrIterableFloat`, plans.py:66). The delay is time-compensated: the
/// emitted `Sleep` is the target minus the wall-clock the shot itself took, so a
/// slow shot shortens the sleep and never lengthens the cadence (matching
/// bluesky's `d - (now - then)` and bsrs's [`stubs::repeat`]).
#[derive(Debug, Clone, Default)]
pub enum CountDelay {
    /// No delay between shots (bluesky `delay=0.0`).
    #[default]
    None,
    /// The same target delay after every shot (bluesky scalar `delay`). Applied
    /// after every shot, including the last — mirroring bluesky, whose scalar
    /// delay is an infinite `itertools.repeat`, and bsrs's [`stubs::repeat`].
    Every(Duration),
    /// One target delay per inter-shot interval (bluesky iterable `delay`);
    /// entry `i` is applied after shot `i`. For a finite `num` this must have at
    /// least `num - 1` entries, or the plan fails immediately with `Msg::Fail`,
    /// mirroring bluesky's `ValueError`. When the sequence is exhausted the run
    /// closes (bluesky's `StopIteration → break`), so no trailing sleep follows
    /// the final delivered entry.
    Sequence(Vec<Duration>),
}

/// `count(detectors, num)` — read each detector `num` times. Convenience form of
/// [`count_per_shot`] with the default per-shot action ([`stubs::one_shot`]).
pub fn count(detectors: Vec<Arc<dyn ReadableObj>>, num: usize) -> Plan {
    count_per_shot(detectors, num, None)
}

/// `count_per_shot(detectors, num, per_shot)` — read the detectors `num` times,
/// delegating each shot to `per_shot`. Convenience form of [`count_ext`] with a
/// finite `num` and no delay. `None` uses [`stubs::read_shot`]
/// (`Checkpoint → Create → Read* → Save`), byte-for-byte the previous `count`
/// body. Ports bluesky's `count` `per_shot` hook (plans.py:66): a custom hook can
/// trigger before reading, read into a different stream, or repeat a shot.
pub fn count_per_shot(
    detectors: Vec<Arc<dyn ReadableObj>>,
    num: usize,
    per_shot: Option<PerShot>,
) -> Plan {
    count_ext(detectors, Some(num), CountDelay::None, per_shot)
}

/// Collect the distinct [`StageableObj`] devices among `detectors` + `motors`
/// — bluesky stages `list(detectors) + motors` before a run and unstages after.
/// Deduplicated by identity so a device appearing as both a reader and a motor
/// is staged once; devices that are not stageable ([`ReadableObj::as_stageable`]
/// / [`MovableObj::as_stageable`] → `None`, e.g. a bare motor or a plain
/// readable) are skipped — the static-typing analogue of bluesky staging only
/// the objects that expose a `stage()` method. A compound plan feeds the result
/// to [`preprocessors::stage_wrapper`]; when nothing is stageable (the common
/// case for sim/test devices) the wrapper emits no `Stage`/`Unstage` and the
/// message stream is unchanged.
fn stageables_for(
    detectors: &[Arc<dyn ReadableObj>],
    motors: &[Arc<dyn MovableObj>],
) -> Vec<Arc<dyn StageableObj>> {
    let mut out: Vec<Arc<dyn StageableObj>> = Vec::new();
    let candidates = detectors
        .iter()
        .cloned()
        .filter_map(|d| d.as_stageable())
        .chain(motors.iter().cloned().filter_map(|m| m.as_stageable()));
    for s in candidates {
        if !out.iter().any(|o| Arc::ptr_eq(o, &s)) {
            out.push(s);
        }
    }
    out
}

/// `count_ext(detectors, num, delay, per_shot)` — the full `count`: read the
/// detectors `num` times (or forever when `num` is `None`), sleeping `delay`
/// between shots and delegating each shot to `per_shot`. Ports bluesky's `count`
/// (plans.py:66) `num`/`delay`/`per_shot`:
///
/// - `num = None` acquires indefinitely until the engine cancels (bluesky's
///   `num=None`); `Some(n)` takes exactly `n` shots.
/// - `delay` is time-compensated per [`CountDelay`].
/// - `per_shot = None` uses [`stubs::read_shot`].
///
/// Each shot carries exactly one `Checkpoint` (inside `per_shot`); unlike bluesky
/// `count == repeat(one_shot)`, which double-checkpoints (both the `repeat` and
/// the `one_shot` emit one), bsrs keeps the single per-shot Checkpoint it already
/// used, so it does not route through [`stubs::repeat`].
pub fn count_ext(
    detectors: Vec<Arc<dyn ReadableObj>>,
    num: Option<usize>,
    delay: CountDelay,
    per_shot: Option<PerShot>,
) -> Plan {
    // Upfront delay-length validation, mirroring bluesky's `ValueError` when a
    // finite `num` outruns an explicit delay sequence (needs `num - 1`
    // intervals). Fail before staging or opening the run so an invalid `count`
    // arms nothing and leaks no partial run.
    if let (Some(n), CountDelay::Sequence(seq)) = (num, &delay) {
        if n > 1 && seq.len() < n - 1 {
            let msg = format!(
                "count: num={n} needs at least {} delay entries but got {}",
                n - 1,
                seq.len()
            );
            return plan_box(async_stream::stream! {
                yield Msg::Fail(msg);
            });
        }
    }
    // The default per_shot carries the per-shot Checkpoint (bluesky one_shot:
    // plan_stubs.py:1622), so a pause/resume rewinds only the current shot.
    let per_shot = per_shot.unwrap_or_else(|| Arc::new(stubs::read_shot) as PerShot);
    // Stage the detectors before the run and unstage after (PLAN-09); `count`
    // has no motors. Non-stageable detectors contribute nothing.
    let staged = stageables_for(&detectors, &[]);
    let inner = plan_box(async_stream::stream! {
        yield Msg::OpenRun(scan_run_md("count", &detectors, &[], num, AxisGrouping::Time));
        let mut i: usize = 0;
        loop {
            if let Some(n) = num {
                if i >= n {
                    break;
                }
            }
            // Captured before the shot so the compensation covers the shot's own
            // duration (bluesky's `now = time.time()` before the plan runs).
            let start = std::time::Instant::now();
            let mut shot = per_shot(detectors.clone());
            while let Some(item) = futures::StreamExt::next(&mut shot).await {
                // Internal Bare-only sub-plan: no `Respond` item to preserve.
                let (PlanItem::Bare(m) | PlanItem::Respond(m, _)) = item;
                yield m;
            }
            // Next inter-shot delay. `None` from a Sequence means it is exhausted
            // (bluesky's `StopIteration`), which ends the run.
            let target: Option<Duration> = match &delay {
                CountDelay::None => Some(Duration::ZERO),
                CountDelay::Every(d) => Some(*d),
                CountDelay::Sequence(seq) => seq.get(i).copied(),
            };
            match target {
                None => break,
                Some(d) => {
                    if !d.is_zero() {
                        let elapsed = start.elapsed();
                        if d > elapsed {
                            yield Msg::Sleep(d - elapsed);
                        }
                    }
                }
            }
            i += 1;
        }
        yield Msg::CloseRun {
            exit_status: "success".into(),
            reason: None,
        };
    });
    preprocessors::stage_wrapper(inner, staged)
}

/// `count_with_trigger(detectors, num)` — trigger then read each iteration.
pub fn count_with_trigger(
    detectors: Vec<Arc<dyn ReadableObj>>,
    triggerables: Vec<Arc<dyn TriggerableObj>>,
    num: usize,
) -> Plan {
    plan_box(async_stream::stream! {
        yield Msg::OpenRun(scan_run_md(
            "count_with_trigger",
            &detectors,
            &[],
            Some(num),
            AxisGrouping::Time,
        ));
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

/// `scan(detectors, axes, num)` — the canonical N-motor step scan: move every
/// axis simultaneously (inner product) from its `start` to its `stop` in `num`
/// steps, reading the detectors at each point. Ports bluesky's `scan`
/// (plans.py:1185), which moves all listed motors together and emits
/// `plan_name = "scan"`. A single-axis call is an ordinary 1-D scan; the
/// [`scan_1d`] convenience takes the motor/bounds inline for that common case.
/// Convenience form of [`scan_per_step`] with the default per-step action.
pub fn scan(detectors: Vec<Arc<dyn ReadableObj>>, axes: Vec<ScanAxis>, num: usize) -> Plan {
    scan_per_step(detectors, axes, num, None)
}

/// N-motor `scan` delegating each point to `per_step` (default
/// [`stubs::one_nd_step`]). Same inner-product traversal as
/// [`inner_product_scan_per_step`] but emits `plan_name = "scan"` (bluesky's
/// canonical name). Ports bluesky's `scan` `per_step` hook (plans.py:1096).
pub fn scan_per_step(
    detectors: Vec<Arc<dyn ReadableObj>>,
    axes: Vec<ScanAxis>,
    num: usize,
    per_step: Option<PerStep>,
) -> Plan {
    inner_product_core(detectors, num, axes, per_step, "scan")
}

/// 1-D step `scan` from `start` to `stop` (inclusive) in `num` steps — the
/// single-motor convenience form of [`scan`]. Ports the common `scan(dets, m,
/// s, e, num=N)` shape; emits `plan_name = "scan"` like the N-motor form.
/// Convenience form of [`scan_1d_per_step`] with the default per-step action
/// ([`stubs::one_nd_step`]).
pub fn scan_1d(
    detectors: Vec<Arc<dyn ReadableObj>>,
    motor: Arc<dyn MovableObj>,
    motor_reader: Arc<dyn ReadableObj>,
    start: f64,
    stop: f64,
    num: usize,
) -> Plan {
    scan_1d_per_step(detectors, motor, motor_reader, start, stop, num, None)
}

/// 1-D step `scan` delegating each step to `per_step`. `None` uses
/// [`stubs::one_nd_step`] (`Checkpoint → Set → Wait → Create → Read* → Save`),
/// byte-for-byte the previous 1-D `scan` body. Ports bluesky's `scan` `per_step`
/// hook (plans.py:1096): a custom hook owns the whole move-and-read step, so it
/// can trigger detectors, read into extra streams, or settle differently.
pub fn scan_1d_per_step(
    detectors: Vec<Arc<dyn ReadableObj>>,
    motor: Arc<dyn MovableObj>,
    motor_reader: Arc<dyn ReadableObj>,
    start: f64,
    stop: f64,
    num: usize,
    per_step: Option<PerStep>,
) -> Plan {
    let step = if num > 1 {
        (stop - start) / (num as f64 - 1.0)
    } else {
        0.0
    };
    let per_step = per_step.unwrap_or_else(|| Arc::new(stubs::one_nd_step) as PerStep);
    // Stage detectors + the motor before the run, unstage after (PLAN-09).
    let staged = stageables_for(&detectors, std::slice::from_ref(&motor));
    let inner = plan_box(async_stream::stream! {
        yield Msg::OpenRun(scan_run_md(
            "scan",
            &detectors,
            std::slice::from_ref(&motor_reader),
            Some(num),
            AxisGrouping::Coupled,
        ));
        for i in 0..num {
            let pos = start + step * (i as f64);
            let motors: Vec<StepMotor> = vec![(motor.clone(), motor_reader.clone(), Some(pos))];
            let mut sub = per_step(detectors.clone(), motors);
            while let Some(item) = futures::StreamExt::next(&mut sub).await {
                // Internal Bare-only sub-plan: no `Respond` item to preserve.
                let (PlanItem::Bare(m) | PlanItem::Respond(m, _)) = item;
                yield m;
            }
        }
        yield Msg::CloseRun {
            exit_status: "success".into(),
            reason: None,
        };
    });
    preprocessors::stage_wrapper(inner, staged)
}

/// `list_scan(detectors, motor, points)` — visit each position in `points`,
/// reading detectors at each.
pub fn list_scan(
    detectors: Vec<Arc<dyn ReadableObj>>,
    motor: Arc<dyn MovableObj>,
    motor_reader: Arc<dyn ReadableObj>,
    points: Vec<f64>,
) -> Plan {
    list_scan_per_step(detectors, motor, motor_reader, points, None)
}

/// 1-D `list_scan` over explicit `points`, delegating each step to `per_step`.
/// `None` uses [`stubs::one_nd_step`] (`Checkpoint → Set → Wait → Create →
/// Read* → Save`), byte-for-byte the previous `list_scan` body. Ports bluesky's
/// `list_scan` `per_step` hook (plans.py:576).
pub fn list_scan_per_step(
    detectors: Vec<Arc<dyn ReadableObj>>,
    motor: Arc<dyn MovableObj>,
    motor_reader: Arc<dyn ReadableObj>,
    points: Vec<f64>,
    per_step: Option<PerStep>,
) -> Plan {
    let per_step = per_step.unwrap_or_else(|| Arc::new(stubs::one_nd_step) as PerStep);
    // Stage detectors + the motor before the run, unstage after (PLAN-09).
    let staged = stageables_for(&detectors, std::slice::from_ref(&motor));
    let inner = plan_box(async_stream::stream! {
        yield Msg::OpenRun(scan_run_md(
            "list_scan",
            &detectors,
            std::slice::from_ref(&motor_reader),
            Some(points.len()),
            AxisGrouping::Coupled,
        ));
        for pos in points {
            let motors: Vec<StepMotor> = vec![(motor.clone(), motor_reader.clone(), Some(pos))];
            let mut sub = per_step(detectors.clone(), motors);
            while let Some(item) = futures::StreamExt::next(&mut sub).await {
                // Internal Bare-only sub-plan: no `Respond` item to preserve.
                let (PlanItem::Bare(m) | PlanItem::Respond(m, _)) = item;
                yield m;
            }
        }
        yield Msg::CloseRun {
            exit_status: "success".into(),
            reason: None,
        };
    });
    preprocessors::stage_wrapper(inner, staged)
}

/// `list_scan_nd(detectors, axes, per_step)` — multi-motor **inner-product**
/// list scan: every axis's Nth position is visited together, so the axes'
/// position lists are zipped, not crossed (bluesky's multi-motor
/// `list_scan(dets, m1, [pts1], m2, [pts2], …)`, plans.py:132, which builds
/// `inner_list_product` then `scan_nd`). The single-axis [`list_scan`] is the
/// one-motor convenience; this is the general form.
///
/// All position lists must be the same length. A mismatch fails the run before
/// it opens by emitting [`Msg::Fail`] — mirroring bluesky's `ValueError`, and
/// matching how [`count_ext`] rejects a too-short delay sequence — rather than
/// silently visiting an empty trajectory (which is what a bare
/// `inner_list_product` returns on unequal lengths).
///
/// Like bluesky's `scan_nd`/`move_per_step`, a motor is not re-commanded at a
/// point where its target equals its previous one (the `pos_cache` in
/// [`scan_nd_with_md`]). All motors are reported as one combined axis in the
/// `dimensions` hint (bluesky's coupled `derive_default_hints`), unlike the
/// per-axis Grid hint of [`list_grid_scan`].
pub fn list_scan_nd(
    detectors: Vec<Arc<dyn ReadableObj>>,
    axes: Vec<ListScanAxis>,
    per_step: Option<PerStep>,
) -> Plan {
    // Equal-length check up front, mirroring bluesky's `ValueError`. Without it,
    // `inner_list_product` silently returns an empty trajectory on a mismatch,
    // hiding the user's error as a zero-point run.
    if let Some((first, rest)) = axes.split_first() {
        let len0 = first.2.len();
        if let Some(bad) = rest.iter().find(|a| a.2.len() != len0) {
            let msg = format!(
                "list_scan: all position lists must be the same length; \
                 '{}' has {} but '{}' has {}",
                first.0.name(),
                len0,
                bad.0.name(),
                bad.2.len()
            );
            return plan_box(async_stream::stream! {
                yield Msg::Fail(msg);
            });
        }
    }
    let lists: Vec<Vec<f64>> = axes.iter().map(|(_, _, l)| l.clone()).collect();
    let pts = patterns::inner_list_product(&lists);
    let motors: Vec<(Arc<dyn MovableObj>, Arc<dyn ReadableObj>)> =
        axes.into_iter().map(|(m, r, _)| (m, r)).collect();
    let motor_readers: Vec<Arc<dyn ReadableObj>> =
        motors.iter().map(|(_, mr)| mr.clone()).collect();
    // Inner product: all motors form one combined axis (Coupled), like the
    // single-motor `list_scan`, not `list_grid_scan`'s per-motor Grid axes.
    let md = scan_run_md(
        "list_scan",
        &detectors,
        &motor_readers,
        Some(pts.len()),
        AxisGrouping::Coupled,
    );
    scan_nd_with_md(detectors, motors, pts, md, per_step)
}

/// `rel_scan(detectors, motor, start, stop, num)` — like `scan` but
/// `start`/`stop` are relative to the motor's current position. Caller
/// supplies `current` (read off the motor before invoking).
///
/// After the scan's run closes, the motor is returned to `current`,
/// mirroring bluesky's `reset_positions_decorator` on `rel_scan`
/// (`plans.py:1591`). Like bsrs's other plan-level brackets, the reset
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
    let inner = scan_1d(
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
            // Internal Bare-only sub-plan: no `Respond` item to preserve.
            let (PlanItem::Bare(m) | PlanItem::Respond(m, _)) = item;
            yield m;
        }
        // `current` is the readback the caller snapshotted before the scan;
        // return the motor there so a relative scan leaves no net motion.
        yield Msg::Set { obj: reset_motor, value: current, group: Some("reset".into()) };
        yield Msg::Wait { group: "reset".into(), error_on_timeout: true, timeout: None };
    })
}

/// `grid_scan(dets, m1, s1, e1, n1, m2, s2, e2, n2)` — 2-D rectilinear scan.
/// `m1` is the slow axis (outer loop), `m2` is the fast axis (inner loop).
/// Every grid point the detectors are read once into `primary`. Natural
/// (non-snaked) order; the non-snaked form of [`grid_scan_snake`].
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
    grid_scan_snake(
        detectors,
        motor1,
        motor1_reader,
        s1,
        e1,
        n1,
        motor2,
        motor2_reader,
        s2,
        e2,
        n2,
        false,
    )
}

/// `grid_scan_snake(..., snake)` — 2-D scan where `snake = true` traverses the
/// fast axis (`m2`) in boustrophedon order: forward on even slow-axis rows,
/// reversed on odd ones, so the stage never flies back across the row. Mirrors
/// bluesky `grid_scan(..., snake_axes=True)`, which snakes the fast axis and
/// never the slowest (plans.py:1294). The slow axis and every emitted document
/// are otherwise identical to [`grid_scan`]; only the fast-axis position order
/// within alternate rows changes.
#[allow(clippy::too_many_arguments)]
pub fn grid_scan_snake(
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
    snake: bool,
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
    // Stage detectors + both motors before the run, unstage after (PLAN-09).
    let staged = stageables_for(&detectors, &[motor1.clone(), motor2.clone()]);
    let inner = plan_box(async_stream::stream! {
        yield Msg::OpenRun(scan_run_md(
            "grid_scan",
            &detectors,
            &[motor1_reader.clone(), motor2_reader.clone()],
            Some(n1 * n2),
            AxisGrouping::Grid,
        ));
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
                // Snake: reverse the fast axis on odd slow-axis rows so the
                // stage winds back and forth instead of flying back each row
                // (bluesky snake_cyclers, utils/__init__.py:656).
                let jj = if snake && i % 2 == 1 { n2 - 1 - j } else { j };
                let p2 = s2 + step2 * (jj as f64);
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
    });
    preprocessors::stage_wrapper(inner, staged)
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

/// One axis of a multi-motor inner-product [`list_scan_nd`]:
/// `(motor, motor_reader, points)`. Every axis's `points` list must be the same
/// length — the lists are zipped position-by-position (bluesky's
/// `inner_list_product`), unlike the outer-product [`ListGridAxis`].
pub type ListScanAxis = (Arc<dyn MovableObj>, Arc<dyn ReadableObj>, Vec<f64>);

/// `inner_product_scan(dets, num, [(motor1, s1, e1), ...])` — all motors move
/// together (linspaced) for `num` points. Mirrors bluesky's
/// `inner_product_scan` for the typical positional-only argument shape.
pub fn inner_product_scan(
    detectors: Vec<Arc<dyn ReadableObj>>,
    num: usize,
    axes: Vec<ScanAxis>,
) -> Plan {
    inner_product_scan_per_step(detectors, num, axes, None)
}

/// N-motor coupled `inner_product_scan`, delegating each point to `per_step`.
/// `None` uses [`stubs::one_nd_step`] (`Checkpoint → Set* → Wait → Create →
/// Read* → Save`, all motors commanded), byte-for-byte the previous
/// `inner_product_scan` body. Ports bluesky's `inner_product_scan` `per_step`
/// hook (plans.py:942).
pub fn inner_product_scan_per_step(
    detectors: Vec<Arc<dyn ReadableObj>>,
    num: usize,
    axes: Vec<ScanAxis>,
    per_step: Option<PerStep>,
) -> Plan {
    inner_product_core(detectors, num, axes, per_step, "inner_product_scan")
}

/// Shared inner-product traversal for [`scan`] and [`inner_product_scan`]: move
/// every axis together (all commanded each point) from `start` to `stop` in
/// `num` steps, delegating each point to `per_step` (default
/// [`stubs::one_nd_step`]). The two public entry points differ only in the
/// emitted `plan_name`, so it is a parameter here; the traversal lives in one
/// place.
fn inner_product_core(
    detectors: Vec<Arc<dyn ReadableObj>>,
    num: usize,
    axes: Vec<ScanAxis>,
    per_step: Option<PerStep>,
    plan_name: &'static str,
) -> Plan {
    let bounds: Vec<(f64, f64)> = axes.iter().map(|(_, _, s, e)| (*s, *e)).collect();
    let pts = patterns::inner_product(num, &bounds);
    let per_step = per_step.unwrap_or_else(|| Arc::new(stubs::one_nd_step) as PerStep);
    // Stage detectors + every axis motor before the run, unstage after (PLAN-09).
    let stage_motors: Vec<Arc<dyn MovableObj>> =
        axes.iter().map(|(m, _, _, _)| m.clone()).collect();
    let staged = stageables_for(&detectors, &stage_motors);
    let inner = plan_box(async_stream::stream! {
        let motor_readers: Vec<Arc<dyn ReadableObj>> =
            axes.iter().map(|(_, mr, _, _)| mr.clone()).collect();
        yield Msg::OpenRun(scan_run_md(
            plan_name,
            &detectors,
            &motor_readers,
            Some(num),
            AxisGrouping::Coupled,
        ));
        for row in pts {
            // Every axis is commanded each point (Some), matching the previous
            // unconditional Set-all loop.
            let motors: Vec<StepMotor> = axes
                .iter()
                .zip(row.iter())
                .map(|((m, mr, _, _), val)| (m.clone(), mr.clone(), Some(*val)))
                .collect();
            let mut sub = per_step(detectors.clone(), motors);
            while let Some(item) = futures::StreamExt::next(&mut sub).await {
                // Internal Bare-only sub-plan: no `Respond` item to preserve.
                let (PlanItem::Bare(m) | PlanItem::Respond(m, _)) = item;
                yield m;
            }
        }
        yield Msg::CloseRun { exit_status: "success".into(), reason: None };
    });
    preprocessors::stage_wrapper(inner, staged)
}

/// `x2x_scan(dets, motor1, m1_reader, motor2, m2_reader, start, stop, num)` —
/// coupled 2:1 *relative* inner-product scan (bluesky `plans.x2x_scan`,
/// a generalised theta-2theta). `motor1` sweeps `start..stop` relative to its
/// current setpoint while `motor2` sweeps `start/2..stop/2` relative to its
/// own; the two move together each step. Built from [`inner_product_scan`]
/// run through `relative_set_wrapper`.
///
/// As with bsrs's other `rel_*` scans, the motors are not returned to their
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
/// accepts `cycler` objects, this one takes the pre-computed list. The motors
/// are reported as a single combined axis in the `dimensions` hint (bluesky's
/// `derive_default_hints` default, plans.py:58-63); outer-product callers that
/// want one axis per motor use [`scan_nd_with_md`] with a `Grid`-grouped md.
pub fn scan_nd(
    detectors: Vec<Arc<dyn ReadableObj>>,
    motors: Vec<(Arc<dyn MovableObj>, Arc<dyn ReadableObj>)>,
    points: Vec<Vec<f64>>,
) -> Plan {
    scan_nd_per_step(detectors, motors, points, None)
}

/// `scan_nd` delegating each point to `per_step`. `None` uses
/// [`stubs::one_nd_step`], which reproduces the previous `scan_nd` traversal
/// byte-for-byte (each point moves only the motors whose target differs from
/// their last-set position — the `pos_cache`/`StepMotor::None` skip — then reads
/// all motor readers and detectors). Ports bluesky's `scan_nd` `per_step` hook
/// (plans.py:271).
pub fn scan_nd_per_step(
    detectors: Vec<Arc<dyn ReadableObj>>,
    motors: Vec<(Arc<dyn MovableObj>, Arc<dyn ReadableObj>)>,
    points: Vec<Vec<f64>>,
    per_step: Option<PerStep>,
) -> Plan {
    let motor_readers: Vec<Arc<dyn ReadableObj>> =
        motors.iter().map(|(_, mr)| mr.clone()).collect();
    let md = scan_run_md(
        "scan_nd",
        &detectors,
        &motor_readers,
        Some(points.len()),
        AxisGrouping::Coupled,
    );
    scan_nd_with_md(detectors, motors, points, md, per_step)
}

/// The shared body of the `scan_nd` family: drive each row of `points` and read
/// `motors`+`detectors` into `primary`, opening the run with the caller-built
/// `md`. Keeps the `dimensions`-grouping decision (coupled vs grid) at the
/// caller — `scan_nd` passes a combined axis, `list_grid_scan` one per motor —
/// while the traversal stays in one place.
fn scan_nd_with_md(
    detectors: Vec<Arc<dyn ReadableObj>>,
    motors: Vec<(Arc<dyn MovableObj>, Arc<dyn ReadableObj>)>,
    points: Vec<Vec<f64>>,
    md: RunMetadata,
    per_step: Option<PerStep>,
) -> Plan {
    let per_step = per_step.unwrap_or_else(|| Arc::new(stubs::one_nd_step) as PerStep);
    // Stage detectors + every motor before the run, unstage after (PLAN-09).
    let stage_motors: Vec<Arc<dyn MovableObj>> = motors.iter().map(|(m, _)| m.clone()).collect();
    let staged = stageables_for(&detectors, &stage_motors);
    let inner = plan_box(async_stream::stream! {
        yield Msg::OpenRun(md);
        // Per-motor last-set position — bsrs's port of bluesky's
        // move_per_step `pos_cache` (plan_stubs.py:1688-1702). A motor whose
        // target equals its last-set value is NOT re-commanded this point:
        // in an N-D grid the slow axes stay constant across a row's inner
        // points, so without this they would receive a spurious "move to where
        // you already are" Set + settle every point. `None` until a motor is
        // first set, so every motor moves on the first point (bluesky seeds
        // pos_cache with a None default, so the first `pos == None` is False).
        // Exact equality mirrors bluesky's `pos == pos_cache[motor]`: grid
        // points recur exactly, so an epsilon is unwanted (it would skip a
        // genuine small move). The pos_cache decision becomes each motor's
        // `StepMotor` target (`Some` to command, `None` to skip); `one_nd_step`
        // then emits Checkpoint/Set*/Wait and reads all motor readers, so the
        // default reproduces the previous inline loop exactly.
        let mut pos_cache: Vec<Option<f64>> = vec![None; motors.len()];
        for row in points {
            let step_motors: Vec<StepMotor> = motors
                .iter()
                .enumerate()
                .map(|(i, (m, mr))| {
                    // Row shorter than motors: trailing motors are never
                    // commanded (target None) but are still read, matching the
                    // previous `for (i, v) in row.iter()` + break/continue.
                    let target = if i < row.len() && pos_cache[i] != Some(row[i]) {
                        pos_cache[i] = Some(row[i]);
                        Some(row[i])
                    } else {
                        None
                    };
                    (m.clone(), mr.clone(), target)
                })
                .collect();
            let mut sub = per_step(detectors.clone(), step_motors);
            while let Some(item) = futures::StreamExt::next(&mut sub).await {
                // Internal Bare-only sub-plan: no `Respond` item to preserve.
                let (PlanItem::Bare(m) | PlanItem::Respond(m, _)) = item;
                yield m;
            }
        }
        yield Msg::CloseRun { exit_status: "success".into(), reason: None };
    });
    preprocessors::stage_wrapper(inner, staged)
}

/// Which axes of an N-D grid scan follow a snake (boustrophedon) trajectory.
/// Ports bluesky's `snake_axes` argument (`bool | list`, plans.py:1294):
/// `None` snakes nothing, `All` snakes every axis except the slowest (which is
/// traversed once, so snaking it is a no-op), and `Axes(idxs)` snakes exactly
/// the listed axis indices (0 = slowest).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum SnakeAxes {
    /// Do not snake any axis — plain outer-product order.
    #[default]
    None,
    /// Snake all axes except the slowest (bluesky `snake_axes=True`).
    All,
    /// Snake exactly these axis indices (bluesky `snake_axes=[m2, m3, …]`).
    Axes(Vec<usize>),
}

impl SnakeAxes {
    /// Expand to a per-axis flag vector of length `n` (index 0 = slowest).
    fn to_flags(&self, n: usize) -> Vec<bool> {
        match self {
            SnakeAxes::None => vec![false; n],
            // The slowest axis (index 0) never snakes: it is traversed once.
            SnakeAxes::All => (0..n).map(|i| i > 0).collect(),
            SnakeAxes::Axes(idxs) => (0..n).map(|i| idxs.contains(&i)).collect(),
        }
    }
}

/// `list_grid_scan(dets, [(motor, [points...]), ...])` — N-D grid where
/// each axis traces a user-supplied list of positions. Natural (non-snaked)
/// order; the non-snaked form of [`list_grid_scan_snake`].
pub fn list_grid_scan(detectors: Vec<Arc<dyn ReadableObj>>, axes: Vec<ListGridAxis>) -> Plan {
    list_grid_scan_snake(detectors, axes, SnakeAxes::None)
}

/// `list_grid_scan_snake(dets, axes, snake_axes)` — N-D list grid with per-axis
/// snake traversal (bluesky `list_grid_scan(..., snake_axes=…)`). The axes
/// selected by `snake_axes` reverse on alternating passes; all emitted
/// documents are otherwise identical to [`list_grid_scan`].
pub fn list_grid_scan_snake(
    detectors: Vec<Arc<dyn ReadableObj>>,
    axes: Vec<ListGridAxis>,
    snake_axes: SnakeAxes,
) -> Plan {
    let lists: Vec<Vec<f64>> = axes.iter().map(|(_, _, l)| l.clone()).collect();
    let pts = patterns::outer_list_product_snake(&lists, &snake_axes.to_flags(lists.len()));
    let motors: Vec<(Arc<dyn MovableObj>, Arc<dyn ReadableObj>)> =
        axes.into_iter().map(|(m, r, _)| (m, r)).collect();
    // A grid: each motor is its own axis in the `dimensions` hint (bluesky's
    // outer-product `motor_hints`, plans.py:350), unlike `scan_nd`'s coupled
    // default.
    let motor_readers: Vec<Arc<dyn ReadableObj>> =
        motors.iter().map(|(_, mr)| mr.clone()).collect();
    let md = scan_run_md(
        "list_grid_scan",
        &detectors,
        &motor_readers,
        Some(pts.len()),
        AxisGrouping::Grid,
    );
    // grid-family per_step is deferred: grid_scan_snake settles the slow axis
    // in a separate "set1" group per row, which does not match one_nd_step's
    // single-group move. Route through the default until that reconciliation.
    scan_nd_with_md(detectors, motors, pts, md, None)
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
            // Internal Bare-only sub-plan: no `Respond` item to preserve.
            let (PlanItem::Bare(m) | PlanItem::Respond(m, _)) = item;
            yield m;
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
        yield Msg::OpenRun(scan_run_md(
            "spiral_square",
            &detectors,
            &[x_reader.clone(), y_reader.clone()],
            None,
            AxisGrouping::Grid,
        ));
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
/// nth, dr_y, tilt)` — Archimedean spiral through `(x, y)` (bluesky
/// `plans.spiral`). `dr` is the minor-axis radial step, `nth` the base angular
/// steps per ring; `dr_y` (`None` ⇒ circular) is the major-axis radial step and
/// `tilt` (radians) shears the clip box. See [`patterns::spiral`].
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
    dr_y: Option<f64>,
    tilt: f64,
) -> Plan {
    let pts = patterns::spiral(x_start, y_start, x_range, y_range, dr, nth, dr_y, tilt);
    plan_box(async_stream::stream! {
        yield Msg::OpenRun(scan_run_md(
            "spiral",
            &detectors,
            &[x_reader.clone(), y_reader.clone()],
            None,
            AxisGrouping::Grid,
        ));
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

/// Builds a fresh inner plan for one ramp sample — bluesky's `inner_plan_func`.
/// Called once per data point (pre, each poll, and post), typically a
/// `trigger_and_read` of the detectors.
pub type RampInnerFn = Arc<dyn Fn() -> Plan + Send + Sync>;

/// Ramp-completion predicate — the bsrs stand-in for bluesky's `status.done`.
/// Polled between samples; returns `true` once the ramp has landed. bsrs plans
/// receive no Status back from the engine, so the caller supplies this (e.g.
/// "motor readback within tolerance of the target").
pub type RampDoneFn = Arc<dyn Fn() -> futures::future::BoxFuture<'static, bool> + Send + Sync>;

/// `ramp_plan(go_plan, monitor_sig, inner, is_complete, take_pre_data, timeout,
/// period)` — take data while ramping a positioner. Ports bluesky's `ramp_plan`
/// (plans.py:2214): an optional pre-sample, start the ramp via `go_plan`, then
/// repeatedly sample with `inner` until `is_complete` reports the ramp landed,
/// then a final post-sample. `monitor_sig` is monitored across the whole run
/// (bluesky's `monitor_during_decorator`).
///
/// bluesky captures a Status from `go_plan` and loops `while not status.done`.
/// bsrs plans get no value back from the engine, so completion is the
/// caller-supplied `is_complete` predicate, polled before each sample. `timeout`
/// bounds the total ramp — exceeding it fails the run (bluesky's `RampFail`).
/// `period` rate-limits sampling to at most one point per `period`; if a sample
/// already took longer, the next runs with no added delay.
#[allow(clippy::too_many_arguments)]
pub fn ramp_plan(
    go_plan: Plan,
    monitor_sig: Arc<dyn MonitorableObj>,
    inner: RampInnerFn,
    is_complete: RampDoneFn,
    take_pre_data: bool,
    timeout: Option<Duration>,
    period: Option<Duration>,
) -> Plan {
    let body = plan_box(async_stream::stream! {
        use futures::StreamExt;
        yield Msg::OpenRun(RunMetadata {
            plan_name: Some("ramp_plan".into()),
            ..Default::default()
        });
        // Watch the clock only if a timeout was given (bluesky `fail_time`).
        let fail_time = timeout.map(|t| std::time::Instant::now() + t);
        // Pre-sample, before the ramp starts.
        if take_pre_data {
            let mut pre = inner();
            while let Some(item) = pre.next().await {
                // Internal Bare-only sub-plan: no `Respond` item to preserve.
                let (PlanItem::Bare(m) | PlanItem::Respond(m, _)) = item;
                yield m;
            }
        }
        // Start the ramp (go_plan issues its Set(s) without waiting).
        let mut go = go_plan;
        while let Some(item) = go.next().await {
            // Internal Bare-only sub-plan: no `Respond` item to preserve.
            let (PlanItem::Bare(m) | PlanItem::Respond(m, _)) = item;
            yield m;
        }
        // Sample until the ramp lands (bluesky `while not status.done`).
        let mut timed_out = false;
        loop {
            if is_complete().await {
                break;
            }
            let start = std::time::Instant::now();
            let mut p = inner();
            while let Some(item) = p.next().await {
                // Internal Bare-only sub-plan: no `Respond` item to preserve.
                let (PlanItem::Bare(m) | PlanItem::Respond(m, _)) = item;
                yield m;
            }
            if let Some(ft) = fail_time {
                if std::time::Instant::now() > ft {
                    timed_out = true;
                    break;
                }
            }
            // Rate-limit: sleep out the remainder of this sample's period.
            if let Some(min_period) = period {
                let remaining =
                    (start + min_period).saturating_duration_since(std::time::Instant::now());
                if !remaining.is_zero() {
                    yield Msg::Sleep(remaining);
                }
            }
        }
        if timed_out {
            // bluesky raises utils.RampFail(); bsrs fails the run.
            yield Msg::Fail("ramp_plan: ramp did not complete within timeout".into());
            return;
        }
        // Post-sample, after completion.
        let mut post = inner();
        while let Some(item) = post.next().await {
            // Internal Bare-only sub-plan: no `Respond` item to preserve.
            let (PlanItem::Bare(m) | PlanItem::Respond(m, _)) = item;
            yield m;
        }
        yield Msg::CloseRun { exit_status: "success".into(), reason: None };
    });
    // Monitor `monitor_sig` for the duration of the run.
    preprocessors::monitor_during_wrapper(body, vec![monitor_sig])
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
            // Internal Bare-only sub-plan: no `Respond` item to preserve.
            let (PlanItem::Bare(m) | PlanItem::Respond(m, _)) = item;
            yield m;
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
            // Internal Bare-only sub-plan: no `Respond` item to preserve.
            let (PlanItem::Bare(m) | PlanItem::Respond(m, _)) = item;
            yield m;
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
/// x_start, y_start, x_range, y_range, dr, factor, dr_y, tilt)` —
/// Fermat (sunflower) spiral via golden-angle increments (bluesky
/// `plans.spiral_fermat`). `dr_y` (`None` ⇒ circular) is the major-axis radial
/// step and `tilt` (radians) shears the clip box. See
/// [`patterns::spiral_fermat_pattern`].
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
    dr_y: Option<f64>,
    tilt: f64,
) -> Plan {
    let pts =
        patterns::spiral_fermat_pattern(x_start, y_start, x_range, y_range, dr, factor, dr_y, tilt);
    plan_box(async_stream::stream! {
        yield Msg::OpenRun(scan_run_md(
            "spiral_fermat",
            &detectors,
            &[x_reader.clone(), y_reader.clone()],
            None,
            AxisGrouping::Grid,
        ));
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
    dr_y: Option<f64>,
    tilt: f64,
) -> Plan {
    let xm: Arc<dyn MovableObj> = x_motor.clone();
    let ym: Arc<dyn MovableObj> = y_motor.clone();
    let inner = spiral(
        detectors, xm, x_reader, ym, y_reader, x_start, y_start, x_range, y_range, dr, nth, dr_y,
        tilt,
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
    dr_y: Option<f64>,
    tilt: f64,
) -> Plan {
    let xm: Arc<dyn MovableObj> = x_motor.clone();
    let ym: Arc<dyn MovableObj> = y_motor.clone();
    let inner = spiral_fermat(
        detectors, xm, x_reader, ym, y_reader, x_start, y_start, x_range, y_range, dr, factor,
        dr_y, tilt,
    );
    let rel = preprocessors::relative_set_wrapper(inner, vec![x_motor.clone(), y_motor.clone()]);
    preprocessors::reset_positions_wrapper(rel, vec![x_motor, y_motor])
}

/// One flyer of a [`fly`] scan: its `Flyable` (kickoff/complete) paired with the
/// `Collectable` that drains its buffered data. bsrs splits bluesky's single
/// `Flyable` interface across two traits, so a flyer is a `(Flyable, Collectable)`
/// pair.
pub type Flyer = (Arc<dyn FlyableObj>, Arc<dyn CollectableObj>);

/// `fly(flyers)` — fly scan over one or more flyers. Ports bluesky's `fly`
/// (plans.py:2305): kick off every flyer, wait, tell every flyer to complete,
/// wait, then collect each. Mirrors `kickoff_all` / `complete_all` (one group,
/// one barrier) so the flyers run concurrently rather than one-at-a-time.
///
/// No staging: as in bluesky, `fly` does not stage its flyers — wrap it with
/// [`preprocessors::stage_wrapper`] (bluesky's `stage_decorator`) when the
/// devices need staging.
pub fn fly(flyers: Vec<Flyer>) -> Plan {
    plan_box(async_stream::stream! {
        yield Msg::OpenRun(RunMetadata {
            plan_name: Some("fly".into()),
            ..Default::default()
        });
        // Kick off every flyer under one group and wait, then tell every flyer
        // to complete under one group and wait — reusing the canonical
        // `kickoff_all` / `complete_all` stubs (bluesky's helpers of the same
        // name). An empty flyer list emits no kickoff/complete/wait at all,
        // matching bluesky's `for flyer in flyers` loops.
        if !flyers.is_empty() {
            let objs: Vec<Arc<dyn FlyableObj>> =
                flyers.iter().map(|(f, _)| f.clone()).collect();
            let mut kick = stubs::kickoff_all(objs.clone(), Some("kick".into()), true);
            while let Some(item) = futures::StreamExt::next(&mut kick).await {
                // Internal Bare-only sub-plan: no `Respond` item to preserve.
                let (PlanItem::Bare(m) | PlanItem::Respond(m, _)) = item;
                yield m;
            }
            let mut done = stubs::complete_all(objs, Some("complete".into()), true);
            while let Some(item) = futures::StreamExt::next(&mut done).await {
                // Internal Bare-only sub-plan: no `Respond` item to preserve.
                let (PlanItem::Bare(m) | PlanItem::Respond(m, _)) = item;
                yield m;
            }
        }
        // Collect each flyer's buffered data.
        for (_, c) in &flyers {
            yield Msg::Collect {
                obj: c.clone(),
                stream_name: None,
            };
        }
        yield Msg::CloseRun {
            exit_status: "success".into(),
            reason: None,
        };
    })
}

/// `adaptive_scan(detectors, signal_field, motor, motor_reader, start,
/// stop, min_step, max_step, target_delta, backstep, threshold)` — adaptive
/// step-sized 1-D scan. Ports bluesky's `adaptive_scan` (plans.py:673)
/// slope-normalised step sizing.
///
/// At each step it reads `signal_field`, forms the gradient
/// `slope = |ΔI| / step`, and picks the next step so the *signal* changes
/// by about `target_delta`: `new_step = clip(target_delta / slope, min_step,
/// max_step)` (or a gentle `min(step*1.1, max_step)` grow when the slope is
/// flat). The applied step is exponentially smoothed (`0.2·new + 0.8·old`).
/// When `backstep` and the new step falls below `step * threshold`, it steps
/// back over the region it overshot and re-scans it with the finer step.
///
/// Requires `0 < min_step < max_step`; otherwise the plan fails immediately
/// (bluesky raises `ValueError`). Useful for scanning across a peak / edge
/// where uniform-step density would either miss the feature or oversample the
/// flat regions.
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
    threshold: f64,
) -> Plan {
    let signal_field = signal_field.into();
    plan_box(async_stream::stream! {
        if !(min_step > 0.0 && min_step < max_step) {
            yield Msg::Fail(format!(
                "adaptive_scan: require 0 < min_step < max_step, got min_step={min_step}, max_step={max_step}"
            ));
            return;
        }
        yield Msg::OpenRun(RunMetadata {
            plan_name: Some("adaptive_scan".into()),
            ..Default::default()
        });
        let direction = if stop >= start { 1.0_f64 } else { -1.0 };
        let mut pos = start;
        let mut past_i: Option<f64> = None;
        // bluesky's initial step is the half-range, not the midpoint.
        let mut step = (max_step - min_step) / 2.0;
        // Safety cap: backstep can oscillate on degenerate signals; bluesky
        // relies on physics to terminate, we bound it to avoid a hang.
        let max_iters = 10_000_usize;
        let mut iter = 0_usize;
        // Strict boundary matches bluesky `while next_pos*dir < stop*dir`.
        while pos * direction < stop * direction {
            iter += 1;
            if iter > max_iters {
                break;
            }
            // Per-step rewind boundary (bluesky emits `checkpoint` before the
            // `mv`, plans.py:764).
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
            // Signal sample for adaptation. bsrs plans do not receive the
            // value bundled by `Msg::Read`, so re-read `signal_field` from the
            // first detector that reports it (bluesky reads it from whichever
            // device carries `target_field`, plans.py:770).
            let mut cur_i: Option<f64> = None;
            for d in &detectors {
                if let Ok(map) = d.read_dyn().await {
                    if let Some(v) = map.get(&signal_field).and_then(|rv| rv.value.as_f64()) {
                        cur_i = Some(v);
                        break;
                    }
                }
            }
            // First point: seed the reference, advance, no adaptation.
            let Some(p) = past_i else {
                past_i = cur_i;
                pos += step * direction;
                continue;
            };
            // No signal this point: keep the step, do not update the reference.
            let Some(n) = cur_i else {
                pos += step * direction;
                continue;
            };
            let di = (n - p).abs();
            let slope = di / step;
            let new_step = if slope != 0.0 {
                (target_delta / slope).clamp(min_step, max_step)
            } else {
                (step * 1.1).min(max_step)
            };
            if backstep && new_step < step * threshold {
                // Overshot: step back over the region and re-scan it finer.
                // Verbatim bluesky arithmetic (`next_pos -= step`, no sign).
                pos -= step;
                step = new_step;
            } else {
                past_i = Some(n);
                step = 0.2 * new_step + 0.8 * step;
            }
            pos += step * direction;
        }
        yield Msg::CloseRun {
            exit_status: "success".into(),
            reason: None,
        };
    })
}

/// `tune_centroid(detectors, signal_field, motor, motor_reader, start, stop,
/// min_step, num, step_factor, snake)` — iteratively tune `motor` to the
/// centroid of `signal_field`. Ports bluesky's multi-pass `tune_centroid`
/// (plans.py:873).
///
/// Each pass scans `num` points across the current window, accumulates the
/// centroid `Σ(xᵢ·Iᵢ) / Σ(Iᵢ)`, then re-centers a window narrowed by
/// `step_factor` on that centroid and rescans — until the per-pass step falls
/// below `min_step`. The motor is finally moved to the converged centroid. If a
/// pass sees no signal (`ΣI == 0`) the plan stops without a final move, matching
/// bluesky. With `snake = true` the scan direction alternates each pass to save
/// return travel.
///
/// Requires `min_step > 0` and `step_factor > 1.0` (bluesky raises
/// `ValueError`); otherwise the plan fails before opening a run. As in bsrs's
/// other feedback plans, the signal is re-read plan-side via `read_dyn`, and the
/// commanded position is used for the centroid abscissa.
#[allow(clippy::too_many_arguments)]
pub fn tune_centroid(
    detectors: Vec<Arc<dyn ReadableObj>>,
    signal_field: impl Into<String>,
    motor: Arc<dyn MovableObj>,
    motor_reader: Arc<dyn ReadableObj>,
    start: f64,
    stop: f64,
    min_step: f64,
    num: usize,
    step_factor: f64,
    snake: bool,
) -> Plan {
    let signal_field = signal_field.into();
    let span = move |a: f64, b: f64| {
        if num > 1 {
            (b - a) / (num as f64 - 1.0)
        } else {
            0.0
        }
    };
    plan_box(async_stream::stream! {
        if min_step <= 0.0 {
            yield Msg::Fail("tune_centroid: min_step must be positive".into());
            return;
        }
        if step_factor <= 1.0 {
            yield Msg::Fail("tune_centroid: step_factor must be greater than 1.0".into());
            return;
        }
        yield Msg::OpenRun(RunMetadata {
            plan_name: Some("tune_centroid".into()),
            ..Default::default()
        });
        // Global bounds are fixed; the per-pass window shrinks inside them.
        let low_limit = start.min(stop);
        let high_limit = start.max(stop);
        let mut pass_start = start;
        let mut pass_stop = stop;
        let mut next_pos = pass_start;
        let mut step = span(pass_start, pass_stop);
        let mut peak_position: Option<f64> = None;
        let mut sum_i = 0.0_f64;
        let mut sum_xi = 0.0_f64;
        // step_factor > 1 guarantees the step shrinks each pass, so this
        // terminates without an iteration cap.
        while step.abs() >= min_step && (low_limit..=high_limit).contains(&next_pos) {
            // Per-step rewind boundary (bluesky emits `checkpoint` before mv,
            // plans.py:990).
            yield Msg::Checkpoint;
            yield Msg::Set { obj: motor.clone(), value: next_pos, group: Some("set".into()) };
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
            // Re-read the signal from the first detector that carries it.
            let mut cur_i: Option<f64> = None;
            for d in &detectors {
                if let Ok(map) = d.read_dyn().await {
                    if let Some(v) = map.get(&signal_field).and_then(|rv| rv.value.as_f64()) {
                        cur_i = Some(v);
                        break;
                    }
                }
            }
            if let Some(y) = cur_i {
                sum_i += y;
                sum_xi += next_pos * y;
            }
            next_pos += step;
            let in_range = pass_start.min(pass_stop) <= next_pos
                && next_pos <= pass_start.max(pass_stop);
            if !in_range {
                if sum_i == 0.0 {
                    // No signal this pass: give up without a final move.
                    yield Msg::CloseRun { exit_status: "success".into(), reason: None };
                    return;
                }
                let centroid = sum_xi / sum_i;
                peak_position = Some(centroid);
                sum_i = 0.0;
                sum_xi = 0.0;
                let new_range = (pass_stop - pass_start) / step_factor;
                pass_start = (centroid - new_range / 2.0).clamp(low_limit, high_limit);
                pass_stop = (centroid + new_range / 2.0).clamp(low_limit, high_limit);
                if snake {
                    std::mem::swap(&mut pass_start, &mut pass_stop);
                }
                step = span(pass_start, pass_stop);
                next_pos = pass_start;
            }
        }
        // Move to the converged centroid (bluesky's trailing `mv`).
        if let Some(peak) = peak_position {
            yield Msg::Set { obj: motor.clone(), value: peak, group: Some("center".into()) };
            yield Msg::Wait {
                group: "center".into(),
                error_on_timeout: true,
                timeout: None,
            };
        }
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
    threshold: f64,
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
            threshold,
        );
        use futures::StreamExt;
        while let Some(item) = inner.next().await {
            // Internal Bare-only sub-plan: no `Respond` item to preserve.
            let (PlanItem::Bare(m) | PlanItem::Respond(m, _)) = item;
            yield m;
        }
    });
    preprocessors::reset_positions_wrapper(inner, vec![reset_motor])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::plan::{Plan, PlanItem};
    use crate::core::status::Status;
    use futures::StreamExt;

    /// Minimal flyer for stub-stream tests. `kickoff_dyn`/`complete_dyn`
    /// are never called by `drain` (only the engine invokes them), so the
    /// returned `Status::done()` is just a stand-in.
    struct FakeFlyer(String);

    impl crate::core::msg::NamedObj for FakeFlyer {
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

    /// Minimal collectable for `fly` drain tests. `describe_collect_dyn` /
    /// `collect_dyn` are never called by `drain` (only the engine invokes
    /// them); the plan only carries the object inside `Msg::Collect`.
    struct FakeCollectable(String);

    impl crate::core::msg::NamedObj for FakeCollectable {
        fn name(&self) -> &str {
            &self.0
        }
    }

    #[async_trait::async_trait]
    impl CollectableObj for FakeCollectable {
        async fn describe_collect_dyn(
            &self,
        ) -> Result<
            std::collections::HashMap<
                String,
                std::collections::HashMap<String, crate::event_model::DataKey>,
            >,
            crate::core::error::BsrsError,
        > {
            Ok(Default::default())
        }
        async fn collect_dyn(
            &self,
        ) -> Result<
            Vec<(
                String,
                std::collections::HashMap<String, serde_json::Value>,
                std::collections::HashMap<String, f64>,
            )>,
            crate::core::error::BsrsError,
        > {
            Ok(Vec::new())
        }
    }

    /// Minimal preparable for `prepare` stub-stream tests. `prepare_dyn` is
    /// never called by `drain` (only the engine invokes it).
    struct FakePreparable(String);

    impl crate::core::msg::NamedObj for FakePreparable {
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
            let (PlanItem::Bare(m) | PlanItem::Respond(m, _)) = item;
            out.push(m);
        }
        out
    }

    fn flyers(n: usize) -> Vec<Arc<dyn FlyableObj>> {
        (0..n)
            .map(|i| Arc::new(FakeFlyer(format!("fly{i}"))) as Arc<dyn FlyableObj>)
            .collect()
    }

    /// `n` `(Flyable, Collectable)` pairs, both named `fly{i}`.
    fn flyer_pairs(n: usize) -> Vec<Flyer> {
        (0..n)
            .map(|i| {
                (
                    Arc::new(FakeFlyer(format!("fly{i}"))) as Arc<dyn FlyableObj>,
                    Arc::new(FakeCollectable(format!("fly{i}"))) as Arc<dyn CollectableObj>,
                )
            })
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

    fn collect_name(m: &Msg) -> Option<&str> {
        match m {
            Msg::Collect { obj, .. } => Some(obj.name()),
            _ => None,
        }
    }

    fn wait_group_name(m: &Msg) -> Option<&str> {
        match m {
            Msg::Wait { group, .. } => Some(group.as_str()),
            _ => None,
        }
    }

    fn colls(n: usize) -> Vec<Arc<dyn CollectableObj>> {
        (0..n)
            .map(|i| Arc::new(FakeCollectable(format!("det{i}"))) as Arc<dyn CollectableObj>)
            .collect()
    }

    fn msg_kind(m: &Msg) -> &'static str {
        match m {
            Msg::Complete { .. } => "complete",
            Msg::Wait { .. } => "wait",
            Msg::Collect { .. } => "collect",
            _ => "other",
        }
    }

    /// Drive a `collect_while_completing`-style plan to completion, answering
    /// each `Respond`-carried `Wait` with the next scripted `done` flag. This
    /// substitutes for the engine (which is what fulfills a `Respond`), so the
    /// plan's response loop runs deterministically with no real flyers/timing.
    /// Panics if the plan issues more `Wait`s than there are scripted flags.
    async fn drain_completing(mut plan: Plan, mut dones: std::vec::IntoIter<bool>) -> Vec<Msg> {
        let mut out = Vec::new();
        while let Some(item) = plan.next().await {
            match item {
                PlanItem::Bare(m) => out.push(m),
                PlanItem::Respond(m, tx) => {
                    out.push(m);
                    let done = dones
                        .next()
                        .expect("plan issued more Wait responses than were scripted");
                    let _ = tx.send(MsgResult::WaitComplete { done });
                }
            }
        }
        out
    }

    use crate::core::msg::{DynLocation, MovableObj, NamedObj, ReadableObj};
    use crate::core::reading::ReadingValue;
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
    impl crate::core::msg::LocatableObj for FakeMotor {
        async fn locate_dyn(&self) -> Result<DynLocation, crate::core::error::BsrsError> {
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
    impl crate::core::msg::LocatableObj for CountingMotor {
        async fn locate_dyn(&self) -> Result<DynLocation, crate::core::error::BsrsError> {
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
        ) -> Result<HashMap<String, ReadingValue>, crate::core::error::BsrsError> {
            Ok(HashMap::new())
        }
        async fn describe_dyn(
            &self,
        ) -> Result<HashMap<String, crate::event_model::DataKey>, crate::core::error::BsrsError>
        {
            Ok(HashMap::new())
        }
    }

    /// Readable that is ALSO stageable: its `as_stageable` returns `Some`, so a
    /// compound plan `Stage`s it before the run and `Unstage`s it after
    /// (PLAN-09). Contrast [`FakeReadable`], which is not stageable and so is
    /// never staged.
    struct StageableFake(String);

    impl NamedObj for StageableFake {
        fn name(&self) -> &str {
            &self.0
        }
    }

    #[async_trait::async_trait]
    impl ReadableObj for StageableFake {
        async fn read_dyn(
            &self,
        ) -> Result<HashMap<String, ReadingValue>, crate::core::error::BsrsError> {
            Ok(HashMap::new())
        }
        async fn describe_dyn(
            &self,
        ) -> Result<HashMap<String, crate::event_model::DataKey>, crate::core::error::BsrsError>
        {
            Ok(HashMap::new())
        }
        fn as_stageable(self: Arc<Self>) -> Option<Arc<dyn StageableObj>> {
            Some(self)
        }
    }

    #[async_trait::async_trait]
    impl StageableObj for StageableFake {
        async fn stage_dyn(&self) -> Result<(), crate::core::error::BsrsError> {
            Ok(())
        }
        async fn unstage_dyn(&self) -> Result<(), crate::core::error::BsrsError> {
            Ok(())
        }
    }

    // PLAN-09: a compound plan stages its stageable detectors before the run
    // and unstages them (LIFO) after, bracketing OpenRun/CloseRun.
    #[tokio::test]
    async fn count_stages_stageable_detector_around_the_run() {
        let det: Arc<dyn ReadableObj> = Arc::new(StageableFake("sdet".into()));
        let msgs = drain(count(vec![det], 1)).await;
        assert!(
            matches!(&msgs[0], Msg::Stage(o) if o.name() == "sdet"),
            "first message must Stage the detector, got {:?}",
            &msgs[0]
        );
        assert!(
            matches!(&msgs[1], Msg::OpenRun(_)),
            "OpenRun must follow the Stage, got {:?}",
            &msgs[1]
        );
        assert!(
            matches!(msgs.last(), Some(Msg::Unstage(o)) if o.name() == "sdet"),
            "last message must Unstage the detector, got {:?}",
            msgs.last()
        );
        let close = msgs
            .iter()
            .position(|m| matches!(m, Msg::CloseRun { .. }))
            .expect("CloseRun present");
        let unstage = msgs
            .iter()
            .rposition(|m| matches!(m, Msg::Unstage(_)))
            .expect("Unstage present");
        assert!(close < unstage, "Unstage must come after CloseRun");
    }

    // The opt-in is honoured: a plain (non-stageable) detector emits no
    // Stage/Unstage, so existing plans over sim/test devices are unchanged.
    #[tokio::test]
    async fn count_does_not_stage_non_stageable_detector() {
        let msgs = drain(count(vec![rdr("d")], 1)).await;
        assert!(
            !msgs
                .iter()
                .any(|m| matches!(m, Msg::Stage(_) | Msg::Unstage(_))),
            "a non-stageable detector must not be staged"
        );
    }

    // The scan family stages too (via scan_1d_per_step), not just count.
    #[tokio::test]
    async fn scan_1d_stages_stageable_detector_before_open() {
        let det: Arc<dyn ReadableObj> = Arc::new(StageableFake("sdet".into()));
        let motor = Arc::new(FakeMotor {
            name: "m".into(),
            bias: 0.0,
        });
        let reader = rdr("m_rbv");
        let msgs = drain(scan_1d(
            vec![det],
            motor as Arc<dyn MovableObj>,
            reader,
            0.0,
            1.0,
            2,
        ))
        .await;
        assert!(
            matches!(&msgs[0], Msg::Stage(o) if o.name() == "sdet"),
            "scan_1d must Stage the detector first, got {:?}",
            &msgs[0]
        );
        assert!(
            matches!(msgs.last(), Some(Msg::Unstage(o)) if o.name() == "sdet"),
            "scan_1d must Unstage the detector last, got {:?}",
            msgs.last()
        );
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

    /// Detector whose `read_dyn` returns a predetermined signal sequence, one
    /// value per call (clamped to the last), under a fixed field name. Lets an
    /// adaptive plan be driven with a deterministic signal so the resulting
    /// motor trajectory can be asserted exactly.
    struct SequenceDetector {
        name: String,
        field: String,
        values: Vec<f64>,
        cursor: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl SequenceDetector {
        fn new(name: &str, field: &str, values: Vec<f64>) -> Arc<Self> {
            Arc::new(Self {
                name: name.into(),
                field: field.into(),
                values,
                cursor: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            })
        }
    }

    impl NamedObj for SequenceDetector {
        fn name(&self) -> &str {
            &self.name
        }
    }

    #[async_trait::async_trait]
    impl ReadableObj for SequenceDetector {
        async fn read_dyn(
            &self,
        ) -> Result<HashMap<String, ReadingValue>, crate::core::error::BsrsError> {
            let i = self
                .cursor
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                .min(self.values.len().saturating_sub(1));
            let v = self.values.get(i).copied().unwrap_or(0.0);
            let mut map = HashMap::new();
            map.insert(
                self.field.clone(),
                ReadingValue::new(serde_json::json!(v), 0.0),
            );
            Ok(map)
        }
        async fn describe_dyn(
            &self,
        ) -> Result<HashMap<String, crate::event_model::DataKey>, crate::core::error::BsrsError>
        {
            Ok(HashMap::new())
        }
    }

    /// Collect the `value`s of every `Msg::Set` targeting motor `name`, in
    /// order — the trajectory an adaptive plan drove the motor through.
    fn set_targets(msgs: &[Msg], name: &str) -> Vec<f64> {
        msgs.iter()
            .filter_map(|m| match m {
                Msg::Set { obj, value, .. } if obj.name() == name => Some(*value),
                _ => None,
            })
            .collect()
    }

    #[tokio::test]
    async fn adaptive_scan_rejects_invalid_step_bounds() {
        // bluesky raises ValueError unless 0 < min_step < max_step. bsrs fails
        // the plan before opening a run.
        let det = SequenceDetector::new("d", "sig", vec![1.0]) as Arc<dyn ReadableObj>;
        let m = Arc::new(FakeMotor {
            name: "m".into(),
            bias: 0.0,
        }) as Arc<dyn MovableObj>;
        let msgs = drain(adaptive_scan(
            vec![det],
            "sig",
            m,
            rdr("mr"),
            0.0,
            2.0,
            0.5, // min_step
            0.5, // max_step == min_step → invalid
            1.0,
            false,
            0.8,
        ))
        .await;
        assert!(
            matches!(msgs.first(), Some(Msg::Fail(_))),
            "expected a leading Fail, got {msgs:?}"
        );
        assert!(
            !msgs.iter().any(|m| matches!(m, Msg::OpenRun(_))),
            "no run should open on invalid bounds"
        );
    }

    #[tokio::test]
    async fn adaptive_scan_initial_step_is_half_range() {
        // First move is at `start`; the second is one initial step later, and
        // the initial step is the half-range (max-min)/2, NOT the midpoint.
        // Constant signal keeps the run finite and the second step exact.
        let det = SequenceDetector::new("d", "sig", vec![5.0; 64]) as Arc<dyn ReadableObj>;
        let m = Arc::new(FakeMotor {
            name: "m".into(),
            bias: 0.0,
        }) as Arc<dyn MovableObj>;
        let msgs = drain(adaptive_scan(
            vec![det],
            "sig",
            m,
            rdr("mr"),
            0.0,  // start
            10.0, // stop
            1.0,  // min_step
            4.0,  // max_step  → half-range = 1.5, midpoint would be 2.5
            2.0,
            false,
            0.8,
        ))
        .await;
        let targets = set_targets(&msgs, "m");
        assert_eq!(targets.first().copied(), Some(0.0), "first move at start");
        assert_eq!(
            targets.get(1).copied(),
            Some(1.5),
            "second move one half-range step later"
        );
        // backstep disabled → the trajectory only advances.
        assert!(
            targets.windows(2).all(|w| w[1] >= w[0]),
            "no backstep without backstep=true: {targets:?}"
        );
    }

    #[tokio::test]
    async fn adaptive_scan_backsteps_when_step_shrinks_past_threshold() {
        // A large signal jump between the first two points forces
        // new_step = clip(target_delta/slope, min, max) down to min_step, well
        // below step*threshold, so with backstep=true the motor steps back
        // over the region it overshot — a non-monotonic trajectory.
        let det = SequenceDetector::new("d", "sig", vec![0.0, 10.0, 10.0, 10.0, 10.0, 10.0])
            as Arc<dyn ReadableObj>;
        let m = Arc::new(FakeMotor {
            name: "m".into(),
            bias: 0.0,
        }) as Arc<dyn MovableObj>;
        let msgs = drain(adaptive_scan(
            vec![det],
            "sig",
            m,
            rdr("mr"),
            0.0,  // start
            20.0, // stop
            0.5,  // min_step  → half-range = 2.25
            5.0,  // max_step
            1.0,  // target_delta
            true, // backstep
            0.8,
        ))
        .await;
        let targets = set_targets(&msgs, "m");
        assert_eq!(targets.first().copied(), Some(0.0));
        assert_eq!(
            targets.get(1).copied(),
            Some(2.25),
            "initial half-range step"
        );
        // Third move steps back: pos -= old_step (2.25) then += new_step (0.5).
        assert_eq!(
            targets.get(2).copied(),
            Some(0.5),
            "backstep over the overshot region: {targets:?}"
        );
    }

    fn create_count(msgs: &[Msg]) -> usize {
        msgs.iter()
            .filter(|m| matches!(m, Msg::Create { .. }))
            .count()
    }

    #[tokio::test]
    async fn tune_centroid_runs_multiple_passes_and_converges() {
        // Flat signal → each pass's centroid is the window midpoint (2.0), and
        // the window re-centers there and narrows by step_factor until the step
        // drops below min_step. With range 4, num 5, step_factor 3, the steps
        // are 1.0, 0.333, 0.111 (all ≥ 0.1) then 0.037 (< 0.1): three passes,
        // 15 points — proof the scan is multi-pass, not single-pass.
        let det = SequenceDetector::new("d", "sig", vec![1.0]) as Arc<dyn ReadableObj>;
        let m = Arc::new(FakeMotor {
            name: "m".into(),
            bias: 0.0,
        }) as Arc<dyn MovableObj>;
        let msgs = drain(tune_centroid(
            vec![det],
            "sig",
            m,
            rdr("mr"),
            0.0, // start
            4.0, // stop
            0.1, // min_step
            5,   // num
            3.0, // step_factor
            false,
        ))
        .await;
        assert_eq!(create_count(&msgs), 15, "three passes of five points");
        let targets = set_targets(&msgs, "m");
        assert!(
            (targets.last().copied().unwrap() - 2.0).abs() < 1e-9,
            "final move to the converged centroid 2.0: {targets:?}"
        );
    }

    #[tokio::test]
    async fn tune_centroid_weights_position_by_signal() {
        // Single pass (min_step 0.5 stops after the first step of 1.0, since the
        // next is 0.333 < 0.5). A peaked signal [0,1,3,0,0] over positions
        // 0,1,2,3,4 gives centroid ΣxI/ΣI = (1·1 + 2·3)/(1+3) = 1.75 — distinct
        // from the plain mean 2.0, so the weighting is exercised.
        let det = SequenceDetector::new("d", "sig", vec![0.0, 1.0, 3.0, 0.0, 0.0])
            as Arc<dyn ReadableObj>;
        let m = Arc::new(FakeMotor {
            name: "m".into(),
            bias: 0.0,
        }) as Arc<dyn MovableObj>;
        let msgs = drain(tune_centroid(
            vec![det],
            "sig",
            m,
            rdr("mr"),
            0.0,
            4.0,
            0.5, // min_step → only the first pass runs
            5,
            3.0,
            false,
        ))
        .await;
        assert_eq!(create_count(&msgs), 5, "one pass of five points");
        let targets = set_targets(&msgs, "m");
        assert!(
            (targets.last().copied().unwrap() - 1.75).abs() < 1e-9,
            "final move to the signal-weighted centroid 1.75: {targets:?}"
        );
    }

    /// Monitorable used only inside `Msg::Monitor`; `subscribe_dyn` is never
    /// reached by `drain` (which just collects yielded messages).
    struct FakeMonitor(String);

    impl NamedObj for FakeMonitor {
        fn name(&self) -> &str {
            &self.0
        }
    }

    #[async_trait::async_trait]
    impl ReadableObj for FakeMonitor {
        async fn read_dyn(
            &self,
        ) -> Result<HashMap<String, ReadingValue>, crate::core::error::BsrsError> {
            Ok(HashMap::new())
        }
        async fn describe_dyn(
            &self,
        ) -> Result<HashMap<String, crate::event_model::DataKey>, crate::core::error::BsrsError>
        {
            Ok(HashMap::new())
        }
    }

    #[async_trait::async_trait]
    impl MonitorableObj for FakeMonitor {
        async fn subscribe_dyn(
            &self,
        ) -> Result<crate::core::subscription::Subscription, crate::core::error::BsrsError>
        {
            Err(crate::core::error::BsrsError::Other(
                "FakeMonitor::subscribe_dyn is never called by drain".into(),
            ))
        }
    }

    /// A ramp-completion predicate that reports "not done" for its first `n`
    /// polls, then "done" — so a ramp plan takes exactly `n` in-loop samples.
    fn done_after(n: usize) -> RampDoneFn {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        Arc::new(move || -> futures::future::BoxFuture<'static, bool> {
            let calls = calls.clone();
            Box::pin(async move { calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) >= n })
        })
    }

    /// A ramp inner-sample plan: one `Create`/`Save` event.
    fn one_event_inner() -> RampInnerFn {
        Arc::new(|| {
            plan_box(async_stream::stream! {
                yield Msg::Create { stream_name: "primary".into() };
                yield Msg::Save;
            })
        })
    }

    fn no_op_go() -> Plan {
        plan_box(async_stream::stream! {
            yield Msg::Null;
        })
    }

    #[tokio::test]
    async fn ramp_plan_samples_until_complete_with_pre_and_post() {
        // is_complete is false for two polls then true: one pre-sample, two
        // in-loop samples, one post-sample = four events. The monitor brackets
        // the run.
        let mon = Arc::new(FakeMonitor("mon".into())) as Arc<dyn MonitorableObj>;
        let msgs = drain(ramp_plan(
            no_op_go(),
            mon,
            one_event_inner(),
            done_after(2),
            true, // take_pre_data
            None, // timeout
            None, // period
        ))
        .await;
        assert_eq!(create_count(&msgs), 4, "pre + 2 in-loop + post");
        assert!(matches!(msgs.first(), Some(Msg::OpenRun(_))));
        assert!(
            msgs.iter().any(|m| matches!(m, Msg::Monitor { .. })),
            "monitor installed after open"
        );
        assert!(
            msgs.iter().any(|m| matches!(m, Msg::Unmonitor(_))),
            "monitor removed before close"
        );
        assert!(matches!(
            msgs.last(),
            Some(Msg::CloseRun { exit_status, .. }) if exit_status == "success"
        ));
    }

    #[tokio::test]
    async fn ramp_plan_without_pre_data_skips_leading_sample() {
        let mon = Arc::new(FakeMonitor("mon".into())) as Arc<dyn MonitorableObj>;
        let msgs = drain(ramp_plan(
            no_op_go(),
            mon,
            one_event_inner(),
            done_after(2),
            false, // take_pre_data
            None,
            None,
        ))
        .await;
        assert_eq!(create_count(&msgs), 3, "2 in-loop + post, no pre-sample");
    }

    #[tokio::test]
    async fn ramp_plan_times_out_and_fails() {
        // A zero timeout trips after the first in-loop sample; the ramp never
        // completes, so the run fails (bluesky RampFail) with no clean close.
        let mon = Arc::new(FakeMonitor("mon".into())) as Arc<dyn MonitorableObj>;
        let msgs = drain(ramp_plan(
            no_op_go(),
            mon,
            one_event_inner(),
            done_after(usize::MAX), // never completes
            false,
            Some(std::time::Duration::ZERO),
            None,
        ))
        .await;
        assert!(
            msgs.iter().any(|m| matches!(m, Msg::Fail(_))),
            "timeout fails the run: {msgs:?}"
        );
        assert!(
            !msgs.iter().any(
                |m| matches!(m, Msg::CloseRun { exit_status, .. } if exit_status == "success")
            ),
            "a timed-out ramp does not close successfully"
        );
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
        }) as Arc<dyn crate::core::msg::LocatableObj>;
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
        }) as Arc<dyn crate::core::msg::LocatableObj>;
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
        Arc<dyn crate::core::msg::LocatableObj>,
        Arc<dyn crate::core::msg::LocatableObj>,
    ) {
        (
            Arc::new(FakeMotor {
                name: "x".into(),
                bias: bx,
            }) as Arc<dyn crate::core::msg::LocatableObj>,
            Arc::new(FakeMotor {
                name: "y".into(),
                bias: by,
            }) as Arc<dyn crate::core::msg::LocatableObj>,
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
            None,
            0.0,
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
            None,
            0.0,
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
            None,
            0.0,
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
            None,
            0.0,
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
        }) as Arc<dyn crate::core::msg::LocatableObj>;
        let unmoved = Arc::new(FakeMotor {
            name: "unmoved".into(),
            bias: 3.0,
        }) as Arc<dyn crate::core::msg::LocatableObj>;
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
                moved as Arc<dyn crate::core::msg::LocatableObj>,
                unmoved as Arc<dyn crate::core::msg::LocatableObj>,
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
        let msgs = drain(scan_1d(vec![], motor, rdr("m_rbv"), 0.0, 10.0, 3)).await;
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

    // A custom per_shot replaces the whole shot: `count_per_shot` runs it once
    // per repetition and emits none of the default read_shot's Checkpoint/reads.
    #[tokio::test]
    async fn count_per_shot_uses_custom_hook_verbatim() {
        let d = rdr("det");
        let per_shot: PerShot = Arc::new(|dets: Vec<Arc<dyn ReadableObj>>| {
            plan_box(async_stream::stream! {
                yield Msg::Create { stream_name: "custom".into() };
                for det in &dets {
                    yield Msg::Read(det.clone());
                }
                yield Msg::Save;
            })
        });
        let msgs = drain(count_per_shot(vec![d], 3, Some(per_shot))).await;
        assert_eq!(
            msgs.iter().filter(|m| matches!(m, Msg::Checkpoint)).count(),
            0,
            "custom per_shot owns the shot; the default Checkpoint is gone"
        );
        assert_eq!(
            msgs.iter()
                .filter(|m| matches!(m, Msg::Create { stream_name } if stream_name == "custom"))
                .count(),
            3,
            "custom shot runs once per repetition"
        );
        assert_eq!(
            msgs.iter().filter(|m| matches!(m, Msg::OpenRun(_))).count(),
            1,
            "count still owns the run envelope"
        );
        assert_eq!(
            msgs.iter()
                .filter(|m| matches!(m, Msg::CloseRun { .. }))
                .count(),
            1,
            "count still owns the run envelope"
        );
    }

    // A scalar delay yields a time-compensated Sleep after every shot, including
    // the last (bluesky's scalar delay is an infinite repeat).
    #[tokio::test]
    async fn count_ext_scalar_delay_sleeps_after_every_shot() {
        let d = rdr("det");
        let dt = Duration::from_millis(100);
        let msgs = drain(count_ext(vec![d], Some(3), CountDelay::Every(dt), None)).await;
        let sleeps: Vec<Duration> = msgs
            .iter()
            .filter_map(|m| match m {
                Msg::Sleep(s) => Some(*s),
                _ => None,
            })
            .collect();
        assert_eq!(sleeps.len(), 3, "one Sleep after each of the 3 shots");
        for s in &sleeps {
            assert!(
                *s > Duration::ZERO && *s <= dt,
                "compensated sleep {s:?} must be in (0, {dt:?}]"
            );
        }
    }

    // `num = None` acquires indefinitely: the stream keeps producing shot bundles
    // and never closes the run on its own.
    #[tokio::test]
    async fn count_ext_num_none_acquires_indefinitely() {
        let d = rdr("det");
        let mut plan = count_ext(vec![d], None, CountDelay::None, None);
        let mut got = Vec::new();
        for _ in 0..20 {
            match plan.next().await {
                Some(PlanItem::Bare(m) | PlanItem::Respond(m, _)) => got.push(m),
                None => break,
            }
        }
        assert_eq!(
            got.iter().filter(|m| matches!(m, Msg::OpenRun(_))).count(),
            1,
            "opens the run once"
        );
        assert_eq!(
            got.iter()
                .filter(|m| matches!(m, Msg::CloseRun { .. }))
                .count(),
            0,
            "never closes on its own — bounded only by the consumer"
        );
        assert!(
            got.iter().filter(|m| matches!(m, Msg::Save)).count() >= 3,
            "keeps taking shots"
        );
    }

    // A finite `num` with too few explicit delays fails upfront, before the run
    // opens (bluesky's ValueError).
    #[tokio::test]
    async fn count_ext_short_delay_sequence_fails_before_open() {
        let d = rdr("det");
        let seq = CountDelay::Sequence(vec![Duration::from_millis(10)]); // 1 < num-1 == 2
        let msgs = drain(count_ext(vec![d], Some(3), seq, None)).await;
        assert!(
            matches!(msgs.first(), Some(Msg::Fail(_))),
            "first Msg is Fail, got {:?}",
            msgs.first()
        );
        assert_eq!(
            msgs.iter().filter(|m| matches!(m, Msg::OpenRun(_))).count(),
            0,
            "no run opened on validation failure"
        );
    }

    // An explicit delay sequence is applied per interval; when it is exhausted the
    // run closes with no trailing sleep (bluesky's StopIteration -> break).
    #[tokio::test]
    async fn count_ext_delay_sequence_applies_per_interval() {
        let d = rdr("det");
        let seq = CountDelay::Sequence(vec![Duration::from_millis(50), Duration::from_millis(70)]);
        let msgs = drain(count_ext(vec![d], Some(3), seq, None)).await;
        assert_eq!(
            msgs.iter().filter(|m| matches!(m, Msg::Save)).count(),
            3,
            "all 3 shots run"
        );
        assert_eq!(
            msgs.iter().filter(|m| matches!(m, Msg::Sleep(_))).count(),
            2,
            "2 intervals delivered, no trailing sleep after the last shot"
        );
        assert_eq!(
            msgs.iter()
                .filter(|m| matches!(m, Msg::CloseRun { .. }))
                .count(),
            1,
            "run closes cleanly once the sequence is exhausted"
        );
    }

    // A custom per_step replaces the whole step and receives this step's motor
    // targets threaded through `StepMotor`.
    #[tokio::test]
    async fn scan_1d_per_step_uses_custom_hook_and_threads_targets() {
        let motor = Arc::new(FakeMotor {
            name: "m".into(),
            bias: 0.0,
        }) as Arc<dyn MovableObj>;
        let per_step: PerStep = Arc::new(
            |_dets: Vec<Arc<dyn ReadableObj>>, motors: Vec<StepMotor>| {
                plan_box(async_stream::stream! {
                    for (m, _r, target) in &motors {
                        if let Some(v) = target {
                            yield Msg::Set { obj: m.clone(), value: *v, group: Some("custom".into()) };
                        }
                    }
                })
            },
        );
        let msgs = drain(scan_1d_per_step(
            vec![],
            motor,
            rdr("m_rbv"),
            0.0,
            10.0,
            3,
            Some(per_step),
        ))
        .await;
        assert_eq!(
            msgs.iter().filter(|m| matches!(m, Msg::Checkpoint)).count(),
            0,
            "custom step omits the default Checkpoint/Create/Read/Save"
        );
        assert_eq!(
            msgs.iter()
                .filter(|m| matches!(m, Msg::Create { .. }))
                .count(),
            0,
            "custom step omits the default Create bundle"
        );
        let sets: Vec<f64> = msgs
            .iter()
            .filter_map(|m| match m {
                Msg::Set { value, group, .. } if group.as_deref() == Some("custom") => Some(*value),
                _ => None,
            })
            .collect();
        assert_eq!(
            sets,
            vec![0.0, 5.0, 10.0],
            "each step's target is threaded through StepMotor to the hook"
        );
    }

    // The canonical N-motor `scan` moves every axis together (inner product) and
    // opens the run with bluesky's `plan_name = "scan"`.
    #[tokio::test]
    async fn scan_moves_all_axes_together_and_opens_run_named_scan() {
        let mx = Arc::new(FakeMotor {
            name: "x".into(),
            bias: 0.0,
        }) as Arc<dyn MovableObj>;
        let my = Arc::new(FakeMotor {
            name: "y".into(),
            bias: 0.0,
        }) as Arc<dyn MovableObj>;
        let axes: Vec<ScanAxis> = vec![(mx, rdr("xr"), 0.0, 2.0), (my, rdr("yr"), 10.0, 12.0)];
        let msgs = drain(scan(vec![], axes, 3)).await;
        assert!(
            matches!(&msgs[0], Msg::OpenRun(md) if md.plan_name.as_deref() == Some("scan")),
            "N-motor scan opens plan_name=scan"
        );
        assert_eq!(
            msgs.iter().filter(|m| matches!(m, Msg::Set { .. })).count(),
            6,
            "3 coupled points × 2 axes = 6 Sets (moved together)"
        );
        assert_eq!(
            msgs.iter().filter(|m| matches!(m, Msg::Save)).count(),
            3,
            "3 inner-product points"
        );
    }

    // A single-axis `scan` is an ordinary 1-D scan: same Checkpoint/Set/Save
    // shape as the `scan_1d` convenience.
    #[tokio::test]
    async fn scan_single_axis_matches_scan_1d_shape() {
        let m = || {
            Arc::new(FakeMotor {
                name: "m".into(),
                bias: 0.0,
            }) as Arc<dyn MovableObj>
        };
        let nd = drain(scan(vec![], vec![(m(), rdr("m_rbv"), 0.0, 10.0)], 3)).await;
        let one_d = drain(scan_1d(vec![], m(), rdr("m_rbv"), 0.0, 10.0, 3)).await;
        let shape = |ms: &[Msg]| {
            (
                ms.iter().filter(|m| matches!(m, Msg::Checkpoint)).count(),
                ms.iter().filter(|m| matches!(m, Msg::Set { .. })).count(),
                ms.iter().filter(|m| matches!(m, Msg::Save)).count(),
                ms.len(),
            )
        };
        assert_eq!(
            shape(&nd),
            shape(&one_d),
            "single-axis scan and scan_1d emit the same Msg shape"
        );
    }

    // scan_nd's pos_cache decision reaches the hook as each motor's StepMotor
    // target: `Some` to command, `None` when already at position (skip). A slow
    // axis constant across inner points is marked `None` on those points.
    #[tokio::test]
    async fn scan_nd_per_step_marks_unchanged_motor_none() {
        let m1 = Arc::new(FakeMotor {
            name: "m1".into(),
            bias: 0.0,
        }) as Arc<dyn MovableObj>;
        let m2 = Arc::new(FakeMotor {
            name: "m2".into(),
            bias: 0.0,
        }) as Arc<dyn MovableObj>;
        let masks: Arc<std::sync::Mutex<Vec<Vec<bool>>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let masks_c = masks.clone();
        let per_step: PerStep = Arc::new(
            move |_dets: Vec<Arc<dyn ReadableObj>>, motors: Vec<StepMotor>| {
                masks_c
                    .lock()
                    .unwrap()
                    .push(motors.iter().map(|(_, _, t)| t.is_some()).collect());
                plan_box(async_stream::stream! { yield Msg::Checkpoint; })
            },
        );
        let motors = vec![(m1, rdr("m1r")), (m2, rdr("m2r"))];
        // Slow axis m1 holds 0 across the first two points, then steps to 1.
        let points = vec![vec![0.0, 0.0], vec![0.0, 1.0], vec![1.0, 0.0]];
        let _ = drain(scan_nd_per_step(vec![], motors, points, Some(per_step))).await;
        let got = masks.lock().unwrap().clone();
        assert_eq!(
            got,
            vec![
                vec![true, true],  // first point: pos_cache empty, both command
                vec![false, true], // m1 holds 0 (skip), m2 steps 0->1
                vec![true, true],  // m1 steps 0->1, m2 steps 1->0
            ],
            "pos_cache skip surfaces as StepMotor::None at the hook"
        );
    }

    // Multi-motor list_scan (PLAN-28): the axes' position lists are zipped
    // (inner product), every point commands all motors together, and the run
    // carries plan_name "list_scan".
    #[tokio::test]
    async fn list_scan_nd_zips_motor_positions_inner_product() {
        let m1 = Arc::new(FakeMotor {
            name: "m1".into(),
            bias: 0.0,
        }) as Arc<dyn MovableObj>;
        let m2 = Arc::new(FakeMotor {
            name: "m2".into(),
            bias: 0.0,
        }) as Arc<dyn MovableObj>;
        let axes: Vec<ListScanAxis> = vec![
            (m1, rdr("m1r"), vec![0.0, 1.0, 2.0]),
            (m2, rdr("m2r"), vec![10.0, 11.0, 12.0]),
        ];
        let msgs = drain(list_scan_nd(vec![], axes, None)).await;
        assert!(
            matches!(&msgs[0], Msg::OpenRun(md) if md.plan_name.as_deref() == Some("list_scan")),
            "opens a list_scan run, got {:?}",
            msgs.first()
        );
        // Zipped, not crossed: row i is (m1[i], m2[i]); all distinct so every
        // motor is commanded at every one of the 3 points.
        assert_eq!(set_targets(&msgs, "m1"), vec![0.0, 1.0, 2.0]);
        assert_eq!(set_targets(&msgs, "m2"), vec![10.0, 11.0, 12.0]);
    }

    // Unequal list lengths fail the run before it opens (bluesky's ValueError),
    // rather than silently visiting an empty inner_list_product trajectory.
    #[tokio::test]
    async fn list_scan_nd_unequal_lengths_fail_before_open() {
        let m1 = Arc::new(FakeMotor {
            name: "m1".into(),
            bias: 0.0,
        }) as Arc<dyn MovableObj>;
        let m2 = Arc::new(FakeMotor {
            name: "m2".into(),
            bias: 0.0,
        }) as Arc<dyn MovableObj>;
        let axes: Vec<ListScanAxis> = vec![
            (m1, rdr("m1r"), vec![0.0, 1.0]),
            (m2, rdr("m2r"), vec![10.0]),
        ];
        let msgs = drain(list_scan_nd(vec![], axes, None)).await;
        assert!(
            matches!(msgs.first(), Some(Msg::Fail(_))),
            "unequal list lengths must Fail first, got {msgs:?}"
        );
        assert!(
            !msgs.iter().any(|m| matches!(m, Msg::OpenRun(_))),
            "no run should open on a length mismatch"
        );
    }

    // Faithful to bluesky scan_nd/move_per_step: a motor is not re-commanded at
    // a point where its target repeats the previous one (the pos_cache in
    // scan_nd_with_md). m1 holds 0.0 across the first two points.
    #[tokio::test]
    async fn list_scan_nd_skips_recommanding_repeated_position() {
        let m1 = Arc::new(FakeMotor {
            name: "m1".into(),
            bias: 0.0,
        }) as Arc<dyn MovableObj>;
        let m2 = Arc::new(FakeMotor {
            name: "m2".into(),
            bias: 0.0,
        }) as Arc<dyn MovableObj>;
        let axes: Vec<ListScanAxis> = vec![
            (m1, rdr("m1r"), vec![0.0, 0.0, 1.0]),
            (m2, rdr("m2r"), vec![5.0, 6.0, 7.0]),
        ];
        let msgs = drain(list_scan_nd(vec![], axes, None)).await;
        // m1's middle 0.0 equals the previous target → skipped (no Set).
        assert_eq!(set_targets(&msgs, "m1"), vec![0.0, 1.0]);
        // m2 changes each point → commanded each point.
        assert_eq!(set_targets(&msgs, "m2"), vec![5.0, 6.0, 7.0]);
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

    #[test]
    fn snake_axes_to_flags_resolves_bluesky_spec() {
        // None snakes nothing; All snakes every axis but the slowest (index 0);
        // Axes(list) snakes exactly the listed 0-based indices.
        assert_eq!(SnakeAxes::None.to_flags(3), vec![false, false, false]);
        assert_eq!(SnakeAxes::All.to_flags(3), vec![false, true, true]);
        assert_eq!(
            SnakeAxes::Axes(vec![0, 2]).to_flags(3),
            vec![true, false, true]
        );
    }

    #[tokio::test]
    async fn grid_scan_snake_reverses_fast_axis_positions() {
        // 2 slow rows x 3 fast points. With snake, the fast axis (m2) runs
        // forward on row 0 and reversed on row 1, so its Set values are
        // [0,1,2, 2,1,0] — a continuous boustrophedon, no fly-back.
        let m1 = Arc::new(FakeMotor {
            name: "m1".into(),
            bias: 0.0,
        }) as Arc<dyn MovableObj>;
        let m2 = Arc::new(FakeMotor {
            name: "m2".into(),
            bias: 0.0,
        }) as Arc<dyn MovableObj>;
        let msgs = drain(grid_scan_snake(
            vec![],
            m1,
            rdr("m1r"),
            0.0,
            1.0,
            2,
            m2,
            rdr("m2r"),
            0.0,
            2.0,
            3,
            true,
        ))
        .await;
        let m2_targets: Vec<f64> = msgs
            .iter()
            .filter_map(|m| match m {
                Msg::Set { obj, value, .. } if obj.name() == "m2" => Some(*value),
                _ => None,
            })
            .collect();
        assert_eq!(m2_targets, vec![0.0, 1.0, 2.0, 2.0, 1.0, 0.0]);
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
        }) as Arc<dyn crate::core::msg::LocatableObj>;
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
        impl crate::core::msg::LocatableObj for SplitMotor {
            async fn locate_dyn(&self) -> Result<DynLocation, crate::core::error::BsrsError> {
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

        let motor: Arc<dyn crate::core::msg::LocatableObj> = Arc::new(SplitMotor);

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

    // -- PLAN-10: multi-motor mv / mvr --------------------------------------

    #[tokio::test]
    async fn mv_many_fires_all_motors_into_one_group_and_waits_once() {
        // bluesky mv(*args) sets every motor into one group and waits ONCE, so
        // the moves run in parallel behind a single barrier.
        struct M(&'static str);
        impl NamedObj for M {
            fn name(&self) -> &str {
                self.0
            }
        }
        #[async_trait::async_trait]
        impl MovableObj for M {
            async fn set_dyn(&self, _value: f64) -> Status {
                Status::done()
            }
        }
        let m1: Arc<dyn MovableObj> = Arc::new(M("x"));
        let m2: Arc<dyn MovableObj> = Arc::new(M("y"));
        let m3: Arc<dyn MovableObj> = Arc::new(M("z"));

        let msgs = drain(stubs::mv_many(vec![(m1, 1.0), (m2, 2.0), (m3, 3.0)])).await;

        let sets: Vec<(&str, f64, Option<&str>)> = msgs
            .iter()
            .filter_map(|m| match m {
                Msg::Set { obj, value, group } => Some((obj.name(), *value, group.as_deref())),
                _ => None,
            })
            .collect();
        assert_eq!(
            sets,
            vec![
                ("x", 1.0, Some("mv")),
                ("y", 2.0, Some("mv")),
                ("z", 3.0, Some("mv")),
            ],
            "one Set per motor, all in the shared \"mv\" group, in order"
        );
        let waits: Vec<&str> = msgs
            .iter()
            .filter_map(|m| match m {
                Msg::Wait { group, .. } => Some(group.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(waits, vec!["mv"], "exactly one Wait, after all Sets");
        assert!(
            matches!(msgs.last(), Some(Msg::Wait { .. })),
            "the single Wait is the last message (parallel barrier)"
        );
    }

    #[tokio::test]
    async fn mvr_many_bases_each_target_on_its_own_setpoint_then_one_wait() {
        struct L {
            name: &'static str,
            setpoint: f64,
        }
        impl NamedObj for L {
            fn name(&self) -> &str {
                self.name
            }
        }
        #[async_trait::async_trait]
        impl MovableObj for L {
            async fn set_dyn(&self, _value: f64) -> Status {
                Status::done()
            }
        }
        #[async_trait::async_trait]
        impl crate::core::msg::LocatableObj for L {
            async fn locate_dyn(&self) -> Result<DynLocation, crate::core::error::BsrsError> {
                Ok(DynLocation {
                    setpoint: self.setpoint,
                    readback: 0.0,
                })
            }
        }
        let a: Arc<dyn crate::core::msg::LocatableObj> = Arc::new(L {
            name: "a",
            setpoint: 5.0,
        });
        let b: Arc<dyn crate::core::msg::LocatableObj> = Arc::new(L {
            name: "b",
            setpoint: 10.0,
        });

        let msgs = drain(stubs::mvr_many(vec![(a, 2.0), (b, -3.0)])).await;

        let sets: Vec<(&str, f64)> = msgs
            .iter()
            .filter_map(|m| match m {
                Msg::Set { obj, value, group } => {
                    assert_eq!(group.as_deref(), Some("mv"), "shared group");
                    Some((obj.name(), *value))
                }
                _ => None,
            })
            .collect();
        // a: 5.0 + 2.0 = 7.0 ; b: 10.0 + (-3.0) = 7.0 — each on its own setpoint.
        assert_eq!(sets, vec![("a", 7.0), ("b", 7.0)]);
        assert_eq!(
            msgs.iter()
                .filter(|m| matches!(m, Msg::Wait { .. }))
                .count(),
            1,
            "one Wait for the whole group"
        );
        assert!(matches!(msgs.last(), Some(Msg::Wait { .. })));
    }

    #[tokio::test]
    async fn mvr_many_locate_failure_aborts_before_any_set() {
        // All setpoints are read BEFORE any Set is emitted, so a locate failure
        // on any motor fails the run before a single motor starts moving.
        struct GoodLoc;
        impl NamedObj for GoodLoc {
            fn name(&self) -> &str {
                "good"
            }
        }
        #[async_trait::async_trait]
        impl MovableObj for GoodLoc {
            async fn set_dyn(&self, _value: f64) -> Status {
                Status::done()
            }
        }
        #[async_trait::async_trait]
        impl crate::core::msg::LocatableObj for GoodLoc {
            async fn locate_dyn(&self) -> Result<DynLocation, crate::core::error::BsrsError> {
                Ok(DynLocation {
                    setpoint: 1.0,
                    readback: 1.0,
                })
            }
        }
        struct BadLoc;
        impl NamedObj for BadLoc {
            fn name(&self) -> &str {
                "bad"
            }
        }
        #[async_trait::async_trait]
        impl MovableObj for BadLoc {
            async fn set_dyn(&self, _value: f64) -> Status {
                Status::done()
            }
        }
        #[async_trait::async_trait]
        impl crate::core::msg::LocatableObj for BadLoc {
            async fn locate_dyn(&self) -> Result<DynLocation, crate::core::error::BsrsError> {
                Err(crate::core::error::BsrsError::Plan("no readback".into()))
            }
        }
        let good: Arc<dyn crate::core::msg::LocatableObj> = Arc::new(GoodLoc);
        let bad: Arc<dyn crate::core::msg::LocatableObj> = Arc::new(BadLoc);

        // Good motor first, bad second: the good setpoint reads OK, but the bad
        // locate aborts before any Set is emitted.
        let msgs = drain(stubs::mvr_many(vec![(good, 1.0), (bad, 1.0)])).await;

        assert!(
            !msgs.iter().any(|m| matches!(m, Msg::Set { .. })),
            "no motion may start before every setpoint is resolved"
        );
        assert!(
            !msgs.iter().any(|m| matches!(m, Msg::Wait { .. })),
            "no barrier without any motion"
        );
        assert!(
            matches!(msgs.last(), Some(Msg::Fail(_))),
            "a locate failure aborts the run via Msg::Fail"
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

    // broadcast_msg (PLAN-29): the generic typed fan applies the per-object
    // builder to each object, in list order, and to nothing else.
    #[tokio::test]
    async fn broadcast_msg_fans_builder_across_objects_in_order() {
        let objs: Vec<Arc<dyn crate::core::msg::TriggerableObj>> = (0..3)
            .map(|i| {
                Arc::new(FakeTriggerable(format!("t{i}")))
                    as Arc<dyn crate::core::msg::TriggerableObj>
            })
            .collect();
        let msgs = drain(stubs::broadcast_msg(objs, |o| Msg::Trigger {
            obj: o,
            group: Some("g".into()),
        }))
        .await;
        assert_eq!(msgs.len(), 3, "one message per object, got {msgs:#?}");
        for (i, m) in msgs.iter().enumerate() {
            assert!(
                matches!(m, Msg::Trigger { obj, group }
                    if obj.name() == format!("t{i}") && group.as_deref() == Some("g")),
                "message {i} must Trigger t{i} in group g, got {m:?}"
            );
        }
    }

    // broadcast_msg over an empty list yields nothing (the `for` runs zero
    // times) — the boundary bluesky's generator also produces.
    #[tokio::test]
    async fn broadcast_msg_empty_objects_yields_no_messages() {
        let objs: Vec<Arc<dyn crate::core::msg::StageableObj>> = Vec::new();
        let msgs = drain(stubs::broadcast_msg(objs, Msg::Stage)).await;
        assert!(msgs.is_empty(), "empty object list fans no messages");
    }

    // fly over several flyers kicks each off under one "kick" group and waits,
    // completes each under one "complete" group and waits, then collects each —
    // the multi-flyer generalization of bluesky's `fly` (PLAN-07).
    #[tokio::test]
    async fn fly_kicks_completes_then_collects_each_flyer() {
        let msgs = drain(fly(flyer_pairs(2))).await;

        // OpenRun("fly"); Kickoff×2/kick; Wait/kick; Complete×2/complete;
        // Wait/complete; Collect×2; CloseRun.
        assert_eq!(msgs.len(), 10, "got {msgs:#?}");
        assert!(
            matches!(&msgs[0], Msg::OpenRun(md) if md.plan_name.as_deref() == Some("fly")),
            "first msg is OpenRun(fly): {:?}",
            msgs[0]
        );

        // Both flyers kicked off first, sharing the "kick" group, then one Wait.
        assert_eq!(kickoff_group(&msgs[1]), Some("kick"));
        assert_eq!(kickoff_group(&msgs[2]), Some("kick"));
        assert!(
            matches!(&msgs[3], Msg::Wait { group, .. } if group == "kick"),
            "msg[3] waits on kick: {:?}",
            msgs[3]
        );

        // Then both completed under the "complete" group, then one Wait.
        assert_eq!(complete_group(&msgs[4]), Some("complete"));
        assert_eq!(complete_group(&msgs[5]), Some("complete"));
        assert!(
            matches!(&msgs[6], Msg::Wait { group, .. } if group == "complete"),
            "msg[6] waits on complete: {:?}",
            msgs[6]
        );

        // Each flyer's collectable is collected, in order.
        assert_eq!(collect_name(&msgs[7]), Some("fly0"));
        assert_eq!(collect_name(&msgs[8]), Some("fly1"));

        assert!(
            matches!(&msgs[9], Msg::CloseRun { exit_status, .. } if exit_status == "success"),
            "last msg is a successful CloseRun: {:?}",
            msgs[9]
        );
    }

    // An empty flyer list opens and closes the run with nothing in between —
    // no spurious kickoff, wait, or collect (bluesky's `for flyer in flyers`
    // loops iterate zero times).
    #[tokio::test]
    async fn fly_with_no_flyers_only_opens_and_closes() {
        let msgs = drain(fly(Vec::new())).await;
        assert_eq!(msgs.len(), 2, "got {msgs:#?}");
        assert!(matches!(&msgs[0], Msg::OpenRun(_)));
        assert!(matches!(&msgs[1], Msg::CloseRun { .. }));
    }

    // collect_while_completing (PLAN-08). Boundary: the group reports done on the
    // very first Wait (flush_period=None, flyers already finished). Every flyer is
    // completed up front against one shared group, then a single Wait/Collect
    // cycle runs — one collect per detector — and the loop stops. All Completes
    // and the Wait must share the minted "complete-N" group.
    #[tokio::test]
    async fn collect_while_completing_single_cycle_when_done_immediately() {
        let msgs = drain_completing(
            stubs::collect_while_completing(flyers(2), colls(2), None, None),
            vec![true].into_iter(),
        )
        .await;

        // Complete×2 (shared group); Wait (same group); Collect×2 (det0, det1).
        assert_eq!(
            msgs.iter().map(msg_kind).collect::<Vec<_>>(),
            vec!["complete", "complete", "wait", "collect", "collect"],
            "got {msgs:#?}"
        );
        let g = complete_group(&msgs[0])
            .expect("complete group")
            .to_string();
        assert!(
            g.starts_with("complete-"),
            "group is short_uid-minted: {g:?}"
        );
        assert_eq!(complete_group(&msgs[1]), Some(g.as_str()));
        assert_eq!(
            wait_group_name(&msgs[2]),
            Some(g.as_str()),
            "the Wait must target the same group the flyers complete against"
        );
        assert_eq!(collect_name(&msgs[3]), Some("det0"));
        assert_eq!(collect_name(&msgs[4]), Some("det1"));
    }

    // collect_while_completing (PLAN-08). Boundary: the group is NOT done on the
    // first two Waits (move-on flushes) and done on the third. Each Wait — the
    // terminal one included — is followed by one collect per detector, matching
    // bluesky's `while not done: done = wait(...); collect(...)` (the collect runs
    // after the wait that reports done, then the loop exits).
    #[tokio::test]
    async fn collect_while_completing_flushes_each_period_until_done() {
        let flush = Some(Duration::from_millis(5));
        let msgs = drain_completing(
            stubs::collect_while_completing(flyers(1), colls(1), flush, Some("primary".into())),
            vec![false, false, true].into_iter(),
        )
        .await;

        // Complete×1; then 3× (Wait, Collect) — two move-on flushes plus the
        // terminal cycle. No extra Wait or Collect after done.
        assert_eq!(
            msgs.iter().map(msg_kind).collect::<Vec<_>>(),
            vec!["complete", "wait", "collect", "wait", "collect", "wait", "collect",],
            "got {msgs:#?}"
        );
        // Named-stream collects carry the requested stream name.
        for m in msgs.iter().filter(|m| matches!(m, Msg::Collect { .. })) {
            assert!(matches!(m, Msg::Collect { stream_name: Some(n), .. } if n == "primary"));
        }
    }

    // collect_while_completing (PLAN-08). Boundary: the engine drops the Wait
    // responder (the run failed before answering). The plan must stop rather than
    // await a response that will never arrive, and must NOT emit a trailing
    // collect after the failure.
    #[tokio::test]
    async fn collect_while_completing_stops_when_wait_responder_dropped() {
        let mut plan = stubs::collect_while_completing(flyers(1), colls(1), None, None);
        let mut kinds = Vec::new();
        while let Some(item) = plan.next().await {
            match item {
                PlanItem::Bare(m) => kinds.push(msg_kind(&m)),
                PlanItem::Respond(m, tx) => {
                    kinds.push(msg_kind(&m));
                    drop(tx); // engine failed the Wait: no response will come.
                }
            }
        }
        assert_eq!(
            kinds,
            vec!["complete", "wait"],
            "a dropped Wait responder stops the loop with no trailing collect"
        );
    }

    // collect_while_completing (PLAN-08). Boundary: no flyers. bluesky's
    // `for flyer in flyers` loops zero times, so nothing is completed; the first
    // Wait on the (empty) group reports done immediately and one flush of each
    // detector still runs.
    #[tokio::test]
    async fn collect_while_completing_with_no_flyers_still_collects_once() {
        let msgs = drain_completing(
            stubs::collect_while_completing(Vec::new(), colls(2), None, None),
            vec![true].into_iter(),
        )
        .await;
        assert_eq!(
            msgs.iter().map(msg_kind).collect::<Vec<_>>(),
            vec!["wait", "collect", "collect"],
            "no flyers: one Wait then one collect per detector, got {msgs:#?}"
        );
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
