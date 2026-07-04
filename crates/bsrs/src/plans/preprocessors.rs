//! `bluesky.preprocessors` equivalents — wrappers that transform a `Plan`.
//!
//! These take a `Plan` (a stream of `Msg`) and return a new `Plan` whose
//! emitted messages are mutated, prepended, appended, or interleaved.

use crate::core::msg::{
    CollectableObj, FlyableObj, LocatableObj, MonitorableObj, Msg, ReadableObj, RunMetadata,
    StageableObj,
};
use crate::core::plan::{plan_box, Plan, PlanItem};
use futures::StreamExt;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Drain a `Plan` stream into a Vec of messages. Useful when a wrapper
/// needs random access (e.g. `relative_set_wrapper` rewrites Set values).
#[allow(dead_code)]
async fn drain(mut plan: Plan) -> Vec<Msg> {
    let mut out = Vec::new();
    while let Some(item) = plan.next().await {
        let PlanItem::Bare(m) = item;
        out.push(m);
    }
    out
}

/// `plan_mutator(plan, f)` — for each `Msg` from `plan`, call `f(msg)`. If
/// `f` returns `Some(replacement_plan)`, the replacement is yielded
/// instead. Otherwise the original `Msg` is yielded unchanged.
///
/// Mirrors `bluesky.preprocessors.plan_mutator`.
pub fn plan_mutator<F>(inner: Plan, mut f: F) -> Plan
where
    F: FnMut(&Msg) -> Option<Plan> + Send + 'static,
{
    plan_box(async_stream::stream! {
        let mut inner = inner;
        while let Some(item) = inner.next().await {
            let PlanItem::Bare(m) = item;
            if let Some(repl) = f(&m) {
                let mut r = repl;
                while let Some(it) = r.next().await {
                    let PlanItem::Bare(rm) = it;
                    yield rm;
                }
            } else {
                yield m;
            }
        }
    })
}

/// `msg_mutator(plan, f)` — replace each `Msg` with `f(msg)`. Like
/// `plan_mutator` but always 1:1, so no replacement-stream draining.
pub fn msg_mutator<F>(inner: Plan, mut f: F) -> Plan
where
    F: FnMut(Msg) -> Msg + Send + 'static,
{
    plan_box(async_stream::stream! {
        let mut inner = inner;
        while let Some(item) = inner.next().await {
            let PlanItem::Bare(m) = item;
            yield f(m);
        }
    })
}

/// `pchain(plans...)` — chain a sequence of plans, yielding each
/// inner plan's messages in order.
pub fn pchain(plans: Vec<Plan>) -> Plan {
    plan_box(async_stream::stream! {
        for plan in plans {
            let mut p = plan;
            while let Some(item) = p.next().await {
                let PlanItem::Bare(m) = item;
                yield m;
            }
        }
    })
}

/// `run_wrapper(plan, md)` — bookend `plan` with `OpenRun(md)` and
/// `CloseRun("success")`. If the inner plan already has its own
/// open/close (e.g. it was the body of a higher-level plan), this will
/// emit nested run messages — caller's responsibility.
pub fn run_wrapper(inner: Plan, md: RunMetadata) -> Plan {
    plan_box(async_stream::stream! {
        yield Msg::OpenRun(md);
        let mut inner = inner;
        while let Some(item) = inner.next().await {
            let PlanItem::Bare(m) = item;
            yield m;
        }
        yield Msg::CloseRun { exit_status: "success".into(), reason: None };
    })
}

/// `inject_md_wrapper(plan, md_extra)` — for every `OpenRun` the inner
/// plan emits, merge `md_extra` into its metadata.
pub fn inject_md_wrapper(inner: Plan, md_extra: HashMap<String, Value>) -> Plan {
    msg_mutator(inner, move |m| match m {
        Msg::OpenRun(mut meta) => {
            for (k, v) in md_extra.clone() {
                meta.extra.entry(k).or_insert(v);
            }
            Msg::OpenRun(meta)
        }
        other => other,
    })
}

/// `rewindable_wrapper(plan, on)` — wrap `plan` with a `Rewindable(on)`
/// at the start and `Rewindable(prev)` at the end. The "previous" state
/// is unknown to the wrapper so we restore to `true` (the default).
pub fn rewindable_wrapper(inner: Plan, on: bool) -> Plan {
    plan_box(async_stream::stream! {
        yield Msg::Rewindable(on);
        let mut inner = inner;
        while let Some(item) = inner.next().await {
            let PlanItem::Bare(m) = item;
            yield m;
        }
        yield Msg::Rewindable(true);
    })
}

/// `during_run_wrapper(plan, after_open, before_close)` — insert messages at
/// the run boundary: `after_open()` is yielded immediately after each
/// `OpenRun`, and `before_close()` immediately before each `CloseRun`. The
/// `OpenRun`/`CloseRun` messages themselves are preserved.
///
/// This is the single owner of "insert inside the run envelope", mirroring
/// bluesky's `insert_after_open` / `insert_before_close` `plan_mutator` pair.
/// Run-scoped messages (`Create`, `Collect`, `Monitor`) inserted this way land
/// *inside* the run, where the engine's bundler is open — prepending/appending
/// them around the whole plan puts them outside the run, where `Create`/
/// `Collect` fail with "… with no open run".
///
/// A run-free inner plan (no `OpenRun`/`CloseRun`) gets no insertion, matching
/// bluesky's "during runs" contract.
fn during_run_wrapper<FA, FB>(inner: Plan, mut after_open: FA, mut before_close: FB) -> Plan
where
    FA: FnMut() -> Vec<Msg> + Send + 'static,
    FB: FnMut() -> Vec<Msg> + Send + 'static,
{
    plan_box(async_stream::stream! {
        let mut inner = inner;
        while let Some(item) = inner.next().await {
            let PlanItem::Bare(m) = item;
            if matches!(m, Msg::OpenRun(_)) {
                yield m;
                for msg in after_open() {
                    yield msg;
                }
            } else if matches!(m, Msg::CloseRun { .. }) {
                for msg in before_close() {
                    yield msg;
                }
                yield m;
            } else {
                yield m;
            }
        }
    })
}

/// `monitor_during_wrapper(plan, signals)` — `Monitor` each signal immediately
/// after the run opens and `Unmonitor` immediately before it closes, so the
/// monitor streams live *inside* the run envelope. Mirrors bluesky's
/// `monitor_during_wrapper` (preprocessors.py:813), which inserts via
/// `plan_mutator` after `open_run` / before `close_run`. A run-free inner plan
/// gets no insertion.
///
/// Each monitor's event stream is named `{signal}_monitor`, matching bluesky
/// (`Msg("monitor", sig, name=sig.name + "_monitor")`, preprocessors.py:836).
/// The engine keys the pump by the object's identity, so `Unmonitor` still
/// removes it regardless of the stream name.
pub fn monitor_during_wrapper(inner: Plan, signals: Vec<Arc<dyn MonitorableObj>>) -> Plan {
    let unmon = signals.clone();
    during_run_wrapper(
        inner,
        move || {
            signals
                .iter()
                .map(|s| Msg::Monitor {
                    obj: s.clone(),
                    name: Some(format!("{}_monitor", s.name())),
                })
                .collect()
        },
        move || {
            unmon
                .iter()
                .rev()
                .map(|s| Msg::Unmonitor(s.clone()))
                .collect()
        },
    )
}

/// `stage_wrapper(plan, devices)` — `Stage` each device before the inner
/// plan, `Unstage` (LIFO) after. Same envelope contract as bluesky.
pub fn stage_wrapper(inner: Plan, devices: Vec<Arc<dyn StageableObj>>) -> Plan {
    plan_box(async_stream::stream! {
        for d in &devices {
            yield Msg::Stage(d.clone());
        }
        let mut inner = inner;
        while let Some(item) = inner.next().await {
            let PlanItem::Bare(m) = item;
            yield m;
        }
        for d in devices.into_iter().rev() {
            yield Msg::Unstage(d);
        }
    })
}

/// `baseline_wrapper(plan, devices, name)` — reads each device once into the
/// named stream (default `"baseline"`) immediately after the run opens and
/// again immediately before it closes, as a `Create + Read* + Save` block.
/// Mirrors bluesky's `baseline_wrapper` (preprocessors.py:1202), which records
/// the baseline after `open_run` and before `close_run` — inside the run, so
/// the `Create` finds an open bundler. A run-free inner plan gets no baseline.
pub fn baseline_wrapper(
    inner: Plan,
    devices: Vec<Arc<dyn ReadableObj>>,
    name: impl Into<String>,
) -> Plan {
    let stream = name.into();
    let pre_stream = stream.clone();
    let pre_devices = devices.clone();
    during_run_wrapper(
        inner,
        move || {
            let mut msgs = vec![Msg::Create {
                stream_name: pre_stream.clone(),
            }];
            msgs.extend(pre_devices.iter().map(|d| Msg::Read(d.clone())));
            msgs.push(Msg::Save);
            msgs
        },
        move || {
            let mut msgs = vec![Msg::Create {
                stream_name: stream.clone(),
            }];
            msgs.extend(devices.iter().map(|d| Msg::Read(d.clone())));
            msgs.push(Msg::Save);
            msgs
        },
    )
}

/// `finalize_wrapper(plan, final_plan)` — run `final_plan` after `plan`
/// regardless of outcome. (This is a *plan-level* bracket; it does not
/// catch panics or engine-side aborts on its own. The engine's own
/// cleanup chain — unstage / stop_movables — runs separately.)
pub fn finalize_wrapper(inner: Plan, final_plan: Plan) -> Plan {
    plan_box(async_stream::stream! {
        let mut inner = inner;
        while let Some(item) = inner.next().await {
            let PlanItem::Bare(m) = item;
            yield m;
        }
        let mut fin = final_plan;
        while let Some(item) = fin.next().await {
            let PlanItem::Bare(m) = item;
            yield m;
        }
    })
}

/// `subs_wrapper(plan, subs)` — prepend a setup sequence that registers
/// document callbacks at the engine level. bsrs's engine handles
/// callbacks via `RunEngine::new(vec![...])` at construction time, so
/// this wrapper is a *no-op* for new code: subscribe at engine creation
/// time instead. Provided for API parity.
pub fn subs_wrapper<F>(inner: Plan, _subs: F) -> Plan
where
    F: Send + 'static,
{
    inner
}

/// `relative_set_wrapper(plan, motors)` — for each motor in `motors`,
/// rewrite every `Msg::Set { obj == motor, value }` into `value + setpoint`,
/// where `setpoint` is the motor's commanded position. The base is captured
/// *lazily* at each motor's **first** `Set` via `locate_dyn` — not eagerly at
/// wrapper entry. Mirrors bluesky's `relative_set_wrapper`, whose `insert_reads`
/// chains `__read_and_stash_a_motor` before the first set per motor
/// (preprocessors.py:1136-1148) and stashes `location["setpoint"]` for a
/// `Locatable` (preprocessors.py:1061) — not the readback, so a relative move is
/// deterministic from the commanded position regardless of motor jitter/lag.
/// Lazy capture means the base reflects the motor's position at the moment it is
/// first moved, and a listed motor the plan never sets is never located. After
/// the inner plan, no automatic restore; pair with `reset_positions_wrapper`.
pub fn relative_set_wrapper(inner: Plan, motors: Vec<Arc<dyn LocatableObj>>) -> Plan {
    plan_box(async_stream::stream! {
        // Motors eligible for relative rewriting, indexed by name.
        let by_name: HashMap<String, Arc<dyn LocatableObj>> =
            motors.iter().map(|m| (m.name().to_string(), m.clone())).collect();
        // Per-motor base setpoints, filled lazily at each motor's first `Set`.
        let mut bases: HashMap<String, f64> = HashMap::new();
        let mut inner = inner;
        while let Some(item) = inner.next().await {
            let PlanItem::Bare(m) = item;
            let m = match m {
                Msg::Set { obj, value, group } if by_name.contains_key(obj.name()) => {
                    let bias = match bases.get(obj.name()) {
                        Some(b) => *b,
                        None => {
                            let base = by_name
                                .get(obj.name())
                                .expect("guard ensures the motor is eligible")
                                .locate_dyn()
                                .await
                                .map(|loc| loc.setpoint)
                                .unwrap_or(0.0);
                            bases.insert(obj.name().to_string(), base);
                            base
                        }
                    };
                    Msg::Set { obj, value: value + bias, group }
                }
                other => other,
            };
            yield m;
        }
    })
}

/// `print_summary_wrapper(plan)` — debug-print every Msg as it flows
/// through. The Msg is printed via its `Debug` impl to stderr just
/// before being yielded to the engine. Mirrors bluesky's
/// `print_summary_wrapper` in spirit (bsrs emits each line eagerly
/// rather than first collecting the full plan).
pub fn print_summary_wrapper(inner: Plan) -> Plan {
    plan_box(async_stream::stream! {
        let mut inner = inner;
        let mut idx = 0usize;
        while let Some(item) = inner.next().await {
            let PlanItem::Bare(m) = item;
            eprintln!("[plan {idx:04}] {m:?}");
            idx += 1;
            yield m;
        }
    })
}

/// `suspend_wrapper(plan, suspender)` — install `suspender` for the
/// duration of `plan`, remove on exit. Mirrors bluesky's
/// `suspend_wrapper`.
pub fn suspend_wrapper(inner: Plan, suspender: Arc<dyn crate::core::Suspender>) -> Plan {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(1);
    let id = SEQ.fetch_add(1, Ordering::Relaxed);
    plan_box(async_stream::stream! {
        let any: Arc<dyn std::any::Any + Send + Sync> = Arc::new(suspender.clone());
        yield Msg::InstallSuspender { id, suspender: any };
        let mut inner = inner;
        while let Some(item) = inner.next().await {
            let PlanItem::Bare(m) = item;
            yield m;
        }
        yield Msg::RemoveSuspender { id };
    })
}

/// `fly_during_wrapper(plan, flyers)` — `Kickoff` each `(flyer, collectable)`
/// pair immediately after the run opens and `Complete + Collect` immediately
/// before it closes, so the fly kickoff/collect live *inside* the run envelope.
/// Mirrors bluesky's `fly_during_wrapper` (preprocessors.py:866), which inserts
/// via `plan_mutator` after `open_run` / before `close_run`. Pairs the Flyable +
/// Collectable explicitly since bsrs's protocol traits split those roles.
///
/// The kickoff/complete `wait` is only inserted when there is at least one
/// flyer (`if flyers:`, preprocessors.py:895) — a wait on a group that received
/// no Kickoff/Complete is a spurious message. A run-free inner plan gets no
/// insertion (and would have failed anyway: `Collect` requires an open run).
pub fn fly_during_wrapper(
    inner: Plan,
    flyers: Vec<(Arc<dyn FlyableObj>, Arc<dyn CollectableObj>)>,
) -> Plan {
    let any_flyers = !flyers.is_empty();
    let close_flyers = flyers.clone();
    during_run_wrapper(
        inner,
        move || {
            let mut msgs: Vec<Msg> = flyers
                .iter()
                .map(|(f, _)| Msg::Kickoff {
                    obj: f.clone(),
                    group: Some("fly_kick".into()),
                })
                .collect();
            if any_flyers {
                msgs.push(Msg::Wait {
                    group: "fly_kick".into(),
                    error_on_timeout: true,
                    timeout: None,
                });
            }
            msgs
        },
        move || {
            let mut msgs: Vec<Msg> = close_flyers
                .iter()
                .map(|(f, _)| Msg::Complete {
                    obj: f.clone(),
                    group: Some("fly_done".into()),
                })
                .collect();
            if any_flyers {
                msgs.push(Msg::Wait {
                    group: "fly_done".into(),
                    error_on_timeout: true,
                    timeout: None,
                });
            }
            msgs.extend(close_flyers.iter().map(|(_, c)| Msg::Collect {
                obj: c.clone(),
                stream_name: None,
            }));
            msgs
        },
    )
}

/// `contingency_wrapper(plan, except_plan, else_plan, final_plan, auto_raise)`
/// — bluesky's full try/except/else/finally plan bracket
/// (preprocessors.py:508). Unlike [`finalize_wrapper`] (a pure stream bracket
/// that only appends `final_plan` when `plan` completes normally), this observes
/// a message *error* mid-plan and branches on it:
///
/// - `except_plan` runs when a message in `plan` errored. After it runs, the
///   original error is re-raised iff `auto_raise` (bluesky's default `True`),
///   so the run still fails; `auto_raise = false` swallows the error and the
///   run continues. A `None` `except_plan` re-raises unconditionally.
/// - `else_plan` runs when `plan` completed with no error.
/// - `final_plan` runs on both paths, before any re-raise propagates — matching
///   Python `finally` executing before the exception leaves the block.
///
/// The error observation is powered by the engine's contingency stack: this
/// wrapper pushes an error sink ([`Msg::PushContingency`]) around `plan`, so a
/// message error is routed into the sink (and the run kept alive) instead of
/// failing immediately; the wrapper reads the sink right after forwarding the
/// offending message. `except`/`else`/`final` run *outside* this wrapper's own
/// contingency (it pops first), so an error in them propagates outward.
///
/// Deviation from bluesky: `except_plan` is a pre-built `Plan`, not a function
/// of the exception — bsrs surfaces the error only as a reason string, threaded
/// onto the re-raised [`Msg::Fail`]. Recovery plans that only need to clean up
/// (e.g. `drop`) do not need the value.
///
/// Engine-teardown paths (abort/halt dropping the plan future) are bsrs's
/// analogue of `GeneratorExit`: the wrapper's future is dropped, so nothing
/// after the suspended `yield` runs — `final_plan` is skipped, matching
/// bluesky's `cleanup = False` branch.
pub fn contingency_wrapper(
    inner: Plan,
    except_plan: Option<Plan>,
    else_plan: Option<Plan>,
    final_plan: Option<Plan>,
    auto_raise: bool,
) -> Plan {
    plan_box(async_stream::stream! {
        let sink: crate::core::msg::ContingencySink = Arc::new(Mutex::new(None));
        yield Msg::PushContingency(sink.clone());

        // try: forward `inner`. A message error under this contingency is
        // written into `sink` by the engine (which keeps running); we detect it
        // right after forwarding the offending message.
        let mut errored: Option<String> = None;
        let mut inner = inner;
        while let Some(item) = inner.next().await {
            let PlanItem::Bare(m) = item;
            yield m;
            if let Some(e) = sink.lock().unwrap().take() {
                errored = Some(e);
                break;
            }
        }

        // Leave our contingency region before running recovery, so an error in
        // the except/else/finally plans propagates outward (to an enclosing
        // contingency, or fails the run) rather than looping back into us.
        yield Msg::PopContingency;

        // except / else — mutually exclusive.
        let mut reraise: Option<String> = None;
        match errored {
            Some(reason) => match except_plan {
                Some(ep) => {
                    let mut ep = ep;
                    while let Some(item) = ep.next().await {
                        let PlanItem::Bare(m) = item;
                        yield m;
                    }
                    if auto_raise {
                        reraise = Some(reason);
                    }
                }
                None => reraise = Some(reason),
            },
            None => {
                if let Some(ep) = else_plan {
                    let mut ep = ep;
                    while let Some(item) = ep.next().await {
                        let PlanItem::Bare(m) = item;
                        yield m;
                    }
                }
            }
        }

        // finally: both paths, before any re-raise leaves the block.
        if let Some(fp) = final_plan {
            let mut fp = fp;
            while let Some(item) = fp.next().await {
                let PlanItem::Bare(m) = item;
                yield m;
            }
        }

        if let Some(reason) = reraise {
            yield Msg::Fail(reason);
        }
    })
}

/// `reset_positions_wrapper(plan, motors)` — capture each motor's position
/// *lazily* at its **first** `Set`, run the inner plan, then issue `mv` back to
/// the captured position for each motor that was actually moved, in first-moved
/// order. Mirrors bluesky's `reset_positions_wrapper`, whose `insert_reads`
/// populates an `OrderedDict` at the first set per motor
/// (preprocessors.py:1177-1189) and whose `reset()` replays those positions in
/// insertion order (preprocessors.py:1191-1197). Eager capture at wrapper entry
/// diverged twice: it restored *every* listed motor even if the plan never moved
/// it, and it snapshotted at entry rather than at the moment of first motion.
///
/// The trailing reset runs only when the inner stream completes normally; the
/// engine's one-way plan stream cannot deliver an abort back into this wrapper,
/// so an engine-side abort skips the reset (unlike bluesky's `finalize_wrapper`
/// composition). That gap is a property of the stream model, not this capture.
pub fn reset_positions_wrapper(inner: Plan, motors: Vec<Arc<dyn LocatableObj>>) -> Plan {
    plan_box(async_stream::stream! {
        let by_name: HashMap<String, Arc<dyn LocatableObj>> =
            motors.iter().map(|m| (m.name().to_string(), m.clone())).collect();
        // Positions to restore, captured lazily at first `Set`, in first-moved
        // order. `seen` gates the one-shot capture per eligible motor.
        let mut initial: Vec<(Arc<dyn crate::core::msg::MovableObj>, f64)> = Vec::new();
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut inner = inner;
        while let Some(item) = inner.next().await {
            let PlanItem::Bare(m) = item;
            if let Msg::Set { obj, .. } = &m {
                if by_name.contains_key(obj.name()) && seen.insert(obj.name().to_string()) {
                    let motor = by_name
                        .get(obj.name())
                        .expect("guard ensures the motor is eligible");
                    if let Ok(loc) = motor.locate_dyn().await {
                        initial.push((
                            motor.clone() as Arc<dyn crate::core::msg::MovableObj>,
                            loc.setpoint,
                        ));
                    }
                }
            }
            yield m;
        }
        for (mv_obj, val) in initial {
            yield Msg::Set { obj: mv_obj, value: val, group: Some("reset".into()) };
        }
        yield Msg::Wait { group: "reset".into(), error_on_timeout: true, timeout: None };
    })
}

/// `configure_count_time_wrapper(plan, time, detectors)` — yields a
/// one-shot `Msg::Configure` for each detector setting the
/// `"count_time"` field to `time`, then runs `inner`. Mirrors
/// bluesky's `configure_count_time_wrapper`. Useful as a quick
/// "all-detectors-set-the-same-exposure" knob without having to
/// bake it into every detector device's API.
///
/// Detectors that don't accept `"count_time"` will surface as
/// `Configure`-time errors via the engine; this wrapper does not
/// suppress them.
pub fn configure_count_time_wrapper(
    inner: Plan,
    time: f64,
    detectors: Vec<Arc<dyn crate::core::msg::ConfigurableObj>>,
) -> Plan {
    plan_box(async_stream::stream! {
        for d in &detectors {
            let mut values = HashMap::new();
            values.insert("count_time".to_string(), Value::from(time));
            yield Msg::Configure {
                obj: d.clone(),
                args: crate::core::msg::ConfigureArgs { values },
            };
        }
        let mut inner = inner;
        use futures::StreamExt;
        while let Some(item) = inner.next().await {
            let crate::core::plan::PlanItem::Bare(m) = item;
            yield m;
        }
    })
}

/// `lazily_stage_wrapper(plan, devices)` — stage each device on its
/// **first** `Read` / `Set` / `Trigger` / `Configure` reference instead
/// of upfront. Devices that the inner plan never touches are not staged
/// (and not unstaged). At the end of the inner plan, unstage everything
/// that was lazily staged, in LIFO order.
///
/// Mirrors `bluesky.preprocessors.lazily_stage_wrapper`. Useful for
/// generic plans where the device list is large but the actual touch
/// set per run is sparse.
pub fn lazily_stage_wrapper(inner: Plan, devices: Vec<Arc<dyn StageableObj>>) -> Plan {
    use std::collections::HashSet;
    plan_box(async_stream::stream! {
        let by_name: HashMap<String, Arc<dyn StageableObj>> =
            devices.into_iter().map(|d| (d.name().to_string(), d)).collect();
        let mut staged: Vec<Arc<dyn StageableObj>> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        let mut inner = inner;
        while let Some(item) = inner.next().await {
            let PlanItem::Bare(m) = item;
            let touched: Option<&str> = match &m {
                Msg::Read(o)            => Some(o.name()),
                Msg::Set { obj, .. }    => Some(obj.name()),
                Msg::Trigger { obj, .. } => Some(obj.name()),
                Msg::Configure { obj, .. } => Some(obj.name()),
                _ => None,
            };
            if let Some(name) = touched {
                if seen.insert(name.to_string()) {
                    if let Some(d) = by_name.get(name) {
                        yield Msg::Stage(d.clone());
                        staged.push(d.clone());
                    }
                }
            }
            yield m;
        }
        for d in staged.into_iter().rev() {
            yield Msg::Unstage(d);
        }
    })
}

/// `set_run_key_wrapper(plan, run_key)` — for every `OpenRun` the inner
/// plan emits, inject `run_key` into `metadata.extra["run_key"]`. Useful
/// for multi-run plans (e.g. `pchain` of two scans) where downstream
/// consumers want to disambiguate the runs after the fact.
///
/// Mirrors `bluesky.preprocessors.set_run_key_wrapper`. If the inner
/// plan already set `run_key`, the existing value is preserved
/// (consistent with `inject_md_wrapper`'s `entry().or_insert()` shape).
pub fn set_run_key_wrapper(inner: Plan, run_key: impl Into<String>) -> Plan {
    let key = run_key.into();
    msg_mutator(inner, move |m| match m {
        Msg::OpenRun(mut meta) => {
            meta.extra
                .entry("run_key".to_string())
                .or_insert_with(|| Value::from(key.clone()));
            Msg::OpenRun(meta)
        }
        other => other,
    })
}

/// `stub_wrapper(plan)` — assert that the inner plan does **not** open
/// any runs. If an `OpenRun` (or `CloseRun`) slips through, abort the
/// plan with `Msg::Fail` carrying a diagnostic. Useful for composing
/// "stub" plans that should be embeddable inside an outer
/// `run_wrapper` without nesting runs.
///
/// Mirrors `bluesky.preprocessors.stub_wrapper`. Bluesky raises an
/// `IllegalMessageSequence` exception; bsrs surfaces the same
/// constraint via the engine's `Msg::Fail` handler, which aborts the
/// run with the supplied reason.
pub fn stub_wrapper(inner: Plan) -> Plan {
    plan_box(async_stream::stream! {
        let mut inner = inner;
        while let Some(item) = inner.next().await {
            let PlanItem::Bare(m) = item;
            match &m {
                Msg::OpenRun(_) => {
                    yield Msg::Fail(
                        "stub_wrapper: inner plan must not emit OpenRun".into(),
                    );
                    return;
                }
                Msg::CloseRun { .. } => {
                    yield Msg::Fail(
                        "stub_wrapper: inner plan must not emit CloseRun".into(),
                    );
                    return;
                }
                _ => {}
            }
            yield m;
        }
    })
}

/// A flyable device paired with its collectable, as consumed by
/// [`fly_during_wrapper`] and stored in [`SupplementalData`]. bsrs splits the
/// Flyable and Collectable roles across two traits, so the pair is explicit.
pub type FlyerPair = (Arc<dyn FlyableObj>, Arc<dyn CollectableObj>);

/// A port of bluesky's `SupplementalData` (`bluesky/preprocessors.py:1274`) —
/// a mutable set of `baseline` devices, `monitors`, and `flyers`. As a plan
/// preprocessor it inserts baseline readings around each run, monitors during
/// it, and flies devices across it, composing [`baseline_wrapper`],
/// [`monitor_during_wrapper`], and [`fly_during_wrapper`]. Register it on the
/// RunEngine:
///
/// ```ignore
/// let sd = std::sync::Arc::new(SupplementalData::new());
/// sd.add_baseline(some_detector);
/// re.add_preprocessor(sd.as_preprocessor());
/// ```
///
/// The three lists are interior-mutable, so edits made after registration take
/// effect on the next plan — the preprocessor re-reads them on every call —
/// mirroring bluesky's interactive `sd.baseline.append(...)`.
#[derive(Default)]
pub struct SupplementalData {
    baseline: Mutex<Vec<Arc<dyn ReadableObj>>>,
    monitors: Mutex<Vec<Arc<dyn MonitorableObj>>>,
    flyers: Mutex<Vec<FlyerPair>>,
}

impl SupplementalData {
    /// An empty set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a device to be read into the `baseline` stream at the start and end
    /// of each run.
    pub fn add_baseline(&self, device: Arc<dyn ReadableObj>) {
        self.baseline.lock().unwrap().push(device);
    }

    /// Add a signal to be monitored (asynchronously) during each run.
    pub fn add_monitor(&self, signal: Arc<dyn MonitorableObj>) {
        self.monitors.lock().unwrap().push(signal);
    }

    /// Add a `(flyer, collectable)` pair to be kicked off at the start of each
    /// run and completed/collected at the end.
    pub fn add_flyer(&self, flyer: Arc<dyn FlyableObj>, collectable: Arc<dyn CollectableObj>) {
        self.flyers.lock().unwrap().push((flyer, collectable));
    }

    /// Remove all baseline devices, monitors, and flyers.
    pub fn clear(&self) {
        self.baseline.lock().unwrap().clear();
        self.monitors.lock().unwrap().clear();
        self.flyers.lock().unwrap().clear();
    }

    /// Apply the supplemental wrappers to `plan`. Composed inside-out
    /// (fly → monitor → baseline), matching bluesky `SupplementalData.__call__`
    /// so the run order is: baseline read, start monitors, kick off flyers,
    /// `plan`, complete/collect flyers, stop monitors, baseline read. Each
    /// wrapper is skipped when its list is empty (bluesky's `if not devices`
    /// no-op), so an all-empty `SupplementalData` is a pure passthrough.
    pub fn apply(&self, plan: Plan) -> Plan {
        let flyers = self.flyers.lock().unwrap().clone();
        let monitors = self.monitors.lock().unwrap().clone();
        let baseline = self.baseline.lock().unwrap().clone();

        let plan = if flyers.is_empty() {
            plan
        } else {
            fly_during_wrapper(plan, flyers)
        };
        let plan = if monitors.is_empty() {
            plan
        } else {
            monitor_during_wrapper(plan, monitors)
        };
        if baseline.is_empty() {
            plan
        } else {
            baseline_wrapper(plan, baseline, "baseline")
        }
    }

    /// A RunEngine preprocessor closure (matching
    /// [`crate::engine::Preprocessor`]) that applies this `SupplementalData` to
    /// every plan, re-reading the current baseline/monitors/flyers on each
    /// call.
    pub fn as_preprocessor(self: &Arc<Self>) -> Arc<dyn Fn(Plan) -> Plan + Send + Sync + 'static> {
        let this = self.clone();
        Arc::new(move |plan| this.apply(plan))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::plan::plan_box;

    #[tokio::test]
    async fn run_wrapper_brackets_plan() {
        let body = plan_box(async_stream::stream! {
            yield Msg::Null;
            yield Msg::Sleep(std::time::Duration::from_millis(0));
        });
        let wrapped = run_wrapper(body, RunMetadata::default());
        let msgs = drain(wrapped).await;
        assert_eq!(msgs.len(), 4);
        assert!(matches!(msgs[0], Msg::OpenRun(_)));
        assert!(matches!(msgs.last(), Some(Msg::CloseRun { .. })));
    }

    #[tokio::test]
    async fn msg_mutator_replaces_each() {
        let body = plan_box(async_stream::stream! {
            yield Msg::Null;
            yield Msg::Null;
        });
        let wrapped = msg_mutator(body, |_m| Msg::Sleep(std::time::Duration::from_secs(0)));
        let msgs = drain(wrapped).await;
        assert!(msgs.iter().all(|m| matches!(m, Msg::Sleep(_))));
        assert_eq!(msgs.len(), 2);
    }

    #[tokio::test]
    async fn pchain_concatenates() {
        let p1 = plan_box(async_stream::stream! { yield Msg::Null; });
        let p2 = plan_box(async_stream::stream! { yield Msg::Null; yield Msg::Null; });
        let chained = pchain(vec![p1, p2]);
        assert_eq!(drain(chained).await.len(), 3);
    }

    use crate::core::error::BsrsError;

    struct FakeStage(String);
    impl crate::core::msg::NamedObj for FakeStage {
        fn name(&self) -> &str {
            &self.0
        }
    }
    #[async_trait::async_trait]
    impl crate::core::msg::StageableObj for FakeStage {
        async fn stage_dyn(&self) -> Result<(), BsrsError> {
            Ok(())
        }
        async fn unstage_dyn(&self) -> Result<(), BsrsError> {
            Ok(())
        }
    }
    #[async_trait::async_trait]
    impl crate::core::msg::ReadableObj for FakeStage {
        async fn read_dyn(
            &self,
        ) -> Result<HashMap<String, crate::core::reading::ReadingValue>, BsrsError> {
            Ok(HashMap::new())
        }
        async fn describe_dyn(
            &self,
        ) -> Result<HashMap<String, crate::event_model::DataKey>, BsrsError> {
            Ok(HashMap::new())
        }
    }

    #[tokio::test]
    async fn lazily_stage_wrapper_stages_only_touched_devices() {
        let a: Arc<FakeStage> = Arc::new(FakeStage("a".into()));
        let b: Arc<FakeStage> = Arc::new(FakeStage("b".into()));
        let a_read: Arc<dyn ReadableObj> = a.clone();
        let body = plan_box(async_stream::stream! {
            yield Msg::Read(a_read);
            yield Msg::Null;
        });
        let stageables: Vec<Arc<dyn StageableObj>> = vec![a.clone(), b.clone()];
        let wrapped = lazily_stage_wrapper(body, stageables);
        let msgs = drain(wrapped).await;
        // Stage(a), Read(a), Null, Unstage(a) — b was never touched
        assert_eq!(msgs.len(), 4);
        assert!(matches!(&msgs[0], Msg::Stage(d) if d.name() == "a"));
        assert!(matches!(&msgs[1], Msg::Read(_)));
        assert!(matches!(&msgs[2], Msg::Null));
        assert!(matches!(&msgs[3], Msg::Unstage(d) if d.name() == "a"));
    }

    #[tokio::test]
    async fn set_run_key_wrapper_injects_run_key() {
        let body = plan_box(async_stream::stream! {
            yield Msg::OpenRun(RunMetadata::default());
            yield Msg::CloseRun { exit_status: "success".into(), reason: None };
        });
        let wrapped = set_run_key_wrapper(body, "scan_42");
        let msgs = drain(wrapped).await;
        match &msgs[0] {
            Msg::OpenRun(md) => {
                assert_eq!(md.extra.get("run_key"), Some(&Value::from("scan_42")));
            }
            _ => panic!("expected OpenRun"),
        }
    }

    #[tokio::test]
    async fn set_run_key_wrapper_preserves_existing() {
        let mut md = RunMetadata::default();
        md.extra
            .insert("run_key".to_string(), Value::from("preset"));
        let body = plan_box(async_stream::stream! {
            yield Msg::OpenRun(md);
        });
        let wrapped = set_run_key_wrapper(body, "should_not_overwrite");
        let msgs = drain(wrapped).await;
        match &msgs[0] {
            Msg::OpenRun(md) => {
                assert_eq!(md.extra.get("run_key"), Some(&Value::from("preset")));
            }
            _ => panic!("expected OpenRun"),
        }
    }

    #[tokio::test]
    async fn stub_wrapper_passes_through_run_free_plan() {
        let body = plan_box(async_stream::stream! {
            yield Msg::Null;
            yield Msg::Sleep(std::time::Duration::from_millis(0));
        });
        let msgs = drain(stub_wrapper(body)).await;
        assert_eq!(msgs.len(), 2);
        assert!(!msgs.iter().any(|m| matches!(m, Msg::Fail(_))));
    }

    #[tokio::test]
    async fn stub_wrapper_fails_on_open_run() {
        let body = plan_box(async_stream::stream! {
            yield Msg::Null;
            yield Msg::OpenRun(RunMetadata::default());
            yield Msg::Null; // never reached
        });
        let msgs = drain(stub_wrapper(body)).await;
        assert_eq!(msgs.len(), 2);
        assert!(matches!(&msgs[0], Msg::Null));
        assert!(matches!(&msgs[1], Msg::Fail(s) if s.contains("OpenRun")));
    }

    #[tokio::test]
    async fn stub_wrapper_fails_on_close_run() {
        let body = plan_box(async_stream::stream! {
            yield Msg::CloseRun { exit_status: "success".into(), reason: None };
        });
        let msgs = drain(stub_wrapper(body)).await;
        assert_eq!(msgs.len(), 1);
        assert!(matches!(&msgs[0], Msg::Fail(s) if s.contains("CloseRun")));
    }

    struct FakeFlyer(String);
    impl crate::core::msg::NamedObj for FakeFlyer {
        fn name(&self) -> &str {
            &self.0
        }
    }
    #[async_trait::async_trait]
    impl FlyableObj for FakeFlyer {
        async fn kickoff_dyn(&self) -> crate::core::status::Status {
            crate::core::status::Status::done()
        }
        async fn complete_dyn(&self) -> crate::core::status::Status {
            crate::core::status::Status::done()
        }
    }
    #[async_trait::async_trait]
    impl CollectableObj for FakeFlyer {
        async fn describe_collect_dyn(
            &self,
        ) -> Result<HashMap<String, HashMap<String, crate::event_model::DataKey>>, BsrsError>
        {
            Ok(HashMap::new())
        }
        async fn collect_dyn(
            &self,
        ) -> Result<Vec<(String, HashMap<String, Value>, HashMap<String, f64>)>, BsrsError>
        {
            Ok(Vec::new())
        }
    }

    fn fake_flyer(name: &str) -> (Arc<dyn FlyableObj>, Arc<dyn CollectableObj>) {
        let f = Arc::new(FakeFlyer(name.into()));
        (
            f.clone() as Arc<dyn FlyableObj>,
            f as Arc<dyn CollectableObj>,
        )
    }

    fn run_body() -> Plan {
        plan_box(async_stream::stream! {
            yield Msg::OpenRun(RunMetadata::default());
            yield Msg::Null;
            yield Msg::CloseRun { exit_status: "success".into(), reason: None };
        })
    }

    #[tokio::test]
    async fn fly_during_wrapper_skips_wait_when_no_flyers() {
        // No flyers -> no kickoff/complete/wait/collect inserted; the run's own
        // messages pass through unchanged.
        let msgs = drain(fly_during_wrapper(run_body(), vec![])).await;
        assert_eq!(msgs.len(), 3, "got {msgs:?}");
        assert!(matches!(&msgs[0], Msg::OpenRun(_)));
        assert!(matches!(&msgs[1], Msg::Null));
        assert!(matches!(&msgs[2], Msg::CloseRun { .. }));
        assert!(
            !msgs.iter().any(|m| matches!(m, Msg::Wait { .. })),
            "no Wait may be emitted when there are no flyers"
        );
    }

    #[tokio::test]
    async fn fly_during_wrapper_inserts_kickoff_and_collect_inside_run() {
        let msgs = drain(fly_during_wrapper(run_body(), vec![fake_flyer("f")])).await;
        // OpenRun, Kickoff, Wait{fly_kick}, Null, Complete, Wait{fly_done},
        // Collect, CloseRun — kickoff/collect land INSIDE the run envelope.
        assert_eq!(msgs.len(), 8, "got {msgs:?}");
        assert!(matches!(&msgs[0], Msg::OpenRun(_)));
        assert!(matches!(&msgs[1], Msg::Kickoff { .. }));
        assert!(matches!(&msgs[2], Msg::Wait { group, .. } if group == "fly_kick"));
        assert!(matches!(&msgs[3], Msg::Null));
        assert!(matches!(&msgs[4], Msg::Complete { .. }));
        assert!(matches!(&msgs[5], Msg::Wait { group, .. } if group == "fly_done"));
        assert!(matches!(&msgs[6], Msg::Collect { .. }));
        assert!(matches!(&msgs[7], Msg::CloseRun { .. }));
    }

    #[tokio::test]
    async fn during_run_wrappers_insert_nothing_without_a_run() {
        // The "during runs" contract: a run-free inner plan gets no insertion,
        // matching bluesky's plan_mutator-on-open_run structure.
        let body = plan_box(async_stream::stream! { yield Msg::Null; });
        let msgs = drain(fly_during_wrapper(body, vec![fake_flyer("f")])).await;
        assert_eq!(msgs.len(), 1, "got {msgs:?}");
        assert!(matches!(&msgs[0], Msg::Null));
    }

    struct FakeMonitorable(String);
    impl crate::core::msg::NamedObj for FakeMonitorable {
        fn name(&self) -> &str {
            &self.0
        }
    }
    #[async_trait::async_trait]
    impl ReadableObj for FakeMonitorable {
        async fn read_dyn(
            &self,
        ) -> Result<HashMap<String, crate::core::reading::ReadingValue>, BsrsError> {
            Ok(HashMap::new())
        }
        async fn describe_dyn(
            &self,
        ) -> Result<HashMap<String, crate::event_model::DataKey>, BsrsError> {
            Ok(HashMap::new())
        }
    }
    #[async_trait::async_trait]
    impl MonitorableObj for FakeMonitorable {
        async fn subscribe_dyn(
            &self,
        ) -> Result<crate::core::subscription::Subscription, BsrsError> {
            // Never called: monitor_during_wrapper only builds Monitor/Unmonitor
            // messages; the engine (not the wrapper) performs the subscribe.
            Err(BsrsError::Plan(
                "subscribe_dyn not used in wrapper test".into(),
            ))
        }
    }

    #[tokio::test]
    async fn monitor_during_wrapper_brackets_inside_run() {
        let sig: Arc<dyn MonitorableObj> = Arc::new(FakeMonitorable("s".into()));
        let msgs = drain(monitor_during_wrapper(run_body(), vec![sig])).await;
        // OpenRun, Monitor{name="s_monitor"}, Null, Unmonitor, CloseRun.
        assert_eq!(msgs.len(), 5, "got {msgs:?}");
        assert!(matches!(&msgs[0], Msg::OpenRun(_)));
        assert!(
            matches!(&msgs[1], Msg::Monitor { name, .. } if name.as_deref() == Some("s_monitor")),
            "monitor stream must be named {{signal}}_monitor; got {:?}",
            &msgs[1]
        );
        assert!(matches!(&msgs[2], Msg::Null));
        assert!(matches!(&msgs[3], Msg::Unmonitor(_)));
        assert!(matches!(&msgs[4], Msg::CloseRun { .. }));
    }

    #[tokio::test]
    async fn baseline_wrapper_reads_inside_run() {
        let dev: Arc<dyn ReadableObj> = Arc::new(FakeStage("d".into()));
        let msgs = drain(baseline_wrapper(run_body(), vec![dev], "baseline")).await;
        // OpenRun, Create, Read, Save, Null, Create, Read, Save, CloseRun.
        assert_eq!(msgs.len(), 9, "got {msgs:?}");
        assert!(matches!(&msgs[0], Msg::OpenRun(_)));
        assert!(matches!(&msgs[1], Msg::Create { stream_name } if stream_name == "baseline"));
        assert!(matches!(&msgs[2], Msg::Read(_)));
        assert!(matches!(&msgs[3], Msg::Save));
        assert!(matches!(&msgs[4], Msg::Null));
        assert!(matches!(&msgs[5], Msg::Create { stream_name } if stream_name == "baseline"));
        assert!(matches!(&msgs[6], Msg::Read(_)));
        assert!(matches!(&msgs[7], Msg::Save));
        assert!(matches!(&msgs[8], Msg::CloseRun { .. }));
    }

    #[tokio::test]
    async fn supplemental_data_empty_is_passthrough() {
        let sd = Arc::new(SupplementalData::new());
        let msgs = drain(sd.apply(run_body())).await;
        // No baseline/monitor/fly insertion: just OpenRun, Null, CloseRun.
        assert_eq!(msgs.len(), 3, "got {msgs:?}");
        assert!(matches!(&msgs[0], Msg::OpenRun(_)));
        assert!(matches!(&msgs[1], Msg::Null));
        assert!(matches!(&msgs[2], Msg::CloseRun { .. }));
    }

    #[tokio::test]
    async fn supplemental_data_inserts_baseline_around_run() {
        let sd = Arc::new(SupplementalData::new());
        sd.add_baseline(Arc::new(FakeStage("d".into())) as Arc<dyn ReadableObj>);
        let msgs = drain(sd.apply(run_body())).await;
        // OpenRun, Create(baseline), Read, Save, Null, Create(baseline), Read, Save, CloseRun.
        assert_eq!(msgs.len(), 9, "got {msgs:?}");
        assert!(matches!(&msgs[1], Msg::Create { stream_name } if stream_name == "baseline"));
        assert!(matches!(&msgs[2], Msg::Read(_)));
        assert!(matches!(&msgs[5], Msg::Create { stream_name } if stream_name == "baseline"));
    }

    #[tokio::test]
    async fn supplemental_data_as_preprocessor_applies_current_lists() {
        let sd = Arc::new(SupplementalData::new());
        // Mutate after construction — the preprocessor must read this on call.
        sd.add_baseline(Arc::new(FakeStage("d".into())) as Arc<dyn ReadableObj>);
        let pp = sd.as_preprocessor();
        let msgs = drain(pp(run_body())).await;
        assert!(
            msgs.iter()
                .any(|m| matches!(m, Msg::Create { stream_name } if stream_name == "baseline")),
            "preprocessor did not insert the baseline stream; got {msgs:?}"
        );
    }
}
