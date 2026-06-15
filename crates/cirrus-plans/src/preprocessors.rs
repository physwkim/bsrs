//! `bluesky.preprocessors` equivalents — wrappers that transform a `Plan`.
//!
//! These take a `Plan` (a stream of `Msg`) and return a new `Plan` whose
//! emitted messages are mutated, prepended, appended, or interleaved.

use cirrus_core::msg::{
    CollectableObj, FlyableObj, LocatableObj, MonitorableObj, Msg, ReadableObj, RunMetadata,
    StageableObj,
};
use cirrus_core::plan::{plan_box, Plan, PlanItem};
use futures::StreamExt;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

/// Drain a `Plan` stream into a Vec of messages. Useful when a wrapper
/// needs random access (e.g. `relative_set_wrapper` rewrites Set values).
#[allow(dead_code)]
async fn drain(mut plan: Plan) -> Vec<Msg> {
    let mut out = Vec::new();
    while let Some(item) = plan.next().await {
        if let PlanItem::Bare(m) = item {
            out.push(m);
        }
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
            let m = match item { PlanItem::Bare(m) => m, _ => continue };
            if let Some(repl) = f(&m) {
                let mut r = repl;
                while let Some(it) = r.next().await {
                    if let PlanItem::Bare(rm) = it {
                        yield rm;
                    }
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
            if let PlanItem::Bare(m) = item {
                yield f(m);
            }
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
                if let PlanItem::Bare(m) = item {
                    yield m;
                }
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
            if let PlanItem::Bare(m) = item {
                yield m;
            }
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
            if let PlanItem::Bare(m) = item {
                yield m;
            }
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
            if let PlanItem::Bare(m) = item {
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
        }
    })
}

/// `monitor_during_wrapper(plan, signals)` — `Monitor` each signal immediately
/// after the run opens and `Unmonitor` immediately before it closes, so the
/// monitor streams live *inside* the run envelope. Mirrors bluesky's
/// `monitor_during_wrapper` (preprocessors.py:813), which inserts via
/// `plan_mutator` after `open_run` / before `close_run`. A run-free inner plan
/// gets no insertion.
pub fn monitor_during_wrapper(inner: Plan, signals: Vec<Arc<dyn MonitorableObj>>) -> Plan {
    let unmon = signals.clone();
    during_run_wrapper(
        inner,
        move || {
            signals
                .iter()
                .map(|s| Msg::Monitor {
                    obj: s.clone(),
                    name: None,
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
            if let PlanItem::Bare(m) = item {
                yield m;
            }
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
            if let PlanItem::Bare(m) = item {
                yield m;
            }
        }
        let mut fin = final_plan;
        while let Some(item) = fin.next().await {
            if let PlanItem::Bare(m) = item {
                yield m;
            }
        }
    })
}

/// `subs_wrapper(plan, subs)` — prepend a setup sequence that registers
/// document callbacks at the engine level. cirrus's engine handles
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
/// rewrite every `Msg::Set { obj == motor, value }` into `value + readback`,
/// where `readback` is captured *once* at wrapper start via `locate_dyn`.
/// After the inner plan, no automatic restore; pair with
/// `reset_positions_wrapper` for that.
pub fn relative_set_wrapper(inner: Plan, motors: Vec<Arc<dyn LocatableObj>>) -> Plan {
    plan_box(async_stream::stream! {
        // Snapshot starting positions.
        let mut starts: HashMap<String, f64> = HashMap::new();
        for m in &motors {
            if let Ok(loc) = m.locate_dyn().await {
                starts.insert(m.name().to_string(), loc.readback);
            }
        }
        let names: std::collections::HashSet<String> =
            motors.iter().map(|m| m.name().to_string()).collect();
        let mut inner = inner;
        while let Some(item) = inner.next().await {
            if let PlanItem::Bare(m) = item {
                let m = match m {
                    Msg::Set { obj, value, group } if names.contains(obj.name()) => {
                        let bias = starts.get(obj.name()).copied().unwrap_or(0.0);
                        Msg::Set { obj, value: value + bias, group }
                    }
                    other => other,
                };
                yield m;
            }
        }
    })
}

/// `print_summary_wrapper(plan)` — debug-print every Msg as it flows
/// through. The Msg is printed via its `Debug` impl to stderr just
/// before being yielded to the engine. Mirrors bluesky's
/// `print_summary_wrapper` in spirit (cirrus emits each line eagerly
/// rather than first collecting the full plan).
pub fn print_summary_wrapper(inner: Plan) -> Plan {
    plan_box(async_stream::stream! {
        let mut inner = inner;
        let mut idx = 0usize;
        while let Some(item) = inner.next().await {
            if let PlanItem::Bare(m) = item {
                eprintln!("[plan {idx:04}] {m:?}");
                idx += 1;
                yield m;
            }
        }
    })
}

/// `suspend_wrapper(plan, suspender)` — install `suspender` for the
/// duration of `plan`, remove on exit. Mirrors bluesky's
/// `suspend_wrapper`.
pub fn suspend_wrapper(inner: Plan, suspender: Arc<dyn cirrus_core::Suspender>) -> Plan {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(1);
    let id = SEQ.fetch_add(1, Ordering::Relaxed);
    plan_box(async_stream::stream! {
        let any: Arc<dyn std::any::Any + Send + Sync> = Arc::new(suspender.clone());
        yield Msg::InstallSuspender { id, suspender: any };
        let mut inner = inner;
        while let Some(item) = inner.next().await {
            if let PlanItem::Bare(m) = item {
                yield m;
            }
        }
        yield Msg::RemoveSuspender { id };
    })
}

/// `fly_during_wrapper(plan, flyers)` — `Kickoff` each `(flyer, collectable)`
/// pair immediately after the run opens and `Complete + Collect` immediately
/// before it closes, so the fly kickoff/collect live *inside* the run envelope.
/// Mirrors bluesky's `fly_during_wrapper` (preprocessors.py:866), which inserts
/// via `plan_mutator` after `open_run` / before `close_run`. Pairs the Flyable +
/// Collectable explicitly since cirrus's protocol traits split those roles.
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

/// `contingency_wrapper(plan, finally)` — run `plan`; whether it
/// finishes normally or aborts, then run `finally`. Bluesky's full
/// contingency_wrapper supports try/except/else branches with
/// exception-class filtering; cirrus's stream model doesn't surface
/// plan-level exceptions, so this is the conservative finalize-style
/// shape (always run `finally`). For now, identical behaviour to
/// `finalize_wrapper`; kept as a separate name so callers expressing
/// intent ("run cleanup if anything goes wrong") see a matching
/// API name.
pub fn contingency_wrapper(inner: Plan, finally: Plan) -> Plan {
    finalize_wrapper(inner, finally)
}

/// `reset_positions_wrapper(plan, motors)` — snapshot motor positions at
/// start, run the inner plan, then issue `mv` back to the snapshot for
/// each motor.
pub fn reset_positions_wrapper(inner: Plan, motors: Vec<Arc<dyn LocatableObj>>) -> Plan {
    plan_box(async_stream::stream! {
        let mut snapshot: Vec<(Arc<dyn cirrus_core::msg::MovableObj>, f64)> = Vec::new();
        for m in &motors {
            if let Ok(loc) = m.locate_dyn().await {
                snapshot.push((m.clone() as Arc<dyn cirrus_core::msg::MovableObj>, loc.readback));
            }
        }
        let mut inner = inner;
        while let Some(item) = inner.next().await {
            if let PlanItem::Bare(m) = item {
                yield m;
            }
        }
        for (mv_obj, val) in snapshot {
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
    detectors: Vec<Arc<dyn cirrus_core::msg::ConfigurableObj>>,
) -> Plan {
    plan_box(async_stream::stream! {
        for d in &detectors {
            let mut values = HashMap::new();
            values.insert("count_time".to_string(), Value::from(time));
            yield Msg::Configure {
                obj: d.clone(),
                args: cirrus_core::msg::ConfigureArgs { values },
            };
        }
        let mut inner = inner;
        use futures::StreamExt;
        while let Some(item) = inner.next().await {
            if let cirrus_core::plan::PlanItem::Bare(m) = item {
                yield m;
            }
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
            if let PlanItem::Bare(m) = item {
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
/// `IllegalMessageSequence` exception; cirrus surfaces the same
/// constraint via the engine's `Msg::Fail` handler, which aborts the
/// run with the supplied reason.
pub fn stub_wrapper(inner: Plan) -> Plan {
    plan_box(async_stream::stream! {
        let mut inner = inner;
        while let Some(item) = inner.next().await {
            if let PlanItem::Bare(m) = item {
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
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use cirrus_core::plan::plan_box;

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

    use cirrus_core::error::CirrusError;

    struct FakeStage(String);
    impl cirrus_core::msg::NamedObj for FakeStage {
        fn name(&self) -> &str {
            &self.0
        }
    }
    #[async_trait::async_trait]
    impl cirrus_core::msg::StageableObj for FakeStage {
        async fn stage_dyn(&self) -> Result<(), CirrusError> {
            Ok(())
        }
        async fn unstage_dyn(&self) -> Result<(), CirrusError> {
            Ok(())
        }
    }
    #[async_trait::async_trait]
    impl cirrus_core::msg::ReadableObj for FakeStage {
        async fn read_dyn(
            &self,
        ) -> Result<HashMap<String, cirrus_core::reading::ReadingValue>, CirrusError> {
            Ok(HashMap::new())
        }
        async fn describe_dyn(
            &self,
        ) -> Result<HashMap<String, cirrus_event_model::DataKey>, CirrusError> {
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
    impl cirrus_core::msg::NamedObj for FakeFlyer {
        fn name(&self) -> &str {
            &self.0
        }
    }
    #[async_trait::async_trait]
    impl FlyableObj for FakeFlyer {
        async fn kickoff_dyn(&self) -> cirrus_core::status::Status {
            cirrus_core::status::Status::done()
        }
        async fn complete_dyn(&self) -> cirrus_core::status::Status {
            cirrus_core::status::Status::done()
        }
    }
    #[async_trait::async_trait]
    impl CollectableObj for FakeFlyer {
        async fn describe_collect_dyn(
            &self,
        ) -> Result<HashMap<String, HashMap<String, cirrus_event_model::DataKey>>, CirrusError>
        {
            Ok(HashMap::new())
        }
        async fn collect_dyn(
            &self,
        ) -> Result<Vec<(String, HashMap<String, Value>, HashMap<String, f64>)>, CirrusError>
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
    impl cirrus_core::msg::NamedObj for FakeMonitorable {
        fn name(&self) -> &str {
            &self.0
        }
    }
    #[async_trait::async_trait]
    impl ReadableObj for FakeMonitorable {
        async fn read_dyn(
            &self,
        ) -> Result<HashMap<String, cirrus_core::reading::ReadingValue>, CirrusError> {
            Ok(HashMap::new())
        }
        async fn describe_dyn(
            &self,
        ) -> Result<HashMap<String, cirrus_event_model::DataKey>, CirrusError> {
            Ok(HashMap::new())
        }
    }
    #[async_trait::async_trait]
    impl MonitorableObj for FakeMonitorable {
        async fn subscribe_dyn(
            &self,
        ) -> Result<cirrus_core::subscription::Subscription, CirrusError> {
            // Never called: monitor_during_wrapper only builds Monitor/Unmonitor
            // messages; the engine (not the wrapper) performs the subscribe.
            Err(CirrusError::Plan(
                "subscribe_dyn not used in wrapper test".into(),
            ))
        }
    }

    #[tokio::test]
    async fn monitor_during_wrapper_brackets_inside_run() {
        let sig: Arc<dyn MonitorableObj> = Arc::new(FakeMonitorable("s".into()));
        let msgs = drain(monitor_during_wrapper(run_body(), vec![sig])).await;
        // OpenRun, Monitor, Null, Unmonitor, CloseRun.
        assert_eq!(msgs.len(), 5, "got {msgs:?}");
        assert!(matches!(&msgs[0], Msg::OpenRun(_)));
        assert!(matches!(&msgs[1], Msg::Monitor { .. }));
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
}
