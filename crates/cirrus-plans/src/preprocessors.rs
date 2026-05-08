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

/// `monitor_during_wrapper(plan, signals)` — `Monitor` each signal at the
/// start of the run, `Unmonitor` at the end. Both happen *inside* the run
/// envelope (after OpenRun, before CloseRun). For now we approximate by
/// prepending Monitors and appending Unmonitors — caller must ensure the
/// inner plan opens/closes its own run if desired.
pub fn monitor_during_wrapper(inner: Plan, signals: Vec<Arc<dyn MonitorableObj>>) -> Plan {
    plan_box(async_stream::stream! {
        for s in &signals {
            yield Msg::Monitor { obj: s.clone(), name: None };
        }
        let mut inner = inner;
        while let Some(item) = inner.next().await {
            if let PlanItem::Bare(m) = item {
                yield m;
            }
        }
        for s in signals.into_iter().rev() {
            yield Msg::Unmonitor(s);
        }
    })
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

/// `baseline_wrapper(plan, devices, name)` — reads each device once into
/// the named stream (default `"baseline"`) before and after the inner
/// plan. Implemented as a pre/post `Create + Read* + Save` block.
pub fn baseline_wrapper(
    inner: Plan,
    devices: Vec<Arc<dyn ReadableObj>>,
    name: impl Into<String>,
) -> Plan {
    let stream = name.into();
    plan_box(async_stream::stream! {
        // Pre-baseline.
        yield Msg::Create { stream_name: stream.clone() };
        for d in &devices {
            yield Msg::Read(d.clone());
        }
        yield Msg::Save;
        let mut inner = inner;
        while let Some(item) = inner.next().await {
            if let PlanItem::Bare(m) = item {
                yield m;
            }
        }
        // Post-baseline.
        yield Msg::Create { stream_name: stream };
        for d in &devices {
            yield Msg::Read(d.clone());
        }
        yield Msg::Save;
    })
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

/// `fly_during_wrapper(plan, flyers)` — for the duration of `plan`,
/// `Kickoff` each `(flyer, collectable)` pair at the start and
/// `Complete + Collect` at the end. Mirrors bluesky's
/// `fly_during_wrapper`. Pairs the Flyable + Collectable explicitly
/// since cirrus's protocol traits split those roles.
pub fn fly_during_wrapper(
    inner: Plan,
    flyers: Vec<(Arc<dyn FlyableObj>, Arc<dyn CollectableObj>)>,
) -> Plan {
    plan_box(async_stream::stream! {
        for (f, _) in &flyers {
            yield Msg::Kickoff { obj: f.clone(), group: Some("fly_kick".into()) };
        }
        yield Msg::Wait { group: "fly_kick".into(), error_on_timeout: true, timeout: None };
        let mut inner = inner;
        while let Some(item) = inner.next().await {
            if let PlanItem::Bare(m) = item {
                yield m;
            }
        }
        for (f, _) in &flyers {
            yield Msg::Complete { obj: f.clone(), group: Some("fly_done".into()) };
        }
        yield Msg::Wait { group: "fly_done".into(), error_on_timeout: true, timeout: None };
        for (_, c) in flyers {
            yield Msg::Collect { obj: c, stream_name: None };
        }
    })
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
}
