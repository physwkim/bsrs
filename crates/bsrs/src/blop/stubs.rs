//! Plan stubs of the optimization loop — bsrs's port of `blop.plan_stubs`.
//!
//! A stub that produces a value (blop's generator `return`) hands it back
//! through a [`PlanWithResult`] channel: the caller forwards the plan and
//! awaits the receiver once the plan has ended.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use serde_json::Value;
use tokio::sync::oneshot;

use crate::core::error::{BsrsError, Result};
use crate::core::msg::{Msg, MsgResult, NamedObj, ReadableObj};
use crate::core::plan::{plan_box, plan_items, respond, Plan, PlanItem};
use crate::core::reading::ReadingValue;
use crate::event_model::DataKey;
use crate::plans::preprocessors::{msg_mutator, plan_mutator};
use crate::plans::{list_scan_nd, stubs as bps, ListScanAxis, PerStep};

use super::protocol::{
    validate_suggestions, AcquisitionUid, Actuator, Outcome, Params, PlanWithResult, Sensor,
    SharedOptimizer, Suggestion, TrialId,
};
use super::routing::route_suggestions;
use super::tracking::{track, tracked_readables, ReadableCache, Source};

/// Event field carrying the acquisition identifier of a step.
pub const ACQUISITION_UID_KEY: &str = "acquisition_uid";
/// Event field carrying the suggestion ids of a step, in sorted order.
pub const SUGGESTION_IDS_KEY: &str = "suggestion_ids";

/// A plan that yields nothing.
pub fn empty_plan() -> Plan {
    plan_box(futures::stream::empty())
}

/// A plan that fails at once with `reason`.
pub fn fail_plan(reason: impl Into<String>) -> Plan {
    let reason = reason.into();
    plan_box(async_stream::stream! {
        yield Msg::Fail(reason);
    })
}

/// Drop every `OpenRun` / `CloseRun` so a run-opening plan executes inside
/// an already-open run — blop's `_drop_run_control_messages`.
pub fn drop_run_control_messages(inner: Plan) -> Plan {
    plan_mutator(inner, |m| {
        matches!(m, Msg::OpenRun(_) | Msg::CloseRun { .. }).then(empty_plan)
    })
}

/// Route every run-scoped message of `inner` to the run named `run` —
/// bluesky's `set_run_key_wrapper`, which sets `msg.run` on each message
/// whose key is still `None`. bsrs carries the key as [`Msg::InRun`], and
/// only the bundler-touching verbs consult it, so those are wrapped and the
/// rest pass through untouched. A message already routed keeps its run, so
/// an acquisition run nested in an outer optimization run stays its own run.
pub fn in_run_wrapper(inner: Plan, run: impl Into<String>) -> Plan {
    let run = run.into();
    msg_mutator(inner, move |m| match m {
        Msg::OpenRun(_)
        | Msg::CloseRun { .. }
        | Msg::Create { .. }
        | Msg::Save
        | Msg::Drop
        | Msg::DeclareStream { .. }
        | Msg::Read(_)
        | Msg::Collect { .. }
        | Msg::Monitor { .. }
        | Msg::Unmonitor(_)
        | Msg::Configure { .. }
        | Msg::Kickoff { .. } => Msg::InRun {
            run: run.clone(),
            inner: Box::new(m),
        },
        other => other,
    })
}

/// Forward `inner` and produce the uid of the first run it opens. The uid
/// is what blop's `default_acquire` returns; bsrs plans open runs with a
/// bare `OpenRun`, so this turns that one message into a responding item.
pub fn capture_run_uid(inner: Plan) -> PlanWithResult<String> {
    let (tx, rx) = oneshot::channel();
    let plan = plan_items(async_stream::stream! {
        let mut uid_rx = None;
        let mut inner = inner;
        while let Some(item) = inner.next().await {
            match item {
                PlanItem::Bare(msg @ Msg::OpenRun(_)) if uid_rx.is_none() => {
                    let (item, rx) = respond(msg);
                    uid_rx = Some(rx);
                    yield item;
                }
                other => yield other,
            }
        }
        if let Some(rx) = uid_rx {
            if let Ok(MsgResult::OpenRun { uid }) = rx.await {
                let _ = tx.send(uid);
            }
        }
    });
    (plan, rx)
}

/// Read each readable's value — blop's `seq_read`, one `bps.rd` per device.
/// A device's value is its first hinted field, or its only field; a device
/// with several unhinted fields fails the plan.
pub fn seq_read(readables: Vec<Arc<dyn ReadableObj>>) -> PlanWithResult<Params> {
    let (tx, rx) = oneshot::channel();
    let plan = plan_items(async_stream::stream! {
        let mut out = Params::new();
        for r in &readables {
            let (item, reply) = respond(Msg::Read(r.clone()));
            yield item;
            let data = match reply.await {
                Ok(MsgResult::Reading { data }) => data,
                // The engine did not answer: the read failed and the run is
                // already ending.
                _ => return,
            };
            match single_value(r.as_ref(), &data) {
                Ok(v) => {
                    out.insert(r.name().to_owned(), v);
                }
                Err(e) => {
                    yield PlanItem::from(Msg::Fail(e.to_string()));
                    return;
                }
            }
        }
        let _ = tx.send(out);
    });
    (plan, rx)
}

/// The one value of a reading — bluesky `rd`'s hinted-or-only-field rule.
fn single_value(obj: &dyn ReadableObj, data: &HashMap<String, ReadingValue>) -> Result<Value> {
    if let Some(field) = obj.hint_fields().and_then(|h| h.into_iter().next()) {
        if let Some(r) = data.get(&field) {
            return Ok(r.value.clone());
        }
    }
    if data.len() == 1 {
        return Ok(data.values().next().unwrap().value.clone());
    }
    Err(BsrsError::Plan(format!(
        "seq_read: '{}' has {} fields and no hint to pick one",
        obj.name(),
        data.len()
    )))
}

/// Record one optimization step as a single event — blop's `read_step`.
///
/// The step's suggestions and outcomes are grouped by id, ordered by the
/// id's string form, and flattened into one array per key; `n_points` fixes
/// the array length, with missing trials padded by `null` (blop pads with
/// NaN, which JSON cannot carry) and their ids by `""`. The
/// `acquisition_uid` field holds [`AcquisitionUid::event_value`].
pub fn read_step<U: AcquisitionUid>(
    uid: &U,
    suggestions: &[Suggestion],
    outcomes: &[Outcome],
    n_points: usize,
    cache: &ReadableCache,
    stream_name: &str,
) -> Plan {
    let uid = uid.event_value();
    let suggestions = suggestions.to_vec();
    let outcomes = outcomes.to_vec();
    let cache = cache.clone();
    let stream_name = stream_name.to_owned();
    plan_items(async_stream::stream! {
        let by_sid: BTreeMap<String, &Params> =
            suggestions.iter().map(|s| (s.id.to_string(), &s.params)).collect();
        let by_oid: BTreeMap<String, &Params> =
            outcomes.iter().map(|o| (o.id.to_string(), &o.values)).collect();
        if by_sid.keys().ne(by_oid.keys()) {
            yield PlanItem::from(Msg::Fail(format!(
                "The suggestions and outcomes must contain the same IDs. Got suggestions: {:?} and outcomes: {:?}",
                by_sid.keys().collect::<Vec<_>>(),
                by_oid.keys().collect::<Vec<_>>(),
            )));
            return;
        }
        let mut sids: Vec<Value> = by_sid.keys().map(|k| Value::from(k.as_str())).collect();
        let mut params_flat: BTreeMap<String, Vec<Value>> = BTreeMap::new();
        let mut outcomes_flat: BTreeMap<String, Vec<Value>> = BTreeMap::new();
        for sid in by_sid.keys() {
            for (name, value) in by_sid[sid] {
                params_flat.entry(name.clone()).or_default().push(value.clone());
            }
            for (name, value) in by_oid[sid] {
                outcomes_flat.entry(name.clone()).or_default().push(value.clone());
            }
        }
        let actual = sids.len();
        if actual < n_points {
            let pad = n_points - actual;
            for values in params_flat.values_mut().chain(outcomes_flat.values_mut()) {
                values.extend(std::iter::repeat_n(Value::Null, pad));
            }
            sids.extend(std::iter::repeat_n(Value::from(""), pad));
        }
        track(&cache, SUGGESTION_IDS_KEY, Source::SuggestionId, Value::Array(sids));
        track(&cache, ACQUISITION_UID_KEY, Source::AcquisitionUid, uid.clone());
        for (name, values) in params_flat {
            track(&cache, &name, Source::Parameter, Value::Array(values));
        }
        for (name, values) in outcomes_flat {
            track(&cache, &name, Source::Outcome, Value::Array(values));
        }
        let mut read = bps::trigger_and_read(vec![], tracked_readables(&cache), stream_name.clone());
        while let Some(item) = read.next().await {
            yield item;
        }
    })
}

/// Move `actuators` to the best point — blop's `navigate_to_best`. With no
/// explicit `parameterization` the optimizer's best point is used; several
/// Pareto-optimal points fail the plan so the caller can choose one.
/// Parameters naming no actuator are ignored.
pub fn navigate_to_best(
    actuators: Vec<Actuator>,
    optimizer: Option<SharedOptimizer>,
    parameterization: Option<Params>,
) -> Plan {
    plan_items(async_stream::stream! {
        let params = match parameterization {
            Some(p) => p,
            None => {
                let Some(optimizer) = optimizer else {
                    yield PlanItem::from(Msg::Fail(
                        "Either pass an explicit parameterization or use an optimizer.".into(),
                    ));
                    return;
                };
                let best = optimizer.lock().unwrap().best_points();
                match best {
                    Ok(mut points) if points.len() == 1 => points.remove(0).params,
                    Ok(points) if points.is_empty() => {
                        yield PlanItem::from(Msg::Fail("The optimizer returned no best point.".into()));
                        return;
                    }
                    Ok(points) => {
                        yield PlanItem::from(Msg::Fail(format!(
                            "The optimizer returned {} Pareto-optimal points. Please call \
                             optimizer.best_points() to inspect them and pass your chosen \
                             parameterization explicitly via the 'parameterization' argument.",
                            points.len()
                        )));
                        return;
                    }
                    Err(e) => {
                        yield PlanItem::from(Msg::Fail(e.to_string()));
                        return;
                    }
                }
            }
        };
        let mut moves = Vec::new();
        for (name, value) in &params {
            let Some(actuator) = actuators.iter().find(|a| a.name() == name) else {
                continue;
            };
            let Some(target) = value.as_f64() else {
                yield PlanItem::from(Msg::Fail(format!(
                    "navigate_to_best: '{name}' = {value} is not numeric"
                )));
                return;
            };
            moves.push((actuator.movable.clone(), target));
        }
        if !moves.is_empty() {
            let mut mv = bps::mv_many(moves);
            while let Some(item) = mv.next().await {
                yield item;
            }
        }
    })
}

/// The list-scan axes for `suggestions` over `actuators` — blop's
/// `_unpack_for_list_scan`. Every suggestion must carry a numeric value for
/// every actuator.
pub fn list_scan_axes(
    suggestions: &[Suggestion],
    actuators: &[Actuator],
) -> Result<Vec<ListScanAxis>> {
    actuators
        .iter()
        .map(|a| {
            let points = suggestions
                .iter()
                .map(|s| {
                    s.params
                        .get(a.name())
                        .and_then(Value::as_f64)
                        .ok_or_else(|| {
                            BsrsError::Plan(format!(
                                "suggestion '{}' has no numeric value for actuator '{}'",
                                s.id,
                                a.name()
                            ))
                        })
                })
                .collect::<Result<Vec<f64>>>()?;
            let reader = a.reader.clone().unwrap_or_else(|| {
                Arc::new(NoReadback(a.name().to_owned())) as Arc<dyn ReadableObj>
            });
            Ok((a.movable.clone(), reader, points))
        })
        .collect()
}

/// The readback slot of a move-only actuator: reads and describes nothing,
/// so the scan's per-step read of "the motor" adds no field.
struct NoReadback(String);

impl NamedObj for NoReadback {
    fn name(&self) -> &str {
        &self.0
    }
}

#[async_trait]
impl ReadableObj for NoReadback {
    async fn read_dyn(&self) -> Result<HashMap<String, ReadingValue>> {
        Ok(HashMap::new())
    }
    async fn describe_dyn(&self) -> Result<HashMap<String, DataKey>> {
        Ok(HashMap::new())
    }
}

/// Route `suggestions` from the actuators' current positions when there is
/// more than one and every actuator can be read; otherwise keep their order.
/// The shared head of `default_acquire` and `list_scan_in_run`.
pub(super) fn routed(
    suggestions: Vec<Suggestion>,
    actuators: &[Actuator],
) -> PlanWithResult<Vec<Suggestion>> {
    let (tx, rx) = oneshot::channel();
    let readers: Option<Vec<Arc<dyn ReadableObj>>> =
        actuators.iter().map(|a| a.reader.clone()).collect();
    let plan = plan_items(async_stream::stream! {
        if suggestions.len() < 2 {
            let _ = tx.send(suggestions);
            return;
        }
        let start = match readers {
            Some(readers) => {
                let (mut read, pos) = seq_read(readers);
                while let Some(item) = read.next().await {
                    yield item;
                }
                match pos.await {
                    Ok(p) => Some(p),
                    Err(_) => return,
                }
            }
            None => None,
        };
        let _ = tx.send(route_suggestions(suggestions, start.as_ref()));
    });
    (plan, rx)
}

/// Acquire `suggestions` inside an already-open run — blop's
/// `list_scan_in_run`. Routes the suggestions, runs a list scan with its
/// run-control messages dropped (stage/unstage are kept), and produces the
/// suggestion ids in the order they were executed. A custom `per_step` may
/// emit any events into any streams; the evaluation function correlates
/// them with the ordered ids.
pub fn list_scan_in_run(
    suggestions: Vec<Suggestion>,
    actuators: Vec<Actuator>,
    sensors: Vec<Sensor>,
    per_step: Option<PerStep>,
) -> PlanWithResult<Vec<TrialId>> {
    let (tx, rx) = oneshot::channel();
    let plan = plan_items(async_stream::stream! {
        if let Err(e) = validate_suggestions(&suggestions) {
            yield PlanItem::from(Msg::Fail(e.to_string()));
            return;
        }
        let (mut route, ordered) = routed(suggestions, &actuators);
        while let Some(item) = route.next().await {
            yield item;
        }
        let Ok(suggestions) = ordered.await else { return };
        let ids: Vec<TrialId> = suggestions.iter().map(|s| s.id.clone()).collect();
        let axes = match list_scan_axes(&suggestions, &actuators) {
            Ok(a) => a,
            Err(e) => {
                yield PlanItem::from(Msg::Fail(e.to_string()));
                return;
            }
        };
        let mut scan = drop_run_control_messages(list_scan_nd(sensors, axes, per_step));
        while let Some(item) = scan.next().await {
            yield item;
        }
        let _ = tx.send(ids);
    });
    (plan, rx)
}
