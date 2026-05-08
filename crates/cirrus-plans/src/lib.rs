//! Plans for cirrus — equivalents of `bluesky.plans` and `bluesky.plan_stubs`.

#![deny(missing_docs)]

use cirrus_core::msg::{
    CollectableObj, ConfigurableObj, ConfigureArgs, FlyableObj, LocatableObj, MonitorableObj,
    MovableObj, Msg, ReadableObj, RunMetadata, StageableObj, StoppableObj, TriggerableObj,
};
use cirrus_core::plan::{plan_box, Plan};
use std::sync::Arc;
use std::time::Duration;

// ===========================================================================
//  plan_stubs (single-Msg / small composites; mirrors bluesky.plan_stubs)
// ===========================================================================

/// `bluesky.plan_stubs` equivalents — single- or few-`Msg` helpers that are
/// the building blocks of compound plans.
pub mod stubs {
    use super::*;

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
    /// readback via `LocatableObj::locate_dyn` *inside* the generator,
    /// adds `delta`, then yields `Set`+`Wait` for the absolute target.
    /// Motor must implement `LocatableObj` (which extends `MovableObj`).
    pub fn mvr(motor: Arc<dyn LocatableObj>, delta: f64) -> Plan {
        plan_box(async_stream::stream! {
            let loc = motor.locate_dyn().await
                .expect("mvr: locate_dyn failed");
            let target = loc.readback + delta;
            let movable: Arc<dyn MovableObj> = motor;
            yield Msg::Set { obj: movable, value: target, group: Some("mv".into()) };
            yield Msg::Wait { group: "mv".into(), error_on_timeout: true, timeout: None };
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
        plan_box(async_stream::stream! {
            for t in &triggerables {
                yield Msg::Trigger { obj: t.clone(), group: Some("trig".into()) };
            }
            yield Msg::Wait { group: "trig".into(), error_on_timeout: true, timeout: None };
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
        trigger_and_read(triggerables, readables, "primary")
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
            for t in &triggerables {
                yield Msg::Trigger { obj: t.clone(), group: Some("trigger".into()) };
            }
            yield Msg::Wait {
                group: "trigger".into(),
                error_on_timeout: true,
                timeout: None,
            };
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
pub fn rel_scan(
    detectors: Vec<Arc<dyn ReadableObj>>,
    motor: Arc<dyn MovableObj>,
    motor_reader: Arc<dyn ReadableObj>,
    current: f64,
    start: f64,
    stop: f64,
    num: usize,
) -> Plan {
    scan(
        detectors,
        motor,
        motor_reader,
        current + start,
        current + stop,
        num,
    )
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
