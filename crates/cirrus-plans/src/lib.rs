//! Plans for cirrus — equivalents of `bluesky.plans` and `bluesky.plan_stubs`.

#![deny(missing_docs)]

pub mod patterns;
pub mod preprocessors;

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

// ---------------------------------------------------------------------------
// Multi-axis & list-grid plans (mirrors bluesky.plans).
// ---------------------------------------------------------------------------

/// One axis of a multi-motor scan: `(motor, motor_reader, start, stop)`.
pub type ScanAxis = (Arc<dyn MovableObj>, Arc<dyn ReadableObj>, f64, f64);

/// One axis of a list-grid scan: `(motor, motor_reader, points)`.
pub type ListGridAxis = (Arc<dyn MovableObj>, Arc<dyn ReadableObj>, Vec<f64>);

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
        for row in points {
            for (i, v) in row.iter().enumerate() {
                if i >= motors.len() { break; }
                yield Msg::Set {
                    obj: motors[i].0.clone(),
                    value: *v,
                    group: Some("set".into()),
                };
            }
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
/// readback once at the start of the plan and offsets the supplied points.
pub fn rel_list_scan(
    detectors: Vec<Arc<dyn ReadableObj>>,
    motor: Arc<dyn LocatableObj>,
    motor_reader: Arc<dyn ReadableObj>,
    points: Vec<f64>,
) -> Plan {
    plan_box(async_stream::stream! {
        let bias = motor.locate_dyn().await
            .map(|l| l.readback)
            .unwrap_or(0.0);
        let abs_points: Vec<f64> = points.iter().map(|p| *p + bias).collect();
        let mv: Arc<dyn MovableObj> = motor;
        let mut inner = list_scan(detectors, mv, motor_reader, abs_points);
        while let Some(item) = futures::StreamExt::next(&mut inner).await {
            if let cirrus_core::plan::PlanItem::Bare(m) = item {
                yield m;
            }
        }
    })
}

/// `rel_grid_scan` — relative variant of `grid_scan`. Both motors are
/// `LocatableObj` so we can snapshot starting positions.
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
    plan_box(async_stream::stream! {
        let b1 = motor1.locate_dyn().await.map(|l| l.readback).unwrap_or(0.0);
        let b2 = motor2.locate_dyn().await.map(|l| l.readback).unwrap_or(0.0);
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
    })
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
/// Reads the motor's current readback once at start, adds the
/// supplied `start`/`stop` offsets, and runs `adaptive_scan` over
/// that absolute range. After the scan, the motor remains at its
/// last position; pair with `reset_positions_wrapper` if you want
/// to restore the original.
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
    plan_box(async_stream::stream! {
        let center = match motor.locate_dyn().await {
            Ok(loc) => loc.readback,
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
    })
}
