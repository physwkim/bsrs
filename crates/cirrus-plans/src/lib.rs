//! Plans for cirrus — equivalents of `bluesky.plans` and `bluesky.plan_stubs`.

#![deny(missing_docs)]

use cirrus_core::msg::{
    CollectableObj, FlyableObj, MovableObj, Msg, ReadableObj, RunMetadata, StageableObj,
    TriggerableObj,
};
use cirrus_core::plan::{plan_box, Plan};
use std::sync::Arc;

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

/// Plan stubs (single-Msg helpers).
pub mod stubs {
    use super::*;

    /// `mv(motor, value)` — set + wait.
    pub fn mv(motor: Arc<dyn MovableObj>, value: f64) -> Plan {
        plan_box(async_stream::stream! {
            yield Msg::Set {
                obj: motor,
                value,
                group: Some("mv".into()),
            };
            yield Msg::Wait {
                group: "mv".into(),
                error_on_timeout: true,
                timeout: None,
            };
        })
    }

    /// `sleep(duration)`.
    pub fn sleep(d: std::time::Duration) -> Plan {
        plan_box(async_stream::stream! {
            yield Msg::Sleep(d);
        })
    }
}
