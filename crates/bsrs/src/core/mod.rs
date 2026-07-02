//! bsrs-core — foundational types: `Reading`, `Status`, `Msg`, `Plan`, runtime.

#![deny(missing_docs)]

pub mod error;
pub mod ext;
pub mod kind;
pub mod lua_exposable;
pub mod msg;
pub mod plan;
pub mod reading;
pub mod runtime;
pub mod status;
pub mod subscription;
pub mod suspender;

pub use error::{BsrsError, Result};
pub use ext::{
    FlyableExt, LocatableExt, MonitorableExt, MovableExt, ReadableExt, StageableExt, StoppableExt,
    TriggerableExt,
};
pub use kind::Kind;
pub use lua_exposable::{LuaExposable, LuaMethodEntry};
pub use msg::{ConfigureArgs, GroupId, Msg, RunMetadata};
pub use plan::{plan_box, Plan, PlanItem};
pub use reading::{ReadingF64, ReadingValue, TypedReading};
pub use runtime::{bsrs_runtime, runtime_handle};
pub use status::{
    CancelGuard, Status, StatusError, StatusOutcome, SubToken, Watcher, WatcherUpdate,
};
pub use subscription::Subscription;
pub use suspender::Suspender;

// re-export selected event-model types so devices/plans don't have to
// depend on bsrs-event-model directly.
pub use crate::event_model::{DataKey, Document, Dtype};
