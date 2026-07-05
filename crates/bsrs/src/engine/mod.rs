//! bsrs-engine — RunEngine, Bundler, Suspender, checkpoint state.

#![deny(missing_docs)]

pub mod bundler;
pub mod run_engine;
pub mod sink;
pub mod suspender;

pub use crate::core::msg::{MsgResult, SubscriptionId};
pub use bundler::RunBundler;
pub use run_engine::{
    CheckpointHook, CheckpointSnapshot, CustomCommandHandler, DocumentCallback, EngineRunState,
    InputHandler, MdNormalizer, MdValidator, PlanHook, Preprocessor, RunEngine, RunOptions,
    RunResult, ScanIdSource, SuspendCallback,
};
pub use sink::{BroadcastSink, DocumentSink};
pub use suspender::{
    SuspendBoolHigh, SuspendBoolLow, SuspendOutsideBand, SuspendThreshold, SuspendWhenChanged,
    Suspender, ThresholdDirection,
};
