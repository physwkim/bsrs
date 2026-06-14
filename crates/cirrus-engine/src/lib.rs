//! cirrus-engine — RunEngine, Bundler, Suspender, checkpoint state.

#![deny(missing_docs)]

pub mod bundler;
pub mod engine;
pub mod sink;
pub mod suspender;

pub use bundler::RunBundler;
pub use engine::{
    CheckpointHook, CheckpointSnapshot, CustomCommandHandler, DocumentCallback, EngineRunState,
    InputHandler, MdNormalizer, MdValidator, MsgResult, PlanHook, Preprocessor, RunEngine,
    RunOptions, RunResult, ScanIdSource, SubscriptionId, SuspendCallback,
};
pub use sink::{BroadcastSink, DocumentSink};
pub use suspender::{
    SuspendBoolHigh, SuspendBoolLow, SuspendOutsideBand, SuspendThreshold, SuspendWhenChanged,
    Suspender, ThresholdDirection,
};
