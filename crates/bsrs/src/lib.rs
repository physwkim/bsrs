//! bsrs facade — exposes the two co-equal API surfaces.

#![deny(missing_docs)]

/// Async (ophyd-async style) module — the default.
pub mod ophyd_async {
    pub use bsrs_devices::*;
    pub use bsrs_plans::*;
    pub use bsrs_protocols_async::*;
}

/// Sync (ophyd style) module — blanket sync impls over the async core.
pub mod ophyd {
    pub use bsrs_devices::*;
    pub use bsrs_plans::*;
    pub use bsrs_protocols_async::{
        AsyncConfigurable, AsyncMovable, AsyncReadable, AsyncSubscribable, Collectable,
        DetectorControl, DetectorWriter, FlyMotorInfo, Flyable, Frame, FrameSink, FrameSource,
        Locatable, Pausable, Preparable, SignalBackend, Stageable, Stoppable, StreamAsset,
        TriggerInfo, Triggerable, WritesStreamAssets,
    };
    pub use bsrs_protocols_sync::{
        Configurable, FlyableSync, Movable, Readable, StageableSync, TriggerableSync,
    };
}

/// Common items re-exported regardless of API surface.
pub mod prelude {
    pub use bsrs_core::reading::{ReadingF64, ReadingValue, TypedReading};
    pub use bsrs_core::{BsrsError, Document, Kind, Msg, Plan, Result, Status, SubToken};
    // Bluesky-style short-name extensions for trait objects:
    //   motor.position().await?     // = motor.locate_dyn().await?.readback
    //   det.read().await?           // = det.read_dyn().await?
    //   motor.set(1.0).await        // returns Status
    //   det.trigger().await         // returns Status
    pub use bsrs_core::{
        FlyableExt, LocatableExt, MonitorableExt, MovableExt, ReadableExt, StageableExt,
        StoppableExt, TriggerableExt,
    };
    pub use bsrs_engine::{BroadcastSink, DocumentSink, RunEngine, RunResult};
    pub use bsrs_event_model::{
        DataKey, EventDescriptor, ExitStatus, RunStart, RunStop, StreamDatum, StreamRange,
        StreamResource,
    };
}

// Convenience re-exports of backends so users can `use bsrs::backends::soft::*`.
/// Backend re-exports.
pub mod backends {
    /// Soft (in-memory) backend.
    pub mod soft {
        pub use bsrs_backend_soft::*;
    }
    /// Mock backend.
    pub mod mock {
        pub use bsrs_backend_mock::*;
    }
}

/// Streaming pipe and reference sources/sinks.
pub mod stream {
    pub use bsrs_stream::*;
}

/// Document sinks (callbacks).
pub mod callbacks {
    pub use bsrs_callbacks::*;
}
