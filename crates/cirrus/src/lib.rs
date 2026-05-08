//! cirrus facade — exposes the two co-equal API surfaces.

#![deny(missing_docs)]

/// Async (ophyd-async style) module — the default.
pub mod ophyd_async {
    pub use cirrus_devices::*;
    pub use cirrus_plans::*;
    pub use cirrus_protocols_async::*;
}

/// Sync (ophyd style) module — blanket sync impls over the async core.
pub mod ophyd {
    pub use cirrus_devices::*;
    pub use cirrus_plans::*;
    pub use cirrus_protocols_async::{
        AsyncConfigurable, AsyncMovable, AsyncReadable, AsyncSubscribable, Collectable,
        DetectorControl, DetectorWriter, Flyable, Frame, FrameSink, FrameSource, Locatable,
        Pausable, Preparable, SignalBackend, Stageable, Stoppable, StreamAsset, TriggerInfo,
        Triggerable, WritesStreamAssets,
    };
    pub use cirrus_protocols_sync::{
        Configurable, FlyableSync, Movable, Readable, StageableSync, TriggerableSync,
    };
}

/// Common items re-exported regardless of API surface.
pub mod prelude {
    pub use cirrus_core::reading::{ReadingF64, ReadingValue, TypedReading};
    pub use cirrus_core::{CirrusError, Document, Kind, Msg, Plan, Result, Status, SubToken};
    pub use cirrus_engine::{BroadcastSink, DocumentSink, RunEngine, RunResult};
    pub use cirrus_event_model::{
        DataKey, EventDescriptor, RunStart, RunStop, StreamDatum, StreamRange, StreamResource,
    };
}

// Convenience re-exports of backends so users can `use cirrus::backends::soft::*`.
/// Backend re-exports.
pub mod backends {
    /// Soft (in-memory) backend.
    pub mod soft {
        pub use cirrus_backend_soft::*;
    }
    /// Mock backend.
    pub mod mock {
        pub use cirrus_backend_mock::*;
    }
}

/// Streaming pipe and reference sources/sinks.
pub mod stream {
    pub use cirrus_stream::*;
}

/// Document sinks (callbacks).
pub mod callbacks {
    pub use cirrus_callbacks::*;
}
