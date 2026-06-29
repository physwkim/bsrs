//! # bsrs
//!
//! Bsrs — a Rust port of the bluesky / ophyd / ophyd-async data-acquisition
//! stack with EPICS backends.
//!
//! Single-crate build: every former `bsrs-*` library crate is now a module
//! here, with the optional integrations gated behind Cargo features
//! (`ca`, `pva`, `tiled`, `zmq`, `kafka`, `hdf5`, `qs`, `host`, `metrics`,
//! `cli`, `frame-source`). The procedural macros live in the companion
//! `bsrs-derive` crate and are re-exported below, so depending on `bsrs`
//! alone is enough.
//!
//! Two co-equal API surfaces are offered through the [`ophyd_async`] and
//! [`ophyd`] facade modules, plus a [`prelude`] of the most common items.

// Lets the `bsrs-derive` macros emit `::bsrs::core::…` / `::bsrs::devices::…`
// absolute paths that resolve identically inside this crate and in downstream
// crates that depend on `bsrs`.
extern crate self as bsrs;

// --- always-on core layers ---
pub mod backends;
pub mod callbacks;
pub mod core;
pub mod devices;
pub mod engine;
pub mod event_model;
pub mod plans;
pub mod protocols_async;
pub mod protocols_sync;
pub mod stream;

/// Queue-server-compatible 0MQ JSON-RPC daemon (feature `qs`).
#[cfg(feature = "qs")]
pub mod qs;

/// Host runtime — Lua bridge + CA/PVA device factories (feature `host`).
#[cfg(feature = "host")]
pub mod host;

// Re-export the derive macros from the companion proc-macro crate so users
// only need to depend on `bsrs`.
pub use bsrs_derive::{lua_methods, Device};

/// Async (ophyd-async style) module — the default surface.
pub mod ophyd_async {
    pub use crate::devices::*;
    pub use crate::plans::*;
    pub use crate::protocols_async::*;
}

/// Sync (ophyd style) module — blanket sync impls over the async core.
pub mod ophyd {
    pub use crate::devices::*;
    pub use crate::plans::*;
    pub use crate::protocols_async::{
        AsyncConfigurable, AsyncMovable, AsyncReadable, AsyncSubscribable, Collectable,
        DetectorControl, DetectorWriter, FlyMotorInfo, Flyable, Frame, FrameSink, FrameSource,
        Locatable, Pausable, Preparable, SignalBackend, Stageable, Stoppable, StreamAsset,
        TriggerInfo, Triggerable, WritesStreamAssets,
    };
    pub use crate::protocols_sync::{
        Configurable, FlyableSync, Movable, Readable, StageableSync, TriggerableSync,
    };
}

/// Common items re-exported regardless of API surface.
pub mod prelude {
    pub use crate::core::reading::{ReadingF64, ReadingValue, TypedReading};
    pub use crate::core::{BsrsError, Document, Kind, Msg, Plan, Result, Status, SubToken};
    // Bluesky-style short-name extensions for trait objects:
    //   motor.position().await?     // = motor.locate_dyn().await?.readback
    //   det.read().await?           // = det.read_dyn().await?
    //   motor.set(1.0).await        // returns Status
    //   det.trigger().await         // returns Status
    pub use crate::core::{
        FlyableExt, LocatableExt, MonitorableExt, MovableExt, ReadableExt, StageableExt,
        StoppableExt, TriggerableExt,
    };
    pub use crate::engine::{BroadcastSink, DocumentSink, RunEngine, RunResult};
    pub use crate::event_model::{
        DataKey, EventDescriptor, ExitStatus, RunStart, RunStop, StreamDatum, StreamRange,
        StreamResource,
    };
}
