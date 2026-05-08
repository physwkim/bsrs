//! Async ophyd-async-style protocol traits.
//!
//! These are the cirrus equivalents of the Python protocols in
//! `bluesky/protocols.py:36-526`. Async-first; `cirrus-protocols-sync` provides
//! a sync facade via blanket impls.

#![deny(missing_docs)]

use async_trait::async_trait;
use bytes::Bytes;
pub use cirrus_core::Subscription;

use cirrus_core::{
    error::Result,
    reading::ReadingValue,
    status::{Status, SubToken},
    ConfigureArgs,
};
use cirrus_event_model::{DataKey, StreamDatum, StreamResource};
use futures::stream::BoxStream;
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::watch;

// -- Sealed trait #1 --------------------------------------------------------

/// Reading callback type for `set_callback`.
pub type ReadingValueCallback<T> = Box<dyn Fn(&T, f64) + Send + Sync>;

/// Sealed: backend for one signal. Direct port of
/// `ophyd_async/core/_signal_backend.py:16-59`.
#[async_trait]
pub trait SignalBackend<T: Clone + Send + Sync + 'static>: Send + Sync {
    /// Connect to the underlying transport.
    async fn connect(&self, timeout: Duration) -> Result<()>;
    /// Put a value, optionally waiting for completion.
    async fn put(&self, value: T, wait: bool, timeout: Option<Duration>) -> Status;
    /// Describe the signal as a `DataKey`.
    async fn get_datakey(&self, source: &str) -> Result<DataKey>;
    /// Read current value as a `Reading` (JSON-erased).
    async fn get_reading(&self) -> Result<ReadingValue>;
    /// Read current value strongly typed.
    async fn get_value(&self) -> Result<T>;
    /// Read current setpoint.
    async fn get_setpoint(&self) -> Result<T>;
    /// Subscribe to value updates. RAII token cleans up on drop.
    fn set_callback(&self, cb: Option<ReadingValueCallback<T>>) -> SubToken;
    /// Source string for `DataKey.source`.
    fn source(&self, name: &str) -> String;
}

// -- ophyd-async protocol traits --------------------------------------------

/// Anything that can be `read()` and `describe()`d.
#[async_trait]
pub trait AsyncReadable: Send + Sync {
    /// Stable name.
    fn name(&self) -> &str;
    /// Read all signals.
    async fn read(&self) -> Result<HashMap<String, ReadingValue>>;
    /// Describe each field.
    async fn describe(&self) -> Result<HashMap<String, DataKey>>;
}

/// Anything that can be moved (`set` returns a `Status`).
#[async_trait]
pub trait AsyncMovable<T = f64>: Send + Sync {
    /// Stable name.
    fn name(&self) -> &str;
    /// Move to `value`; returns a `Status` that resolves when the move completes.
    async fn set(&self, value: T) -> Status;
}

/// Anything that can be triggered.
#[async_trait]
pub trait Triggerable: Send + Sync {
    /// Stable name.
    fn name(&self) -> &str;
    /// Trigger; status resolves when triggering is complete.
    async fn trigger(&self) -> Status;
}

/// Anything that can be staged before a run.
#[async_trait]
pub trait Stageable: Send + Sync {
    /// Stable name.
    fn name(&self) -> &str;
    /// Stage.
    async fn stage(&self) -> Result<()>;
    /// Unstage.
    async fn unstage(&self) -> Result<()>;
}

/// Anything that can fly (kickoff/complete).
#[async_trait]
pub trait Flyable: Send + Sync {
    /// Stable name.
    fn name(&self) -> &str;
    /// Begin acquisition; returns when arming is acknowledged.
    async fn kickoff(&self) -> Status;
    /// Wait for the acquisition to complete (target frames done, etc.).
    async fn complete(&self) -> Status;
}

/// Slow-changing fields read into `EventDescriptor.configuration`.
#[async_trait]
pub trait AsyncConfigurable: Send + Sync {
    /// Stable name.
    fn name(&self) -> &str;
    /// Read configuration values.
    async fn read_configuration(&self) -> Result<HashMap<String, ReadingValue>>;
    /// Describe configuration fields.
    async fn describe_configuration(&self) -> Result<HashMap<String, DataKey>>;
    /// Apply configuration.
    async fn configure(&self, args: ConfigureArgs) -> Result<()>;
}

/// Has the concept of "where it is" + "where it's going".
#[async_trait]
pub trait Locatable<T = f64>: AsyncMovable<T> {
    /// Return current setpoint and readback.
    async fn locate(&self) -> Result<Location<T>>;
}

/// Setpoint + readback record.
#[derive(Clone, Debug)]
pub struct Location<T> {
    /// Where the device was last asked to go.
    pub setpoint: T,
    /// Where the device currently is.
    pub readback: T,
}

/// Subscribable: callback + RAII token.
#[async_trait]
pub trait AsyncSubscribable<T: Send + Sync + 'static = f64>: Send + Sync {
    /// Stable name.
    fn name(&self) -> &str;
    /// Subscribe; returns a `Subscription` whose Drop unsubscribes (K2).
    async fn subscribe(&self) -> Result<Subscription>;
}

/// Stoppable: safe shutdown of a device.
#[async_trait]
pub trait Stoppable: Send + Sync {
    /// `success = true` for a planned stop, `false` for emergency.
    async fn stop(&self, success: bool) -> Result<()>;
}

/// Pausable: device-specific pause/resume hooks.
#[async_trait]
pub trait Pausable: Send + Sync {
    /// Called when the engine pauses.
    async fn pause(&self) -> Result<()>;
    /// Called when the engine resumes.
    async fn resume(&self) -> Result<()>;
}

/// Preparable: scan-specific setup.
#[async_trait]
pub trait Preparable<V = serde_json::Value>: Send + Sync {
    /// Stable name.
    fn name(&self) -> &str;
    /// Prepare; status resolves when ready.
    async fn prepare(&self, value: V) -> Status;
}

/// Collectable: describe and yield events from a flying device.
#[async_trait]
pub trait Collectable: Send + Sync {
    /// Stable name.
    fn name(&self) -> &str;
    /// Describe the streams that will be collected.
    async fn describe_collect(&self) -> Result<HashMap<String, HashMap<String, DataKey>>>;
    /// Yield events. Empty vec if nothing buffered.
    async fn collect(&self) -> Result<Vec<(String, HashMap<String, Value>, HashMap<String, f64>)>>;
}

/// Stream-asset emitter (resource + datum docs).
pub enum StreamAsset {
    /// A new stream resource.
    Resource(StreamResource),
    /// A new stream datum.
    Datum(StreamDatum),
}

/// Devices that write external assets and emit `StreamResource`/`StreamDatum`.
#[async_trait]
pub trait WritesStreamAssets: Send + Sync {
    /// Stable name.
    fn name(&self) -> &str;
    /// Returns the current write index (frames written so far).
    async fn get_index(&self) -> Result<u64>;
    /// Yield asset documents up to `up_to`.
    fn collect_asset_docs(&self, up_to: u64) -> BoxStream<'_, StreamAsset>;
}

/// Sealed: detector control half (`prepare`/`arm`/`wait_for_idle`/`disarm`).
#[async_trait]
pub trait DetectorControl: Send + Sync {
    /// For a given exposure, return the minimum dead-time.
    fn deadtime(&self, exposure: Option<Duration>) -> Duration;
    /// Configure trigger info (number, type, livetime, multiplier, ...).
    async fn prepare(&self, info: TriggerInfo) -> Result<()>;
    /// Arm; status resolves when armed.
    async fn arm(&self) -> Status;
    /// Wait for the detector to return to idle.
    async fn wait_for_idle(&self) -> Result<()>;
    /// Disarm.
    async fn disarm(&self) -> Result<()>;
}

/// Detector trigger configuration.
#[derive(Clone, Debug)]
pub struct TriggerInfo {
    /// Number of triggers, 0 for infinite.
    pub number: u32,
    /// Live (exposure) time.
    pub livetime: Option<Duration>,
    /// Required dead-time.
    pub deadtime: Option<Duration>,
    /// Triggers per emitted index.
    pub multiplier: u32,
}

impl Default for TriggerInfo {
    fn default() -> Self {
        Self {
            number: 1,
            livetime: None,
            deadtime: None,
            multiplier: 1,
        }
    }
}

/// Sealed: detector writer half (open / observe / collect_stream_docs / close).
#[async_trait]
pub trait DetectorWriter: Send + Sync {
    /// Open the writer; returns the `data_keys` that the writer will produce.
    async fn open(&self, multiplier: u32) -> Result<HashMap<String, DataKey>>;
    /// Observe the per-frame index counter.
    fn observe_indices_written(&self) -> watch::Receiver<u64>;
    /// Read the current index synchronously (atomic load).
    async fn indices_written(&self) -> u64;
    /// Yield asset documents for frames up to `up_to`.
    fn collect_stream_docs(&self, up_to: u64) -> BoxStream<'_, StreamAsset>;
    /// Close the writer.
    async fn close(&self) -> Result<()>;
}

// -- FrameSource / FrameSink -------------------------------------------------

/// Bulk-data unit. Zero-copy clone via `Bytes`.
#[derive(Clone, Debug)]
pub struct Frame {
    /// Payload bytes.
    pub payload: Bytes,
    /// Wall-clock timestamp (ns).
    pub ts_ns: u64,
    /// Channel id (rogue compatibility).
    pub channel: u8,
    /// Flags (rogue compatibility).
    pub flags: u16,
    /// Sequence number.
    pub seq: u64,
}

/// Sealed: produces `Frame`s.
#[async_trait]
pub trait FrameSource: Send + Sync {
    /// Stream of frames.
    fn frames(&self) -> BoxStream<'static, Frame>;
    /// Optional downstream-allocator.
    fn pool(&self) -> Option<&dyn FrameAllocator> {
        None
    }
    /// Begin producing frames.
    async fn start(&self) -> Result<()>;
    /// Stop producing frames.
    async fn stop(&self) -> Result<()>;
}

/// Sealed: consumes `Frame`s.
#[async_trait]
pub trait FrameSink: Send + Sync {
    /// Accept a frame.
    async fn accept(&self, frame: Frame) -> Result<()>;
}

/// rogue Pool equivalent.
#[async_trait]
pub trait FrameAllocator: Send + Sync {
    /// Allocate a buffer of at least `min_bytes`.
    async fn alloc(&self, min_bytes: usize, zero_copy: bool) -> bytes::BytesMut;
    /// Return a buffer to the pool (optional).
    fn ret(&self, _buf: bytes::BytesMut) {}
}
