//! Async ophyd-async-style protocol traits.
//!
//! These are the bsrs equivalents of the Python protocols in
//! `bluesky/protocols.py:36-526`. Async-first; `bsrs-protocols-sync` provides
//! a sync facade via blanket impls.

#![deny(missing_docs)]

use async_trait::async_trait;
pub use bsrs_core::Subscription;
use bytes::Bytes;

use bsrs_core::{
    error::Result,
    reading::ReadingValue,
    status::{Status, SubToken},
    ConfigureArgs,
};
use bsrs_event_model::{DataKey, StreamDatum, StreamResource};
use futures::stream::BoxStream;
use serde_json::Value;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::watch;

// -- Sealed trait #1 --------------------------------------------------------

/// Reading callback type for `set_callback`: `(value, timestamp,
/// alarm_severity)`. `alarm_severity` follows the bsrs/ophyd-async
/// convention (NO_ALARM=0 / MINOR=1 / MAJOR=2 / INVALID=-1); `None` when the
/// transport does not deliver alarm state on a monitor update (soft/mock
/// signals, or a PVA monitor whose projection omits the `alarm` field).
pub type ReadingValueCallback<T> = Box<dyn Fn(&T, f64, Option<i32>) + Send + Sync>;

/// Sealed: backend for one signal. Direct port of
/// `ophyd_async/core/_signal_backend.py:16-59`.
#[async_trait]
pub trait SignalBackend<T: Clone + Send + Sync + 'static>: Send + Sync {
    /// Connect to the underlying transport.
    async fn connect(&self, timeout: Duration) -> Result<()>;
    /// Put a value to the signal, awaiting completion. `None` writes the
    /// backend's default value (used by trigger-style `SignalX` puts).
    /// Mirrors `ophyd_async/core/_signal_backend.py:82` `put(value: T | None)`:
    /// waiting-for-completion is implicit, and any timeout lives on the
    /// `Signal` layer (`SignalW::set` / `SignalX::trigger`), not here.
    async fn put(&self, value: Option<T>) -> Result<()>;
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
    ///
    /// `read=true` returns the URI used to GET (read back) the signal;
    /// `read=false` returns the URI used to PUT (write) the signal.
    /// Mirrors `ophyd_async/core/_signal_backend.py:70-75` `source(name, read)`.
    fn source(&self, name: &str, read: bool) -> String;
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

/// Structured progress-update sink — re-exported from `bsrs-core`, where it
/// lives alongside [`WatcherUpdate`](bsrs_core::status::WatcherUpdate) and
/// [`Status`] so a status can drive it directly via
/// [`observe_watcher`](bsrs_core::status::Status::observe_watcher). A
/// `LiveTable` / progress bar implements this.
pub use bsrs_core::status::Watcher;

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
    /// Yield asset documents up to `up_to`, stamping each `StreamDatum` with
    /// `descriptor` (the EventDescriptor UID linking the stream data to its
    /// schema; empty when the caller has no descriptor context, e.g. a raw
    /// streaming tool with no open run).
    fn collect_asset_docs(&self, up_to: u64, descriptor: &str) -> BoxStream<'_, StreamAsset>;
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

/// Mechanism for triggering a detector to take exposures. Direct port of
/// `ophyd_async/core/_detector.py:50-62` `DetectorTrigger`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DetectorTrigger {
    /// On arm, generate internally timed exposures.
    #[default]
    Internal,
    /// On every (normally rising) edge of an external input, generate an
    /// internally timed exposure.
    ExternalEdge,
    /// On a rising edge of an external input start an exposure, ending on the
    /// falling edge.
    ExternalLevel,
}

/// Detector trigger configuration. Direct port of
/// `ophyd_async/core/_detector.py:65-113` `TriggerInfo`.
#[derive(Clone, Debug)]
pub struct TriggerInfo {
    /// What sort of triggering the detector should be set for.
    pub trigger: DetectorTrigger,
    /// Number of bluesky events that will be emitted (`0` = infinite).
    pub number_of_events: u32,
    /// Live (exposure) time. `None` leaves whatever the detector currently has.
    pub livetime: Option<Duration>,
    /// Dead-time required between exposures. `None` = the detector minimum.
    pub deadtime: Option<Duration>,
    /// Exposures averaged/combined into a single collection. `> 1` drives the
    /// areaDetector `NumExposures` PV.
    pub exposures_per_collection: u32,
    /// Collections published per emitted bluesky event — lets a fast detector
    /// zip several collections against a slower detector's single collection.
    pub collections_per_event: u32,
    /// Maximum time to wait for one exposure (guards `complete()` loops).
    /// `None` = derive from `livetime + deadtime` plus an implementation
    /// default.
    pub exposure_timeout: Option<Duration>,
}

impl Default for TriggerInfo {
    fn default() -> Self {
        Self {
            trigger: DetectorTrigger::Internal,
            number_of_events: 1,
            livetime: None,
            deadtime: None,
            exposures_per_collection: 1,
            collections_per_event: 1,
            exposure_timeout: None,
        }
    }
}

impl TriggerInfo {
    /// Total collections taken: `number_of_events * collections_per_event`.
    /// Fly-scan kickoff watches the write index up to this count.
    pub fn number_of_collections(&self) -> u32 {
        self.number_of_events * self.collections_per_event
    }

    /// Total detector exposures (triggers sent):
    /// `number_of_collections * exposures_per_collection`.
    pub fn number_of_exposures(&self) -> u32 {
        self.number_of_collections() * self.exposures_per_collection
    }
}

/// Minimal set of information required to fly a motor. Direct port of
/// `ophyd_async/core/_flyer.py` `FlyMotorInfo`.
///
/// The constant-velocity phase runs from `start_position` to `end_position` in
/// `time_for_move` seconds; [`velocity`](Self::velocity) is therefore derived,
/// not stored. [`ramp_up_start_pos`](Self::ramp_up_start_pos) /
/// [`ramp_down_end_pos`](Self::ramp_down_end_pos) extend each end by the run-up
/// / run-down distance so the motor is already at constant velocity when it
/// crosses `start_position` and only decelerates after `end_position`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct FlyMotorInfo {
    /// Absolute position of the motor once it finishes accelerating to the
    /// desired velocity, in motor EGUs.
    pub start_position: f64,
    /// Absolute position of the motor once it begins decelerating from the
    /// desired velocity, in motor EGUs.
    pub end_position: f64,
    /// Time taken to get from `start_position` to `end_position`, excluding
    /// run-up and run-down, in seconds. Must be `> 0`.
    pub time_for_move: f64,
    /// Maximum time for the complete move, including run up and run down.
    /// `None` mirrors ophyd-async's `CALCULATE_TIMEOUT` sentinel — the motor
    /// derives it (`time_for_move` + run-up + run-down + a default) at prepare
    /// time — matching how [`TriggerInfo::exposure_timeout`] treats `None`.
    pub timeout: Option<Duration>,
}

impl FlyMotorInfo {
    /// Signed velocity of the constant-velocity phase:
    /// `(end_position - start_position) / time_for_move`. Negative when the move
    /// runs from a higher to a lower position.
    pub fn velocity(&self) -> f64 {
        (self.end_position - self.start_position) / self.time_for_move
    }

    /// `start_position` with run-up distance added on, so the motor reaches
    /// `start_position` already at constant velocity:
    /// `start_position - acceleration_time * velocity / 2`. The `velocity` sign
    /// places the run-up on the correct side for either move direction.
    pub fn ramp_up_start_pos(&self, acceleration_time: Duration) -> f64 {
        self.start_position - acceleration_time.as_secs_f64() * self.velocity() / 2.0
    }

    /// `end_position` with run-down distance added on, so the motor only begins
    /// decelerating after `end_position`:
    /// `end_position + acceleration_time * velocity / 2`.
    pub fn ramp_down_end_pos(&self, acceleration_time: Duration) -> f64 {
        self.end_position + acceleration_time.as_secs_f64() * self.velocity() / 2.0
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
    /// Yield asset documents for frames up to `up_to`, stamping each
    /// `StreamDatum` with `descriptor` (the EventDescriptor UID; empty when
    /// the caller has no descriptor context).
    fn collect_stream_docs(&self, up_to: u64, descriptor: &str) -> BoxStream<'_, StreamAsset>;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trigger_info_computed_counts() {
        // ophyd _detector.py:90-93 worked example: number_of_events=10,
        // collections_per_event=5 → 50 collections; exposures_per_collection=3
        // → 150 exposures.
        let info = TriggerInfo {
            number_of_events: 10,
            collections_per_event: 5,
            exposures_per_collection: 3,
            ..Default::default()
        };
        assert_eq!(info.number_of_collections(), 50);
        assert_eq!(info.number_of_exposures(), 150);
    }

    #[test]
    fn trigger_info_default_counts_are_one() {
        let info = TriggerInfo::default();
        assert_eq!(info.trigger, DetectorTrigger::Internal);
        assert_eq!(info.number_of_collections(), 1);
        assert_eq!(info.number_of_exposures(), 1);
    }

    // FlyMotorInfo boundaries: velocity is signed by move direction, and the
    // run-up/run-down extensions land on the correct side for either sign.

    #[test]
    fn fly_motor_velocity_is_signed_distance_over_time() {
        // 0 -> 10 in 2s => +5 EGU/s; reverse => -5 EGU/s.
        let fwd = FlyMotorInfo {
            start_position: 0.0,
            end_position: 10.0,
            time_for_move: 2.0,
            timeout: None,
        };
        let rev = FlyMotorInfo {
            start_position: 10.0,
            end_position: 0.0,
            time_for_move: 2.0,
            timeout: None,
        };
        assert_eq!(fwd.velocity(), 5.0);
        assert_eq!(rev.velocity(), -5.0);
    }

    #[test]
    fn fly_motor_ramp_extends_each_end_forward() {
        // accel 1s, v = 5 => run-up/run-down distance = 1 * 5 / 2 = 2.5.
        let info = FlyMotorInfo {
            start_position: 0.0,
            end_position: 10.0,
            time_for_move: 2.0,
            timeout: None,
        };
        let t = Duration::from_secs(1);
        assert_eq!(info.ramp_up_start_pos(t), -2.5); // before start_position
        assert_eq!(info.ramp_down_end_pos(t), 12.5); // past end_position
    }

    #[test]
    fn fly_motor_ramp_follows_velocity_sign_reverse() {
        // 10 -> 0, v = -5: run-up sits at the HIGHER position, run-down LOWER.
        let info = FlyMotorInfo {
            start_position: 10.0,
            end_position: 0.0,
            time_for_move: 2.0,
            timeout: None,
        };
        let t = Duration::from_secs(1);
        assert_eq!(info.ramp_up_start_pos(t), 12.5); // 10 - 1*(-5)/2
        assert_eq!(info.ramp_down_end_pos(t), -2.5); // 0 + 1*(-5)/2
    }
}
