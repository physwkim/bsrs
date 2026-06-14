//! `Msg` — the typed message that plans yield to the RunEngine.
//!
//! See `bluesky/src/bluesky/run_engine.py:_command_registry` for the reference
//! command set.

use futures::future::BoxFuture;
use serde_json::Value;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// Group identifier used to tie multiple Statuses together for `Wait`.
pub type GroupId = String;

/// Callback type for `Msg::Subscribe` — receives every emitted
/// `Document`. Mirrors bluesky's positional `subs` callback.
pub type SubscribeCallback = Arc<dyn Fn(&cirrus_event_model::Document) + Send + Sync + 'static>;

/// Run-level metadata attached to `OpenRun`.
#[derive(Clone, Debug, Default)]
pub struct RunMetadata {
    /// Optional user-supplied scan ID.
    pub scan_id: Option<u64>,
    /// Free-form metadata.
    pub extra: HashMap<String, Value>,
    /// Optional plan name (for human-readable logs).
    pub plan_name: Option<String>,
}

/// Arguments passed to `Configure`.
#[derive(Clone, Debug, Default)]
pub struct ConfigureArgs {
    /// Configuration values to apply.
    pub values: HashMap<String, Value>,
}

/// The complete set of commands that plans can issue. Closed enum + `Custom`.
#[non_exhaustive]
pub enum Msg {
    /// Open a new run; engine emits `RunStart`.
    OpenRun(RunMetadata),
    /// Close the current run; engine emits `RunStop`.
    CloseRun {
        /// Final exit_status (`success` / `abort` / `fail`).
        exit_status: String,
        /// Optional reason string.
        reason: Option<String>,
    },

    /// Open a new event bundle for a stream.
    Create {
        /// Stream name (e.g. `primary`).
        stream_name: String,
    },
    /// Save the open bundle as one or more `Event` documents.
    Save,
    /// Discard the open bundle.
    Drop,
    /// Pre-declare a stream (for fly scans without `Read`+`Save`).
    DeclareStream {
        /// Stream name.
        stream_name: String,
        /// Pre-declared data keys.
        data_keys: HashMap<String, cirrus_event_model::DataKey>,
    },

    /// Read all signals on `obj` into the open bundle.
    Read(Arc<dyn ReadableObj>),
    /// Set a movable to a value.
    Set {
        /// Target device.
        obj: Arc<dyn MovableObj>,
        /// Setpoint.
        value: f64,
        /// Optional group for `Wait`.
        group: Option<GroupId>,
    },
    /// Trigger a detector.
    Trigger {
        /// Triggerable device.
        obj: Arc<dyn TriggerableObj>,
        /// Optional group for `Wait`.
        group: Option<GroupId>,
    },

    /// Stage a device.
    Stage(Arc<dyn StageableObj>),
    /// Unstage a device.
    Unstage(Arc<dyn StageableObj>),

    /// Stop a `Stoppable` device. `success=true` is a planned stop;
    /// `success=false` is an emergency stop (device may take defensive action).
    Stop {
        /// Stoppable device.
        obj: Arc<dyn StoppableObj>,
        /// Whether the stop is part of a normal plan (`true`) or emergency
        /// (`false`).
        success: bool,
    },

    /// Begin a fly-scan acquisition.
    Kickoff {
        /// Flyable device.
        obj: Arc<dyn FlyableObj>,
        /// Optional group.
        group: Option<GroupId>,
    },
    /// Wait for a fly-scan to finish (or signal that it should).
    Complete {
        /// Flyable device.
        obj: Arc<dyn FlyableObj>,
        /// Optional group.
        group: Option<GroupId>,
    },
    /// Collect documents from a flying detector.
    Collect {
        /// Collectable device.
        obj: Arc<dyn CollectableObj>,
        /// Optional stream name override.
        stream_name: Option<String>,
    },

    /// Read setpoint + readback from a `LocatableObj`. The result lands
    /// in the engine's `MsgResult::Location` slot and is surfaced back
    /// to the plan stream's caller (e.g. the Lua coroutine bridge) on
    /// the next `Plan::next` poll.
    Locate(Arc<dyn LocatableObj>),

    /// Subscribe a device's monitor stream.
    Monitor {
        /// Device.
        obj: Arc<dyn MonitorableObj>,
        /// Optional stream name.
        name: Option<String>,
    },
    /// Unsubscribe a device's monitor stream.
    Unmonitor(Arc<dyn MonitorableObj>),

    /// Wait for all Statuses in `group` to complete.
    Wait {
        /// Group to wait on.
        group: GroupId,
        /// Whether to error if one Status fails.
        error_on_timeout: bool,
        /// Optional timeout.
        timeout: Option<Duration>,
    },

    /// Sleep for a duration.
    Sleep(Duration),

    /// Mark a checkpoint at this point in the plan.
    Checkpoint,
    /// Drop the checkpoint cache (no-rewind region begins).
    ClearCheckpoint,

    /// Request a pause (block until `resume()` is called).
    Pause {
        /// If true, defer to the next checkpoint.
        defer: bool,
    },

    /// Configure a device (slow-changing fields).
    Configure {
        /// Target device.
        obj: Arc<dyn ConfigurableObj>,
        /// Configuration arguments.
        args: ConfigureArgs,
    },

    /// Prepare a `Preparable` device for a step or fly scan.
    /// Mirrors bluesky `Msg('prepare', flyer_object, value)`. The
    /// resulting `Status` is added to the named wait group like
    /// `Set` / `Kickoff` / `Complete`.
    Prepare {
        /// Target device.
        obj: Arc<dyn PreparableObj>,
        /// Initial state / per-scan parameters. Opaque to the engine;
        /// the device's `prepare_dyn` interprets it.
        value: Value,
        /// Optional group for `Wait`.
        group: Option<GroupId>,
    },

    /// Wait for arbitrary `Future`s — the cirrus equivalent of
    /// bluesky's `Msg('wait_for', None, awaitable_factories)`. The
    /// factories are invoked each time the message is processed, so
    /// the message can be re-issued during rewind.
    WaitFor {
        /// Awaitable factories. Each factory produces a fresh future
        /// on every call.
        factories: Vec<Arc<dyn Fn() -> BoxFuture<'static, crate::error::Result<()>> + Send + Sync>>,
        /// Optional timeout. If exceeded, the engine returns
        /// `CirrusError::Timeout`.
        timeout: Option<Duration>,
    },

    /// Register a device for pause/resume hooks. The engine calls
    /// `PausableObj::pause_dyn` on every registered device when it
    /// enters the pause gate, and `resume_dyn` on resume.
    RegisterPausable(Arc<dyn PausableObj>),
    /// Remove a previously-registered Pausable device.
    UnregisterPausable(Arc<dyn PausableObj>),

    /// Read a line of user input. The engine routes the `prompt`
    /// through its configured input handler (see
    /// `RunEngine::set_input_handler`); without a handler the run
    /// fails with `CirrusError::Plan`.
    Input {
        /// Prompt to display.
        prompt: String,
    },

    /// Surface the engine type name as a `MsgResult::EngineClass`.
    /// Mirrors bluesky's `Msg('RE_class')`. Useful for plans that
    /// need to introspect whether they are running under cirrus or
    /// the legacy bluesky RunEngine.
    ReClass,

    /// Add a temporary Document subscriber. Returned id lands in
    /// `MsgResult::SubscriptionId`. Mirrors bluesky's
    /// `Msg('subscribe', None, callback, name)`. Subscribers added
    /// this way are auto-removed at run end.
    Subscribe(SubscribeCallback),
    /// Remove a subscriber by id (the value previously returned via
    /// `MsgResult::SubscriptionId`).
    Unsubscribe(u64),

    /// Abort the plan stream cleanly: the engine fails the current run
    /// with `exit_status="fail"` and `reason = <message>`. Designed for
    /// plan-internal abort paths (e.g. a `mvr` that lost its motor
    /// connection mid-locate) so they can fail the run without
    /// panicking the async task.
    Fail(String),

    /// Resume after a deferred pause / suspend.
    Resume,

    /// Install a suspender. The boxed object is opaque to the engine; the
    /// suspender registry lives in `cirrus-engine`.
    InstallSuspender {
        /// Stable identifier for later removal.
        id: u64,
        /// Opaque suspender object.
        suspender: Arc<dyn Any + Send + Sync>,
    },

    /// Remove a previously-installed suspender.
    RemoveSuspender {
        /// Identifier returned by `InstallSuspender`.
        id: u64,
    },

    /// Set whether the current region is rewindable. Mirrors bluesky's
    /// `Msg('rewindable', None, bool)`.
    Rewindable(bool),

    /// Custom user command. Dispatched via `RunEngine::register_command`.
    Custom {
        /// Command name.
        name: &'static str,
        /// Opaque payload.
        payload: Box<dyn Any + Send + Sync>,
    },

    /// Publish a pre-built `Document` directly through the engine's sinks
    /// and dynamic subscribers. Escape hatch for detector writers and
    /// other producers of `Resource`, `Datum`, `StreamResource`,
    /// `StreamDatum`, `EventPage`, or `DatumPage` documents that the
    /// standard `Read` / `Save` / `Collect` path does not construct.
    Publish(Box<cirrus_event_model::Document>),

    /// No-op message — useful for spinning the loop.
    Null,
}

impl Msg {
    /// Whether this message should be added to the rewind cache when the
    /// engine is in a rewindable region (between `Checkpoint` and the next
    /// `ClearCheckpoint` / non-rewindable command).
    ///
    /// Mirrors bluesky's `UNCACHEABLE_COMMANDS` set
    /// (`run_engine.py` ≈ `_UNCACHEABLE_COMMANDS`).
    pub fn is_cacheable(&self) -> bool {
        !matches!(
            self,
            Msg::OpenRun(_)
                | Msg::CloseRun { .. }
                | Msg::Wait { .. }
                | Msg::Pause { .. }
                | Msg::Resume
                | Msg::Checkpoint
                | Msg::ClearCheckpoint
                | Msg::Configure { .. }
                | Msg::Monitor { .. }
                | Msg::Unmonitor(_)
                | Msg::InstallSuspender { .. }
                | Msg::RemoveSuspender { .. }
                | Msg::RegisterPausable(_)
                | Msg::UnregisterPausable(_)
                | Msg::Rewindable(_)
                | Msg::Custom { .. }
                | Msg::Publish(_)
                | Msg::Locate(_)
                | Msg::Input { .. }
                | Msg::ReClass
                | Msg::Subscribe(_)
                | Msg::Unsubscribe(_)
                | Msg::Fail(_)
                | Msg::Null
        )
    }

    /// `Some(name)` if the message targets a named device that should
    /// be tracked across the run for cleanup. Used by the engine to
    /// build `objs_seen` / `movable_objs_touched` registers.
    pub fn obj_name(&self) -> Option<&str> {
        match self {
            Msg::Read(o) => Some(o.name()),
            Msg::Set { obj, .. } => Some(obj.name()),
            Msg::Trigger { obj, .. } => Some(obj.name()),
            Msg::Locate(o) => Some(o.name()),
            Msg::Stage(o) => Some(o.name()),
            Msg::Unstage(o) => Some(o.name()),
            Msg::Stop { obj, .. } => Some(obj.name()),
            Msg::Kickoff { obj, .. } => Some(obj.name()),
            Msg::Complete { obj, .. } => Some(obj.name()),
            Msg::Collect { obj, .. } => Some(obj.name()),
            Msg::Monitor { obj, .. } => Some(obj.name()),
            Msg::Unmonitor(o) => Some(o.name()),
            Msg::Configure { obj, .. } => Some(obj.name()),
            Msg::Prepare { obj, .. } => Some(obj.name()),
            Msg::RegisterPausable(o) => Some(o.name()),
            Msg::UnregisterPausable(o) => Some(o.name()),
            _ => None,
        }
    }
}

impl Clone for Msg {
    fn clone(&self) -> Self {
        match self {
            Msg::OpenRun(m) => Msg::OpenRun(m.clone()),
            Msg::CloseRun {
                exit_status,
                reason,
            } => Msg::CloseRun {
                exit_status: exit_status.clone(),
                reason: reason.clone(),
            },
            Msg::Create { stream_name } => Msg::Create {
                stream_name: stream_name.clone(),
            },
            Msg::Save => Msg::Save,
            Msg::Drop => Msg::Drop,
            Msg::DeclareStream {
                stream_name,
                data_keys,
            } => Msg::DeclareStream {
                stream_name: stream_name.clone(),
                data_keys: data_keys.clone(),
            },
            Msg::Read(o) => Msg::Read(o.clone()),
            Msg::Set { obj, value, group } => Msg::Set {
                obj: obj.clone(),
                value: *value,
                group: group.clone(),
            },
            Msg::Trigger { obj, group } => Msg::Trigger {
                obj: obj.clone(),
                group: group.clone(),
            },
            Msg::Stage(o) => Msg::Stage(o.clone()),
            Msg::Unstage(o) => Msg::Unstage(o.clone()),
            Msg::Stop { obj, success } => Msg::Stop {
                obj: obj.clone(),
                success: *success,
            },
            Msg::Kickoff { obj, group } => Msg::Kickoff {
                obj: obj.clone(),
                group: group.clone(),
            },
            Msg::Complete { obj, group } => Msg::Complete {
                obj: obj.clone(),
                group: group.clone(),
            },
            Msg::Collect { obj, stream_name } => Msg::Collect {
                obj: obj.clone(),
                stream_name: stream_name.clone(),
            },
            Msg::Monitor { obj, name } => Msg::Monitor {
                obj: obj.clone(),
                name: name.clone(),
            },
            Msg::Unmonitor(o) => Msg::Unmonitor(o.clone()),
            Msg::Wait {
                group,
                error_on_timeout,
                timeout,
            } => Msg::Wait {
                group: group.clone(),
                error_on_timeout: *error_on_timeout,
                timeout: *timeout,
            },
            Msg::Sleep(d) => Msg::Sleep(*d),
            Msg::Checkpoint => Msg::Checkpoint,
            Msg::ClearCheckpoint => Msg::ClearCheckpoint,
            Msg::Pause { defer } => Msg::Pause { defer: *defer },
            Msg::Configure { obj, args } => Msg::Configure {
                obj: obj.clone(),
                args: args.clone(),
            },
            Msg::Prepare { obj, value, group } => Msg::Prepare {
                obj: obj.clone(),
                value: value.clone(),
                group: group.clone(),
            },
            Msg::WaitFor { factories, timeout } => Msg::WaitFor {
                factories: factories.clone(),
                timeout: *timeout,
            },
            Msg::RegisterPausable(o) => Msg::RegisterPausable(o.clone()),
            Msg::UnregisterPausable(o) => Msg::UnregisterPausable(o.clone()),
            Msg::Input { prompt } => Msg::Input {
                prompt: prompt.clone(),
            },
            Msg::ReClass => Msg::ReClass,
            Msg::Subscribe(cb) => Msg::Subscribe(cb.clone()),
            Msg::Unsubscribe(id) => Msg::Unsubscribe(*id),
            Msg::Fail(reason) => Msg::Fail(reason.clone()),
            Msg::Resume => Msg::Resume,
            Msg::InstallSuspender { id, suspender } => Msg::InstallSuspender {
                id: *id,
                suspender: suspender.clone(),
            },
            Msg::RemoveSuspender { id } => Msg::RemoveSuspender { id: *id },
            Msg::Rewindable(b) => Msg::Rewindable(*b),
            // `Custom` carries `Box<dyn Any>` which has no clone bound; cloning
            // is never the right thing for these. We collapse to `Null` so
            // higher-level code can still operate safely; in practice
            // `is_cacheable()` returns false for `Custom`, so this branch is
            // unreachable from the rewind path.
            Msg::Custom { .. } => Msg::Null,
            Msg::Publish(d) => Msg::Publish(d.clone()),
            Msg::Locate(o) => Msg::Locate(o.clone()),
            Msg::Null => Msg::Null,
        }
    }
}

impl std::fmt::Debug for Msg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Msg::OpenRun(_) => write!(f, "OpenRun"),
            Msg::CloseRun { exit_status, .. } => write!(f, "CloseRun({exit_status})"),
            Msg::Create { stream_name } => write!(f, "Create({stream_name})"),
            Msg::Save => write!(f, "Save"),
            Msg::Drop => write!(f, "Drop"),
            Msg::DeclareStream { stream_name, .. } => write!(f, "DeclareStream({stream_name})"),
            Msg::Read(o) => write!(f, "Read({})", o.name()),
            Msg::Set { obj, value, .. } => write!(f, "Set({}, {value})", obj.name()),
            Msg::Trigger { obj, .. } => write!(f, "Trigger({})", obj.name()),
            Msg::Stage(o) => write!(f, "Stage({})", o.name()),
            Msg::Unstage(o) => write!(f, "Unstage({})", o.name()),
            Msg::Stop { obj, success } => write!(f, "Stop({}, success={success})", obj.name()),
            Msg::Kickoff { obj, .. } => write!(f, "Kickoff({})", obj.name()),
            Msg::Complete { obj, .. } => write!(f, "Complete({})", obj.name()),
            Msg::Collect { obj, .. } => write!(f, "Collect({})", obj.name()),
            Msg::Monitor { obj, .. } => write!(f, "Monitor({})", obj.name()),
            Msg::Unmonitor(o) => write!(f, "Unmonitor({})", o.name()),
            Msg::Wait { group, .. } => write!(f, "Wait({group})"),
            Msg::Sleep(d) => write!(f, "Sleep({d:?})"),
            Msg::Checkpoint => write!(f, "Checkpoint"),
            Msg::ClearCheckpoint => write!(f, "ClearCheckpoint"),
            Msg::Pause { defer } => write!(f, "Pause(defer={defer})"),
            Msg::Configure { obj, .. } => write!(f, "Configure({})", obj.name()),
            Msg::Prepare { obj, .. } => write!(f, "Prepare({})", obj.name()),
            Msg::WaitFor { factories, .. } => write!(f, "WaitFor(n={})", factories.len()),
            Msg::RegisterPausable(o) => write!(f, "RegisterPausable({})", o.name()),
            Msg::UnregisterPausable(o) => write!(f, "UnregisterPausable({})", o.name()),
            Msg::Input { prompt } => write!(f, "Input({prompt:?})"),
            Msg::ReClass => write!(f, "ReClass"),
            Msg::Subscribe(_) => write!(f, "Subscribe(<cb>)"),
            Msg::Unsubscribe(id) => write!(f, "Unsubscribe({id})"),
            Msg::Fail(reason) => write!(f, "Fail({reason:?})"),
            Msg::Resume => write!(f, "Resume"),
            Msg::InstallSuspender { id, .. } => write!(f, "InstallSuspender({id})"),
            Msg::RemoveSuspender { id } => write!(f, "RemoveSuspender({id})"),
            Msg::Rewindable(b) => write!(f, "Rewindable({b})"),
            Msg::Custom { name, .. } => write!(f, "Custom({name})"),
            Msg::Publish(d) => write!(f, "Publish({})", document_label(d)),
            Msg::Locate(o) => write!(f, "Locate({})", o.name()),
            Msg::Null => write!(f, "Null"),
        }
    }
}

fn document_label(d: &cirrus_event_model::Document) -> &'static str {
    use cirrus_event_model::Document::*;
    match d {
        Start(_) => "RunStart",
        Descriptor(_) => "EventDescriptor",
        Event(_) => "Event",
        EventPage(_) => "EventPage",
        Resource(_) => "Resource",
        Datum(_) => "Datum",
        DatumPage(_) => "DatumPage",
        StreamResource(_) => "StreamResource",
        StreamDatum(_) => "StreamDatum",
        Stop(_) => "RunStop",
    }
}

// --- Object trait aliases --------------------------------------------------
//
// These are intentionally object-safe and live here in cirrus-core so that the
// `Msg` enum does not depend on the protocols crate. The concrete protocol
// traits (`AsyncReadable`, `AsyncMovable`, ...) all extend these.

/// Anything with a name.
pub trait NamedObj: Send + Sync {
    /// Stable identifier for logs and error messages.
    fn name(&self) -> &str;

    /// Dump the device's current state as a JSON value, for
    /// debugging and operational inspection. The default returns
    /// `{ "name": <name>, "type": "Unknown" }`. Devices should
    /// override to surface readback / setpoint / connection state /
    /// pending status / error fields — whatever a debugger needs to
    /// answer "is this device healthy and what is it doing?".
    ///
    /// The shape is unspecified beyond the `name` and `type` fields;
    /// downstream consumers (`obj:inspect()` in Lua, `cirrus qs
    /// inspect <name>` over RPC) treat it as an opaque JSON object
    /// for display.
    ///
    /// This method is sync and best-effort: it must not block on I/O
    /// or hold heavy locks. For values requiring I/O (e.g. fresh
    /// readback from a real PV), prefer caching the last observed
    /// value rather than blocking the inspect call.
    fn inspect_dyn(&self) -> serde_json::Value {
        serde_json::json!({
            "name": self.name(),
            "type": "Unknown",
        })
    }
}

/// Anything that can be `read()`.
#[async_trait::async_trait]
pub trait ReadableObj: NamedObj {
    /// Read all signals into a JSON-shaped reading set.
    async fn read_dyn(
        &self,
    ) -> Result<HashMap<String, crate::reading::ReadingValue>, crate::error::CirrusError>;
    /// Describe each field.
    async fn describe_dyn(
        &self,
    ) -> Result<HashMap<String, cirrus_event_model::DataKey>, crate::error::CirrusError>;
    /// Hint contributions (object name → list of fields).
    fn hint_fields(&self) -> Option<Vec<String>> {
        None
    }
}

/// Anything that can be moved (set to a value).
#[async_trait::async_trait]
pub trait MovableObj: NamedObj {
    /// Move and return a `Status`.
    async fn set_dyn(&self, value: f64) -> crate::status::Status;
    /// Engine-side hook invoked on pause for every object that this
    /// run has set. Defaults to a no-op so existing impls keep
    /// working; overrides should delegate to the device's own stop
    /// path. Mirrors bluesky's `_stop_movable_objects` walk.
    async fn stop_on_pause(&self, success: bool) -> Result<(), crate::error::CirrusError> {
        let _ = success;
        Ok(())
    }
}

/// Setpoint + readback record returned by [`LocatableObj::locate_dyn`].
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DynLocation {
    /// Where the device was last requested to move.
    pub setpoint: f64,
    /// Where the device currently is.
    pub readback: f64,
}

/// Anything with a notion of "where it is" + "where it's going".
/// Required by `mvr` plan stub for relative motion.
#[async_trait::async_trait]
pub trait LocatableObj: MovableObj {
    /// Read setpoint + readback in one round-trip.
    async fn locate_dyn(&self) -> Result<DynLocation, crate::error::CirrusError>;
}

/// Anything that can be safely stopped (`Stoppable` from bluesky.protocols).
/// Engine dispatches `Msg::Stop` to this trait.
#[async_trait::async_trait]
pub trait StoppableObj: NamedObj {
    /// Safely stop the device. `success` mirrors `bluesky.protocols.Stoppable`.
    async fn stop_dyn(&self, success: bool) -> Result<(), crate::error::CirrusError>;
}

/// Anything that can be triggered.
#[async_trait::async_trait]
pub trait TriggerableObj: NamedObj {
    /// Trigger and return a `Status`.
    async fn trigger_dyn(&self) -> crate::status::Status;
}

/// Anything that can be staged/unstaged.
#[async_trait::async_trait]
pub trait StageableObj: NamedObj {
    /// Stage.
    async fn stage_dyn(&self) -> Result<(), crate::error::CirrusError>;
    /// Unstage.
    async fn unstage_dyn(&self) -> Result<(), crate::error::CirrusError>;
}

/// Anything that can be flown (kickoff/complete).
#[async_trait::async_trait]
pub trait FlyableObj: NamedObj {
    /// Begin acquisition.
    async fn kickoff_dyn(&self) -> crate::status::Status;
    /// Wait for completion.
    async fn complete_dyn(&self) -> crate::status::Status;
    /// Engine-side hook invoked on pause for every object that this
    /// run has kickoff'd. Default no-op; override on flyers that
    /// should stop their hardware when the engine pauses.
    async fn stop_on_pause(&self, success: bool) -> Result<(), crate::error::CirrusError> {
        let _ = success;
        Ok(())
    }
}

/// Anything that can be `prepare()`'d for a step / fly scan.
/// Object-safe analogue of [`crate::ext`]'s `Preparable` trait.
#[async_trait::async_trait]
pub trait PreparableObj: NamedObj {
    /// Prepare; returned `Status` resolves when the device is ready.
    async fn prepare_dyn(&self, value: Value) -> crate::status::Status;
}

/// Devices that want pause/resume hooks called on them by the engine.
/// Mirrors `bluesky.protocols.Pausable` — the engine walks every
/// registered `PausableObj` on pause (after `stop_on_pause` runs) and
/// on resume.
#[async_trait::async_trait]
pub trait PausableObj: NamedObj {
    /// Called after the engine enters the pause gate.
    async fn pause_dyn(&self) -> Result<(), crate::error::CirrusError>;
    /// Called just before the engine resumes the message loop.
    async fn resume_dyn(&self) -> Result<(), crate::error::CirrusError>;
}

/// Anything that can be collected (Flyable companion).
#[async_trait::async_trait]
pub trait CollectableObj: NamedObj {
    /// Describe the stream(s) this object will collect into.
    async fn describe_collect_dyn(
        &self,
    ) -> Result<
        HashMap<String, HashMap<String, cirrus_event_model::DataKey>>,
        crate::error::CirrusError,
    >;
    /// Yield events. Empty vec if nothing buffered.
    async fn collect_dyn(
        &self,
    ) -> Result<
        Vec<(String, HashMap<String, Value>, HashMap<String, f64>)>,
        crate::error::CirrusError,
    >;
    /// Drain stream-asset documents (`StreamResource` / `StreamDatum`) buffered
    /// since the last call, each `StreamDatum` stamped with `descriptor` — the
    /// EventDescriptor UID the engine just composed for this collect stream.
    /// The engine broadcasts the returned documents so consumers can link
    /// stream data back to the descriptor that describes its data keys
    /// (CBEM-13). Default yields none; asset-writing collectables (e.g.
    /// `StandardDetector`) override it.
    async fn collect_stream_docs_dyn(
        &self,
        _descriptor: &str,
    ) -> Result<Vec<cirrus_event_model::Document>, crate::error::CirrusError> {
        Ok(Vec::new())
    }
}

/// Anything that can be subscribed to (monitor stream). A monitorable
/// object is also `Readable`: the engine uses `describe_dyn` / `read_dyn`
/// to get the data keys for the monitor stream's `EventDescriptor`, and
/// to seed the first Event before any rx-side updates arrive.
#[async_trait::async_trait]
pub trait MonitorableObj: ReadableObj {
    /// Subscribe — engine receives a `Subscription` (rx + RAII token).
    async fn subscribe_dyn(
        &self,
    ) -> Result<crate::subscription::Subscription, crate::error::CirrusError>;
}

/// Anything that can be configured.
#[async_trait::async_trait]
pub trait ConfigurableObj: NamedObj {
    /// Read the current configuration.
    async fn read_configuration_dyn(
        &self,
    ) -> Result<HashMap<String, crate::reading::ReadingValue>, crate::error::CirrusError>;
    /// Describe configuration fields.
    async fn describe_configuration_dyn(
        &self,
    ) -> Result<HashMap<String, cirrus_event_model::DataKey>, crate::error::CirrusError>;
    /// Apply a configuration change.
    async fn configure_dyn(&self, args: ConfigureArgs) -> Result<(), crate::error::CirrusError>;
}
