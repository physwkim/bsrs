//! `Msg` — the typed message that plans yield to the RunEngine.
//!
//! See `bluesky/src/bluesky/run_engine.py:_command_registry` for the reference
//! command set.

use serde_json::Value;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// Group identifier used to tie multiple Statuses together for `Wait`.
pub type GroupId = String;

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

    /// Custom user command.
    Custom {
        /// Command name.
        name: &'static str,
        /// Opaque payload.
        payload: Box<dyn Any + Send + Sync>,
    },

    /// No-op message — useful for spinning the loop.
    Null,
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
            Msg::Custom { name, .. } => write!(f, "Custom({name})"),
            Msg::Null => write!(f, "Null"),
        }
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
}

/// Anything that can be subscribed to (monitor stream).
#[async_trait::async_trait]
pub trait MonitorableObj: NamedObj {
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
