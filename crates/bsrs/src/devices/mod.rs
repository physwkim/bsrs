//! bsrs-devices — Signal, StandardDetector, and helpers for building Device
//! trees on top of `SignalBackend`.

#![deny(missing_docs)]

pub mod command;
pub mod detector;
pub mod device;
pub mod observe;
pub mod path_provider;
pub mod signal;
pub mod signal_cache;
pub mod stage_sigs;
pub mod standard_readable;

pub use command::{
    soft_command, Command, CommandBackend, MockCommandBackend, MockExecuteCallback,
    SoftCommandBackend, TriggerableCommand,
};
pub use detector::{DataLogic, DetectorTrigger, StandardDetector, TriggerInfo};
pub use device::{wait_for_connection, walk_signal_sources, Device, DeviceVector};
pub use observe::{observe_signals_value, observe_value, wait_for_value};
pub use path_provider::{
    AutoIncrementFilenameProvider, AutoIncrementingPathProvider, AutoMaxIncrementingPathProvider,
    FilenameProvider, PathInfo, PathProvider, PurePath, StaticFilenameProvider, StaticPathProvider,
    UuidFilenameProvider, YmdPathProvider,
};
pub use signal::{
    Access, Execute, Read, ReadWrite, Readable, Signal, SignalConfig, SignalKind, SignalR,
    SignalRW, SignalW, SignalX, Writable, Write,
};
pub use signal_cache::SignalCache;
pub use stage_sigs::{StageSetting, StageSigs};
pub use standard_readable::{StandardReadable, StandardReadableFormat};

/// Re-export of the `#[derive(Device)]` proc-macro. (The like-named [`Device`]
/// trait lives in a separate namespace, as with `serde::Serialize`.)
pub use bsrs_derive::Device;

/// Trait implemented by every `SignalBackend` that can be constructed from
/// just a PV name. Required by `#[derive(Device)]` to wire signals.
pub trait BackendFromPv: Sized {
    /// Build a backend instance from a PV / source identifier.
    fn from_pv(pv: &str) -> Self;
}

/// Internal helpers used by the `#[derive(Device)]` proc-macro. Not part of
/// the public API — name and shape may change.
#[doc(hidden)]
pub mod __derive {
    use super::{Access, BackendFromPv, Signal};
    use crate::core::error::Result;
    use crate::protocols_async::SignalBackend;
    use std::future::Future;
    use std::pin::Pin;
    use std::time::Duration;

    /// Substitute `{prefix}` in `template` with `prefix`.
    pub fn expand(template: &str, prefix: &str) -> String {
        template.replace("{prefix}", prefix)
    }

    /// Build the default backend instance for a given PV.
    pub fn default_backend<B: BackendFromPv>(pv: &str) -> B {
        B::from_pv(pv)
    }

    /// Connect a Signal — used by generated `connect_all` to homogenize
    /// future types for [`wait_for_connection`](super::wait_for_connection).
    pub fn connect_signal<'a, T, B, A>(
        sig: &'a Signal<T, B, A>,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>
    where
        T: Clone + Send + Sync + serde::Serialize + 'static,
        B: SignalBackend<T> + 'static,
        A: Access + 'static,
    {
        Box::pin(sig.connect(timeout))
    }
}
