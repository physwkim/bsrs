//! cirrus-devices — Signal, StandardDetector, and helpers for building Device
//! trees on top of `SignalBackend`.

#![deny(missing_docs)]

pub mod detector;
pub mod device;
pub mod observe;
pub mod signal;
pub mod standard_readable;

pub use detector::{StandardDetector, TriggerInfo};
pub use device::{Device, DeviceVector};
pub use observe::{observe_value, wait_for_value};
pub use signal::{
    Access, Execute, Read, ReadWrite, Readable, Signal, SignalConfig, SignalKind, SignalR,
    SignalRW, SignalW, SignalX, Writable, Write,
};
pub use standard_readable::{StandardReadable, StandardReadableFormat};

/// Re-export of the `#[derive(Device)]` proc-macro. (The like-named [`Device`]
/// trait lives in a separate namespace, as with `serde::Serialize`.)
pub use cirrus_derive::Device;

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
    use cirrus_core::error::Result;
    use cirrus_protocols_async::SignalBackend;
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
    /// future types so they can be `try_join`'d.
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

    /// `try_join_all` over heterogeneous boxed futures.
    pub async fn try_join_all_connects(
        futs: Vec<Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>>,
    ) -> Result<()> {
        let results = futures::future::join_all(futs).await;
        for r in results {
            r?;
        }
        Ok(())
    }
}
