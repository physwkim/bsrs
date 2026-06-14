//! `Signal<T, B, A>` — generic over `SignalBackend<T>` and an access role `A`.
//!
//! The access role ([`Read`] / [`Write`] / [`ReadWrite`]) is encoded in the
//! type, mirroring ophyd-async's SignalR / SignalW / SignalRW split
//! (`core/_signal.py:189,276,305`): a read-only signal has no `put`, and a
//! write-only signal has no `get` / `read` / `subscribe`, enforced at compile
//! time. `A` defaults to [`ReadWrite`] so `Signal<T, B>` keeps the full
//! surface and existing code is unaffected.

use async_trait::async_trait;
use cirrus_core::error::Result;
use cirrus_core::reading::ReadingValue;
use cirrus_core::status::{Status, StatusError, SubToken};
use cirrus_core::Kind;
use cirrus_event_model::DataKey;
use cirrus_protocols_async::{
    AsyncReadable, AsyncSubscribable, ReadingValueCallback, SignalBackend, Subscription,
};
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

/// Re-export so users don't have to depend on cirrus-core directly.
pub use cirrus_core::Kind as SignalKind;

/// Type-state markers for a [`Signal`]'s access role.
///
/// The set is sealed to [`Read`], [`Write`], and [`ReadWrite`] so downstream
/// code cannot invent roles that would break the read/write gating.
pub mod access {
    mod sealed {
        pub trait Sealed {}
    }

    /// Access role of a signal (sealed: [`Read`] / [`Write`] / [`ReadWrite`]).
    pub trait Access: sealed::Sealed + Send + Sync + 'static {}
    /// Roles that permit reading / monitoring the current value.
    pub trait Readable: Access {}
    /// Roles that permit writing (putting) a value.
    pub trait Writable: Access {}

    /// Read-only access: monitor + `get` / `read` / `describe` / `subscribe`.
    pub struct Read;
    /// Write-only access: `put` only.
    pub struct Write;
    /// Read + write access — the default for `Signal<T, B>`.
    pub struct ReadWrite;

    impl sealed::Sealed for Read {}
    impl sealed::Sealed for Write {}
    impl sealed::Sealed for ReadWrite {}
    impl Access for Read {}
    impl Access for Write {}
    impl Access for ReadWrite {}
    impl Readable for Read {}
    impl Readable for ReadWrite {}
    impl Writable for Write {}
    impl Writable for ReadWrite {}
}

pub use access::{Access, Read, ReadWrite, Readable, Writable, Write};

/// Read-only signal: monitor + `get` / `read` / `describe` / `subscribe`,
/// no `put`. Mirrors ophyd-async `SignalR`.
pub type SignalR<T, B> = Signal<T, B, Read>;
/// Write-only signal: `put` only. Mirrors ophyd-async `SignalW`.
pub type SignalW<T, B> = Signal<T, B, Write>;
/// Read + write signal: the full surface (the default for `Signal<T, B>`).
/// Mirrors ophyd-async `SignalRW`.
pub type SignalRW<T, B> = Signal<T, B, ReadWrite>;

/// Per-signal configuration (PV name + kind + units).
#[derive(Clone, Debug, Default)]
pub struct SignalConfig {
    /// PV/source name.
    pub source: String,
    /// Kind (Normal/Config/Hinted/Omitted).
    pub kind: Kind,
    /// Human-friendly name appearing in `Reading` keys.
    pub name: String,
}

/// A signal: name + backend + kind, parameterized by access role `A`
/// ([`Read`] / [`Write`] / [`ReadWrite`]). `A` defaults to [`ReadWrite`], so
/// `Signal<T, B>` keeps the full read+write surface; use the [`SignalR`] /
/// [`SignalW`] / [`SignalRW`] aliases to restrict access at the type level.
pub struct Signal<T, B: SignalBackend<T>, A: Access = ReadWrite>
where
    T: Clone + Send + Sync + 'static,
{
    backend: Arc<B>,
    config: SignalConfig,
    _marker: std::marker::PhantomData<(T, A)>,
}

// -- Available for every access role ----------------------------------------

impl<T, B, A> Signal<T, B, A>
where
    T: Clone + Send + Sync + Serialize + 'static,
    B: SignalBackend<T>,
    A: Access,
{
    /// Build a fresh `Signal`.
    pub fn new(backend: Arc<B>, config: SignalConfig) -> Self {
        Self {
            backend,
            config,
            _marker: std::marker::PhantomData,
        }
    }

    /// Connect the underlying backend.
    pub async fn connect(&self, timeout: Duration) -> Result<()> {
        self.backend.connect(timeout).await
    }

    /// Get the kind.
    pub fn kind(&self) -> Kind {
        self.config.kind
    }

    /// Get the human-friendly name.
    pub fn name(&self) -> &str {
        &self.config.name
    }
}

// -- Readable roles only ([`Read`], [`ReadWrite`]) ---------------------------

impl<T, B, A> Signal<T, B, A>
where
    T: Clone + Send + Sync + Serialize + 'static,
    B: SignalBackend<T>,
    A: Readable,
{
    /// Read the typed value.
    pub async fn get(&self) -> Result<T> {
        self.backend.get_value().await
    }

    /// Read a `(key, ReadingValue)` map containing this one signal.
    pub async fn read(&self) -> Result<HashMap<String, ReadingValue>> {
        let r = self.backend.get_reading().await?;
        let mut out = HashMap::new();
        out.insert(self.config.name.clone(), r);
        Ok(out)
    }

    /// Describe this one signal as a `(key, DataKey)` map.
    pub async fn describe(&self) -> Result<HashMap<String, DataKey>> {
        let mut dk = self.backend.get_datakey(&self.config.source).await?;
        // Annotate the source if the backend left it blank.
        if dk.source.is_empty() {
            dk.source = self.backend.source(&self.config.source);
        }
        let mut out = HashMap::new();
        out.insert(self.config.name.clone(), dk);
        Ok(out)
    }

    /// Subscribe to value changes.
    pub fn subscribe(&self, cb: ReadingValueCallback<T>) -> SubToken {
        self.backend.set_callback(Some(cb))
    }
}

// -- Writable roles only ([`Write`], [`ReadWrite`]) --------------------------

impl<T, B, A> Signal<T, B, A>
where
    T: Clone + Send + Sync + Serialize + 'static,
    B: SignalBackend<T>,
    A: Writable,
{
    /// Put a value, awaiting completion. Returns a resolved `Status`
    /// reflecting success or failure. The backend `put` always waits for
    /// completion (CP-11); a per-call timeout, when needed, is applied by
    /// the caller (mirrors `SignalW::set(value, timeout)`).
    pub async fn put(&self, value: T) -> Status {
        match self.backend.put(Some(value)).await {
            Ok(()) => Status::done(),
            Err(e) => Status::fail(StatusError::Failed(e.to_string())),
        }
    }
}

// -- Read + write only: setpoint readback feeds `locate` ([`ReadWrite`]) -----

impl<T, B, A> Signal<T, B, A>
where
    T: Clone + Send + Sync + Serialize + 'static,
    B: SignalBackend<T>,
    A: Readable + Writable,
{
    /// Get the most recent setpoint. Available only on read+write signals
    /// (matches ophyd-async, where setpoint readback is part of
    /// `SignalRW::locate`).
    pub async fn get_setpoint(&self) -> Result<T> {
        self.backend.get_setpoint().await
    }
}

#[async_trait]
impl<T, B, A> AsyncReadable for Signal<T, B, A>
where
    T: Clone + Send + Sync + Serialize + 'static,
    B: SignalBackend<T>,
    A: Readable,
{
    fn name(&self) -> &str {
        &self.config.name
    }
    async fn read(&self) -> Result<HashMap<String, ReadingValue>> {
        self.read().await
    }
    async fn describe(&self) -> Result<HashMap<String, DataKey>> {
        self.describe().await
    }
}

#[async_trait]
impl<T, B, A> AsyncSubscribable<T> for Signal<T, B, A>
where
    T: Clone + Send + Sync + Serialize + 'static,
    B: SignalBackend<T>,
    A: Readable,
{
    fn name(&self) -> &str {
        &self.config.name
    }
    async fn subscribe(&self) -> Result<Subscription> {
        let (tx, rx) = watch::channel(ReadingValue {
            value: Value::Null,
            timestamp: 0.0,
            alarm_severity: None,
            message: None,
        });
        let tx = Arc::new(tx);
        let cb: ReadingValueCallback<T> = {
            let tx = tx.clone();
            Box::new(move |v: &T, ts: f64| {
                if let Ok(json) = serde_json::to_value(v) {
                    let _ = tx.send(ReadingValue {
                        value: json,
                        timestamp: ts,
                        alarm_severity: None,
                        message: None,
                    });
                }
            })
        };
        // K2: SubToken lives inside Subscription. Drop of Subscription removes
        // the backend slot via the token's Drop impl.
        let token = self.backend.set_callback(Some(cb));
        Ok(Subscription::new(rx, token))
    }
}

// -- ReadableObj impl so `Msg::Read(signal.into())` works in plans -----------

#[async_trait]
impl<T, B, A> cirrus_core::msg::NamedObj for Signal<T, B, A>
where
    T: Clone + Send + Sync + Serialize + 'static,
    B: SignalBackend<T>,
    A: Access,
{
    fn name(&self) -> &str {
        &self.config.name
    }
    fn inspect_dyn(&self) -> serde_json::Value {
        serde_json::json!({
            "name": self.config.name,
            "type": "Signal",
            "source": self.config.source,
            "kind": format!("{:?}", self.config.kind),
        })
    }
}

#[async_trait]
impl<T, B, A> cirrus_core::msg::ReadableObj for Signal<T, B, A>
where
    T: Clone + Send + Sync + Serialize + 'static,
    B: SignalBackend<T>,
    A: Readable,
{
    async fn read_dyn(&self) -> Result<HashMap<String, ReadingValue>> {
        self.read().await
    }
    async fn describe_dyn(&self) -> Result<HashMap<String, DataKey>> {
        self.describe().await
    }
    fn hint_fields(&self) -> Option<Vec<String>> {
        if matches!(self.config.kind, Kind::Hinted) {
            Some(vec![self.config.name.clone()])
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The access role gates the method surface at compile time: this test
    // exercises that `SignalR` reads, `SignalW` writes, and `SignalRW` does
    // both. (A `SignalR::put` / `SignalW::get` call would not compile.)
    #[tokio::test]
    async fn access_roles_expose_the_right_surface() {
        use cirrus_event_model::Dtype;

        fn cfg(name: &str) -> SignalConfig {
            SignalConfig {
                source: name.into(),
                kind: Kind::Normal,
                name: name.into(),
            }
        }

        let backend = || {
            Arc::new(cirrus_backend_soft::SoftSignalBackend::new(
                0.0_f64,
                Dtype::Number,
            ))
        };

        let r: SignalR<f64, _> = Signal::new(backend(), cfg("ro"));
        let w: SignalW<f64, _> = Signal::new(backend(), cfg("wo"));
        let rw: SignalRW<f64, _> = Signal::new(backend(), cfg("rw"));

        // Read-only: get works.
        assert_eq!(r.get().await.unwrap(), 0.0);
        // Write-only: put works (await the returned Status to completion).
        assert!(w.put(1.0).await.await.is_ok());
        // Read+write: both, plus setpoint readback.
        assert!(rw.put(2.0).await.await.is_ok());
        assert_eq!(rw.get().await.unwrap(), 2.0);
        assert_eq!(rw.get_setpoint().await.unwrap(), 2.0);
    }
}
