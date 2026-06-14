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
    Triggerable,
};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

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
    /// Execute-only access: `trigger` only (no `get`/`read`/`put`). Mirrors
    /// ophyd-async `SignalX`, whose `trigger` writes the backend default via
    /// `put(None)` (`core/_signal.py:330`).
    pub struct Execute;

    impl sealed::Sealed for Read {}
    impl sealed::Sealed for Write {}
    impl sealed::Sealed for ReadWrite {}
    impl sealed::Sealed for Execute {}
    impl Access for Read {}
    impl Access for Write {}
    impl Access for ReadWrite {}
    impl Access for Execute {}
    impl Readable for Read {}
    impl Readable for ReadWrite {}
    impl Writable for Write {}
    impl Writable for ReadWrite {}
}

pub use access::{Access, Execute, Read, ReadWrite, Readable, Writable, Write};

/// Read-only signal: monitor + `get` / `read` / `describe` / `subscribe`,
/// no `put`. Mirrors ophyd-async `SignalR`.
pub type SignalR<T, B> = Signal<T, B, Read>;
/// Write-only signal: `put` only. Mirrors ophyd-async `SignalW`.
pub type SignalW<T, B> = Signal<T, B, Write>;
/// Read + write signal: the full surface (the default for `Signal<T, B>`).
/// Mirrors ophyd-async `SignalRW`.
pub type SignalRW<T, B> = Signal<T, B, ReadWrite>;
/// Execute signal: `trigger` only (writes the backend default via
/// `put(None)`). Mirrors ophyd-async `SignalX`.
pub type SignalX<T, B> = Signal<T, B, Execute>;

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
    /// Lazily created shared monitor (CP-08). `None` until the signal is first
    /// subscribed or staged; afterwards the cache fans one backend monitor out
    /// to every subscriber and holds the latest value.
    cache: std::sync::Mutex<Option<Arc<crate::signal_cache::SignalCache<T, B>>>>,
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
            cache: std::sync::Mutex::new(None),
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

    /// Get (or lazily create) the shared monitor cache for this signal (CP-08).
    fn cache(&self) -> Arc<crate::signal_cache::SignalCache<T, B>> {
        let mut g = self.cache.lock().unwrap();
        if let Some(c) = g.as_ref() {
            return c.clone();
        }
        let c = crate::signal_cache::SignalCache::new(self.backend.clone());
        *g = Some(c.clone());
        c
    }

    /// Latest cached reading if a cache exists and has seen a value.
    fn cache_snapshot(&self) -> Option<ReadingValue> {
        self.cache
            .lock()
            .unwrap()
            .as_ref()
            .and_then(|c| c.cached_reading())
    }

    /// Read a `(key, ReadingValue)` map containing this one signal, defaulting
    /// to the cached value when one is available (`cached = None`).
    pub async fn read(&self) -> Result<HashMap<String, ReadingValue>> {
        self.read_cached(None).await
    }

    /// Read with explicit cache control (mirrors ophyd-async `read(cached=)`):
    /// `Some(false)` always hits the backend; `Some(true)` / `None` return the
    /// cached value when present, else fall back to a backend read.
    pub async fn read_cached(&self, cached: Option<bool>) -> Result<HashMap<String, ReadingValue>> {
        let r = match cached {
            Some(false) => self.backend.get_reading().await?,
            _ => match self.cache_snapshot() {
                Some(rv) => rv,
                None => self.backend.get_reading().await?,
            },
        };
        let mut out = HashMap::new();
        out.insert(self.config.name.clone(), r);
        Ok(out)
    }

    /// Stage: open and hold the shared monitor across subscriber churn (CP-08).
    pub fn stage(&self) {
        self.cache().set_staged(true);
    }

    /// Unstage: release the staged hold; the monitor is torn down once no
    /// listeners remain.
    pub fn unstage(&self) {
        self.cache().set_staged(false);
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

// -- Execute role only ([`Execute`]) -----------------------------------------

impl<T, B> Signal<T, B, Execute>
where
    T: Clone + Send + Sync + Serialize + 'static,
    B: SignalBackend<T>,
{
    /// Trigger by writing the backend default (`put(None)`), awaiting
    /// completion. Returns a resolved `Status`. Mirrors ophyd-async
    /// `SignalX::trigger` (`core/_signal.py:330`), where `None` is the
    /// put-default sentinel (CP-11).
    pub async fn trigger(&self) -> Status {
        match self.backend.put(None).await {
            Ok(()) => Status::done(),
            Err(e) => Status::fail(StatusError::Failed(e.to_string())),
        }
    }
}

#[async_trait]
impl<T, B> Triggerable for Signal<T, B, Execute>
where
    T: Clone + Send + Sync + Serialize + 'static,
    B: SignalBackend<T>,
{
    fn name(&self) -> &str {
        &self.config.name
    }
    async fn trigger(&self) -> Status {
        self.trigger().await
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
        // CP-08: route through the shared cache so N subscribers demultiplex
        // one backend monitor instead of opening one each. K2: the returned
        // SubToken decrements the cache's listener count on Subscription drop
        // (and tears the monitor down if it was the last and unstaged).
        let (rx, token) = self.cache().add_listener();
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

// -- Stageable: staging a readable signal holds its shared monitor (CP-08) ----

#[async_trait]
impl<T, B, A> cirrus_protocols_async::Stageable for Signal<T, B, A>
where
    T: Clone + Send + Sync + Serialize + 'static,
    B: SignalBackend<T>,
    A: Readable,
{
    fn name(&self) -> &str {
        &self.config.name
    }
    async fn stage(&self) -> Result<()> {
        self.cache().set_staged(true);
        Ok(())
    }
    async fn unstage(&self) -> Result<()> {
        self.cache().set_staged(false);
        Ok(())
    }
}

#[async_trait]
impl<T, B, A> cirrus_core::msg::StageableObj for Signal<T, B, A>
where
    T: Clone + Send + Sync + Serialize + 'static,
    B: SignalBackend<T>,
    A: Readable,
{
    async fn stage_dyn(&self) -> Result<()> {
        self.cache().set_staged(true);
        Ok(())
    }
    async fn unstage_dyn(&self) -> Result<()> {
        self.cache().set_staged(false);
        Ok(())
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

    // SignalX (Execute role) exposes only `trigger`, which writes the
    // backend default via `put(None)`. (A `SignalX::get`/`put` call would
    // not compile.)
    #[tokio::test]
    async fn signal_x_trigger_writes_backend_default() {
        use cirrus_event_model::Dtype;

        // Soft backend's `put(None)` writes the configured initial value
        // (`_soft_signal_backend.py:164`); seed a non-default initial so the
        // trigger is observable through a separate read handle.
        let backend = Arc::new(cirrus_backend_soft::SoftSignalBackend::new(
            42.0_f64,
            Dtype::Number,
        ));
        let x: SignalX<f64, _> = Signal::new(
            backend.clone(),
            SignalConfig {
                source: "proc".into(),
                kind: Kind::Normal,
                name: "proc".into(),
            },
        );
        // Move the cell away from the initial, then trigger to restore it.
        SignalBackend::put(&*backend, Some(0.0)).await.unwrap();
        assert_eq!(backend.current_value(), 0.0);
        assert!(x.trigger().await.await.is_ok());
        assert_eq!(backend.current_value(), 42.0);

        // Triggerable trait object path resolves to the same behavior.
        let dynx: &dyn Triggerable = &x;
        assert_eq!(dynx.name(), "proc");
    }

    // CP-08: staging opens one shared backend monitor; multiple subscriptions
    // demultiplex it; cached reads return the monitored value; cached=false
    // forces a backend read; unstaging with no listeners tears the monitor down.
    #[tokio::test]
    async fn staged_signal_caches_reads_and_shares_one_monitor() {
        use cirrus_event_model::Dtype;

        let backend = Arc::new(cirrus_backend_soft::SoftSignalBackend::new(
            1.0_f64,
            Dtype::Number,
        ));
        let s: SignalR<f64, _> = Signal::new(
            backend.clone(),
            SignalConfig {
                source: "x".into(),
                kind: Kind::Normal,
                name: "x".into(),
            },
        );

        s.stage();
        assert_eq!(backend.subscriber_count(), 1, "stage opens the monitor");

        let sub1 = AsyncSubscribable::subscribe(&s).await.unwrap();
        let sub2 = AsyncSubscribable::subscribe(&s).await.unwrap();
        assert_eq!(
            backend.subscriber_count(),
            1,
            "subscriptions share the staged monitor"
        );

        backend.write_now(9.0);
        let cached = s.read_cached(Some(true)).await.unwrap();
        assert_eq!(cached["x"].value, serde_json::json!(9.0));
        let live = s.read_cached(Some(false)).await.unwrap();
        assert_eq!(live["x"].value, serde_json::json!(9.0));

        drop(sub1);
        drop(sub2);
        assert_eq!(
            backend.subscriber_count(),
            1,
            "still staged: monitor survives dropped subscriptions"
        );
        s.unstage();
        assert_eq!(
            backend.subscriber_count(),
            0,
            "unstaged + no listeners: torn down"
        );
    }
}
