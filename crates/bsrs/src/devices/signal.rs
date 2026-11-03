//! `Signal<T, B, A>` — generic over `SignalBackend<T>` and an access role `A`.
//!
//! The access role ([`Read`] / [`Write`] / [`ReadWrite`]) is encoded in the
//! type, mirroring ophyd-async's SignalR / SignalW / SignalRW split
//! (`core/_signal.py:189,276,305`): a read-only signal has no `put`, and a
//! write-only signal has no `get` / `read` / `subscribe`, enforced at compile
//! time. `A` defaults to [`ReadWrite`] so `Signal<T, B>` keeps the full
//! surface and existing code is unaffected.

use crate::core::error::Result;
use crate::core::reading::ReadingValue;
use crate::core::status::{Status, StatusError, SubToken};
use crate::core::Kind;
use crate::devices::device::ConnectOnce;
use crate::event_model::DataKey;
use crate::protocols_async::{
    AsyncMovable, AsyncReadable, AsyncSubscribable, Locatable, Location, PutCompletion,
    ReadingValueCallback, SignalBackend, Subscription, Triggerable,
};
use async_trait::async_trait;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// Re-export so users don't have to depend on bsrs-core directly.
pub use crate::core::Kind as SignalKind;

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
    cache: std::sync::Mutex<Option<Arc<crate::devices::signal_cache::SignalCache<T, B>>>>,
    connect: ConnectOnce,
    _marker: std::marker::PhantomData<(T, A)>,
}

/// `value` as ophyd-async's log lines show one (`str(value)`): a string
/// bare, anything else spelled as a Python literal.
pub(crate) fn shown(value: &impl Serialize) -> String {
    match serde_json::to_string(value) {
        Ok(json) => serde_json::from_str::<String>(&json).unwrap_or_else(|_| python_literal(&json)),
        Err(e) => format!("<{e}>"),
    }
}

/// Respell serde_json output token by token, so struct fields keep their
/// declared order (a `serde_json::Value` would sort them).
fn python_literal(json: &str) -> String {
    let mut out = String::with_capacity(json.len());
    let mut rest = json;
    while let Some(c) = rest.chars().next() {
        let (text, len) = match c {
            '"' => {
                let len = json_string_len(rest);
                let s = serde_json::from_str::<String>(&rest[..len]).unwrap_or_default();
                (python_repr(&s), len)
            }
            ':' | ',' => (format!("{c} "), 1),
            't' if rest.starts_with("true") => ("True".into(), 4),
            'f' if rest.starts_with("false") => ("False".into(), 5),
            'n' if rest.starts_with("null") => ("None".into(), 4),
            _ => (c.to_string(), c.len_utf8()),
        };
        out.push_str(&text);
        rest = &rest[len..];
    }
    out
}

/// Byte length of the JSON string literal `s` starts with.
fn json_string_len(s: &str) -> usize {
    let mut escaped = false;
    for (i, b) in s.bytes().enumerate().skip(1) {
        match b {
            _ if escaped => escaped = false,
            b'\\' => escaped = true,
            b'"' => return i + 1,
            _ => {}
        }
    }
    s.len()
}

/// Python `repr(s)`.
fn python_repr(s: &str) -> String {
    let quote = if s.contains('\'') && !s.contains('"') {
        '"'
    } else {
        '\''
    };
    let mut out = String::from(quote);
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c == quote => {
                out.push('\\');
                out.push(c);
            }
            c if c < ' ' || c == '\x7f' => out.push_str(&format!("\\x{:02x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push(quote);
    out
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
            connect: ConnectOnce::default(),
            _marker: std::marker::PhantomData,
        }
    }

    /// Get the kind.
    pub fn kind(&self) -> Kind {
        self.config.kind
    }

    /// Get the human-friendly name.
    pub fn name(&self) -> &str {
        &self.config.name
    }

    /// Transport source URI for this signal — the same string `describe`
    /// reports in [`DataKey.source`](crate::core::DataKey). Sync (no backend
    /// round-trip), so a device tree can be introspected via
    /// [`walk_signal_sources`](crate::devices::device::walk_signal_sources). Mirrors
    /// ophyd-async `Signal.source` (`core/_signal.py`).
    pub fn source(&self) -> String {
        self.backend.source(&self.config.source, true)
    }
}

impl<T, B, A> Signal<T, B, A>
where
    T: Clone + Send + Sync + Serialize + 'static,
    B: SignalBackend<T> + 'static,
    A: Access,
{
    /// Connect the backend. A connect still running, or one that succeeded,
    /// is joined instead of starting another; one that failed is retried
    /// (ophyd-async `Device.connect`).
    pub async fn connect(&self, timeout: Duration) -> Result<()> {
        self.join_connect(timeout, false).await
    }

    /// Start a new backend connect even if the last one succeeded
    /// (ophyd-async `connect(force_reconnect=True)`).
    pub async fn reconnect(&self, timeout: Duration) -> Result<()> {
        self.join_connect(timeout, true).await
    }

    async fn join_connect(&self, timeout: Duration, force: bool) -> Result<()> {
        self.connect
            .join(force, || {
                tracing::debug!("Connecting to {}", self.source());
                let backend = self.backend.clone();
                async move { backend.connect(timeout).await }
            })
            .await
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
        let value = self.backend.get_value().await?;
        tracing::debug!(
            "get_value() on source {} returned {}",
            self.source(),
            shown(&value)
        );
        Ok(value)
    }

    /// Get (or lazily create) the shared monitor cache for this signal (CP-08).
    fn cache(&self) -> Arc<crate::devices::signal_cache::SignalCache<T, B>> {
        let mut g = self.cache.lock().unwrap();
        if let Some(c) = g.as_ref() {
            return c.clone();
        }
        let c = crate::devices::signal_cache::SignalCache::new(
            self.backend.clone(),
            &self.config.source,
        );
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
        // `source()` owns DataKey.source: a backend that builds its datakey
        // through a wrapped one (mock over soft) formats it for the wrong
        // scheme, while ophyd-async passes `Signal.source` to `get_datakey`.
        dk.source = self.source();
        let mut out = HashMap::new();
        out.insert(self.config.name.clone(), dk);
        Ok(out)
    }

    /// Subscribe to value changes.
    pub fn subscribe(&self, cb: ReadingValueCallback<T>) -> SubToken {
        crate::devices::signal_cache::open_monitor(&*self.backend, &self.source(), cb)
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
        let (source, shown) = self.putting(&value);
        match self.backend.put(Some(value)).await {
            Ok(()) => {
                tracing::debug!("Successfully put value {shown} to backend at source {source}");
                Status::done()
            }
            Err(e) => Status::fail(StatusError::Failed(e.to_string())),
        }
    }

    /// Send a put and return its completion, logged as [`put`](Self::put)
    /// logs. The request is on the wire when this returns
    /// ([`SignalBackend::put_start`]); the completion has no deadline.
    pub async fn put_start(&self, value: T) -> Result<PutCompletion> {
        let (source, shown) = self.putting(&value);
        let done = self.backend.put_start(Some(value)).await?;
        Ok(Box::pin(async move {
            done.await?;
            tracing::debug!("Successfully put value {shown} to backend at source {source}");
            Ok(())
        }))
    }

    /// Log the put of `value` about to start; returns the write source and
    /// the value as shown, for the line logged once it completes.
    fn putting(&self, value: &T) -> (String, String) {
        let source = self.backend.source(&self.config.source, false);
        // Rendered only for DEBUG: a put may carry a whole waveform.
        let shown = if tracing::enabled!(tracing::Level::DEBUG) {
            shown(value)
        } else {
            String::new()
        };
        tracing::debug!("Putting value {shown} to backend at source {source}");
        (source, shown)
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

// -- Movable / Locatable: ophyd-async `SignalW.set`, `SignalRW.locate` --------

#[async_trait]
impl<T, B, A> AsyncMovable<T> for Signal<T, B, A>
where
    T: Clone + Send + Sync + Serialize + 'static,
    B: SignalBackend<T>,
    A: Writable,
{
    fn name(&self) -> &str {
        &self.config.name
    }
    async fn set(&self, value: T) -> Status {
        self.put(value).await
    }
}

#[async_trait]
impl<T, B, A> Locatable<T> for Signal<T, B, A>
where
    T: Clone + Send + Sync + Serialize + 'static,
    B: SignalBackend<T>,
    A: Readable + Writable,
{
    async fn locate(&self) -> Result<Location<T>> {
        let (setpoint, readback) =
            futures::try_join!(self.backend.get_setpoint(), self.backend.get_value())?;
        Ok(Location { setpoint, readback })
    }
}

#[async_trait]
impl<B, A> crate::core::msg::MovableObj for Signal<f64, B, A>
where
    B: SignalBackend<f64>,
    A: Writable,
{
    async fn set_dyn(&self, value: f64) -> Status {
        self.put(value).await
    }
}

#[async_trait]
impl<B, A> crate::core::msg::LocatableObj for Signal<f64, B, A>
where
    B: SignalBackend<f64>,
    A: Readable + Writable,
{
    async fn locate_dyn(&self) -> Result<crate::core::msg::DynLocation> {
        let Location { setpoint, readback } = Locatable::locate(self).await?;
        Ok(crate::core::msg::DynLocation { setpoint, readback })
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
        let source = self.backend.source(&self.config.source, false);
        tracing::debug!("Putting default value to backend at source {source}");
        match self.backend.put(None).await {
            Ok(()) => {
                tracing::debug!("Successfully put default value to backend at source {source}");
                Status::done()
            }
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
        Ok(Subscription::new(rx, token, self.config.name.clone()))
    }
}

// -- ReadableObj impl so `Msg::Read(signal.into())` works in plans -----------

#[async_trait]
impl<T, B, A> crate::core::msg::NamedObj for Signal<T, B, A>
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
impl<T, B, A> crate::core::msg::ReadableObj for Signal<T, B, A>
where
    T: Clone + Send + Sync + Serialize + 'static,
    B: SignalBackend<T> + 'static,
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
    fn as_stageable(self: Arc<Self>) -> Option<Arc<dyn crate::core::msg::StageableObj>> {
        Some(self)
    }
}

// -- MonitorableObj: `Msg::Monitor(signal.into())` streams the shared monitor --

#[async_trait]
impl<T, B, A> crate::core::msg::MonitorableObj for Signal<T, B, A>
where
    T: Clone + Send + Sync + Serialize + 'static,
    B: SignalBackend<T> + 'static,
    A: Readable,
{
    async fn subscribe_dyn(&self) -> Result<Subscription> {
        // Same path as `AsyncSubscribable::subscribe`: one shared backend
        // monitor (CP-08) whose slot is released when the engine drops the
        // `Subscription` on Unmonitor / pause / run close (K2).
        AsyncSubscribable::subscribe(self).await
    }
}

// -- Stageable: staging a readable signal holds its shared monitor (CP-08) ----

#[async_trait]
impl<T, B, A> crate::protocols_async::Stageable for Signal<T, B, A>
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
impl<T, B, A> crate::core::msg::StageableObj for Signal<T, B, A>
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
        fn cfg(name: &str) -> SignalConfig {
            SignalConfig {
                source: name.into(),
                kind: Kind::Normal,
                name: name.into(),
            }
        }

        let backend = || Arc::new(crate::backends::soft::SoftSignalBackend::new(0.0_f64));

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
        // Soft backend's `put(None)` writes the configured initial value
        // (`_soft_signal_backend.py:164`); seed a non-default initial so the
        // trigger is observable through a separate read handle.
        let backend = Arc::new(crate::backends::soft::SoftSignalBackend::new(42.0_f64));
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
        let backend = Arc::new(crate::backends::soft::SoftSignalBackend::new(1.0_f64));
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

    // A `Signal` is a `MonitorableObj`, so `Msg::Monitor` can carry it: the
    // dyn subscription rides the same shared monitor as `subscribe()`, sees
    // backend updates, and releases the backend slot when dropped (K2).
    #[tokio::test]
    async fn signal_is_monitorable_through_the_shared_monitor() {
        use crate::core::msg::MonitorableObj;

        let backend = Arc::new(crate::backends::soft::SoftSignalBackend::new(1.0_f64));
        let s: Arc<SignalR<f64, _>> = Arc::new(Signal::new(
            backend.clone(),
            SignalConfig {
                source: "x".into(),
                kind: Kind::Normal,
                name: "x".into(),
            },
        ));
        let mon: Arc<dyn MonitorableObj> = s.clone();

        let mut sub = mon.subscribe_dyn().await.unwrap();
        assert_eq!(sub.key(), "x", "keyed like describe()");
        let _sub2 = AsyncSubscribable::subscribe(&*s).await.unwrap();
        assert_eq!(
            backend.subscriber_count(),
            1,
            "dyn and typed subscriptions share one backend monitor"
        );

        backend.write_now(7.0);
        sub.rx_mut().changed().await.unwrap();
        assert_eq!(sub.rx().borrow().value, serde_json::json!(7.0));

        drop(sub);
        drop(_sub2);
        assert_eq!(
            backend.subscriber_count(),
            0,
            "last subscription dropped: monitor torn down"
        );
    }

    fn logged(name: &str) -> SignalConfig {
        SignalConfig {
            source: name.into(),
            kind: Kind::Normal,
            name: name.into(),
        }
    }

    // ophyd-async test_signal_connects_to_previous_backend and
    // test_signal_connects_with_force_reconnect: "Connecting to" is logged
    // per backend connect started, not per connect joined.
    #[tokio::test]
    async fn connect_is_logged_once_per_backend_connect() {
        let (caplog, _guard) = crate::caplog::Caplog::start();
        let backend = crate::backends::soft::SoftSignalBackend::new(0_i64);
        let sig: SignalRW<i64, _> = Signal::new(Arc::new(backend), logged("x"));
        let connecting = || caplog.text().matches("Connecting to soft://x").count();
        let t = Duration::from_secs(1);

        sig.connect(t).await.unwrap();
        assert_eq!(connecting(), 1);
        let (a, b, c) = tokio::join!(sig.connect(t), sig.connect(t), sig.connect(t));
        a.and(b).and(c).unwrap();
        assert_eq!(connecting(), 1, "joined connects are not logged");
        sig.reconnect(t).await.unwrap();
        assert_eq!(connecting(), 2);
    }

    // ophyd-async test_signal_get_and_set_logging, and SignalX.trigger's
    // put-default lines.
    #[tokio::test]
    async fn put_get_and_trigger_are_logged() {
        let (caplog, _guard) = crate::caplog::Caplog::start();
        let backend = crate::backends::mock::MockSignalBackend::with_value(0_i64);
        let sig: SignalRW<i64, _> = Signal::new(Arc::new(backend), logged("mock_signal"));
        sig.connect(Duration::from_secs(1)).await.unwrap();

        assert!(sig.put(0).await.await.is_ok());
        let text = caplog.text();
        assert!(text.contains("Putting value 0 to backend at source mock+soft://mock_signal"));
        assert!(
            text.contains("Successfully put value 0 to backend at source mock+soft://mock_signal")
        );
        sig.get().await.unwrap();
        assert!(caplog
            .text()
            .contains("get_value() on source mock+soft://mock_signal returned 0"));

        let x: SignalX<i64, _> = Signal::new(
            Arc::new(crate::backends::soft::SoftSignalBackend::new(0)),
            logged("proc"),
        );
        assert!(x.trigger().await.await.is_ok());
        let text = caplog.text();
        assert!(text.contains("Putting default value to backend at source soft://proc"));
        assert!(text.contains("Successfully put default value to backend at source soft://proc"));
    }

    // Log lines spell values as ophyd-async's do, Python `str(value)`.
    #[test]
    fn logged_values_are_spelled_as_python_str() {
        assert_eq!(shown(&"it's"), "it's");
        assert_eq!(shown(&true), "True");
        assert_eq!(shown(&None::<i64>), "None");
        assert_eq!(shown(&1.5), "1.5");
        assert_eq!(shown(&vec![1, 2]), "[1, 2]");
        assert_eq!(
            shown(&vec!["a", "it's", "\\\n"]),
            r#"['a', "it's", '\\\n']"#
        );
        let reading = ReadingValue {
            value: serde_json::json!(false),
            timestamp: 2.0,
            alarm_severity: Some(0),
            message: None,
        };
        assert_eq!(
            shown(&reading),
            "{'value': False, 'timestamp': 2.0, 'alarm_severity': 0}"
        );
    }

    // ophyd-async test_subscription_logs: subscribing opens the shared
    // monitor and dropping the last subscription closes it, each logged, as
    // is every reading the monitor delivers.
    #[tokio::test]
    async fn subscription_is_logged_from_opening_to_closing() {
        let (caplog, _guard) = crate::caplog::Caplog::start();
        let backend = Arc::new(crate::backends::soft::SoftSignalBackend::new(
            "a".to_string(),
        ));
        let sig: SignalR<String, _> = Signal::new(backend.clone(), logged("x"));

        let sub = AsyncSubscribable::subscribe(&sig).await.unwrap();
        assert!(caplog
            .text()
            .contains("Making subscription on source soft://x"));
        backend.write_now("b".to_string());
        let text = caplog.text();
        assert!(text.contains(
            "Updated subscription: reading of source soft://x changed from None to {'value': 'a', "
        ));
        assert!(text.contains(
            "Updated subscription: reading of source soft://x changed from {'value': 'a', "
        ));
        drop(sub);
        assert!(caplog
            .text()
            .contains("Closing subscription on source soft://x"));

        // A callback subscription opens and closes its own monitor.
        let token = sig.subscribe(Box::new(|_, _, _| {}));
        drop(token);
        let text = caplog.text();
        assert_eq!(
            text.matches("Making subscription on source soft://x")
                .count(),
            2
        );
        assert_eq!(
            text.matches("Closing subscription on source soft://x")
                .count(),
            2
        );
    }
}
