//! `SignalCache<T, B>` — one shared backend monitor demultiplexed to N
//! listeners, mirroring ophyd-async `_SignalCache` (`core/_signal.py:116-186`).
//!
//! Without it, every `subscribe()` opens a fresh CA/PVA monitor and staging
//! does not persist a subscription. The cache fires `backend.set_callback`
//! exactly once and queues every value to each listener through a
//! [`Fanout`]; the monitor stays alive while the signal is staged or has at
//! least one listener.

use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

use crate::core::reading::ReadingValue;
use crate::core::status::SubToken;
use crate::core::subscription::Fanout;
use crate::devices::signal::shown;
use crate::protocols_async::{ReadingValueCallback, SignalBackend};
use serde::Serialize;
use tokio::sync::mpsc;

/// Lifecycle of the shared backend monitor. Mutated only through
/// [`SignalCache`] methods while holding the `state` lock (single owner).
///
/// INVARIANT: `token.is_some()` ⟺ `staged || listeners > 0` — the backend
/// monitor is alive exactly while the cache is staged or has ≥1 listener.
struct CacheState {
    staged: bool,
    listeners: usize,
    token: Option<SubToken>,
}

/// Open a monitor of `backend` delivering to `cb`, logging its opening and
/// closing as ophyd-async `_SignalCache` does; the monitor closes when the
/// returned token drops.
pub(crate) fn open_monitor<T, B>(backend: &B, source: &str, cb: ReadingValueCallback<T>) -> SubToken
where
    T: Clone + Send + Sync + 'static,
    B: SignalBackend<T>,
{
    tracing::debug!("Making subscription on source {source}");
    let monitor = backend.set_callback(Some(cb));
    let source = source.to_owned();
    SubToken::new(move || {
        drop(monitor);
        tracing::debug!("Closing subscription on source {source}");
    })
}

/// Shared monitor + latest-value cache for one signal.
pub struct SignalCache<T, B> {
    backend: Arc<B>,
    /// `backend.source(name, true)`, which the cache's logs name.
    source: Arc<str>,
    // Held apart from `CacheState` so the backend callback can capture it
    // without a reference cycle through the `SubToken` living in the state.
    fanout: Arc<Fanout<ReadingValue>>,
    // `Arc<Mutex<..>>` (not `Mutex<..>`) so the per-listener drop closure can
    // own a handle to the state + fanout without capturing `Arc<Self>` — that
    // would force `B: 'static` on every caller. Teardown touches neither the
    // backend nor `B`, so the closure stays `'static` for any `B`.
    state: Arc<Mutex<CacheState>>,
    _marker: PhantomData<fn() -> T>,
}

impl<T, B> SignalCache<T, B>
where
    T: Clone + Send + Sync + Serialize + 'static,
    B: SignalBackend<T>,
{
    /// Build a cache over `backend` for the signal `name` (the name a
    /// [`Signal`](crate::devices::Signal) passes its backend: the PV). No
    /// backend monitor is opened until the cache is staged or gains a
    /// listener.
    pub fn new(backend: Arc<B>, name: &str) -> Arc<Self> {
        Arc::new(Self {
            source: backend.source(name, true).into(),
            backend,
            fanout: Arc::default(),
            state: Arc::new(Mutex::new(CacheState {
                staged: false,
                listeners: 0,
                token: None,
            })),
            _marker: PhantomData,
        })
    }

    fn make_callback(&self) -> ReadingValueCallback<T> {
        let fanout = self.fanout.clone();
        let source = self.source.clone();
        Box::new(move |v: &T, ts: f64, alarm_severity: Option<i32>| {
            if let Ok(json) = serde_json::to_value(v) {
                let reading = ReadingValue {
                    value: json,
                    timestamp: ts,
                    alarm_severity,
                    message: None,
                };
                let prev = fanout.post(reading.clone());
                tracing::debug!(
                    "Updated subscription: reading of source {source} changed from {} to {}",
                    prev.as_ref().map_or_else(|| "None".into(), shown),
                    shown(&reading),
                );
            }
        })
    }

    /// Open the backend monitor if not already open. Caller holds `state`.
    fn ensure_token(&self, st: &mut CacheState) {
        if st.token.is_none() {
            st.token = Some(open_monitor(
                &*self.backend,
                &self.source,
                self.make_callback(),
            ));
        }
    }

    /// Drop the backend monitor when neither staged nor any listeners remain.
    /// Caller holds `state`.
    fn maybe_teardown(&self, st: &mut CacheState) {
        if !st.staged && st.listeners == 0 {
            // Dropping the SubToken unsubscribes from the backend.
            st.token = None;
            self.fanout.clear();
        }
    }

    /// Set (or clear) the staged flag. Staging keeps the monitor alive across
    /// listener churn; unstaging tears it down if no listeners remain.
    pub fn set_staged(&self, staged: bool) {
        let mut st = self.state.lock().unwrap();
        st.staged = staged;
        if staged {
            self.ensure_token(&mut st);
        } else {
            self.maybe_teardown(&mut st);
        }
    }

    /// Register a listener. Returns its own queue of every reading and a
    /// `SubToken` that decrements the listener count (and tears the monitor
    /// down if it was the last and the cache is not staged) on drop.
    ///
    /// When a reading is already cached the queue starts with it, so the
    /// listener's first `recv()` yields the current value: ophyd-async
    /// `_SignalCache.subscribe` notifies a new listener at once when valid.
    pub fn add_listener(&self) -> (mpsc::UnboundedReceiver<ReadingValue>, SubToken) {
        let mut st = self.state.lock().unwrap();
        st.listeners += 1;
        self.ensure_token(&mut st);
        let rx = self.fanout.listen();
        drop(st);

        // Capture only the state + fanout handles (no backend / `B`) so the
        // closure is `'static` regardless of `B`. Teardown mirrors
        // `maybe_teardown` but inline, since it must not reference `self`.
        let state = self.state.clone();
        let fanout = self.fanout.clone();
        let token = SubToken::new(move || {
            let mut st = state.lock().unwrap();
            st.listeners = st.listeners.saturating_sub(1);
            if !st.staged && st.listeners == 0 {
                st.token = None;
                fanout.clear();
            }
        });
        (rx, token)
    }

    /// Latest cached reading, or `None` if no callback has fired yet (or the
    /// monitor has been torn down).
    pub fn cached_reading(&self) -> Option<ReadingValue> {
        self.fanout.latest()
    }

    /// Whether the cache is currently staged.
    pub fn is_staged(&self) -> bool {
        self.state.lock().unwrap().staged
    }

    /// Number of live listeners.
    pub fn listener_count(&self) -> usize {
        self.state.lock().unwrap().listeners
    }

    /// Whether the backend monitor is currently open.
    pub fn is_monitoring(&self) -> bool {
        self.state.lock().unwrap().token.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::soft::SoftSignalBackend;
    use crate::core::error::Result;
    use crate::event_model::DataKey;
    use crate::protocols_async::PutCompletion;
    use std::time::Duration;

    fn backend(v: f64) -> Arc<SoftSignalBackend<f64>> {
        Arc::new(SoftSignalBackend::new(v))
    }

    // INVARIANT boundary: monitor alive iff staged || listeners>0. Walk each
    // corner — stage-only, listener-only, both, and the teardown when both
    // reach zero.
    #[tokio::test]
    async fn monitor_lifetime_tracks_staged_or_listeners() {
        let be = backend(0.0);
        let cache = SignalCache::new(be.clone(), "x");
        assert!(!cache.is_monitoring(), "idle: no monitor");
        assert_eq!(be.subscriber_count(), 0);

        // staged-only.
        cache.set_staged(true);
        assert!(cache.is_monitoring());
        assert_eq!(be.subscriber_count(), 1, "one shared backend monitor");

        // staged + listener: still exactly one backend monitor.
        let (_rx1, t1) = cache.add_listener();
        assert_eq!(cache.listener_count(), 1);
        assert_eq!(
            be.subscriber_count(),
            1,
            "monitor is shared, not per-listener"
        );

        // unstage with a listener still present: monitor stays.
        cache.set_staged(false);
        assert!(cache.is_monitoring());
        assert_eq!(be.subscriber_count(), 1);

        // drop last listener while unstaged: teardown.
        drop(t1);
        assert_eq!(cache.listener_count(), 0);
        assert!(!cache.is_monitoring(), "both zero: monitor torn down");
        assert_eq!(be.subscriber_count(), 0);
    }

    #[tokio::test]
    async fn listeners_share_one_monitor_and_see_updates() {
        let be = backend(0.0);
        let cache = SignalCache::new(be.clone(), "x");

        let (mut rx_a, _ta) = cache.add_listener();
        let (mut rx_b, _tb) = cache.add_listener();
        assert_eq!(be.subscriber_count(), 1, "N listeners → 1 backend monitor");
        // Opening the soft monitor delivers the current value to everyone.
        assert_eq!(rx_a.recv().await.unwrap().value, serde_json::json!(0.0));
        assert_eq!(rx_b.recv().await.unwrap().value, serde_json::json!(0.0));

        // A backend write fans out to every listener and seeds the cache.
        be.write_now(4.5);
        assert_eq!(rx_a.recv().await.unwrap().value, serde_json::json!(4.5));
        assert_eq!(rx_b.recv().await.unwrap().value, serde_json::json!(4.5));
        assert_eq!(
            cache.cached_reading().unwrap().value,
            serde_json::json!(4.5)
        );
    }

    /// Monitor whose first reading arrives after `set_callback` returns, as
    /// a CA/PVA monitor's does; the soft backend delivers it inside the call.
    #[derive(Default)]
    struct DeferredBackend {
        cb: Mutex<Option<ReadingValueCallback<f64>>>,
    }

    impl DeferredBackend {
        fn post(&self, v: f64) {
            let cb = self.cb.lock().unwrap();
            cb.as_ref().expect("monitor open")(&v, 0.0, None);
        }
    }

    #[async_trait::async_trait]
    impl SignalBackend<f64> for DeferredBackend {
        async fn connect(&self, _: Duration) -> Result<()> {
            Ok(())
        }
        async fn put_start(&self, _: Option<f64>) -> Result<PutCompletion> {
            unimplemented!()
        }
        async fn get_datakey(&self, _: &str) -> Result<DataKey> {
            unimplemented!()
        }
        async fn get_reading(&self) -> Result<ReadingValue> {
            unimplemented!()
        }
        async fn get_value(&self) -> Result<f64> {
            unimplemented!()
        }
        async fn get_setpoint(&self) -> Result<f64> {
            unimplemented!()
        }
        fn set_callback(&self, cb: Option<ReadingValueCallback<f64>>) -> SubToken {
            *self.cb.lock().unwrap() = cb;
            SubToken::noop()
        }
        fn source(&self, name: &str, _: bool) -> String {
            name.into()
        }
    }

    // Boundary: a listener added before the monitor's first reading waits
    // for it, with no placeholder queued; one added after starts with the
    // cached reading (ophyd-async `_SignalCache.subscribe` notifies at once
    // when valid).
    #[tokio::test]
    async fn a_listener_added_after_a_reading_starts_with_it_queued() {
        let be = Arc::new(DeferredBackend::default());
        let cache = SignalCache::new(be.clone(), "x");
        let (mut early, _t0) = cache.add_listener();
        assert!(early.try_recv().is_err(), "nothing cached yet");

        be.post(3.0);
        let (mut late, _t1) = cache.add_listener();
        assert_eq!(late.try_recv().unwrap().value, serde_json::json!(3.0));
        assert_eq!(early.try_recv().unwrap().value, serde_json::json!(3.0));
    }

    // A pulse delivered faster than the listener reads (DMOV 1 → 0 → 1 on a
    // null move) reaches it whole.
    #[tokio::test]
    async fn a_pulse_between_reads_is_queued_whole() {
        let be = Arc::new(DeferredBackend::default());
        let cache = SignalCache::new(be.clone(), "x");
        let (mut rx, _t) = cache.add_listener();
        for v in [1.0, 0.0, 1.0] {
            be.post(v);
        }
        let got: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok())
            .map(|r| r.value)
            .collect();
        assert_eq!(got, [1.0, 0.0, 1.0].map(|v| serde_json::json!(v)));
    }

    // INVARIANT boundary: listeners == 0 while staged. No listener queue
    // takes the value then, and the cache must still hold it.
    #[tokio::test]
    async fn staged_cache_without_listeners_caches_the_update() {
        let be = backend(0.0);
        let cache = SignalCache::new(be.clone(), "x");
        cache.set_staged(true);
        assert_eq!(cache.listener_count(), 0);

        be.write_now(4.5);
        assert_eq!(
            cache.cached_reading().expect("value cached").value,
            serde_json::json!(4.5),
            "staged-only cache must hold the latest backend value"
        );
    }

    // The backend-facing callback must thread `alarm_severity` through to the
    // cached reading. CA/PVA monitors now deliver `Some(severity)` on each
    // update; the cache previously hardcoded `None` and dropped it. The soft
    // backend used elsewhere always passes `None`, so exercise the boundary by
    // invoking the callback directly with a known severity.
    #[tokio::test]
    async fn callback_threads_alarm_severity_into_cache() {
        let cache = SignalCache::new(backend(0.0), "x");
        // Skip `add_listener` (it installs the backend monitor, which would
        // race its own None-severity updates into the channel).
        let cb = cache.make_callback();
        cb(&7.5_f64, 12.0, Some(2)); // MAJOR
        let r = cache.cached_reading().expect("value cached after callback");
        assert_eq!(r.value, serde_json::json!(7.5));
        assert_eq!(r.timestamp, 12.0);
        assert_eq!(r.alarm_severity, Some(2), "cache must not drop severity");
        // `None` (soft/mock, or an alarm-less monitor frame) passes through.
        cb(&8.0_f64, 13.0, None);
        assert_eq!(cache.cached_reading().unwrap().alarm_severity, None);
    }
}
