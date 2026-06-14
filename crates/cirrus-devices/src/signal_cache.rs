//! `SignalCache<T, B>` — one shared backend monitor demultiplexed to N
//! listeners, mirroring ophyd-async `_SignalCache` (`core/_signal.py:116-186`).
//!
//! Without it, every `subscribe()` opens a fresh CA/PVA monitor and staging
//! does not persist a subscription. The cache fires `backend.set_callback`
//! exactly once and fans the values out over a `watch` channel; the monitor
//! stays alive while the signal is staged or has at least one listener.

use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use cirrus_core::reading::ReadingValue;
use cirrus_core::status::SubToken;
use cirrus_protocols_async::{ReadingValueCallback, SignalBackend};
use serde::Serialize;
use serde_json::Value;
use tokio::sync::watch;

fn null_reading() -> ReadingValue {
    ReadingValue {
        value: Value::Null,
        timestamp: 0.0,
        alarm_severity: None,
        message: None,
    }
}

/// Fan-out sink updated by the single backend callback. Held separately from
/// [`CacheState`] so the callback can capture it without forming a reference
/// cycle through the `SubToken` (which itself lives in `CacheState`).
struct Fanout {
    tx: watch::Sender<ReadingValue>,
    has_value: AtomicBool,
}

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

/// Shared monitor + latest-value cache for one signal.
pub struct SignalCache<T, B> {
    backend: Arc<B>,
    fanout: Arc<Fanout>,
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
    /// Build a cache over `backend`. No backend monitor is opened until the
    /// cache is staged or gains a listener.
    pub fn new(backend: Arc<B>) -> Arc<Self> {
        let (tx, _) = watch::channel(null_reading());
        Arc::new(Self {
            backend,
            fanout: Arc::new(Fanout {
                tx,
                has_value: AtomicBool::new(false),
            }),
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
        Box::new(move |v: &T, ts: f64, alarm_severity: Option<i32>| {
            if let Ok(json) = serde_json::to_value(v) {
                let _ = fanout.tx.send(ReadingValue {
                    value: json,
                    timestamp: ts,
                    alarm_severity,
                    message: None,
                });
                fanout.has_value.store(true, Ordering::SeqCst);
            }
        })
    }

    /// Open the backend monitor if not already open. Caller holds `state`.
    fn ensure_token(&self, st: &mut CacheState) {
        if st.token.is_none() {
            st.token = Some(self.backend.set_callback(Some(self.make_callback())));
        }
    }

    /// Drop the backend monitor when neither staged nor any listeners remain.
    /// Caller holds `state`.
    fn maybe_teardown(&self, st: &mut CacheState) {
        if !st.staged && st.listeners == 0 {
            // Dropping the SubToken unsubscribes from the backend.
            st.token = None;
            self.fanout.has_value.store(false, Ordering::SeqCst);
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

    /// Register a listener. Returns a `watch::Receiver` over the shared fan-out
    /// and a `SubToken` that decrements the listener count (and tears the
    /// monitor down if it was the last and the cache is not staged) on drop.
    pub fn add_listener(&self) -> (watch::Receiver<ReadingValue>, SubToken) {
        let mut st = self.state.lock().unwrap();
        st.listeners += 1;
        self.ensure_token(&mut st);
        let rx = self.fanout.tx.subscribe();
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
                fanout.has_value.store(false, Ordering::SeqCst);
            }
        });
        (rx, token)
    }

    /// Latest cached reading, or `None` if no callback has fired yet (or the
    /// monitor has been torn down).
    pub fn cached_reading(&self) -> Option<ReadingValue> {
        if self.fanout.has_value.load(Ordering::SeqCst) {
            Some(self.fanout.tx.borrow().clone())
        } else {
            None
        }
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
    use cirrus_backend_soft::SoftSignalBackend;
    use cirrus_event_model::Dtype;

    fn backend(v: f64) -> Arc<SoftSignalBackend<f64>> {
        Arc::new(SoftSignalBackend::new(v, Dtype::Number))
    }

    // INVARIANT boundary: monitor alive iff staged || listeners>0. Walk each
    // corner — stage-only, listener-only, both, and the teardown when both
    // reach zero.
    #[tokio::test]
    async fn monitor_lifetime_tracks_staged_or_listeners() {
        let be = backend(0.0);
        let cache = SignalCache::new(be.clone());
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
        let cache = SignalCache::new(be.clone());

        let (mut rx_a, _ta) = cache.add_listener();
        let (mut rx_b, _tb) = cache.add_listener();
        assert_eq!(be.subscriber_count(), 1, "N listeners → 1 backend monitor");
        assert!(
            cache.cached_reading().is_none(),
            "no value before first post"
        );

        // A backend write fans out to every listener and seeds the cache.
        be.write_now(4.5);
        rx_a.changed().await.unwrap();
        rx_b.changed().await.unwrap();
        assert_eq!(rx_a.borrow().value, serde_json::json!(4.5));
        assert_eq!(rx_b.borrow().value, serde_json::json!(4.5));
        assert_eq!(
            cache.cached_reading().unwrap().value,
            serde_json::json!(4.5)
        );
    }

    // The backend-facing callback must thread `alarm_severity` through to the
    // cached reading. CA/PVA monitors now deliver `Some(severity)` on each
    // update; the cache previously hardcoded `None` and dropped it. The soft
    // backend used elsewhere always passes `None`, so exercise the boundary by
    // invoking the callback directly with a known severity.
    #[tokio::test]
    async fn callback_threads_alarm_severity_into_cache() {
        let cache = SignalCache::new(backend(0.0));
        // Keep a live fan-out receiver so the watch retains sent values, but
        // skip `add_listener` (it installs the backend monitor, which would
        // race its own None-severity updates into the channel).
        let _rx = cache.fanout.tx.subscribe();
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
