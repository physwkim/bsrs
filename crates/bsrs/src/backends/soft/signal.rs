//! `SoftSignalBackend<T>` — in-memory backend.

use crate::core::datatype::SignalDatatype;
use crate::core::error::Result;
use crate::core::reading::ReadingValue;
use crate::core::status::SubToken;
use crate::event_model::{DataKey, SignalMetadata};
use crate::protocols_async::{PutCompletion, ReadingValueCallback, SignalBackend};
use async_trait::async_trait;
use futures::future::BoxFuture;
use serde::Serialize;
use std::collections::VecDeque;
use std::future::Future;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// ophyd-async stamps every soft reading `alarm_severity=0` (no alarm).
const NO_ALARM: Option<i32> = Some(0);

fn now_ts() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or_default()
}

/// What a store changes, as one record: a reader never pairs a value with
/// another write's timestamp, and a subscriber registers against exactly one
/// stored reading.
struct State<T> {
    value: T,
    setpoint: T,
    /// Wall-clock time the `value` last changed (set at construction and on
    /// every `put`/`write_now`). ophyd-async stamps the reading once at
    /// `set_value`/`put` and returns it from every `get_reading`
    /// (`_soft_signal_backend.py:147-173`); a read must NOT mint a fresh
    /// timestamp, or an unchanged value would appear to update on every read.
    timestamp: f64,
    subscribers: Vec<(u64, Arc<Subscriber<T>>)>,
    next_id: u64,
    /// `Some` iff the getter has a poll period and `subscribers` is not
    /// empty; set and cleared only where `subscribers` gains its first entry
    /// or loses its last.
    poll: Option<tokio::task::AbortHandle>,
}

type GetterFn<T> = Arc<dyn Fn() -> BoxFuture<'static, Result<T>> + Send + Sync>;
type SetterFn<T> = Arc<dyn Fn(Option<T>) -> BoxFuture<'static, Result<Option<T>>> + Send + Sync>;

/// The device read behind a soft signal (ophyd-async `getter`), and how often
/// to poll it while subscribed; a poll period has no meaning without it.
#[derive(Clone)]
struct Getter<T> {
    read: GetterFn<T>,
    poll_period: Option<Duration>,
}

/// One registered callback and the readings queued for it.
///
/// INVARIANT: a callback receives readings in the order they were stored,
/// starting with the reading current at its registration. Readings are
/// queued only under the `State` lock (so queue order is store order), and
/// only the thread that found the queue idle delivers from it (so no two
/// deliveries to one callback run at once or overtake each other). Callbacks
/// run with no lock held, so one may read, write or subscribe this backend.
struct Subscriber<T> {
    cb: ReadingValueCallback<T>,
    mailbox: Mutex<Mailbox<T>>,
}

struct Mailbox<T> {
    pending: VecDeque<(T, f64)>,
    draining: bool,
}

impl<T> Subscriber<T> {
    /// Queue a reading; `true` if the caller must now `drain`. Called with
    /// the `State` lock held.
    fn enqueue(&self, value: T, ts: f64) -> bool {
        let mut mb = self.mailbox.lock().unwrap();
        mb.pending.push_back((value, ts));
        !std::mem::replace(&mut mb.draining, true)
    }

    /// Deliver queued readings until none remain. A write from another thread
    /// meanwhile, or from the callback itself, is delivered by this loop.
    fn drain(&self) {
        // A panicking callback must not leave the mailbox claimed forever; the
        // next store then becomes the drainer and delivers the backlog.
        struct ReleaseOnPanic<'a, T>(&'a Mutex<Mailbox<T>>);
        impl<T> Drop for ReleaseOnPanic<'_, T> {
            fn drop(&mut self) {
                if std::thread::panicking() {
                    self.0.lock().unwrap_or_else(|e| e.into_inner()).draining = false;
                }
            }
        }
        let _release = ReleaseOnPanic(&self.mailbox);
        loop {
            let (value, ts) = {
                let mut mb = self.mailbox.lock().unwrap();
                match mb.pending.pop_front() {
                    Some(reading) => reading,
                    // Released in the same critical section that saw the
                    // queue empty, so a concurrent enqueue either lands
                    // before (and is popped) or claims the next drain.
                    None => {
                        mb.draining = false;
                        return;
                    }
                }
            };
            (self.cb)(&value, ts, NO_ALARM);
        }
    }
}

struct Inner<T: Clone + Send + Sync + 'static> {
    state: Mutex<State<T>>,
    /// Value written by `put(None)` — the ophyd-async `initial_value`
    /// (`_soft_signal_backend.py:164`). Fixed at construction.
    initial: T,
    /// The ophyd-async backend `metadata` dict: units and precision, which
    /// a mock may change after construction.
    metadata: Mutex<SignalMetadata>,
    getter: Option<Getter<T>>,
    /// The put action (ophyd-async `setter`); `Ok(Some(v))` is the settled
    /// readback.
    setter: Option<SetterFn<T>>,
}

/// Soft (in-memory) signal backend, parameterized by value type.
pub struct SoftSignalBackend<T: Clone + Send + Sync + 'static> {
    inner: Arc<Inner<T>>,
}

impl<T: Clone + Send + Sync + 'static> Clone for SoftSignalBackend<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> crate::devices::BackendFromPv for SoftSignalBackend<T>
where
    T: Clone + Default + Send + Sync + Serialize + 'static,
{
    fn from_pv(_pv: &str) -> Self {
        Self::new(T::default())
    }
}

impl<T> SoftSignalBackend<T>
where
    T: Clone + Send + Sync + Serialize + 'static,
{
    /// Build holding `initial`, which `put(None)` writes again. The
    /// [`DataKey`] is the current value's ([`SignalDatatype`]).
    pub fn new(initial: T) -> Self {
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(State {
                    value: initial.clone(),
                    setpoint: initial.clone(),
                    timestamp: now_ts(),
                    subscribers: Vec::new(),
                    next_id: 0,
                    poll: None,
                }),
                initial,
                metadata: Mutex::default(),
                getter: None,
                setter: None,
            }),
        }
    }

    /// Copy of the stored reading and metadata, without subscribers.
    fn rebuilt(&self) -> Inner<T> {
        let st = self.inner.state.lock().unwrap();
        Inner {
            state: Mutex::new(State {
                value: st.value.clone(),
                setpoint: st.setpoint.clone(),
                timestamp: st.timestamp,
                subscribers: Vec::new(),
                next_id: 0,
                poll: None,
            }),
            initial: self.inner.initial.clone(),
            metadata: Mutex::new(self.inner.metadata.lock().unwrap().clone()),
            getter: self.inner.getter.clone(),
            setter: self.inner.setter.clone(),
        }
    }

    /// Set engineering units.
    pub fn with_units(self, units: impl Into<String>) -> Self {
        let inner = self.rebuilt();
        inner.metadata.lock().unwrap().units = Some(units.into());
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Set the digits after the decimal place to display.
    pub fn with_precision(self, precision: i64) -> Self {
        let inner = self.rebuilt();
        inner.metadata.lock().unwrap().precision = Some(precision);
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Change the described metadata in place, for every clone of this
    /// backend (ophyd-async `MockSignalBackend.set_units`/`set_precision`).
    pub(crate) fn update_metadata(&self, f: impl FnOnce(&mut SignalMetadata)) {
        f(&mut self.inner.metadata.lock().unwrap());
    }

    /// Take the value from `getter` on every `get_value`/`get_reading`, and
    /// every `poll_period` while anything is subscribed (ophyd-async `getter`,
    /// `poll_period`). A failed poll keeps the last value. Polling spawns on
    /// the Tokio runtime current at the first subscription.
    pub fn with_getter<F, Fut>(self, getter: F, poll_period: Option<Duration>) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        let read: GetterFn<T> = Arc::new(move || Box::pin(getter()));
        let inner = Inner {
            getter: Some(Getter { read, poll_period }),
            ..self.rebuilt()
        };
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Run `setter` with the put value on every put (ophyd-async `setter`).
    /// The readback becomes the `Some` it returns; on `None`, the getter's
    /// value if there is a getter, else the setpoint.
    pub fn with_setter<F, Fut>(self, setter: F) -> Self
    where
        F: Fn(Option<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Option<T>>> + Send + 'static,
    {
        let setter: SetterFn<T> = Arc::new(move |v| Box::pin(setter(v)));
        let inner = Inner {
            setter: Some(setter),
            ..self.rebuilt()
        };
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Store the getter's value, if there is a getter (ophyd-async
    /// `_update_value_from_getter`).
    async fn refresh(&self) -> Result<()> {
        if let Some(getter) = &self.inner.getter {
            let value = (getter.read)().await?;
            self.store(value, false);
        }
        Ok(())
    }

    /// Read the current value synchronously, for inspect/debug paths
    /// that must not block. Returns a clone.
    pub fn current_value(&self) -> T {
        self.inner.state.lock().unwrap().value.clone()
    }

    /// Read the last setpoint synchronously.
    pub fn current_setpoint(&self) -> T {
        self.inner.state.lock().unwrap().setpoint.clone()
    }

    /// Number of subscribers currently registered on this backend.
    pub fn subscriber_count(&self) -> usize {
        self.inner.state.lock().unwrap().subscribers.len()
    }

    /// Synchronously poke a new value (for sim drivers).
    pub fn write_now(&self, v: T) {
        self.store(v, false);
    }

    /// The one write path: store the reading (and the setpoint, for a put)
    /// and deliver it. Under contention a callback being delivered to by
    /// another thread gets this reading from that thread, after the ones
    /// stored before it, and this call returns without waiting for it.
    fn store(&self, value: T, put: bool) {
        let ts = now_ts();
        let mut to_drain = Vec::new();
        {
            let mut st = self.inner.state.lock().unwrap();
            if put {
                st.setpoint = value.clone();
            }
            st.value = value.clone();
            st.timestamp = ts;
            for (_, sub) in &st.subscribers {
                if sub.enqueue(value.clone(), ts) {
                    to_drain.push(sub.clone());
                }
            }
        }
        for sub in to_drain {
            sub.drain();
        }
    }
}

#[async_trait]
impl<T> SignalBackend<T> for SoftSignalBackend<T>
where
    T: SignalDatatype + Clone + Send + Sync + Serialize + 'static,
{
    async fn connect(&self, _timeout: Duration) -> Result<()> {
        Ok(())
    }
    async fn put_start(&self, value: Option<T>) -> Result<PutCompletion> {
        // `None` writes the configured initial value (ophyd-async
        // `_soft_signal_backend.py:164`).
        let setpoint = value.clone().unwrap_or_else(|| self.inner.initial.clone());
        match &self.inner.setter {
            None => self.store(setpoint, true),
            Some(setter) => {
                self.inner.state.lock().unwrap().setpoint = setpoint.clone();
                match setter(value).await? {
                    Some(readback) => self.store(readback, false),
                    None if self.inner.getter.is_some() => self.refresh().await?,
                    None => self.store(setpoint, false),
                }
            }
        }
        Ok(Box::pin(std::future::ready(Ok(()))))
    }
    async fn get_datakey(&self, source: &str) -> Result<DataKey> {
        // Of the stored value, as ophyd-async's `make_datakey(datatype,
        // self.reading["value"], ...)`: an array's shape follows its puts.
        let meta = self.inner.metadata.lock().unwrap().clone();
        self.current_value()
            .make_datakey(format!("soft://{source}"), meta)
    }
    async fn get_reading(&self) -> Result<ReadingValue> {
        self.refresh().await?;
        // The value's last-change time, captured at construction / `put` /
        // `write_now` — NOT a fresh read-time stamp. Mirrors ophyd-async
        // returning the stored `self.reading` (`_soft_signal_backend.py:172`).
        let (v, timestamp) = {
            let st = self.inner.state.lock().unwrap();
            (st.value.clone(), st.timestamp)
        };
        Ok(ReadingValue {
            value: serde_json::to_value(v)?,
            timestamp,
            alarm_severity: NO_ALARM,
            message: None,
        })
    }
    async fn get_value(&self) -> Result<T> {
        self.refresh().await?;
        Ok(self.current_value())
    }
    async fn get_setpoint(&self) -> Result<T> {
        Ok(self.current_setpoint())
    }
    fn set_callback(&self, cb: Option<ReadingValueCallback<T>>) -> SubToken {
        let Some(cb) = cb else {
            return SubToken::noop();
        };
        let sub = Arc::new(Subscriber {
            cb,
            mailbox: Mutex::new(Mailbox {
                pending: VecDeque::new(),
                draining: false,
            }),
        });
        // Taken before the lock: outside a runtime this panics, and must not
        // poison the state.
        let poller = self.inner.getter.as_ref().and_then(|g| {
            let period = g.poll_period?;
            Some((tokio::runtime::Handle::current(), g.read.clone(), period))
        });
        let id = {
            let mut st = self.inner.state.lock().unwrap();
            // A new monitor starts with the current reading, as ophyd-async
            // `SoftSignalBackend.set_callback` calls `callback(self.reading)`;
            // queued with the registration, so every later store follows it.
            sub.enqueue(st.value.clone(), st.timestamp);
            let id = st.next_id;
            st.next_id += 1;
            st.subscribers.push((id, sub.clone()));
            if st.subscribers.len() == 1 {
                st.poll = poller.map(|(rt, read, period)| {
                    let inner = Arc::downgrade(&self.inner);
                    rt.spawn(poll(inner, read, period)).abort_handle()
                });
            }
            id
        };
        sub.drain();
        let inner = self.inner.clone();
        SubToken::new(move || {
            let removed = {
                let mut st = inner.state.lock().unwrap();
                let at = st.subscribers.iter().position(|(i, _)| *i == id);
                let removed = at.map(|at| st.subscribers.remove(at).1);
                if st.subscribers.is_empty() {
                    if let Some(poll) = st.poll.take() {
                        poll.abort();
                    }
                }
                removed
            };
            // No store reaches it now; drop what is still queued so no
            // delivery starts after unsubscribe.
            if let Some(sub) = removed {
                sub.mailbox.lock().unwrap().pending.clear();
            }
        })
    }
    fn source(&self, name: &str, _read: bool) -> String {
        format!("soft://{name}")
    }
}

/// ophyd-async `SoftSignalBackend._poll`. Holds the backend weakly, so
/// dropping it ends the loop as well as the last unsubscribe does.
async fn poll<T>(inner: Weak<Inner<T>>, read: GetterFn<T>, period: Duration)
where
    T: Clone + Send + Sync + Serialize + 'static,
{
    loop {
        tokio::time::sleep(period).await;
        let Some(inner) = inner.upgrade() else {
            return;
        };
        if let Ok(value) = read().await {
            SoftSignalBackend { inner }.store(value, false);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocols_async::SignalBackend;
    use futures::executor::block_on;

    // CP-11 invariant boundary: `put(Some(v))` writes `v`; `put(None)`
    // writes the configured initial value (SignalX-style default put).
    #[test]
    fn put_some_writes_value_and_setpoint() {
        let b = SoftSignalBackend::new(0.0_f64);
        block_on(SignalBackend::put(&b, Some(3.5))).unwrap();
        assert_eq!(b.current_value(), 3.5);
        assert_eq!(b.current_setpoint(), 3.5);
    }

    #[test]
    fn put_none_writes_initial() {
        let b = SoftSignalBackend::new(7.0_f64);
        block_on(SignalBackend::put(&b, Some(3.0))).unwrap();
        assert_eq!(b.current_value(), 3.0);
        block_on(SignalBackend::put(&b, None)).unwrap();
        assert_eq!(b.current_value(), 7.0);
        assert_eq!(b.current_setpoint(), 7.0);
    }

    // get_reading must return the value's last-change timestamp (captured at
    // put), not a fresh read-time stamp: two reads with no intervening put
    // return the SAME timestamp, and a put advances it. ophyd-async returns the
    // stored `self.reading` from every get_reading (_soft_signal_backend.py:172),
    // so an unchanged value keeps its timestamp instead of appearing to update.
    #[test]
    fn get_reading_timestamp_is_put_time_not_read_time() {
        let b = SoftSignalBackend::new(0.0_f64);
        block_on(SignalBackend::put(&b, Some(1.0))).unwrap();
        let r1 = block_on(SignalBackend::get_reading(&b)).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let r2 = block_on(SignalBackend::get_reading(&b)).unwrap();
        assert!(
            (r1.timestamp - r2.timestamp).abs() < f64::EPSILON,
            "unchanged value must keep its put-time timestamp across reads ({} vs {})",
            r1.timestamp,
            r2.timestamp
        );
        std::thread::sleep(std::time::Duration::from_millis(2));
        block_on(SignalBackend::put(&b, Some(2.0))).unwrap();
        let r3 = block_on(SignalBackend::get_reading(&b)).unwrap();
        assert!(
            r3.timestamp > r2.timestamp,
            "a put must advance the reading timestamp ({} !> {})",
            r3.timestamp,
            r2.timestamp
        );
    }

    type Log = Arc<Mutex<Vec<f64>>>;

    /// A callback that records each value once its call returns, and whose
    /// call for `hold` blocks until released: `entered` fires on entry.
    fn holding_recorder(
        hold: f64,
    ) -> (
        ReadingValueCallback<f64>,
        Log,
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::Sender<()>,
    ) {
        let (entered_tx, entered) = std::sync::mpsc::channel();
        let (release, release_rx) = std::sync::mpsc::channel::<()>();
        let (entered_tx, release_rx) = (Mutex::new(entered_tx), Mutex::new(release_rx));
        let log: Log = Arc::default();
        let seen = log.clone();
        let cb: ReadingValueCallback<f64> = Box::new(move |v: &f64, _, _| {
            if *v == hold {
                entered_tx.lock().unwrap().send(()).unwrap();
                release_rx.lock().unwrap().recv().unwrap();
            }
            seen.lock().unwrap().push(*v);
        });
        (cb, log, entered, release)
    }

    fn recorder() -> (ReadingValueCallback<f64>, Log) {
        let log: Log = Arc::default();
        let seen = log.clone();
        (
            Box::new(move |v: &f64, _, _| seen.lock().unwrap().push(*v)),
            log,
        )
    }

    // Boundary: a store while another thread is mid-delivery of an earlier one.
    #[test]
    fn a_store_during_another_delivery_reaches_each_callback_after_it() {
        let b = SoftSignalBackend::new(0.0_f64);
        let (slow, _, entered, release) = holding_recorder(1.0);
        let (fast, log) = recorder();
        let _t1 = b.set_callback(Some(slow));
        let _t2 = b.set_callback(Some(fast));
        let writer = {
            let b = b.clone();
            std::thread::spawn(move || b.write_now(1.0))
        };
        entered.recv().unwrap();
        b.write_now(2.0);
        release.send(()).unwrap();
        writer.join().unwrap();
        assert_eq!(*log.lock().unwrap(), [0.0, 1.0, 2.0]);
        assert_eq!(b.current_value(), 2.0);
    }

    // Boundary: an unsubscribe while a store is queued for that callback.
    #[test]
    fn no_delivery_starts_after_unsubscribe() {
        let b = SoftSignalBackend::new(0.0_f64);
        let (slow, _, entered, release) = holding_recorder(1.0);
        let (fast, log) = recorder();
        let _t1 = b.set_callback(Some(slow));
        let t2 = b.set_callback(Some(fast));
        let writer = {
            let b = b.clone();
            std::thread::spawn(move || b.write_now(1.0))
        };
        entered.recv().unwrap();
        drop(t2);
        release.send(()).unwrap();
        writer.join().unwrap();
        assert_eq!(*log.lock().unwrap(), [0.0]);
    }

    // Boundary: a store while a new callback is getting its initial reading.
    #[test]
    fn a_store_during_the_initial_delivery_follows_it() {
        let b = SoftSignalBackend::new(0.0_f64);
        let (cb, log, entered, release) = holding_recorder(0.0);
        let registrar = {
            let b = b.clone();
            std::thread::spawn(move || b.set_callback(Some(cb)))
        };
        entered.recv().unwrap();
        b.write_now(2.0);
        release.send(()).unwrap();
        let _token = registrar.join().unwrap();
        assert_eq!(*log.lock().unwrap(), [0.0, 2.0]);
    }

    // Boundary: a store from inside a callback, on the delivering thread.
    #[test]
    fn a_store_from_a_callback_is_delivered_after_it_returns() {
        let b = SoftSignalBackend::new(0.0_f64);
        let log: Log = Arc::default();
        let (seen, writer) = (log.clone(), b.clone());
        let _token = b.set_callback(Some(Box::new(move |v: &f64, _, _| {
            if *v == 1.0 {
                writer.write_now(2.0);
            }
            seen.lock().unwrap().push(*v);
        })));
        b.write_now(1.0);
        assert_eq!(*log.lock().unwrap(), [0.0, 1.0, 2.0]);
    }
}
