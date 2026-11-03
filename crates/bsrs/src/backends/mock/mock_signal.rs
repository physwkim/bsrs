//! `MockSignalBackend<T>` — the ophyd-async testing backend
//! (`core/_mock_signal_backend.py`). Wraps a [`SoftSignalBackend`] for real
//! state, records every `put`, lets a callback rewrite the put value, and
//! gates put completion on a `put_proceeds` flag so tests can block puts.

use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::backends::soft::SoftSignalBackend;
use crate::core::datatype::SignalDatatype;
use crate::core::error::Result;
use crate::core::reading::ReadingValue;
use crate::core::status::SubToken;
use crate::event_model::DataKey;
use crate::protocols_async::{PutCompletion, ReadingValueCallback, SignalBackend};
use async_trait::async_trait;
use futures::future::BoxFuture;
use serde::Serialize;
use std::future::Future;
use tokio::sync::watch;

/// Callback run on every `put`. Returns `Some(v)` to override the value written
/// to the readback, or `None` to keep the put value. Mirrors ophyd-async
/// `MockPutCallback` (the sync form).
pub type MockPutCallback<T> = Box<dyn Fn(Option<T>) -> Option<T> + Send + Sync>;

/// The installed put callback, sync ones wrapped as already-ready futures.
type PutCallbackFn<T> = Arc<dyn Fn(Option<T>) -> BoxFuture<'static, Option<T>> + Send + Sync>;

struct Inner<T: Clone + Send + Sync + 'static> {
    soft: SoftSignalBackend<T>,
    /// Every `put` argument, in call order (the ophyd `put_mock` call log).
    put_log: Mutex<Vec<Option<T>>>,
    /// `true` ⟹ puts complete immediately; `false` ⟹ a put writes the value
    /// but blocks (does not complete) until set back to `true`. Initially set.
    proceeds: watch::Sender<bool>,
    put_callback: Mutex<Option<PutCallbackFn<T>>>,
}

/// Mock signal backend for unit tests. Cheap to clone (shared inner) so a test
/// can hold a handle to drive `set_value` / `mock_puts_blocked` while a
/// `Signal` owns another clone.
pub struct MockSignalBackend<T: Clone + Send + Sync + 'static> {
    inner: Arc<Inner<T>>,
}

impl<T: Clone + Send + Sync + 'static> Clone for MockSignalBackend<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> crate::devices::BackendFromPv for MockSignalBackend<T>
where
    T: Clone + Default + Send + Sync + Serialize + 'static,
{
    fn from_pv(_pv: &str) -> Self {
        Self::new(SoftSignalBackend::new(T::default()))
    }
}

impl<T> MockSignalBackend<T>
where
    T: Clone + Send + Sync + Serialize + 'static,
{
    /// Wrap an existing [`SoftSignalBackend`] as the mock's real state.
    pub fn new(soft: SoftSignalBackend<T>) -> Self {
        let (proceeds, _) = watch::channel(true);
        Self {
            inner: Arc::new(Inner {
                soft,
                put_log: Mutex::new(Vec::new()),
                proceeds,
                put_callback: Mutex::new(None),
            }),
        }
    }

    /// Convenience: build a mock over a fresh soft backend holding `initial`.
    pub fn with_value(initial: T) -> Self {
        Self::new(SoftSignalBackend::new(initial))
    }

    /// Directly set the readback value (ophyd `set_mock_value`). Runs the soft
    /// backend's subscribers.
    pub fn set_value(&self, value: T) {
        self.inner.soft.write_now(value);
    }

    /// An iterator whose every `next` sets the readback to the next of
    /// `values` and yields it (ophyd `set_mock_values`). With
    /// `require_all_consumed`, dropping it before `values` is exhausted
    /// panics, naming the consumed and unconsumed values; `values` must then
    /// be finite.
    pub fn set_values<I>(&self, values: I, require_all_consumed: bool) -> SetValues<T, I::IntoIter>
    where
        T: Debug,
        I: IntoIterator<Item = T>,
    {
        SetValues {
            backend: self.clone(),
            values: values.into_iter(),
            consumed: require_all_consumed.then(Vec::new),
        }
    }

    /// Set the units `describe` reports (ophyd `set_mock_units`).
    pub fn set_units(&self, units: impl Into<String>) {
        let units = units.into();
        self.inner.soft.update_metadata(|m| m.units = Some(units));
    }

    /// Set the precision `describe` reports (ophyd `set_mock_precision`).
    pub fn set_precision(&self, precision: i64) {
        self.inner
            .soft
            .update_metadata(|m| m.precision = Some(precision));
    }

    /// Allow (`true`) or block (`false`) puts from completing (ophyd
    /// `set_mock_put_proceeds`).
    pub fn set_put_proceeds(&self, proceeds: bool) {
        self.inner.proceeds.send_replace(proceeds);
    }

    /// Install (or clear with `None`) the put callback (ophyd
    /// `set_mock_put_callback`).
    pub fn set_put_callback(&self, cb: Option<MockPutCallback<T>>) {
        *self.inner.put_callback.lock().unwrap() = cb
            .map(|f| -> PutCallbackFn<T> { Arc::new(move |v| Box::pin(std::future::ready(f(v)))) });
    }

    /// Install an async put callback (ophyd `set_mock_put_callback` with a
    /// coroutine function). The put awaits it before writing the readback,
    /// so a slow callback delays the readback. Clear it with
    /// `set_put_callback(None)`.
    pub fn set_put_callback_async<F, Fut>(&self, cb: F)
    where
        F: Fn(Option<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Option<T>> + Send + 'static,
    {
        *self.inner.put_callback.lock().unwrap() = Some(Arc::new(move |v| Box::pin(cb(v))));
    }

    /// Snapshot of every `put` argument received, in call order (ophyd
    /// `get_mock_put` — inspect calls).
    pub fn put_calls(&self) -> Vec<Option<T>> {
        self.inner.put_log.lock().unwrap().clone()
    }

    /// Number of `put` calls recorded.
    pub fn put_count(&self) -> usize {
        self.inner.put_log.lock().unwrap().len()
    }

    /// Forget the recorded `put` calls (ophyd `get_mock_put(...).reset_mock()`).
    pub fn reset_put_calls(&self) {
        self.inner.put_log.lock().unwrap().clear();
    }

    /// Block puts for the lifetime of the returned guard; puts unblock when it
    /// drops (ophyd `mock_puts_blocked` context manager).
    pub fn mock_puts_blocked(&self) -> PutsBlockedGuard<T> {
        self.set_put_proceeds(false);
        PutsBlockedGuard {
            backend: self.clone(),
        }
    }

    /// Reference the wrapped soft backend (for direct state inspection).
    pub fn soft(&self) -> &SoftSignalBackend<T> {
        &self.inner.soft
    }

    async fn wait_proceeds(&self) {
        let mut rx = self.inner.proceeds.subscribe();
        loop {
            if *rx.borrow_and_update() {
                return;
            }
            // Sender lives in `inner` (held by self), so this only errors if
            // every sender dropped — treat as "proceed".
            if rx.changed().await.is_err() {
                return;
            }
        }
    }
}

/// Iterator from [`MockSignalBackend::set_values`].
pub struct SetValues<T, I>
where
    T: Clone + Debug + Send + Sync + Serialize + 'static,
    I: Iterator<Item = T>,
{
    backend: MockSignalBackend<T>,
    values: I,
    /// `Some` iff all values must be consumed; kept only then, since the
    /// values may otherwise be endless.
    consumed: Option<Vec<T>>,
}

impl<T, I> Iterator for SetValues<T, I>
where
    T: Clone + Debug + Send + Sync + Serialize + 'static,
    I: Iterator<Item = T>,
{
    type Item = T;
    fn next(&mut self) -> Option<T> {
        let value = self.values.next()?;
        self.backend.set_value(value.clone());
        if let Some(consumed) = &mut self.consumed {
            consumed.push(value.clone());
        }
        Some(value)
    }
}

impl<T, I> Drop for SetValues<T, I>
where
    T: Clone + Debug + Send + Sync + Serialize + 'static,
    I: Iterator<Item = T>,
{
    fn drop(&mut self) {
        let Some(consumed) = self.consumed.take() else {
            return;
        };
        // A second panic while unwinding would abort the test binary.
        if std::thread::panicking() {
            return;
        }
        let rest: Vec<T> = self.values.by_ref().collect();
        assert!(
            rest.is_empty(),
            "{consumed:?} were consumed but {rest:?} were not consumed"
        );
    }
}

/// RAII guard from [`MockSignalBackend::mock_puts_blocked`]; unblocks puts on drop.
pub struct PutsBlockedGuard<T: Clone + Send + Sync + Serialize + 'static> {
    backend: MockSignalBackend<T>,
}

impl<T: Clone + Send + Sync + Serialize + 'static> Drop for PutsBlockedGuard<T> {
    fn drop(&mut self) {
        self.backend.set_put_proceeds(true);
    }
}

#[async_trait]
impl<T> SignalBackend<T> for MockSignalBackend<T>
where
    T: SignalDatatype + Clone + Send + Sync + Serialize + 'static,
{
    async fn connect(&self, _timeout: Duration) -> Result<()> {
        // ophyd-async raises here because Device.connect(mock=True) swaps the
        // backend without calling connect. bsrs has no runtime backend swap
        // (the backend type is fixed on the Signal), so a device built over the
        // mock connects transparently. No-op.
        Ok(())
    }
    async fn put_start(&self, value: Option<T>) -> Result<PutCompletion> {
        self.inner.put_log.lock().unwrap().push(value.clone());
        // Callback may rewrite the value written to the readback; `None` keeps
        // the put value (ophyd: new_value = put_mock(value) or value).
        let cb = self.inner.put_callback.lock().unwrap().clone();
        let overridden = match cb {
            Some(f) => f(value.clone()).await,
            None => None,
        };
        let new_value = overridden.or(value);
        self.inner.soft.put(new_value).await?;
        // Completion gates on the proceeds flag (blocked puts write the value
        // but do not resolve until unblocked).
        let this = self.clone();
        Ok(Box::pin(async move {
            this.wait_proceeds().await;
            Ok(())
        }))
    }
    async fn get_datakey(&self, source: &str) -> Result<DataKey> {
        self.inner.soft.get_datakey(source).await
    }
    async fn get_reading(&self) -> Result<ReadingValue> {
        self.inner.soft.get_reading().await
    }
    async fn get_value(&self) -> Result<T> {
        self.inner.soft.get_value().await
    }
    async fn get_setpoint(&self) -> Result<T> {
        self.inner.soft.get_setpoint().await
    }
    fn set_callback(&self, cb: Option<ReadingValueCallback<T>>) -> SubToken {
        self.inner.soft.set_callback(cb)
    }
    fn source(&self, name: &str, read: bool) -> String {
        format!("mock+{}", self.inner.soft.source(name, read))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[tokio::test]
    async fn records_puts_and_callback_overrides_readback() {
        let b = MockSignalBackend::with_value(0.0_f64);
        // Callback doubles the put value into the readback.
        b.set_put_callback(Some(Box::new(|v: Option<f64>| v.map(|x| x * 2.0))));
        SignalBackend::put(&b, Some(3.0)).await.unwrap();
        assert_eq!(b.get_value().await.unwrap(), 6.0);
        // Call log records the original argument, not the override.
        assert_eq!(b.put_calls(), vec![Some(3.0)]);
        assert_eq!(b.put_count(), 1);

        // Clearing the callback keeps the put value verbatim.
        b.set_put_callback(None);
        SignalBackend::put(&b, Some(5.0)).await.unwrap();
        assert_eq!(b.get_value().await.unwrap(), 5.0);
    }

    #[tokio::test]
    async fn blocked_put_writes_value_but_does_not_complete() {
        let b = MockSignalBackend::with_value(0.0_f64);
        let done = Arc::new(AtomicBool::new(false));

        let guard = b.mock_puts_blocked();
        let b2 = b.clone();
        let done2 = done.clone();
        let h = tokio::spawn(async move {
            SignalBackend::put(&b2, Some(7.0)).await.unwrap();
            done2.store(true, Ordering::SeqCst);
        });

        // Let the put run far enough to write the value and reach the gate.
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            b.get_value().await.unwrap(),
            7.0,
            "value written while blocked"
        );
        assert!(
            !done.load(Ordering::SeqCst),
            "put must not complete while blocked"
        );

        // Dropping the guard unblocks; the put now completes.
        drop(guard);
        h.await.unwrap();
        assert!(done.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn set_value_and_source_prefix() {
        let b = MockSignalBackend::with_value(1.0_f64);
        b.set_value(42.0);
        assert_eq!(b.get_value().await.unwrap(), 42.0);
        // read=true (read-back source) and read=false (write source) are
        // identical for soft/mock backends, which have a single PV.
        assert_eq!(b.source("dev", true), "mock+soft://dev");
        assert_eq!(b.source("dev", false), "mock+soft://dev");
    }
}
