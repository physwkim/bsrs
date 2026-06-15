//! `MockSignalBackend<T>` — the ophyd-async testing backend
//! (`core/_mock_signal_backend.py`). Wraps a [`SoftSignalBackend`] for real
//! state, records every `put`, lets a callback rewrite the put value, and
//! gates put completion on a `put_proceeds` flag so tests can block puts.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use cirrus_backend_soft::SoftSignalBackend;
use cirrus_core::error::Result;
use cirrus_core::reading::ReadingValue;
use cirrus_core::status::SubToken;
use cirrus_event_model::{DataKey, Dtype};
use cirrus_protocols_async::{ReadingValueCallback, SignalBackend};
use serde::Serialize;
use tokio::sync::watch;

/// Callback run on every `put`. Returns `Some(v)` to override the value written
/// to the readback, or `None` to keep the put value. Mirrors ophyd-async
/// `MockPutCallback` (the sync form).
pub type MockPutCallback<T> = Box<dyn Fn(Option<T>) -> Option<T> + Send + Sync>;

struct Inner<T: Clone + Send + Sync + 'static> {
    soft: SoftSignalBackend<T>,
    /// Every `put` argument, in call order (the ophyd `put_mock` call log).
    put_log: Mutex<Vec<Option<T>>>,
    /// `true` ⟹ puts complete immediately; `false` ⟹ a put writes the value
    /// but blocks (does not complete) until set back to `true`. Initially set.
    proceeds: watch::Sender<bool>,
    put_callback: Mutex<Option<MockPutCallback<T>>>,
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

impl<T> cirrus_devices::BackendFromPv for MockSignalBackend<T>
where
    T: Clone + Default + Send + Sync + Serialize + 'static,
{
    fn from_pv(_pv: &str) -> Self {
        Self::new(SoftSignalBackend::new(T::default(), Dtype::Number))
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

    /// Convenience: build a mock over a fresh soft backend with `initial`/`dtype`.
    pub fn with_value(initial: T, dtype: Dtype) -> Self {
        Self::new(SoftSignalBackend::new(initial, dtype))
    }

    /// Directly set the readback value (ophyd `set_mock_value`). Runs the soft
    /// backend's subscribers.
    pub fn set_value(&self, value: T) {
        self.inner.soft.write_now(value);
    }

    /// Allow (`true`) or block (`false`) puts from completing (ophyd
    /// `set_mock_put_proceeds`).
    pub fn set_put_proceeds(&self, proceeds: bool) {
        self.inner.proceeds.send_replace(proceeds);
    }

    /// Install (or clear with `None`) the put callback (ophyd
    /// `set_mock_put_callback`).
    pub fn set_put_callback(&self, cb: Option<MockPutCallback<T>>) {
        *self.inner.put_callback.lock().unwrap() = cb;
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
    T: Clone + Send + Sync + Serialize + 'static,
{
    async fn connect(&self, _timeout: Duration) -> Result<()> {
        // ophyd-async raises here because Device.connect(mock=True) swaps the
        // backend without calling connect. cirrus has no runtime backend swap
        // (the backend type is fixed on the Signal), so a device built over the
        // mock connects transparently. No-op.
        Ok(())
    }
    async fn put(&self, value: Option<T>) -> Result<()> {
        self.inner.put_log.lock().unwrap().push(value.clone());
        // Callback may rewrite the value written to the readback; `None` keeps
        // the put value (ophyd: new_value = put_mock(value) or value).
        let overridden = {
            let cb = self.inner.put_callback.lock().unwrap();
            cb.as_ref().and_then(|f| f(value.clone()))
        };
        let new_value = overridden.or(value);
        self.inner.soft.put(new_value).await?;
        // Completion gates on the proceeds flag (blocked puts write the value
        // but do not resolve until unblocked).
        self.wait_proceeds().await;
        Ok(())
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
        let b = MockSignalBackend::with_value(0.0_f64, Dtype::Number);
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
        let b = MockSignalBackend::with_value(0.0_f64, Dtype::Number);
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
        let b = MockSignalBackend::with_value(1.0_f64, Dtype::Number);
        b.set_value(42.0);
        assert_eq!(b.get_value().await.unwrap(), 42.0);
        // read=true (read-back source) and read=false (write source) are
        // identical for soft/mock backends, which have a single PV.
        assert_eq!(b.source("dev", true), "mock+soft://dev");
        assert_eq!(b.source("dev", false), "mock+soft://dev");
    }
}
