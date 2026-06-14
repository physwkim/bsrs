//! `SoftSignalBackend<T>` — in-memory backend.

use async_trait::async_trait;
use cirrus_core::error::Result;
use cirrus_core::reading::ReadingValue;
use cirrus_core::status::SubToken;
use cirrus_event_model::{make_datakey, DataKey, Dtype, SignalMetadata};
use cirrus_protocols_async::{ReadingValueCallback, SignalBackend};
use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn now_ts() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or_default()
}

struct Inner<T: Clone + Send + Sync + 'static> {
    value: Mutex<T>,
    setpoint: Mutex<T>,
    /// Value written by `put(None)` — the ophyd-async `initial_value`
    /// (`_soft_signal_backend.py:164`). Fixed at construction.
    initial: T,
    callbacks: Mutex<Vec<(u64, Arc<ReadingValueCallback<T>>)>>,
    next_id: AtomicU64,
    units: Option<String>,
    dtype: Dtype,
    dtype_numpy: Option<String>,
    shape: Vec<Option<u64>>,
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

impl<T> cirrus_devices::BackendFromPv for SoftSignalBackend<T>
where
    T: Clone + Default + Send + Sync + Serialize + 'static,
{
    fn from_pv(_pv: &str) -> Self {
        Self::new(T::default(), Dtype::Number)
    }
}

impl<T> SoftSignalBackend<T>
where
    T: Clone + Send + Sync + Serialize + 'static,
{
    /// Build with an initial value and a `Dtype` annotation for descriptors.
    pub fn new(initial: T, dtype: Dtype) -> Self {
        Self {
            inner: Arc::new(Inner {
                value: Mutex::new(initial.clone()),
                setpoint: Mutex::new(initial.clone()),
                initial,
                callbacks: Mutex::new(Vec::new()),
                next_id: AtomicU64::new(0),
                units: None,
                dtype,
                dtype_numpy: None,
                shape: vec![],
            }),
        }
    }

    /// Set engineering units.
    pub fn with_units(self, units: impl Into<String>) -> Self {
        let inner = Arc::new(Inner {
            value: Mutex::new(self.inner.value.lock().unwrap().clone()),
            setpoint: Mutex::new(self.inner.setpoint.lock().unwrap().clone()),
            initial: self.inner.initial.clone(),
            callbacks: Mutex::new(Vec::new()),
            next_id: AtomicU64::new(0),
            units: Some(units.into()),
            dtype: self.inner.dtype,
            dtype_numpy: self.inner.dtype_numpy.clone(),
            shape: self.inner.shape.clone(),
        });
        Self { inner }
    }

    /// Set the dtype_numpy metadata.
    pub fn with_dtype_numpy(self, np: impl Into<String>) -> Self {
        let inner = Arc::new(Inner {
            value: Mutex::new(self.inner.value.lock().unwrap().clone()),
            setpoint: Mutex::new(self.inner.setpoint.lock().unwrap().clone()),
            initial: self.inner.initial.clone(),
            callbacks: Mutex::new(Vec::new()),
            next_id: AtomicU64::new(0),
            units: self.inner.units.clone(),
            dtype: self.inner.dtype,
            dtype_numpy: Some(np.into()),
            shape: self.inner.shape.clone(),
        });
        Self { inner }
    }

    /// Read the current value synchronously, for inspect/debug paths
    /// that must not block. Returns a clone.
    pub fn current_value(&self) -> T {
        self.inner.value.lock().unwrap().clone()
    }

    /// Read the last setpoint synchronously.
    pub fn current_setpoint(&self) -> T {
        self.inner.setpoint.lock().unwrap().clone()
    }

    /// Number of subscribers currently registered on this backend.
    pub fn subscriber_count(&self) -> usize {
        self.inner.callbacks.lock().unwrap().len()
    }

    /// Synchronously poke a new value (for sim drivers).
    pub fn write_now(&self, v: T) {
        *self.inner.value.lock().unwrap() = v.clone();
        let ts = now_ts();
        let cbs: Vec<_> = self
            .inner
            .callbacks
            .lock()
            .unwrap()
            .iter()
            .map(|(_, cb)| cb.clone())
            .collect();
        for cb in cbs {
            cb(&v, ts, None);
        }
    }
}

#[async_trait]
impl<T> SignalBackend<T> for SoftSignalBackend<T>
where
    T: Clone + Send + Sync + Serialize + 'static,
{
    async fn connect(&self, _timeout: Duration) -> Result<()> {
        Ok(())
    }
    async fn put(&self, value: Option<T>) -> Result<()> {
        // `None` writes the configured initial value (ophyd-async
        // `_soft_signal_backend.py:164`).
        let value = value.unwrap_or_else(|| self.inner.initial.clone());
        *self.inner.setpoint.lock().unwrap() = value.clone();
        *self.inner.value.lock().unwrap() = value.clone();
        let ts = now_ts();
        let cbs: Vec<_> = self
            .inner
            .callbacks
            .lock()
            .unwrap()
            .iter()
            .map(|(_, cb)| cb.clone())
            .collect();
        for cb in cbs {
            cb(&value, ts, None);
        }
        Ok(())
    }
    async fn get_datakey(&self, source: &str) -> Result<DataKey> {
        Ok(make_datakey(
            format!("soft://{source}"),
            self.inner.dtype,
            self.inner.shape.clone(),
            self.inner.dtype_numpy.clone(),
            SignalMetadata {
                units: self.inner.units.clone(),
                ..Default::default()
            },
        ))
    }
    async fn get_reading(&self) -> Result<ReadingValue> {
        let v = self.inner.value.lock().unwrap().clone();
        Ok(ReadingValue {
            value: serde_json::to_value(v)?,
            timestamp: now_ts(),
            alarm_severity: None,
            message: None,
        })
    }
    async fn get_value(&self) -> Result<T> {
        Ok(self.inner.value.lock().unwrap().clone())
    }
    async fn get_setpoint(&self) -> Result<T> {
        Ok(self.inner.setpoint.lock().unwrap().clone())
    }
    fn set_callback(&self, cb: Option<ReadingValueCallback<T>>) -> SubToken {
        match cb {
            None => SubToken::noop(),
            Some(cb) => {
                let id = self.inner.next_id.fetch_add(1, Ordering::SeqCst);
                self.inner
                    .callbacks
                    .lock()
                    .unwrap()
                    .push((id, Arc::new(cb)));
                let inner = self.inner.clone();
                SubToken::new(move || {
                    inner.callbacks.lock().unwrap().retain(|(i, _)| *i != id);
                })
            }
        }
    }
    fn source(&self, name: &str) -> String {
        format!("soft://{name}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cirrus_protocols_async::SignalBackend;
    use futures::executor::block_on;

    // CP-11 invariant boundary: `put(Some(v))` writes `v`; `put(None)`
    // writes the configured initial value (SignalX-style default put).
    #[test]
    fn put_some_writes_value_and_setpoint() {
        let b = SoftSignalBackend::new(0.0_f64, Dtype::Number);
        block_on(SignalBackend::put(&b, Some(3.5))).unwrap();
        assert_eq!(b.current_value(), 3.5);
        assert_eq!(b.current_setpoint(), 3.5);
    }

    #[test]
    fn put_none_writes_initial() {
        let b = SoftSignalBackend::new(7.0_f64, Dtype::Number);
        block_on(SignalBackend::put(&b, Some(3.0))).unwrap();
        assert_eq!(b.current_value(), 3.0);
        block_on(SignalBackend::put(&b, None)).unwrap();
        assert_eq!(b.current_value(), 7.0);
        assert_eq!(b.current_setpoint(), 7.0);
    }
}
