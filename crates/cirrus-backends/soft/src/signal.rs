//! `SoftSignalBackend<T>` — in-memory backend.

use async_trait::async_trait;
use cirrus_core::error::Result;
use cirrus_core::reading::ReadingValue;
use cirrus_core::status::{Status, SubToken};
use cirrus_event_model::{DataKey, Dtype};
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
                setpoint: Mutex::new(initial),
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
            cb(&v, ts);
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
    async fn put(&self, value: T, _wait: bool, _timeout: Option<Duration>) -> Status {
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
            cb(&value, ts);
        }
        Status::done()
    }
    async fn get_datakey(&self, source: &str) -> Result<DataKey> {
        Ok(DataKey {
            source: format!("soft://{source}"),
            dtype: self.inner.dtype,
            shape: self.inner.shape.clone(),
            dtype_numpy: self.inner.dtype_numpy.clone(),
            external: None,
            units: self.inner.units.clone(),
            precision: None,
            object_name: None,
            dims: None,
            limits: None,
        })
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
