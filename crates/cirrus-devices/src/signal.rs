//! `Signal<T>` — generic over `SignalBackend<T>`.

use async_trait::async_trait;
use cirrus_core::error::Result;
use cirrus_core::reading::ReadingValue;
use cirrus_core::status::{Status, SubToken};
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

/// A signal: name + backend + kind.
pub struct Signal<T, B: SignalBackend<T>>
where
    T: Clone + Send + Sync + 'static,
{
    backend: Arc<B>,
    config: SignalConfig,
    _marker: std::marker::PhantomData<T>,
}

impl<T, B> Signal<T, B>
where
    T: Clone + Send + Sync + Serialize + 'static,
    B: SignalBackend<T>,
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

    /// Read the typed value.
    pub async fn get(&self) -> Result<T> {
        self.backend.get_value().await
    }

    /// Get the most recent setpoint.
    pub async fn get_setpoint(&self) -> Result<T> {
        self.backend.get_setpoint().await
    }

    /// Put a value, returning a `Status`.
    pub async fn put(&self, value: T) -> Status {
        self.backend.put(value, true, None).await
    }

    /// Put a value without waiting for completion.
    pub async fn put_no_wait(&self, value: T) -> Status {
        self.backend.put(value, false, None).await
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

    /// Get the kind.
    pub fn kind(&self) -> Kind {
        self.config.kind
    }

    /// Get the human-friendly name.
    pub fn name(&self) -> &str {
        &self.config.name
    }
}

#[async_trait]
impl<T, B> AsyncReadable for Signal<T, B>
where
    T: Clone + Send + Sync + Serialize + 'static,
    B: SignalBackend<T>,
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
impl<T, B> AsyncSubscribable<T> for Signal<T, B>
where
    T: Clone + Send + Sync + Serialize + 'static,
    B: SignalBackend<T>,
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
impl<T, B> cirrus_core::msg::NamedObj for Signal<T, B>
where
    T: Clone + Send + Sync + Serialize + 'static,
    B: SignalBackend<T>,
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
impl<T, B> cirrus_core::msg::ReadableObj for Signal<T, B>
where
    T: Clone + Send + Sync + Serialize + 'static,
    B: SignalBackend<T>,
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
