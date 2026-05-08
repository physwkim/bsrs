//! Real EPICS Channel Access backend, wired to `epics-ca-rs`.
//!
//! Architecture:
//!
//! - One process-wide `CaClient`, lazily initialized via `OnceCell`.
//! - Channel registry sharded across 64 mutexes (rule **K3**) — `connect()`
//!   does the slow path (`wait_connected`) outside the shard lock.
//! - In-flight de-dup via `pending: HashMap<PvName, Arc<Notify>>` (rule **K4**).
//! - `set_callback` spawns a forwarder task per channel, returning a `SubToken`
//!   whose Drop aborts the task; dropping the underlying `MonitorHandle` then
//!   unsubscribes on the wire (rule **K2**).

use async_trait::async_trait;
use cirrus_core::error::{CirrusError, Result};
use cirrus_core::reading::ReadingValue;
use cirrus_core::status::{Status, StatusError, SubToken};
use cirrus_event_model::{DataKey, Dtype};
use cirrus_protocols_async::{ReadingValueCallback, SignalBackend};
use epics_ca_rs::client::{CaChannel, CaClient};
use epics_ca_rs::EpicsValue;
use std::array;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Notify;

const SHARDS: usize = 64;

fn shard_for(pv: &str) -> usize {
    use std::hash::{BuildHasher, Hasher};
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write(pv.as_bytes());
    (h.finish() as usize) % SHARDS
}

fn now_ts() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or_default()
}

/// Process-wide CA context.
pub struct CaContext {
    client: Arc<CaClient>,
    shards: [Mutex<HashMap<String, Arc<CaChannel>>>; SHARDS],
    pending: [Mutex<HashMap<String, Arc<Notify>>>; SHARDS],
}

impl CaContext {
    fn new(client: CaClient) -> Self {
        Self {
            client: Arc::new(client),
            shards: array::from_fn(|_| Mutex::new(HashMap::new())),
            pending: array::from_fn(|_| Mutex::new(HashMap::new())),
        }
    }

    async fn get_or_open(&self, pv: &str, timeout: Duration) -> Result<Arc<CaChannel>> {
        let s = shard_for(pv);
        // Fast path
        if let Some(ch) = self.shards[s].lock().unwrap().get(pv).cloned() {
            return Ok(ch);
        }
        // K4: in-flight dedup
        let notify = {
            let mut p = self.pending[s].lock().unwrap();
            if let Some(n) = p.get(pv).cloned() {
                Some(n)
            } else {
                let n = Arc::new(Notify::new());
                p.insert(pv.to_string(), n.clone());
                None
            }
        };
        if let Some(n) = notify {
            n.notified().await;
            // Either the in-flight winner inserted, or it failed — either way
            // re-check the cache.
            if let Some(ch) = self.shards[s].lock().unwrap().get(pv).cloned() {
                return Ok(ch);
            }
            return Err(CirrusError::Backend(format!(
                "ca: peer connect for {pv} failed"
            )));
        }

        // K3: do the I/O (wait_connected) outside the shard lock.
        let ch = self.client.create_channel(pv);
        let res: epics_ca_rs::CaResult<()> = ch.wait_connected(timeout).await;

        // Commit and notify waiters either way.
        let arc = Arc::new(ch);
        let mut p = self.pending[s].lock().unwrap();
        let n = p.remove(pv);
        if res.is_ok() {
            self.shards[s]
                .lock()
                .unwrap()
                .insert(pv.to_string(), arc.clone());
        }
        if let Some(n) = n {
            n.notify_waiters();
        }
        res.map_err(|e| CirrusError::Backend(format!("ca connect {pv}: {e}")))?;
        Ok(arc)
    }
}

static CTX: OnceLock<Arc<CaContext>> = OnceLock::new();

/// Get the shared CA context. Initializes a `CaClient` on first call.
pub fn ca_context() -> Arc<CaContext> {
    if let Some(c) = CTX.get() {
        return c.clone();
    }
    // CaClient::new is async; bootstrap via the cirrus runtime.
    let client = cirrus_core::runtime::cirrus_runtime()
        .block_on(CaClient::new())
        .expect("CaClient::new failed");
    let ctx = Arc::new(CaContext::new(client));
    let _ = CTX.set(ctx.clone());
    ctx
}

/// CA backend for a single PV.
pub struct EpicsCaBackend<T: Clone + Send + Sync + 'static> {
    pv: String,
    ctx: Arc<CaContext>,
    channel: tokio::sync::OnceCell<Arc<CaChannel>>,
    _marker: std::marker::PhantomData<T>,
}

impl<T: Clone + Send + Sync + 'static> EpicsCaBackend<T> {
    /// Build with a PV name.
    pub fn new(pv: impl Into<String>) -> Self {
        Self {
            pv: pv.into(),
            ctx: ca_context(),
            channel: tokio::sync::OnceCell::new(),
            _marker: std::marker::PhantomData,
        }
    }

    async fn ensure_channel(&self, timeout: Duration) -> Result<Arc<CaChannel>> {
        self.channel
            .get_or_try_init(|| self.ctx.get_or_open(&self.pv, timeout))
            .await
            .cloned()
    }
}

fn epics_to_f64(v: &EpicsValue) -> Option<f64> {
    match v {
        EpicsValue::Double(d) => Some(*d),
        EpicsValue::Float(f) => Some(*f as f64),
        EpicsValue::Long(l) => Some(*l as f64),
        EpicsValue::Short(s) => Some(*s as f64),
        EpicsValue::Char(c) => Some(*c as f64),
        EpicsValue::Int64(i) => Some(*i as f64),
        EpicsValue::Enum(e) => Some(*e as f64),
        _ => None,
    }
}

#[async_trait]
impl SignalBackend<f64> for EpicsCaBackend<f64> {
    async fn connect(&self, timeout: Duration) -> Result<()> {
        self.ensure_channel(timeout).await.map(|_| ())
    }
    async fn put(&self, value: f64, wait: bool, timeout: Option<Duration>) -> Status {
        let ch = match self
            .ensure_channel(timeout.unwrap_or(Duration::from_secs(2)))
            .await
        {
            Ok(c) => c,
            Err(e) => return Status::fail(StatusError::Failed(format!("{e}"))),
        };
        let v = EpicsValue::Double(value);
        let res = if wait {
            ch.put(&v).await
        } else {
            ch.put_nowait(&v).await
        };
        match res {
            Ok(()) => Status::done(),
            Err(e) => Status::fail(StatusError::Failed(format!("ca put: {e}"))),
        }
    }
    async fn get_datakey(&self, source: &str) -> Result<DataKey> {
        let ch = self.ensure_channel(Duration::from_secs(2)).await?;
        let info = ch
            .info()
            .await
            .map_err(|e| CirrusError::Backend(format!("ca info: {e}")))?;
        Ok(DataKey {
            source: format!("ca://{source}"),
            dtype: Dtype::Number,
            shape: if info.element_count > 1 {
                vec![Some(info.element_count as u64)]
            } else {
                vec![]
            },
            dtype_numpy: Some("<f8".into()),
            external: None,
            units: None,
            precision: None,
            object_name: None,
            dims: None,
            limits: None,
        })
    }
    async fn get_reading(&self) -> Result<ReadingValue> {
        let ch = self.ensure_channel(Duration::from_secs(2)).await?;
        let (_ty, v) = ch
            .get()
            .await
            .map_err(|e| CirrusError::Backend(format!("ca get: {e}")))?;
        let f = epics_to_f64(&v)
            .ok_or_else(|| CirrusError::Backend(format!("ca: not numeric: {v:?}")))?;
        Ok(ReadingValue {
            value: serde_json::Value::from(f),
            timestamp: now_ts(),
            alarm_severity: None,
            message: None,
        })
    }
    async fn get_value(&self) -> Result<f64> {
        let ch = self.ensure_channel(Duration::from_secs(2)).await?;
        let (_ty, v) = ch
            .get()
            .await
            .map_err(|e| CirrusError::Backend(format!("ca get: {e}")))?;
        epics_to_f64(&v).ok_or_else(|| CirrusError::Backend(format!("ca: not numeric: {v:?}")))
    }
    async fn get_setpoint(&self) -> Result<f64> {
        // CA channels expose only one read path; we treat readback as the
        // best-effort setpoint as well.
        SignalBackend::<f64>::get_value(self).await
    }
    fn set_callback(&self, cb: Option<ReadingValueCallback<f64>>) -> SubToken {
        let cb = match cb {
            None => return SubToken::noop(),
            Some(cb) => Arc::new(cb),
        };
        let ctx = self.ctx.clone();
        let pv = self.pv.clone();
        // Spawn a forwarder. Held tokio::JoinHandle is aborted on Drop (K1).
        let handle = tokio::spawn(async move {
            let ch = match ctx.get_or_open(&pv, Duration::from_secs(2)).await {
                Ok(c) => c,
                Err(_) => return,
            };
            let mut sub = match ch.subscribe().await {
                Ok(s) => s,
                Err(_) => return,
            };
            while let Some(Ok(snap)) = sub.recv().await {
                if let Some(f) = epics_to_f64(&snap.value) {
                    let ts = snap
                        .timestamp
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_secs_f64())
                        .unwrap_or_default();
                    cb(&f, ts);
                }
            }
        });
        let abort = handle.abort_handle();
        SubToken::new(move || abort.abort())
    }
    fn source(&self, name: &str) -> String {
        format!("ca://{name}")
    }
}
