//! `StandardReadable` — compositional device that aggregates child signals
//! into read / read_configuration / stage / hints buckets, mirroring
//! ophyd-async `StandardReadable` + `StandardReadableFormat`
//! (`core/_readable.py:83-288`).
//!
//! Without it, every detector/device must hand-route its signals through
//! `AsyncReadable` / `AsyncConfigurable` / `Stageable`. `StandardReadable`
//! holds the accumulators and implements those traits (plus the engine-facing
//! `ReadableObj` / `ConfigurableObj` / `StageableObj` bridges) by delegation.

use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::sync::Arc;

use crate::core::error::{BsrsError, Result};
use crate::core::msg::{ConfigurableObj, NamedObj, ReadableObj, StageableObj};
use crate::core::reading::ReadingValue;
use crate::core::ConfigureArgs;
use crate::devices::signal::{Readable, Signal};
use crate::event_model::DataKey;
use crate::protocols_async::{AsyncConfigurable, AsyncReadable, SignalBackend, Stageable};
use async_trait::async_trait;
use serde::Serialize;

/// How a readable signal contributes to a [`StandardReadable`]'s documents,
/// given to [`StandardReadable::add_signal`]. Mirrors the signal formats of
/// ophyd-async `StandardReadableFormat` (`core/_readable.py`), which accept
/// only a `SignalR`; its `CHILD` format, which takes any device, is
/// [`StandardReadable::add_child`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StandardReadableFormat {
    /// Contributes to `read_configuration` / `describe_configuration` only.
    ConfigSignal,
    /// Contributes to `read` / `describe` and to `hints`.
    HintedSignal,
    /// Contributes to `read` / `describe`.
    UncachedSignal,
    /// Contributes to `read` / `describe` and to `hints`.
    HintedUncachedSignal,
}

/// A compositional device that merges its children's readings, configuration,
/// staging and hints. Register children with [`add_child`] / [`add_signal`]
/// and staging with [`add_stageable`]; the trait impls then expose the
/// aggregate.
///
/// [`add_child`]: StandardReadable::add_child
/// [`add_signal`]: StandardReadable::add_signal
/// [`add_stageable`]: StandardReadable::add_stageable
pub struct StandardReadable {
    name: String,
    read: Vec<Source>,
    config: Vec<Source>,
    stageables: Vec<Arc<dyn StageableObj>>,
    hints: Vec<String>,
}

/// The `SignalR` surface the signal formats read through, object-safe so
/// signals of any value type and backend share one bucket.
#[async_trait]
trait SignalRead: Send + Sync {
    async fn read_signal(&self) -> Result<HashMap<String, ReadingValue>>;
    async fn describe_signal(&self) -> Result<HashMap<String, DataKey>>;
}

#[async_trait]
impl<T, B, A> SignalRead for Signal<T, B, A>
where
    T: Clone + Send + Sync + Serialize + 'static,
    B: SignalBackend<T>,
    A: Readable,
{
    async fn read_signal(&self) -> Result<HashMap<String, ReadingValue>> {
        self.read().await
    }
    async fn describe_signal(&self) -> Result<HashMap<String, DataKey>> {
        self.describe().await
    }
}

/// One contributor to a bucket: an entry of ophyd-async's paired
/// `_describe_funcs` / `_read_funcs` (or `_describe_config_funcs` /
/// `_read_config_funcs`).
enum Source {
    /// A `CHILD` in the `read` bucket: its own `read` / `describe`.
    Device(Arc<dyn ReadableObj>),
    /// A `CHILD` in the configuration bucket: its configuration view.
    DeviceConfig(Arc<dyn ReadableObj>),
    /// A signal format: the signal's `read` / `describe`.
    Signal(Arc<dyn SignalRead>),
}

impl Source {
    async fn read(&self) -> Result<HashMap<String, ReadingValue>> {
        match self {
            Self::Device(d) => d.read_dyn().await,
            Self::DeviceConfig(d) => match d.as_configurable() {
                Some(cfg) => cfg.read_configuration_dyn().await,
                None => Ok(HashMap::new()),
            },
            Self::Signal(s) => s.read_signal().await,
        }
    }

    async fn describe(&self) -> Result<HashMap<String, DataKey>> {
        match self {
            Self::Device(d) => d.describe_dyn().await,
            Self::DeviceConfig(d) => match d.as_configurable() {
                Some(cfg) => cfg.describe_configuration_dyn().await,
                None => Ok(HashMap::new()),
            },
            Self::Signal(s) => s.describe_signal().await,
        }
    }
}

impl StandardReadable {
    /// Build an empty `StandardReadable` with the given name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            read: Vec::new(),
            config: Vec::new(),
            stageables: Vec::new(),
            hints: Vec::new(),
        }
    }

    /// Register any readable device the way ophyd-async's `CHILD` format
    /// does: its reading goes to `read`, its configuration view (if
    /// [`ReadableObj::as_configurable`] has one) to `read_configuration`, and
    /// its [`ReadableObj::hint_fields`] to `hints`.
    pub fn add_child(&mut self, child: Arc<dyn ReadableObj>) {
        if let Some(fields) = child.hint_fields() {
            self.hints.extend(fields);
        }
        if child.as_configurable().is_some() {
            self.config.push(Source::DeviceConfig(child.clone()));
        }
        self.read.push(Source::Device(child));
    }

    /// Register a readable signal under one of the signal formats. Only a
    /// signal with a readable access role is accepted, which is ophyd-async's
    /// `assert_device_is_signalr` check made at compile time. The `Hinted*`
    /// formats hint the signal's name, or nothing when it is unnamed.
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use bsrs::backends::soft::SoftSignalBackend;
    /// # use bsrs::core::{Dtype, Kind};
    /// # use bsrs::devices::{Signal, SignalConfig, SignalR, StandardReadable, StandardReadableFormat};
    /// let backend = Arc::new(SoftSignalBackend::new(0.1, Dtype::Number));
    /// let config = SignalConfig { source: "exp".into(), kind: Kind::Config, name: "exp".into() };
    /// let exposure: Arc<SignalR<f64, SoftSignalBackend<f64>>> = Arc::new(Signal::new(backend, config));
    /// let mut sr = StandardReadable::new("sr");
    /// sr.add_signal(exposure, StandardReadableFormat::ConfigSignal);
    /// ```
    ///
    /// A device that is not a signal is rejected:
    ///
    /// ```compile_fail,E0308
    /// # use std::sync::Arc;
    /// # use bsrs::devices::{StandardReadable, StandardReadableFormat};
    /// let mut sr = StandardReadable::new("sr");
    /// let child = Arc::new(StandardReadable::new("child"));
    /// sr.add_signal(child, StandardReadableFormat::ConfigSignal);
    /// ```
    pub fn add_signal<T, B, A>(
        &mut self,
        signal: Arc<Signal<T, B, A>>,
        format: StandardReadableFormat,
    ) where
        T: Clone + Send + Sync + Serialize + 'static,
        B: SignalBackend<T> + 'static,
        A: Readable,
    {
        use StandardReadableFormat::*;
        let name = signal.name().to_string();
        let source = Source::Signal(signal);
        match format {
            ConfigSignal => self.config.push(source),
            UncachedSignal => self.read.push(source),
            HintedSignal | HintedUncachedSignal => {
                if !name.is_empty() {
                    self.hints.push(name);
                }
                self.read.push(source);
            }
        }
    }

    /// Register a child that is staged/unstaged together with this device.
    /// (`StandardReadableFormat::CHILD` auto-stages a `Stageable` child in
    /// ophyd-async; Rust trait objects can't be probed for capability, so
    /// staging is registered explicitly.)
    pub fn add_stageable(&mut self, child: Arc<dyn StageableObj>) {
        self.stageables.push(child);
    }

    /// Stable name.
    pub fn name(&self) -> &str {
        &self.name
    }
}

// -- aggregating async protocol impls ---------------------------------------

/// Merge per-child maps like ophyd-async `merge_gathered_dicts`: a key
/// produced by two children is an error, never a silent overwrite.
async fn merge_unique_keys<V, F>(parts: Vec<F>) -> Result<HashMap<String, V>>
where
    F: Future<Output = Result<HashMap<String, V>>>,
{
    let mut out = HashMap::new();
    let mut duplicates = BTreeSet::new();
    for part in parts {
        for (key, value) in part.await? {
            match out.entry(key) {
                Entry::Occupied(e) => {
                    duplicates.insert(e.key().clone());
                }
                Entry::Vacant(e) => {
                    e.insert(value);
                }
            }
        }
    }
    if duplicates.is_empty() {
        return Ok(out);
    }
    let keys = duplicates.into_iter().collect::<Vec<_>>().join(", ");
    Err(BsrsError::Other(if keys.is_empty() {
        "Have you remembered to name the Device?".to_string()
    } else {
        format!("Duplicate keys found while merging dictionaries: {keys}")
    }))
}

#[async_trait]
impl AsyncReadable for StandardReadable {
    fn name(&self) -> &str {
        &self.name
    }
    async fn read(&self) -> Result<HashMap<String, ReadingValue>> {
        merge_unique_keys(self.read.iter().map(|s| s.read()).collect()).await
    }
    async fn describe(&self) -> Result<HashMap<String, DataKey>> {
        merge_unique_keys(self.read.iter().map(|s| s.describe()).collect()).await
    }
}

#[async_trait]
impl AsyncConfigurable for StandardReadable {
    fn name(&self) -> &str {
        &self.name
    }
    async fn read_configuration(&self) -> Result<HashMap<String, ReadingValue>> {
        merge_unique_keys(self.config.iter().map(|s| s.read()).collect()).await
    }
    async fn describe_configuration(&self) -> Result<HashMap<String, DataKey>> {
        merge_unique_keys(self.config.iter().map(|s| s.describe()).collect()).await
    }
    async fn configure(&self, _args: ConfigureArgs) -> Result<()> {
        // ophyd-async `StandardReadable` is Configurable only for
        // read/describe_configuration; it does not route a generic configure()
        // to children. No-op.
        Ok(())
    }
}

#[async_trait]
impl Stageable for StandardReadable {
    fn name(&self) -> &str {
        &self.name
    }
    async fn stage(&self) -> Result<()> {
        for child in &self.stageables {
            child.stage_dyn().await?;
        }
        Ok(())
    }
    async fn unstage(&self) -> Result<()> {
        for child in &self.stageables {
            child.unstage_dyn().await?;
        }
        Ok(())
    }
}

// -- engine-facing `*Obj` bridges -------------------------------------------

impl NamedObj for StandardReadable {
    fn name(&self) -> &str {
        &self.name
    }
    fn inspect_dyn(&self) -> serde_json::Value {
        serde_json::json!({
            "name": self.name,
            "type": "StandardReadable",
            "read": self.read.len(),
            "config": self.config.len(),
            "stageables": self.stageables.len(),
            "hints": self.hints,
        })
    }
}

#[async_trait]
impl ReadableObj for StandardReadable {
    async fn read_dyn(&self) -> Result<HashMap<String, ReadingValue>> {
        AsyncReadable::read(self).await
    }
    async fn describe_dyn(&self) -> Result<HashMap<String, DataKey>> {
        AsyncReadable::describe(self).await
    }
    fn as_stageable(self: Arc<Self>) -> Option<Arc<dyn StageableObj>> {
        Some(self)
    }
    fn hint_fields(&self) -> Option<Vec<String>> {
        if self.hints.is_empty() {
            None
        } else {
            Some(self.hints.clone())
        }
    }
    fn as_configurable(&self) -> Option<&dyn ConfigurableObj> {
        Some(self)
    }
}

#[async_trait]
impl ConfigurableObj for StandardReadable {
    async fn read_configuration_dyn(&self) -> Result<HashMap<String, ReadingValue>> {
        AsyncConfigurable::read_configuration(self).await
    }
    async fn describe_configuration_dyn(&self) -> Result<HashMap<String, DataKey>> {
        AsyncConfigurable::describe_configuration(self).await
    }
    async fn configure_dyn(&self, args: ConfigureArgs) -> Result<()> {
        AsyncConfigurable::configure(self, args).await
    }
}

#[async_trait]
impl StageableObj for StandardReadable {
    async fn stage_dyn(&self) -> Result<()> {
        Stageable::stage(self).await
    }
    async fn unstage_dyn(&self) -> Result<()> {
        Stageable::unstage(self).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::soft::SoftSignalBackend;
    use crate::core::Kind;
    use crate::devices::{Signal, SignalConfig, SignalR};
    use crate::event_model::Dtype;

    fn sig(name: &str, kind: Kind, v: f64) -> Arc<SignalR<f64, SoftSignalBackend<f64>>> {
        Arc::new(Signal::new(
            Arc::new(SoftSignalBackend::new(v, Dtype::Number)),
            SignalConfig {
                source: name.into(),
                kind,
                name: name.into(),
            },
        ))
    }

    // Boundary: each format routes to its bucket, and hints come from both
    // CHILD auto-detection (Kind::Hinted) and explicit HintedSignal (the
    // signal name, whatever its kind).
    #[tokio::test]
    async fn aggregates_read_config_and_hints() {
        let mut sr = StandardReadable::new("dev");
        // CHILD + Kind::Hinted → read bucket + auto hint "temp".
        sr.add_child(sig("temp", Kind::Hinted, 1.0));
        // HintedSignal + Kind::Normal → read bucket + explicit hint "x".
        sr.add_signal(
            sig("x", Kind::Normal, 2.0),
            StandardReadableFormat::HintedSignal,
        );
        // ConfigSignal → configuration bucket only.
        sr.add_signal(
            sig("exposure", Kind::Config, 0.1),
            StandardReadableFormat::ConfigSignal,
        );

        let read = AsyncReadable::read(&sr).await.unwrap();
        assert_eq!(read.len(), 2);
        assert!(read.contains_key("temp"));
        assert!(read.contains_key("x"));
        assert!(!read.contains_key("exposure"));

        let cfg = AsyncConfigurable::read_configuration(&sr).await.unwrap();
        assert_eq!(cfg.len(), 1);
        assert!(cfg.contains_key("exposure"));

        let mut hints = ReadableObj::hint_fields(&sr).unwrap();
        hints.sort();
        assert_eq!(hints, vec!["temp".to_string(), "x".to_string()]);

        // describe mirrors the read/config split.
        assert_eq!(AsyncReadable::describe(&sr).await.unwrap().len(), 2);
        assert_eq!(
            AsyncConfigurable::describe_configuration(&sr)
                .await
                .unwrap()
                .len(),
            1
        );
    }
}
