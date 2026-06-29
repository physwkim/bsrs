//! `StandardReadable` — compositional device that aggregates child signals
//! into read / read_configuration / stage / hints buckets, mirroring
//! ophyd-async `StandardReadable` + `StandardReadableFormat`
//! (`core/_readable.py:83-288`).
//!
//! Without it, every detector/device must hand-route its signals through
//! `AsyncReadable` / `AsyncConfigurable` / `Stageable`. `StandardReadable`
//! holds the accumulators and implements those traits (plus the engine-facing
//! `ReadableObj` / `ConfigurableObj` / `StageableObj` bridges) by delegation.

use std::collections::HashMap;
use std::sync::Arc;

use crate::core::error::Result;
use crate::core::msg::{ConfigurableObj, NamedObj, ReadableObj, StageableObj};
use crate::core::reading::ReadingValue;
use crate::core::ConfigureArgs;
use crate::event_model::DataKey;
use crate::protocols_async::{AsyncConfigurable, AsyncReadable, Stageable};
use async_trait::async_trait;

/// How a child contributes to a [`StandardReadable`]'s documents. Mirrors
/// ophyd-async `StandardReadableFormat` (`core/_readable.py`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StandardReadableFormat {
    /// Auto-detect: contributes to `read` / `describe`, and to `hints` via the
    /// child's [`ReadableObj::hint_fields`].
    Child,
    /// Contributes to `read_configuration` / `describe_configuration` only.
    ConfigSignal,
    /// Contributes to `read` / `describe` and to `hints`.
    HintedSignal,
    /// Contributes to `read` / `describe`. The value is read live (not cached);
    /// until CP-08 adds the signal cache this is identical to a cached read.
    UncachedSignal,
    /// Contributes to `read` / `describe` and to `hints`, uncached.
    HintedUncachedSignal,
}

/// A compositional device that merges its children's readings, configuration,
/// staging and hints. Register children with [`add_readables`] /
/// [`add_stageable`]; the trait impls then expose the aggregate.
///
/// [`add_readables`]: StandardReadable::add_readables
/// [`add_stageable`]: StandardReadable::add_stageable
pub struct StandardReadable {
    name: String,
    read: Vec<Arc<dyn ReadableObj>>,
    config: Vec<Arc<dyn ReadableObj>>,
    stageables: Vec<Arc<dyn StageableObj>>,
    hints: Vec<String>,
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

    /// Register a readable child, routing it to the buckets implied by
    /// `format`. A child is always read via its [`ReadableObj`] surface; the
    /// format decides whether its reading lands in `read` or in
    /// `read_configuration`, and whether its fields become hints.
    ///
    /// For [`Child`](StandardReadableFormat::Child), hints are auto-detected
    /// from the child's [`ReadableObj::hint_fields`]. For the explicit
    /// `Hinted*` formats, the child's declared hint fields are used if present,
    /// otherwise the child's own name (a signal reads as `{name: value}`).
    pub fn add_readables(&mut self, child: Arc<dyn ReadableObj>, format: StandardReadableFormat) {
        use StandardReadableFormat::*;
        match format {
            ConfigSignal => self.config.push(child),
            UncachedSignal => self.read.push(child),
            Child => {
                if let Some(fields) = child.hint_fields() {
                    self.hints.extend(fields);
                }
                self.read.push(child);
            }
            HintedSignal | HintedUncachedSignal => {
                match child.hint_fields() {
                    Some(fields) => self.hints.extend(fields),
                    None => self.hints.push(child.name().to_string()),
                }
                self.read.push(child);
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

#[async_trait]
impl AsyncReadable for StandardReadable {
    fn name(&self) -> &str {
        &self.name
    }
    async fn read(&self) -> Result<HashMap<String, ReadingValue>> {
        let mut out = HashMap::new();
        for child in &self.read {
            out.extend(child.read_dyn().await?);
        }
        Ok(out)
    }
    async fn describe(&self) -> Result<HashMap<String, DataKey>> {
        let mut out = HashMap::new();
        for child in &self.read {
            out.extend(child.describe_dyn().await?);
        }
        Ok(out)
    }
}

#[async_trait]
impl AsyncConfigurable for StandardReadable {
    fn name(&self) -> &str {
        &self.name
    }
    async fn read_configuration(&self) -> Result<HashMap<String, ReadingValue>> {
        let mut out = HashMap::new();
        for child in &self.config {
            out.extend(child.read_dyn().await?);
        }
        Ok(out)
    }
    async fn describe_configuration(&self) -> Result<HashMap<String, DataKey>> {
        let mut out = HashMap::new();
        for child in &self.config {
            out.extend(child.describe_dyn().await?);
        }
        Ok(out)
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
    fn hint_fields(&self) -> Option<Vec<String>> {
        if self.hints.is_empty() {
            None
        } else {
            Some(self.hints.clone())
        }
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
    use crate::devices::{Signal, SignalConfig};
    use crate::event_model::Dtype;

    fn sig(name: &str, kind: Kind, v: f64) -> Arc<dyn ReadableObj> {
        Arc::new(Signal::<f64, SoftSignalBackend<f64>>::new(
            Arc::new(SoftSignalBackend::new(v, Dtype::Number)),
            SignalConfig {
                source: name.into(),
                kind,
                name: name.into(),
            },
        ))
    }

    // Boundary: each format routes to its bucket, and hints come from both
    // CHILD auto-detection (Kind::Hinted) and explicit HintedSignal (name
    // fallback when the child declares no hint fields).
    #[tokio::test]
    async fn aggregates_read_config_and_hints() {
        let mut sr = StandardReadable::new("dev");
        // CHILD + Kind::Hinted → read bucket + auto hint "temp".
        sr.add_readables(
            sig("temp", Kind::Hinted, 1.0),
            StandardReadableFormat::Child,
        );
        // HintedSignal + Kind::Normal → read bucket + explicit hint "x"
        // (name fallback, since a Normal signal declares no hint fields).
        sr.add_readables(
            sig("x", Kind::Normal, 2.0),
            StandardReadableFormat::HintedSignal,
        );
        // ConfigSignal → configuration bucket only.
        sr.add_readables(
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
