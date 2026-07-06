//! `RunBundler` — owns per-run state, emits descriptors and events as plans
//! call `create / read / save` etc.

use crate::core::error::{BsrsError, Result};
use crate::core::reading::ReadingValue;
use crate::event_model::compose::RunBundle;
use crate::event_model::{Configuration, DataKey, Document, EventDescriptor, PerObjectHint};
use std::collections::HashMap;
use std::sync::Arc;

/// State of one open bundle (between `create` and `save`/`drop`).
///
/// All descriptor-shaping accumulators (`data_keys`, `object_keys`, `hints`)
/// live *here*, per bundle — not on the `RunBundler` — so a `drop` discards
/// them with the bundle and they cannot leak into the next bundle's
/// descriptor. This mirrors bluesky, which builds each descriptor from the
/// per-event `_objs_read` / `read_cache`, both reset on the next `create`
/// (bundlers.py:357,385); a dropped bundle's reads never reach the next
/// descriptor.
struct OpenBundle {
    stream_name: String,
    readings: HashMap<String, ReadingValue>,
    /// Data keys accumulated from this bundle's `Read`s, used to synthesize
    /// the stream descriptor at `save`.
    data_keys: HashMap<String, DataKey>,
    /// Object → field-list mapping accumulated for this bundle's descriptor.
    object_keys: HashMap<String, Vec<String>>,
    /// Object → fields hint accumulator for this bundle's descriptor.
    hints: Option<HashMap<String, PerObjectHint>>,
    /// Per-object configuration accumulated from this bundle's `Read`s,
    /// keyed by object name — one entry per object read, empty for
    /// non-configurable objects. Folded into the descriptor at `save`
    /// (bluesky `_prepare_stream` builds `config[obj.name]` from the
    /// stream cache, bundlers.py:286-290).
    config: HashMap<String, Configuration>,
    /// Whether at least one `Read` has been folded into this bundle. The
    /// bsrs equivalent of bluesky's `_objs_read` non-emptiness: a `save`
    /// with no preceding `read` emits no Event (bundlers.py:570-573).
    had_read: bool,
}

/// Per-stream descriptor cache entry.
#[derive(Clone, Default)]
struct DescriptorState {
    uid: String,
}

/// Per-run bundler. Lives inside the RunEngine.
pub struct RunBundler {
    bundle: Arc<RunBundle>,
    /// Per-stream descriptor cache, keyed by stream name.
    descriptors: HashMap<String, DescriptorState>,
    /// Currently open event bundle, if any.
    open: Option<OpenBundle>,
    /// Run start UID.
    pub start_uid: String,
    /// Run-scoped per-object configuration cache, keyed by object name: the
    /// engine reads an object's configuration once per run (at its first
    /// bundled read / declare) and re-reads only on `Msg::Configure`,
    /// mirroring bluesky's `ensure_cached` config caches
    /// (`_StreamCache.config_*_cache`, bundlers.py:85-130). bsrs keeps one
    /// run-wide cache where bluesky keeps one per stream — the values only
    /// change via `configure`, which updates this cache, so the per-stream
    /// split buys nothing here.
    config_cache: HashMap<String, Configuration>,
    /// Snapshot of per-stream sequence counters taken at the last checkpoint,
    /// used to roll them back on `rewind` so a replayed `save` re-emits the same
    /// `seq_num`. `None` when no checkpoint region is active. bluesky
    /// `RunBundler._sequence_counters_copy` (bundlers.py:167).
    seq_snapshot: Option<HashMap<String, u64>>,
    /// Every `StreamResource` uid emitted this run → its `data_key`. The
    /// engine's asset-drain validation records resources here and requires
    /// each later `StreamDatum` to reference a known uid. Run-scoped and
    /// never rewound — a datum after a rewind still legitimately references
    /// the resource emitted before it. bluesky
    /// `RunBundler._stream_resource_data_keys`.
    stream_resource_data_keys: HashMap<String, String>,
}

impl RunBundler {
    /// Build with an existing run-start UID and a shared `RunBundle`.
    pub fn new(bundle: Arc<RunBundle>) -> Self {
        Self {
            start_uid: bundle.start_uid().to_string(),
            bundle,
            descriptors: HashMap::new(),
            open: None,
            config_cache: HashMap::new(),
            seq_snapshot: None,
            stream_resource_data_keys: HashMap::new(),
        }
    }

    /// The run's `StreamResource` uid → `data_key` registry, for the engine's
    /// asset-drain validation (see the field doc).
    pub fn stream_resource_data_keys_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.stream_resource_data_keys
    }

    /// Look up the run's cached configuration for `object_name` (see the
    /// `config_cache` field doc).
    pub fn cached_configuration(&self, object_name: &str) -> Option<Configuration> {
        self.config_cache.get(object_name).cloned()
    }

    /// Insert or replace the run's cached configuration for `object_name` —
    /// called at an object's first bundled read/declare, and again from
    /// `Msg::Configure` so future descriptors carry the new values (bluesky
    /// `RunBundler.configure` re-runs `cache_read_config`, bundlers.py:1209).
    pub fn cache_configuration(&mut self, object_name: String, config: Configuration) {
        self.config_cache.insert(object_name, config);
    }

    /// Build — but do **not** install — the next descriptor generation for
    /// every declared stream whose current descriptor includes `object_name`,
    /// carrying `configuration` freshly read after a `configure`. Returns the
    /// candidate descriptors (each names its own stream) for the engine to
    /// broadcast; the streams' current descriptors and the local uid cache are
    /// untouched until [`install_reconfigured`](RunBundler::install_reconfigured)
    /// is called with the broadcast result. Splitting compose from install lets
    /// the engine emit each new descriptor before it becomes the generation a
    /// concurrent monitor pump would stamp onto an event, so
    /// descriptor-before-event holds by construction. Ports bluesky
    /// `RunBundler.configure`'s invalidation loop (bundlers.py:1213-1218).
    pub fn compose_reconfigure(
        &self,
        object_name: &str,
        configuration: Configuration,
    ) -> Vec<EventDescriptor> {
        let mut out = Vec::new();
        for name in self.descriptors.keys() {
            if let Some(desc) =
                self.bundle
                    .compose_redescribe(name, object_name, configuration.clone())
            {
                out.push(desc);
            }
        }
        out
    }

    /// Install the descriptors returned by
    /// [`compose_reconfigure`](RunBundler::compose_reconfigure) — call this only
    /// after they have all been broadcast. Swaps each stream's current
    /// descriptor (`RunBundle::streams`, new uid, same `seq_num`) and the local
    /// uid cache (`self.descriptors`) in lockstep — the single point that keeps
    /// the two descriptor caches in sync, so no later `descriptor_uid` lookup
    /// can return a stale generation.
    pub fn install_reconfigured(&mut self, descriptors: &[EventDescriptor]) {
        for desc in descriptors {
            let Some(name) = desc.name.clone() else {
                continue;
            };
            self.bundle.install_descriptor(&name, desc.clone());
            self.descriptors.insert(
                name,
                DescriptorState {
                    uid: desc.uid.clone(),
                },
            );
        }
    }

    /// Snapshot the current per-stream sequence counters as the rewind target.
    /// Called at every checkpoint reset (the `Checkpoint` message plus the
    /// stage/unstage/monitor/subscribe lifecycle handlers), mirroring bluesky's
    /// `RunBundler.reset_checkpoint_state` (bundlers.py:651-656).
    pub fn reset_checkpoint_state(&mut self) {
        self.seq_snapshot = Some(self.bundle.snapshot_seq_nums());
    }

    /// Drop the rewind target — the checkpoint region is being cleared, so there
    /// is nothing to roll back to. bluesky `clear_checkpoint` clears
    /// `_sequence_counters_copy` (bundlers.py:669-670).
    pub fn clear_checkpoint(&mut self) {
        self.seq_snapshot = None;
    }

    /// Begin a new event bundle for `stream_name`.
    pub fn create(&mut self, stream_name: String) -> Result<()> {
        if self.open.is_some() {
            return Err(BsrsError::Plan(
                "create called while a previous bundle is still open".into(),
            ));
        }
        self.open = Some(OpenBundle {
            stream_name,
            readings: HashMap::new(),
            data_keys: HashMap::new(),
            object_keys: HashMap::new(),
            hints: None,
            config: HashMap::new(),
            had_read: false,
        });
        Ok(())
    }

    /// Add readings (from a single `Read` of one device) to the open bundle.
    pub fn add_readings(
        &mut self,
        readings: HashMap<String, ReadingValue>,
        data_keys: HashMap<String, DataKey>,
        object_name: Option<String>,
        hint_fields: Option<Vec<String>>,
    ) -> Result<()> {
        let bundle = self
            .open
            .as_mut()
            .ok_or_else(|| BsrsError::Plan("read with no open bundle".into()))?;
        bundle.had_read = true;
        // Reject colliding field names within one event bundle. Two reads in the
        // same create/save that share a data key would silently overwrite each
        // other (last write wins), dropping one object's reading and leaving the
        // descriptor inconsistent with the event. bluesky raises ValueError on
        // this collision (bundlers.py:422-433); mirror that with an explicit
        // error instead of the silent HashMap overwrite.
        if let Some(k) = readings.keys().find(|k| bundle.readings.contains_key(*k)) {
            return Err(BsrsError::Plan(format!(
                "Data keys (field names) collide in the open event: '{k}'"
            )));
        }
        for (k, v) in readings {
            bundle.readings.insert(k, v);
        }
        // Stash data keys on the bundle for descriptor synthesis at save time.
        // Per-bundle (not RunBundler-level) so a `drop` discards them.
        for (k, v) in data_keys {
            bundle.data_keys.insert(k, v);
        }
        // Hints + object_keys, likewise per-bundle.
        if let (Some(obj), Some(fields)) = (object_name, hint_fields) {
            bundle.object_keys.insert(obj.clone(), fields.clone());
            let hint_map = bundle.hints.get_or_insert_with(HashMap::new);
            hint_map.entry(obj).or_default().fields = Some(fields);
        }
        Ok(())
    }

    /// Record `object_name`'s configuration on the open bundle, for the
    /// descriptor synthesized at `save`. Called by the engine alongside
    /// [`RunBundler::add_readings`] for every bundled read — with an empty
    /// [`Configuration`] for non-configurable objects, matching bluesky's
    /// per-object `config[obj.name]` entries (bundlers.py:286-290).
    pub fn add_configuration(&mut self, object_name: String, config: Configuration) -> Result<()> {
        let bundle = self
            .open
            .as_mut()
            .ok_or_else(|| BsrsError::Plan("read with no open bundle".into()))?;
        bundle.config.insert(object_name, config);
        Ok(())
    }

    /// Save the open bundle as documents. Emits a Descriptor on first save
    /// per stream, then an Event.
    pub fn save(&mut self) -> Result<Vec<Document>> {
        let mut bundle = self
            .open
            .take()
            .ok_or_else(|| BsrsError::Plan("save with no open bundle".into()))?;
        // Short-circuit an empty bundle: a `create`/`save` pair with no
        // intervening `read` emits no Event and no Descriptor. Taking `open`
        // above already closed the bundle (bundling=false), matching bluesky's
        // `save`, which sets bundling=False and returns early when nothing was
        // read (bundlers.py:570-573, "Do not create empty Events.").
        if !bundle.had_read {
            return Ok(Vec::new());
        }
        let stream_name = bundle.stream_name.clone();
        let mut out = Vec::new();

        let needs_descriptor = self
            .descriptors
            .get(&stream_name)
            .map(|d| d.uid.is_empty())
            .unwrap_or(true);
        if needs_descriptor {
            let (descriptor, _new) = self.bundle.descriptor(
                &stream_name,
                std::mem::take(&mut bundle.data_keys),
                std::mem::take(&mut bundle.config),
                bundle.hints.take(),
                std::mem::take(&mut bundle.object_keys),
            );
            self.descriptors.insert(
                stream_name.clone(),
                DescriptorState {
                    uid: descriptor.uid.clone(),
                },
            );
            out.push(Document::Descriptor(descriptor));
        }

        let mut data = HashMap::new();
        let mut timestamps = HashMap::new();
        for (k, r) in bundle.readings {
            data.insert(k.clone(), r.value);
            timestamps.insert(k, r.timestamp);
        }
        let ev = self
            .bundle
            .event(&stream_name, data, timestamps)
            .ok_or_else(|| BsrsError::Plan("event for unknown stream".into()))?;
        out.push(Document::Event(ev));
        Ok(out)
    }

    /// Whether an event bundle is currently open — after `create`, before the
    /// paired `save`/`drop`/`rewind`. The bsrs equivalent of bluesky's
    /// `RunBundler.bundling` flag (bundlers.py:147, set on `create`:386,
    /// cleared on `save`/`drop`/`rewind`:533/573/584). Used to reject an
    /// illegal `checkpoint` issued inside an open bundle.
    pub fn is_bundling(&self) -> bool {
        self.open.is_some()
    }

    /// Stream name of the currently open event bundle, if any. Lets the engine
    /// look up the stream's descriptor UID to stamp the bundle's external-asset
    /// docs at `save` — captured *before* `save` consumes the open bundle.
    pub fn open_stream_name(&self) -> Option<String> {
        self.open.as_ref().map(|b| b.stream_name.clone())
    }

    /// Discard the open bundle.
    pub fn drop_bundle(&mut self) -> Result<()> {
        if self.open.take().is_none() {
            return Err(BsrsError::Plan("drop with no open bundle".into()));
        }
        Ok(())
    }

    /// Roll back checkpoint state before the rewind cache is replayed on
    /// resume. Mirrors bluesky's `RunBundler.rewind` (bundlers.py:520-533):
    /// cancel any bundle left open (created but not yet saved) when the pause
    /// landed mid-event — after `create`, before the paired `save`. Without
    /// this, the replayed `Create` collides with the still-open bundle and
    /// `create` errors with "create called while a previous bundle is still
    /// open", aborting the run on resume. The replay re-issues `Create` (now
    /// against `open == None`) and the cached `Read`s, so the bundle and its
    /// readings are faithfully rebuilt.
    ///
    /// It also rolls the per-stream sequence counters back to the snapshot taken
    /// at the last checkpoint (via [`RunBundler::reset_checkpoint_state`]), so a
    /// `save` replayed after a post-`save` pause re-emits the *same* `seq_num`
    /// instead of advancing past it. Streams declared after the checkpoint roll
    /// back to 0. Mirrors bluesky restoring `_sequence_counters` from the copy
    /// (bundlers.py:520-528).
    pub fn rewind(&mut self) {
        self.open = None;
        if let Some(snap) = self.seq_snapshot.as_ref() {
            self.bundle.restore_seq_nums(snap);
        }
    }

    /// Pre-declare a stream (fly scans). `configuration` carries the
    /// declaring object(s)' per-name configuration, read by the engine
    /// (bluesky `declare_stream` → `_prepare_stream` folds it the same way,
    /// bundlers.py:318-352); empty when no object is available to read from.
    pub fn declare_stream(
        &mut self,
        stream_name: String,
        data_keys: HashMap<String, DataKey>,
        configuration: HashMap<String, Configuration>,
    ) -> Result<EventDescriptor> {
        let (descriptor, _new) =
            self.bundle
                .descriptor(&stream_name, data_keys, configuration, None, HashMap::new());
        self.descriptors.insert(
            stream_name,
            DescriptorState {
                uid: descriptor.uid.clone(),
            },
        );
        Ok(descriptor)
    }

    /// Underlying compose handle.
    pub fn compose(&self) -> &RunBundle {
        &self.bundle
    }

    /// Clone the underlying `RunBundle` for use in spawned tasks (monitor
    /// pumps, etc.) that need to compose Events for *already-declared*
    /// streams. The pump must not race with `Save` / `Drop` for the
    /// primary bundle.
    pub fn bundle(&self) -> Arc<RunBundle> {
        self.bundle.clone()
    }

    /// Look up an already-emitted descriptor UID.
    pub fn descriptor_uid(&self, stream_name: &str) -> Option<String> {
        self.descriptors
            .get(stream_name)
            .map(|d| d.uid.clone())
            .filter(|s| !s.is_empty())
    }
}
