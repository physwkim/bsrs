//! `RunBundler` — owns per-run state, emits descriptors and events as plans
//! call `create / read / save` etc.

use crate::core::error::{BsrsError, Result};
use crate::core::reading::ReadingValue;
use crate::event_model::compose::RunBundle;
use crate::event_model::{Configuration, DataKey, Document, EventDescriptor, PerObjectHint};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

/// One object's contribution to a stream descriptor: bluesky
/// `_prepare_stream`'s `objs_dks` entry together with the per-object caches
/// it reads (`hints`, `config_*_cache`, bundlers.py:267-290). `object` is
/// `None` for data keys that belong to no object — the engine's own
/// `interruptions` key, or a raw `Msg::DeclareStream` key with no
/// `object_name` — which join the descriptor's `data_keys` and nothing else.
pub struct StreamObject {
    /// The object's name; `None` for object-less keys.
    pub object: Option<String>,
    /// The object's `describe()` (or `describe_collect()` slice).
    pub data_keys: HashMap<String, DataKey>,
    /// The object's hinted fields (`ReadableObj::hint_fields`).
    pub hint_fields: Option<Vec<String>>,
    /// The object's configuration, empty for non-configurables.
    pub configuration: Configuration,
}

impl StreamObject {
    /// Regroup bare data keys by their `object_name` annotation (bsrs devices
    /// stamp it in `describe_dyn`), so a stream declared from raw keys still
    /// lists each object's keys the way bluesky's object-driven
    /// `declare_stream` does. Keys without one form the object-less entry.
    pub fn from_data_keys(data_keys: HashMap<String, DataKey>) -> Vec<Self> {
        let mut by_object: BTreeMap<Option<String>, HashMap<String, DataKey>> = BTreeMap::new();
        for (key, dk) in data_keys {
            by_object
                .entry(dk.object_name.clone())
                .or_default()
                .insert(key, dk);
        }
        by_object
            .into_iter()
            .map(|(object, data_keys)| Self {
                object,
                data_keys,
                hint_fields: None,
                configuration: Configuration::default(),
            })
            .collect()
    }
}

/// State of one open bundle (between `create` and `save`/`drop`).
///
/// Everything the first `save` of a stream shapes its descriptor from lives
/// *here*, per bundle — not on the `RunBundler` — so a `drop` discards it
/// with the bundle and it cannot leak into the next bundle's descriptor.
/// This mirrors bluesky, which builds each descriptor from the per-event
/// `_objs_read` / `read_cache`, both reset on the next `create`
/// (bundlers.py:357,385); a dropped bundle's reads never reach the next
/// descriptor.
struct OpenBundle {
    stream_name: String,
    readings: HashMap<String, ReadingValue>,
    /// The objects read into this bundle, in read order — bluesky's
    /// `_objs_read` deque with the per-object caches `save` folds into a
    /// first descriptor (bundlers.py:600-607). Non-empty once a `Read` has
    /// landed: a `save` with no preceding `read` emits no Event
    /// (bundlers.py:570-573).
    objs: Vec<StreamObject>,
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
            objs: Vec::new(),
        });
        Ok(())
    }

    /// Fold one `Read` of one object into the open bundle: its `readings`
    /// for the Event, and the object itself for the descriptor the stream's
    /// first `save` composes.
    pub fn add_read(
        &mut self,
        obj: StreamObject,
        readings: HashMap<String, ReadingValue>,
    ) -> Result<()> {
        let bundle = self
            .open
            .as_mut()
            .ok_or_else(|| BsrsError::Plan("read with no open bundle".into()))?;
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
        bundle.readings.extend(readings);
        bundle.objs.push(obj);
        Ok(())
    }

    /// The one place a descriptor's `data_keys`, `object_keys`, `hints` and
    /// `configuration` are shaped from the objects behind a stream — bluesky
    /// `_prepare_stream` (bundlers.py:267-303). Every key is stamped with its
    /// object's name; `object_keys[obj]` lists that object's keys (bluesky
    /// keeps `describe()` order, bsrs's maps have none, so they are sorted);
    /// `hints[obj].fields` comes from the object's hint fields and
    /// `configuration[obj]` from its configuration. Both the first `save` of
    /// a stream and `declare_stream` build their descriptor here, so a
    /// collect, monitor or pre-declared stream describes its objects the
    /// same way a `read`/`save` stream does.
    fn prepare_stream(&mut self, stream_name: String, objs: Vec<StreamObject>) -> EventDescriptor {
        let mut data_keys = HashMap::new();
        let mut object_keys = HashMap::new();
        let mut hints: HashMap<String, PerObjectHint> = HashMap::new();
        let mut configuration = HashMap::new();
        for obj in objs {
            let Some(name) = obj.object else {
                data_keys.extend(obj.data_keys);
                continue;
            };
            let mut keys: Vec<String> = obj.data_keys.keys().cloned().collect();
            keys.sort();
            object_keys.insert(name.clone(), keys);
            for (key, mut dk) in obj.data_keys {
                dk.object_name = Some(name.clone());
                data_keys.insert(key, dk);
            }
            if let Some(fields) = obj.hint_fields {
                hints.entry(name.clone()).or_default().fields = Some(fields);
            }
            configuration.insert(name, obj.configuration);
        }
        let hints = (!hints.is_empty()).then_some(hints);
        let (descriptor, _new) =
            self.bundle
                .descriptor(&stream_name, data_keys, configuration, hints, object_keys);
        self.descriptors.insert(
            stream_name,
            DescriptorState {
                uid: descriptor.uid.clone(),
            },
        );
        descriptor
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
        if bundle.objs.is_empty() {
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
            let descriptor =
                self.prepare_stream(stream_name.clone(), std::mem::take(&mut bundle.objs));
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

    /// Pre-declare a stream from the objects behind it (bluesky
    /// `declare_stream` → `_prepare_stream`, bundlers.py:325-352): collect
    /// and monitor streams, `Msg::DeclareStream`, the interruptions stream.
    pub fn declare_stream(
        &mut self,
        stream_name: String,
        objs: Vec<StreamObject>,
    ) -> EventDescriptor {
        self.prepare_stream(stream_name, objs)
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
