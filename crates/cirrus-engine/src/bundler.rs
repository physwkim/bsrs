//! `RunBundler` — owns per-run state, emits descriptors and events as plans
//! call `create / read / save` etc.

use cirrus_core::error::{CirrusError, Result};
use cirrus_core::reading::ReadingValue;
use cirrus_event_model::compose::RunBundle;
use cirrus_event_model::{Configuration, DataKey, Document, EventDescriptor, PerObjectHint};
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
    /// Whether at least one `Read` has been folded into this bundle. The
    /// cirrus equivalent of bluesky's `_objs_read` non-emptiness: a `save`
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
    /// Configuration accumulated for the next descriptor.
    pending_config: HashMap<String, Configuration>,
    /// Snapshot of per-stream sequence counters taken at the last checkpoint,
    /// used to roll them back on `rewind` so a replayed `save` re-emits the same
    /// `seq_num`. `None` when no checkpoint region is active. bluesky
    /// `RunBundler._sequence_counters_copy` (bundlers.py:167).
    seq_snapshot: Option<HashMap<String, u64>>,
}

impl RunBundler {
    /// Build with an existing run-start UID and a shared `RunBundle`.
    pub fn new(bundle: Arc<RunBundle>) -> Self {
        Self {
            start_uid: bundle.start_uid().to_string(),
            bundle,
            descriptors: HashMap::new(),
            open: None,
            pending_config: HashMap::new(),
            seq_snapshot: None,
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
            return Err(CirrusError::Plan(
                "create called while a previous bundle is still open".into(),
            ));
        }
        self.open = Some(OpenBundle {
            stream_name,
            readings: HashMap::new(),
            data_keys: HashMap::new(),
            object_keys: HashMap::new(),
            hints: None,
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
            .ok_or_else(|| CirrusError::Plan("read with no open bundle".into()))?;
        bundle.had_read = true;
        // Reject colliding field names within one event bundle. Two reads in the
        // same create/save that share a data key would silently overwrite each
        // other (last write wins), dropping one object's reading and leaving the
        // descriptor inconsistent with the event. bluesky raises ValueError on
        // this collision (bundlers.py:422-433); mirror that with an explicit
        // error instead of the silent HashMap overwrite.
        if let Some(k) = readings.keys().find(|k| bundle.readings.contains_key(*k)) {
            return Err(CirrusError::Plan(format!(
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

    /// Save the open bundle as documents. Emits a Descriptor on first save
    /// per stream, then an Event.
    pub fn save(&mut self) -> Result<Vec<Document>> {
        let mut bundle = self
            .open
            .take()
            .ok_or_else(|| CirrusError::Plan("save with no open bundle".into()))?;
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
                std::mem::take(&mut self.pending_config),
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
            .ok_or_else(|| CirrusError::Plan("event for unknown stream".into()))?;
        out.push(Document::Event(ev));
        Ok(out)
    }

    /// Whether an event bundle is currently open — after `create`, before the
    /// paired `save`/`drop`/`rewind`. The cirrus equivalent of bluesky's
    /// `RunBundler.bundling` flag (bundlers.py:147, set on `create`:386,
    /// cleared on `save`/`drop`/`rewind`:533/573/584). Used to reject an
    /// illegal `checkpoint` issued inside an open bundle.
    pub fn is_bundling(&self) -> bool {
        self.open.is_some()
    }

    /// Discard the open bundle.
    pub fn drop_bundle(&mut self) -> Result<()> {
        if self.open.take().is_none() {
            return Err(CirrusError::Plan("drop with no open bundle".into()));
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

    /// Pre-declare a stream (fly scans).
    pub fn declare_stream(
        &mut self,
        stream_name: String,
        data_keys: HashMap<String, DataKey>,
    ) -> Result<EventDescriptor> {
        let (descriptor, _new) = self.bundle.descriptor(
            &stream_name,
            data_keys,
            HashMap::new(),
            None,
            HashMap::new(),
        );
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
