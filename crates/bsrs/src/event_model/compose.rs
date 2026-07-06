//! Compose helpers — UID generation, descriptor caching, sequence-number bookkeeping.
//!
//! Mirrors `event_model.compose_*` (`__init__.py:1852-2528`).

use crate::event_model::documents::*;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// Returns the current Unix epoch time in seconds.
pub fn now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or_default()
}

/// Generates a fresh v4 UUID hex string.
pub fn new_uid() -> String {
    Uuid::new_v4().to_string()
}

/// Per-run composer: caches one descriptor per stream name, increments seq nums.
#[derive(Debug)]
pub struct RunBundle {
    start_uid: String,
    streams: Mutex<HashMap<String, StreamState>>,
}

#[derive(Debug)]
struct StreamState {
    /// The stream's *current* descriptor. Minted on the first
    /// [`RunBundle::descriptor`] call for the stream name and returned
    /// unchanged on every later `descriptor()` call, so the uid that
    /// `event()`/`stop()` stamp always matches the descriptor that was
    /// emitted (CBEM-21). It is replaced — a new generation, new uid, same
    /// `seq_num` — only by [`RunBundle::install_descriptor`] on a configuration
    /// change (bluesky re-emits a stream's descriptor when a member object is
    /// reconfigured). The new generation is built by
    /// [`RunBundle::compose_redescribe`] first and installed here only after it
    /// has been broadcast, so a concurrent monitor pump can never compose an
    /// `event()` that references a descriptor not yet on the wire (see those
    /// methods). Within one generation the uid is still immutable; the
    /// invariant is "one *current* descriptor per stream", not "one descriptor
    /// per stream for the whole run".
    descriptor: EventDescriptor,
    seq_num: AtomicU64,
}

impl RunBundle {
    /// Construct from an existing `RunStart` document.
    pub fn open(start: &RunStart) -> Self {
        Self {
            start_uid: start.uid.clone(),
            streams: Mutex::new(HashMap::new()),
        }
    }

    /// Compose a `RunStart` document for a new run.
    pub fn start(scan_id: Option<u64>, hints: Option<Hints>) -> RunStart {
        RunStart {
            uid: new_uid(),
            time: now(),
            scan_id,
            hints,
            ..Default::default()
        }
    }

    /// Compose a stream descriptor. The first call for a given stream name mints
    /// the stream's one and only descriptor and returns it with `is_new == true`.
    /// Every later call for the same name returns that same cached descriptor
    /// unchanged with `is_new == false` — its uid therefore always matches the
    /// uid that `event()` stamps on the stream's events and that
    /// `descriptor_uid_for()`/`stop()` report.
    ///
    /// A stream's *schema* is fixed at first declaration: re-composing with a
    /// different `data_keys` shape returns the original descriptor (first
    /// definition wins). Only the `configuration` sub-document changes over a
    /// stream's life, and only through
    /// [`compose_redescribe`](RunBundle::compose_redescribe) +
    /// [`install_descriptor`](RunBundle::install_descriptor) — a caller that
    /// needs a different `data_keys` schema must use a different stream name.
    pub fn descriptor(
        &self,
        name: &str,
        data_keys: HashMap<String, DataKey>,
        configuration: HashMap<String, Configuration>,
        hints: Option<HashMap<String, PerObjectHint>>,
        object_keys: HashMap<String, Vec<String>>,
    ) -> (EventDescriptor, bool) {
        let mut streams = self.streams.lock().unwrap();
        if let Some(st) = streams.get(name) {
            return (st.descriptor.clone(), false);
        }
        let descriptor = EventDescriptor {
            uid: new_uid(),
            run_start: self.start_uid.clone(),
            time: now(),
            data_keys,
            configuration,
            name: Some(name.to_string()),
            hints,
            object_keys,
        };
        streams.insert(
            name.to_string(),
            StreamState {
                descriptor: descriptor.clone(),
                seq_num: AtomicU64::new(0),
            },
        );
        (descriptor, true)
    }

    /// Build — but do **not** install — the next generation of a stream's
    /// descriptor, carrying fresh `configuration` for `object_name` and keeping
    /// every other field (`data_keys`, `hints`, `object_keys`, `name`). Mints a
    /// new uid. Returns the candidate descriptor, or `None` when the stream is
    /// undeclared or its current descriptor does not include `object_name`
    /// (nothing to reconfigure — a raw-`data_keys` stream with no objects, or a
    /// stream this object never joined).
    ///
    /// This is a pure read: the stream's *current* descriptor is unchanged, so
    /// any `event()` composed between `compose_redescribe` and
    /// [`RunBundle::install_descriptor`] still references the OLD generation.
    /// The caller must broadcast the returned descriptor and only then install
    /// it, which makes "descriptor-on-the-wire before any event that references
    /// it" hold by construction even against a concurrent monitor pump — the
    /// pump can compose an `event()` referencing the new generation only after
    /// `install_descriptor`, by which point the descriptor is already emitted.
    ///
    /// Ports bluesky's configure-time invalidation: `del self._descriptors[name]`
    /// then `_prepare_stream(name, obj_set)`, whose sequence counter is left
    /// intact because `desc_key` is already in `_sequence_counters`
    /// (bundlers.py:1213-1218, 308-310).
    pub fn compose_redescribe(
        &self,
        stream_name: &str,
        object_name: &str,
        configuration: Configuration,
    ) -> Option<EventDescriptor> {
        let streams = self.streams.lock().unwrap();
        let st = streams.get(stream_name)?;
        if !st.descriptor.configuration.contains_key(object_name) {
            return None;
        }
        let mut new_desc = st.descriptor.clone();
        new_desc.uid = new_uid();
        new_desc.time = now();
        new_desc
            .configuration
            .insert(object_name.to_string(), configuration);
        Some(new_desc)
    }

    /// Install `descriptor` as `stream_name`'s current generation — the one
    /// sanctioned mutation of a live descriptor besides first declaration. Keeps
    /// the stream's `seq_num`, so events before and after the change share one
    /// monotonic sequence axis (bluesky keeps the counter: `_prepare_stream`
    /// does not reset an existing `desc_key`, bundlers.py:308). No-op if the
    /// stream was never declared. Must be called only after `descriptor` has
    /// been broadcast (see [`RunBundle::compose_redescribe`]).
    pub fn install_descriptor(&self, stream_name: &str, descriptor: EventDescriptor) {
        let mut streams = self.streams.lock().unwrap();
        if let Some(st) = streams.get_mut(stream_name) {
            st.descriptor = descriptor;
        }
    }

    /// Compose an `Event` document for a stream that already has a descriptor.
    /// Returns `None` if the stream name was never declared.
    pub fn event(
        &self,
        stream_name: &str,
        data: HashMap<String, Value>,
        timestamps: HashMap<String, f64>,
    ) -> Option<Event> {
        let streams = self.streams.lock().unwrap();
        let st = streams.get(stream_name)?;
        let n = st.seq_num.fetch_add(1, Ordering::SeqCst) + 1;
        Some(Event {
            uid: new_uid(),
            descriptor: st.descriptor.uid.clone(),
            time: now(),
            seq_num: n,
            data,
            timestamps,
            filled: HashMap::new(),
        })
    }

    /// Compose an `EventPage` (bulk events) for `stream_name` from column-store
    /// `data`/`timestamps`, assigning `n` contiguous 1-based `seq_num`s from the
    /// stream's running counter (advancing it by `n`, so single `event()`s and
    /// pages share one sequence). Mirrors `ComposeRunBundle.compose_event_page`.
    /// Returns `None` if the stream is not declared. `n` is the row count; the
    /// caller ensures every `data`/`timestamps` column has `n` rows (matching
    /// the unvalidated [`ResourceComposer::datum_page`] contract).
    pub fn event_page(
        &self,
        stream_name: &str,
        data: HashMap<String, Vec<Value>>,
        timestamps: HashMap<String, Vec<f64>>,
        n: usize,
    ) -> Option<EventPage> {
        let streams = self.streams.lock().unwrap();
        let st = streams.get(stream_name)?;
        let start = st.seq_num.fetch_add(n as u64, Ordering::SeqCst);
        let seq_num = (1..=n as u64).map(|j| start + j).collect();
        let uid = (0..n).map(|_| new_uid()).collect();
        let time = vec![now(); n];
        Some(EventPage {
            uid,
            descriptor: st.descriptor.uid.clone(),
            time,
            seq_num,
            data,
            timestamps,
            filled: HashMap::new(),
        })
    }

    /// The `seq_num` the stream's next `Event` would receive (last used + 1),
    /// without advancing the counter. The engine reads this to stamp
    /// `StreamDatum.seq_nums` at the Collect drain — the sequence counter is
    /// the run's, never the writer's (bluesky
    /// `_pack_seq_nums_into_stream_datum`, bundlers.py:830). Returns `None`
    /// for an undeclared stream.
    pub fn peek_next_seq(&self, stream_name: &str) -> Option<u64> {
        let streams = self.streams.lock().unwrap();
        streams
            .get(stream_name)
            .map(|st| st.seq_num.load(Ordering::SeqCst) + 1)
    }

    /// Advance the stream's sequence counter by `n` without composing events.
    /// The Collect drain applies the indices width this way because no Event
    /// per frame exists to advance it ("Since there are no events or
    /// event_pages incrementing the sequence counter, we do it ourselves",
    /// bluesky bundlers.py:1180). Returns `false` for an undeclared stream.
    pub fn advance_seq(&self, stream_name: &str, n: u64) -> bool {
        let streams = self.streams.lock().unwrap();
        match streams.get(stream_name) {
            Some(st) => {
                st.seq_num.fetch_add(n, Ordering::SeqCst);
                true
            }
            None => false,
        }
    }

    /// The stream descriptor's external (`STREAM:`-prefixed) data keys — the
    /// keys whose data arrives via `StreamResource`/`StreamDatum` rather than
    /// inline in events. The engine validates drained asset documents against
    /// this set (bluesky `get_external_data_keys` /
    /// `_pack_external_assets`). Returns `None` for an undeclared stream.
    pub fn external_data_keys(&self, stream_name: &str) -> Option<Vec<String>> {
        let streams = self.streams.lock().unwrap();
        let st = streams.get(stream_name)?;
        Some(
            st.descriptor
                .data_keys
                .iter()
                .filter(|(_, dk)| {
                    dk.external
                        .as_deref()
                        .is_some_and(|e| e.starts_with("STREAM:"))
                })
                .map(|(k, _)| k.clone())
                .collect(),
        )
    }

    /// Snapshot every stream's current sequence counter. Paired with
    /// [`RunBundle::restore_seq_nums`] to implement checkpoint/rewind: the
    /// engine snapshots at each checkpoint and restores on a rewind so a
    /// replayed `save` re-emits the same `seq_num` instead of advancing past
    /// it. Mirrors bluesky's `RunBundler.reset_checkpoint_state` copying
    /// `_sequence_counters` into `_sequence_counters_copy` (bundlers.py:651-656).
    pub fn snapshot_seq_nums(&self) -> HashMap<String, u64> {
        let streams = self.streams.lock().unwrap();
        streams
            .iter()
            .map(|(name, st)| (name.clone(), st.seq_num.load(Ordering::SeqCst)))
            .collect()
    }

    /// Restore stream sequence counters from a [`RunBundle::snapshot_seq_nums`]
    /// snapshot. A stream present in `snapshot` rolls back to its snapshotted
    /// value; a stream declared *after* the snapshot was taken (absent from it)
    /// rolls back to 0 — from the checkpoint's vantage its events have not yet
    /// happened. Mirrors bluesky's `RunBundler.rewind`, which restores
    /// `_sequence_counters` from the copy and forces freshly-declared streams
    /// back to their start (bundlers.py:520-528).
    pub fn restore_seq_nums(&self, snapshot: &HashMap<String, u64>) {
        let streams = self.streams.lock().unwrap();
        for (name, st) in streams.iter() {
            let val = snapshot.get(name).copied().unwrap_or(0);
            st.seq_num.store(val, Ordering::SeqCst);
        }
    }

    /// Compose a `RunStop` document. Closes the bundle.
    pub fn stop(&self, exit_status: &str, reason: Option<String>) -> RunStop {
        let streams = self.streams.lock().unwrap();
        let mut num_events = HashMap::new();
        for (name, st) in streams.iter() {
            num_events.insert(name.clone(), st.seq_num.load(Ordering::SeqCst));
        }
        RunStop {
            uid: new_uid(),
            run_start: self.start_uid.clone(),
            time: now(),
            exit_status: exit_status
                .parse::<ExitStatus>()
                .expect("close_run_if_open normalizes exit_status to a schema-valid value"),
            reason,
            num_events,
            ..Default::default()
        }
    }

    /// Compose a `StreamResource` for a fly-style data path, returning a
    /// [`StreamResourceComposer`] that mints `StreamDatum`s into it. Mirrors
    /// `ComposeStreamResource`/`ComposeStreamResourceBundle`: the composer owns
    /// the resource doc and a monotone counter, so each `StreamDatum.uid` is
    /// `<stream_resource_uid>/<n>` by construction.
    pub fn stream_resource(
        &self,
        data_key: String,
        mimetype: String,
        uri: String,
        parameters: HashMap<String, Value>,
    ) -> StreamResourceComposer {
        StreamResourceComposer {
            stream_resource: StreamResource {
                uid: new_uid(),
                data_key,
                mimetype,
                uri,
                parameters,
                run_start: Some(self.start_uid.clone()),
            },
            counter: AtomicU64::new(0),
        }
    }

    /// Compose a legacy `Resource` and a `ResourceComposer` that mints
    /// `Datum`/`DatumPage` documents addressing rows inside it.
    ///
    /// Mirrors `ComposeRunBundle.compose_resource`: `run_start` is back-filled,
    /// `path_semantics` defaults to `posix` when unspecified, and the returned
    /// composer issues `datum_id`s of the form `<resource_uid>/<counter>`.
    pub fn resource(
        &self,
        spec: String,
        root: String,
        resource_path: String,
        path_semantics: Option<String>,
        resource_kwargs: HashMap<String, Value>,
    ) -> ResourceComposer {
        ResourceComposer {
            resource: Resource {
                uid: new_uid(),
                spec,
                root,
                resource_path,
                path_semantics: path_semantics.or_else(|| Some("posix".to_string())),
                resource_kwargs,
                run_start: Some(self.start_uid.clone()),
            },
            counter: AtomicU64::new(0),
        }
    }

    /// Get the run-start UID.
    pub fn start_uid(&self) -> &str {
        &self.start_uid
    }

    /// Lookup the descriptor UID for a stream, if declared.
    pub fn descriptor_uid_for(&self, stream_name: &str) -> Option<String> {
        self.streams
            .lock()
            .unwrap()
            .get(stream_name)
            .map(|s| s.descriptor.uid.clone())
    }
}

/// Mints `Datum`/`DatumPage` documents for one `Resource`, returned by
/// [`RunBundle::resource`]. Holds the composed `Resource` and a monotone
/// datum counter (`<resource_uid>/<n>`), mirroring the Python
/// `ComposeResourceBundle`.
#[derive(Debug)]
pub struct ResourceComposer {
    resource: Resource,
    counter: AtomicU64,
}

impl ResourceComposer {
    /// The `Resource` document to emit before any datum it owns.
    pub fn resource(&self) -> &Resource {
        &self.resource
    }

    /// Compose the next `Datum` pointing into this resource.
    pub fn datum(&self, datum_kwargs: HashMap<String, Value>) -> Datum {
        let i = self.counter.fetch_add(1, Ordering::SeqCst);
        Datum {
            datum_id: format!("{}/{}", self.resource.uid, i),
            resource: self.resource.uid.clone(),
            datum_kwargs,
        }
    }

    /// Compose a `DatumPage` of `n` rows from column-store `datum_kwargs`.
    pub fn datum_page(&self, datum_kwargs: HashMap<String, Vec<Value>>, n: usize) -> DatumPage {
        let start = self.counter.fetch_add(n as u64, Ordering::SeqCst);
        let datum_id = (0..n as u64)
            .map(|j| format!("{}/{}", self.resource.uid, start + j))
            .collect();
        DatumPage {
            datum_id,
            resource: self.resource.uid.clone(),
            datum_kwargs,
        }
    }
}

/// Mints `StreamDatum` documents for one `StreamResource`, returned by
/// [`RunBundle::stream_resource`]. Holds the composed `StreamResource` and a
/// monotone counter (`<stream_resource_uid>/<n>`), mirroring the Python
/// `ComposeStreamResourceBundle` / `ComposeStreamDatum`.
#[derive(Debug)]
pub struct StreamResourceComposer {
    stream_resource: StreamResource,
    counter: AtomicU64,
}

impl StreamResourceComposer {
    /// The `StreamResource` document to emit before any datum it owns.
    pub fn stream_resource(&self) -> &StreamResource {
        &self.stream_resource
    }

    /// Compose the next `StreamDatum` pointing into this stream resource. The
    /// `uid` is `<stream_resource_uid>/<counter>` (mirrors event_model
    /// `ComposeStreamDatum`, and the legacy `Datum.datum_id` form) — NOT a fresh
    /// random uid — so chunks are identifiable and ordered within the resource.
    pub fn stream_datum(
        &self,
        indices: StreamRange,
        seq_nums: StreamRange,
        descriptor_uid: String,
    ) -> StreamDatum {
        let i = self.counter.fetch_add(1, Ordering::SeqCst);
        StreamDatum {
            uid: format!("{}/{}", self.stream_resource.uid, i),
            stream_resource: self.stream_resource.uid.clone(),
            descriptor: descriptor_uid,
            indices,
            seq_nums,
        }
    }
}

/// Convenience: a thread-safe `Arc<RunBundle>`.
pub type SharedBundle = Arc<RunBundle>;

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal scalar `DataKey` for schema-shape tests.
    fn data_key(source: &str) -> DataKey {
        DataKey {
            source: source.to_string(),
            dtype: Dtype::Number,
            shape: vec![],
            dtype_numpy: None,
            external: None,
            units: None,
            precision: None,
            object_name: None,
            dims: None,
            limits: None,
            choices: None,
        }
    }

    // CBEM-21 invariant: for a stream name there is exactly one descriptor uid,
    // and the value `descriptor()` returns must equal the uid `event()`/
    // `descriptor_uid_for()` read — on the first call AND on every re-compose.

    #[test]
    fn first_compose_is_new_recompose_returns_cached_uid() {
        let start = RunBundle::start(Some(1), None);
        let bundle = RunBundle::open(&start);
        let (d1, new1) = bundle.descriptor(
            "primary",
            HashMap::new(),
            HashMap::new(),
            None,
            HashMap::new(),
        );
        assert!(new1, "first compose of a stream name is new");
        let (d2, new2) = bundle.descriptor(
            "primary",
            HashMap::new(),
            HashMap::new(),
            None,
            HashMap::new(),
        );
        assert!(!new2, "re-compose of an existing stream is not new");
        // The defect: re-compose must return the SAME uid, not a fresh one.
        assert_eq!(d1.uid, d2.uid, "re-compose must return the cached uid");
        // And that uid is exactly what event() stamps and descriptor_uid_for reports.
        let ev = bundle
            .event("primary", HashMap::new(), HashMap::new())
            .expect("stream declared");
        assert_eq!(ev.descriptor, d1.uid);
        assert_eq!(
            bundle.descriptor_uid_for("primary").as_deref(),
            Some(d1.uid.as_str())
        );
    }

    #[test]
    fn recompose_keeps_original_schema_and_seq_num() {
        let start = RunBundle::start(Some(1), None);
        let bundle = RunBundle::open(&start);
        let keys_a = HashMap::from([("x".to_string(), data_key("ca://X"))]);
        let (d1, _) = bundle.descriptor("primary", keys_a, HashMap::new(), None, HashMap::new());
        // Advance the seq counter before re-composing.
        bundle.event("primary", HashMap::new(), HashMap::new());
        bundle.event("primary", HashMap::new(), HashMap::new());
        // Re-compose with a DIFFERENT data_keys shape.
        let keys_b = HashMap::from([("y".to_string(), data_key("ca://Y"))]);
        let (d2, new2) = bundle.descriptor("primary", keys_b, HashMap::new(), None, HashMap::new());
        assert!(!new2);
        assert_eq!(d1.uid, d2.uid, "re-compose must not mint a new uid");
        // First definition wins: schema stays the original (x), not the re-composed (y).
        assert!(
            d2.data_keys.contains_key("x") && !d2.data_keys.contains_key("y"),
            "re-compose must keep the original schema, not adopt the new keys"
        );
        // Re-compose must NOT reset the stream's seq counter.
        let ev = bundle
            .event("primary", HashMap::new(), HashMap::new())
            .expect("stream declared");
        assert_eq!(ev.seq_num, 3, "re-compose must not reset the seq_num");
    }

    /// `peek_next_seq` reads without advancing; `advance_seq` moves the same
    /// counter `event()` uses, so datums filled by the Collect drain and the
    /// summary event that follows share one monotonic sequence axis.
    #[test]
    fn peek_and_advance_share_the_event_seq_counter() {
        let start = RunBundle::start(Some(1), None);
        let bundle = RunBundle::open(&start);
        assert_eq!(bundle.peek_next_seq("primary"), None, "undeclared stream");
        assert!(!bundle.advance_seq("primary", 3), "undeclared stream");
        bundle.descriptor(
            "primary",
            HashMap::new(),
            HashMap::new(),
            None,
            HashMap::new(),
        );
        assert_eq!(bundle.peek_next_seq("primary"), Some(1));
        assert_eq!(
            bundle.peek_next_seq("primary"),
            Some(1),
            "peek must not advance"
        );
        assert!(bundle.advance_seq("primary", 3));
        assert_eq!(bundle.peek_next_seq("primary"), Some(4));
        let ev = bundle
            .event("primary", HashMap::new(), HashMap::new())
            .expect("stream declared");
        assert_eq!(ev.seq_num, 4, "event continues after the advanced span");
    }

    #[test]
    fn distinct_stream_names_get_distinct_descriptors() {
        let start = RunBundle::start(Some(1), None);
        let bundle = RunBundle::open(&start);
        let (p, pnew) = bundle.descriptor(
            "primary",
            HashMap::new(),
            HashMap::new(),
            None,
            HashMap::new(),
        );
        let (b, bnew) = bundle.descriptor(
            "baseline",
            HashMap::new(),
            HashMap::new(),
            None,
            HashMap::new(),
        );
        assert!(pnew && bnew);
        assert_ne!(
            p.uid, b.uid,
            "different stream names get different descriptors"
        );
    }

    #[test]
    fn redescribe_swaps_config_new_uid_same_seq_num() {
        let start = RunBundle::start(Some(1), None);
        let bundle = RunBundle::open(&start);
        // A stream whose descriptor has a configuration entry for "cam".
        let mut config = HashMap::new();
        config.insert(
            "cam".to_string(),
            Configuration {
                data: HashMap::from([("cam_gain".to_string(), serde_json::Value::from(2.5))]),
                ..Default::default()
            },
        );
        let (v1, _) = bundle.descriptor("primary", HashMap::new(), config, None, HashMap::new());
        // Advance the counter, then reconfigure: seq must survive.
        bundle.advance_seq("primary", 4);
        let fresh = Configuration {
            data: HashMap::from([("cam_gain".to_string(), serde_json::Value::from(5.0))]),
            ..Default::default()
        };
        let v2 = bundle
            .compose_redescribe("primary", "cam", fresh)
            .expect("stream includes cam");
        assert_ne!(v1.uid, v2.uid, "compose_redescribe mints a new uid");
        assert_eq!(
            v2.configuration.get("cam").unwrap().data.get("cam_gain"),
            Some(&serde_json::Value::from(5.0)),
            "new generation carries the fresh config"
        );
        bundle.install_descriptor("primary", v2.clone());
        // The counter is preserved: the next event follows the advanced span,
        // and it references the new descriptor uid.
        assert_eq!(bundle.peek_next_seq("primary"), Some(5));
        let ev = bundle
            .event("primary", HashMap::new(), HashMap::new())
            .unwrap();
        assert_eq!(ev.seq_num, 5, "seq continues across redescribe");
        assert_eq!(ev.descriptor, v2.uid, "events reference the new generation");
        assert_eq!(
            bundle.descriptor_uid_for("primary").as_deref(),
            Some(v2.uid.as_str())
        );
    }

    #[test]
    fn compose_redescribe_leaves_current_generation_until_install() {
        // Emit-before-install: the descriptor a concurrent pump would stamp onto
        // an event stays the OLD generation between compose and install, so the
        // new descriptor is always broadcast before any event can reference it.
        let start = RunBundle::start(Some(1), None);
        let bundle = RunBundle::open(&start);
        let mut config = HashMap::new();
        config.insert(
            "primary".to_string(),
            Configuration {
                data: HashMap::from([("cam_gain".to_string(), serde_json::Value::from(2.5))]),
                ..Default::default()
            },
        );
        let (v1, _) = bundle.descriptor("primary", HashMap::new(), config, None, HashMap::new());
        let fresh = Configuration {
            data: HashMap::from([("cam_gain".to_string(), serde_json::Value::from(5.0))]),
            ..Default::default()
        };
        let v2 = bundle
            .compose_redescribe("primary", "primary", fresh)
            .expect("stream includes the object");
        // Before install: current descriptor is still v1, and an event composed
        // now (as a racing pump would) references v1 — not the un-emitted v2.
        assert_eq!(
            bundle.descriptor_uid_for("primary").as_deref(),
            Some(v1.uid.as_str()),
            "compose_redescribe must not install"
        );
        let ev_before = bundle
            .event("primary", HashMap::new(), HashMap::new())
            .unwrap();
        assert_eq!(ev_before.descriptor, v1.uid, "pre-install event is old gen");
        // After install: only now do events reference v2.
        bundle.install_descriptor("primary", v2.clone());
        assert_eq!(
            bundle.descriptor_uid_for("primary").as_deref(),
            Some(v2.uid.as_str())
        );
        let ev_after = bundle
            .event("primary", HashMap::new(), HashMap::new())
            .unwrap();
        assert_eq!(ev_after.descriptor, v2.uid, "post-install event is new gen");
        assert_eq!(ev_after.seq_num, ev_before.seq_num + 1, "seq preserved");
    }

    #[test]
    fn redescribe_returns_none_when_object_absent_or_stream_undeclared() {
        let start = RunBundle::start(Some(1), None);
        let bundle = RunBundle::open(&start);
        // Undeclared stream.
        assert!(bundle
            .compose_redescribe("primary", "cam", Configuration::default())
            .is_none());
        // Declared, but the object is not in this stream's configuration.
        bundle.descriptor(
            "primary",
            HashMap::new(),
            HashMap::new(),
            None,
            HashMap::new(),
        );
        assert!(
            bundle
                .compose_redescribe("primary", "cam", Configuration::default())
                .is_none(),
            "an object not in the stream does not reconfigure it"
        );
    }

    #[test]
    fn resource_composer_mints_sequential_datum_ids() {
        let start = RunBundle::start(Some(1), None);
        let bundle = RunBundle::open(&start);
        let rc = bundle.resource(
            "AD_HDF5".to_string(),
            "/data".to_string(),
            "scan.h5".to_string(),
            None,
            HashMap::new(),
        );
        assert_eq!(rc.resource().path_semantics.as_deref(), Some("posix"));
        assert_eq!(rc.resource().run_start.as_deref(), Some(start.uid.as_str()));
        let res_uid = rc.resource().uid.clone();
        let d0 = rc.datum(HashMap::new());
        let d1 = rc.datum(HashMap::new());
        assert_eq!(d0.datum_id, format!("{res_uid}/0"));
        assert_eq!(d1.datum_id, format!("{res_uid}/1"));
        let page = rc.datum_page(HashMap::new(), 3);
        assert_eq!(
            page.datum_id,
            vec![
                format!("{res_uid}/2"),
                format!("{res_uid}/3"),
                format!("{res_uid}/4"),
            ]
        );
    }

    // Parity with event_model ComposeStreamDatum: StreamDatum.uid is
    // `<stream_resource_uid>/<counter>`, not a fresh random uid, and the
    // indices/seq_nums/descriptor pass through verbatim.
    #[test]
    fn stream_resource_composer_mints_resource_scoped_stream_datum_uids() {
        let start = RunBundle::start(Some(1), None);
        let bundle = RunBundle::open(&start);
        let src = bundle.stream_resource(
            "img".to_string(),
            "application/x-hdf5".to_string(),
            "file:///data/scan.h5".to_string(),
            HashMap::new(),
        );
        assert_eq!(
            src.stream_resource().run_start.as_deref(),
            Some(start.uid.as_str())
        );
        let sr_uid = src.stream_resource().uid.clone();

        let sd0 = src.stream_datum(
            StreamRange { start: 0, stop: 5 },
            StreamRange { start: 1, stop: 6 },
            "desc-uid".to_string(),
        );
        let sd1 = src.stream_datum(
            StreamRange { start: 5, stop: 9 },
            StreamRange { start: 6, stop: 10 },
            "desc-uid".to_string(),
        );
        assert_eq!(sd0.uid, format!("{sr_uid}/0"), "counter-based, not random");
        assert_eq!(sd1.uid, format!("{sr_uid}/1"));
        assert_eq!(sd0.stream_resource, sr_uid);
        assert_eq!(sd1.stream_resource, sr_uid);
        // Args thread through unchanged.
        assert_eq!(sd0.indices, StreamRange { start: 0, stop: 5 });
        assert_eq!(sd1.seq_nums, StreamRange { start: 6, stop: 10 });
        assert_eq!(sd0.descriptor, "desc-uid");
    }

    #[test]
    fn distinct_stream_resources_have_independent_datum_counters() {
        let start = RunBundle::start(Some(1), None);
        let bundle = RunBundle::open(&start);
        let a = bundle.stream_resource(
            "a".into(),
            "application/octet-stream".into(),
            "file:///a".into(),
            HashMap::new(),
        );
        let b = bundle.stream_resource(
            "b".into(),
            "application/octet-stream".into(),
            "file:///b".into(),
            HashMap::new(),
        );
        let r = StreamRange { start: 0, stop: 1 };
        // Each resource's counter starts at 0 independently.
        assert_eq!(
            a.stream_datum(r, r, "d".into()).uid,
            format!("{}/0", a.stream_resource().uid)
        );
        assert_eq!(
            b.stream_datum(r, r, "d".into()).uid,
            format!("{}/0", b.stream_resource().uid)
        );
        assert_ne!(a.stream_resource().uid, b.stream_resource().uid);
    }

    // compose_event_page parity: a page draws contiguous 1-based seq_nums from
    // the SAME stream counter as single event()s, so singles and pages share
    // one monotone sequence; one uid/time per row; descriptor matches.
    #[test]
    fn event_page_assigns_contiguous_seqnums_sharing_the_event_counter() {
        let start = RunBundle::start(Some(1), None);
        let bundle = RunBundle::open(&start);
        let (desc, _) = bundle.descriptor(
            "primary",
            HashMap::new(),
            HashMap::new(),
            None,
            HashMap::new(),
        );

        // One single event first (seq_num 1).
        let e1 = bundle
            .event("primary", HashMap::new(), HashMap::new())
            .unwrap();
        assert_eq!(e1.seq_num, 1);

        // A page of 2 continues the sequence: 2, 3.
        let page = bundle
            .event_page(
                "primary",
                HashMap::from([("x".to_string(), vec![Value::from(10), Value::from(20)])]),
                HashMap::from([("x".to_string(), vec![1.0, 2.0])]),
                2,
            )
            .expect("stream declared");
        assert_eq!(page.seq_num, vec![2, 3]);
        assert_eq!(page.uid.len(), 2, "one uid per row");
        assert_ne!(page.uid[0], page.uid[1], "row uids are distinct");
        assert_eq!(page.time.len(), 2, "one time per row");
        assert_eq!(page.descriptor, desc.uid);
        assert_eq!(page.data["x"], vec![Value::from(10), Value::from(20)]);

        // A subsequent single event continues from where the page left off: 4.
        let e2 = bundle
            .event("primary", HashMap::new(), HashMap::new())
            .unwrap();
        assert_eq!(e2.seq_num, 4);

        // RunStop counts all four events across singles and the page.
        let stop = bundle.stop("success", None);
        assert_eq!(stop.num_events["primary"], 4);
    }

    #[test]
    fn event_page_unknown_stream_returns_none() {
        let start = RunBundle::start(Some(1), None);
        let bundle = RunBundle::open(&start);
        assert!(bundle
            .event_page("nope", HashMap::new(), HashMap::new(), 3)
            .is_none());
    }
}
