//! Compose helpers — UID generation, descriptor caching, sequence-number bookkeeping.
//!
//! Mirrors `event_model.compose_*` (`__init__.py:1852-2528`).

use crate::documents::*;
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

/// Per-run composer: caches descriptors by data-key shape, increments seq nums.
#[derive(Debug)]
pub struct RunBundle {
    start_uid: String,
    streams: Mutex<HashMap<String, StreamState>>,
}

#[derive(Debug)]
struct StreamState {
    descriptor_uid: String,
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

    /// Compose a stream descriptor. If a descriptor with the same shape already
    /// exists for this stream name, returns its UID and emits no new descriptor.
    pub fn descriptor(
        &self,
        name: &str,
        data_keys: HashMap<String, DataKey>,
        configuration: HashMap<String, Configuration>,
        hints: Option<HashMap<String, PerObjectHint>>,
        object_keys: HashMap<String, Vec<String>>,
    ) -> (EventDescriptor, bool) {
        let mut streams = self.streams.lock().unwrap();
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
        let is_new = !streams.contains_key(name);
        streams
            .entry(name.to_string())
            .or_insert_with(|| StreamState {
                descriptor_uid: descriptor.uid.clone(),
                seq_num: AtomicU64::new(0),
            });
        (descriptor, is_new)
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
            descriptor: st.descriptor_uid.clone(),
            time: now(),
            seq_num: n,
            data,
            timestamps,
            filled: HashMap::new(),
        })
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
            exit_status: exit_status.to_string(),
            reason,
            num_events,
        }
    }

    /// Compose a `StreamResource` for a fly-style data path.
    pub fn stream_resource(
        &self,
        data_key: String,
        mimetype: String,
        uri: String,
        parameters: HashMap<String, Value>,
    ) -> StreamResource {
        StreamResource {
            uid: new_uid(),
            data_key,
            mimetype,
            uri,
            parameters,
            run_start: Some(self.start_uid.clone()),
        }
    }

    /// Compose a `StreamDatum` for a previously-emitted `StreamResource`.
    pub fn stream_datum(
        &self,
        stream_resource_uid: String,
        descriptor_uid: String,
        indices: StreamRange,
        seq_nums: StreamRange,
    ) -> StreamDatum {
        StreamDatum {
            uid: new_uid(),
            stream_resource: stream_resource_uid,
            descriptor: descriptor_uid,
            indices,
            seq_nums,
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
            .map(|s| s.descriptor_uid.clone())
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

/// Convenience: a thread-safe `Arc<RunBundle>`.
pub type SharedBundle = Arc<RunBundle>;

#[cfg(test)]
mod tests {
    use super::*;

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
}
