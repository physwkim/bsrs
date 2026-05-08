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
            sample: None,
            extra: HashMap::new(),
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

/// Convenience: a thread-safe `Arc<RunBundle>`.
pub type SharedBundle = Arc<RunBundle>;
