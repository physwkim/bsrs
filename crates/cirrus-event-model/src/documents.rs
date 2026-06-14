//! Document type definitions, ported from the event-model JSON schemas.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

// -- run_start.json -----------------------------------------------------------

/// Visualization hints carried in `RunStart`.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Hints {
    /// Independent axes of the experiment, ordered slow-to-fast.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub dimensions: Option<Vec<Vec<Vec<String>>>>,
}

/// Document created at the start of every run.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RunStart {
    /// Globally unique ID for this run.
    pub uid: String,
    /// Unix epoch time the run started.
    pub time: f64,
    /// Scan ID number (not globally unique).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub scan_id: Option<u64>,
    /// Visualization hints.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub hints: Option<Hints>,
    /// Information about the sample, may be a UID to another collection.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub sample: Option<Value>,
    /// Free-form additional metadata.
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

// -- run_stop.json ------------------------------------------------------------

/// Final document of a run.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RunStop {
    /// UID of this stop document.
    pub uid: String,
    /// UID of the run start this stop closes.
    pub run_start: String,
    /// Unix epoch time the run ended.
    pub time: f64,
    /// One of `success` / `abort` / `fail`.
    pub exit_status: String,
    /// Optional human-readable reason.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub reason: Option<String>,
    /// Per-stream sequence-number counters at run close.
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub num_events: HashMap<String, u64>,
}

// -- event_descriptor.json ----------------------------------------------------

/// Broad JSON schema type for a `DataKey`.
#[derive(Copy, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Dtype {
    /// JSON string.
    String,
    /// JSON number.
    Number,
    /// JSON array.
    Array,
    /// JSON boolean.
    Boolean,
    /// JSON integer.
    Integer,
}

/// Inclusive numeric range used in EPICS-style limits.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct LimitsRange {
    /// Upper bound (None = no limit).
    pub high: Option<f64>,
    /// Lower bound (None = no limit).
    pub low: Option<f64>,
}

/// EPICS limits attached to a data key.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Limits {
    /// Alarm limits.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub alarm: Option<LimitsRange>,
    /// Control (writable) limits.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub control: Option<LimitsRange>,
    /// Display limits.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub display: Option<LimitsRange>,
    /// Warning limits.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub warning: Option<LimitsRange>,
    /// Hysteresis (single number).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub hysteresis: Option<f64>,
}

/// Per-stream descriptor of a single field.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct DataKey {
    /// Source identifier (e.g. CA URL).
    pub source: String,
    /// Broad JSON dtype.
    pub dtype: Dtype,
    /// Shape; `[]` for scalar.
    pub shape: Vec<Option<u64>>,
    /// Optional numpy dtype string (e.g. `<f8`).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub dtype_numpy: Option<String>,
    /// `STREAM:` if data is referenced via StreamResource/StreamDatum.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub external: Option<String>,
    /// Engineering units.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub units: Option<String>,
    /// Floating-point precision (digits after decimal).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub precision: Option<i64>,
    /// Object that produced this key.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub object_name: Option<String>,
    /// Dimension names.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub dims: Option<Vec<String>>,
    /// EPICS limits.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub limits: Option<Limits>,
}

/// Per-object hint hung off the descriptor.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[allow(non_snake_case)]
pub struct PerObjectHint {
    /// Names of fields considered "interesting".
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub fields: Option<Vec<String>>,
    /// NeXus class for the device. Field name preserves the schema spelling.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub NX_class: Option<String>,
}

/// Configuration sub-document (slow-changing fields).
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Configuration {
    /// Field values.
    #[serde(default)]
    pub data: HashMap<String, Value>,
    /// Field descriptors.
    #[serde(default)]
    pub data_keys: HashMap<String, DataKey>,
    /// Field timestamps.
    #[serde(default)]
    pub timestamps: HashMap<String, f64>,
}

/// Describes a stream of events.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct EventDescriptor {
    /// UID of this descriptor.
    pub uid: String,
    /// UID of the run-start document.
    pub run_start: String,
    /// Time the descriptor was emitted.
    pub time: f64,
    /// Field descriptors keyed by field name.
    pub data_keys: HashMap<String, DataKey>,
    /// Configuration sub-readings keyed by object name.
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub configuration: HashMap<String, Configuration>,
    /// Stream name (e.g. `primary`, `baseline`).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub name: Option<String>,
    /// Per-object hints.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub hints: Option<HashMap<String, PerObjectHint>>,
    /// Object → fields mapping.
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub object_keys: HashMap<String, Vec<String>>,
}

// -- event.json ---------------------------------------------------------------

/// One reading of one field — value, timestamp, optional alarm.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Reading {
    /// The current value (any JSON-encodable type).
    pub value: Value,
    /// Unix epoch timestamp in seconds.
    pub timestamp: f64,
    /// EPICS alarm severity (0 = ok).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub alarm_severity: Option<i32>,
    /// Alarm message.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub message: Option<String>,
}

/// One row of measurements for one stream.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Event {
    /// UID of this event.
    pub uid: String,
    /// UID of the descriptor.
    pub descriptor: String,
    /// Time the event was assembled.
    pub time: f64,
    /// Sequence number within the stream.
    pub seq_num: u64,
    /// Field values keyed by field name.
    pub data: HashMap<String, Value>,
    /// Per-field timestamps.
    pub timestamps: HashMap<String, f64>,
    /// Filled (true/false) state of external references.
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub filled: HashMap<String, bool>,
}

/// Page-form Event (multiple rows in one document).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct EventPage {
    /// UID for each row in this page (one per event).
    pub uid: Vec<String>,
    /// Descriptor UID.
    pub descriptor: String,
    /// Times for each event.
    pub time: Vec<f64>,
    /// Sequence numbers for each event.
    pub seq_num: Vec<u64>,
    /// Column-store of field values (field name → list of values).
    pub data: HashMap<String, Vec<Value>>,
    /// Column-store of timestamps.
    pub timestamps: HashMap<String, Vec<f64>>,
}

// -- resource.json + datum.json ----------------------------------------------

/// External resource (file) that holds data referenced by Events.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Resource {
    /// UID of this resource.
    pub uid: String,
    /// Filing format identifier (e.g. `AD_HDF5`).
    pub spec: String,
    /// Root directory path.
    pub root: String,
    /// Resource path relative to `root`.
    pub resource_path: String,
    /// Path semantics (`posix` / `windows`).
    pub path_semantics: String,
    /// Format-specific arguments.
    #[serde(default)]
    pub resource_kwargs: HashMap<String, Value>,
    /// UID of the run-start.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub run_start: Option<String>,
}

/// Pointer to a single data row inside a Resource.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Datum {
    /// Datum UID; convention `<resource>/<index>`.
    pub datum_id: String,
    /// UID of the parent resource.
    pub resource: String,
    /// Format-specific arguments to address this row.
    #[serde(default)]
    pub datum_kwargs: HashMap<String, Value>,
}

/// Page-form datum (bulk).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct DatumPage {
    /// Datum UIDs.
    pub datum_id: Vec<String>,
    /// Parent resource UID.
    pub resource: String,
    /// Per-field bulk arguments.
    #[serde(default)]
    pub datum_kwargs: HashMap<String, Vec<Value>>,
}

// -- stream_resource.json + stream_datum.json --------------------------------

/// Stream-style resource (the modern replacement for `Resource` for bulk data).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct StreamResource {
    /// UID of this stream resource.
    pub uid: String,
    /// Data key in the descriptor that this resource serves.
    pub data_key: String,
    /// Mimetype identifier (e.g. `application/x-hdf5`).
    pub mimetype: String,
    /// URI for locating this resource.
    pub uri: String,
    /// Handler-specific parameters.
    #[serde(default)]
    pub parameters: HashMap<String, Value>,
    /// UID of the run-start.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub run_start: Option<String>,
}

/// Sequence-of-integers range used by `StreamDatum`.
#[derive(Copy, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct StreamRange {
    /// First number in the range.
    pub start: u64,
    /// One past the last number.
    pub stop: u64,
}

/// A slice of stream data inside a `StreamResource`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct StreamDatum {
    /// UID for this datum.
    pub uid: String,
    /// UID of the parent stream resource.
    pub stream_resource: String,
    /// UID of the descriptor.
    pub descriptor: String,
    /// Slice into the resource's data.
    pub indices: StreamRange,
    /// Slice into the event sequence-number space.
    pub seq_nums: StreamRange,
}

// -- top-level enum -----------------------------------------------------------

/// One of the ten document kinds, with the document name as the discriminant.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "name", content = "doc", rename_all = "snake_case")]
pub enum Document {
    /// Run start.
    Start(RunStart),
    /// Stream descriptor.
    Descriptor(EventDescriptor),
    /// One event.
    Event(Event),
    /// Page of events.
    EventPage(EventPage),
    /// External resource.
    Resource(Resource),
    /// Datum (pointer into a resource).
    Datum(Datum),
    /// Page of datums.
    DatumPage(DatumPage),
    /// Stream resource (modern).
    StreamResource(StreamResource),
    /// Slice of a stream resource.
    StreamDatum(StreamDatum),
    /// Run stop.
    Stop(RunStop),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn document_round_trip() {
        let docs = vec![
            Document::Start(RunStart {
                uid: "run-1".into(),
                time: 1700000000.0,
                scan_id: Some(1),
                hints: None,
                sample: None,
                extra: HashMap::new(),
            }),
            Document::Stop(RunStop {
                uid: "stop-1".into(),
                run_start: "run-1".into(),
                time: 1700000005.0,
                exit_status: "success".into(),
                reason: None,
                num_events: HashMap::new(),
            }),
        ];
        let json = serde_json::to_string(&docs).unwrap();
        let back: Vec<Document> = serde_json::from_str(&json).unwrap();
        assert_eq!(docs, back);
    }
}
