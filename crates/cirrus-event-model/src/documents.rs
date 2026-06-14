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

/// A projection spec carried in `RunStart.projections`. Describes how to
/// interpret a run as a named projection.
///
/// Mirrors `event_model`'s `Projections`. The individual `projection` entries
/// (linked / calculated / static, discriminated by `location`+`type`) are kept
/// as free-form `Value`s: cirrus passes projections through without
/// interpreting them, so the four-variant union is not modeled here.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Projections {
    /// Static information about the projection.
    #[serde(default)]
    pub configuration: HashMap<String, Value>,
    /// Name of the projection.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub name: String,
    /// Per-field projection entries, keyed by the projected field name.
    #[serde(default)]
    pub projection: HashMap<String, Value>,
    /// Version of the projection spec (may reference an external spec).
    #[serde(default)]
    pub version: String,
}

/// Document created at the start of every run.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
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
    /// Data access groups meaningful to an external system (facility,
    /// beamline, proposal, safety form, …).
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub data_groups: Vec<String>,
    /// Data-management grouping of runs (e.g. a visit or set of trials).
    /// Not a scientific grouping.
    #[serde(skip_serializing_if = "String::is_empty", default)]
    pub data_session: String,
    /// Free-form recursive run-level metadata (`data_type` in the schema).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub data_type: Option<Value>,
    /// Unix group to associate this data with.
    #[serde(skip_serializing_if = "String::is_empty", default)]
    pub group: String,
    /// Unix owner to associate this data with.
    #[serde(skip_serializing_if = "String::is_empty", default)]
    pub owner: String,
    /// Name of the project this run is part of.
    #[serde(skip_serializing_if = "String::is_empty", default)]
    pub project: String,
    /// Projection specs describing how to interpret this run.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub projections: Vec<Projections>,
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

/// Read-different-than-set tolerance (Tango RDS): the acceptable drift
/// between a setpoint and its readback before they are considered diverged.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct RdsRange {
    /// Maximum age (seconds) of a readback before it is considered stale.
    pub time_difference: f64,
    /// Maximum |readback − setpoint| considered "in sync".
    pub value_difference: f64,
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
    /// Read-different-than-set tolerance (Tango sources).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub rds: Option<RdsRange>,
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
    /// Enumerated string choices for mbbi/mbbo/enum records, ordered by
    /// integer index. Used by LiveTable/Tiled to render enum values.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub choices: Option<Vec<String>>,
}

/// Optional, consistently-named metadata for a signal's [`DataKey`], mirroring
/// ophyd-async `SignalMetadata` (`core/_signal_backend.py:114`). Every field is
/// optional; a backend fills in what its transport knows — `units`/`precision`
/// for numerics, `choices` for enums, `limits` for records with control fields.
///
/// This is the canonical vocabulary so that the same concept lands in the same
/// `DataKey` field across every backend, rather than each backend deciding ad
/// hoc which optional fields to populate.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SignalMetadata {
    /// Control / display / warning / alarm limits for a numeric datatype.
    pub limits: Option<Limits>,
    /// Possible values for an enum datatype, ordered by integer index.
    pub choices: Option<Vec<String>>,
    /// Digits after the decimal place to display for a float datatype.
    pub precision: Option<i64>,
    /// Engineering units of a numeric value.
    pub units: Option<String>,
}

/// Build a [`DataKey`] from the transport-known shape (`dtype`, `shape`,
/// `dtype_numpy`) plus optional [`SignalMetadata`], mirroring ophyd-async
/// `make_datakey` (`core/_signal_backend.py:180`).
///
/// The descriptor fields that are *not* signal-level metadata — `external`,
/// `object_name`, `dims` — default to `None` in this one place, so backends
/// stop re-spelling six always-`None` fields at every `get_datakey` call site
/// (and cannot silently disagree on which optional fields exist).
pub fn make_datakey(
    source: impl Into<String>,
    dtype: Dtype,
    shape: Vec<Option<u64>>,
    dtype_numpy: Option<String>,
    meta: SignalMetadata,
) -> DataKey {
    DataKey {
        source: source.into(),
        dtype,
        shape,
        dtype_numpy,
        external: None,
        units: meta.units,
        precision: meta.precision,
        object_name: None,
        dims: None,
        limits: meta.limits,
        choices: meta.choices,
    }
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
    /// Fill state of external references: `false` = unfilled external slot,
    /// a string = the foreign key after a Filler resolves the reference and
    /// moves the data into `data`. Matches the schema's `bool | str` union.
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub filled: HashMap<String, Value>,
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
    /// Column-store of external-reference fill state. Each entry maps a
    /// data key to one `bool | str` per row: `false` = unfilled external
    /// slot, a string = the foreign key after a Filler resolves it.
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub filled: HashMap<String, Vec<Value>>,
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
    /// Path semantics (`posix` / `windows`). Optional per schema; absent on
    /// Resources that do not pin a platform.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub path_semantics: Option<String>,
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

/// Document-type filter for selective subscriptions, mirroring the
/// bluesky `name` argument to `RE.subscribe`/`Msg('subscribe', …, name)`.
///
/// `All` matches every document; each named variant matches exactly its
/// corresponding [`Document`] variant. Documents without a named variant
/// (e.g. `EventPage`, `Resource`, `Datum`) are delivered only to `All`
/// subscribers — one uniform rule, no per-boundary special-casing.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum DocFilter {
    /// Receive every document.
    #[default]
    All,
    /// Receive only `Start`.
    Start,
    /// Receive only `Descriptor`.
    Descriptor,
    /// Receive only `Event`.
    Event,
    /// Receive only `Stop`.
    Stop,
}

impl DocFilter {
    /// True if a subscriber with this filter should receive `doc`.
    pub fn matches(&self, doc: &Document) -> bool {
        match self {
            DocFilter::All => true,
            DocFilter::Start => matches!(doc, Document::Start(_)),
            DocFilter::Descriptor => matches!(doc, Document::Descriptor(_)),
            DocFilter::Event => matches!(doc, Document::Event(_)),
            DocFilter::Stop => matches!(doc, Document::Stop(_)),
        }
    }
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
                ..Default::default()
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

    #[test]
    fn doc_filter_matches_by_variant() {
        let start = Document::Start(RunStart {
            uid: "r".into(),
            time: 0.0,
            ..Default::default()
        });
        let stop = Document::Stop(RunStop {
            uid: "s".into(),
            run_start: "r".into(),
            time: 0.0,
            exit_status: "success".into(),
            reason: None,
            num_events: HashMap::new(),
        });
        let page = Document::EventPage(EventPage {
            uid: vec![],
            descriptor: "d".into(),
            time: vec![],
            seq_num: vec![],
            data: HashMap::new(),
            timestamps: HashMap::new(),
            filled: HashMap::new(),
        });

        // All matches everything, including variants without a named filter.
        assert!(DocFilter::All.matches(&start));
        assert!(DocFilter::All.matches(&stop));
        assert!(DocFilter::All.matches(&page));

        // Named filters match exactly their variant and nothing else.
        assert!(DocFilter::Start.matches(&start));
        assert!(!DocFilter::Start.matches(&stop));
        assert!(DocFilter::Stop.matches(&stop));
        assert!(!DocFilter::Stop.matches(&start));

        // EventPage has no named filter — only All delivers it.
        assert!(!DocFilter::Event.matches(&page));
        assert!(!DocFilter::Start.matches(&page));
    }

    #[test]
    fn make_datakey_fills_metadata_and_defaults_non_metadata_fields() {
        // Metadata fields land in their canonical DataKey slots; the
        // non-metadata fields (external/object_name/dims) are None by default.
        let limits = Limits {
            display: Some(LimitsRange {
                low: Some(0.0),
                high: Some(10.0),
            }),
            ..Default::default()
        };
        let dk = make_datakey(
            "soft://m1",
            Dtype::Number,
            vec![],
            Some("<f8".into()),
            SignalMetadata {
                limits: Some(limits.clone()),
                choices: Some(vec!["a".into(), "b".into()]),
                precision: Some(3),
                units: Some("mm".into()),
            },
        );
        assert_eq!(dk.source, "soft://m1");
        assert_eq!(dk.dtype, Dtype::Number);
        assert_eq!(dk.shape, Vec::<Option<u64>>::new());
        assert_eq!(dk.dtype_numpy.as_deref(), Some("<f8"));
        assert_eq!(dk.units.as_deref(), Some("mm"));
        assert_eq!(dk.precision, Some(3));
        assert_eq!(dk.limits, Some(limits));
        assert_eq!(dk.choices, Some(vec!["a".into(), "b".into()]));
        // Not signal-level metadata: always defaulted here.
        assert!(dk.external.is_none());
        assert!(dk.object_name.is_none());
        assert!(dk.dims.is_none());
    }

    #[test]
    fn make_datakey_empty_metadata_leaves_all_optionals_none() {
        let dk = make_datakey(
            "mock://x",
            Dtype::Number,
            vec![],
            None,
            SignalMetadata::default(),
        );
        for absent in [
            dk.dtype_numpy.is_some(),
            dk.external.is_some(),
            dk.units.is_some(),
            dk.precision.is_some(),
            dk.object_name.is_some(),
            dk.dims.is_some(),
            dk.limits.is_some(),
            dk.choices.is_some(),
        ] {
            assert!(!absent, "empty SignalMetadata must leave optionals None");
        }
    }

    #[test]
    fn run_start_data_management_fields_round_trip() {
        let start = RunStart {
            uid: "run-1".into(),
            time: 1.0,
            data_groups: vec!["beamline-x".into(), "proposal-42".into()],
            data_session: "visit-7".into(),
            group: "users".into(),
            owner: "alice".into(),
            project: "imaging".into(),
            projections: vec![Projections {
                name: "primary".into(),
                version: "1.0".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let json = serde_json::to_value(&start).unwrap();
        // Named fields land at the top level, not inside `extra`.
        assert_eq!(json["data_session"], "visit-7");
        assert_eq!(json["owner"], "alice");
        assert_eq!(json["projections"][0]["name"], "primary");
        let back: RunStart = serde_json::from_value(json).unwrap();
        assert_eq!(start, back);
    }

    #[test]
    fn run_start_empty_data_management_fields_are_skipped() {
        let start = RunStart {
            uid: "run-1".into(),
            time: 1.0,
            ..Default::default()
        };
        let json = serde_json::to_value(&start).unwrap();
        for absent in [
            "data_groups",
            "data_session",
            "data_type",
            "group",
            "owner",
            "project",
            "projections",
        ] {
            assert!(
                json.get(absent).is_none(),
                "empty `{absent}` must not serialize"
            );
        }
    }
}
