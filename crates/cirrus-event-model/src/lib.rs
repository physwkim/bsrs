//! Bluesky event-model document types.
//!
//! These are hand-ported from the JSON schemas at
//! `event-model/src/event_model/schemas/*.json`. The shapes match the schemas;
//! optional fields use `Option<T>` and skip on serialization. A future revision
//! will switch to `typify`-generated types — the API will not change.

#![deny(missing_docs)]

pub mod compose;
pub mod documents;
pub mod page;

pub use documents::{
    make_datakey, Configuration, DataKey, Datum, DatumPage, DocFilter, Document, Dtype, Event,
    EventDescriptor, EventPage, Hints, Limits, LimitsRange, PerObjectHint, Projections, RdsRange,
    Reading, Resource, RunStart, RunStop, SignalMetadata, StreamDatum, StreamRange, StreamResource,
};
pub use page::{
    merge_datum_pages, merge_event_pages, pack_datum_page, pack_event_page, rechunk_datum_pages,
    rechunk_event_pages, unpack_datum_page, unpack_event_page,
};

/// Errors when composing or routing documents.
#[derive(Debug, thiserror::Error)]
pub enum EventModelError {
    /// A `data_keys` set was inconsistent across composes for the same stream.
    #[error("mismatched data keys for stream `{0}`")]
    MismatchedDataKeys(String),
    /// A reference UID could not be resolved.
    #[error("unknown reference uid: {0}")]
    UnknownUid(String),
    /// JSON encode/decode failure.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}
