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
    make_datakey, Configuration, DataKey, Datum, DatumPage, DimensionItem, DocFilter, Document,
    Dtype, DtypeNumpy, Event, EventDescriptor, EventPage, ExitStatus, Hints, Limits, LimitsRange,
    PerObjectHint, Projections, RdsRange, Reading, Resource, RunStart, RunStop, SignalMetadata,
    StreamDatum, StreamRange, StreamResource,
};
pub use page::{
    merge_datum_pages, merge_event_pages, pack_datum_page, pack_event_page, rechunk_datum_pages,
    rechunk_event_pages, unpack_datum_page, unpack_event_page,
};

/// Compose a `file://` URI for an emitted document (`StreamResource.uri`,
/// external `DataKey.source`). Absolute paths get the explicit `localhost`
/// authority — `file://localhost/data/x.h5` — matching ophyd-async's
/// `PathInfo.directory_uri` convention so downstream consumers see one form.
/// A relative path (no leading `/`) cannot take an authority without
/// corrupting its first segment, so it keeps the bare `file://` form.
pub fn file_uri(path: &str) -> String {
    if path.starts_with('/') {
        format!("file://localhost{path}")
    } else {
        format!("file://{path}")
    }
}

#[cfg(test)]
mod file_uri_tests {
    use super::file_uri;

    #[test]
    fn absolute_path_gets_localhost_authority() {
        assert_eq!(
            file_uri("/data/scans/scan.h5"),
            "file://localhost/data/scans/scan.h5"
        );
    }

    #[test]
    fn relative_path_keeps_bare_form() {
        assert_eq!(file_uri("scans/scan.h5"), "file://scans/scan.h5");
    }
}

/// Errors when composing or routing documents.
#[derive(Debug, thiserror::Error)]
pub enum EventModelError {
    /// A `data_keys` set was inconsistent across composes for the same stream.
    #[error("mismatched data keys for stream `{0}`")]
    MismatchedDataKeys(String),
    /// A reference UID could not be resolved.
    #[error("unknown reference uid: {0}")]
    UnknownUid(String),
    /// `pack_event_page` / `pack_datum_page` was called with zero rows. A page
    /// cannot be built from an empty collection because its `{field}` field
    /// (taken from the first row) would be null, which the schema forbids.
    /// Mirrors the reference `ValueError`.
    #[error("cannot pack an empty {kind} collection: a page's `{field}` field cannot be null")]
    EmptyPack {
        /// Row document kind being packed (`Event` or `Datum`).
        kind: &'static str,
        /// Page field that would be left null (`descriptor` or `resource`).
        field: &'static str,
    },
    /// JSON encode/decode failure.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}
