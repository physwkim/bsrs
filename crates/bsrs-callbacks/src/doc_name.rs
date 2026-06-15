//! Mapping from `Document` variant to bluesky's wire-level document name.

use bsrs_event_model::Document;

/// The bluesky document name string for envelope encoding.
///
/// Matches `event_model.DocumentNames` (`__init__.py:94`).
pub fn document_name(doc: &Document) -> &'static str {
    match doc {
        Document::Start(_) => "start",
        Document::Descriptor(_) => "descriptor",
        Document::Event(_) => "event",
        Document::EventPage(_) => "event_page",
        Document::Resource(_) => "resource",
        Document::Datum(_) => "datum",
        Document::DatumPage(_) => "datum_page",
        Document::StreamResource(_) => "stream_resource",
        Document::StreamDatum(_) => "stream_datum",
        Document::Stop(_) => "stop",
    }
}
