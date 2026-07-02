//! Shared document-body encoding.
//!
//! Sinks that carry the document kind in an *out-of-band* channel (ZMQ's
//! multipart `<name>` frame, Kafka's message key) must serialize the raw
//! inner variant of [`Document`], not the adjacently-tagged
//! `{"name": ..., "doc": ...}` wrapper that `serde`'s default `Document`
//! serialization produces. Centralising the per-variant match here makes this
//! the single owner: a new such sink cannot re-implement the match and
//! accidentally ship the envelope (CBEM-01).
//!
//! Sinks with no out-of-band name channel (JSONL files) keep the tagged
//! wrapper instead — a `.jsonl` line is otherwise unrecoverable, since the
//! reader cannot tell a start from an event from a stop. See `basic::JsonlSink`.

use crate::event_model::Document;

/// Expand a 10-arm match over every [`Document`] variant, applying `$f` to the
/// *inner* document so the raw dict is serialized rather than the tagged
/// `Document` wrapper. `$f` is a serializer such as `serde_json::to_vec` or
/// `rmp_serde::to_vec_named`, each generic over `T: Serialize`.
macro_rules! encode_inner {
    ($doc:expr, $f:path) => {
        match $doc {
            Document::Start(d) => $f(d),
            Document::Descriptor(d) => $f(d),
            Document::Event(d) => $f(d),
            Document::EventPage(d) => $f(d),
            Document::Resource(d) => $f(d),
            Document::Datum(d) => $f(d),
            Document::DatumPage(d) => $f(d),
            Document::StreamResource(d) => $f(d),
            Document::StreamDatum(d) => $f(d),
            Document::Stop(d) => $f(d),
        }
    };
}

/// Serialize the inner document body as JSON — the raw event-model dict, with
/// no `name`/`doc` envelope.
pub(crate) fn encode_inner_json(doc: &Document) -> serde_json::Result<Vec<u8>> {
    encode_inner!(doc, serde_json::to_vec)
}

/// Serialize the inner document body as msgpack named maps — the raw dict.
#[cfg(any(feature = "zmq", feature = "kafka"))]
pub(crate) fn encode_inner_msgpack(doc: &Document) -> Result<Vec<u8>, rmp_serde::encode::Error> {
    encode_inner!(doc, rmp_serde::to_vec_named)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_model::{Document, ExitStatus, RunStop};

    fn stop_doc() -> Document {
        Document::Stop(RunStop {
            uid: "u".into(),
            run_start: "r".into(),
            time: 0.0,
            exit_status: ExitStatus::Success,
            reason: None,
            num_events: Default::default(),
            ..Default::default()
        })
    }

    #[test]
    fn json_emits_raw_dict_not_tagged_wrapper() {
        let body = encode_inner_json(&stop_doc()).expect("encode");
        let v: serde_json::Value = serde_json::from_slice(&body).expect("parse");
        // Raw event-model dict: fields sit at the top level, with no
        // {"name": .., "doc": ..} adjacently-tagged Document envelope.
        assert_eq!(v["exit_status"], "success");
        assert!(v.get("run_start").is_some());
        assert!(
            v.get("name").is_none() && v.get("doc").is_none(),
            "must be the raw document dict, not the tagged Document wrapper: {v}"
        );
    }
}
