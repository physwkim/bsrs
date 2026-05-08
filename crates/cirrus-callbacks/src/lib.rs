//! Document sinks for cirrus.
//!
//! Each sink implements [`cirrus_engine::DocumentSink`] and consumes the
//! [`cirrus_event_model::Document`] stream the RunEngine emits. The sinks here
//! are *Document producers' adapters* — they push documents at the bluesky
//! Python ecosystem (Tiled, BestEffortCallback, suitcase) without requiring
//! any Python code on the cirrus side.

#![deny(missing_docs)]

mod basic;
mod doc_name;

pub use basic::{CapturingSink, JsonlSink, StderrTraceSink};

#[cfg(feature = "zmq")]
mod zmq_sink;
#[cfg(feature = "zmq")]
pub use zmq_sink::{Serializer, ZmqDocumentSink};

#[cfg(feature = "tiled")]
mod tiled_sink;
#[cfg(feature = "tiled")]
pub use tiled_sink::TiledSink;

pub use doc_name::document_name;
