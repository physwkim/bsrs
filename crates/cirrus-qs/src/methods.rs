//! bluesky-queueserver plain-dict wire types.
//!
//! The bluesky-queueserver ZMQ protocol uses plain Python dicts, not
//! JSON-RPC 2.0 envelopes:
//!   - Request:  `{"method": <str>, "params": {...}}`
//!   - Response: flat dict `{"success": bool, "msg": str, ...fields...}`
//!
//! Reference: `bluesky_queueserver/manager/comms.py:_create_msg` and
//! `manager.py:_zmq_execute` (enforces `allowed_keys = ("method", "params")`).

use serde::Deserialize;
use serde_json::{json, Value};

/// Inbound request: `{"method": <str>, "params": {...}}`.
#[derive(Debug, Clone, Deserialize)]
pub struct QsRequest {
    /// Method name, e.g. `"queue_item_add"`.
    pub method: String,
    /// Method parameters — typically a JSON object/dict.
    #[serde(default)]
    pub params: Value,
}

/// Build a flat error response: `{"success": false, "msg": <msg>}`.
pub fn err(msg: impl Into<String>) -> Value {
    json!({"success": false, "msg": msg.into()})
}
