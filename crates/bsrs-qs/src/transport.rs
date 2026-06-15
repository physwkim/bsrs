//! 0MQ REP transport. The control socket of the queueserver-compatible API.

use crate::methods::QsRequest;
use bsrs_core::error::{BsrsError, Result};
use serde_json::{json, Value};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;

/// Wire encoding for a single REQ/REP exchange.
///
/// The server probes the first byte of each inbound frame:
/// `0x7B` (the ASCII `{`) → JSON; anything else → msgpack.
/// The response is encoded in the same format as the request,
/// matching the bluesky-queueserver client behaviour (ref: comms.py).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MsgEncoding {
    Json,
    MsgPack,
}

/// Wraps a 0MQ REP socket. `try_recv` waits for a request; `send` posts a response.
#[derive(Clone)]
pub struct ReqRepSocket {
    socket: Arc<StdMutex<zmq::Socket>>,
    /// When set to true (e.g. by `Server::shutdown`), the next `recv` poll
    /// returns `None` and the rep loop exits cleanly.
    shutdown: Arc<std::sync::atomic::AtomicBool>,
}

impl ReqRepSocket {
    /// Bind to a tcp/ipc address.
    ///
    /// If `curve_private_key_z85` is `Some(key)`, the socket is configured
    /// for ZMQ CURVE encryption (`ZMQ_CURVE_SERVER=1`, `ZMQ_CURVE_SECRETKEY`
    /// set to the provided Z85 key). Mirrors the reference server setup in
    /// `manager.py::zmq_server_comm`. Without a key the socket accepts
    /// plain-text connections (reference default).
    pub fn bind(address: &str, curve_private_key_z85: Option<&str>) -> Result<Self> {
        let ctx = zmq::Context::new();
        let socket = ctx
            .socket(zmq::REP)
            .map_err(|e| BsrsError::Backend(format!("zmq REP socket: {e}")))?;
        // Use a 200 ms recv timeout so the rep loop can poll `shutdown` and
        // exit cleanly when the server is dropped.
        let _ = socket.set_rcvtimeo(200);
        // Apply CURVE before bind, matching the reference's socket setup order.
        if let Some(key) = curve_private_key_z85 {
            crate::curve::apply_curve_server_key(&socket, key)?;
        }
        socket
            .bind(address)
            .map_err(|e| BsrsError::Backend(format!("zmq REP bind {address}: {e}")))?;
        Ok(Self {
            socket: Arc::new(StdMutex::new(socket)),
            shutdown: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        })
    }

    /// Signal the rep loop to exit at its next iteration.
    pub fn shutdown(&self) {
        self.shutdown
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Has shutdown been requested?
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Try receive — returns `Ok(None)` on timeout, `Ok(Some((req, encoding)))` on a
    /// valid request, `Err(_)` on a parse failure (error already replied in same encoding).
    pub fn try_recv(&self) -> Result<Option<(QsRequest, MsgEncoding)>> {
        let s = self.socket.lock().unwrap();
        match s.recv_bytes(0) {
            Ok(bytes) => {
                let enc = if bytes.first() == Some(&b'{') {
                    MsgEncoding::Json
                } else {
                    MsgEncoding::MsgPack
                };
                let parse_result: std::result::Result<QsRequest, String> = match enc {
                    MsgEncoding::Json => serde_json::from_slice(&bytes).map_err(|e| e.to_string()),
                    MsgEncoding::MsgPack => {
                        rmp_serde::from_slice(&bytes).map_err(|e| e.to_string())
                    }
                };
                match parse_result {
                    Ok(req) => Ok(Some((req, enc))),
                    Err(e) => {
                        let resp =
                            json!({"success": false, "msg": format!("invalid request: {e}")});
                        let error_bytes = encode_value(&resp, enc);
                        let _ = s.send(error_bytes, 0);
                        Err(BsrsError::Backend(format!("zmq REP parse: {e}")))
                    }
                }
            }
            Err(zmq::Error::EAGAIN) => Ok(None),
            Err(e) => Err(BsrsError::Backend(format!("zmq REP recv: {e}"))),
        }
    }

    /// Send a flat-dict response. Must be called in lock-step with `try_recv` (REP socket).
    /// `encoding` must match the encoding returned by the corresponding `try_recv` call.
    pub fn send(&self, resp: &Value, encoding: MsgEncoding) -> Result<()> {
        let s = self.socket.lock().unwrap();
        let bytes = encode_value(resp, encoding);
        s.send(bytes, 0)
            .map_err(|e| BsrsError::Backend(format!("zmq REP send: {e}")))?;
        Ok(())
    }
}

/// Encode `value` using the specified encoding.
/// For msgpack, string map keys are used (`to_vec_named`) so Python clients
/// can index by key name, matching `msgpack.packb` / `msgpack.unpackb` defaults.
fn encode_value(value: &Value, enc: MsgEncoding) -> Vec<u8> {
    match enc {
        MsgEncoding::Json => serde_json::to_vec(value).unwrap_or_default(),
        MsgEncoding::MsgPack => rmp_serde::to_vec_named(value).unwrap_or_default(),
    }
}
