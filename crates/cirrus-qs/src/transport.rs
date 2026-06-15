//! 0MQ REP transport. The control socket of the queueserver-compatible API.

use crate::methods::QsRequest;
use cirrus_core::error::{CirrusError, Result};
use serde_json::{json, Value};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;

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
    pub fn bind(address: &str) -> Result<Self> {
        let ctx = zmq::Context::new();
        let socket = ctx
            .socket(zmq::REP)
            .map_err(|e| CirrusError::Backend(format!("zmq REP socket: {e}")))?;
        // Use a 200 ms recv timeout so the rep loop can poll `shutdown` and
        // exit cleanly when the server is dropped.
        let _ = socket.set_rcvtimeo(200);
        socket
            .bind(address)
            .map_err(|e| CirrusError::Backend(format!("zmq REP bind {address}: {e}")))?;
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

    /// Try receive — returns `Ok(None)` on timeout, `Ok(Some(req))` on a
    /// valid request, `Err(_)` on a parse failure (error already replied).
    pub fn try_recv(&self) -> Result<Option<QsRequest>> {
        let s = self.socket.lock().unwrap();
        match s.recv_bytes(0) {
            Ok(bytes) => match serde_json::from_slice(&bytes) {
                Ok(req) => Ok(Some(req)),
                Err(e) => {
                    let resp = json!({"success": false, "msg": format!("invalid request: {e}")});
                    let _ = s.send(serde_json::to_vec(&resp).unwrap_or_default(), 0);
                    Err(CirrusError::Backend(format!("zmq REP parse: {e}")))
                }
            },
            Err(zmq::Error::EAGAIN) => Ok(None),
            Err(e) => Err(CirrusError::Backend(format!("zmq REP recv: {e}"))),
        }
    }

    /// Send a flat-dict response. Must be called in lock-step with `try_recv` (REP socket).
    pub fn send(&self, resp: &Value) -> Result<()> {
        let s = self.socket.lock().unwrap();
        let bytes = serde_json::to_vec(resp)?;
        s.send(bytes, 0)
            .map_err(|e| CirrusError::Backend(format!("zmq REP send: {e}")))?;
        Ok(())
    }
}
