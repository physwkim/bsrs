//! 0MQ REP transport. The control socket of the queueserver-compatible API.

use crate::methods::{codes, RpcRequest, RpcResponse};
use cirrus_core::error::{CirrusError, Result};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;

/// Wraps a 0MQ REP socket. `recv` waits for a request; `send` posts a response.
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
    /// valid request, `Err(_)` on a parse failure (already responded).
    pub fn try_recv(&self) -> Result<Option<RpcRequest>> {
        let s = self.socket.lock().unwrap();
        match s.recv_bytes(0) {
            Ok(bytes) => match serde_json::from_slice(&bytes) {
                Ok(req) => Ok(Some(req)),
                Err(e) => {
                    let _ = s.send(
                        serde_json::to_vec(&RpcResponse::err(
                            None,
                            codes::INVALID_REQUEST,
                            format!("invalid JSON: {e}"),
                        ))
                        .unwrap_or_default(),
                        0,
                    );
                    Err(CirrusError::Backend(format!("zmq REP parse: {e}")))
                }
            },
            Err(zmq::Error::EAGAIN) => Ok(None),
            Err(e) => Err(CirrusError::Backend(format!("zmq REP recv: {e}"))),
        }
    }

    /// Blocking recv (legacy — kept for parity, prefer `try_recv` in loops).
    #[allow(dead_code)]
    pub fn recv(&self) -> Result<RpcRequest> {
        let s = self.socket.lock().unwrap();
        let bytes = s
            .recv_bytes(0)
            .map_err(|e| CirrusError::Backend(format!("zmq REP recv: {e}")))?;
        serde_json::from_slice(&bytes).map_err(|e| {
            // Send a parse-error response and let the caller bail out next iter.
            let _ = s.send(
                serde_json::to_vec(&RpcResponse::err(
                    None,
                    codes::INVALID_REQUEST,
                    format!("invalid JSON: {e}"),
                ))
                .unwrap_or_default(),
                0,
            );
            CirrusError::Backend(format!("zmq REP parse: {e}"))
        })
    }

    /// Send a response. Must be called in lock-step with `recv` (REP socket).
    pub fn send(&self, resp: &RpcResponse) -> Result<()> {
        let s = self.socket.lock().unwrap();
        let bytes = serde_json::to_vec(resp)?;
        s.send(bytes, 0)
            .map_err(|e| CirrusError::Backend(format!("zmq REP send: {e}")))?;
        Ok(())
    }
}
