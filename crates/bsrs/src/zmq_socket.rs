//! A blocking omq socket that keeps libzmq's close semantics.

use std::ops::Deref;

use omq::blocking::Socket;
use omq::{Options, SocketType};

/// A blocking socket on its own omq context that, on drop, waits for its
/// queued sends to drain, as `zmq_ctx_term` does at libzmq's default
/// `ZMQ_LINGER` of -1. omq defaults to zero linger, and dropping the last
/// handle of an omq context aborts a drain still in flight, so a reply or
/// document sent just before shutdown would otherwise be lost.
pub(crate) struct LingeringSocket(Option<Socket>);

impl LingeringSocket {
    pub(crate) fn new(socket_type: SocketType, options: Options) -> Self {
        Self(Some(
            omq::Context::new().blocking_socket(socket_type, options.linger_forever()),
        ))
    }
}

impl Deref for LingeringSocket {
    type Target = Socket;

    fn deref(&self) -> &Socket {
        self.0.as_ref().expect("taken only by drop")
    }
}

impl Drop for LingeringSocket {
    fn drop(&mut self) {
        if let Some(socket) = self.0.take() {
            let _ = socket.close();
        }
    }
}
