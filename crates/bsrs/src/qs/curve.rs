//! ZMQ CURVE encryption helpers, mirroring `comms.py::generate_zmq_keys`.
//!
//! The server reads the private key from the `QSERVER_ZMQ_PRIVATE_KEY`
//! environment variable (same name as the bluesky-queueserver reference).
//! If unset, CURVE is disabled and the server accepts plain-text connections.

use crate::core::error::{BsrsError, Result};
use omq::{CurveKeypair, CurveSecretKey, Options};

/// Generate a new CURVE keypair.
///
/// Returns `(public_key_z85, private_key_z85)` — two 40-character Z85
/// strings. Mirrors `comms.py::generate_zmq_keys()`.
///
/// The private key goes to the server (`ServerBuilder::curve_private_key`
/// or `QSERVER_ZMQ_PRIVATE_KEY`). The public key is distributed to clients
/// out-of-band.
pub fn generate_zmq_keys() -> Result<(String, String)> {
    let pair = CurveKeypair::generate();
    Ok((pair.public.to_z85(), pair.secret.to_z85()))
}

/// Socket options for a CURVE server holding the given private key.
///
/// The equivalent of the reference's
///   `self._zmq_socket.set(zmq.CURVE_SERVER, 1)`
///   `self._zmq_socket.set(zmq.CURVE_SECRETKEY, key.encode("utf-8"))`
/// with the public half derived from the private key, as libzmq does.
///
/// Returns an error if the key is not a 40-character Z85 string.
pub(crate) fn curve_server_options(private_key_z85: &str) -> Result<Options> {
    if private_key_z85.len() != 40 {
        return Err(BsrsError::Backend(format!(
            "QSERVER_ZMQ_PRIVATE_KEY must be a 40-char Z85 string, got {} chars",
            private_key_z85.len()
        )));
    }
    let secret = CurveSecretKey::from_z85(private_key_z85)
        .map_err(|e| BsrsError::Backend(format!("CURVE_SECRETKEY: {e}")))?;
    Ok(Options::default().curve_server(CurveKeypair {
        public: secret.derive_public(),
        secret,
    }))
}

/// Returns `true` if ZMQ CURVE is available in this build. omq implements
/// CURVE in Rust and is always built with it.
pub fn curve_supported() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_zmq_keys_returns_two_valid_z85_strings() {
        let (public, secret) = generate_zmq_keys().expect("keypair generation must succeed");
        assert_eq!(public.len(), 40, "public key must be 40 chars");
        assert_eq!(secret.len(), 40, "secret key must be 40 chars");
        assert!(
            public.is_ascii() && public.chars().all(|c| !c.is_control()),
            "public key must be printable ASCII: {public}"
        );
        assert!(
            secret.is_ascii() && secret.chars().all(|c| !c.is_control()),
            "secret key must be printable ASCII: {secret}"
        );
        // Two independent calls must produce different keys.
        let (public2, _) = generate_zmq_keys().expect("second keypair");
        assert_ne!(public, public2, "each call must generate a unique keypair");
    }
}
