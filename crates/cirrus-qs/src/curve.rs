//! ZMQ CURVE encryption helpers, mirroring `comms.py::generate_zmq_keys`.
//!
//! The server reads the private key from the `QSERVER_ZMQ_PRIVATE_KEY`
//! environment variable (same name as the bluesky-queueserver reference).
//! If unset, CURVE is disabled and the server accepts plain-text connections.

use cirrus_core::error::{CirrusError, Result};

/// Generate a new CURVE keypair.
///
/// Returns `(public_key_z85, private_key_z85)` — two 40-character Z85
/// strings. Mirrors `comms.py::generate_zmq_keys()`.
///
/// The private key goes to the server (`ServerBuilder::curve_private_key`
/// or `QSERVER_ZMQ_PRIVATE_KEY`). The public key is distributed to clients
/// out-of-band.
pub fn generate_zmq_keys() -> Result<(String, String)> {
    let pair = zmq::CurveKeyPair::new()
        .map_err(|e| CirrusError::Backend(format!("zmq::CurveKeyPair::new: {e}")))?;
    let public = zmq::z85_encode(&pair.public_key)
        .map_err(|_| CirrusError::Backend("z85_encode public key failed".into()))?;
    let secret = zmq::z85_encode(&pair.secret_key)
        .map_err(|_| CirrusError::Backend("z85_encode secret key failed".into()))?;
    Ok((public, secret))
}

/// Apply a CURVE private key to a REP socket.
///
/// Sets `ZMQ_CURVE_SERVER=1` and `ZMQ_CURVE_SECRETKEY` to the provided
/// Z85-encoded private key. Matches the reference:
///   `self._zmq_socket.set(zmq.CURVE_SERVER, 1)`
///   `self._zmq_socket.set(zmq.CURVE_SECRETKEY, key.encode("utf-8"))`
///
/// The key is passed as raw Z85 bytes (40 bytes), which libzmq interprets
/// as a Z85-encoded key per the ZMQ RFC 32 convention.
///
/// Returns an error if the key is not exactly 40 characters or the socket
/// options cannot be set.
pub(crate) fn apply_curve_server_key(socket: &zmq::Socket, private_key_z85: &str) -> Result<()> {
    if private_key_z85.len() != 40 {
        return Err(CirrusError::Backend(format!(
            "QSERVER_ZMQ_PRIVATE_KEY must be a 40-char Z85 string, got {} chars",
            private_key_z85.len()
        )));
    }
    socket
        .set_curve_server(true)
        .map_err(|e| CirrusError::Backend(format!("CURVE_SERVER: {e}")))?;
    // Pass the Z85 string as ASCII bytes (40 bytes); libzmq accepts either
    // 32 raw bytes or 40 Z85 bytes for CURVE_SECRETKEY.
    socket
        .set_curve_secretkey(private_key_z85.as_bytes())
        .map_err(|e| CirrusError::Backend(format!("CURVE_SECRETKEY: {e}")))?;
    Ok(())
}

/// Returns `true` if ZMQ CURVE is available in this build (requires libsodium).
pub fn curve_supported() -> bool {
    zmq::CurveKeyPair::new().is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_zmq_keys_returns_two_valid_z85_strings() {
        if !curve_supported() {
            eprintln!("CURVE not available in this libzmq build (no libsodium) — skipping");
            return;
        }
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
