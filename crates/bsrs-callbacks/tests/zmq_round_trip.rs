//! End-to-end test: bsrs RunEngine → ZmqDocumentSink → SUB socket
//! decodes the bluesky envelope and verifies the document order.

#![cfg(feature = "zmq")]

use std::sync::Arc;
use std::time::Duration;

use bsrs_callbacks::{Serializer, ZmqDocumentSink};
use bsrs_engine::{DocumentSink, RunEngine};
use bsrs_event_model::Document;

fn rand_id() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    // Combine a monotonically-incrementing counter with the wall-clock
    // nanos so two tests scheduled within the same nanosecond still get
    // distinct ids. Without the counter, parallel test runs occasionally
    // collide on the IPC socket path and the second test fails with
    // "Address already in use".
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    nanos.wrapping_add(n.wrapping_mul(0x9E37_79B9_7F4A_7C15))
}

/// Wait until the PUB socket has at least one connected peer that has
/// subscribed. Send a short marker bytes loop until SUB receives one. Returns
/// a `()` once primed.
fn prime_pub_sub(sink: &ZmqDocumentSink, sub: &zmq::Socket, marker: &str) {
    // Bind/connect must already be done on caller side. We send a marker
    // up to N times (every 50 ms) and bail when SUB sees it.
    for _ in 0..40 {
        // Synthetic envelope shaped like the real one, with a unique name we
        // can drop on the receiver side. Send via the same context as `sink`.
        let env = {
            let mut buf = Vec::new();
            buf.push(b' ');
            buf.extend_from_slice(marker.as_bytes());
            buf.push(b' ');
            buf
        };
        sink.send_raw_for_test(&env).ok();
        // Try a short recv; if we see the marker, drop it and return.
        match sub.recv_bytes(0) {
            Ok(_) => return,
            Err(_) => std::thread::sleep(Duration::from_millis(50)),
        }
    }
    panic!("ZMQ PUB/SUB never primed");
}

#[tokio::test]
async fn zmq_envelope_pub_sub_round_trip() {
    let addr = format!(
        "ipc:///tmp/bsrs-zmq-test-{}-{}.sock",
        std::process::id(),
        rand_id()
    );

    let sink = Arc::new(
        ZmqDocumentSink::bind(&addr)
            .expect("bind sink")
            .with_serializer(Serializer::Msgpack),
    );

    let ctx = zmq::Context::new();
    let sub = ctx.socket(zmq::SUB).unwrap();
    sub.set_rcvtimeo(50).unwrap();
    sub.connect(&addr).unwrap();
    sub.set_subscribe(b"").unwrap();

    prime_pub_sub(&sink, &sub, "__prime__");

    // Now switch SUB to a longer recv timeout for the real test.
    sub.set_rcvtimeo(3_000).unwrap();

    let det = bsrs_backend_soft::SoftDetector::new("zd");
    let re = RunEngine::new(vec![sink.clone() as Arc<dyn DocumentSink>]);
    let plan = bsrs_plans::count(vec![det as Arc<dyn bsrs_core::msg::ReadableObj>], 3);
    let result = re.run_async(plan).await.unwrap();
    assert_eq!(result.exit_status, "success");

    let mut names: Vec<String> = Vec::new();
    for _ in 0..6 {
        let msg = sub.recv_bytes(0).expect("recv");
        let first = msg.iter().position(|&b| b == b' ').unwrap();
        let rest = &msg[first + 1..];
        let second = rest.iter().position(|&b| b == b' ').unwrap();
        let name = std::str::from_utf8(&rest[..second]).unwrap().to_string();
        names.push(name);
    }
    assert_eq!(
        names,
        vec!["start", "descriptor", "event", "event", "event", "stop"]
    );
}

#[tokio::test]
async fn zmq_msgpack_body_decodes_to_runstart() {
    let addr = format!(
        "ipc:///tmp/bsrs-zmq-test-{}-{}.sock",
        std::process::id(),
        rand_id()
    );

    let sink = Arc::new(
        ZmqDocumentSink::bind(&addr)
            .unwrap()
            .with_serializer(Serializer::Msgpack),
    );

    let ctx = zmq::Context::new();
    let sub = ctx.socket(zmq::SUB).unwrap();
    sub.set_rcvtimeo(50).unwrap();
    sub.connect(&addr).unwrap();
    sub.set_subscribe(b"").unwrap();

    prime_pub_sub(&sink, &sub, "__prime2__");

    sub.set_rcvtimeo(3_000).unwrap();

    let start = bsrs_event_model::RunStart {
        uid: "test-uid".into(),
        time: 12345.0,
        scan_id: Some(7),
        hints: None,
        ..Default::default()
    };
    sink.dispatch(&Document::Start(start.clone()))
        .await
        .unwrap();

    let msg = sub.recv_bytes(0).unwrap();
    let body_start = msg.iter().position(|&b| b == b' ').unwrap() + 1 + b"start ".len();
    let body = &msg[body_start..];
    let decoded: bsrs_event_model::RunStart = rmp_serde::from_slice(body).unwrap();
    assert_eq!(decoded.uid, "test-uid");
    assert_eq!(decoded.time, 12345.0);
    assert_eq!(decoded.scan_id, Some(7));
}
