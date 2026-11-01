//! End-to-end test: bsrs RunEngine → ZmqDocumentSink → SUB socket
//! decodes the bluesky envelope and verifies the document order.

#![cfg(feature = "zmq")]

use std::sync::Arc;
use std::time::Duration;

use bsrs::callbacks::{Serializer, ZmqDocumentSink};
use bsrs::engine::{DocumentSink, RunEngine};
use bsrs::event_model::Document;
use omq::blocking::Socket;
use omq::{Options, SocketType};

const RECV_TIMEOUT: Duration = Duration::from_secs(3);

/// Wait until the PUB socket has at least one connected peer that has
/// subscribed. Send a short marker bytes loop until SUB receives one. Returns
/// a `()` once primed.
fn prime_pub_sub(sink: &ZmqDocumentSink, sub: &Socket, marker: &str) {
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
        match sub.recv_timeout(Duration::from_millis(50)) {
            Ok(_) => return,
            Err(_) => std::thread::sleep(Duration::from_millis(50)),
        }
    }
    panic!("ZMQ PUB/SUB never primed");
}

#[tokio::test]
async fn zmq_envelope_pub_sub_round_trip() {
    let sink = Arc::new(
        ZmqDocumentSink::bind("tcp://127.0.0.1:*")
            .expect("bind sink")
            .with_serializer(Serializer::Msgpack),
    );
    let addr = sink.endpoint().to_string();

    let sub = omq::Context::new().blocking_socket(SocketType::Sub, Options::default());
    sub.connect(&addr).unwrap();
    sub.subscribe(&b""[..]).unwrap();

    prime_pub_sub(&sink, &sub, "__prime__");

    let det = bsrs::backends::soft::SoftDetector::new("zd");
    let re = RunEngine::new(vec![sink.clone() as Arc<dyn DocumentSink>]);
    let plan = bsrs::plans::count(vec![det as Arc<dyn bsrs::core::msg::ReadableObj>], 3);
    let result = re.run_async(plan).await.unwrap();
    assert_eq!(result.exit_status, "success");

    let mut names: Vec<String> = Vec::new();
    for _ in 0..6 {
        let msg = sub.recv_timeout(RECV_TIMEOUT).expect("recv");
        let msg = msg.part_slice(0).unwrap();
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
    let sink = Arc::new(
        ZmqDocumentSink::bind("tcp://127.0.0.1:*")
            .unwrap()
            .with_serializer(Serializer::Msgpack),
    );
    let addr = sink.endpoint().to_string();

    let sub = omq::Context::new().blocking_socket(SocketType::Sub, Options::default());
    sub.connect(&addr).unwrap();
    sub.subscribe(&b""[..]).unwrap();

    prime_pub_sub(&sink, &sub, "__prime2__");

    let start = bsrs::event_model::RunStart {
        uid: "test-uid".into(),
        time: 12345.0,
        scan_id: Some(7),
        hints: None,
        ..Default::default()
    };
    sink.dispatch(&Document::Start(start.clone()))
        .await
        .unwrap();

    let msg = sub.recv_timeout(RECV_TIMEOUT).unwrap();
    let msg = msg.part_slice(0).unwrap();
    let body_start = msg.iter().position(|&b| b == b' ').unwrap() + 1 + b"start ".len();
    let body = &msg[body_start..];
    let decoded: bsrs::event_model::RunStart = rmp_serde::from_slice(body).unwrap();
    assert_eq!(decoded.uid, "test-uid");
    assert_eq!(decoded.time, 12345.0);
    assert_eq!(decoded.scan_id, Some(7));
}

/// A 9 MB burst to a slow subscriber, still queued when the sink drops,
/// reaches it whole and in order: the sink drains on drop, and only the
/// message HWM, not a per-peer byte cap, mutes a subscriber.
#[test]
fn large_burst_before_drop_is_delivered_whole() {
    let sink = ZmqDocumentSink::bind("tcp://127.0.0.1:*").unwrap();
    let sub = omq::Context::new().blocking_socket(SocketType::Sub, Options::default().recv_hwm(1));
    sub.connect(sink.endpoint()).unwrap();
    sub.subscribe(&b""[..]).unwrap();
    prime_pub_sub(&sink, &sub, "__prime3__");
    while sub.recv_timeout(Duration::from_millis(100)).is_ok() {}

    for i in 0..900u16 {
        let mut doc = vec![0; 10_000];
        doc[..2].copy_from_slice(&i.to_be_bytes());
        sink.send_raw_for_test(&doc).unwrap();
    }
    let dropper = std::thread::spawn(move || drop(sink));

    let mut got = Vec::new();
    while let Ok(msg) = sub.recv_timeout(Duration::from_secs(1)) {
        let doc = msg.part_slice(0).unwrap();
        got.push(u16::from_be_bytes([doc[0], doc[1]]));
    }
    dropper.join().unwrap();
    assert_eq!(got, (0..900).collect::<Vec<u16>>());
}
