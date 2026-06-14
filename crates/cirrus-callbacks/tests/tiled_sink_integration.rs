//! Integration test for `TiledSink` (tiled-client backed).
//!
//! Spins up a minimal axum-free mock HTTP server that records the
//! incoming POST/PATCH requests, then drives `TiledSink::dispatch`
//! over Start + Stop documents and asserts the wire payloads.
//!
//! This proves the tiled-client integration is end-to-end functional
//! without requiring tiled-server to support the register / metadata
//! patch endpoints (it currently does not — tiled-server is read-only
//! at the HTTP layer).

#![cfg(feature = "tiled")]

use std::collections::HashMap;
use std::sync::Arc;

use cirrus_callbacks::TiledSink;
use cirrus_engine::DocumentSink;
use cirrus_event_model::{Document, RunStart, RunStop};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

#[derive(Default, Clone, Debug)]
struct CapturedRequest {
    method: String,
    path: String,
    body: String,
}

async fn spawn_mock(captured: Arc<Mutex<Vec<CapturedRequest>>>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base = format!("http://{addr}");
    tokio::spawn(async move {
        loop {
            let (mut stream, _) = match listener.accept().await {
                Ok(p) => p,
                Err(_) => return,
            };
            let captured = captured.clone();
            tokio::spawn(async move {
                let (rd, mut wr) = stream.split();
                let mut rd = BufReader::new(rd);
                // Parse request line.
                let mut line = String::new();
                if rd.read_line(&mut line).await.is_err() {
                    return;
                }
                let mut parts = line.split_whitespace();
                let method = parts.next().unwrap_or("").to_string();
                let path = parts.next().unwrap_or("").to_string();
                // Parse headers, find Content-Length.
                let mut content_length = 0usize;
                loop {
                    let mut h = String::new();
                    if rd.read_line(&mut h).await.is_err() {
                        return;
                    }
                    if h == "\r\n" || h.is_empty() {
                        break;
                    }
                    if let Some(rest) = h.to_ascii_lowercase().strip_prefix("content-length:") {
                        if let Ok(n) = rest.trim().parse::<usize>() {
                            content_length = n;
                        }
                    }
                }
                let mut body_buf = vec![0u8; content_length];
                if content_length > 0 {
                    let _ = rd.read_exact(&mut body_buf).await;
                }
                let body = String::from_utf8_lossy(&body_buf).to_string();
                captured
                    .lock()
                    .await
                    .push(CapturedRequest { method, path, body });
                let resp = b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}";
                let _ = wr.write_all(resp).await;
                let _ = wr.shutdown().await;
            });
        }
    });
    // Give the listener a tick to be ready.
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    base
}

fn make_run_start() -> RunStart {
    let mut extra = HashMap::new();
    extra.insert(
        "plan_name".into(),
        serde_json::Value::String("count".into()),
    );
    extra.insert("operator".into(), serde_json::Value::String("alice".into()));
    RunStart {
        uid: "run-1".into(),
        time: 1.0,
        scan_id: Some(7),
        hints: None,
        extra,
        ..Default::default()
    }
}

fn make_run_stop() -> RunStop {
    RunStop {
        uid: "stop-1".into(),
        run_start: "run-1".into(),
        time: 2.0,
        exit_status: "success".into(),
        reason: None,
        num_events: HashMap::new(),
    }
}

#[tokio::test]
async fn dispatch_start_then_stop_hits_register_then_patch() {
    let captured: Arc<Mutex<Vec<CapturedRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let base = spawn_mock(captured.clone()).await;

    let sink = TiledSink::new(base, "bluesky").expect("build sink");

    sink.dispatch(&Document::Start(make_run_start()))
        .await
        .expect("start dispatch ok");
    sink.dispatch(&Document::Stop(make_run_stop()))
        .await
        .expect("stop dispatch ok");

    let reqs = captured.lock().await.clone();
    assert!(
        reqs.len() >= 2,
        "expected at least 2 requests, got {reqs:#?}"
    );

    // First: POST /api/v1/register/bluesky with the start metadata.
    let post = reqs
        .iter()
        .find(|r| r.method == "POST" && r.path.contains("/register/bluesky"))
        .unwrap_or_else(|| panic!("no register POST in {reqs:#?}"));
    assert!(post.body.contains("\"key\":\"run-1\""));
    assert!(post.body.contains("\"BlueskyRun\""));
    assert!(post.body.contains("\"plan_name\":\"count\""));
    assert!(post.body.contains("\"operator\":\"alice\""));

    // Second: PATCH /api/v1/metadata/bluesky/run-1 with the stop body.
    let patch = reqs
        .iter()
        .find(|r| r.method == "PATCH" && r.path.contains("/metadata/bluesky/run-1"))
        .unwrap_or_else(|| panic!("no metadata PATCH in {reqs:#?}"));
    assert!(patch.body.contains("\"exit_status\":\"success\""));
    assert!(patch.body.contains("\"run_start\":\"run-1\""));
}

#[tokio::test]
async fn dispatch_start_is_idempotent_for_same_run_uid() {
    let captured: Arc<Mutex<Vec<CapturedRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let base = spawn_mock(captured.clone()).await;
    let sink = TiledSink::new(base, "bluesky").expect("build sink");

    let s = make_run_start();
    sink.dispatch(&Document::Start(s.clone())).await.unwrap();
    sink.dispatch(&Document::Start(s.clone())).await.unwrap();
    sink.dispatch(&Document::Start(s)).await.unwrap();

    let reqs = captured.lock().await.clone();
    let posts: Vec<_> = reqs
        .iter()
        .filter(|r| r.method == "POST" && r.path.contains("/register/"))
        .collect();
    assert_eq!(
        posts.len(),
        1,
        "register POST must fire only once per run uid; got {} (reqs={reqs:#?})",
        posts.len()
    );
}

#[tokio::test]
async fn drop_event_documents_emit_no_http_traffic() {
    let captured: Arc<Mutex<Vec<CapturedRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let base = spawn_mock(captured.clone()).await;
    let sink = TiledSink::new(base, "bluesky").expect("build sink");

    // EventDescriptor / Event etc. should be silently dropped (a TiledFullSink
    // would handle them; this minimal sink does not).
    let descriptor = cirrus_event_model::EventDescriptor {
        uid: "d-1".into(),
        run_start: "run-1".into(),
        time: 1.5,
        name: Some("primary".into()),
        data_keys: HashMap::new(),
        object_keys: HashMap::new(),
        configuration: HashMap::new(),
        hints: None,
    };
    sink.dispatch(&Document::Descriptor(descriptor))
        .await
        .unwrap();

    let reqs = captured.lock().await.clone();
    assert!(
        reqs.is_empty(),
        "non-Start/Stop docs must not generate HTTP traffic; got {reqs:#?}"
    );
}
