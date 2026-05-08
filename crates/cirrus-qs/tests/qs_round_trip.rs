//! End-to-end test: REQ → cirrus-qs REP → engine → response.

use std::sync::Arc;
use std::time::Duration;

use cirrus_backend_soft::SoftDetector;
use cirrus_core::msg::ReadableObj;
use cirrus_qs::{Registry, Server, ServerShutdown};
use serde_json::{json, Value};

fn rand_port() -> u16 {
    use std::sync::atomic::{AtomicU16, Ordering};
    static NEXT: AtomicU16 = AtomicU16::new(0);
    let bump = NEXT.fetch_add(1, Ordering::SeqCst);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u32;
    let base = 49_000u16;
    let offset = ((nanos.wrapping_add(bump as u32 * 17)) & 0x3FF) as u16;
    base.saturating_add(offset)
}

fn endpoint(port: u16) -> String {
    format!(
        "ipc:///tmp/cirrus-qs-test-{}-{}.sock",
        std::process::id(),
        port
    )
}

fn rpc(socket: &zmq::Socket, method: &str, params: Value) -> Value {
    let req = json!({
        "jsonrpc": "2.0",
        "method": method,
        "params": params,
        "id": 1,
    });
    socket.send(serde_json::to_vec(&req).unwrap(), 0).unwrap();
    let resp = socket.recv_bytes(0).unwrap();
    serde_json::from_slice(&resp).unwrap()
}

fn spawn_server(reg: Registry, port: u16) -> ServerShutdown {
    let ep = endpoint(port);
    let server = Server::builder()
        .control_address(ep)
        .document_address(format!(
            "ipc:///tmp/cirrus-qs-doc-{}-{}.sock",
            std::process::id(),
            port
        ))
        .registry(reg)
        .build()
        .expect("server build");
    let shutdown = server.shutdown_handle();
    tokio::spawn(async move {
        let _ = server.run_async().await;
    });
    shutdown
}

fn req_socket(port: u16) -> zmq::Socket {
    let ctx = zmq::Context::new();
    let req = ctx.socket(zmq::REQ).unwrap();
    req.set_rcvtimeo(3_000).unwrap();
    req.set_sndtimeo(3_000).unwrap();
    req.connect(&endpoint(port)).unwrap();
    req
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ping_works() {
    let port = rand_port();
    let mut reg = Registry::new();
    reg.register_plan_count("count");
    let shutdown = spawn_server(reg, port);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let req = req_socket(port);
    let r = rpc(&req, "ping", json!({}));
    assert_eq!(r["result"]["msg"], "pong");

    shutdown.shutdown();
    tokio::time::sleep(Duration::from_millis(300)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn end_to_end_count_through_qs() {
    let port = rand_port();
    let det = SoftDetector::new("det1");
    let mut reg = Registry::new();
    reg.register_readable("det1", det as Arc<dyn ReadableObj>);
    reg.register_plan_count("count");
    let shutdown = spawn_server(reg, port);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let req = req_socket(port);

    let r = rpc(&req, "environment_open", json!({}));
    assert_eq!(r["result"]["success"], true);

    let r = rpc(&req, "plans_allowed", json!({}));
    let plans = r["result"]["plans_allowed"].as_array().unwrap();
    assert!(plans.iter().any(|v| v == "count"));

    let r = rpc(&req, "devices_allowed", json!({}));
    let devs = r["result"]["devices_allowed"].as_array().unwrap();
    assert!(devs.iter().any(|v| v == "det1"));

    let r = rpc(
        &req,
        "queue_item_add",
        json!({"item": {"name": "count", "args": ["det1", 3]}}),
    );
    assert_eq!(r["result"]["success"], true);
    assert_eq!(r["result"]["qsize"], 1);

    let r = rpc(&req, "status", json!({}));
    assert_eq!(r["result"]["items_in_queue"], 1);
    assert_eq!(r["result"]["manager_state"], "idle");

    let r = rpc(&req, "queue_start", json!({}));
    assert_eq!(r["result"]["success"], true);

    let mut done = false;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let r = rpc(&req, "status", json!({}));
        if r["result"]["plans_run"].as_u64().unwrap_or(0) >= 1
            && r["result"]["items_in_queue"] == 0
            && r["result"]["manager_state"] == "idle"
        {
            done = true;
            break;
        }
    }
    assert!(done, "queue did not finish");

    shutdown.shutdown();
    tokio::time::sleep(Duration::from_millis(300)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_plan_rejected() {
    let port = rand_port();
    let mut reg = Registry::new();
    reg.register_plan_count("count");
    let shutdown = spawn_server(reg, port);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let req = req_socket(port);

    let r = rpc(
        &req,
        "queue_item_add",
        json!({"item": {"name": "no_such_plan", "args": []}}),
    );
    assert!(r["error"]["message"]
        .as_str()
        .unwrap()
        .contains("unknown plan"));

    shutdown.shutdown();
    tokio::time::sleep(Duration::from_millis(300)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_method_returns_jsonrpc_error() {
    let port = rand_port();
    let mut reg = Registry::new();
    reg.register_plan_count("count");
    let shutdown = spawn_server(reg, port);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let req = req_socket(port);

    let r = rpc(&req, "no_such_method", json!({}));
    assert_eq!(r["error"]["code"], -32601);

    shutdown.shutdown();
    tokio::time::sleep(Duration::from_millis(300)).await;
}
