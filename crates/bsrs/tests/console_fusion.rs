//! Fused-console wiring: the `bsrs console` mode pre-opens the environment at
//! startup (seeding the shared engine slot instead of waiting for a remote
//! `environment_open`) and builds a local Lua state on that *same* engine.
//!
//! These tests exercise the wiring the console assembles — they do not spawn
//! the interactive binary (which blocks on a TTY). The mutual-exclusion between
//! a local `RE:run` and the queue worker is enforced structurally by the engine
//! itself and covered by `run_async_rejects_concurrent_plan_and_releases_after`
//! in `runengine_features.rs`; the engine is the single owner of that contract,
//! so it is proven at that boundary rather than duplicated through the server.

#![cfg(all(feature = "qs", feature = "host"))]

use std::sync::Arc;
use std::time::Duration;

use bsrs::backends::soft::{SoftDetector, SoftMotor};
use bsrs::core::msg::{MovableObj, ReadableObj};
use bsrs::engine::RunEngine;
use bsrs::host::manager_lua::build_shared_lua;
use bsrs::qs::{EState, Registry, Server};
use serde_json::{json, Value};
use tokio::sync::Mutex as TMutex;

/// Send a plain bluesky-queueserver request and return the flat response dict.
fn rpc(socket: &zmq::Socket, method: &str, params: Value) -> Value {
    let req = json!({ "method": method, "params": params });
    socket.send(serde_json::to_vec(&req).unwrap(), 0).unwrap();
    let resp = socket.recv_bytes(0).unwrap();
    serde_json::from_slice(&resp).unwrap()
}

fn req_socket(control: &str) -> zmq::Socket {
    let ctx = zmq::Context::new();
    let req = ctx.socket(zmq::REQ).unwrap();
    req.set_rcvtimeo(3_000).unwrap();
    req.set_sndtimeo(3_000).unwrap();
    req.connect(control).unwrap();
    req
}

/// The console pre-opens the environment: build the engine, seed the shared
/// slot, mark the state idle — then remote `status` must report the environment
/// open **without** any `environment_open` call. Also exercises the
/// `without_document_socket()` builder path the console relies on (the engine,
/// not the server, owns the document PUB).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn console_preopen_reports_environment_open_without_open_call() {
    let mut reg = Registry::new();
    reg.register_plan_count("count");

    let re = Arc::new(RunEngine::new(vec![]));
    let engine_slot = Arc::new(TMutex::new(Some(re.clone())));

    let server = Server::builder()
        .control_address("tcp://127.0.0.1:*")
        .without_document_socket()
        .registry(reg)
        .engine_slot(engine_slot)
        .build()
        .expect("server build");
    // Pre-open marks the state idle (what env_open would have done).
    server.state_arc().lock().unwrap().state = Some(EState::Idle);

    let shutdown = server.shutdown_handle();
    tokio::spawn(async move {
        let _ = server.run_async().await;
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    let req = req_socket(shutdown.control_endpoint());
    let r = rpc(&req, "status", json!({}));
    assert_eq!(r["success"], true, "status failed: {r}");
    // No environment_open was ever sent, yet the pre-opened engine reports open.
    assert_eq!(
        r["worker_environment_exists"], true,
        "pre-opened environment must report existing: {r}"
    );
    assert_eq!(
        r["manager_state"], "idle",
        "pre-opened environment must report idle: {r}"
    );

    shutdown.shutdown();
    tokio::time::sleep(Duration::from_millis(200)).await;
}

/// The console builds its local prompt's Lua state via `build_shared_lua` on the
/// shared engine. That state must expose every registered device as a global
/// (the same publishing the daemon-side `lua_eval` state performs), so the local
/// face can drive the same devices the queue worker sees.
#[test]
fn console_local_lua_state_sees_shared_registry_devices() {
    let det = SoftDetector::new("det1");
    let motor = Arc::new(SoftMotor::new("m1", Some(0.0)));
    let mut reg = Registry::new();
    reg.register_readable("det1", det as Arc<dyn ReadableObj>);
    reg.register_readable("m1", motor.clone() as Arc<dyn ReadableObj>);
    reg.register_movable("m1", motor as Arc<dyn MovableObj>);
    reg.register_plan_count("count");

    let re = Arc::new(RunEngine::new(vec![]));
    let lua = build_shared_lua(re, &reg).expect("build local lua state");

    let present: bool = lua
        .load("return (det1 ~= nil) and (m1 ~= nil) and (nonexistent_dev == nil)")
        .eval()
        .expect("eval device-global probe");
    assert!(
        present,
        "local Lua state must expose shared registry devices as globals"
    );
}
