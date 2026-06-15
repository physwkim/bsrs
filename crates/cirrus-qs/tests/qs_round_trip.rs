//! End-to-end test: REQ → cirrus-qs REP → engine → response.
//!
//! All requests use the plain bluesky-queueserver wire format:
//!   `{"method": ..., "params": {...}}`
//! Responses are flat dicts `{"success": bool, "msg": str, ...fields...}`.

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
    let base = 32_768u16;
    let offset = ((nanos.wrapping_add(bump as u32 * 16_777_213)) & 0x3FFF) as u16;
    base.saturating_add(offset)
}

fn endpoint(port: u16) -> String {
    format!(
        "ipc:///tmp/cirrus-qs-test-{}-{}.sock",
        std::process::id(),
        port
    )
}

/// Send a plain bluesky-queueserver request `{"method", "params"}` and return
/// the flat response dict.
fn rpc(socket: &zmq::Socket, method: &str, params: Value) -> Value {
    let req = json!({
        "method": method,
        "params": params,
    });
    socket.send(serde_json::to_vec(&req).unwrap(), 0).unwrap();
    let resp = socket.recv_bytes(0).unwrap();
    serde_json::from_slice(&resp).unwrap()
}

fn spawn_server(reg: Registry, port: u16) -> ServerShutdown {
    spawn_server_inner(reg, port, None)
}

fn spawn_server_with_perms(
    reg: Registry,
    port: u16,
    perms_path: std::path::PathBuf,
) -> ServerShutdown {
    spawn_server_inner(reg, port, Some(perms_path))
}

fn spawn_server_inner(
    reg: Registry,
    port: u16,
    perms_path: Option<std::path::PathBuf>,
) -> ServerShutdown {
    let ep = endpoint(port);
    let mut builder = Server::builder()
        .control_address(ep)
        .document_address(format!(
            "ipc:///tmp/cirrus-qs-doc-{}-{}.sock",
            std::process::id(),
            port
        ))
        .registry(reg);
    if let Some(p) = perms_path {
        builder = builder.permissions_path(p);
    }
    let server = builder.build().expect("server build");
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
    // ping returns the full status dict (same as status, ref manager.py:1888).
    let r = rpc(&req, "ping", json!({}));
    assert_eq!(r["success"], true);
    assert!(
        r["manager_state"].is_string(),
        "ping should return status dict: {r}"
    );

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
    assert_eq!(r["success"], true);

    let r = rpc(&req, "plans_allowed", json!({}));
    let plans = r["plans_allowed"].as_object().unwrap();
    assert!(
        plans.contains_key("count"),
        "plans_allowed should contain 'count': {r}"
    );

    let r = rpc(&req, "devices_allowed", json!({}));
    let devs = r["devices_allowed"].as_object().unwrap();
    assert!(
        devs.contains_key("det1"),
        "devices_allowed should contain 'det1': {r}"
    );

    let r = rpc(
        &req,
        "queue_item_add",
        json!({"item": {"name": "count", "args": ["det1", 3]}}),
    );
    assert_eq!(r["success"], true);
    assert_eq!(r["qsize"], 1);

    let r = rpc(&req, "status", json!({}));
    assert_eq!(r["items_in_queue"], 1);
    assert_eq!(r["manager_state"], "idle");

    let r = rpc(&req, "queue_start", json!({}));
    assert_eq!(r["success"], true);

    let mut done = false;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let r = rpc(&req, "status", json!({}));
        if r["plans_run"].as_u64().unwrap_or(0) >= 1
            && r["items_in_queue"] == 0
            && r["manager_state"] == "idle"
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
    assert!(!r["success"].as_bool().unwrap_or(true));
    assert!(r["msg"].as_str().unwrap().contains("unknown plan"));

    shutdown.shutdown();
    tokio::time::sleep(Duration::from_millis(300)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_aborts_running_queue_task() {
    use cirrus_core::msg::Msg;
    use cirrus_core::plan::plan_box;
    use std::sync::atomic::{AtomicU64, Ordering};

    let port = rand_port();
    let counter = Arc::new(AtomicU64::new(0));
    let counter_for_factory = counter.clone();
    let mut reg = Registry::new();
    let factory: cirrus_qs::PlanFactory = Arc::new(move |_reg, _args| {
        let c = counter_for_factory.clone();
        Ok(plan_box(async_stream::stream! {
            loop {
                yield Msg::Sleep(Duration::from_millis(50));
                c.fetch_add(1, Ordering::SeqCst);
            }
        }))
    });
    reg.register_plan("long_loop", factory);
    let shutdown = spawn_server(reg, port);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let req = req_socket(port);
    rpc(&req, "environment_open", json!({}));
    rpc(
        &req,
        "queue_item_add",
        json!({"item": {"name": "long_loop", "args": []}}),
    );
    rpc(&req, "queue_start", json!({}));
    tokio::time::sleep(Duration::from_millis(300)).await;

    let mid = counter.load(Ordering::SeqCst);
    assert!(mid > 0, "queue worker did not advance pre-shutdown");

    shutdown.shutdown();
    tokio::time::sleep(Duration::from_millis(400)).await;
    let after = counter.load(Ordering::SeqCst);

    tokio::time::sleep(Duration::from_millis(500)).await;
    let later = counter.load(Ordering::SeqCst);
    assert_eq!(
        after, later,
        "queue worker continued ticking after shutdown — abort did not propagate"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_method_returns_error() {
    let port = rand_port();
    let mut reg = Registry::new();
    reg.register_plan_count("count");
    let shutdown = spawn_server(reg, port);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let req = req_socket(port);

    let r = rpc(&req, "no_such_method", json!({}));
    assert!(!r["success"].as_bool().unwrap_or(true));
    assert!(r["msg"].as_str().unwrap().contains("unknown method"));

    shutdown.shutdown();
    tokio::time::sleep(Duration::from_millis(300)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn config_get_returns_implementation_metadata() {
    let port = rand_port();
    let reg = Registry::new();
    let shutdown = spawn_server(reg, port);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let req = req_socket(port);
    let r = rpc(&req, "config_get", json!({}));
    assert_eq!(r["config"]["implementation"], "cirrus-qs");
    assert!(r["config"]["version"].is_string());
    shutdown.shutdown();
    tokio::time::sleep(Duration::from_millis(300)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn plans_existing_matches_plans_allowed() {
    let port = rand_port();
    let mut reg = Registry::new();
    reg.register_plan_count("count");
    let shutdown = spawn_server(reg, port);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let req = req_socket(port);
    let allowed = rpc(&req, "plans_allowed", json!({}));
    let existing = rpc(&req, "plans_existing", json!({}));
    assert_eq!(allowed["plans_allowed"], existing["plans_existing"]);
    shutdown.shutdown();
    tokio::time::sleep(Duration::from_millis(300)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queue_clear_empties_queue() {
    let port = rand_port();
    let det = SoftDetector::new("det1");
    let mut reg = Registry::new();
    reg.register_readable("det1", det as Arc<dyn ReadableObj>);
    reg.register_plan_count("count");
    let shutdown = spawn_server(reg, port);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let req = req_socket(port);
    rpc(
        &req,
        "queue_item_add",
        json!({"item": {"name": "count", "args": ["det1", 1]}}),
    );
    rpc(
        &req,
        "queue_item_add",
        json!({"item": {"name": "count", "args": ["det1", 1]}}),
    );
    let s = rpc(&req, "status", json!({}));
    assert_eq!(s["items_in_queue"], 2);
    rpc(&req, "queue_clear", json!({}));
    let s = rpc(&req, "status", json!({}));
    assert_eq!(s["items_in_queue"], 0);
    shutdown.shutdown();
    tokio::time::sleep(Duration::from_millis(300)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queue_item_move_and_get_by_uid() {
    let port = rand_port();
    let det = SoftDetector::new("det1");
    let mut reg = Registry::new();
    reg.register_readable("det1", det as Arc<dyn ReadableObj>);
    reg.register_plan_count("count");
    let shutdown = spawn_server(reg, port);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let req = req_socket(port);
    let r1 = rpc(
        &req,
        "queue_item_add",
        json!({"item": {"name": "count", "args": ["det1", 1]}}),
    );
    let r2 = rpc(
        &req,
        "queue_item_add",
        json!({"item": {"name": "count", "args": ["det1", 2]}}),
    );
    let uid_first = r1["item"]["item_uid"].as_str().unwrap().to_string();
    let uid_second = r2["item"]["item_uid"].as_str().unwrap().to_string();

    // Move the second item to the front.
    let mv = rpc(
        &req,
        "queue_item_move",
        json!({"uid": uid_second, "pos_dest": "front"}),
    );
    assert_eq!(mv["success"], true);

    // Verify queue order via queue_get.
    let q = rpc(&req, "queue_get", json!({}));
    let items = q["items"].as_array().unwrap();
    assert_eq!(items[0]["item_uid"], uid_second);
    assert_eq!(items[1]["item_uid"], uid_first);

    // queue_item_get by uid.
    let one = rpc(&req, "queue_item_get", json!({"uid": uid_first}));
    assert_eq!(one["item"]["item_uid"], uid_first);

    shutdown.shutdown();
    tokio::time::sleep(Duration::from_millis(300)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn history_populates_after_run() {
    let port = rand_port();
    let det = SoftDetector::new("det1");
    let mut reg = Registry::new();
    reg.register_readable("det1", det as Arc<dyn ReadableObj>);
    reg.register_plan_count("count");
    let shutdown = spawn_server(reg, port);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let req = req_socket(port);

    rpc(&req, "environment_open", json!({}));
    rpc(
        &req,
        "queue_item_add",
        json!({"item": {"name": "count", "args": ["det1", 1]}}),
    );
    rpc(&req, "queue_start", json!({}));

    let mut done = false;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(80)).await;
        let s = rpc(&req, "status", json!({}));
        if s["plans_run"].as_u64().unwrap_or(0) >= 1 {
            done = true;
            break;
        }
    }
    assert!(done);

    let h = rpc(&req, "history_get", json!({}));
    let items = h["items"].as_array().unwrap();
    assert!(!items.is_empty(), "history should have at least one item");
    assert_eq!(items[0]["name"], "count");

    rpc(&req, "history_clear", json!({}));
    let h = rpc(&req, "history_get", json!({}));
    assert_eq!(h["items"].as_array().unwrap().len(), 0);

    shutdown.shutdown();
    tokio::time::sleep(Duration::from_millis(300)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lock_blocks_queue_ops_unless_keyed() {
    let port = rand_port();
    let mut reg = Registry::new();
    reg.register_plan_count("count");
    let shutdown = spawn_server(reg, port);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let req = req_socket(port);

    let r = rpc(
        &req,
        "lock",
        json!({"lock_key": "secret", "queue": true, "user": "alice"}),
    );
    assert_eq!(r["success"], true);

    // Without lock_key — must be rejected.
    let r = rpc(&req, "queue_clear", json!({}));
    assert!(!r["success"].as_bool().unwrap_or(true));
    assert!(r["msg"].as_str().unwrap().contains("locked"));

    // With wrong key — also rejected.
    let r = rpc(&req, "queue_clear", json!({"lock_key": "wrong"}));
    assert!(!r["success"].as_bool().unwrap_or(true));
    assert!(r["msg"].as_str().unwrap().contains("locked"));

    // With correct key — allowed.
    let r = rpc(&req, "queue_clear", json!({"lock_key": "secret"}));
    assert_eq!(r["success"], true);

    // Unlock.
    let r = rpc(&req, "unlock", json!({"lock_key": "secret"}));
    assert_eq!(r["success"], true);

    shutdown.shutdown();
    tokio::time::sleep(Duration::from_millis(300)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn re_metadata_round_trip() {
    let port = rand_port();
    let reg = Registry::new();
    let shutdown = spawn_server(reg, port);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let req = req_socket(port);
    rpc(&req, "environment_open", json!({}));
    rpc(
        &req,
        "re_metadata",
        json!({"metadata": {"operator": "alice", "beamline": "BL-7"}}),
    );
    let r = rpc(&req, "re_metadata", json!({}));
    assert_eq!(r["re_metadata"]["operator"], "alice");
    assert_eq!(r["re_metadata"]["beamline"], "BL-7");
    shutdown.shutdown();
    tokio::time::sleep(Duration::from_millis(300)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn not_implemented_methods_return_defined_error() {
    let port = rand_port();
    let reg = Registry::new();
    let shutdown = spawn_server(reg, port);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let req = req_socket(port);
    for m in [
        "permissions_set",
        "script_upload",
        "kernel_interrupt",
        "manager_kill",
    ] {
        let r = rpc(&req, m, json!({}));
        assert!(
            !r["success"].as_bool().unwrap_or(true),
            "method {m} should report failure (not implemented)"
        );
        assert!(
            r["msg"].as_str().unwrap_or("").contains("not implemented"),
            "method {m} msg should mention 'not implemented': {r}"
        );
    }
    shutdown.shutdown();
    tokio::time::sleep(Duration::from_millis(300)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn manager_stop_shuts_down_server() {
    let port = rand_port();
    let reg = Registry::new();
    let _shutdown = spawn_server(reg, port);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let req = req_socket(port);
    let r = rpc(&req, "manager_stop", json!({}));
    assert_eq!(r["success"], true, "manager_stop must succeed: {r}");
    // After manager_stop the server exits its rep loop; further requests time out.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let ctx = zmq::Context::new();
    let probe = ctx.socket(zmq::REQ).unwrap();
    probe.set_rcvtimeo(300).unwrap();
    probe.set_sndtimeo(300).unwrap();
    probe
        .connect(&format!(
            "ipc:///tmp/cirrus-qs-test-{}-{}.sock",
            std::process::id(),
            port
        ))
        .unwrap();
    probe
        .send(
            serde_json::to_vec(&json!({"method": "status", "params": {}})).unwrap(),
            0,
        )
        .unwrap();
    let gone = probe.recv_bytes(0).is_err();
    assert!(gone, "server should not respond after manager_stop");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_includes_bluesky_fields() {
    let port = rand_port();
    let reg = Registry::new();
    let shutdown = spawn_server(reg, port);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let req = req_socket(port);
    let r = rpc(&req, "status", json!({}));
    for k in [
        "manager_state",
        "items_in_queue",
        "items_in_history",
        "plans_run",
        "plans_failed",
        "re_state",
        "worker_environment_exists",
        "queue_stop_pending",
        "queue_autostart_enabled",
        "plan_queue_uid",
        "plan_history_uid",
        "lock_info_uid",
    ] {
        assert!(!r[k].is_null(), "status missing field: {k} (got {r})");
    }
    shutdown.shutdown();
    tokio::time::sleep(Duration::from_millis(300)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn task_status_returns_completed_for_any_uid() {
    let port = rand_port();
    let reg = Registry::new();
    let shutdown = spawn_server(reg, port);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let req = req_socket(port);
    let r = rpc(&req, "task_status", json!({"task_uid": "anything"}));
    assert_eq!(r["status"], "completed");
    let r = rpc(&req, "task_result", json!({"task_uid": "anything"}));
    assert_eq!(r["status"], "completed");
    let r = rpc(&req, "manager_test", json!({}));
    assert_eq!(r["success"], true);
    let r = rpc(&req, "permissions_get", json!({}));
    assert!(r["user_group_permissions"].is_object());
    shutdown.shutdown();
    tokio::time::sleep(Duration::from_millis(300)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_includes_manager_version() {
    let port = rand_port();
    let reg = Registry::new();
    let shutdown = spawn_server(reg, port);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let req = req_socket(port);
    let r = rpc(&req, "status", json!({}));
    let v = &r["manager_version"];
    assert!(v.is_string(), "manager_version should be a string, got {v}");
    shutdown.shutdown();
    tokio::time::sleep(Duration::from_millis(300)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn device_inspect_returns_state_json() {
    let port = rand_port();
    let det = SoftDetector::new("det1");
    let mut reg = Registry::new();
    reg.register_readable("det1", det as Arc<dyn ReadableObj>);
    let shutdown = spawn_server(reg, port);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let req = req_socket(port);

    let r = rpc(&req, "device_inspect", json!({"name": "det1"}));
    assert!(
        r["success"].as_bool().unwrap_or(false),
        "device_inspect should succeed: {r}"
    );
    assert_eq!(r["name"], "det1");
    assert_eq!(r["state"]["type"], "SoftDetector");
    assert_eq!(r["state"]["name"], "det1");
    assert!(r["state"]["counts"].is_number());

    // Unknown device.
    let r = rpc(&req, "device_inspect", json!({"name": "nope"}));
    assert!(
        !r["success"].as_bool().unwrap_or(true),
        "unknown device should fail: {r}"
    );
    assert!(
        r["msg"].as_str().unwrap_or("").contains("no device"),
        "expected 'no device' message, got {r}"
    );

    // Missing name param.
    let r = rpc(&req, "device_inspect", json!({}));
    assert!(
        !r["success"].as_bool().unwrap_or(true),
        "missing name should fail: {r}"
    );

    shutdown.shutdown();
    tokio::time::sleep(Duration::from_millis(300)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rbac_denies_mutation_for_read_only_group() {
    let toml = r#"
        default_group = "viewer"

        [user_groups.viewer]
        read_only = true
        allowed_plans = []
        allowed_devices = []

        [user_groups.admin]
        admin = true
        allowed_plans = [".*"]
        allowed_devices = [".*"]

        [api_keys]
        "admin-key" = "admin"
    "#;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("permissions.toml");
    std::fs::write(&path, toml).unwrap();

    let port = rand_port();
    let reg = Registry::new();
    let shutdown = spawn_server_with_perms(reg, port, path);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let req = req_socket(port);

    // Info RPC always succeeds — even for read_only.
    let r = rpc(&req, "ping", json!({}));
    assert!(r["success"].as_bool().unwrap_or(false));

    // Mutation RPC: anonymous → denied (RBAC).
    let r = rpc(&req, "queue_clear", json!({}));
    assert!(
        !r["success"].as_bool().unwrap_or(true),
        "viewer should be denied: {r}"
    );
    assert!(
        r["msg"].as_str().unwrap_or("").contains("RBAC"),
        "viewer denial should mention RBAC: {r}"
    );

    // Mutation RPC: admin-key → succeeds.
    let r = rpc(&req, "queue_clear", json!({"api_key": "admin-key"}));
    assert!(
        r["success"].as_bool().unwrap_or(false),
        "admin should succeed: {r}"
    );

    // permissions_reload (Admin class): viewer denied, admin OK.
    let r = rpc(&req, "permissions_reload", json!({}));
    assert!(
        !r["success"].as_bool().unwrap_or(true),
        "viewer permissions_reload should be denied: {r}"
    );
    let r = rpc(&req, "permissions_reload", json!({"api_key": "admin-key"}));
    assert!(
        r["success"].as_bool().unwrap_or(false),
        "admin permissions_reload: {r}"
    );

    shutdown.shutdown();
    tokio::time::sleep(Duration::from_millis(300)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rbac_filters_plan_name_on_queue_add() {
    let toml = r#"
        default_group = "primary"
        [user_groups.primary]
        allowed_plans = ["count", "scan_.*"]
        allowed_devices = [".*"]
    "#;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("permissions.toml");
    std::fs::write(&path, toml).unwrap();

    let port = rand_port();
    let reg = Registry::new();
    let shutdown = spawn_server_with_perms(reg, port, path);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let req = req_socket(port);

    // "fly" is not in allowed_plans → denied.
    let r = rpc(
        &req,
        "queue_item_add",
        json!({"item": {"name": "fly", "args": []}}),
    );
    assert!(
        !r["success"].as_bool().unwrap_or(true),
        "fly should be RBAC-denied for primary: {r}"
    );
    assert!(
        r["msg"].as_str().unwrap_or("").contains("RBAC"),
        "fly denial should mention RBAC: {r}"
    );

    // "count" passes RBAC (may then fail because it's not registered
    // in this test's empty Registry, but that's a different failure).
    let r = rpc(
        &req,
        "queue_item_add",
        json!({"item": {"name": "count", "args": []}}),
    );
    // Either it succeeded (registered + RBAC allowed) or it failed for
    // a non-RBAC reason (plan not registered in this Registry).
    let msg = r["msg"].as_str().unwrap_or("");
    assert!(
        r["success"].as_bool().unwrap_or(false) || !msg.contains("RBAC"),
        "count should not be RBAC-denied: {r}"
    );

    shutdown.shutdown();
    tokio::time::sleep(Duration::from_millis(300)).await;
}

// -- QS-10: function_execute -----------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn function_execute_returns_task_uid_and_item() {
    let port = rand_port();
    let reg = Registry::new();
    let shutdown = spawn_server_with_eval(reg, port, Arc::new(MockEval));
    tokio::time::sleep(Duration::from_millis(300)).await;
    let req = req_socket(port);

    // function_execute with item_type="function".
    let r = rpc(
        &req,
        "function_execute",
        json!({
            "item": {"item_type": "function", "name": "my_func", "args": [1, 2], "kwargs": {}},
            "user": "alice",
            "user_group": "primary",
        }),
    );
    assert_eq!(r["success"], true, "function_execute failed: {r}");
    assert!(
        r["task_uid"].is_string(),
        "function_execute must return task_uid: {r}"
    );
    assert_eq!(
        r["item"]["item_type"], "function",
        "item_type must be function: {r}"
    );
    assert_eq!(
        r["item"]["name"], "my_func",
        "item name must round-trip: {r}"
    );
    assert!(
        r["item"]["item_uid"].is_string(),
        "item must have item_uid: {r}"
    );

    let uid = r["task_uid"].as_str().unwrap().to_string();
    let mut completed = false;
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let s = rpc(&req, "task_status", json!({"task_uid": uid}));
        if s["status"] == "completed" || s["status"] == "failed" {
            completed = true;
            break;
        }
    }
    assert!(completed, "function_execute task never completed");

    // Missing item → error.
    let r = rpc(&req, "function_execute", json!({}));
    assert!(
        !r["success"].as_bool().unwrap_or(true),
        "missing item should fail: {r}"
    );

    // Wrong item_type → error.
    let r = rpc(
        &req,
        "function_execute",
        json!({"item": {"item_type": "plan", "name": "count"}}),
    );
    assert!(
        !r["success"].as_bool().unwrap_or(true),
        "wrong item_type should fail: {r}"
    );

    shutdown.shutdown();
    tokio::time::sleep(Duration::from_millis(300)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn function_execute_without_evaluator_returns_error() {
    let port = rand_port();
    let reg = Registry::new();
    let shutdown = spawn_server(reg, port);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let req = req_socket(port);

    let r = rpc(
        &req,
        "function_execute",
        json!({"item": {"item_type": "function", "name": "my_func"}}),
    );
    assert!(!r["success"].as_bool().unwrap_or(true), "{r}");
    assert!(
        r["msg"].as_str().unwrap_or("").contains("no Lua evaluator"),
        "expected 'no Lua evaluator' message: {r}"
    );

    shutdown.shutdown();
    tokio::time::sleep(Duration::from_millis(300)).await;
}

// -- QS-14: user_group-filtered plan listing --------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn plans_allowed_filtered_by_caller_group() {
    let toml = r#"
        default_group = "restricted"

        [user_groups.restricted]
        allowed_plans = ["count"]
        allowed_devices = [".*"]

        [user_groups.admin]
        admin = true
        allowed_plans = [".*"]
        allowed_devices = [".*"]

        [api_keys]
        "admin-key" = "admin"
    "#;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("permissions.toml");
    std::fs::write(&path, toml).unwrap();

    let port = rand_port();
    let mut reg = Registry::new();
    reg.register_plan_count("count");
    reg.register_plan_count("scan"); // add a second plan
    let shutdown = spawn_server_with_perms(reg, port, path);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let req = req_socket(port);

    // Restricted group sees only "count".
    let r = rpc(&req, "plans_allowed", json!({}));
    let plans = r["plans_allowed"].as_object().expect("must be object");
    assert!(
        plans.contains_key("count"),
        "restricted must see count: {r}"
    );
    assert!(
        !plans.contains_key("scan"),
        "restricted must NOT see scan: {r}"
    );

    // Admin group (all plans) sees both.
    let r = rpc(&req, "plans_allowed", json!({"api_key": "admin-key"}));
    let plans = r["plans_allowed"].as_object().expect("must be object");
    assert!(plans.contains_key("count"), "admin must see count: {r}");
    assert!(plans.contains_key("scan"), "admin must see scan: {r}");

    // Absent user_group param is tolerated (caller group from api_key used).
    let r = rpc(&req, "plans_allowed", json!({"user_group": "some_group"}));
    assert_eq!(
        r["success"], true,
        "absent/unknown user_group must not error: {r}"
    );

    shutdown.shutdown();
    tokio::time::sleep(Duration::from_millis(300)).await;
}

// -- QS-03: plans_allowed / devices_allowed rich dict ----------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn plans_allowed_returns_rich_dict() {
    let port = rand_port();
    let det = SoftDetector::new("det1");
    let mut reg = Registry::new();
    reg.register_readable("det1", det as Arc<dyn ReadableObj>);
    reg.register_plan_count("count");
    let shutdown = spawn_server(reg, port);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let req = req_socket(port);

    // plans_allowed: dict keyed by plan name.
    let r = rpc(&req, "plans_allowed", json!({}));
    assert_eq!(r["success"], true, "{r}");
    let plans = r["plans_allowed"]
        .as_object()
        .expect("plans_allowed must be object");
    assert!(
        plans.contains_key("count"),
        "plans_allowed must contain 'count': {r}"
    );
    let entry = &plans["count"];
    assert_eq!(entry["name"], "count");
    assert!(
        entry["description"].is_string(),
        "description must be string: {entry}"
    );
    assert!(
        entry["parameters"].is_array(),
        "parameters must be array: {entry}"
    );
    assert_eq!(entry["module"], "cirrus_qs");

    // devices_allowed: dict keyed by device name.
    let r = rpc(&req, "devices_allowed", json!({}));
    let devs = r["devices_allowed"]
        .as_object()
        .expect("devices_allowed must be object");
    assert!(
        devs.contains_key("det1"),
        "devices_allowed must contain 'det1': {r}"
    );
    assert_eq!(devs["det1"]["name"], "det1");

    // plans_existing and devices_existing must also be dicts.
    let r = rpc(&req, "plans_existing", json!({}));
    assert!(
        r["plans_existing"].as_object().is_some(),
        "plans_existing must be object: {r}"
    );
    let r = rpc(&req, "devices_existing", json!({}));
    assert!(
        r["devices_existing"].as_object().is_some(),
        "devices_existing must be object: {r}"
    );

    shutdown.shutdown();
    tokio::time::sleep(Duration::from_millis(300)).await;
}

// -- QS-07: manager_state transitional values ------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn manager_state_transitional_values_serialise_correctly() {
    use cirrus_qs::EState;
    // Verify wire strings match bluesky MState.value for all EState variants.
    assert_eq!(EState::EnvironmentClosed.as_str(), "environment_closed");
    assert_eq!(EState::Idle.as_str(), "idle");
    assert_eq!(EState::ExecutingQueue.as_str(), "executing_queue");
    assert_eq!(EState::Paused.as_str(), "paused");
    assert_eq!(EState::Aborting.as_str(), "aborting");
    assert_eq!(EState::CreatingEnvironment.as_str(), "creating_environment");
    assert_eq!(EState::ClosingEnvironment.as_str(), "closing_environment");
    assert_eq!(
        EState::DestroyingEnvironment.as_str(),
        "destroying_environment"
    );

    // Verify environment_open leaves the state as "idle" after completion
    // (the creating_environment transition is too fast to observe externally).
    let port = rand_port();
    let reg = Registry::new();
    let shutdown = spawn_server(reg, port);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let req = req_socket(port);

    rpc(&req, "environment_open", json!({}));
    let s = rpc(&req, "status", json!({}));
    assert_eq!(
        s["manager_state"], "idle",
        "state after environment_open should be idle: {s}"
    );

    rpc(&req, "environment_close", json!({}));
    let s = rpc(&req, "status", json!({}));
    assert_eq!(
        s["manager_state"], "environment_closed",
        "state after environment_close should be environment_closed: {s}"
    );

    shutdown.shutdown();
    tokio::time::sleep(Duration::from_millis(300)).await;
}

// -- QS-11: instruction item_type ------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queue_stop_instruction_stops_queue_after_current_plan() {
    let port = rand_port();
    let det = SoftDetector::new("det1");
    let mut reg = Registry::new();
    reg.register_readable("det1", det as Arc<dyn ReadableObj>);
    reg.register_plan_count("count");
    let shutdown = spawn_server(reg, port);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let req = req_socket(port);

    rpc(&req, "environment_open", json!({}));

    // Add: plan A, queue_stop instruction, plan B
    rpc(
        &req,
        "queue_item_add",
        json!({"item": {"name": "count", "args": ["det1", 1]}}),
    );
    let ri = rpc(
        &req,
        "queue_item_add",
        json!({"item": {"item_type": "instruction", "name": "queue_stop"}}),
    );
    assert_eq!(ri["success"], true, "instruction add failed: {ri}");
    assert_eq!(
        ri["item"]["item_type"], "instruction",
        "item_type must be instruction: {ri}"
    );
    rpc(
        &req,
        "queue_item_add",
        json!({"item": {"name": "count", "args": ["det1", 2]}}),
    );
    assert_eq!(rpc(&req, "status", json!({}))["items_in_queue"], 3);

    rpc(&req, "queue_start", json!({}));

    let mut done = false;
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let s = rpc(&req, "status", json!({}));
        if s["manager_state"] == "idle" {
            done = true;
            break;
        }
    }
    assert!(done, "queue did not reach idle");

    // Plan A ran, then instruction stopped the queue; plan B is still pending.
    let s = rpc(&req, "status", json!({}));
    assert_eq!(s["plans_run"], 1, "exactly one plan should have run: {s}");
    assert_eq!(s["items_in_queue"], 1, "plan B should remain in queue: {s}");

    // Instruction should be in history.
    let h = rpc(&req, "history_get", json!({}));
    let hist = h["items"].as_array().unwrap();
    assert!(
        hist.iter()
            .any(|i| i["item_type"] == "instruction" && i["name"] == "queue_stop"),
        "queue_stop instruction should appear in history: {h}"
    );

    // Unknown instruction type → rejected.
    let r = rpc(
        &req,
        "queue_item_add",
        json!({"item": {"item_type": "instruction", "name": "not_a_real_instruction"}}),
    );
    assert!(
        !r["success"].as_bool().unwrap_or(true),
        "unknown instruction should fail: {r}"
    );

    // Unknown item_type → rejected.
    let r = rpc(
        &req,
        "queue_item_add",
        json!({"item": {"item_type": "widget", "name": "count"}}),
    );
    assert!(
        !r["success"].as_bool().unwrap_or(true),
        "unknown item_type should fail: {r}"
    );

    shutdown.shutdown();
    tokio::time::sleep(Duration::from_millis(300)).await;
}

// -- QS-09: positional insertion -------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queue_item_add_positional_insertion() {
    let port = rand_port();
    let det = SoftDetector::new("det1");
    let mut reg = Registry::new();
    reg.register_readable("det1", det as Arc<dyn ReadableObj>);
    reg.register_plan_count("count");
    let shutdown = spawn_server(reg, port);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let req = req_socket(port);

    // Add A, B, C at "back" (default).
    let ra = rpc(
        &req,
        "queue_item_add",
        json!({"item": {"name": "count", "args": ["det1", 1]}, "meta": {"label": "A"}}),
    );
    let rb = rpc(
        &req,
        "queue_item_add",
        json!({"item": {"name": "count", "args": ["det1", 2]}, "meta": {"label": "B"}}),
    );
    let rc = rpc(
        &req,
        "queue_item_add",
        json!({"item": {"name": "count", "args": ["det1", 3]}, "meta": {"label": "C"}}),
    );
    let uid_a = ra["item"]["item_uid"].as_str().unwrap().to_string();
    let uid_b = rb["item"]["item_uid"].as_str().unwrap().to_string();
    let uid_c = rc["item"]["item_uid"].as_str().unwrap().to_string();
    assert_eq!(rc["qsize"], 3);

    // Insert D at front (pos="front") → D,A,B,C
    let rd = rpc(
        &req,
        "queue_item_add",
        json!({"item": {"name": "count", "args": ["det1", 4]}, "pos": "front"}),
    );
    assert_eq!(rd["success"], true);
    let uid_d = rd["item"]["item_uid"].as_str().unwrap().to_string();

    // Insert E before B → D,A,E,B,C
    let re_ = rpc(
        &req,
        "queue_item_add",
        json!({"item": {"name": "count", "args": ["det1", 5]}, "before_uid": uid_b}),
    );
    assert_eq!(re_["success"], true);
    let uid_e = re_["item"]["item_uid"].as_str().unwrap().to_string();

    // Insert F after A → D,A,F,E,B,C
    let rf = rpc(
        &req,
        "queue_item_add",
        json!({"item": {"name": "count", "args": ["det1", 6]}, "after_uid": uid_a}),
    );
    assert_eq!(rf["success"], true);
    let uid_f = rf["item"]["item_uid"].as_str().unwrap().to_string();

    // Verify order: D,A,F,E,B,C
    let q = rpc(&req, "queue_get", json!({}));
    let items = q["items"].as_array().unwrap();
    assert_eq!(items.len(), 6);
    assert_eq!(items[0]["item_uid"], uid_d);
    assert_eq!(items[1]["item_uid"], uid_a);
    assert_eq!(items[2]["item_uid"], uid_f);
    assert_eq!(items[3]["item_uid"], uid_e);
    assert_eq!(items[4]["item_uid"], uid_b);
    assert_eq!(items[5]["item_uid"], uid_c);

    // pos integer: insert G at integer pos=1 → G inserts before index 1 (A) → D,G,A,F,E,B,C
    let rg = rpc(
        &req,
        "queue_item_add",
        json!({"item": {"name": "count", "args": ["det1", 7]}, "pos": 1}),
    );
    assert_eq!(rg["success"], true, "{rg}");
    let uid_g = rg["item"]["item_uid"].as_str().unwrap().to_string();
    let q = rpc(&req, "queue_get", json!({}));
    let items = q["items"].as_array().unwrap();
    assert_eq!(items[0]["item_uid"], uid_d);
    assert_eq!(items[1]["item_uid"], uid_g);

    // Mutual exclusion: pos + before_uid → error
    let r = rpc(
        &req,
        "queue_item_add",
        json!({"item": {"name": "count", "args": ["det1", 1]}, "pos": "front", "before_uid": uid_a}),
    );
    assert!(
        !r["success"].as_bool().unwrap_or(true),
        "pos+before_uid should fail: {r}"
    );

    // before_uid + after_uid → error
    let r = rpc(
        &req,
        "queue_item_add",
        json!({"item": {"name": "count", "args": ["det1", 1]}, "before_uid": uid_a, "after_uid": uid_b}),
    );
    assert!(
        !r["success"].as_bool().unwrap_or(true),
        "before+after should fail: {r}"
    );

    shutdown.shutdown();
    tokio::time::sleep(Duration::from_millis(300)).await;
}

// -- lua_eval async RPC -----------------------------------------------------

/// Mock LuaEvaluator: echoes the source as stdout, parses a leading
/// integer from `source` as a sleep delay (ms) before completing.
struct MockEval;

#[async_trait::async_trait]
impl cirrus_qs::LuaEvaluator for MockEval {
    async fn eval(&self, source: &str) -> cirrus_qs::EvalResult {
        let ms: u64 = source
            .split_whitespace()
            .next()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        if ms > 0 {
            tokio::time::sleep(Duration::from_millis(ms)).await;
        }
        if source.contains("BOOM") {
            return cirrus_qs::EvalResult {
                stdout: String::new(),
                return_value: None,
                error: Some("synthetic".into()),
            };
        }
        cirrus_qs::EvalResult {
            stdout: format!("echo: {source}"),
            return_value: Some("nil".into()),
            error: None,
        }
    }
}

fn spawn_server_with_eval(
    reg: Registry,
    port: u16,
    ev: Arc<dyn cirrus_qs::LuaEvaluator>,
) -> ServerShutdown {
    let ep = endpoint(port);
    let server = Server::builder()
        .control_address(ep)
        .document_address(format!(
            "ipc:///tmp/cirrus-qs-doc-{}-{}.sock",
            std::process::id(),
            port
        ))
        .registry(reg)
        .lua_evaluator(ev)
        .build()
        .expect("server build");
    let shutdown = server.shutdown_handle();
    tokio::spawn(async move {
        let _ = server.run_async().await;
    });
    shutdown
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lua_eval_async_returns_task_uid_and_completes() {
    let port = rand_port();
    let reg = Registry::new();
    let shutdown = spawn_server_with_eval(reg, port, Arc::new(MockEval));
    tokio::time::sleep(Duration::from_millis(300)).await;
    let req = req_socket(port);

    let r = rpc(&req, "lua_eval", json!({"source": "0"}));
    assert!(r["success"].as_bool().unwrap_or(false), "{r}");
    let uid = r["task_uid"].as_str().expect("task_uid").to_string();

    let mut completed = false;
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let s = rpc(&req, "task_status", json!({"task_uid": uid}));
        if s["status"] == "completed" {
            completed = true;
            break;
        }
    }
    assert!(completed, "task_status never reached completed");

    let r = rpc(&req, "task_result", json!({"task_uid": uid}));
    assert_eq!(r["status"], "completed");
    assert_eq!(r["result"]["success"], true);
    assert_eq!(r["result"]["stdout"], "echo: 0");
    assert_eq!(r["result"]["return_value"], "nil");

    shutdown.shutdown();
    tokio::time::sleep(Duration::from_millis(300)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lua_eval_running_state_visible_during_eval() {
    let port = rand_port();
    let reg = Registry::new();
    let shutdown = spawn_server_with_eval(reg, port, Arc::new(MockEval));
    tokio::time::sleep(Duration::from_millis(300)).await;
    let req = req_socket(port);

    let r = rpc(&req, "lua_eval", json!({"source": "500"}));
    let uid = r["task_uid"].as_str().unwrap().to_string();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let s = rpc(&req, "task_status", json!({"task_uid": uid}));
    assert_eq!(s["status"], "running", "{s}");

    let mut completed = false;
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let s = rpc(&req, "task_status", json!({"task_uid": uid}));
        if s["status"] == "completed" {
            completed = true;
            break;
        }
    }
    assert!(completed);

    shutdown.shutdown();
    tokio::time::sleep(Duration::from_millis(300)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lua_eval_propagates_failure() {
    let port = rand_port();
    let reg = Registry::new();
    let shutdown = spawn_server_with_eval(reg, port, Arc::new(MockEval));
    tokio::time::sleep(Duration::from_millis(300)).await;
    let req = req_socket(port);

    let r = rpc(&req, "lua_eval", json!({"source": "0 BOOM"}));
    let uid = r["task_uid"].as_str().unwrap().to_string();
    let mut done = false;
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let s = rpc(&req, "task_status", json!({"task_uid": uid}));
        if s["status"] != "running" {
            assert_eq!(s["status"], "failed", "expected failed: {s}");
            let r2 = rpc(&req, "task_result", json!({"task_uid": uid}));
            assert_eq!(r2["result"]["success"], false);
            assert_eq!(r2["result"]["traceback"], "synthetic");
            done = true;
            break;
        }
    }
    assert!(done);

    shutdown.shutdown();
    tokio::time::sleep(Duration::from_millis(300)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lua_eval_rejects_oversize_source() {
    let port = rand_port();
    let reg = Registry::new();
    let shutdown = spawn_server_with_eval(reg, port, Arc::new(MockEval));
    tokio::time::sleep(Duration::from_millis(300)).await;
    let req = req_socket(port);

    let huge: String = "a".repeat((1 << 20) + 1);
    let r = rpc(&req, "lua_eval", json!({"source": huge}));
    assert!(!r["success"].as_bool().unwrap_or(true), "{r}");
    assert!(
        r["msg"].as_str().unwrap_or("").contains("source too large"),
        "expected 'source too large' message: {r}"
    );

    shutdown.shutdown();
    tokio::time::sleep(Duration::from_millis(300)).await;
}

struct PanickingEval;

#[async_trait::async_trait]
impl cirrus_qs::LuaEvaluator for PanickingEval {
    async fn eval(&self, _source: &str) -> cirrus_qs::EvalResult {
        panic!("synthetic eval panic");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lua_eval_panic_surfaces_as_failed_task() {
    let port = rand_port();
    let reg = Registry::new();
    let shutdown = spawn_server_with_eval(reg, port, Arc::new(PanickingEval));
    tokio::time::sleep(Duration::from_millis(300)).await;
    let req = req_socket(port);

    let r = rpc(&req, "lua_eval", json!({"source": "anything"}));
    let uid = r["task_uid"].as_str().expect("task_uid").to_string();

    let mut got_failed = false;
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let s = rpc(&req, "task_status", json!({"task_uid": uid}));
        if s["status"] == "failed" {
            got_failed = true;
            let r2 = rpc(&req, "task_result", json!({"task_uid": uid}));
            let tb = r2["result"]["traceback"].as_str().unwrap_or("");
            assert!(
                tb.contains("panicked") && tb.contains("synthetic"),
                "traceback should name the panic: {r2}"
            );
            break;
        }
    }
    assert!(
        got_failed,
        "panicking eval task did not surface as failed within 1.5s"
    );

    shutdown.shutdown();
    tokio::time::sleep(Duration::from_millis(300)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lua_eval_without_evaluator_returns_not_implemented() {
    let port = rand_port();
    let reg = Registry::new();
    let shutdown = spawn_server(reg, port);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let req = req_socket(port);

    let r = rpc(&req, "lua_eval", json!({"source": "1+1"}));
    assert!(!r["success"].as_bool().unwrap_or(true), "{r}");
    assert!(
        r["msg"].as_str().unwrap_or("").contains("no Lua evaluator"),
        "expected 'no Lua evaluator' message: {r}"
    );

    shutdown.shutdown();
    tokio::time::sleep(Duration::from_millis(300)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rbac_admin_task_result_blocked_for_non_admin() {
    let toml = r#"
        default_group = "viewer"

        [user_groups.viewer]
        read_only = true
        allowed_plans = []
        allowed_devices = []

        [user_groups.boss]
        admin = true
        allowed_plans = [".*"]
        allowed_devices = [".*"]

        [api_keys]
        "boss-key" = "boss"
    "#;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("permissions.toml");
    std::fs::write(&path, toml).unwrap();

    let port = rand_port();
    let reg = Registry::new();
    let server = Server::builder()
        .control_address(endpoint(port))
        .document_address(format!(
            "ipc:///tmp/cirrus-qs-doc-{}-{}.sock",
            std::process::id(),
            port
        ))
        .registry(reg)
        .permissions_path(path)
        .lua_evaluator(Arc::new(MockEval))
        .build()
        .expect("server build");
    let shutdown = server.shutdown_handle();
    tokio::spawn(async move {
        let _ = server.run_async().await;
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    let req = req_socket(port);

    // Admin issues lua_eval, gets a task_uid.
    let r = rpc(
        &req,
        "lua_eval",
        json!({"source": "0", "api_key": "boss-key"}),
    );
    let uid = r["task_uid"].as_str().expect("task_uid").to_string();

    // Viewer (no api_key) tries to poll status — must be denied.
    let r = rpc(&req, "task_status", json!({"task_uid": uid}));
    assert!(
        !r["success"].as_bool().unwrap_or(true),
        "viewer should be denied: {r}"
    );

    // Viewer tries task_result — also denied.
    let r = rpc(&req, "task_result", json!({"task_uid": uid}));
    assert!(
        !r["success"].as_bool().unwrap_or(true),
        "viewer task_result should be denied: {r}"
    );

    // Admin can poll fine.
    let r = rpc(
        &req,
        "task_status",
        json!({"task_uid": uid, "api_key": "boss-key"}),
    );
    assert!(
        r["status"].as_str().is_some(),
        "admin task_status should succeed: {r}"
    );

    shutdown.shutdown();
    tokio::time::sleep(Duration::from_millis(300)).await;
}

// -- QS-12: msgpack encoding -----------------------------------------------

/// Send a raw msgpack request (not via the JSON `rpc` helper) and return raw response bytes.
fn rpc_msgpack_raw(socket: &zmq::Socket, method: &str, params: serde_json::Value) -> Vec<u8> {
    let msg = serde_json::json!({"method": method, "params": params});
    let bytes = rmp_serde::to_vec_named(&msg).expect("msgpack encode");
    // First byte must NOT be b'{' so the server routes to the msgpack path.
    assert_ne!(
        bytes.first().copied(),
        Some(b'{'),
        "msgpack-encoded message must not start with 0x7B"
    );
    socket.send(bytes, 0).unwrap();
    socket.recv_bytes(0).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn msgpack_request_receives_msgpack_response() {
    let port = rand_port();
    let reg = Registry::new();
    let shutdown = spawn_server(reg, port);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let ctx = zmq::Context::new();
    let sock = ctx.socket(zmq::REQ).unwrap();
    sock.set_rcvtimeo(3_000).unwrap();
    sock.set_sndtimeo(3_000).unwrap();
    sock.connect(&endpoint(port)).unwrap();

    // Send status via msgpack; response must also be msgpack.
    let resp_bytes = rpc_msgpack_raw(&sock, "status", serde_json::json!({}));
    assert_ne!(
        resp_bytes.first().copied(),
        Some(b'{'),
        "response to a msgpack request must be msgpack, not JSON: first byte = {:?}",
        resp_bytes.first()
    );
    let resp_mp: serde_json::Value =
        rmp_serde::from_slice(&resp_bytes).expect("response must be valid msgpack");
    assert_eq!(
        resp_mp["success"], true,
        "msgpack status must succeed: {resp_mp}"
    );
    assert!(
        resp_mp["manager_state"].is_string(),
        "msgpack response must include manager_state: {resp_mp}"
    );

    // JSON request on a separate socket still replies JSON.
    let json_sock = req_socket(port);
    let json_resp = rpc(&json_sock, "status", serde_json::json!({}));
    assert_eq!(
        json_resp["success"], true,
        "JSON path still works: {json_resp}"
    );

    // Both paths must return the same manager_state.
    assert_eq!(
        resp_mp["manager_state"], json_resp["manager_state"],
        "msgpack and JSON paths must return the same manager_state"
    );

    shutdown.shutdown();
    tokio::time::sleep(Duration::from_millis(300)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn msgpack_ping_round_trip_returns_success() {
    let port = rand_port();
    let reg = Registry::new();
    let shutdown = spawn_server(reg, port);
    tokio::time::sleep(Duration::from_millis(300)).await;

    let ctx = zmq::Context::new();
    let sock = ctx.socket(zmq::REQ).unwrap();
    sock.set_rcvtimeo(3_000).unwrap();
    sock.set_sndtimeo(3_000).unwrap();
    sock.connect(&endpoint(port)).unwrap();

    let resp_bytes = rpc_msgpack_raw(&sock, "ping", serde_json::json!({}));
    assert_ne!(
        resp_bytes.first().copied(),
        Some(b'{'),
        "ping reply must be msgpack"
    );
    let v: serde_json::Value = rmp_serde::from_slice(&resp_bytes).expect("msgpack decode");
    assert_eq!(v["success"], true, "msgpack ping: {v}");

    shutdown.shutdown();
    tokio::time::sleep(Duration::from_millis(300)).await;
}

// -- QS-23: ZMQ CURVE encryption -------------------------------------------

/// Build a CURVE-enabled REQ socket with a given server public key (Z85).
/// Client key pair can be any valid pair — use generate_zmq_keys() for tests.
fn curve_req_socket(
    port: u16,
    server_public_z85: &str,
    client_pub_z85: &str,
    client_priv_z85: &str,
) -> zmq::Socket {
    let ctx = zmq::Context::new();
    let sock = ctx.socket(zmq::REQ).unwrap();
    sock.set_rcvtimeo(3_000).unwrap();
    sock.set_sndtimeo(3_000).unwrap();
    sock.set_curve_serverkey(server_public_z85.as_bytes())
        .unwrap();
    sock.set_curve_publickey(client_pub_z85.as_bytes()).unwrap();
    sock.set_curve_secretkey(client_priv_z85.as_bytes())
        .unwrap();
    sock.connect(&endpoint(port)).unwrap();
    sock
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn curve_server_enabled_via_builder_key_round_trip() {
    if !cirrus_qs::curve_supported() {
        eprintln!("CURVE not available in this libzmq build — skipping");
        return;
    }
    let (server_pub, server_priv) =
        cirrus_qs::generate_zmq_keys().expect("generate server keypair");
    let (client_pub, client_priv) =
        cirrus_qs::generate_zmq_keys().expect("generate client keypair");

    let port = rand_port();
    let reg = Registry::new();
    // Build server with CURVE via the explicit builder method.
    let server = Server::builder()
        .control_address(endpoint(port))
        .document_address(format!(
            "ipc:///tmp/cirrus-qs-doc-{}-{}.sock",
            std::process::id(),
            port
        ))
        .registry(reg)
        .curve_private_key(&server_priv)
        .build()
        .expect("CURVE server build");
    let shutdown = server.shutdown_handle();
    tokio::spawn(async move {
        let _ = server.run_async().await;
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Plain-text connection must NOT work (CURVE server drops unauthenticated frames).
    // We only assert the CURVE path works; testing plaintext rejection is slow (timeout).

    // CURVE-authenticated client round-trip.
    let sock = curve_req_socket(port, &server_pub, &client_pub, &client_priv);
    let r = rpc(&sock, "ping", json!({}));
    assert_eq!(r["success"], true, "CURVE ping failed: {r}");
    assert!(
        r["manager_state"].is_string(),
        "CURVE ping must return manager_state: {r}"
    );

    shutdown.shutdown();
    tokio::time::sleep(Duration::from_millis(300)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn curve_server_enabled_via_env_var() {
    if !cirrus_qs::curve_supported() {
        eprintln!("CURVE not available in this libzmq build — skipping");
        return;
    }
    let (server_pub, server_priv) =
        cirrus_qs::generate_zmq_keys().expect("generate server keypair");
    let (client_pub, client_priv) =
        cirrus_qs::generate_zmq_keys().expect("generate client keypair");

    let port = rand_port();
    let reg = Registry::new();
    // Set env var before building — build() reads it exactly once.
    std::env::set_var("QSERVER_ZMQ_PRIVATE_KEY", &server_priv);
    let build_result = Server::builder()
        .control_address(endpoint(port))
        .document_address(format!(
            "ipc:///tmp/cirrus-qs-doc-{}-{}.sock",
            std::process::id(),
            port
        ))
        .registry(reg)
        .build();
    // Unset immediately so other tests are unaffected.
    std::env::remove_var("QSERVER_ZMQ_PRIVATE_KEY");

    let server = build_result.expect("CURVE server via env var");
    let shutdown = server.shutdown_handle();
    tokio::spawn(async move {
        let _ = server.run_async().await;
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    let sock = curve_req_socket(port, &server_pub, &client_pub, &client_priv);
    let r = rpc(&sock, "ping", json!({}));
    assert_eq!(r["success"], true, "CURVE-via-env-var ping: {r}");

    shutdown.shutdown();
    tokio::time::sleep(Duration::from_millis(300)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_curve_when_env_var_unset_plaintext_works() {
    // Ensure env var is not set for this test.
    std::env::remove_var("QSERVER_ZMQ_PRIVATE_KEY");

    let port = rand_port();
    let reg = Registry::new();
    let shutdown = spawn_server(reg, port);
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Plain-text connection must succeed when CURVE is not configured.
    let sock = req_socket(port);
    let r = rpc(&sock, "ping", json!({}));
    assert_eq!(
        r["success"], true,
        "plain-text ping must succeed without CURVE: {r}"
    );

    shutdown.shutdown();
    tokio::time::sleep(Duration::from_millis(300)).await;
}
