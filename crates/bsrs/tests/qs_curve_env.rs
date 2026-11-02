//! `Server::build` takes its CURVE key from `QSERVER_ZMQ_PRIVATE_KEY`. The
//! variable is process-wide, so this check has a binary of its own: set
//! in `qs_round_trip`, it turned on CURVE for the servers other tests built
//! meanwhile, and their plain-text requests timed out.

#![cfg(feature = "qs")]

use std::time::Duration;

use bsrs::qs::{generate_zmq_keys, Registry, Server, ServerShutdown};
use omq::blocking::Socket;
use omq::{CurveKeypair, CurvePublicKey, CurveSecretKey, Message, Options, SocketType};
use serde_json::{json, Value};

const KEY_VAR: &str = "QSERVER_ZMQ_PRIVATE_KEY";
const RPC_TIMEOUT: Duration = Duration::from_secs(3);

fn ping(socket: &Socket) -> Value {
    let req = json!({"method": "ping", "params": {}});
    socket
        .send_timeout(
            Message::single(serde_json::to_vec(&req).unwrap()),
            RPC_TIMEOUT,
        )
        .unwrap();
    let resp = socket.recv_timeout(RPC_TIMEOUT).unwrap();
    serde_json::from_slice(resp.part_slice(0).unwrap()).unwrap()
}

async fn serve() -> ServerShutdown {
    let server = Server::builder()
        .control_address("tcp://127.0.0.1:*")
        .document_address("tcp://127.0.0.1:*")
        .registry(Registry::new())
        .build()
        .expect("server build");
    let shutdown = server.shutdown_handle();
    tokio::spawn(async move {
        let _ = server.run_async().await;
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    shutdown
}

fn client(control: &str, options: Options) -> Socket {
    let socket = omq::Context::new().blocking_socket(SocketType::Req, options);
    socket.connect(control).unwrap();
    socket
}

/// Both cases in one test, so no other server is built while the variable
/// is set.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn curve_key_comes_from_env_var() {
    std::env::remove_var(KEY_VAR);
    let shutdown = serve().await;
    let r = ping(&client(shutdown.control_endpoint(), Options::default()));
    assert_eq!(r["success"], true, "plain-text ping without {KEY_VAR}: {r}");
    shutdown.shutdown();

    let (server_pub, server_priv) = generate_zmq_keys().expect("server keypair");
    let (client_pub, client_priv) = generate_zmq_keys().expect("client keypair");
    std::env::set_var(KEY_VAR, &server_priv);
    let shutdown = serve().await;
    let keys = CurveKeypair {
        public: CurvePublicKey::from_z85(&client_pub).unwrap(),
        secret: CurveSecretKey::from_z85(&client_priv).unwrap(),
    };
    let server = CurvePublicKey::from_z85(&server_pub).unwrap();
    let options = Options::default().curve_client(keys, server);
    let r = ping(&client(shutdown.control_endpoint(), options));
    assert_eq!(r["success"], true, "CURVE ping with {KEY_VAR}: {r}");
    shutdown.shutdown();
    tokio::time::sleep(Duration::from_millis(300)).await;
}
