//! `cirrus qs <subcommand>` — REQ client for a running cirrus-qs server.

use std::time::Duration;

use clap::{Args, Subcommand};
use serde_json::{json, Value};

/// Top-level args for `cirrus qs`.
#[derive(Args, Debug)]
pub struct ClientArgs {
    /// Control REP socket address of the running cirrus-qs server.
    #[arg(long, default_value = "tcp://localhost:60615", global = true)]
    address: String,

    /// REQ recv timeout in milliseconds.
    #[arg(long, default_value_t = 5_000, global = true)]
    timeout_ms: i32,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Health-check ping.
    Ping,
    /// Server status: state, queue length, plans run / failed.
    Status,
    /// Open or close the engine environment.
    #[command(subcommand)]
    Environment(EnvCmd),
    /// Queue operations (add / get / remove / start).
    #[command(subcommand)]
    Queue(QueueCmd),
    /// RunEngine control (pause / resume / abort / halt).
    #[command(subcommand)]
    Re(ReCmd),
    /// List allowed plans / devices.
    #[command(subcommand)]
    Allowed(AllowedCmd),
}

#[derive(Subcommand, Debug)]
enum EnvCmd {
    /// `environment_open` — instantiate a fresh `RunEngine`.
    Open,
    /// `environment_close` — drop the engine.
    Close,
}

#[derive(Subcommand, Debug)]
enum QueueCmd {
    /// Add a plan to the queue. ARGS are passed positionally to the
    /// plan factory. For example: `queue add count det1 5`.
    Add {
        /// Plan name (must be registered server-side, e.g. `count`).
        plan: String,
        /// Positional args. Strings stay strings; numeric strings are
        /// parsed as numbers.
        #[arg(num_args = 0..)]
        args: Vec<String>,
    },
    /// `queue_get` — list queued items.
    Get,
    /// Remove an item by `item_uid`.
    Remove {
        /// `item_uid` to remove.
        uid: String,
    },
    /// `queue_start` — begin executing the queue.
    Start,
}

#[derive(Subcommand, Debug)]
enum ReCmd {
    /// `re_pause [--deferred]`.
    Pause {
        /// Pause at the next checkpoint (deferred). Default = immediate.
        #[arg(long)]
        deferred: bool,
    },
    /// `re_resume`.
    Resume,
    /// `re_abort`.
    Abort,
    /// `re_halt`.
    Halt,
}

#[derive(Subcommand, Debug)]
enum AllowedCmd {
    /// `plans_allowed`.
    Plans,
    /// `devices_allowed`.
    Devices,
}

/// Entry point — returns process exit code.
pub async fn run(args: ClientArgs) -> i32 {
    let result = tokio::task::spawn_blocking(move || dispatch(args))
        .await
        .unwrap_or_else(|_| Err("client task panicked".into()));
    match result {
        Ok(value) => {
            if let Ok(s) = serde_json::to_string_pretty(&value) {
                println!("{s}");
            } else {
                println!("{value}");
            }
            0
        }
        Err(e) => {
            eprintln!("cirrus qs: {e}");
            1
        }
    }
}

fn dispatch(args: ClientArgs) -> Result<Value, String> {
    let (method, params) = match args.cmd {
        Cmd::Ping => ("ping", json!({})),
        Cmd::Status => ("status", json!({})),
        Cmd::Environment(EnvCmd::Open) => ("environment_open", json!({})),
        Cmd::Environment(EnvCmd::Close) => ("environment_close", json!({})),
        Cmd::Queue(QueueCmd::Add { plan, args }) => (
            "queue_item_add",
            json!({
                "item": {
                    "name": plan,
                    "args": parse_positional_args(&args),
                }
            }),
        ),
        Cmd::Queue(QueueCmd::Get) => ("queue_get", json!({})),
        Cmd::Queue(QueueCmd::Remove { uid }) => ("queue_item_remove", json!({"uid": uid})),
        Cmd::Queue(QueueCmd::Start) => ("queue_start", json!({})),
        Cmd::Re(ReCmd::Pause { deferred }) => (
            "re_pause",
            json!({"option": if deferred { "deferred" } else { "immediate" }}),
        ),
        Cmd::Re(ReCmd::Resume) => ("re_resume", json!({})),
        Cmd::Re(ReCmd::Abort) => ("re_abort", json!({})),
        Cmd::Re(ReCmd::Halt) => ("re_halt", json!({})),
        Cmd::Allowed(AllowedCmd::Plans) => ("plans_allowed", json!({})),
        Cmd::Allowed(AllowedCmd::Devices) => ("devices_allowed", json!({})),
    };
    let req = json!({
        "jsonrpc": "2.0",
        "method": method,
        "params": params,
        "id": 1,
    });
    let bytes = serde_json::to_vec(&req).map_err(|e| format!("encode request: {e}"))?;

    let ctx = zmq::Context::new();
    let sock = ctx
        .socket(zmq::REQ)
        .map_err(|e| format!("zmq REQ socket: {e}"))?;
    sock.set_rcvtimeo(args.timeout_ms)
        .map_err(|e| format!("set_rcvtimeo: {e}"))?;
    sock.set_sndtimeo(args.timeout_ms)
        .map_err(|e| format!("set_sndtimeo: {e}"))?;
    sock.set_linger(0)
        .map_err(|e| format!("set_linger: {e}"))?;
    sock.connect(&args.address)
        .map_err(|e| format!("connect {}: {e}", args.address))?;
    sock.send(bytes, 0)
        .map_err(|e| format!("send: {e} (server not running?)"))?;
    let resp = sock
        .recv_bytes(0)
        .map_err(|e| format!("recv: {e} (server not responding within {} ms — start `cirrus qs-manager`?)", args.timeout_ms))?;
    let _ = Duration::from_millis(args.timeout_ms.unsigned_abs() as u64);
    let value: Value = serde_json::from_slice(&resp)
        .map_err(|e| format!("decode response: {e}; raw = {:?}", String::from_utf8_lossy(&resp)))?;

    if let Some(err) = value.get("error") {
        let msg = err
            .get("message")
            .and_then(|m| m.as_str())
            .unwrap_or("unknown error");
        let code = err.get("code").and_then(|c| c.as_i64()).unwrap_or(0);
        return Err(format!("server error (code={code}): {msg}"));
    }
    if let Some(result) = value.get("result").cloned() {
        return Ok(result);
    }
    Ok(value)
}

/// Convert positional `args: Vec<String>` to a JSON array, parsing
/// numeric strings as numbers and `true`/`false`/`null` as those typed
/// values. Anything else stays a string.
fn parse_positional_args(args: &[String]) -> Value {
    let mut out = Vec::with_capacity(args.len());
    for a in args {
        out.push(parse_one(a));
    }
    Value::Array(out)
}

fn parse_one(s: &str) -> Value {
    if s == "true" {
        Value::Bool(true)
    } else if s == "false" {
        Value::Bool(false)
    } else if s == "null" {
        Value::Null
    } else if let Ok(i) = s.parse::<i64>() {
        Value::from(i)
    } else if let Ok(f) = s.parse::<f64>() {
        Value::from(f)
    } else {
        Value::String(s.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn positional_args_mix_strings_ints_floats_bools() {
        let v = parse_positional_args(
            &["det1", "5", "2.5", "true", "false", "null", "hello world"]
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>(),
        );
        let arr = v.as_array().unwrap();
        assert_eq!(arr[0], json!("det1"));
        assert_eq!(arr[1], json!(5));
        assert_eq!(arr[2], json!(2.5));
        assert_eq!(arr[3], json!(true));
        assert_eq!(arr[4], json!(false));
        assert_eq!(arr[5], Value::Null);
        assert_eq!(arr[6], json!("hello world"));
    }

    #[test]
    fn negative_and_scientific_floats_parse() {
        assert_eq!(parse_one("-5"), json!(-5));
        assert_eq!(parse_one("-2.5"), json!(-2.5));
        assert_eq!(parse_one("1e3"), json!(1000.0));
    }

    #[test]
    fn pv_strings_remain_strings() {
        // "BL10:m1.RBV" must NOT be parsed as a number despite leading digit-like content.
        assert_eq!(parse_one("BL10:m1.RBV"), json!("BL10:m1.RBV"));
    }
}
