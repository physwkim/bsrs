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
    /// `config_get` — implementation + version metadata.
    Config,
    /// Open or close the engine environment.
    #[command(subcommand)]
    Environment(EnvCmd),
    /// Queue operations (add / get / remove / start / stop / mode).
    #[command(subcommand)]
    Queue(QueueCmd),
    /// RunEngine control (pause / resume / abort / halt / stop /
    /// metadata / runs).
    #[command(subcommand)]
    Re(ReCmd),
    /// List allowed / existing plans / devices.
    #[command(subcommand)]
    Allowed(AllowedCmd),
    /// Plan history (`history_get` / `history_clear`).
    #[command(subcommand)]
    History(HistoryCmd),
    /// Lock manager (`lock` / `lock_info` / `unlock`).
    #[command(subcommand)]
    Lock(LockCmd),
    /// Send a raw JSON-RPC method by name. Fallback for any method not
    /// exposed by a dedicated subcommand.
    Raw {
        /// Method name to call.
        method: String,
        /// Optional JSON params object (default `{}`).
        #[arg(default_value = "{}")]
        params: String,
    },
}

#[derive(Subcommand, Debug)]
enum EnvCmd {
    /// `environment_open` — instantiate a fresh `RunEngine`.
    Open,
    /// `environment_close` — drop the engine.
    Close,
    /// `environment_destroy` — force-drop without checks (cirrus aliases
    /// to `environment_close`).
    Destroy,
    /// `environment_update` — refresh registry (no-op in cirrus).
    Update,
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
    /// `queue_clear` — drop all queued items.
    Clear,
    /// `queue_item_get` — fetch one queued item by uid.
    Item {
        /// `item_uid` to fetch.
        uid: String,
    },
    /// `queue_item_move` — reorder by uid.
    Move {
        /// `item_uid` to move.
        uid: String,
        /// Destination position (`front`, `back`, or 0-based index).
        pos_dest: String,
    },
    /// `queue_item_execute` — run a one-off plan without queueing.
    Execute {
        /// Plan name.
        plan: String,
        /// Positional args.
        #[arg(num_args = 0..)]
        args: Vec<String>,
    },
    /// `queue_start` — begin executing the queue.
    Start,
    /// `queue_stop` — halt the queue worker after the current item.
    Stop,
    /// `queue_stop_cancel` — cancel a pending stop.
    StopCancel,
    /// `queue_autostart` — toggle the autostart flag.
    Autostart {
        /// `enable` or `disable`.
        option: String,
    },
    /// `queue_mode_set` — set queue mode flags. The arg is a JSON object,
    /// e.g. `'{"loop": true}'`.
    Mode {
        /// JSON object describing the mode.
        mode: String,
    },
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
    /// `re_stop` — graceful stop, closes run with `success` status.
    Stop,
    /// `re_runs` — list recent run UIDs.
    Runs,
    /// `re_metadata` — get / set `RE.md`.
    Metadata {
        /// Optional JSON object to merge into `RE.md`. If absent, returns
        /// the current metadata.
        #[arg(long)]
        set: Option<String>,
    },
}

#[derive(Subcommand, Debug)]
enum AllowedCmd {
    /// `plans_allowed`.
    Plans,
    /// `plans_existing` — superset of plans_allowed (cirrus has no
    /// permissions filter, so they match).
    PlansExisting,
    /// `devices_allowed`.
    Devices,
    /// `devices_existing`.
    DevicesExisting,
}

#[derive(Subcommand, Debug)]
enum HistoryCmd {
    /// `history_get`.
    Get,
    /// `history_clear`.
    Clear,
}

#[derive(Subcommand, Debug)]
enum LockCmd {
    /// `lock` — install a lock with the given key. At least one of
    /// `--queue` or `--environment` must be set.
    Apply {
        /// Lock key string. Required to unlock.
        key: String,
        /// Lock the queue subsystem.
        #[arg(long)]
        queue: bool,
        /// Lock the environment subsystem.
        #[arg(long)]
        environment: bool,
        /// Free-form note.
        #[arg(long)]
        note: Option<String>,
        /// User name.
        #[arg(long)]
        user: Option<String>,
    },
    /// `lock_info` — current lock state.
    Info,
    /// `unlock` — release the lock (key must match).
    Release {
        /// Lock key to verify.
        key: String,
    },
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
    let (method, params): (String, Value) = match args.cmd {
        Cmd::Ping => ("ping".into(), json!({})),
        Cmd::Status => ("status".into(), json!({})),
        Cmd::Config => ("config_get".into(), json!({})),
        Cmd::Environment(EnvCmd::Open) => ("environment_open".into(), json!({})),
        Cmd::Environment(EnvCmd::Close) => ("environment_close".into(), json!({})),
        Cmd::Environment(EnvCmd::Destroy) => ("environment_destroy".into(), json!({})),
        Cmd::Environment(EnvCmd::Update) => ("environment_update".into(), json!({})),
        Cmd::Queue(QueueCmd::Add { plan, args }) => (
            "queue_item_add".into(),
            json!({
                "item": {
                    "name": plan,
                    "args": parse_positional_args(&args),
                }
            }),
        ),
        Cmd::Queue(QueueCmd::Get) => ("queue_get".into(), json!({})),
        Cmd::Queue(QueueCmd::Remove { uid }) => ("queue_item_remove".into(), json!({"uid": uid})),
        Cmd::Queue(QueueCmd::Clear) => ("queue_clear".into(), json!({})),
        Cmd::Queue(QueueCmd::Item { uid }) => ("queue_item_get".into(), json!({"uid": uid})),
        Cmd::Queue(QueueCmd::Move { uid, pos_dest }) => {
            let pd = if let Ok(n) = pos_dest.parse::<u64>() {
                Value::from(n)
            } else {
                Value::String(pos_dest)
            };
            (
                "queue_item_move".into(),
                json!({"uid": uid, "pos_dest": pd}),
            )
        }
        Cmd::Queue(QueueCmd::Execute { plan, args }) => (
            "queue_item_execute".into(),
            json!({
                "item": {
                    "name": plan,
                    "args": parse_positional_args(&args),
                }
            }),
        ),
        Cmd::Queue(QueueCmd::Start) => ("queue_start".into(), json!({})),
        Cmd::Queue(QueueCmd::Stop) => ("queue_stop".into(), json!({})),
        Cmd::Queue(QueueCmd::StopCancel) => ("queue_stop_cancel".into(), json!({})),
        Cmd::Queue(QueueCmd::Autostart { option }) => (
            "queue_autostart".into(),
            json!({"enable": option == "enable"}),
        ),
        Cmd::Queue(QueueCmd::Mode { mode }) => {
            let parsed: Value =
                serde_json::from_str(&mode).map_err(|e| format!("invalid mode JSON: {e}"))?;
            ("queue_mode_set".into(), json!({"mode": parsed}))
        }
        Cmd::Re(ReCmd::Pause { deferred }) => (
            "re_pause".into(),
            json!({"option": if deferred { "deferred" } else { "immediate" }}),
        ),
        Cmd::Re(ReCmd::Resume) => ("re_resume".into(), json!({})),
        Cmd::Re(ReCmd::Abort) => ("re_abort".into(), json!({})),
        Cmd::Re(ReCmd::Halt) => ("re_halt".into(), json!({})),
        Cmd::Re(ReCmd::Stop) => ("re_stop".into(), json!({})),
        Cmd::Re(ReCmd::Runs) => ("re_runs".into(), json!({})),
        Cmd::Re(ReCmd::Metadata { set }) => match set {
            Some(s) => {
                let parsed: Value =
                    serde_json::from_str(&s).map_err(|e| format!("invalid metadata JSON: {e}"))?;
                ("re_metadata".into(), json!({"metadata": parsed}))
            }
            None => ("re_metadata".into(), json!({})),
        },
        Cmd::Allowed(AllowedCmd::Plans) => ("plans_allowed".into(), json!({})),
        Cmd::Allowed(AllowedCmd::PlansExisting) => ("plans_existing".into(), json!({})),
        Cmd::Allowed(AllowedCmd::Devices) => ("devices_allowed".into(), json!({})),
        Cmd::Allowed(AllowedCmd::DevicesExisting) => ("devices_existing".into(), json!({})),
        Cmd::History(HistoryCmd::Get) => ("history_get".into(), json!({})),
        Cmd::History(HistoryCmd::Clear) => ("history_clear".into(), json!({})),
        Cmd::Lock(LockCmd::Apply {
            key,
            queue,
            environment,
            note,
            user,
        }) => (
            "lock".into(),
            json!({
                "lock_key": key,
                "queue": queue,
                "environment": environment,
                "note": note,
                "user": user,
            }),
        ),
        Cmd::Lock(LockCmd::Info) => ("lock_info".into(), json!({})),
        Cmd::Lock(LockCmd::Release { key }) => ("unlock".into(), json!({"lock_key": key})),
        Cmd::Raw { method, params } => {
            let parsed: Value =
                serde_json::from_str(&params).map_err(|e| format!("invalid params JSON: {e}"))?;
            (method, parsed)
        }
    };
    let req = json!({
        "jsonrpc": "2.0",
        "method": &method,
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
    sock.set_linger(0).map_err(|e| format!("set_linger: {e}"))?;
    sock.connect(&args.address)
        .map_err(|e| format!("connect {}: {e}", args.address))?;
    sock.send(bytes, 0)
        .map_err(|e| format!("send: {e} (server not running?)"))?;
    let resp = sock.recv_bytes(0).map_err(|e| {
        format!(
            "recv: {e} (server not responding within {} ms — start `cirrus qs-manager`?)",
            args.timeout_ms
        )
    })?;
    let _ = Duration::from_millis(args.timeout_ms.unsigned_abs() as u64);
    let value: Value = serde_json::from_slice(&resp).map_err(|e| {
        format!(
            "decode response: {e}; raw = {:?}",
            String::from_utf8_lossy(&resp)
        )
    })?;

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
