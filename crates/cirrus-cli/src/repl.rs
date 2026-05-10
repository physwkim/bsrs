//! `cirrus repl` — interactive Lua REPL for cirrus.
//!
//! Drives an in-process `RunEngine`, with cirrus types/factories
//! pre-registered as Lua globals. Goal: IPython-equivalent dev/test
//! surface without a Python install.
//!
//! Built-ins available at the prompt:
//!
//! ```lua
//! det1 = soft_detector("det1")
//! m1   = soft_motor("m1", 0.0)
//!
//! RE:run(count({det1}, 5))
//! RE:run(scan({det1}, m1, 0, 10, 11))
//! RE:run(mvr(m1, 1.0))
//!
//! RE:md_set("operator", "alice")
//! print(RE:md_get())
//! print(RE:state())
//! ```
//!
//! Slash-style helpers: type `:help`, `:quit`, `:reset`, `:script <path>`.

use std::path::PathBuf;
use std::sync::Arc;

use cirrus_engine::RunEngine;
use clap::Args;
use rustyline::completion::{Completer, Pair};
use rustyline::error::ReadlineError;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::history::FileHistory;
use rustyline::validate::Validator;
use rustyline::{Context, Editor, Helper};

use crate::lua_env::build_lua;

/// Static completion of cirrus's well-known Lua globals + namespaces.
///
/// rustyline lets us register a `Helper` that provides Tab completions.
/// We don't introspect the Lua state at completion time (that would
/// require sharing a `!Send` Lua handle into the editor); instead we
/// expose the curated list of globals we register at REPL startup.
struct CirrusReplHelper {
    keywords: Vec<&'static str>,
}

impl Default for CirrusReplHelper {
    fn default() -> Self {
        let keywords = vec![
            // Engine handle.
            "RE",
            "RE:run",
            "RE:run_async_with",
            "RE:pause",
            "RE:resume",
            "RE:abort",
            "RE:halt",
            "RE:stop",
            "RE:state",
            "RE:md_get",
            "RE:md_set",
            "RE:md_remove",
            "RE:md_replace",
            "RE:is_paused",
            "RE:current_run_uid",
            "RE:set_loop_timeout",
            "RE:set_record_interruptions",
            "RE:record_interruptions_enabled",
            "RE:set_input_handler",
            "RE:set_md_validator",
            "RE:set_md_normalizer",
            "RE:set_scan_id_source",
            "RE:set_before_plan",
            "RE:set_after_plan",
            "RE:register_command",
            "RE:unregister_command",
            "RE:subscribe",
            "RE:unsubscribe",
            "RE:register_pausable",
            "RE:unregister_pausable",
            "RE:suspend_until_seconds",
            "RE:install_signal_handler",
            "RE:next_suspender_id",
            "RE:request_pause",
            "RE:request_suspend",
            "RE:take_msg_result",
            "RE:clear_preprocessors",
            // Device factories.
            "soft_detector",
            "soft_motor",
            "soft_pausable",
            // Plan factories.
            "count",
            "scan",
            "mvr",
            "sleep",
            "null",
            "plan",
            "print",
            // Bluesky-style namespaces (top-level globals).
            "msg",
            "bp",
            "bps",
            "bpt",
            "bpp",
            "tiled",
            // Common msg.* tokens.
            "msg.open_run",
            "msg.close_run",
            "msg.create",
            "msg.save",
            "msg.drop",
            "msg.read",
            "msg.set",
            "msg.trigger",
            "msg.wait",
            "msg.sleep",
            "msg.checkpoint",
            "msg.clear_checkpoint",
            "msg.rewindable",
            "msg.pause",
            "msg.resume",
            "msg.null",
            "msg.stage",
            "msg.unstage",
            "msg.stop_dev",
            "msg.monitor",
            "msg.unmonitor",
            "msg.locate",
            "msg.kickoff",
            "msg.complete",
            "msg.prepare",
            "msg.wait_for",
            "msg.input",
            "msg.re_class",
            "msg.configure",
            "msg.declare_stream",
            "msg.collect",
            "msg.publish",
            "msg.subscribe",
            "msg.unsubscribe",
            "msg.register_pausable",
            "msg.unregister_pausable",
            "msg.remove_suspender",
            // Lua keywords commonly typed.
            "function",
            "local",
            "return",
            "coroutine.yield",
            "coroutine.create",
            "if",
            "then",
            "else",
            "elseif",
            "end",
            "for",
            "while",
            "do",
            "repeat",
            "until",
            // Slash commands.
            ":help",
            ":quit",
            ":exit",
            ":reset",
            ":script",
        ];
        Self { keywords }
    }
}

impl Completer for CirrusReplHelper {
    type Candidate = Pair;
    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        // Find the start of the word being completed (last whitespace,
        // `(`, `,`, `=`, `{`, `[`, or BOL).
        let prefix_end = pos.min(line.len());
        let bytes = &line.as_bytes()[..prefix_end];
        let mut start = prefix_end;
        for (i, &b) in bytes.iter().enumerate().rev() {
            if matches!(b, b' ' | b'\t' | b'(' | b',' | b'=' | b'{' | b'[' | b'\n') {
                start = i + 1;
                break;
            }
            start = i;
        }
        let word = &line[start..prefix_end];
        if word.is_empty() {
            return Ok((start, Vec::new()));
        }
        let candidates: Vec<Pair> = self
            .keywords
            .iter()
            .filter(|k| k.starts_with(word))
            .map(|k| Pair {
                display: (*k).to_string(),
                replacement: (*k).to_string(),
            })
            .collect();
        Ok((start, candidates))
    }
}

impl Hinter for CirrusReplHelper {
    type Hint = String;
}
impl Highlighter for CirrusReplHelper {}
impl Validator for CirrusReplHelper {}
impl Helper for CirrusReplHelper {}

/// Arguments for `cirrus repl`.
#[derive(Args, Debug)]
pub struct ReplArgs {
    /// Optional file with Lua statements to execute before the prompt
    /// opens. Useful as a `~/.cirrusrc.lua` style init.
    #[arg(long)]
    pub init: Option<PathBuf>,

    /// Optional script file to run non-interactively. The REPL exits
    /// after the script finishes.
    #[arg(long, value_name = "FILE")]
    pub script: Option<PathBuf>,

    /// Optional ZMQ PUB endpoint. When set, every Document the
    /// engine emits is published to this address using bluesky's
    /// `Publisher` envelope (msgpack body). Connect a Python
    /// `bluesky.callbacks.zmq.RemoteDispatcher` on the receiving
    /// side to consume them. Example: `--doc-zmq tcp://*:5577`.
    #[arg(long, value_name = "ADDR")]
    pub doc_zmq: Option<String>,

    /// Optional Tiled HTTP endpoint. When set, every Document the
    /// engine emits is registered into the named container on the
    /// Tiled catalog. Example: `--doc-tiled http://localhost:8000`.
    /// Requires the `tiled` Cargo feature.
    #[cfg(feature = "tiled")]
    #[arg(long, value_name = "URL")]
    pub doc_tiled: Option<String>,

    /// Container name under the Tiled catalog (default `cirrus`).
    /// Used only when `--doc-tiled` is set.
    #[cfg(feature = "tiled")]
    #[arg(long, value_name = "NAME", default_value = "cirrus")]
    pub doc_tiled_container: String,

    /// Single-user API key for the Tiled server. Reads
    /// `TILED_SINGLE_USER_API_KEY` env var if not given.
    #[cfg(feature = "tiled")]
    #[arg(long, value_name = "KEY")]
    pub doc_tiled_key: Option<String>,
}

/// Entry point — returns process exit code.
pub fn run(args: ReplArgs) -> i32 {
    // Bootstrap the CA backend's global client BEFORE building the
    // Lua state. The CA backend's `ca_context()` block_on's
    // `CaClient::new()` once, which panics if called from inside an
    // active tokio runtime. Calling it here from the sync `repl::run`
    // entry pre-warms the cache so subsequent `ca_motor` /
    // `ca_detector` Lua factories don't trip the runtime check.
    #[cfg(feature = "ca")]
    crate::ca_devices::bootstrap_ca();

    // Optional ZMQ document fan-out — bluesky `Publisher` envelope.
    // Bound on a separate PUB socket; downstream Python consumers
    // attach a `bluesky.callbacks.zmq.RemoteDispatcher`.
    let mut sinks: Vec<Arc<dyn cirrus_engine::DocumentSink>> = Vec::new();
    if let Some(addr) = &args.doc_zmq {
        match cirrus_callbacks::ZmqDocumentSink::bind(addr) {
            Ok(s) => {
                eprintln!("cirrus repl: publishing Documents on ZMQ {addr}");
                sinks.push(Arc::new(s) as Arc<dyn cirrus_engine::DocumentSink>);
            }
            Err(e) => {
                eprintln!("cirrus repl: failed to bind ZMQ {addr}: {e}");
                return 2;
            }
        }
    }

    #[cfg(feature = "tiled")]
    if let Some(url) = &args.doc_tiled {
        let mut sink = match cirrus_callbacks::TiledSink::new(url, &args.doc_tiled_container) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("cirrus repl: failed to build TiledSink for {url}: {e}");
                return 2;
            }
        };
        let key = args
            .doc_tiled_key
            .clone()
            .or_else(|| std::env::var("TILED_SINGLE_USER_API_KEY").ok());
        if let Some(k) = key {
            sink = sink.with_api_key(k);
        }
        eprintln!(
            "cirrus repl: registering Documents into Tiled at {url} container={:?}",
            args.doc_tiled_container
        );
        sinks.push(Arc::new(sink) as Arc<dyn cirrus_engine::DocumentSink>);
    }

    let re = Arc::new(RunEngine::new(sinks));
    let lua = match build_lua(re) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("cirrus repl: failed to initialize Lua: {e}");
            return 2;
        }
    };

    if let Some(path) = &args.init {
        if let Err(e) = run_file(&lua, path) {
            eprintln!("cirrus repl: --init failed: {e}");
            return 1;
        }
    }

    if let Some(path) = &args.script {
        return match run_file(&lua, path) {
            Ok(_) => 0,
            Err(e) => {
                eprintln!("cirrus repl: --script failed: {e}");
                1
            }
        };
    }

    interactive_loop(&lua)
}

fn run_file(lua: &mlua::Lua, path: &std::path::Path) -> Result<(), String> {
    let src = std::fs::read_to_string(path).map_err(|e| format!("read {path:?}: {e}"))?;
    lua.load(&src)
        .set_name(path.to_string_lossy())
        .exec()
        .map_err(|e| format!("{e}"))
}

fn interactive_loop(lua: &mlua::Lua) -> i32 {
    let mut rl: Editor<CirrusReplHelper, FileHistory> = match Editor::new() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("cirrus repl: rustyline init failed: {e}");
            return 2;
        }
    };
    rl.set_helper(Some(CirrusReplHelper::default()));
    let _ = rl.load_history(&history_path());

    println!("cirrus repl (Lua 5.4) — type `:help` for commands, Ctrl-D to exit");

    let mut buffer = String::new();
    loop {
        let prompt = if buffer.is_empty() {
            "cirrus> "
        } else {
            "    ... "
        };
        match rl.readline(prompt) {
            Ok(line) => {
                let _ = rl.add_history_entry(&line);
                let trimmed = line.trim();
                // Slash-style commands.
                if buffer.is_empty() {
                    match trimmed {
                        ":help" => {
                            print_help();
                            continue;
                        }
                        ":quit" | ":exit" => break,
                        ":reset" => {
                            buffer.clear();
                            continue;
                        }
                        cmd if cmd.starts_with(":script ") => {
                            let path = cmd["script ".len()..].trim();
                            if let Err(e) = run_file(lua, std::path::Path::new(path)) {
                                eprintln!("error: {e}");
                            }
                            continue;
                        }
                        _ => {}
                    }
                }
                if !buffer.is_empty() {
                    buffer.push('\n');
                }
                buffer.push_str(&line);

                // Try evaluating as expression first (so `1+1` prints `2`).
                let as_expr = format!("return {buffer}");
                match lua.load(&as_expr).set_name("=stdin").eval::<mlua::Value>() {
                    Ok(v) => {
                        match v {
                            mlua::Value::Nil => {}
                            mlua::Value::String(s) => println!(
                                "{}",
                                s.to_str()
                                    .map(|c| c.to_string())
                                    .unwrap_or_else(|_| String::new())
                            ),
                            other => println!("{other:?}"),
                        }
                        buffer.clear();
                    }
                    Err(_) => {
                        // Try as a statement.
                        match lua.load(&buffer).set_name("=stdin").exec() {
                            Ok(()) => buffer.clear(),
                            Err(mlua::Error::SyntaxError {
                                incomplete_input: true,
                                ..
                            }) => {
                                // Need more input — keep buffer.
                            }
                            Err(e) => {
                                eprintln!("error: {e}");
                                buffer.clear();
                            }
                        }
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                // Ctrl-C: clear current buffer.
                buffer.clear();
                println!("(buffer cleared)");
            }
            Err(ReadlineError::Eof) => break,
            Err(e) => {
                eprintln!("readline error: {e}");
                break;
            }
        }
    }
    let _ = rl.save_history(&history_path());
    0
}

fn print_help() {
    println!(
        r#"cirrus REPL commands:
  :help              show this help
  :quit / :exit      leave the REPL
  :reset             clear the multi-line input buffer
  :script <path>     load and run a Lua file

Lua globals registered:
  RE                 RunEngine handle
                       RE:run(plan)            execute and report exit_status
                       RE:pause(deferred?)
                       RE:resume()
                       RE:abort([reason])
                       RE:halt()
                       RE:stop()
                       RE:state()              -> "Idle" / "Running" / ...
                       RE:md_get()             pretty-printed JSON
                       RE:md_set(key, value)
  soft_detector(name)
  soft_motor(name, init?)

Bluesky-style device methods (mirrors cirrus-core::ext):
  motor:position()              -> number     (locatable readback)
  motor:target()                -> number     (locatable setpoint)
  motor:locate()                -> {{setpoint=, readback=}}
  det:read()                    -> {{field={{value=, timestamp=, ...}}}}
  det:describe()                -> {{field={{source=, dtype=, ...}}}}
  motor:set(v)                  -> Status     (call s:wait() to block)
  motor:move_to(v)              -> nil        (set + wait combined)
  det:trigger()                 -> Status
  motor:stop() / :stop_emergency() -> nil
  dev:stage() / :unstage()      -> nil
  flyer:kickoff() / :complete() -> Status
  Status:wait()                 -> nil (raises on failure)
  Status:done()                 -> bool
  count({{detectors}}, n)        plan
  scan({{detectors}}, motor, start, stop, n)
  mvr(motor, delta)
  sleep(seconds)
  null()                        no-op plan
  plan(fn, ...)                 wrap a Lua coroutine into a Plan

bluesky-style namespaces (full surface):
  bp.*    compound plans  (count, scan, list_scan, rel_scan,
                            rel_list_scan, grid_scan, rel_grid_scan,
                            inner_product_scan, scan_nd, spiral,
                            spiral_square, spiral_fermat, ramp_plan,
                            log_scan, count_with_trigger)
  bps.*   1-Msg / small stubs (open_run, close_run, create, save, drop,
                                read, null, abs_set, mv, mvr, trigger,
                                stop_dev, sleep, wait, checkpoint,
                                clear_checkpoint, pause, deferred_pause,
                                resume, kickoff, complete, stage,
                                unstage, stage_all, unstage_all,
                                monitor, unmonitor, trigger_and_read,
                                one_shot, repeater)
  bpt.*   coordinate generators returning Lua tables
                                (inner_product, outer_product,
                                 inner_list_product, outer_list_product,
                                 spiral, spiral_square, spiral_fermat)
  bpp.*   preprocessors taking and returning a Plan
                                (run_wrapper, inject_md, rewindable,
                                 monitor_during, stage_wrapper,
                                 baseline_wrapper, finalize_wrapper,
                                 subs_wrapper, relative_set,
                                 reset_positions, print_summary,
                                 contingency, pchain, msg_mutator)

Coroutine plans (generator-style) — yield Msg values via the `msg.*`
namespace:

  msg.open_run([{{plan_name=...}}])    msg.close_run([exit_status, [reason]])
  msg.create([stream])                 msg.save()        msg.drop()
  msg.read(device)                     msg.set(device, value, [group])
  msg.trigger(device, [group])         msg.wait(group, [timeout], [err])
  msg.checkpoint()                     msg.clear_checkpoint()
  msg.rewindable(bool)                 msg.pause([deferred])  msg.resume()
  msg.stage(device)                    msg.unstage(device)
  msg.stop_dev(device, [success])
  msg.monitor(device, [stream])        msg.unmonitor(device)
  msg.sleep(seconds)                   msg.null()

Example:
  local function my_scan(detectors, motor, n)
    coroutine.yield(msg.open_run({{plan_name="x"}}))
    for i = 0, n-1 do
      local pos = i / (n-1)
      coroutine.yield(msg.set(motor, pos, "g"))
      coroutine.yield(msg.wait("g"))
      coroutine.yield(msg.create("primary"))
      coroutine.yield(msg.read(motor))
      for _, d in ipairs(detectors) do coroutine.yield(msg.read(d)) end
      coroutine.yield(msg.save())
    end
    coroutine.yield(msg.close_run("success"))
  end
  RE:run(plan(my_scan, {{det1}}, m1, 5))

Coroutine yield return values:
  msg.open_run                            -> run UID (string)
  msg.set / trigger / kickoff / complete  -> wait-group string
                                             (auto-allocated if not given;
                                              feed back into msg.wait)
  msg.locate                              -> {{setpoint=, readback=}}
  msg.read                                -> {{field={{value=, timestamp=, ...}}}}
  msg.close_run                           -> exit_status string
  every other msg.*                       -> nil

Multi-line: incomplete input keeps the prompt at `... `; type `:reset` to drop.
"#
    );
}

fn history_path() -> PathBuf {
    if let Some(home) = std::env::var_os("HOME") {
        let mut p = PathBuf::from(home);
        p.push(".cirrus_repl_history");
        p
    } else {
        PathBuf::from(".cirrus_repl_history")
    }
}
