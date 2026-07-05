//! `bsrs repl` — interactive Lua REPL for bsrs.
//!
//! Drives an in-process `RunEngine`, with bsrs types/factories
//! pre-registered as Lua globals. Goal: IPython-equivalent dev/test
//! surface without a Python install.
//!
//! Line editing is `reedline` (the Nushell editor, prompt_toolkit's
//! equivalent): a completion menu (Tab), fish-style history autosuggestion,
//! reverse history search (Ctrl-R), and true in-place multi-line editing —
//! an incomplete Lua chunk drops to a `... ` continuation line.
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
//! Slash-style helpers: type `:help`, `:quit`, `:exit`, `:script <path>`.

use std::borrow::Cow;
use std::path::PathBuf;
use std::sync::Arc;

use bsrs::engine::RunEngine;
use clap::Args;
use nu_ansi_term::{Color, Style};
use reedline::{
    default_emacs_keybindings, ColumnarMenu, Completer, DefaultHinter, Emacs, FileBackedHistory,
    History, KeyCode, KeyModifiers, MenuBuilder, Prompt, PromptEditMode, PromptHistorySearch,
    Reedline, ReedlineEvent, ReedlineMenu, Signal, Span, Suggestion, ValidationResult, Validator,
};

use bsrs::host::lua_env::build_lua;

/// Curated completion tokens: bsrs's well-known Lua globals + namespaces,
/// Lua keywords, and slash commands. Shared by the completer and its tests.
///
/// We do not introspect live Lua state here (commit 3 adds a post-eval global
/// snapshot); this is the always-available baseline.
fn base_keywords() -> Vec<&'static str> {
    vec![
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
        ":script",
    ]
}

/// reedline `Completer` over bsrs's curated Lua tokens (prefix match on the
/// word under the cursor). Drives the columnar completion menu.
struct BsrsCompleter {
    keywords: Vec<&'static str>,
}

impl BsrsCompleter {
    fn new() -> Self {
        Self {
            keywords: base_keywords(),
        }
    }

    /// Start of the word under `pos`: scan back to the previous whitespace or
    /// `(`, `,`, `=`, `{`, `[`, newline delimiter, else beginning of line.
    fn word_start(line: &str, pos: usize) -> usize {
        let end = pos.min(line.len());
        let bytes = &line.as_bytes()[..end];
        let mut start = end;
        for (i, &b) in bytes.iter().enumerate().rev() {
            if matches!(b, b' ' | b'\t' | b'(' | b',' | b'=' | b'{' | b'[' | b'\n') {
                start = i + 1;
                break;
            }
            start = i;
        }
        start
    }

    /// Pure candidate logic (unit-tested): `(word_start, matching tokens)`.
    fn candidates(&self, line: &str, pos: usize) -> (usize, Vec<String>) {
        let start = Self::word_start(line, pos);
        let end = pos.min(line.len());
        let word = &line[start..end];
        if word.is_empty() {
            return (start, Vec::new());
        }
        let hits = self
            .keywords
            .iter()
            .filter(|k| k.starts_with(word))
            .map(|k| (*k).to_string())
            .collect();
        (start, hits)
    }
}

impl Completer for BsrsCompleter {
    fn complete(&mut self, line: &str, pos: usize) -> Vec<Suggestion> {
        let (start, hits) = self.candidates(line, pos);
        let end = pos.min(line.len());
        hits.into_iter()
            .map(|value| Suggestion {
                value,
                display_override: None,
                description: None,
                style: None,
                extra: None,
                span: Span::new(start, end),
                append_whitespace: false,
                match_indices: None,
            })
            .collect()
    }
}

/// reedline `Validator` for multi-line input. Compiles the buffer WITHOUT
/// executing it (via a private parse-only Lua): only an `incomplete_input`
/// syntax error keeps the prompt open for more lines. A real syntax error is
/// `Complete` — the eval loop reports it. Running Lua here would fire side
/// effects on every Enter, so we must parse, not execute.
struct LuaValidator {
    parser: mlua::Lua,
}

impl LuaValidator {
    fn new() -> Self {
        Self {
            parser: mlua::Lua::new(),
        }
    }

    fn is_incomplete(&self, line: &str) -> bool {
        matches!(
            self.parser.load(line).into_function(),
            Err(mlua::Error::SyntaxError {
                incomplete_input: true,
                ..
            })
        )
    }
}

impl Validator for LuaValidator {
    fn validate(&self, line: &str) -> ValidationResult {
        if self.is_incomplete(line) {
            ValidationResult::Incomplete
        } else {
            ValidationResult::Complete
        }
    }
}

/// `bsrs> ` main prompt, `... ` for explicit-newline continuation.
struct BsrsPrompt;

impl Prompt for BsrsPrompt {
    fn render_prompt_left(&self) -> Cow<'_, str> {
        Cow::Borrowed("bsrs")
    }
    fn render_prompt_right(&self) -> Cow<'_, str> {
        Cow::Borrowed("")
    }
    fn render_prompt_indicator(&self, _mode: PromptEditMode) -> Cow<'_, str> {
        Cow::Borrowed("> ")
    }
    fn render_prompt_multiline_indicator(&self) -> Cow<'_, str> {
        Cow::Borrowed("... ")
    }
    fn render_prompt_history_search_indicator(
        &self,
        _history_search: PromptHistorySearch,
    ) -> Cow<'_, str> {
        Cow::Borrowed("(reverse-search) ")
    }
}

/// Arguments for `bsrs repl`.
#[derive(Args, Debug)]
pub struct ReplArgs {
    /// Optional file with Lua statements to execute before the prompt
    /// opens. Useful as a `~/.bsrsrc.lua` style init.
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

    /// Optional path to a JSONL file. Every Document the engine
    /// emits is appended as one JSON line. File is opened in
    /// append mode — multiple runs accumulate.
    #[arg(long, value_name = "PATH")]
    pub doc_jsonl: Option<std::path::PathBuf>,

    /// Optional Tiled HTTP endpoint. When set, every Document the
    /// engine emits is registered into the named container on the
    /// Tiled catalog. Example: `--doc-tiled http://localhost:8000`.
    /// Requires the `tiled` Cargo feature.
    #[cfg(feature = "tiled")]
    #[arg(long, value_name = "URL")]
    pub doc_tiled: Option<String>,

    /// Container name under the Tiled catalog (default `bsrs`).
    /// Used only when `--doc-tiled` is set.
    #[cfg(feature = "tiled")]
    #[arg(long, value_name = "NAME", default_value = "bsrs")]
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
    bsrs::host::ca_devices::bootstrap_ca();

    // Optional ZMQ document fan-out — bluesky `Publisher` envelope.
    // Bound on a separate PUB socket; downstream Python consumers
    // attach a `bluesky.callbacks.zmq.RemoteDispatcher`.
    let mut sinks: Vec<Arc<dyn bsrs::engine::DocumentSink>> = Vec::new();
    if let Some(addr) = &args.doc_zmq {
        match bsrs::callbacks::ZmqDocumentSink::bind(addr) {
            Ok(s) => {
                eprintln!("bsrs repl: publishing Documents on ZMQ {addr}");
                sinks.push(Arc::new(s) as Arc<dyn bsrs::engine::DocumentSink>);
            }
            Err(e) => {
                eprintln!("bsrs repl: failed to bind ZMQ {addr}: {e}");
                return 2;
            }
        }
    }

    if let Some(path) = &args.doc_jsonl {
        match bsrs::core::runtime::bsrs_runtime().block_on(bsrs::callbacks::JsonlSink::open(path)) {
            Ok(s) => {
                eprintln!(
                    "bsrs repl: appending Documents as JSONL to {}",
                    path.display()
                );
                sinks.push(Arc::new(s) as Arc<dyn bsrs::engine::DocumentSink>);
            }
            Err(e) => {
                eprintln!("bsrs repl: failed to open JSONL {}: {e}", path.display());
                return 2;
            }
        }
    }

    #[cfg(feature = "tiled")]
    if let Some(url) = &args.doc_tiled {
        let mut sink = match bsrs::callbacks::TiledSink::new(url, &args.doc_tiled_container) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("bsrs repl: failed to build TiledSink for {url}: {e}");
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
            "bsrs repl: registering Documents into Tiled at {url} container={:?}",
            args.doc_tiled_container
        );
        sinks.push(Arc::new(sink) as Arc<dyn bsrs::engine::DocumentSink>);
    }

    let re = Arc::new(RunEngine::new(sinks));
    let lua = match build_lua(re) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("bsrs repl: failed to initialize Lua: {e}");
            return 2;
        }
    };

    if let Some(path) = &args.init {
        if let Err(e) = run_file(&lua, path) {
            eprintln!("bsrs repl: --init failed: {e}");
            return 1;
        }
    }

    if let Some(path) = &args.script {
        return match run_file(&lua, path) {
            Ok(_) => 0,
            Err(e) => {
                eprintln!("bsrs repl: --script failed: {e}");
                1
            }
        };
    }

    interactive_loop(&lua)
}

pub(crate) fn run_file(lua: &mlua::Lua, path: &std::path::Path) -> Result<(), String> {
    let src = std::fs::read_to_string(path).map_err(|e| format!("read {path:?}: {e}"))?;
    lua.load(&src)
        .set_name(path.to_string_lossy())
        .exec()
        .map_err(|e| format!("{e}"))
}

pub(crate) fn interactive_loop(lua: &mlua::Lua) -> i32 {
    // Persistent, file-backed history (fish-style autosuggestion reads from it
    // via the DefaultHinter below). Fall back to in-memory if the file can't
    // be opened.
    let history: Box<dyn History> = match FileBackedHistory::with_file(1000, history_path()) {
        Ok(h) => Box::new(h),
        Err(_) => Box::new(FileBackedHistory::new(1000).expect("in-memory history")),
    };

    // Tab opens / advances the columnar completion menu; otherwise Emacs keys.
    let mut keybindings = default_emacs_keybindings();
    keybindings.add_binding(
        KeyModifiers::NONE,
        KeyCode::Tab,
        ReedlineEvent::UntilFound(vec![
            ReedlineEvent::Menu("completion_menu".to_string()),
            ReedlineEvent::MenuNext,
        ]),
    );
    let edit_mode = Box::new(Emacs::new(keybindings));
    let completion_menu = Box::new(ColumnarMenu::default().with_name("completion_menu"));

    let mut line_editor = Reedline::create()
        .with_completer(Box::new(BsrsCompleter::new()))
        .with_validator(Box::new(LuaValidator::new()))
        .with_hinter(Box::new(
            DefaultHinter::default().with_style(Style::new().fg(Color::DarkGray)),
        ))
        .with_history(history)
        .with_menu(ReedlineMenu::EngineCompleter(completion_menu))
        .with_edit_mode(edit_mode);

    let prompt = BsrsPrompt;

    println!(
        "bsrs repl (Lua 5.4, reedline) — `:help` for commands, Tab to complete, Ctrl-D to exit"
    );

    loop {
        match line_editor.read_line(&prompt) {
            Ok(Signal::Success(line)) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                // Slash-style commands.
                match trimmed {
                    ":help" => {
                        print_help();
                        continue;
                    }
                    ":quit" | ":exit" => break,
                    cmd if cmd.starts_with(":script ") => {
                        let path = cmd[":script ".len()..].trim();
                        if let Err(e) = run_file(lua, std::path::Path::new(path)) {
                            eprintln!("error: {e}");
                        }
                        continue;
                    }
                    _ => {}
                }
                // reedline's validator has already ensured the input is a
                // syntactically complete chunk (possibly multi-line).
                eval_line(lua, &line);
            }
            // Ctrl-C aborts the line being edited; keep the session open.
            Ok(Signal::CtrlC) => println!("(interrupted)"),
            Ok(Signal::CtrlD) => break,
            // `Signal` is `#[non_exhaustive]`; treat any future variant as a
            // benign no-op rather than exiting.
            Ok(_) => {}
            Err(e) => {
                eprintln!("readline error: {e}");
                break;
            }
        }
    }
    0
}

/// Evaluate one complete input: try it as an expression first (so `1+1` prints
/// `2`), then fall back to executing it as a statement.
fn eval_line(lua: &mlua::Lua, src: &str) {
    let as_expr = format!("return {src}");
    match lua.load(&as_expr).set_name("=stdin").eval::<mlua::Value>() {
        Ok(v) => match v {
            mlua::Value::Nil => {}
            mlua::Value::String(s) => {
                println!("{}", s.to_str().map(|c| c.to_string()).unwrap_or_default())
            }
            other => println!("{other:?}"),
        },
        Err(_) => {
            if let Err(e) = lua.load(src).set_name("=stdin").exec() {
                eprintln!("error: {e}");
            }
        }
    }
}

fn print_help() {
    println!(
        r#"bsrs REPL commands:
  :help              show this help
  :quit / :exit      leave the REPL
  :script <path>     load and run a Lua file
  Tab                open the completion menu; Ctrl-R searches history

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

Bluesky-style device methods (mirrors bsrs-core::ext):
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
                            spiral_square, spiral_fermat,
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
                                 subs_wrapper, lazily_stage_wrapper,
                                 set_run_key_wrapper, stub_wrapper,
                                 relative_set, reset_positions,
                                 print_summary, contingency, pchain,
                                 msg_mutator)

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

Multi-line: an incomplete chunk (open `function`/`do`/`(` ...) drops to a
`... ` continuation line you can edit in place; Ctrl-C abandons it.
"#
    );
}

fn history_path() -> PathBuf {
    if let Some(home) = std::env::var_os("HOME") {
        let mut p = PathBuf::from(home);
        p.push(".bsrs_repl_history");
        p
    } else {
        PathBuf::from(".bsrs_repl_history")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validator_flags_incomplete_blocks_but_not_errors() {
        let v = LuaValidator::new();
        // Open blocks / unbalanced delimiters → keep editing.
        assert!(v.is_incomplete("function foo()"));
        assert!(v.is_incomplete("for i=1,3 do"));
        assert!(v.is_incomplete("if x then"));
        assert!(v.is_incomplete("count({det1}, 5")); // unclosed paren
                                                     // Complete chunks → submit.
        assert!(!v.is_incomplete("x = 1"));
        assert!(!v.is_incomplete("function foo() end"));
        assert!(!v.is_incomplete("RE:run(count({det1}, 5))"));
        // A real syntax error is "complete" (not incomplete): the eval loop
        // reports it rather than the prompt hanging on a continuation line.
        assert!(!v.is_incomplete("1 +* 2"));
    }

    #[test]
    fn completer_prefix_matches_at_word_boundaries() {
        let c = BsrsCompleter::new();

        // Bare prefix at BOL.
        let (start, hits) = c.candidates("cou", 3);
        assert_eq!(start, 0);
        assert!(hits.iter().any(|h| h == "count"));

        // Word start is after the `(` delimiter.
        let (start2, hits2) = c.candidates("RE:run(cou", 10);
        assert_eq!(start2, 7);
        assert!(hits2.iter().any(|h| h == "count"));

        // Namespaced token.
        let (_, hits3) = c.candidates("msg.op", 6);
        assert!(hits3.iter().any(|h| h == "msg.open_run"));

        // Empty word (cursor right after a delimiter) → no candidates.
        let (_, none) = c.candidates("RE:run(", 7);
        assert!(none.is_empty());
    }
}
