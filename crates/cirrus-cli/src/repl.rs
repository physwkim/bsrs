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

use clap::Args;
use cirrus_engine::RunEngine;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;

use crate::lua_env::build_lua;

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
}

/// Entry point — returns process exit code.
pub fn run(args: ReplArgs) -> i32 {
    let re = Arc::new(RunEngine::new(Vec::new()));
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
    let mut rl: DefaultEditor = match DefaultEditor::new() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("cirrus repl: rustyline init failed: {e}");
            return 2;
        }
    };
    let _ = rl.load_history(&history_path());

    println!(
        "cirrus repl (Lua 5.4) — type `:help` for commands, Ctrl-D to exit"
    );

    let mut buffer = String::new();
    loop {
        let prompt = if buffer.is_empty() { "cirrus> " } else { "    ... " };
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
  count({{detectors}}, n)        plan
  scan({{detectors}}, motor, start, stop, n)
  mvr(motor, delta)
  sleep(seconds)
  null()                        no-op plan

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
