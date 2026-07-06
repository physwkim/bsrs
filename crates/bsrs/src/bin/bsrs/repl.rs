//! `bsrs repl` — interactive Lua REPL for bsrs.
//!
//! Drives an in-process `RunEngine`, with bsrs types/factories
//! pre-registered as Lua globals. Goal: IPython-equivalent dev/test
//! surface without a Python install.
//!
//! Line editing is `reedline` (the Nushell editor, prompt_toolkit's
//! equivalent): live Lua syntax highlighting, a completion menu (Tab) that
//! learns the globals and table fields you define, fish-style history
//! autosuggestion, reverse history search (Ctrl-R), and true in-place
//! multi-line editing — an incomplete Lua chunk drops to a `... `
//! continuation line. `name?` / `name??` introspect a value the way IPython's
//! `obj?` does (type, signature, fields / methods).
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
use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use bsrs::engine::RunEngine;
use clap::Args;
use nu_ansi_term::{Color, Style};
use reedline::{
    default_emacs_keybindings, ColumnarMenu, Completer, DefaultHinter, Emacs, FileBackedHistory,
    Highlighter, History, KeyCode, KeyModifiers, MenuBuilder, Prompt, PromptEditMode,
    PromptHistorySearch, Reedline, ReedlineEvent, ReedlineMenu, Signal, Span, StyledText,
    Suggestion, ValidationResult, Validator,
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

/// Reflect the method names of a `UserData` value (`det1:read`, `RE:run`, ...).
///
/// bsrs's `impl UserData` types register methods with `add_method` and add no
/// fields or custom `__index`, so mlua stores the methods in an enumerable
/// `__index` *table* on the metatable (see mlua `raw.rs`). We read it through
/// the sanctioned `UserDataMetatable` API — no `getmetatable`, no host-side
/// method list to keep in sync. A userdata whose `__index` is a function
/// (field-based) yields nothing here and falls back to curated names.
fn userdata_methods(ud: &mlua::AnyUserData) -> Vec<String> {
    let Ok(metatable) = ud.metatable() else {
        return Vec::new();
    };
    let Ok(index) = metatable.get::<mlua::Table>("__index") else {
        return Vec::new();
    };
    let mut names: Vec<String> = Vec::new();
    for pair in index.pairs::<mlua::String, mlua::Value>() {
        let Ok((k, _)) = pair else { continue };
        if let Ok(s) = k.to_str() {
            if !s.starts_with('_') {
                names.push(s.to_string());
            }
        }
    }
    names.sort();
    names.dedup();
    names
}

/// Completion candidates: top-level names (no separator) plus per-container
/// members reached via `base.field` / `base:method`.
///
/// Seeded from the curated [`base_keywords`], then refreshed after every eval
/// from the live Lua state so user-defined variables (`det1 = soft_detector(…)`)
/// and their table fields become completable. Held behind an `Arc<Mutex<…>>`
/// shared between the completer and the eval loop.
#[derive(Default)]
struct CompletionModel {
    /// Names completable with no separator: globals + built-ins + slash cmds.
    globals: Vec<String>,
    /// Member names per container, for `base.` / `base:` completion.
    members: HashMap<String, Vec<String>>,
}

impl CompletionModel {
    /// The always-available baseline derived from the curated token list.
    fn with_static() -> Self {
        let mut m = CompletionModel::default();
        for tok in base_keywords() {
            // Slash commands (`:help`, ...) are whole-token globals, not a
            // `base:member` access — keep them intact.
            if tok.starts_with(':') {
                m.globals.push(tok.to_string());
                continue;
            }
            match tok.find([':', '.']) {
                Some(idx) => {
                    let base = &tok[..idx];
                    let member = &tok[idx + 1..];
                    m.globals.push(base.to_string());
                    m.members
                        .entry(base.to_string())
                        .or_default()
                        .push(member.to_string());
                }
                None => m.globals.push(tok.to_string()),
            }
        }
        m
    }

    /// Fold live Lua state on top: every non-`_` global name, one level of
    /// fields for table-valued globals (`msg.*`, `string.*`, user tables, ...),
    /// and the methods of userdata globals (`det1:trigger`, `RE:run`, ...) via
    /// [`userdata_methods`].
    fn add_live(&mut self, lua: &mlua::Lua) {
        for pair in lua.globals().pairs::<mlua::String, mlua::Value>() {
            let Ok((k, v)) = pair else { continue };
            let Ok(name) = k.to_str() else { continue };
            let name = name.to_string();
            if name.starts_with('_') {
                continue;
            }
            self.globals.push(name.clone());
            match &v {
                mlua::Value::Table(t) => {
                    let entry = self.members.entry(name).or_default();
                    for sub in t.pairs::<mlua::String, mlua::Value>() {
                        let Ok((sk, _sv)) = sub else { continue };
                        if let Ok(field) = sk.to_str() {
                            if !field.starts_with('_') {
                                entry.push(field.to_string());
                            }
                        }
                    }
                }
                mlua::Value::UserData(ud) => {
                    let methods = userdata_methods(ud);
                    if !methods.is_empty() {
                        self.members.entry(name).or_default().extend(methods);
                    }
                }
                _ => {}
            }
        }
    }

    /// Sort + dedup so the menu is stable and free of repeats.
    fn finish(&mut self) {
        self.globals.sort();
        self.globals.dedup();
        for members in self.members.values_mut() {
            members.sort();
            members.dedup();
        }
    }

    /// Candidate replacements for `word` (the whole dotted path under the
    /// cursor). Splits on the last `.`/`:` for member completion, otherwise a
    /// prefix match over `globals`.
    fn candidates_for(&self, word: &str) -> Vec<String> {
        // Slash command prefix — match the whole token, not a member access.
        if word.starts_with(':') {
            return self
                .globals
                .iter()
                .filter(|g| g.starts_with(word))
                .cloned()
                .collect();
        }
        if let Some(idx) = word.rfind(['.', ':']) {
            let base = &word[..idx];
            let sep = &word[idx..=idx];
            let partial = &word[idx + 1..];
            return match self.members.get(base) {
                Some(members) => members
                    .iter()
                    .filter(|m| m.starts_with(partial))
                    .map(|m| format!("{base}{sep}{m}"))
                    .collect(),
                None => Vec::new(),
            };
        }
        self.globals
            .iter()
            .filter(|g| g.starts_with(word))
            .cloned()
            .collect()
    }
}

/// Build a fresh model (static baseline + live snapshot) and publish it to the
/// shared handle. Called once at startup and after every input that can define
/// or remove globals.
fn refresh_completion(model: &Arc<Mutex<CompletionModel>>, lua: &mlua::Lua) {
    let mut m = CompletionModel::with_static();
    m.add_live(lua);
    m.finish();
    if let Ok(mut guard) = model.lock() {
        *guard = m;
    }
}

/// reedline `Completer` backed by the live [`CompletionModel`]. Drives the
/// columnar completion menu.
struct BsrsCompleter {
    model: Arc<Mutex<CompletionModel>>,
}

impl BsrsCompleter {
    fn new(model: Arc<Mutex<CompletionModel>>) -> Self {
        Self { model }
    }

    /// A completer over just the static baseline (used in unit tests).
    #[cfg(test)]
    fn new_static() -> Self {
        Self {
            model: Arc::new(Mutex::new({
                let mut m = CompletionModel::with_static();
                m.finish();
                m
            })),
        }
    }

    /// Start of the word under `pos`: scan back to the previous whitespace or
    /// `(`, `,`, `=`, `{`, `[`, newline delimiter, else beginning of line. Note
    /// `.` and `:` are NOT delimiters — a dotted path is one word.
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
        let hits = match self.model.lock() {
            Ok(m) => m.candidates_for(word),
            Err(_) => Vec::new(),
        };
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

/// Lexical token classes we color at the prompt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TokKind {
    /// Lua reserved word (`function`, `local`, `end`, `if`, ...).
    Keyword,
    /// A bsrs top-level global (`RE`, `count`, `scan`, `msg`, ...).
    Global,
    /// String literal (`"..."`, `'...'`, `[[...]]`, `[=[...]=]`).
    StringLit,
    /// Numeric literal (decimal / hex / float / exponent).
    Number,
    /// Comment (`-- ...` line or `--[[ ... ]]` block).
    Comment,
    /// Anything else — identifiers, operators, whitespace, punctuation.
    Text,
}

/// Lua 5.4 reserved words.
const LUA_KEYWORDS: &[&str] = &[
    "and", "break", "do", "else", "elseif", "end", "false", "for", "function", "goto", "if", "in",
    "local", "nil", "not", "or", "repeat", "return", "then", "true", "until", "while",
];

/// bsrs top-level globals registered in the REPL environment (bare names only —
/// method forms like `RE:run` color via the `RE` head).
const BSRS_GLOBALS: &[&str] = &[
    "RE",
    "soft_detector",
    "soft_motor",
    "soft_pausable",
    "count",
    "scan",
    "mvr",
    "sleep",
    "null",
    "plan",
    "print",
    "msg",
    "bp",
    "bps",
    "bpt",
    "bpp",
    "tiled",
    "coroutine",
];

/// If `chars[i..]` opens a Lua long bracket (`[` `=`* `[`), return its level
/// (the count of `=`); otherwise `None`.
fn long_bracket_open(chars: &[char], i: usize) -> Option<usize> {
    if chars.get(i) != Some(&'[') {
        return None;
    }
    let mut j = i + 1;
    let mut level = 0;
    while chars.get(j) == Some(&'=') {
        level += 1;
        j += 1;
    }
    if chars.get(j) == Some(&'[') {
        Some(level)
    } else {
        None
    }
}

/// Index just past the closing `]` `=`{level} `]` of a long bracket that opens
/// at `i`; returns `chars.len()` if unterminated (highlight to end of buffer).
fn long_bracket_scan(chars: &[char], i: usize, level: usize) -> usize {
    let n = chars.len();
    // Opening is `[` + level*`=` + `[` → content starts after it.
    let mut j = i + level + 2;
    while j < n {
        if chars[j] == ']' {
            let mut k = j + 1;
            let mut cnt = 0;
            while chars.get(k) == Some(&'=') {
                cnt += 1;
                k += 1;
            }
            if cnt == level && chars.get(k) == Some(&']') {
                return k + 1;
            }
        }
        j += 1;
    }
    n
}

/// reedline `Highlighter`: a single-pass Lua lexer that colors keywords,
/// bsrs globals, strings, numbers, and comments. Classification lives in the
/// pure `lex` (unit-tested); `highlight` only maps `TokKind` → color.
struct LuaHighlighter {
    keywords: HashSet<&'static str>,
    globals: HashSet<&'static str>,
}

impl LuaHighlighter {
    fn new() -> Self {
        Self {
            keywords: LUA_KEYWORDS.iter().copied().collect(),
            globals: BSRS_GLOBALS.iter().copied().collect(),
        }
    }

    fn style_for(kind: TokKind) -> Style {
        match kind {
            TokKind::Keyword => Style::new().fg(Color::Purple),
            TokKind::Global => Style::new().fg(Color::LightBlue),
            TokKind::StringLit => Style::new().fg(Color::Green),
            TokKind::Number => Style::new().fg(Color::Cyan),
            TokKind::Comment => Style::new().fg(Color::DarkGray),
            TokKind::Text => Style::new(),
        }
    }

    /// Tokenize `src` into `(kind, text)` segments covering it exactly (the
    /// concatenation of the texts equals `src`).
    fn lex(&self, src: &str) -> Vec<(TokKind, String)> {
        let chars: Vec<char> = src.chars().collect();
        let n = chars.len();
        let mut i = 0usize;
        let mut out: Vec<(TokKind, String)> = Vec::new();

        while i < n {
            let c = chars[i];

            // Comment: `--` line, or `--[[ ... ]]` / `--[=[ ... ]=]` block.
            if c == '-' && chars.get(i + 1) == Some(&'-') {
                let start = i;
                let after = i + 2;
                let end = match long_bracket_open(&chars, after) {
                    Some(level) => long_bracket_scan(&chars, after, level),
                    None => {
                        let mut j = after;
                        while j < n && chars[j] != '\n' {
                            j += 1;
                        }
                        j
                    }
                };
                out.push((TokKind::Comment, chars[start..end].iter().collect()));
                i = end;
                continue;
            }

            // Long string: `[[ ... ]]` / `[=[ ... ]=]`.
            if c == '[' {
                if let Some(level) = long_bracket_open(&chars, i) {
                    let end = long_bracket_scan(&chars, i, level);
                    out.push((TokKind::StringLit, chars[i..end].iter().collect()));
                    i = end;
                    continue;
                }
            }

            // Short string: `"..."` / `'...'` (with `\` escapes; stops at an
            // unescaped newline so a stray quote doesn't paint the whole buffer).
            if c == '"' || c == '\'' {
                let start = i;
                let mut j = i + 1;
                while j < n {
                    if chars[j] == '\\' && j + 1 < n {
                        j += 2;
                        continue;
                    }
                    if chars[j] == c {
                        j += 1;
                        break;
                    }
                    if chars[j] == '\n' {
                        break;
                    }
                    j += 1;
                }
                out.push((TokKind::StringLit, chars[start..j].iter().collect()));
                i = j;
                continue;
            }

            // Number: decimal / hex / float / exponent.
            if c.is_ascii_digit()
                || (c == '.' && chars.get(i + 1).is_some_and(|d| d.is_ascii_digit()))
            {
                let start = i;
                let mut j = i;
                if c == '0' && matches!(chars.get(i + 1), Some('x' | 'X')) {
                    j += 2;
                    while j < n
                        && (chars[j].is_ascii_hexdigit()
                            || chars[j] == '.'
                            || matches!(chars[j], 'p' | 'P')
                            || (matches!(chars[j], '+' | '-') && matches!(chars[j - 1], 'p' | 'P')))
                    {
                        j += 1;
                    }
                } else {
                    while j < n
                        && (chars[j].is_ascii_digit()
                            || chars[j] == '.'
                            || matches!(chars[j], 'e' | 'E')
                            || (matches!(chars[j], '+' | '-') && matches!(chars[j - 1], 'e' | 'E')))
                    {
                        j += 1;
                    }
                }
                out.push((TokKind::Number, chars[start..j].iter().collect()));
                i = j;
                continue;
            }

            // Identifier / keyword / global.
            if c.is_alphabetic() || c == '_' {
                let start = i;
                let mut j = i;
                while j < n && (chars[j].is_alphanumeric() || chars[j] == '_') {
                    j += 1;
                }
                let word: String = chars[start..j].iter().collect();
                let kind = if self.keywords.contains(word.as_str()) {
                    TokKind::Keyword
                } else if self.globals.contains(word.as_str()) {
                    TokKind::Global
                } else {
                    TokKind::Text
                };
                out.push((kind, word));
                i = j;
                continue;
            }

            // Default run: whitespace / operators / punctuation, up to the next
            // token start.
            let start = i;
            let mut j = i;
            while j < n {
                let d = chars[j];
                let boundary = (d == '-' && chars.get(j + 1) == Some(&'-'))
                    || d == '"'
                    || d == '\''
                    || d.is_alphabetic()
                    || d == '_'
                    || d.is_ascii_digit()
                    || (d == '[' && long_bracket_open(&chars, j).is_some())
                    || (d == '.' && chars.get(j + 1).is_some_and(|x| x.is_ascii_digit()));
                if j > start && boundary {
                    break;
                }
                j += 1;
            }
            out.push((TokKind::Text, chars[start..j].iter().collect()));
            i = j;
        }
        out
    }
}

impl Highlighter for LuaHighlighter {
    fn highlight(&self, line: &str, _cursor: usize) -> StyledText {
        let mut styled = StyledText::new();
        for (kind, text) in self.lex(line) {
            styled.push((Self::style_for(kind), text));
        }
        styled
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

    // Completion model shared with the completer; seeded from the live Lua
    // state now (built-ins + anything `--init` defined) and refreshed after
    // every input that can change globals.
    let model = Arc::new(Mutex::new(CompletionModel::default()));
    refresh_completion(&model, lua);

    let mut line_editor = Reedline::create()
        .with_completer(Box::new(BsrsCompleter::new(model.clone())))
        .with_validator(Box::new(LuaValidator::new()))
        .with_highlighter(Box::new(LuaHighlighter::new()))
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
                // IPython-style introspection: `name?` / `name??`.
                if let Some((target, verbose)) = parse_introspect(trimmed) {
                    print!("{}", introspect_report(lua, &target, verbose));
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
                        refresh_completion(&model, lua);
                        continue;
                    }
                    _ => {}
                }
                // reedline's validator has already ensured the input is a
                // syntactically complete chunk (possibly multi-line).
                eval_line(lua, &line);
                // Pick up any globals the input defined (or removed).
                refresh_completion(&model, lua);
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

/// Parse an IPython-style introspection request. `name?` / `?name` → brief,
/// `name??` / `??name` → verbose. Returns `(target, verbose)` or `None` if the
/// line is not an introspection request. `?` is not a Lua operator, so any
/// stray `?` at either end is unambiguous.
fn parse_introspect(trimmed: &str) -> Option<(String, bool)> {
    if let Some(t) = trimmed.strip_suffix("??") {
        return Some((t.trim().to_string(), true));
    }
    if let Some(t) = trimmed.strip_prefix("??") {
        return Some((t.trim().to_string(), true));
    }
    if let Some(t) = trimmed.strip_suffix('?') {
        return Some((t.trim().to_string(), false));
    }
    if let Some(t) = trimmed.strip_prefix('?') {
        return Some((t.trim().to_string(), false));
    }
    None
}

/// Curated `(signature, summary)` for the well-known bsrs names — Lua carries
/// no signature/docstring metadata, so this is our stand-in for IPython's
/// `obj?` docstring panel.
fn doc_for(name: &str) -> Option<(&'static str, &'static str)> {
    let entry = match name {
        "count" => (
            "count(detectors, num=1, delay=nil)",
            "Plan: read `detectors` `num` times into one run.",
        ),
        "scan" => (
            "scan(detectors, motor, start, stop, num)",
            "Plan: step `motor` start→stop over `num` points, reading `detectors`.",
        ),
        "mvr" => (
            "mvr(motor, delta)",
            "Plan: move `motor` by a relative `delta`.",
        ),
        "sleep" => ("sleep(seconds)", "Plan: pause the plan for `seconds`."),
        "null" => ("null()", "Plan: a no-op (emits no Msg)."),
        "plan" => ("plan(fn, ...)", "Wrap a Lua coroutine `fn` into a Plan."),
        "print" => ("print(...)", "Print values to stdout (Lua base library)."),
        "soft_detector" => (
            "soft_detector(name)",
            "Create an in-memory detector device.",
        ),
        "soft_motor" => (
            "soft_motor(name, initial=0.0)",
            "Create an in-memory motor device.",
        ),
        "soft_pausable" => ("soft_pausable(name)", "Create a pausable suspender source."),
        "RE" => (
            "RE:run(plan) / RE:pause() / RE:resume() / RE:abort() / RE:state()",
            "The RunEngine handle. Use `RE??` to list the method surface.",
        ),
        "msg" => (
            "msg.<verb>(...)",
            "Coroutine-plan Msg constructors (open_run, read, set, ...). `msg??` lists them.",
        ),
        "bp" => (
            "bp.<plan>(...)",
            "Compound plans (count, scan, grid_scan, spiral, ...).",
        ),
        "bps" => (
            "bps.<stub>(...)",
            "Plan stubs (open_run, mv, trigger, read, ...).",
        ),
        "bpt" => (
            "bpt.<gen>(...)",
            "Coordinate generators returning Lua tables.",
        ),
        "bpp" => (
            "bpp.<wrapper>(plan, ...)",
            "Plan preprocessors (run_wrapper, monitor_during, ...).",
        ),
        "tiled" => ("tiled.<fn>(...)", "Tiled document-sink helpers."),
        _ => return None,
    };
    Some(entry)
}

/// Member names of `base` drawn from the curated token list (`base:member` /
/// `base.member`). Used to describe userdata like `RE` whose methods mlua
/// cannot enumerate at runtime.
fn curated_members(base: &str) -> Vec<String> {
    let mut out: Vec<String> = base_keywords()
        .into_iter()
        .filter(|tok| !tok.starts_with(':'))
        .filter_map(|tok| {
            let idx = tok.find([':', '.'])?;
            (&tok[..idx] == base).then(|| tok[idx + 1..].to_string())
        })
        .collect();
    out.sort();
    out.dedup();
    out
}

/// Append a `label (n): a  b  c ...` block, wrapping at 4 per line and (unless
/// `verbose`) truncating long lists with a `?? for all` hint.
fn write_names(report: &mut String, label: &str, names: &[String], verbose: bool) {
    if names.is_empty() {
        return;
    }
    let limit = if verbose {
        names.len()
    } else {
        names.len().min(12)
    };
    let _ = writeln!(report, "  {label} ({}):", names.len());
    for chunk in names[..limit].chunks(4) {
        let _ = writeln!(report, "    {}", chunk.join("  "));
    }
    if limit < names.len() {
        let _ = writeln!(
            report,
            "    ... (+{} more; use `??` for all)",
            names.len() - limit
        );
    }
}

/// Build the `obj?` / `obj??` report: runtime type from the live Lua value,
/// curated signature/summary, plus fields (tables) or methods (userdata).
fn introspect_report(lua: &mlua::Lua, target: &str, verbose: bool) -> String {
    let target = target.trim();
    let mut report = String::new();
    if target.is_empty() {
        return "usage: `name?` shows a summary, `name??` shows fields / methods too\n".to_string();
    }
    let _ = writeln!(report, "{target}");

    let evaluated = lua.load(format!("return {target}")).eval::<mlua::Value>();
    match evaluated {
        // A real live value (not nil).
        Ok(value) if !matches!(value, mlua::Value::Nil) => {
            let _ = writeln!(report, "  Type:      {}", value.type_name());
            if let Some((sig, summary)) = doc_for(target) {
                let _ = writeln!(report, "  Signature: {sig}");
                let _ = writeln!(report, "  Summary:   {summary}");
            }
            match &value {
                mlua::Value::Table(t) => {
                    let mut keys: Vec<String> = Vec::new();
                    for pair in t.pairs::<mlua::String, mlua::Value>() {
                        let Ok((k, _)) = pair else { continue };
                        if let Ok(s) = k.to_str() {
                            if !s.starts_with('_') {
                                keys.push(s.to_string());
                            }
                        }
                    }
                    keys.sort();
                    keys.dedup();
                    write_names(&mut report, "Fields", &keys, verbose);
                }
                mlua::Value::UserData(ud) => {
                    // Prefer live reflection; fall back to curated names for a
                    // userdata whose `__index` is field-based (not a table).
                    let mut methods = userdata_methods(ud);
                    if methods.is_empty() {
                        methods = curated_members(target);
                    }
                    if methods.is_empty() {
                        let _ = writeln!(report, "  (no methods found)");
                    } else {
                        write_names(&mut report, "Methods", &methods, verbose);
                    }
                }
                mlua::Value::Function(_) => {
                    if doc_for(target).is_none() {
                        let _ = writeln!(report, "  (a function; no signature metadata)");
                    }
                }
                mlua::Value::String(s) => {
                    let _ = writeln!(
                        report,
                        "  Value:     {:?}",
                        s.to_str().map(|c| c.to_string()).unwrap_or_default()
                    );
                }
                other => {
                    let _ = writeln!(report, "  Value:     {other:?}");
                }
            }
        }
        // No live value: an undefined global is `nil` (not an error) in Lua, and
        // a malformed target is a syntax error — both mean "nothing to reflect".
        // Fall back to the curated doc if we carry one.
        _ => match doc_for(target) {
            Some((sig, summary)) => {
                let _ = writeln!(report, "  Signature: {sig}");
                let _ = writeln!(report, "  Summary:   {summary}");
            }
            None => {
                let _ = writeln!(report, "  (not defined)");
            }
        },
    }
    report
}

fn print_help() {
    println!(
        r#"bsrs REPL commands:
  :help              show this help
  :quit / :exit      leave the REPL
  :script <path>     load and run a Lua file
  name? / name??     introspect: type, signature, fields / methods
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
        let c = BsrsCompleter::new_static();

        // Bare prefix at BOL.
        let (start, hits) = c.candidates("cou", 3);
        assert_eq!(start, 0);
        assert!(hits.iter().any(|h| h == "count"));

        // Word start is after the `(` delimiter.
        let (start2, hits2) = c.candidates("RE:run(cou", 10);
        assert_eq!(start2, 7);
        assert!(hits2.iter().any(|h| h == "count"));

        // Namespaced token completes via the `base.member` split.
        let (_, hits3) = c.candidates("msg.op", 6);
        assert!(hits3.iter().any(|h| h == "msg.open_run"));

        // `RE:` with an empty partial lists the curated methods.
        let (_, hits4) = c.candidates("RE:", 3);
        assert!(hits4.iter().any(|h| h == "RE:run"));

        // Slash-command prefix.
        let (_, hits5) = c.candidates(":he", 3);
        assert!(hits5.iter().any(|h| h == ":help"));

        // Empty word (cursor right after a delimiter) → no candidates.
        let (_, none) = c.candidates("RE:run(", 7);
        assert!(none.is_empty());
    }

    #[test]
    fn completion_model_reflects_live_globals() {
        let lua = mlua::Lua::new();
        lua.load("det1 = 5\nmytab = { alpha = 1, beta = 2 }")
            .exec()
            .unwrap();

        let mut m = CompletionModel::with_static();
        m.add_live(&lua);
        m.finish();

        // A user-defined global becomes completable by name.
        assert!(m.candidates_for("det").iter().any(|c| c == "det1"));
        // Its table fields complete via both `.` and `:`.
        assert!(m
            .candidates_for("mytab.al")
            .iter()
            .any(|c| c == "mytab.alpha"));
        assert!(m
            .candidates_for("mytab:be")
            .iter()
            .any(|c| c == "mytab:beta"));
        // The curated static tokens survive the merge.
        assert!(m.candidates_for("RE:ru").iter().any(|c| c == "RE:run"));
        // `_`-prefixed internals (`_G`, `_VERSION`) are filtered out.
        assert!(m.candidates_for("_").is_empty());
    }

    /// The lexer must tile the input exactly: concatenating the segment texts
    /// reproduces the source (no dropped or duplicated characters).
    fn assert_covers(h: &LuaHighlighter, src: &str) -> Vec<(TokKind, String)> {
        let toks = h.lex(src);
        let joined: String = toks.iter().map(|(_, t)| t.as_str()).collect();
        assert_eq!(joined, src, "lexer must cover the whole input");
        toks
    }

    fn has(toks: &[(TokKind, String)], kind: TokKind, text: &str) -> bool {
        toks.iter().any(|(k, t)| *k == kind && t == text)
    }

    #[test]
    fn highlighter_classifies_core_tokens() {
        let h = LuaHighlighter::new();

        let toks = assert_covers(&h, "local x = 1");
        assert!(has(&toks, TokKind::Keyword, "local"));
        assert!(has(&toks, TokKind::Number, "1"));
        // A plain identifier is Text, not Keyword/Global.
        assert!(has(&toks, TokKind::Text, "x"));

        let toks = assert_covers(&h, "RE:run(count({det1}, 5))");
        assert!(has(&toks, TokKind::Global, "RE"));
        assert!(has(&toks, TokKind::Global, "count"));
        assert!(has(&toks, TokKind::Number, "5"));
        // `run` is a method name, not a bsrs global → Text.
        assert!(has(&toks, TokKind::Text, "run"));

        // Strings: short, single-quoted, and long-bracket.
        assert!(has(
            &assert_covers(&h, "\"hi\""),
            TokKind::StringLit,
            "\"hi\""
        ));
        assert!(has(
            &assert_covers(&h, "'a\\'b'"),
            TokKind::StringLit,
            "'a\\'b'"
        ));
        assert!(has(
            &assert_covers(&h, "s = [[multi]]"),
            TokKind::StringLit,
            "[[multi]]"
        ));

        // Numbers: hex + float-with-exponent.
        assert!(has(&assert_covers(&h, "x = 0xFF"), TokKind::Number, "0xFF"));
        assert!(has(
            &assert_covers(&h, "y = 1.5e-3"),
            TokKind::Number,
            "1.5e-3"
        ));

        // Comments: line + block.
        assert!(has(
            &assert_covers(&h, "-- a note"),
            TokKind::Comment,
            "-- a note"
        ));
        let toks = assert_covers(&h, "--[[block]] x");
        assert!(has(&toks, TokKind::Comment, "--[[block]]"));
        assert!(has(&toks, TokKind::Text, "x"));
    }

    #[test]
    fn highlighter_handles_unterminated_and_multiline() {
        let h = LuaHighlighter::new();
        // Unterminated long string paints to end without panicking.
        let toks = assert_covers(&h, "s = [[open");
        assert!(has(&toks, TokKind::StringLit, "[[open"));
        // A line comment stops at the newline; the next line lexes normally.
        let toks = assert_covers(&h, "-- c\nlocal y");
        assert!(has(&toks, TokKind::Comment, "-- c"));
        assert!(has(&toks, TokKind::Keyword, "local"));
    }

    #[test]
    fn parse_introspect_recognizes_both_forms() {
        assert_eq!(parse_introspect("det1?"), Some(("det1".to_string(), false)));
        assert_eq!(parse_introspect("det1??"), Some(("det1".to_string(), true)));
        assert_eq!(parse_introspect("?det1"), Some(("det1".to_string(), false)));
        assert_eq!(parse_introspect("??det1"), Some(("det1".to_string(), true)));
        // `??` wins over `?` (checked first).
        assert_eq!(parse_introspect("RE??"), Some(("RE".to_string(), true)));
        // No `?` → not an introspection request.
        assert_eq!(parse_introspect("count({det1}, 5)"), None);
        assert_eq!(parse_introspect("x = 1"), None);
    }

    #[test]
    fn curated_members_lists_re_methods() {
        let m = curated_members("RE");
        assert!(m.iter().any(|s| s == "run"));
        assert!(m.iter().any(|s| s == "pause"));
        // `msg` members come from the `msg.<verb>` tokens.
        assert!(curated_members("msg").iter().any(|s| s == "open_run"));
        // Sorted + deduped.
        let mut sorted = m.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(m, sorted);
    }

    #[test]
    fn introspect_report_reflects_runtime_value() {
        let lua = mlua::Lua::new();
        lua.load("greeting = \"hi\"\nnums = { a = 1, b = 2 }")
            .exec()
            .unwrap();

        // A live string shows its type and value.
        let r = introspect_report(&lua, "greeting", false);
        assert!(r.contains("Type:      string"));
        assert!(r.contains("\"hi\""));

        // A live table lists its fields.
        let r = introspect_report(&lua, "nums", false);
        assert!(r.contains("Type:      table"));
        assert!(r.contains("Fields"));
        assert!(r.contains('a') && r.contains('b'));

        // A curated name that is not a live global still shows its doc.
        let r = introspect_report(&lua, "count", false);
        assert!(r.contains("Signature: count("));
        assert!(r.contains("Summary:"));

        // An unknown, undefined name says so.
        let r = introspect_report(&lua, "nope_not_here", false);
        assert!(r.contains("(not defined)"));

        // Empty target → usage hint.
        assert!(introspect_report(&lua, "", false).contains("usage:"));
    }

    #[test]
    fn userdata_methods_reflect_device_and_engine() {
        // A real bsrs Lua environment so `soft_detector` / `RE` are registered.
        let sinks: Vec<Arc<dyn bsrs::engine::DocumentSink>> = Vec::new();
        let re = Arc::new(RunEngine::new(sinks));
        let lua = build_lua(re).expect("build_lua");
        lua.load("det1 = soft_detector('det1')").exec().unwrap();

        // The device userdata reflects its `add_method` surface.
        let det1: mlua::Value = lua.globals().get("det1").unwrap();
        let mlua::Value::UserData(ud) = det1 else {
            panic!("det1 should be userdata");
        };
        let methods = userdata_methods(&ud);
        for m in ["read", "trigger", "describe", "set"] {
            assert!(
                methods.iter().any(|x| x == m),
                "device method `{m}` missing from {methods:?}"
            );
        }
        // `_`-prefixed metamethods are filtered.
        assert!(!methods.iter().any(|m| m.starts_with('_')));

        // The RunEngine handle reflects too (previously curated-only).
        let re_val: mlua::Value = lua.globals().get("RE").unwrap();
        let mlua::Value::UserData(re_ud) = re_val else {
            panic!("RE should be userdata");
        };
        let re_methods = userdata_methods(&re_ud);
        for m in ["run", "pause", "resume", "state"] {
            assert!(
                re_methods.iter().any(|x| x == m),
                "engine method `{m}` missing"
            );
        }

        // …and it flows into completion: `det1:re`<Tab> → `det1:read`.
        let mut model = CompletionModel::with_static();
        model.add_live(&lua);
        model.finish();
        assert!(model
            .candidates_for("det1:re")
            .iter()
            .any(|c| c == "det1:read"));
        assert!(model
            .candidates_for("det1:tr")
            .iter()
            .any(|c| c == "det1:trigger"));

        // …and into introspection.
        let report = introspect_report(&lua, "det1", true);
        assert!(report.contains("Type:      userdata"));
        assert!(report.contains("Methods"));
        assert!(report.contains("trigger"));
    }
}
