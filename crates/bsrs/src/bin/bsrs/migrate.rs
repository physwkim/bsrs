//! `bsrs migrate` — state-dir version migration scaffolding.
//!
//! bsrs persists almost no on-disk state by default (RunStart/Stop
//! flow through Documents to Tiled / Kafka / files via sinks; the
//! engine itself is in-memory). The pieces that DO live on disk:
//!
//! - `~/.bsrs/profiles/*.toml` — saved Tiled / qs connection
//!   profiles (tiled-rs's `tiled_rs::client::profiles` mechanism).
//! - `~/.bsrs/runs.jsonl` — JsonlSink output, one Document per line.
//! - `~/.bsrs/tokens/*` — cached OAuth tokens for tiled-rs.
//!
//! When bsrs's on-disk format changes between versions, this
//! command applies a minimal migration: it scans those paths, prints
//! what it found, and (with `--apply`) rewrites them to the current
//! schema.
//!
//! Today no schema break has happened yet — the migration table is
//! empty. The CLI is here so future releases can add steps without
//! changing the user-facing entry point.

use clap::Args;
use std::path::PathBuf;

/// CLI arguments for `bsrs migrate`.
#[derive(Args, Debug)]
pub struct MigrateArgs {
    /// State directory. Defaults to `$XDG_CONFIG_HOME/bsrs` (or
    /// `~/.bsrs` if XDG is unset).
    #[arg(long)]
    pub state_dir: Option<PathBuf>,

    /// Without `--apply`, the command prints what it WOULD do (dry
    /// run). With `--apply`, it executes the migration.
    #[arg(long)]
    pub apply: bool,

    /// Target schema version. Default = current bsrs version.
    /// Migration steps required to reach this version are applied in
    /// sequence.
    #[arg(long, default_value = "current")]
    pub to: String,
}

/// Entry point. Returns process exit code.
pub fn run(args: MigrateArgs) -> i32 {
    let dir = args.state_dir.unwrap_or_else(default_state_dir);
    println!("bsrs migrate: state_dir = {}", dir.display());

    if !dir.exists() {
        println!("  (does not exist; nothing to migrate)");
        return 0;
    }

    // Inventory.
    let inventory = collect_inventory(&dir);
    if inventory.is_empty() {
        println!("  (no recognizable bsrs state files)");
        return 0;
    }
    for entry in &inventory {
        println!("  found: {}", entry.display());
    }

    // Migration steps. Today: zero steps because no schema break has
    // shipped. Add steps here as `Step { from, to, run }` records;
    // each step's `run` is a small closure taking &Path.
    let steps: Vec<Step> = vec![
        // Example placeholder — kept for the doctest of the CLI:
        // Step { from: "0.1", to: "0.2", run: Box::new(|_| Ok(())) },
    ];

    if steps.is_empty() {
        println!("\nNo migration steps required (target = {}).", args.to);
        return 0;
    }

    if args.apply {
        for step in steps {
            println!(
                "\napplying: {} → {}  (touching {} files)",
                step.from,
                step.to,
                inventory.len()
            );
            if let Err(e) = (step.run)(&dir) {
                eprintln!("  step {} → {} failed: {e}", step.from, step.to);
                return 1;
            }
        }
        println!("\nDone.");
    } else {
        println!("\n(dry run — re-run with --apply to execute)");
    }
    0
}

/// Closure that performs one migration step.
type StepFn = Box<dyn FnOnce(&std::path::Path) -> Result<(), String>>;

/// One migration step from one version to the next.
struct Step {
    from: &'static str,
    to: &'static str,
    run: StepFn,
}

fn default_state_dir() -> PathBuf {
    if let Ok(xdg) = std::env::var("XDG_CONFIG_HOME") {
        if !xdg.is_empty() {
            return PathBuf::from(xdg).join("bsrs");
        }
    }
    if let Ok(home) = std::env::var("HOME") {
        return PathBuf::from(home).join(".bsrs");
    }
    PathBuf::from(".bsrs")
}

fn collect_inventory(dir: &std::path::Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let known = ["profiles", "runs.jsonl", "tokens", "config.toml"];
    for k in known {
        let p = dir.join(k);
        if p.exists() {
            out.push(p);
        }
    }
    out
}
