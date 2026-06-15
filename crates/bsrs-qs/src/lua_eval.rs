//! `LuaEvaluator` trait — abstract async hook for the `lua_eval` RPC.
//!
//! `bsrs-qs` itself doesn't depend on `mlua`. Instead it accepts a
//! caller-supplied evaluator at server-build time. The `bsrs-cli`
//! crate (which already embeds `mlua` for the local REPL) implements
//! the trait by sharing one mlua state across the daemon — see
//! `bsrs-cli::manager` for the wiring.
//!
//! Without an evaluator wired up, the `lua_eval` RPC returns
//! `NOT_IMPLEMENTED`. With one wired, the RPC spawns a task and
//! returns immediately with a `task_uid` the client polls.

use crate::tasks::EvalResult;
use async_trait::async_trait;

/// Abstract evaluator. Implementations may share state across
/// invocations (recommended) or build a fresh interpreter per call.
#[async_trait]
pub trait LuaEvaluator: Send + Sync {
    /// Evaluate `source` and return the captured outcome. This must
    /// not panic on bad input — Lua syntax / runtime errors should
    /// surface via `EvalResult.error`.
    async fn eval(&self, source: &str) -> EvalResult;
}
