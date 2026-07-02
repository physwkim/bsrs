//! RBAC for the JSON-RPC dispatcher.
//!
//! bsrs-qs ships with an opt-in role-based access control layer. By
//! default the server runs *permissive* (any caller is treated as group
//! `primary`, and `primary` allows everything). When the server is
//! built with [`ServerBuilder::permissions_path`](crate::qs::ServerBuilder),
//! a TOML file is loaded at startup and consulted on every RPC.
//!
//! ## File shape
//!
//! ```toml
//! default_group = "primary"
//!
//! [user_groups.primary]
//! allowed_plans   = [".*"]
//! allowed_devices = [".*"]
//!
//! [user_groups.viewer]
//! read_only       = true
//! allowed_plans   = []
//! allowed_devices = []
//!
//! [user_groups.admin]
//! admin           = true
//! allowed_plans   = [".*"]
//! allowed_devices = [".*"]
//!
//! [api_keys]
//! "k-primary-abc" = "primary"
//! "k-viewer-xyz"  = "viewer"
//! "k-admin-001"   = "admin"
//! ```
//!
//! Callers identify themselves by adding `"api_key": "k-..."` to the
//! JSON-RPC `params`. Without an api_key, the request is treated as
//! `default_group`.
//!
//! ## Method classes
//!
//! Methods are bucketed into [`MethodClass`]:
//! - [`MethodClass::Info`] — `ping`, `status`, `*_get`, `*_existing`,
//!   `*_allowed`, `lock_info`, `task_*`, `permissions_get`,
//!   `manager_test`, `manager_version`. Always allowed.
//! - [`MethodClass::QueueAdd`] — `queue_item_add` / `queue_item_add_batch`
//!   / `queue_item_execute`. Validate the plan name against the
//!   group's `allowed_plans` regex set.
//! - [`MethodClass::QueueMutate`] — other queue ops, environment
//!   ops, RE control. Denied for `read_only` groups.
//! - [`MethodClass::Admin`] — `permissions_*`, `manager_stop`,
//!   `manager_kill`, `script_upload`, `function_execute`,
//!   `kernel_interrupt`. Allowed only for groups with `admin = true`.
//! - [`MethodClass::Lock`] — `lock`, `unlock`. Always allowed (the
//!   lock subsystem has its own key check).

use regex::Regex;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::RwLock;

/// Bucket a method falls into for the purposes of permission checks.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum MethodClass {
    /// Read-only, harmless to all groups.
    Info,
    /// Adds a plan to the queue / executes a one-off plan; the plan
    /// name in `params` must match the group's `allowed_plans`.
    QueueAdd,
    /// Mutates queue / environment / RE state. Blocked for read-only
    /// groups.
    QueueMutate,
    /// Manager-level operations (kill, reload permissions, ...).
    /// Allowed only for `admin = true` groups.
    Admin,
    /// `lock` / `unlock` — uses its own key check, RBAC just allows it.
    Lock,
    /// Method not registered for RBAC; the dispatcher will return
    /// `METHOD_NOT_FOUND`.
    Unknown,
}

/// Classify a method name. Mirrors the dispatch table in `dispatch.rs`.
pub fn classify(method: &str) -> MethodClass {
    match method {
        // Read-only / informational.
        "ping" | "status" | "config_get" | "queue_get" | "history_get" | "plans_allowed"
        | "plans_existing" | "devices_allowed" | "devices_existing" | "device_inspect"
        | "lock_info" | "task_status" | "task_result" | "permissions_get" | "manager_test"
        | "manager_version" | "re_runs" => MethodClass::Info,

        // Queue add — plan-name-checked.
        "queue_item_add" | "queue_item_add_batch" | "queue_item_execute" | "queue_item_update" => {
            MethodClass::QueueAdd
        }

        // State-mutating.
        "queue_item_remove"
        | "queue_item_remove_batch"
        | "queue_item_move"
        | "queue_item_move_batch"
        | "queue_clear"
        | "queue_start"
        | "queue_stop"
        | "queue_stop_cancel"
        | "queue_autostart"
        | "queue_mode_set"
        | "history_clear"
        | "environment_open"
        | "environment_close"
        | "environment_destroy"
        | "environment_update"
        | "re_pause"
        | "re_resume"
        | "re_abort"
        | "re_halt"
        | "re_stop"
        | "re_metadata"
        | "re_runs_clear" => MethodClass::QueueMutate,

        // Admin-only.
        "permissions_reload" | "permissions_set" | "manager_stop" | "manager_kill"
        | "script_upload" | "function_execute" | "kernel_interrupt" | "lua_eval" => {
            MethodClass::Admin
        }

        // Lock subsystem — has its own auth.
        "lock" | "unlock" => MethodClass::Lock,

        _ => MethodClass::Unknown,
    }
}

// ---------------------------------------------------------------------------

/// On-disk permissions file (TOML).
#[derive(Debug, Deserialize, Default)]
struct File {
    #[serde(default)]
    default_group: Option<String>,
    #[serde(default)]
    user_groups: HashMap<String, GroupFile>,
    #[serde(default)]
    api_keys: HashMap<String, String>,
}

#[derive(Debug, Deserialize, Default, Clone)]
struct GroupFile {
    #[serde(default)]
    read_only: bool,
    #[serde(default)]
    admin: bool,
    #[serde(default)]
    allowed_plans: Vec<String>,
    #[serde(default)]
    allowed_devices: Vec<String>,
}

/// Resolved per-group policy with compiled regexes.
#[derive(Debug, Clone)]
pub struct GroupPolicy {
    /// `read_only` groups can call `Info` and `Lock` methods only.
    pub read_only: bool,
    /// `admin` groups may call `Admin` methods.
    pub admin: bool,
    /// Regexes; a plan name passes if any matches.
    pub allowed_plans: Vec<Regex>,
    /// Regexes; a device name passes if any matches.
    pub allowed_devices: Vec<Regex>,
}

impl GroupPolicy {
    fn permissive_root() -> Self {
        Self {
            read_only: false,
            admin: true,
            allowed_plans: vec![Regex::new(".*").unwrap()],
            allowed_devices: vec![Regex::new(".*").unwrap()],
        }
    }
    fn permissive_primary() -> Self {
        Self {
            read_only: false,
            admin: false,
            allowed_plans: vec![Regex::new(".*").unwrap()],
            allowed_devices: vec![Regex::new(".*").unwrap()],
        }
    }
    fn from_file(g: &GroupFile) -> Result<Self, String> {
        let to_re = |xs: &[String]| -> Result<Vec<Regex>, String> {
            xs.iter()
                .map(|s| Regex::new(s).map_err(|e| format!("regex {s:?}: {e}")))
                .collect()
        };
        Ok(Self {
            read_only: g.read_only,
            admin: g.admin,
            allowed_plans: to_re(&g.allowed_plans)?,
            allowed_devices: to_re(&g.allowed_devices)?,
        })
    }
    fn plan_allowed(&self, name: &str) -> bool {
        self.allowed_plans.iter().any(|r| r.is_match(name))
    }
}

/// Live RBAC state. Hot-reloadable via [`Permissions::reload`].
pub struct Permissions {
    inner: RwLock<Inner>,
}

struct Inner {
    /// `false` for the no-file permissive default — all methods are
    /// allowed for every caller regardless of group classification.
    /// `true` once a permissions.toml has been loaded.
    enforced: bool,
    default_group: String,
    groups: HashMap<String, GroupPolicy>,
    api_keys: HashMap<String, String>,
    /// Source path (for `permissions_reload`). `None` for in-memory permissive.
    path: Option<PathBuf>,
}

impl Permissions {
    /// Build a permissive default: no api_keys, two preconfigured
    /// groups (`root`, `primary`) both allow-everything, and
    /// enforcement disabled. Used when no `--permissions` file
    /// is configured — preserves the pre-RBAC behavior where every
    /// method is allowed.
    pub fn permissive() -> Self {
        let mut groups = HashMap::new();
        groups.insert("root".to_string(), GroupPolicy::permissive_root());
        groups.insert("primary".to_string(), GroupPolicy::permissive_primary());
        Self {
            inner: RwLock::new(Inner {
                enforced: false,
                default_group: "primary".into(),
                groups,
                api_keys: HashMap::new(),
                path: None,
            }),
        }
    }

    /// Load a permissions file from disk. Returns the loaded Permissions
    /// or a stringified error.
    pub fn load_from_file(path: impl AsRef<Path>) -> Result<Self, String> {
        let p = path.as_ref().to_path_buf();
        let text = std::fs::read_to_string(&p).map_err(|e| format!("read {}: {e}", p.display()))?;
        let file: File =
            toml::from_str(&text).map_err(|e| format!("parse {}: {e}", p.display()))?;
        let default_group = file
            .default_group
            .clone()
            .unwrap_or_else(|| "primary".into());
        let mut groups: HashMap<String, GroupPolicy> = HashMap::new();
        for (name, g) in &file.user_groups {
            groups.insert(name.clone(), GroupPolicy::from_file(g)?);
        }
        if !groups.contains_key(&default_group) {
            return Err(format!(
                "default_group {default_group:?} has no matching [user_groups.{default_group}] section"
            ));
        }
        Ok(Self {
            inner: RwLock::new(Inner {
                enforced: true,
                default_group,
                groups,
                api_keys: file.api_keys,
                path: Some(p),
            }),
        })
    }

    /// Re-read the configured file. Errors leave the existing state in
    /// place. Returns `Err("no source file")` for in-memory permissive.
    pub fn reload(&self) -> Result<(), String> {
        let path = {
            let g = self.inner.read().unwrap();
            g.path.clone().ok_or("no source file")?
        };
        let new = Self::load_from_file(&path)?;
        let new_inner = new.inner.into_inner().unwrap();
        let mut g = self.inner.write().unwrap();
        *g = new_inner;
        Ok(())
    }

    /// Determine the caller's group from `params`. Looks for `api_key`
    /// (string) and consults the configured map; falls back to
    /// `default_group`.
    pub fn resolve_group(&self, params: &Value) -> String {
        let g = self.inner.read().unwrap();
        if let Some(key) = params.get("api_key").and_then(|v| v.as_str()) {
            if let Some(group) = g.api_keys.get(key) {
                return group.clone();
            }
        }
        g.default_group.clone()
    }

    /// Authorize `method` for `group`. On `QueueAdd`, also validate
    /// **every** plan name in `params["item"]["name"]` /
    /// `params["items"][].name` (queueserver convention) — denying if
    /// *any* item is outside the group's `allowed_plans`.
    /// Returns `Ok(())` on success, `Err(reason)` on denial.
    ///
    /// When `Permissions` was built via [`Self::permissive`] (no file
    /// configured), `check()` always returns `Ok` — the dispatcher
    /// behaves as it did before RBAC.
    pub fn check(&self, method: &str, params: &Value, group: &str) -> Result<(), String> {
        let g = self.inner.read().unwrap();
        if !g.enforced {
            return Ok(());
        }
        let policy = g.groups.get(group).ok_or_else(|| {
            format!("RBAC: caller's group {group:?} is not configured in permissions.toml")
        })?;
        match classify(method) {
            MethodClass::Info | MethodClass::Lock => Ok(()),
            MethodClass::Unknown => Ok(()),
            MethodClass::QueueAdd => {
                if policy.read_only {
                    return Err(format!(
                        "RBAC: group {group:?} is read-only; '{method}' denied"
                    ));
                }
                for name in plan_names_from_params(params) {
                    if !policy.plan_allowed(&name) {
                        return Err(format!(
                            "RBAC: group {group:?} not allowed to run plan {name:?}"
                        ));
                    }
                }
                Ok(())
            }
            MethodClass::QueueMutate => {
                if policy.read_only {
                    Err(format!(
                        "RBAC: group {group:?} is read-only; '{method}' denied"
                    ))
                } else {
                    Ok(())
                }
            }
            MethodClass::Admin => {
                if policy.admin {
                    Ok(())
                } else {
                    Err(format!(
                        "RBAC: group {group:?} is not admin; '{method}' denied"
                    ))
                }
            }
        }
    }

    /// Filter `plan_names` to those allowed for `group`.
    /// Returns `plan_names` unchanged when RBAC is not enforced or the group
    /// is not found (tolerates absent / unknown user_group).
    pub fn filter_plans_for_group<'a>(
        &self,
        group: &str,
        plan_names: &'a [String],
    ) -> Vec<&'a String> {
        let g = self.inner.read().unwrap();
        if !g.enforced {
            return plan_names.iter().collect();
        }
        match g.groups.get(group) {
            Some(policy) => plan_names
                .iter()
                .filter(|n| policy.plan_allowed(n))
                .collect(),
            None => plan_names.iter().collect(), // unknown group → return all
        }
    }

    /// True if `group` has `admin = true`. Used by the dispatcher to
    /// gate per-task lookups (admin-originated task results stay
    /// readable only by other admins).
    pub fn is_admin(&self, group: &str) -> bool {
        let g = self.inner.read().unwrap();
        if !g.enforced {
            return true;
        }
        g.groups.get(group).map(|p| p.admin).unwrap_or(false)
    }

    /// Snapshot the current policy for `permissions_get`. Returns a
    /// JSON object that mirrors the bluesky-queueserver
    /// `user_group_permissions` shape:
    ///
    /// ```json
    /// {"user_groups": {"primary": {"allowed_plans": [...], "allowed_devices": [...]}}}
    /// ```
    pub fn snapshot_for_get(&self) -> serde_json::Value {
        use serde_json::json;
        let g = self.inner.read().unwrap();
        let mut user_groups = serde_json::Map::new();
        for (name, p) in &g.groups {
            user_groups.insert(
                name.clone(),
                json!({
                    "allowed_plans": p.allowed_plans.iter().map(|r| r.as_str()).collect::<Vec<_>>(),
                    "allowed_devices": p.allowed_devices.iter().map(|r| r.as_str()).collect::<Vec<_>>(),
                    "read_only": p.read_only,
                    "admin": p.admin,
                }),
            );
        }
        json!({ "user_groups": user_groups })
    }
}

/// Extract every plan name from a queueserver-shape `params`:
/// `params.item.name` (single-add) or every entry of
/// `params.items[].name` (batch-add). Returning all names lets the
/// caller reject when *any* item is outside the policy — fixes a
/// batch-add bypass where only the first plan name was checked.
fn plan_names_from_params(params: &Value) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(name) = params
        .get("item")
        .and_then(|v| v.get("name"))
        .and_then(|v| v.as_str())
    {
        out.push(name.to_string());
    }
    if let Some(arr) = params.get("items").and_then(|v| v.as_array()) {
        for it in arr {
            if let Some(name) = it.get("name").and_then(|v| v.as_str()) {
                out.push(name.to_string());
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn permissive_default_allows_everything() {
        // No file configured → enforcement disabled → every method
        // succeeds for every group, including admin-class methods
        // for non-admin groups.
        let p = Permissions::permissive();
        let pp = json!({});
        assert!(p.check("queue_item_add", &pp, "primary").is_ok());
        assert!(p.check("re_pause", &pp, "primary").is_ok());
        assert!(p.check("manager_kill", &pp, "root").is_ok());
        assert!(p.check("manager_kill", &pp, "primary").is_ok());
    }

    #[test]
    fn read_only_blocks_mutation() {
        let toml = r#"
            default_group = "viewer"
            [user_groups.viewer]
            read_only = true
            [user_groups.primary]
            allowed_plans = [".*"]
            allowed_devices = [".*"]
        "#;
        let path = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(path.path(), toml).unwrap();
        let p = Permissions::load_from_file(path.path()).unwrap();
        assert_eq!(p.resolve_group(&json!({})), "viewer");
        assert!(p.check("status", &json!({}), "viewer").is_ok());
        assert!(p.check("queue_item_add", &json!({}), "viewer").is_err());
        assert!(p.check("re_pause", &json!({}), "viewer").is_err());
    }

    #[test]
    fn plan_regex_filter() {
        let toml = r#"
            default_group = "scientist"
            [user_groups.scientist]
            allowed_plans = ["count", "scan.*"]
            allowed_devices = [".*"]
        "#;
        let path = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(path.path(), toml).unwrap();
        let p = Permissions::load_from_file(path.path()).unwrap();
        let g = "scientist";
        assert!(p
            .check("queue_item_add", &json!({"item": {"name": "count"}}), g)
            .is_ok());
        assert!(p
            .check("queue_item_add", &json!({"item": {"name": "scan_grid"}}), g)
            .is_ok());
        assert!(p
            .check("queue_item_add", &json!({"item": {"name": "fly"}}), g)
            .is_err());
    }

    #[test]
    fn api_key_resolves_group() {
        let toml = r#"
            default_group = "primary"
            [user_groups.primary]
            allowed_plans = [".*"]
            [user_groups.viewer]
            read_only = true
            [api_keys]
            "k-view" = "viewer"
        "#;
        let path = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(path.path(), toml).unwrap();
        let p = Permissions::load_from_file(path.path()).unwrap();
        assert_eq!(p.resolve_group(&json!({"api_key": "k-view"})), "viewer");
        assert_eq!(p.resolve_group(&json!({})), "primary");
        assert_eq!(p.resolve_group(&json!({"api_key": "unknown"})), "primary");
    }

    #[test]
    fn batch_add_denies_when_any_item_disallowed() {
        // R1.1: a malicious caller could pass [{name: "count"}, {name:
        // "fly"}] and the prior single-name check would let it through.
        // Now ALL items are validated.
        let toml = r#"
            default_group = "scientist"
            [user_groups.scientist]
            allowed_plans = ["count"]
            allowed_devices = [".*"]
        "#;
        let path = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(path.path(), toml).unwrap();
        let p = Permissions::load_from_file(path.path()).unwrap();

        // Single allowed name → ok.
        assert!(p
            .check(
                "queue_item_add_batch",
                &json!({"items": [{"name": "count"}]}),
                "scientist"
            )
            .is_ok());

        // Two items, second disallowed → must deny.
        let r = p.check(
            "queue_item_add_batch",
            &json!({"items": [{"name": "count"}, {"name": "fly"}]}),
            "scientist",
        );
        assert!(r.is_err(), "expected deny, got {r:?}");
        assert!(
            r.unwrap_err().contains("fly"),
            "error must name the offending plan"
        );
    }

    #[test]
    fn is_admin_reflects_group_flag() {
        let toml = r#"
            default_group = "primary"
            [user_groups.primary]
            allowed_plans = [".*"]
            [user_groups.boss]
            admin = true
            allowed_plans = [".*"]
        "#;
        let path = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(path.path(), toml).unwrap();
        let p = Permissions::load_from_file(path.path()).unwrap();
        assert!(!p.is_admin("primary"));
        assert!(p.is_admin("boss"));
        // Permissive default treats everyone as admin (enforcement off).
        assert!(Permissions::permissive().is_admin("anyone"));
    }

    #[test]
    fn classify_buckets() {
        assert_eq!(classify("ping"), MethodClass::Info);
        assert_eq!(classify("status"), MethodClass::Info);
        assert_eq!(classify("queue_item_add"), MethodClass::QueueAdd);
        assert_eq!(classify("queue_clear"), MethodClass::QueueMutate);
        assert_eq!(classify("re_pause"), MethodClass::QueueMutate);
        assert_eq!(classify("manager_kill"), MethodClass::Admin);
        assert_eq!(classify("lock"), MethodClass::Lock);
        assert_eq!(classify("does_not_exist"), MethodClass::Unknown);
    }

    #[test]
    fn snapshot_shape() {
        let p = Permissions::permissive();
        let snap = p.snapshot_for_get();
        assert!(snap.get("user_groups").is_some());
        assert!(snap["user_groups"]["primary"]["allowed_plans"].is_array());
    }
}
