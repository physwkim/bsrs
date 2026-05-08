//! Plan + device registry. The set of plans + devices `cirrus-qs` can run.

use cirrus_core::msg::{
    CollectableObj, FlyableObj, MovableObj, ReadableObj, StageableObj, TriggerableObj,
};
use cirrus_core::plan::Plan;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

/// Plan factory function. Builds a `Plan` from JSON arguments
/// (`{"args": [...], "kwargs": {...}}`).
pub type PlanFactory =
    Arc<dyn Fn(&Registry, &Value) -> Result<Plan, String> + Send + Sync + 'static>;

/// Registered devices, indexed by string name. The same physical device
/// usually appears under more than one trait — register each role explicitly.
#[derive(Default)]
pub struct Registry {
    plans: HashMap<String, PlanFactory>,
    readables: HashMap<String, Arc<dyn ReadableObj>>,
    movables: HashMap<String, Arc<dyn MovableObj>>,
    triggerables: HashMap<String, Arc<dyn TriggerableObj>>,
    stageables: HashMap<String, Arc<dyn StageableObj>>,
    flyables: HashMap<String, Arc<dyn FlyableObj>>,
    collectables: HashMap<String, Arc<dyn CollectableObj>>,
}

impl Registry {
    /// Build empty.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a plan factory.
    pub fn register_plan(&mut self, name: impl Into<String>, factory: PlanFactory) {
        self.plans.insert(name.into(), factory);
    }

    /// Register a `ReadableObj` device under a name.
    pub fn register_readable(&mut self, name: impl Into<String>, obj: Arc<dyn ReadableObj>) {
        self.readables.insert(name.into(), obj);
    }

    /// Register a `MovableObj` device.
    pub fn register_movable(&mut self, name: impl Into<String>, obj: Arc<dyn MovableObj>) {
        self.movables.insert(name.into(), obj);
    }

    /// Register a `TriggerableObj` device.
    pub fn register_triggerable(&mut self, name: impl Into<String>, obj: Arc<dyn TriggerableObj>) {
        self.triggerables.insert(name.into(), obj);
    }

    /// Register a `StageableObj` device.
    pub fn register_stageable(&mut self, name: impl Into<String>, obj: Arc<dyn StageableObj>) {
        self.stageables.insert(name.into(), obj);
    }

    /// Register a `FlyableObj` device.
    pub fn register_flyable(&mut self, name: impl Into<String>, obj: Arc<dyn FlyableObj>) {
        self.flyables.insert(name.into(), obj);
    }

    /// Register a `CollectableObj` device.
    pub fn register_collectable(&mut self, name: impl Into<String>, obj: Arc<dyn CollectableObj>) {
        self.collectables.insert(name.into(), obj);
    }

    /// Look up a `ReadableObj` by name.
    pub fn readable(&self, name: &str) -> Option<&Arc<dyn ReadableObj>> {
        self.readables.get(name)
    }

    /// Look up a `MovableObj` by name.
    pub fn movable(&self, name: &str) -> Option<&Arc<dyn MovableObj>> {
        self.movables.get(name)
    }

    /// Look up a registered plan factory by name.
    pub fn plan(&self, name: &str) -> Option<&PlanFactory> {
        self.plans.get(name)
    }

    /// All plan names.
    pub fn plan_names(&self) -> Vec<String> {
        let mut v: Vec<_> = self.plans.keys().cloned().collect();
        v.sort();
        v
    }

    /// All device names (union over all role registrations).
    pub fn device_names(&self) -> Vec<String> {
        let mut s = std::collections::BTreeSet::new();
        for k in self.readables.keys() {
            s.insert(k.clone());
        }
        for k in self.movables.keys() {
            s.insert(k.clone());
        }
        for k in self.triggerables.keys() {
            s.insert(k.clone());
        }
        for k in self.stageables.keys() {
            s.insert(k.clone());
        }
        for k in self.flyables.keys() {
            s.insert(k.clone());
        }
        for k in self.collectables.keys() {
            s.insert(k.clone());
        }
        s.into_iter().collect()
    }

    /// Convenience: register the canonical `count` plan (`count(detectors, num)`).
    /// Args shape: `{"args": ["det1", "det2", ...], "kwargs": {"num": 5}}`
    /// or `{"args": [["det1", "det2"], 5]}`.
    pub fn register_plan_count(&mut self, name: impl Into<String>) {
        let factory: PlanFactory = Arc::new(|reg, args| {
            let (dets_raw, num) = parse_count_args(args)?;
            let mut dets: Vec<Arc<dyn ReadableObj>> = Vec::with_capacity(dets_raw.len());
            for d in &dets_raw {
                let obj = reg
                    .readable(d)
                    .ok_or_else(|| format!("unknown detector: {d}"))?
                    .clone();
                dets.push(obj);
            }
            Ok(cirrus_plans::count(dets, num))
        });
        self.register_plan(name, factory);
    }
}

fn parse_count_args(args: &Value) -> Result<(Vec<String>, usize), String> {
    // Accept both kwargs- and positional-style.
    if let Some(obj) = args.as_object() {
        let detectors = obj
            .get("args")
            .and_then(|a| a.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect::<Vec<_>>()
            })
            .or_else(|| {
                obj.get("kwargs")
                    .and_then(|k| k.get("detectors"))
                    .and_then(|d| d.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().map(|s| s.to_string()))
                            .collect::<Vec<_>>()
                    })
            })
            .unwrap_or_default();
        let num = obj
            .get("kwargs")
            .and_then(|k| k.get("num"))
            .and_then(|n| n.as_u64())
            .or_else(|| {
                obj.get("args")
                    .and_then(|a| a.as_array())
                    .and_then(|a| a.last())
                    .and_then(|v| v.as_u64())
            })
            .ok_or_else(|| "count: missing 'num' argument".to_string())? as usize;
        Ok((detectors, num))
    } else {
        Err(format!("count args must be object; got {args}"))
    }
}
