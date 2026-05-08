//! Plan queue — FIFO of [`QueuedItem`].

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::VecDeque;
use uuid::Uuid;

/// One queued plan item — name + JSON args, with an item UID.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct QueuedItem {
    /// Stable item identifier (UUID v4).
    pub item_uid: String,
    /// Discriminator — `"plan"` for plans (the only kind we support).
    pub item_type: String,
    /// Plan / function name.
    pub name: String,
    /// Positional or keyword arguments — typically `{"args": [...], "kwargs": {...}}`.
    pub args: Value,
    /// Free-form metadata attached by the submitter.
    #[serde(default)]
    pub meta: Value,
}

impl QueuedItem {
    /// Build a plan item, allocating a fresh UID.
    pub fn plan(name: impl Into<String>, args: Value) -> Self {
        Self {
            item_uid: Uuid::new_v4().to_string(),
            item_type: "plan".into(),
            name: name.into(),
            args,
            meta: Value::Null,
        }
    }
}

/// Plan queue.
#[derive(Default, Debug)]
pub struct PlanQueue {
    items: VecDeque<QueuedItem>,
}

impl PlanQueue {
    /// Build empty.
    pub fn new() -> Self {
        Self::default()
    }
    /// Append.
    pub fn push_back(&mut self, item: QueuedItem) {
        self.items.push_back(item)
    }
    /// Pop the next item.
    pub fn pop_front(&mut self) -> Option<QueuedItem> {
        self.items.pop_front()
    }
    /// Length.
    pub fn len(&self) -> usize {
        self.items.len()
    }
    /// Empty?
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
    /// Snapshot the queue.
    pub fn snapshot(&self) -> Vec<QueuedItem> {
        self.items.iter().cloned().collect()
    }
    /// Remove an item by UID. Returns the removed item if found.
    pub fn remove_by_uid(&mut self, uid: &str) -> Option<QueuedItem> {
        let pos = self.items.iter().position(|i| i.item_uid == uid)?;
        self.items.remove(pos)
    }
    /// Clear all pending items.
    pub fn clear(&mut self) {
        self.items.clear();
    }
}
