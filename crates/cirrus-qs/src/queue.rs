//! Plan queue + plan history. Mirrors `bluesky_queueserver`'s
//! `PlanQueueOperations`: a FIFO queue of pending items, plus a history
//! of completed items.

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
    /// User who submitted this item (from params["user"], mirrors bluesky).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    /// User group of the submitter (from params["user_group"]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_group: Option<String>,
    /// Result of running this item (set when moved into the history).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
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
            user: None,
            user_group: None,
            result: None,
        }
    }

    /// Build an instruction item (e.g. `queue_stop`), allocating a fresh UID.
    pub fn instruction(name: impl Into<String>) -> Self {
        Self {
            item_uid: Uuid::new_v4().to_string(),
            item_type: "instruction".into(),
            name: name.into(),
            args: Value::Null,
            meta: Value::Null,
            user: None,
            user_group: None,
            result: None,
        }
    }

    /// Attach a result (used when archiving into history).
    pub fn with_result(mut self, result: Value) -> Self {
        self.result = Some(result);
        self
    }
}

/// Queue + history. Both have stable `*_uid` strings that change every
/// time the underlying VecDeque mutates.
#[derive(Default, Debug)]
pub struct PlanQueue {
    items: VecDeque<QueuedItem>,
    history: VecDeque<QueuedItem>,
    queue_uid: String,
    history_uid: String,
}

impl PlanQueue {
    /// Build empty.
    pub fn new() -> Self {
        Self {
            items: VecDeque::new(),
            history: VecDeque::new(),
            queue_uid: Uuid::new_v4().to_string(),
            history_uid: Uuid::new_v4().to_string(),
        }
    }
    fn bump_queue_uid(&mut self) {
        self.queue_uid = Uuid::new_v4().to_string();
    }
    fn bump_history_uid(&mut self) {
        self.history_uid = Uuid::new_v4().to_string();
    }
    /// Stable UID identifying the *current* queue contents.
    pub fn queue_uid(&self) -> &str {
        &self.queue_uid
    }
    /// Stable UID identifying the *current* history contents.
    pub fn history_uid(&self) -> &str {
        &self.history_uid
    }
    /// Append.
    pub fn push_back(&mut self, item: QueuedItem) {
        self.items.push_back(item);
        self.bump_queue_uid();
    }
    /// Pop the next item.
    pub fn pop_front(&mut self) -> Option<QueuedItem> {
        let it = self.items.pop_front();
        if it.is_some() {
            self.bump_queue_uid();
        }
        it
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
    /// Look up by UID.
    pub fn get_by_uid(&self, uid: &str) -> Option<&QueuedItem> {
        self.items.iter().find(|i| i.item_uid == uid)
    }
    /// Update an item by UID, preserving the UID. Returns the new item if found.
    pub fn update(&mut self, uid: &str, replacement: QueuedItem) -> Option<QueuedItem> {
        let pos = self.items.iter().position(|i| i.item_uid == uid)?;
        let mut new_item = replacement;
        new_item.item_uid = uid.to_string();
        self.items[pos] = new_item.clone();
        self.bump_queue_uid();
        Some(new_item)
    }
    /// Replace an item by UID with a fresh UID (bluesky `replace=True` behaviour).
    pub fn replace_at_uid(&mut self, uid: &str, new_item: QueuedItem) -> Option<QueuedItem> {
        let pos = self.items.iter().position(|i| i.item_uid == uid)?;
        self.items[pos] = new_item.clone();
        self.bump_queue_uid();
        Some(new_item)
    }
    /// Remove an item by UID. Returns the removed item if found.
    pub fn remove_by_uid(&mut self, uid: &str) -> Option<QueuedItem> {
        let pos = self.items.iter().position(|i| i.item_uid == uid)?;
        let r = self.items.remove(pos);
        if r.is_some() {
            self.bump_queue_uid();
        }
        r
    }
    /// Move an item identified by UID to position `dest` (0-based).
    /// Returns the moved item if found.
    pub fn move_to(&mut self, uid: &str, dest: usize) -> Option<QueuedItem> {
        let pos = self.items.iter().position(|i| i.item_uid == uid)?;
        let it = self.items.remove(pos)?;
        let dest = dest.min(self.items.len());
        self.items.insert(dest, it.clone());
        self.bump_queue_uid();
        Some(it)
    }
    /// Move item `uid` to the position immediately before `ref_uid`.
    /// Returns the moved item if both UIDs exist.
    pub fn move_before_uid(&mut self, uid: &str, ref_uid: &str) -> Option<QueuedItem> {
        let pos = self.items.iter().position(|i| i.item_uid == uid)?;
        let it = self.items.remove(pos)?;
        let ref_pos = self.items.iter().position(|i| i.item_uid == ref_uid)?;
        self.items.insert(ref_pos, it.clone());
        self.bump_queue_uid();
        Some(it)
    }
    /// Move item `uid` to the position immediately after `ref_uid`.
    /// Returns the moved item if both UIDs exist.
    pub fn move_after_uid(&mut self, uid: &str, ref_uid: &str) -> Option<QueuedItem> {
        let pos = self.items.iter().position(|i| i.item_uid == uid)?;
        let it = self.items.remove(pos)?;
        let ref_pos = self.items.iter().position(|i| i.item_uid == ref_uid)?;
        self.items.insert(ref_pos + 1, it.clone());
        self.bump_queue_uid();
        Some(it)
    }
    /// Prepend an item to the queue (insert at position 0).
    pub fn push_front(&mut self, item: QueuedItem) {
        self.items.push_front(item);
        self.bump_queue_uid();
    }
    /// Insert at 0-based index `idx`, clamped to `0..=len`.
    pub fn insert_at(&mut self, idx: usize, item: QueuedItem) {
        let idx = idx.min(self.items.len());
        self.items.insert(idx, item);
        self.bump_queue_uid();
    }
    /// Insert `item` immediately before the item whose `item_uid == ref_uid`.
    /// Returns `false` (and does not mutate) if `ref_uid` is not found.
    pub fn insert_before_uid(&mut self, ref_uid: &str, item: QueuedItem) -> bool {
        match self.items.iter().position(|i| i.item_uid == ref_uid) {
            Some(pos) => {
                self.items.insert(pos, item);
                self.bump_queue_uid();
                true
            }
            None => false,
        }
    }
    /// Insert `item` immediately after the item whose `item_uid == ref_uid`.
    /// Returns `false` (and does not mutate) if `ref_uid` is not found.
    pub fn insert_after_uid(&mut self, ref_uid: &str, item: QueuedItem) -> bool {
        match self.items.iter().position(|i| i.item_uid == ref_uid) {
            Some(pos) => {
                self.items.insert(pos + 1, item);
                self.bump_queue_uid();
                true
            }
            None => false,
        }
    }
    /// Clear all pending items.
    pub fn clear(&mut self) {
        if !self.items.is_empty() {
            self.items.clear();
            self.bump_queue_uid();
        }
    }

    // -- history -----------------------------------------------------------

    /// Snapshot the history.
    pub fn history_snapshot(&self) -> Vec<QueuedItem> {
        self.history.iter().cloned().collect()
    }
    /// History size.
    pub fn history_size(&self) -> usize {
        self.history.len()
    }
    /// Append a finished item to the history.
    pub fn push_history(&mut self, item: QueuedItem) {
        self.history.push_back(item);
        self.bump_history_uid();
    }
    /// Clear history.
    pub fn clear_history(&mut self) {
        if !self.history.is_empty() {
            self.history.clear();
            self.bump_history_uid();
        }
    }
}
