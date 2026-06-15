# 06 — Rules (K1–K12)

These rules are extracted from kodex bug-pattern entries on the surrounding `epics-rs`
workspace and from the rogue thread-safety hardening sweep (PRs #1188–#1193). Every
bsrs crate must follow them; reviews check for K-rule violations explicitly.

## K1. `JoinHandle::drop` does not abort

`tokio::spawn` returns a `JoinHandle` whose `Drop` does **not** abort the task.
`let _ = tokio::spawn(...)` is a silent leak — when the parent owner drops, the
spawned task continues running, holding any captured `Arc`s and any sockets / FDs.

Origin: kodex `b11af558`; bridge-rs `12cca94e` B2-G1 (PvaServer drop).

Fix: every owner of a spawned task wraps it in an `AbortOnDrop` guard, or holds it
inside a `JoinSet` and calls `abort_all()` in `Drop`.

```rust
struct AbortOnDrop(JoinHandle<()>);
impl Drop for AbortOnDrop { fn drop(&mut self) { self.0.abort(); } }
```

## K2. Subscribe RAII

A `subscribe` that returns a token must guarantee that **dropping the token** removes
the subscriber slot from the backend. Otherwise dead `Sender`s accumulate and every
`notify` does wasted clone+try_send work.

Origin: kodex `bc8466b2` (DbSubscription Drop must remove subscriber slot).

Fix: `set_callback` returns a `SubToken` whose `Drop` runs the unsubscribe path.
Never expose raw subscriber IDs to callers.

## K3. Sharded RwLock; do I/O outside the lock

A single global `RwLock` that is held during `connect()` / `channel.get()` /
`subscribe()` serializes all concurrent operations on it. Search storms collapse to
~2 PV/s.

Origin: kodex `12cca94e` B2-G2 (UpstreamManager write-lock contention).

Fix: shard maps by hash bucket (typically 64 shards), and **release the shard lock
before** calling out to I/O.

## K4. In-flight dedup with `pending: Notify`

Double-checked locking on `get_or_open` does not dedup *concurrent first openers*.
Two callers both run the open path; one wins the DCL race, the other's resources are
wasted (extra connect round-trip + monitor task spawn).

Origin: kodex `12cca94e` B2-G3 (pvalink registry).

Fix:

```rust
pending: HashMap<Key, Arc<Notify>>,

async fn get_or_open(&self, k: Key) -> Arc<T> {
    if let Some(t) = self.shard(k).get(&k) { return t; }
    if let Some(notify) = self.pending(k).get(&k) {
        notify.notified().await;
        return self.shard(k).get(&k).unwrap();
    }
    let notify = self.pending(k).entry(k.clone()).or_insert_with(Arc::new);
    let t = self.actually_open(k).await;
    self.shard(k).insert(k.clone(), t.clone());
    notify.notify_waiters();
    t
}
```

## K5. `subscribe` must not return an immediately-closed channel

If a `subscribe` returns an `mpsc::Receiver` whose `Sender` was dropped at the end of
the `subscribe` call, `recv().await` returns `None` on the first iteration → the
server emits a MONITOR FINISH frame. Clients auto-retry, causing search storms.

Origin: kodex `12cca94e` B2-G4 (ControlSource subscribe).

Fix: hold the `Sender` in a per-PV slot for the lifetime of the subscription, OR
arrange for `subscribe` to periodically post a fresh snapshot via
`tokio::time::interval`.

## K6. Bounded channel lag must be observable

`broadcast::Sender::send` returns `Err(Lagged)` when receivers fall behind, and the
stream silently skips. `mpsc::Sender::try_send` returns `Err(Full)` and the producer
silently drops. Operators must be told.

Origin: kodex `12cca94e` B2-G7 (group monitor mpsc(64) silent drops).

Fix: every bounded channel pairs with `AtomicU64` overflow counter. The counter is
exposed (configuration meta-key in the bundler, or a meta-PV).

## K7. Reap dead Senders on subscribe, not only on event

If subscriber slot reaping happens only inside the event-emit path (`tx.try_send()
fails → remove`), a plugin with no incoming events never reaps anything. Dead
slots accumulate forever.

Origin: kodex `12cca94e` B2-G5 (NDPluginPva subscribers Vec).

Fix: every `subscribe` call also walks the existing slots and reaps dead ones (cheap
O(N) check), OR use `tokio::sync::broadcast` whose Receiver count is intrinsic.

## K8. Single CancellationToken tree

Cancellation flags scattered across modules (atomic `bool` per worker) lead to
fragility — one cancel does not propagate.

Origin: rogue update H1 (`atomic threadEn_` was applied piecemeal across ~15 modules
in the recent hardening sweep).

Fix: a single root `tokio_util::sync::CancellationToken` per RunEngine. Every child
task uses `token.child_token()`. Cancellation propagates with one call.

## K9. Spawn after commit, not during build

If a builder spawns background tasks midway and then fails on a later step, the
spawned task continues running with a half-built parent — equivalent to a
partial-construction leak.

Origin: rogue update H2 (`stream::Fifo` partial-construction guard).

Fix: builders accumulate config but do not spawn. The final `start()` /
`commit()` / `connect()` step is the only place that calls `tokio::spawn`. If any
step before fails, all owned resources Drop cleanly.

## K10. PyO3 join inside `allow_threads`

`JoinHandle::await` (or any blocking wait on a tokio task) inside a `pyo3::Python`
GIL-holding scope deadlocks when the joined task itself wants to acquire the GIL.

Origin: rogue update H4 (memory::TcpClient/Server: GIL-released join).

Fix: every long-running call from PyO3 wraps in `Python::allow_threads(|| ...)`. The
`bsrs-py` crate enforces this with a macro-generated wrapper.

## K11. ZMQ messages are RAII only

Raw `zmq_msg_t` handles crossing FFI / PyO3 boundaries leak when an exception unwinds
between `zmq_msg_init` and `zmq_msg_close`.

Origin: rogue update H4 (`ZmqClient/ZmqServer` lifecycle, ESROGUE-740 zmq_msg leaks).

Fix: bsrs uses the `tmq` or `async-zmq` crate exclusively. Owned `Message` type;
no raw handles exposed.

## K12. External I/O is the last builder step

`bind` / `listen` / `connect` should be the final step of construction, so that on
failure all earlier resources unwind via Drop. Otherwise a failed `bind` after
half-allocated thread / socket / fd resources leaks them.

Origin: rogue update H2 (`memory::TcpClient/TcpServer` ctor-throw cleanup,
`hardware::AxiMemMap` fd cleanup, `protocols::udp` ctor fd/addrinfo cleanup).

Fix: builder accumulates configuration. A final `connect(timeout)` opens the socket
and commits.

## Quick reference table

| # | Rule | Where it bites |
|---|---|---|
| K1 | `JoinHandle::drop ≠ abort` | Every `tokio::spawn` site |
| K2 | SubToken RAII | Every `subscribe` API |
| K3 | sharded lock + I/O outside | Channel registries |
| K4 | `pending: Notify` dedup | `get_or_open` patterns |
| K5 | no immediately-closed rx | Every subscribe API |
| K6 | overflow counter | Every bounded channel |
| K7 | reap on subscribe | Slot-based subscriber lists |
| K8 | single CancellationToken | All RunEngine-owned tasks |
| K9 | spawn after commit | Every builder |
| K10 | `allow_threads` | `bsrs-py` |
| K11 | RAII zmq_msg | `bsrs-backends/rogue` (Phase 2) |
| K12 | bind/connect last | Every backend constructor |
