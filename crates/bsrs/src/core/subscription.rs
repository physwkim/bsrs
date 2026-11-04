//! `Subscription` — RAII bundle of one listener queue of readings, the data
//! key those readings belong to, and the `SubToken` that keeps the backend
//! slot alive (rule **K2**). [`Fanout`] is the producer side that feeds the
//! queues.

use std::sync::Mutex;

use crate::core::reading::ReadingValue;
use crate::core::status::SubToken;
use tokio::sync::mpsc;

/// One producer's values fanned out to per-listener FIFO queues, as
/// ophyd-async `observe_signals_value` queues each callback on an
/// `asyncio.Queue`. A listener receives the latest value at registration (if
/// any), then every later [`post`](Self::post) in order, exactly once: a
/// transient (`0 → 1 → 0`) is never coalesced away before a slow reader
/// looks.
pub struct Fanout<T> {
    inner: Mutex<FanoutState<T>>,
}

struct FanoutState<T> {
    latest: Option<T>,
    listeners: Vec<mpsc::UnboundedSender<T>>,
}

impl<T> Default for Fanout<T> {
    fn default() -> Self {
        Self {
            inner: Mutex::new(FanoutState {
                latest: None,
                listeners: Vec::new(),
            }),
        }
    }
}

impl<T: Clone> Fanout<T> {
    /// Queue `v` to every live listener and make it the latest value.
    /// Returns the value it replaces.
    pub fn post(&self, v: T) -> Option<T> {
        let mut st = self.inner.lock().unwrap();
        st.listeners.retain(|tx| tx.send(v.clone()).is_ok());
        st.latest.replace(v)
    }

    /// Register a listener, starting with the latest value queued when there
    /// is one.
    pub fn listen(&self) -> mpsc::UnboundedReceiver<T> {
        let (tx, rx) = mpsc::unbounded_channel();
        let mut st = self.inner.lock().unwrap();
        st.listeners.retain(|tx| !tx.is_closed());
        if let Some(v) = &st.latest {
            let _ = tx.send(v.clone());
        }
        st.listeners.push(tx);
        rx
    }

    /// The latest posted value, unless none was posted since the last
    /// [`clear`](Self::clear).
    pub fn latest(&self) -> Option<T> {
        self.inner.lock().unwrap().latest.clone()
    }

    /// Forget the latest value; a listener registered next starts empty.
    pub fn clear(&self) {
        self.inner.lock().unwrap().latest = None;
    }

    /// Number of listeners whose receiver is still alive.
    pub fn listener_count(&self) -> usize {
        let st = self.inner.lock().unwrap();
        st.listeners.iter().filter(|tx| !tx.is_closed()).count()
    }
}

/// Live subscription to one signal. Drop releases the backend slot.
///
/// `key` names the reading the way the device's `describe()` / `read()` do
/// (ophyd-async `subscribe_reading` delivers `{signal.name: reading}`), so a
/// consumer that turns readings into Events needs no mapping of its own.
pub struct Subscription {
    rx: mpsc::UnboundedReceiver<ReadingValue>,
    key: String,
    _token: SubToken,
}

impl Subscription {
    /// Build from parts. `rx` is this listener's own queue (one
    /// [`Fanout::listen`]); the `token` is held until `self` is dropped.
    pub fn new(
        rx: mpsc::UnboundedReceiver<ReadingValue>,
        token: SubToken,
        key: impl Into<String>,
    ) -> Self {
        Self {
            rx,
            key: key.into(),
            _token: token,
        }
    }
    /// Data key of the readings this subscription streams.
    pub fn key(&self) -> &str {
        &self.key
    }
    /// Next queued reading, waiting for one; `None` once the producer is
    /// gone and the queue is drained.
    pub async fn recv(&mut self) -> Option<ReadingValue> {
        self.rx.recv().await
    }
    /// Next queued reading if one is waiting.
    pub fn try_recv(&mut self) -> Option<ReadingValue> {
        self.rx.try_recv().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Boundary: a listener registered before any post starts empty; one
    // registered after starts with the latest value, then sees later posts.
    #[test]
    fn listen_starts_with_the_latest_value_only_when_there_is_one() {
        let f = Fanout::<i32>::default();
        let mut early = f.listen();
        assert!(early.try_recv().is_err(), "nothing posted yet");

        assert_eq!(f.post(1), None);
        let mut late = f.listen();
        assert_eq!(f.post(2), Some(1));
        assert_eq!(early.try_recv().unwrap(), 1);
        assert_eq!(early.try_recv().unwrap(), 2);
        assert_eq!(late.try_recv().unwrap(), 1, "latest at registration");
        assert_eq!(late.try_recv().unwrap(), 2);
        assert!(late.try_recv().is_err());
    }

    // A pulse posted back to back reaches a listener that has not polled
    // in between: nothing is coalesced.
    #[test]
    fn back_to_back_posts_all_queue() {
        let f = Fanout::<i32>::default();
        let mut rx = f.listen();
        f.post(0);
        f.post(1);
        f.post(0);
        let got: Vec<i32> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
        assert_eq!(got, vec![0, 1, 0]);
    }

    #[test]
    fn clear_forgets_the_latest_and_dropped_listeners_are_pruned() {
        let f = Fanout::<i32>::default();
        let rx = f.listen();
        f.post(3);
        assert_eq!(f.listener_count(), 1);
        drop(rx);
        assert_eq!(f.listener_count(), 0);
        f.clear();
        assert_eq!(f.latest(), None);
        assert!(
            f.listen().try_recv().is_err(),
            "nothing replayed after clear"
        );
    }
}
