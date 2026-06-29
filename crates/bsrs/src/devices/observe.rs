//! `observe_value` / `wait_for_value` — combinators over a [`Subscription`]
//! that poll a signal until a condition holds.
//!
//! Mirrors ophyd-async `observe_value` / `wait_for_value`
//! (`core/_signal.py:380-580`). bsrs subscriptions carry JSON-erased
//! [`ReadingValue`]s, so these operate on `ReadingValue` rather than a strongly
//! typed value. As in the reference, the first item observed is the current
//! value, followed by every subsequent change (even if equal to the previous).

use crate::core::error::BsrsError;
use crate::core::reading::ReadingValue;
use crate::core::Subscription;
use futures::stream::{select_all, BoxStream};
use futures::{Stream, StreamExt};
use std::time::Duration;

/// Stream each value from `sub`, starting with the current one and then every
/// subsequent change. The returned stream owns the subscription, so dropping
/// the stream unsubscribes (releases the backend slot). The stream ends when
/// the backend closes the channel.
///
/// ```ignore
/// let mut values = Box::pin(observe_value(signal.subscribe_channel().await?));
/// while let Some(r) = values.next().await {
///     // handle each ReadingValue
/// }
/// ```
pub fn observe_value(sub: Subscription) -> impl Stream<Item = ReadingValue> {
    futures::stream::unfold((sub, true), |(mut sub, first)| async move {
        if first {
            // Yield the current value and mark it seen so the next `changed()`
            // waits for a genuinely new update.
            let v = sub.rx_mut().borrow_and_update().clone();
            Some((v, (sub, false)))
        } else {
            match sub.rx_mut().changed().await {
                Ok(()) => {
                    let v = sub.rx_mut().borrow_and_update().clone();
                    Some((v, (sub, false)))
                }
                Err(_) => None,
            }
        }
    })
}

/// Observe several subscriptions as a single merged stream, tagging each value
/// with the zero-based index of its subscription in `subs`. Equivalent to
/// running [`observe_value`] on each subscription and merging the results:
/// every subscription's current value is yielded first, then each subsequent
/// change, paired with the originating index so the caller can tell them apart.
///
/// Mirrors ophyd-async `observe_signals_value` (`core/_signal.py:432`), which
/// tags each value with the originating signal. bsrs tags by input index
/// because a [`Subscription`] carries no identity of its own. ophyd-async folds
/// `timeout`/`done_status`/`done_timeout` into the generator since a Python
/// async generator cannot compose them externally; in Rust those compose at the
/// call site (`tokio::time::timeout`, selecting the stream against a status
/// future), so they are intentionally not parameters here.
///
/// The merged stream ends once every subscription's channel has closed. An
/// empty `subs` produces an immediately-terminated stream.
pub fn observe_signals_value(
    subs: impl IntoIterator<Item = Subscription>,
) -> impl Stream<Item = (usize, ReadingValue)> {
    let streams: Vec<BoxStream<'static, (usize, ReadingValue)>> = subs
        .into_iter()
        .enumerate()
        .map(|(i, sub)| observe_value(sub).map(move |v| (i, v)).boxed())
        .collect();
    select_all(streams)
}

/// Wait until `sub`'s value satisfies `predicate`, returning the matching
/// reading. The current value is checked first, then each subsequent change.
///
/// Returns [`BsrsError::Timeout`] if `timeout` elapses before a match, or
/// [`BsrsError::Backend`] if the channel closes first. With `timeout = None`
/// it waits indefinitely.
pub async fn wait_for_value<F>(
    sub: &mut Subscription,
    mut predicate: F,
    timeout: Option<Duration>,
) -> Result<ReadingValue, BsrsError>
where
    F: FnMut(&ReadingValue) -> bool,
{
    let fut = async {
        // The current value is observed first (mirrors observe_value).
        {
            let cur = sub.rx_mut().borrow_and_update();
            if predicate(&cur) {
                return Ok(cur.clone());
            }
        }
        loop {
            sub.rx_mut()
                .changed()
                .await
                .map_err(|_| BsrsError::Backend("subscription channel closed".into()))?;
            let v = sub.rx_mut().borrow_and_update().clone();
            if predicate(&v) {
                return Ok(v);
            }
        }
    };
    match timeout {
        Some(d) => tokio::time::timeout(d, fut)
            .await
            .map_err(|_| BsrsError::Timeout(d))?,
        None => fut.await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::status::SubToken;
    use serde_json::json;
    use tokio::sync::watch;

    fn reading(v: f64) -> ReadingValue {
        ReadingValue {
            value: json!(v),
            timestamp: 0.0,
            alarm_severity: None,
            message: None,
        }
    }

    #[tokio::test]
    async fn wait_for_value_matches_current_immediately() {
        let (tx, rx) = watch::channel(reading(5.0));
        let mut sub = Subscription::new(rx, SubToken::noop());
        let got = wait_for_value(&mut sub, |r| r.value == json!(5.0), None)
            .await
            .unwrap();
        assert_eq!(got.value, json!(5.0));
        drop(tx);
    }

    #[tokio::test]
    async fn wait_for_value_waits_for_a_matching_change() {
        let (tx, rx) = watch::channel(reading(0.0));
        let mut sub = Subscription::new(rx, SubToken::noop());
        let h = tokio::spawn(async move {
            wait_for_value(
                &mut sub,
                |r| r.value == json!(3.0),
                Some(Duration::from_secs(2)),
            )
            .await
        });
        tokio::task::yield_now().await;
        tx.send(reading(1.0)).unwrap();
        tx.send(reading(3.0)).unwrap();
        let got = h.await.unwrap().unwrap();
        assert_eq!(got.value, json!(3.0));
    }

    #[tokio::test]
    async fn wait_for_value_times_out() {
        let (tx, rx) = watch::channel(reading(0.0));
        let mut sub = Subscription::new(rx, SubToken::noop());
        let r = wait_for_value(
            &mut sub,
            |r| r.value == json!(9.0),
            Some(Duration::from_millis(50)),
        )
        .await;
        assert!(matches!(r, Err(BsrsError::Timeout(_))));
        // Keep the sender alive so the failure is a timeout, not a closed channel.
        let _ = tx;
    }

    #[tokio::test]
    async fn observe_value_yields_current_then_changes() {
        use futures::StreamExt;
        let (tx, rx) = watch::channel(reading(1.0));
        let sub = Subscription::new(rx, SubToken::noop());
        let mut s = Box::pin(observe_value(sub));
        assert_eq!(s.next().await.unwrap().value, json!(1.0));
        tx.send(reading(2.0)).unwrap();
        assert_eq!(s.next().await.unwrap().value, json!(2.0));
    }

    #[tokio::test]
    async fn observe_signals_value_merges_and_tags_by_index() {
        use futures::StreamExt;
        use std::collections::HashMap;
        let (tx0, rx0) = watch::channel(reading(1.0));
        let (tx1, rx1) = watch::channel(reading(2.0));
        let s0 = Subscription::new(rx0, SubToken::noop());
        let s1 = Subscription::new(rx1, SubToken::noop());
        let mut merged = Box::pin(observe_signals_value(vec![s0, s1]));

        // Both current values arrive first, each tagged with its input index.
        let mut initial = HashMap::new();
        for _ in 0..2 {
            let (i, v) = merged.next().await.unwrap();
            initial.insert(i, v.value);
        }
        assert_eq!(initial[&0], json!(1.0));
        assert_eq!(initial[&1], json!(2.0));

        // A change on subscription 1 is reported with index 1.
        tx1.send(reading(5.0)).unwrap();
        let (i, v) = merged.next().await.unwrap();
        assert_eq!(i, 1);
        assert_eq!(v.value, json!(5.0));

        // A change on subscription 0 is reported with index 0.
        tx0.send(reading(9.0)).unwrap();
        let (i, v) = merged.next().await.unwrap();
        assert_eq!(i, 0);
        assert_eq!(v.value, json!(9.0));
    }

    #[tokio::test]
    async fn observe_signals_value_ends_when_all_channels_close() {
        use futures::StreamExt;
        let (tx0, rx0) = watch::channel(reading(1.0));
        let (tx1, rx1) = watch::channel(reading(2.0));
        let s0 = Subscription::new(rx0, SubToken::noop());
        let s1 = Subscription::new(rx1, SubToken::noop());
        let mut merged = Box::pin(observe_signals_value(vec![s0, s1]));
        // Drain the two initial values.
        merged.next().await.unwrap();
        merged.next().await.unwrap();
        drop(tx0);
        drop(tx1);
        assert!(merged.next().await.is_none());
    }

    #[tokio::test]
    async fn observe_signals_value_empty_terminates() {
        use futures::StreamExt;
        let mut merged = Box::pin(observe_signals_value(Vec::<Subscription>::new()));
        assert!(merged.next().await.is_none());
    }
}
