//! `observe_value` / `wait_for_value` — combinators over a [`Subscription`]
//! that poll a signal until a condition holds.
//!
//! Mirrors ophyd-async `observe_value` / `wait_for_value`
//! (`core/_signal.py:380-580`). cirrus subscriptions carry JSON-erased
//! [`ReadingValue`]s, so these operate on `ReadingValue` rather than a strongly
//! typed value. As in the reference, the first item observed is the current
//! value, followed by every subsequent change (even if equal to the previous).

use cirrus_core::error::CirrusError;
use cirrus_core::reading::ReadingValue;
use cirrus_core::Subscription;
use futures::Stream;
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

/// Wait until `sub`'s value satisfies `predicate`, returning the matching
/// reading. The current value is checked first, then each subsequent change.
///
/// Returns [`CirrusError::Timeout`] if `timeout` elapses before a match, or
/// [`CirrusError::Backend`] if the channel closes first. With `timeout = None`
/// it waits indefinitely.
pub async fn wait_for_value<F>(
    sub: &mut Subscription,
    mut predicate: F,
    timeout: Option<Duration>,
) -> Result<ReadingValue, CirrusError>
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
                .map_err(|_| CirrusError::Backend("subscription channel closed".into()))?;
            let v = sub.rx_mut().borrow_and_update().clone();
            if predicate(&v) {
                return Ok(v);
            }
        }
    };
    match timeout {
        Some(d) => tokio::time::timeout(d, fut)
            .await
            .map_err(|_| CirrusError::Timeout(d))?,
        None => fut.await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cirrus_core::status::SubToken;
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
        assert!(matches!(r, Err(CirrusError::Timeout(_))));
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
}
