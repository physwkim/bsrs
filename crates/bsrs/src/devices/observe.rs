//! `observe_value` / `wait_for_value` — combinators over a [`Subscription`]
//! that poll a signal until a condition holds.
//!
//! Mirrors ophyd-async `observe_value` / `wait_for_value`
//! (`core/_signal.py:380-580`). bsrs subscriptions carry JSON-erased
//! [`ReadingValue`]s, so these operate on `ReadingValue` rather than a strongly
//! typed value. As in the reference, the first item observed is the current
//! value, followed by every subsequent update in order (even if equal to the
//! previous): each subscription queues them, so none is skipped.

use crate::core::error::BsrsError;
use crate::core::reading::ReadingValue;
use crate::core::Subscription;
use crate::devices::signal::repr;
use futures::stream::{select_all, BoxStream};
use futures::{Stream, StreamExt};
use serde_json::Value;
use std::time::Duration;

/// Stream each value from `sub`, starting with the current one and then every
/// subsequent update. The returned stream owns the subscription, so dropping
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
    futures::stream::unfold(sub, |mut sub| async move {
        let v = sub.recv().await?;
        Some((v, sub))
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

/// What [`wait_for_value`] waits for, as ophyd-async's `match`: a value the
/// reading equals as Python `==` has it (`1` matches `1.0`), or a predicate
/// over the reading. A closure needs its argument typed,
/// `|r: &ReadingValue| ...`, as this bound does not infer it.
pub trait Matcher {
    /// Whether `r` matches.
    fn matches(&mut self, r: &ReadingValue) -> bool;
    /// How a timeout names the matcher: a value's `repr`, a predicate's fn
    /// name, or `<closure>`.
    fn name(&self) -> String;
}

impl<F: FnMut(&ReadingValue) -> bool> Matcher for F {
    fn matches(&mut self, r: &ReadingValue) -> bool {
        self(r)
    }
    fn name(&self) -> String {
        matcher_name::<F>()
    }
}

macro_rules! value_matcher {
    ($($t:ty),*) => {$(
        impl Matcher for $t {
            fn matches(&mut self, r: &ReadingValue) -> bool {
                serde_json::to_value(&*self).is_ok_and(|v| py_eq(&r.value, &v))
            }
            fn name(&self) -> String {
                repr(self)
            }
        }
    )*};
}

value_matcher!(bool, i8, i16, i32, i64, u8, u16, u32, u64, f32, f64, String, &str, Value);

/// Python `==` over JSON: numbers by value, containers item by item.
fn py_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) if x.is_f64() || y.is_f64() => {
            x.as_f64() == y.as_f64()
        }
        (Value::Number(x), Value::Number(y)) => {
            x.as_i64() == y.as_i64() && x.as_u64() == y.as_u64()
        }
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(x, y)| py_eq(x, y))
        }
        (Value::Object(x), Value::Object(y)) => {
            x.len() == y.len() && x.iter().all(|(k, v)| y.get(k).is_some_and(|w| py_eq(v, w)))
        }
        _ => a == b,
    }
}

/// Wait until `sub`'s value matches `matcher`, returning the matching
/// reading. The current value is checked first, then each subsequent update.
/// `sub` is consumed, as ophyd-async subscribes afresh for every wait: a
/// reused one would start from whatever it queued before.
///
/// Returns [`BsrsError::ValueTimeout`] if `timeout` elapses before a match,
/// carrying the last value seen (`None` if none arrived) and the
/// [`Matcher::name`] as ophyd-async's `TimeoutError` does, or
/// [`BsrsError::Backend`] if the channel closes first. With
/// `timeout = None` it waits indefinitely.
pub async fn wait_for_value(
    mut sub: Subscription,
    mut matcher: impl Matcher,
    timeout: Option<Duration>,
) -> Result<ReadingValue, BsrsError> {
    // The timer covers the first value too (ophyd-async wraps the whole
    // `observe_value` in `asyncio.timeout`).
    let mut last = None;
    let fut = next_match(&mut sub, &mut matcher, &mut last);
    match timeout {
        Some(d) => match tokio::time::timeout(d, fut).await {
            Ok(r) => r,
            Err(_) => Err(value_timeout(&sub, &matcher, d, last)),
        },
        None => fut.await,
    }
}

/// Wait for `sub`'s first value, run `set`, then wait until a value
/// matches `matcher`, all within `timeout`; returns what `set` returned.
/// Port of ophyd-async `set_and_wait_for_other_value(...,
/// wait_for_set_completion=False)`: the first value is in hand before `set`
/// runs, so a change `set` causes, even one already undone, is queued
/// behind it. With no first value it fails with ophyd-async's
/// "didn't provide an initial value" text, else as [`wait_for_value`].
pub async fn set_and_wait_for_other_value<R>(
    mut sub: Subscription,
    set: impl std::future::Future<Output = Result<R, BsrsError>>,
    mut matcher: impl Matcher,
    timeout: Duration,
) -> Result<R, BsrsError> {
    let deadline = tokio::time::Instant::now() + timeout;
    let Ok(first) = tokio::time::timeout_at(deadline, sub.recv()).await else {
        return Err(BsrsError::Other(format!(
            "{} didn't provide an initial value within {}s, is it connected?",
            sub.key(),
            timeout.as_secs_f64()
        )));
    };
    let first = first.ok_or_else(closed)?;
    let out = set.await?;
    if !matcher.matches(&first) {
        let mut last = Some(first);
        let fut = next_match(&mut sub, &mut matcher, &mut last);
        match tokio::time::timeout_at(deadline, fut).await {
            Ok(r) => {
                r?;
            }
            Err(_) => return Err(value_timeout(&sub, &matcher, timeout, last)),
        }
    }
    Ok(out)
}

/// The next value from `sub` that `matcher` matches, keeping each one that
/// does not in `last`.
async fn next_match(
    sub: &mut Subscription,
    matcher: &mut impl Matcher,
    last: &mut Option<ReadingValue>,
) -> Result<ReadingValue, BsrsError> {
    loop {
        let r = sub.recv().await.ok_or_else(closed)?;
        if matcher.matches(&r) {
            return Ok(r);
        }
        *last = Some(r);
    }
}

fn closed() -> BsrsError {
    BsrsError::Backend("subscription channel closed".into())
}

fn value_timeout(
    sub: &Subscription,
    matcher: &impl Matcher,
    timeout: Duration,
    last: Option<ReadingValue>,
) -> BsrsError {
    BsrsError::ValueTimeout {
        name: sub.key().to_string(),
        matcher: matcher.name(),
        timeout,
        last: last.map(|r| Box::new(r.value)),
    }
}

/// ophyd-async names the matcher by `__name__`, else `<TypeName>`: a fn item
/// gives its name, a closure (Python's `<lambda>`) `<closure>`, and any other
/// nameless callable (a fn pointer) `<its type>`.
fn matcher_name<F>() -> String {
    let ty = std::any::type_name::<F>();
    if ty.ends_with("{{closure}}") {
        return "<closure>".into();
    }
    // A fn item's type name is its path, plus generic args if it has any.
    let path = ty.trim_start_matches("&mut ").trim_start_matches('&');
    let path = path.split('<').next().unwrap_or(path);
    let ident = |seg: &str| !seg.is_empty() && seg.chars().all(|c| c.is_alphanumeric() || c == '_');
    match path.rsplit_once("::") {
        Some((_, name)) if path.split("::").all(ident) => name.to_string(),
        _ => format!("<{ty}>"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::status::SubToken;
    use crate::core::subscription::Fanout;
    use serde_json::json;

    fn reading(v: impl serde::Serialize) -> ReadingValue {
        ReadingValue {
            value: json!(v),
            timestamp: 0.0,
            alarm_severity: None,
            message: None,
        }
    }

    /// A fan-out whose latest value is `v`, and a subscription to it.
    fn fanout_at(v: impl serde::Serialize, key: &str) -> (Fanout<ReadingValue>, Subscription) {
        let f = Fanout::default();
        f.post(reading(v));
        let sub = Subscription::new(f.listen(), SubToken::noop(), key);
        (f, sub)
    }

    #[tokio::test]
    async fn wait_for_value_matches_current_immediately() {
        let (_f, sub) = fanout_at(5.0, "x");
        let got = wait_for_value(sub, 5.0, None).await.unwrap();
        assert_eq!(got.value, json!(5.0));
    }

    #[tokio::test]
    async fn wait_for_value_waits_for_a_matching_change() {
        let (f, sub) = fanout_at(0.0, "x");
        let h = tokio::spawn(wait_for_value(sub, 3.0, Some(Duration::from_secs(2))));
        tokio::task::yield_now().await;
        f.post(reading(1.0));
        f.post(reading(3.0));
        let got = h.await.unwrap().unwrap();
        assert_eq!(got.value, json!(3.0));
    }

    // A pulse that is over before the waiter polls (DMOV 1 → 0 → 1 on a
    // null move) still matches: a latest-value channel kept only the final 1.
    #[tokio::test]
    async fn wait_for_value_sees_a_pulse_posted_before_it_polls() {
        let (f, sub) = fanout_at(1.0, "x");
        f.post(reading(0.0));
        f.post(reading(1.0));
        let got = wait_for_value(sub, 0.0, Some(Duration::ZERO))
            .await
            .unwrap();
        assert_eq!(got.value, json!(0.0));
    }

    #[tokio::test(start_paused = true)]
    async fn wait_for_value_times_out_with_the_last_value_seen() {
        let (f, sub) = fanout_at(0.0, "x");
        let h = tokio::spawn(wait_for_value(sub, 9.0, Some(Duration::from_millis(50))));
        tokio::task::yield_now().await;
        // The fan-out stays alive, so the failure is a timeout, not a close.
        f.post(reading(1.0));
        match h.await.unwrap() {
            Err(BsrsError::ValueTimeout {
                name,
                matcher,
                timeout,
                last,
            }) => assert_eq!(
                (name.as_str(), matcher.as_str(), timeout, last.map(|v| *v)),
                ("x", "9.0", Duration::from_millis(50), Some(json!(1.0)))
            ),
            other => panic!("{other:?}"),
        }
    }

    // Boundary: no value ever arrives. The timeout reports `None`, as
    // ophyd-async prints `last value None`.
    #[tokio::test(start_paused = true)]
    async fn wait_for_value_times_out_with_no_value_seen() {
        let f = Fanout::<ReadingValue>::default();
        let sub = Subscription::new(f.listen(), SubToken::noop(), "x");
        let err = wait_for_value(
            sub,
            |_: &ReadingValue| true,
            Some(Duration::from_millis(50)),
        )
        .await
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "x didn't match <closure> in 0.05s, last value None"
        );
    }

    // ophyd-async test_set_and_wait_for_other_value_gives_helpful_error_with_no_first_value
    #[tokio::test(start_paused = true)]
    async fn set_and_wait_with_no_first_value_never_sets() {
        let f = Fanout::<ReadingValue>::default();
        let sub = Subscription::new(f.listen(), SubToken::noop(), "m");
        let set = async { Err::<(), _>(BsrsError::Other("set ran".into())) };
        let err = set_and_wait_for_other_value(sub, set, true, Duration::from_millis(50))
            .await
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            "m didn't provide an initial value within 0.05s, is it connected?"
        );
    }

    // ophyd-async test_set_and_wait_for_value_different_set_and_read_times_out,
    // with test_acquire_logic_when_arming_times_out's message.
    #[tokio::test(start_paused = true)]
    async fn set_and_wait_times_out_with_the_last_value_seen() {
        // Acquire_RBV stays False after the put.
        let (f, sub) = fanout_at(false, "m");
        let set = async {
            f.post(reading(false));
            Ok(())
        };
        let err = set_and_wait_for_other_value(sub, set, true, Duration::from_millis(50));
        assert_eq!(
            err.await.unwrap_err().to_string(),
            "m didn't match True in 0.05s, last value False"
        );
    }

    // A pulse the set causes is over before the wait polls (a 1 ms
    // acquisition's Acquire_RBV 0 → 1 → 0), and still matches.
    #[tokio::test(start_paused = true)]
    async fn set_and_wait_sees_a_pulse_the_set_caused() {
        let (f, sub) = fanout_at(0.0, "m");
        let set = async {
            f.post(reading(1.0));
            f.post(reading(0.0));
            Ok("set")
        };
        let out = set_and_wait_for_other_value(sub, set, 1.0, Duration::ZERO)
            .await
            .unwrap();
        assert_eq!(out, "set");
    }

    #[test]
    fn a_matcher_is_named_by_its_repr_fn_name_or_nameless_type() {
        fn is_nine(r: &ReadingValue) -> bool {
            r.value == json!(9.0)
        }
        assert_eq!(is_nine.name(), "is_nine");
        assert_eq!((|_: &ReadingValue| true).name(), "<closure>");
        let ptr: fn(&ReadingValue) -> bool = is_nine;
        assert!(ptr.name().starts_with("<fn("), "{}", ptr.name());
        assert_eq!("something".name(), "'something'");
        assert_eq!(true.name(), "True");
        assert_eq!(45.8_f64.name(), "45.8");
        assert_eq!(json!([1, "a", null]).name(), "[1, 'a', None]");
    }

    // A value matches as Python `==` does: across int and float, item by item.
    #[test]
    fn a_value_matches_as_python_eq() {
        let m = |mut v: Value, r| v.matches(&reading(r));
        assert!(1_i64.matches(&reading(1.0)));
        assert!(1.5_f64.matches(&reading(1.5)));
        assert!(!1_i64.matches(&reading("1")));
        assert!(!(-1_i64).matches(&reading(u64::MAX)));
        assert!(m(json!([1, {"a": 2}]), json!([1.0, {"a": 2.0}])));
        assert!(!m(json!([1, 2]), json!([1, 2, 3])));
        assert!(!m(json!({"a": 1}), json!({"b": 1})));
        assert!(Matcher::matches(&mut "blah", &reading("blah")));
    }

    #[tokio::test]
    async fn observe_value_yields_current_then_changes() {
        use futures::StreamExt;
        let (f, sub) = fanout_at(1.0, "x");
        let mut s = Box::pin(observe_value(sub));
        assert_eq!(s.next().await.unwrap().value, json!(1.0));
        f.post(reading(2.0));
        assert_eq!(s.next().await.unwrap().value, json!(2.0));
    }

    #[tokio::test]
    async fn observe_signals_value_merges_and_tags_by_index() {
        use futures::StreamExt;
        use std::collections::HashMap;
        let (f0, s0) = fanout_at(1.0, "s0");
        let (f1, s1) = fanout_at(2.0, "s1");
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
        f1.post(reading(5.0));
        let (i, v) = merged.next().await.unwrap();
        assert_eq!(i, 1);
        assert_eq!(v.value, json!(5.0));

        // A change on subscription 0 is reported with index 0.
        f0.post(reading(9.0));
        let (i, v) = merged.next().await.unwrap();
        assert_eq!(i, 0);
        assert_eq!(v.value, json!(9.0));
    }

    #[tokio::test]
    async fn observe_signals_value_ends_when_all_channels_close() {
        use futures::StreamExt;
        let (f0, s0) = fanout_at(1.0, "s0");
        let (f1, s1) = fanout_at(2.0, "s1");
        let mut merged = Box::pin(observe_signals_value(vec![s0, s1]));
        // Drain the two initial values.
        merged.next().await.unwrap();
        merged.next().await.unwrap();
        drop(f0);
        drop(f1);
        assert!(merged.next().await.is_none());
    }

    #[tokio::test]
    async fn observe_signals_value_empty_terminates() {
        use futures::StreamExt;
        let mut merged = Box::pin(observe_signals_value(Vec::<Subscription>::new()));
        assert!(merged.next().await.is_none());
    }
}
