//! `Status` against the behaviour ophyd's `StatusBase` and ophyd-async's
//! `AsyncStatus` / `WatchableAsyncStatus` tests check.

use std::fmt::Debug;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bsrs::core::status::WaitError;
use bsrs::core::{Status, StatusError, StatusOutcome, Watcher, WatcherUpdate};
use tokio::time::{sleep, Instant};

const MS: Duration = Duration::from_millis(1);
const LONG: Option<Duration> = Some(Duration::from_secs(1));

/// A device operation the way bsrs devices build one: the work runs in a task
/// that settles the setter, and drops it unfinished once the consumer cancels.
fn spawn_op<F>(work: F) -> Status
where
    F: Future<Output = Result<(), StatusError>> + Send + 'static,
{
    let (status, setter) = Status::new();
    tokio::spawn(async move {
        let outcome = tokio::select! {
            biased;
            _ = setter.cancelled() => None,
            r = work => Some(r),
        };
        match outcome {
            Some(Ok(())) => setter.success(),
            Some(Err(e)) => setter.fail(e),
            None => {}
        }
    });
    status
}

fn failed(msg: &str) -> StatusError {
    StatusError::Failed(msg.into())
}

/// `StatusError` has no `PartialEq`; compare outcomes by their debug form.
fn dbg<T: Debug>(v: T) -> String {
    format!("{v:?}")
}

fn record(status: &Status) -> Arc<Mutex<Vec<StatusOutcome>>> {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let s2 = seen.clone();
    status.add_callback(move |o| s2.lock().unwrap().push(o.clone()));
    seen
}

// ophyd test_status_post / test_status_pre: a callback added before completion
// waits for it, one added after fires at once, each exactly once.
#[test]
fn callback_fires_once_whether_added_before_or_after_completion() {
    let (status, setter) = Status::new();
    let before = record(&status);
    assert!(before.lock().unwrap().is_empty());

    setter.success();
    status.wait(LONG).unwrap();
    let after = record(&status);

    for seen in [before, after] {
        assert_eq!(dbg(&*seen.lock().unwrap()), dbg([StatusOutcome::Success]));
    }
}

// ophyd test_exception_success_path / test_exception_fail_path /
// test_wait_timeout / test_external_timeout: the blocking wait returns the
// outcome, and a wait that runs out (WaitTimeoutError) leaves the status
// pending and is not an operation that timed out (StatusTimeoutError).
#[test]
fn blocking_wait_returns_the_outcome_and_a_wait_timeout_changes_nothing() {
    let (status, setter) = Status::new();
    assert!(matches!(
        status.wait(Some(10 * MS)),
        Err(WaitError::Timeout)
    ));
    assert!(!status.done_state());
    assert!(status.exception().is_none());
    setter.success();
    assert!(status.wait(LONG).is_ok());
    assert!(status.exception().is_none());

    for err in [failed("local"), StatusError::Timeout] {
        let (status, setter) = Status::new();
        setter.fail(err.clone());
        assert_eq!(dbg(status.exception()), dbg(Some(&err)));
        assert_eq!(
            dbg(status.wait(LONG)),
            dbg(Err::<(), _>(WaitError::Status(err)))
        );
    }
}

// ophyd-async test_async_status_success / _propagates_exception /
// _has_no_exception_if_coroutine_successful / test_completed_status, and the
// self-cancelling device of _propagates_cancelled_error_with_message, whose
// task ends cancelled.
#[tokio::test(start_paused = true)]
async fn awaiting_an_operation_yields_its_outcome() {
    for result in [
        Ok(()),
        Err(failed("ValueError")),
        Err(StatusError::Cancelled),
    ] {
        let r = result.clone();
        let status = spawn_op(async move {
            sleep(100 * MS).await;
            r
        });
        assert!(!status.done_state());
        assert!(!status.success());
        assert!(status.exception().is_none());

        assert_eq!(dbg(status.clone().await), dbg(&result));
        assert!(status.done_state());
        assert_eq!(status.success(), result.is_ok());
        assert_eq!(dbg(status.exception()), dbg(result.as_ref().err()));
        let cancelled = matches!(result, Err(StatusError::Cancelled));
        assert_eq!(status.cancelled(), cancelled);
        assert_eq!(status.inspect()["cancelled"], cancelled);
    }

    assert!(Status::done().await.is_ok());
    assert_eq!(
        dbg(Status::fail(failed("ValueError")).await),
        dbg(Err::<(), _>(failed("ValueError")))
    );
}

// ophyd-async test_async_status_success_if_cancelled[wait_to_run=True/False]:
// cancelling, before or after the work starts, resolves the status Cancelled,
// runs the callback once and stops the work.
#[tokio::test(start_paused = true)]
async fn cancel_before_or_after_the_work_starts_resolves_cancelled() {
    for wait_to_run in [true, false] {
        let running = Arc::new(tokio::sync::Notify::new());
        let started = Arc::new(AtomicBool::new(false));
        let finished = Arc::new(AtomicBool::new(false));
        let status = spawn_op({
            let (running, started, finished) = (running.clone(), started.clone(), finished.clone());
            async move {
                started.store(true, Ordering::SeqCst);
                running.notify_one();
                sleep(10 * MS).await;
                finished.store(true, Ordering::SeqCst);
                Ok(())
            }
        });
        let seen = record(&status);
        if wait_to_run {
            running.notified().await;
        }

        status.cancel();
        assert!(matches!(status.clone().await, Err(StatusError::Cancelled)));
        assert!(!status.success());
        assert!(matches!(status.exception(), Some(StatusError::Cancelled)));
        assert_eq!(
            dbg(&*seen.lock().unwrap()),
            dbg([StatusOutcome::Failed(StatusError::Cancelled)])
        );

        sleep(50 * MS).await;
        assert_eq!(started.load(Ordering::SeqCst), wait_to_run);
        assert!(!finished.load(Ordering::SeqCst), "work ran on after cancel");
        assert!(status.cancelled());
    }
}

// ophyd-async test_status_complete_loop_continues /
// test_awaiting_status_inside_its_own_context / test_no_await_points_in_loop_body
// / test_status_coroutine_raises_exception: the guard never cuts the scope
// short, and an outcome reached inside the scope survives its exit.
#[tokio::test(start_paused = true)]
async fn cancel_guard_keeps_an_outcome_reached_inside_the_scope() {
    for result in [Ok(()), Err(failed("ValueError"))] {
        let r = result.clone();
        let status = spawn_op(async move {
            sleep(10 * MS).await;
            r
        });
        let t0 = Instant::now();
        let mut vals = Vec::new();
        {
            let guard = status.cancel_on_drop();
            for i in 0..10 {
                vals.push(i);
                sleep(10 * MS).await;
            }
            assert_eq!(dbg(guard.status().clone().await), dbg(&result));
        }
        assert_eq!(vals, (0..10).collect::<Vec<_>>());
        assert_eq!(t0.elapsed(), 100 * MS);
        assert!(!status.cancelled());
        assert_eq!(dbg(status.clone().await), dbg(&result));
    }
}

// ophyd-async test_loop_complete_before_status_complete /
// test_error_raised_from_with_status / test_both_status_and_loop_raise_exceptions:
// leaving the scope before the status completes, normally or with an error,
// cancels the work at once and the scope's own error is the one returned.
#[tokio::test(start_paused = true)]
async fn cancel_guard_exit_before_completion_cancels_the_work() {
    // (iterations before leaving, leave with an error, work result at 50 ms)
    let cases = [
        (2, false, Ok(())),
        (0, true, Ok(())),
        (1, true, Err(failed("Status failed"))),
    ];
    for (iters, leave_with_error, result) in cases {
        let finished = Arc::new(AtomicBool::new(false));
        let status = spawn_op({
            let finished = finished.clone();
            async move {
                sleep(50 * MS).await;
                finished.store(true, Ordering::SeqCst);
                result
            }
        });
        let t0 = Instant::now();
        let mut vals = Vec::new();
        let out = async {
            let _guard = status.cancel_on_drop();
            for i in 0..iters {
                vals.push(i);
                sleep(10 * MS).await;
            }
            if leave_with_error {
                return Err("breaking out");
            }
            Ok(())
        }
        .await;
        assert_eq!(t0.elapsed(), iters * 10 * MS);
        assert_eq!(vals, (0..iters).collect::<Vec<_>>());
        assert_eq!(
            out,
            if leave_with_error {
                Err("breaking out")
            } else {
                Ok(())
            }
        );
        assert!(matches!(status.clone().await, Err(StatusError::Cancelled)));

        sleep(100 * MS).await;
        assert!(!finished.load(Ordering::SeqCst), "work ran on after cancel");
        assert!(matches!(status.exception(), Some(StatusError::Cancelled)));
    }
}

#[derive(Default)]
struct Currents(Vec<f64>);

impl Watcher for Currents {
    fn watch(&mut self, update: &WatcherUpdate) {
        self.0.push(update.current);
    }
}

// ophyd-async test_asyncstatus_wraps_set_iterator_with_class_or_func_watcher,
// test_watchableasyncstatus_wraps_failing_set_iterator,
// test_watchableasyncstatus_times_out and
// test_watchable_async_status_reports_watch_type_not_result_type: every
// watcher sees every update, in order, up to the operation's outcome.
#[tokio::test(start_paused = true)]
async fn every_watcher_sees_every_update_up_to_the_outcome() {
    // (currents posted 10 ms apart, one posted together with the outcome, outcome)
    let cases = [
        (vec![1.0, 2.0, 3.0, 4.0, 5.0], Some(6.0), Ok(())),
        (vec![1.0, 2.0, 3.0], None, Err(failed("SetFailedError"))),
        (vec![1.0], None, Err(StatusError::Timeout)),
        (vec![], Some(1.5), Ok(())),
    ];
    for (spaced, last, result) in cases {
        let expect: Vec<f64> = spaced.iter().copied().chain(last).collect();
        let (status, setter) = Status::new();
        let r = result.clone();
        tokio::spawn(async move {
            for c in spaced {
                sleep(10 * MS).await;
                setter.update_watcher(WatcherUpdate::new(c, 0.0, 6.0));
            }
            if let Some(c) = last {
                setter.update_watcher(WatcherUpdate::new(c, 0.0, c));
            }
            match r {
                Ok(()) => setter.success(),
                Err(e) => setter.fail(e),
            }
        });
        let watchers: Vec<_> = (0..2)
            .map(|_| {
                let s = status.clone();
                tokio::spawn(async move {
                    let mut w = Currents::default();
                    s.observe_watcher(&mut w).await;
                    w.0
                })
            })
            .collect();
        for w in watchers {
            assert_eq!(w.await.unwrap(), expect);
        }
        assert_eq!(dbg(status.clone().await), dbg(&result));
        assert_eq!(status.success(), result.is_ok());
        assert_eq!(dbg(status.exception()), dbg(result.as_ref().err()));
    }
}

/// Wakes by sending on a channel, so a reader can tell a wake from a lost one.
struct ChannelWaker(std::sync::mpsc::Sender<()>);

impl std::task::Wake for ChannelWaker {
    fn wake(self: Arc<Self>) {
        let _ = self.0.send(());
    }
}

// ophyd add_callback (status.py:502) runs each callback exactly once and is
// thread-safe. Readers racing `fail` must see the recorded error, never a
// missing or "unknown" one; a racing callback must fire and a racing waiter
// must be woken.
#[test]
fn readers_racing_completion_see_its_outcome() {
    use std::pin::pin;
    use std::sync::{mpsc, Barrier};
    use std::task::{Context, Poll, Waker};

    let boom = dbg(Err::<(), _>(failed("boom")));
    for _ in 0..2000 {
        let (status, setter) = Status::new();
        let barrier = Barrier::new(5);
        let (callback, spin_poll, woken_poll, exception) = std::thread::scope(|s| {
            let callback = s.spawn(|| {
                let (tx, rx) = mpsc::channel();
                barrier.wait();
                status.add_callback(move |o| tx.send(o.clone()).unwrap());
                dbg(rx.recv_timeout(Duration::from_secs(5)).ok())
            });
            let spin_poll = s.spawn(|| {
                let mut cx = Context::from_waker(Waker::noop());
                let mut fut = pin!(status.clone());
                barrier.wait();
                loop {
                    if let Poll::Ready(r) = fut.as_mut().poll(&mut cx) {
                        break dbg(r);
                    }
                }
            });
            let woken_poll = s.spawn(|| {
                let (tx, rx) = mpsc::channel();
                let waker = Waker::from(Arc::new(ChannelWaker(tx)));
                let mut cx = Context::from_waker(&waker);
                let mut fut = pin!(status.clone());
                barrier.wait();
                if fut.as_mut().poll(&mut cx).is_pending()
                    && rx.recv_timeout(Duration::from_secs(5)).is_err()
                {
                    return "waker lost".into();
                }
                match fut.as_mut().poll(&mut cx) {
                    Poll::Ready(r) => dbg(r),
                    Poll::Pending => "pending after wake".into(),
                }
            });
            let exception = s.spawn(|| {
                barrier.wait();
                while !status.done_state() {}
                dbg(status.exception())
            });
            barrier.wait();
            setter.fail(failed("boom"));
            (
                callback.join().unwrap(),
                spin_poll.join().unwrap(),
                woken_poll.join().unwrap(),
                exception.join().unwrap(),
            )
        });
        assert_eq!(callback, dbg(Some(StatusOutcome::Failed(failed("boom")))));
        assert_eq!(spin_poll, boom);
        assert_eq!(woken_poll, boom);
        assert_eq!(exception, dbg(Some(failed("boom"))));
    }
}

/// A mover whose every move times out.
struct TimingOut;

impl bsrs::core::msg::NamedObj for TimingOut {
    fn name(&self) -> &str {
        "timing_out"
    }
}

#[async_trait::async_trait]
impl bsrs::core::msg::MovableObj for TimingOut {
    async fn set_dyn(&self, _value: f64) -> Status {
        Status::fail(StatusError::Timeout)
    }
}

// bluesky _status_object_completed raises FailedStatus for a status that timed
// out on its own; WaitForTimeoutError is only the group wait running out.
#[tokio::test]
async fn an_operation_timeout_fails_the_plan_as_a_status_failure() {
    use bsrs::core::msg::{MovableObj, Msg};
    use bsrs::core::plan::plan_box;
    use bsrs::core::BsrsError;
    use bsrs::engine::RunEngine;

    for group in [Some("g".to_string()), None] {
        let mover: Arc<dyn MovableObj> = Arc::new(TimingOut);
        let plan = plan_box(async_stream::stream! {
            yield Msg::Set { obj: mover, value: 1.0, group: group.clone() };
            if let Some(group) = group {
                yield Msg::Wait { group, error_on_timeout: true, timeout: None };
            }
        });
        let result = RunEngine::new(vec![]).run_async(plan).await.unwrap();
        assert!(
            matches!(
                result.exception,
                Some(BsrsError::Status(StatusError::Timeout))
            ),
            "{:?}",
            result.exception
        );
    }
}
