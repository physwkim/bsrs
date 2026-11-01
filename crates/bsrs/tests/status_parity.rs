//! `Status` against the behaviour ophyd's `StatusBase` and ophyd-async's
//! `AsyncStatus` / `WatchableAsyncStatus` tests check.

use std::fmt::Debug;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bsrs::core::{Status, StatusError, StatusOutcome};
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
// test_wait_timeout: the blocking wait returns the outcome, and a wait that
// runs out leaves the status pending.
#[test]
fn blocking_wait_returns_the_outcome_and_a_wait_timeout_changes_nothing() {
    let (status, setter) = Status::new();
    assert!(matches!(
        status.wait(Some(10 * MS)),
        Err(StatusError::Timeout)
    ));
    assert!(!status.done_state());
    assert!(status.exception().is_none());
    setter.success();
    assert!(status.wait(LONG).is_ok());
    assert!(status.exception().is_none());

    let (status, setter) = Status::new();
    setter.fail(failed("local"));
    assert_eq!(dbg(status.exception()), dbg(Some(failed("local"))));
    assert_eq!(dbg(status.wait(LONG)), dbg(Err::<(), _>(failed("local"))));
}

// ophyd-async test_async_status_success / _propagates_exception /
// _has_no_exception_if_coroutine_successful / test_completed_status, and the
// self-cancelling device of _propagates_cancelled_error_with_message.
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
