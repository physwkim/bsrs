//! The [`Status`] of a move, shared by the CA and PVA motors.

use crate::core::status::{Status, StatusError};
use crate::protocols_async::{PutCompletion, SignalBackend};

/// Send `value` to `setpoint`, then return a pending [`Status`] that `finish`
/// settles from the put's completion. The engine's `Set` handler awaits
/// `set_dyn` before it moves on to the next message, so a `set_dyn` that
/// awaited the move itself would serialize a multi-axis `mv`; bluesky's
/// `set()` returns its status immediately and the motion completes it. Only
/// the wait is spawned: a spawned send lets `set(a); set(b)` reach the IOC as
/// b, a, and the IOC then finishes at `a`.
pub(crate) async fn move_status<B, F, Fut>(
    setpoint: &B,
    value: f64,
    what: &str,
    finish: F,
) -> Status
where
    B: SignalBackend<f64>,
    F: FnOnce(PutCompletion) -> Fut,
    Fut: std::future::Future<Output = std::result::Result<(), String>> + Send + 'static,
{
    let (status, setter) = Status::new();
    match setpoint.put_start(Some(value)).await {
        Ok(done) => {
            let mv = finish(done);
            tokio::spawn(async move {
                match mv.await {
                    Ok(()) => setter.success(),
                    Err(e) => setter.fail(StatusError::Failed(e)),
                }
            });
        }
        Err(e) => setter.fail(StatusError::Failed(format!("{what}: {e}"))),
    }
    status
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::mock::MockSignalBackend;
    use crate::core::Dtype;
    use std::time::Duration;

    #[tokio::test]
    async fn move_status_sends_puts_in_call_order() {
        let sp = MockSignalBackend::with_value(0.0_f64, Dtype::Number);
        let blocked = sp.mock_puts_blocked();
        let finish = |done: PutCompletion| async move { done.await.map_err(|e| e.to_string()) };

        // No yield between the two calls: the puts must already be sent when
        // `move_status` returns, not left to spawned tasks.
        let a = move_status(&sp, 1.0, "t", finish).await;
        assert_eq!(sp.put_calls(), vec![Some(1.0)]);
        let b = move_status(&sp, 2.0, "t", finish).await;
        assert_eq!(sp.put_calls(), vec![Some(1.0), Some(2.0)]);
        assert!(!a.done_state() && !b.done_state());

        drop(blocked);
        let both = async { (a.await, b.await) };
        let (ra, rb) = tokio::time::timeout(Duration::from_secs(1), both)
            .await
            .unwrap();
        ra.unwrap();
        rb.unwrap();
    }
}
