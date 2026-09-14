//! CA-backed `SuspendBoolHigh` / `SuspendBoolLow` /
//! `SuspendThreshold` factories for the daemon Lua surface.
//!
//! The user-facing Suspender impls in `bsrs-engine` watch a
//! `tokio::sync::watch::Receiver`. To wire a live EPICS PV in we
//! subscribe via bsrs's CA backend and pump every monitor update
//! into the matching `watch::Sender`, then hand the suspender to
//! `RunEngine::install_suspender`, whose watcher asks the engine to
//! pause / resume.
//!
//! Lua surface exposed by `build_lua` when the `ca` feature is on:
//!
//! ```lua
//! ca_suspend_threshold("low_beam", "mini:current", 200.0, "below")
//! ca_suspend_bool_high("shutter_open", "BL:Shutter:State")
//! ca_suspend_bool_low("permit", "BL:RunPermit")
//! ```
//!
//! Each factory installs the suspender on the in-process `RE`
//! captured at REPL build time; `RE:clear_suspenders` removes them.

use std::sync::Arc;
use std::time::Duration;

use crate::backends::epics_ca::EpicsCaBackend;
use crate::core::error::Result;
use crate::engine::{
    RunEngine, SuspendBoolHigh, SuspendBoolLow, SuspendThreshold, ThresholdDirection,
};
use crate::protocols_async::SignalBackend;
use tokio::sync::watch;

/// Subscribe to `pv` and pump every monitor update into a fresh
/// `watch::Sender<f64>`. Returns the receiver. The subscription
/// (a `SubToken` with RAII unsubscribe) is leaked to the heap so
/// the pump survives this fn returning.
pub async fn ca_watch_f64(pv: &str) -> Result<watch::Receiver<f64>> {
    let backend = Arc::new(EpicsCaBackend::<f64>::new(pv));
    backend.connect(Duration::from_secs(5)).await?;
    let initial: f64 = backend.get_value().await.unwrap_or(0.0);
    let (tx, rx) = watch::channel(initial);
    let cb: crate::protocols_async::ReadingValueCallback<f64> =
        Box::new(move |v: &f64, _ts, _sev| {
            let _ = tx.send(*v);
        });
    let sub_token = backend.set_callback(Some(cb));
    Box::leak(Box::new(sub_token));
    // Pin the backend too — once dropped, the channel goes idle.
    Box::leak(Box::new(backend));
    Ok(rx)
}

/// Bool variant: same shape but converts the f64 to bool via
/// `!= 0.0`. EPICS bi/bo records are doubles on the wire.
pub async fn ca_watch_bool(pv: &str) -> Result<watch::Receiver<bool>> {
    let f_rx = ca_watch_f64(pv).await?;
    let initial = *f_rx.borrow() != 0.0;
    let (tx, rx) = watch::channel(initial);
    let mut f_rx = f_rx;
    tokio::spawn(async move {
        loop {
            if f_rx.changed().await.is_err() {
                break;
            }
            let v = *f_rx.borrow();
            let _ = tx.send(v != 0.0);
        }
    });
    Ok(rx)
}

/// Build + install a `SuspendThreshold`. `direction` is `"above"`
/// or `"below"` (the BAD region).
pub async fn install_suspend_threshold(
    name: &str,
    pv: &str,
    threshold: f64,
    direction: &str,
    re: Arc<RunEngine>,
) -> Result<()> {
    let dir = match direction {
        "above" => ThresholdDirection::BadIfAbove,
        "below" => ThresholdDirection::BadIfBelow,
        other => {
            return Err(crate::core::error::BsrsError::InvalidValue(format!(
                "ca_suspend_threshold: direction must be 'above' or 'below', got {other:?}"
            )))
        }
    };
    let rx = ca_watch_f64(pv).await?;
    re.install_suspender(Arc::new(SuspendThreshold::new(name, rx, threshold, dir)))
        .await;
    Ok(())
}

/// `SuspendBoolHigh` against a PV.
pub async fn install_suspend_bool_high(name: &str, pv: &str, re: Arc<RunEngine>) -> Result<()> {
    let rx = ca_watch_bool(pv).await?;
    re.install_suspender(Arc::new(SuspendBoolHigh::new(name, rx)))
        .await;
    Ok(())
}

/// `SuspendBoolLow` against a PV.
pub async fn install_suspend_bool_low(name: &str, pv: &str, re: Arc<RunEngine>) -> Result<()> {
    let rx = ca_watch_bool(pv).await?;
    re.install_suspender(Arc::new(SuspendBoolLow::new(name, rx)))
        .await;
    Ok(())
}
