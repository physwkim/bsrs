//! CA-backed `SuspendBoolHigh` / `SuspendBoolLow` /
//! `SuspendThreshold` factories for the daemon Lua surface.
//!
//! The user-facing Suspender impls in `bsrs-engine` watch a
//! [`Fanout`]. To wire a live EPICS PV in we subscribe via bsrs's CA
//! backend and post every monitor update to a fan-out, then hand the
//! suspender to `RunEngine::install_suspender`, whose watcher asks the
//! engine to pause / resume.
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
use crate::core::subscription::Fanout;
use crate::devices::SignalR;
use crate::engine::{
    RunEngine, SuspendBoolHigh, SuspendBoolLow, SuspendThreshold, ThresholdDirection,
};
use crate::host::motor_move::pv_signal;

/// Subscribe to `pv` and post every monitor update, mapped by `map`, to a
/// fresh fan-out seeded with the current value. The subscription (a
/// `SubToken` with RAII unsubscribe) is leaked to the heap so it survives
/// this fn returning.
async fn ca_watch<T>(pv: &str, map: fn(f64) -> T) -> Result<Arc<Fanout<T>>>
where
    T: Clone + Send + Sync + 'static,
{
    let signal: SignalR<f64, EpicsCaBackend<f64>> =
        pv_signal(EpicsCaBackend::new_inferred(pv), pv, pv.to_string());
    signal.connect(Duration::from_secs(5)).await?;
    let initial: f64 = signal.get().await.unwrap_or(0.0);
    let fanout = Arc::new(Fanout::default());
    fanout.post(map(initial));
    let sink = fanout.clone();
    let cb: crate::protocols_async::ReadingValueCallback<f64> =
        Box::new(move |v: &f64, _ts, _sev| {
            sink.post(map(*v));
        });
    let sub_token = signal.subscribe(cb);
    Box::leak(Box::new(sub_token));
    // Pin the signal too — once dropped, the channel goes idle.
    Box::leak(Box::new(signal));
    Ok(fanout)
}

/// Every monitor update of `pv`, as an `f64`.
pub async fn ca_watch_f64(pv: &str) -> Result<Arc<Fanout<f64>>> {
    ca_watch(pv, |v| v).await
}

/// Bool variant: converts the f64 to bool via `!= 0.0`. EPICS bi/bo records
/// are doubles on the wire.
pub async fn ca_watch_bool(pv: &str) -> Result<Arc<Fanout<bool>>> {
    ca_watch(pv, |v| v != 0.0).await
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
    re.install_suspender(Arc::new(SuspendThreshold::new(name, &rx, threshold, dir)))
        .await;
    Ok(())
}

/// `SuspendBoolHigh` against a PV.
pub async fn install_suspend_bool_high(name: &str, pv: &str, re: Arc<RunEngine>) -> Result<()> {
    let rx = ca_watch_bool(pv).await?;
    re.install_suspender(Arc::new(SuspendBoolHigh::new(name, &rx)))
        .await;
    Ok(())
}

/// `SuspendBoolLow` against a PV.
pub async fn install_suspend_bool_low(name: &str, pv: &str, re: Arc<RunEngine>) -> Result<()> {
    let rx = ca_watch_bool(pv).await?;
    re.install_suspender(Arc::new(SuspendBoolLow::new(name, &rx)))
        .await;
    Ok(())
}
