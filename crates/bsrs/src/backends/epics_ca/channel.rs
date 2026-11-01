//! The cached CA channel every backend for one PV shares, and the single owner
//! of its puts.
//!
//! rsrv holds a channel's second WRITE_NOTIFY, and with it every request
//! behind it on the circuit, until the first is answered
//! (`camessage.c:1660-1706`, up to 60 s); a plain WRITE it does not hold. So
//! a channel has at most one put-callback in flight: a put made meanwhile is
//! written plain at once, and its completion is that of one trailing
//! put-callback of the latest value written, sent when the in-flight one is
//! answered.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use epics_base_rs::server::snapshot::{DbrClass, Snapshot};
use epics_ca_rs::client::{CaChannel, ChannelInfo, MonitorHandle, ReqCount};
use epics_ca_rs::{CaResult, DbFieldType, EpicsValue};
use tokio::sync::oneshot;

/// A put's outcome as its waiters see it: `CaError` is not `Clone`.
pub(super) type PutResult = std::result::Result<(), String>;

type Completion = Pin<Box<dyn Future<Output = CaResult<()>> + Send>>;

/// A PV's channel. Reads go straight through; puts only through
/// [`Self::put_callback`] and [`Self::put_no_callback`].
pub(super) struct SharedChannel {
    ca: CaChannel,
    puts: Mutex<PutSlot>,
}

/// `Busy` exactly while a [`SharedChannel::drive`] task runs for the channel.
enum PutSlot {
    Idle,
    /// `next` is the trailing put-callback, if a put arrived since the
    /// in-flight one was sent.
    Busy {
        next: Option<Trailing>,
    },
}

struct Trailing {
    value: EpicsValue,
    waiters: Vec<oneshot::Sender<PutResult>>,
}

impl SharedChannel {
    pub(super) fn new(ca: CaChannel) -> Self {
        Self {
            ca,
            puts: Mutex::new(PutSlot::Idle),
        }
    }

    /// Resolves once the channel is connected, at once if it is. Any number
    /// of callers may wait, each with its own `timeout`; the channel keeps
    /// searching after one gives up.
    pub(super) async fn wait_connected(&self, timeout: Duration) -> CaResult<()> {
        self.ca.wait_connected(timeout).await
    }

    pub(super) async fn info(&self) -> CaResult<ChannelInfo> {
        self.ca.info().await
    }

    /// Every read asks for count 0, the record's current element count, as
    /// aioca's `ca_array_get_callback` and the monitor do; a one-element
    /// answer decodes as the scalar `EpicsValue`.
    pub(super) async fn get_with_timeout(
        &self,
        timeout: Duration,
    ) -> CaResult<(DbFieldType, EpicsValue)> {
        self.ca
            .get_with_timeout_count(timeout, ReqCount::Autosize(0))
            .await
    }

    pub(super) async fn get_with_metadata(
        &self,
        class: DbrClass,
        timeout: Duration,
    ) -> CaResult<Snapshot> {
        self.ca
            .get_with_metadata_count(class, ReqCount::Autosize(0), timeout)
            .await
    }

    pub(super) async fn subscribe(&self) -> CaResult<MonitorHandle> {
        self.ca.subscribe().await
    }

    /// Put `v`, sent on the circuit in call order when this returns. The
    /// receiver resolves when the channel's put-callback covering `v` is
    /// answered, with no deadline of its own.
    pub(super) fn put_callback(
        self: &Arc<Self>,
        v: &EpicsValue,
    ) -> CaResult<oneshot::Receiver<PutResult>> {
        let (tx, rx) = oneshot::channel();
        let mut slot = self.puts.lock().unwrap();
        match &mut *slot {
            PutSlot::Idle => {
                let done = self.ca.put_callback(v)?;
                *slot = PutSlot::Busy { next: None };
                crate::core::runtime::bsrs_runtime()
                    .spawn(Arc::clone(self).drive(Box::pin(done), vec![tx]));
            }
            PutSlot::Busy { next } => {
                self.ca.put_no_callback(v)?;
                match next {
                    Some(t) => {
                        t.value = v.clone();
                        t.waiters.push(tx);
                    }
                    None => {
                        *next = Some(Trailing {
                            value: v.clone(),
                            waiters: vec![tx],
                        })
                    }
                }
            }
        }
        Ok(rx)
    }

    /// `ca_array_put` of `v`. A trailing put-callback still to be sent
    /// carries `v` instead, so it cannot rewrite an older value after it.
    pub(super) fn put_no_callback(&self, v: &EpicsValue) -> CaResult<()> {
        let mut slot = self.puts.lock().unwrap();
        self.ca.put_no_callback(v)?;
        if let PutSlot::Busy { next: Some(t) } = &mut *slot {
            t.value = v.clone();
        }
        Ok(())
    }

    /// Answer each in-flight put-callback's waiters, sending the trailing one
    /// first, until none is left.
    async fn drive(
        self: Arc<Self>,
        mut done: Completion,
        mut waiters: Vec<oneshot::Sender<PutResult>>,
    ) {
        loop {
            let r = done.await.map_err(|e| e.to_string());
            // Settled before anyone is answered: a waiter that puts again on
            // its answer must not find this put-callback still in flight.
            let next = {
                let mut slot = self.puts.lock().unwrap();
                match std::mem::replace(&mut *slot, PutSlot::Idle) {
                    PutSlot::Busy { next: Some(t) } => {
                        let sent = self.ca.put_callback(&t.value);
                        if sent.is_ok() {
                            *slot = PutSlot::Busy { next: None };
                        }
                        Some((sent, t.waiters))
                    }
                    _ => None,
                }
            };
            answer(waiters, r);
            let Some((sent, next_waiters)) = next else {
                return;
            };
            match sent {
                Ok(f) => {
                    done = Box::pin(f);
                    waiters = next_waiters;
                }
                Err(e) => return answer(next_waiters, Err(e.to_string())),
            }
        }
    }
}

fn answer(waiters: Vec<oneshot::Sender<PutResult>>, r: PutResult) {
    for w in waiters {
        let _ = w.send(r.clone());
    }
}
