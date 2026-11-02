//! Stops the IOC [`super::example_ioc::boot`] started, and starts it again
//! on the same ports, so the clients' channels disconnect and reconnect.
//! Every test in a binary that uses it shares that IOC: hold [`serial`].

use std::sync::{Mutex, MutexGuard, PoisonError};

use super::example_ioc::{Booted, BOOTED};

fn booted() -> &'static Booted {
    BOOTED.get().expect("boot the IOC first")
}

/// Held by each test in the binary, so no test sees another's outage.
pub fn serial() -> MutexGuard<'static, ()> {
    static SERIAL: Mutex<()> = Mutex::new(());
    SERIAL.lock().unwrap_or_else(PoisonError::into_inner)
}

/// End the IOC; its clients' circuits close.
pub fn stop() {
    if let Some(mut ioc) = booted().lock().take() {
        let _ = ioc.kill();
        ioc.wait().expect("reap the IOC");
    }
}

/// Start the IOC again on the ports the clients use. Its records start over
/// from their database values.
pub fn start() {
    let booted = booted();
    let mut ioc = booted.lock();
    *ioc = Some(booted.start_again());
}

pub fn restart() {
    stop();
    start();
}
