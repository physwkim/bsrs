//! pytest's `caplog` for unit tests.

use std::sync::{Arc, Mutex};

/// What `tracing` logs at DEBUG and above on this thread, from
/// [`Caplog::start`] until its guard drops.
#[derive(Clone, Default)]
pub(crate) struct Caplog(Arc<Mutex<Vec<u8>>>);

impl Caplog {
    pub(crate) fn start() -> (Self, tracing::subscriber::DefaultGuard) {
        let caplog = Self::default();
        let writer = caplog.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || writer.clone())
            .with_max_level(tracing::Level::DEBUG)
            .with_ansi(false)
            .finish();
        (caplog, tracing::subscriber::set_default(subscriber))
    }

    pub(crate) fn text(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

impl std::io::Write for Caplog {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
