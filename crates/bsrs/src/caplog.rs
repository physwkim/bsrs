//! pytest's `caplog` for unit tests.

use std::cell::RefCell;
use std::sync::{Arc, Mutex, Once};

use tracing::level_filters::LevelFilter;
use tracing_subscriber::layer::SubscriberExt;

/// What `tracing` logs at DEBUG and above on this thread, from
/// [`Caplog::start`] until its guard drops.
#[derive(Clone, Default)]
pub(crate) struct Caplog(Arc<Mutex<Vec<u8>>>);

thread_local! {
    static CAPTURE: RefCell<Option<Caplog>> = const { RefCell::new(None) };
}

/// Ends the capture [`Caplog::start`] began, restoring the one before it.
pub(crate) struct CaplogGuard(Option<Caplog>);

impl Drop for CaplogGuard {
    fn drop(&mut self) {
        CAPTURE.with(|c| *c.borrow_mut() = self.0.take());
    }
}

impl Caplog {
    pub(crate) fn start() -> (Self, CaplogGuard) {
        // One global subscriber writing to each thread's capture, not a
        // `set_default` per test: tracing caches a callsite's interest
        // process-wide, and with one scoped subscriber registered it asks
        // only the hitting thread's, so a test thread without one cached
        // "never" for the line another test was capturing. The filter
        // opens after the subscriber is global, so no callsite registers
        // against no subscriber at all.
        static INSTALL: Once = Once::new();
        INSTALL.call_once(|| {
            let (filter, level) = tracing_subscriber::reload::Layer::new(LevelFilter::OFF);
            let subscriber = tracing_subscriber::registry().with(filter).with(
                tracing_subscriber::fmt::layer()
                    .with_writer(|| ThreadCapture)
                    .with_ansi(false),
            );
            tracing::subscriber::set_global_default(subscriber).expect("caplog subscriber");
            level.reload(LevelFilter::DEBUG).expect("caplog level");
        });
        let caplog = Self::default();
        let previous = CAPTURE.with(|c| c.borrow_mut().replace(caplog.clone()));
        (caplog, CaplogGuard(previous))
    }

    pub(crate) fn text(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

/// The global subscriber's writer: this thread's [`Caplog`], if capturing.
struct ThreadCapture;

impl std::io::Write for ThreadCapture {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        CAPTURE.with(|c| {
            if let Some(caplog) = &*c.borrow() {
                caplog.0.lock().unwrap().extend_from_slice(buf);
            }
        });
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
