//! The [`Device`] trait and [`DeviceVector`] collection.
//!
//! `Device` is the object-safe interface every `#[derive(Device)]` type
//! implements (the derive emits it). It lets heterogeneous / generic device
//! collections — notably [`DeviceVector`] — connect and walk children
//! uniformly, mirroring ophyd-async `Device` + `DeviceVector`
//! (`core/_device.py:129-330`).

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use crate::core::error::{BsrsError, NotConnected, Result};

/// Object-safe device interface: a stable name plus connect-all.
///
/// The connect method is hand-boxed rather than `async fn` so the trait stays
/// object-safe without pulling `async_trait` into every crate that derives a
/// device; `#[derive(Device)]` boxes its inherent `connect_all`.
pub trait Device: Send + Sync {
    /// Stable device name.
    fn name(&self) -> &str;
    /// Connect every signal / sub-device of this device.
    fn connect_all_boxed<'a>(
        &'a self,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
    /// Push `(dotted_path, source)` for every signal in this device's subtree
    /// into `out`, recursing into sub-devices. `prefix` is the accumulated
    /// dotted path — empty at the device the walk started from, `"sub."`
    /// inside a sub-device named `sub`.
    ///
    /// `#[derive(Device)]` emits this by walking the struct's `#[signal]` and
    /// `#[device]` fields at compile time (the same field walk that drives
    /// `connect_all`). The default is a no-op so hand-written `Device` impls
    /// with no introspectable signals need not override it. Prefer the
    /// [`walk_signal_sources`] free function over calling this directly.
    fn walk_signal_sources(&self, _prefix: &str, _out: &mut Vec<(String, String)>) {}
}

impl<D: Device + ?Sized> Device for Arc<D> {
    fn name(&self) -> &str {
        (**self).name()
    }
    fn connect_all_boxed<'a>(
        &'a self,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        (**self).connect_all_boxed(timeout)
    }
    fn walk_signal_sources(&self, prefix: &str, out: &mut Vec<(String, String)>) {
        (**self).walk_signal_sources(prefix, out)
    }
}

/// Collect `(dotted_path, source)` for every signal in `root`'s device tree,
/// depth-first in field-declaration order. The paths are relative to `root`
/// (the root device's own name is not prefixed), so a signal field `setpoint`
/// yields `"setpoint"` and a signal `vel` on a sub-device `x` yields `"x.vel"`.
///
/// Mirrors ophyd-async `walk_signal_sources` (`core/_signal.py`), adapted to
/// bsrs where signals are not themselves `Device`s: the per-field pushes are
/// emitted by `#[derive(Device)]` rather than discovered by recursing a uniform
/// `children()` of `Signal`-typed devices.
pub fn walk_signal_sources(root: &dyn Device) -> Vec<(String, String)> {
    let mut out = Vec::new();
    root.walk_signal_sources("", &mut out);
    out
}

/// An integer-keyed collection of identical child devices (e.g. eight cameras
/// indexed 1–8), participating in the device tree via [`connect_all`] and
/// [`children`]. Mirrors ophyd-async `DeviceVector` (`core/_device.py:285-330`).
///
/// Children are constructed by the caller (with their final names, e.g. via
/// `Sub::new_named`) and inserted; the vector does not build them, since each
/// concrete device type owns its own construction.
///
/// [`connect_all`]: DeviceVector::connect_all
/// [`children`]: DeviceVector::children
pub struct DeviceVector<D> {
    children: BTreeMap<u32, D>,
}

impl<D> Default for DeviceVector<D> {
    fn default() -> Self {
        Self::new()
    }
}

impl<D> DeviceVector<D> {
    /// Build an empty vector.
    pub fn new() -> Self {
        Self {
            children: BTreeMap::new(),
        }
    }

    /// Insert (or replace) the child at `key`, returning any previous child.
    pub fn insert(&mut self, key: u32, device: D) -> Option<D> {
        self.children.insert(key, device)
    }

    /// Borrow the child at `key`.
    pub fn get(&self, key: u32) -> Option<&D> {
        self.children.get(&key)
    }

    /// Mutably borrow the child at `key`.
    pub fn get_mut(&mut self, key: u32) -> Option<&mut D> {
        self.children.get_mut(&key)
    }

    /// Whether a child exists at `key`.
    pub fn contains_key(&self, key: u32) -> bool {
        self.children.contains_key(&key)
    }

    /// Number of children.
    pub fn len(&self) -> usize {
        self.children.len()
    }

    /// Whether there are no children.
    pub fn is_empty(&self) -> bool {
        self.children.is_empty()
    }

    /// Iterate keys in ascending order.
    pub fn keys(&self) -> impl Iterator<Item = u32> + '_ {
        self.children.keys().copied()
    }

    /// Iterate `(key, &child)` in ascending key order.
    pub fn iter(&self) -> impl Iterator<Item = (u32, &D)> + '_ {
        self.children.iter().map(|(k, v)| (*k, v))
    }

    /// Iterate `&child` in ascending key order.
    pub fn values(&self) -> impl Iterator<Item = &D> + '_ {
        self.children.values()
    }
}

impl<D> FromIterator<(u32, D)> for DeviceVector<D> {
    fn from_iter<I: IntoIterator<Item = (u32, D)>>(iter: I) -> Self {
        Self {
            children: iter.into_iter().collect(),
        }
    }
}

impl<D> std::ops::Index<u32> for DeviceVector<D> {
    type Output = D;
    fn index(&self, key: u32) -> &D {
        &self.children[&key]
    }
}

impl<D: Device> DeviceVector<D> {
    /// bluesky-style `children()`: yields `("1", &child)`, `("2", &child)`, …
    /// in ascending key order.
    pub fn children(&self) -> impl Iterator<Item = (String, &D)> + '_ {
        self.children.iter().map(|(k, v)| (k.to_string(), v))
    }

    /// Connect every child concurrently; a failure names every child that
    /// failed by its key.
    pub async fn connect_all(&self, timeout: Duration) -> Result<()> {
        wait_for_connection(
            self.children
                .iter()
                .map(|(k, d)| (k.to_string(), d.connect_all_boxed(timeout))),
        )
        .await
    }
}

/// Run every named connect concurrently, then fail with
/// [`BsrsError::NotConnected`] naming each one that failed, in the order
/// given: ophyd-async `wait_for_connection`. A failure that is not itself a
/// not-connected one is logged at error, as ophyd-async logs an "unexpected
/// exception".
pub async fn wait_for_connection<N, F>(connects: impl IntoIterator<Item = (N, F)>) -> Result<()>
where
    N: Into<String>,
    F: Future<Output = Result<()>>,
{
    let (names, futs): (Vec<N>, Vec<F>) = connects.into_iter().unzip();
    let failed: Vec<(String, BsrsError)> = names
        .into_iter()
        .zip(futures::future::join_all(futs).await)
        .filter_map(|(name, r)| r.err().map(|e| (name.into(), e)))
        .collect();
    if failed.is_empty() {
        return Ok(());
    }
    for (name, err) in &failed {
        if err.as_not_connected().is_none() {
            tracing::error!("device `{name}` raised unexpected exception: {err}");
        }
    }
    Err(BsrsError::NotConnected(NotConnected::Children(failed)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct Dummy {
        name: String,
        connected: AtomicBool,
    }

    impl Device for Dummy {
        fn name(&self) -> &str {
            &self.name
        }
        fn connect_all_boxed<'a>(
            &'a self,
            _timeout: Duration,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
            Box::pin(async move {
                self.connected.store(true, Ordering::SeqCst);
                Ok(())
            })
        }
    }

    fn dummy(name: &str) -> Arc<Dummy> {
        Arc::new(Dummy {
            name: name.into(),
            connected: AtomicBool::new(false),
        })
    }

    #[tokio::test]
    async fn vector_indexes_iterates_and_connects() {
        let mut v: DeviceVector<Arc<Dummy>> = DeviceVector::new();
        // Insert out of order; BTreeMap iterates ascending.
        v.insert(2, dummy("cam2"));
        v.insert(1, dummy("cam1"));
        assert_eq!(v.len(), 2);
        assert!(v.contains_key(1));
        assert_eq!(v[1].name(), "cam1");

        let listed: Vec<_> = v
            .children()
            .map(|(k, d)| (k, d.name().to_string()))
            .collect();
        assert_eq!(
            listed,
            vec![
                ("1".to_string(), "cam1".to_string()),
                ("2".to_string(), "cam2".to_string()),
            ]
        );

        v.connect_all(Duration::from_millis(10)).await.unwrap();
        assert!(v[1].connected.load(Ordering::SeqCst));
        assert!(v[2].connected.load(Ordering::SeqCst));
    }

    /// Collects what `tracing` logs on this thread into a buffer, as
    /// pytest's `caplog`.
    #[derive(Clone, Default)]
    struct Caplog(Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for Caplog {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    // ophyd-async test_utils::test_error_handling_value_errors: an unexpected
    // child failure is logged at error once, by the device that has the
    // child; a not-connected one, shared signal or nested device, only at
    // debug where the signal gave up.
    #[tokio::test]
    async fn wait_for_connection_logs_only_unexpected_failures() {
        use futures::FutureExt;
        let caplog = Caplog::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer({
                let caplog = caplog.clone();
                move || caplog.clone()
            })
            .with_max_level(tracing::Level::DEBUG)
            .with_ansi(false)
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);
        let shared = |e| Err(BsrsError::Connect(Arc::new(e)));
        let sub_device = wait_for_connection([
            (
                "value_error_signal",
                async { shared(BsrsError::InvalidValue("Some ValueError text".into())) }.boxed(),
            ),
            (
                "timeout_signal",
                async {
                    let source = "ca://A_NON_EXISTENT_SIGNAL".to_string();
                    shared(BsrsError::not_connected(source, "no value in 10ms"))
                }
                .boxed(),
            ),
        ]);
        let err = wait_for_connection([
            ("sub_device", sub_device.boxed()),
            ("working_signal", async { Ok(()) }.boxed()),
        ])
        .await
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "not connected:\n\
             sub_device:\n    \
             value_error_signal: invalid value: Some ValueError text\n    \
             timeout_signal: not connected: ca://A_NON_EXISTENT_SIGNAL"
        );
        let text = String::from_utf8(caplog.0.lock().unwrap().clone()).unwrap();
        let logged = |level: &str| -> Vec<String> {
            text.lines()
                .filter_map(|l| l.split_once(&format!(" {level} ")))
                .map(|(_, rest)| rest.split_once(": ").unwrap().1.to_string())
                .collect()
        };
        assert_eq!(
            logged("ERROR"),
            ["device `value_error_signal` raised unexpected exception: \
              invalid value: Some ValueError text"],
            "{text}"
        );
        assert_eq!(
            logged("DEBUG"),
            ["signal ca://A_NON_EXISTENT_SIGNAL not connected: no value in 10ms"],
            "{text}"
        );
    }
}
