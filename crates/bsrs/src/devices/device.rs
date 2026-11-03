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
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::future::{BoxFuture, FutureExt, Shared};

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

/// A spawned connect: every caller that joins it gets its one outcome, and a
/// caller dropped meanwhile does not cancel it.
type ConnectTask = Shared<BoxFuture<'static, std::result::Result<(), Arc<BsrsError>>>>;

/// The last connect a signal or command started (ophyd-async
/// `Device._connect_task`).
#[derive(Default)]
pub(crate) struct ConnectOnce(Mutex<Option<ConnectTask>>);

impl ConnectOnce {
    /// Join the connect still running, or the one that succeeded; otherwise,
    /// or when `force`d, spawn `start()` as the new one (ophyd-async
    /// `Device.connect`).
    pub(crate) async fn join<F>(&self, force: bool, start: impl FnOnce() -> F) -> Result<()>
    where
        F: Future<Output = Result<()>> + Send + 'static,
    {
        let task = {
            let mut slot = self.0.lock().unwrap();
            match slot.as_ref() {
                Some(task) if !force && !matches!(task.peek(), Some(Err(_))) => task.clone(),
                _ => {
                    let run = tokio::spawn(start());
                    let task = async move {
                        match run.await {
                            Ok(outcome) => outcome.map_err(Arc::new),
                            Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
                            Err(_) => Err(Arc::new(BsrsError::Cancelled)),
                        }
                    }
                    .boxed()
                    .shared();
                    *slot = Some(task.clone());
                    task
                }
            }
        };
        task.await.map_err(BsrsError::Connect)
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

/// ophyd-async `init_devices`: declare devices, name each one after its
/// variable, and connect them all concurrently, `?`-returning a
/// [`BsrsError::NotConnected`] that names every variable whose device failed.
///
/// `let x = Type(prefix);` builds `Type::new_named(prefix, "x")`, as
/// ophyd-async names a device left unnamed; any other `let x = device;`
/// keeps the device's own name and is only connected. Every binding stays in
/// scope after the macro. `timeout = ...;` first replaces ophyd-async's
/// 10 s default. Mock is the devices' backend type rather than a flag.
///
/// The connect is awaited, so the macro belongs in an async fn; after
/// `blocking;` it blocks on the bsrs runtime instead, which panics inside an
/// async context (a plan), where ophyd-async's sync form raises.
///
/// ```
/// # use bsrs::backends::soft::SoftSignalBackend;
/// # use bsrs::devices::SignalRW;
/// #[derive(bsrs::Device)]
/// struct Motor {
///     name: String,
///     #[signal(rw, "{prefix}.VAL")]
///     setpoint: SignalRW<f64, SoftSignalBackend<f64>>,
/// }
///
/// # fn main() -> bsrs::core::Result<()> {
/// bsrs::init_devices! {
///     blocking;
///     let t1x = Motor("BLxxI-MO-TABLE-01:X");
///     let t1y = Motor::new_named("BLxxI-MO-TABLE-01:Y", "table_y");
/// }
/// assert_eq!(t1x.name(), "t1x");
/// assert_eq!(t1x.setpoint.name(), "t1x-setpoint");
/// assert_eq!(t1y.name(), "table_y");
/// # Ok(())
/// # }
/// ```
#[macro_export]
macro_rules! init_devices {
    (blocking; timeout = $timeout:expr; $($decls:tt)*) => {
        $crate::__init_devices!(@decl [block_on $timeout] [] $($decls)*);
    };
    (blocking; $($decls:tt)*) => {
        $crate::init_devices!(
            blocking; timeout = ::std::time::Duration::from_secs(10); $($decls)*
        );
    };
    (timeout = $timeout:expr; $($decls:tt)*) => {
        $crate::__init_devices!(@decl [connect $timeout] [] $($decls)*);
    };
    ($($decls:tt)*) => {
        $crate::init_devices!(timeout = ::std::time::Duration::from_secs(10); $($decls)*);
    };
}

/// [`init_devices!`]'s declarations, one `let` at a time, then the connect.
#[doc(hidden)]
#[macro_export]
macro_rules! __init_devices {
    (@decl $how:tt [$($names:ident)*] let $id:ident = $ty:ident($prefix:expr); $($rest:tt)*) => {
        let $id = <$ty>::new_named(&$prefix, ::std::stringify!($id));
        $crate::__init_devices!(@decl $how [$($names)* $id] $($rest)*);
    };
    (@decl $how:tt [$($names:ident)*] let $id:ident = $device:expr; $($rest:tt)*) => {
        let $id = $device;
        $crate::__init_devices!(@decl $how [$($names)* $id] $($rest)*);
    };
    (@decl $how:tt []) => {};
    (@decl [connect $timeout:expr] [$($names:ident)+]) => {
        $crate::__init_devices!(@connect $timeout, $($names)+).await?;
    };
    (@decl [block_on $timeout:expr] [$($names:ident)+]) => {
        $crate::core::runtime::block_on($crate::__init_devices!(@connect $timeout, $($names)+))?;
    };
    (@connect $timeout:expr, $($names:ident)+) => {{
        let timeout = $timeout;
        $crate::devices::wait_for_connection([$((
            ::std::stringify!($names),
            $crate::devices::Device::connect_all_boxed(&$names, timeout),
        )),+])
    }};
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

    // ophyd-async test_utils::test_error_handling_value_errors: an unexpected
    // child failure is logged at error once, by the device that has the
    // child; a not-connected one, shared signal or nested device, only at
    // debug where the signal gave up.
    #[tokio::test]
    async fn wait_for_connection_logs_only_unexpected_failures() {
        use futures::FutureExt;
        let (caplog, _guard) = crate::caplog::Caplog::start();
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
        let text = caplog.text();
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

#[cfg(test)]
mod init_devices_tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use crate::backends::mock::MockSignalBackend;
    use crate::core::error::{BsrsError, Result};
    use crate::core::msg::{MovableObj, NamedObj};
    use crate::core::reading::ReadingValue;
    use crate::core::runtime::block_on;
    use crate::core::{Status, SubToken};
    use crate::devices::{BackendFromPv, Device, SignalRW};
    use crate::engine::RunEngine;
    use crate::event_model::DataKey;
    use crate::protocols_async::{PutCompletion, ReadingValueCallback, SignalBackend};

    /// Connects, except a `ca://`/`pva://` PV, which never answers, and
    /// `value_error:{text}`, ophyd-async's `ValueErrorBackend(exc_text)`.
    struct Probe {
        pv: String,
        connected: AtomicBool,
    }

    impl BackendFromPv for Probe {
        fn from_pv(pv: &str) -> Self {
            Self {
                pv: pv.to_string(),
                connected: AtomicBool::new(false),
            }
        }
    }

    #[async_trait::async_trait]
    impl SignalBackend<f64> for Probe {
        async fn connect(&self, timeout: Duration) -> Result<()> {
            if self.pv.starts_with("ca://") || self.pv.starts_with("pva://") {
                let why = format!("no value in {timeout:?}");
                return Err(BsrsError::not_connected(self.pv.clone(), why));
            }
            if let Some(text) = self.pv.strip_prefix("value_error:") {
                return Err(BsrsError::InvalidValue(text.to_string()));
            }
            self.connected.store(true, Ordering::SeqCst);
            Ok(())
        }
        async fn put_start(&self, _value: Option<f64>) -> Result<PutCompletion> {
            Err(BsrsError::Backend("probe does not put".into()))
        }
        async fn get_datakey(&self, _source: &str) -> Result<DataKey> {
            Err(BsrsError::Backend("probe has no datakey".into()))
        }
        async fn get_reading(&self) -> Result<ReadingValue> {
            Err(BsrsError::Backend("probe has no reading".into()))
        }
        async fn get_value(&self) -> Result<f64> {
            if self.connected.load(Ordering::SeqCst) {
                Ok(0.0)
            } else {
                Err(BsrsError::Backend(format!("{} not connected", self.pv)))
            }
        }
        async fn get_setpoint(&self) -> Result<f64> {
            self.get_value().await
        }
        fn set_callback(&self, _cb: Option<ReadingValueCallback<f64>>) -> SubToken {
            SubToken::noop()
        }
        fn source(&self, _name: &str, _read: bool) -> String {
            self.pv.clone()
        }
    }

    type ProbeRW = SignalRW<f64, Probe>;

    #[derive(crate::Device)]
    struct WorkingChild {
        name: String,
        #[signal(rw, "{prefix}working")]
        working_signal: ProbeRW,
    }

    #[derive(crate::Device)]
    struct TimeoutChildCa {
        name: String,
        #[signal(rw, "ca://A_NON_EXISTENT_SIGNAL")]
        timeout_signal: ProbeRW,
    }

    #[derive(crate::Device)]
    struct TimeoutChildPva {
        name: String,
        #[signal(rw, "pva://A_NON_EXISTENT_SIGNAL")]
        timeout_signal: ProbeRW,
    }

    /// Fails to connect with its prefix as the `ValueError` text.
    #[derive(crate::Device)]
    struct ValueErrorChild {
        name: String,
        #[signal(rw, "value_error:{prefix}")]
        value_error_signal: ProbeRW,
    }

    #[derive(crate::Device)]
    struct OneWorkingOneTimeout {
        name: String,
        #[device("{prefix}:w")]
        working_child_device: Arc<WorkingChild>,
        #[device("")]
        timeout_child_device: Arc<TimeoutChildCa>,
    }

    #[derive(crate::Device)]
    struct TwoWorkingTwoTimeoutTwoValueError {
        name: String,
        #[device("{prefix}:w1")]
        working_child_device1: Arc<WorkingChild>,
        #[device("{prefix}:w2")]
        working_child_device2: Arc<WorkingChild>,
        #[device("")]
        timeout_child_device_ca: Arc<TimeoutChildCa>,
        #[device("")]
        timeout_child_device_pva: Arc<TimeoutChildPva>,
        #[device("Some ValueError text")]
        value_error_child_device1: Arc<ValueErrorChild>,
        #[device("")]
        value_error_child_device2: Arc<ValueErrorChild>,
    }

    #[derive(crate::Device)]
    struct TopLevelSignalAndSubDevice {
        name: String,
        #[signal(rw, "ca://A_NON_EXISTENT_SIGNAL")]
        timeout_signal: ProbeRW,
        #[device("Some ValueError text")]
        sub_device: Arc<ValueErrorChild>,
    }

    /// The `(level, message)` of each line `caplog` holds, leaving out the
    /// signals' own, as ophyd-async's tests leave out `_signal`'s.
    fn logged(caplog: &crate::caplog::Caplog) -> Vec<(String, String)> {
        let mut logs: Vec<(String, String)> = caplog
            .text()
            .lines()
            .filter_map(|l| {
                let (level, rest) = ["ERROR", "DEBUG"]
                    .into_iter()
                    .find_map(|level| Some((level, l.split_once(&format!(" {level} "))?.1)))?;
                let (target, message) = rest.split_once(": ")?;
                (target != "bsrs::devices::signal").then(|| (level.into(), message.into()))
            })
            .collect();
        logs.sort();
        logs
    }

    fn logs(expected: &[(&str, &str)]) -> Vec<(String, String)> {
        let mut logs: Vec<_> = expected
            .iter()
            .map(|(l, m)| (l.to_string(), m.to_string()))
            .collect();
        logs.sort();
        logs
    }

    // ophyd-async test_utils::test_error_handling_init_devices: each device
    // that failed is named by its variable over its own failed children, and
    // only the unexpected failures log at error, once each.
    #[tokio::test]
    async fn init_devices_names_failed_devices_by_variable() {
        let (caplog, _guard) = crate::caplog::Caplog::start();
        let connect = async {
            crate::init_devices! {
                timeout = Duration::from_millis(100);
                let dummy_device_two_working_one_timeout_two_value_error =
                    TwoWorkingTwoTimeoutTwoValueError("D2");
                let dummy_device_one_working_one_timeout = OneWorkingOneTimeout("D1");
            }
            Ok::<_, BsrsError>(())
        };
        let err = connect.await.unwrap_err();
        assert_eq!(
            err.to_string(),
            "not connected:\n\
             dummy_device_two_working_one_timeout_two_value_error:\n    \
             timeout_child_device_ca:\n        \
             timeout_signal: not connected: ca://A_NON_EXISTENT_SIGNAL\n    \
             timeout_child_device_pva:\n        \
             timeout_signal: not connected: pva://A_NON_EXISTENT_SIGNAL\n    \
             value_error_child_device1:\n        \
             value_error_signal: invalid value: Some ValueError text\n    \
             value_error_child_device2:\n        \
             value_error_signal: invalid value: \n\
             dummy_device_one_working_one_timeout:\n    \
             timeout_child_device:\n        \
             timeout_signal: not connected: ca://A_NON_EXISTENT_SIGNAL"
        );
        let unexpected = "device `value_error_signal` raised unexpected exception: invalid value:";
        let timeout = |source| format!("signal {source} not connected: no value in 100ms");
        let (ca, pva) = (
            timeout("ca://A_NON_EXISTENT_SIGNAL"),
            timeout("pva://A_NON_EXISTENT_SIGNAL"),
        );
        assert_eq!(
            logged(&caplog),
            logs(&[
                ("ERROR", &format!("{unexpected} Some ValueError text")),
                ("ERROR", &format!("{unexpected} ")),
                ("DEBUG", &ca),
                ("DEBUG", &ca),
                ("DEBUG", &pva),
            ]),
            "{}",
            caplog.text()
        );
    }

    // ophyd-async test_utils::test_combining_top_level_signal_and_child_device.
    #[tokio::test]
    async fn init_devices_nests_a_failed_signal_and_sub_device_under_the_variable() {
        let connect = async {
            crate::init_devices! {
                timeout = Duration::from_millis(100);
                let dummy_device2 = TopLevelSignalAndSubDevice("C");
            }
            Ok::<_, BsrsError>(())
        };
        assert_eq!(
            connect.await.unwrap_err().to_string(),
            "not connected:\n\
             dummy_device2:\n    \
             timeout_signal: not connected: ca://A_NON_EXISTENT_SIGNAL\n    \
             sub_device:\n        \
             value_error_signal: invalid value: Some ValueError text"
        );
    }

    /// ophyd-async test_auto_init_devices's `FailingDevice`.
    struct FailingDevice {
        name: String,
    }

    impl Device for FailingDevice {
        fn name(&self) -> &str {
            &self.name
        }
        fn connect_all_boxed<'a>(
            &'a self,
            _timeout: Duration,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
            Box::pin(async { Err(BsrsError::Other("AttributeError".into())) })
        }
    }

    // ophyd-async test_init_devices_handles_top_level_errors.
    #[tokio::test]
    async fn init_devices_logs_an_unexpected_failure_once() {
        let (caplog, _guard) = crate::caplog::Caplog::start();
        let connect = async {
            crate::init_devices! {
                let failing = FailingDevice { name: "somename".into() };
            }
            Ok::<_, BsrsError>(())
        };
        let err = connect.await.unwrap_err();
        assert_eq!(err.to_string(), "not connected:\nfailing: AttributeError");
        assert_eq!(
            logged(&caplog),
            logs(&[(
                "ERROR",
                "device `failing` raised unexpected exception: AttributeError"
            )])
        );
    }

    #[derive(crate::Device)]
    struct Group {
        name: String,
        #[device("{prefix}:1")]
        child1: Arc<WorkingChild>,
        #[device("{prefix}:2")]
        _child2: Arc<WorkingChild>,
    }

    // ophyd-async test_device::test_device_with_init_devices (bsrs devices
    // keep no parent link, and the derive takes no DeviceVector field).
    #[tokio::test]
    async fn init_devices_names_and_connects_a_device_tree() -> Result<()> {
        crate::init_devices! {
            let parent = Group("P");
        }
        assert_eq!(parent.name(), "parent");
        assert_eq!(parent.child1.name(), "parent-child1");
        assert_eq!(parent._child2.name(), "parent-_child2");
        assert_eq!(
            parent.child1.working_signal.name(),
            "parent-child1-working_signal"
        );
        parent.child1.working_signal.get().await?;
        parent._child2.working_signal.get().await?;
        Ok(())
    }

    // ophyd-async test_init_devices_detects_redeclared_devices.
    #[tokio::test]
    async fn init_devices_takes_only_the_devices_it_declares() -> Result<()> {
        let working_device = WorkingChild::new("W");
        let original_working_device = working_device.clone();
        crate::init_devices! {
            let working_device = WorkingChild("W");
        }
        assert!(!Arc::ptr_eq(&original_working_device, &working_device));
        assert_eq!(working_device.name(), "working_device");
        working_device.working_signal.get().await?;
        assert_eq!(original_working_device.name(), "W");
        assert!(original_working_device.working_signal.get().await.is_err());
        Ok(())
    }

    // ophyd-async test_sync_init_devices_run_engine_created_connects; a
    // device already named keeps its name.
    #[test]
    fn blocking_init_devices_connects() -> Result<()> {
        crate::init_devices! {
            blocking;
            let working_device = WorkingChild::new_named("W", "somename");
        }
        assert_eq!(working_device.name(), "somename");
        block_on(working_device.working_signal.get())?;
        Ok(())
    }

    // ophyd-async test_connecting_in_plan_raises: a bsrs plan is an async
    // stream, so inside one the blocking form is inside an async context.
    #[tokio::test]
    #[should_panic(expected = "called from inside an async context")]
    async fn blocking_init_devices_panics_inside_an_async_context() {
        let connect = async {
            crate::init_devices! {
                blocking;
                let working_device = WorkingChild("W");
            }
            Ok::<_, BsrsError>(working_device)
        };
        let _ = connect.await;
    }

    #[derive(crate::Device)]
    struct MockMotor {
        name: String,
        #[signal(rw, "{prefix}.VAL")]
        user_setpoint: SignalRW<f64, MockSignalBackend<f64>>,
    }

    impl NamedObj for MockMotor {
        fn name(&self) -> &str {
            &self.name
        }
    }

    #[async_trait::async_trait]
    impl MovableObj for MockMotor {
        async fn set_dyn(&self, value: f64) -> Status {
            self.user_setpoint.put(value).await
        }
    }

    // ophyd-async test_async_init_devices_run_engine_same_event_loop.
    #[test]
    fn init_devices_devices_move_in_the_run_engine() -> Result<()> {
        let mock_motor = block_on(async {
            crate::init_devices! {
                let mock_motor = MockMotor("BLxxI-MO-TABLE-01:X");
            }
            Ok::<_, BsrsError>(mock_motor)
        })?;
        RunEngine::new(vec![]).run_blocking(crate::plans::stubs::mv(mock_motor.clone(), 2.5))?;
        assert_eq!(mock_motor.user_setpoint.name(), "mock_motor-user_setpoint");
        assert_eq!(block_on(mock_motor.user_setpoint.get())?, 2.5);
        Ok(())
    }
}
