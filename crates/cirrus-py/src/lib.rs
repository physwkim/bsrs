//! `cirrus_native` — Python bindings to cirrus's RunEngine, soft
//! devices, and a curated set of plan factories. Built as a Python
//! extension module via PyO3.
//!
//! ## Status
//!
//! M7 milestone — the goal is "use cirrus from Python without
//! Python on the IOC host". This first cut covers:
//!
//! - `cirrus_native.SoftMotor(name, initial=0.0)`
//! - `cirrus_native.SoftDetector(name)`
//! - `cirrus_native.RunEngine()` with `.run(plan)` returning a
//!   `(exit_status, run_uid)` tuple.
//! - `cirrus_native.count(detectors, num)`,
//!   `cirrus_native.scan(detectors, motor, motor_reader, start, stop, num)`
//!   — plan factories.
//! - Plan handles are opaque, single-use (consumed by `run`).
//! - Soft device protocol methods callable directly: `SoftMotor.read()` /
//!   `.describe()` / `.set(value)`, `SoftDetector.read()` / `.describe()`
//!   (ophyd/bluesky `Readable` + `Movable`).
//!
//! Future cuts could add: `RemoteDispatcher` analogue, `bp.*`
//! mirror, async run signatures, document subscribe callbacks.

#![allow(non_local_definitions)]
// pyo3's #[pyfunction] / #[pymethods] macros expand to code that
// clippy flags as useless conversions on the return type. The
// generated calls are inside macro expansion, so we can't fix them
// at the source level — silence the lint at the crate root.
#![allow(clippy::useless_conversion)]

use std::sync::Arc;

use cirrus_backend_soft::{SoftDetector, SoftMotor};
use cirrus_core::msg::{MovableObj, ReadableObj};
use cirrus_core::plan::Plan;
use cirrus_core::Document;
use cirrus_engine::{DocumentCallback, DocumentSink, RunEngine};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};
use std::sync::Mutex;

/// `cirrus_native.SoftMotor` — wrapper around `cirrus-backend-soft::SoftMotor`.
#[pyclass(name = "SoftMotor", module = "cirrus_native")]
struct PySoftMotor {
    inner: Arc<SoftMotor>,
}

#[pymethods]
impl PySoftMotor {
    #[new]
    #[pyo3(signature = (name, initial = 0.0))]
    fn new(name: &str, initial: f64) -> Self {
        Self {
            inner: Arc::new(SoftMotor::new(name, Some(initial))),
        }
    }

    fn name(&self) -> String {
        cirrus_core::msg::NamedObj::name(&*self.inner).to_string()
    }

    /// `motor.read()` — read the device's signals as a
    /// `{name: {value, timestamp, …}}` dict (ophyd/bluesky `Readable.read`).
    fn read(&self, py: Python<'_>) -> PyResult<PyObject> {
        read_obj(py, self.inner.clone() as Arc<dyn ReadableObj>)
    }

    /// `motor.describe()` — the data-key description companion to `read`.
    fn describe(&self, py: Python<'_>) -> PyResult<PyObject> {
        describe_obj(py, self.inner.clone() as Arc<dyn ReadableObj>)
    }

    /// `motor.set(value)` — move and block until the move completes
    /// (ophyd/bluesky `Movable.set` awaited to completion). Raises on failure.
    fn set(&self, py: Python<'_>, value: f64) -> PyResult<()> {
        let m: Arc<dyn MovableObj> = self.inner.clone();
        py.allow_threads(move || {
            cirrus_core::runtime::block_on(async move {
                let status = m.set_dyn(value).await;
                status.await
            })
        })
        .map_err(|e| PyRuntimeError::new_err(format!("set: {e}")))
    }

    fn __repr__(&self) -> String {
        format!("SoftMotor({:?})", self.name())
    }
}

/// `cirrus_native.SoftDetector`.
#[pyclass(name = "SoftDetector", module = "cirrus_native")]
struct PySoftDetector {
    inner: Arc<SoftDetector>,
}

#[pymethods]
impl PySoftDetector {
    #[new]
    fn new(name: &str) -> Self {
        Self {
            inner: SoftDetector::new(name),
        }
    }

    fn name(&self) -> String {
        cirrus_core::msg::NamedObj::name(&*self.inner).to_string()
    }

    /// `detector.read()` — read the detector's signals as a
    /// `{key: {value, timestamp, …}}` dict (ophyd/bluesky `Readable.read`).
    fn read(&self, py: Python<'_>) -> PyResult<PyObject> {
        read_obj(py, self.inner.clone() as Arc<dyn ReadableObj>)
    }

    /// `detector.describe()` — the data-key description companion to `read`.
    fn describe(&self, py: Python<'_>) -> PyResult<PyObject> {
        describe_obj(py, self.inner.clone() as Arc<dyn ReadableObj>)
    }

    fn __repr__(&self) -> String {
        format!("SoftDetector({:?})", self.name())
    }
}

/// Opaque plan handle. Single-use — `RunEngine.run` consumes it.
#[pyclass(name = "Plan", module = "cirrus_native", unsendable)]
struct PyPlan {
    inner: Mutex<Option<Plan>>,
    label: String,
}

#[pymethods]
impl PyPlan {
    fn __repr__(&self) -> String {
        format!("Plan({:?})", self.label)
    }
}

/// `cirrus_native.RunEngine` — drives plans synchronously.
#[pyclass(name = "RunEngine", module = "cirrus_native")]
struct PyRunEngine {
    inner: Arc<RunEngine>,
}

#[pymethods]
impl PyRunEngine {
    #[new]
    fn new() -> Self {
        let sinks: Vec<Arc<dyn DocumentSink>> = Vec::new();
        Self {
            inner: Arc::new(RunEngine::new(sinks)),
        }
    }

    /// Run a plan. Releases the GIL while the plan is executing so
    /// Python callers don't starve other threads. Returns
    /// `(exit_status, run_uid)`.
    fn run<'py>(
        &self,
        py: Python<'py>,
        plan: &Bound<'py, PyPlan>,
    ) -> PyResult<(String, Option<String>)> {
        let plan = plan.borrow().inner.lock().unwrap().take().ok_or_else(|| {
            PyRuntimeError::new_err("Plan already consumed; build a new plan via the factory")
        })?;
        let re = self.inner.clone();
        py.allow_threads(move || {
            let r = cirrus_core::runtime::block_on(re.run_async(plan));
            match r {
                Ok(rr) => Ok((rr.exit_status, rr.run_uid)),
                Err(e) => Err(PyRuntimeError::new_err(format!("RunEngine: {e}"))),
            }
        })
    }

    /// Subscribe a Python callable `cb(name, doc)` to every emitted document
    /// (bluesky `RunEngine.subscribe`). `name` is the document type
    /// (`"start"`, `"descriptor"`, `"event"`, `"stop"`, …) and `doc` is the
    /// document body as a `dict`. Returns an integer token; pass it to
    /// `unsubscribe` to remove the callback. Exceptions raised by the callback
    /// are printed and swallowed — a misbehaving callback does not abort the
    /// run (bluesky logs and continues).
    fn subscribe(&self, callable: Py<PyAny>) -> u64 {
        let cb: DocumentCallback = Arc::new(move |doc: &Document| {
            let (name, value) = doc_name_and_value(doc);
            Python::with_gil(|py| {
                let py_doc = json_to_py(py, &value);
                if let Err(e) = callable.call1(py, (name, py_doc)) {
                    e.print(py);
                }
            });
        });
        self.inner.subscribe(cb)
    }

    /// Remove a callback previously added with `subscribe`. Unknown tokens are
    /// ignored.
    fn unsubscribe(&self, token: u64) {
        self.inner.unsubscribe(token);
    }
}

/// Map a [`Document`] to its bluesky document name and a JSON value of the
/// inner document body (no enum tag), matching the `(name, doc)` pair a
/// bluesky `RunEngine.subscribe` callback receives.
fn doc_name_and_value(doc: &Document) -> (&'static str, serde_json::Value) {
    use cirrus_core::Document::*;
    let v = |r: Result<serde_json::Value, serde_json::Error>| r.unwrap_or(serde_json::Value::Null);
    match doc {
        Start(d) => ("start", v(serde_json::to_value(d))),
        Descriptor(d) => ("descriptor", v(serde_json::to_value(d))),
        Event(d) => ("event", v(serde_json::to_value(d))),
        EventPage(d) => ("event_page", v(serde_json::to_value(d))),
        Resource(d) => ("resource", v(serde_json::to_value(d))),
        Datum(d) => ("datum", v(serde_json::to_value(d))),
        DatumPage(d) => ("datum_page", v(serde_json::to_value(d))),
        StreamResource(d) => ("stream_resource", v(serde_json::to_value(d))),
        StreamDatum(d) => ("stream_datum", v(serde_json::to_value(d))),
        Stop(d) => ("stop", v(serde_json::to_value(d))),
    }
}

/// Recursively convert a `serde_json::Value` into a Python object so document
/// bodies reach Python as native dicts / lists / scalars rather than a JSON
/// string.
fn json_to_py(py: Python<'_>, value: &serde_json::Value) -> PyObject {
    use serde_json::Value::*;
    match value {
        Null => py.None(),
        Bool(b) => b.into_py(py),
        Number(n) => {
            if let Some(i) = n.as_i64() {
                i.into_py(py)
            } else if let Some(u) = n.as_u64() {
                u.into_py(py)
            } else {
                n.as_f64().unwrap_or(f64::NAN).into_py(py)
            }
        }
        String(s) => s.into_py(py),
        Array(items) => {
            let list = PyList::empty_bound(py);
            for item in items {
                let _ = list.append(json_to_py(py, item));
            }
            list.into_py(py)
        }
        Object(map) => {
            let dict = PyDict::new_bound(py);
            for (k, val) in map {
                let _ = dict.set_item(k, json_to_py(py, val));
            }
            dict.into_py(py)
        }
    }
}

/// Run a device's `read_dyn` on the cirrus runtime with the GIL released, then
/// return its `{key: {value, timestamp, …}}` reading set as a native Python
/// dict. Shared by `SoftMotor.read` and `SoftDetector.read`.
fn read_obj(py: Python<'_>, obj: Arc<dyn ReadableObj>) -> PyResult<PyObject> {
    let reading = py
        .allow_threads(move || cirrus_core::runtime::block_on(async move { obj.read_dyn().await }))
        .map_err(|e| PyRuntimeError::new_err(format!("read: {e}")))?;
    let value = serde_json::to_value(&reading).unwrap_or(serde_json::Value::Null);
    Ok(json_to_py(py, &value))
}

/// Run a device's `describe_dyn` (GIL released) and return its `{key: <data
/// key>}` description as a native Python dict — the read protocol's
/// `describe()` companion to [`read_obj`]. Shared by both soft devices.
fn describe_obj(py: Python<'_>, obj: Arc<dyn ReadableObj>) -> PyResult<PyObject> {
    let desc = py
        .allow_threads(move || {
            cirrus_core::runtime::block_on(async move { obj.describe_dyn().await })
        })
        .map_err(|e| PyRuntimeError::new_err(format!("describe: {e}")))?;
    let value = serde_json::to_value(&desc).unwrap_or(serde_json::Value::Null);
    Ok(json_to_py(py, &value))
}

/// `cirrus_native.count(detectors, num)` — bluesky `bp.count` mirror.
#[pyfunction]
fn count(detectors: &Bound<'_, PyList>, num: usize) -> PyResult<PyPlan> {
    let mut dets: Vec<Arc<dyn ReadableObj>> = Vec::with_capacity(detectors.len());
    for d in detectors.iter() {
        if let Ok(det) = d.downcast::<PySoftDetector>() {
            dets.push(det.borrow().inner.clone() as Arc<dyn ReadableObj>);
        } else if let Ok(m) = d.downcast::<PySoftMotor>() {
            dets.push(m.borrow().inner.clone() as Arc<dyn ReadableObj>);
        } else {
            return Err(PyValueError::new_err(
                "count: every detector must be a SoftDetector or SoftMotor",
            ));
        }
    }
    Ok(PyPlan {
        inner: Mutex::new(Some(cirrus_plans::count(dets, num))),
        label: format!("count(n={num})"),
    })
}

/// `cirrus_native.scan(detectors, motor, motor_reader, start, stop, num)`
/// — bluesky `bp.scan` mirror.
#[pyfunction]
fn scan(
    detectors: &Bound<'_, PyList>,
    motor: &Bound<'_, PySoftMotor>,
    motor_reader: &Bound<'_, PySoftMotor>,
    start: f64,
    stop: f64,
    num: usize,
) -> PyResult<PyPlan> {
    let mut dets: Vec<Arc<dyn ReadableObj>> = Vec::with_capacity(detectors.len());
    for d in detectors.iter() {
        if let Ok(det) = d.downcast::<PySoftDetector>() {
            dets.push(det.borrow().inner.clone() as Arc<dyn ReadableObj>);
        } else if let Ok(m) = d.downcast::<PySoftMotor>() {
            dets.push(m.borrow().inner.clone() as Arc<dyn ReadableObj>);
        } else {
            return Err(PyValueError::new_err(
                "scan: every detector must be a SoftDetector or SoftMotor",
            ));
        }
    }
    let mv: Arc<dyn MovableObj> = motor.borrow().inner.clone();
    let mr: Arc<dyn ReadableObj> = motor_reader.borrow().inner.clone();
    Ok(PyPlan {
        inner: Mutex::new(Some(cirrus_plans::scan(dets, mv, mr, start, stop, num))),
        label: format!("scan({start}..{stop}, n={num})"),
    })
}

/// `cirrus_native.version()` — package version string.
#[pyfunction]
fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[pymodule]
fn cirrus_native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PySoftMotor>()?;
    m.add_class::<PySoftDetector>()?;
    m.add_class::<PyPlan>()?;
    m.add_class::<PyRunEngine>()?;
    m.add_function(wrap_pyfunction!(count, m)?)?;
    m.add_function(wrap_pyfunction!(scan, m)?)?;
    m.add_function(wrap_pyfunction!(version, m)?)?;
    Ok(())
}

// Suppress unused warnings on PyTuple; reserved for future
// multi-return shapes.
const _: fn() = || {
    let _: fn(&Bound<'_, PyTuple>) = |_| {};
};
