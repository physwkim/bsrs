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
use cirrus_engine::{DocumentSink, RunEngine};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};
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
