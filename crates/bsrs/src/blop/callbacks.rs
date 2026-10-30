//! Document sinks for optimization runs — bsrs's port of `blop.callbacks`.
//!
//! [`OptimizationRouter`] is the sink to subscribe on the RunEngine: it
//! forwards the documents of `optimize` and `sample_suggestions` runs, and
//! only their `primary` stream, to a shared list of sinks that can be
//! edited between runs. [`OptimizationLogger`] is such a sink: it prints
//! each iteration's parameters and outcomes as a table and running
//! statistics of the outcomes, as blop's does with `rich`, here as plain
//! text on any `Write`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Write;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::Value;

use crate::blop::plans::{OPTIMIZE_RUN_KEY, SAMPLE_SUGGESTIONS_RUN_KEY};
use crate::blop::stubs::{ACQUISITION_UID_KEY, SUGGESTION_IDS_KEY};
use crate::blop::tracking::Source;
use crate::core::error::{BsrsError, Result};
use crate::engine::sink::DocumentSink;
use crate::event_model::{Document, Event, EventDescriptor, ExitStatus, RunStart, RunStop};

// ---------------------------------------------------------------------------
//  RunningStats
// ---------------------------------------------------------------------------

/// Count, min, max, mean and sample standard deviation of a stream of
/// values, by Welford's online algorithm; NaN and infinities are skipped.
#[derive(Clone, Debug)]
pub struct RunningStats {
    /// Values observed.
    pub count: usize,
    /// Smallest value, `+inf` before the first.
    pub min: f64,
    /// Largest value, `-inf` before the first.
    pub max: f64,
    mean: f64,
    m2: f64,
}

impl Default for RunningStats {
    fn default() -> Self {
        Self {
            count: 0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            mean: 0.0,
            m2: 0.0,
        }
    }
}

impl RunningStats {
    /// No observations yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Incorporate one observation.
    pub fn update(&mut self, value: f64) {
        if !value.is_finite() {
            return;
        }
        self.count += 1;
        self.min = self.min.min(value);
        self.max = self.max.max(value);
        let delta = value - self.mean;
        self.mean += delta / self.count as f64;
        self.m2 += delta * (value - self.mean);
    }

    /// Mean of the observations, NaN before the first.
    pub fn mean(&self) -> f64 {
        if self.count > 0 {
            self.mean
        } else {
            f64::NAN
        }
    }

    /// Sample standard deviation, NaN before the second observation.
    pub fn std(&self) -> f64 {
        if self.count < 2 {
            f64::NAN
        } else {
            (self.m2 / (self.count - 1) as f64).sqrt()
        }
    }
}

// ---------------------------------------------------------------------------
//  OptimizationRouter
// ---------------------------------------------------------------------------

/// The sinks an [`OptimizationRouter`] forwards to; edit it between runs.
pub type SinkList = Arc<Mutex<Vec<Arc<dyn DocumentSink>>>>;

/// A matching run in flight: the sinks captured at its start and the uids
/// of its `primary` descriptors.
struct RoutedRun {
    sinks: Vec<Arc<dyn DocumentSink>>,
    primary: HashSet<String>,
}

#[derive(Default)]
struct RouterState {
    runs: HashMap<String, RoutedRun>,
    /// Accepted descriptor uid → run start uid.
    descriptors: HashMap<String, String>,
}

/// Forwards the documents of `optimize` and `sample_suggestions` runs to a
/// list of sinks — blop's `OptimizationCallbackRouter` with its
/// `_PrimaryStreamFilter`. Descriptors of other streams (a `baseline`
/// from [`SupplementalData`](crate::plans::preprocessors::SupplementalData),
/// say) and their events are dropped; start and stop always pass. The
/// list is read at each matching start, so a sink added between runs
/// takes part in the next one.
pub struct OptimizationRouter {
    sinks: SinkList,
    state: Mutex<RouterState>,
}

impl OptimizationRouter {
    /// Route to `sinks`.
    pub fn new(sinks: SinkList) -> Self {
        Self {
            sinks,
            state: Mutex::new(RouterState::default()),
        }
    }

    /// Route to these sinks; [`sinks`](Self::sinks) gives the list to edit.
    pub fn with_sinks(sinks: impl IntoIterator<Item = Arc<dyn DocumentSink>>) -> Self {
        Self::new(Arc::new(Mutex::new(sinks.into_iter().collect())))
    }

    /// The list forwarded to.
    pub fn sinks(&self) -> SinkList {
        self.sinks.clone()
    }

    /// Whether a start document opens a run the router forwards.
    fn matches(start: &RunStart) -> bool {
        matches!(
            start.extra.get("run_key").and_then(Value::as_str),
            Some(OPTIMIZE_RUN_KEY) | Some(SAMPLE_SUGGESTIONS_RUN_KEY)
        )
    }

    /// The sinks a document goes to, or none.
    fn targets(&self, doc: &Document) -> Vec<Arc<dyn DocumentSink>> {
        let mut state = self.state.lock().unwrap();
        match doc {
            Document::Start(start) if Self::matches(start) => {
                let sinks = self.sinks.lock().unwrap().clone();
                state.runs.insert(
                    start.uid.clone(),
                    RoutedRun {
                        sinks: sinks.clone(),
                        primary: HashSet::new(),
                    },
                );
                sinks
            }
            Document::Descriptor(desc) => {
                let Some(run) = state.runs.get_mut(&desc.run_start) else {
                    return Vec::new();
                };
                if desc.name.as_deref() != Some("primary") {
                    return Vec::new();
                }
                run.primary.insert(desc.uid.clone());
                let sinks = run.sinks.clone();
                state
                    .descriptors
                    .insert(desc.uid.clone(), desc.run_start.clone());
                sinks
            }
            Document::Event(event) => state
                .descriptors
                .get(&event.descriptor)
                .and_then(|run| state.runs.get(run))
                .map(|run| run.sinks.clone())
                .unwrap_or_default(),
            Document::Stop(stop) => {
                let Some(run) = state.runs.remove(&stop.run_start) else {
                    return Vec::new();
                };
                for uid in run.primary {
                    state.descriptors.remove(&uid);
                }
                run.sinks
            }
            _ => Vec::new(),
        }
    }
}

#[async_trait]
impl DocumentSink for OptimizationRouter {
    async fn dispatch(&self, doc: &Document) -> Result<()> {
        for sink in self.targets(doc) {
            sink.dispatch(doc).await?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
//  OptimizationLogger
// ---------------------------------------------------------------------------

/// Prints optimization progress as plain text — blop's `OptimizationLogger`
/// without `rich`: a header at each start, a table of the parameters and
/// outcomes of every iteration with the outcomes' running min/max/mean,
/// and a summary statistics table at stop. Iteration counts and statistics
/// accumulate across consecutive runs, as blop's do.
pub struct OptimizationLogger {
    out: Mutex<Box<dyn Write + Send>>,
    state: Mutex<LoggerState>,
}

#[derive(Default)]
struct LoggerState {
    parameter_keys: Vec<String>,
    outcome_keys: Vec<String>,
    total_iterations: Option<usize>,
    current_iteration: usize,
    stats: BTreeMap<String, RunningStats>,
}

impl OptimizationLogger {
    /// Print to `out`.
    pub fn new(out: impl Write + Send + 'static) -> Self {
        Self {
            out: Mutex::new(Box::new(out)),
            state: Mutex::new(LoggerState::default()),
        }
    }

    /// Print to stderr.
    pub fn stderr() -> Self {
        Self::new(std::io::stderr())
    }

    fn render(&self, doc: &Document) -> String {
        let mut state = self.state.lock().unwrap();
        match doc {
            Document::Start(start) => render_start(&mut state, start),
            Document::Descriptor(desc) => {
                cache_keys(&mut state, desc);
                String::new()
            }
            Document::Event(event) => render_event(&mut state, event),
            Document::Stop(stop) => render_stop(&mut state, stop),
            _ => String::new(),
        }
    }
}

#[async_trait]
impl DocumentSink for OptimizationLogger {
    async fn dispatch(&self, doc: &Document) -> Result<()> {
        let text = self.render(doc);
        if text.is_empty() {
            return Ok(());
        }
        let mut out = self.out.lock().unwrap();
        out.write_all(text.as_bytes())
            .and_then(|_| out.flush())
            .map_err(|e| BsrsError::Backend(format!("optimization logger: {e}")))
    }
}

fn rule(label: &str) -> String {
    format!("---- {label} ----\n")
}

fn names(value: Option<&Value>) -> String {
    let names: Vec<&str> = value
        .and_then(Value::as_array)
        .map(|items| items.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    if names.is_empty() {
        "N/A".to_owned()
    } else {
        names.join(", ")
    }
}

fn render_start(state: &mut LoggerState, start: &RunStart) -> String {
    let md = &start.extra;
    let iterations = md
        .get("iterations")
        .and_then(Value::as_u64)
        .map(|n| n as usize);
    let n_points = md.get("n_points").and_then(Value::as_u64).unwrap_or(1);
    let optimizer = md
        .get("optimizer")
        .and_then(Value::as_str)
        .unwrap_or("Unknown");
    state.total_iterations = iterations.map(|n| state.current_iteration + n);

    let mut text = String::new();
    text.push_str("\n== Optimization ==\n");
    text.push_str(&format!("Optimizer   {optimizer}\n"));
    text.push_str(&format!("Actuators   {}\n", names(md.get("actuators"))));
    text.push_str(&format!("Sensors     {}\n", names(md.get("sensors"))));
    text.push_str("Iterations  ");
    match iterations {
        None => {
            text.push_str("Until stopping criterion");
            if state.current_iteration > 0 {
                text.push_str(&format!(" ({} completed)", state.current_iteration));
            }
        }
        Some(n) if state.current_iteration > 0 => text.push_str(&format!(
            "{n} more ({} completed, {} total)",
            state.current_iteration,
            state.total_iterations.unwrap_or(n)
        )),
        Some(n) => text.push_str(&n.to_string()),
    }
    if n_points > 1 {
        text.push_str(&format!("  Points/iter {n_points}"));
    }
    text.push('\n');
    if !start.uid.is_empty() {
        text.push_str(&format!("Run UID     {}\n", start.uid));
    }
    text
}

fn cache_keys(state: &mut LoggerState, desc: &EventDescriptor) {
    let mut by_source: BTreeMap<Source, Vec<String>> = BTreeMap::new();
    for (key, data_key) in &desc.data_keys {
        by_source
            .entry(Source::from_data_key_source(&data_key.source))
            .or_default()
            .push(key.clone());
    }
    let mut take = |source| {
        let mut keys = by_source.remove(&source).unwrap_or_default();
        keys.sort();
        keys
    };
    state.parameter_keys = take(Source::Parameter);
    state.outcome_keys = take(Source::Outcome);
}

/// A scalar as a one-element list, an array as its items.
fn to_list(value: &Value) -> Vec<Value> {
    match value {
        Value::Array(items) => items.clone(),
        other => vec![other.clone()],
    }
}

/// Six significant figures for a float (Python's `:.6g`), everything else
/// as is; a `null` (bsrs's NaN pad) prints as `nan`.
fn format_value(value: &Value) -> String {
    match value {
        Value::Null => "nan".to_owned(),
        Value::Number(n) if n.is_f64() => sig6(n.as_f64().unwrap_or(f64::NAN)),
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn format_stat(value: f64) -> String {
    if value.is_finite() {
        sig6(value)
    } else {
        "--".to_owned()
    }
}

/// Python's `f"{v:.6g}"`.
fn sig6(v: f64) -> String {
    if !v.is_finite() {
        return if v.is_nan() {
            "nan".to_owned()
        } else if v > 0.0 {
            "inf".to_owned()
        } else {
            "-inf".to_owned()
        };
    }
    if v == 0.0 {
        return "0".to_owned();
    }
    let sci = format!("{v:.5e}");
    let (mantissa, exponent) = sci.split_once('e').expect("scientific form");
    let exponent: i32 = exponent.parse().expect("exponent");
    if !(-4..6).contains(&exponent) {
        let sign = if exponent < 0 { '-' } else { '+' };
        return format!(
            "{}e{sign}{:02}",
            trim_zeros(mantissa),
            exponent.unsigned_abs()
        );
    }
    let decimals = (5 - exponent).max(0) as usize;
    trim_zeros(&format!("{v:.decimals$}"))
}

fn trim_zeros(s: &str) -> String {
    if !s.contains('.') {
        return s.to_owned();
    }
    s.trim_end_matches('0').trim_end_matches('.').to_owned()
}

fn is_padding(sid: &Value) -> bool {
    match sid {
        Value::Null => true,
        Value::String(s) => s.trim().is_empty(),
        _ => false,
    }
}

/// Right-justified columns, one header row, one blank between columns.
fn table(headers: &[String], rows: &[Vec<String>]) -> String {
    let widths: Vec<usize> = headers
        .iter()
        .enumerate()
        .map(|(i, h)| {
            rows.iter()
                .map(|r| r.get(i).map_or(0, String::len))
                .chain(std::iter::once(h.len()))
                .max()
                .unwrap_or(0)
        })
        .collect();
    let line = |cells: &[String]| {
        let cells: Vec<String> = cells
            .iter()
            .zip(&widths)
            .map(|(c, w)| format!("{c:>w$}"))
            .collect();
        format!("  {}\n", cells.join("  "))
    };
    let mut text = line(headers);
    for row in rows {
        text.push_str(&line(row));
    }
    text
}

fn render_event(state: &mut LoggerState, event: &Event) -> String {
    let data = &event.data;
    if data.is_empty() {
        return String::new();
    }
    state.current_iteration += 1;

    let columns = |keys: &[String]| -> Vec<(String, Vec<Value>)> {
        keys.iter()
            .filter_map(|k| data.get(k).map(|v| (k.clone(), to_list(v))))
            .collect()
    };
    let param_columns = columns(&state.parameter_keys);
    let outcome_columns = columns(&state.outcome_keys);

    let suggestion_ids = data
        .get(SUGGESTION_IDS_KEY)
        .map(to_list)
        .unwrap_or_default();
    let acquire_uid = data
        .get(ACQUISITION_UID_KEY)
        .map(|v| match v {
            Value::Array(items) => items.first().cloned().unwrap_or(Value::Null),
            other => other.clone(),
        })
        .filter(|v| !v.is_null());

    let n_total = param_columns
        .iter()
        .chain(&outcome_columns)
        .map(|(_, v)| v.len())
        .max()
        .unwrap_or(1);
    let valid: Vec<usize> = if suggestion_ids.is_empty() {
        (0..n_total).collect()
    } else {
        (0..suggestion_ids.len())
            .filter(|&i| !is_padding(&suggestion_ids[i]))
            .collect()
    };
    let n_valid = if valid.is_empty() {
        n_total
    } else {
        valid.len()
    };

    for (key, values) in param_columns.iter().chain(&outcome_columns) {
        for &i in &valid {
            if let Some(v) = values.get(i).and_then(Value::as_f64) {
                state.stats.entry(key.clone()).or_default().update(v);
            }
        }
    }

    let mut label = format!("Iteration {}", state.current_iteration);
    if let Some(total) = state.total_iterations {
        label.push_str(&format!(" / {total}"));
    }
    if n_valid > 1 {
        label.push_str(&format!("  ({n_valid} points)"));
    }
    let mut text = String::from("\n");
    text.push_str(&rule(&label));
    if let Some(uid) = acquire_uid {
        text.push_str(&format!("  Acquire UID  {}\n", format_value(&uid)));
    }

    let mut headers = vec!["Event".to_owned(), "Suggestion ID".to_owned()];
    headers.extend(
        param_columns
            .iter()
            .chain(&outcome_columns)
            .map(|(k, _)| k.clone()),
    );
    let rows: Vec<Vec<String>> = valid
        .iter()
        .enumerate()
        .map(|(row, &i)| {
            let mut cells = vec![
                row.to_string(),
                suggestion_ids.get(i).map(format_value).unwrap_or_default(),
            ];
            cells.extend(
                param_columns
                    .iter()
                    .chain(&outcome_columns)
                    .map(|(_, values)| values.get(i).map(format_value).unwrap_or_default()),
            );
            cells
        })
        .collect();
    text.push_str(&table(&headers, &rows));

    let tracked: Vec<(&String, &RunningStats)> = state
        .outcome_keys
        .iter()
        .filter_map(|k| state.stats.get(k).filter(|s| s.count > 0).map(|s| (k, s)))
        .collect();
    if let Some((_, first)) = tracked.first() {
        for (key, s) in &tracked {
            text.push_str(&format!(
                "  {key}  min: {}  max: {}  mean: {}\n",
                format_stat(s.min),
                format_stat(s.max),
                format_stat(s.mean())
            ));
        }
        text.push_str(&format!("  ({} pts sampled)\n", first.count));
    }
    text
}

fn render_stop(state: &mut LoggerState, stop: &RunStop) -> String {
    let mut text = String::new();
    let rows: Vec<Vec<String>> = state
        .parameter_keys
        .iter()
        .map(|k| (k, "param"))
        .chain(state.outcome_keys.iter().map(|k| (k, "outcome")))
        .filter_map(|(key, kind)| {
            let s = state.stats.get(key).filter(|s| s.count > 0)?;
            Some(vec![
                key.clone(),
                kind.to_owned(),
                format_stat(s.min),
                format_stat(s.max),
                format_stat(s.mean()),
                format_stat(s.std()),
                s.count.to_string(),
            ])
        })
        .collect();
    if !rows.is_empty() {
        text.push_str("\nSummary Statistics\n");
        let headers: Vec<String> = ["Name", "Type", "Min", "Max", "Mean", "Std", "Count"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        text.push_str(&table(&headers, &rows));
    }
    let reason = stop.reason.as_deref().unwrap_or("");
    let label = match stop.exit_status {
        ExitStatus::Success => "Optimization Complete".to_owned(),
        ExitStatus::Abort if reason.is_empty() => "Optimization Aborted".to_owned(),
        ExitStatus::Abort => format!("Optimization Aborted  ({reason})"),
        other if reason.is_empty() => format!("Optimization Stopped  ({other})"),
        other => format!("Optimization Stopped  ({other})  {reason}"),
    };
    text.push_str(&rule(&label));
    text.push('\n');
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn running_stats_empty() {
        let s = RunningStats::new();
        assert_eq!(s.count, 0);
        assert_eq!(s.min, f64::INFINITY);
        assert_eq!(s.max, f64::NEG_INFINITY);
        assert!(s.mean().is_nan());
        assert!(s.std().is_nan());
    }

    #[test]
    fn running_stats_single_value() {
        let mut s = RunningStats::new();
        s.update(5.0);
        assert_eq!((s.count, s.min, s.max, s.mean()), (1, 5.0, 5.0, 5.0));
        assert!(s.std().is_nan());
    }

    #[test]
    fn running_stats_two_identical_values() {
        let mut s = RunningStats::new();
        s.update(3.0);
        s.update(3.0);
        assert_eq!((s.count, s.mean(), s.std()), (2, 3.0, 0.0));
    }

    fn close(a: f64, b: f64) -> bool {
        (a - b).abs() <= 1e-9 * a.abs().max(b.abs()).max(1.0)
    }

    #[test]
    fn running_stats_known_values() {
        let values = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        let mut s = RunningStats::new();
        values.iter().for_each(|&v| s.update(v));
        assert_eq!((s.count, s.min, s.max), (8, 2.0, 9.0));
        assert!(close(s.mean(), 5.0));
        assert!(close(s.std(), (32.0f64 / 7.0).sqrt()));
    }

    #[test]
    fn running_stats_large_offset() {
        let values = [1e9 + 1.0, 1e9 + 2.0, 1e9 + 3.0];
        let mut s = RunningStats::new();
        values.iter().for_each(|&v| s.update(v));
        assert!(close(s.mean(), 1e9 + 2.0));
        assert!(close(s.std(), 1.0));
    }

    #[test]
    fn running_stats_negative_values() {
        let mut s = RunningStats::new();
        [-10.0, -5.0, 0.0, 5.0, 10.0]
            .iter()
            .for_each(|&v| s.update(v));
        assert_eq!((s.min, s.max), (-10.0, 10.0));
        assert!(close(s.mean(), 0.0));
        assert!(close(s.std(), (250.0f64 / 4.0).sqrt()));
    }

    #[test]
    fn running_stats_skips_nan_and_inf() {
        let mut s = RunningStats::new();
        for v in [1.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 3.0] {
            s.update(v);
        }
        assert_eq!((s.count, s.min, s.max), (2, 1.0, 3.0));
        assert!(close(s.mean(), 2.0));
    }

    #[test]
    fn sig6_matches_python_g() {
        for (v, want) in [
            (1.5, "1.5"),
            (3.24159265, "3.24159"),
            (1234567.0, "1.23457e+06"),
            (0.0001234567, "0.000123457"),
            (0.00001234567, "1.23457e-05"),
            (100.0, "100"),
            (-2.5, "-2.5"),
            (999999.5, "1e+06"),
            (0.0, "0"),
        ] {
            assert_eq!(sig6(v), want, "{v}");
        }
        assert_eq!(sig6(f64::NAN), "nan");
        assert_eq!(sig6(f64::INFINITY), "inf");
    }
}
