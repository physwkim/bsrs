//! Readables that carry the optimization step into the event stream — bsrs's
//! port of blop's `InferredReadable`.
//!
//! [`read_step`](super::stubs::read_step) keeps one [`TrackingReadable`] per
//! recorded key (each parameter, each outcome, `suggestion_ids`,
//! `acquisition_uid`) in a [`ReadableCache`], updates their values every
//! iteration, and reads them into one event. The data key is inferred from
//! the first value and cached, as the descriptor is emitted once per stream.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, OnceLock};

use async_trait::async_trait;
use serde_json::Value;

use crate::core::error::Result;
use crate::core::msg::{NamedObj, ReadableObj};
use crate::core::reading::ReadingValue;
use crate::event_model::{DataKey, Dtype, DtypeNumpy};

/// Where a tracked key's data comes from; recorded as the data key's
/// `source` so downstream callbacks can tell parameters from outcomes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Source {
    /// An objective or constraint value produced by the evaluation function.
    Outcome,
    /// A parameter value the optimizer suggested.
    Parameter,
    /// The `suggestion_ids` field.
    SuggestionId,
    /// The `acquisition_uid` field.
    AcquisitionUid,
    /// Anything else.
    Other,
}

impl Source {
    /// The blop `Source` enum value.
    pub fn as_str(self) -> &'static str {
        match self {
            Source::Outcome => "optimization-outcome",
            Source::Parameter => "optimization-parameter",
            Source::SuggestionId => "optimization-suggestion-id",
            Source::AcquisitionUid => "optimization-acquisition-uid",
            Source::Other => "optimization-other",
        }
    }
}

/// A readable whose value is set by the plan and whose data key is inferred
/// from the first value it was given.
pub struct TrackingReadable {
    name: String,
    source: Source,
    value: Mutex<Value>,
    data_key: OnceLock<DataKey>,
}

impl TrackingReadable {
    /// Create with an initial value. A one-element array collapses to its
    /// element, as blop's does, so a single-point step reads as a scalar.
    pub fn new(name: impl Into<String>, source: Source, initial: Value) -> Self {
        Self {
            name: name.into(),
            source,
            value: Mutex::new(collapse(initial)),
            data_key: OnceLock::new(),
        }
    }

    /// Replace the stored value.
    pub fn update(&self, value: Value) {
        *self.value.lock().unwrap() = collapse(value);
    }

    /// The stored value.
    pub fn value(&self) -> Value {
        self.value.lock().unwrap().clone()
    }

    /// The data key's source.
    pub fn source(&self) -> Source {
        self.source
    }
}

fn collapse(value: Value) -> Value {
    match value {
        Value::Array(mut items) if items.len() == 1 => items.remove(0),
        other => other,
    }
}

/// Infer the data key from a value: an array of more than one element is an
/// `array` of that shape, a single element or scalar is a `number`,
/// `integer`, `boolean` or `string`. Strings reserve room for a UUID
/// (`<U36`), as the description is cached across updates.
fn infer_data_key(source: Source, value: &Value) -> DataKey {
    let (dtype, shape, element) = match value {
        Value::Array(items) if items.len() != 1 => (
            Dtype::Array,
            vec![Some(items.len() as u64)],
            items.first().cloned().unwrap_or(Value::Null),
        ),
        Value::Array(items) => (scalar_dtype(&items[0]), vec![], items[0].clone()),
        other => (scalar_dtype(other), vec![], other.clone()),
    };
    // An array's numpy dtype is that of its elements; a NaN pad (`Null`)
    // among numbers must not turn a numeric column into an object column,
    // so the first non-null element decides.
    let element = match (&element, value) {
        (Value::Null, Value::Array(items)) => items
            .iter()
            .find(|v| !v.is_null())
            .cloned()
            .unwrap_or(Value::Null),
        _ => element,
    };
    DataKey {
        source: source.as_str().to_owned(),
        dtype,
        shape,
        dtype_numpy: Some(DtypeNumpy::Scalar(numpy_dtype(&element).to_owned())),
        external: None,
        units: None,
        precision: None,
        object_name: None,
        dims: None,
        limits: None,
        choices: None,
    }
}

fn scalar_dtype(value: &Value) -> Dtype {
    match value {
        Value::Number(n) if n.is_f64() => Dtype::Number,
        Value::Number(_) => Dtype::Integer,
        Value::Bool(_) => Dtype::Boolean,
        _ => Dtype::String,
    }
}

fn numpy_dtype(value: &Value) -> &'static str {
    match value {
        Value::Number(n) if n.is_f64() => "<f8",
        Value::Number(_) => "<i8",
        Value::Bool(_) => "|b1",
        _ => "<U36",
    }
}

impl NamedObj for TrackingReadable {
    fn name(&self) -> &str {
        &self.name
    }

    fn inspect_dyn(&self) -> Value {
        serde_json::json!({
            "name": self.name,
            "type": "TrackingReadable",
            "source": self.source.as_str(),
            "value": self.value(),
        })
    }
}

#[async_trait]
impl ReadableObj for TrackingReadable {
    async fn read_dyn(&self) -> Result<HashMap<String, ReadingValue>> {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);
        Ok(HashMap::from([(
            self.name.clone(),
            ReadingValue::new(self.value(), ts),
        )]))
    }

    async fn describe_dyn(&self) -> Result<HashMap<String, DataKey>> {
        let key = self
            .data_key
            .get_or_init(|| infer_data_key(self.source, &self.value()))
            .clone();
        Ok(HashMap::from([(self.name.clone(), key)]))
    }

    fn hint_fields(&self) -> Option<Vec<String>> {
        Some(vec![self.name.clone()])
    }
}

/// The per-key readables an optimization plan reuses across iterations —
/// blop's `readable_cache`. Pass the same cache to several plans to keep one
/// descriptor shape across them.
pub type ReadableCache = Arc<Mutex<BTreeMap<String, Arc<TrackingReadable>>>>;

/// An empty cache.
pub fn readable_cache() -> ReadableCache {
    Arc::new(Mutex::new(BTreeMap::new()))
}

/// Set `name` to `value` in `cache`, creating the readable on first sight.
pub fn track(cache: &ReadableCache, name: &str, source: Source, value: Value) {
    let mut cache = cache.lock().unwrap();
    match cache.get(name) {
        Some(r) => r.update(value),
        None => {
            cache.insert(
                name.to_owned(),
                Arc::new(TrackingReadable::new(name, source, value)),
            );
        }
    }
}

/// Every readable in `cache`, as the objects to read into one event.
pub fn tracked_readables(cache: &ReadableCache) -> Vec<Arc<dyn ReadableObj>> {
    cache
        .lock()
        .unwrap()
        .values()
        .map(|r| r.clone() as Arc<dyn ReadableObj>)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_element_array_collapses_to_scalar() {
        let r = TrackingReadable::new("x", Source::Parameter, serde_json::json!([0.5]));
        assert_eq!(r.value(), serde_json::json!(0.5));
        r.update(serde_json::json!(["a"]));
        assert_eq!(r.value(), serde_json::json!("a"));
    }

    #[tokio::test]
    async fn data_key_is_inferred_once_from_the_first_value() {
        let r = TrackingReadable::new("x", Source::Outcome, serde_json::json!([0.0, 1.5]));
        let dk = r.describe_dyn().await.unwrap().remove("x").unwrap();
        assert_eq!(dk.dtype, Dtype::Array);
        assert_eq!(dk.shape, vec![Some(2)]);
        assert_eq!(dk.dtype_numpy, Some(DtypeNumpy::Scalar("<f8".into())));
        assert_eq!(dk.source, "optimization-outcome");
        r.update(serde_json::json!("changed"));
        let again = r.describe_dyn().await.unwrap().remove("x").unwrap();
        assert_eq!(again, dk);
    }

    #[tokio::test]
    async fn scalar_kinds() {
        for (value, dtype, numpy) in [
            (serde_json::json!(1.0), Dtype::Number, "<f8"),
            (serde_json::json!(1), Dtype::Integer, "<i8"),
            (serde_json::json!(true), Dtype::Boolean, "|b1"),
            (serde_json::json!("id"), Dtype::String, "<U36"),
        ] {
            let r = TrackingReadable::new("k", Source::Other, value);
            let dk = r.describe_dyn().await.unwrap().remove("k").unwrap();
            assert_eq!(dk.dtype, dtype);
            assert!(dk.shape.is_empty());
            assert_eq!(dk.dtype_numpy, Some(DtypeNumpy::Scalar(numpy.into())));
        }
    }

    #[tokio::test]
    async fn null_padding_keeps_numeric_dtype() {
        let r = TrackingReadable::new("k", Source::Parameter, serde_json::json!([0.5, null]));
        let dk = r.describe_dyn().await.unwrap().remove("k").unwrap();
        assert_eq!(dk.dtype, Dtype::Array);
        assert_eq!(dk.dtype_numpy, Some(DtypeNumpy::Scalar("<f8".into())));
    }
}
