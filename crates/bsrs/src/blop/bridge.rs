//! An [`Optimizer`] served by another process over ZMQ REQ/REP — the way to
//! drive Ax (`blop.ax.optimizer.AxOptimizer`) or any other blop optimizer
//! from bsrs without porting it (feature `blop-bridge`).
//!
//! The server side is the `bsrs_blop_bridge` Python package under
//! `python/`: `python -m bsrs_blop_bridge --endpoint tcp://127.0.0.1:5590
//! --backend ax`. Each optimizer call is one request frame and one reply
//! frame of JSON:
//!
//! ```text
//! {"method": "suggest", "params": {"n": 2}}
//! {"result": [{"_id": 0, "x": 1.5}, {"_id": 1, "x": 7.0}]}
//! {"error": "RuntimeError: ..."}
//! ```
//!
//! [`BridgeOptimizer::connect`] sends the agent's
//! [`ExperimentSpec`] as `configure` and learns the server's optimizer
//! name and which optional protocols it implements; those answer the
//! `supports_*` methods of [`Optimizer`]. The calls block the caller for up
//! to [`BridgeOptions::timeout`]; on a multi-thread tokio runtime they run
//! under `block_in_place` so the worker is released, on a current-thread
//! runtime (`#[tokio::test]`) they block it, as no other task can make
//! progress inside a plan step anyway.
//!
//! A reply that does not arrive in time fails that call and leaves the
//! socket usable: the REQ socket runs with `ZMQ_REQ_RELAXED` and
//! `ZMQ_REQ_CORRELATE`, so the next request goes out regardless and a late
//! reply to the old one is dropped.

use std::sync::Mutex;
use std::time::Duration;

use serde::Deserialize;
use serde_json::{json, Value};

use crate::blop::agent::ExperimentSpec;
use crate::blop::protocol::{
    shared, BestPoint, Candidate, Optimizer, Outcome, Params, SharedOptimizer, Suggestion, TrialId,
    ID_KEY,
};
use crate::core::error::{BsrsError, Result};

/// How to reach and configure the served optimizer.
#[derive(Clone, Debug)]
pub struct BridgeOptions {
    /// Passed to the server's optimizer factory as `backend`: for the
    /// `random` backend `{"seed": n}`, for `ax` `checkpoint_path`,
    /// `client_kwargs` and `configure_experiment` keywords.
    pub backend: Value,
    /// How long one request may take, `suggest` included; Ax fits a model
    /// before suggesting, which can take minutes on a large experiment.
    pub timeout: Duration,
}

impl Default for BridgeOptions {
    fn default() -> Self {
        Self {
            backend: json!({}),
            timeout: Duration::from_secs(300),
        }
    }
}

/// Which optional blop protocols the served optimizer implements, as
/// reported by `configure`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
pub struct Capabilities {
    /// `CanRegisterSuggestions`.
    #[serde(default)]
    pub register_suggestions: bool,
    /// `SupportsStoppingCriteria`.
    #[serde(default)]
    pub stopping_criteria: bool,
    /// `Checkpointable`.
    #[serde(default)]
    pub checkpoint: bool,
}

#[derive(Deserialize)]
struct Configured {
    name: String,
    #[serde(default)]
    capabilities: Capabilities,
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum Reply {
    Result(Value),
    Error(String),
}

/// The client end of the bridge; one per served optimizer.
pub struct BridgeOptimizer {
    _context: zmq::Context,
    socket: Mutex<zmq::Socket>,
    endpoint: String,
    timeout: Duration,
    name: String,
    capabilities: Capabilities,
}

impl std::fmt::Debug for BridgeOptimizer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BridgeOptimizer")
            .field("endpoint", &self.endpoint)
            .field("name", &self.name)
            .field("capabilities", &self.capabilities)
            .finish()
    }
}

impl BridgeOptimizer {
    /// Connect to the server at `endpoint` and configure its optimizer
    /// with `spec`. Fails if the server does not answer `configure` within
    /// the timeout, or its optimizer rejects the spec.
    pub fn connect(endpoint: &str, spec: &ExperimentSpec, options: BridgeOptions) -> Result<Self> {
        let context = zmq::Context::new();
        let socket = context
            .socket(zmq::REQ)
            .map_err(|e| Self::zmq_error(endpoint, "socket", e))?;
        let millis = i32::try_from(options.timeout.as_millis()).unwrap_or(i32::MAX);
        socket
            .set_linger(0)
            .and_then(|_| socket.set_rcvtimeo(millis))
            .and_then(|_| socket.set_sndtimeo(millis))
            .and_then(|_| socket.set_req_relaxed(true))
            .and_then(|_| socket.set_req_correlate(true))
            .and_then(|_| socket.connect(endpoint))
            .map_err(|e| Self::zmq_error(endpoint, "connect", e))?;
        let mut this = Self {
            _context: context,
            socket: Mutex::new(socket),
            endpoint: endpoint.to_owned(),
            timeout: options.timeout,
            name: String::new(),
            capabilities: Capabilities::default(),
        };
        let reply = this.call(
            "configure",
            json!({ "spec": spec, "backend": options.backend }),
        )?;
        let configured: Configured = serde_json::from_value(reply)
            .map_err(|e| this.error("configure", format!("malformed reply: {e}")))?;
        this.name = configured.name;
        this.capabilities = configured.capabilities;
        Ok(this)
    }

    /// An optimizer factory for
    /// [`AgentBuilder::build`](crate::blop::agent::AgentBuilder::build):
    /// connects to `endpoint` with the agent's spec.
    pub fn factory(
        endpoint: impl Into<String>,
        options: BridgeOptions,
    ) -> impl FnOnce(&ExperimentSpec) -> Result<SharedOptimizer> {
        let endpoint = endpoint.into();
        move |spec| Ok(shared(Self::connect(&endpoint, spec, options)?))
    }

    /// The endpoint this client is connected to.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// What the server reported at `configure`.
    pub fn capabilities(&self) -> Capabilities {
        self.capabilities
    }

    /// Send `method` with `params` and wait for the reply's `result`.
    pub fn call(&self, method: &str, params: Value) -> Result<Value> {
        let request = serde_json::to_vec(&json!({ "method": method, "params": params }))
            .map_err(|e| self.error(method, e.to_string()))?;
        let bytes = blocking(|| {
            let socket = self.socket.lock().unwrap();
            socket
                .send(request, 0)
                .and_then(|_| socket.recv_bytes(0))
                .map_err(|e| match e {
                    zmq::Error::EAGAIN => {
                        self.error(method, format!("no reply within {:?}", self.timeout))
                    }
                    other => self.error(method, other.to_string()),
                })
        })?;
        match serde_json::from_slice::<Reply>(&bytes) {
            Ok(Reply::Result(value)) => Ok(value),
            Ok(Reply::Error(message)) => Err(self.error(method, message)),
            Err(e) => Err(self.error(method, format!("malformed reply: {e}"))),
        }
    }

    fn error(&self, method: &str, message: impl std::fmt::Display) -> BsrsError {
        BsrsError::Backend(format!(
            "blop bridge {method} at {}: {message}",
            self.endpoint
        ))
    }

    fn zmq_error(endpoint: &str, what: &str, e: zmq::Error) -> BsrsError {
        BsrsError::Backend(format!("blop bridge {what} at {endpoint}: {e}"))
    }

    fn suggestions(&self, method: &str, reply: Value) -> Result<Vec<Suggestion>> {
        let items = match reply {
            Value::Array(items) => items,
            other => return Err(self.error(method, format!("expected a list, got {other}"))),
        };
        items
            .into_iter()
            .map(|item| {
                let (id, params) = split_id(item).map_err(|m| self.error(method, m))?;
                Ok(Suggestion { id, params })
            })
            .collect()
    }
}

/// Run `f`, releasing the tokio worker while it blocks when there is one to
/// release.
fn blocking<T>(f: impl FnOnce() -> T) -> T {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(f)
        }
        _ => f(),
    }
}

fn trial_id(value: &Value) -> std::result::Result<TrialId, String> {
    match value {
        Value::Number(n) => n
            .as_i64()
            .map(TrialId::Int)
            .ok_or_else(|| format!("non-integer id {n}")),
        Value::String(s) => Ok(TrialId::Str(s.clone())),
        other => Err(format!("id must be an integer or string, got {other}")),
    }
}

/// `{"_id": id, k: v, …}` → `(id, {k: v, …})`.
fn split_id(item: Value) -> std::result::Result<(TrialId, Params), String> {
    let Value::Object(mut map) = item else {
        return Err(format!("expected a mapping, got {item}"));
    };
    let id = map
        .remove(ID_KEY)
        .ok_or_else(|| format!("mapping without {ID_KEY:?}"))?;
    Ok((trial_id(&id)?, map.into_iter().collect()))
}

fn outcome_value(outcome: &Outcome) -> Value {
    let mut map = serde_json::Map::new();
    map.insert(ID_KEY.into(), Value::from(&outcome.id));
    for (k, v) in &outcome.values {
        map.insert(k.clone(), v.clone());
    }
    Value::Object(map)
}

impl Optimizer for BridgeOptimizer {
    fn name(&self) -> String {
        self.name.clone()
    }

    fn suggest(&mut self, n: usize) -> Result<Vec<Suggestion>> {
        let reply = self.call("suggest", json!({ "n": n }))?;
        self.suggestions("suggest", reply)
    }

    fn ingest(&mut self, outcomes: Vec<Outcome>) -> Result<()> {
        let points: Vec<Value> = outcomes.iter().map(outcome_value).collect();
        self.call("ingest", json!({ "points": points })).map(|_| ())
    }

    fn best_points(&self) -> Result<Vec<BestPoint>> {
        #[derive(Deserialize)]
        struct Item {
            #[serde(rename = "_id")]
            id: Value,
            params: Params,
            outcomes: Params,
        }
        let items: Vec<Item> = serde_json::from_value(self.call("best_points", json!({}))?)
            .map_err(|e| self.error("best_points", format!("malformed reply: {e}")))?;
        items
            .into_iter()
            .map(|item| {
                Ok(BestPoint {
                    id: trial_id(&item.id).map_err(|m| self.error("best_points", m))?,
                    params: item.params,
                    outcomes: item.outcomes,
                })
            })
            .collect()
    }

    fn supports_register_suggestions(&self) -> bool {
        self.capabilities.register_suggestions
    }

    fn register_suggestions(&mut self, candidates: Vec<Candidate>) -> Result<Vec<Suggestion>> {
        let suggestions: Vec<&Params> = candidates.iter().map(|c| &c.params).collect();
        let reply = self.call(
            "register_suggestions",
            json!({ "suggestions": suggestions }),
        )?;
        self.suggestions("register_suggestions", reply)
    }

    fn register_failures(&mut self, suggestions: &[Suggestion]) {
        let suggestions: Vec<Value> = suggestions.iter().map(Suggestion::to_value).collect();
        if let Err(e) = self.call("register_failures", json!({ "suggestions": suggestions })) {
            tracing::warn!("{e}");
        }
    }

    fn supports_stopping_criteria(&self) -> bool {
        self.capabilities.stopping_criteria
    }

    fn should_stop(&self) -> (bool, Option<String>) {
        match self.call("should_stop", json!({})).and_then(|reply| {
            serde_json::from_value::<(bool, Option<String>)>(reply)
                .map_err(|e| self.error("should_stop", format!("malformed reply: {e}")))
        }) {
            Ok(answer) => answer,
            Err(e) => {
                tracing::warn!("{e}");
                (false, None)
            }
        }
    }

    fn supports_checkpoint(&self) -> bool {
        self.capabilities.checkpoint
    }

    fn checkpoint(&mut self) -> Result<()> {
        self.call("checkpoint", json!({})).map(|_| ())
    }
}
