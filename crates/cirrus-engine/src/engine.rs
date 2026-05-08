//! The RunEngine: consumes a `Plan`, dispatches `Msg`, emits `Document`s.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use cirrus_core::error::{CirrusError, Result};
use cirrus_core::msg::{Msg, RunMetadata};
use cirrus_core::plan::{Plan, PlanItem};
use cirrus_core::status::{Status, StatusError};
use cirrus_event_model::compose::RunBundle;
use cirrus_event_model::Document;
use futures::StreamExt;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::bundler::RunBundler;
use crate::sink::DocumentSink;

/// Final state of a finished run.
#[derive(Debug, Clone)]
pub struct RunResult {
    /// Run start UID, if a run was opened.
    pub run_uid: Option<String>,
    /// Final exit status (`success` / `abort` / `fail` / `no-run`).
    pub exit_status: String,
}

/// Pending status group bookkeeping.
#[derive(Default)]
struct WaitGroup {
    members: Vec<Status>,
}

/// The RunEngine.
pub struct RunEngine {
    sinks: Vec<Arc<dyn DocumentSink>>,
    cancel: CancellationToken,
    state: Mutex<EngineState>,
}

#[derive(Default)]
struct EngineState {
    bundler: Option<RunBundler>,
    groups: HashMap<String, WaitGroup>,
    staged: Vec<Arc<dyn cirrus_core::msg::StageableObj>>,
    monitors: Vec<(String, Arc<dyn cirrus_core::msg::MonitorableObj>)>,
}

impl RunEngine {
    /// Construct a fresh RunEngine with the given sinks.
    pub fn new(sinks: Vec<Arc<dyn DocumentSink>>) -> Self {
        Self {
            sinks,
            cancel: CancellationToken::new(),
            state: Mutex::new(EngineState::default()),
        }
    }

    /// Async entry point — drive a plan to completion.
    pub async fn run_async(&self, plan: Plan) -> Result<RunResult> {
        let outcome = self.run_loop(plan).await;
        // Cleanup on exit (run_loop's defer-style cleanup is handled inline).
        let mut state = self.state.lock().await;
        // Unstage anything left.
        let staged = std::mem::take(&mut state.staged);
        drop(state);
        for s in staged {
            let _ = s.unstage_dyn().await;
        }
        outcome
    }

    /// Sync entry point — drive a plan via the cirrus runtime.
    /// Must not be called from inside an async task.
    pub fn run_blocking(&self, plan: Plan) -> Result<RunResult> {
        cirrus_core::runtime::block_on(self.run_async(plan))
    }

    /// The main message loop.
    async fn run_loop(&self, mut plan: Plan) -> Result<RunResult> {
        let mut run_uid: Option<String> = None;
        let mut exit_status = String::from("no-run");

        while let Some(item) = plan.next().await {
            if self.cancel.is_cancelled() {
                exit_status = "abort".into();
                break;
            }
            let msg = match item {
                PlanItem::Bare(m) => m,
                _ => continue,
            };
            tracing::debug!("RE msg: {:?}", &msg);
            match self.handle(msg).await {
                Ok(Some(uid)) => {
                    run_uid = Some(uid);
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::error!("plan error: {e}");
                    exit_status = "fail".into();
                    // Best-effort run close if a run is open
                    self.close_run_if_open("fail", Some(format!("{e}"))).await?;
                    return Ok(RunResult {
                        run_uid,
                        exit_status,
                    });
                }
            }
        }

        // If a run is still open at end of plan, close it as success.
        let still_open = {
            let state = self.state.lock().await;
            state.bundler.is_some()
        };
        if still_open {
            self.close_run_if_open("success", None).await?;
            exit_status = "success".into();
        } else if run_uid.is_some() && exit_status == "no-run" {
            exit_status = "success".into();
        }

        Ok(RunResult {
            run_uid,
            exit_status,
        })
    }

    /// Returns the run UID if `OpenRun` was processed.
    async fn handle(&self, msg: Msg) -> Result<Option<String>> {
        match msg {
            Msg::OpenRun(meta) => {
                let uid = self.open_run(meta).await?;
                return Ok(Some(uid));
            }
            Msg::CloseRun {
                exit_status,
                reason,
            } => {
                self.close_run_if_open(&exit_status, reason).await?;
            }
            Msg::Create { stream_name } => {
                let mut state = self.state.lock().await;
                state
                    .bundler
                    .as_mut()
                    .ok_or_else(|| CirrusError::Plan("Create with no open run".into()))?
                    .create(stream_name)?;
            }
            Msg::Save => {
                let docs = {
                    let mut state = self.state.lock().await;
                    state
                        .bundler
                        .as_mut()
                        .ok_or_else(|| CirrusError::Plan("Save with no open run".into()))?
                        .save()?
                };
                for d in docs {
                    self.broadcast(&d).await?;
                }
            }
            Msg::Drop => {
                let mut state = self.state.lock().await;
                state
                    .bundler
                    .as_mut()
                    .ok_or_else(|| CirrusError::Plan("Drop with no open run".into()))?
                    .drop_bundle()?;
            }
            Msg::DeclareStream {
                stream_name,
                data_keys,
            } => {
                let descriptor = {
                    let mut state = self.state.lock().await;
                    state
                        .bundler
                        .as_mut()
                        .ok_or_else(|| CirrusError::Plan("DeclareStream with no open run".into()))?
                        .declare_stream(stream_name, data_keys)?
                };
                self.broadcast(&Document::Descriptor(descriptor)).await?;
            }
            Msg::Read(obj) => {
                let readings = obj.read_dyn().await?;
                let data_keys = obj.describe_dyn().await?;
                let object_name = Some(obj.name().to_string());
                let hint_fields = obj.hint_fields();
                let mut state = self.state.lock().await;
                state
                    .bundler
                    .as_mut()
                    .ok_or_else(|| CirrusError::Plan("Read with no open run".into()))?
                    .add_readings(readings, data_keys, object_name, hint_fields)?;
            }
            Msg::Set { obj, value, group } => {
                let status = obj.set_dyn(value).await;
                self.handle_status(status, group).await?;
            }
            Msg::Trigger { obj, group } => {
                let status = obj.trigger_dyn().await;
                self.handle_status(status, group).await?;
            }
            Msg::Stage(obj) => {
                obj.stage_dyn().await?;
                self.state.lock().await.staged.push(obj);
            }
            Msg::Unstage(obj) => {
                obj.unstage_dyn().await?;
                let mut state = self.state.lock().await;
                state
                    .staged
                    .retain(|o| !Arc::ptr_eq(&(o.clone() as Arc<_>), &(obj.clone() as Arc<_>)));
            }
            Msg::Kickoff { obj, group } => {
                let status = obj.kickoff_dyn().await;
                self.handle_status(status, group).await?;
            }
            Msg::Complete { obj, group } => {
                let status = obj.complete_dyn().await;
                self.handle_status(status, group).await?;
            }
            Msg::Collect { obj, stream_name } => {
                let descs = obj.describe_collect_dyn().await?;
                // Declare any streams not yet declared.
                let new_descriptors: Vec<cirrus_event_model::EventDescriptor> = {
                    let mut state = self.state.lock().await;
                    let bundler = state
                        .bundler
                        .as_mut()
                        .ok_or_else(|| CirrusError::Plan("Collect with no open run".into()))?;
                    let mut out = Vec::new();
                    for (name, dks) in &descs {
                        if bundler.descriptor_uid(name).is_none() {
                            out.push(bundler.declare_stream(name.clone(), dks.clone())?);
                        }
                    }
                    out
                };
                for descriptor in new_descriptors {
                    self.broadcast(&Document::Descriptor(descriptor)).await?;
                }
                let events = obj.collect_dyn().await?;
                for (name, data, timestamps) in events {
                    let stream = stream_name.clone().unwrap_or(name);
                    let ev = {
                        let state = self.state.lock().await;
                        let bundler = state.bundler.as_ref().unwrap();
                        bundler
                            .compose()
                            .event(&stream, data, timestamps)
                            .ok_or_else(|| CirrusError::Plan("event for unknown stream".into()))?
                    };
                    self.broadcast(&Document::Event(ev)).await?;
                }
            }
            Msg::Monitor { obj, name } => {
                // Subscription; engine just notes ownership. Real monitor pump
                // is M4 work — for now we accept and store.
                let _ = obj.subscribe_dyn().await?;
                let stream = name.unwrap_or_else(|| "primary".into());
                self.state.lock().await.monitors.push((stream, obj));
            }
            Msg::Unmonitor(obj) => {
                let mut state = self.state.lock().await;
                state.monitors.retain(|(_, o)| {
                    !Arc::ptr_eq(&(o.clone() as Arc<_>), &(obj.clone() as Arc<_>))
                });
            }
            Msg::Wait {
                group,
                error_on_timeout,
                timeout,
            } => {
                self.wait_group(&group, error_on_timeout, timeout).await?;
            }
            Msg::Sleep(d) => {
                tokio::select! {
                    _ = tokio::time::sleep(d) => {}
                    _ = self.cancel.cancelled() => {
                        return Err(CirrusError::Cancelled);
                    }
                }
            }
            Msg::Checkpoint | Msg::ClearCheckpoint => {
                // No rewind in M0/M1 — accept silently.
            }
            Msg::Pause { defer: _ } => {
                // M0/M1: pause is best-effort — return a Plan error so users see it.
                return Err(CirrusError::Plan("Pause not yet implemented".into()));
            }
            Msg::Configure { obj, args } => {
                obj.configure_dyn(args).await?;
            }
            Msg::Custom { name, .. } => {
                tracing::warn!("ignoring unknown custom Msg: {name}");
            }
            Msg::Null => {}
            _ => {
                tracing::warn!("ignoring unhandled Msg variant");
            }
        }
        Ok(None)
    }

    async fn open_run(&self, meta: RunMetadata) -> Result<String> {
        let start = RunBundle::start(
            meta.scan_id,
            None, // hints come from per-object during bundling
        );
        let mut start_doc = start;
        for (k, v) in meta.extra {
            start_doc.extra.insert(k, v);
        }
        if let Some(plan_name) = meta.plan_name {
            start_doc
                .extra
                .insert("plan_name".into(), serde_json::Value::String(plan_name));
        }
        let bundle = Arc::new(RunBundle::open(&start_doc));
        let uid = start_doc.uid.clone();
        // Emit start
        self.broadcast(&Document::Start(start_doc)).await?;
        // Install bundler
        let mut state = self.state.lock().await;
        if state.bundler.is_some() {
            return Err(CirrusError::Plan(
                "OpenRun while a previous run is still open".into(),
            ));
        }
        state.bundler = Some(RunBundler::new(bundle));
        Ok(uid)
    }

    async fn close_run_if_open(&self, exit_status: &str, reason: Option<String>) -> Result<()> {
        let stop_doc = {
            let mut state = self.state.lock().await;
            state
                .bundler
                .take()
                .map(|bundler| bundler.compose().stop(exit_status, reason))
        };
        if let Some(stop) = stop_doc {
            self.broadcast(&Document::Stop(stop)).await?;
        }
        Ok(())
    }

    async fn broadcast(&self, doc: &Document) -> Result<()> {
        for s in &self.sinks {
            let _ = s.dispatch(doc).await;
        }
        Ok(())
    }

    async fn handle_status(&self, status: Status, group: Option<String>) -> Result<()> {
        match group {
            Some(g) => {
                self.state
                    .lock()
                    .await
                    .groups
                    .entry(g)
                    .or_default()
                    .members
                    .push(status);
                Ok(())
            }
            None => match status.await {
                Ok(()) => Ok(()),
                Err(StatusError::Cancelled) => Err(CirrusError::Cancelled),
                Err(StatusError::Timeout) => Err(CirrusError::Timeout(Duration::from_secs(0))),
                Err(StatusError::Failed(s)) => Err(CirrusError::Backend(s)),
            },
        }
    }

    async fn wait_group(
        &self,
        group: &str,
        error_on_timeout: bool,
        timeout: Option<Duration>,
    ) -> Result<()> {
        let members = {
            let mut state = self.state.lock().await;
            state
                .groups
                .remove(group)
                .map(|g| g.members)
                .unwrap_or_default()
        };
        if members.is_empty() {
            return Ok(());
        }
        let fut = async {
            for s in members {
                if let Err(e) = s.await {
                    if error_on_timeout {
                        return Err(match e {
                            StatusError::Cancelled => CirrusError::Cancelled,
                            StatusError::Timeout => CirrusError::Timeout(Duration::from_secs(0)),
                            StatusError::Failed(s) => CirrusError::Backend(s),
                        });
                    }
                }
            }
            Ok(())
        };
        match timeout {
            Some(d) => match tokio::time::timeout(d, fut).await {
                Ok(r) => r,
                Err(_) => {
                    if error_on_timeout {
                        Err(CirrusError::Timeout(d))
                    } else {
                        Ok(())
                    }
                }
            },
            None => fut.await,
        }
    }
}

impl Default for RunEngine {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}
