// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! The native reserved cron cell. Schedule and retry policy remain in
//! cellar_logic::cron; storage, wake discovery and ownership remain the host's.

mod history;
mod state;
mod types;
pub use types::Scheduled;
pub(crate) use types::{Configuration as HistoryConfiguration, Query as HistoryQuery};

use futures_util::TryStreamExt;
use serde_json::{json, Value};

use super::{Application, Cell, Context, EventState, HandlerFuture, Invocation, Output, Storage};
use crate::engine_api::HttpResponse;
use anyhow::{anyhow, Result};
use cellar_logic::cron as policy;
use state::{Owed, State};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};

struct Schedule {
    storage: Storage,
    application: Arc<dyn Application>,
    expressions: Vec<String>,
    parsed: Vec<policy::Cron>,
    positions: Vec<usize>,
    workflow_targets: Vec<Option<super::workflow::Workflow>>,
    workflow_bindings: Vec<Option<String>>,
    configuration: Option<HistoryConfiguration>,
    application_name: String,
    node: String,
    runtime_generation: u64,
    state_configuration: String,
}

pub(crate) fn create_cell(
    context: &Context,
    application: Arc<dyn Application>,
    mut expressions: Vec<String>,
) -> Result<Arc<dyn Cell>> {
    anyhow::ensure!(
        context
            .identity()
            .is_some_and(|identity| identity.scope
                == policy::reserved_cell(&context.worker_config.script_name)),
        "cron application identity mismatch"
    );
    let mut workflow_targets = vec![None; expressions.len()];
    let mut workflow_bindings = vec![None; expressions.len()];
    let mut bindings = context.workflow_bindings.iter().collect::<Vec<_>>();
    bindings.sort_by(|left, right| left.0.cmp(right.0));
    for (binding, workflow) in bindings {
        workflow.settings.validate()?;
        for expression in &workflow.settings.schedules {
            expressions.push(expression.clone());
            workflow_targets.push(Some(workflow.clone()));
            workflow_bindings.push(Some(binding.clone()));
        }
    }
    let mut parsed = Vec::new();
    let mut positions = Vec::new();
    // Match the existing op: an invalid control-plane entry must neither
    // silence the valid expressions nor shift their reported original text.
    for (index, expression) in expressions.iter().enumerate() {
        if let Ok(cron) = policy::parse(expression) {
            parsed.push(cron);
            positions.push(index);
        }
    }
    use sha2::{Digest as _, Sha256};
    let state_configuration = format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&json!({
            "application":context.worker_config.script_name,
            "configuration":context.worker_config.cron_history,
            "expressions":expressions,
            "workflows":workflow_targets.iter().map(|target|target.as_ref().map(|workflow|workflow.name())).collect::<Vec<_>>(),
            "bindings":workflow_bindings,
        }))?)
    );
    Ok(Arc::new(Schedule {
        storage: context.storage()?,
        application,
        expressions,
        parsed,
        positions,
        workflow_targets,
        workflow_bindings,
        configuration: context.worker_config.cron_history.clone(),
        application_name: context.worker_config.script_name.clone(),
        node: context.worker_config.node.clone(),
        runtime_generation: context.generation,
        state_configuration,
    }))
}

fn now() -> i64 {
    crate::ownership_store::now_ms() as i64
}

impl Schedule {
    fn arm(
        &self,
        mut state: State,
        occurrence: Option<i64>,
        failed: Vec<usize>,
        retried: bool,
    ) -> Result<()> {
        state.retry = if failed.is_empty() {
            0
        } else if retried {
            state.retry.saturating_add(1)
        } else {
            1
        };
        let plan = policy::plan(&self.parsed, None, now(), state.retry, !failed.is_empty());
        state.owed = if plan.arm_is_retry {
            Some(Owed {
                occurrence: occurrence.expect("a retry follows an occurrence"),
                indices: failed,
                armed_for: plan.arm_at.expect("retry has an alarm"),
                failed: Vec::new(),
                attempt: state.retry,
                running: false,
                in_flight: false,
            })
        } else {
            None
        };
        self.storage.transaction_sync(|storage| {
            state::save(storage, &self.state_configuration, &state)?;
            match plan.arm_at {
                Some(at) => storage.set_alarm(at),
                None => storage.delete_alarm(),
            }
        })
    }

    fn fetch(&self) -> Result<Output> {
        let mut state = state::load(
            &self.storage,
            &self.state_configuration,
            self.expressions.len(),
        )?;
        if !state::has_alarm(
            &self.storage,
            &policy::reserved_cell(&self.application_name),
        )? {
            // The native alarm's bounded failure policy can exhaust a never-
            // completing handler. Resume the calendar, not another blind retry.
            state.owed = None;
        }
        if state.owed.is_none() {
            let armed = self.storage.get_alarm()?;
            // Preserve a due occurrence after downtime. A pending retry
            // preserves both its deadline and its persisted owed set.
            if armed.is_none_or(|at| at > now()) {
                self.arm(state, None, Vec::new(), false)?;
            }
        }
        Ok(Output::Http(HttpResponse {
            status: 204,
            body: Vec::new(),
            stream: None,
            headers: Vec::new(),
            websocket: None,
            write_position: None,
            observed_position: None,
        }))
    }

    async fn alarm(&self, context: &Context, fired: i64) -> Result<()> {
        let mut state = state::load(
            &self.storage,
            &self.state_configuration,
            self.expressions.len(),
        )?;
        let retried = match state
            .owed
            .as_mut()
            .filter(|owed| owed.running || owed.armed_for == fired)
        {
            Some(owed) => {
                if owed.running && owed.in_flight {
                    state.retry = state.retry.saturating_add(1);
                    owed.attempt = owed.attempt.saturating_add(1);
                } else if !owed.running {
                    owed.attempt = state.retry.saturating_add(1);
                }
                owed.running = true;
                owed.in_flight = false;
                true
            }
            None => {
                let plan = policy::plan(&self.parsed, Some(fired), now(), 0, false);
                state.retry = 0;
                state.owed = Some(Owed {
                    occurrence: fired,
                    indices: plan
                        .matching
                        .into_iter()
                        .map(|index| self.positions[index])
                        .collect(),
                    failed: Vec::new(),
                    armed_for: fired,
                    attempt: 1,
                    running: true,
                    in_flight: false,
                });
                false
            }
        };
        state::save(&self.storage, &self.state_configuration, &state)?;
        let pending = state.owed.as_ref().expect("running cron occurrence");
        let (occurrence, due, attempt) =
            (pending.occurrence, pending.indices.clone(), pending.attempt);
        for index in due {
            let started = now();
            let clock = crate::asyncrt::mono_ms();
            let workflow = self.workflow_targets[index].as_ref();
            let schedule = super::workflow::CronSchedule {
                cron: self.expressions[index].clone(),
                scheduled_ms: occurrence,
            };
            let mut record = json!({
                "expression":self.expressions[index],"schedule_index":index,"scheduled_at":occurrence,
                "started_at":started,"ended_at":null,"duration_ms":null,"status":"started","attempt":attempt,
                "application":self.application_name,"node":self.node,"runtime_generation":self.runtime_generation,
                "configuration":self.configuration,"target":if workflow.is_some(){"workflow"}else{"application"},
                "workflow_name":workflow.map(|w|w.name()),"workflow_binding":self.workflow_bindings[index],
                "workflow_instance":workflow.map(|_|schedule.instance_id()),"retry_requested":null,
            });
            state
                .owed
                .as_mut()
                .expect("running cron occurrence")
                .in_flight = true;
            let id = history::begin(
                &self.storage,
                &self.expressions[index],
                record.clone(),
                || state::save(&self.storage, &self.state_configuration, &state),
            )?;
            let mut retry = false;
            if let Some(workflow) = workflow {
                match workflow.create_scheduled(context, schedule).await {
                    Ok(()) => {
                        record["status"] = json!("workflow_accepted");
                        record["retry_requested"] = json!(false);
                    }
                    Err(error) => {
                        tracing::error!(cron=%self.expressions[index],%error,"scheduled workflow creation failed");
                        record["status"] = json!("failed");
                        record["error"] = json!(history::error_text(error));
                        record["retry_requested"] = json!(true);
                        retry = true;
                    }
                }
            } else {
                let controller = Scheduled::new(occurrence, self.expressions[index].clone());
                let outcome =
                    ScheduledRun::new(context, self.application.clone(), controller).await;
                if outcome.retry {
                    retry = true;
                }
                record["status"] = json!(if outcome.error.is_some() {
                    if outcome.retry {
                        "failed"
                    } else {
                        "failed_no_retry"
                    }
                } else if outcome.background_errors > 0 {
                    "completed_with_background_errors"
                } else {
                    "completed"
                });
                record["error"] = json!(outcome.error);
                record["result"] = outcome.output;
                record["background_errors"] = json!(outcome.background_errors);
                record["retry_requested"] = json!(outcome.retry);
            }
            record["ended_at"] = json!(now());
            record["duration_ms"] = json!(crate::asyncrt::mono_ms().saturating_sub(clock));
            let pending = state.owed.as_mut().expect("running cron occurrence");
            pending.indices.retain(|pending| *pending != index);
            if retry {
                pending.failed.push(index);
            }
            pending.in_flight = false;
            self.storage.transaction_sync(|storage| {
                history::finish(storage, id, record)?;
                state::save(storage, &self.state_configuration, &state)
            })?;
        }
        let failed =
            std::mem::take(&mut state.owed.as_mut().expect("running cron occurrence").failed);
        self.arm(state, Some(occurrence), failed, retried)
    }
}

impl Cell for Schedule {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            match invocation {
                Invocation::Fetch(request) => {
                    let mut stream = context.take_body(request.body)?;
                    let mut bytes = Vec::new();
                    while let Some(chunk) = stream.try_next().await.map_err(anyhow::Error::msg)? {
                        anyhow::ensure!(
                            bytes.len().saturating_add(chunk.len()) <= 4096,
                            "cron inspection request exceeds 4 KiB"
                        );
                        bytes.extend(chunk);
                    }
                    anyhow::ensure!(bytes.is_empty(),"cron arming does not accept a body; inspect history through __cronHistory RPC");
                    self.fetch()
                }
                Invocation::CellRpc { method, args } => {
                    anyhow::ensure!(method == "__cronHistory", "unsupported cron RPC");
                    let crate::engine_api::RpcData::Json(body) = args else {
                        return Err(anyhow!("cron history requires JSON"));
                    };
                    anyhow::ensure!(body.len() <= 4096, "cron inspection request exceeds 4 KiB");
                    let query: HistoryQuery = serde_json::from_str(&body)?;
                    let page = history::page(&self.storage, query)?;
                    Ok(Output::Rpc(crate::engine_api::RpcData::Json(
                        serde_json::to_string(&page)?,
                    )))
                }
                Invocation::Alarm { scheduled_ms } => {
                    self.alarm(&context, scheduled_ms).await?;
                    Ok(Output::Done)
                }
                _ => Err(anyhow!("the cron schedule received an unsupported event")),
            }
        })
    }
}

/// A scheduled handler is a worker event inside the alarm's host turn.
/// It has its own registration lifetime while retaining the exact parent
/// I/O authority and output gate. Background work is polled concurrently
/// with its handler and drained before the next expression starts.
struct ScheduledRun {
    context: Context,
    controller: Scheduled,
    handler: Option<HandlerFuture<Output>>,
    background: Vec<HandlerFuture<()>>,
    retry: bool,
    error: Option<String>,
    output: Value,
    background_errors: u32,
}

struct RunOutcome {
    retry: bool,
    error: Option<String>,
    output: Value,
    background_errors: u32,
}

impl ScheduledRun {
    fn new(parent: &Context, application: Arc<dyn Application>, controller: Scheduled) -> Self {
        let mut context = parent.clone();
        context.identity = None;
        context.state = Arc::new(EventState::default());
        context
            .state
            .blocks
            .initializing
            .store(false, Ordering::Release);
        let handler = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            application.invoke(context.clone(), Invocation::Scheduled(controller.clone()))
        }))
        .unwrap_or_else(|panic| {
            let error = crate::native_worker::panic_error(panic);
            Box::pin(async move { Err(error) })
        });
        Self {
            context,
            controller,
            handler: Some(handler),
            background: Vec::new(),
            retry: false,
            error: None,
            output: Value::Null,
            background_errors: 0,
        }
    }
}

impl Future for ScheduledRun {
    type Output = RunOutcome;

    fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<RunOutcome> {
        let this = self.get_mut();
        if let Some(handler) = &mut this.handler {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                handler.as_mut().poll(cx)
            }))
            .unwrap_or_else(|panic| Poll::Ready(Err(crate::native_worker::panic_error(panic))));
            if let Poll::Ready(result) = result {
                this.handler = None;
                if let Ok(output) = &result {
                    this.output = history::output_summary(output);
                }
                if let Err(error) = result {
                    this.error = Some(history::error_text(&error));
                    tracing::error!(cron=%this.controller.cron,%error,"scheduled handler failed");
                    // The existing cell records this before draining waitUntil.
                    this.retry = !this.controller.retry_declined();
                }
            }
        }
        this.background
            .append(&mut this.context.state.background.lock().unwrap());
        this.background.retain_mut(|future| {
            let result =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| future.as_mut().poll(cx)))
                    .unwrap_or_else(|panic| {
                        Poll::Ready(Err(crate::native_worker::panic_error(panic)))
                    });
            match result {
                Poll::Pending => true,
                Poll::Ready(result) => {
                    if let Err(error) = result {
                        this.background_errors = this.background_errors.saturating_add(1);
                        tracing::error!(%error,"scheduled background work failed");
                    }
                    false
                }
            }
        });
        let queued = !this.context.state.background.lock().unwrap().is_empty();
        if queued {
            cx.waker().wake_by_ref();
        }
        if this.handler.is_none() && this.background.is_empty() && !queued {
            Poll::Ready(RunOutcome {
                retry: this.retry,
                error: this.error.take(),
                output: std::mem::take(&mut this.output),
                background_errors: this.background_errors,
            })
        } else {
            Poll::Pending
        }
    }
}

impl Drop for ScheduledRun {
    fn drop(&mut self) {
        self.context.state.retired.store(true, Ordering::Release);
        self.context.state.background.lock().unwrap().clear();
    }
}
