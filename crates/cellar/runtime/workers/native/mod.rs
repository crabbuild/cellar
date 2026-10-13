// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Adapted for Cellar from Celld: crates/celld/js.rs.

//! Native execution behind the existing Worker/turn boundary. No V8 handle
//! is created on this path. The host still owns admission and durability.

use crate::application::{
    Activation, Application, Cell, CellIdentity, Context, EventState, HandlerFuture, Invocation,
    Output, Request,
};
use crate::engine_api::{
    fail_in_turn_error, HttpChunkStream, HttpResponse, RequestId, RpcData, RpcOutcome,
};
use crate::host_channels::{
    begin_call_cancellation, DoCallReq, DO_CALL_TX, GATE_TX, QUEUE_DISPATCH_TX,
};
use crate::host_gates::*;
use crate::host_io::{installed_context, ContextRegistry, EgressFrame, IoContext};
use crate::host_turn::*;
use crate::http_streams::RequestBodyGuard;
use crate::wake_entry::launch_arm_gate;
use crate::worker_config::{effective_resource_limits, ResourceLimits, WorkerConfig};
use crate::ws_registry::WsDispatch;
use crate::{asyncrt, storage};
use anyhow::{anyhow, Context as _, Result};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::task::{Context as TaskContext, Poll};
use std::time::Instant;

mod facets;
mod gate;
#[cfg(test)]
mod tests;
pub(crate) use gate::{InputGate, Ready};

fn primitive_wall_ms() -> f64 {
    crate::asyncrt::wall_ms() as f64
}

pub(crate) struct NativeWorker {
    pub(crate) cells: storage::Cells,
    config: Arc<WorkerConfig>,
    application: Arc<dyn Application>,
    registry: Arc<ContextRegistry>,
    instances: HashMap<String, Arc<dyn Cell>>,
    activations: HashMap<String, Arc<Activation>>,
    facets: HashMap<String, Arc<crate::application::FacetContext>>,
    vars: Arc<HashMap<String, String>>,
    sql_bindings: Arc<HashMap<String, String>>,
    kv_bindings: Arc<HashMap<String, String>>,
    queue_bindings: Arc<HashMap<String, crate::application::queue::Producer>>,
    workflow_bindings: Arc<HashMap<String, crate::application::workflow::Workflow>>,
    blob_bindings: Arc<HashMap<String, crate::application::blob::Bucket>>,
    asset_binding: Option<(Arc<str>, crate::application::assets::Fetcher)>,
    queue_config: Arc<crate::application::queue::Config>,
    loader_owner: Arc<crate::application::loader::Owner>,
    loaded_environment: Option<Arc<cellar_native::clone::StructuredValue>>,
}

pub(crate) struct NativeFlight {
    handler: Option<HandlerFuture<Output>>,
    background: Vec<HandlerFuture<()>>,
    unrefed: Vec<HandlerFuture<()>>,
    incoming_abort: Option<cellar_native::web::AbortController>,
    state: Arc<EventState>,
    initialization: Vec<Ready>,
    delivery_gate: Option<tokio::sync::oneshot::Receiver<Result<(), String>>>,
    // Retain the sender even for a pending future that does not clone a waker.
    wake: Option<(u64, Arc<WakeTurn>)>,
}

struct Start {
    invocation: Invocation,
    answer: Answer,
    request_id: Option<RequestId>,
    scope: Option<String>,
    trace: Option<crate::telemetry::TraceContext>,
    alarm: Option<AlarmClaim>,
    limits: Option<ResourceLimits>,
    tail: Option<TailReportState>,
}

impl NativeFlight {
    pub(crate) fn has_work(&self) -> bool {
        self.handler.is_some()
            || !self.background.is_empty()
            || !self.state.background.lock().unwrap().is_empty()
    }

    pub(crate) fn stop_handler(&mut self) {
        self.handler = None;
        self.state.discard_websocket_capture();
    }
}

impl Drop for NativeFlight {
    fn drop(&mut self) {
        self.state.retired.store(true, Ordering::Release);
        self.state.discard_websocket_capture();
    }
}

pub(crate) struct WakeTurn(Mutex<Option<tokio::sync::oneshot::Sender<()>>>);

impl WakeTurn {
    pub(crate) fn pending(&self) -> bool {
        self.0
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|sender| !sender.is_closed())
    }
}

impl Drop for NativeWorker {
    fn drop(&mut self) {
        self.loader_owner.retire();
        let _clock = cellar_native::web::WallClockGuard::enter(primitive_wall_ms);
        let _installed = self.cells.install();
        for activation in self.activations.values() {
            retire_activation(activation);
        }
        self.instances.clear();
        // Cells::drop finalizes cursors and caches before closing databases.
    }
}

fn retire_activation(activation: &Activation) {
    // Called only with this worker's original Cells installed. Never promise
    // a position from an open transaction that retirement will roll back.
    let position = storage::sync_sample(&activation.scope)
        .ok()
        .flatten()
        .filter(|sample| !sample.in_transaction && sample.epoch == activation.epoch)
        .and_then(|_| storage::write_position(&activation.scope).ok().flatten());
    *activation.resources.retired_position.lock().unwrap() = position;
    activation.retire();
}

impl futures_util::task::ArcWake for WakeTurn {
    fn wake_by_ref(wake: &Arc<Self>) {
        let sender = wake.0.lock().unwrap().take();
        if let Some(sender) = sender {
            let _ = sender.send(());
        }
    }
}

pub(crate) fn panic_error(panic: Box<dyn std::any::Any + Send>) -> anyhow::Error {
    anyhow!(
        "native handler panicked: {}",
        panic
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| panic.downcast_ref::<&str>().copied())
            .unwrap_or("non-string panic")
    )
}

pub(crate) fn validate_application_classes<'a>(
    application: &dyn crate::application::Application,
    script: &str,
    classes: impl IntoIterator<Item = &'a str>,
    workflows: impl IntoIterator<Item = &'a str>,
) -> Result<()> {
    for class in classes {
        anyhow::ensure!(
            class == crate::deploy::SQL_CLASS
                || class == crate::deploy::KV_CLASS
                || class == crate::deploy::QUEUE_CLASS
                || class == cellar_logic::cron::RESERVED_CLASS
                || class == crate::deploy::workflow_class(script)
                || application.supports_cell(class),
            "native application {script} has no factory for {class}"
        );
    }
    for class in workflows {
        anyhow::ensure!(
            application.supports_workflow(class),
            "native application {script} has no workflow handler for {class}"
        );
    }
    Ok(())
}

fn failed(error: anyhow::Error) -> HandlerFuture<Output> {
    Box::pin(async move { Err(error) })
}

impl NativeWorker {
    pub(crate) fn new(config: Arc<WorkerConfig>) -> Result<Self> {
        let _clock = cellar_native::web::WallClockGuard::enter(primitive_wall_ms);
        let application = config
            .native_application
            .clone()
            .expect("native configuration");
        let loader_owner = crate::application::loader::Owner::new(&config);
        let loaded_environment = config
            .loader_env
            .as_ref()
            .map(|env| -> Result<_> { Ok(Arc::new(cellar_native::clone::decode(&env.bytes)?)) })
            .transpose()?;
        validate_application_classes(
            application.as_ref(),
            &config.script_name,
            config
                .do_classes
                .iter()
                .map(String::as_str)
                .chain((!config.crons.is_empty()).then_some(cellar_logic::cron::RESERVED_CLASS)),
            config
                .workflow_bindings
                .iter()
                .map(|binding| binding.class.as_str()),
        )?;
        let registry = Arc::new(ContextRegistry::default());
        let vars = Arc::new(config.vars.iter().cloned().collect());
        let sql_bindings = Arc::new(config.sql_bindings.iter().cloned().collect());
        let kv_bindings = Arc::new(config.kv_bindings.iter().cloned().collect());
        let workflow_bindings = Arc::new(
            config
                .workflow_bindings
                .iter()
                .map(|binding| {
                    (
                        binding.environment.clone(),
                        crate::application::workflow::Workflow::new(
                            &config.script_name,
                            &binding.workflow,
                        ),
                    )
                })
                .collect(),
        );
        let blob_bindings = Arc::new(
            config
                .blob_bindings
                .iter()
                .map(|(binding, bucket)| {
                    (
                        binding.clone(),
                        crate::application::blob::Bucket::new(binding, bucket),
                    )
                })
                .collect(),
        );
        let queue_config = Arc::new(serde_json::from_value(
            crate::queue_policy::consumer_config(&config)?,
        )?);
        let asset_binding = config.asset_binding.as_ref().map(|binding| {
            (
                Arc::from(binding.as_str()),
                crate::application::assets::Fetcher::new(&config.script_name),
            )
        });
        let queue_bindings = Arc::new(
            config
                .queue_bindings
                .iter()
                .map(|binding| {
                    (
                        binding.environment.clone(),
                        crate::application::queue::Producer::new(
                            binding.queue.clone(),
                            binding.delivery_delay,
                            config.compat.queue_json_messages,
                        ),
                    )
                })
                .collect(),
        );
        Ok(Self {
            cells: storage::Cells::default(),
            application,
            config,
            registry,
            vars,
            sql_bindings,
            kv_bindings,
            queue_bindings,
            workflow_bindings,
            blob_bindings,
            asset_binding,
            queue_config,
            loader_owner,
            loaded_environment,
            instances: HashMap::new(),
            activations: HashMap::new(),
            facets: HashMap::new(),
        })
    }

    pub(crate) fn own_cell(
        &mut self,
        scope: &str,
        authority: Option<CellStorage<'_>>,
    ) -> Result<cellar_logic::wake::AlarmSnapshot> {
        let _installed = self.cells.install();
        let _clock = cellar_native::web::WallClockGuard::enter(primitive_wall_ms);
        if let Some(authority) = authority {
            anyhow::ensure!(
                storage::activation_epoch(scope).is_none(),
                "native cell is already resident: {scope}"
            );
            storage::open_at_epoch(
                scope,
                authority.path,
                authority.epoch,
                authority.replicated_wake,
                authority.vfs,
                self.config.compat.sqlite_vec,
            )?;
            self.activations.insert(
                scope.to_string(),
                Arc::new(Activation {
                    scope: scope.to_string(),
                    epoch: authority.epoch,
                    delete_all_deletes_alarm: self.config.compat.delete_all_deletes_alarm,
                    resources: Default::default(),
                    websocket_states: Default::default(),
                }),
            );
        } else {
            self.retire_facet_descendants(scope);
            self.close_native_cell(scope);
        }
        Ok(observe_alarm(scope, storage::get_alarm(scope)))
    }

    fn context(
        &self,
        scope: Option<&str>,
        trace: Option<crate::telemetry::TraceContext>,
        limits: Option<ResourceLimits>,
        tails: bool,
    ) -> Result<Context> {
        let identity = scope
            .map(|scope| -> Result<CellIdentity> {
                let (class, _) = scope
                    .split_once(':')
                    .ok_or_else(|| anyhow!("cell scope has no class"))?;
                let epoch = storage::activation_epoch(scope)
                    .ok_or_else(|| anyhow!("native cell is not resident: {scope}"))?;
                Ok(CellIdentity {
                    scope: scope.to_string(),
                    class: class.to_string(),
                    epoch,
                    name: match self.facets.get(scope) {
                        Some(facet) => facets::identity_name(&facet.id)?,
                        None => storage::get_actor_name(scope)?,
                    },
                })
            })
            .transpose()?;
        let mut io = IoContext::with_options(
            effective_resource_limits(self.config.resource_limits, limits),
            tails,
        );
        Arc::get_mut(&mut io)
            .expect("new context has no external owners")
            .native_activation = scope.and_then(|scope| self.activations.get(scope).cloned());
        self.registry.register(&mut io);
        io.begin_event();
        if let Some(identity) = &identity {
            io.egress.lock().unwrap().push(EgressFrame {
                storage: identity.scope.clone(),
                before: storage::event_start_position(&identity.scope).unwrap_or(0),
                root: storage::embedded_root_gate(&identity.scope),
            });
        }
        Ok(Context {
            io,
            state: Arc::new(EventState::default()),
            identity,
            facet: scope.and_then(|scope| self.facets.get(scope).cloned()),
            trace,
            vars: self.vars.clone(),
            sql_bindings: self.sql_bindings.clone(),
            kv_bindings: self.kv_bindings.clone(),
            queue_bindings: self.queue_bindings.clone(),
            workflow_bindings: self.workflow_bindings.clone(),
            blob_bindings: self.blob_bindings.clone(),
            asset_binding: self.asset_binding.clone(),
            queue_config: self.queue_config.clone(),
            generation: self.config.generation,
            worker_config: self.config.clone(),
            loader_owner: self.loader_owner.clone(),
            loaded_environment: self.loaded_environment.clone(),
        })
    }

    // A response deadline measures host elapsed time across turns and awaits.
    #[allow(clippy::disallowed_methods)]
    fn entry(
        &self,
        context: &Context,
        answer: Answer,
        request_id: Option<RequestId>,
        writes_before: Option<u64>,
        handler: HandlerFuture<Output>,
    ) -> InFlight {
        context
            .state
            .blocks
            .initializing
            .store(false, Ordering::Release);
        InFlight {
            engine: None,
            native: Some(Mutex::new(NativeFlight {
                handler: Some(handler),
                background: Vec::new(),
                unrefed: Vec::new(),
                incoming_abort: None,
                state: context.state.clone(),
                initialization: std::mem::take(
                    &mut *context.state.blocks.initialization.lock().unwrap(),
                ),
                delivery_gate: None,
                wake: None,
            })),
            context: context.io.clone(),
            scope: context.identity.as_ref().map(|id| id.scope.clone()),
            writes_before,
            request_id,
            active_request_id: None,
            reply: Some(answer),
            gated_reply: None,
            completed_cell_event: false,
            ops: HashSet::new(),
            io_context_ops: HashSet::new(),
            unrefed_ops: HashSet::new(),
            alarm: None,
            started: Instant::now(),
            trace: context.trace,
            failure: None,
            tail_report: None,
            registry: self.registry.clone(),
        }
    }

    pub(crate) fn begin(
        &mut self,
        job: crate::WorkerJob,
        trace: Option<crate::telemetry::TraceContext>,
    ) -> (Option<InFlight>, Vec<Op>) {
        let (invocation, answer, request_id, limits, tail) = match job {
            crate::WorkerJob::Fetch {
                entrypoint,
                url,
                method,
                body,
                headers,
                request_id,
                invocation_limits,
                tail_report,
                reply,
                ..
            } => {
                let tail = tail_report.map(|reply| TailReportState {
                    reply,
                    script_name: self.config.script_name.clone(),
                    event_timestamp: unix_now_ms(),
                    url: url.clone(),
                    method: method.clone(),
                    headers: tail_request_headers(&headers),
                    response_status: None,
                    failure: None,
                });
                (
                    Invocation::Fetch(Request {
                        entrypoint,
                        url,
                        method,
                        body,
                        headers,
                    }),
                    Answer::Fetch(reply),
                    request_id,
                    invocation_limits,
                    tail,
                )
            }
            crate::WorkerJob::Rpc {
                entrypoint,
                operation,
                props,
                request_id,
                invocation_limits,
                reply,
            } => (
                Invocation::Rpc {
                    entrypoint,
                    operation,
                    props,
                },
                Answer::Rpc(reply),
                request_id,
                invocation_limits,
                None,
            ),
            crate::WorkerJob::Queue { batch, reply, .. } => (
                Invocation::Queue(batch),
                Answer::Queue(reply),
                None,
                None,
                None,
            ),
        };
        self.start(Start {
            invocation,
            answer,
            request_id,
            scope: None,
            trace,
            alarm: None,
            limits,
            tail,
        })
    }

    fn start(&mut self, start: Start) -> (Option<InFlight>, Vec<Op>) {
        let _clock = cellar_native::web::WallClockGuard::enter(primitive_wall_ms);
        let Start {
            invocation,
            answer,
            request_id,
            scope,
            trace,
            alarm,
            limits,
            tail,
        } = start;
        let _installed = self.cells.install();
        if let Some(scope) = &scope {
            if self
                .activations
                .get(scope)
                .is_some_and(|activation| activation.resources.failure.lock().unwrap().is_some())
            {
                self.retire_facet_descendants(scope);
                let old = self.activations.remove(scope).expect("reset activation");
                retire_activation(&old);
                self.instances.remove(scope);
                old.drain_resources();
                storage::reset_native_resources(scope);
                self.activations.insert(
                    scope.clone(),
                    Arc::new(Activation {
                        scope: scope.clone(),
                        epoch: old.epoch,
                        delete_all_deletes_alarm: old.delete_all_deletes_alarm,
                        resources: Default::default(),
                        websocket_states: Default::default(),
                    }),
                );
            }
        }
        if let Some(activation) = scope.as_ref().and_then(|scope| self.activations.get(scope)) {
            activation.drain_resources();
        }
        let context = match self.context(scope.as_deref(), trace, limits, tail.is_some()) {
            Ok(context) => context,
            Err(error) => {
                if let (Some(scope), Some(claim)) = (&scope, &alarm) {
                    storage::finish_alarm_handler_with_retry_policy(
                        scope,
                        false,
                        claim.now_ms,
                        false,
                    );
                }
                answer.fail(error);
                return (None, Vec::new());
            }
        };
        let writes_before = scope.as_deref().and_then(storage::event_start_position);
        if matches!(&invocation, Invocation::WebSocketMessage { .. }) {
            *context.state.websocket_capture.lock().unwrap() = Some(Default::default());
        }
        let incoming_abort = matches!(&invocation, Invocation::Fetch(_))
            .then(cellar_native::web::AbortController::new);
        if let Some(controller) = &incoming_abort {
            *context.state.incoming_abort.lock().unwrap() = Some(controller.signal().downgrade());
        }
        if let Invocation::Fetch(request) = &invocation {
            if let Some(stream) = request.body.stream_id() {
                context.io.own_body_stream(stream);
            }
        }
        let _guard = crate::host_io::CurrentGuard::enter(context.io.clone());
        if let Invocation::WebSocketClose { id, .. } = &invocation {
            crate::application::websocket::WebSocket::record_close(&context, *id);
        }
        if let Invocation::WebSocketOpen { id, protocol } = &invocation {
            crate::application::websocket::WebSocket::record_open(&context, *id, protocol);
        }
        context.io.begin_cpu_turn();
        let handler = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
            || -> Result<HandlerFuture<Output>> {
                match &context.identity {
                    Some(identity) => {
                        let cell = match self.instances.entry(identity.scope.clone()) {
                            std::collections::hash_map::Entry::Occupied(entry) => {
                                entry.get().clone()
                            }
                            std::collections::hash_map::Entry::Vacant(entry) => {
                                let cell = if identity.class == crate::deploy::SQL_CLASS {
                                    crate::application::sql::create_cell(&context)?
                                } else if identity.class == crate::deploy::KV_CLASS {
                                    crate::application::kv::create_cell(&context)?
                                } else if identity.class == crate::deploy::QUEUE_CLASS {
                                    crate::application::queue::create_cell(&context)?
                                } else if identity.class == cellar_logic::cron::RESERVED_CLASS {
                                    crate::application::cron::create_cell(
                                        &context,
                                        self.application.clone(),
                                        self.config.crons.clone(),
                                    )?
                                } else if identity.class
                                    == crate::deploy::workflow_class(&self.config.script_name)
                                {
                                    crate::application::workflow::create_cell(
                                        &context,
                                        self.application.clone(),
                                        self.config
                                            .workflow_bindings
                                            .iter()
                                            .map(|binding| {
                                                (binding.workflow.clone(), binding.class.clone())
                                            })
                                            .collect(),
                                    )?
                                } else {
                                    self.application.create_cell(context.clone(), identity)?
                                };
                                entry.insert(cell).clone()
                            }
                        };
                        let context = context.clone();
                        Ok(Box::pin(
                            async move { cell.invoke(context, invocation).await },
                        ))
                    }
                    None => {
                        let application = self.application.clone();
                        let context = context.clone();
                        Ok(Box::pin(async move {
                            application.invoke(context, invocation).await
                        }))
                    }
                }
            },
        ))
        .unwrap_or_else(|panic| Err(panic_error(panic)));
        let handler = match context
            .io
            .finish_cpu_turn()
            .map_err(anyhow::Error::msg)
            .and(handler)
        {
            Ok(handler) => handler,
            Err(error) => {
                // A failed constructor cannot leave its initialization work
                // running after the event reports the failure.
                context.state.background.lock().unwrap().clear();
                failed(error)
            }
        };
        let mut entry = self.entry(&context, answer, request_id, writes_before, handler);
        entry
            .native
            .as_ref()
            .expect("new native flight")
            .lock()
            .unwrap()
            .incoming_abort = incoming_abort;
        entry.alarm = alarm;
        entry.tail_report = tail;
        let ops = self.poll(&mut entry);
        (Some(entry), ops)
    }

    pub(crate) fn begin_cell(
        &mut self,
        job: CellJob,
        trace: Option<crate::telemetry::TraceContext>,
    ) -> (Option<InFlight>, Vec<Op>) {
        let _installed = self.cells.install();
        let scope = job.scope().to_string();
        let request_id = job.request_id();
        let mut alarm = None;
        let (invocation, answer) = match job {
            CellJob::Fetch {
                url,
                method,
                body,
                headers,
                name,
                reply,
                ..
            } => {
                if let Some(name) = name {
                    if let Err(error) = storage::set_actor_name(&scope, &name) {
                        let _ = reply.send(Err(error));
                        return (None, Vec::new());
                    }
                }
                (
                    Invocation::Fetch(Request {
                        url,
                        method,
                        body,
                        headers,
                        entrypoint: None,
                    }),
                    Answer::Fetch(reply),
                )
            }
            CellJob::Rpc {
                method,
                args,
                name,
                reply,
                ..
            } => {
                if let Some(name) = name {
                    if let Err(error) = storage::set_actor_name(&scope, &name) {
                        let _ = reply.send(Err(error));
                        return (None, Vec::new());
                    }
                }
                (Invocation::CellRpc { method, args }, Answer::CellRpc(reply))
            }
            CellJob::Alarm {
                scheduled_ms,
                claim,
                reply,
                ..
            } => {
                let now_ms = unix_now_ms();
                if now_ms < scheduled_ms {
                    let _ = reply.send(Err(anyhow!("alarm dispatched before its deadline")));
                    return (None, Vec::new());
                }
                let due_by = match claim {
                    AlarmDispatch::Due => now_ms,
                    #[cfg(cellar_internal_tests)]
                    AlarmDispatch::Armed => i64::MAX,
                    #[cfg(cellar_internal_tests)]
                    AlarmDispatch::Claimed(_) => now_ms,
                };
                let Some((scheduled_ms, _retry)) = storage::due_alarm_entry(&scope, due_by) else {
                    let _ = reply.send(Ok((
                        observe_alarm(&scope, storage::get_alarm(&scope)),
                        None,
                    )));
                    return (None, Vec::new());
                };
                storage::begin_alarm_handler(&scope, scheduled_ms);
                alarm = Some(AlarmClaim { now_ms });
                (Invocation::Alarm { scheduled_ms }, Answer::Alarm(reply))
            }
            CellJob::WsOpen {
                ws_id,
                protocol,
                reply,
                facet_opening,
                ..
            } => {
                if let Some(error) = facet_socket_refusal(&scope, facet_opening) {
                    let _ = reply.send(Err(error));
                    return (None, Vec::new());
                }
                (
                    Invocation::WebSocketOpen {
                        id: ws_id,
                        protocol,
                    },
                    Answer::Ack(reply),
                )
            }
            CellJob::WsMessage {
                ws_id,
                data,
                reply,
                facet_opening,
                ..
            } => {
                if let Some(error) = facet_socket_refusal(&scope, facet_opening) {
                    let _ = reply.send(Err(error));
                    return (None, Vec::new());
                }
                (
                    Invocation::WebSocketMessage { id: ws_id, data },
                    Answer::WsMessage(reply),
                )
            }
            CellJob::WsClosed {
                ws_id,
                code,
                reason,
                was_clean,
                reply,
                facet_opening,
                ..
            } => {
                if facet_socket_refusal(&scope, facet_opening).is_some() {
                    let _ = reply.send(Ok(None));
                    return (None, Vec::new());
                }
                (
                    Invocation::WebSocketClose {
                        id: ws_id,
                        code,
                        reason,
                        was_clean,
                    },
                    Answer::Ack(reply),
                )
            }
            #[cfg(cellar_internal_tests)]
            CellJob::SyncErrorForTest { reply, .. } => {
                let _ = reply.send(Err(anyhow!("native synchronous test event is unsupported")));
                return (None, Vec::new());
            }
        };
        self.start(Start {
            invocation,
            answer,
            request_id,
            scope: Some(scope),
            trace,
            alarm,
            limits: None,
            tail: None,
        })
    }

    pub(crate) fn poll(&mut self, entry: &mut InFlight) -> Vec<Op> {
        let _installed = self.cells.install();
        let _clock = cellar_native::web::WallClockGuard::enter(primitive_wall_ms);
        let _guard = crate::host_io::CurrentGuard::enter(entry.context.clone());
        if self.activation_retired(entry) {
            entry.native = None;
            // This opening no longer owns the scope. Neither settling its
            // alarm nor sampling a write delta may touch a replacement's DB.
            entry.alarm = None;
            let position = entry
                .context
                .native_activation
                .as_ref()
                .and_then(|activation| *activation.resources.retired_position.lock().unwrap());
            let failure = position.and_then(|_| {
                entry
                    .context
                    .native_activation
                    .as_ref()
                    .and_then(|activation| activation.resources.failure.lock().unwrap().clone())
            });
            entry.fail(fail_in_turn_error(
                anyhow!(failure.unwrap_or_else(|| "native cell activation has retired".into())),
                (position, None),
            ));
            entry.context.end_event_registrations();
            entry.context.seal_wait_until();
            entry.context.close_sockets();
            return adopt(entry);
        }
        let failure = entry
            .context
            .native_activation
            .as_ref()
            .and_then(|activation| activation.resources.failure.lock().unwrap().clone());
        if let Some(failure) = failure {
            entry.native = None;
            entry.settle_alarm(false, false);
            entry.fail_in_turn(anyhow!(failure));
            entry.context.end_event_registrations();
            entry.context.seal_wait_until();
            entry.context.close_sockets();
            return adopt(entry);
        }
        if entry.reply.is_some()
            && entry
                .context
                .host_services()
                .gates()
                .take_request_cancellation(entry.request_id)
        {
            let shutdown = entry
                .context
                .host_services()
                .gates()
                .take_shutdown_cancellation(entry.request_id);
            return self.cancel(entry, shutdown);
        }
        let Some(native) = entry.native.take() else {
            return Vec::new();
        };
        if let Some(activation) = &entry.context.native_activation {
            activation.drain_resources();
        }
        let mut native = native.into_inner().expect("native flight is not poisoned");
        let (wake_id, wake, receive) = match native.wake.take() {
            Some((id, wake)) if entry.ops.contains(&id) => (Some(id), wake, None),
            _ => {
                let (sender, receive) = tokio::sync::oneshot::channel();
                (
                    None,
                    Arc::new(WakeTurn(Mutex::new(Some(sender)))),
                    Some(receive),
                )
            }
        };
        let waker = futures_util::task::waker(wake.clone());
        if let Some(activation) = &entry.context.native_activation {
            activation.register_wake(&wake);
        }
        let mut cx = TaskContext::from_waker(&waker);
        // A continuation of an earlier event is an input too. A pending
        // native future must not observe the holder's uncommitted state.
        let mut admitted = true;
        let mut admission_error = None;
        if gate_engagement(entry.scope.as_deref(), &entry.context) == GateEngagement::Holds {
            native.delivery_gate = None;
        } else if let Some(scope) = &entry.scope {
            if native.delivery_gate.is_none() {
                native.delivery_gate =
                    cell_gate_wait_in(&entry.context.host_services().gates(), scope);
            }
            if let Some(wait) = &mut native.delivery_gate {
                match Pin::new(wait).poll(&mut cx) {
                    Poll::Pending => admitted = false,
                    Poll::Ready(outcome) => {
                        native.delivery_gate = None;
                        match outcome {
                            Ok(Ok(())) => {
                                // Opening a gate wakes every waiter; another
                                // holder can take it before this event's turn.
                                if let Some(wait) =
                                    cell_gate_wait_in(&entry.context.host_services().gates(), scope)
                                {
                                    native.delivery_gate = Some(wait);
                                    // Register this turn's wake before yielding.
                                    let _ = Pin::new(native.delivery_gate.as_mut().unwrap())
                                        .poll(&mut cx);
                                    admitted = false;
                                }
                            }
                            Ok(Err(failure)) => admission_error = Some(anyhow!(failure)),
                            Err(_) => {
                                admission_error =
                                    Some(anyhow!("cell stopped while waiting for its input gate"))
                            }
                        }
                    }
                }
            }
        }
        entry.context.begin_cpu_turn();
        let mut handler_result = None;
        let initialized = if admitted {
            match crate::application::concurrency::poll_initialization(
                &mut native.initialization,
                &mut cx,
            ) {
                Poll::Pending => false,
                Poll::Ready(Ok(())) => true,
                Poll::Ready(Err(error)) => {
                    admission_error = Some(
                        entry
                            .context
                            .native_activation
                            .as_ref()
                            .and_then(|activation| {
                                activation.resources.failure.lock().unwrap().clone()
                            })
                            .map_or(error, anyhow::Error::msg),
                    );
                    false
                }
            }
        } else {
            false
        };
        if let Some(error) = admission_error {
            native.handler = None;
            native.background.clear();
            native.state.background.lock().unwrap().clear();
            handler_result = Some(Err(error));
            admitted = false;
        }
        if let Some(handler) = native.handler.as_mut().filter(|_| admitted && initialized) {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                handler.as_mut().poll(&mut cx)
            }))
            .unwrap_or_else(|panic| Poll::Ready(Err(panic_error(panic))));
            if let Poll::Ready(result) = result {
                native.handler = None;
                handler_result = Some(result);
            }
        }
        if let Err(error) = entry.context.finish_cpu_turn() {
            native.handler = None;
            native.background.clear();
            native.state.background.lock().unwrap().clear();
            handler_result = Some(Err(anyhow!(error)));
            admitted = false;
        }
        if let Some(result) = handler_result {
            settle(entry, &native.state, result);
        }
        native
            .background
            .append(&mut native.state.background.lock().unwrap());
        entry.context.begin_cpu_turn();
        native.background.retain_mut(|future| {
            if entry
                .context
                .native_activation
                .as_ref()
                .is_some_and(|activation| activation.resources.failure.lock().unwrap().is_some())
            {
                return false;
            }
            if !admitted {
                return true;
            }
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                future.as_mut().poll(&mut cx)
            }))
            .unwrap_or_else(|panic| Poll::Ready(Err(panic_error(panic))))
            {
                Poll::Pending => true,
                Poll::Ready(result) => {
                    if let Err(error) = result {
                        entry.failure = Some(crate::telemetry::cap_error(error.to_string()));
                    }
                    false
                }
            }
        });
        if entry
            .context
            .native_activation
            .as_ref()
            .is_some_and(|activation| activation.resources.failure.lock().unwrap().is_some())
        {
            native.state.background.lock().unwrap().clear();
        }
        if let Err(error) = entry.context.finish_cpu_turn() {
            native.handler = None;
            native.background.clear();
            native.state.background.lock().unwrap().clear();
            if entry.reply.is_some() {
                settle(entry, &native.state, Err(anyhow!(error)));
            } else {
                entry.failure = Some(crate::telemetry::cap_error(error.to_string()));
            }
        }
        native.unrefed.append(&mut std::mem::take(
            &mut *native.state.unrefed.lock().unwrap(),
        ));
        if native.has_work() {
            entry.context.begin_cpu_turn();
            native.unrefed.retain_mut(|future| {
                if !admitted {
                    return true;
                }
                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    future.as_mut().poll(&mut cx)
                }))
                .unwrap_or_else(|panic| Poll::Ready(Err(panic_error(panic))))
                {
                    Poll::Pending => true,
                    Poll::Ready(result) => {
                        if let Err(error) = result {
                            entry.failure = Some(crate::telemetry::cap_error(error.to_string()));
                        }
                        false
                    }
                }
            });
            if let Err(error) = entry.context.finish_cpu_turn() {
                native.handler = None;
                native.background.clear();
                native.state.background.lock().unwrap().clear();
                if entry.reply.is_some() {
                    settle(entry, &native.state, Err(anyhow!(error)));
                } else {
                    entry.failure = Some(crate::telemetry::cap_error(error.to_string()));
                }
            }
        }
        let work = native.has_work();
        if native.handler.is_some() && admitted {
            flush_websocket_capture(&native.state, &entry.context);
        }
        if !work {
            native.unrefed.clear();
            let unrefed = std::mem::take(&mut *native.state.unrefed.lock().unwrap());
            drop(unrefed);
        }
        if !native.state.background.lock().unwrap().is_empty()
            || (work && !native.state.unrefed.lock().unwrap().is_empty())
        {
            futures_util::task::ArcWake::wake_by_ref(&wake);
        }
        if work {
            let id = match receive {
                Some(receive) => asyncrt::enqueue(async move {
                    receive
                        .await
                        .map_err(|_| "native wake sender dropped".to_string())?;
                    Ok(asyncrt::OpOut::Str(String::new()))
                }),
                None => wake_id.expect("retained wake has an operation"),
            };
            native.wake = Some((id, wake));
        }
        entry.native = Some(Mutex::new(native));
        if entry.retired() {
            entry.context.close_sockets();
        }
        adopt(entry)
    }

    pub(crate) fn cancel(&mut self, entry: &mut InFlight, shutdown: bool) -> Vec<Op> {
        let _clock = cellar_native::web::WallClockGuard::enter(primitive_wall_ms);
        if self.activation_retired(entry) {
            return self.poll(entry);
        }
        let _installed = self.cells.install();
        let _guard = crate::host_io::CurrentGuard::enter(entry.context.clone());
        if let Some(native) = &entry.native {
            let mut callback_budget_failed = false;
            let controller = native.lock().unwrap().incoming_abort.clone();
            if let Some(controller) = controller {
                // JS also reports listener exceptions independently of the
                // client-disconnect result. A throw/panic cannot undo abort or
                // keep the abandoned handler and routed calls alive.
                entry.context.begin_cpu_turn();
                let callback = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    controller.abort(Some(cellar_native::web::AbortReason::error(
                        "The client has disconnected",
                    )))
                }));
                if let Err(error) = entry.context.finish_cpu_turn() {
                    callback_budget_failed = true;
                    entry.failure = Some(crate::telemetry::cap_error(error));
                } else {
                    let failure = match callback {
                        Ok(Ok(())) => None,
                        Ok(Err(error)) => Some(error.to_string()),
                        Err(panic) => Some(panic_error(panic).to_string()),
                    };
                    if let Some(failure) = failure {
                        entry.failure = Some(crate::telemetry::cap_error(failure));
                    }
                }
            }
            let mut native = native.lock().unwrap();
            native.stop_handler();
            if shutdown || callback_budget_failed {
                native.background.clear();
                native.state.background.lock().unwrap().clear();
            }
        }
        entry.fail_cancelled(anyhow!("The client has disconnected"));
        entry.context.end_event_registrations();
        if shutdown {
            entry.context.seal_wait_until();
        }
        self.poll(entry)
    }

    pub(crate) fn retire(&mut self, entry: &InFlight) {
        let _installed = self.cells.install();
        let _clock = cellar_native::web::WallClockGuard::enter(primitive_wall_ms);
        let _guard = crate::host_io::CurrentGuard::enter(entry.context.clone());
        if let Some(native) = &entry.native {
            let mut native = native.lock().unwrap();
            native.state.retired.store(true, Ordering::Release);
            native.stop_handler();
            native.background.clear();
            native.state.background.lock().unwrap().clear();
            native.unrefed.clear();
            let unrefed = std::mem::take(&mut *native.state.unrefed.lock().unwrap());
            drop(unrefed);
            native.initialization.clear();
            native.delivery_gate = None;
            native.wake = None;
        }
    }

    pub(crate) fn finish_alarm(&self, entry: &mut InFlight) -> Result<Option<u64>> {
        let _installed = self.cells.install();
        if self.activation_retired(entry) {
            entry.alarm = None;
            return Ok(entry
                .context
                .native_activation
                .as_ref()
                .and_then(|activation| *activation.resources.retired_position.lock().unwrap()));
        }
        entry.settle_alarm(false, false);
        entry.write_delta()
    }

    fn activation_retired(&self, entry: &InFlight) -> bool {
        entry
            .context
            .native_activation
            .as_ref()
            .is_some_and(|activation| {
                activation.resources.retired.load(Ordering::Acquire)
                    || self
                        .activations
                        .get(&activation.scope)
                        .is_none_or(|current| !Arc::ptr_eq(current, activation))
            })
    }
}

fn flush_websocket_capture(state: &EventState, context: &IoContext) {
    let frames = {
        let mut capture = state.websocket_capture.lock().unwrap();
        let Some(frames) = capture.as_mut().filter(|frames| !frames.is_empty()) else {
            return;
        };
        std::mem::take(frames)
    };
    let gate = egress_gate_request(context, cellar_logic::Channel::WsHibernatable);
    asyncrt::spawn(async move {
        frames.release(await_egress_gate(gate).await);
    })
    .detach();
}

fn settle(entry: &mut InFlight, state: &EventState, result: Result<Output>) {
    let result = result.and_then(|output| {
        if let Some(error) = entry.scope.as_deref().and_then(storage::sql_critical_error) {
            return Err(anyhow!(error));
        }
        let valid = matches!(
            (&entry.reply, &output),
            (Some(Answer::Fetch(_)), Output::Http(_))
                | (Some(Answer::Rpc(_)), Output::Bytes(_))
                | (Some(Answer::CellRpc(_)), Output::Rpc(_))
                | (Some(Answer::Queue(_)), Output::Queue(_))
                | (Some(Answer::WsMessage(_)), Output::WebSocket(_))
                | (Some(Answer::WsMessage(_)), Output::Done)
                | (Some(Answer::Ack(_) | Answer::Alarm(_)), Output::Done)
        );
        anyhow::ensure!(
            valid,
            "native handler returned the wrong output for its invocation"
        );
        if matches!(&entry.reply, Some(Answer::WsMessage(_))) {
            let captured = state
                .websocket_capture
                .lock()
                .unwrap()
                .take()
                .unwrap_or_default();
            return Ok(Output::WebSocket(match output {
                Output::WebSocket(frames) => captured.join(frames),
                Output::Done => captured,
                _ => unreachable!("validated WebSocket message output"),
            }));
        }
        Ok(output)
    });
    if result.is_err() {
        state.discard_websocket_capture();
    }
    if let Some(report) = &mut entry.tail_report {
        match &result {
            Ok(Output::Http(response)) => report.response_status = Some(response.status),
            Err(error) => report.record_failure(format!("{error:#}")),
            _ => {}
        }
    }
    entry.context.end_event_registrations();
    let output = match result {
        Ok(output) => output,
        Err(error) => {
            entry.settle_alarm(false, true);
            entry.fail_in_turn(error);
            return;
        }
    };
    // Sampling and reply construction stay in the installed storage turn.
    if matches!(&entry.reply, Some(Answer::Alarm(_))) {
        entry.settle_alarm(true, false);
    }
    let positions = match entry.gate_positions() {
        Ok(positions) => positions,
        Err(error) => {
            entry.fail(error);
            return;
        }
    };
    entry.completed_cell_event = entry.scope.is_some();
    let gates = entry.context.take_arm_gates();
    entry.gated_reply = match (
        entry.reply.take().expect("native handler has a reply"),
        output,
    ) {
        (Answer::Fetch(reply), Output::Http(mut response)) => {
            if let Some(crate::engine_api::HttpResponseWebSocket::Cell(target)) =
                &response.websocket
            {
                entry
                    .context
                    .sockets
                    .lock()
                    .unwrap()
                    .retain(|id| *id != target.id);
            }
            response.write_position = positions.0;
            response.observed_position = positions.1;
            send_answer_after_arm_gates(reply, Ok(response), gates)
        }
        (Answer::Rpc(reply), Output::Bytes(bytes)) => {
            send_answer_after_arm_gates(reply, Ok(bytes), gates)
        }
        (Answer::CellRpc(reply), Output::Rpc(data)) => send_answer_after_arm_gates(
            reply,
            Ok(RpcOutcome {
                data,
                write_position: positions.0,
                observed_position: positions.1,
            }),
            gates,
        ),
        (Answer::Queue(reply), Output::Queue(result)) => {
            send_answer_after_arm_gates(reply, Ok(result), gates)
        }
        (Answer::WsMessage(reply), Output::WebSocket(frames)) => send_answer_after_arm_gates(
            reply,
            Ok(WsDispatch {
                frames,
                write_position: positions.0,
                observed_position: positions.1,
            }),
            gates,
        ),
        (Answer::Ack(reply), Output::Done) => {
            send_answer_after_arm_gates(reply, Ok(positions.0), gates)
        }
        (Answer::Alarm(reply), Output::Done) => {
            let scope = entry.scope.as_ref().expect("alarm has a cell");
            send_answer_after_arm_gates(
                reply,
                Ok((observe_alarm(scope, storage::get_alarm(scope)), positions.0)),
                gates,
            )
        }
        _ => unreachable!("output shape was checked before consuming the reply"),
    };
}

pub(crate) fn take_body(context: &Context, stream: u64) -> Result<HttpChunkStream> {
    let mut guard = context
        .io
        .transfer_body_stream(stream)
        .ok_or_else(|| anyhow!("request body is not owned by this event"))?;
    guard.take_stream(stream).map_err(anyhow::Error::msg)
}

pub(crate) fn queue_alarm_wait(context: &Context, at_ms: i64) -> HandlerFuture<()> {
    let prepared: Result<_> = (|| {
        context.storage()?.get_alarm()?;
        let scope = &context.identity().expect("checked cell storage").scope;
        let committed = storage::set_alarm(scope, at_ms)?;
        Ok(committed
            .filter(|at| *at >= 0)
            .and_then(|at| launch_arm_gate(scope, observe_alarm(scope, Some(at)))))
    })();
    Box::pin(async move {
        if let Some(gate) = prepared? {
            gate.await
                .context("Queue wake-entry gate task dropped")?
                .map_err(|error| anyhow!("Queue wake-entry gate: {error}"))?;
        }
        Ok(())
    })
}

pub(crate) fn queue_dispatch(
    context: &Context,
    script: String,
    queue: String,
    envelope: serde_json::Value,
) -> HandlerFuture<()> {
    let prepared: Result<_> = (|| {
        context.check_turn()?;
        let scope = context
            .identity()
            .ok_or_else(|| anyhow!("Queue dispatch requires a cell event"))?
            .scope
            .clone();
        let gate = egress_gate_request(&context.io, cellar_logic::Channel::Queue);
        let request = crate::queue_policy::dispatch_request(
            context.generation,
            scope,
            script,
            queue,
            &envelope.to_string(),
        )?;
        Ok((gate, request))
    })();
    Box::pin(async move {
        let (gate, request) = prepared?;
        gated_channel_send(
            gate,
            &QUEUE_DISPATCH_TX,
            request,
            "no Queue dispatch channel",
        )
        .await
        .map_err(anyhow::Error::msg)
    })
}

pub(crate) fn set_alarm(scope: &str, at_ms: i64) -> Result<()> {
    anyhow::ensure!(
        storage::facet_opening(scope).is_none(),
        "Facets currently cannot set alarms."
    );
    if let Some(at_ms) = storage::set_alarm(scope, at_ms)? {
        if at_ms >= 0 {
            spawn_arm_gate(scope, at_ms, installed_context());
        }
    }
    Ok(())
}

pub(crate) fn transaction_control(
    scope: &str,
    action: &str,
    nested: bool,
    savepoint: &str,
) -> Result<()> {
    if let Some(at_ms) = storage::transaction_control(scope, action, nested, savepoint)
        .map_err(anyhow::Error::msg)?
    {
        if at_ms >= 0 {
            spawn_arm_gate(scope, at_ms, installed_context());
        }
    }
    Ok(())
}

pub(crate) fn storage_sync(scope: &str) -> HandlerFuture<()> {
    let prepared = (|| -> Result<_> {
        anyhow::ensure!(
            GATE_TX.get().is_some(),
            "storage.sync: this process has no output-gate channel"
        );
        let context =
            installed_context().ok_or_else(|| anyhow!("storage.sync: no running cell event"))?;
        let frame = current_cell_event(&context)
            .ok_or_else(|| anyhow!("storage.sync: no running cell event"))?;
        anyhow::ensure!(
            frame.storage == scope,
            "storage.sync: {scope} is not the running event's cell"
        );
        let sample = storage::sync_sample(scope)?
            .ok_or_else(|| anyhow!("storage.sync: {scope} is not resident"))?;
        anyhow::ensure!(!sample.in_transaction, "storage.sync: {scope} has an open transaction; its writes are not committed, so no durability proof can cover them. Call sync() after the transaction commits");
        let gate = match frame.root {
            Some(root) => match storage::sql_critical_error(scope) {
                Some(error) => EgressGate::Unpersisted(error),
                None => EgressGate::Facet(
                    Box::new(EgressGate::Wrote(
                        root.cell,
                        cellar_logic::Channel::Sync,
                        root.position,
                        Some(root.epoch),
                    )),
                    root.stream,
                    root.epoch,
                ),
            },
            None => EgressGate::Wrote(
                scope.to_string(),
                cellar_logic::Channel::Sync,
                sample.position,
                Some(sample.epoch),
            ),
        };
        Ok(gate)
    })();
    let scope = scope.to_string();
    Box::pin(async move {
        egress_gate_verdict(prepared?)
            .await
            .map_err(|refusal| anyhow!("storage.sync: {scope}: {refusal}"))
    })
}

pub(crate) fn fetch_cell(
    context: &Context,
    scope: String,
    name: Option<String>,
    request: Request,
    deliver_abort_to_handler: bool,
) -> HandlerFuture<HttpResponse> {
    if let Err(error) = context
        .check_turn()
        .and_then(|()| context.io.charge_subrequest().map_err(anyhow::Error::msg))
    {
        return Box::pin(async move { Err(error) });
    }
    let gate = egress_gate_request(&context.io, cellar_logic::Channel::CellRpc);
    let order = Some(
        context
            .io
            .call_chains
            .enter_with_owner(&scope, context.io.clone()),
    );
    let body_guard = match request.body.stream_id() {
        Some(stream) => match context.io.transfer_body_stream(stream) {
            Some(guard) => guard,
            None => {
                return Box::pin(async {
                    Err(anyhow!("cell request body is not owned by this event"))
                })
            }
        },
        None => RequestBodyGuard::of(&request.body),
    };
    let (request_id, cancel, mut cancel_guard) = begin_call_cancellation();
    let (reply, receive) = tokio::sync::oneshot::channel();
    let call = DoCallReq {
        request_id: Some(request_id),
        cancel: Some(cancel),
        deliver_abort_to_handler,
        scope,
        name,
        url: request.url,
        method: request.method,
        headers: request.headers,
        body: request.body,
        body_guard,
        order,
        parent: context.trace,
        reply,
    };
    Box::pin(async move {
        gated_channel_send(gate, &DO_CALL_TX, call, "no proxy channel")
            .await
            .map_err(anyhow::Error::msg)?;
        let result = receive.await.context("cell proxy dropped")?;
        cancel_guard.disarm();
        result
    })
}

/// Capture the original activation and output gate before bucket I/O yields.
pub(crate) fn kv_blob(
    context: &Context,
    request: serde_json::Value,
    value: Option<Vec<u8>>,
) -> HandlerFuture<crate::kv_blob::BlobReply> {
    let prepared: Result<_> = (|| {
        context.check_turn()?;
        let identity = context
            .identity()
            .ok_or_else(|| anyhow!("KV blob I/O requires active cell storage"))?;
        Ok((
            identity.scope.clone(),
            identity.epoch,
            egress_gate_request(&context.io, cellar_logic::Channel::Service),
        ))
    })();
    Box::pin(async move {
        let (scope, epoch, gate) = prepared?;
        await_egress_gate(gate).await.map_err(anyhow::Error::msg)?;
        crate::kv_blob::run(&scope, epoch, &request.to_string(), value)
            .await
            .map_err(anyhow::Error::msg)
    })
}

pub(crate) fn rpc_cell(
    context: &Context,
    scope: String,
    name: Option<String>,
    method: String,
    args: RpcData,
) -> HandlerFuture<RpcData> {
    if let Err(error) = context
        .check_turn()
        .and_then(|()| context.io.charge_subrequest().map_err(anyhow::Error::msg))
    {
        return Box::pin(async move { Err(error) });
    }
    let gate = egress_gate_request(&context.io, cellar_logic::Channel::CellRpc);
    Box::pin(crate::host_rpc::call(gate, scope, name, method, args))
}

pub(crate) fn blob_gate(context: &Context) -> HandlerFuture<()> {
    channel_gate(context, cellar_logic::Channel::Blob)
}

pub(crate) fn channel_gate(context: &Context, channel: cellar_logic::Channel) -> HandlerFuture<()> {
    let gate = context
        .check_turn()
        .map(|()| egress_gate_request(&context.io, channel));
    let context = context.clone();
    Box::pin(async move {
        await_egress_gate(gate?).await.map_err(anyhow::Error::msg)?;
        context.check_turn()
    })
}

impl crate::worker::WorkerBackend for NativeWorker {
    fn heap_bytes(&self) -> Option<crate::worker::HeapBytes> {
        None
    }

    fn turn_begin(
        &mut self,
        job: crate::WorkerJob,
        trace: Option<crate::telemetry::TraceContext>,
    ) -> (Option<InFlight>, Vec<Op>) {
        self.begin(job, trace)
    }

    fn turn_cancel(&mut self, entry: &mut InFlight) -> Vec<Op> {
        self.cancel(entry, false)
    }

    fn turn_cancel_for_shutdown(&mut self, entry: &mut InFlight) -> Vec<Op> {
        self.cancel(entry, true)
    }

    fn turn_retire_input_gates(&mut self, entry: &InFlight) {
        self.retire(entry);
        entry.context.force_retire_cross_entry_gates();
        let _ = crate::host_gates::abandon_context_input_gates(&entry.context);
    }

    fn turn_abandon_input_gates(&mut self, entry: &InFlight) {
        let _ = crate::host_gates::abandon_context_input_gates(&entry.context);
    }

    fn turn_deliver(
        &mut self,
        entry: &mut InFlight,
        op: u64,
        res: Result<crate::asyncrt::OpOut, String>,
    ) -> Vec<Op> {
        let _ = res;
        entry.ops.remove(&op);
        self.poll(entry)
    }

    fn own_cell(
        &mut self,
        cell: &str,
        storage: Option<CellStorage<'_>>,
    ) -> Result<cellar_logic::wake::AlarmSnapshot> {
        crate::native_worker::NativeWorker::own_cell(self, cell, storage)
    }

    fn settle_heap(&mut self) {}

    fn own_embedded_cell(
        &mut self,
        cell: &str,
        parent: &storage::StorageIdentity,
        name: &str,
        id_sc: Vec<u8>,
        props_sc: Vec<u8>,
        file: (std::path::PathBuf, bool),
    ) -> Result<u64> {
        NativeWorker::own_embedded_cell(self, cell, parent, name, id_sc, props_sc, file)
    }

    fn close_facet_tree(
        &mut self,
        cell: &str,
        root: &str,
        facet_path: &[String],
    ) -> (Vec<String>, Result<()>) {
        NativeWorker::close_facet_tree(self, cell, root, facet_path)
    }

    fn take_alarm_moves(&mut self) -> Vec<(String, cellar_logic::wake::AlarmSnapshot)> {
        let _installed = self.cells.install();
        crate::storage::take_alarm_moves()
    }

    fn set_id_name(&mut self, scope: &str, name: &str) -> Result<()> {
        let _installed = self.cells.install();
        crate::storage::set_actor_name(scope, name)
    }

    fn turn_begin_cell(
        &mut self,
        job: CellJob,
        trace: Option<crate::telemetry::TraceContext>,
    ) -> (Option<InFlight>, Vec<Op>) {
        self.begin_cell(job, trace)
    }

    fn turn_poll(&mut self, entry: &mut InFlight) -> Vec<Op> {
        self.poll(entry)
    }

    fn turn_cancel_pending_events(&mut self, entry: &mut InFlight) -> Vec<Op> {
        self.poll(entry)
    }

    fn turn_finish_alarm(&mut self, entry: &mut InFlight) -> Result<Option<u64>> {
        self.finish_alarm(entry)
    }
}
