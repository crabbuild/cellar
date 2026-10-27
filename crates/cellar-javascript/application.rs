// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Translate native application calls into bounded engine messages, then execute
//! each requested SDK operation inside the original native invocation.
#[cfg(test)]
#[path = "application_tests.rs"]
mod tests;

use crate::runtime::{Event, Life, Runtime, Start};
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{
    queue,
    rpc::{Catalog, Contract, Prepared},
    Application, Cell, CellIdentity, Context, HandlerFuture, Invocation, Output,
};
use cellar_guest::{
    operations::{self, State},
    values,
};
use futures_util::{stream::FuturesUnordered, Future, StreamExt};
use serde_json::{json, Value};
use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

/// Handler registrations validated before the Rust host admits any work.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum EventKind {
    Fetch,
    /// Stateless Worker entrypoint RPC through native service/loopback routes.
    #[serde(rename = "worker_rpc")]
    WorkerRpc,
    /// Native cell RPC, with structured arguments and results.
    #[serde(rename = "cell_rpc")]
    CellRpc,
    Alarm,
    Queue,
    Scheduled,
    #[serde(rename = "websocket_open")]
    WebSocketOpen,
    #[serde(rename = "websocket_message")]
    WebSocketMessage,
    #[serde(rename = "websocket_close")]
    WebSocketClose,
}

impl EventKind {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Fetch => "fetch",
            Self::WorkerRpc => "worker_rpc",
            Self::CellRpc => "cell_rpc",
            Self::Alarm => "alarm",
            Self::Queue => "queue",
            Self::Scheduled => "scheduled",
            Self::WebSocketOpen => "websocket_open",
            Self::WebSocketMessage => "websocket_message",
            Self::WebSocketClose => "websocket_close",
        }
    }
}

/// JavaScript source and handler registrations checked before host admission.
///
/// Use [`Source::new`] for an embedded bundle. With the `node` feature,
/// `Source::module` describes registrations for an external Node module.
#[derive(Clone)]
pub struct Source {
    pub(crate) workers_lifetimes: bool,
    containers: cellar_guest::containers::Adapter,
    container_bindings: cellar_guest::containers::Bindings,
    pub(crate) code: String,
    pub(crate) filename: String,
    pub(crate) classes: Vec<String>,
    pub(crate) events: Vec<EventKind>,
    pub(crate) cell_events: std::collections::BTreeMap<String, Vec<EventKind>>,
    pub(crate) rpc: Catalog,
}

impl Source {
    /// Registrations for an external Node module. `NodeOptions::entry` selects
    /// the actual module; its exports are validated before host admission.
    #[cfg(feature = "node")]
    pub fn module() -> Self {
        Self::new("external Node module", "node")
    }

    /// `code` must define `CellarApp`, as emitted by the Cellar npm build tool.
    pub fn new(code: impl Into<String>, filename: impl Into<String>) -> Self {
        Self {
            workers_lifetimes: false,
            containers: Default::default(),
            container_bindings: Default::default(),
            code: code.into(),
            filename: filename.into(),
            classes: Vec::new(),
            events: Vec::new(),
            cell_events: Default::default(),
            rpc: Catalog::new(),
        }
    }

    /// Register a cell class with a fetch handler. Use [`Self::cell_events`]
    /// when the class also handles alarms or other supported events.
    pub fn cell(mut self, name: impl Into<String>) -> Self {
        self = self.cell_events(name, [EventKind::Fetch]);
        self
    }

    /// Declare a service without requiring an HTTP handler. The native catalog
    /// validates requests before the lazy JavaScript business constructor runs.
    pub fn cell_rpc(mut self, class: &str, service: &str, contract: Arc<Contract>) -> Result<Self> {
        self.rpc.register(class, service, contract)?;
        if !self.classes.iter().any(|name| name == class) {
            self.classes.push(class.into());
            self.cell_events.insert(class.into(), Vec::new());
        }
        Ok(self)
    }

    /// Include a consumer contract without declaring a provider class.
    pub fn rpc_contract(mut self, contract: Arc<Contract>) -> Result<Self> {
        self.rpc.add_contract(contract)?;
        Ok(self)
    }

    /// Set application-level handlers, such as ingress fetches or queue batches.
    /// Registrations describe exports; deployment configuration supplies bindings.
    pub fn events(mut self, events: impl IntoIterator<Item = EventKind>) -> Self {
        self.events = events.into_iter().collect();
        self
    }

    /// Register a named cell class and its supported handlers.
    /// Duplicate class or event registrations fail during runtime startup.
    pub fn cell_events(
        mut self,
        name: impl Into<String>,
        events: impl IntoIterator<Item = EventKind>,
    ) -> Self {
        let name = name.into();
        let rpc_only = self.rpc.classes().any(|class| class == name)
            && self.cell_events.get(&name).is_some_and(Vec::is_empty);
        if !rpc_only {
            self.classes.push(name.clone());
        }
        self.cell_events.insert(name, events.into_iter().collect());
        self
    }

    /// Select a trusted container/sandbox backend before starting the runtime.
    /// Omission uses Cellar's native Docker/Podman provider.
    pub fn container_provider(
        mut self,
        provider: Arc<dyn cellar_guest::containers::Provider>,
    ) -> Self {
        self.containers = cellar_guest::containers::Adapter::new(provider);
        self
    }

    /// Declare an additional provider slot before runtime startup. The provider's
    /// durable binding name must equal `name`; duplicate slots refuse.
    pub fn container_binding(
        mut self,
        name: impl Into<String>,
        provider: Arc<dyn cellar_guest::containers::Provider>,
    ) -> Result<Self> {
        self.container_bindings.insert(name, provider)?;
        self.container_bindings
            .validate(self.containers.provider())?;
        Ok(self)
    }

    /// Enable native-owned Workers event/background lifetimes. This selects
    /// lifetime semantics only, not a complete Workers API compatibility profile.
    pub fn workers_lifetimes(mut self) -> Self {
        self.workers_lifetimes = true;
        self
    }

    pub(crate) fn descriptor(&self) -> Value {
        let mut descriptor = json!({
            "workers_lifetimes": self.workers_lifetimes,
            "events": self.events,
            "cells": self.cell_events
        });
        let contracts = self.rpc.codec_inventory();
        if contracts
            .as_object()
            .is_some_and(|entries| !entries.is_empty())
        {
            descriptor["rpc_contracts"] = contracts;
        }
        if self.rpc.classes().next().is_some() {
            descriptor["rpc"] = self.rpc.inventory();
        }
        descriptor
    }

    fn supports(&self, class: Option<&str>, kind: &str) -> bool {
        if kind == "rpc" {
            return class.is_some_and(|class| self.rpc.classes().any(|name| name == class));
        }
        let events = match class {
            Some(class) => self.cell_events.get(class),
            None => Some(&self.events),
        };
        events.is_some_and(|events| events.iter().any(|event| event.name() == kind))
    }

    pub(crate) fn validate(&self) -> Result<()> {
        self.container_bindings
            .validate(self.containers.provider())?;
        ensure!(
            !self.code.is_empty() && !self.filename.is_empty(),
            "JavaScript source and filename must be supplied"
        );
        ensure!(
            !self.code.as_bytes().contains(&0) && !self.filename.as_bytes().contains(&0),
            "JavaScript source and filename must not contain NUL bytes"
        );
        for events in std::iter::once(&self.events).chain(self.cell_events.values()) {
            let unique: HashSet<_> = events.iter().collect();
            ensure!(
                unique.len() == events.len(),
                "duplicate JavaScript event registration"
            );
        }
        let mut seen = HashSet::new();
        for name in &self.classes {
            ensure!(
                !name.is_empty() && seen.insert(name),
                "duplicate or empty JavaScript class"
            );
        }
        Ok(())
    }
}

/// Native application proxy. JavaScript never receives a raw Rust SDK context.
pub struct JavaScriptApplication {
    pub(crate) runtime: Arc<Runtime>,
    source: Arc<Source>,
    rpc_sessions: Arc<crate::rpc_sessions::Registry>,
}

impl JavaScriptApplication {
    pub(crate) fn new(runtime: Arc<Runtime>, source: Source) -> Self {
        Self {
            runtime,
            source: Arc::new(source),
            rpc_sessions: Default::default(),
        }
    }

    /// Completes when the engine stops. Pass this to `Host::run_until` so a
    /// runtime failure also drains the native node before bootstrap returns.
    pub async fn stopped(&self) {
        let mut stopped = self.runtime.stopped.subscribe();
        while !*stopped.borrow_and_update() {
            if stopped.changed().await.is_err() {
                break;
            }
        }
    }

    /// Observe native invocation and payload reservations. A stopped engine
    /// can still have native work retiring; check cleanup after callers and
    /// the host have joined, rather than interpreting `stopped` as a drain.
    pub fn bridge_usage(&self) -> crate::BridgeUsage {
        self.runtime.usage()
    }

    async fn run(
        self: &Arc<Self>,
        context: Context,
        event: Invocation,
        cell: Option<(u64, String, Life)>,
        mut state: State,
    ) -> Result<Output> {
        if matches!(&event, Invocation::CellRpc { method, .. } if method == crate::rpc_sessions::METHOD)
        {
            let Invocation::CellRpc { args, .. } = event else {
                anyhow::bail!("invalid delegate event")
            };
            ensure!(
                self.source.workers_lifetimes,
                "Workers result sessions are not enabled"
            );
            let object = cell
                .as_ref()
                .map(|cell| cell.0)
                .context("delegate requires an activation")?;
            ensure!(
                self.source
                    .supports(cell.as_ref().map(|cell| cell.1.as_str()), "cell_rpc"),
                "delegate requires a declared cell RPC handler"
            );
            return self
                .rpc_sessions
                .dispatch(&context, object, args, &self.runtime)
                .await
                .map(Output::Rpc);
        }
        let retained = self.source.workers_lifetimes
            && !matches!(&event, Invocation::CellRpc { .. }
            if context.identity().is_some_and(|identity| self.source.rpc.classes().any(|class| class == identity.class)));
        state.workers_lifetimes = retained;
        if matches!(event, Invocation::Fetch(_)) && state.queue.is_none() {
            let app = self.clone();
            let producer = context.clone();
            let make = move |session| async move {
                state.transport = session;
                app.run_inner(producer, event, cell, state, None).await
            };
            return if retained {
                cellar_guest::transport::serve_retained(&context, make).await
            } else {
                cellar_guest::transport::serve(&context, make).await
            };
        }
        if retained {
            let app = self.clone();
            let producer = context.clone();
            return crate::lifetime::serve(&context, move |reply| async move {
                app.run_inner(producer, event, cell, state, Some(reply))
                    .await
            })
            .await;
        }
        self.run_inner(context, event, cell, state, None).await
    }

    async fn run_inner(
        &self,
        context: Context,
        event: Invocation,
        cell: Option<(u64, String, Life)>,
        state: State,
        reply: Option<crate::lifetime::Reply>,
    ) -> Result<Output> {
        let mut state = state;
        state.containers = self.source.containers.session();
        state.container_bindings = self.source.container_bindings.sessions();
        let state = Arc::new(state);
        let callbacks = Arc::new(crate::callbacks::Callbacks::default());
        let _permit = self
            .runtime
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| anyhow::anyhow!("JavaScript invocation capacity exhausted"))?;

        let mut body = None;
        let mut rpc_prepared = None;
        let (kind, payload) = if let Some(batch) = &state.queue {
            (
                "queue",
                json!({
                    "queue": batch.queue(),
                    "messages": batch.messages().iter().map(|message| {
                        Ok(json!({
                            "id": message.id,
                            "attempts": message.attempts,
                            "timestamp_ms": message.timestamp_ms,
                            "body": values::queue_body(&message.body)?
                        }))
                    }).collect::<Result<Vec<_>>>()?
                }),
            )
        } else {
            match event {
                Invocation::CellRpc { method, args }
                    if context.identity().is_some_and(|identity| {
                        !self
                            .source
                            .rpc
                            .classes()
                            .any(|class| class == identity.class)
                    }) =>
                {
                    (
                        "cell_rpc",
                        cellar_guest::workers::rpc::request(
                            method,
                            args,
                            self.runtime.options.max_body_bytes,
                        )?,
                    )
                }
                Invocation::CellRpc { method, args } => {
                    let identity = context
                        .identity()
                        .ok_or_else(|| anyhow::anyhow!("cell RPC requires a cell identity"))?;
                    if let Some(refusal) =
                        self.source
                            .rpc
                            .refusal_after_factory(&identity.class, &method, &args)?
                    {
                        return Ok(Output::Rpc(refusal));
                    }
                    let contract = self
                        .source
                        .rpc
                        .provider(&identity.class, &method)
                        .ok_or_else(|| anyhow::anyhow!("undeclared JavaScript RPC method"))?;
                    let (digest, bytes) = cellar::sdk::rpc::decode_envelope(
                        &args,
                        contract.limits().max_message_bytes,
                    )?;
                    let mut prepared = Prepared::new(contract, &method, digest, bytes)?;
                    body = Some(prepared.take_request());
                    rpc_prepared = Some(prepared);
                    ("rpc", json!({"method":method}))
                }
                Invocation::Fetch(mut request) => {
                    let entrypoint = request.entrypoint.take();
                    let mut frame =
                        values::request(&context, request, self.runtime.options.max_body_bytes)
                            .await?;
                    if let Some(entrypoint) = entrypoint {
                        frame["entrypoint"] = json!({"name":entrypoint.name, "props": cellar_guest::workers::service::props(&entrypoint.props, self.runtime.options.max_body_bytes)?});
                    }
                    ("fetch", frame)
                }
                Invocation::Rpc {
                    entrypoint,
                    operation,
                    props,
                } => (
                    "worker_rpc",
                    cellar_guest::workers::service::request(
                        entrypoint,
                        operation,
                        props,
                        self.runtime.options.max_body_bytes,
                    )?,
                ),
                Invocation::Alarm { scheduled_ms } => {
                    ("alarm", json!({"scheduled_ms": scheduled_ms}))
                }
                Invocation::Scheduled(ref scheduled) => (
                    "scheduled",
                    json!({
                        "scheduled_ms": scheduled.scheduled_ms,
                        "cron": scheduled.cron
                    }),
                ),
                event => {
                    cellar_guest::transport::invocation(event, self.runtime.options.max_body_bytes)?
                }
            }
        };

        // Epochs travel as strings so JavaScript cannot round a native u64.
        let identity = context.identity().map(|id| {
            json!({
                "name": id.name,
                "class": id.class,
                "scope": id.scope,
                "epoch": id.epoch.to_string()
            })
        });
        let (object, class, owner) = cell
            .map(|(object, class, life)| (Some(object), Some(class), life))
            .unwrap_or((None, None, Arc::new(AtomicBool::new(true))));
        let rpc_session = Arc::new(crate::rpc_sessions::Session::new(
            self.rpc_sessions.clone(),
            &context,
            object,
            owner.clone(),
            self.runtime.options.clone(),
        )?);
        ensure!(
            self.source.supports(class.as_deref(), kind),
            "undeclared JavaScript event {kind}"
        );

        // Invocation authority and activation ownership have separate lifetimes.
        // Dropping this future revokes `active`; dropping the cell revokes `owner`.
        let active = Arc::new(AtomicBool::new(true));
        let _lifetime = Lifetime(active.clone());
        // A completion must fit even when every operation slot is occupied.
        let (sender, mut receive) =
            tokio::sync::mpsc::channel(self.runtime.options.max_pending_operations + 1);
        let invocation = self.runtime.id()?;
        let payload = serde_json::to_string(&json!({
            "kind": kind,
            "event": payload,
            "object": object,
            "class": class,
            "identity": identity
        }))?;
        let charge = crate::runtime::value_size(&payload, body.as_deref())?;
        ensure!(
            charge <= self.runtime.options.max_value_bytes,
            "JavaScript invocation exceeds value budget"
        );
        self.runtime.start(Start {
            _completion: Default::default(),
            budget_source: Some(cellar::sdk::rpc::BudgetSource::capture(&context)?),
            id: invocation,
            _bytes: self.runtime.reserve(charge)?,
            payload,
            body,
            sender,
            active: active.clone(),
            owner: owner.clone(),
        })?;

        let mut stopped = self.runtime.stopped.subscribe();
        let mut pending = FuturesUnordered::new();
        let mut published = false;
        loop {
            ensure!(!*stopped.borrow(), "JavaScript runtime stopped");
            // All futures stay inside this invocation and its original native
            // authority. Poll the mailbox while a nested SDK call awaits I/O.
            let event = cellar::asyncrt::select_biased! { "runtime shutdown revokes authority before queued operations";
                _ = stopped.changed() => anyhow::bail!("JavaScript runtime stopped"),
                result = pending.next(), if !pending.is_empty() => {
                    let (id, encoded, synchronous) = result.expect("nonempty native operation set");
                    self.runtime.respond_parts(invocation, id, encoded?, synchronous)?;
                    continue;
                },
                event = receive.recv() => event,
            };
            let Some(event) = event else { break };
            ensure!(
                !self.runtime.closed.load(Ordering::Acquire),
                "JavaScript runtime is closed"
            );
            match event {
                Event::Operation {
                    id,
                    name,
                    payload,
                    callback_scope,
                    blocking,
                    synchronous,
                    body,
                    signal: _,
                    budget: _,
                    _bytes,
                } if name == "workers.lifetime.response" => {
                    let result = (|| -> Result<Value> {
                        ensure!(
                            self.source.workers_lifetimes
                                && kind != "rpc"
                                && body.is_none()
                                && !published
                                && !blocking
                                && callback_scope.is_none(),
                            cellar_guest::failure::Validation
                        );
                        let input: Value = serde_json::from_str(&payload)
                            .context(cellar_guest::failure::Validation)?;
                        ensure!(
                            input.as_object().is_some_and(|value| value.len() == 2),
                            cellar_guest::failure::Validation
                        );
                        let encoded = input["value"]
                            .as_str()
                            .context(cellar_guest::failure::Validation)?;
                        let value: Value = serde_json::from_str(encoded)
                            .context(cellar_guest::failure::Validation)?;
                        let failed = input["failed"]
                            .as_bool()
                            .context(cellar_guest::failure::Validation)?;
                        let output = if failed {
                            Err(anyhow::anyhow!("JavaScript handler failed"))
                        } else {
                            self.output(&context, kind, &state, &rpc_session, value)
                        };
                        rpc_session.discard_unpublished();
                        if let Some(reply) = &reply {
                            crate::lifetime::publish(reply, output);
                        } else {
                            state.transport.publish(output)?;
                        }
                        published = true;
                        Ok(Value::Null)
                    })();
                    self.runtime.respond(
                        invocation,
                        id,
                        cellar_guest::failure::result(
                            &name,
                            result,
                            self.runtime.options.max_value_bytes,
                        )?,
                        synchronous,
                    )?;
                }
                event if matches!(&event, Event::Operation { name, .. } if name == "workers.callback.start_sync") =>
                {
                    crate::callbacks::Pump {
                        runtime: &self.runtime,
                        context: &context,
                        state: &state,
                        callbacks: &callbacks,
                        receive: &mut receive,
                        active: &active,
                        owner: &owner,
                        invocation,
                    }
                    .serve(event)?;
                }
                Event::Operation {
                    id,
                    name,
                    payload,
                    callback_scope,
                    blocking: _,
                    synchronous,
                    body,
                    signal,
                    budget,
                    _bytes,
                } => {
                    if !active.load(Ordering::Acquire) {
                        continue;
                    }
                    let context = context.clone();
                    let state = state.clone();
                    let callbacks = callbacks.clone();
                    let runtime = self.runtime.clone();
                    let catalog = self.source.rpc.clone();
                    let rpc_session = rpc_session.clone();
                    let rpc_enabled = self.source.workers_lifetimes;
                    let mut operation = Box::pin(async move {
                        // The reservation is retained by the native future even
                        // if the awaiting JavaScript task has already been cancelled.
                        let _bytes = _bytes;
                        let result =
                            match operations::decode_metadata_with(&name, &payload, |bytes| {
                                Ok(runtime
                                    .bytes
                                    .clone()
                                    .try_acquire_many_owned(u32::try_from(bytes)?)?)
                            }) {
                                Ok((mut payload, _control_bytes)) => {
                                    // Automatic discard readers hold an invocation-owned
                                    // process capability. This route has no SQL view and
                                    // is limited to observations of that event's handles.
                                    let event_owned = match &mut payload {
                                        Value::Object(fields)
                                            if crate::callbacks::event_owned_observation(&name)
                                                && fields.get("$event_owned")
                                                    == Some(&Value::Bool(true)) =>
                                        {
                                            fields.remove("$event_owned");
                                            true
                                        }
                                        _ => false,
                                    };
                                    // A parent can be draining an unawaited child
                                    // after its JS callback returned. Completion is
                                    // owned by this invocation and the named child;
                                    // it must not borrow the already closed parent.
                                    let scope = if event_owned
                                        || matches!(
                                            name.as_str(),
                                            "workers.callback.finish" | "container.release"
                                        ) {
                                        // Releasing an event-owned resource is cleanup,
                                        // and never borrows a callback's storage view.
                                        Ok(None)
                                    } else {
                                        callback_scope
                                            .map(|id| callbacks.scope(&context, id))
                                            .transpose()
                                    };
                                    match scope {
                                        Err(error) => Err(error),
                                        // A transaction cannot prove its own commit
                                        // while awaiting an external container effect.
                                        Ok(Some(scope))
                                            if scope.is_transaction()
                                                && matches!(
                                                    name.as_str(),
                                                    "workers.container.admit"
                                                        | "workers.container.perform"
                                                        | "container.exec"
                                                        | "container.exec_output"
                                                ) =>
                                        {
                                            Err(anyhow::Error::new(
                                                cellar_guest::failure::Validation,
                                            ))
                                        }
                                        Ok(scope) => {
                                            let scoped_context = context.clone();
                                            let operation_name = name.clone();
                                            let options = runtime.options.clone();
                                            let scope_id = id;
                                            let future =
                                                async move {
                                                    if operation_name == "cell.call" {
                                                        let digest = payload
                                                            .get("contract_digest")
                                                            .and_then(Value::as_str)
                                                            .context("missing RPC contract digest")
                                                            .context(
                                                                cellar_guest::failure::Validation,
                                                            )?;
                                                        let contract = catalog
                                                            .contract(digest)
                                                            .context("unregistered RPC contract")
                                                            .context(
                                                                cellar_guest::failure::Validation,
                                                            )?;
                                                        let bytes = body
                                                    .context("RPC call requires a byte attachment")
                                                    .context(cellar_guest::failure::Validation)?;
                                                        let result =
                                                            operations::call_bytes_in_budget(
                                                                &scoped_context,
                                                                contract,
                                                                payload,
                                                                bytes,
                                                                signal,
                                                                budget,
                                                            )
                                                            .await?;
                                                        return Ok((Value::Null, Some(result)));
                                                    }
                                                    ensure!(
                                                        body.is_none(),
                                                        cellar_guest::failure::Validation
                                                    );
                                                    if rpc_enabled
                                                        && callback_scope.is_none()
                                                        && ((operation_name == "workers.rpc"
                                                            && payload.get("service").is_none())
                                                            || operation_name
                                                                .starts_with("workers.rpc."))
                                                    {
                                                        rpc_session
                                                            .execute(
                                                                &scoped_context,
                                                                &operation_name,
                                                                payload,
                                                                signal,
                                                            )
                                                            .await
                                                            .map(|value| (value, None))
                                                    } else if operation_name
                                                        .starts_with("workers.callback.")
                                                    {
                                                        callbacks
                                                            .execute(
                                                                &scoped_context,
                                                                &operation_name,
                                                                payload,
                                                                scope_id,
                                                                options.max_batch_operations,
                                                            )
                                                            .await
                                                            .map(|value| (value, None))
                                                    } else {
                                                        operations::execute_with_signal(
                                                            &scoped_context,
                                                            &state,
                                                            &operation_name,
                                                            payload,
                                                            &cellar_guest::Limits {
                                                                max_body_bytes: options
                                                                    .max_body_bytes,
                                                                max_batch_operations: options
                                                                    .max_batch_operations,
                                                            },
                                                            signal,
                                                        )
                                                        .await
                                                        .map(|value| (value, None))
                                                    }
                                                };
                                            match scope {
                                            Some(scope)
                                                if !crate::callbacks::event_owned_observation(
                                                    &name,
                                                ) =>
                                            {
                                                scope.run(&context, future).await
                                            }
                                            _ => future.await,
                                        }
                                        }
                                    }
                                }
                                Err(error) => Err(error),
                            };
                        let encoded = cellar_guest::failure::result_parts(
                            &name,
                            result,
                            runtime.options.max_value_bytes,
                        );
                        (id, encoded, synchronous)
                    });
                    // Initiate operations in receive order. Polling once here
                    // establishes FIFO start even for futures awaiting nested
                    // work; subsequent readiness may finish out of order.
                    match std::future::poll_fn(|cx| {
                        std::task::Poll::Ready(operation.as_mut().poll(cx))
                    })
                    .await
                    {
                        std::task::Poll::Ready((id, encoded, synchronous)) => self
                            .runtime
                            .respond_parts(invocation, id, encoded?, synchronous)?,
                        std::task::Poll::Pending => pending.push(operation),
                    }
                }
                Event::Complete {
                    payload,
                    body,
                    failed,
                    _bytes,
                } => {
                    let _bytes = _bytes;
                    if let Some(prepared) = rpc_prepared.take() {
                        let result = if failed {
                            Err(cellar_guest::rpc_failure::failure(&payload))
                        } else {
                            body.ok_or_else(|| {
                                cellar::sdk::rpc::Failure::new(
                                    cellar::sdk::rpc::Code::InvalidResponse,
                                )
                            })
                        };
                        return Ok(Output::Rpc(prepared.finish(result)?));
                    }
                    ensure!(!failed, "JavaScript handler failed");
                    let value: Value = serde_json::from_str(&payload).map_err(|_| {
                        anyhow::anyhow!("JavaScript handler completion decoding failed")
                    })?;
                    return if published {
                        Ok(Output::Done)
                    } else {
                        self.output(&context, kind, &state, &rpc_session, value)
                    };
                }
            }
        }
        anyhow::bail!("JavaScript invocation channel closed before completion")
    }

    fn output(
        &self,
        context: &Context,
        kind: &str,
        state: &State,
        rpc_session: &crate::rpc_sessions::Session,
        value: Value,
    ) -> Result<Output> {
        if kind == "fetch" {
            if let Some(output) = state
                .transport
                .output(context, &value)
                .map_err(|_| anyhow::anyhow!("invalid guest transport response"))?
            {
                return Ok(output);
            }
            // Serde and HTTP conversion errors may quote malformed
            // application values. Native ingress formats the full
            // handler error chain, so replace it at this boundary.
            values::output(context, &value, self.runtime.options.max_body_bytes)
                .map(Output::Http)
                .map_err(|_| anyhow::anyhow!("JavaScript response conversion failed"))
        } else if kind == "worker_rpc" {
            cellar_guest::workers::service::output(value, self.runtime.options.max_body_bytes)
                .map(Output::Bytes)
                .map_err(|_| anyhow::anyhow!("JavaScript Worker RPC response conversion failed"))
        } else if kind == "cell_rpc" {
            rpc_session
                .output(value)
                .map(Output::Rpc)
                .map_err(|_| anyhow::anyhow!("JavaScript RPC response conversion failed"))
        } else {
            Ok(Output::Done)
        }
    }
}

/// Revoke invocation authority on completion, error, or future cancellation.
struct Lifetime(Life);

impl Drop for Lifetime {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

impl Application for JavaScriptApplication {
    fn rpc_catalog(&self) -> Option<&Catalog> {
        Some(&self.source.rpc)
    }

    fn validate_configuration(
        &self,
        options: &cellar::engine_api::WorkerConfigOptions,
    ) -> Result<()> {
        ensure!(
            options.queue_consumers.is_empty() || self.source.supports(None, "queue"),
            "JavaScript queue consumers require a declared queue handler"
        );
        for class in &options.do_classes {
            ensure!(
                cellar::deploy::is_reserved_class(class) || self.supports_cell(class),
                "JavaScript deployment names an unregistered cell class"
            );
        }
        Ok(())
    }

    fn validate_deployment(&self, deployment: &cellar::fleet::LoadedDeployment) -> Result<()> {
        self.validate_configuration(&deployment.options)?;
        ensure!(
            deployment.crons.is_empty() || self.source.supports(None, "scheduled"),
            "JavaScript cron declarations require a declared scheduled handler"
        );
        Ok(())
    }

    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        if let Invocation::Queue(batch) = invocation {
            return queue::dispatch(batch, move |event| async move {
                self.run(
                    context,
                    // Queue state selects the event before this placeholder is read.
                    Invocation::Alarm { scheduled_ms: 0 },
                    None,
                    State {
                        transport: Default::default(),
                        queue: Some(event),
                        scheduled: None,
                        ..Default::default()
                    },
                )
                .await?;
                Ok(())
            });
        }
        let scheduled = if let Invocation::Scheduled(event) = &invocation {
            Some(event.clone())
        } else {
            None
        };
        Box::pin(async move {
            self.run(
                context,
                invocation,
                None,
                State {
                    transport: Default::default(),
                    queue: None,
                    scheduled,
                    ..Default::default()
                },
            )
            .await
        })
    }

    fn supports_cell(&self, class: &str) -> bool {
        self.source.classes.iter().any(|name| name == class)
    }

    fn create_cell(&self, _context: Context, identity: &CellIdentity) -> Result<Arc<dyn Cell>> {
        ensure!(
            self.supports_cell(&identity.class),
            "unknown JavaScript cell class"
        );
        Ok(Arc::new(JavaScriptCell {
            application: Arc::new(Self {
                runtime: self.runtime.clone(),
                source: self.source.clone(),
                rpc_sessions: self.rpc_sessions.clone(),
            }),
            object: self.runtime.id()?,
            class: identity.class.clone(),
            life: Arc::new(AtomicBool::new(true)),
        }))
    }
}

/// One activation-local object identity, shared across calls until cell retirement.
struct JavaScriptCell {
    application: Arc<JavaScriptApplication>,
    object: u64,
    class: String,
    life: Life,
}

impl Drop for JavaScriptCell {
    fn drop(&mut self) {
        self.life.store(false, Ordering::Release);
    }
}

impl Cell for JavaScriptCell {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            self.application
                .run(
                    context,
                    event,
                    Some((self.object, self.class.clone(), self.life.clone())),
                    State {
                        transport: Default::default(),
                        queue: None,
                        scheduled: None,
                        ..Default::default()
                    },
                )
                .await
        })
    }
}
