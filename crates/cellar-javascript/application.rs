// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Translate native application calls into bounded engine messages, then execute
//! each requested SDK operation inside the original native invocation.
use crate::runtime::{Event, Life, Runtime, Start};
use anyhow::{ensure, Result};
use cellar::sdk::{
    queue, Application, Cell, CellIdentity, Context, HandlerFuture, Invocation, Output,
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
    containers: cellar_guest::containers::Adapter,
    pub(crate) code: String,
    pub(crate) filename: String,
    pub(crate) classes: Vec<String>,
    pub(crate) events: Vec<EventKind>,
    pub(crate) cell_events: std::collections::BTreeMap<String, Vec<EventKind>>,
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
            containers: Default::default(),
            code: code.into(),
            filename: filename.into(),
            classes: Vec::new(),
            events: Vec::new(),
            cell_events: Default::default(),
        }
    }

    /// Register a cell class with a fetch handler. Use [`Self::cell_events`]
    /// when the class also handles alarms or other supported events.
    pub fn cell(mut self, name: impl Into<String>) -> Self {
        self = self.cell_events(name, [EventKind::Fetch]);
        self
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
        self.classes.push(name.clone());
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

    pub(crate) fn descriptor(&self) -> Value {
        json!({
            "events": self.events,
            "cells": self.cell_events
        })
    }

    fn supports(&self, class: Option<&str>, kind: &str) -> bool {
        let events = match class {
            Some(class) => self.cell_events.get(class),
            None => Some(&self.events),
        };
        events.is_some_and(|events| events.iter().any(|event| event.name() == kind))
    }

    pub(crate) fn validate(&self) -> Result<()> {
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
}

impl JavaScriptApplication {
    pub(crate) fn new(runtime: Arc<Runtime>, source: Source) -> Self {
        Self {
            runtime,
            source: Arc::new(source),
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

    async fn run(
        self: &Arc<Self>,
        context: Context,
        event: Invocation,
        cell: Option<(u64, String, Life)>,
        mut state: State,
    ) -> Result<Output> {
        if matches!(event, Invocation::Fetch(_)) && state.queue.is_none() {
            let app = self.clone();
            let producer = context.clone();
            return cellar_guest::transport::serve(&context, move |session| async move {
                state.transport = session;
                app.run_inner(producer, event, cell, state).await
            })
            .await;
        }
        self.run_inner(context, event, cell, state).await
    }

    async fn run_inner(
        &self,
        context: Context,
        event: Invocation,
        cell: Option<(u64, String, Life)>,
        state: State,
    ) -> Result<Output> {
        let mut state = state;
        state.containers = self.source.containers.session();
        let _permit = self
            .runtime
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| anyhow::anyhow!("JavaScript invocation capacity exhausted"))?;

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
                Invocation::Fetch(request) => (
                    "fetch",
                    values::request(&context, request, self.runtime.options.max_body_bytes).await?,
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
        ensure!(
            payload.len() <= self.runtime.options.max_value_bytes,
            "JavaScript invocation exceeds value budget"
        );
        self.runtime.start(Start {
            id: invocation,
            _bytes: self.runtime.reserve(payload.len())?,
            payload,
            sender,
            active: active.clone(),
            owner,
        })?;

        let mut stopped = self.runtime.stopped.subscribe();
        let mut pending = FuturesUnordered::new();
        loop {
            ensure!(!*stopped.borrow(), "JavaScript runtime stopped");
            // All futures stay inside this invocation and its original native
            // authority. Poll the mailbox while a nested SDK call awaits I/O.
            let event = cellar::asyncrt::select_biased! { "runtime shutdown revokes authority before queued operations";
                _ = stopped.changed() => anyhow::bail!("JavaScript runtime stopped"),
                result = pending.next(), if !pending.is_empty() => {
                    let (id, encoded) = result.expect("nonempty native operation set");
                    self.runtime.reply(invocation, id, encoded?)?;
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
                    _bytes,
                } => {
                    if !active.load(Ordering::Acquire) {
                        continue;
                    }
                    let context = &context;
                    let state = &state;
                    let mut operation = Box::pin(async move {
                        // The reservation is retained by the native future even
                        // if the awaiting JavaScript task has already been cancelled.
                        let _bytes = _bytes;
                        let result = match serde_json::from_str(&payload) {
                            Ok(payload) => {
                                operations::execute(
                                    context,
                                    state,
                                    &name,
                                    payload,
                                    &cellar_guest::Limits {
                                        max_body_bytes: self.runtime.options.max_body_bytes,
                                        max_batch_operations: self
                                            .runtime
                                            .options
                                            .max_batch_operations,
                                    },
                                )
                                .await
                            }
                            Err(error) => Err(anyhow::Error::from(error)
                                .context(cellar_guest::failure::Validation)),
                        };
                        let encoded = cellar_guest::failure::result(
                            &name,
                            result,
                            self.runtime.options.max_value_bytes,
                        );
                        (id, encoded)
                    });
                    // Initiate operations in receive order. Polling once here
                    // establishes FIFO start even for futures awaiting nested
                    // work; subsequent readiness may finish out of order.
                    match std::future::poll_fn(|cx| {
                        std::task::Poll::Ready(operation.as_mut().poll(cx))
                    })
                    .await
                    {
                        std::task::Poll::Ready((id, encoded)) => {
                            self.runtime.reply(invocation, id, encoded?)?
                        }
                        std::task::Poll::Pending => pending.push(operation),
                    }
                }
                Event::Complete {
                    payload,
                    failed,
                    _bytes,
                } => {
                    let _bytes = _bytes;
                    ensure!(!failed, "JavaScript handler failed");
                    let value: Value = serde_json::from_str(&payload).map_err(|_| {
                        anyhow::anyhow!("JavaScript handler completion decoding failed")
                    })?;
                    return if kind == "fetch" {
                        if let Some(output) = state
                            .transport
                            .output(&context, &value)
                            .map_err(|_| anyhow::anyhow!("invalid guest transport response"))?
                        {
                            return Ok(output);
                        }
                        // Serde and HTTP conversion errors may quote malformed
                        // application values. Native ingress formats the full
                        // handler error chain, so replace it at this boundary.
                        values::output(&context, &value, self.runtime.options.max_body_bytes)
                            .map(Output::Http)
                            .map_err(|_| anyhow::anyhow!("JavaScript response conversion failed"))
                    } else {
                        Ok(Output::Done)
                    };
                }
            }
        }
        anyhow::bail!("JavaScript invocation channel closed before completion")
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
