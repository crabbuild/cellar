// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use crate::{
    operations::{self, State},
    runtime::{self, Event, InvocationBridge, InvocationState, Life, Lifetime, Runtime, Start},
    values,
};
use anyhow::{ensure, Result};
use cellar::sdk::rpc::{Catalog, Contract, Prepared};
use cellar::sdk::{
    queue, Application, Cell, CellIdentity, Context, HandlerFuture, Invocation, Output,
};
use futures_util::{stream::FuturesUnordered, Future, StreamExt};
use serde_json::{json, Value};
use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};

/// Handler registrations validated before the Rust host admits any work.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum EventKind {
    Fetch,
    Alarm,
    Queue,
    Scheduled,
}

impl EventKind {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Fetch => "fetch",
            Self::Alarm => "alarm",
            Self::Queue => "queue",
            Self::Scheduled => "scheduled",
        }
    }
}

/// Python source included in the compiled host, with explicitly registered cells.
#[derive(Clone)]
pub struct Source {
    pub(crate) rpc: Catalog,
    pub(crate) code: String,
    pub(crate) filename: String,
    pub(crate) classes: Vec<String>,
    pub(crate) events: Vec<EventKind>,
    pub(crate) cell_events: std::collections::BTreeMap<String, Vec<EventKind>>,
    pub(crate) initial_reads:
        std::collections::BTreeMap<String, std::collections::BTreeMap<EventKind, Vec<String>>>,
    pub(crate) distributions: Vec<(String, String)>,
}

impl Source {
    pub fn new(code: impl Into<String>, filename: impl Into<String>) -> Self {
        Self {
            rpc: Catalog::new(),
            code: code.into(),
            filename: filename.into(),
            classes: Vec::new(),
            distributions: Vec::new(),
            events: Vec::new(),
            cell_events: Default::default(),
            initial_reads: Default::default(),
        }
    }

    pub fn cell(mut self, name: impl Into<String>) -> Self {
        self = self.cell_events(name, [EventKind::Fetch]);
        self
    }

    /// Register unary business methods without requiring an HTTP fetch handler.
    pub fn cell_rpc(mut self, class: &str, service: &str, contract: Arc<Contract>) -> Result<Self> {
        self.rpc.register(class, service, contract)?;
        if !self.classes.iter().any(|name| name == class) {
            self.classes.push(class.into());
            self.cell_events.insert(class.into(), Vec::new());
        }
        Ok(self)
    }

    /// Install an interface used by generated callers, without exposing methods.
    pub fn rpc_contract(mut self, contract: Arc<Contract>) -> Result<Self> {
        self.rpc.add_contract(contract)?;
        Ok(self)
    }

    pub fn events(mut self, events: impl IntoIterator<Item = EventKind>) -> Self {
        self.events = events.into_iter().collect();
        self
    }

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

    /// Capture declared JSON snapshots on the original native cell turn before
    /// dispatching this event to Python. The handler receives `ctx.initial_storage`.
    /// These snapshots do not reserve keys or refresh after an await or mutation;
    /// use checked writes to detect intervening changes. Capture failure refuses
    /// the invocation before the Python handler runs.
    pub fn cell_initial_reads<K: Into<String>>(
        mut self,
        class: impl Into<String>,
        event: EventKind,
        keys: impl IntoIterator<Item = K>,
    ) -> Self {
        self.initial_reads
            .entry(class.into())
            .or_default()
            .insert(event, keys.into_iter().map(Into::into).collect());
        self
    }

    pub(crate) fn descriptor(&self) -> Value {
        let mut descriptor = json!({"events":self.events, "cells":self.cell_events});
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
        if !self.initial_reads.is_empty() {
            descriptor["initial_reads"] = json!(self.initial_reads);
        }
        descriptor
    }

    pub(crate) fn validate_initial_reads(&self, maximum: usize) -> Result<()> {
        for (class, events) in &self.initial_reads {
            for (event, keys) in events {
                ensure!(
                    self.supports(Some(class), event.name()),
                    "initial reads require a registered Python cell event"
                );
                ensure!(
                    keys.len() <= maximum,
                    "initial reads exceed operation limit"
                );
                let unique: HashSet<_> = keys.iter().collect();
                ensure!(unique.len() == keys.len(), "duplicate initial read key");
            }
        }
        Ok(())
    }

    fn supports(&self, class: Option<&str>, kind: &str) -> bool {
        if kind == "rpc" {
            return class.is_some_and(|name| self.rpc.classes().any(|class| class == name));
        }
        let events = match class {
            Some(class) => self.cell_events.get(class),
            None => Some(&self.events),
        };
        events.is_some_and(|events| events.iter().any(|event| event.name() == kind))
    }

    /// Refuse startup unless the installed distribution has this exact version.
    /// Deploy the same locked environment alongside every executable replica.
    pub fn require_distribution(
        mut self,
        name: impl Into<String>,
        version: impl Into<String>,
    ) -> Self {
        self.distributions.push((name.into(), version.into()));
        self
    }

    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            !self.code.is_empty() && !self.filename.is_empty(),
            "Python source and filename must be supplied"
        );
        ensure!(
            !self.code.as_bytes().contains(&0) && !self.filename.as_bytes().contains(&0),
            "Python source and filename must not contain NUL bytes"
        );
        for events in std::iter::once(&self.events).chain(self.cell_events.values()) {
            let unique: HashSet<_> = events.iter().collect();
            ensure!(
                unique.len() == events.len(),
                "duplicate Python event registration"
            );
        }
        let mut seen = HashSet::new();
        for name in &self.classes {
            ensure!(
                !name.is_empty() && seen.insert(name),
                "duplicate or empty Python class"
            );
        }
        let mut distributions = HashSet::new();
        for (name, version) in &self.distributions {
            ensure!(
                !name.is_empty() && !version.is_empty() && distributions.insert(name),
                "empty or duplicate Python distribution pin"
            );
        }
        Ok(())
    }
}

/// Native application proxy. Python never receives a raw Rust SDK context.
pub struct PythonApplication {
    runtime: Arc<Runtime>,
    source: Arc<Source>,
}

impl PythonApplication {
    pub(crate) fn new(runtime: Arc<Runtime>, source: Source) -> Self {
        Self {
            runtime,
            source: Arc::new(source),
        }
    }

    /// A live, nonblocking snapshot of native bridge resource accounting.
    /// Does not enter Python or confer invocation authority.
    pub fn bridge_report(&self) -> crate::BridgeReport {
        self.runtime.budget.report()
    }

    /// Last sampled interpreter state, without acquiring the GIL or blocking.
    /// `None` means startup has not sampled yet or the publisher is busy. Check
    /// the returned age; a blocked Python loop cannot refresh this observation.
    pub fn interpreter_snapshot(&self) -> Option<crate::InterpreterSnapshot> {
        self.runtime.samples.read()
    }

    async fn run(
        &self,
        context: Context,
        event: Invocation,
        cell: Option<(u64, String, Life)>,
        state: State,
    ) -> Result<Output> {
        let _permit =
            self.runtime
                .permits
                .clone()
                .try_acquire_owned()
                .map_err(|error| match error {
                    tokio::sync::TryAcquireError::NoPermits => {
                        anyhow::Error::new(cellar::sdk::ApplicationOverloaded)
                    }
                    tokio::sync::TryAcquireError::Closed => {
                        anyhow::anyhow!("Python runtime admission is closed")
                    }
                })?;
        let mut body = Vec::new();
        let mut rpc_prepared = None;
        let (kind, payload) = if let Some(batch) = &state.queue {
            (
                "queue",
                json!({"queue":batch.queue(),"messages":batch.messages().iter().map(|message|Ok(json!({"id":message.id,"attempts":message.attempts,"timestamp_ms":message.timestamp_ms,"body":values::queue_body(&message.body)?}))).collect::<Result<Vec<_>>>()?}),
            )
        } else {
            match event {
                Invocation::CellRpc { method, args } => {
                    let class = &context
                        .identity()
                        .ok_or_else(|| anyhow::anyhow!("cell RPC requires a cell identity"))?
                        .class;
                    if let Some(refusal) = self
                        .source
                        .rpc
                        .refusal_after_factory(class, &method, &args)?
                    {
                        return Ok(Output::Rpc(refusal));
                    }
                    let contract = self
                        .source
                        .rpc
                        .provider(class, &method)
                        .ok_or_else(|| anyhow::anyhow!("undeclared Python RPC method"))?;
                    let (digest, bytes) = cellar::sdk::rpc::decode_envelope(
                        &args,
                        contract.limits().max_message_bytes,
                    )?;
                    let mut prepared = Prepared::new(contract, &method, digest, bytes)?;
                    body = prepared.take_request();
                    rpc_prepared = Some(prepared);
                    ("rpc", json!({"method":method}))
                }
                Invocation::Fetch(request) => {
                    let (metadata, bytes) =
                        values::request(&context, request, self.runtime.options.max_body_bytes)
                            .await?;
                    body = bytes;
                    ("fetch", metadata)
                }
                Invocation::Alarm { scheduled_ms } => {
                    ("alarm", json!({"scheduled_ms":scheduled_ms}))
                }
                Invocation::Scheduled(ref scheduled) => (
                    "scheduled",
                    json!({"scheduled_ms":scheduled.scheduled_ms,"cron":scheduled.cron}),
                ),
                _ => anyhow::bail!("unsupported Python invocation"),
            }
        };
        let identity = context
            .identity()
            .map(|id| json!({"name":id.name,"class":id.class,"scope":id.scope,"epoch":id.epoch}));
        let (object, class, owner) = cell
            .map(|(object, class, life)| (Some(object), Some(class), life))
            .unwrap_or((None, None, Arc::new(AtomicBool::new(true))));
        ensure!(
            self.source.supports(class.as_deref(), kind),
            "undeclared Python event {kind}"
        );
        let active = Arc::new(InvocationState::new());
        let _lifetime = Lifetime(active.clone(), self.runtime.clone());
        let (sender, mut receive) = runtime::channel(self.runtime.options.max_pending_operations);
        let (cancelled, mut cancellation) = tokio::sync::watch::channel(false);
        let invocation = self.runtime.id()?;
        let mut payload = serde_json::to_string(
            &json!({"kind":kind,"event":payload,"object":object,"class":class,"identity":identity}),
        )?;
        if let Some(keys) = class
            .as_ref()
            .and_then(|class| self.source.initial_reads.get(class))
            .and_then(|events| events.iter().find(|(event, _)| event.name() == kind))
            .map(|(_, keys)| keys)
            .filter(|keys| !keys.is_empty())
        {
            let body_charge = values::envelope_size(0, body.len())?;
            let maximum = self
                .runtime
                .options
                .max_value_bytes
                .checked_sub(body_charge)
                .ok_or_else(|| anyhow::anyhow!("Python invocation exceeds value budget"))?;
            payload = crate::read_batch::append_initial(&context, payload, keys, maximum)?;
        }
        let charge = if kind == "rpc" {
            payload
                .len()
                .checked_add(body.len())
                .ok_or_else(|| anyhow::anyhow!("RPC invocation size overflow"))?
        } else {
            values::envelope_size(payload.len(), body.len())?
        };
        ensure!(
            charge <= self.runtime.options.max_value_bytes,
            "Python invocation exceeds value budget"
        );
        self.runtime.start(Start {
            queued_at: cellar::asyncrt::instant(),
            id: invocation,
            reservation: self.runtime.budget.reserve(charge)?,
            payload,
            body,
            bridge: InvocationBridge {
                lifecycle: self.runtime.lifecycle(),
                sender,
                active: active.clone(),
                // Keep a sender here so ordinary bridge collection cannot
                // close the cancellation channel ahead of queued completion.
                cancelled: cancelled.clone(),
                completion_enqueued: AtomicBool::new(false),
                owner,
                operations: AtomicUsize::new(0),
                mutation: Arc::new(AtomicBool::new(false)),
                rpc_calls: Arc::default(),
                rpc_budget_source: cellar::sdk::rpc::BudgetSource::capture(&context)?,
                max_operations: self.runtime.options.max_operations,
                max_bytes: self.runtime.options.max_value_bytes,
                raw_rpc: kind == "rpc",
                budget: self.runtime.budget.clone(),
                event_loop: None,
                scope: None,
                clock_origin: cellar::asyncrt::instant(),
            },
        })?;
        let mut stopped = self.runtime.stopped.subscribe();
        let mut pending = FuturesUnordered::new();
        loop {
            ensure!(!*stopped.borrow(), "Python runtime stopped");
            // All futures stay inside this invocation and its original native
            // authority. Poll the mailbox while a nested SDK call awaits I/O.
            let event = cellar::asyncrt::select_biased! { "runtime shutdown revokes authority before queued operations";
                _ = stopped.changed() => anyhow::bail!("Python runtime stopped"),
                _ = cancellation.changed() => anyhow::bail!("Python invocation cancelled"),
                result = pending.next(), if !pending.is_empty() => {
                    let (id, encoded, observation, raw) = result.expect("nonempty native operation set");
                    self.runtime.reply(invocation, id, encoded?, observation, raw)?;
                    continue;
                },
                event = receive.recv() => event,
            };
            let Some(event) = event else { break };
            ensure!(
                !self.runtime.closed.load(Ordering::Acquire),
                "Python runtime is closed"
            );
            match event {
                Event::Operation {
                    id,
                    name,
                    payload,
                    body,
                    rpc,
                    rpc_budget,
                    mutation,
                    reservation,
                    mut observation,
                } => {
                    if !active.active() {
                        continue;
                    }
                    let context = &context;
                    let state = &state;
                    let mut operation = Box::pin(async move {
                        observation.begin();
                        // The reservation is retained by the native future even
                        // if the awaiting Python task has already been cancelled.
                        let _mutation = mutation;
                        let _reservation = reservation;
                        let _rpc = rpc;
                        let result = match serde_json::from_str(&payload) {
                            Ok(payload) if name == "cell.call" => {
                                operations::call(
                                    context,
                                    &self.source.rpc,
                                    payload,
                                    body,
                                    _rpc.as_ref().map(|call| call.signal()),
                                    rpc_budget,
                                )
                                .await
                            }
                            Ok(payload) => {
                                operations::execute(
                                    context,
                                    state,
                                    &name,
                                    payload,
                                    body,
                                    &self.runtime.options,
                                )
                                .await
                            }
                            Err(error) => {
                                Err(anyhow::Error::from(error).context(crate::failure::Validation))
                            }
                        };
                        observation.complete(result.is_ok());
                        let encoded = crate::failure::result(
                            &name,
                            result,
                            self.runtime.options.max_value_bytes,
                        );
                        (id, encoded, observation, name == "cell.call")
                    });
                    // Initiate operations in receive order. Polling once here
                    // establishes FIFO start even for futures awaiting nested
                    // work; subsequent readiness may finish out of order.
                    match std::future::poll_fn(|cx| {
                        std::task::Poll::Ready(operation.as_mut().poll(cx))
                    })
                    .await
                    {
                        std::task::Poll::Ready((id, encoded, observation, raw)) => self
                            .runtime
                            .reply(invocation, id, encoded?, observation, raw)?,
                        std::task::Poll::Pending => pending.push(operation),
                    }
                }
                Event::Complete {
                    queued_at,
                    payload,
                    body,
                    failed,
                    reservation,
                } => {
                    let _reservation = reservation;
                    self.runtime
                        .budget
                        .completion_handoff
                        .observe(queued_at.elapsed());
                    if let Some(prepared) = rpc_prepared.take() {
                        let result = if failed {
                            let failure = match payload.as_str() {
                                "{\"error\":\"InvalidResponse\"}" => {
                                    cellar::sdk::rpc::Failure::new(
                                        cellar::sdk::rpc::Code::InvalidResponse,
                                    )
                                }
                                "{\"error\":\"Overloaded\"}" => cellar::sdk::rpc::Failure::new(
                                    cellar::sdk::rpc::Code::ResourceExhausted,
                                ),
                                _ => cellar_guest::rpc_failure::failure(&payload),
                            };
                            Err(failure)
                        } else {
                            body.ok_or_else(|| {
                                cellar::sdk::rpc::Failure::new(
                                    cellar::sdk::rpc::Code::InvalidResponse,
                                )
                            })
                        };
                        return Ok(Output::Rpc(prepared.finish(result)?));
                    }
                    if failed {
                        if payload == "{\"error\":\"Overloaded\"}" {
                            return Err(cellar::sdk::ApplicationOverloaded.into());
                        }
                        anyhow::bail!("Python handler failed");
                    }
                    let value: Value = serde_json::from_str(&payload).map_err(|_| {
                        anyhow::anyhow!("Python handler completion decoding failed")
                    })?;
                    return if kind == "fetch" {
                        // Serde and HTTP conversion errors may quote malformed
                        // application values. Native ingress formats the full
                        // handler error chain, so replace it at this boundary.
                        values::output(&context, &value, body, self.runtime.options.max_body_bytes)
                            .map(Output::Http)
                            .map_err(|_| anyhow::anyhow!("Python response conversion failed"))
                    } else {
                        ensure!(body.is_none(), "Python event completion has an HTTP body");
                        Ok(Output::Done)
                    };
                }
            }
        }
        anyhow::bail!("Python invocation channel closed before completion")
    }
}

impl Application for PythonApplication {
    fn rpc_catalog(&self) -> Option<&Catalog> {
        Some(&self.source.rpc)
    }

    fn validate_configuration(
        &self,
        options: &cellar::engine_api::WorkerConfigOptions,
    ) -> Result<()> {
        ensure!(
            options.queue_consumers.is_empty() || self.source.supports(None, "queue"),
            "Python queue consumers require a declared queue handler"
        );
        for class in &options.do_classes {
            ensure!(
                cellar::deploy::is_reserved_class(class) || self.supports_cell(class),
                "Python deployment names an unregistered cell class"
            );
        }
        Ok(())
    }

    fn validate_deployment(&self, deployment: &cellar::fleet::LoadedDeployment) -> Result<()> {
        self.validate_configuration(&deployment.options)?;
        ensure!(
            deployment.crons.is_empty() || self.source.supports(None, "scheduled"),
            "Python cron declarations require a declared scheduled handler"
        );
        Ok(())
    }

    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        if let Invocation::Queue(batch) = invocation {
            return queue::dispatch(batch, move |event| async move {
                self.run(
                    context,
                    Invocation::Alarm { scheduled_ms: 0 },
                    None,
                    State {
                        queue: Some(event),
                        scheduled: None,
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
                    queue: None,
                    scheduled,
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
            "unknown Python cell class"
        );
        Ok(Arc::new(PythonCell {
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

struct PythonCell {
    application: Arc<PythonApplication>,
    object: u64,
    class: String,
    life: Life,
}

impl Drop for PythonCell {
    fn drop(&mut self) {
        self.life.store(false, Ordering::Release);
        self.application.runtime.retire();
    }
}

impl Cell for PythonCell {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            self.application
                .run(
                    context,
                    event,
                    Some((self.object, self.class.clone(), self.life.clone())),
                    State {
                        queue: None,
                        scheduled: None,
                    },
                )
                .await
        })
    }
}

#[cfg(test)]
#[path = "application_tests.rs"]
mod tests;
