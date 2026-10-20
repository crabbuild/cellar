use crate::{
    operations::{self, State},
    runtime::{self, Event, InvocationBridge, Life, Runtime, Start},
    values,
};
use anyhow::{ensure, Result};
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
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, serde::Serialize)]
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
    pub(crate) code: String,
    pub(crate) filename: String,
    pub(crate) classes: Vec<String>,
    pub(crate) events: Vec<EventKind>,
    pub(crate) cell_events: std::collections::BTreeMap<String, Vec<EventKind>>,
    pub(crate) distributions: Vec<(String, String)>,
}

impl Source {
    pub fn new(code: impl Into<String>, filename: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            filename: filename.into(),
            classes: Vec::new(),
            distributions: Vec::new(),
            events: Vec::new(),
            cell_events: Default::default(),
        }
    }

    pub fn cell(mut self, name: impl Into<String>) -> Self {
        self = self.cell_events(name, [EventKind::Fetch]);
        self
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
        self.classes.push(name.clone());
        self.cell_events.insert(name, events.into_iter().collect());
        self
    }

    pub(crate) fn descriptor(&self) -> Value {
        json!({"events":self.events, "cells":self.cell_events})
    }

    fn supports(&self, class: Option<&str>, kind: &str) -> bool {
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
        let _permit = self
            .runtime
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| anyhow::anyhow!("Python invocation capacity exhausted"))?;
        let (kind, payload) = if let Some(batch) = &state.queue {
            (
                "queue",
                json!({"queue":batch.queue(),"messages":batch.messages().iter().map(|message|Ok(json!({"id":message.id,"attempts":message.attempts,"timestamp_ms":message.timestamp_ms,"body":values::queue_body(&message.body)?}))).collect::<Result<Vec<_>>>()?}),
            )
        } else {
            match event {
                Invocation::Fetch(request) => (
                    "fetch",
                    values::request(&context, request, self.runtime.options.max_body_bytes).await?,
                ),
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
        let active = Arc::new(AtomicBool::new(true));
        let _lifetime = Lifetime(active.clone());
        let (sender, mut receive) = runtime::channel();
        let invocation = self.runtime.id();
        let payload = serde_json::to_string(
            &json!({"kind":kind,"event":payload,"object":object,"class":class,"identity":identity}),
        )?;
        ensure!(
            payload.len() <= self.runtime.options.max_value_bytes,
            "Python invocation exceeds value budget"
        );
        self.runtime.start(Start {
            id: invocation,
            reservation: self.runtime.budget.reserve(payload.len())?,
            payload,
            bridge: InvocationBridge {
                sender,
                active: active.clone(),
                owner,
                operations: AtomicUsize::new(0),
                mutation: Arc::new(AtomicBool::new(false)),
                max_operations: self.runtime.options.max_operations,
                max_bytes: self.runtime.options.max_value_bytes,
                budget: self.runtime.budget.clone(),
                event_loop: None,
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
                result = pending.next(), if !pending.is_empty() => {
                    let (id, encoded, observation) = result.expect("nonempty native operation set");
                    self.runtime.reply(invocation, id, encoded?, observation)?;
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
                    mutation,
                    reservation,
                    mut observation,
                } => {
                    if !active.load(Ordering::Acquire) {
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
                        let result = match serde_json::from_str(&payload) {
                            Ok(payload) => {
                                operations::execute(
                                    context,
                                    state,
                                    &name,
                                    payload,
                                    &self.runtime.options,
                                )
                                .await
                            }
                            Err(error) => {
                                Err(anyhow::Error::from(error).context(crate::failure::Validation))
                            }
                        };
                        observation.complete(result.is_ok());
                        let envelope = match result {
                            Ok(value) => json!({"ok":true,"value":value}),
                            Err(error) => crate::failure::envelope(&name, &error),
                        };
                        let encoded = serde_json::to_string(&envelope)
                            .and_then(|encoded| {
                                if encoded.len() <= self.runtime.options.max_value_bytes {
                                    Ok(encoded)
                                } else {
                                    Err(serde::ser::Error::custom(
                                        "Python SDK result exceeds value limit",
                                    ))
                                }
                            })
                            .map_err(anyhow::Error::from);
                        (id, encoded, observation)
                    });
                    // Initiate operations in receive order. Polling once here
                    // establishes FIFO start even for futures awaiting nested
                    // work; subsequent readiness may finish out of order.
                    match std::future::poll_fn(|cx| {
                        std::task::Poll::Ready(operation.as_mut().poll(cx))
                    })
                    .await
                    {
                        std::task::Poll::Ready((id, encoded, observation)) => {
                            self.runtime.reply(invocation, id, encoded?, observation)?
                        }
                        std::task::Poll::Pending => pending.push(operation),
                    }
                }
                Event::Complete {
                    payload,
                    failed,
                    reservation,
                } => {
                    let _reservation = reservation;
                    ensure!(!failed, "Python handler failed");
                    let value: Value = serde_json::from_str(&payload)?;
                    return if kind == "fetch" {
                        Ok(Output::Http(values::output(
                            &context,
                            &value,
                            self.runtime.options.max_body_bytes,
                        )?))
                    } else {
                        Ok(Output::Done)
                    };
                }
            }
        }
        anyhow::bail!("Python invocation channel closed before completion")
    }
}

struct Lifetime(Life);

impl Drop for Lifetime {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

impl Application for PythonApplication {
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
            object: self.runtime.id(),
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
