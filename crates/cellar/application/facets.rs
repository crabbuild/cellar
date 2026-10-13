//! Activation-owned child cells using the ordinary host turn and stream gates.
//! Pass the current event context to every operation, including operations on
//! handles retained by a constructor. Startup callbacks run once, on first use.

use super::concurrency::{CallTask, Cancellation};
use super::*;
use crate::host_facets::{self, FacetHost, FacetStart};
use crate::host_gates::{await_egress_gate, egress_gate_request};
use crate::http_streams::RequestBodyGuard;
use cellar_native::clone::{encode, Object, StructuredValue, Value};
use futures_util::FutureExt;
use std::sync::atomic::Ordering;
use std::sync::Weak;
use tokio::sync::watch;

/// An exported cell class and its copied startup properties. A class with a
/// storage migration must be addressed through its namespace instead.
#[derive(Clone)]
pub struct Class {
    name: String,
    slot: Weak<crate::pool::Slot>,
    props: Arc<Vec<u8>>,
    loaded: Option<super::loader::Worker>,
    service: Option<Weak<dyn host_facets::FacetService>>,
    props_limit: usize,
}

impl Class {
    pub(crate) fn loaded(worker: super::loader::Worker, name: String) -> Self {
        let props_limit = worker.props_limit();
        Self {
            name,
            props_limit,
            slot: Weak::new(),
            props: Arc::new(Vec::new()),
            loaded: Some(worker),
            service: None,
        }
    }

    pub fn with_props(&self, props: &StructuredValue) -> Result<Self> {
        // Loaded entrypoints/classes accept every clonable value. The local
        // ctx.exports specialization below retains its object-only contract.
        if self.loaded.is_some() {
            return Ok(Self {
                props: Arc::new(super::loader::props_bytes(props, self.props_limit)?),
                ..self.clone()
            });
        }
        let default_props;
        let props = if props.root == Value::Undefined {
            default_props = StructuredValue::from_json(serde_json::json!({}))?;
            &default_props
        } else {
            anyhow::ensure!(
                matches!(props.root, Value::Reference(_)),
                "DurableObjectClass props must be an object."
            );
            props
        };
        let bytes = super::loader::props_bytes(props, self.props_limit)?;
        Ok(Self {
            props: Arc::new(bytes),
            ..self.clone()
        })
    }

    fn host(&self, caller: &Weak<crate::pool::Slot>) -> Result<FacetHost> {
        if let Some(worker) = &self.loaded {
            return Ok(match worker.capture()? {
                Some(state) => FacetHost::Loaded(state),
                None => FacetHost::NativeLoaded(worker.clone()),
            });
        }
        let service = self
            .service
            .as_ref()
            .and_then(Weak::upgrade)
            .ok_or_else(|| anyhow!("facet class execution domain has retired"))?;
        host_facets::check_facet_service(&service).map_err(anyhow::Error::msg)?;
        let slot = self
            .slot
            .upgrade()
            .ok_or_else(|| anyhow!("facet class worker has retired"))?;
        Ok(if Weak::ptr_eq(caller, &self.slot) {
            FacetHost::Own(slot)
        } else {
            FacetHost::Referenced(slot)
        })
    }
}

pub struct Startup {
    pub class: Class,
    /// A cloned literal string or `[class, id, name]` Durable Object ID tuple.
    /// Omission inherits the parent's identity, including its name and kind.
    pub id: Option<StructuredValue>,
}

impl Startup {
    pub fn new(class: Class) -> Self {
        Self { class, id: None }
    }
}

/// The original structured reason supplied to `Facets::abort`.
#[derive(Clone, Debug)]
pub struct AbortError {
    pub reason: Arc<StructuredValue>,
}

impl std::fmt::Display for AbortError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.reason.root {
            Value::String(text) => f.write_str(&String::from_utf16_lossy(text)),
            _ => f.write_str("Durable Object facet was aborted"),
        }
    }
}

impl std::error::Error for AbortError {}

/// A shared startup or lifecycle failure, with its original Rust error and
/// source chain retained across callers rather than reduced to its message.
#[derive(Clone, Debug)]
pub struct CompletionError {
    pub cause: Arc<anyhow::Error>,
}

impl std::fmt::Display for CompletionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.cause, f)
    }
}

impl std::error::Error for CompletionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.cause.as_ref().as_ref())
    }
}

fn completion_error(cause: Arc<anyhow::Error>) -> anyhow::Error {
    CompletionError { cause }.into()
}

#[derive(Clone)]
struct Completion<T: Clone> {
    receiver: watch::Receiver<Option<std::result::Result<T, Arc<anyhow::Error>>>>,
}

impl<T: Clone> Completion<T> {
    async fn wait(mut self) -> std::result::Result<T, Arc<anyhow::Error>> {
        loop {
            if let Some(value) = self.receiver.borrow().clone() {
                return value;
            }
            if self.receiver.changed().await.is_err() {
                return Err(Arc::new(anyhow!(
                    "facet startup event retired before completion"
                )));
            }
        }
    }
}

type Callback = Box<dyn FnOnce(Context) -> HandlerFuture<Startup> + Send>;

struct Started {
    class: Class,
    id: Vec<u8>,
}

struct Starting {
    callback: Option<Callback>,
    completion: Option<Completion<Arc<Started>>>,
}

struct Record {
    name: String,
    owner: String,
    aborted: Mutex<Option<Arc<StructuredValue>>>,
    starting: Mutex<Starting>,
    barrier: Option<Completion<()>>,
}

#[derive(Default)]
struct Entries {
    running: HashMap<String, Arc<Record>>,
    barriers: HashMap<String, (u64, Completion<()>)>,
    next: u64,
}

pub(crate) struct State {
    activation: Weak<Activation>,
    slot: Weak<crate::pool::Slot>,
    scope: String,
    owner: u64,
    entries: Mutex<Entries>,
}

/// One manager per cell activation, shared by its events and constructor.
#[derive(Clone)]
pub struct Facets(Arc<State>);

#[derive(Clone)]
pub struct Facet {
    state: Arc<State>,
    record: Arc<Record>,
}

impl Context {
    pub fn exported_cell_class(&self, name: &str) -> Result<Class> {
        self.check_turn()?;
        anyhow::ensure!(
            !self
                .worker_config
                .do_classes
                .iter()
                .any(|class| class == name),
            "a class with a storage migration cannot supply a facet"
        );
        anyhow::ensure!(
            self.worker_config
                .native_application
                .as_ref()
                .is_some_and(|application| application.supports_cell(name)),
            "no native cell class named {name}"
        );
        let slot = crate::pool::current_slot()
            .ok_or_else(|| anyhow!("facet class: no worker runs this turn"))?;
        Ok(Class {
            name: name.into(),
            slot: Arc::downgrade(&slot),
            props: Arc::new(super::loader::props_bytes(
                &StructuredValue::from_json(serde_json::json!({}))?,
                self.io.host_services().loader_policy().max_props_bytes,
            )?),
            loaded: None,
            service: Some(Arc::downgrade(&self.io.host_services().facets())),
            props_limit: self.io.host_services().loader_policy().max_props_bytes,
        })
    }

    pub fn facets(&self) -> Result<Facets> {
        self.check_turn()?;
        let activation =
            self.io.native_activation.as_ref().ok_or_else(|| {
                anyhow!("facets are available only inside a Durable Object event")
            })?;
        let slot = crate::pool::current_slot()
            .ok_or_else(|| anyhow!("facets: no worker runs this turn"))?;
        let mut stored = activation.resources.facets.lock().unwrap();
        Ok(Facets(
            stored
                .get_or_insert_with(|| {
                    Arc::new(State {
                        activation: Arc::downgrade(activation),
                        slot: Arc::downgrade(&slot),
                        scope: activation.scope.clone(),
                        owner: self.io.host_services().facets().next_manager(),
                        entries: Mutex::new(Entries::default()),
                    })
                })
                .clone(),
        ))
    }
}

impl State {
    fn check(&self, context: &Context) -> Result<Arc<Activation>> {
        context.check_turn()?;
        let activation = self
            .activation
            .upgrade()
            .ok_or_else(|| anyhow!("facet manager activation has retired"))?;
        anyhow::ensure!(
            context
                .io
                .native_activation
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, &activation)),
            "facet handle belongs to another cell activation"
        );
        Ok(activation)
    }
}

fn valid_name(name: &str) -> Result<()> {
    anyhow::ensure!(
        name.len() <= 256,
        "Facet name is too long (max 256 characters)."
    );
    Ok(())
}

fn inherited_id(context: &Context) -> Result<StructuredValue> {
    if let Some(facet) = context.facet() {
        return Ok(facet.id.clone());
    }
    let identity = context
        .identity()
        .ok_or_else(|| anyhow!("facet parent has no identity"))?;
    let (_, id) = identity
        .scope
        .split_once(':')
        .ok_or_else(|| anyhow!("facet parent scope has no class"))?;
    Ok(StructuredValue {
        root: Value::Reference(0),
        objects: vec![Object::DenseArray {
            elements: vec![
                Value::from(identity.class.as_str()),
                Value::from(id),
                identity
                    .name
                    .as_deref()
                    .map(Value::from)
                    .unwrap_or(Value::Undefined),
            ],
            properties: Vec::new(),
        }],
    })
}

impl Record {
    fn check(&self) -> Result<()> {
        if let Some(reason) = self.aborted.lock().unwrap().clone() {
            return Err(AbortError { reason }.into());
        }
        Ok(())
    }

    fn start(&self, context: &Context) -> Result<Completion<Arc<Started>>> {
        self.check()?;
        let mut starting = self.starting.lock().unwrap();
        if let Some(completion) = &starting.completion {
            return Ok(completion.clone());
        }
        let callback = starting
            .callback
            .take()
            .expect("facet callback is consumed once");
        let (sender, receiver) = watch::channel(None);
        let completion = Completion { receiver };
        starting.completion = Some(completion.clone());
        drop(starting);
        let barrier = self.barrier.clone();
        let callback_context = context.clone();
        context.wait_until(async move {
            let result = std::panic::AssertUnwindSafe(async move {
                if let Some(barrier) = barrier {
                    barrier.wait().await.map_err(completion_error)?;
                }
                callback_context.check_turn()?;
                let startup = callback(callback_context.clone()).await?;
                callback_context.check_turn()?;
                let id = startup.id.unwrap_or(inherited_id(&callback_context)?);
                Ok::<_, anyhow::Error>(Arc::new(Started {
                    class: startup.class,
                    id: encode(&id)?,
                }))
            })
            .catch_unwind()
            .await
            .unwrap_or_else(|panic| Err(crate::native_worker::panic_error(panic)))
            .map_err(Arc::new);
            sender.send_replace(Some(result));
            Ok(())
        })?;
        Ok(completion)
    }
}

impl Facets {
    pub fn get(
        &self,
        context: &Context,
        name: impl Into<String>,
        callback: impl FnOnce(Context) -> HandlerFuture<Startup> + Send + 'static,
    ) -> Result<Facet> {
        self.0.check(context)?;
        let name = name.into();
        valid_name(&name)?;
        anyhow::ensure!(context.facet().map_or(0, |facet| facet.depth) < 3, "Facet nesting depth limit exceeded. The maximum depth including the root Durable Object is 4.");
        let mut entries = self.0.entries.lock().unwrap();
        if let Some(record) = entries.running.get(&name).cloned() {
            drop(entries);
            return Ok(Facet {
                state: self.0.clone(),
                record,
            });
        }
        entries.next += 1;
        let record = Arc::new(Record {
            name: name.clone(),
            owner: format!("{}:{}", self.0.owner, entries.next),
            aborted: Mutex::new(None),
            starting: Mutex::new(Starting {
                callback: Some(Box::new(callback)),
                completion: None,
            }),
            barrier: entries
                .barriers
                .get(&name)
                .map(|(_, barrier)| barrier.clone()),
        });
        entries.running.insert(name, record.clone());
        Ok(Facet {
            state: self.0.clone(),
            record,
        })
    }

    /// Invalidates existing stubs immediately; storage closure continues on
    /// the host even when the returned wait future is dropped.
    pub fn abort(
        &self,
        context: &Context,
        name: &str,
        reason: StructuredValue,
    ) -> HandlerFuture<()> {
        self.lifecycle(context, name, reason, false)
    }

    /// Deletes a facet and its descendants after their abort barrier. Failed
    /// deletion remains the name's barrier until an explicit successful retry.
    pub fn delete(&self, context: &Context, name: &str) -> HandlerFuture<()> {
        self.lifecycle(
            context,
            name,
            StructuredValue {
                root: Value::from("Facet was deleted."),
                objects: Vec::new(),
            },
            true,
        )
    }

    fn lifecycle(
        &self,
        context: &Context,
        name: &str,
        reason: StructuredValue,
        delete: bool,
    ) -> HandlerFuture<()> {
        let prepared = (|| -> Result<_> {
            let authority = self.0.check(context)?;
            valid_name(name)?;
            let parent = crate::storage::storage_lifecycle_identity(&self.0.scope)
                .ok_or_else(|| anyhow!("facet parent storage has retired"))?;
            let owner = self
                .0
                .slot
                .upgrade()
                .ok_or_else(|| anyhow!("facet parent worker has retired"))?;
            // This event already holds the worker turn. Retain its executor
            // through host work without reserving another request admission.
            let pin = owner.retain_worker();
            let mut entries = self.0.entries.lock().unwrap();
            let record = entries.running.remove(name);
            if let Some(record) = &record {
                *record.aborted.lock().unwrap() = Some(Arc::new(reason));
            }
            let previous = entries
                .barriers
                .get(name)
                .map(|(_, barrier)| barrier.clone());
            let (sender, receiver) = watch::channel(None);
            let completion = Completion { receiver };
            entries.next += 1;
            let serial = entries.next;
            entries
                .barriers
                .insert(name.into(), (serial, completion.clone()));
            drop(entries);
            Ok((
                authority, parent, owner, pin, record, previous, sender, completion, serial,
            ))
        })();
        let (authority, parent, owner, pin, record, previous, sender, completion, serial) =
            match prepared {
                Ok(prepared) => prepared,
                Err(error) => return Box::pin(async move { Err(error) }),
            };
        let state = Arc::downgrade(&self.0);
        let slot = self.0.slot.clone();
        let scope = self.0.scope.clone();
        let name = name.to_string();
        crate::asyncrt::spawn(async move {
            let operation = async {
                if let Some(previous) = previous {
                    let _ = previous.wait().await;
                }
                if let Some(record) = record {
                    let starting = record.starting.lock().unwrap().completion.clone();
                    if let Some(starting) = starting {
                        if let Ok(started) = starting.wait().await {
                            let host = started.class.host(&slot)?;
                            let scope = host_facets::facet_scope(
                                &started.class.name,
                                &scope,
                                &record.owner,
                                &name,
                            );
                            let mut path = parent.facet_path.clone();
                            path.push(name.clone());
                            host_facets::abort_facet(
                                host,
                                scope,
                                Some((parent.root_scope.clone(), parent.epoch, path)),
                            )
                            .await
                            .map_err(anyhow::Error::msg)?;
                        }
                    }
                }
                if delete {
                    delete_native_facet(owner, authority, parent, name.clone()).await?;
                }
                Ok::<(), anyhow::Error>(())
            };
            let result = std::panic::AssertUnwindSafe(operation)
                .catch_unwind()
                .await
                .unwrap_or_else(|panic| Err(crate::native_worker::panic_error(panic)))
                .map_err(Arc::new);
            drop(pin);
            let clear = !delete || result.is_ok();
            sender.send_replace(Some(result));
            if clear {
                if let Some(state) = state.upgrade() {
                    let mut entries = state.entries.lock().unwrap();
                    if entries
                        .barriers
                        .get(&name)
                        .is_some_and(|(current, _)| *current == serial)
                    {
                        entries.barriers.remove(&name);
                    }
                }
            }
        })
        .detach();
        Box::pin(async move { completion.wait().await.map_err(completion_error) })
    }
}

pub(crate) async fn delete_native_facet(
    owner: Arc<crate::pool::Slot>,
    authority: Arc<super::Activation>,
    parent: crate::storage::StorageIdentity,
    name: String,
) -> Result<()> {
    let deletion = owner
        .turn(|_| {
            anyhow::ensure!(
                !authority.resources.retired.load(Ordering::Acquire)
                    && authority.resources.failure.lock().unwrap().is_none(),
                "facet parent activation has retired"
            );
            Ok::<_, anyhow::Error>(host_facets::delete_facet_files(parent, name))
        })
        .await?;
    deletion.await.map_err(anyhow::Error::msg)
}

impl Facet {
    fn check(&self, context: &Context) -> Result<Arc<Activation>> {
        let authority = self.state.check(context)?;
        self.record.check()?;
        Ok(authority)
    }

    pub fn fetch(&self, context: &Context, request: Request) -> HandlerFuture<HttpResponse> {
        let prepared = (|| -> Result<_> {
            self.check(context)?;
            anyhow::ensure!(
                request.entrypoint.is_none(),
                "facet fetch has no entrypoint selection"
            );
            context.io.charge_subrequest().map_err(anyhow::Error::msg)?;
            let body_guard = match request.body.stream_id() {
                Some(stream) => context
                    .io
                    .transfer_body_stream(stream)
                    .ok_or_else(|| anyhow!("facet request body is not owned by this event"))?,
                None => RequestBodyGuard::of(&request.body),
            };
            let order = context.io.call_chains.enter_with_owner(
                &format!("facet:{}:{}", self.state.owner, self.record.name),
                context.io.clone(),
            );
            Ok((self.record.start(context)?, body_guard, order))
        })();
        let facet = self.clone();
        let context = context.clone();
        let lifetime = crate::runtime::RequestCancellationLifetime::stateless();
        let mut cancellation = Cancellation {
            lifetime,
            armed: true,
        };
        Box::pin(async move {
            let (starting, body_guard, order) = prepared?;
            let started = starting.wait().await.map_err(completion_error)?;
            let authority = facet.check(&context)?;
            let parent = crate::storage::storage_identity(&facet.state.scope)?
                .ok_or_else(|| anyhow!("facet parent storage has retired"))?;
            let host = started.class.host(&facet.state.slot)?;
            let prepared = host_facets::prepare_native_facet(
                host,
                FacetStart {
                    class_name: started.class.name.clone(),
                    parent_scope: facet.state.scope.clone(),
                    owner: facet.record.owner.clone(),
                    name: facet.record.name.clone(),
                    id_sc: started.id.clone(),
                    props_sc: (*started.class.props).clone(),
                    parent,
                },
                authority,
            );
            let gate = egress_gate_request(&context.io, cellar_logic::Channel::CellRpc);
            let lifetime = cancellation.lifetime.clone();
            let trace = context.trace;
            let task = crate::asyncrt::spawn(async move {
                await_egress_gate(gate).await?;
                let facet = prepared.await?;
                facet
                    .fetch(request, body_guard, Some(lifetime), trace, Some(order))
                    .await
            });
            let task = CallTask {
                task,
                lifetime: cancellation.lifetime.clone(),
                finished: false,
            };
            let result = task
                .await
                .map_err(anyhow::Error::from)?
                .map_err(anyhow::Error::msg);
            cancellation.disarm();
            facet.record.check()?;
            result
        })
    }

    pub fn rpc(
        &self,
        context: &Context,
        method: impl Into<String>,
        args: RpcData,
    ) -> HandlerFuture<RpcData> {
        let prepared = self
            .check(context)
            .and_then(|_| context.io.charge_subrequest().map_err(anyhow::Error::msg))
            .and_then(|_| self.record.start(context));
        let facet = self.clone();
        let context = context.clone();
        let method = method.into();
        let lifetime = crate::runtime::RequestCancellationLifetime::stateless();
        let mut cancellation = Cancellation {
            lifetime,
            armed: true,
        };
        Box::pin(async move {
            let started = prepared?.wait().await.map_err(completion_error)?;
            let authority = facet.check(&context)?;
            let parent = crate::storage::storage_identity(&facet.state.scope)?
                .ok_or_else(|| anyhow!("facet parent storage has retired"))?;
            let host = started.class.host(&facet.state.slot)?;
            let prepared = host_facets::prepare_native_facet(
                host,
                FacetStart {
                    class_name: started.class.name.clone(),
                    parent_scope: facet.state.scope.clone(),
                    owner: facet.record.owner.clone(),
                    name: facet.record.name.clone(),
                    id_sc: started.id.clone(),
                    props_sc: (*started.class.props).clone(),
                    parent,
                },
                authority,
            );
            let gate = egress_gate_request(&context.io, cellar_logic::Channel::CellRpc);
            let lifetime = cancellation.lifetime.clone();
            let trace = context.trace;
            let task = crate::asyncrt::spawn(async move {
                await_egress_gate(gate).await?;
                let facet = prepared.await?;
                facet.rpc(method, args, Some(lifetime), trace).await
            });
            let task = CallTask {
                task,
                lifetime: cancellation.lifetime.clone(),
                finished: false,
            };
            let result = task
                .await
                .map_err(anyhow::Error::from)?
                .map_err(anyhow::Error::msg);
            cancellation.disarm();
            facet.record.check()?;
            result
        })
    }
}
