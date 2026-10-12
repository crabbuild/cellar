//! Dynamically loaded compiled applications using host admission and turns.
//! A named load shares its startup and lifetime with all derived handles.

use super::concurrency::{CallTask, Cancellation};
use super::*;
use crate::engine_api::{Compat, WorkerConfigOptions};
use crate::host_gates::EgressGate;
use crate::host_gates::{await_egress_gate, egress_gate_request};
use crate::host_loader::{self, LoaderOwner, LoaderState};
use crate::http_streams::RequestBodyGuard;
use crate::worker_config::{LoaderEnv, WorkerConfig};
use cellar_native::clone::{encode, Object, StructuredValue, Value};
use futures_util::FutureExt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Weak;
use tokio::sync::{oneshot, watch};

static NEXT_LOAD: AtomicU64 = AtomicU64::new(1);

/// Native execution description with copied values and host-minted services.
/// Complete module formats and other capability kinds still require adapters.
pub struct WorkerCode {
    pub application: Arc<dyn Application>,
    pub environment: Option<StructuredValue>,
    pub limits: Option<crate::WorkerInvocationLimits>,
    pub compat: Compat,
    pub outbound: Outbound,
    environment_fetchers: Vec<super::service::Fetcher>,
}
#[derive(Clone, Default)]
pub enum Outbound {
    #[default]
    Inherit,
    Deny,
    Broker(super::service::Fetcher),
}
impl WorkerCode {
    pub fn new(application: Arc<dyn Application>) -> Self {
        Self {
            application,
            environment: None,
            limits: None,
            compat: Compat::default(),
            outbound: Outbound::Inherit,
            environment_fetchers: Vec::new(),
        }
    }
    /// Insert a capability placeholder into this environment's graph. Attach
    /// the returned reference wherever the loaded code should receive it.
    pub fn environment_fetcher(&mut self, fetcher: super::service::Fetcher) -> Result<Value> {
        if self
            .environment
            .as_ref()
            .is_none_or(|graph| matches!(graph.root, Value::Null | Value::Undefined))
        {
            self.environment = Some(StructuredValue::from_json(serde_json::json!({}))?);
        }
        let graph = self.environment.as_mut().unwrap();
        let index = u32::try_from(graph.objects.len())
            .map_err(|_| anyhow!("environment graph has too many objects"))?;
        let route = self.environment_fetchers.len();
        graph.objects.push(Object::Object(vec![(
            Value::from(super::service::ENV_SERVICE_MARKER),
            Value::Number(route as f64),
        )]));
        self.environment_fetchers.push(fetcher);
        Ok(Value::Reference(index))
    }
}

pub(crate) struct Owner {
    host: LoaderOwner,
    active: AtomicBool,
    managers: Mutex<HashMap<String, Arc<Manager>>>,
}
impl Owner {
    pub(crate) fn new(config: &WorkerConfig) -> Arc<Self> {
        Arc::new(Self {
            host: LoaderOwner::fresh(&config.script_name, config.generation),
            active: AtomicBool::new(true),
            managers: Mutex::new(HashMap::new()),
        })
    }
    pub(crate) fn retire(&self) {
        self.active.store(false, Ordering::Release);
        let states = host_loader::take_loader_owner(&self.host);
        // Child destruction runs after the parent has left its turn, and
        // in-flight calls retain their already captured loading receivers.
        if !states.is_empty() {
            if let Some(service) = self.host.service.upgrade() {
                service.defer_drop(Box::new(states));
            }
        }
    }
}
struct Manager {
    owner: Weak<Owner>,
    names: Mutex<HashMap<String, Weak<Load>>>,
}
#[derive(Clone)]
pub struct Loader(Arc<Manager>);

impl Context {
    pub fn loader(&self, binding: &str) -> Result<Loader> {
        self.check_turn()?;
        anyhow::ensure!(
            self.worker_config
                .loader_bindings
                .iter()
                .any(|name| name == binding),
            "no Worker Loader binding named {binding}"
        );
        let mut managers = self.loader_owner.managers.lock().unwrap();
        let manager = managers.get(binding).cloned().unwrap_or_else(|| {
            let manager = Arc::new(Manager {
                owner: Arc::downgrade(&self.loader_owner),
                names: Mutex::new(HashMap::new()),
            });
            managers.insert(binding.into(), manager.clone());
            manager
        });
        Ok(Loader(manager))
    }
    /// A copied graph, available to a loaded worker and its facet constructors.
    pub fn loaded_environment(&self) -> Result<Option<&StructuredValue>> {
        self.check_turn()?;
        Ok(self.loaded_environment.as_deref())
    }
}

#[derive(Clone)]
struct Ready {
    id: u64,
}
type LoadResult = std::result::Result<Ready, Arc<anyhow::Error>>;
pub(crate) struct Load {
    service: Arc<dyn host_loader::LoaderService>,
    serial: u64,
    manager: Weak<Manager>,
    name: Option<String>,
    disposed: AtomicBool,
    result: watch::Receiver<Option<LoadResult>>,
}
impl Load {
    fn remove_name(&self) {
        if let (Some(manager), Some(name)) = (self.manager.upgrade(), &self.name) {
            let mut names = manager.names.lock().unwrap();
            if names
                .get(name)
                .and_then(Weak::upgrade)
                .is_none_or(|load| load.serial == self.serial)
            {
                names.remove(name);
            }
        }
    }
    fn release(&self) {
        self.disposed.store(true, Ordering::Release);
        self.remove_name();
        if let Some(Ok(ready)) = self.result.borrow().clone() {
            if let Some(state) = self.service.take(ready.id) {
                self.service.defer_drop(Box::new(state));
            }
        }
    }
}
impl Drop for Load {
    fn drop(&mut self) {
        self.release();
    }
}

/// Shared with entrypoints and classes. Disposing any clone releases registry
/// ownership and invalidates new calls; already captured calls may complete.
#[derive(Clone)]
pub struct Worker(Arc<Load>);

impl Loader {
    fn check(&self, context: &Context) -> Result<Arc<Owner>> {
        context.check_turn()?;
        let owner = self
            .0
            .owner
            .upgrade()
            .ok_or_else(|| anyhow!("worker loader owner has retired"))?;
        anyhow::ensure!(
            owner
                .host
                .service
                .upgrade()
                .is_some_and(|service| Arc::ptr_eq(&service, &context.io.host_services().loader())),
            "worker loader belongs to another execution domain"
        );
        anyhow::ensure!(
            Arc::ptr_eq(&owner, &context.loader_owner),
            "worker loader belongs to another worker"
        );
        anyhow::ensure!(
            owner.active.load(Ordering::Acquire),
            "worker loader owner has retired"
        );
        Ok(owner)
    }
    pub fn load(&self, context: &Context, code: WorkerCode) -> Result<Worker> {
        self.start(context, None, move |_| Box::pin(async move { Ok(code) }))
    }
    pub fn get(
        &self,
        context: &Context,
        name: &str,
        callback: impl FnOnce(Context) -> HandlerFuture<WorkerCode> + Send + 'static,
    ) -> Result<Worker> {
        self.check(context)?;
        let existing = {
            self.0
                .names
                .lock()
                .unwrap()
                .get(name)
                .and_then(Weak::upgrade)
        };
        if let Some(load) = existing {
            if !load.disposed.load(Ordering::Acquire) {
                return Ok(Worker(load));
            }
        }
        self.start(context, Some(name.into()), callback)
    }
    fn start(
        &self,
        context: &Context,
        name: Option<String>,
        callback: impl FnOnce(Context) -> HandlerFuture<WorkerCode> + Send + 'static,
    ) -> Result<Worker> {
        let owner = self.check(context)?;
        let (sender, result) = watch::channel(None);
        let load = Arc::new(Load {
            service: context.io.host_services().loader(),
            serial: NEXT_LOAD.fetch_add(1, Ordering::Relaxed),
            manager: Arc::downgrade(&self.0),
            name: name.clone(),
            disposed: AtomicBool::new(false),
            result,
        });
        if let Some(name) = name {
            self.0
                .names
                .lock()
                .unwrap()
                .insert(name, Arc::downgrade(&load));
        }
        let producer = load.clone();
        let context_copy = context.clone();
        context.wait_until(async move {
            let loading = async {
                context_copy.check_turn()?;
                let code = callback(context_copy.clone()).await?;
                context_copy.check_turn()?;
                anyhow::ensure!(
                    owner.active.load(Ordering::Acquire),
                    "worker loader owner has retired"
                );
                anyhow::ensure!(
                    !producer.disposed.load(Ordering::Acquire),
                    "worker loader: worker is disposed"
                );
                let mut environment_fetchers = code.environment_fetchers;
                let environment = code
                    .environment
                    .filter(|value| !matches!(value.root, Value::Undefined | Value::Null))
                    .map(|value| -> Result<_> {
                        let bytes = encode(&value)?;
                        let mut routes = Vec::new();
                        let mut total = bytes.len();
                        for fetcher in &environment_fetchers {
                            anyhow::ensure!(
                                fetcher.generation == context_copy.generation,
                                "worker loader: environment fetcher belongs to another generation"
                            );
                            let route = fetcher.route();
                            total = total
                                .checked_add(route.script.len())
                                .and_then(|n| {
                                    n.checked_add(route.entrypoint.as_ref().map_or(0, String::len))
                                })
                                .and_then(|n| n.checked_add(route.props.len()))
                                .ok_or_else(|| {
                                    anyhow!("worker loader: environment exceeds size limit")
                                })?;
                            routes.push(route);
                        }
                        anyhow::ensure!(
                            total <= host_loader::MAX_DYNAMIC_WORKER_ENV_SIZE,
                            "worker loader: environment exceeds size limit"
                        );
                        Ok(LoaderEnv { bytes, routes })
                    })
                    .transpose()?;
                if environment.is_none() {
                    environment_fetchers.clear();
                }
                let (egress, native_outbound) = match code.outbound {
                    Outbound::Inherit => (
                        context_copy.worker_config.egress.clone(),
                        context_copy.worker_config.native_outbound_broker.clone(),
                    ),
                    Outbound::Deny => (crate::worker_config::EgressPolicy::Deny, None),
                    Outbound::Broker(fetcher) => {
                        anyhow::ensure!(
                            fetcher.generation == context_copy.generation,
                            "worker loader: outbound fetcher belongs to another generation"
                        );
                        (
                            crate::worker_config::EgressPolicy::Broker(fetcher.broker()),
                            Some(fetcher),
                        )
                    }
                };
                let (loaded, state) = watch::channel(LoaderState::Loading);
                let id = host_loader::admit_loaded_worker(owner.host.clone(), state)
                    .map_err(anyhow::Error::msg)?;
                let mut compat = code.compat;
                compat.js_rpc = true;
                let config = Arc::new(
                    WorkerConfig::new(WorkerConfigOptions {
                        src: String::new(),
                        script_name: format!("__loader:{id}"),
                        do_classes: Vec::new(),
                        bindings: Vec::new(),
                        blob_bindings: Vec::new(),
                        sql_bindings: Vec::new(),
                        kv_bindings: Vec::new(),
                        queue_bindings: Vec::new(),
                        queue_consumers: Vec::new(),
                        workflow_bindings: Vec::new(),
                        vars: Vec::new(),
                        node: String::new(),
                        modules: Vec::new(),
                        compat,
                    })
                    .into_dynamic_worker()
                    .with_generation(context_copy.generation)
                    .with_egress(egress)
                    .with_resource_limits(code.limits)
                    .with_loader_env(environment)
                    .with_native_load(
                        Arc::downgrade(&producer),
                        environment_fetchers,
                        native_outbound,
                    )
                    .with_native_application(code.application),
                );
                host_loader::spawn_loaded_worker(
                    crate::asyncrt::op_handle(),
                    loaded,
                    config.clone(),
                );
                Ok::<_, anyhow::Error>(Ready { id })
            };
            let result = std::panic::AssertUnwindSafe(loading)
                .catch_unwind()
                .await
                .unwrap_or_else(|panic| Err(crate::native_worker::panic_error(panic)))
                .map_err(Arc::new);
            sender.send_replace(Some(result));
            // Disposal can precede code completion; do not leave an admitted
            // child behind if the producer was the final lifetime reference.
            if producer.disposed.load(Ordering::Acquire) {
                producer.release();
            }
            Ok(())
        })?;
        Ok(Worker(load))
    }
}

impl Worker {
    fn check_host(&self) -> Result<()> {
        anyhow::ensure!(
            Arc::ptr_eq(&self.0.service, &crate::asyncrt::services().loader()),
            "loaded worker belongs to another execution domain"
        );
        anyhow::ensure!(
            self.0.service.accepts_work(),
            "worker loader: execution domain is closed"
        );
        Ok(())
    }
    pub(crate) fn from_origin(load: &Weak<Load>) -> Result<Self> {
        load.upgrade()
            .map(Self)
            .ok_or_else(|| anyhow!("worker loader: origin lifetime has retired"))
    }
    async fn ready(&self) -> Result<Ready> {
        let mut result = self.0.result.clone();
        loop {
            anyhow::ensure!(
                !self.0.disposed.load(Ordering::Acquire),
                "worker loader: worker is disposed"
            );
            if let Some(value) = result.borrow().clone() {
                return value.map_err(|cause| super::facets::CompletionError { cause }.into());
            }
            result
                .changed()
                .await
                .map_err(|_| anyhow!("worker loader: startup event retired before completion"))?;
        }
    }
    pub(crate) async fn slot(&self) -> Result<Arc<crate::pool::Slot>> {
        self.check_host()?;
        let ready = self.ready().await?;
        self.check_host()?;
        let state = self
            .0
            .service
            .state(ready.id)
            .ok_or_else(|| anyhow!("worker loader: unknown worker"))?;
        host_loader::loaded_worker_slot(state)
            .await
            .map_err(anyhow::Error::msg)
    }
    pub(crate) fn capture(&self) -> Result<Option<watch::Receiver<LoaderState>>> {
        self.check_host()?;
        anyhow::ensure!(
            !self.0.disposed.load(Ordering::Acquire),
            "worker loader: worker is disposed"
        );
        let Some(ready) = self.0.result.borrow().clone() else {
            return Ok(None);
        };
        let ready = ready.map_err(|cause| super::facets::CompletionError { cause })?;
        self.0
            .service
            .state(ready.id)
            .map(Some)
            .ok_or_else(|| anyhow!("worker loader: unknown worker"))
    }
    pub fn dispose(&self, context: &Context) -> Result<()> {
        context.check_turn()?;
        self.check_host()?;
        self.0.release();
        Ok(())
    }
    pub fn entrypoint(&self, name: impl Into<String>) -> Entrypoint {
        Entrypoint {
            origin: CallOrigin::Loader(self.clone()),
            name: name.into(),
            props: Vec::new(),
            limits: None,
        }
    }
    pub fn cell_class(&self, name: impl Into<String>) -> super::facets::Class {
        super::facets::Class::loaded(self.clone(), name.into())
    }
    pub async fn wait_ready(&self) -> Result<()> {
        self.slot().await.map(|_| ())
    }
}

/// A self capability follows its loaded facet's original opening, not the
/// disposed loader registry. Weak links prevent a facet retaining its own Slot.
#[derive(Clone)]
pub(crate) enum CallOrigin {
    Loader(Worker),
    Activation {
        service: Weak<dyn host_loader::LoaderService>,
        slot: Weak<crate::pool::Slot>,
        activation: Weak<Activation>,
    },
    Event {
        service: Weak<dyn host_loader::LoaderService>,
        slot: Weak<crate::pool::Slot>,
        event: Weak<EventState>,
    },
}

enum CallCapture {
    Loader(watch::Receiver<LoaderState>),
    Execution(Arc<crate::pool::Slot>),
}

impl CallOrigin {
    pub(crate) fn for_context(context: &Context) -> Result<Self> {
        if let Some(activation) = &context.io.native_activation {
            let slot = crate::pool::current_slot()
                .ok_or_else(|| anyhow!("loaded self capability has no executing worker"))?;
            return Ok(Self::Activation {
                service: Arc::downgrade(&context.io.host_services().loader()),
                slot: Arc::downgrade(&slot),
                activation: Arc::downgrade(activation),
            });
        }
        if let Some(load) = &context.worker_config.native_load {
            if let Ok(worker) = Worker::from_origin(load) {
                if !worker.0.disposed.load(Ordering::Acquire) {
                    return Ok(Self::Loader(worker));
                }
            }
        }
        // A captured stateless call can still execute after disposal. Its
        // own event authorizes nested self calls until that event retires.
        let slot = crate::pool::current_slot()
            .ok_or_else(|| anyhow!("loaded self capability has no executing worker"))?;
        Ok(Self::Event {
            service: Arc::downgrade(&context.io.host_services().loader()),
            slot: Arc::downgrade(&slot),
            event: Arc::downgrade(&context.state),
        })
    }

    pub(crate) fn routed_entrypoint(
        &self,
        entrypoint: Option<&crate::WorkerFetchEntrypoint>,
    ) -> Entrypoint {
        Entrypoint {
            origin: self.clone(),
            name: entrypoint
                .map_or("default", |entry| entry.name.as_str())
                .into(),
            props: entrypoint.map_or_else(Vec::new, |entry| entry.props.clone()),
            limits: None,
        }
    }

    fn check(&self) -> Result<()> {
        match self {
            Self::Loader(worker) => worker.check_host()?,
            Self::Activation { service, .. } | Self::Event { service, .. } => {
                anyhow::ensure!(
                    service.upgrade().is_some_and(|service| Arc::ptr_eq(
                        &service,
                        &crate::asyncrt::services().loader()
                    ) && service.accepts_work()),
                    "loaded capability belongs to another or retired execution domain"
                );
            }
        }
        if let Self::Activation { activation, .. } = self {
            let activation = activation
                .upgrade()
                .ok_or_else(|| anyhow!("loaded self capability activation has retired"))?;
            anyhow::ensure!(
                !activation.resources.retired.load(Ordering::Acquire),
                "loaded self capability activation has retired"
            );
            if let Some(failure) = activation.resources.failure.lock().unwrap().as_ref() {
                anyhow::bail!("loaded self capability activation was reset: {failure}");
            };
        }
        if let Self::Event { event, .. } = self {
            let event = event
                .upgrade()
                .ok_or_else(|| anyhow!("loaded self capability event has retired"))?;
            anyhow::ensure!(
                !event.retired.load(Ordering::Acquire),
                "loaded self capability event has retired"
            );
        }
        Ok(())
    }

    fn capture(&self) -> Result<Option<CallCapture>> {
        self.check()?;
        match self {
            Self::Loader(worker) => worker.capture().map(|state| state.map(CallCapture::Loader)),
            Self::Activation { slot, .. } | Self::Event { slot, .. } => {
                Ok(Some(CallCapture::Execution(slot.upgrade().ok_or_else(
                    || anyhow!("loaded self capability worker has retired"),
                )?)))
            }
        }
    }

    async fn slot(&self) -> Result<Arc<crate::pool::Slot>> {
        match self {
            Self::Loader(worker) => worker.slot().await,
            Self::Activation { .. } | Self::Event { .. } => match self.capture()? {
                Some(CallCapture::Execution(slot)) => Ok(slot),
                _ => unreachable!("an activation origin captures its executing slot"),
            },
        }
    }
}

impl CallCapture {
    async fn slot(self) -> Result<Arc<crate::pool::Slot>> {
        match self {
            Self::Loader(state) => host_loader::loaded_worker_slot(state)
                .await
                .map_err(anyhow::Error::msg),
            Self::Execution(slot) => Ok(slot),
        }
    }
}

#[derive(Clone)]
pub struct Entrypoint {
    origin: CallOrigin,
    name: String,
    props: Vec<u8>,
    limits: Option<crate::WorkerInvocationLimits>,
}
pub struct FetchWithTail {
    pub response: HandlerFuture<HttpResponse>,
    pub report: HandlerFuture<String>,
}
type PreparedFetch = Result<(RequestBodyGuard, EgressGate, Option<CallCapture>)>;
pub(crate) fn props_bytes(props: &StructuredValue) -> Result<Vec<u8>> {
    let bytes = if props.root == Value::Undefined {
        Vec::new()
    } else {
        encode(props)?
    };
    anyhow::ensure!(
        bytes.len() <= host_loader::MAX_DYNAMIC_WORKER_PROPS_SIZE,
        "worker loader: props exceed size limit"
    );
    Ok(bytes)
}
impl Entrypoint {
    pub fn with_props(&self, props: &StructuredValue) -> Result<Self> {
        Ok(Self {
            props: props_bytes(props)?,
            ..self.clone()
        })
    }
    pub fn with_limits(&self, limits: crate::WorkerInvocationLimits) -> Self {
        Self {
            limits: Some(limits),
            ..self.clone()
        }
    }
    pub fn fetch(&self, context: &Context, request: Request) -> HandlerFuture<HttpResponse> {
        self.fetch_in(context, request, None)
    }
    pub fn fetch_with_tail(&self, context: &Context, request: Request) -> FetchWithTail {
        let (send, receive) = oneshot::channel();
        FetchWithTail {
            response: self.fetch_in(context, request, Some(send)),
            report: Box::pin(async move { receive.await.map_err(anyhow::Error::from) }),
        }
    }
    fn fetch_in(
        &self,
        context: &Context,
        mut request: Request,
        tail_report: Option<oneshot::Sender<String>>,
    ) -> HandlerFuture<HttpResponse> {
        let prepared = (|| -> Result<_> {
            context.check_turn()?;
            request.method = super::http::normalize_method(&request.method)?;
            context.io.charge_subrequest().map_err(anyhow::Error::msg)?;
            request.entrypoint = None;
            let body_guard = match request.body.stream_id() {
                Some(id) => context
                    .io
                    .transfer_body_stream(id)
                    .ok_or_else(|| anyhow!("worker loader: body stream is not owned"))?,
                None => RequestBodyGuard::of(&request.body),
            };
            if self.name != "default" || !self.props.is_empty() {
                request.entrypoint = Some(crate::WorkerFetchEntrypoint {
                    name: self.name.clone(),
                    props: self.props.clone(),
                });
            }
            Ok((
                body_guard,
                egress_gate_request(&context.io, cellar_logic::Channel::Service),
                self.origin.capture()?,
            ))
        })();
        self.fetch_job(context, request, tail_report, prepared)
    }
    /// Invoke a supplied broker using its caller's already charged budget,
    /// captured gate and transferred body. Sampling/claiming again would lose
    /// that original write ticket and request-body authority.
    pub(crate) fn fetch_prepared(
        &self,
        context: &Context,
        mut request: Request,
        guard: RequestBodyGuard,
        gate: EgressGate,
    ) -> HandlerFuture<HttpResponse> {
        let prepared = (|| -> Result<_> {
            context.check_turn()?;
            request.entrypoint = None;
            request.method = super::http::normalize_method(&request.method)?;
            if self.name != "default" || !self.props.is_empty() {
                request.entrypoint = Some(crate::WorkerFetchEntrypoint {
                    name: self.name.clone(),
                    props: self.props.clone(),
                });
            }
            Ok((guard, gate, self.origin.capture()?))
        })();
        self.fetch_job(context, request, None, prepared)
    }
    fn fetch_job(
        &self,
        context: &Context,
        request: Request,
        tail_report: Option<oneshot::Sender<String>>,
        prepared: PreparedFetch,
    ) -> HandlerFuture<HttpResponse> {
        let entrypoint = self.clone();
        let context = context.clone();
        let lifetime = crate::runtime::RequestCancellationLifetime::stateless();
        let mut cancellation = Cancellation {
            lifetime,
            armed: true,
        };
        Box::pin(async move {
            let (guard, gate, captured) = prepared?;
            let lifetime = cancellation.lifetime.clone();
            let task = crate::asyncrt::spawn(async move {
                let slot = match captured {
                    Some(captured) => captured.slot().await?,
                    None => entrypoint.origin.slot().await?,
                };
                await_egress_gate(gate).await.map_err(anyhow::Error::msg)?;
                entrypoint.origin.check()?;
                host_loader::calls::fetch(
                    slot,
                    |reply| crate::WorkerJob::Fetch {
                        queued_at: crate::asyncrt::instant(),
                        entrypoint: request.entrypoint,
                        invocation_limits: entrypoint.limits,
                        url: request.url,
                        method: request.method,
                        body: request.body,
                        headers: request.headers,
                        request_id: None,
                        tail_report,
                        reply,
                    },
                    guard,
                    lifetime,
                    None,
                )
                .await
                .map_err(anyhow::Error::msg)
            });
            let result = CallTask {
                task,
                lifetime: cancellation.lifetime.clone(),
                finished: false,
            }
            .await?;
            cancellation.disarm();
            context.check_turn()?;
            result
        })
    }
    pub fn rpc(
        &self,
        context: &Context,
        operation: crate::WorkerRpcOperation,
    ) -> HandlerFuture<Vec<u8>> {
        let prepared = context
            .check_turn()
            .and_then(|_| context.io.charge_subrequest().map_err(anyhow::Error::msg))
            .and_then(|_| {
                Ok((
                    egress_gate_request(&context.io, cellar_logic::Channel::Service),
                    self.origin.capture()?,
                ))
            });
        let entrypoint = self.clone();
        let context = context.clone();
        Box::pin(async move {
            let (gate, captured) = prepared?;
            let slot = match captured {
                Some(captured) => captured.slot().await?,
                None => entrypoint.origin.slot().await?,
            };
            await_egress_gate(gate).await.map_err(anyhow::Error::msg)?;
            entrypoint.origin.check()?;
            let result = host_loader::calls::rpc(slot, |reply| crate::WorkerJob::Rpc {
                entrypoint: entrypoint.name,
                operation,
                props: entrypoint.props,
                request_id: None,
                invocation_limits: entrypoint.limits,
                reply,
            })
            .await
            .map_err(anyhow::Error::msg)?;
            context.check_turn()?;
            Ok(result)
        })
    }
    /// Cancel one loaded RPC while retaining the entrypoint's loader lifetime.
    pub fn rpc_with_signal(
        &self,
        context: &Context,
        operation: crate::WorkerRpcOperation,
        signal: cellar_native::web::AbortSignal,
    ) -> HandlerFuture<Vec<u8>> {
        if let Err(reason) = signal.throw_if_aborted() {
            return Box::pin(async move { Err(reason.into()) });
        }
        let call = self.rpc(context, operation);
        Box::pin(async move {
            crate::asyncrt::select_biased! { "a loaded RPC caller abort wins an undelivered reply";
                reason=signal.cancelled()=>Err(reason.into()),result=call=>result,
            }
        })
    }
}
