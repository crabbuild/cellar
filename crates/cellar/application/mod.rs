// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#![doc = include_str!("README.md")]

use crate::engine_api::{HttpResponse, QueueBatch, QueueDispatchResult, RequestBody, RpcData};
use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};

mod admission;
pub use admission::ApplicationOverloaded;
pub mod assets;
pub mod blob;
pub mod cache;
pub mod cells;
pub(crate) mod concurrency;
pub mod consensus;
pub mod containers;
pub mod cron;
pub mod event_stream;
pub mod events;
pub mod facets;
pub mod html_rewriter;
pub mod http;
pub mod kv;
pub mod limits;
pub mod loader;
pub(crate) mod outbox;
pub mod queue;
pub mod service;
pub mod sockets;
pub mod sql;
pub mod storage;
pub mod streams;
pub mod websocket;
pub mod workflow;
pub use storage::{
    ListOptions, OutboxBlocked, OutboxLane, OutboxObservation, OutboxOptions, OutboxPage,
    OutboxRevision, Storage,
};

/// An owned application callback; the host supplies its execution and lifetime.
pub type HandlerFuture<T> =
    std::pin::Pin<Box<dyn Future<Output = anyhow::Result<T>> + Send + 'static>>;

pub type SqlResult = (Vec<String>, Vec<Vec<serde_json::Value>>, u64);

/// A persisted structured value. A capability tree retains its original
/// marker; callers must materialize its stubs through the host protocol.
#[derive(Clone, Debug)]
pub struct StoredClone {
    pub value: cellar_native::clone::StructuredValue,
    pub stored_stubs: bool,
}

pub fn decode_rpc(data: &RpcData) -> Result<cellar_native::clone::StructuredValue> {
    match data {
        RpcData::Json(json) => {
            cellar_native::clone::StructuredValue::from_json(serde_json::from_str(json)?)
        }
        RpcData::Structured(bytes) => cellar_native::clone::decode(bytes),
    }
}

pub fn encode_rpc(value: &cellar_native::clone::StructuredValue) -> Result<RpcData> {
    Ok(RpcData::Structured(
        cellar_native::clone::encode(value)?.into(),
    ))
}

/// Read a response, retaining the original remote error envelope on failure.
pub fn decode_rpc_result(data: &RpcData) -> Result<cellar_native::clone::StructuredValue> {
    match data {
        RpcData::Json(_) => decode_rpc(data),
        RpcData::Structured(bytes) => cellar_native::rpc::decode_response(bytes),
    }
}

pub struct Request {
    pub url: String,
    pub method: String,
    pub headers: Vec<(String, String)>,
    pub body: RequestBody,
    pub entrypoint: Option<crate::WorkerFetchEntrypoint>,
}

/// All events entering an application use the existing host job protocol.
pub enum Invocation {
    Fetch(Request),
    Rpc {
        entrypoint: String,
        operation: crate::WorkerRpcOperation,
        props: Vec<u8>,
    },
    CellRpc {
        method: String,
        args: RpcData,
    },
    Queue(QueueBatch),
    Scheduled(cron::Scheduled),
    Alarm {
        scheduled_ms: i64,
    },
    WebSocketOpen {
        id: u64,
        protocol: String,
    },
    WebSocketMessage {
        id: u64,
        data: crate::ws_registry::WsIn,
    },
    WebSocketClose {
        id: u64,
        code: u16,
        reason: String,
        was_clean: bool,
    },
}

pub enum Output {
    Http(HttpResponse),
    Bytes(Vec<u8>),
    Rpc(RpcData),
    Queue(QueueDispatchResult),
    WebSocket(crate::ws_registry::WsBatch),
    Done,
}

pub trait Cell: Send + Sync + 'static {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output>;
}

pub trait Application: Send + Sync + 'static {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output>;

    /// Validate compiled application requirements against this deployment before
    /// it becomes active. Implementations must not perform I/O or create cells.
    fn validate_configuration(
        &self,
        _options: &crate::engine_api::WorkerConfigOptions,
    ) -> Result<()> {
        Ok(())
    }

    /// Validate deployment-level event declarations before activation. This
    /// hook must perform no I/O or cell construction. The default retains the
    /// existing configuration validator for ordinary native applications.
    fn validate_deployment(&self, deployment: &crate::fleet::LoadedDeployment) -> Result<()> {
        self.validate_configuration(&deployment.options)
    }

    fn supports_cell(&self, class: &str) -> bool;

    fn create_cell(&self, context: Context, identity: &CellIdentity) -> Result<Arc<dyn Cell>>;

    /// Resolve a restricted Raft definition without invoking a mutable cell
    /// factory. Custom applications opt in explicitly; existing applications
    /// keep their Fleet dispatch behavior.
    fn resolve_raft_class(
        &self,
        class: &str,
    ) -> std::result::Result<Arc<consensus::RaftClass>, consensus::RegistrationError> {
        Err(
            if self.supports_cell(class) || self.supports_workflow(class) {
                consensus::RegistrationError::UnsupportedFactory(class.into())
            } else {
                consensus::RegistrationError::UnknownClass(class.into())
            },
        )
    }

    fn supports_workflow(&self, _class: &str) -> bool {
        false
    }

    fn run_workflow(
        self: Arc<Self>,
        class: String,
        _context: Context,
        _event: workflow::Event,
        _steps: workflow::Step,
    ) -> HandlerFuture<cellar_native::clone::StructuredValue> {
        Box::pin(async move { Err(anyhow!("no native workflow handler for {class}")) })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CellIdentity {
    pub scope: String,
    pub class: String,
    pub name: Option<String>,
    pub epoch: u64,
}

type Factory = Arc<dyn Fn(Context, &CellIdentity) -> Result<Arc<dyn Cell>> + Send + Sync>;
type Handler = Arc<dyn Fn(Context, Invocation) -> HandlerFuture<Output> + Send + Sync>;
type WorkflowHandler = Arc<
    dyn Fn(
            Context,
            workflow::Event,
            workflow::Step,
        ) -> HandlerFuture<cellar_native::clone::StructuredValue>
        + Send
        + Sync,
>;

/// A compiled Rust application's handlers, Fleet factories and Raft definitions.
pub struct Registry {
    handler: Handler,
    entrypoints: HashMap<String, Handler>,
    cells: HashMap<String, Factory>,
    raft_classes: HashMap<String, Arc<consensus::RaftClass>>,
    workflows: HashMap<String, WorkflowHandler>,
}

impl Registry {
    pub fn new(
        handler: impl Fn(Context, Invocation) -> HandlerFuture<Output> + Send + Sync + 'static,
    ) -> Self {
        Self {
            handler: Arc::new(handler),
            entrypoints: HashMap::new(),
            cells: HashMap::new(),
            raft_classes: HashMap::new(),
            workflows: HashMap::new(),
        }
    }

    /// Register a named stateless service. Once names are registered, unknown
    /// names fail explicitly; the constructor's handler remains `default`.
    /// A registry without named registrations retains its custom dispatcher.
    ///
    /// ```
    /// use cellar::application::{Invocation, Output, Registry};
    /// let app = Registry::new(|_, _| Box::pin(async { Ok(Output::Done) }))
    ///     .entrypoint("Ledger", |context, invocation| Box::pin(async move {
    ///         let Invocation::Fetch(request) = invocation else {
    ///             anyhow::bail!("Ledger expects an HTTP request");
    ///         };
    ///         let target = context.service("ACCOUNTS")?;
    ///         Ok(Output::Http(target.fetch(&context, request).await?))
    ///     }))?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn entrypoint(
        mut self,
        name: impl Into<String>,
        handler: impl Fn(Context, Invocation) -> HandlerFuture<Output> + Send + Sync + 'static,
    ) -> Result<Self> {
        let name = name.into();
        anyhow::ensure!(!name.is_empty(), "entrypoint name must not be empty");
        anyhow::ensure!(
            name != "default",
            "default entrypoint is supplied by Registry::new"
        );
        anyhow::ensure!(
            !self.entrypoints.contains_key(&name),
            "duplicate native entrypoint {name}"
        );
        self.entrypoints.insert(name, Arc::new(handler));
        Ok(self)
    }

    pub fn cell(
        mut self,
        class: impl Into<String>,
        factory: impl Fn(Context, &CellIdentity) -> Result<Arc<dyn Cell>> + Send + Sync + 'static,
    ) -> Result<Self> {
        let class = class.into();
        anyhow::ensure!(!class.is_empty(), "cell class must not be empty");
        anyhow::ensure!(
            !self.cells.contains_key(&class) && !self.raft_classes.contains_key(&class),
            "duplicate native cell class {class}"
        );
        self.cells.insert(class, Arc::new(factory));
        Ok(self)
    }

    /// Register synchronous SQL commands and read-only queries. Raft classes
    /// cannot also have an ordinary `Cell` factory under the same class name.
    pub fn raft_class(mut self, class: consensus::RaftClass) -> Result<Self> {
        anyhow::ensure!(
            !self.cells.contains_key(class.name())
                && !self.raft_classes.contains_key(class.name())
                && !self.workflows.contains_key(class.name()),
            consensus::RegistrationError::Duplicate(class.name().into())
        );
        self.raft_classes
            .insert(class.name().into(), Arc::new(class));
        Ok(self)
    }

    pub fn workflow(
        mut self,
        class: impl Into<String>,
        handler: impl Fn(
                Context,
                workflow::Event,
                workflow::Step,
            ) -> HandlerFuture<cellar_native::clone::StructuredValue>
            + Send
            + Sync
            + 'static,
    ) -> Result<Self> {
        let class = class.into();
        anyhow::ensure!(!class.is_empty(), "workflow class must not be empty");
        anyhow::ensure!(
            !self.workflows.contains_key(&class) && !self.raft_classes.contains_key(&class),
            "duplicate native workflow class {class}"
        );
        self.workflows.insert(class, Arc::new(handler));
        Ok(self)
    }
}

impl Application for Registry {
    fn validate_configuration(
        &self,
        options: &crate::engine_api::WorkerConfigOptions,
    ) -> Result<()> {
        for class in options
            .do_classes
            .iter()
            .chain(options.bindings.iter().map(|(_, class)| class))
            .chain(
                options
                    .workflow_bindings
                    .iter()
                    .map(|binding| &binding.class),
            )
        {
            anyhow::ensure!(
                !self.raft_classes.contains_key(class),
                consensus::RegistrationError::WrongMode {
                    class: class.clone(),
                    mode: consensus::NamespaceMode::Fleet,
                }
            );
        }
        Ok(())
    }

    fn resolve_raft_class(
        &self,
        class: &str,
    ) -> std::result::Result<Arc<consensus::RaftClass>, consensus::RegistrationError> {
        self.raft_classes.get(class).cloned().ok_or_else(|| {
            if self.cells.contains_key(class) || self.workflows.contains_key(class) {
                consensus::RegistrationError::UnsupportedFactory(class.into())
            } else {
                consensus::RegistrationError::UnknownClass(class.into())
            }
        })
    }

    fn supports_workflow(&self, class: &str) -> bool {
        self.workflows.contains_key(class)
    }

    fn run_workflow(
        self: Arc<Self>,
        class: String,
        context: Context,
        event: workflow::Event,
        steps: workflow::Step,
    ) -> HandlerFuture<cellar_native::clone::StructuredValue> {
        match self.workflows.get(&class) {
            Some(handler) => handler(context, event, steps),
            None => Box::pin(async move { Err(anyhow!("no native workflow handler for {class}")) }),
        }
    }

    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        let name = match &invocation {
            Invocation::Fetch(request) => {
                request.entrypoint.as_ref().map(|entry| entry.name.as_str())
            }
            Invocation::Rpc { entrypoint, .. } => Some(entrypoint.as_str()),
            _ => None,
        };
        let handler = match name {
            None | Some("default") => &self.handler,
            Some(_) if self.entrypoints.is_empty() => &self.handler,
            Some(name) => match self.entrypoints.get(name) {
                Some(handler) => handler,
                None => {
                    let name = name.to_string();
                    return Box::pin(
                        async move { Err(anyhow!("no native entrypoint named {name}")) },
                    );
                }
            },
        };
        handler(context, invocation)
    }

    fn supports_cell(&self, class: &str) -> bool {
        self.cells.contains_key(class)
    }

    fn create_cell(&self, context: Context, identity: &CellIdentity) -> Result<Arc<dyn Cell>> {
        anyhow::ensure!(
            !self.raft_classes.contains_key(&identity.class),
            consensus::RegistrationError::WrongMode {
                class: identity.class.clone(),
                mode: consensus::NamespaceMode::Fleet,
            }
        );
        self.cells
            .get(&identity.class)
            .ok_or_else(|| anyhow!("no native factory for {}", identity.class))?(
            context, identity
        )
    }
}

#[derive(Default)]
pub(crate) struct EventState {
    pub background: Mutex<Vec<HandlerFuture<()>>>,
    pub(crate) blocks: concurrency::Blocks,
    pub(crate) retired: std::sync::atomic::AtomicBool,
    pub(crate) incoming_abort: Mutex<Option<cellar_native::web::WeakAbortSignal>>,
    pub(crate) unrefed: Mutex<Vec<HandlerFuture<()>>>,
    pub(crate) websocket_capture: Mutex<Option<crate::ws_registry::WsBatch>>,
}

impl EventState {
    pub(crate) fn discard_websocket_capture(&self) {
        let capture = self.websocket_capture.lock().unwrap().take();
        if let Some(capture) = capture {
            capture.discard();
        }
    }
}

pub(crate) struct Activation {
    pub(crate) worker_config: Arc<crate::worker_config::WorkerConfig>,
    pub scope: String,
    pub epoch: u64,
    pub delete_all_deletes_alarm: bool,
    pub(crate) resources: storage::Resources,
    pub(crate) websocket_states: Mutex<HashMap<u64, Arc<Mutex<websocket::SocketState>>>>,
}

#[derive(Clone)]
pub struct Context {
    pub(crate) event_authority: Option<event_stream::Authority>,
    pub(crate) name_authority: Option<event_stream::name_resolution::NameAuthority>,
    pub(crate) catalog_authority: Option<crate::runtime::rpc_authority::CatalogAuthority>,
    pub(crate) io: Arc<crate::host_io::IoContext>,
    pub(crate) state: Arc<EventState>,
    pub(crate) identity: Option<CellIdentity>,
    pub(crate) facet: Option<Arc<FacetContext>>,
    pub(crate) trace: Option<crate::telemetry::TraceContext>,
    pub(crate) vars: Arc<HashMap<String, String>>,
    pub(crate) sql_bindings: Arc<HashMap<String, String>>,
    pub(crate) kv_bindings: Arc<HashMap<String, String>>,
    pub(crate) queue_bindings: Arc<HashMap<String, queue::Producer>>,
    pub(crate) workflow_bindings: Arc<HashMap<String, workflow::Workflow>>,
    pub(crate) blob_bindings: Arc<HashMap<String, blob::Bucket>>,
    pub(crate) asset_binding: Option<(Arc<str>, assets::Fetcher)>,
    pub(crate) queue_config: Arc<queue::Config>,
    pub(crate) generation: crate::generation::GenerationId,
    pub(crate) worker_config: Arc<crate::worker_config::WorkerConfig>,
    pub(crate) loader_owner: Arc<loader::Owner>,
    pub(crate) loaded_environment: Option<Arc<cellar_native::clone::StructuredValue>>,
}

/// Startup values of a facet, retained for the lifetime of its activation.
/// Identity is a literal string or the cloned `[class, id, name]` tuple used
/// by named Durable Object IDs. Properties retain their structured graph.
#[derive(Clone, Debug)]
pub struct FacetContext {
    pub id: cellar_native::clone::StructuredValue,
    pub props: cellar_native::clone::StructuredValue,
    /// Number of facet ancestors, excluding the root cell.
    pub depth: usize,
}

impl Context {
    pub fn assets(&self, name: &str) -> Result<assets::Fetcher> {
        self.asset_binding
            .as_ref()
            .filter(|(binding, _)| binding.as_ref() == name)
            .map(|(_, fetcher)| fetcher.clone())
            .ok_or_else(|| anyhow!("no assets binding named {name}"))
    }

    pub fn caches(&self) -> cache::CacheStorage {
        cache::CacheStorage::new()
    }

    pub fn blob(&self, name: &str) -> Result<blob::Bucket> {
        self.blob_bindings
            .get(name)
            .cloned()
            .ok_or_else(|| anyhow!("no Blob binding named {name}"))
    }

    pub fn workflow(&self, name: &str) -> Result<workflow::Workflow> {
        self.workflow_bindings
            .get(name)
            .cloned()
            .ok_or_else(|| anyhow!("no Workflow binding named {name}"))
    }

    pub fn identity(&self) -> Option<&CellIdentity> {
        self.identity.as_ref()
    }

    pub fn facet(&self) -> Option<&FacetContext> {
        self.facet.as_deref()
    }

    pub fn var(&self, name: &str) -> Option<&str> {
        self.vars.get(name).map(String::as_str)
    }

    pub fn sql(&self, name: &str) -> Result<sql::Database> {
        self.sql_bindings
            .get(name)
            .cloned()
            .map(sql::Database::new)
            .ok_or_else(|| anyhow!("no SQL binding named {name}"))
    }

    pub fn kv(&self, name: &str) -> Result<kv::Namespace> {
        self.kv_bindings
            .get(name)
            .cloned()
            .map(kv::Namespace::new)
            .ok_or_else(|| anyhow!("no KV binding named {name}"))
    }

    pub fn queue(&self, name: &str) -> Result<queue::Producer> {
        self.queue_bindings
            .get(name)
            .cloned()
            .ok_or_else(|| anyhow!("no Queue binding named {name}"))
    }

    pub(crate) fn check_turn(&self) -> Result<()> {
        anyhow::ensure!(
            crate::host_io::installed_context()
                .as_ref()
                .is_some_and(|io| Arc::ptr_eq(io, &self.io)),
            "native context is outside its executing turn"
        );
        anyhow::ensure!(
            Arc::ptr_eq(self.io.host_services(), &crate::asyncrt::services()),
            "native context belongs to another execution domain"
        );
        anyhow::ensure!(
            self.io.host_services().accepts_work(),
            "native context execution domain is closed"
        );
        anyhow::ensure!(
            !self
                .state
                .retired
                .load(std::sync::atomic::Ordering::Acquire),
            "native event context has retired"
        );
        if let Some(identity) = &self.identity {
            anyhow::ensure!(
                crate::storage::activation_epoch(&identity.scope) == Some(identity.epoch),
                "native cell context belongs to a retired activation"
            );
        }
        if let Some(activation) = &self.io.native_activation {
            anyhow::ensure!(
                cellar_logic::event_stream::source_admitted(
                    self.io.source_generation,
                    activation
                        .resources
                        .source_generation
                        .load(std::sync::atomic::Ordering::Acquire),
                    activation
                        .resources
                        .source_closed
                        .load(std::sync::atomic::Ordering::Acquire),
                    self.io
                        .framework_source
                        .load(std::sync::atomic::Ordering::Acquire)
                ),
                "source business admission is closed or its incarnation has changed"
            );
            if let Some(failure) = activation.resources.failure.lock().unwrap().as_ref() {
                anyhow::bail!("native cell was reset: {failure}");
            }
            anyhow::ensure!(
                !activation
                    .resources
                    .retired
                    .load(std::sync::atomic::Ordering::Acquire),
                "native cell activation has retired"
            );
        }
        Ok(())
    }

    /// Background futures are polled by the same turn driver and retain the
    /// event's residency after the response has been sent.
    pub fn wait_until(
        &self,
        future: impl Future<Output = Result<()>> + Send + 'static,
    ) -> Result<()> {
        self.check_turn()?;
        self.state.background.lock().unwrap().push(Box::pin(future));
        Ok(())
    }

    pub fn storage(&self) -> Result<Storage> {
        self.check_turn()?;
        anyhow::ensure!(
            self.identity.is_some(),
            "stateless handlers have no cell storage"
        );
        Ok(Storage(
            self.io
                .native_activation
                .clone()
                .expect("native cell has an activation"),
            None,
            self.io.source_generation,
        ))
    }

    /// Dispatch through cellar's ordinary routing and output-gate channels.
    pub fn fetch_cell(
        &self,
        scope: String,
        name: Option<String>,
        request: Request,
    ) -> HandlerFuture<HttpResponse> {
        crate::native_worker::fetch_cell(self, scope, name, request, false)
    }

    /// Call a cell through the same host RPC and durability channel used by
    /// system bindings. The host resolves its owner for each invocation.
    pub fn rpc_cell(
        &self,
        scope: String,
        name: Option<String>,
        method: String,
        args: RpcData,
    ) -> HandlerFuture<RpcData> {
        crate::native_worker::rpc_cell(self, scope, name, method, args)
    }

    /// Move an incoming body into an owned, backpressured stream. Dropping the
    /// stream cancels the source through the ordinary host ownership claim.
    pub fn take_body(&self, body: RequestBody) -> Result<crate::engine_api::HttpChunkStream> {
        self.check_turn()?;
        match body {
            RequestBody::Bytes(bytes) => Ok(Box::pin(futures_util::stream::once(async move {
                Ok(bytes.to_vec())
            }))),
            RequestBody::Stream(id) => crate::native_worker::take_body(self, id),
        }
    }
}
