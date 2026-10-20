//! Immutable application declarations shared by native and Axum hosts.

use crate::{AxumApplication, CellContext};
use anyhow::{anyhow, ensure, Result};
use axum::{
    extract::Path,
    response::IntoResponse,
    routing::{any, Route},
    Router,
};
use cellar::sdk::{
    workflow, Application, Cell, CellIdentity, Context, DeploymentGraph, HandlerFuture, Invocation,
    Output, RaftClass, Registry,
};
use std::{
    collections::{HashMap, HashSet},
    convert::Infallible,
    sync::Arc,
};
use tower::{Layer, Service};

type Handler = Arc<dyn Fn(Context, Invocation) -> HandlerFuture<Output> + Send + Sync>;
type Factory = Arc<dyn Fn(Context, &CellIdentity) -> Result<Arc<dyn Cell>> + Send + Sync>;
type Workflow = Arc<
    dyn Fn(
            Context,
            workflow::Event,
            workflow::Steps,
        ) -> HandlerFuture<cellar_native::clone::StructuredValue>
        + Send
        + Sync,
>;

struct CellRoute {
    path: String,
    namespace: String,
    class: String,
    parameter: String,
}

/// Compose public HTTP, named entrypoints, SDK events, and cell factories.
/// Authentication layers apply to the completed definition, including gateways.
pub struct ApplicationBuilder {
    router: Router,
    entrypoints: HashMap<String, Router>,
    cells: HashMap<String, Factory>,
    routes: Vec<CellRoute>,
    workflows: HashMap<String, Workflow>,
    raft_classes: HashMap<String, RaftClass>,
    events: Handler,
    #[cfg(feature = "openapi")]
    api: Option<utoipa::openapi::OpenApi>,
    #[cfg(feature = "openapi")]
    server: Option<String>,
    #[cfg(feature = "openapi")]
    info: Option<utoipa::openapi::Info>,
}

impl Default for ApplicationBuilder {
    fn default() -> Self {
        Self::new(Router::new())
    }
}

impl ApplicationBuilder {
    pub fn new(router: Router) -> Self {
        Self {
            router,
            entrypoints: HashMap::new(),
            cells: HashMap::new(),
            routes: Vec::new(),
            workflows: HashMap::new(),
            raft_classes: HashMap::new(),
            events: Arc::new(|_, _| {
                Box::pin(async { Err(anyhow!("no non-HTTP application handler registered")) })
            }),
            #[cfg(feature = "openapi")]
            api: None,
            #[cfg(feature = "openapi")]
            server: None,
            #[cfg(feature = "openapi")]
            info: None,
        }
    }

    pub fn events(
        mut self,
        handler: impl Fn(Context, Invocation) -> HandlerFuture<Output> + Send + Sync + 'static,
    ) -> Self {
        self.events = Arc::new(handler);
        self
    }

    pub fn cell(
        mut self,
        class: impl Into<String>,
        factory: impl Fn(Context, &CellIdentity) -> Result<Arc<dyn Cell>> + Send + Sync + 'static,
    ) -> Result<Self> {
        let class = class.into();
        ensure!(
            !class.is_empty()
                && !self.cells.contains_key(&class)
                && !self.raft_classes.contains_key(&class),
            "empty or duplicate cell class {class}"
        );
        self.cells.insert(class, Arc::new(factory));
        Ok(self)
    }

    /// Register synchronous command/query definitions. HTTP routers call the
    /// node's Raft handles; Fleet cell routes cannot instantiate these classes.
    pub fn raft_class(mut self, class: RaftClass) -> Result<Self> {
        ensure!(
            !self.cells.contains_key(class.name())
                && !self.workflows.contains_key(class.name())
                && !self.raft_classes.contains_key(class.name()),
            "duplicate or conflicting Raft class {}",
            class.name()
        );
        self.raft_classes.insert(class.name().into(), class);
        Ok(self)
    }

    pub fn workflow(
        mut self,
        class: impl Into<String>,
        handler: impl Fn(
                Context,
                workflow::Event,
                workflow::Steps,
            ) -> HandlerFuture<cellar_native::clone::StructuredValue>
            + Send
            + Sync
            + 'static,
    ) -> Result<Self> {
        let class = class.into();
        ensure!(
            !class.is_empty()
                && !self.workflows.contains_key(&class)
                && !self.raft_classes.contains_key(&class),
            "empty or duplicate workflow {class}"
        );
        self.workflows.insert(class, Arc::new(handler));
        Ok(self)
    }

    pub fn entrypoint(mut self, name: impl Into<String>, router: Router) -> Result<Self> {
        let name = name.into();
        ensure!(
            !name.is_empty() && name != "default" && !self.entrypoints.contains_key(&name),
            "invalid or duplicate entrypoint {name}"
        );
        self.entrypoints.insert(name, router);
        Ok(self)
    }

    /// Dispatch a canonical path via the namespace stub, preserving owner routing.
    /// The class's router must register this same path and name capture.
    pub fn cell_route(
        mut self,
        path: impl Into<String>,
        namespace: impl Into<String>,
        class: impl Into<String>,
        parameter: impl Into<String>,
    ) -> Result<Self> {
        let route = CellRoute {
            path: path.into(),
            namespace: namespace.into(),
            class: class.into(),
            parameter: parameter.into(),
        };
        validate_path(&route.path, &route.parameter)?;
        ensure!(
            !route.namespace.is_empty() && !route.class.is_empty(),
            "cell route requires namespace and class"
        );
        let normalized = normalized_path(&route.path);
        ensure!(
            !self
                .routes
                .iter()
                .any(|existing| normalized_path(&existing.path) == normalized),
            "duplicate or ambiguous cell route {}",
            route.path
        );
        self.routes.push(route);
        Ok(self)
    }

    #[cfg(feature = "openapi")]
    pub fn api(router: utoipa_axum::router::OpenApiRouter) -> Result<Self> {
        let (router, api) = router.split_for_parts();
        crate::openapi::validate(&api)?;
        let mut builder = Self::new(router);
        builder.api = Some(api);
        Ok(builder)
    }

    /// Use one registration for class handlers, public gateway paths and schemas.
    /// For mutable activation State use a custom `cell` factory and `cell_route`.
    #[cfg(feature = "openapi")]
    pub fn cell_api(
        mut self,
        class: impl Into<String>,
        namespace: impl Into<String>,
        parameter: impl Into<String>,
        router: utoipa_axum::router::OpenApiRouter,
    ) -> Result<Self> {
        let class = class.into();
        let namespace = namespace.into();
        let parameter = parameter.into();
        let (router, mut api) = router.split_for_parts();
        for path in api.paths.paths.keys() {
            self = self.cell_route(
                path.clone(),
                namespace.clone(),
                class.clone(),
                parameter.clone(),
            )?;
        }
        crate::openapi::cell_parameters(&mut api, &parameter, &format!("{class}.{namespace}"))?;
        match &mut self.api {
            Some(existing) => crate::openapi::merge(existing, api)?,
            None => {
                crate::openapi::validate(&api)?;
                self.api = Some(api);
            }
        }
        self.cell(class, move |_, _| {
            Ok(Arc::new(crate::AxumCell::new(router.clone())))
        })
    }

    #[cfg(feature = "openapi")]
    pub fn server(mut self, url: impl Into<String>) -> Self {
        self.server = Some(url.into());
        self
    }

    #[cfg(feature = "openapi")]
    pub fn info(mut self, info: utoipa::openapi::Info) -> Self {
        self.info = Some(info);
        self
    }

    pub fn build(mut self) -> Result<ApplicationDefinition> {
        #[cfg(feature = "openapi")]
        if let (Some(api), Some(info)) = (&mut self.api, self.info.take()) {
            api.info = info;
        }
        #[cfg(feature = "openapi")]
        if let (Some(api), Some(server)) = (&mut self.api, self.server.take()) {
            api.servers = Some(vec![utoipa::openapi::Server::new(server)]);
        }
        for route in &self.routes {
            ensure!(
                self.cells.contains_key(&route.class),
                "no factory for class {}",
                route.class
            );
        }
        let events = self.events;
        let mut registry = Registry::new(move |context, invocation| events(context, invocation));
        for (class, factory) in self.cells {
            registry = registry.cell(class, move |context, identity| factory(context, identity))?;
        }
        for (class, handler) in self.workflows {
            registry = registry.workflow(class, move |context, event, steps| {
                handler(context, event, steps)
            })?;
        }
        for (_, class) in self.raft_classes {
            registry = registry.raft_class(class)?;
        }
        let bindings = self
            .routes
            .iter()
            .map(|route| (route.namespace.clone(), route.class.clone()))
            .collect();
        let mut gateway = Router::new();
        for route in std::mem::take(&mut self.routes) {
            let namespace = route.namespace;
            let parameter = route.parameter;
            gateway = gateway.route(
                &route.path,
                any(
                    move |CellContext(context): CellContext,
                          Path(parameters): Path<HashMap<String, String>>,
                          request: axum::extract::Request| {
                        let namespace = namespace.clone();
                        let parameter = parameter.clone();
                        async move {
                            if !["GET", "HEAD", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"]
                                .contains(&request.method().as_str())
                            {
                                return (
                                    axum::http::StatusCode::NOT_IMPLEMENTED,
                                    "HTTP method is unsupported by Cellar cell fetch",
                                )
                                    .into_response();
                            }
                            let result = async {
                                let name = parameters
                                    .get(&parameter)
                                    .ok_or_else(|| anyhow!("missing cell name"))?;
                                ensure!(!name.is_empty(), "empty cell name");
                                let request = crate::http::forwarded_request(&context, request)?;
                                let response = context
                                    .namespace(&namespace)?
                                    .get_by_name(name)
                                    .fetch(&context, request)
                                    .await?;
                                crate::http::response(response)
                            }
                            .await;
                            match result {
                                Ok(response) => response,
                                Err(error) => crate::http::failure(error),
                            }
                        }
                    },
                ),
            );
        }
        #[cfg(feature = "openapi")]
        if let Some(api) = &self.api {
            crate::openapi::validate(api)?;
            ensure!(
                !api.paths.paths.contains_key("/openapi.json"),
                "reserved OpenAPI document path"
            );
            let json = api.to_json()?;
            gateway = gateway.route(
                "/openapi.json",
                axum::routing::get(move || {
                    let json = json.clone();
                    async move { ([("content-type", "application/json")], json) }
                }),
            );
        }
        Ok(ApplicationDefinition {
            router: gateway.fallback_service(self.router),
            entrypoints: self.entrypoints,
            registry: Arc::new(registry),
            bindings,
            #[cfg(feature = "openapi")]
            api: self.api,
        })
    }
}

/// Starts no node and performs no I/O. Construct this inside the compiled
/// factory when mutable Router State must be initialized afresh for each node.
#[derive(Clone)]
pub struct ApplicationDefinition {
    router: Router,
    entrypoints: HashMap<String, Router>,
    registry: Arc<Registry>,
    bindings: HashSet<(String, String)>,
    #[cfg(feature = "openapi")]
    api: Option<utoipa::openapi::OpenApi>,
}

impl ApplicationDefinition {
    pub fn instantiate(&self) -> Result<Arc<dyn Application>> {
        let mut application =
            AxumApplication::with_application(self.router.clone(), self.registry.clone())
                .require_bindings(self.bindings.clone());
        for (name, router) in &self.entrypoints {
            application = application.entrypoint(name, router.clone())?;
        }
        Ok(Arc::new(application))
    }

    pub fn validate_deployment(&self, graph: &DeploymentGraph) -> Result<()> {
        validate_bindings(&self.bindings, &graph.primary.options)
    }

    /// Apply middleware to all public routes, including gateways and OpenAPI.
    /// Layers that spawn handlers on ambient executors are unsupported.
    pub fn layer<L>(mut self, layer: L) -> Self
    where
        L: Layer<Route> + Clone + Send + Sync + 'static,
        L::Service: Service<axum::extract::Request> + Clone + Send + Sync + 'static,
        <L::Service as Service<axum::extract::Request>>::Response: IntoResponse + 'static,
        <L::Service as Service<axum::extract::Request>>::Error: Into<Infallible> + 'static,
        <L::Service as Service<axum::extract::Request>>::Future: Send + 'static,
    {
        for router in self.entrypoints.values_mut() {
            *router = std::mem::take(router).layer(layer.clone());
        }
        self.router = self.router.layer(layer);
        self
    }

    #[cfg(feature = "openapi")]
    pub fn openapi(&self) -> Option<&utoipa::openapi::OpenApi> {
        self.api.as_ref()
    }

    #[cfg(feature = "openapi")]
    pub fn export_openapi(&self) -> Result<String> {
        self.api
            .as_ref()
            .ok_or_else(|| anyhow!("application has no OpenAPI document"))?
            .to_pretty_json()
            .map_err(Into::into)
    }
}

pub(crate) fn validate_bindings(
    bindings: &HashSet<(String, String)>,
    options: &cellar::engine_api::WorkerConfigOptions,
) -> Result<()> {
    for (namespace, class) in bindings {
        ensure!(
            options
                .bindings
                .iter()
                .any(|binding| binding == &(namespace.clone(), class.clone())),
            "missing or mismatched namespace {namespace} for class {class}"
        );
        ensure!(
            options.do_classes.contains(class),
            "deployment does not declare class {class}"
        );
    }
    Ok(())
}

pub(crate) fn normalized_path(path: &str) -> String {
    path.split('/')
        .map(|segment| {
            if segment.starts_with('{') {
                "{}"
            } else {
                segment
            }
        })
        .collect::<Vec<_>>()
        .join("/")
}

fn validate_path(path: &str, parameter: &str) -> Result<()> {
    ensure!(
        path.starts_with('/') && !path.contains(['?', '#']) && !parameter.is_empty(),
        "invalid cell path or name parameter"
    );
    let mut captures = HashSet::new();
    for segment in path.split('/') {
        if segment.contains(['{', '}']) {
            let name = segment
                .strip_prefix('{')
                .and_then(|segment| segment.strip_suffix('}'))
                .ok_or_else(|| anyhow!("captures must occupy a complete path segment"))?;
            ensure!(
                !name.is_empty()
                    && name
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
                    && captures.insert(name),
                "invalid or repeated capture {name}"
            );
        } else {
            ensure!(!segment.starts_with([':', '*']), "use Axum brace captures");
        }
    }
    ensure!(
        captures.contains(parameter),
        "cell path must capture {{{parameter}}}"
    );
    Ok(())
}
