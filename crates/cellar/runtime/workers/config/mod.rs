// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Adapted for Cellar from Celld: crates/celld/js.rs.

//! Deployment configuration shared by compiled Rust and transitional engines.

use crate::engine_api::{Compat, ModuleSource, QueueBinding, WorkerConfigOptions, WorkflowBinding};
use std::sync::Arc;

pub(crate) mod imports;

const ENTRY_MODULE_NAME: &str = "worker.js";

/// One queue's push consumer after the owning script has been resolved.
#[derive(Clone)]
pub struct QueueConsumerRegistration {
    pub script: String,
    pub config: crate::protocol::QueueConsumerConfig,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum WorkerOrigin {
    Deployment,
    Dynamic,
}

pub(crate) type ResourceLimits = crate::WorkerInvocationLimits;

pub(crate) fn effective_resource_limits(
    configured: Option<ResourceLimits>,
    invocation: Option<ResourceLimits>,
) -> Option<ResourceLimits> {
    let lower = |configured: Option<u32>, invocation: Option<u32>| match (configured, invocation) {
        (Some(configured), Some(invocation)) => Some(configured.min(invocation)),
        (configured, invocation) => configured.or(invocation),
    };
    let limits = ResourceLimits {
        cpu_ms: lower(
            configured.and_then(|limits| limits.cpu_ms),
            invocation.and_then(|limits| limits.cpu_ms),
        ),
        sub_requests: lower(
            configured.and_then(|limits| limits.sub_requests),
            invocation.and_then(|limits| limits.sub_requests),
        ),
    };
    (limits.cpu_ms.is_some() || limits.sub_requests.is_some()).then_some(limits)
}

#[derive(Clone)]
pub(crate) struct LoaderEnvRoute {
    pub(crate) script: String,
    pub(crate) entrypoint: Option<String>,
    pub(crate) props: Vec<u8>,
}

#[derive(Clone)]
pub(crate) struct LoaderEnv {
    pub(crate) bytes: Vec<u8>,
    pub(crate) routes: Vec<LoaderEnvRoute>,
}

pub struct WorkerConfig {
    pub(crate) native_application: Option<Arc<dyn crate::application::Application>>,
    // The origin link is weak: a loaded worker cannot keep its own loader
    // registry entry alive merely by retaining its configuration.
    pub(crate) native_load: Option<std::sync::Weak<crate::application::loader::Load>>,
    pub(crate) native_environment_fetchers: Vec<crate::application::service::Fetcher>,
    pub(crate) native_outbound_broker: Option<crate::application::service::Fetcher>,
    pub(crate) src: String,
    /// The canonical module name for `src`. Dynamic Workers preserve the
    /// caller's `mainModule` path so relative imports use its directory.
    pub(crate) main_module_name: String,
    pub script_name: String,
    /// The construction path that grants Dynamic Worker load semantics.
    /// A script name is caller-controlled, so it cannot prove this origin.
    pub(crate) origin: WorkerOrigin,
    pub(crate) do_classes: Vec<String>,
    pub(crate) bindings: Vec<(String, String)>,
    /// `blob_buckets`: (environment name, bucket name). The bucket name is
    /// the key space the binding owns inside the fleet bucket; see [[blob]].
    pub(crate) blob_bindings: Vec<(String, String)>,
    /// `sql_databases`: (environment name, stable database identity). The
    /// identity addresses the cell that holds the database; see [[sql]].
    pub(crate) sql_bindings: Vec<(String, String)>,
    /// `kv_namespaces`: (environment name, namespace identity). The identity is
    /// the config's `id` verbatim, and it addresses the namespace's cells; see
    /// [[kv]].
    pub(crate) kv_bindings: Vec<(String, String)>,
    pub(crate) queue_bindings: Vec<QueueBinding>,
    pub(crate) stream_bindings: Vec<crate::application::event_stream::Binding>,
    pub(crate) event_readiness: Arc<crate::application::event_stream::readiness::Readiness>,
    pub(crate) queue_consumers: Vec<QueueConsumerRegistration>,
    /// This script declared a consumer in its own manifest. The deployment-
    /// wide catalog later replaces `queue_consumers` in every isolate, so the
    /// catalog cannot answer whether this particular default export can omit
    /// `fetch` and provide only `queue`.
    pub(crate) declares_queue_consumer: bool,
    pub(crate) workflow_bindings: Vec<WorkflowBinding>,
    pub(crate) vars: Vec<(String, String)>,
    pub(crate) node: String,
    /// The worker's non-main modules, so the main module can import siblings.
    pub(crate) modules: Vec<(String, ModuleSource)>,
    pub(crate) compat: Compat,
    /// `[[services]]`: (binding name, target script, optional entrypoint).
    /// The target runs in this process; see [[service-bindings]].
    pub(crate) services: Vec<(String, String, Option<String>)>,
    pub(crate) asset_binding: Option<String>,
    /// `env` names of the Worker Loader bindings, if this Worker can spawn
    /// dynamic isolates. A config can declare more than one loader, and each
    /// binding gets a cache of its own, so a name loaded through one loader
    /// is a different Worker from the same name loaded through another.
    pub(crate) loader_bindings: Vec<String>,
    /// Ambient outbound authority. Loaded workers may be denied.
    pub(crate) egress: EgressPolicy,
    /// Structured-clone values and host-minted service routes that a loaded
    /// worker receives. Loader-only; empty for a normal worker.
    pub(crate) loader_env: Option<LoaderEnv>,
    /// Per-invocation limits supplied with a dynamically loaded Worker.
    /// Deployment Workers leave this empty.
    pub(crate) resource_limits: Option<ResourceLimits>,
    /// Whether a Dynamic Worker can receive a tail report sender on a fetch.
    pub(crate) tail_reporting: bool,
    /// `triggers.crons` from the deployment. Empty for a loaded worker and for
    /// any script without cron triggers.
    pub crons: Vec<String>,
    /// `containers` from the deployment: the classes whose `ctx.container`
    /// exists. Empty for a loaded worker.
    pub containers: Vec<crate::container::ContainerSpec>,
    /// The application generation this configuration belongs to. Every
    /// isolate built from it carries the value as a slot, so its host calls
    /// resolve against the deployment graph it was built with.
    pub generation: crate::generation::GenerationId,
    /// The external (`node:*`/`cloudflare:*`) imports of `src`.
    ///
    /// The scan walks the whole bundle, which an esbuild artifact makes
    /// megabytes, and its answer depends only on `src` — a field nothing
    /// mutates after construction. It used to run inside `load_config`,
    /// so a deployment paid it again on every cell wake and on every
    /// stateless pool thread. One `Arc<WorkerConfig>` backs all of those
    /// isolates, so scanning here pays it once instead.
    pub(crate) main_imports: imports::ExternalImports,
    /// The same scan for each `ModuleSource::EsModule` sibling, in the order
    /// `es_module_sources` yields them. Read through
    /// [`WorkerConfig::es_modules`], which hands a module and its scan out
    /// together so no caller can pair a module with another module's scan.
    pub(crate) module_imports: Vec<imports::ExternalImports>,
}

/// The `ModuleSource::EsModule` siblings of a worker, as (name, source).
///
/// The single definition of that order: `WorkerConfig::new` scans in it and
/// `WorkerConfig::es_modules` zips against it, so the two cannot disagree.
fn es_module_sources(modules: &[(String, ModuleSource)]) -> impl Iterator<Item = (&str, &str)> {
    modules.iter().filter_map(|(name, source)| match source {
        ModuleSource::EsModule(source) => Some((name.as_str(), source.as_str())),
        _ => None,
    })
}

impl WorkerConfig {
    pub fn new(options: WorkerConfigOptions) -> Self {
        let WorkerConfigOptions {
            src,
            script_name,
            do_classes,
            bindings,
            blob_bindings,
            sql_bindings,
            kv_bindings,
            queue_bindings,
            stream_bindings,
            queue_consumers,
            workflow_bindings,
            vars,
            node,
            modules,
            compat,
        } = options;
        let main_imports = imports::scan_external_imports(&src);
        let module_imports = es_module_sources(&modules)
            .map(|(_name, source)| imports::scan_external_imports(source))
            .collect();
        let declares_queue_consumer = !queue_consumers.is_empty();
        let queue_consumers = queue_consumers
            .into_iter()
            .map(|config| QueueConsumerRegistration {
                script: script_name.clone(),
                config,
            })
            .collect();
        Self {
            event_readiness: Arc::new(crate::application::event_stream::readiness::Readiness::new(
                true,
            )),
            native_application: None,
            src,
            main_module_name: ENTRY_MODULE_NAME.to_string(),
            script_name,
            origin: WorkerOrigin::Deployment,
            do_classes,
            bindings,
            blob_bindings,
            sql_bindings,
            kv_bindings,
            queue_bindings,
            stream_bindings,
            queue_consumers,
            declares_queue_consumer,
            workflow_bindings,
            vars,
            node,
            modules,
            compat,
            services: Vec::new(),
            asset_binding: None,
            loader_bindings: Vec::new(),
            egress: EgressPolicy::Allow,
            loader_env: None,
            native_load: None,
            native_environment_fetchers: Vec::new(),
            native_outbound_broker: None,
            resource_limits: None,
            tail_reporting: false,
            crons: Vec::new(),
            containers: Vec::new(),
            generation: 0,
            main_imports,
            module_imports,
        }
    }

    /// Execute compiled Rust handlers through the ordinary runtime pool.
    pub fn with_native_application(
        mut self,
        application: Arc<dyn crate::application::Application>,
    ) -> Self {
        self.native_application = Some(application);
        self
    }

    pub(crate) fn with_native_load(
        mut self,
        load: std::sync::Weak<crate::application::loader::Load>,
        environment: Vec<crate::application::service::Fetcher>,
        outbound: Option<crate::application::service::Fetcher>,
    ) -> Self {
        self.native_load = Some(load);
        self.native_environment_fetchers = environment;
        self.native_outbound_broker = outbound;
        self
    }

    pub fn with_containers(mut self, containers: Vec<crate::container::ContainerSpec>) -> Self {
        self.containers = containers;
        self
    }

    /// Whether cells of `class` supervise a container.
    pub fn container_class(&self, class: &str) -> bool {
        self.containers.iter().any(|spec| spec.class_name == class)
    }

    /// Stamp this Worker with the application generation it serves.
    pub fn with_generation(mut self, generation: crate::generation::GenerationId) -> Self {
        self.generation = generation;
        self
    }

    pub fn with_queue_consumers(mut self, consumers: Vec<QueueConsumerRegistration>) -> Self {
        self.queue_consumers = consumers;
        self
    }

    /// Grant the relaxed load contract used only by a Worker Loader isolate.
    pub(crate) fn into_dynamic_worker(mut self) -> Self {
        self.origin = WorkerOrigin::Dynamic;
        self
    }

    pub(crate) fn with_main_module_name(mut self, name: String) -> Self {
        self.main_module_name = name;
        self
    }

    pub(crate) fn with_tail_reporting(mut self, enabled: bool) -> Self {
        self.tail_reporting = enabled;
        self
    }

    /// Every `ModuleSource::EsModule` sibling with its name, its source and
    /// its scanned external imports.
    pub(crate) fn es_modules(
        &self,
    ) -> impl Iterator<Item = (&str, &str, &imports::ExternalImports)> {
        // `zip` would truncate silently if the two ever disagreed, and a
        // truncated scan means a sibling links against a stub missing its
        // names. Both are filled from `es_module_sources` in `new`, so a
        // disagreement is a bug in a future mutator, not a reachable state.
        debug_assert_eq!(
            es_module_sources(&self.modules).count(),
            self.module_imports.len(),
            "an ES sibling lost its scanned imports",
        );
        es_module_sources(&self.modules)
            .zip(&self.module_imports)
            .map(|((name, source), imports)| (name, source, imports))
    }

    /// Give this Worker the deployment's cron trigger expressions.
    pub fn with_crons(mut self, crons: Vec<String>) -> Self {
        self.crons = crons;
        self
    }

    /// Grant this Worker a Worker Loader binding at each `env` name.
    pub fn with_loaders(mut self, bindings: Vec<String>) -> Self {
        self.loader_bindings = bindings;
        self
    }

    /// Set this Worker's ambient outbound authority (loaded workers only).
    pub(crate) fn with_egress(mut self, egress: EgressPolicy) -> Self {
        self.egress = egress;
        self
    }

    /// Install the limits that every invocation of this Dynamic Worker owns.
    pub(crate) fn with_resource_limits(mut self, limits: Option<ResourceLimits>) -> Self {
        self.resource_limits = limits;
        self
    }

    /// Merge the caller's structured-clone `env` onto a loaded worker's env.
    pub(crate) fn with_loader_env(mut self, env: Option<LoaderEnv>) -> Self {
        self.loader_env = env;
        self
    }

    /// Declare the service bindings this Worker may call.
    pub fn with_services(mut self, services: Vec<(String, String, Option<String>)>) -> Self {
        self.services = services;
        self
    }

    pub fn with_asset_binding(mut self, binding: Option<String>) -> Self {
        self.asset_binding = binding;
        self
    }
}

/// The generation and the complete target travel together, so a loaded Worker
/// cannot accidentally resolve the route against a later deployment.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct OutboundBroker {
    pub(crate) generation: crate::generation::GenerationId,
    pub(crate) script: String,
    pub(crate) entrypoint: Option<crate::WorkerFetchEntrypoint>,
}

/// Ambient outbound authority for an isolate. Normal workers keep `Allow`; a
/// Worker Loader can deny egress or route it through a transferred Fetcher.
#[derive(Clone, Default, PartialEq)]
pub(crate) enum EgressPolicy {
    #[default]
    Allow,
    Deny,
    Broker(OutboundBroker),
}
