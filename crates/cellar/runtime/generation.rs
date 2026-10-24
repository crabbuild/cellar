// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Modified for Cellar.

//! One application deployment as the running process holds it.
//!
//! A [`Generation`] is everything a node derives from a deployment: the
//! compiled Worker configurations, the isolate pools they run in, the
//! Durable Object class registry, the service-binding graph, the asset
//! resolvers, and the cron schedule. A node serves exactly one current
//! generation and reaches it through a snapshot, so a request that started
//! on one generation finishes on it even after the node adopts another.
//!
//! Boot and reload construct a generation through the same two functions,
//! [`DeploymentGraph::load`] and `Generation::build`. Nothing else reads a
//! deployment manifest into runtime state. A value a deployment implies
//! therefore has one place it can be computed, and a reload cannot miss what
//! a boot did, because there is no second path for it to miss.

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::Arc;

use anyhow::{anyhow, Context};

use crate::assets::AssetResolver;
use crate::bucket::Bucket;
use crate::engine_api::WorkerConfigOptions;
use crate::fleet::{self, LoadedDeployment};
use crate::worker_config::WorkerConfig;

/// Which generation, within this process. Monotonic from one at boot; never
/// reused, never persisted, and never compared across nodes — the fleet-wide
/// identity of a deployment is its version string.
pub type GenerationId = u64;

/// The generation a node boots on. Later generations count up from it.
pub const FIRST_GENERATION: GenerationId = 1;

/// Ask the node to adopt the deployment `deploy/current.json` names now.
pub struct ReloadRequest {
    /// Rebuild even when the pointer names the current deployment, so a
    /// memoised failure is retried and fresh isolates replace the serving
    /// ones. `POST /reload` sets it; a poll tick and a managed nudge do not.
    pub force: bool,
    /// Where to report the outcome. A poll tick has nobody to tell.
    pub reply: Option<tokio::sync::oneshot::Sender<ReloadOutcome>>,
}

/// What one adoption attempt concluded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReloadOutcome {
    /// A new generation serves. Requests admitted from now on use it.
    Adopted {
        generation: GenerationId,
        version: String,
        prefix: String,
    },
    /// The pointer names the deployment already serving and the request did
    /// not force a rebuild.
    Unchanged {
        generation: GenerationId,
        version: String,
    },
    /// The deployment did not build. The current generation is untouched.
    Failed {
        version: String,
        prefix: String,
        error: String,
    },
}

pub type ReloadSender = tokio::sync::mpsc::UnboundedSender<ReloadRequest>;
pub type ReloadReceiver = tokio::sync::mpsc::UnboundedReceiver<ReloadRequest>;

pub fn reload_channel() -> (ReloadSender, ReloadReceiver) {
    tokio::sync::mpsc::unbounded_channel()
}

/// Ask for a poll now, without waiting for the outcome. A managed
/// `deployment_current` message and a successful apply both end here: the
/// pointer is the authority, and the nudge only shortens the wait for it.
pub fn nudge(reload: &ReloadSender) {
    let _ = reload.send(ReloadRequest {
        force: false,
        reply: None,
    });
}

/// How often a node re-reads the pointer without a nudge. For a standalone
/// node this is the deploy latency; for a managed node it is the backstop
/// behind the push. `CELLAR_DEPLOY_POLL_S`, default 30.
pub fn poll_interval() -> std::time::Duration {
    let seconds = crate::env_vars::positive_or("CELLAR_DEPLOY_POLL_S", 30u64)
        .expect("validated CELLAR_DEPLOY_POLL_S");
    std::time::Duration::from_secs(seconds)
}

/// How long a resident cell may keep running a superseded generation after
/// the node adopts a new one, before its swap is forced: its activity
/// cancelled and its regular WebSockets closed with 1012. Zero forces every
/// resident cell at the flip, which is what a Cloudflare deployment does.
/// `CELLAR_DEPLOY_MAX_AGE_S`, default 60.
pub fn max_age() -> std::time::Duration {
    // Not `positive_or`: zero is meaningful here.
    let seconds = crate::env_vars::with_default("CELLAR_DEPLOY_MAX_AGE_S", 60u64)
        .expect("validated CELLAR_DEPLOY_MAX_AGE_S");
    std::time::Duration::from_secs(seconds)
}

/// The scripts one deployment reaches: the primary script plus every
/// service-binding target and queue consumer it declares, transitively.
///
/// [`DeploymentGraph::load`] is the only bucket walk, so the boot path and
/// the reload path cannot resolve a deployment's dependencies differently.
pub struct DeploymentGraph {
    pub primary: LoadedDeployment,
    pub cohosted: Vec<LoadedDeployment>,
}

impl DeploymentGraph {
    /// One script and nothing else: the local-script mode a runtime test
    /// starts from a file, which declares no services and has no bucket to
    /// resolve them from.
    pub fn single(primary: LoadedDeployment) -> Self {
        Self {
            primary,
            cohosted: Vec::new(),
        }
    }

    /// Resolve the fleet-wide pointer and every script it depends on.
    /// All resolvers retain the supplied cache, including cohosted scripts;
    /// configuration is never inferred from the loading process.
    ///
    /// A service binding names a script, and that script's own pointer names
    /// its deployment. A queue dependency names a queue, and the queue's
    /// consumer attachment names an exact deployment. The walk refuses a
    /// script that resolves twice and a queue attached to a deployment other
    /// than the one already loaded, because either would give one script two
    /// bodies in one process.
    pub async fn load(
        bucket: &Bucket,
        node: String,
        cache: &crate::assets::AssetCache,
    ) -> anyhow::Result<Self> {
        let primary = fleet::load_current_worker(bucket, node.clone(), cache).await?;
        let primary_script = primary.script_name.clone();
        let mut loaded_scripts = BTreeMap::from([(primary_script.clone(), primary.prefix.clone())]);
        let mut loaded_consumers =
            BTreeMap::from([(primary_script.clone(), consumed_queues(&primary.options))]);
        let mut visited_queues = BTreeSet::new();
        let mut dependencies = dependencies_of(&primary);
        let mut cohosted = Vec::new();
        while let Some(dependency) = dependencies.pop_front() {
            let loaded = match dependency {
                Dependency::Service(target) => {
                    if target == primary_script || loaded_scripts.contains_key(&target) {
                        continue;
                    }
                    let loaded = fleet::load_named_worker(bucket, &target, node.clone(), cache)
                        .await
                        .with_context(|| format!("load service binding target {target}"))?;
                    if loaded.script_name != target {
                        anyhow::bail!(
                            "service pointer {target} resolved script {}",
                            loaded.script_name
                        );
                    }
                    loaded
                }
                Dependency::Queue(queue) => {
                    if !visited_queues.insert(queue.clone()) {
                        continue;
                    }
                    let declared_by = loaded_consumers
                        .iter()
                        .find_map(|(script, queues)| queues.contains(&queue).then_some(script));
                    let Some(consumer) =
                        fleet::load_queue_consumer_attachment(bucket, &queue).await?
                    else {
                        if let Some(script) = declared_by {
                            anyhow::bail!(
                                "script {script:?} consumes queue {queue:?}, but the queue has no active consumer attachment; re-run `cellar deploy`"
                            );
                        }
                        continue;
                    };
                    if let Some(script) = declared_by {
                        anyhow::ensure!(
                            script == &consumer.script_name,
                            "queue {queue:?} is attached to script {:?}, but loaded script {script:?} also consumes it",
                            consumer.script_name
                        );
                    }
                    if let Some(prefix) = loaded_scripts.get(&consumer.script_name) {
                        anyhow::ensure!(
                            prefix == &consumer.prefix,
                            "queue {queue:?} is attached to deployment {} of script {:?}, but deployment {prefix} is already loaded; re-run `cellar deploy`",
                            consumer.version,
                            consumer.script_name
                        );
                        anyhow::ensure!(
                            loaded_consumers
                                .get(&consumer.script_name)
                                .is_some_and(|queues| queues.contains(&queue)),
                            "queue {queue:?} is attached to script {:?}, but its loaded deployment does not consume that queue",
                            consumer.script_name
                        );
                        continue;
                    }
                    fleet::load_queue_consumer_worker(
                        bucket,
                        &queue,
                        &consumer,
                        node.clone(),
                        cache,
                    )
                    .await
                    .with_context(|| format!("load consumer for queue {queue:?}"))?
                }
            };
            let target = loaded.script_name.clone();
            anyhow::ensure!(
                loaded_scripts
                    .insert(target.clone(), loaded.prefix.clone())
                    .is_none(),
                "script {target:?} was loaded twice"
            );
            loaded_consumers.insert(target.clone(), consumed_queues(&loaded.options));
            dependencies.extend(dependencies_of(&loaded));
            // A node runs the schedule of the deployment it was given and of
            // no other. The reserved class is one key, so a second script's
            // cron cell would resolve to the first script's config and run
            // the wrong `scheduled` handler. Dropping the schedule is the
            // safe half of that trade and this says so out loud, because a
            // trigger that never fires and says nothing is the failure the
            // whole feature is built to avoid. Deploy the script as a node's
            // own deployment to run its crons.
            if !loaded.crons.is_empty()
                || loaded
                    .options
                    .workflow_bindings
                    .iter()
                    .any(|binding| !binding.settings.schedules.is_empty())
            {
                tracing::warn!(
                    script = %target,
                    crons = %loaded.crons.join(", "),
                    "a service binding target declares cron triggers; a node fires only its own deployment's schedule, so these never run here"
                );
            }
            cohosted.push(loaded);
        }
        Ok(Self { primary, cohosted })
    }
}

enum Dependency {
    Service(String),
    Queue(String),
}

fn consumed_queues(options: &WorkerConfigOptions) -> BTreeSet<String> {
    options
        .queue_consumers
        .iter()
        .map(|consumer| consumer.queue.clone())
        .collect()
}

fn dependencies_of(loaded: &LoadedDeployment) -> VecDeque<Dependency> {
    let queues = loaded
        .options
        .queue_bindings
        .iter()
        .map(|binding| binding.queue.clone())
        .chain(loaded.options.queue_consumers.iter().flat_map(|consumer| {
            std::iter::once(consumer.queue.clone()).chain(consumer.dead_letter_queue.clone())
        }))
        .collect::<BTreeSet<_>>();
    loaded
        .services
        .iter()
        .map(|(_, script, _)| Dependency::Service(script.clone()))
        .chain(queues.into_iter().map(Dependency::Queue))
        .collect()
}

/// Node-level inputs `Generation::build` needs beside the deployment itself.
pub struct GenerationOptions {
    pub node: String,
    pub region: String,
}

/// A deployment, built and ready to serve.
///
/// The fields are the four maps `RuntimeManager` once held for the life of
/// the process, plus the asset resolvers and the cron schedule that lived on
/// the application handle. They are private and reached through
/// `RuntimeManager`, which hands out this struct only as a snapshot.
pub struct Generation {
    pub(crate) native_applications: HashMap<String, Arc<dyn crate::application::Application>>,
    event_streams: Vec<(String, crate::application::event_stream::Definition)>,
    event_bindings: Vec<crate::application::event_stream::BindingConfig>,
    pub(crate) event_readiness: Arc<crate::application::event_stream::readiness::Readiness>,
    pub(crate) id: GenerationId,
    pub(crate) version: String,
    pub(crate) prefix: String,
    pub(crate) script_name: String,
    pub(crate) cron_revision: Option<String>,
    pub(crate) primary_crons: Vec<String>,
    pub(crate) stateless: crate::runtime::StatelessRuntime,
    pub(crate) services: HashMap<String, crate::runtime::StatelessRuntime>,
    pub(crate) source_recovery: Arc<WorkerConfig>,
    pub(crate) source_recovery_pool: Arc<crate::pool::Pool>,
    pub(crate) cell_configs: HashMap<String, Arc<WorkerConfig>>,
    /// The isolates a Worker script's cells live in — the same `Pool` the
    /// stateless path admits into, because an isolate is an isolate. Cells
    /// of one script share them, so cells of one class share module scope
    /// exactly when they are colocated, which is what Durable Objects do.
    pub(crate) cell_isolates: HashMap<String, Arc<crate::pool::Pool>>,
    pub(crate) default_do_class: Option<Arc<str>>,
    pub(crate) assets: HashMap<String, AssetResolver>,
    /// Every container class of every script in the graph.
    pub(crate) containers: Vec<crate::container::ContainerSpec>,
    /// See `Manifest::fence_image`; the first script's, they are all the
    /// same image when one cellar deployed them.
    pub(crate) fence_image: Option<String>,
    pub(crate) container_images: Vec<crate::container::ImageArchive>,
}

impl Generation {
    pub(crate) fn event_bindings(&self) -> &[crate::application::event_stream::BindingConfig] {
        &self.event_bindings
    }

    pub(crate) fn begin_event_activation(&self) {
        if !self.event_streams.is_empty() || !self.event_bindings.is_empty() {
            self.event_readiness.begin();
        }
    }

    /// Desired configurations captured from trusted deployment state. These
    /// still require catalog reconciliation before bindings become ready.
    pub fn event_streams(&self) -> &[(String, crate::application::event_stream::Definition)] {
        &self.event_streams
    }

    /// Prepare trusted host provisioning for an exact captured deployment
    /// declaration. Hosts must route this request through their ordinary owner,
    /// recovery and proof path. SDK bindings cannot supply this authority.
    pub fn event_provisioning_request(
        &self,
        resource: &crate::application::event_stream::Resource,
        operation: &str,
        reply: tokio::sync::oneshot::Sender<anyhow::Result<crate::engine_api::RpcData>>,
    ) -> anyhow::Result<crate::host_channels::RpcCallReq> {
        use crate::application::event_stream as events;
        use base64::Engine;
        anyhow::ensure!(
            operation.len() == 32
                && operation
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
            "provisioning requires a canonical operation UUID"
        );
        let (script, definition) = self
            .event_streams
            .iter()
            .find(|(_, definition)| definition.resource == *resource)
            .ok_or_else(|| {
                anyhow::anyhow!("resource is not a captured provisioning declaration")
            })?;
        let bytes = events::encoding::json(definition, events::MAX_DEFINITION_BYTES)?;
        Ok(crate::host_channels::RpcCallReq {
            parent_cancellation: None,
            signal: None,
            deadline_ms: None,
            authority: Some(events::authority::Authority::deployment(script, definition)?.into()),
            request_id: Some(crate::engine_api::next_request_id()),
            cancel: None,
            scope: events::catalog_scope(resource)?,
            name: Some(events::catalog_name(resource)?),
            method: "__catalogConfigure".into(),
            args: crate::application::encode_rpc(
                &cellar_native::clone::StructuredValue::from_json(serde_json::json!([
                    {"operation":operation,"definition":base64::engine::general_purpose::STANDARD.encode(bytes)}
                ]))?,
            )?,
            reply,
        })
    }

    pub fn id(&self) -> GenerationId {
        self.id
    }

    pub fn version(&self) -> &str {
        &self.version
    }

    /// The schedule override this generation adopted, if any.
    pub fn cron_revision(&self) -> Option<&str> {
        self.cron_revision.as_deref()
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// The primary script: the one ingress serves and whose assets ingress
    /// consults before running the Worker.
    pub fn script_name(&self) -> &str {
        &self.script_name
    }

    /// The asset resolver of the named script, if that script deployed
    /// assets.
    pub fn assets(&self, script: &str) -> Option<&AssetResolver> {
        self.assets.get(script)
    }

    /// The primary script's asset resolver, which ingress consults.
    pub fn ingress_assets(&self) -> Option<&AssetResolver> {
        self.assets.get(&self.script_name)
    }

    pub fn has_cell_classes(&self) -> bool {
        self.cell_configs
            .keys()
            .any(|class| class != cellar_logic::cron::RESERVED_CLASS)
            || self.cron_cell().is_some()
    }

    /// The reserved cell carrying this deployment's cron schedule, or `None`
    /// when no schedules or overrides need adopting. The class also remains
    /// registered for read-only retained-history inspection after removal.
    pub fn cron_cell(&self) -> Option<String> {
        self.cell_configs
            .get(cellar_logic::cron::RESERVED_CLASS)
            .filter(|config| {
                self.cron_revision.is_some()
                    || !config.crons.is_empty()
                    || config
                        .workflow_bindings
                        .iter()
                        .any(|binding| !binding.settings.schedules.is_empty())
            })
            .map(|config| cellar_logic::cron::reserved_cell(&config.script_name))
    }

    pub(crate) fn cell_config(&self, class: &str) -> Option<Arc<WorkerConfig>> {
        self.cell_configs.get(class).cloned()
    }

    /// The engine's reserved Durable Object classes this generation
    /// registers: cron, queue, workflow, SQL, KV. Their cells hold no
    /// application state worth waiting for, so an adoption moves them at
    /// once — and the cron cell must run the new schedule before the
    /// adoption arms it.
    ///
    /// Cron execution inspection uses the same authenticated operator route
    /// as the other runtime classes; anonymous cell fetches cannot inspect it.
    pub fn reserved_classes(&self) -> Vec<String> {
        self.cell_configs
            .keys()
            .filter(|class| {
                crate::deploy::is_reserved_class(class)
                    || class.as_str() == cellar_logic::cron::RESERVED_CLASS
            })
            .cloned()
            .collect()
    }

    pub(crate) fn cell_isolates(&self, script: &str) -> Option<Arc<crate::pool::Pool>> {
        self.cell_isolates.get(script).cloned()
    }

    pub(crate) fn service(&self, script: &str) -> Option<crate::runtime::StatelessRuntime> {
        self.services.get(script).cloned()
    }

    pub(crate) fn default_do_class(&self) -> Option<&str> {
        self.default_do_class.as_deref()
    }

    /// Stop every isolate of this generation from taking new work. Stateless
    /// isolates free as their affiliations drop; cell isolates free as their
    /// cells move to a newer generation.
    pub(crate) fn retire(&self) {
        for service in self.services.values() {
            service.isolates.retire_all();
        }
        for pool in self.cell_isolates.values() {
            pool.retire_all();
        }
        self.source_recovery_pool.retire_all();
    }

    /// Whether every isolate of this generation has been freed, so the
    /// generation itself can be dropped.
    pub(crate) fn is_drained(&self) -> bool {
        self.services
            .values()
            .all(|service| service.isolates.is_drained())
            && self.cell_isolates.values().all(|pool| pool.is_drained())
            && self.source_recovery_pool.is_drained()
    }

    /// One maintenance pass over the cell pools: retire and free every empty
    /// isolate. An empty cell heap carries no warm request capacity worth
    /// preserving, unlike a stateless one.
    pub(crate) fn reap_cell_pools(&self) {
        for pool in self.cell_isolates.values() {
            pool.reap_empty();
        }
        self.source_recovery_pool.reap_empty();
    }

    /// The isolates this generation holds right now, for `/state`.
    pub(crate) fn isolate_census(&self) -> IsolateCensus {
        IsolateCensus {
            source_recovery: self.source_recovery_pool.census(),
            stateless: self.stateless.isolates.census(),
            services: self
                .services
                .iter()
                .map(|(name, service)| (name.clone(), service.isolates.census()))
                .collect(),
            cells: self
                .cell_isolates
                .iter()
                .map(|(script, pool)| (script.clone(), pool.census()))
                .collect(),
        }
    }
}

/// The isolates one generation holds, by the pool they belong to, as
/// `/state` reports them.
#[derive(Debug, serde::Serialize)]
pub struct IsolateCensus {
    pub source_recovery: crate::engine_api::PoolCensus,
    /// The pool that serves the primary script's stateless handlers.
    pub stateless: crate::pool::PoolCensus,
    /// The pools of the named services, by service name.
    pub services: BTreeMap<String, crate::pool::PoolCensus>,
    /// The pools the cells live in, by Worker script.
    pub cells: BTreeMap<String, crate::pool::PoolCensus>,
}

/// The generation an isolate was built for, installed as an isolate slot by
/// `Worker::load_config` so a call the isolate makes into the host — a
/// service binding, an assets binding, a queue dispatch — resolves against
/// the graph the caller was built with rather than whichever generation is
/// current when the call lands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GenerationTag(pub GenerationId);

impl Generation {
    /// Build compiled Rust applications on the same deployment graph, pools,
    /// and class registry. Every script must be registered before any worker
    /// is constructed; this path never initializes the JavaScript engine.
    pub fn build_native(
        id: GenerationId,
        graph: DeploymentGraph,
        options: GenerationOptions,
        applications: HashMap<String, Arc<dyn crate::application::Application>>,
    ) -> anyhow::Result<Self> {
        let mut scripts = BTreeSet::new();
        for deployment in std::iter::once(&graph.primary).chain(&graph.cohosted) {
            anyhow::ensure!(
                scripts.insert(&deployment.script_name),
                "duplicate native application script {}",
                deployment.script_name
            );
            anyhow::ensure!(
                deployment.options.script_name == deployment.script_name,
                "native application script disagrees with its Worker configuration"
            );
            anyhow::ensure!(
                applications.contains_key(&deployment.script_name),
                "no native application registered for script {}",
                deployment.script_name
            );
        }
        Self::build_with_applications(id, graph, options, applications)
    }

    fn build_with_applications(
        id: GenerationId,
        graph: DeploymentGraph,
        options: GenerationOptions,
        mut applications: HashMap<String, Arc<dyn crate::application::Application>>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            crate::asyncrt::services().accepts_work(),
            "generation execution domain is closed"
        );
        crate::asyncrt::services().loader_policy();
        let native_applications = applications.clone();
        let cell_limits = crate::application::limits::current();
        let managed_events =
            std::iter::once(&graph.primary)
                .chain(&graph.cohosted)
                .any(|deployment| {
                    !deployment.event_streams.is_empty()
                        || (deployment.native.is_some()
                            && !deployment.options.stream_bindings.is_empty())
                });
        let event_readiness = Arc::new(
            crate::application::event_stream::readiness::Readiness::new(!managed_events),
        );
        let mut event_streams = Vec::new();
        let mut event_bindings = Vec::new();
        let mut stream_identities = BTreeSet::new();
        let mut stream_names = BTreeSet::new();
        for deployment in std::iter::once(&graph.primary).chain(&graph.cohosted) {
            let options = &deployment.options;
            let mut occupied = options
                .bindings
                .iter()
                .chain(&options.blob_bindings)
                .chain(&options.sql_bindings)
                .chain(&options.kv_bindings)
                .chain(&options.vars)
                .map(|(name, _)| name.clone())
                .collect::<BTreeSet<_>>();
            occupied.extend(
                options
                    .queue_bindings
                    .iter()
                    .map(|binding| binding.environment.clone()),
            );
            occupied.extend(
                options
                    .workflow_bindings
                    .iter()
                    .map(|binding| binding.environment.clone()),
            );
            occupied.extend(deployment.loader_bindings.iter().cloned());
            occupied.extend(deployment.services.iter().map(|(name, _, _)| name.clone()));
            occupied.extend(deployment.asset_binding.iter().cloned());
            crate::protocol::validate_event_resources(
                &deployment.script_name,
                &deployment.event_streams,
                &options.stream_bindings,
                &occupied,
                &cell_limits.events,
            )?;
            event_bindings.extend(options.stream_bindings.iter().cloned());
            for definition in &deployment.event_streams {
                anyhow::ensure!(
                    stream_identities.insert(definition.resource.id.clone())
                        && stream_names.insert((
                            definition.resource.tenant.clone(),
                            definition.resource.application.clone(),
                            definition.resource.name.clone()
                        )),
                    "more than one application declares the same event stream resource or name"
                );
                event_streams.push((deployment.script_name.clone(), definition.clone()));
            }
        }
        let GenerationOptions { node, region } = options;
        let DeploymentGraph { primary, cohosted } = graph;

        // A Queue class is shared by every co-hosted script, but its broker
        // needs the consumer script rather than whichever script happened to
        // register `__Queue` first. Resolve one deployment-wide catalog before
        // constructing any WorkerConfig, then install the same catalog into
        // every isolate that could host a queue cell.
        let mut queue_catalog = BTreeMap::new();
        for (script, consumers) in
            std::iter::once((&primary.script_name, &primary.options.queue_consumers)).chain(
                cohosted
                    .iter()
                    .map(|target| (&target.script_name, &target.options.queue_consumers)),
            )
        {
            for consumer in consumers {
                cellar_logic::queue::validate_config_with_limits(
                    &cellar_logic::queue::QueueConfig {
                        max_batch_size: consumer.max_batch_size,
                        max_batch_timeout_seconds: consumer.max_batch_timeout,
                        max_retries: consumer.max_retries,
                        max_concurrency: consumer.max_concurrency,
                        delivery_delay_seconds: 0,
                        retry_delay_seconds: consumer.retry_delay,
                    },
                    &cell_limits.queue,
                )
                .map_err(anyhow::Error::new)?;
                let registration = crate::worker_config::QueueConsumerRegistration {
                    script: script.clone(),
                    config: consumer.clone(),
                };
                if let Some(previous) = queue_catalog.insert(consumer.queue.clone(), registration) {
                    return Err(anyhow!(
                        "queue {:?} is consumed by both {} and {}",
                        consumer.queue,
                        previous.script,
                        script
                    ));
                }
            }
        }
        let queue_catalog = queue_catalog.into_values().collect::<Vec<_>>();

        let node: Arc<str> = Arc::from(node);
        let region: Arc<str> = Arc::from(region);
        let primary_script = primary.script_name.clone();
        let version = primary.version.clone();
        let prefix = primary.prefix.clone();
        let cron_revision = primary.cron_revision.clone();
        let primary_crons = primary.crons.clone();
        let mut all_containers = Vec::new();
        let mut all_container_images = Vec::new();
        let mut fence_image = None;
        // Only classes the user declared can be a bare-id default. Every
        // runtime-supplied class rides in `do_classes` so that its namespace
        // key is minted, and counting one here made adding any SQL binding flip
        // a one-class project past the `len == 1` condition — every
        // `/do/<bare-id>` request then failed with "requires exactly one
        // configured Durable Object class" for a config that still declared
        // exactly one.
        //
        // This asks `deploy::is_reserved_class` rather than naming one class,
        // because the first version of this filter named `__SqlDatabase` alone
        // and `__Workflow` walked straight back into the same bug when it
        // shipped. A fourth reserved class must not be able to do it a third
        // time.
        let user_classes: Vec<&String> = primary
            .options
            .do_classes
            .iter()
            .filter(|class| !crate::deploy::is_reserved_class(class))
            .collect();
        let default_do_class =
            (user_classes.len() == 1).then(|| Arc::from(user_classes[0].as_str()));
        // Two scripts exporting one class name is genuinely ambiguous and is
        // refused — but a *reserved* class is not an export, and the two kinds
        // behave differently:
        //
        // `__SqlDatabase` is deliberately shared. Its namespace is fleet-wide so
        // that several Workers can bind one database and a rename cannot rename
        // it, so two scripts declaring `sql_databases` address the same cells on
        // purpose. A SQL cell runs SQL and reads no user code, binding or var,
        // so whichever config serves it is immaterial and the first wins.
        // Refusing the second made the node exit instead of start.
        //
        // A workflow class is script-scoped (`deploy::workflow_class`), so two
        // scripts produce two distinct names and never reach this branch. If
        // one ever did, it would be a real collision and must still be refused.
        let mut cell_configs: HashMap<String, Arc<WorkerConfig>> = HashMap::new();
        let mut service_pools = HashMap::new();
        let mut assets = HashMap::new();
        // Primary first: shared reserved classes retain the first script's
        // config. Only this script owns ingress and the node's cron schedule;
        // bindings and runtime construction follow the same path for all scripts.
        for (index, deployment) in std::iter::once(primary).chain(cohosted).enumerate() {
            let is_primary = index == 0;
            let crate::fleet::LoadedDeployment {
                options,
                script_name: script,
                asset_binding,
                loader_bindings,
                assets: resolver,
                services,
                crons,
                containers,
                fence_image: fence,
                container_images,
                ..
            } = deployment;
            all_containers.extend(containers.iter().cloned());
            all_container_images.extend(container_images);
            fence_image = fence_image.or(fence);
            if let Some(resolver) = resolver {
                assets.insert(script.clone(), resolver);
            }
            let mut classes = options.do_classes.clone();
            // System cells must remain routable while a candidate provisions
            // resources, including when the serving generation has no binding.
            classes.extend([
                crate::deploy::EVENT_STREAM_CLASS.into(),
                crate::deploy::EVENT_CATALOG_CLASS.into(),
            ]);
            classes.sort();
            classes.dedup();
            for binding in &options.queue_bindings {
                anyhow::ensure!(
                    binding.delivery_delay <= cell_limits.queue.max_delay_seconds,
                    "Queue binding {:?} delivery delay exceeds the host limit",
                    binding.environment
                );
            }
            let mut config = WorkerConfig::new(options)
                .with_services(services)
                .with_asset_binding(asset_binding)
                .with_loaders(loader_bindings)
                .with_queue_consumers(queue_catalog.clone())
                .with_crons(if is_primary { crons } else { Vec::new() })
                .with_cron_history(is_primary.then(|| {
                    crate::application::cron::HistoryConfiguration {
                        prefix: prefix.clone(),
                        deployment_version: version.clone(),
                        schedule_revision: cron_revision.clone(),
                    }
                }))
                .with_containers(containers)
                .with_generation(id);
            if let Some(application) = applications.remove(&script) {
                config = config.with_native_application(application);
            }
            config.event_readiness = event_readiness.clone();
            let config = Arc::new(config);
            let pool = crate::runtime::StatelessRuntime::start(
                config.clone(),
                node.clone(),
                region.clone(),
            )?;
            if service_pools.insert(script.clone(), pool).is_some() {
                return Err(anyhow!("duplicate co-hosted Worker script {script}"));
            }
            for class in classes {
                register_cell_class(&mut cell_configs, class, config.clone(), &|class| {
                    if is_primary {
                        anyhow!("duplicate Durable Object class {class}")
                    } else {
                        anyhow!(
                            "Durable Object class {class} is exported by more than one co-hosted script"
                        )
                    }
                })?;
            }
            // Cron is runtime-supplied, not a manifest class. Its alarm calls
            // the primary script's scheduled handler with that script's bindings.
            // Keep the class available to inspect retained execution records
            // after schedules are removed. cron_cell() separately determines
            // whether the node must arm/adopt an active schedule.
            if is_primary {
                cell_configs.insert(cellar_logic::cron::RESERVED_CLASS.to_string(), config);
            }
        }
        let stateless = service_pools[&primary_script].clone();
        let cell_isolates: HashMap<String, Arc<crate::pool::Pool>> = cell_configs
            .values()
            .map(|config| {
                (
                    config.script_name.clone(),
                    crate::runtime::cell_pool(config.clone()),
                )
            })
            .collect();
        let source_recovery = crate::runtime::source_recovery::configuration(id, &node);
        let source_recovery_pool = crate::runtime::cell_pool(source_recovery.clone());
        Ok(Generation {
            native_applications,
            source_recovery,
            source_recovery_pool,
            event_streams,
            event_bindings,
            event_readiness,
            id,
            version,
            prefix,
            script_name: primary_script,
            cron_revision,
            primary_crons,
            stateless,
            services: service_pools,
            cell_configs,
            cell_isolates,
            default_do_class,
            assets,
            containers: all_containers,
            container_images: all_container_images,
            fence_image,
        })
    }
}

/// Claim one class name in a deployment's cell registry.
///
/// A duplicate is an error for a user class, because `start_cell` resolves a
/// cell from the class its scope names and two exports of one name are
/// genuinely ambiguous. A duplicate of a *shared* reserved class is not: see
/// `deploy::is_shared_reserved_class`. The caller supplies the error so the
/// message can say whether the collision was inside one script or across two.
fn register_cell_class(
    configs: &mut HashMap<String, Arc<WorkerConfig>>,
    class: String,
    config: Arc<WorkerConfig>,
    duplicate: &dyn Fn(&str) -> anyhow::Error,
) -> anyhow::Result<()> {
    match configs.entry(class) {
        Entry::Vacant(slot) => {
            slot.insert(config);
            Ok(())
        }
        Entry::Occupied(slot) if crate::deploy::is_shared_reserved_class(slot.key()) => Ok(()),
        Entry::Occupied(slot) => Err(duplicate(slot.key())),
    }
}
