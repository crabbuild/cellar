//! Application-owned production node bootstrap and supervision.

use super::*;
use crate::asyncrt::ExecutionDomain;
use crate::host_services::{HostServices, MetricsSampler};
use crate::loader_policy::LoaderPolicy;
use crate::log_options::LogOptions;
use crate::node_lifecycle::{NodeOutcome, ShutdownPolicy};
use crate::node_log::{DurabilityOwner, LogTransport, NodeLogManager, OwnLog};
use crate::peer_probe::PeerProbeSigner;
use crate::replica_storage::ReplicaStorage;
use crate::replication_options::ReplicationOptions;
use crate::runtime_policy::RuntimePolicy;
use futures_util::FutureExt;
use std::path::PathBuf;
use std::time::Duration;

#[cfg(test)]
#[path = "embedded_tests.rs"]
mod tests;

async fn build_generation(
    backend: Backend,
    id: crate::generation::GenerationId,
    graph: DeploymentGraph,
    options: GenerationOptions,
) -> anyhow::Result<Generation> {
    crate::asyncrt::try_blocking(move || backend.build(id, graph, options))?.await?
}

async fn sample_load() -> anyhow::Result<crate::host_services::HostMetricsSample> {
    let services = crate::asyncrt::services();
    Ok(crate::asyncrt::try_blocking(move || services.sample_metrics())?.await?)
}

/// Bounds initial lease establishment and predecessor recovery. Recovery must
/// succeed before this node can install its lease, in either durability posture.
#[derive(Clone, Debug)]
pub struct NodeStartupPolicy {
    pub bootstrap_timeout: Duration,
    pub recovery_attempts: u32,
    pub recovery_attempt_timeout: Duration,
    pub recovery_retry_backoff: Duration,
    pub authority_timeout: Duration,
}

impl Default for NodeStartupPolicy {
    fn default() -> Self {
        Self {
            bootstrap_timeout: Duration::from_secs(60 * 60),
            recovery_attempts: 4,
            recovery_attempt_timeout: Duration::from_secs(15 * 60),
            recovery_retry_backoff: Duration::from_secs(10),
            authority_timeout: Duration::from_secs(10),
        }
    }
}

/// Periodic engine work. The readiness observation deadline reports a stalled
/// fleet; it never opens readiness on failure. None explicitly skips that gate.
#[derive(Clone, Debug)]
pub struct NodeMaintenancePolicy {
    pub load_sample_period: Duration,
    pub fleet_sample_period: Duration,
    pub rebalance: bool,
    pub readiness_observation_deadline: Option<Duration>,
    pub wake_period: Duration,
    pub local_cache_prune_period: Duration,
    pub local_cache_max_bytes: Option<u64>,
}

impl Default for NodeMaintenancePolicy {
    fn default() -> Self {
        Self {
            load_sample_period: LOAD_SAMPLE_PERIOD,
            fleet_sample_period: Duration::from_millis(DEFAULT_REBALANCE_INTERVAL_MS),
            rebalance: true,
            readiness_observation_deadline: Some(Duration::from_secs(120)),
            wake_period: Duration::from_secs(60),
            local_cache_prune_period: LOCAL_CACHE_PRUNE_PERIOD,
            local_cache_max_bytes: None,
        }
    }
}

/// Captured node policy. This constructor and its defaults never read process
/// environment, install logging or TLS providers, tune allocators, or bind ports.
pub struct NodeConfig {
    pub identity: ActorIdentity,
    pub actor: crate::actor::ActorOptions,
    pub data_dir: PathBuf,
    pub runtime: RuntimePolicy,
    /// SDK HTTP request deadline, including response-body reads.
    pub http: crate::http_policy::HttpPolicy,
    pub cell_limits: crate::application::limits::CellLimits,
    pub loader: LoaderPolicy,
    pub shutdown: ShutdownPolicy,
    pub startup: NodeStartupPolicy,
    pub maintenance: NodeMaintenancePolicy,
    pub trust_forwarded_headers: bool,
    pub max_request_body_bytes: usize,
    pub generation_max_age: Duration,
}

impl NodeConfig {
    pub fn new(
        identity: ActorIdentity,
        actor: crate::actor::ActorOptions,
        data_dir: PathBuf,
    ) -> Self {
        let startup = NodeStartupPolicy {
            recovery_retry_backoff: Duration::from_millis(actor.lease_ttl_ms.max(1_000)),
            ..Default::default()
        };
        Self {
            identity,
            actor,
            data_dir,
            runtime: RuntimePolicy::default(),
            http: crate::http_policy::HttpPolicy::default(),
            cell_limits: Default::default(),
            loader: LoaderPolicy::default(),
            shutdown: ShutdownPolicy::default(),
            startup,
            maintenance: NodeMaintenancePolicy::default(),
            trust_forwarded_headers: false,
            max_request_body_bytes: DEFAULT_MAX_REQUEST_BODY_BYTES,
            generation_max_age: Duration::from_secs(60),
        }
    }

    fn validate(&self, services: &StartupServices) -> anyhow::Result<()> {
        self.actor.validate_policy()?;
        anyhow::ensure!(
            self.actor.core.peer_protocol == crate::peer_auth::PROTOCOL_VERSION,
            "node actor policy must advertise the current peer protocol {}",
            crate::peer_auth::PROTOCOL_VERSION
        );
        self.runtime.validate()?;
        self.http.validate()?;
        self.cell_limits.validate()?;
        self.loader.validate()?;
        self.shutdown.validate()?;
        crate::machine::validate_node_name(&self.identity.node)?;
        crate::startup::parse_advertise(&self.identity.advertise)?;
        anyhow::ensure!(
            services.peer_auth.source() == self.identity.node,
            "node and peer authentication must identify the same node"
        );
        anyhow::ensure!(
            self.actor.resume_generation.is_none(),
            "node startup derives resume generation from its certified local marker"
        );
        anyhow::ensure!(
            self.max_request_body_bytes > 0
                && self.max_request_body_bytes <= DEFAULT_MAX_REQUEST_BODY_BYTES,
            "node request body limit must be positive and at most {DEFAULT_MAX_REQUEST_BODY_BYTES}"
        );
        anyhow::ensure!(
            services.public_address != services.internal_listener.local_addr()?,
            "public and internal node listeners must be distinct"
        );
        anyhow::ensure!(
            services.public_address.port() != 0,
            "public address must be bound"
        );
        anyhow::ensure!(
            self.startup.recovery_attempts > 0,
            "predecessor recovery requires at least one attempt"
        );
        anyhow::ensure!(
            self.startup.recovery_retry_backoff >= Duration::from_millis(self.actor.lease_ttl_ms),
            "recovery retry backoff must cover the predecessor lease TTL"
        );
        validate_duration(
            "shutdown budget",
            Duration::from_millis(self.shutdown.total_ms),
        )?;
        for (name, duration) in [
            ("bootstrap timeout", self.startup.bootstrap_timeout),
            (
                "recovery attempt timeout",
                self.startup.recovery_attempt_timeout,
            ),
            (
                "recovery retry backoff",
                self.startup.recovery_retry_backoff,
            ),
            ("authority timeout", self.startup.authority_timeout),
            ("load sample period", self.maintenance.load_sample_period),
            ("fleet sample period", self.maintenance.fleet_sample_period),
            ("wake period", self.maintenance.wake_period),
            (
                "local cache prune period",
                self.maintenance.local_cache_prune_period,
            ),
        ] {
            validate_duration(name, duration)?;
        }
        duration_ms("generation maximum age", self.generation_max_age)?;
        if let Some(deadline) = self.maintenance.readiness_observation_deadline {
            validate_duration("readiness observation deadline", deadline)?;
        }
        if let Some(fleet) = &services.fleet {
            anyhow::ensure!(
                self.actor.generation == services.probe_signer.public_key_hex(),
                "fleet lease generation must be the supplied probe signer's public key"
            );
            anyhow::ensure!(
                fleet.bucket.name == fleet.lease_bucket.name
                    && fleet.bucket.prefix == fleet.lease_bucket.prefix
                    && fleet.bucket.backend() == fleet.lease_bucket.backend(),
                "ordinary and lease clients must identify the same fleet bucket"
            );
            anyhow::ensure!(
                fleet.bucket.prefix == fleet.replica_storage.prefix(),
                "node ownership and replication must use the same fleet prefix"
            );
            fleet.replication.validate()?;
            fleet.log.validate()?;
        } else {
            anyhow::ensure!(
                self.actor.core.ownership_on_evict == cellar_logic::OwnershipOnEvict::Sticky,
                "a local node must retain its sole data copy"
            );
        }
        Ok(())
    }
}

fn duration_ms(name: &str, duration: Duration) -> anyhow::Result<u64> {
    duration
        .as_millis()
        .try_into()
        .with_context(|| format!("{name} exceeds the millisecond clock range"))
}

fn validate_duration(name: &str, duration: Duration) -> anyhow::Result<()> {
    anyhow::ensure!(
        !duration.is_zero() && duration.as_millis() > 0,
        "{name} must be at least one millisecond"
    );
    duration_ms(name, duration)?;
    anyhow::ensure!(
        tokio::time::Instant::now().checked_add(duration).is_some(),
        "{name} exceeds the host clock range"
    );
    Ok(())
}

/// The exact ordinary, conditional-write, replication and authenticated log
/// transports owned by the application. No replacement pool is constructed.
pub struct NodeFleet {
    pub bucket: Bucket,
    pub lease_bucket: Bucket,
    pub replica_storage: ReplicaStorage,
    pub replication: ReplicationOptions,
    pub log_transport: Arc<dyn LogTransport>,
    pub log: LogOptions,
    /// Fleet proof uses follower fsyncs when available; false requires bucket proof.
    pub fleet_durability: bool,
}

/// Supplied process infrastructure. Keep both executors alive through node
/// completion. The public and internal listeners must already be bound.
pub struct NodeServices {
    /// Optional caller-owned UTC clock, captured before bootstrap. None uses
    /// system time. This does not replace monotonic timers or peer wire time.
    pub wall_clock: Option<Arc<crate::asyncrt::WallClock>>,
    /// Optional application-owned TLS, DNS, proxy and pool configuration for
    /// SDK HTTP egress. Cellar installs each mode's required redirect policy.
    pub http_client: Option<crate::http_policy::HttpClientFactory>,
    pub host_executor: tokio::runtime::Handle,
    pub core_executor: tokio::runtime::Handle,
    pub listener: tokio::net::TcpListener,
    pub internal_listener: tokio::net::TcpListener,
    pub peer_http: reqwest::Client,
    pub peer_auth: Arc<PeerAuth>,
    pub probe_signer: Arc<PeerProbeSigner>,
    pub fleet: Option<NodeFleet>,
    pub containers: Option<crate::container::EngineOptions>,
    pub metrics: Option<Arc<MetricsSampler>>,
}

/// Services for a node whose public HTTP transport is owned by the application.
/// Bind the public listener first and supply its address. The private peer
/// listener remains owned by Cellar. `ingress_capacity` bounds accepted calls,
/// including queued requests and responses whose bodies are still open.
pub struct ExternalNodeServices {
    /// Optional caller-owned UTC clock; see NodeServices::wall_clock.
    pub wall_clock: Option<Arc<crate::asyncrt::WallClock>>,
    /// Optional application-owned configuration for SDK HTTP egress.
    pub http_client: Option<crate::http_policy::HttpClientFactory>,
    pub host_executor: tokio::runtime::Handle,
    pub core_executor: tokio::runtime::Handle,
    pub public_address: std::net::SocketAddr,
    pub ingress_capacity: usize,
    pub internal_listener: tokio::net::TcpListener,
    pub peer_http: reqwest::Client,
    pub peer_auth: Arc<PeerAuth>,
    pub probe_signer: Arc<PeerProbeSigner>,
    pub fleet: Option<NodeFleet>,
    pub containers: Option<crate::container::EngineOptions>,
    pub metrics: Option<Arc<MetricsSampler>>,
}

struct StartupServices {
    filesystem: Arc<dyn cellar_ltx::FileSystem>,
    wall_clock: Option<Arc<crate::asyncrt::WallClock>>,
    http_client: Option<crate::http_policy::HttpClientFactory>,
    host_executor: tokio::runtime::Handle,
    core_executor: tokio::runtime::Handle,
    public_address: std::net::SocketAddr,
    listener: Option<tokio::net::TcpListener>,
    external_ingress: Option<mpsc::UnboundedReceiver<super::ingress::IngressCall>>,
    internal_listener: tokio::net::TcpListener,
    peer_http: reqwest::Client,
    peer_auth: Arc<PeerAuth>,
    probe_signer: Arc<PeerProbeSigner>,
    fleet: Option<NodeFleet>,
    containers: Option<crate::container::EngineOptions>,
    metrics: Option<Arc<MetricsSampler>>,
}

impl StartupServices {
    fn native(services: NodeServices) -> anyhow::Result<Self> {
        Ok(Self {
            filesystem: Arc::new(cellar_ltx::DirectFileSystem),
            wall_clock: services.wall_clock,
            http_client: services.http_client,
            public_address: services.listener.local_addr()?,
            listener: Some(services.listener),
            external_ingress: None,
            host_executor: services.host_executor,
            core_executor: services.core_executor,
            internal_listener: services.internal_listener,
            peer_http: services.peer_http,
            peer_auth: services.peer_auth,
            probe_signer: services.probe_signer,
            fleet: services.fleet,
            containers: services.containers,
            metrics: services.metrics,
        })
    }

    fn external(
        services: ExternalNodeServices,
        receiver: mpsc::UnboundedReceiver<super::ingress::IngressCall>,
    ) -> Self {
        Self {
            filesystem: Arc::new(cellar_ltx::DirectFileSystem),
            wall_clock: services.wall_clock,
            http_client: services.http_client,
            public_address: services.public_address,
            listener: None,
            external_ingress: Some(receiver),
            host_executor: services.host_executor,
            core_executor: services.core_executor,
            internal_listener: services.internal_listener,
            peer_http: services.peer_http,
            peer_auth: services.peer_auth,
            probe_signer: services.probe_signer,
            fleet: services.fleet,
            containers: services.containers,
            metrics: services.metrics,
        }
    }
}

/// Published only after recovery and node authority have been established.
#[derive(Clone, Debug)]
pub struct NodeInfo {
    pub node: String,
    pub advertise: String,
    pub public_address: std::net::SocketAddr,
    pub internal_address: std::net::SocketAddr,
    pub lease_generation: String,
    pub data_dir: PathBuf,
}

/// One application-owned engine. It never installs process signals or exits.
/// Outcomes are delivered only after its execution domain has closed and joined.
/// Shutdown policy bounds the semantic drain; started blocking work must finish
/// before the join can complete. Dropping this handle forces closure, and the
/// supervisor keeps joining on the supplied executor. Keep that executor alive.
pub struct Node {
    domain: Arc<ExecutionDomain>,
    raft_auth: Arc<PeerAuth>,
    app: AppHandle,
    info: NodeInfo,
    command: mpsc::UnboundedSender<ShutdownMode>,
    supervisor: tokio::task::JoinHandle<anyhow::Result<NodeOutcome>>,
    generation_max_age: Duration,
    completion: Option<Result<NodeOutcome, String>>,
}

struct CloseOnDrop(Option<Arc<ExecutionDomain>>);

impl Drop for CloseOnDrop {
    fn drop(&mut self) {
        if let Some(domain) = &self.0 {
            domain.close();
        }
    }
}

struct NodeDirectory(std::fs::File);

impl NodeDirectory {
    fn acquire(path: &std::path::Path) -> anyhow::Result<(Self, PathBuf)> {
        std::fs::create_dir_all(path)?;
        let path = path.canonicalize()?;
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path.join("__sys_node.lock"))?;
        file.try_lock()
            .with_context(|| format!("node data directory is already owned: {}", path.display()))?;
        Ok((Self(file), path))
    }
}

impl Drop for NodeDirectory {
    fn drop(&mut self) {
        let _ = self.0.unlock();
    }
}

async fn wait_for_event_activation(
    readiness: &crate::application::event_stream::readiness::Readiness,
    domain: &ExecutionDomain,
    mut supervisor: tokio::task::JoinHandle<anyhow::Result<NodeOutcome>>,
) -> anyhow::Result<tokio::task::JoinHandle<anyhow::Result<NodeOutcome>>> {
    // The driver can exit before activation is first polled, before its Pending
    // guard exists. Retaining the generation also retains the readiness sender,
    // so waiting only on that latch cannot observe the supervisor's exit.
    let completion = crate::asyncrt::select! {
        result = readiness.wait() => {
            if let Err(error) = result {
                domain.close();
                let completion = supervisor
                    .await
                    .context("node supervisor task failed during event resource activation")
                    .and_then(|result| result);
                return Err(match completion {
                    Ok(outcome) => error.context(format!(
                        "node event resource activation failed; supervisor ended: {outcome:?}"
                    )),
                    Err(supervisor) => supervisor.context(format!(
                        "node event resource activation failed: {error:#}"
                    )),
                });
            }
            // Readiness and completion can both be ready in this poll. A
            // completed supervisor must never become a successful Node handle.
            if !supervisor.is_finished() {
                return Ok(supervisor);
            }
            supervisor.await
        },
        result = &mut supervisor => result,
    };
    domain.close();
    match completion.context("node supervisor task failed during event resource activation")? {
        Err(error) => Err(error.context("node stopped during event resource activation")),
        Ok(outcome) => {
            anyhow::bail!("node startup ended during event resource activation: {outcome:?}")
        }
    }
}

impl Node {
    /// Start the full production runtime on the supplied executors. Local
    /// directories are exclusive through completion, including failed or
    /// cancelled startup. Every compiled generation is built in its own domain.
    /// The factory serves authenticated follower recovery before installing a
    /// replacement lease. A failed recovery cannot overwrite its predecessor.
    pub async fn start(
        config: NodeConfig,
        services: NodeServices,
        graph: DeploymentGraph,
        applications: CompiledApplications,
    ) -> anyhow::Result<Self> {
        Self::start_with_services(
            config,
            StartupServices::native(services)?,
            graph,
            applications,
        )
        .await
    }

    /// Start with caller-owned durability I/O captured by this node's execution
    /// domain. File/rename/directory sync semantics must match `FileSystem`.
    /// Directory ownership locks and application SQLite execution continue to
    /// use host OS paths; the provider must address those same local files.
    pub async fn start_with_filesystem(
        config: NodeConfig,
        services: NodeServices,
        graph: DeploymentGraph,
        applications: CompiledApplications,
        filesystem: Arc<dyn cellar_ltx::FileSystem>,
    ) -> anyhow::Result<Self> {
        let mut services = StartupServices::native(services)?;
        services.filesystem = filesystem;
        Self::start_with_services(config, services, graph, applications).await
    }

    /// Start the same production engine without a Cellar-owned public listener.
    /// The returned ingress handle is ready only after recovery and startup.
    /// Keep the caller's executors and public transport alive until node shutdown
    /// has joined; close public admission before waiting for server drain.
    pub async fn start_external(
        config: NodeConfig,
        services: ExternalNodeServices,
        graph: DeploymentGraph,
        applications: CompiledApplications,
    ) -> anyhow::Result<(Self, super::IngressHandle)> {
        anyhow::ensure!(
            services.ingress_capacity > 0
                && services.ingress_capacity <= tokio::sync::Semaphore::MAX_PERMITS,
            "ingress capacity must be positive and within the semaphore limit"
        );
        let capacity = services.ingress_capacity;
        let (sender, receiver) = mpsc::unbounded_channel();
        let node = Self::start_with_services(
            config,
            StartupServices::external(services, receiver),
            graph,
            applications,
        )
        .await?;
        let ingress = super::IngressHandle::new(
            node.app.clone(),
            Arc::downgrade(&node.domain),
            sender,
            capacity,
        );
        Ok((node, ingress))
    }

    async fn start_with_services(
        mut config: NodeConfig,
        services: StartupServices,
        graph: DeploymentGraph,
        applications: CompiledApplications,
    ) -> anyhow::Result<Self> {
        config.validate(&services)?;
        let raft_auth = services.peer_auth.clone();
        let (directory, data_dir) = NodeDirectory::acquire(&config.data_dir)?;
        config.data_dir = data_dir;
        let generation_max_age = config.generation_max_age;
        let domain = Arc::new(ExecutionDomain::with_filesystem(
            services.host_executor.clone(),
            services.filesystem.clone(),
            services.wall_clock.clone(),
        ));
        let mut startup_guard = CloseOnDrop(Some(domain.clone()));
        config.actor.resume_generation = domain
            .run(async {
                crate::runtime::take_clean_reload_generation(
                    &config.data_dir,
                    &config.identity.node,
                )
            })
            .await?;
        domain.services().configure_actor(&config.actor)?;
        domain.services().configure_runtime(config.runtime)?;
        domain
            .services()
            .configure_http(config.http, services.http_client.clone())?;
        domain
            .services()
            .configure_cell_limits(config.cell_limits)?;
        domain.services().configure_loader(config.loader)?;
        domain
            .services()
            .configure_peer_probe(services.probe_signer.clone())?;
        domain
            .services()
            .configure_containers(services.containers.clone().unwrap_or_else(|| {
                crate::container::EngineOptions {
                    socket: PathBuf::from("/var/run/docker.sock"),
                    namespace: None,
                    runtime: None,
                    dns: Vec::new(),
                }
            }))?;
        if let Some(metrics) = services.metrics.clone() {
            domain.services().configure_metrics(metrics)?;
        }
        let (ready, readiness) = oneshot::channel();
        let (command, mut commands) = mpsc::unbounded_channel();
        let owner = domain.clone();
        let executor = services.host_executor.clone();
        let task_guard = CloseOnDrop(Some(domain.clone()));
        let supervisor = executor.spawn(async move {
            let _guard = task_guard;
            let _directory = directory;
            let boot = std::panic::AssertUnwindSafe(owner.run(async {
                let startup_timeout = config.startup.bootstrap_timeout;
                let (prepared, info) = tokio::time::timeout(
                    startup_timeout,
                    prepare(&owner, config, services, graph, applications),
                )
                .await
                .context("node bootstrap deadline elapsed")??;
                if let Some(bucket) = prepared.fleet_bucket.clone() {
                    super::operations::NodeOperations::install(
                        bucket,
                        prepared.clean_reload_node.clone(),
                        prepared.process_generation.clone(),
                    )?;
                }
                let app = prepared.app.clone();
                if ready.send((app, info)).is_err() {
                    return Ok(NodeOutcome::Cancelled);
                }
                driver::run(prepared, async {
                    commands.recv().await.unwrap_or(ShutdownMode::Handoff)
                })
                .await
            }))
            .catch_unwind()
            .await;
            // The entered root is gone before joining; joining inside it would
            // wait for the supervisor's own root and never complete.
            owner.shutdown().await;
            let outcome = match boot {
                Ok(Ok(result)) => result,
                Ok(Err(_)) => Ok(NodeOutcome::Cancelled),
                Err(panic) => Err(anyhow::anyhow!(
                    "node supervisor panicked: {}",
                    panic_message(panic)
                )),
            };
            if let Some(operations) = owner.services().node_operations() {
                operations
                    .joined(&outcome, tokio::runtime::Handle::current())
                    .await;
            }
            outcome
        });
        let (app, info) = match readiness.await {
            Ok(ready) => ready,
            Err(_) => {
                return match supervisor.await.context("node bootstrap task failed")? {
                    Err(error) => Err(error),
                    Ok(outcome) => Err(anyhow::anyhow!(
                        "node startup ended before readiness: {outcome:?}"
                    )),
                }
            }
        };
        let generation = app
            .runtime
            .as_ref()
            .expect("embedded node has a runtime")
            .generation();
        let supervisor =
            wait_for_event_activation(&generation.event_readiness, &domain, supervisor).await?;
        // Transfer domain ownership to the handle; do not close it when the
        // successful startup guard is retired.
        startup_guard.0 = None;
        Ok(Self {
            domain,
            raft_auth,
            app,
            info,
            command,
            supervisor,
            generation_max_age,
            completion: None,
        })
    }

    pub fn info(&self) -> &NodeInfo {
        &self.info
    }

    /// Start registered Raft cells on this node's executor. Namespace roots and
    /// permanent member claims precede voting; the node owns command producers
    /// and joins SQLite work even when client handles or waiters survive.
    pub async fn start_raft_cells(
        &self,
        config: crate::application::consensus::RaftCellsConfig,
        services: crate::application::consensus::RaftCellsServices,
    ) -> anyhow::Result<crate::application::consensus::RaftCells> {
        anyhow::ensure!(
            !self.is_closed() && !self.app.is_draining(),
            "node is closing"
        );
        self.domain
            .run(async {
                super::raft_startup::Startup {
                    domain: self
                        .domain
                        .services()
                        .domain_token()
                        .ok_or(crate::asyncrt::DomainClosed)?,
                    data_dir: &self.info.data_dir,
                    node: &self.info.node,
                    auth: self.raft_auth.clone(),
                }
                .start(config, services)
                .await
            })
            .await?
    }

    /// Start opt-in verification infrastructure. Recovered groups remain parked;
    /// callers own dispatch and must reconcile catalog claims before attachment.
    #[doc(hidden)]
    pub async fn start_raft_service(
        &self,
        config: super::NodeRaftConfig,
        listeners: super::NodeRaftListeners,
        tls: crate::application::consensus::verification::RaftTls,
    ) -> anyhow::Result<super::NodeRaftService> {
        super::raft_startup::Startup {
            domain: self
                .domain
                .services()
                .domain_token()
                .ok_or(crate::asyncrt::DomainClosed)?,
            data_dir: &self.info.data_dir,
            node: &self.info.node,
            auth: self.raft_auth.clone(),
        }
        .infrastructure(config, listeners, tls)
        .await
    }

    pub fn services(&self) -> Arc<HostServices> {
        self.domain.services()
    }

    pub fn is_closed(&self) -> bool {
        self.domain.is_closed()
    }

    pub async fn is_ready(&self) -> anyhow::Result<bool> {
        Ok(self
            .domain
            .run(async {
                self.app
                    .runtime
                    .as_ref()
                    .is_some_and(|runtime| runtime.generation().event_readiness.check().is_ok())
                    && self.app.fleet_ready()
                    && !self.app.is_draining()
                    && self.app.healthy().await
            })
            .await?)
    }

    /// Build before adoption. A failed application factory leaves the current
    /// generation serving. The replacement uses the captured generation budget.
    pub async fn adopt(
        &mut self,
        graph: DeploymentGraph,
        applications: CompiledApplications,
    ) -> anyhow::Result<u64> {
        anyhow::ensure!(!self.app.is_draining(), "node is draining");
        let runtime = self
            .app
            .runtime
            .as_ref()
            .expect("embedded nodes have a runtime")
            .clone();
        let app = self.app.clone();
        let node = self.info.node.clone();
        let region = runtime.region.to_string();
        let max_age_ms = duration_ms("generation maximum age", self.generation_max_age)?;
        self.domain
            .run(async move {
                let id = runtime.next_generation_id();
                let generation = build_generation(
                    Backend::Native(Arc::new(applications)),
                    id,
                    graph,
                    GenerationOptions { node, region },
                )
                .await?;
                event_resources::activate(app.clone(), &generation).await?;
                // The candidate is retained through activation and moved only
                // after its catalog and binding proofs pass.
                let adopted = runtime.adopt(generation);
                app.tx.send(Message::GenerationChanged {
                    generation: adopted.id(),
                    max_age_ms,
                    eager_classes: adopted.reserved_classes(),
                })?;
                spawn_cron_arm(app);
                Ok(adopted.id())
            })
            .await?
    }

    /// Supervise until the application requests a stop or the node returns a
    /// failure. Cancelling this future forces closure through the owning handle.
    pub async fn run_until<F>(mut self, stop: F) -> anyhow::Result<NodeOutcome>
    where
        F: std::future::Future<Output = ShutdownMode>,
    {
        let next =
            match futures_util::future::select(Box::pin(self.finished()), Box::pin(stop)).await {
                futures_util::future::Either::Left((outcome, _)) => Ok(outcome),
                futures_util::future::Either::Right((mode, pending)) => {
                    drop(pending);
                    Err(mode)
                }
            };
        match next {
            Ok(outcome) => outcome,
            Err(mode) => self.shutdown(mode).await,
        }
    }

    /// Stop admitting work, request the bounded production drain, then close
    /// and join all remaining domain-owned work without stopping either executor.
    pub async fn shutdown(mut self, mode: ShutdownMode) -> anyhow::Result<NodeOutcome> {
        let _ = self.command.send(mode);
        self.finished().await
    }

    /// Force closure without claiming a graceful durability proof.
    pub async fn cancel(mut self) -> anyhow::Result<NodeOutcome> {
        self.domain.close();
        self.finished().await
    }

    /// Observe fencing or a component failure and join the node's domain.
    pub async fn finished(&mut self) -> anyhow::Result<NodeOutcome> {
        if let Some(completion) = &self.completion {
            return completion.clone().map_err(anyhow::Error::msg);
        }
        let result = (&mut self.supervisor)
            .await
            .context("node supervisor task failed")
            .and_then(|result| result);
        self.completion = Some(
            result
                .as_ref()
                .map(Clone::clone)
                .map_err(|error| format!("{error:#}")),
        );
        result
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.domain.close();
    }
}

fn panic_message(panic: Box<dyn std::any::Any + Send>) -> String {
    panic
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| panic.downcast_ref::<&str>().map(|s| (*s).to_string()))
        .unwrap_or_else(|| "non-string panic".into())
}

async fn prepare(
    domain: &ExecutionDomain,
    config: NodeConfig,
    services: StartupServices,
    graph: DeploymentGraph,
    applications: CompiledApplications,
) -> anyhow::Result<(driver::PreparedDriver, NodeInfo)> {
    let StartupServices {
        core_executor,
        public_address,
        listener,
        external_ingress,
        internal_listener,
        peer_http,
        peer_auth,
        probe_signer,
        fleet,
        ..
    } = services;
    let node = config.identity.node.clone();
    let region = config.identity.region.clone();
    let (tx, rx) = mpsc::unbounded_channel();
    let alarm_tx = tx.clone();
    let alarm_observer: crate::runtime::AlarmObserver = Arc::new(move |cell, alarm| {
        let _ = alarm_tx.send(Message::AlarmObserved {
            cell,
            alarm,
            covered: false,
        });
    });
    let fleet_sample_ms = duration_ms(
        "fleet sample period",
        config.maintenance.fleet_sample_period,
    )?;
    let (replication, ownership, wake, bucket) = if let Some(fleet) = &fleet {
        crate::fleet::validate_bucket(&fleet.bucket).await?;
        crate::wake_format::ensure_ready(&fleet.bucket).await?;
        // A violated CAS/range contract is fatal. The supplied clients retain
        // their retry policy and provider; the engine does not create a pool.
        match fleet.bucket.probe_startup_storage_steps().await? {
            crate::bucket::StorageProbeVerdict::Conformant => {}
            crate::bucket::StorageProbeVerdict::Violation(reason) => {
                anyhow::bail!("fleet storage contract is violated: {reason}")
            }
        }
        let ownership = Arc::new(
            BucketOwnership::new(
                fleet.bucket.clone(),
                fleet.lease_bucket.clone(),
                node.clone(),
                probe_signer.public_key_hex(),
            )
            .with_lease_ttl_ms(config.actor.lease_ttl_ms)
            .with_fleet_sample_ms(fleet_sample_ms),
        );
        let wake = Arc::new(crate::wake::WakeFlusher::new());
        crate::wake_entry::set_arm_gate(ArmGate {
            bucket: fleet.bucket.clone(),
            flusher: wake.clone(),
        });
        crate::host_channels::set_blob_store(fleet.bucket.clone());
        crate::host_channels::set_kv_blob_store(fleet.bucket.clone());
        let replication = Replication::start(
            &config.data_dir,
            fleet.replica_storage.clone(),
            fleet.replication.clone(),
        )?;
        (
            Some(replication),
            Some(ownership),
            Some(wake),
            Some(fleet.bucket.clone()),
        )
    } else {
        (None, None, None, None)
    };
    let generation_options = GenerationOptions {
        node: node.clone(),
        region: region.clone(),
    };
    let generation = build_generation(
        Backend::Native(Arc::new(applications)),
        FIRST_GENERATION,
        graph,
        generation_options,
    )
    .await?;
    generation.begin_event_activation();
    let runtime = RuntimeManager::start(
        generation,
        RuntimeOptions {
            data_dir: config.data_dir.clone(),
            replication,
            wake,
            alarm_observer,
            node: node.clone(),
            region,
            bucket,
        },
    )?;
    let (fence_tx, fence_rx) = mpsc::unbounded_channel();
    let drain_pins = DrainPinRegistry::default();
    let actor = Actor::new(
        config.actor.clone(),
        fence_tx,
        ActorServices {
            runtime: Some(runtime.clone()),
            drain_pins: drain_pins.clone(),
            ownership: ownership.clone().map(Ownership::Bucket),
            peer_http: peer_http.clone(),
            peer_auth: peer_auth.clone(),
        },
        config.identity,
    )
    .await?;
    let (reload, reload_rx) = crate::generation::reload_channel();
    // Deployment configuration belongs to the application; Node::adopt is the
    // explicit API. No hidden pointer watcher or process control-plane agent.
    drop(reload_rx);
    let (websockets, websocket_rx) = mpsc::unbounded_channel();
    let follower = ownership.as_ref().map(|ownership| {
        Arc::new(crate::node_log::FollowerStore::new(
            &config.data_dir,
            Some(Arc::new(ownership.bucket_client())),
            &node,
        ))
    });
    let app = AppHandle {
        tx,
        runtime: Some(runtime.clone()),
        reload,
        peer_http,
        peer_auth,
        advertise: actor.lease_spec.addr.clone(),
        websockets,
        draining: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        fleet_ready: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        public_in_flight: Arc::new(AtomicUsize::new(0)),
        drain_pins,
        trust_forwarded_headers: config.trust_forwarded_headers,
        max_outbound_websockets: config.actor.core.max_outbound_websockets,
        max_request_body_bytes: config.max_request_body_bytes,
        operation_deadline_ms: config.actor.core.operation_deadline_ms.unwrap(),
        follower: follower.clone(),
    };
    let durability_owner = match (&fleet, &ownership) {
        (Some(fleet), Some(ownership)) => {
            let nudge = app.tx.clone();
            let own_log = Arc::new(OwnLog {
                ownership: ownership.clone(),
                nudge: Box::new(move || {
                    let _ = nudge.send(Message::NudgeNodeLease);
                }),
                write_lock: tokio::sync::Mutex::new(()),
            });
            let manager = Arc::new(NodeLogManager::new(
                &ownership.log_session()?,
                Arc::new(fleet.bucket.clone()),
                own_log,
                runtime.replication().unwrap().ltx(),
                fleet.log_transport.clone(),
                fleet.log.clone(),
            )?);
            *actor.node_log.lock().unwrap() = Some(manager.clone());
            let recovery = async {
                for attempt in 1..=config.startup.recovery_attempts {
                    let recovered = tokio::time::timeout(
                        config.startup.recovery_attempt_timeout,
                        manager.recover_self(),
                    )
                    .await
                    .unwrap_or_else(|_| Err(anyhow::anyhow!("predecessor recovery timed out")));
                    match recovered {
                        Ok(()) => return Ok(()),
                        Err(error) if attempt == config.startup.recovery_attempts => anyhow::bail!("refusing to install a lease over an unrecovered predecessor log: {error:#}"),
                        Err(error) => {
                            tracing::warn!(attempt, error = format!("{error:#}"), "predecessor recovery failed; retrying before lease installation");
                            manager.wait_for_startup_retry(
                                &error,
                                config.startup.recovery_retry_backoff.saturating_mul(attempt),
                            ).await;
                        }
                    }
                }
                unreachable!("recovery policy requires a positive attempt count")
            };
            recover_with_follower_listener(&internal_listener, app.clone(), recovery).await?;
            let mut owner = DurabilityOwner::new(manager, fleet.fleet_durability);
            owner.start_background(follower.clone());
            Some(owner)
        }
        _ => None,
    };
    let channels = driver::Channels::install(websocket_rx);
    let sample = sample_load().await?;
    actor.store_load_sample(sample);
    let (actor_exit_tx, actor_exit_rx) = mpsc::unbounded_channel();
    let actor_task = actor.spawn(domain, &core_executor, rx)?;
    crate::asyncrt::spawn(async move {
        let result = match actor_task.await {
            Ok(result) => result.map_err(|e| e.to_string()),
            Err(error) => Err(error.to_string()),
        };
        let _ = actor_exit_tx.send(result);
    })
    .detach();
    start_maintenance(
        app.clone(),
        ownership.clone(),
        runtime.replication(),
        &node,
        &config.maintenance,
        config.actor.core.max_activations as u64,
    )?;
    let authority_deadline = tokio::time::Instant::now()
        .checked_add(config.startup.authority_timeout)
        .context("authority timeout exceeds host clock")?;
    while !app.healthy().await {
        anyhow::ensure!(
            tokio::time::Instant::now() < authority_deadline,
            "node authority was not established before startup deadline"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    if let Some(ownership) = &ownership {
        super::wake_discovery::start(
            &app,
            ownership.bucket_client(),
            ownership.clone(),
            &node,
            duration_ms("wake period", config.maintenance.wake_period)?,
        )?;
    }
    spawn_cron_arm(app.clone());
    let info = NodeInfo {
        node: node.clone(),
        advertise: app.advertise.clone(),
        public_address,
        internal_address: internal_listener.local_addr()?,
        lease_generation: config.actor.generation.clone(),
        data_dir: config.data_dir.clone(),
    };
    let prepared = driver::PreparedDriver {
        app,
        listener,
        external_ingress,
        internal_listener,
        channels,
        fence_rx,
        actor_exit_rx,
        ready_ownership: ownership.clone(),
        fleet_bucket: ownership.map(|o| o.bucket_client()),
        clean_reload_node: node,
        process_generation: config.actor.generation,
        data_dir: config.data_dir,
        durability_owner,
        policy: driver::RunPolicy {
            shutdown: config.shutdown,
            local_cache_max_bytes: config.maintenance.local_cache_max_bytes,
            local_cache_prune_period: config.maintenance.local_cache_prune_period,
        },
    };
    Ok((prepared, info))
}

fn start_maintenance(
    app: AppHandle,
    ownership: Option<Arc<BucketOwnership>>,
    replication: Option<Replication>,
    node: &str,
    policy: &NodeMaintenancePolicy,
    max_restoring: u64,
) -> anyhow::Result<()> {
    let sample_period = policy.load_sample_period;
    let tx = app.tx.clone();
    crate::asyncrt::spawn(async move {
        let mut tick = crate::asyncrt::interval(sample_period);
        tick.set_missed_tick_behavior(crate::asyncrt::MissedTickBehavior::Delay);
        tick.tick().await;
        loop {
            tick.tick().await;
            let sample = match sample_load().await {
                Ok(sample) => sample,
                Err(error)
                    if error
                        .downcast_ref::<crate::asyncrt::DomainClosed>()
                        .is_some() =>
                {
                    return
                }
                Err(error) => {
                    tracing::warn!(%error, "node metrics sampling failed");
                    continue;
                }
            };
            if tx.send(Message::SampleLoad(sample)).is_err() {
                return;
            }
        }
    })
    .detach();
    let Some(ownership) = ownership else {
        app.fleet_ready
            .store(true, std::sync::atomic::Ordering::SeqCst);
        return Ok(());
    };
    if let Some(observation_deadline) = policy.readiness_observation_deadline {
        let ownership = ownership.clone();
        let app = app.clone();
        let node = node.to_string();
        crate::asyncrt::spawn(async move {
            let started = tokio::time::Instant::now();
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            let mut reported = false;
            loop {
                tick.tick().await;
                let bucket = ownership.bucket_client();
                let (token, peers) = tokio::join!(crate::drain_token::read(&bucket), ownership.read_capacity_peers());
                let now_ms = now_ms();
                let settled = match (&token, &peers) { (Ok(token), Ok(peers)) => cellar_logic::drain::fleet_status(token.as_ref().and_then(|t| t.token.as_ref()), &node, now_ms, peers, max_restoring), _ => Err(cellar_logic::drain::FleetUnsettled::Unreadable) };
                if settled.is_ok() { app.fleet_ready.store(true, std::sync::atomic::Ordering::SeqCst); return; }
                if !reported && started.elapsed() >= observation_deadline { reported = true; tracing::warn!(reason = ?settled.err(), "fleet readiness observation deadline elapsed; readiness remains closed"); }
            }
        }).detach();
    } else {
        app.fleet_ready
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
    let app = app.clone();
    let node = node.to_string();
    let period = policy.fleet_sample_period;
    let rebalance = policy.rebalance;
    let sample_ms = duration_ms("fleet sample period", period)?;
    crate::asyncrt::spawn(async move {
        let mut tick = tokio::time::interval(period);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut settled_after_ms = 0;
        loop {
            tick.tick().await;
            let now_ms = now_ms();
            let (view_ms, peers) = ownership
                .read_shared_capacity_peers(sample_ms)
                .await
                .unwrap_or_else(|error| {
                    tracing::info!(%error, "fleet sample failed");
                    (now_ms, Vec::new())
                });
            if let Some(replication) = &replication {
                replication.set_paged_fleet(cellar_logic::format::fleet_reads(
                    &peers,
                    view_ms,
                    cellar_logic::format::BUCKET_FORMAT,
                ));
            }
            if !rebalance
                || !app.fleet_ready()
                || app.is_draining()
                || ownership
                    .live()
                    .rebalance_paused
                    .load(std::sync::atomic::Ordering::Relaxed)
            {
                continue;
            }
            let status = app.drain_status().await;
            let in_flight = status.quiescing + status.evicting + status.releasing + status.adopting;
            if in_flight >= DEFAULT_REBALANCE_BATCH_CELLS {
                continue;
            }
            if let Some(cells) = cellar_logic::rebalance::surplus(
                &peers,
                &node,
                view_ms,
                ownership.lease_ttl_ms(),
                settled_after_ms,
                DEFAULT_REBALANCE_BATCH_CELLS - in_flight,
            ) {
                if app.tx.send(Message::Rebalance { cells }).is_err() {
                    return;
                }
                settled_after_ms = now_ms + sample_ms;
            }
        }
    })
    .detach();
    Ok(())
}
