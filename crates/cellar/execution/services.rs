// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Modified for Cellar.

//! Host services owned by one execution domain.
//!
//! The process runner installs its service set once. Library execution domains
//! and deterministic domains each install their own set, so routing authority
//! and resource registries follow their owner.

pub use crate::outbound_rpc::Usage as OutboundRpcUsage;
use crate::ownership_store::LiveLoad;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

/// Complete application event lifetimes, including post-response background
/// work. Maintenance tasks and idle resident cells do not hold these counts.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ApplicationWorkSnapshot {
    /// Event drivers whose full lifetime has not ended, even if their handler
    /// already replied. Counts belong to this host across its generations.
    pub pending_events: usize,
    /// Events which have ended, including failure or cancellation. This is a
    /// progress counter; it does not by itself assert a durability proof.
    pub retired_events: u64,
}

#[derive(Default)]
pub(crate) struct ApplicationWork {
    pending: AtomicUsize,
    retired: AtomicU64,
}

impl ApplicationWork {
    pub(crate) fn enter(self: &Arc<Self>) -> ApplicationWorkGuard {
        self.pending.fetch_add(1, Ordering::SeqCst);
        ApplicationWorkGuard(self.clone())
    }

    pub(crate) fn snapshot(&self) -> ApplicationWorkSnapshot {
        ApplicationWorkSnapshot {
            pending_events: self.pending.load(Ordering::SeqCst),
            retired_events: self.retired.load(Ordering::SeqCst),
        }
    }
}

pub(crate) struct ApplicationWorkGuard(Arc<ApplicationWork>);

impl Drop for ApplicationWorkGuard {
    fn drop(&mut self) {
        self.0.retired.fetch_add(1, Ordering::SeqCst);
        self.0.pending.fetch_sub(1, Ordering::SeqCst);
    }
}

/// One resource observation shared by pressure decisions and node leases.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HostMetricsSample {
    pub cpu_percent_x100: u64,
    pub rss_bytes: u64,
    pub in_use_bytes: u64,
    pub cgroup_working_set_bytes: Option<u64>,
    pub cgroup_current_bytes: Option<u64>,
    pub open_fds: u64,
    pub fd_limit: u64,
}

/// Caller-owned observation source for one embedded node.
pub type MetricsSampler = dyn Fn() -> HostMetricsSample + Send + Sync;

enum MetricsBackend {
    Production(Mutex<ProcessLoadSampler>),
    #[cfg(cellar_internal_tests)]
    Scripted(Mutex<HostMetricsSample>),
}

/// Services and resource registries owned by one execution domain.
pub struct HostServices {
    domain: OnceLock<crate::asyncrt::DomainToken>,
    application_work: Arc<ApplicationWork>,
    channels: crate::host_channels::HostChannels,
    gates: Arc<crate::host_gates::GateService>,
    tcp: Arc<crate::host_tcp::TcpService>,
    loader: Arc<dyn crate::host_loader::LoaderService>,
    facets: Arc<dyn crate::host_facets::FacetService>,
    containers: Arc<crate::container::ContainerService>,
    node_load: OnceLock<Arc<LiveLoad>>,
    node_operations: OnceLock<Arc<crate::daemon::operations::NodeOperations>>,
    raft_lifecycle: Arc<crate::raft::operations::NodeLifecycle>,
    peer_probe: OnceLock<Arc<crate::peer_probe::PeerProbeSigner>>,
    metrics_override: OnceLock<Option<Arc<MetricsSampler>>>,
    actor_options: OnceLock<crate::actor::ActorOptions>,
    operation_deadline_ms: OnceLock<u64>,
    runtime_policy: OnceLock<crate::runtime_policy::RuntimePolicy>,
    outbound_rpc: OnceLock<crate::outbound_rpc::Budget>,
    http: OnceLock<crate::http_policy::HttpService>,
    cell_limits: OnceLock<crate::application::limits::CellLimits>,
    events: Arc<crate::application::event_stream::service::Service>,
    wake_entry: crate::wake_entry::WakeEntryService,
    websockets: crate::ws_registry::WebSocketService,
    http_streams: Arc<crate::http_streams::HttpStreamService>,
    metrics: MetricsBackend,
    #[cfg(all(test, cellar_internal_tests))]
    handoff_transport: Mutex<Option<Arc<dyn crate::actor::HandoffTransportForTest>>>,
}

impl HostServices {
    pub(crate) fn production() -> Self {
        Self {
            domain: OnceLock::new(),
            application_work: Arc::new(ApplicationWork::default()),
            channels: crate::host_channels::HostChannels::default(),
            gates: Arc::new(crate::host_gates::GateService::default()),
            tcp: Arc::new(crate::host_tcp::TcpService::default()),
            loader: Arc::new(crate::host_loader::LoaderRegistry::default()),
            facets: Arc::new(crate::host_facets::FacetRegistry::default()),
            containers: Arc::new(crate::container::ContainerService::default()),
            node_load: OnceLock::new(),
            node_operations: OnceLock::new(),
            raft_lifecycle: Arc::new(crate::raft::operations::NodeLifecycle::default()),
            peer_probe: OnceLock::new(),
            metrics_override: OnceLock::new(),
            actor_options: OnceLock::new(),
            operation_deadline_ms: OnceLock::new(),
            runtime_policy: OnceLock::new(),
            outbound_rpc: OnceLock::new(),
            http: OnceLock::new(),
            cell_limits: OnceLock::new(),
            events: Arc::new(crate::application::event_stream::service::Service::default()),
            wake_entry: crate::wake_entry::WakeEntryService::default(),
            websockets: crate::ws_registry::WebSocketService::default(),
            http_streams: Arc::new(crate::http_streams::HttpStreamService::default()),
            metrics: MetricsBackend::Production(Mutex::new(ProcessLoadSampler::default())),
            #[cfg(all(test, cellar_internal_tests))]
            handoff_transport: Mutex::new(None),
        }
    }

    #[cfg(cellar_internal_tests)]
    pub fn scripted() -> Self {
        Self {
            domain: OnceLock::new(),
            application_work: Arc::new(ApplicationWork::default()),
            channels: crate::host_channels::HostChannels::default(),
            gates: Arc::new(crate::host_gates::GateService::default()),
            tcp: Arc::new(crate::host_tcp::TcpService::default()),
            loader: Arc::new(crate::host_loader::LoaderRegistry::default()),
            facets: Arc::new(crate::host_facets::FacetRegistry::default()),
            containers: Arc::new(crate::container::ContainerService::default()),
            node_load: OnceLock::new(),
            node_operations: OnceLock::new(),
            raft_lifecycle: Arc::new(crate::raft::operations::NodeLifecycle::default()),
            peer_probe: OnceLock::new(),
            metrics_override: OnceLock::new(),
            actor_options: OnceLock::new(),
            operation_deadline_ms: OnceLock::new(),
            runtime_policy: OnceLock::new(),
            outbound_rpc: OnceLock::new(),
            http: OnceLock::new(),
            cell_limits: OnceLock::new(),
            events: Arc::new(crate::application::event_stream::service::Service::default()),
            wake_entry: crate::wake_entry::WakeEntryService::default(),
            websockets: crate::ws_registry::WebSocketService::default(),
            http_streams: Arc::new(crate::http_streams::HttpStreamService::default()),
            metrics: MetricsBackend::Scripted(Mutex::new(HostMetricsSample::default())),
            #[cfg(all(test, cellar_internal_tests))]
            handoff_transport: Mutex::new(None),
        }
    }

    /// Observe this host's complete event drivers across all generations,
    /// services, cell pools, loaded workers and facets.
    pub fn application_work(&self) -> ApplicationWorkSnapshot {
        self.application_work.snapshot()
    }

    /// Fixed-cardinality event observations for this host across generations.
    /// These counters are not a backlog census or durability receipt ledger.
    pub fn event_metrics(&self) -> crate::event_metrics::EventMetricsSnapshot {
        self.events.metrics_snapshot()
    }

    pub(crate) fn application_work_registry(&self) -> Arc<ApplicationWork> {
        self.application_work.clone()
    }

    pub(crate) fn configure_node_operations(
        &self,
        operations: Arc<crate::daemon::operations::NodeOperations>,
    ) -> anyhow::Result<()> {
        self.node_operations
            .set(operations)
            .map_err(|_| anyhow::anyhow!("node operation reporter is already configured"))
    }

    pub(crate) fn node_operations(&self) -> Option<Arc<crate::daemon::operations::NodeOperations>> {
        self.node_operations.get().cloned()
    }

    pub(crate) fn raft_lifecycle(&self) -> Arc<crate::raft::operations::NodeLifecycle> {
        self.raft_lifecycle.clone()
    }

    pub(crate) fn channels(&self) -> &crate::host_channels::HostChannels {
        &self.channels
    }

    pub(crate) fn gates(&self) -> Arc<crate::host_gates::GateService> {
        self.gates.clone()
    }

    pub(crate) fn events(&self) -> Arc<crate::application::event_stream::service::Service> {
        self.events.clone()
    }

    pub(crate) fn tcp(&self) -> Arc<crate::host_tcp::TcpService> {
        self.tcp.clone()
    }

    pub(crate) fn loader(&self) -> Arc<dyn crate::host_loader::LoaderService> {
        self.loader.clone()
    }

    pub(crate) fn facets(&self) -> Arc<dyn crate::host_facets::FacetService> {
        self.facets.clone()
    }

    pub(crate) fn containers(&self) -> Arc<crate::container::ContainerService> {
        self.containers.clone()
    }

    /// Select this host's container infrastructure before starting its runtime.
    /// Hosts sharing a daemon must use distinct namespaces; the same namespace
    /// identifies resources to recover after a restart.
    pub fn configure_containers(
        &self,
        options: crate::container::EngineOptions,
    ) -> anyhow::Result<()> {
        self.containers.configure_options(options)
    }

    /// Install this node's policy before building application generations.
    /// SDK queue settlement and actor effects share its operation deadline.
    /// Once a generation has used a deadline it cannot be changed underneath
    /// queued work. Repeating an identical configuration is harmless.
    pub fn configure_actor(&self, options: &crate::actor::ActorOptions) -> anyhow::Result<()> {
        anyhow::ensure!(self.accepts_work(), "actor execution domain is closed");
        options.validate_policy()?;
        if let Some(existing) = self.actor_options.get() {
            anyhow::ensure!(
                existing == options,
                "actor policy is already configured for this host"
            );
            return Ok(());
        }
        self.pin_operation_deadline(options.core.operation_deadline_ms.unwrap())?;
        if self.actor_options.set(options.clone()).is_err() {
            anyhow::ensure!(
                self.actor_options.get() == Some(options),
                "actor policy is already configured for this host"
            );
        }
        Ok(())
    }

    pub(crate) fn actor_process_generation(&self) -> Option<&str> {
        self.actor_options
            .get()
            .map(|options| options.generation.as_str())
    }

    /// Install this host's signing key; another host may use a different key.
    pub fn configure_peer_probe(
        &self,
        signer: Arc<crate::peer_probe::PeerProbeSigner>,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(self.accepts_work(), "peer probe execution domain is closed");
        if let Some(existing) = self.peer_probe.get() {
            anyhow::ensure!(
                Arc::ptr_eq(existing, &signer),
                "peer probe signer is already configured for this host"
            );
            return Ok(());
        }
        if let Err(candidate) = self.peer_probe.set(signer) {
            anyhow::ensure!(
                self.peer_probe
                    .get()
                    .is_some_and(|existing| Arc::ptr_eq(existing, &candidate)),
                "peer probe signer is already configured for this host"
            );
        }
        Ok(())
    }

    pub(crate) fn peer_probe_signer(
        &self,
    ) -> anyhow::Result<Arc<crate::peer_probe::PeerProbeSigner>> {
        anyhow::ensure!(self.accepts_work(), "peer probe execution domain is closed");
        self.peer_probe
            .get()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("peer probe signer is not configured for this host"))
    }

    /// Pin the application's metrics source before this node starts sampling.
    pub fn configure_metrics(&self, sampler: Arc<MetricsSampler>) -> anyhow::Result<()> {
        anyhow::ensure!(self.accepts_work(), "metrics execution domain is closed");
        if let Some(existing) = self.metrics_override.get() {
            anyhow::ensure!(
                existing
                    .as_ref()
                    .is_some_and(|existing| Arc::ptr_eq(existing, &sampler)),
                "metrics source is already configured for this host"
            );
            return Ok(());
        }
        if let Err(candidate) = self.metrics_override.set(Some(sampler)) {
            anyhow::ensure!(
                self.metrics_override
                    .get()
                    .and_then(Option::as_ref)
                    .zip(candidate.as_ref())
                    .is_some_and(|(existing, candidate)| Arc::ptr_eq(existing, candidate)),
                "metrics source is already configured for this host"
            );
        }
        Ok(())
    }

    /// Fix loader admission and copied-value budgets before building a generation.
    /// Repeating the same policy is harmless; a live policy cannot be replaced.
    pub fn configure_loader(
        &self,
        policy: crate::loader_policy::LoaderPolicy,
    ) -> anyhow::Result<()> {
        self.loader.configure_policy(policy)
    }

    /// This host's immutable loader policy, including environment-free defaults.
    pub fn loader_policy(&self) -> crate::loader_policy::LoaderPolicy {
        self.loader.policy()
    }

    /// Fix this host's runtime limits before building application generations.
    /// Reading the policy or building a pool pins the library defaults if no
    /// policy was supplied. A different policy cannot replace a live one.
    pub fn configure_runtime(
        &self,
        policy: crate::runtime_policy::RuntimePolicy,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(self.accepts_work(), "runtime execution domain is closed");
        policy.validate()?;
        if self.runtime_policy.set(policy).is_err() {
            anyhow::ensure!(self.runtime_policy.get() == Some(&policy),
                "runtime policy is already in use; configure runtime policy before building the generation");
        }
        Ok(())
    }

    /// This host's immutable runtime policy, including library defaults when
    /// the application did not configure it. No environment is read here.
    pub fn runtime_policy(&self) -> crate::runtime_policy::RuntimePolicy {
        *self.runtime_policy.get_or_init(Default::default)
    }

    pub(crate) fn reserve_outbound_rpc(
        &self,
        scope: &str,
        name: Option<&str>,
        method: &str,
        args: &crate::engine_api::RpcData,
    ) -> Option<crate::host_channels::RpcReservation> {
        if !self.accepts_work() {
            return None;
        }
        self.outbound_rpc
            .get_or_init(|| crate::outbound_rpc::Budget::new(self.runtime_policy()))
            .reserve(scope, name, method, args)
    }

    /// SDK submission reservations, sampled independently. Counts include
    /// gate/queue/routing waits, retained local native drivers and undelivered
    /// replies. Bytes are a length-weighted request allowance, not measured
    /// allocations, guest heaps, responses or receiving-peer transport buffers.
    pub fn outbound_rpc_usage(&self) -> OutboundRpcUsage {
        self.outbound_rpc
            .get()
            .map_or_else(Default::default, crate::outbound_rpc::Budget::usage)
    }

    /// Fix outbound HTTP configuration before the first fetch. Factory identity
    /// and deadline remain pinned across application generations and workers.
    pub fn configure_http(
        &self,
        policy: crate::http_policy::HttpPolicy,
        factory: Option<crate::http_policy::HttpClientFactory>,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(self.accepts_work(), "HTTP execution domain is closed");
        policy.validate()?;
        if let Err(candidate) = self
            .http
            .set(crate::http_policy::HttpService::new(policy, factory))
        {
            anyhow::ensure!(
                self.http
                    .get()
                    .unwrap()
                    .matches(candidate.policy, candidate.factory()),
                "HTTP policy is already in use; configure HTTP before the first fetch"
            );
        }
        if !self.accepts_work() {
            self.http.get().unwrap().close();
            anyhow::bail!("HTTP execution domain is closed");
        }
        Ok(())
    }

    pub(crate) fn http(&self) -> &crate::http_policy::HttpService {
        let http = self.http.get_or_init(Default::default);
        // Closure can race OnceLock publication, when close_resources cannot
        // yet see the service. A retained host may never recreate open pools.
        if !self.accepts_work() {
            http.close();
        }
        http
    }

    /// Inspect this host's HTTP deadline. Reading it pins library defaults if
    /// no explicit HTTP configuration was installed.
    pub fn http_policy(&self) -> crate::http_policy::HttpPolicy {
        self.http().policy
    }

    /// Capture API budgets before constructing generations or opening storage.
    /// An identical repeated configuration is harmless; live budgets are fixed.
    pub fn configure_cell_limits(
        &self,
        limits: crate::application::limits::CellLimits,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(self.accepts_work(), "cell execution domain is closed");
        limits.validate()?;
        if self.cell_limits.set(limits).is_err() {
            anyhow::ensure!(
                self.cell_limits.get() == Some(&limits),
                "cell limits are already in use; configure them before building the generation"
            );
        }
        Ok(())
    }

    pub fn cell_limits(&self) -> crate::application::limits::CellLimits {
        *self.cell_limits.get_or_init(Default::default)
    }

    pub(crate) fn pin_operation_deadline(&self, deadline: u64) -> anyhow::Result<u64> {
        if self.operation_deadline_ms.set(deadline).is_err() {
            anyhow::ensure!(self.operation_deadline_ms.get() == Some(&deadline),
                "application generation and actor must use the same operation deadline; configure actor policy before building the generation");
        }
        Ok(deadline)
    }

    pub(crate) fn operation_deadline_ms(&self) -> anyhow::Result<u64> {
        if let Some(deadline) = self.operation_deadline_ms.get() {
            return Ok(*deadline);
        }
        self.pin_operation_deadline(crate::actor::DEFAULT_OPERATION_DEADLINE_MS)
    }

    pub(crate) fn pinned_operation_deadline_ms(&self) -> Option<u64> {
        self.operation_deadline_ms.get().copied()
    }

    pub(crate) fn close_resources(&self) {
        if let Some(http) = self.http.get() {
            http.close();
        }
        self.tcp.close();
        self.facets.close();
        self.loader.close();
        self.containers.close();
    }

    pub(crate) fn accepts_work(&self) -> bool {
        self.domain_token()
            .is_none_or(|domain| domain.mono_ms().is_ok())
    }

    pub fn set_node_load(&self, load: Arc<LiveLoad>) {
        let _ = self.node_load.set(load);
    }

    #[cfg(all(test, cellar_internal_tests))]
    pub(crate) fn set_handoff_transport_for_test(
        &self,
        transport: Arc<dyn crate::actor::HandoffTransportForTest>,
    ) {
        let mut slot = self.handoff_transport.lock().unwrap();
        assert!(slot.is_none(), "the handoff transport is already installed");
        *slot = Some(transport);
    }

    #[cfg(all(test, cellar_internal_tests))]
    pub(crate) fn handoff_transport_for_test(
        &self,
    ) -> Option<Arc<dyn crate::actor::HandoffTransportForTest>> {
        self.handoff_transport.lock().unwrap().clone()
    }

    pub(crate) fn bind_domain(&self, domain: crate::asyncrt::DomainToken) {
        if let Err(candidate) = self.domain.set(domain) {
            assert!(
                self.domain
                    .get()
                    .is_some_and(|bound| bound.same_owner(&candidate)),
                "HostServices cannot belong to more than one execution domain"
            );
        }
        self.loader.bind_domain(self.domain.get().unwrap().clone());
        self.facets.bind_domain(self.domain.get().unwrap().clone());
        self.containers
            .bind_domain(self.domain.get().unwrap().clone());
        self.http_streams.bind_domain(
            self.domain
                .get()
                .expect("a HostServices domain was just installed")
                .clone(),
        );
    }

    pub(crate) fn domain_token(&self) -> Option<crate::asyncrt::DomainToken> {
        self.domain.get().cloned()
    }

    pub fn node_load(&self) -> Option<Arc<LiveLoad>> {
        self.node_load.get().cloned()
    }

    pub(crate) fn wake_entry(&self) -> &crate::wake_entry::WakeEntryService {
        &self.wake_entry
    }

    pub(crate) fn websockets(&self) -> &crate::ws_registry::WebSocketService {
        &self.websockets
    }

    pub(crate) fn http_streams(&self) -> Arc<crate::http_streams::HttpStreamService> {
        if let Some(domain) = self.domain_token() {
            self.http_streams.bind_domain(domain);
        }
        self.http_streams.clone()
    }

    #[cfg(all(test, cellar_internal_tests))]
    pub(crate) fn quarantine_http_streams(&self) {
        self.http_streams.quarantine();
    }

    #[cfg(all(test, cellar_internal_tests))]
    pub(crate) fn close_http_streams(&self) {
        self.http_streams.close();
    }

    /// Collect off the core so filesystem reads cannot delay a lease.
    #[allow(clippy::disallowed_methods)] // `/proc` is host telemetry, not node storage.
    pub fn sample_metrics(&self) -> HostMetricsSample {
        if let Some(sample) = self.metrics_override.get_or_init(|| None) {
            return sample();
        }
        match &self.metrics {
            MetricsBackend::Production(sampler) => {
                let mut sampler = sampler.lock().unwrap();
                let memory = crate::memory::sample();
                #[cfg(target_os = "linux")]
                let open_fds = std::fs::read_dir("/proc/self/fd")
                    .map(|entries| entries.count() as u64)
                    .unwrap_or_default();
                #[cfg(not(target_os = "linux"))]
                let open_fds = 0;

                #[cfg(unix)]
                let fd_limit = {
                    let mut limit = libc::rlimit {
                        rlim_cur: 0,
                        rlim_max: 0,
                    };
                    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } == 0 {
                        limit.rlim_cur
                    } else {
                        0
                    }
                };
                #[cfg(not(unix))]
                let fd_limit = 0;

                HostMetricsSample {
                    cpu_percent_x100: sampler.sample_cpu_percent_x100(),
                    rss_bytes: memory.rss_bytes,
                    in_use_bytes: memory.in_use_bytes,
                    cgroup_working_set_bytes: memory.cgroup_working_set_bytes,
                    cgroup_current_bytes: memory.cgroup_current_bytes,
                    open_fds,
                    fd_limit,
                }
            }
            #[cfg(cellar_internal_tests)]
            MetricsBackend::Scripted(sample) => *sample.lock().unwrap(),
        }
    }

    #[cfg(cellar_internal_tests)]
    pub fn set_scripted_metrics(&self, sample: HostMetricsSample) {
        let MetricsBackend::Scripted(current) = &self.metrics else {
            panic!("scripted metrics require a simulation HostServices instance");
        };
        *current.lock().unwrap() = sample;
    }
}

#[derive(Default)]
struct ProcessLoadSampler {
    previous_cpu_ticks: Option<u64>,
    previous_sample: Option<std::time::Instant>,
}

impl ProcessLoadSampler {
    // This is the production metrics arm. A simulation HostServices returns a
    // scripted sample and never calls this host-clock sampler.
    #[allow(clippy::disallowed_methods)]
    fn sample_cpu_percent_x100(&mut self) -> u64 {
        let Some(ticks) = process_cpu_ticks() else {
            return 0;
        };
        let now = std::time::Instant::now();
        let value = match (self.previous_cpu_ticks, self.previous_sample) {
            (Some(previous_ticks), Some(previous_sample)) => {
                let elapsed = previous_sample.elapsed().as_secs_f64();
                let ticks_per_second = clock_ticks_per_second() as f64;
                if elapsed > 0.0 && ticks_per_second > 0.0 {
                    (((ticks.saturating_sub(previous_ticks)) as f64 / ticks_per_second / elapsed)
                        * 10_000.0) as u64
                } else {
                    0
                }
            }
            _ => 0,
        };
        self.previous_cpu_ticks = Some(ticks);
        self.previous_sample = Some(now);
        value
    }
}

#[cfg(target_os = "linux")]
#[allow(clippy::disallowed_methods)] // `/proc` is host telemetry, not node storage.
fn process_cpu_ticks() -> Option<u64> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    let fields = stat
        .get(stat.rfind(')')? + 2..)?
        .split_whitespace()
        .collect::<Vec<_>>();
    Some(fields.get(11)?.parse::<u64>().ok()? + fields.get(12)?.parse::<u64>().ok()?)
}

#[cfg(not(target_os = "linux"))]
fn process_cpu_ticks() -> Option<u64> {
    None
}

#[cfg(unix)]
fn clock_ticks_per_second() -> u64 {
    let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    u64::try_from(ticks)
        .ok()
        .filter(|ticks| *ticks > 0)
        .unwrap_or(100)
}

#[cfg(not(unix))]
fn clock_ticks_per_second() -> u64 {
    100
}

#[cfg(all(test, not(cellar_internal_tests)))]
mod runtime_policy_tests {
    use crate::asyncrt::ExecutionDomain;
    use crate::engine_api::WorkerConfigOptions;
    use crate::runtime_policy::RuntimePolicy;
    use crate::worker_config::WorkerConfig;
    use std::time::Duration;

    // A queue claim must outlive this host's admission, handler, and output
    // settlement windows; borrowing another host's budget can redeliver work
    // while its original consumer still runs.
    #[tokio::test]
    async fn queue_lease_uses_its_own_host_runtime_and_settlement_policy() -> anyhow::Result<()> {
        let config = WorkerConfig::new(WorkerConfigOptions {
            stream_bindings: Vec::new(),
            src: String::new(),
            script_name: "queue-policy".into(),
            do_classes: Vec::new(),
            bindings: Vec::new(),
            blob_bindings: Vec::new(),
            sql_bindings: Vec::new(),
            kv_bindings: Vec::new(),
            queue_bindings: Vec::new(),
            queue_consumers: Vec::new(),
            workflow_bindings: Vec::new(),
            vars: Vec::new(),
            node: "test".into(),
            modules: Vec::new(),
            compat: Default::default(),
        });
        let a = ExecutionDomain::new(tokio::runtime::Handle::current());
        let b = ExecutionDomain::new(tokio::runtime::Handle::current());
        a.services().configure_runtime(RuntimePolicy {
            admission_wait: Duration::from_millis(100),
            handler_budget: Duration::from_millis(2_000),
            ..Default::default()
        })?;
        a.services().pin_operation_deadline(3_000)?;
        b.services().configure_runtime(RuntimePolicy {
            admission_wait: Duration::ZERO,
            handler_budget: Duration::from_millis(7_000),
            ..Default::default()
        })?;
        b.services().pin_operation_deadline(4_000)?;
        for (domain, expected) in [(&a, 5_100), (&b, 11_000), (&a, 5_100)] {
            let policy = domain
                .run(async { crate::queue_policy::consumer_config(&config) })
                .await??;
            assert_eq!(policy["leaseDurationMs"], expected);
        }
        a.shutdown().await;
        b.shutdown().await;
        Ok(())
    }
}
