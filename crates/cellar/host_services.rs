// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Modified for Cellar.

//! Host services owned by one execution domain.
//!
//! The process runner installs its service set once. Library execution domains
//! and deterministic domains each install their own set, so routing authority
//! and resource registries follow their owner.

use crate::ownership_store::LiveLoad;
use std::sync::{Arc, Mutex, OnceLock};

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

enum MetricsBackend {
    Production(Mutex<ProcessLoadSampler>),
    #[cfg(cellar_internal_tests)]
    Scripted(Mutex<HostMetricsSample>),
}

/// Services and resource registries owned by one execution domain.
pub struct HostServices {
    domain: OnceLock<crate::asyncrt::DomainToken>,
    channels: crate::host_channels::HostChannels,
    gates: Arc<crate::host_gates::GateService>,
    tcp: Arc<crate::host_tcp::TcpService>,
    loader: Arc<dyn crate::host_loader::LoaderService>,
    facets: Arc<dyn crate::host_facets::FacetService>,
    containers: Arc<crate::container::ContainerService>,
    node_load: OnceLock<Arc<LiveLoad>>,
    actor_options: OnceLock<crate::actor::ActorOptions>,
    operation_deadline_ms: OnceLock<u64>,
    runtime_policy: OnceLock<crate::runtime_policy::RuntimePolicy>,
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
            channels: crate::host_channels::HostChannels::default(),
            gates: Arc::new(crate::host_gates::GateService::default()),
            tcp: Arc::new(crate::host_tcp::TcpService::default()),
            loader: Arc::new(crate::host_loader::LoaderRegistry::default()),
            facets: Arc::new(crate::host_facets::FacetRegistry::default()),
            containers: Arc::new(crate::container::ContainerService::default()),
            node_load: OnceLock::new(),
            actor_options: OnceLock::new(),
            operation_deadline_ms: OnceLock::new(),
            runtime_policy: OnceLock::new(),
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
            channels: crate::host_channels::HostChannels::default(),
            gates: Arc::new(crate::host_gates::GateService::default()),
            tcp: Arc::new(crate::host_tcp::TcpService::default()),
            loader: Arc::new(crate::host_loader::LoaderRegistry::default()),
            facets: Arc::new(crate::host_facets::FacetRegistry::default()),
            containers: Arc::new(crate::container::ContainerService::default()),
            node_load: OnceLock::new(),
            actor_options: OnceLock::new(),
            operation_deadline_ms: OnceLock::new(),
            runtime_policy: OnceLock::new(),
            wake_entry: crate::wake_entry::WakeEntryService::default(),
            websockets: crate::ws_registry::WebSocketService::default(),
            http_streams: Arc::new(crate::http_streams::HttpStreamService::default()),
            metrics: MetricsBackend::Scripted(Mutex::new(HostMetricsSample::default())),
            #[cfg(all(test, cellar_internal_tests))]
            handoff_transport: Mutex::new(None),
        }
    }

    pub(crate) fn channels(&self) -> &crate::host_channels::HostChannels {
        &self.channels
    }

    pub(crate) fn gates(&self) -> Arc<crate::host_gates::GateService> {
        self.gates.clone()
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

    fn pin_operation_deadline(&self, deadline: u64) -> anyhow::Result<u64> {
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
        let deadline = crate::env_vars::positive_or(
            "CELLAR_OPERATION_DEADLINE_MS",
            crate::actor::DEFAULT_OPERATION_DEADLINE_MS,
        )?;
        self.pin_operation_deadline(deadline)
    }

    pub(crate) fn close_resources(&self) {
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
