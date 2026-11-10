// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Managed node owners and physical inventory for the public request drill.
use super::*;
use cellar::application::consensus::{
    verification::cell_groups::cell_routing_key, NamespaceQueryOrigin,
};

pub(super) const INGRESS: usize = 3;

pub(super) struct Cluster {
    pub managed: ManagedCluster,
    nodes: Vec<Option<Node>>,
    apis: Vec<Arc<counter::CounterApi>>,
    web: Option<http::Web>,
}

fn definition(observer: Option<Arc<dyn Fn() + Send + Sync>>) -> Result<RaftClass> {
    match observer {
        Some(observer) => counter::definition_with_callback(move || observer()),
        None => counter::definition(),
    }
}

impl Cluster {
    pub async fn start(rooted: bool, store: Arc<fault::Store>) -> Result<Self> {
        Box::pin(Self::start_configured(rooted, store, None, true)).await
    }

    pub async fn start_with_callback(
        rooted: bool,
        store: Arc<fault::Store>,
        on_command: Arc<dyn Fn() + Send + Sync>,
    ) -> Result<Self> {
        Box::pin(Self::start_configured(
            rooted,
            store,
            Some(on_command),
            true,
        ))
        .await
    }

    pub async fn start_with_cold_ingress(
        rooted: bool,
        store: Arc<fault::Store>,
        on_command: Arc<dyn Fn() + Send + Sync>,
    ) -> Result<Self> {
        Box::pin(Self::start_configured(
            rooted,
            store,
            Some(on_command),
            false,
        ))
        .await
    }

    async fn start_configured(
        rooted: bool,
        store: Arc<fault::Store>,
        on_command: Option<Arc<dyn Fn() + Send + Sync>>,
        install_router: bool,
    ) -> Result<Self> {
        let class = definition(on_command.clone())?;
        let first = Fixture::with_store(rooted, class, store).await?;
        let mut apis = Vec::new();
        let mut applications = Vec::new();
        for _ in 0..3 {
            let class = definition(on_command.clone())?;
            let api = counter::CounterApi::new(
                http::secret(),
                first.declaration.namespace.cluster,
                first.declaration.namespace.namespace,
                &class,
            )?;
            applications.push(counter::application(api.clone(), class)?);
            apis.push(api);
        }
        let mut managed = Box::pin(ManagedCluster::start_with_applications(
            first,
            &[],
            ArchiveSetup::Initialized,
            true,
            Some(applications),
            FirstGroupSetup::Operator,
        ))
        .await?;
        let nodes = std::mem::take(&mut managed.nodes)
            .into_iter()
            .map(Some)
            .collect();
        let mut cluster = Self {
            managed,
            nodes,
            apis,
            web: None,
        };
        let started = async {
            Box::pin(cluster.start_ingress(on_command, install_router)).await?;
            cluster.start_web().await
        }
        .await;
        if let Err(error) = started {
            let joined = cluster.close().await;
            cluster.managed.fixtures[0].domain.shutdown().await;
            joined?;
            return Err(error);
        }
        Ok(cluster)
    }

    async fn start_ingress(
        &mut self,
        on_command: Option<Arc<dyn Fn() + Send + Sync>>,
        install_router: bool,
    ) -> Result<()> {
        let mut fixture = peer_fixture(&self.managed.fixtures[0], 7).await?;
        let class = definition(on_command)?;
        let group = self.managed.history;
        let api =
            counter::CounterApi::with_history_resolver(http::secret(), &class, move |name| {
                // Submit canonical cell IDs with an out-of-scope cluster or
                // namespace so the SDK must enforce its own namespace binding.
                match name {
                    "foreign-cluster" => {
                        counter::history(ClusterId::new([211; 16])?, group.namespace, "one")
                    }
                    "foreign-namespace" => {
                        counter::history(group.cluster, NamespaceId::new([211; 16])?, "one")
                    }
                    _ => counter::history(group.cluster, group.namespace, name),
                }
            })?;
        fixture.application = counter::application(api.clone(), class)?;
        let root = tempfile::tempdir()?;
        let node = start_node(
            root.path(),
            fixture.application.clone(),
            &fixture.spec.label,
        )
        .await?;
        let started = async {
            let (mut config, services) = fixture.config(&node, LogOpenMode::Create, 27).await?;
            if install_router {
                config.groups.push(RaftGroupAssignment {
                    history: self.managed.history,
                    placement: RaftGroupPlacement::QueryRouter,
                });
            }
            let peer = config.peers[0].clone();
            let host = node.start_raft_cells(config, services).await?;
            Ok::<_, anyhow::Error>((peer, host))
        }
        .await;
        let (peer, host) = match started {
            Ok(started) => started,
            Err(error) => {
                eprintln!(
                    "retained failed ingress startup disk at {}",
                    root.keep().display()
                );
                node.shutdown(ShutdownMode::Preserve).await?;
                return Err(error);
            }
        };
        self.managed.fixtures.push(fixture);
        self.managed.roots.push(root);
        self.managed.peers.push(peer);
        self.managed.hosts.push(host);
        self.nodes.push(Some(node));
        self.apis.push(api);
        Ok(())
    }

    async fn start_web(&mut self) -> Result<()> {
        self.refresh().await?;
        self.web = Some(http::Web::start(&self.nodes, &self.apis).await?);
        Ok(())
    }

    async fn refresh(&self) -> Result<()> {
        let cut = self.managed.fixtures[0].catalog.read(TIMEOUT).await?;
        for host in &self.managed.hosts {
            for fixture in &self.managed.fixtures {
                inspect_current(host, fixture.spec.node, cut.revision()).await?;
            }
        }
        Ok(())
    }

    pub fn bind(&self) -> Result<()> {
        for (api, host) in self.apis.iter().zip(&self.managed.hosts) {
            *api.cells
                .lock()
                .map_err(|_| anyhow::anyhow!("counter binding poisoned"))? = Some(host.clone());
        }
        Ok(())
    }

    pub fn authorize_commands(&self) -> Result<()> {
        for (index, host) in self.managed.hosts.iter().enumerate() {
            for peer in &self.managed.peers {
                if peer.node == self.managed.fixtures[index].spec.node {
                    continue;
                }
                host.grant_command_origin(NamespaceCommandOrigin {
                    cluster: self.managed.history.cluster,
                    namespace: self.managed.history.namespace,
                    revision: 1,
                    peer: peer.clone(),
                })?;
            }
        }
        Ok(())
    }

    pub fn authorize_ingress_reads(&self) -> Result<()> {
        for host in &self.managed.hosts[..INGRESS] {
            host.grant_query_origin(NamespaceQueryOrigin {
                cluster: self.managed.history.cluster,
                namespace: self.managed.history.namespace,
                revision: 1,
                peer: self.managed.peers[INGRESS].clone(),
            })?;
        }
        Ok(())
    }

    pub fn web(&self) -> Result<&http::Web> {
        self.web.as_ref().context("joined HTTP owner")
    }

    pub async fn leader(&self) -> Result<usize> {
        tokio::time::timeout(TIMEOUT, async {
            loop {
                for (i, host) in self.managed.hosts[..INGRESS].iter().enumerate() {
                    if self.nodes[i].is_none() {
                        continue;
                    }
                    let status = host.group(self.managed.history)?.status().await?;
                    if status.ready && status.leader == Some(i as u64 + 1) {
                        return Ok::<_, anyhow::Error>(i);
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .context("actual managed leader")?
    }

    pub async fn inventory(&self) -> Result<()> {
        let ingress = &self.managed.hosts[INGRESS];
        ensure!(ingress.observe_node(TIMEOUT).await?.node.groups == 0);
        ensure!(ingress.resource_usage().host.controller.groups == 0);
        ensure!(matches!(
            ingress.group(self.managed.history),
            Err(RuntimeError::NotFound)
        ));
        tokio::time::timeout(TIMEOUT, async {
            loop {
                let mut complete = true;
                for host in &self.managed.hosts[..INGRESS] {
                    let local = host.observe_node(TIMEOUT).await?;
                    ensure!(local.fleet_boot.is_some());
                    let observed = host.group(self.managed.history)?.observe(TIMEOUT).await?;
                    ensure!(observed.replica.scope == self.managed.history);
                    complete &= local.node.groups == 1
                        && observed.cells == 2
                        && host.resource_usage().retained_manifest_cells == Some(2);
                }
                if complete {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .context("one physical group and two independently enrolled cells")??;

        let history = self.managed.history;
        let voter = &self.managed.hosts[self.leader().await?];
        let sample = voter.group(history)?.observe_partition(TIMEOUT).await?;
        let one = counter::history(history.cluster, history.namespace, "one")?;
        let two = counter::history(history.cluster, history.namespace, "two")?;
        let mut keys = [
            cell_routing_key(history.namespace, &one.cell),
            cell_routing_key(history.namespace, &two.cell),
        ];
        keys.sort_unstable();
        let [left, right] = keys;
        ensure!(sample.route.history == history && sample.cells == 2);
        ensure!(sample.balanced_pivot.as_ref() == Some(&right));
        let largest = sample
            .largest_cell
            .context("largest image in a nonempty group")?;
        ensure!(sample.logical_bytes >= largest.logical_bytes);
        ensure!(largest.key == left || largest.key == right);
        ensure!(largest.has_predecessor == (largest.key == right));
        let expected_next = (largest.key == left).then_some(&right);
        ensure!(largest.next.as_ref() == expected_next);
        Ok(())
    }

    /// Retain the whole public node drain when only its observing waiter drops.
    pub fn stop_owned(&mut self, index: usize) -> tokio::task::JoinHandle<Result<RaftCloseReport>> {
        let node = self.nodes[index].take();
        let host = self.managed.hosts[index].clone();
        tokio::spawn(async move {
            let stopped = if let Some(node) = node {
                let NodeOutcome::Stopped(report) = node.shutdown(ShutdownMode::Preserve).await?
                else {
                    anyhow::bail!("node did not complete its preserved shutdown")
                };
                ensure!(report.application_work_drained);
                Some(
                    report
                        .raft
                        .context("native node's Raft close report")?
                        .result?,
                )
            } else {
                None
            };
            let report = host.close().await?;
            if let Some(stopped) = stopped {
                ensure!(stopped == report);
            }
            ensure!(
                report.groups_total == usize::from(index != INGRESS)
                    && report.groups.iter().all(|g| !g.failed)
            );
            ensure!(report.mutation_outcomes == MutationOutcomes::NoUnresolvedLocalProducers);
            Ok(report)
        })
    }

    pub async fn stop(&mut self, index: usize) -> Result<()> {
        self.stop_owned(index).await??;
        Ok(())
    }

    pub async fn close(&mut self) -> Result<()> {
        let mut failures = Vec::new();
        if let Some(web) = self.web.take() {
            if let Err(error) = web.close().await {
                failures.push(format!("HTTP join: {error:?}"));
            }
        }
        for i in 0..self.nodes.len() {
            if let Err(error) = self.stop(i).await {
                failures.push(format!("node {i}: {error:?}"));
            }
        }
        for api in &self.apis {
            match api.cells.lock() {
                Ok(mut cells) => {
                    cells.take();
                }
                Err(_) => failures.push("counter binding poisoned".into()),
            }
        }
        ensure!(
            failures.is_empty(),
            "joined managed HTTP shutdown: {failures:?}"
        );
        Ok(())
    }

    pub async fn restart(&mut self) -> Result<()> {
        self.close().await?;
        self.managed.hosts.clear();
        self.nodes.clear();
        self.managed.peers.clear();
        for (i, fixture) in self.managed.fixtures.iter().enumerate() {
            let node = start_node(
                self.managed.roots[i].path(),
                fixture.application.clone(),
                &fixture.spec.label,
            )
            .await?;
            let (mut config, services) = fixture
                .config(&node, LogOpenMode::Resume, 104 + i as u8)
                .await?;
            if i == INGRESS {
                config.groups.push(RaftGroupAssignment {
                    history: self.managed.history,
                    placement: RaftGroupPlacement::QueryRouter,
                });
            } else {
                archives::configure(&mut config);
            }
            self.managed.peers.push(config.peers[0].clone());
            let host = node.start_raft_cells(config, services).await?;
            self.managed.hosts.push(host);
            self.nodes.push(Some(node));
        }
        futures_util::future::try_join_all(
            self.managed.hosts[..INGRESS]
                .iter()
                .map(|voter| voter.start_managed_first_group(self.managed.history, TIMEOUT)),
        )
        .await?;
        self.bind()?;
        self.start_web().await?;
        self.authorize_commands()?;
        self.authorize_ingress_reads()
    }
}
