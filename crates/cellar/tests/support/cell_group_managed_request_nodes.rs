// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Managed node owners and physical inventory for the public request drill.
use super::*;

pub(super) struct Cluster {
    pub managed: ManagedCluster,
    nodes: Vec<Option<Node>>,
    apis: Vec<Arc<counter::CounterApi>>,
    web: Option<http::Web>,
}

impl Cluster {
    pub async fn start(rooted: bool, store: Arc<fault::Store>) -> Result<Self> {
        let class = counter::definition()?;
        let first = Fixture::with_store(rooted, class, store).await?;
        let mut apis = Vec::new();
        let mut applications = Vec::new();
        for _ in 0..3 {
            let class = counter::definition()?;
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
        let started = cluster.start_web().await;
        if let Err(error) = started {
            let joined = cluster.close().await;
            cluster.managed.fixtures[0].domain.shutdown().await;
            joined?;
            return Err(error);
        }
        Ok(cluster)
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

    pub fn web(&self) -> Result<&http::Web> {
        self.web.as_ref().context("joined HTTP owner")
    }

    pub async fn leader(&self) -> Result<usize> {
        tokio::time::timeout(TIMEOUT, async {
            loop {
                for (i, host) in self.managed.hosts.iter().enumerate() {
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
        tokio::time::timeout(TIMEOUT, async {
            loop {
                let mut complete = true;
                for host in &self.managed.hosts {
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
        Ok(())
    }

    pub async fn stop(&mut self, index: usize) -> Result<()> {
        if let Some(node) = self.nodes[index].take() {
            ensure!(matches!(
                node.shutdown(ShutdownMode::Preserve).await?,
                NodeOutcome::Stopped(_)
            ));
        }
        let report = self.managed.hosts[index].close().await?;
        ensure!(report.groups_total == 1 && report.groups.iter().all(|g| !g.failed));
        ensure!(report.mutation_outcomes == MutationOutcomes::NoUnresolvedLocalProducers);
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
            archives::configure(&mut config);
            config.groups.push(RaftGroupAssignment {
                history: self.managed.history,
                placement: RaftGroupPlacement::Replica,
            });
            self.managed.peers.push(config.peers[0].clone());
            let host = node.start_raft_cells(config, services).await?;
            self.managed.hosts.push(host);
            self.nodes.push(Some(node));
        }
        self.bind()?;
        self.start_web().await?;
        self.authorize_commands()
    }
}
