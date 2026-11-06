// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Shared advertised endpoints and process fences across two physical groups.
use super::{public_node, store};
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{consensus::*, *};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

#[path = "raft_directory_managed.rs"]
mod managed;
use managed::Managed;
#[path = "raft_peer_inspection_native.rs"]
mod inspection;

const TIMEOUT: Duration = Duration::from_secs(5);
const PREFIX: &str = "directory/native";
const GROUPS: usize = 2;
const VOTERS: usize = 3;

struct Cluster {
    managed: Option<Managed>,
    roots: Vec<tempfile::TempDir>,
    nodes: Vec<Option<Node>>,
    services: Vec<Option<RaftCells>>,
    peers: Vec<RaftPeerNode>,
    applications: Vec<Arc<dyn Application>>,
    namespaces: Vec<NamespaceConfig>,
    births: Vec<CellGroupBirthSpec>,
    store: Arc<store::Store>,
    calls: Vec<Arc<AtomicUsize>>,
}

fn catalog_options() -> CatalogOptions {
    CatalogOptions {
        prefix: PREFIX.into(),
        ..Default::default()
    }
}

fn limits() -> CellGroupNodeLimits {
    let mut limits = public_node::limits();
    limits.namespaces.max_namespaces = GROUPS;
    limits
}

fn counter(calls: Arc<AtomicUsize>) -> Result<RaftClass> {
    Ok(RaftClass::new("Counter", 1, [21; 32])?
        .initializer([24; 32], |_, sql| {
            sql.execute_batch(
                "CREATE TABLE counter(value INTEGER NOT NULL); INSERT INTO counter VALUES(0)",
            )?;
            Ok(())
        })?
        .command("Increment", [22; 32], move |context, input| {
            calls.fetch_add(1, Ordering::SeqCst);
            let delta = i64::from_be_bytes(input.try_into()?);
            context
                .sql()
                .execute("UPDATE counter SET value=value+?1", [delta])?;
            let value: i64 = context
                .sql()
                .query_row("SELECT value FROM counter", [], |row| row.get(0))?;
            Ok(value.to_be_bytes().to_vec())
        })?
        .query("Get", [23; 32], |context, _| {
            let value: i64 =
                context.query_row("SELECT value FROM counter", [], |row| row.get(0))?;
            Ok(value.to_be_bytes().to_vec())
        })?)
}

async fn listeners() -> Result<NodeRaftListeners> {
    Ok(NodeRaftListeners {
        control: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
        data: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
        query: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
    })
}

fn tls() -> Result<RaftTls> {
    Ok(RaftTls::new(
        vec![include_bytes!("../../transport/tcp/fixtures/ca.der")
            .to_vec()
            .into()],
        vec![include_bytes!("../../transport/tcp/fixtures/server.der")
            .to_vec()
            .into()],
        rustls::pki_types::PrivatePkcs8KeyDer::from(
            include_bytes!("../../transport/tcp/fixtures/key.der").to_vec(),
        )
        .into(),
    )?)
}

impl Cluster {
    async fn start() -> Result<Self> {
        Self::open(false).await
    }

    async fn open(managed: bool) -> Result<Self> {
        let count = VOTERS + usize::from(managed);
        let calls: Vec<_> = (0..count).map(|_| Arc::new(AtomicUsize::new(0))).collect();
        let definitions = calls
            .iter()
            .map(|calls| counter(calls.clone()))
            .collect::<Result<Vec<_>>>()?;
        let cluster_id = ClusterId::new([1; 16])?;
        let namespaces = (0..GROUPS)
            .map(|index| {
                Ok(NamespaceConfig::new(
                    cluster_id,
                    NamespaceId::new([2 + index as u8; 16])?,
                    NamespaceMode::Raft,
                    definitions[0].schema_digest(),
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let births = namespaces
            .iter()
            .enumerate()
            .map(|(index, namespace)| {
                Ok(CellGroupBirthSpec {
                    history: GroupHistory {
                        cluster: namespace.cluster,
                        namespace: namespace.namespace,
                        group: GroupId::new([50 + index as u8; 16])?,
                        incarnation: IncarnationId::new([3; 16])?,
                    },
                    range: CellRange::all(),
                    epoch: RouteEpoch::new(1)?,
                    creation: ProposalId::new([70 + index as u8; 16])?,
                    voters: (1..=VOTERS)
                        .map(|id| {
                            Ok(InitialMember {
                                member: MemberId::new(id as u64)?,
                                node: NodeId::new([id as u8; 16])?,
                            })
                        })
                        .collect::<Result<_>>()?,
                    learners: vec![],
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let store = Arc::new(store::Store::default());
        let managed = if managed {
            Some(Managed::open(store.clone(), &namespaces, &definitions[0]).await?)
        } else {
            None
        };
        let applications = definitions
            .into_iter()
            .map(|class| {
                Ok(Arc::new(
                    Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_class(class)?,
                ) as Arc<dyn Application>)
            })
            .collect::<Result<Vec<_>>>()?;
        let mut cluster = Self {
            managed,
            roots: vec![],
            nodes: vec![],
            services: (0..count).map(|_| None).collect(),
            peers: vec![],
            applications,
            namespaces,
            births,
            store,
            calls,
        };
        let mut bound = Vec::new();
        for index in 0..count {
            let root = tempfile::tempdir()?;
            let label = format!("directory-native-{}", index + 1);
            let node =
                public_node::node_named(root.path(), cluster.applications[index].clone(), &label)
                    .await?;
            let listeners = listeners().await?;
            cluster.peers.push(RaftPeerNode {
                node: NodeId::new([index as u8 + 1; 16])?,
                disk: DiskLineage::new([index as u8 + 20; 16])?,
                process: ProcessGeneration::new([index as u8 + 30; 16])?,
                label,
                server_name: "localhost".into(),
                control_address: listeners.control.local_addr()?,
                data_address: listeners.data.local_addr()?,
                query_address: listeners.query.local_addr()?,
            });
            cluster.roots.push(root);
            cluster.nodes.push(Some(node));
            bound.push(listeners);
        }
        if let Some(managed) = &cluster.managed {
            managed.enroll(&cluster.peers).await?;
        }
        for (index, bound) in bound.into_iter().enumerate() {
            cluster.start_service(index, bound, false).await?;
        }
        if let Some(managed) = &cluster.managed {
            Box::pin(managed.attach(&cluster)).await?;
        }
        for index in 0..GROUPS {
            let leader = cluster.leader(index).await?;
            cluster
                .cell(leader, index)?
                .enroll(
                    CellEnrollmentRequest {
                        creation: ProposalId::new([80 + index as u8; 16])?,
                        durability: Durability::Quorum,
                    },
                    TIMEOUT,
                )
                .await?;
        }
        Ok(cluster)
    }

    async fn start_service(
        &mut self,
        index: usize,
        listeners: NodeRaftListeners,
        resume: bool,
    ) -> Result<()> {
        let node = self.nodes[index].as_ref().context("missing node")?;
        let peer = &self.peers[index];
        let managed = if let Some(fleet) = &self.managed {
            Some(
                fleet
                    .domain
                    .run(async {
                        let cut = fleet.fleet.read(TIMEOUT).await?;
                        let authorization =
                            fleet.fleet.authorize_host(&cut, peer.node, TIMEOUT).await?;
                        Ok::<_, anyhow::Error>(RaftManagedHostOptions {
                            authorization,
                            namespaces: fleet.namespaces.clone(),
                        })
                    })
                    .await??,
            )
        } else {
            None
        };
        let namespaces = self
            .namespaces
            .iter()
            .enumerate()
            .map(|(position, namespace)| {
                let namespace = managed
                    .as_ref()
                    .map(|mode| mode.namespaces[position].namespace.clone())
                    .unwrap_or_else(|| namespace.clone());
                let initialize = if managed.is_none() && index == 0 && !resume {
                    Some(RaftNamespaceInitialization {
                        operation: ProposalId::new([60 + position as u8; 16])?,
                        groups: vec![self.births[position].clone()],
                    })
                } else {
                    None
                };
                Ok(RaftNamespaceConfig {
                    namespace,
                    class: "Counter".into(),
                    initialize,
                })
            })
            .collect::<Result<_>>()?;
        let groups = if index < VOTERS && (managed.is_none() || resume) {
            self.births
                .iter()
                .map(|birth| RaftGroupAssignment {
                    history: birth.history,
                    placement: RaftGroupPlacement::Replica,
                })
                .collect()
        } else {
            vec![]
        };
        let mut log = ReplicaLogOptions::new(
            node.info().data_dir.join("raft"),
            peer.node,
            peer.disk,
            peer.process,
            if resume {
                LogOpenMode::Resume
            } else {
                LogOpenMode::Create
            },
        );
        log.max_groups = GROUPS;
        let storage = RaftHostOptions::default().images.max_bytes;
        let shared = limits();
        self.services[index] = Some(
            node.start_raft_cells(
                RaftCellsConfig {
                    infrastructure: NodeRaftConfig {
                        log,
                        consensus: RaftRuntimeOptions {
                            tick_interval_ms: Some(20),
                            max_groups: GROUPS,
                            ..Default::default()
                        },
                        transport: TransportOptions::default(),
                    },
                    catalog: catalog_options(),
                    namespaces,
                    groups,
                    peers: if managed.is_some() {
                        vec![peer.clone()]
                    } else {
                        self.peers.clone()
                    },
                    query_origins: if managed.is_none() && index == 0 {
                        vec![NamespaceQueryOrigin {
                            cluster: self.namespaces[0].cluster,
                            namespace: self.namespaces[0].namespace,
                            revision: 1,
                            peer: self.peers[2].clone(),
                        }]
                    } else {
                        vec![]
                    },
                    command_origins: vec![],
                    host: RaftHostOptions {
                        managed,
                        max_groups: GROUPS,
                        max_input_bytes: shared.dispatch.max_input_bytes,
                        inspection_scan: RaftInspectionScanOptions {
                            interval_ms: 20,
                            groups_per_batch: 1,
                            ..Default::default()
                        },
                        node_profile: Some(RaftNodeProfile {
                            pool: "native".into(),
                            region: "local".into(),
                            zone: format!("zone-{index}"),
                            group_capacity: GROUPS as u32,
                            byte_capacity: storage * GROUPS as u64,
                            bytes_per_group: storage,
                            leader_capacity: GROUPS as u32,
                            draining: false,
                        }),
                        node_directory: Some(RaftNodeDirectoryOptions {
                            poll_ms: 20,
                            timeout_ms: 5000,
                            batch_nodes: 3,
                        }),
                        ..Default::default()
                    },
                    shared,
                },
                RaftCellsServices {
                    recovery: None,
                    listeners,
                    tls: tls()?,
                    store: self.store.clone(),
                    application: self.applications[index].clone(),
                    versions: VersionKeys::new(
                        VersionSigningKey::new([31; 32], u64::MAX)?,
                        vec![],
                        Duration::from_secs(86400),
                    )?,
                },
            )
            .await?,
        );
        Ok(())
    }

    fn service(&self, index: usize) -> Result<&RaftCells> {
        self.services[index].as_ref().context("missing service")
    }

    fn cell(&self, index: usize, group: usize) -> Result<RaftCellHandle> {
        let history = self.births[group].history;
        Ok(self.service(index)?.cell(CellHistory {
            cluster: history.cluster,
            namespace: history.namespace,
            cell: CellId::new(format!("Counter:directory-{group}"))?,
            incarnation: IncarnationId::new([4; 16])?,
        })?)
    }

    async fn inspect(
        &self,
        source: usize,
        target: usize,
        revision: u64,
    ) -> Result<RaftPeerNodeObservation> {
        let service = self.service(source)?;
        let peer = &self.peers[target];
        let deadline = Instant::now() + TIMEOUT;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let result = service
                .inspect_peer_physical_inventory(
                    peer.node,
                    revision,
                    remaining.min(Duration::from_millis(500)),
                )
                .await;
            match result {
                Ok(proof) if proof.view.node.identity.process == peer.process => return Ok(proof),
                _ if Instant::now() >= deadline => anyhow::bail!(
                    "physical inspection {source} -> {target} at {revision}: {result:?}"
                ),
                _ => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
    }

    async fn leader(&self, group: usize) -> Result<usize> {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                for index in 0..VOTERS {
                    if let Ok(service) = self.service(index) {
                        if !service.is_closed()
                            && service
                                .group(self.births[group].history)?
                                .status()
                                .await
                                .is_ok_and(|status| {
                                    status.ready && status.leader == Some(index as u64 + 1)
                                })
                        {
                            return Ok::<_, anyhow::Error>(index);
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await?
    }

    async fn write(&self, group: usize, sequence: u8, value: i64) -> Result<CommandReceipt> {
        let retry = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis() as u64
            + 60_000;
        let mut options = CommandOptions::new(
            CommandId::new(ClientId::new([7; 16])?, u64::from(sequence))?,
            retry,
        );
        let deadline = Instant::now() + options.timeout;
        loop {
            let primary = self.leader(group).await?;
            options.timeout = deadline.saturating_duration_since(Instant::now());
            ensure!(
                !options.timeout.is_zero(),
                "directory fixture command deadline"
            );
            let cell = self.cell(primary, group)?;
            match cell
                .command(
                    &cell.contract("Increment")?,
                    value.to_be_bytes().to_vec(),
                    options.clone(),
                )
                .await
            {
                Err(error)
                    if !error.ambiguous
                        && matches!(
                            *error.cause,
                            CellDispatchError::Runtime(
                                RuntimeError::Overloaded | RuntimeError::NotLeader
                            )
                        ) =>
                {
                    tokio::time::sleep(Duration::from_millis(5)).await
                }
                result => return Ok(result?),
            }
        }
    }

    async fn read(
        &self,
        index: usize,
        group: usize,
        receipt: &CommandReceipt,
    ) -> Result<QueryResult> {
        let cell = self.cell(index, group)?;
        Ok(cell
            .query(
                ProposalId::new([180 + (index * GROUPS + group) as u8; 16])?,
                &cell.contract("Get")?,
                vec![],
                QueryOptions {
                    consistency: QueryConsistency::AtLeast(receipt.version_token.clone()),
                    ..Default::default()
                },
            )
            .await?)
    }

    async fn stop_node(&mut self, index: usize) -> Result<()> {
        let service = self.services[index].take().context("missing service")?;
        let node = self.nodes[index].take().context("missing node")?;
        ensure!(matches!(
            node.shutdown(ShutdownMode::Preserve).await?,
            NodeOutcome::Stopped(_)
        ));
        ensure!(
            service.close().await?.mutation_outcomes
                == MutationOutcomes::NoUnresolvedLocalProducers
        );
        Ok(())
    }

    async fn restart(&mut self, index: usize) -> Result<()> {
        let bound = listeners().await?;
        self.stop_node(index).await?;
        let peer = &mut self.peers[index];
        peer.process = ProcessGeneration::new([index as u8 + 70; 16])?;
        peer.control_address = bound.control.local_addr()?;
        peer.data_address = bound.data.local_addr()?;
        peer.query_address = bound.query.local_addr()?;
        self.nodes[index] = Some(
            public_node::node_named(
                self.roots[index].path(),
                self.applications[index].clone(),
                &peer.label,
            )
            .await?,
        );
        self.start_service(index, bound, true).await
    }

    async fn close(mut self) -> Result<()> {
        for index in 0..self.services.len() {
            self.stop_node(index).await?;
        }
        if let Some(managed) = &self.managed {
            managed.domain.shutdown().await;
        }
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn native_timer_refreshes_restarted_process_across_cell_groups() -> Result<()> {
    let mut cluster = Cluster::start().await?;
    for group in 0..2 {
        assert_eq!(
            cluster.write(group, 1, 5).await?.result,
            5_i64.to_be_bytes()
        );
    }
    let target = (cluster.leader(0).await? + 1) % 3;
    let retired = cluster.service(target)?.clone();
    let previous = cluster.peers[target].clone();
    cluster.restart(target).await?;
    ensure!(retired.is_closed());
    ensure!(matches!(
        retired.observe_node(TIMEOUT).await,
        Err(CommandFailure::Unavailable)
    ));
    assert_ne!(cluster.peers[target].process, previous.process);
    assert_ne!(
        cluster.peers[target].control_address,
        previous.control_address
    );
    let before = cluster.store.view_reads.load(Ordering::Acquire);
    tokio::time::sleep(Duration::from_millis(200)).await;
    ensure!(
        cluster.store.view_reads.load(Ordering::Acquire) > before + 3,
        "directory did not run without requests"
    );
    for group in 0..2 {
        let receipt = cluster.write(group, 2, 2).await?;
        assert_eq!(receipt.result, 7_i64.to_be_bytes());
        for reader in 0..3 {
            let read = cluster.read(reader, group, &receipt).await?;
            assert_eq!(read.result, 7_i64.to_be_bytes());
        }
    }
    assert_eq!(
        cluster
            .calls
            .iter()
            .map(|calls| calls.load(Ordering::Acquire))
            .sum::<usize>(),
        4
    );
    cluster.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn native_directory_outage_preserves_cached_quorum_routes() -> Result<()> {
    let cluster = Cluster::start().await?;
    assert_eq!(cluster.write(0, 1, 1).await?.result, 1_i64.to_be_bytes());
    // Node zero accepts namespace read grants from node two. That grant is
    // insufficient for the Fleet physical-inspection protocol.
    ensure!(matches!(
        cluster
            .service(2)?
            .inspect_peer_physical_inventory(cluster.peers[0].node, 1, TIMEOUT)
            .await,
        Err(InspectionError::Refused)
    ));
    cluster
        .store
        .fail_directory_reads
        .store(true, Ordering::Release);
    tokio::time::sleep(Duration::from_millis(100)).await;
    for id in 2..=5 {
        assert_eq!(
            cluster.write(0, id, 1).await?.result,
            i64::from(id).to_be_bytes()
        );
    }
    cluster
        .store
        .fail_directory_reads
        .store(false, Ordering::Release);
    cluster.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn native_directory_shutdown_joins_paused_metadata() -> Result<()> {
    let cluster = Cluster::start().await?;
    let store = cluster.store.clone();
    store.hold_view_reads.store(true, Ordering::Release);
    tokio::time::timeout(Duration::from_secs(5), async {
        while store.active_view_reads.load(Ordering::Acquire) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    // The directory joins an entered read before returning from shutdown.
    let mut closing = Box::pin(cluster.close());
    ensure!(futures_util::poll!(closing.as_mut()).is_pending());
    ensure!(store.active_view_reads.load(Ordering::Acquire) > 0);
    store.hold_view_reads.store(false, Ordering::Release);
    store.view_reads_resume.notify_waiters();
    tokio::time::timeout(Duration::from_secs(5), closing).await??;
    assert_eq!(store.active_view_reads.load(Ordering::Acquire), 0);
    Ok(())
}

fn superseded_view(bytes: &[u8], process: ProcessGeneration, revision: u64) -> Result<Vec<u8>> {
    use sha2::{Digest, Sha256};
    ensure!(&bytes[..8] == b"CLRCAT01" && bytes[8] == 7);
    let payload = std::str::from_utf8(&bytes[13..bytes.len() - 32])?;
    let value: serde_json::Value = serde_json::from_str(payload)?;
    let old_process = format!("\"process\":{}", serde_json::to_string(&value["process"])?);
    let old_revision = format!(
        "\"revision\":{}",
        serde_json::to_string(&value["revision"])?
    );
    // Preserve the struct's canonical field order. This is a synthetic trusted
    // directory observation, not another process opening or copying live votes.
    let payload = payload
        .replace(
            &old_process,
            &format!("\"process\":{}", serde_json::to_string(&process.bytes())?),
        )
        .replace(&old_revision, &format!("\"revision\":\"{revision}\""));
    let mut result = bytes[..9].to_vec();
    result.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    result.extend_from_slice(payload.as_bytes());
    result.extend_from_slice(&Sha256::digest(&result));
    Ok(result)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn native_self_view_ignores_old_revision_and_joins_superseded_host() -> Result<()> {
    use object_store::ObjectStore;
    let mut cluster = Cluster::start().await?;
    cluster.restart(0).await?;
    let service = cluster.services[0]
        .as_ref()
        .context("missing service")?
        .clone();
    let key = object_store::path::Path::from(format!(
        "directory/native/raft/{}/nodes/{}/view",
        hex::encode([1; 16]),
        hex::encode([1; 16])
    ));
    let original = cluster.store.inner.get(&key).await?.bytes().await?;
    let value: serde_json::Value = serde_json::from_slice(&original[13..original.len() - 32])?;
    let revision = value["revision"]
        .as_str()
        .context("revision")?
        .parse::<u64>()?;
    ensure!(revision > 1);
    let process = ProcessGeneration::new([90; 16])?;
    let stale = superseded_view(&original, process, revision - 1)?;
    cluster.store.inner.put(&key, stale.into()).await?;
    tokio::time::sleep(Duration::from_millis(200)).await;
    ensure!(
        !service.is_closed(),
        "stale self observation closed the current owner"
    );
    cluster
        .store
        .inner
        .put(&key, original.clone().into())
        .await?;
    let newer = superseded_view(&original, process, revision + 100)?;
    cluster.store.inner.put(&key, newer.into()).await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        while !service.is_closed() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    ensure!(matches!(
        service.observe_node(TIMEOUT).await,
        Err(CommandFailure::Unavailable)
    ));
    ensure!(
        service.close().await?.mutation_outcomes == MutationOutcomes::NoUnresolvedLocalProducers
    );
    assert_eq!(cluster.write(0, 1, 1).await?.result, 1_i64.to_be_bytes());
    cluster.close().await
}
