//! Shared advertised endpoints; process refresh never bypasses a fault proxy.
use super::{public_node, store};
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{consensus::*, Node, NodeRaftConfig, NodeRaftListeners, ShutdownMode};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

#[path = "raft_peer_inspection_native.rs"]
mod inspection;

struct Cluster {
    inspection: bool,
    roots: Vec<tempfile::TempDir>,
    nodes: Vec<Option<Node>>,
    services: Vec<Option<RaftCells>>,
    peers: Vec<RaftPeerNode>,
    definitions: Vec<public_node::Counter>,
    registrations: Vec<RaftCellRegistration>,
    store: Arc<store::Store>,
    calls: Vec<Arc<AtomicUsize>>,
    _entered: tokio::sync::mpsc::UnboundedReceiver<()>,
}

async fn listeners() -> Result<NodeRaftListeners> {
    Ok(NodeRaftListeners {
        control: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
        data: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
        query: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
    })
}

impl Cluster {
    async fn start() -> Result<Self> {
        Self::start_with_inspection(false).await
    }

    async fn start_with_inspection(inspection: bool) -> Result<Self> {
        let count = if inspection { 4 } else { 3 };
        let (gate, entered) = public_node::Gate::new();
        gate.release();
        let calls: Vec<_> = (0..count).map(|_| Arc::new(AtomicUsize::new(0))).collect();
        let definitions = calls
            .iter()
            .map(|calls| public_node::counter(calls.clone(), gate.clone()))
            .collect::<Result<Vec<_>>>()?;
        let namespace = NamespaceConfig::new(
            ClusterId::new([1; 16])?,
            NamespaceId::new([2; 16])?,
            NamespaceMode::Raft,
            definitions[0].schema,
        );
        let registrations = (0..2)
            .map(|group| {
                Ok(RaftCellRegistration {
                    placement: RaftCellPlacement::Replica,
                    namespace: namespace.clone(),
                    class: "Counter".into(),
                    birth: BirthProposal {
                        scope: GroupScope {
                            cluster: namespace.cluster,
                            namespace: namespace.namespace,
                            cell: format!("Counter:directory-{group}"),
                            group: GroupId::new([50 + group; 16])?,
                            incarnation: IncarnationId::new([3; 16])?,
                        },
                        voters: (1..=3)
                            .map(|n| {
                                Ok(InitialMember {
                                    member: MemberId::new(n)?,
                                    node: NodeId::new([n as u8; 16])?,
                                })
                            })
                            .collect::<Result<Vec<_>>>()?,
                        learners: vec![],
                    },
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut cluster = Self {
            inspection,
            roots: vec![],
            nodes: vec![],
            services: (0..count).map(|_| None).collect(),
            peers: vec![],
            definitions,
            registrations,
            store: Arc::new(store::Store::default()),
            calls,
            _entered: entered,
        };
        let mut bound = vec![];
        for i in 0..count {
            let root = tempfile::tempdir()?;
            let label = format!("directory-native-{}", i + 1);
            let node = public_node::node_named(
                root.path(),
                cluster.definitions[i].application.clone(),
                &label,
            )
            .await?;
            let listeners = listeners().await?;
            cluster.peers.push(RaftPeerNode {
                node: NodeId::new([i as u8 + 1; 16])?,
                disk: DiskLineage::new([i as u8 + 20; 16])?,
                process: ProcessGeneration::new([i as u8 + 30; 16])?,
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
        for (i, listeners) in bound.into_iter().enumerate() {
            cluster.start_service(i, listeners, false).await?;
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
        self.services[index] = Some(
            node.start_raft_cells(
                RaftCellsConfig {
                    infrastructure: NodeRaftConfig {
                        log: ReplicaLogOptions::new(
                            node.info().data_dir.join("raft"),
                            peer.node,
                            peer.disk,
                            peer.process,
                            if resume {
                                LogOpenMode::Resume
                            } else {
                                LogOpenMode::Create
                            },
                        ),
                        consensus: RaftRuntimeOptions {
                            tick_interval_ms: Some(20),
                            ..Default::default()
                        },
                        transport: TransportOptions::default(),
                    },
                    catalog: CatalogOptions {
                        prefix: "directory/native".into(),
                        ..Default::default()
                    },
                    cells: self
                        .registrations
                        .iter()
                        .cloned()
                        .map(|mut registration| {
                            if index >= 3 {
                                registration.placement = RaftCellPlacement::Standby;
                            }
                            registration
                        })
                        .collect(),
                    peers: self.peers.clone(),
                    // One read grant deliberately lacks a matching inspection
                    // grant; successful inspection elsewhere has no read grant.
                    query_origins: if self.inspection && index == 0 {
                        vec![NamespaceQueryOrigin {
                            cluster: self.registrations[0].namespace.cluster,
                            namespace: self.registrations[0].namespace.namespace,
                            revision: 1,
                            peer: self.peers[2].clone(),
                        }]
                    } else {
                        vec![]
                    },
                    host: RaftHostOptions {
                        inspection_scan: RaftInspectionScanOptions {
                            interval_ms: 20,
                            groups_per_batch: 1,
                            ..Default::default()
                        },
                        inspection_origins: if self.inspection {
                            self.peers
                                .iter()
                                .enumerate()
                                .filter(|(source, _)| {
                                    *source != index && !(index == 0 && *source == 2)
                                })
                                .map(|(_, peer)| NamespaceInspectionOrigin {
                                    cluster: self.registrations[0].namespace.cluster,
                                    namespace: self.registrations[0].namespace.namespace,
                                    revision: 1,
                                    peer: peer.clone(),
                                })
                                .collect()
                        } else {
                            vec![]
                        },
                        node_profile: if self.inspection {
                            let reservation = RaftHostOptions::default().images.max_bytes;
                            Some(RaftNodeProfile {
                                pool: "native".into(),
                                region: "local".into(),
                                zone: format!("zone-{index}"),
                                group_capacity: 2,
                                byte_capacity: reservation * 2,
                                bytes_per_group: reservation,
                                leader_capacity: 2,
                                draining: false,
                            })
                        } else {
                            None
                        },
                        node_directory: Some(RaftNodeDirectoryOptions {
                            poll_ms: 20,
                            timeout_ms: 5000,
                            batch_nodes: 3,
                        }),
                        ..Default::default()
                    },
                },
                RaftCellsServices {
                    listeners,
                    tls: public_node::tls()?,
                    store: self.store.clone(),
                    application: self.definitions[index].application.clone(),
                    versions: public_node::version_keys()?,
                },
            )
            .await?,
        );
        Ok(())
    }

    fn cell(&self, index: usize, group: usize) -> Result<RaftCellHandle> {
        let registration = &self.registrations[group];
        Ok(self.services[index]
            .as_ref()
            .context("missing service")?
            .cell(
                registration.namespace.namespace,
                &registration.birth.scope.cell,
            )?)
    }

    async fn leader(&self, group: usize) -> Result<usize> {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                for i in 0..self.services.len() {
                    if let Ok(cell) = self.cell(i, group) {
                        if cell.status().await.is_ok_and(|s| s.command_ready) {
                            return Ok::<_, anyhow::Error>(i);
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await?
    }

    async fn write(&self, group: usize, id: u8, value: i64) -> Result<CommandReceipt> {
        let mut options = public_node::options(u64::from(id))?;
        let deadline = tokio::time::Instant::now() + options.timeout;
        loop {
            let primary = self.leader(group).await?;
            options.timeout = deadline.saturating_duration_since(tokio::time::Instant::now());
            ensure!(
                !options.timeout.is_zero(),
                "directory fixture command deadline"
            );
            let result = self
                .cell(primary, group)?
                .command(
                    self.definitions[primary].increment.clone(),
                    value.to_be_bytes().to_vec(),
                    options.clone(),
                )
                .await;
            match result {
                Err(CommandFailure::Overloaded | CommandFailure::NotLeader { .. }) => {
                    tokio::time::sleep(Duration::from_millis(5)).await
                }
                result => return Ok(result?),
            }
        }
    }

    async fn restart(&mut self, index: usize) -> Result<()> {
        let bound = listeners().await?;
        ensure!(self.services[index]
            .take()
            .context("missing service")?
            .close()
            .await?
            .unfinished
            .is_empty());
        self.nodes[index]
            .take()
            .context("missing node")?
            .shutdown(ShutdownMode::Preserve)
            .await?;
        let peer = &mut self.peers[index];
        peer.process = ProcessGeneration::new([index as u8 + 70; 16])?;
        peer.control_address = bound.control.local_addr()?;
        peer.data_address = bound.data.local_addr()?;
        peer.query_address = bound.query.local_addr()?;
        self.nodes[index] = Some(
            public_node::node_named(
                self.roots[index].path(),
                self.definitions[index].application.clone(),
                &peer.label,
            )
            .await?,
        );
        self.start_service(index, bound, true).await
    }

    async fn close(mut self) -> Result<()> {
        for service in self.services.drain(..).flatten() {
            ensure!(service.close().await?.unfinished.is_empty());
        }
        for node in self.nodes.drain(..).flatten() {
            node.shutdown(ShutdownMode::Preserve).await?;
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
    let retired = cluster.cell(target, 0)?;
    let previous = cluster.peers[target].clone();
    cluster.restart(target).await?;
    ensure!(matches!(
        retired.status().await,
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
            let read = cluster
                .cell(reader, group)?
                .query(
                    cluster.definitions[reader].get.clone(),
                    vec![],
                    QueryOptions {
                        consistency: QueryConsistency::AtLeast(receipt.version_token.clone()),
                        ..Default::default()
                    },
                )
                .await?;
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
    tokio::time::timeout(Duration::from_secs(5), cluster.close()).await??;
    assert_eq!(store.active_view_reads.load(Ordering::Acquire), 0);
    store.hold_view_reads.store(false, Ordering::Release);
    store.view_reads_resume.notify_waiters();
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
    let retained = cluster.cell(0, 0)?;
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
        retained.status().await,
        Err(CommandFailure::Unavailable)
    ));
    ensure!(service.close().await?.unfinished.is_empty());
    assert_eq!(cluster.write(0, 1, 1).await?.result, 1_i64.to_be_bytes());
    cluster.close().await
}
