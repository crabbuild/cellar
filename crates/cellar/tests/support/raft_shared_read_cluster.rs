// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Public shared-group SDK startup with directional real TCP faults.
use super::{public_node, Definition};
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{consensus::*, Node, NodeRaftConfig, NodeRaftListeners, ShutdownMode};
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
use tokio::net::TcpListener;

#[allow(dead_code)]
#[path = "raft_tcp_fault.rs"]
mod tcp_fault;
pub(super) use tcp_fault::HeldResponses;
use tcp_fault::Proxy;

const TIMEOUT: Duration = Duration::from_secs(5);
const VOTERS: usize = 3;
pub(super) const STALE_SOURCE_TIMEOUT: Duration = Duration::from_millis(200);

#[derive(Clone, Copy, PartialEq, Eq)]
enum Lane {
    Control,
    Data,
    Query,
}

struct Link {
    source: usize,
    target: usize,
    lane: Lane,
    proxy: Proxy,
}

pub(super) struct Cluster {
    pub(super) services: Vec<RaftCells>,
    nodes: Vec<Node>,
    links: Vec<Link>,
    _roots: Vec<tempfile::TempDir>,
    pub(super) history: GroupHistory,
}

impl Cluster {
    pub(super) async fn start(
        definitions: &[Definition; VOTERS],
        mut shared: CellGroupNodeLimits,
        mut host: RaftHostOptions,
    ) -> Result<Self> {
        shared.controller.max_groups = 1;
        shared.controller.max_parallel_apply = 1;
        shared.directory.max_groups = 1;
        shared.entries.envelope = host.execution.envelope;
        host.max_groups = 1;
        host.max_input_bytes = shared.dispatch.max_input_bytes;
        host.stale_source_timeout_ms = u64::try_from(STALE_SOURCE_TIMEOUT.as_millis())?;
        let cluster = ClusterId::new([1; 16])?;
        let namespace = NamespaceId::new([2; 16])?;
        let history = GroupHistory {
            cluster,
            namespace,
            group: GroupId::new([3; 16])?,
            incarnation: IncarnationId::new([4; 16])?,
        };
        let roots = (0..VOTERS)
            .map(|_| tempfile::tempdir())
            .collect::<Result<Vec<_>, _>>()?;
        let mut nodes = Vec::new();
        let mut listeners = Vec::new();
        let mut peers = Vec::new();
        for (index, root) in roots.iter().enumerate() {
            let id = u8::try_from(index + 1)?;
            let label = format!("shared-read-{id}");
            nodes.push(
                public_node::node_named(
                    root.path(),
                    definitions[index].application.clone(),
                    &label,
                )
                .await?,
            );
            let bound = NodeRaftListeners {
                control: TcpListener::bind("127.0.0.1:0").await?,
                data: TcpListener::bind("127.0.0.1:0").await?,
                query: TcpListener::bind("127.0.0.1:0").await?,
            };
            peers.push(RaftPeerNode {
                node: NodeId::new([id; 16])?,
                disk: DiskLineage::new([id + 10; 16])?,
                process: ProcessGeneration::new([id + 20; 16])?,
                label,
                server_name: "localhost".into(),
                control_address: bound.control.local_addr()?,
                data_address: bound.data.local_addr()?,
                query_address: bound.query.local_addr()?,
            });
            listeners.push(bound);
        }
        let birth = CellGroupBirthSpec {
            history,
            range: CellRange::all(),
            epoch: RouteEpoch::new(1)?,
            creation: ProposalId::new([5; 16])?,
            voters: peers
                .iter()
                .enumerate()
                .map(|(index, peer)| {
                    Ok(InitialMember {
                        member: MemberId::new(index as u64 + 1)?,
                        node: peer.node,
                    })
                })
                .collect::<Result<_>>()?,
            learners: vec![],
        };
        let store = Arc::new(object_store::memory::InMemory::new());
        let mut links = Vec::new();
        let mut services = Vec::new();
        for (source, listeners) in listeners.into_iter().enumerate() {
            let mut routes = peers.clone();
            for (target, peer) in routes.iter_mut().enumerate() {
                if source == target {
                    continue;
                }
                for (lane, address) in [
                    (Lane::Control, &mut peer.control_address),
                    (Lane::Data, &mut peer.data_address),
                    (Lane::Query, &mut peer.query_address),
                ] {
                    let proxy = Proxy::start(*address).await?;
                    *address = proxy.address;
                    links.push(Link {
                        source,
                        target,
                        lane,
                        proxy,
                    });
                }
            }
            let peer = &peers[source];
            let definition = &definitions[source];
            let mut log = ReplicaLogOptions::new(
                nodes[source].info().data_dir.join("raft"),
                peer.node,
                peer.disk,
                peer.process,
                LogOpenMode::Create,
            );
            log.max_groups = 1;
            services.push(
                nodes[source]
                    .start_raft_cells(
                        RaftCellsConfig {
                            infrastructure: NodeRaftConfig {
                                log,
                                consensus: RaftRuntimeOptions {
                                    tick_interval_ms: Some(20),
                                    max_groups: 1,
                                    ..Default::default()
                                },
                                transport: TransportOptions::default(),
                            },
                            catalog: CatalogOptions {
                                prefix: "partition/shared-reads".into(),
                                ..Default::default()
                            },
                            namespaces: vec![RaftNamespaceConfig {
                                namespace: NamespaceConfig::new(
                                    cluster,
                                    namespace,
                                    NamespaceMode::Raft,
                                    definition.schema,
                                ),
                                class: "Counter".into(),
                                initialize: if source == 0 {
                                    Some(RaftNamespaceInitialization {
                                        operation: ProposalId::new([6; 16])?,
                                        groups: vec![birth.clone()],
                                    })
                                } else {
                                    None
                                },
                            }],
                            groups: vec![RaftGroupAssignment {
                                history,
                                placement: RaftGroupPlacement::Replica,
                            }],
                            peers: routes,
                            query_origins: vec![],
                            command_origins: vec![],
                            host: host.clone(),
                            shared,
                        },
                        RaftCellsServices {
                            recovery: None,
                            listeners,
                            tls: tls()?,
                            store: store.clone(),
                            application: definition.application.clone(),
                            versions: VersionKeys::new(
                                VersionSigningKey::new([31; 32], u64::MAX)?,
                                vec![],
                                Duration::from_secs(86400),
                            )?,
                        },
                    )
                    .await?,
            );
        }
        Ok(Self {
            services,
            nodes,
            links,
            _roots: roots,
            history,
        })
    }

    pub(super) async fn leader(&self, excluded: Option<usize>) -> Result<usize> {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                for (index, service) in self.services.iter().enumerate() {
                    if excluded != Some(index)
                        && service
                            .group(self.history)?
                            .status()
                            .await
                            .is_ok_and(|status| {
                                status.ready && status.leader == Some(index as u64 + 1)
                            })
                    {
                        return Ok(index);
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?
    }

    pub(super) async fn isolate(&self, node: usize) -> Result<()> {
        self.cut(|link| link.target == node || link.source == node)
            .await
    }

    pub(super) async fn isolate_consensus(&self, node: usize) -> Result<()> {
        self.cut(|link| (link.target == node || link.source == node) && link.lane != Lane::Query)
            .await
    }

    pub(super) async fn block_control(&self, source: usize, target: usize) -> Result<()> {
        self.cut(|link| {
            link.source == source && link.target == target && link.lane == Lane::Control
        })
        .await
    }

    async fn cut(&self, select: impl Fn(&Link) -> bool) -> Result<()> {
        for link in self.links.iter().filter(|link| select(link)) {
            link.proxy.block(true);
        }
        tokio::time::timeout(TIMEOUT, async {
            while self
                .links
                .iter()
                .filter(|link| select(link))
                .any(|link| link.proxy.active.load(Ordering::SeqCst) != 0)
            {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .context("join cut TCP connections")?;
        Ok(())
    }

    fn query_link(&self, source: usize, target: usize) -> Result<&Proxy> {
        self.links
            .iter()
            .find(|link| link.source == source && link.target == target && link.lane == Lane::Query)
            .map(|link| &link.proxy)
            .context("directional query link")
    }

    pub(super) fn hold_query_responses(
        &self,
        source: usize,
        target: usize,
    ) -> Result<HeldResponses> {
        let proxy = self.query_link(source, target)?;
        proxy.state.send_modify(|fault| fault.hold_response = true);
        Ok(HeldResponses(proxy.state.clone()))
    }

    pub(super) fn held_query_response_bytes(&self, source: usize, target: usize) -> Result<usize> {
        Ok(self
            .query_link(source, target)?
            .held_response_bytes
            .load(Ordering::SeqCst))
    }

    pub(super) fn heal(&self) {
        for link in &self.links {
            link.proxy.block(false);
        }
    }

    pub(super) async fn close(mut self) -> Result<()> {
        for service in &self.services {
            let report = service.close().await?;
            ensure!(
                report.mutation_outcomes == MutationOutcomes::NoUnresolvedLocalProducers
                    && report.retained_calls_at_shutdown == 0,
                "unresolved request after healed partition"
            );
        }
        for node in self.nodes {
            node.shutdown(ShutdownMode::Preserve).await?;
        }
        for link in &mut self.links {
            link.proxy.close().await?;
        }
        Ok(())
    }
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
