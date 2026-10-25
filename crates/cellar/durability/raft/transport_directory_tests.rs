// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Physical refresh never advances configuration or escapes owner budgets.
use super::*;
use crate::raft::catalog::{Catalog, CatalogOptions, NamespaceCatalog, NamespaceConfig, NodeView};
use cellar_logic::NamespaceMode;
use std::time::Duration;

#[path = "transport_fleet_tests.rs"]
mod fleet;

struct Publisher {
    root: tempfile::TempDir,
    domain: crate::asyncrt::ExecutionDomain,
    log: ReplicaLog,
    namespace: NamespaceCatalog,
    peer: RaftPeerNode,
    store: Arc<object_store::memory::InMemory>,
}

impl Publisher {
    async fn new(peer: RaftPeerNode) -> anyhow::Result<Self> {
        let root = tempfile::tempdir()?;
        let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
        let log = domain
            .run(ReplicaLog::open(
                ReplicaLogOptions::new(
                    root.path().join("log"),
                    peer.node,
                    peer.disk,
                    peer.process,
                    LogOpenMode::Create,
                ),
                Arc::new(cellar_ltx::DirectFileSystem),
            ))
            .await??;
        let store = Arc::new(object_store::memory::InMemory::new());
        let catalog = Catalog::new(store.clone(), CatalogOptions::default())?;
        let namespace = catalog
            .open_namespace(NamespaceConfig::new(
                ClusterId::new([1; 16])?,
                NamespaceId::new([2; 16])?,
                NamespaceMode::Raft,
                [9; 32],
            ))
            .await?;
        Ok(Self {
            root,
            domain,
            log,
            namespace,
            peer,
            store,
        })
    }

    async fn view(&self) -> anyhow::Result<NodeView> {
        let registration = self
            .namespace
            .register_node_storage(&self.log, Duration::from_secs(5))
            .await?;
        Ok(registration
            .publication(self.peer.clone(), Duration::from_secs(5))
            .await?
            .publish(Duration::from_secs(5))
            .await?)
    }

    async fn restart(&mut self) -> anyhow::Result<()> {
        self.log.close().await?;
        self.peer.process = ProcessGeneration::new([44; 16])?;
        self.peer.control_address = "127.0.0.1:42001".parse()?;
        self.peer.data_address = "127.0.0.1:42002".parse()?;
        self.peer.query_address = "127.0.0.1:42003".parse()?;
        self.log = self
            .domain
            .run(ReplicaLog::open(
                ReplicaLogOptions::new(
                    self.root.path().join("log"),
                    self.peer.node,
                    self.peer.disk,
                    self.peer.process,
                    LogOpenMode::Resume,
                ),
                Arc::new(cellar_ltx::DirectFileSystem),
            ))
            .await??;
        Ok(())
    }

    async fn conflicting_attempt(&self) -> anyhow::Result<NodeView> {
        use object_store::ObjectStore;
        use sha2::{Digest, Sha256};
        let key = object_store::path::Path::from(format!(
            "raft/{}/nodes/{}/view",
            hex::encode([1; 16]),
            hex::encode(self.peer.node.bytes())
        ));
        let bytes = self.store.get(&key).await?.bytes().await?;
        let payload = std::str::from_utf8(&bytes[13..bytes.len() - 32])?;
        let value: serde_json::Value = serde_json::from_str(payload)?;
        let old = serde_json::to_string(&value["boot_attempt"])?;
        let mut attempt: Vec<u8> = serde_json::from_value(value["boot_attempt"].clone())?;
        attempt[0] ^= 1;
        if attempt.iter().all(|byte| *byte == 0) {
            attempt[1] = 1;
        }
        let payload = payload.replace(
            &format!("\"boot_attempt\":{old}"),
            &format!("\"boot_attempt\":{}", serde_json::to_string(&attempt)?),
        );
        let mut changed = bytes[..9].to_vec();
        changed.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        changed.extend_from_slice(payload.as_bytes());
        changed.extend_from_slice(&Sha256::digest(&changed));
        self.store.put(&key, changed.into()).await?;
        let registration = self
            .namespace
            .register_node_storage(&self.log, Duration::from_secs(5))
            .await?;
        Ok(registration
            .observe_node(&self.peer, Duration::from_secs(5))
            .await?
            .unwrap())
    }

    async fn close(self) -> anyhow::Result<()> {
        self.log.close().await?;
        self.domain.shutdown().await;
        Ok(())
    }
}

fn peer(id: u8) -> anyhow::Result<RaftPeerNode> {
    Ok(RaftPeerNode {
        node: NodeId::new([id; 16])?,
        disk: DiskLineage::new([id + 1; 16])?,
        process: ProcessGeneration::new([id + 2; 16])?,
        label: format!("node-{id}"),
        server_name: "localhost".into(),
        control_address: format!("127.0.0.1:{}", 40000 + u16::from(id) * 3).parse()?,
        data_address: format!("127.0.0.1:{}", 40001 + u16::from(id) * 3).parse()?,
        query_address: format!("127.0.0.1:{}", 40002 + u16::from(id) * 3).parse()?,
    })
}

fn scope(id: u8) -> anyhow::Result<GroupScope> {
    Ok(GroupScope {
        cluster: ClusterId::new([1; 16])?,
        namespace: NamespaceId::new([id; 16])?,
        cell: format!("Counter:{id}"),
        group: GroupId::new([id; 16])?,
        incarnation: IncarnationId::new([4; 16])?,
    })
}

fn route(fixture: &Fixture, scope: &GroupScope, query: bool) -> anyhow::Result<GroupRoute> {
    let mut peers = vec![];
    for id in if query { 2..=3 } else { 1..=3 } {
        let mut peer = peer(id)?;
        if id == 1 {
            peer.control_address = fixture.transport.control_address();
            peer.data_address = fixture.transport.data_address();
            peer.query_address = fixture.transport.query_address();
        }
        peers.push(RaftPeer {
            replica: ReplicaIdentity {
                scope: scope.clone(),
                member: MemberId::new(u64::from(id))?,
                node: peer.node,
                disk: peer.disk,
                process: peer.process,
            },
            label: peer.label,
            server_name: peer.server_name,
            control_address: peer.control_address,
            data_address: peer.data_address,
            query_address: peer.query_address,
        });
    }
    Ok(GroupRoute {
        scope: scope.clone(),
        schema_digest: [9; 32],
        revision: 1,
        peers,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn process_refresh_is_atomic_across_routes_grants_and_capacity() -> anyhow::Result<()> {
    use crate::raft::storage::GroupBootstrap;
    let scopes = [scope(2)?, scope(3)?, scope(4)?];
    let groups = scopes[..2]
        .iter()
        .map(|scope| {
            Ok(GroupBootstrap {
                scope: scope.clone(),
                member: MemberId::new(1)?,
                birth_digest: [10; 32],
                voters: (1..=3).map(MemberId::new).collect::<Result<_, _>>()?,
                learners: vec![],
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let fixture = Fixture::start(
        TransportOptions {
            max_peers: 3,
            max_connections_per_lane: 1,
            ..Default::default()
        },
        groups,
    )
    .await?;
    let routes = scopes
        .iter()
        .enumerate()
        .map(|(i, s)| route(&fixture, s, i == 2))
        .collect::<anyhow::Result<Vec<_>>>()?;
    for r in &routes[..2] {
        fixture.transport.install_group(r.clone())?;
    }
    fixture.transport.install_query_group(routes[2].clone())?;
    let trusted = peer(2)?;
    let mut publisher = Publisher::new(trusted.clone()).await?;
    let first = publisher.view().await?;
    fixture
        .transport
        .accept_node_view(first.clone(), &trusted)?;
    let grant = NamespaceQueryOrigin {
        cluster: scopes[0].cluster,
        namespace: NamespaceId::new([5; 16])?,
        revision: 1,
        peer: trusted.clone(),
    };
    fixture.transport.grant_query_origin(grant.clone())?;
    let state = &fixture.transport.handle.0;
    let key = (grant.cluster, grant.namespace, trusted.node);
    let (old_routes, old_pool, old_grant) = {
        let r = state.registry.lock().unwrap();
        (
            scopes
                .iter()
                .map(|s| {
                    r.groups
                        .get(s)
                        .or_else(|| r.query_groups.get(s))
                        .unwrap()
                        .clone()
                })
                .collect::<Vec<_>>(),
            r.pools[&trusted.node].clone(),
            r.origins[&key].clone(),
        )
    };
    let seed_epoch = state.seed_peer_epoch(&trusted)?;
    let conf = fixture.log.group(&scopes[0])?.read()?.conf_state.clone();
    publisher.restart().await?;
    let latest = publisher.view().await?;
    assert!(latest.revision() > first.revision());
    // Retain actual resource owners so replacement cannot hide old workers.
    let (release, wait) = watch::channel(false);
    while state.usage().tasks < state.task_limit() - 2 {
        let mut wait = wait.clone();
        state.spawn(async move {
            if !*wait.borrow_and_update() {
                let _ = wait.changed().await;
            }
        })?;
    }
    assert_eq!(
        fixture.transport.accept_node_view(latest.clone(), &trusted),
        Err(TransportError::Overloaded)
    );
    {
        let r = state.registry.lock().unwrap();
        assert!(Arc::ptr_eq(&r.pools[&trusted.node], &old_pool));
        assert!(Arc::ptr_eq(&r.groups[&scopes[0]], &old_routes[0]));
        assert_eq!(r.node_views[&trusted.node], first);
    }
    assert!(!*old_pool.stop.borrow());
    assert!(!*old_grant.stop.borrow());
    release.send_replace(true);
    fixture
        .domain
        .run(crate::asyncrt::timeout(Duration::from_secs(5), async {
            while state.usage().tasks > 9 {
                tokio::task::yield_now().await;
            }
        }))
        .await??;
    fixture
        .transport
        .accept_node_view(latest.clone(), &trusted)?;
    assert!(*seed_epoch.borrow());
    let deadline = state.domain.mono_ms()?.saturating_add(5000);
    assert_eq!(
        state
            .seed_process_until(&seed_epoch, deadline, async { Ok(()) })
            .await,
        Err(TransportError::StalePeer)
    );
    assert!(matches!(
        state.seed_peer_epoch(&trusted),
        Err(TransportError::StalePeer)
    ));
    let fresh_epoch = state.seed_peer_epoch(&publisher.peer)?;
    assert_eq!(
        state
            .seed_process_until(&fresh_epoch, deadline, async { Ok(()) })
            .await,
        Ok(())
    );
    assert!(*old_pool.stop.borrow());
    assert!(*old_grant.stop.borrow());
    {
        let r = state.registry.lock().unwrap();
        for (i, s) in scopes.iter().enumerate() {
            let current = r.groups.get(s).or_else(|| r.query_groups.get(s)).unwrap();
            assert!(!Arc::ptr_eq(current, &old_routes[i]));
            assert_eq!(current.revision, 1);
            assert_eq!(
                RaftPeerNode::from(
                    current
                        .peers
                        .iter()
                        .find(|p| p.replica.node == trusted.node)
                        .unwrap()
                ),
                publisher.peer
            );
        }
        assert_eq!(r.origins[&key].revision, 1);
        assert_eq!(r.origins[&key].peer.as_ref(), Some(&publisher.peer));
    }
    assert_eq!(fixture.log.group(&scopes[0])?.read()?.conf_state, conf);
    for (i, r) in routes.into_iter().enumerate() {
        let result = if i == 2 {
            fixture.transport.install_query_group(r)
        } else {
            fixture.transport.install_group(r)
        };
        assert_eq!(result, Err(TransportError::StalePeer));
    }
    assert_eq!(
        fixture.transport.grant_query_origin(NamespaceQueryOrigin {
            revision: 2,
            ..grant
        }),
        Err(TransportError::StalePeer)
    );
    assert_eq!(
        fixture.transport.accept_node_view(first, &trusted),
        Err(TransportError::StalePeer)
    );
    fixture.transport.accept_node_view(latest, &trusted)?;
    publisher.close().await?;
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn process_refresh_pins_instance_and_future_route_admission() -> anyhow::Result<()> {
    let fixture = Fixture::start(TransportOptions::default(), vec![]).await?;
    let trusted = peer(2)?;
    let mut publisher = Publisher::new(trusted.clone()).await?;
    let first = publisher.view().await?;
    fixture
        .transport
        .accept_node_view(first.clone(), &trusted)?;
    // Discovery alone installs no group, pool or query permission.
    assert_eq!(fixture.transport.usage().peers, 0);
    assert_eq!(fixture.transport.usage().groups, 0);
    assert!(fixture.log.groups()?.is_empty());
    let conflict = publisher.conflicting_attempt().await?;
    assert_eq!(conflict.revision(), first.revision());
    assert_eq!(conflict.peer(), first.peer());
    assert_ne!(conflict, first);
    assert_eq!(
        fixture.transport.accept_node_view(conflict, &trusted),
        Err(TransportError::StalePeer)
    );
    let routed = route(&fixture, &scope(4)?, true)?;
    fixture.transport.install_query_group(routed.clone())?;
    let mut wrong = trusted.clone();
    wrong.disk = DiskLineage::new([99; 16])?;
    assert_eq!(
        fixture.transport.accept_node_view(first, &wrong),
        Err(TransportError::Identity)
    );
    publisher.restart().await?;
    let current = publisher.view().await?;
    fixture
        .transport
        .accept_node_view(current.clone(), &trusted)?;
    let mut stale = routed;
    stale.revision = 2;
    assert_eq!(
        fixture.transport.install_query_group(stale),
        Err(TransportError::StalePeer)
    );
    let clone = Publisher::new(publisher.peer.clone()).await?;
    let impostor = clone.view().await?;
    assert_ne!(impostor.storage_instance(), current.storage_instance());
    assert_eq!(
        fixture.transport.accept_node_view(impostor, &trusted),
        Err(TransportError::Identity)
    );
    {
        let r = fixture.transport.handle.0.registry.lock().unwrap();
        assert_eq!(r.node_views[&trusted.node], current);
    }
    clone.close().await?;
    publisher.close().await?;
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn process_observation_and_seed_epochs_have_fixed_node_limits() -> anyhow::Result<()> {
    let fixture = Fixture::start(
        TransportOptions {
            max_peers: 1,
            ..Default::default()
        },
        vec![],
    )
    .await?;
    let mut publishers = vec![];
    for id in 2..=4 {
        publishers.push(Publisher::new(peer(id)?).await?);
    }
    for publisher in &publishers[..2] {
        fixture
            .transport
            .accept_node_view(publisher.view().await?, &publisher.peer)?;
    }
    let rejected = publishers[2].view().await?;
    assert_eq!(
        fixture
            .transport
            .accept_node_view(rejected, &publishers[2].peer),
        Err(TransportError::Overloaded)
    );
    let state = &fixture.transport.handle.0;
    let epoch = state.seed_peer_epoch(&publishers[0].peer)?;
    assert!(!*epoch.borrow());
    assert!(matches!(
        state.seed_peer_epoch(&publishers[1].peer),
        Err(TransportError::Overloaded)
    ));
    assert_eq!(fixture.transport.usage().peers, 0);
    assert!(fixture.log.groups()?.is_empty());
    for publisher in publishers {
        publisher.close().await?;
    }
    fixture.close().await
}

#[path = "transport_inspection_tests.rs"]
mod inspection;
