//! Rooted trust is distinct from directory discovery and placement eligibility.
use super::*;
use crate::bucket::{Bucket, StorageBackend};
use crate::raft::fleet_catalog::*;
use crate::raft::transport_options::NamespaceInspectionOrigin;
use object_store::list::*;

const TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug)]
struct NoListing;

#[async_trait::async_trait]
impl PaginatedListStore for NoListing {
    async fn list_paginated(
        &self,
        _: Option<&str>,
        _: PaginatedListOptions,
    ) -> object_store::Result<PaginatedListResult> {
        Err(object_store::Error::NotSupported {
            source: "no fleet listing".into(),
        })
    }
}

async fn catalog(fixture: &Fixture) -> anyhow::Result<FleetCatalog> {
    catalog_at(fixture, "fleet/").await
}

async fn catalog_at(fixture: &Fixture, prefix: &str) -> anyhow::Result<FleetCatalog> {
    let store = Arc::new(object_store::memory::InMemory::new());
    let bucket = Bucket::from_clients(
        store.clone(),
        store,
        Arc::new(NoListing),
        StorageBackend::S3,
        "test".into(),
        "".into(),
    )?;
    let catalog = FleetCatalog::new(
        &fixture.domain,
        bucket,
        ClusterId::new([1; 16])?,
        IncarnationId::new([80; 16])?,
        prefix,
    )?;
    catalog.initialize(TIMEOUT).await?;
    Ok(catalog)
}

async fn register(
    catalog: &FleetCatalog,
    peer: &RaftPeerNode,
    instance: cellar_logic::consensus::EngineInstanceId,
    id: u8,
) -> anyhow::Result<FleetHostAuthorization> {
    let snapshot = catalog.read(TIMEOUT).await?;
    let commit = catalog
        .register_node(
            &snapshot,
            &FleetRequestIdentity {
                id: [id; 16],
                operation: [100; 16],
                principal: "fleet-test".into(),
                expected_revision: snapshot.revision(),
            },
            &FleetNodeSpec {
                node: peer.node,
                disk: peer.disk,
                physical_host: IncarnationId::new([id; 16])?,
                label: peer.label.clone(),
                server_name: peer.server_name.clone(),
                capability_digest: [9; 32],
                storage_instance: Some(instance),
            },
            TIMEOUT,
        )
        .await?;
    Ok(catalog
        .authorize_host(&commit.snapshot, peer.node, TIMEOUT)
        .await?)
}

async fn change(
    catalog: &FleetCatalog,
    node: NodeId,
    id: u8,
    edit: impl FnOnce(&mut serde_json::Value),
) -> anyhow::Result<()> {
    let snapshot = catalog.read(TIMEOUT).await?;
    let key = hex::encode(node.bytes());
    let bytes = catalog
        .get(&snapshot, FleetTable::Nodes, &key, TIMEOUT)
        .await?
        .unwrap();
    let mut record: serde_json::Value = serde_json::from_slice(&bytes[13..bytes.len() - 32])?;
    edit(&mut record);
    // Preserve the strict canonical NodeRecord field order while changing its
    // contents. The serde_json map's ordering is not the stored struct order.
    let original: serde_json::Value = serde_json::from_slice(&bytes[13..bytes.len() - 32])?;
    let mut payload = std::str::from_utf8(&bytes[13..bytes.len() - 32])?.to_owned();
    for (field, value) in record.as_object().unwrap() {
        if value != &original[field] {
            payload = payload.replace(
                &format!("\"{field}\":{}", serde_json::to_string(&original[field])?),
                &format!("\"{field}\":{}", serde_json::to_string(value)?),
            );
        }
    }
    use sha2::{Digest, Sha256};
    let mut value = bytes[..9].to_vec();
    value.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    value.extend_from_slice(payload.as_bytes());
    value.extend_from_slice(&Sha256::digest(&value));
    catalog
        .commit(
            &snapshot,
            &FleetMutation {
                id: [id; 16],
                operation: [100; 16],
                principal: "fleet-test".into(),
                expected_revision: snapshot.revision(),
                changes: vec![FleetChange {
                    table: FleetTable::Nodes,
                    key,
                    value: Some(value),
                }],
            },
            TIMEOUT,
        )
        .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rooted_trust_rejects_unenrolled_views_and_changed_pins() -> anyhow::Result<()> {
    let fixture = Fixture::start(TransportOptions::default(), vec![]).await?;
    let catalog = catalog(&fixture).await?;
    let own = peer(1)?;
    let authorization = register(&catalog, &own, fixture.log.storage_instance()?, 11).await?;
    let first = authorization.trust_snapshot(TIMEOUT).await?;
    fixture.transport.install_fleet_trust(first.clone())?;
    let foreign = catalog_at(&fixture, "foreign/").await?;
    let other = register(&foreign, &own, fixture.log.storage_instance()?, 11)
        .await?
        .trust_snapshot(TIMEOUT)
        .await?;
    assert_ne!(first.catalog(), other.catalog());
    assert!(authorization
        .refresh_trust(Some(&other), TIMEOUT)
        .await
        .is_err());
    assert_eq!(
        fixture.transport.install_fleet_trust(other),
        Err(TransportError::StalePeer)
    );
    let publisher = Publisher::new(peer(2)?).await?;
    let view = publisher.view().await?;
    assert_eq!(
        fixture
            .transport
            .accept_node_view(view.clone(), &publisher.peer),
        Err(TransportError::Refused)
    );
    assert!(matches!(
        fixture.transport.handle.0.seed_peer_epoch(&publisher.peer),
        Err(TransportError::Refused)
    ));
    let query = NamespaceQueryOrigin {
        cluster: scope(2)?.cluster,
        namespace: scope(2)?.namespace,
        revision: 1,
        peer: publisher.peer.clone(),
    };
    assert_eq!(
        fixture.transport.grant_query_origin(query.clone()),
        Err(TransportError::Refused)
    );
    assert_eq!(
        fixture
            .transport
            .grant_inspection_origin(NamespaceInspectionOrigin {
                cluster: query.cluster,
                namespace: query.namespace,
                revision: 1,
                peer: publisher.peer.clone(),
            }),
        Err(TransportError::Refused)
    );
    register(
        &catalog,
        &publisher.peer,
        publisher.log.storage_instance()?,
        12,
    )
    .await?;
    let next = authorization.trust_snapshot(TIMEOUT).await?;
    fixture.transport.install_fleet_trust(next.clone())?;
    let registration = publisher
        .namespace
        .register_node_storage(&fixture.log, TIMEOUT)
        .await?;
    let discovered = registration
        .observe_fleet_node(next.node(publisher.peer.node).unwrap(), TIMEOUT)
        .await?
        .unwrap();
    assert_eq!(discovered, view);
    fixture
        .transport
        .accept_node_view(view.clone(), &publisher.peer)?;
    fixture.transport.grant_query_origin(query)?;
    let same = authorization.refresh_trust(Some(&next), TIMEOUT).await?;
    fixture.transport.install_fleet_trust(same)?;
    assert_eq!(
        fixture.transport.install_fleet_trust(first),
        Err(TransportError::StalePeer)
    );
    change(&catalog, publisher.peer.node, 13, |record| {
        record["storage_instance"] = serde_json::json!(vec![99; 16]);
        record["epoch"] = serde_json::json!(2);
    })
    .await?;
    let wrong_pin = authorization.trust_snapshot(TIMEOUT).await?;
    assert_eq!(
        fixture.transport.install_fleet_trust(wrong_pin),
        Err(TransportError::StalePeer)
    );
    {
        let registry = fixture.transport.handle.0.registry.lock().unwrap();
        assert_eq!(
            registry.fleet_trust.as_ref().unwrap().revision(),
            next.revision()
        );
        assert_eq!(registry.node_views[&publisher.peer.node], view);
    }
    publisher.close().await?;
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rooted_removal_cancels_pools_origins_and_seed_owners() -> anyhow::Result<()> {
    let scope = scope(2)?;
    let fixture = Fixture::start(
        TransportOptions::default(),
        vec![crate::raft::storage::GroupBootstrap {
            scope: scope.clone(),
            member: MemberId::new(1)?,
            birth_digest: [10; 32],
            voters: (1..=3).map(MemberId::new).collect::<Result<_, _>>()?,
            learners: vec![],
        }],
    )
    .await?;
    let catalog = catalog(&fixture).await?;
    let own = register(&catalog, &peer(1)?, fixture.log.storage_instance()?, 11).await?;
    let publisher = Publisher::new(peer(2)?).await?;
    let third = Publisher::new(peer(3)?).await?;
    register(
        &catalog,
        &publisher.peer,
        publisher.log.storage_instance()?,
        12,
    )
    .await?;
    register(&catalog, &third.peer, third.log.storage_instance()?, 13).await?;
    let first = own.trust_snapshot(TIMEOUT).await?;
    fixture.transport.install_fleet_trust(first.clone())?;
    let view = publisher.view().await?;
    fixture
        .transport
        .accept_node_view(view.clone(), &publisher.peer)?;
    let route = route(&fixture, &scope, false)?;
    fixture.transport.install_group(route.clone())?;
    let query = NamespaceQueryOrigin {
        cluster: scope.cluster,
        namespace: scope.namespace,
        revision: 1,
        peer: publisher.peer.clone(),
    };
    fixture.transport.grant_query_origin(query.clone())?;
    let inspection = NamespaceInspectionOrigin {
        cluster: scope.cluster,
        namespace: scope.namespace,
        revision: 1,
        peer: publisher.peer.clone(),
    };
    fixture
        .transport
        .grant_inspection_origin(inspection.clone())?;
    let state = fixture.transport.handle.0.clone();
    let epoch = state.seed_peer_epoch(&publisher.peer)?;
    let (pool, grant, inspect) = {
        let r = state.registry.lock().unwrap();
        (
            r.pools[&publisher.peer.node].clone(),
            r.origins[&(scope.cluster, scope.namespace, publisher.peer.node)].clone(),
            r.inspection_origins[&(scope.cluster, scope.namespace, publisher.peer.node)].clone(),
        )
    };
    for (i, mode) in [
        FleetNodeState::RestartPending,
        FleetNodeState::ResizePending,
        FleetNodeState::Draining,
        FleetNodeState::Evacuated,
    ]
    .into_iter()
    .enumerate()
    {
        change(&catalog, publisher.peer.node, 20 + i as u8, |record| {
            record["state"] = serde_json::to_value(mode).unwrap();
            record["operation"] = serde_json::json!(vec![100; 16]);
            record["epoch"] = serde_json::json!(2 + i);
        })
        .await?;
        fixture
            .transport
            .install_fleet_trust(own.trust_snapshot(TIMEOUT).await?)?;
        assert!(!*epoch.borrow());
        assert!(!*pool.stop.borrow());
        assert!(!*grant.stop.borrow());
        assert!(!*inspect.stop.borrow());
        assert!(!mode.eligibility().receive_replicas);
    }
    let (entered, started) = oneshot::channel();
    let worker = state.clone();
    let captured = epoch.clone();
    let (done, joined) = oneshot::channel();
    let deadline = state.domain.mono_ms()? + 5000;
    state.spawn(async move {
        let result = worker
            .seed_process_until(&captured, deadline, async {
                let _ = entered.send(());
                std::future::pending::<Result<(), TransportError>>().await
            })
            .await;
        let _ = done.send(result);
    })?;
    started.await?;
    change(&catalog, publisher.peer.node, 30, |record| {
        record["state"] = serde_json::json!("Removed");
        record["epoch"] = serde_json::json!(6);
    })
    .await?;
    fixture
        .transport
        .install_fleet_trust(own.trust_snapshot(TIMEOUT).await?)?;
    assert_eq!(joined.await?, Err(TransportError::StalePeer));
    assert!(
        *epoch.borrow() && *pool.stop.borrow() && *grant.stop.borrow() && *inspect.stop.borrow()
    );
    assert_eq!(
        fixture.transport.accept_node_view(view, &publisher.peer),
        Err(TransportError::Refused)
    );
    assert_eq!(
        fixture.transport.install_group(route),
        Err(TransportError::Refused)
    );
    assert_eq!(
        fixture.transport.grant_query_origin(query),
        Err(TransportError::Refused)
    );
    assert_eq!(
        fixture.transport.grant_inspection_origin(inspection),
        Err(TransportError::Refused)
    );
    assert!(matches!(
        state.seed_peer_epoch(&publisher.peer),
        Err(TransportError::Refused)
    ));
    assert_eq!(
        fixture.transport.install_fleet_trust(first),
        Err(TransportError::StalePeer)
    );
    {
        let r = state.registry.lock().unwrap();
        assert!(!r.pools.contains_key(&publisher.peer.node));
        assert!(
            r.origins[&(scope.cluster, scope.namespace, publisher.peer.node)]
                .peer
                .is_none()
        );
        assert!(
            r.inspection_origins[&(scope.cluster, scope.namespace, publisher.peer.node)]
                .peer
                .is_none()
        );
    }
    fixture.transport.close().await;
    assert_eq!(fixture.transport.usage().tasks, 0);
    assert_eq!(fixture.transport.usage().control_bytes, 0);
    assert_eq!(fixture.transport.usage().data_bytes, 0);
    publisher.close().await?;
    third.close().await?;
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rooted_boot_refresh_restores_only_retained_routes() -> anyhow::Result<()> {
    let scope = scope(2)?;
    let fixture = Fixture::start(
        TransportOptions::default(),
        vec![crate::raft::storage::GroupBootstrap {
            scope: scope.clone(),
            member: MemberId::new(1)?,
            birth_digest: [10; 32],
            voters: (1..=3).map(MemberId::new).collect::<Result<_, _>>()?,
            learners: vec![],
        }],
    )
    .await?;
    let catalog = catalog(&fixture).await?;
    let own = register(&catalog, &peer(1)?, fixture.log.storage_instance()?, 11).await?;
    let mut publisher = Publisher::new(peer(2)?).await?;
    let target = register(
        &catalog,
        &publisher.peer,
        publisher.log.storage_instance()?,
        12,
    )
    .await?
    .bind_boot(
        publisher.log.storage_instance()?,
        publisher.peer.process,
        cellar_logic::consensus::ProposalId::new([21; 16])?,
        TIMEOUT,
    )
    .await?;
    let third = Publisher::new(peer(3)?).await?;
    register(&catalog, &third.peer, third.log.storage_instance()?, 13).await?;
    fixture
        .transport
        .install_fleet_trust(own.trust_snapshot(TIMEOUT).await?)?;
    let old_view = publisher.view().await?;
    fixture
        .transport
        .accept_node_view(old_view.clone(), &publisher.peer)?;
    // Discovery alone must not create a business connection pool.
    assert!(!fixture
        .transport
        .handle
        .0
        .registry
        .lock()
        .unwrap()
        .pools
        .contains_key(&publisher.peer.node));
    fixture
        .transport
        .install_group(route(&fixture, &scope, false)?)?;
    fixture.transport.grant_query_origin(NamespaceQueryOrigin {
        cluster: scope.cluster,
        namespace: scope.namespace,
        revision: 1,
        peer: publisher.peer.clone(),
    })?;
    let key = (scope.cluster, scope.namespace, publisher.peer.node);
    let state = &fixture.transport.handle.0;
    let (old_pool, old_grant) = {
        let registry = state.registry.lock().unwrap();
        (
            registry.pools[&publisher.peer.node].clone(),
            registry.origins[&key].clone(),
        )
    };
    publisher.restart().await?;
    target
        .bind_boot(
            publisher.log.storage_instance()?,
            publisher.peer.process,
            cellar_logic::consensus::ProposalId::new([22; 16])?,
            TIMEOUT,
        )
        .await?;
    fixture
        .transport
        .install_fleet_trust(own.trust_snapshot(TIMEOUT).await?)?;
    assert!(*old_pool.stop.borrow() && *old_grant.stop.borrow());
    assert!(!state
        .registry
        .lock()
        .unwrap()
        .pools
        .contains_key(&publisher.peer.node));
    assert_eq!(
        fixture
            .transport
            .accept_node_view(old_view.clone(), old_view.peer()),
        Err(TransportError::Refused)
    );
    fixture
        .transport
        .accept_node_view(publisher.view().await?, &publisher.peer)?;
    {
        let registry = state.registry.lock().unwrap();
        let pool = &registry.pools[&publisher.peer.node];
        assert!(!Arc::ptr_eq(pool, &old_pool) && !*pool.stop.borrow());
        assert_eq!(pool.peer, publisher.peer);
        let grant = &registry.origins[&key];
        assert!(!Arc::ptr_eq(grant, &old_grant) && !*grant.stop.borrow());
        assert_eq!(grant.peer.as_ref(), Some(&publisher.peer));
        assert_eq!(grant.revision, old_grant.revision);
        let group = &registry.groups[&scope];
        assert_eq!(group.revision, 1);
        assert_eq!(
            group
                .peers
                .iter()
                .find(|peer| peer.replica.node == publisher.peer.node)
                .unwrap()
                .replica
                .process,
            publisher.peer.process
        );
        assert!(!registry.pools.contains_key(&peer(1)?.node));
    }
    publisher.close().await?;
    third.close().await?;
    fixture.close().await
}
