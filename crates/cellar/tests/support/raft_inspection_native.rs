// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Local inspection uses real SDK producers and dormant physical storage.
use super::*;
use cellar::sdk::{consensus::*, ShutdownMode};
use futures_util::TryStreamExt as _;

fn options() -> RaftHostOptions {
    let mut options = RaftHostOptions::default();
    let reservation = options.images.max_bytes;
    options.node_profile = Some(RaftNodeProfile {
        pool: "native".into(),
        region: "local".into(),
        zone: "a".into(),
        group_capacity: 2,
        byte_capacity: reservation * 2,
        bytes_per_group: reservation,
        leader_capacity: 2,
        draining: false,
    });
    options
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn member_inspection_preserves_published_cut_while_callback_is_private() -> Result<()> {
    let (gate, mut entered) = public_node::Gate::new();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let definition = public_node::counter(calls.clone(), gate.clone())?;
    let root = tempfile::tempdir()?;
    let node = public_node::node(root.path(), definition.application.clone()).await?;
    let mut namespace = config(2, NamespaceMode::Raft, true);
    namespace.schema_digest = definition.schema;
    let registration = RaftCellRegistration {
        placement: RaftCellPlacement::Replica,
        namespace,
        class: "Counter".into(),
        birth: proposal(2, "Counter:inspection", 5, 1),
    };
    let service = public_node::cells_with_host(
        &node,
        definition.application.clone(),
        Arc::new(object_store::memory::InMemory::new()),
        registration.clone(),
        false,
        options(),
    )
    .await?;
    let cell = service.cell(
        registration.namespace.namespace,
        &registration.birth.scope.cell,
    )?;
    let release = public_node::Release(gate);
    public_node::ready(&cell).await?;
    let first = cell
        .command(
            definition.increment.clone(),
            1_i64.to_be_bytes().to_vec(),
            public_node::archived_options(1)?,
        )
        .await?;
    let second = cell.command(
        definition.increment,
        1_i64.to_be_bytes().to_vec(),
        public_node::options(2)?,
    );
    tokio::time::timeout(Duration::from_secs(5), entered.recv())
        .await?
        .context("held callback did not enter")?;
    let observed = cell.inspect().await?;
    assert_eq!(observed.scope, registration.birth.scope);
    assert_eq!(observed.schema_digest, definition.schema);
    assert_eq!(observed.node.identity.node, NodeId::new([1; 16])?);
    assert_eq!(observed.node.groups, 1);
    assert_eq!(
        observed.node.reserved_bytes,
        observed.node.profile.bytes_per_group
    );
    assert_eq!(
        observed.occupancy,
        ReplicaOccupancy::Member(MemberId::new(1)?)
    );
    let member = observed
        .member
        .as_ref()
        .context("member observation missing")?;
    assert_eq!(member.published, Some(first.version.position));
    assert_eq!(member.applied, first.version.position);
    assert_eq!(member.committed, first.version.position.index);
    assert_eq!(calls.load(Ordering::Acquire), 2);
    drop(release);
    let second = second.await?;
    let observed = cell.inspect().await?;
    assert_eq!(
        observed.member.as_ref().and_then(|member| member.published),
        Some(second.version.position)
    );
    assert_eq!(
        cell.query(
            definition.get,
            vec![],
            QueryOptions {
                consistency: QueryConsistency::AtLeast(second.version_token),
                ..Default::default()
            }
        )
        .await?
        .result,
        2_i64.to_be_bytes()
    );
    ensure!(service.close().await?.unfinished.is_empty());
    node.shutdown(ShutdownMode::Preserve).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn standby_inspection_creates_no_member_or_group() -> Result<()> {
    let (gate, _entered) = public_node::Gate::new();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let definition = public_node::counter(calls.clone(), gate)?;
    let root = tempfile::tempdir()?;
    let node = public_node::node(root.path(), definition.application.clone()).await?;
    let store = Arc::new(object_store::memory::InMemory::new());
    let mut namespace = config(2, NamespaceMode::Raft, true);
    namespace.schema_digest = definition.schema;
    let mut birth = proposal(2, "Counter:standby-inspection", 5, 1);
    birth.voters[0].node = NodeId::new([2; 16])?;
    let registration = RaftCellRegistration {
        placement: RaftCellPlacement::Standby,
        namespace,
        birth,
        class: "Counter".into(),
    };
    let catalog = Catalog::new(
        store.clone(),
        CatalogOptions {
            prefix: "host/test".into(),
            ..Default::default()
        },
    )?;
    catalog
        .open_namespace(registration.namespace.clone())
        .await?
        .resolve_birth(registration.birth.clone())
        .await?;
    let service = public_node::cells_with_host(
        &node,
        definition.application,
        store.clone(),
        registration.clone(),
        false,
        options(),
    )
    .await?;
    let cell = service.cell(
        registration.namespace.namespace,
        &registration.birth.scope.cell,
    )?;
    let observed = cell.inspect().await?;
    assert_eq!(observed.occupancy, ReplicaOccupancy::Standby);
    assert!(observed.member.is_none());
    assert_eq!(observed.node.groups, 0);
    assert_eq!(observed.node.reserved_bytes, 0);
    ensure!(matches!(
        cell.status().await,
        Err(CommandFailure::UnsupportedCapability)
    ));
    ensure!(matches!(
        cell.command(
            definition.increment,
            1_i64.to_be_bytes().to_vec(),
            public_node::options(1)?
        )
        .await,
        Err(CommandFailure::UnsupportedCapability)
    ));
    assert_eq!(calls.load(Ordering::Acquire), 0);
    let objects = store.list(None).try_collect::<Vec<_>>().await?;
    assert!(
        objects
            .iter()
            .all(|object| !object.location.as_ref().contains("/claims/")),
        "inspection cannot claim an identity"
    );
    ensure!(service.close().await?.unfinished.is_empty());
    node.shutdown(ShutdownMode::Preserve).await?;
    Ok(())
}
