// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real engine/registration ownership, not quorum or network-stream qualification.
use super::*;
use crate::raft::transport::shared_fixture::Fixture;
use crate::raft::{cell_groups::*, storage::CellGroupBootstrap};
use cellar_logic::{
    cell_group::{CellRange, RouteEpoch},
    consensus::{DiskLineage, GroupId, IncarnationId, MemberId, ProcessGeneration},
};

async fn fixture() -> anyhow::Result<(
    Fixture,
    GroupRoute<GroupHistory>,
    CellGroupApplier,
    tempfile::TempDir,
)> {
    let history = GroupHistory {
        cluster: ClusterId::new([1; 16])?,
        namespace: NamespaceId::new([31; 16])?,
        group: GroupId::new([32; 16])?,
        incarnation: IncarnationId::new([33; 16])?,
    };
    let fixture = Fixture::start(
        TransportOptions::default(),
        vec![CellGroupBootstrap {
            route: cellar_logic::cell_group::GroupRoute {
                history,
                range: CellRange::all(),
                epoch: RouteEpoch::new(1)?,
                birth_digest: [10; 32],
                schema_digest: [9; 32],
            },
            member: MemberId::new(1)?,
            voters: (1..=3).map(MemberId::new).collect::<Result<Vec<_>, _>>()?,
            learners: vec![],
        }],
    )
    .await?;
    let peers = (1..=3)
        .map(|member| {
            Ok(RaftPeer {
                replica: ReplicaIdentity {
                    scope: history,
                    member: MemberId::new(member)?,
                    node: NodeId::new([member as u8; 16])?,
                    disk: DiskLineage::new([member as u8 + 1; 16])?,
                    process: ProcessGeneration::new([member as u8 + 2; 16])?,
                },
                label: format!("node-{member}"),
                server_name: "localhost".into(),
                control_address: if member == 1 {
                    fixture.transport.control_address()
                } else {
                    format!("127.0.0.1:{}", 42000 + member * 3).parse()?
                },
                data_address: if member == 1 {
                    fixture.transport.data_address()
                } else {
                    format!("127.0.0.1:{}", 42001 + member * 3).parse()?
                },
                query_address: if member == 1 {
                    fixture.transport.query_address()
                } else {
                    format!("127.0.0.1:{}", 42002 + member * 3).parse()?
                },
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let group = fixture.log.cell_group(&history)?;
    let directory = tempfile::tempdir()?;
    let base = directory.path().to_owned();
    let (applier, current) = fixture
        .domain
        .blocking(move || {
            let images = cellar_ltx::generation::GenerationStore::open(
                base.join("images"),
                Arc::new(cellar_ltx::DirectFileSystem),
                Default::default(),
            )?;
            let pages = ManifestPageStore::open(
                &base.join("pages"),
                Arc::new(cellar_ltx::DirectFileSystem),
                1024,
                16 * 1024 * 1024,
                16,
            )?;
            Ok::<_, anyhow::Error>(CellGroupApplier::recover(
                group,
                pages,
                CellGroupApplyImages::Materialized {
                    images,
                    services: cellar_ltx::generation::ReadServices {
                        now_unix_millis: Arc::new(crate::asyncrt::wall_ms),
                        fill_random: Arc::new(|bytes| bytes.fill(1)),
                    },
                },
                ManifestLimits::default(),
                crate::application::consensus::group_protocol::GroupEntryLimits::default(),
                100,
            )?)
        })?
        .await??;
    drop(current);
    // Keep the directory until every real page/image owner has been released.
    fixture
        .transport
        .enable_group_checkpoints(CellGroupCheckpointPool::new(CellGroupCheckpointLimits {
            max_sources: 1,
            max_retained_bytes: 32 * 1024 * 1024,
            max_cells: 64,
            max_transfer_bytes: 32 * 1024 * 1024,
            max_chunk_bytes: 4093,
        })?)?;
    Ok((
        fixture,
        GroupRoute {
            scope: history,
            schema_digest: [9; 32],
            revision: 1,
            peers,
        },
        applier,
        directory,
    ))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn advisory_refresh_keeps_factory_and_physical_owner_but_revokes_old_stream(
) -> anyhow::Result<()> {
    let (fixture, route, applier, _directory) = fixture().await?;
    let registration = fixture.transport.register_physical_group(route.clone())?;
    let checkpoint = fixture
        .transport
        .register_group_checkpoint(applier.clone())?;
    let state = &fixture.transport.handle.0;
    let (old, _) = state.checkpoint_endpoint(&route.scope)?;
    let owner = state.registry.lock().unwrap().physical_owners[&route.scope].clone();
    let mut refreshed = route.clone();
    refreshed.revision += 1;
    fixture
        .transport
        .install_physical(refreshed, false, Some(owner))?;
    let wire = Route::new_group(
        &route.peers[1].replica,
        &route.peers[1].label,
        &route.peers[0].replica,
        &route.peers[0].label,
        route.schema_digest,
    )?;
    assert!(matches!(
        state.checkpoint_current(&old, &wire),
        Err(TransportError::StalePeer)
    ));
    let (current, _) = state.checkpoint_endpoint(&route.scope)?;
    assert!(Arc::ptr_eq(&old.registration, &current.registration));
    assert!(state.checkpoint_current(&current, &wire).is_ok());
    drop(registration);
    assert!(!current.active.load(Ordering::Acquire));
    assert!(!state
        .registry
        .lock()
        .unwrap()
        .physical_groups
        .contains_key(&route.scope));
    assert!(!state
        .registry
        .lock()
        .unwrap()
        .physical_owners
        .contains_key(&route.scope));
    assert!(state.checkpoint_endpoint(&route.scope).is_err());
    drop((checkpoint, applier, old, current));
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explicit_route_replacement_is_not_owned_by_old_registration() -> anyhow::Result<()> {
    let (fixture, route, applier, _directory) = fixture().await?;
    let old_registration = fixture.transport.register_physical_group(route.clone())?;
    let old_checkpoint = fixture
        .transport
        .register_group_checkpoint(applier.clone())?;
    let state = &fixture.transport.handle.0;
    let old_owner = state.registry.lock().unwrap().physical_owners[&route.scope].clone();
    let mut replacement = route.clone();
    replacement.revision += 1;
    fixture
        .transport
        .install_physical_group(replacement.clone())?;
    let checkpoint = fixture
        .transport
        .register_group_checkpoint(applier.clone())?;
    assert!(!old_checkpoint.endpoint.active.load(Ordering::Acquire));
    let mut stale_refresh = replacement.clone();
    stale_refresh.revision += 1;
    assert!(matches!(
        fixture
            .transport
            .install_physical(stale_refresh, false, Some(old_owner)),
        Err(TransportError::StalePeer)
    ));
    drop(old_registration);
    drop(old_checkpoint);
    let (current, _) = state.checkpoint_endpoint(&route.scope)?;
    assert_eq!(*current.route, replacement);
    fixture.transport.remove_physical_group(&route.scope)?;
    assert!(!current.active.load(Ordering::Acquire));
    drop((checkpoint, applier, current));
    fixture.close().await
}
