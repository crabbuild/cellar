// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Storage preparation tests; no marker here grants catalog or quorum authority.
use super::super::*;
use crate::application::consensus::group_protocol::{
    CellGroupEntry, GroupCommand, GroupEntryBinding, GroupEntryLimits,
};
use cellar_logic::{cell_group::*, consensus::*};
use cellar_ltx::{
    generation::{GenerationStore, ReadServices},
    DirectFileSystem,
};
use raft::prelude::Entry;
use std::sync::Arc;

fn route() -> GroupRoute {
    GroupRoute {
        history: GroupHistory {
            cluster: ClusterId::new([1; 16]).unwrap(),
            namespace: NamespaceId::new([2; 16]).unwrap(),
            group: GroupId::new([3; 16]).unwrap(),
            incarnation: IncarnationId::new([4; 16]).unwrap(),
        },
        range: CellRange::all(),
        epoch: RouteEpoch::new(2).unwrap(),
        birth_digest: [5; 32],
        schema_digest: [6; 32],
    }
}

fn operation() -> ProposalId {
    ProposalId::new([7; 16]).unwrap()
}

fn position(index: u64) -> LogPosition {
    LogPosition {
        index: LogIndex::new(index),
        term: Term::new(2),
    }
}

fn pending() -> GroupManifest {
    GroupManifest::recovery_genesis(route(), ManifestRoot::default(), [8; 32], operation()).unwrap()
}

#[test]
fn recovery_marker_requires_explicit_lifecycle_and_valid_activation() {
    let marker = pending();
    assert!(!marker.is_active());
    let value = serde_json::to_value(&marker).unwrap();
    assert_eq!(
        serde_json::from_value::<GroupManifest>(value.clone()).unwrap(),
        marker
    );
    let mut malformed = value.clone();
    malformed.as_object_mut().unwrap().remove("recovery");
    assert!(serde_json::from_value::<GroupManifest>(malformed).is_err());
    let mut malformed = value.clone();
    malformed["recovery"]
        .as_object_mut()
        .unwrap()
        .remove("activation");
    assert!(serde_json::from_value::<GroupManifest>(malformed).is_err());
    for invalid in [
        serde_json::json!({"publication":vec![0; 32],"manifest":vec![9; 32],"index":2,"term":2}),
        serde_json::json!({"publication":vec![9; 32],"manifest":vec![9; 32],"index":1,"term":1}),
        serde_json::json!({"publication":vec![9; 32],"manifest":vec![9; 32],"index":3,"term":2}),
        serde_json::json!({"publication":vec![9; 32],"manifest":vec![9; 32],"index":2,"term":1}),
    ] {
        let mut malformed = value.clone();
        malformed["recovery"]["activation"] = invalid;
        assert!(serde_json::from_value::<GroupManifest>(malformed).is_err());
    }
    let ordinary = GroupManifest::genesis(route()).unwrap();
    assert!(ordinary.is_active());
    assert_eq!(
        serde_json::to_value(ordinary).unwrap()["recovery"],
        serde_json::Value::Null
    );
    let mut invalid = pending();
    invalid.position = position(2);
    invalid.membership_revision = 2;
    assert!(invalid.check().is_err());
}

#[test]
fn recovery_commitment_is_bound_and_activation_cannot_be_replaced() {
    let mut marker = pending();
    let expected = marker
        .recovery
        .as_ref()
        .unwrap()
        .seed_commitment(&marker)
        .unwrap();
    let original = marker.clone();
    marker.position = position(2);
    assert_eq!(
        marker
            .recovery
            .as_ref()
            .unwrap()
            .seed_commitment(&marker)
            .unwrap(),
        expected
    );
    let foreign = super::RecoveryState::pending([11; 32], operation()).unwrap();
    assert!(foreign.seed_commitment(&marker).is_err());
    let transition = TransitionId::new(operation().bytes()).unwrap();
    let state = marker.recovery.as_mut().unwrap();
    for (id, publication, manifest) in [
        (TransitionId::new([12; 16]).unwrap(), [13; 32], expected),
        (transition, [0; 32], expected),
        (transition, [13; 32], [14; 32]),
    ] {
        assert!(state
            .activate(id, publication, manifest, expected, position(2))
            .is_err());
        assert!(!state.active());
    }
    state
        .activate(transition, [13; 32], expected, expected, position(2))
        .unwrap();
    let activated = state.clone();
    state
        .activate(transition, [13; 32], expected, expected, position(3))
        .unwrap();
    assert_eq!(*state, activated);
    assert!(state
        .activate(transition, [15; 32], expected, expected, position(3))
        .is_err());
    assert!(marker.is_active());
    assert!(marker.preserves_recovery(&original));
    assert!(!original.preserves_recovery(&marker));
    assert!(!GroupManifest::genesis(route())
        .unwrap()
        .preserves_recovery(&marker));
    assert!(!marker.preserves_recovery(&GroupManifest::genesis(route()).unwrap()));
    marker.check().unwrap();
}

fn activation(marker: &GroupManifest, index: u64, manifest: [u8; 32]) -> Entry {
    let bytes = CellGroupEntry {
        binding: GroupEntryBinding {
            group: marker.route().history,
            route_epoch: marker.route().epoch,
            birth_digest: marker.route().birth_digest,
            schema_digest: marker.route().schema_digest,
            proposal: operation(),
        },
        command: GroupCommand::Activate {
            transition: TransitionId::new(operation().bytes()).unwrap(),
            publication: [9; 32],
            manifest,
        },
    }
    .encode(marker.route(), GroupEntryLimits::default())
    .unwrap();
    Entry {
        index,
        term: 2,
        data: bytes.into(),
        ..Default::default()
    }
}

#[test]
fn recovery_apply_requires_exact_activation_and_keeps_noop_prefix_closed() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let pages = ManifestPageStore::open(
        &root.path().join("pages"),
        Arc::new(DirectFileSystem),
        128,
        1024 * 1024,
        16,
    )?;
    let images = CellGroupApplyImages::Materialized {
        images: GenerationStore::open(
            root.path().join("images"),
            Arc::new(DirectFileSystem),
            Default::default(),
        )?,
        services: ReadServices {
            now_unix_millis: Arc::new(|| 10),
            fill_random: Arc::new(|bytes| bytes.fill(17)),
        },
    };
    let mut storage = apply::Storage::new(
        route(),
        pages,
        images,
        Default::default(),
        Default::default(),
        100,
    )?;
    let marker = pending();
    let commitment = marker.recovery.as_ref().unwrap().seed_commitment(&marker)?;
    let owner = root::ArchiveManifestOwner::new();
    let base =
        storage
            .tree
            .prepare_archive_complete(&owner, &mut storage.pages, marker, &mut |_| {
                unreachable!("empty root")
            })?;
    let noop = storage.prepare_entry(
        &base,
        &Entry {
            index: 2,
            term: 2,
            ..Default::default()
        },
        None,
        false,
    )?;
    assert!(!noop.marker().is_active());
    assert_eq!(
        noop.marker().recovery_request(),
        base.marker().recovery_request()
    );
    assert!(storage
        .prepare_entry(&noop, &activation(noop.marker(), 3, [99; 32]), None, false)
        .is_err());
    assert!(!noop.marker().is_active());
    let active = storage.prepare_entry(
        &noop,
        &activation(noop.marker(), 3, commitment),
        None,
        false,
    )?;
    assert!(active.marker().is_active());
    assert_eq!(active.marker().root(), base.marker().root());
    assert!(active.marker().preserves_recovery(base.marker()));
    let duplicate = storage.prepare_entry(
        &active,
        &activation(active.marker(), 4, commitment),
        None,
        false,
    )?;
    assert_eq!(duplicate.marker().recovery, active.marker().recovery);
    let ordinary = storage.tree.prepare_archive_complete(
        &owner,
        &mut storage.pages,
        GroupManifest::genesis(route())?,
        &mut |_| unreachable!("empty root"),
    )?;
    assert!(storage
        .prepare_entry(
            &ordinary,
            &activation(ordinary.marker(), 2, commitment),
            None,
            false
        )
        .is_err());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovery_pending_root_denies_stale_and_execution_authorities() -> anyhow::Result<()> {
    use crate::{
        asyncrt::ExecutionDomain,
        raft::{
            budget::Budget,
            options::{LogOpenMode, ReplicaLogOptions},
            storage::{CellGroupBootstrap, ReplicaLog},
        },
    };
    use sha2::{Digest, Sha256};
    use std::sync::atomic::AtomicU64;
    let directory = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let log = domain
        .run(ReplicaLog::open(
            ReplicaLogOptions::new(
                directory.path().join("log"),
                NodeId::new([21; 16])?,
                DiskLineage::new([22; 16])?,
                ProcessGeneration::new([23; 16])?,
                LogOpenMode::Create,
            ),
            Arc::new(DirectFileSystem),
        ))
        .await??;
    let group = log
        .create_cell_group(CellGroupBootstrap {
            route: route(),
            member: MemberId::new(1)?,
            voters: vec![MemberId::new(1)?],
            learners: vec![],
        })
        .await?;
    domain
        .blocking(move || -> anyhow::Result<()> {
            let cell = CellHistory {
                cluster: route().history.cluster,
                namespace: route().history.namespace,
                cell: CellId::new("restored")?,
                incarnation: route().history.incarnation,
            };
            let image = CellImage {
                epoch: 1,
                txid: 0,
                checksum: 0,
                commitment: CellImageCommitment::Materialized(Sha256::digest([]).into()),
            };
            let leaf = CellManifestLeaf {
                enrollment: EnrollmentRecord::new(
                    cell.clone(),
                    operation(),
                    route().schema_digest,
                    image,
                )?,
                version: CellVersion {
                    history: cell.clone(),
                    sequence: MutationSequence::new(1)?,
                    image,
                },
                geometry: CellImageGeometry {
                    length: 0,
                    page_size: 512,
                },
                last_mutation: pending().position(),
                witness: CommitWitness {
                    group: route().history,
                    position: pending().position(),
                    proposal: operation(),
                },
                effects: Default::default(),
            };
            let mut pages = ManifestPageStore::open(
                &directory.path().join("pages"),
                Arc::new(DirectFileSystem),
                128,
                1024 * 1024,
                16,
            )?;
            let tree = ManifestTree::new(route(), Default::default())?;
            let root = tree.update(&mut pages, ManifestRoot::default(), leaf.clone(), None)?;
            let marker = GroupManifest::recovery_genesis(route(), root, [8; 32], operation())?;
            let prepared =
                tree.prepare_bound_complete(&group, &mut pages, marker.clone(), &mut |_| {
                    Ok(CellImagePin::Empty)
                })?;
            // This is an admission-negative fixture, not a supported genesis installer:
            // emulate the selected cache to exercise every actual authority boundary.
            group.cache.write().unwrap().group_manifest = Some(marker.clone());
            let leader = Arc::new(AtomicU64::new(1));
            assert!(CellExecutionAuthority::new(
                group.clone(),
                &prepared,
                &cell,
                Term::new(1),
                leader.clone()
            )
            .is_err());
            assert!(CellReconciliationAuthority::new(group.clone(), &prepared, &cell).is_err());
            assert!(CellEnrollmentState::new(group.clone(), &prepared, cell.clone()).is_err());
            let node = Budget::new(1024 * 1024, 1);
            let local = Budget::new(1024 * 1024, 1);
            let read = CellReadAuthority::new(
                read_authority::ReadSelection {
                    group: group.clone(),
                    route: route(),
                    cell: leaf.clone(),
                    image: CellImagePin::Empty,
                    applied: marker.position(),
                    barrier: None,
                    leader: None,
                    source: None,
                    source_deadline: None,
                },
                100,
                node.acquire(4096).unwrap(),
                local.acquire(4096).unwrap(),
            )?;
            assert!(matches!(
                read.check(10),
                Err(crate::raft::completion::RuntimeError::NotReady)
            ));
            assert!(tree
                .prepare_complete(&group, &mut pages, root, position(2), 1, |_| Ok(
                    CellImagePin::Empty
                ))
                .is_err());
            let reopened =
                tree.prepare_complete(&group, &mut pages, root, marker.position(), 1, |_| {
                    Ok(CellImagePin::Empty)
                })?;
            assert_eq!(reopened.marker(), &marker);
            assert!(!reopened.marker().is_active());
            let mut changed = leaf;
            changed.last_mutation = position(2);
            changed.witness.position = position(2);
            assert!(tree
                .prepare_update(&mut pages, &prepared, changed, None, CellImagePin::Empty)
                .is_err());
            drop(read);
            drop(reopened);
            drop(prepared);
            Ok(())
        })?
        .await??;
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}
