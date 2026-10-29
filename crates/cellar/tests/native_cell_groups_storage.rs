// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Manifest-page contracts on independently sealed SQLite images. Engine marker
//! publication and complete recovery qualification are added at the next boundary.
#![allow(clippy::disallowed_methods)]

#[path = "support/raft_ltx_fixture.rs"]
#[allow(dead_code)]
mod images;

#[path = "support/raft_ltx_fault.rs"]
#[allow(dead_code)]
mod faults;

#[path = "support/raft_storage_fault.rs"]
#[allow(dead_code)]
mod engine_fault;

use anyhow::Result;
use cellar::application::consensus::{
    verification::{cell_groups::*, CommittedImage},
    *,
};
use cellar_ltx::{
    generation::{GenerationLimits, GenerationPin, GenerationStore},
    DirectFileSystem,
};
use std::{collections::BTreeMap, sync::Arc};

fn route() -> GroupRoute {
    GroupRoute {
        history: GroupHistory {
            cluster: ClusterId::new([1; 16]).unwrap(),
            namespace: NamespaceId::new([2; 16]).unwrap(),
            group: GroupId::new([3; 16]).unwrap(),
            incarnation: IncarnationId::new([4; 16]).unwrap(),
        },
        range: CellRange::all(),
        epoch: RouteEpoch::new(1).unwrap(),
        birth_digest: [5; 32],
        schema_digest: [6; 32],
    }
}

fn position(index: u64) -> LogPosition {
    LogPosition {
        index: LogIndex::new(index),
        term: Term::new(1),
    }
}

fn leaf(id: u8, pin: &GenerationPin, sequence: u64, index: u64) -> CellManifestLeaf {
    let state = pin.state();
    let image = CommittedImage {
        name: format!("{}.sqlite", hex::encode(pin.id().bytes())),
        commitment: state.digest.into(),
        length: state.length,
        ltx_epoch: state.epoch,
        txid: state.txid.0,
        checksum: state.checksum,
        page_size: state.page_size,
    };
    CellManifestLeaf {
        version: CellVersion {
            history: CellHistory {
                cluster: route().history.cluster,
                namespace: route().history.namespace,
                cell: CellId::new(format!("cell-{id}")).unwrap(),
                incarnation: IncarnationId::new([7; 16]).unwrap(),
            },
            sequence: MutationSequence::new(sequence).unwrap(),
            image: LtxImage {
                epoch: image.ltx_epoch,
                txid: image.txid,
                checksum: image.checksum,
                commitment: image.commitment,
            }
            .into(),
        },
        geometry: CellImageGeometry {
            length: image.length,
            page_size: image.page_size,
        },
        last_mutation: position(index),
        effects: EffectSummary::default(),
    }
}

#[test]
fn manifest_updates_only_changed_path() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let fs = Arc::new(DirectFileSystem);
    let generations = GenerationStore::open(
        temp.path().join("images"),
        fs.clone(),
        GenerationLimits {
            max_generations: 128,
            ..Default::default()
        },
    )?;
    let mut pages = ManifestPageStore::open(
        &temp.path().join("pages"),
        fs.clone(),
        4096,
        64 * 1024 * 1024,
    )?;
    let tree = ManifestTree::new(route(), ManifestLimits::default())?;
    let mut root = ManifestRoot::default();
    let mut cells = BTreeMap::new();
    for id in 1..=64 {
        let bytes = images::image(
            None,
            &format!("CREATE TABLE value(n); INSERT INTO value VALUES ({id});"),
        )?;
        let cut = images::Cut::new(&[], &bytes, 0, true, false);
        let pin = generations.prepare(images::id(id), None, &cut.update())?;
        assert_eq!(images::read(&pin)?, bytes);
        let value = leaf(id, &pin, 1, u64::from(id));
        root = tree.update(&mut pages, root, value.clone(), None)?;
        cells.insert(value.version.history.cell.clone(), (value, pin));
    }
    assert_eq!(root.cells(), 64);
    assert!(root.depth() <= 9, "AVL height must remain logarithmic");
    let old_root = root;
    let target = CellId::new("cell-32").unwrap();
    let (old, pin) = cells.get(&target).unwrap();
    let before = images::read(pin)?;
    let after = images::image(Some(&before), "UPDATE value SET n = 99;")?;
    let cut = images::Cut::new(&before, &after, 1, true, false);
    let changed = generations.prepare(images::id(100), Some(pin), &cut.update())?;
    let value = leaf(32, &changed, 2, 65);
    let written = pages.usage().written_pages;
    root = tree.update(&mut pages, root, value.clone(), Some(&old.version))?;
    assert!(pages.usage().written_pages - written <= u64::from(old_root.depth()));
    assert_eq!(tree.get(&pages, old_root, &target)?.unwrap(), *old);
    assert_eq!(tree.get(&pages, root, &target)?.unwrap(), value);
    for (cell, (expected, _)) in &cells {
        if cell != &target {
            assert_eq!(tree.get(&pages, root, cell)?.unwrap(), *expected);
        }
    }
    tree.verify(&pages, root, position(65), |actual| {
        let pin = if actual.version.history.cell == target {
            &changed
        } else {
            &cells[&actual.version.history.cell].1
        };
        if actual.version.image.commitment != CellImageCommitment::Materialized(pin.state().digest)
        {
            return Err(ManifestError::Invalid("fixture image"));
        }
        // The retained GenerationPin is an independently verified/synced image.
        Ok(())
    })?;
    drop(pages);
    let reopened = ManifestPageStore::open(&temp.path().join("pages"), fs, 4096, 64 * 1024 * 1024)?;
    assert_eq!(tree.get(&reopened, root, &target)?.unwrap(), value);
    tree.verify(&reopened, root, position(65), |_| Ok(()))?;
    Ok(())
}

#[test]
fn manifest_rejects_conflicts_bounds_and_missing_pages() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let fs = Arc::new(DirectFileSystem);
    let generations = GenerationStore::open(
        temp.path().join("images"),
        fs.clone(),
        GenerationLimits::default(),
    )?;
    let bytes = images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES (1);")?;
    let cut = images::Cut::new(&[], &bytes, 0, true, false);
    let pin = generations.prepare(images::id(1), None, &cut.update())?;
    let value = leaf(1, &pin, 1, 1);
    let mut pages = ManifestPageStore::open(&temp.path().join("pages"), fs, 64, 1024 * 1024)?;
    let tree = ManifestTree::new(
        route(),
        ManifestLimits {
            max_cells: 1,
            max_depth: 1,
        },
    )?;
    let root = tree.update(&mut pages, ManifestRoot::default(), value.clone(), None)?;
    let before = pages.usage();
    assert!(tree.update(&mut pages, root, value.clone(), None).is_err());
    assert!(tree
        .update(&mut pages, root, value.clone(), Some(&value.version))
        .is_err());
    assert!(tree
        .update(&mut pages, root, leaf(2, &pin, 1, 2), None)
        .is_err());
    assert_eq!(pages.usage(), before);
    assert!(tree.verify(&pages, root, position(0), |_| Ok(())).is_err());
    let mut other = route();
    other.schema_digest = [9; 32];
    assert!(ManifestTree::new(other, ManifestLimits::default())?
        .get(&pages, root, &value.version.history.cell)
        .is_err());
    let path = temp
        .path()
        .join("pages")
        .join(format!("{}.page", hex::encode(root.digest().unwrap())));
    let original = std::fs::read(&path)?;
    std::fs::write(&path, vec![0; 16 * 1024 + 1])?;
    assert!(tree.get(&pages, root, &value.version.history.cell).is_err());
    std::fs::write(&path, original)?;
    std::fs::remove_file(&path)?;
    assert!(tree.verify(&pages, root, position(1), |_| Ok(())).is_err());
    Ok(())
}

#[test]
fn manifest_ignores_local_image_names_and_quarantines_sync_failure() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let fs = Arc::new(DirectFileSystem);
    let generations = GenerationStore::open(
        temp.path().join("images"),
        fs.clone(),
        GenerationLimits::default(),
    )?;
    let bytes = images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES (3);")?;
    let cut = images::Cut::new(&[], &bytes, 0, true, false);
    let first = generations.prepare(images::id(1), None, &cut.update())?;
    let second = generations.prepare(images::id(2), None, &cut.update())?;
    let tree = ManifestTree::new(route(), ManifestLimits::default())?;
    let mut a = ManifestPageStore::open(&temp.path().join("a"), fs.clone(), 64, 1024 * 1024)?;
    let mut b = ManifestPageStore::open(&temp.path().join("b"), fs, 64, 1024 * 1024)?;
    let a_root = tree.update(&mut a, ManifestRoot::default(), leaf(1, &first, 1, 1), None)?;
    let b_root = tree.update(
        &mut b,
        ManifestRoot::default(),
        leaf(1, &second, 1, 1),
        None,
    )?;
    assert_ne!(first.id(), second.id());
    assert_eq!(
        a_root, b_root,
        "local generation names must not enter logical roots"
    );
    let directory = temp.path().join("faulted");
    let faults = faults::Faults::new(directory.clone(), None);
    let filesystem = Arc::new(faults::FaultFileSystem {
        faults: faults.clone(),
    });
    let mut pages = ManifestPageStore::open(&directory, filesystem.clone(), 64, 1024 * 1024)?;
    faults.mode.store(
        faults::FAIL_DIRECTORY_SYNC,
        std::sync::atomic::Ordering::Release,
    );
    assert!(tree
        .update(
            &mut pages,
            ManifestRoot::default(),
            leaf(1, &first, 1, 1),
            None
        )
        .is_err());
    assert!(matches!(
        tree.update(
            &mut pages,
            ManifestRoot::default(),
            leaf(1, &first, 1, 1),
            None
        ),
        Err(ManifestError::Quarantined)
    ));
    drop(pages);
    let mut recovered = ManifestPageStore::open(&directory, filesystem, 64, 1024 * 1024)?;
    // Retried bytes must be independently checked and synced, even if the
    // interrupted attempt left a complete content-addressed filename.
    let recovered_root = tree.update(
        &mut recovered,
        ManifestRoot::default(),
        leaf(1, &first, 1, 1),
        None,
    )?;
    assert_eq!(a_root, recovered_root);
    assert!(!faults.synced_paths.lock().unwrap().is_empty());
    Ok(())
}

fn engine_bootstrap() -> cellar::application::consensus::verification::GroupBootstrap {
    cellar::application::consensus::verification::GroupBootstrap {
        scope: GroupScope {
            cluster: route().history.cluster,
            namespace: route().history.namespace,
            group: route().history.group,
            incarnation: route().history.incarnation,
            cell: "storage-fixture-group".into(),
        },
        member: MemberId::new(1).unwrap(),
        birth_digest: route().birth_digest,
        voters: vec![MemberId::new(1).unwrap()],
        learners: vec![],
    }
}

fn engine_options(
    path: &std::path::Path,
    mode: cellar::application::consensus::verification::LogOpenMode,
    process: u8,
) -> cellar::application::consensus::verification::ReplicaLogOptions {
    cellar::application::consensus::verification::ReplicaLogOptions::new(
        path.to_owned(),
        NodeId::new([8; 16]).unwrap(),
        DiskLineage::new([9; 16]).unwrap(),
        ProcessGeneration::new([process; 16]).unwrap(),
        mode,
    )
}

fn root_write(
    group: &cellar::application::consensus::verification::GroupLog,
    prepared: PreparedGroupManifest,
) -> Result<cellar::application::consensus::verification::GroupWrite> {
    use raft::Storage as _;
    let index = prepared.marker().position().index.get();
    let mut write = cellar::application::consensus::verification::GroupWrite::new(group);
    // Storage fixture payload: application interpretation is qualified by the
    // group apply owner separately; the real engine still checks term/prefix.
    write.entries = vec![raft::prelude::Entry {
        index,
        term: 1,
        data: b"manifest-cut".to_vec().into(),
        ..Default::default()
    }];
    let mut hard = group.initial_state()?.hard_state;
    hard.commit = index;
    write.hard_state = Some(hard);
    write.group_manifest = Some(prepared);
    Ok(write)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn group_manifest_engine_sync_publication() -> Result<()> {
    use cellar::application::consensus::verification::{
        DurableApply, GroupWrite, LogOpenMode, ReplicaLog,
    };
    use cellar::sdk::ExecutionDomain;
    use cellar_logic::consensus::OperationId;
    let temp = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let gate = engine_fault::Gate::new(None);
    let filesystem = Arc::new(engine_fault::FaultFileSystem { gate: gate.clone() });
    let node = temp.path().join("node");
    let log = domain
        .run(ReplicaLog::open(
            engine_options(&node, LogOpenMode::Create, 1),
            filesystem.clone(),
        ))
        .await??;
    let group = log.create_group(engine_bootstrap()).await?;
    let fs = Arc::new(DirectFileSystem);
    let generations = GenerationStore::open(
        temp.path().join("images"),
        fs.clone(),
        GenerationLimits::default(),
    )?;
    let bytes = images::image(
        None,
        "CREATE TABLE value(n); INSERT INTO value VALUES (11);",
    )?;
    let cut = images::Cut::new(&[], &bytes, 0, true, false);
    let pin = generations.prepare(images::id(1), None, &cut.update())?;
    let tree = ManifestTree::new(route(), ManifestLimits::default())?;
    let mut pages =
        ManifestPageStore::open(&temp.path().join("pages"), fs.clone(), 256, 4 * 1024 * 1024)?;
    let root = tree.update(
        &mut pages,
        ManifestRoot::default(),
        leaf(1, &pin, 1, 2),
        None,
    )?;
    assert!(tree
        .prepare_complete(&group, &mut pages, root, position(2), 1, |_| Ok(
            CellImagePin::Empty
        ))
        .is_err());
    let prepared = tree.prepare_complete(&group, &mut pages, root, position(2), 1, |_| {
        Ok(CellImagePin::Materialized(pin.clone()))
    })?;
    let expected = prepared.marker().clone();
    let stale_preparation = prepared.clone();
    let release = gate.arm(engine_fault::HOLD_SYNC);
    let write = root_write(&group, prepared)?;
    let selected = log.clone();
    let mut pending =
        domain.spawn(async move { selected.persist(OperationId::new(1), vec![write]).await })?;
    tokio::time::timeout(std::time::Duration::from_secs(5), gate.entered.notified()).await?;
    assert!(group.group_manifest()?.is_none());
    assert_eq!(group.applied()?.position, position(1));
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(30), &mut pending)
            .await
            .is_err()
    );
    drop(release);
    pending.await??;
    assert_eq!(group.group_manifest()?.as_ref(), Some(&expected));
    assert!(
        group.applied().is_err(),
        "legacy empty-image consumers must refuse"
    );
    let mut legacy = GroupWrite::new(&group);
    legacy.applied = Some(DurableApply {
        position: position(3),
        image: None,
    });
    assert!(log
        .persist(OperationId::new(2), vec![legacy])
        .await
        .is_err());
    // A complete closure does not permit skipping an unapplied log index.
    let gap = tree.prepare_complete(&group, &mut pages, root, position(4), 1, |_| {
        Ok(CellImagePin::Materialized(pin.clone()))
    })?;
    assert!(log
        .persist(OperationId::new(3), vec![root_write(&group, gap)?])
        .await
        .is_err());
    assert_eq!(group.group_manifest()?.as_ref(), Some(&expected));
    log.close().await?;
    drop(group);
    drop(log);
    drop(pin);
    drop(generations);
    drop(pages);
    let resumed = domain
        .run(ReplicaLog::open(
            engine_options(&node, LogOpenMode::Resume, 2),
            filesystem,
        ))
        .await??;
    let group = resumed.group(&engine_bootstrap().scope)?;
    let observed = group.group_manifest()?.unwrap();
    assert_eq!(observed, expected);
    let mut stale = GroupWrite::new(&group);
    stale.group_manifest = Some(stale_preparation);
    assert!(
        resumed
            .persist(OperationId::new(4), vec![stale])
            .await
            .is_err(),
        "prepared closure belongs to the old engine instance"
    );
    let generations = GenerationStore::open(
        temp.path().join("images"),
        fs.clone(),
        GenerationLimits::default(),
    )?;
    let mut pages = ManifestPageStore::open(&temp.path().join("pages"), fs, 256, 4 * 1024 * 1024)?;
    let restored = tree.prepare_complete(
        &group,
        &mut pages,
        observed.root(),
        observed.position(),
        observed.membership_revision(),
        |_| {
            generations
                .adopt(images::id(1), cut.result)
                .map(CellImagePin::Materialized)
                .map_err(|_| ManifestError::Invalid("recovery image"))
        },
    )?;
    assert_eq!(restored.marker(), &expected);
    assert_eq!(restored.pinned_images(), 1);
    resumed.close().await?;
    domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn group_manifest_recovery_requires_every_image() -> Result<()> {
    use cellar::application::consensus::verification::{LogOpenMode, ReplicaLog};
    use cellar::sdk::ExecutionDomain;
    use cellar_logic::consensus::OperationId;
    let temp = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let fs = Arc::new(DirectFileSystem);
    let node = temp.path().join("node");
    let tree = ManifestTree::new(route(), ManifestLimits::default())?;
    let mut states = Vec::new();
    {
        let log = domain
            .run(ReplicaLog::open(
                engine_options(&node, LogOpenMode::Create, 1),
                fs.clone(),
            ))
            .await??;
        let group = log.create_group(engine_bootstrap()).await?;
        let generations = GenerationStore::open(
            temp.path().join("images"),
            fs.clone(),
            GenerationLimits::default(),
        )?;
        let mut pages =
            ManifestPageStore::open(&temp.path().join("pages"), fs.clone(), 256, 4 * 1024 * 1024)?;
        let mut pins = Vec::new();
        let mut root = ManifestRoot::default();
        for id in 1..=2 {
            let bytes = images::image(
                None,
                &format!("CREATE TABLE value(n); INSERT INTO value VALUES ({id});"),
            )?;
            let cut = images::Cut::new(&[], &bytes, 0, true, false);
            let pin = generations.prepare(images::id(id), None, &cut.update())?;
            states.push(cut.result);
            pins.push(pin.clone());
            root = tree.update(&mut pages, root, leaf(id, &pin, 1, u64::from(id) + 1), None)?;
            let prepared = tree.prepare_complete(
                &group,
                &mut pages,
                root,
                position(u64::from(id) + 1),
                1,
                |cell| {
                    let selected = if cell.version.history.cell.as_str() == "cell-1" {
                        0
                    } else {
                        1
                    };
                    Ok(CellImagePin::Materialized(pins[selected].clone()))
                },
            )?;
            log.persist(
                OperationId::new(u64::from(id)),
                vec![root_write(&group, prepared)?],
            )
            .await?;
        }
        log.close().await?;
    }
    // Only this owned fixture's image is removed. The actual engine marker and
    // every manifest page survive; recovering the other cell cannot cover it.
    std::fs::remove_file(
        temp.path()
            .join("images")
            .join(format!("{}.sqlite", hex::encode(images::id(1).bytes()))),
    )?;
    let log = domain
        .run(ReplicaLog::open(
            engine_options(&node, LogOpenMode::Resume, 2),
            fs.clone(),
        ))
        .await??;
    let group = log.group(&engine_bootstrap().scope)?;
    let marker = group.group_manifest()?.unwrap();
    assert_eq!(marker.root().cells(), 2);
    let generations = GenerationStore::open(
        temp.path().join("images"),
        fs.clone(),
        GenerationLimits::default(),
    )?;
    let mut pages = ManifestPageStore::open(&temp.path().join("pages"), fs, 256, 4 * 1024 * 1024)?;
    assert!(tree
        .prepare_complete(
            &group,
            &mut pages,
            marker.root(),
            marker.position(),
            marker.membership_revision(),
            |cell| {
                let id = if cell.version.history.cell.as_str() == "cell-1" {
                    1
                } else {
                    2
                };
                generations
                    .adopt(images::id(id), states[usize::from(id - 1)])
                    .map(CellImagePin::Materialized)
                    .map_err(|_| ManifestError::Invalid("missing selected cell image"))
            }
        )
        .is_err());
    assert!(group.applied().is_err());
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}
