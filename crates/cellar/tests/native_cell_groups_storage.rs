// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Manifest and engine publication contracts on independently sealed SQLite images.
//! Full crash-cut and both-image-mode qualification remain separate gates.
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
        64,
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
    let reopened =
        ManifestPageStore::open(&temp.path().join("pages"), fs, 4096, 64 * 1024 * 1024, 64)?;
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
    let mut pages = ManifestPageStore::open(&temp.path().join("pages"), fs, 64, 1024 * 1024, 64)?;
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
    let mut a = ManifestPageStore::open(&temp.path().join("a"), fs.clone(), 64, 1024 * 1024, 64)?;
    let mut b = ManifestPageStore::open(&temp.path().join("b"), fs, 64, 1024 * 1024, 64)?;
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
    let mut pages = ManifestPageStore::open(&directory, filesystem.clone(), 64, 1024 * 1024, 64)?;
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
    let mut recovered = ManifestPageStore::open(&directory, filesystem, 64, 1024 * 1024, 64)?;
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
    let mut pages = ManifestPageStore::open(
        &temp.path().join("pages"),
        fs.clone(),
        256,
        4 * 1024 * 1024,
        64,
    )?;
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
    let mut pages =
        ManifestPageStore::open(&temp.path().join("pages"), fs, 256, 4 * 1024 * 1024, 64)?;
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
        let mut pages = ManifestPageStore::open(
            &temp.path().join("pages"),
            fs.clone(),
            256,
            4 * 1024 * 1024,
            64,
        )?;
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
    let mut pages =
        ManifestPageStore::open(&temp.path().join("pages"), fs, 256, 4 * 1024 * 1024, 64)?;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incremental_manifest_preserves_images_and_rejects_forks() -> Result<()> {
    use cellar::application::consensus::verification::{LogOpenMode, ReplicaLog};
    use cellar::sdk::ExecutionDomain;
    use cellar_logic::consensus::OperationId;
    let temp = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let fs = Arc::new(DirectFileSystem);
    let log = domain
        .run(ReplicaLog::open(
            engine_options(&temp.path().join("node"), LogOpenMode::Create, 1),
            fs.clone(),
        ))
        .await??;
    let group = log.create_group(engine_bootstrap()).await?;
    let generations = GenerationStore::open(
        temp.path().join("images"),
        fs.clone(),
        GenerationLimits {
            max_generations: 128,
            ..Default::default()
        },
    )?;
    let page_path = temp.path().join("pages");
    let trace = faults::Faults::new(page_path.clone(), None);
    let page_fs = Arc::new(faults::FaultFileSystem {
        faults: trace.clone(),
    });
    let mut pages =
        ManifestPageStore::open(&page_path, page_fs.clone(), 4096, 64 * 1024 * 1024, 64)?;
    let tree = ManifestTree::new(route(), ManifestLimits::default())?;
    let mut pins = BTreeMap::new();
    let mut root = ManifestRoot::default();
    // All cells are part of this fixture's first complete checkpoint at index 2.
    for id in 1..=64 {
        let bytes = images::image(
            None,
            &format!("CREATE TABLE value(n); INSERT INTO value VALUES ({id});"),
        )?;
        let cut = images::Cut::new(&[], &bytes, 0, true, false);
        let pin = generations.prepare(images::id(id), None, &cut.update())?;
        let value = leaf(id, &pin, 1, 2);
        root = tree.update(&mut pages, root, value.clone(), None)?;
        pins.insert(value.version.history.cell, pin);
    }
    let original = tree.prepare_complete(&group, &mut pages, root, position(2), 1, |cell| {
        Ok(CellImagePin::Materialized(
            pins[&cell.version.history.cell].clone(),
        ))
    })?;
    log.persist(
        OperationId::new(1),
        vec![root_write(&group, original.clone())?],
    )
    .await?;
    drop(pins);
    let target = CellId::new("cell-32")?;
    let (old_leaf, CellImagePin::Materialized(reader)) = original.pin_cell(&target).unwrap() else {
        panic!("materialized fixture");
    };
    let before = images::read(&reader)?;
    let after = images::image(Some(&before), "UPDATE value SET n = 999;")?;
    let cut = images::Cut::new(&before, &after, 1, true, false);
    let changed = generations.prepare(images::id(100), Some(&reader), &cut.update())?;
    let next_leaf = leaf(32, &changed, 2, 3);
    let written = pages.usage().written_pages;
    let reads = trace.reads.load(std::sync::atomic::Ordering::Acquire);
    trace.synced_paths.lock().unwrap().clear();
    assert!(tree
        .prepare_update(
            &mut pages,
            &original,
            next_leaf.clone(),
            Some(&old_leaf.version),
            CellImagePin::Empty
        )
        .is_err());
    assert_eq!(pages.usage().written_pages, written);
    let next = tree.prepare_update(
        &mut pages,
        &original,
        next_leaf.clone(),
        Some(&old_leaf.version),
        CellImagePin::Materialized(changed.clone()),
    )?;
    assert_eq!(next.pinned_images(), 64);
    assert!(pages.usage().written_pages - written <= u64::from(root.depth()));
    let read_count = trace.reads.load(std::sync::atomic::Ordering::Acquire) - reads;
    assert!(read_count <= 4 * usize::from(root.depth()) + 2);
    assert!(
        trace.synced_paths.lock().unwrap().len() <= usize::from(root.depth()) * 2,
        "mutation must not sync the complete closure"
    );
    assert_eq!(original.pin_cell(&target).unwrap().0, old_leaf);
    assert_eq!(next.pin_cell(&target).unwrap().0, &next_leaf);
    tree.verify(&pages, next.marker().root(), position(3), |cell| {
        assert_eq!(next.pin_cell(&cell.version.history.cell).unwrap().0, cell);
        if cell.version.history.cell != target {
            assert_eq!(
                original.pin_cell(&cell.version.history.cell).unwrap().0,
                cell
            );
        }
        Ok(())
    })?;
    // Stage a different branch at 3 and its successor at 4. Matching the next
    // index alone must never allow it to replace the selected branch at 3.
    let mut branch = original.clone();
    for id in 65..=66 {
        let bytes = images::image(
            None,
            &format!("CREATE TABLE value(n); INSERT INTO value VALUES ({id});"),
        )?;
        let cut = images::Cut::new(&[], &bytes, 0, true, false);
        let pin = generations.prepare(images::id(id), None, &cut.update())?;
        branch = tree.prepare_update(
            &mut pages,
            &branch,
            leaf(id, &pin, 1, u64::from(id) - 62),
            None,
            CellImagePin::Materialized(pin),
        )?;
    }
    log.persist(OperationId::new(2), vec![root_write(&group, next.clone())?])
        .await?;
    assert!(log
        .persist(OperationId::new(3), vec![root_write(&group, branch)?])
        .await
        .is_err());
    assert_eq!(group.group_manifest()?.as_ref(), Some(next.marker()));
    let mut retry = cellar::application::consensus::verification::GroupWrite::new(&group);
    retry.group_manifest = Some(next.clone());
    log.persist(OperationId::new(4), vec![retry]).await?;
    // Explicitly retire fixture inventory after authoritative roots are pinned.
    // Only the two abandoned branch images may disappear at this point.
    generations.retain_only(&[])?;
    assert_eq!(generations.reclaim(128)?.files, 2);
    drop(original);
    assert_eq!(
        generations.reclaim(128)?.files,
        0,
        "old reader still pins cell-32"
    );
    assert_eq!(images::read(&reader)?, before);
    drop(reader);
    assert_eq!(
        generations.reclaim(128)?.files,
        1,
        "only the superseded cell image is now reclaimable"
    );
    drop(changed);
    assert_eq!(generations.reclaim(128)?.files, 0);
    // Live page pins keep the directory lock even after the store handle drops.
    // A different owner also cannot accept this preparation capability.
    drop(pages);
    assert!(
        ManifestPageStore::open(&page_path, page_fs.clone(), 4096, 64 * 1024 * 1024, 64).is_err()
    );
    let mut pages = ManifestPageStore::open(
        &temp.path().join("other-pages"),
        page_fs.clone(),
        4096,
        64 * 1024 * 1024,
        64,
    )?;
    let mut candidate = next_leaf.clone();
    candidate.last_mutation = position(4);
    assert!(matches!(
        tree.prepare_update(
            &mut pages,
            &next,
            candidate,
            Some(&next_leaf.version),
            next.pin_cell(&target).unwrap().1
        ),
        Err(ManifestError::Invalid("root/page owner"))
    ));
    drop(next);
    assert_eq!(
        generations.reclaim(128)?.files,
        0,
        "engine owns the selected root's images"
    );
    log.close().await?;
    drop(group);
    drop(log);
    assert_eq!(generations.reclaim(128)?.files, 64);
    let _reopened = ManifestPageStore::open(&page_path, page_fs, 4096, 64 * 1024 * 1024, 64)?;
    domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incremental_enrollment_preserves_rotated_subtrees() -> Result<()> {
    use cellar::application::consensus::verification::{LogOpenMode, ReplicaLog};
    use cellar::sdk::ExecutionDomain;
    let temp = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let fs = Arc::new(DirectFileSystem);
    let log = domain
        .run(ReplicaLog::open(
            engine_options(&temp.path().join("node"), LogOpenMode::Create, 1),
            fs.clone(),
        ))
        .await??;
    let group = log.create_group(engine_bootstrap()).await?;
    let generations = GenerationStore::open(
        temp.path().join("images"),
        fs.clone(),
        GenerationLimits::default(),
    )?;
    // Identical initial bytes may be physically shared; logical cell identity
    // and independently ordered enrollment still remain distinct.
    let bytes = images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES (1);")?;
    let cut = images::Cut::new(&[], &bytes, 0, true, false);
    let pin = generations.prepare(images::id(1), None, &cut.update())?;
    let mut pages =
        ManifestPageStore::open(&temp.path().join("pages"), fs, 4096, 64 * 1024 * 1024, 64)?;
    let tree = ManifestTree::new(route(), ManifestLimits::default())?;
    let mut current = tree.prepare_complete(
        &group,
        &mut pages,
        ManifestRoot::default(),
        position(1),
        1,
        |_| panic!("empty closure"),
    )?;
    let empty = current.clone();
    let mut historical = None;
    for id in 1..=96 {
        let before = pages.usage().written_pages;
        let depth = current.marker().root().depth();
        current = tree.prepare_update(
            &mut pages,
            &current,
            leaf(id, &pin, 1, u64::from(id) + 1),
            None,
            CellImagePin::Materialized(pin.clone()),
        )?;
        assert_eq!(current.pinned_images(), usize::from(id));
        assert!(pages.usage().written_pages - before <= 3 * u64::from(depth) + 3);
        tree.verify(
            &pages,
            current.marker().root(),
            current.marker().position(),
            |cell| {
                assert_eq!(
                    current.pin_cell(&cell.version.history.cell).unwrap().0,
                    cell
                );
                Ok(())
            },
        )?;
        if id == 32 {
            historical = Some(current.clone());
        }
    }
    assert_eq!(empty.pinned_images(), 0);
    let historical = historical.unwrap();
    assert_eq!(historical.pinned_images(), 32);
    assert!(historical.pin_cell(&CellId::new("cell-33")?).is_none());
    tree.verify(
        &pages,
        historical.marker().root(),
        historical.marker().position(),
        |cell| {
            assert_eq!(
                historical.pin_cell(&cell.version.history.cell).unwrap().0,
                cell
            );
            Ok(())
        },
    )?;
    let full = tree.prepare_complete(
        &group,
        &mut pages,
        current.marker().root(),
        current.marker().position(),
        1,
        |_| Ok(CellImagePin::Materialized(pin.clone())),
    )?;
    assert_eq!(full.marker(), current.marker());
    assert_eq!(full.pinned_images(), current.pinned_images());
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}

fn collect_pages(pages: &mut ManifestPageStore) -> Result<usize> {
    let mut deleted = 0;
    for _ in 0..257 {
        let report = pages.reclaim(16)?;
        assert!(report.scanned <= 16);
        deleted += report.pages;
        if report.scanned < 16 {
            return Ok(deleted);
        }
    }
    anyhow::bail!("bounded fixture inventory did not finish scanning")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manifest_pin_reclamation() -> Result<()> {
    use cellar::application::consensus::verification::{LogOpenMode, ReplicaLog};
    use cellar::sdk::ExecutionDomain;
    use cellar_logic::consensus::OperationId;
    let temp = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let fs = Arc::new(DirectFileSystem);
    let gate = engine_fault::Gate::new(None);
    let log = domain
        .run(ReplicaLog::open(
            engine_options(&temp.path().join("node"), LogOpenMode::Create, 1),
            Arc::new(engine_fault::FaultFileSystem { gate: gate.clone() }),
        ))
        .await??;
    let group = log.create_group(engine_bootstrap()).await?;
    let generations = GenerationStore::open(
        temp.path().join("images"),
        fs.clone(),
        GenerationLimits::default(),
    )?;
    let bytes = images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES (1);")?;
    let cut = images::Cut::new(&[], &bytes, 0, true, false);
    let pin = generations.prepare(images::id(1), None, &cut.update())?;
    let mut pages =
        ManifestPageStore::open(&temp.path().join("pages"), fs, 256, 4 * 1024 * 1024, 16)?;
    let tree = ManifestTree::new(route(), ManifestLimits::default())?;
    let root = tree.update(
        &mut pages,
        ManifestRoot::default(),
        leaf(1, &pin, 1, 2),
        None,
    )?;
    let old = tree.prepare_complete(&group, &mut pages, root, position(2), 1, |_| {
        Ok(CellImagePin::Materialized(pin.clone()))
    })?;
    log.persist(OperationId::new(1), vec![root_write(&group, old.clone())?])
        .await?;
    assert!(
        pages.reclaim(16).is_err(),
        "inventory must first be reconciled"
    );
    let reader = old.clone();
    let transition = old.clone();
    // The stored snapshot reference survives release of the caller's handle.
    assert!(matches!(
        pages.reconcile_retained(&vec![old.clone(); 15]),
        Err(ManifestError::Limit)
    ));
    assert!(
        pages.reclaim(16).is_err(),
        "failed reconciliation must not open collection"
    );
    pages.reconcile_retained(std::slice::from_ref(&old))?;
    let orphan = tree.update(&mut pages, root, leaf(3, &pin, 1, 3), None)?;
    let next = tree.prepare_update(
        &mut pages,
        &old,
        leaf(2, &pin, 1, 3),
        None,
        CellImagePin::Materialized(pin.clone()),
    )?;
    let release = gate.arm(engine_fault::HOLD_SYNC);
    let writer = log.clone();
    let write = root_write(&group, next.clone())?;
    let pending =
        domain.spawn(async move { writer.persist(OperationId::new(2), vec![write]).await })?;
    tokio::time::timeout(std::time::Duration::from_secs(5), gate.entered.notified()).await?;
    assert!(matches!(pages.reclaim(16), Err(ManifestError::Conflict)));
    assert!(
        pages.reconcile_retained(&[]).is_err(),
        "cannot retire roots during publication"
    );
    drop(release);
    pending.await??;
    assert!(
        collect_pages(&mut pages)? > 0,
        "unmarked paths are reclaimable"
    );
    assert!(tree
        .verify(&pages, orphan, position(3), |_| Ok(()))
        .is_err());
    tree.verify(&pages, root, position(2), |_| Ok(()))?;
    drop(old);
    drop(reader);
    pages.reconcile_retained(&[])?;
    collect_pages(&mut pages)?;
    tree.verify(&pages, root, position(2), |_| Ok(()))?;
    pages.reconcile_retained(std::slice::from_ref(&transition))?;
    drop(transition);
    collect_pages(&mut pages)?;
    tree.verify(&pages, root, position(2), |_| Ok(()))?;
    pages.reconcile_retained(&[])?;
    assert!(collect_pages(&mut pages)? > 0);
    assert!(tree.verify(&pages, root, position(2), |_| Ok(())).is_err());
    tree.verify(&pages, next.marker().root(), position(3), |_| Ok(()))?;
    assert_eq!(pages.usage().pages, 2);
    drop(next);
    assert_eq!(
        collect_pages(&mut pages)?,
        0,
        "engine-selected root survives caller release"
    );
    log.close().await?;
    assert!(matches!(pages.reclaim(16), Err(ManifestError::Quarantined)));
    domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn uncertain_manifest_publication_retains_both_roots() -> Result<()> {
    use cellar::application::consensus::verification::{LogOpenMode, ReplicaLog};
    use cellar::sdk::ExecutionDomain;
    use cellar_logic::consensus::OperationId;
    let temp = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let fs = Arc::new(DirectFileSystem);
    let gate = engine_fault::Gate::new(None);
    let node = temp.path().join("node");
    let log = domain
        .run(ReplicaLog::open(
            engine_options(&node, LogOpenMode::Create, 1),
            Arc::new(engine_fault::FaultFileSystem { gate: gate.clone() }),
        ))
        .await??;
    let group = log.create_group(engine_bootstrap()).await?;
    let image_path = temp.path().join("images");
    let page_path = temp.path().join("pages");
    let generations =
        GenerationStore::open(image_path.clone(), fs.clone(), GenerationLimits::default())?;
    let before = images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES (1);")?;
    let cut = images::Cut::new(&[], &before, 0, true, false);
    let pin = generations.prepare(images::id(1), None, &cut.update())?;
    let mut pages = ManifestPageStore::open(&page_path, fs.clone(), 256, 4 * 1024 * 1024, 16)?;
    let tree = ManifestTree::new(route(), ManifestLimits::default())?;
    let old_leaf = leaf(1, &pin, 1, 2);
    let root = tree.update(&mut pages, ManifestRoot::default(), old_leaf.clone(), None)?;
    let current = tree.prepare_complete(&group, &mut pages, root, position(2), 1, |_| {
        Ok(CellImagePin::Materialized(pin.clone()))
    })?;
    log.persist(
        OperationId::new(1),
        vec![root_write(&group, current.clone())?],
    )
    .await?;
    pages.reconcile_retained(&[])?;
    let after = images::image(Some(&before), "UPDATE value SET n = 2;")?;
    let changed = images::Cut::new(&before, &after, 1, true, false);
    let next_pin = generations.prepare(images::id(2), Some(&pin), &changed.update())?;
    let next = tree.prepare_update(
        &mut pages,
        &current,
        leaf(1, &next_pin, 2, 3),
        Some(&old_leaf.version),
        CellImagePin::Materialized(next_pin.clone()),
    )?;
    let expected = next.marker().clone();
    drop(pin);
    drop(next_pin);
    drop(current);
    generations.retain_only(&[])?;
    let release = gate.arm(engine_fault::FAIL_AFTER_SYNC);
    assert!(log
        .persist(OperationId::new(2), vec![root_write(&group, next)?])
        .await
        .is_err());
    drop(release);
    assert!(log.close().await.is_err());
    assert!(matches!(pages.reclaim(16), Err(ManifestError::Quarantined)));
    assert!(pages.reconcile_retained(&[]).is_err());
    assert_eq!(
        generations.reclaim(16)?.files,
        0,
        "both images survive loss of request and engine authority"
    );
    drop(group);
    drop(log);
    // The page owner, rather than the failed engine cache, still retains both.
    assert_eq!(generations.reclaim(16)?.files, 0);
    drop(pages);
    drop(generations);
    let log = domain
        .run(ReplicaLog::open(
            engine_options(&node, LogOpenMode::Resume, 2),
            fs.clone(),
        ))
        .await??;
    let group = log.group(&engine_bootstrap().scope)?;
    assert_eq!(
        group.group_manifest()?.as_ref(),
        Some(&expected),
        "injected failure was after actual fsync"
    );
    let generations = GenerationStore::open(image_path, fs.clone(), GenerationLimits::default())?;
    let mut pages = ManifestPageStore::open(&page_path, fs, 256, 4 * 1024 * 1024, 16)?;
    assert!(pages.reclaim(16).is_err());
    let restored = tree.prepare_complete(
        &group,
        &mut pages,
        expected.root(),
        expected.position(),
        expected.membership_revision(),
        |_| {
            generations
                .adopt(images::id(2), changed.result)
                .map(CellImagePin::Materialized)
                .map_err(|_| ManifestError::Invalid("recovered image"))
        },
    )?;
    pages.reconcile_retained(&[])?;
    assert_eq!(collect_pages(&mut pages)?, 1);
    generations.retain_only(&[])?;
    assert_eq!(generations.reclaim(16)?.files, 1);
    let (_, CellImagePin::Materialized(restored_image)) =
        restored.pin_cell(&CellId::new("cell-1")?).unwrap()
    else {
        panic!("materialized fixture");
    };
    assert_eq!(images::read(&restored_image)?, after);
    drop(restored_image);
    drop(restored);
    assert_eq!(
        generations.reclaim(16)?.files,
        0,
        "recovery registers selected root retention without a new write"
    );
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}

#[test]
fn manifest_collection_barrier_failure_keeps_quota() -> Result<()> {
    for mode in [faults::FAIL_REMOVE, faults::FAIL_DIRECTORY_SYNC] {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("pages");
        let trace = faults::Faults::new(path.clone(), None);
        let fs = Arc::new(faults::FaultFileSystem {
            faults: trace.clone(),
        });
        let generations = GenerationStore::open(
            temp.path().join("images"),
            Arc::new(DirectFileSystem),
            GenerationLimits::default(),
        )?;
        let bytes = images::image(None, "CREATE TABLE value(n);")?;
        let cut = images::Cut::new(&[], &bytes, 0, true, false);
        let pin = generations.prepare(images::id(1), None, &cut.update())?;
        let tree = ManifestTree::new(route(), ManifestLimits::default())?;
        let mut pages = ManifestPageStore::open(&path, fs.clone(), 64, 1024 * 1024, 8)?;
        // No engine root was published: every page in this fixture is an orphan.
        tree.update(
            &mut pages,
            ManifestRoot::default(),
            leaf(1, &pin, 1, 2),
            None,
        )?;
        pages.reconcile_retained(&[])?;
        let before = pages.usage();
        trace.mode.store(mode, std::sync::atomic::Ordering::Release);
        assert!(pages.reclaim(16).is_err());
        assert_eq!(
            pages.usage(),
            before,
            "uncertain deletion must not refund quota"
        );
        assert!(matches!(pages.reclaim(16), Err(ManifestError::Quarantined)));
        drop(pages);
        let mut pages = ManifestPageStore::open(&path, fs, 64, 1024 * 1024, 8)?;
        assert!(pages.reclaim(16).is_err());
        pages.reconcile_retained(&[])?;
        collect_pages(&mut pages)?;
        assert_eq!(pages.usage().pages, 0);
        assert_eq!(pages.usage().bytes, 0);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manifest_batch_retention_refusal_cancels_unwritten_reservations() -> Result<()> {
    use cellar::application::consensus::verification::{LogOpenMode, ReplicaLog};
    use cellar::sdk::ExecutionDomain;
    use cellar_logic::consensus::OperationId;
    use raft::Storage as _;
    let temp = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let fs = Arc::new(DirectFileSystem);
    let log = domain
        .run(ReplicaLog::open(
            engine_options(&temp.path().join("node"), LogOpenMode::Create, 1),
            fs.clone(),
        ))
        .await??;
    let first_group = log.create_group(engine_bootstrap()).await?;
    let mut second_birth = engine_bootstrap();
    second_birth.scope.group = GroupId::new([33; 16])?;
    let second_group = log.create_group(second_birth).await?;
    let mut second_route = route();
    second_route.history.group = second_group.identity().scope.group;
    let first_tree = ManifestTree::new(route(), ManifestLimits::default())?;
    let second_tree = ManifestTree::new(second_route, ManifestLimits::default())?;
    let generations = GenerationStore::open(
        temp.path().join("images"),
        fs.clone(),
        GenerationLimits::default(),
    )?;
    let bytes = images::image(None, "CREATE TABLE value(n);")?;
    let cut = images::Cut::new(&[], &bytes, 0, true, false);
    let pin = generations.prepare(images::id(1), None, &cut.update())?;
    // Capacity admits one group's current/pending pair, never two groups.
    let mut pages = ManifestPageStore::open(&temp.path().join("pages"), fs, 64, 1024 * 1024, 2)?;
    let first_root = first_tree.update(
        &mut pages,
        ManifestRoot::default(),
        leaf(1, &pin, 1, 2),
        None,
    )?;
    let second_root = second_tree.update(
        &mut pages,
        ManifestRoot::default(),
        leaf(2, &pin, 1, 2),
        None,
    )?;
    let first = first_tree.prepare_complete(
        &first_group,
        &mut pages,
        first_root,
        position(2),
        1,
        |_| Ok(CellImagePin::Materialized(pin.clone())),
    )?;
    let second = second_tree.prepare_complete(
        &second_group,
        &mut pages,
        second_root,
        position(2),
        1,
        |_| Ok(CellImagePin::Materialized(pin.clone())),
    )?;
    assert!(log
        .persist(
            OperationId::new(1),
            vec![
                root_write(&first_group, first.clone())?,
                root_write(&second_group, second.clone())?
            ]
        )
        .await
        .is_err());
    for group in [&first_group, &second_group] {
        assert!(group.group_manifest()?.is_none());
        assert_eq!(
            group.last_index()?,
            1,
            "no part of the rejected batch reached storage"
        );
    }
    pages.reconcile_retained(&[])?;
    assert_eq!(
        collect_pages(&mut pages)?,
        0,
        "prepared apply pins protect both staged roots"
    );
    log.persist(OperationId::new(2), vec![root_write(&first_group, first)?])
        .await?;
    drop(second);
    assert_eq!(collect_pages(&mut pages)?, 1);
    assert_eq!(pages.usage().pages, 1);
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}
