// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Owned process kills around an authenticated, whole-group learner install.
//! Frozen synthetic catalog bytes are fixture input, not an S3 recovery protocol.
use super::crash_fixture::{save, stop_at};
use super::*;
use cellar::application::consensus::verification::JoiningMember;
use raft::prelude::{ConfChangeTransition, ConfChangeType};
use raft::Storage;
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Oracle {
    rooted: bool,
    cut: String,
    staged: GroupManifest,
    expected: GroupManifest,
    objects: Vec<(String, Vec<u8>)>,
}

pub(crate) async fn seed_crash(rooted: bool) -> Result<()> {
    for cut in [
        "staged",
        "image-prefix",
        "image-synced",
        "before-sync",
        "after-sync",
        "published",
    ] {
        let directory = tempfile::tempdir()?;
        let root = directory.path();
        crash_fixture::kill_at(root, "seed_process_fixture", rooted, cut).await?;
        Box::pin(recover(root, rooted, cut))
            .await
            .with_context(|| format!("cold recovery {rooted}/{cut}"))?;
    }
    Ok(())
}

pub(crate) async fn seed_process_fixture() -> Result<()> {
    let (root, rooted, cut) = crash_fixture::parameters()?;
    anyhow::ensure!(
        matches!(
            cut.as_str(),
            "staged" | "image-prefix" | "image-synced" | "before-sync" | "after-sync" | "published"
        ),
        "invalid crash cut"
    );
    let class = RaftClass::new("Counter", 1, [81; 32])?;
    let (mut fixture, route, _, _) = Box::pin(registered::setup_in(
        rooted,
        &class,
        tempfile::tempdir_in(&root)?,
        Arc::new(DirectFileSystem),
    ))
    .await?;
    let _controllers = controller::start_with_limits(&mut fixture, controller::limits()).await?;
    let local = seed_install::target(route.history)?;
    membership::configure(
        &fixture,
        &route,
        membership::change(
            4,
            ConfChangeType::AddLearnerNode,
            ConfChangeTransition::Auto,
        ),
        &[JoiningMember {
            member: local.member,
            node: local.node,
            disk: local.disk,
        }],
    )
    .await?;
    let base = root.join("learner");
    let gate = fault::Gate::new(Some(root.join("signal")));
    let log = fixture
        .cluster
        .domain
        .run(ReplicaLog::open(
            ReplicaLogOptions::new(
                base.join("log"),
                local.node,
                local.disk,
                local.process,
                LogOpenMode::Create,
            ),
            Arc::new(fault::FaultFileSystem { gate: gate.clone() }),
        ))
        .await??;
    let (runtime, _events) = CellGroupRuntime::start(
        log.clone(),
        RaftRuntimeOptions {
            tick_interval_ms: None,
            ..Default::default()
        },
    )
    .await?;
    let transport = tcp::start(&runtime, 3).await?;
    let pool = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    transport.enable_group_checkpoints(pool.clone())?;
    // The complete transfer uses the production source and destination over TLS.
    // Learner enrollment is committed before its authenticated route is installed.
    let mut physical = fixture.tcp_route(route.history)?;
    physical.revision += 1;
    physical.peers.push(RaftPeer {
        replica: local.clone(),
        label: "node-4".into(),
        server_name: "localhost".into(),
        control_address: transport.control_address(),
        data_address: transport.data_address(),
        query_address: transport.query_address(),
    });
    let source_pool = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    fixture.transports[0].enable_group_checkpoints(source_pool.clone())?;
    for peer in &fixture.transports {
        peer.install_physical_group(physical.clone())?;
    }
    let _registration = fixture.transports[0]
        .register_group_checkpoint(fixture.appliers[&(0, route.history)].1.clone())?;
    let session = Box::pin(transport.prepare_group_seed(
        local,
        physical.peers[0].clone(),
        fixture.cluster.directory.clone(),
        fixture.cluster.birth.clone(),
        reads::deadline(&fixture, 10_000).await?,
    ))
    .await?;
    let group = session.group().clone();
    let staged = group.group_manifest()?.context("staged root")?;
    let expected = fixture.cluster.voters[0]
        .log
        .cell_group(&route.history)?
        .group_manifest()?
        .context("source root")?;
    // The catalog is external to the learner disk. Freeze its synthetic objects
    // before the fault, including the permanent destination claim. The parent
    // never manufactures a replacement claim from the recovered disk.
    let objects = crash_fixture::capture_catalog(fixture.cluster.store.as_ref()).await?;
    save(
        &root.join("oracle.json"),
        &serde_json::to_vec(&Oracle {
            rooted,
            cut: cut.clone(),
            staged,
            expected: expected.clone(),
            objects,
        })?,
    )?;
    if cut == "staged" {
        return stop_at(&root).await;
    }
    let image_gate = fault::Gate::new(None);
    let (pages, images) = seed_install::stores_with_filesystem(
        &base,
        rooted,
        Arc::new(fault::FaultFileSystem {
            gate: image_gate.clone(),
        }),
    )?;
    let (applier, empty) = fixture
        .cluster
        .domain
        .blocking(move || {
            CellGroupApplier::recover(
                group,
                pages,
                images,
                ManifestLimits::default(),
                GroupEntryLimits::default(),
                100,
            )
        })?
        .await??;
    assert_eq!(empty.marker().root().cells(), 0);
    drop(empty);
    let _release = match cut.as_str() {
        "before-sync" => Some(gate.arm(fault::CRASH_BEFORE_SYNC)),
        "after-sync" => Some(gate.arm(fault::CRASH_AFTER_SYNC)),
        _ => None,
    };
    if matches!(cut.as_str(), "image-prefix" | "image-synced") {
        let _release_image = image_gate.arm(if cut == "image-prefix" {
            fault::HOLD_DEPENDENCY_PREFIX
        } else {
            fault::HOLD_DEPENDENCY_SYNC
        });
        let _installer = fixture
            .cluster
            .domain
            .spawn(async move { session.install(applier).await })?;
        tokio::time::timeout(Duration::from_secs(5), image_gate.entered.notified()).await?;
        let dependency = image_gate
            .dependency_cut
            .lock()
            .unwrap()
            .clone()
            .context("physical dependency cut")?;
        assert!(dependency.1 > 0);
        let group = log.cell_group(&route.history)?;
        assert_eq!(
            group
                .group_manifest()?
                .context("staged at image cut")?
                .root()
                .cells(),
            0
        );
        assert!(group.checkpoint_manifest()?.is_none());
        assert_eq!(pool.usage().sources, 1);
        assert!(fixture
            .cluster
            .domain
            .run(fixture.cluster.directory.bootstrap_joined_group_member(
                &fixture.cluster.birth,
                MemberId::new(4)?,
                &log
            ))
            .await?
            .is_err());
        save(
            &root.join("dependency.json"),
            &serde_json::to_vec(&dependency)?,
        )?;
        return stop_at(&root).await;
    }
    let (_permit, installed) = session.install(applier).await?;
    assert_eq!(
        cut, "published",
        "a held physical fence cannot complete before kill"
    );
    assert_eq!(installed.marker(), &expected);
    stop_at(&root).await
}

async fn recover(root: &Path, rooted: bool, cut: &str) -> Result<()> {
    let oracle: Oracle = serde_json::from_slice(&std::fs::read(root.join("oracle.json"))?)?;
    assert_eq!(oracle.rooted, rooted);
    assert_eq!(oracle.cut, cut);
    if matches!(cut, "image-prefix" | "image-synced") {
        let (path, length): (std::path::PathBuf, u64) =
            serde_json::from_slice(&std::fs::read(root.join("dependency.json"))?)?;
        assert!(length > 0);
        if rooted {
            assert_eq!(path.parent(), Some(root.join("learner/extents").as_path()));
            assert_eq!(std::fs::metadata(&path)?.len(), length);
            let name = path
                .file_name()
                .context("extent file")?
                .to_str()
                .context("extent name")?;
            let complete = u64::from_str_radix(&name[2..10], 16)?;
            if cut == "image-prefix" {
                assert!(length < complete);
            } else {
                assert_eq!(length, complete);
            }
        } else {
            assert_eq!(path, std::path::PathBuf::from("snapshot.scratch"));
        }
    }
    let route = oracle.expected.route().clone();
    let local = seed_install::target(route.history)?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let (directory, birth) =
        crash_fixture::restore_catalog(oracle.objects, &route, &domain).await?;
    let base = root.join("learner");
    let log = domain
        .run(ReplicaLog::open(
            ReplicaLogOptions::new(
                base.join("log"),
                local.node,
                local.disk,
                ProcessGeneration::new([94; 16])?,
                LogOpenMode::Resume,
            ),
            Arc::new(DirectFileSystem),
        ))
        .await??;
    let group = log.cell_group(&route.history)?;
    let selected = group.group_manifest()?.context("recovered selected root")?;
    match cut {
        "staged" | "image-prefix" | "image-synced" => assert_eq!(selected, oracle.staged),
        "before-sync" => assert!(selected == oracle.staged || selected == oracle.expected),
        "after-sync" | "published" => assert_eq!(selected, oracle.expected),
        _ => unreachable!(),
    }
    let (pages, images) = seed_install::stores(&base, rooted)?;
    let recovering = group.clone();
    let (applier, recovered) = domain
        .blocking(move || {
            CellGroupApplier::recover(
                recovering,
                pages,
                images,
                ManifestLimits::default(),
                GroupEntryLimits::default(),
                100,
            )
        })?
        .await??;
    assert_eq!(recovered.marker(), &selected);
    let permit = domain
        .run(directory.bootstrap_joined_group_member(&birth, local.member, &log))
        .await?;
    if selected == oracle.expected {
        assert_eq!(
            group.checkpoint_manifest()?.as_ref(),
            Some(&oracle.expected)
        );
        assert_eq!(recovered.pinned_images(), 2);
        for name in ["registered-a", "registered-b"] {
            let cell = history(&route, name)?;
            let (leaf, _) = recovered.pin_cell(&cell.cell).context("recovered cell")?;
            assert_eq!(leaf.version.history, cell);
        }
        assert_eq!(group.initial_state()?.conf_state.learners, vec![4]);
        let (runtime, events) = CellGroupRuntime::start(
            log.clone(),
            RaftRuntimeOptions {
                tick_interval_ms: None,
                ..Default::default()
            },
        )
        .await?;
        runtime.attach(permit?, recovered.clone()).await?;
        assert!(!runtime.status(route.history).await?.ready);
        let bytes = images::image(None, "CREATE TABLE value(n);")?;
        let proposal =
            apply::enrollment_bytes(&route, &history(&route, "without-quorum")?, &bytes, rooted)?;
        assert!(matches!(
            runtime.propose(route.history, proposal).await,
            Err(RuntimeError::NotReady)
        ));
        assert_eq!(group.group_manifest()?.as_ref(), Some(&oracle.expected));
        runtime.close().await?;
        drop(events);
    } else {
        assert!(permit.is_err(), "empty stage has no local seed receipt");
        assert!(group.checkpoint_manifest()?.is_none());
        assert_eq!(recovered.pinned_images(), 0);
    }
    eprintln!(
        "seed crash rooted={rooted} cut={cut} killed=true recovered_cells={} recovered_index={}",
        selected.root().cells(),
        selected.position().index.get()
    );
    drop((applier, recovered, group));
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}
