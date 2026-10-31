// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Retained voter disks resume joint consensus through production TCP controllers.
use super::*;
use raft::{
    prelude::{ConfChangeTransition, ConfChangeType, ConfChangeV2, ConfState},
    Storage,
};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Oracle {
    rooted: bool,
    joint: bool,
    source_directory: String,
    expected: GroupManifest,
    objects: Vec<(String, Vec<u8>)>,
}

fn configuration(conf: &ConfState, joint: bool) {
    assert_eq!(conf.voters, vec![1, 2]);
    if joint {
        assert_eq!(conf.voters_outgoing, vec![1, 2, 3]);
        assert_eq!(conf.learners_next, vec![3]);
        assert!(conf.learners.is_empty());
    } else {
        assert!(conf.voters_outgoing.is_empty());
        assert!(conf.learners_next.is_empty());
        assert_eq!(conf.learners, vec![3]);
    }
    assert!(!conf.auto_leave);
}

pub(crate) async fn membership_crash(rooted: bool) -> Result<()> {
    for cut in ["joint", "left"] {
        let directory = tempfile::tempdir()?;
        crash_fixture::kill_at(directory.path(), "membership_process_fixture", rooted, cut).await?;
        Box::pin(recover(directory.path(), rooted, cut == "joint"))
            .await
            .with_context(|| format!("membership restart rooted={rooted} cut={cut}"))?;
    }
    Ok(())
}

pub(crate) async fn membership_process_fixture() -> Result<()> {
    let (root, rooted, cut) = crash_fixture::parameters()?;
    anyhow::ensure!(
        cut == "joint" || cut == "left",
        "invalid membership crash cut"
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
    membership::configure(
        &fixture,
        &route,
        membership::change(
            3,
            ConfChangeType::AddLearnerNode,
            ConfChangeTransition::Explicit,
        ),
        &[],
    )
    .await?;
    if cut == "left" {
        membership::configure(&fixture, &route, ConfChangeV2::default(), &[]).await?;
    }
    let expected = fixture.cluster.voters[0]
        .log
        .cell_group(&route.history)?
        .group_manifest()?
        .context("source root")?;
    for voter in &fixture.cluster.voters {
        let group = voter.log.cell_group(&route.history)?;
        configuration(&group.initial_state()?.conf_state, cut == "joint");
        assert_eq!(group.group_manifest()?.as_ref(), Some(&expected));
    }
    let source_directory = fixture
        .cluster
        ._root
        .path()
        .file_name()
        .context("fixture directory")?
        .to_str()
        .context("fixture directory name")?
        .to_owned();
    let objects = crash_fixture::capture_catalog(fixture.cluster.store.as_ref()).await?;
    crash_fixture::save(
        &root.join("oracle.json"),
        &serde_json::to_vec(&Oracle {
            rooted,
            joint: cut == "joint",
            source_directory,
            expected,
            objects,
        })?,
    )?;
    crash_fixture::stop_at(&root).await
}

struct Node {
    log: ReplicaLog,
    runtime: CellGroupRuntime,
    transport: RaftTransport,
    controller: CellGroupController,
}

async fn applied(nodes: &[Node], history: GroupHistory, index: LogIndex) -> Result<()> {
    controller::wait_for(async || {
        for node in nodes {
            if node.runtime.status(history).await?.applied.index < index {
                return Ok(false);
            }
        }
        Ok(true)
    })
    .await
}

async fn enroll(
    nodes: &[Node],
    leader: usize,
    route: &GroupRoute,
    rooted: bool,
    name: &str,
) -> Result<LogIndex> {
    let cell = history(route, name)?;
    let bytes = images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES(91);")?;
    let accepted = nodes[leader]
        .runtime
        .propose(
            route.history,
            apply::enrollment_bytes(route, &cell, &bytes, rooted)?,
        )
        .await?;
    applied(nodes, route.history, accepted.position.index).await?;
    Ok(accepted.position.index)
}

async fn recover(root: &Path, rooted: bool, joint: bool) -> Result<()> {
    let oracle: Oracle = serde_json::from_slice(&std::fs::read(root.join("oracle.json"))?)?;
    assert_eq!(oracle.rooted, rooted);
    assert_eq!(oracle.joint, joint);
    let name = Path::new(&oracle.source_directory);
    anyhow::ensure!(
        name.components().count() == 1
            && matches!(
                name.components().next(),
                Some(std::path::Component::Normal(_))
            ),
        "owned source directory name"
    );
    let disk = root.join(name);
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let route = oracle.expected.route().clone();
    let (directory, birth) =
        crash_fixture::restore_catalog(oracle.objects, &route, &domain).await?;
    let mut nodes = Vec::new();
    let mut peers = Vec::new();
    for n in 1..=3u8 {
        let base = disk.join(format!("node-{n}"));
        let log = domain
            .run(ReplicaLog::open(
                ReplicaLogOptions::new(
                    base.join("log"),
                    NodeId::new([n; 16])?,
                    DiskLineage::new([n + 10; 16])?,
                    ProcessGeneration::new([n + 90; 16])?,
                    LogOpenMode::Resume,
                ),
                Arc::new(DirectFileSystem),
            ))
            .await??;
        let group = log.cell_group(&route.history)?;
        assert_eq!(group.group_manifest()?.as_ref(), Some(&oracle.expected));
        configuration(&group.initial_state()?.conf_state, joint);
        assert_eq!(
            group.membership()?.revision().get(),
            oracle.expected.membership_revision()
        );
        let permit = domain
            .run(directory.bootstrap_group_member(&birth, &log))
            .await??;
        let filesystem = Arc::new(DirectFileSystem);
        let pages = ManifestPageStore::open(
            &base.join("pages"),
            filesystem.clone(),
            1024,
            16 * 1024 * 1024,
            16,
        )?;
        let images =
            GenerationStore::open(base.join("images"), filesystem.clone(), Default::default())?;
        let extents = if rooted {
            Some(NodeExtentStore::open(
                disk.join(format!("rooted-{}", n - 1)),
                filesystem,
                Default::default(),
            )?)
        } else {
            None
        };
        let config = super::super::checkpoint_receive::config(images, extents, &base)?;
        let recovering = group.clone();
        let (applier, recovered) = domain
            .blocking(move || {
                CellGroupApplier::recover(
                    recovering,
                    pages,
                    config,
                    ManifestLimits::default(),
                    GroupEntryLimits::default(),
                    100,
                )
            })?
            .await??;
        assert_eq!(recovered.marker(), &oracle.expected);
        assert_eq!(recovered.pinned_images(), 2);
        let (runtime, events) = CellGroupRuntime::start(
            log.clone(),
            RaftRuntimeOptions {
                tick_interval_ms: None,
                ..Default::default()
            },
        )
        .await?;
        runtime.attach(permit, recovered).await?;
        assert!(
            !runtime.status(route.history).await?.ready,
            "retained joint membership is not fresh quorum authority"
        );
        let transport = tcp::start(&runtime, usize::from(n - 1)).await?;
        peers.push(RaftPeer {
            replica: group.identity().clone(),
            label: format!("node-{n}"),
            server_name: "localhost".into(),
            control_address: transport.control_address(),
            data_address: transport.data_address(),
            query_address: transport.query_address(),
        });
        let sql = CellGroupSqlPool::start(
            &log,
            CellSqlLimits {
                workers: 1,
                queue_per_worker: 1,
                max_pending_cells: 2,
                group_pending_cells: 2,
                node_bytes: 1024 * 1024 * 1024,
                group_bytes: 512 * 1024 * 1024,
            },
        )?;
        let controller = CellGroupController::start(
            runtime.clone(),
            events,
            transport.clone(),
            sql,
            vec![applier],
            controller::limits(),
        )
        .await?;
        nodes.push(Node {
            log,
            runtime,
            transport,
            controller,
        });
    }
    let physical = cellar::application::consensus::verification::GroupRoute {
        scope: route.history,
        schema_digest: route.schema_digest,
        revision: 1,
        peers,
    };
    for node in &nodes {
        node.transport.install_physical_group(physical.clone())?;
    }
    nodes[0].runtime.campaign(route.history).await?;
    let mut leader = None;
    controller::wait_for(async || {
        for (i, node) in nodes.iter().enumerate() {
            if node.runtime.status(route.history).await?.ready {
                leader = Some(i);
                return Ok(true);
            }
            node.runtime.tick().await?;
        }
        Ok(false)
    })
    .await
    .context("retained voters regain current-term quorum")?;
    let leader = leader.context("actual ready leader")?;
    assert!(
        leader < 2,
        "demoted/outgoing-only member cannot lead the incoming configuration"
    );
    enroll(&nodes, leader, &route, rooted, "after-joint-restart").await?;
    for node in &nodes {
        configuration(
            &node
                .log
                .cell_group(&route.history)?
                .initial_state()?
                .conf_state,
            joint,
        );
    }
    if joint {
        let group = nodes[leader].log.cell_group(&route.history)?;
        let request = CellGroupMembershipChange::new(
            &route,
            &group.membership()?,
            ConfChangeV2::default(),
            &[],
        )?;
        let left = nodes[leader]
            .runtime
            .configure(route.history, request)
            .await?;
        applied(&nodes, route.history, left.position.index).await?;
    }
    for node in &nodes {
        configuration(
            &node
                .log
                .cell_group(&route.history)?
                .initial_state()?
                .conf_state,
            false,
        );
    }
    let final_index = enroll(&nodes, leader, &route, rooted, "after-joint-completion").await?;
    for node in &nodes {
        let manifest = node
            .log
            .cell_group(&route.history)?
            .group_manifest()?
            .context("final root")?;
        assert_eq!(manifest.root().cells(), 4);
        assert!(manifest.position().index >= final_index);
    }
    for name in [
        "registered-a",
        "registered-b",
        "after-joint-restart",
        "after-joint-completion",
    ] {
        let cell = history(&route, name)?;
        let deadline = domain
            .run(async { cellar::asyncrt::mono_ms() + 5000 })
            .await?;
        let selected = nodes[leader]
            .runtime
            .read_authority(
                route.history,
                cell.clone(),
                CellReadConsistency::Linearizable,
                deadline,
            )
            .await?;
        assert_eq!(selected.cell().version.history, cell);
        assert_eq!(selected.cell().version.sequence.get(), 1);
    }
    assert!(!nodes[2].runtime.status(route.history).await?.ready);
    eprintln!("membership crash rooted={rooted} joint_at_kill={joint} killed=true recovered_replicas=3 final_cells=4 final_index={}", final_index.get());
    for node in nodes {
        node.controller.close().await?;
        node.log.close().await?;
    }
    domain.shutdown().await;
    Ok(())
}
