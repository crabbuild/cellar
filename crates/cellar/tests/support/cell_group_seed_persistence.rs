// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Authenticated seed grants cross the real engine queue and filesystem fence.
use super::*;
use cellar::application::consensus::verification::JoiningMember;
use cellar_logic::consensus::OperationId;
use raft::prelude::{ConfChangeTransition, ConfChangeType};
use raft::Storage;
use tokio::net::TcpListener;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Cut {
    StreamingRevocation,
    QueuedRevocation,
    EnteredRevocation,
    SyncError,
    AfterSyncError,
}

pub(crate) async fn seed_transfer_revocation(rooted: bool) -> Result<()> {
    Box::pin(run(rooted, Cut::StreamingRevocation)).await
}

pub(crate) async fn seed_persistence(rooted: bool) -> Result<()> {
    for cut in [
        Cut::QueuedRevocation,
        Cut::EnteredRevocation,
        Cut::SyncError,
        Cut::AfterSyncError,
    ] {
        Box::pin(run(rooted, cut))
            .await
            .with_context(|| format!("seed persistence {cut:?}"))?;
    }
    Ok(())
}

fn queued(log: &ReplicaLog) -> Result<u64> {
    let sample = log.performance_snapshot();
    assert!(sample.available && !sample.saturated);
    let queue = sample
        .operations
        .iter()
        .find(|op| op.name == "engine_queue")
        .context("queue metric")?;
    Ok(queue.started - queue.succeeded - queue.failed - queue.abandoned)
}

async fn run(rooted: bool, cut: Cut) -> Result<()> {
    let class = RaftClass::new("Counter", 1, [81; 32])?;
    let (mut fixture, route, a, b) = Box::pin(registered::setup(rooted, &class)).await?;
    let mut controllers = controller::start_with_limits(&mut fixture, controller::limits()).await?;
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
    let captured = seed_receiver::capture(&fixture, route.history).await?;
    let expected = fixture.cluster.voters[0]
        .log
        .cell_group(&route.history)?
        .group_manifest()?
        .context("source root")?;
    let listener = Arc::new(TcpListener::bind("127.0.0.1:0").await?);
    let mut peer = fixture.tcp_route(route.history)?.peers[0].clone();
    peer.data_address = listener.local_addr()?;
    let base = fixture.cluster._root.path().join("seed-persistence");
    let gate = fault::Gate::new(None);
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
    let (runtime, events) = CellGroupRuntime::start(
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
    let baseline = transport.usage().tasks;
    let service = fixture.cluster.domain.spawn(seed_receiver::serve(
        listener,
        captured,
        seed_receiver::Fault::Valid,
    ))?;
    let session = Box::pin(transport.prepare_group_seed(
        local.clone(),
        peer.clone(),
        fixture.cluster.directory.clone(),
        fixture.cluster.birth.clone(),
        reads::deadline(&fixture, 10_000).await?,
    ))
    .await?;
    let group = session.group().clone();
    let staged = group.group_manifest()?.context("staged root")?;
    let (pages, images) = seed_install::stores_with_filesystem(
        &base,
        rooted,
        Arc::new(fault::FaultFileSystem { gate: gate.clone() }),
    )
    .context("open destination stores")?;
    let recovering = group.clone();
    let (applier, root) = fixture
        .cluster
        .domain
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
    assert_eq!(root.marker(), &staged);
    drop(root);

    let release = gate.arm(match cut {
        Cut::StreamingRevocation => fault::HOLD_DEPENDENCY_PREFIX,
        Cut::QueuedRevocation | Cut::EnteredRevocation => fault::HOLD_SYNC,
        Cut::SyncError => fault::FAIL_SYNC,
        Cut::AfterSyncError => fault::FAIL_AFTER_SYNC,
    });
    // Hold an unrelated node revision fence ahead of the seed. No other local
    // group/controller is attached, so the next queued request is this install.
    let ahead = if cut == Cut::QueuedRevocation {
        let writer = log.clone();
        let ahead = fixture
            .cluster
            .domain
            .spawn(async move { writer.reserve_node_revision().await })?;
        tokio::time::timeout(Duration::from_secs(5), gate.entered.notified()).await?;
        assert_eq!(queued(&log)?, 0);
        Some(ahead)
    } else {
        None
    };
    let installer = applier.clone();
    let mut pending = fixture
        .cluster
        .domain
        .spawn(async move { session.install(installer).await })?;
    let mut restarted_source = None;
    let mut early_result = None;
    if matches!(
        cut,
        Cut::StreamingRevocation | Cut::QueuedRevocation | Cut::EnteredRevocation
    ) {
        if cut == Cut::QueuedRevocation {
            controller::wait_for(async || Ok(queued(&log)? == 1)).await?;
        } else {
            tokio::time::timeout(Duration::from_secs(5), gate.entered.notified()).await?;
        }
        assert_eq!(group.group_manifest()?.as_ref(), Some(&staged));
        assert!(group.checkpoint_manifest()?.is_none());
        assert_eq!(pool.usage().sources, 1, "physical closure remains owned");
        assert!(
            fixture
                .cluster
                .domain
                .run(fixture.cluster.directory.bootstrap_joined_group_member(
                    &fixture.cluster.birth,
                    local.member,
                    &log,
                ))
                .await?
                .is_err(),
            "no receipt before the engine fence"
        );
        restarted_source = Some(
            seed_receiver::revoke_source(&fixture, &mut controllers, &transport, &peer, &route)
                .await?,
        );
        if cut == Cut::StreamingRevocation {
            assert!(
                gate.dependency_cut
                    .lock()
                    .unwrap()
                    .as_ref()
                    .context("held dependency")?
                    .1
                    > 0
            );
            early_result = Some(tokio::time::timeout(Duration::from_secs(5), &mut pending).await??);
            assert!(
                transport.usage().data_items > 0,
                "failed network waiter cannot refund the physical receive owner"
            );
            assert!(pool.usage().retained_bytes > 0);
        } else {
            assert!(
                tokio::time::timeout(Duration::from_millis(30), &mut pending)
                    .await
                    .is_err(),
                "revocation cannot abandon entered/queued physical work"
            );
        }
        assert_eq!(pool.usage().sources, 1);
        drop(release);
    } else {
        // The one-shot sync error consumes the armed fault before completion.
        // Retain its release guard until the result has been observed.
    }
    let result = match early_result {
        Some(result) => result,
        None => pending.await?,
    };
    if let Some(ahead) = ahead {
        ahead.await??;
    }
    match cut {
        Cut::StreamingRevocation => assert!(
            matches!(
                result,
                Err(CellGroupSeedError::Transport(
                    TransportError::StalePeer | TransportError::Closed
                ))
            ),
            "streaming revoked grant: {:?}",
            result.err()
        ),
        Cut::QueuedRevocation => assert!(
            matches!(
                result,
                Err(CellGroupSeedError::Log(LogError::WrongIdentity))
            ),
            "queued revoked grant: {:?}",
            result.err()
        ),
        Cut::EnteredRevocation => {
            let (_permit, installed) = result?;
            assert_eq!(installed.marker(), &expected);
            assert!(installed.pin_cell(&a.cell).is_some() && installed.pin_cell(&b.cell).is_some());
        }
        Cut::SyncError | Cut::AfterSyncError => assert!(
            matches!(result, Err(CellGroupSeedError::Log(LogError::Io(_)))),
            "sync must report uncertainty: {:?}",
            result.err()
        ),
    }
    service.await??;
    controller::wait_for(async || {
        Ok(transport.usage().tasks == baseline && pool.usage().sources == 0)
    })
    .await?;
    assert_eq!(transport.usage().data_items, 0);
    assert_eq!(pool.usage().retained_bytes, 0);
    if matches!(cut, Cut::StreamingRevocation | Cut::QueuedRevocation) {
        assert_eq!(
            group.group_manifest()?.as_ref(),
            Some(&staged),
            "failed work cannot publish cache state"
        );
        assert!(group.checkpoint_manifest()?.is_none());
    }
    if matches!(cut, Cut::SyncError | Cut::AfterSyncError) {
        assert!(
            matches!(group.group_manifest(), Err(LogError::Closed)),
            "uncertain engine cannot expose a selected root"
        );
        assert!(matches!(group.checkpoint_manifest(), Err(LogError::Closed)));
        assert!(
            fixture
                .cluster
                .domain
                .run(fixture.cluster.directory.bootstrap_joined_group_member(
                    &fixture.cluster.birth,
                    local.member,
                    &log,
                ))
                .await?
                .is_err(),
            "uncertain engine cannot grant bootstrap"
        );
        assert!(
            matches!(
                log.persist_cells(OperationId::new(5101), vec![GroupWrite::new(&group)])
                    .await,
                Err(LogError::Closed)
            ),
            "uncertain engine refuses later writes"
        );
    }
    transport.close().await;
    let runtime_closed = runtime.close().await;
    if matches!(cut, Cut::SyncError | Cut::AfterSyncError) {
        assert!(
            matches!(runtime_closed, Err(RuntimeError::Storage(LogError::Closed))),
            "failed engine must propagate through joined runtime close: {runtime_closed:?}"
        );
    } else {
        runtime_closed?;
    }
    drop((events, applier, group, runtime, transport));
    let closed = log.close().await;
    if matches!(cut, Cut::SyncError | Cut::AfterSyncError) {
        assert!(matches!(closed, Err(LogError::Io(_))));
    } else {
        closed?;
    }
    drop(log);

    // Cold reopen resolves the on-disk outcome independently of the failed
    // caller and old cache. A pre-sync error can leave either complete root;
    // an error after successful sync must retain the complete authenticated seed.
    let log = fixture
        .cluster
        .domain
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
    let selected = group.group_manifest()?.context("reopened root")?;
    match cut {
        Cut::StreamingRevocation | Cut::QueuedRevocation => assert_eq!(selected, staged),
        Cut::EnteredRevocation | Cut::AfterSyncError => assert_eq!(selected, expected),
        Cut::SyncError => assert!(selected == staged || selected == expected),
    }
    let (pages, images) = seed_install::stores(&base, rooted)
        .context("reopen destination stores after joined close")?;
    let recovering = group.clone();
    let (applier, recovered) = fixture
        .cluster
        .domain
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
    let permit = fixture
        .cluster
        .domain
        .run(fixture.cluster.directory.bootstrap_joined_group_member(
            &fixture.cluster.birth,
            local.member,
            &log,
        ))
        .await?;
    if selected == expected {
        assert_eq!(group.checkpoint_manifest()?.as_ref(), Some(&expected));
        assert!(recovered.pin_cell(&a.cell).is_some() && recovered.pin_cell(&b.cell).is_some());
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
        assert!(
            !runtime.status(route.history).await?.ready,
            "seed receipt is not quorum authority"
        );
        let bytes = images::image(None, "CREATE TABLE value(n);")?;
        let proposal =
            apply::enrollment_bytes(&route, &history(&route, "without-quorum")?, &bytes, rooted)?;
        assert!(matches!(
            runtime.propose(route.history, proposal).await,
            Err(RuntimeError::NotReady)
        ));
        assert_eq!(group.group_manifest()?.as_ref(), Some(&expected));
        runtime.close().await?;
        drop(events);
    } else {
        assert!(permit.is_err());
        assert!(group.checkpoint_manifest()?.is_none());
        assert_eq!(recovered.marker().root().cells(), 0);
    }
    cellar::note!("verified seed persistence rooted={rooted} cut={cut:?} recovered_cells={} recovered_index={}", selected.root().cells(), selected.position().index.get());
    drop((applier, recovered, group));
    log.close().await?;
    if let Some(source) = restarted_source {
        source.close().await?;
    }
    for controller in controllers {
        controller.close().await?;
    }
    fixture.close().await
}
