// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Live-quorum seed source boundaries; no fresh destination is admitted here.
use super::*;
use cellar::application::consensus::verification::JoiningMember;
use cellar_logic::consensus::ReplicaIdentity;
use raft::prelude::{ConfChangeTransition, ConfChangeType};

fn target(history: GroupHistory) -> Result<ReplicaIdentity<GroupHistory>> {
    Ok(ReplicaIdentity {
        scope: history,
        node: NodeId::new([4; 16])?,
        disk: DiskLineage::new([14; 16])?,
        member: MemberId::new(4)?,
        process: ProcessGeneration::new([24; 16])?,
    })
}

async fn join(fixture: &RuntimeCluster, route: &GroupRoute) -> Result<()> {
    let target = target(route.history)?;
    membership::configure(
        fixture,
        route,
        membership::change(
            4,
            ConfChangeType::AddLearnerNode,
            ConfChangeTransition::Auto,
        ),
        &[JoiningMember {
            member: target.member,
            node: target.node,
            disk: target.disk,
        }],
    )
    .await?;
    Ok(())
}

async fn authorize(
    fixture: &RuntimeCluster,
    history: GroupHistory,
) -> Result<CellGroupSeedAuthority> {
    Ok(fixture.runtimes[0]
        .seed_authority(target(history)?, reads::deadline(fixture, 10_000).await?)
        .await?)
}

async fn capture(
    fixture: &RuntimeCluster,
    history: GroupHistory,
    pool: &CellGroupCheckpointPool,
) -> Result<CellGroupSeedSource> {
    let authority = authorize(fixture, history).await?;
    let pool = pool.clone();
    Ok(fixture
        .cluster
        .domain
        .blocking(move || authority.capture(&pool))?
        .await??)
}

pub(crate) async fn seed_source(rooted: bool) -> Result<()> {
    let class = RaftClass::new("Counter", 1, [81; 32])?;
    let (mut fixture, route, _, _) = Box::pin(registered::setup(rooted, &class)).await?;
    let second = controller::add_second(&mut fixture, route.history).await?;
    let controllers = controller::start_with_limits(&mut fixture, controller::limits()).await?;
    fixture.runtimes[0].campaign(second).await?;
    controller::wait_for(async || Ok(fixture.runtimes[0].status(second).await?.ready)).await?;
    let group = fixture.cluster.voters[0].log.cell_group(&route.history)?;
    let original = group.group_manifest()?.context("root")?;
    assert!(fixture.runtimes[0]
        .seed_authority(
            target(route.history)?,
            reads::deadline(&fixture, 1000).await?
        )
        .await
        .is_err());
    join(&fixture, &route).await?;
    for wrong_disk in [false, true] {
        let mut invalid = target(route.history)?;
        if wrong_disk {
            invalid.disk = DiskLineage::new([99; 16])?;
        } else {
            invalid.node = NodeId::new([99; 16])?;
        }
        assert!(fixture.runtimes[0]
            .seed_authority(invalid, reads::deadline(&fixture, 1000).await?)
            .await
            .is_err());
    }
    let mut voter = target(route.history)?;
    voter.member = MemberId::new(1)?;
    voter.node = NodeId::new([1; 16])?;
    voter.disk = DiskLineage::new([11; 16])?;
    assert!(fixture.runtimes[0]
        .seed_authority(voter, reads::deadline(&fixture, 1000).await?)
        .await
        .is_err());
    assert!(matches!(
        fixture.runtimes[1]
            .seed_authority(
                target(route.history)?,
                reads::deadline(&fixture, 1000).await?
            )
            .await,
        Err(RuntimeError::NotReady | RuntimeError::NotLeader)
    ));
    let pool = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    let mut source = capture(&fixture, route.history, &pool).await?;
    assert_eq!(source.target(), &target(route.history)?);
    assert_eq!(source.marker().root(), original.root());
    assert!(source.marker().position().index >= source.barrier().index);
    assert!(source.barrier().index >= group.membership()?.revision());
    let expected = source.snapshot()?;
    let receiver_pool = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    let applier = fixture.appliers[&(2, route.history)].1.clone();
    let scratch = super::super::checkpoint_receive::scratch()?;
    let directory = fixture.cluster._root.path().to_path_buf();
    let prepared = fixture
        .cluster
        .domain
        .blocking(move || -> Result<_> {
            let mut receiver = receiver_pool.receive(applier, scratch, directory)?;
            source.stream(|chunk| receiver.append(chunk))?;
            assert!(source.stream(|_| Ok(())).is_err(), "one-shot stream");
            Ok(receiver.finish()?)
        })?
        .await??;
    assert_eq!(prepared.snapshot(), &expected);
    assert_eq!(prepared.marker().root(), original.root());
    drop(prepared);
    assert_eq!(pool.usage().sources, 0);

    // A captured complete cut may stream while later cell enrollment advances
    // the group. An authority not yet captured must instead refuse that old cut.
    let old = authorize(&fixture, route.history).await?;
    let mut captured = capture(&fixture, route.history, &pool).await?;
    let before = captured.marker().clone();
    let cell = history(&route, "after-seed-capture")?;
    let bytes = images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES(73);")?;
    let accepted = fixture.runtimes[0]
        .propose(
            route.history,
            apply::enrollment_bytes(&route, &cell, &bytes, rooted)?,
        )
        .await?;
    controller::applied(&fixture, route.history, accepted.position.index).await?;
    let stale_pool = pool.clone();
    assert!(fixture
        .cluster
        .domain
        .blocking(move || old.capture(&stale_pool))?
        .await?
        .is_err());
    fixture
        .cluster
        .domain
        .blocking(move || -> Result<()> {
            captured.stream(|chunk| {
                if let CellGroupCheckpointPart::Materialized(cell)
                | CellGroupCheckpointPart::ExtentManifest(cell) = chunk.part
                {
                    assert_ne!(cell.as_str(), "after-seed-capture");
                }
                Ok(())
            })?;
            assert_eq!(captured.marker(), &before);
            Ok(())
        })?
        .await??;

    // A fresh ReadIndex also works without a cell; seeding cannot be implemented
    // by choosing an arbitrary enrolled cell or replaying its cached observation.
    let empty_route = fixture.cluster.voters[0]
        .log
        .cell_group(&second)?
        .group_manifest()?
        .context("empty group")?
        .route()
        .clone();
    join(&fixture, &empty_route).await?;
    let mut empty = capture(&fixture, second, &pool).await?;
    assert_eq!(empty.marker().root().cells(), 0);
    fixture
        .cluster
        .domain
        .blocking(move || -> Result<()> {
            let mut header = 0;
            empty.stream(|chunk| {
                assert_eq!(chunk.part, CellGroupCheckpointPart::Header);
                header += chunk.bytes.len();
                Ok(())
            })?;
            assert!(header > 0);
            Ok(())
        })?
        .await??;

    // Source authority shares the existing per-group read limit; another group's
    // progress and fresh barrier remain available when this group fills it.
    let mut held = Vec::new();
    for _ in 0..32 {
        held.push(authorize(&fixture, route.history).await?);
    }
    assert!(matches!(
        fixture.runtimes[0]
            .seed_authority(
                target(route.history)?,
                reads::deadline(&fixture, 1000).await?
            )
            .await,
        Err(RuntimeError::Overloaded)
    ));
    drop(authorize(&fixture, second).await?);
    drop(held);
    let revoked = authorize(&fixture, route.history).await?;
    let mut streaming = capture(&fixture, route.history, &pool).await?;
    let (entered, started) = tokio::sync::oneshot::channel();
    let (release, hold) = std::sync::mpsc::sync_channel(1);
    let worker = fixture.cluster.domain.blocking(move || {
        let mut entered = Some(entered);
        streaming.stream(|_| {
            let _ = entered
                .take()
                .expect("only the first chunk is allowed")
                .send(());
            hold.recv_timeout(Duration::from_secs(10))
                .map_err(|e| CellGroupCheckpointError::Sink(std::io::Error::other(e)))?;
            Ok(())
        })
    })?;
    tokio::time::timeout(Duration::from_secs(5), started).await??;
    assert_eq!(pool.usage().sources, 1);
    membership::configure(
        &fixture,
        &route,
        membership::change(4, ConfChangeType::RemoveNode, ConfChangeTransition::Auto),
        &[],
    )
    .await?;
    assert!(revoked.check().is_err());
    assert_eq!(
        pool.usage().sources,
        1,
        "revocation cannot release a blocked physical owner"
    );
    release.send(())?;
    let refused = worker.await?;
    assert_eq!(pool.usage().sources, 0);
    assert!(matches!(
        revoked.capture(&pool),
        Err(CellGroupCheckpointError::Authority(_))
    ));
    assert!(matches!(
        refused,
        Err(CellGroupCheckpointError::Authority(_))
    ));
    assert!(fixture.runtimes[0]
        .seed_authority(
            target(route.history)?,
            reads::deadline(&fixture, 1000).await?
        )
        .await
        .is_err());

    let revoked = capture(&fixture, second, &pool).await?;
    controller::wait_for(
        async || match fixture.runtimes[0].transfer_leader(second, 2).await {
            Ok(()) => Ok(true),
            Err(RuntimeError::NotReady) => Ok(false),
            Err(e) => Err(e.into()),
        },
    )
    .await?;
    assert!(revoked.check().is_err());
    controller::wait_for(async || Ok(fixture.runtimes[1].status(second).await?.ready)).await?;
    let new = fixture.runtimes[1]
        .seed_authority(target(second)?, reads::deadline(&fixture, 1000).await?)
        .await?;
    assert!(new.barrier().term > revoked.barrier().term);
    drop(new);
    drop(revoked);
    for controller in &controllers {
        controller.close().await?;
    }
    drop(controllers);
    assert_eq!(pool.usage().sources, 0);
    fixture.close().await
}

pub(crate) async fn seed_minority() -> Result<()> {
    let class = RaftClass::new("Counter", 1, [81; 32])?;
    let (mut fixture, route, _, _) = Box::pin(registered::setup(false, &class)).await?;
    let second = controller::add_second(&mut fixture, route.history).await?;
    let controllers = controller::start_with_limits(&mut fixture, controller::limits()).await?;
    fixture.runtimes[0].campaign(second).await?;
    controller::wait_for(async || Ok(fixture.runtimes[0].status(second).await?.ready)).await?;
    join(&fixture, &route).await?;
    let second_route = fixture.cluster.voters[0]
        .log
        .cell_group(&second)?
        .group_manifest()?
        .context("second")?
        .route()
        .clone();
    join(&fixture, &second_route).await?;
    let pool = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    // A timeout during the final sink call must refuse End, even if the group
    // remains leader and there are no further chunks at which to notice expiry.
    let authority = fixture.runtimes[0]
        .seed_authority(target(second)?, reads::deadline(&fixture, 500).await?)
        .await?;
    let capture_pool = pool.clone();
    let expired = fixture
        .cluster
        .domain
        .blocking(move || -> Result<_> {
            let mut source = authority.capture(&capture_pool)?;
            assert_eq!(source.marker().root().cells(), 0);
            let result = source.stream(|chunk| {
                assert_eq!(chunk.part, CellGroupCheckpointPart::Header);
                assert_eq!(chunk.offset, 0);
                assert_eq!(
                    chunk.length,
                    chunk.bytes.len() as u64,
                    "only the final header chunk"
                );
                std::thread::sleep(Duration::from_millis(600));
                Ok(())
            });
            Ok(result)
        })?
        .await??;
    assert!(matches!(
        expired,
        Err(CellGroupCheckpointError::Authority(
            RuntimeError::DeadlineExceeded
        ))
    ));
    // Drain an earlier successful barrier, then isolate this group on all routes.
    // The runtime still reports ready because fixture clocks do not auto-tick;
    // a fresh seed must prove quorum instead of trusting that cached status.
    drop(authorize(&fixture, route.history).await?);
    for transport in &fixture.transports {
        transport.remove_physical_group(&route.history)?;
    }
    assert!(fixture.runtimes[0].status(route.history).await?.ready);
    for _ in 0..3 {
        assert!(matches!(
            fixture.runtimes[0]
                .seed_authority(
                    target(route.history)?,
                    reads::deadline(&fixture, 100).await?
                )
                .await,
            Err(RuntimeError::DeadlineExceeded)
        ));
    }
    let authority = authorize(&fixture, second).await?;
    let accepted = fixture.runtimes[0]
        .propose(
            second,
            apply::enrollment_bytes(
                &second_route,
                &history(&second_route, "healthy")?,
                &images::image(None, "CREATE TABLE value(n);")?,
                false,
            )?,
        )
        .await?;
    controller::applied(&fixture, second, accepted.position.index).await?;
    for controller in &controllers {
        controller.close().await?;
    }
    assert!(
        authority.check().is_err(),
        "shutdown revokes a selected source"
    );
    drop(authority);
    drop(controllers);
    assert_eq!(pool.usage().sources, 0);
    fixture.close().await
}
