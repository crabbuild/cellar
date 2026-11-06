// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Authenticated physical metadata for current voters and an empty standby.
use super::*;

async fn revision(cluster: &Cluster) -> Result<u64> {
    let managed = cluster
        .managed
        .as_ref()
        .context("managed inspection fixture")?;
    Ok(managed
        .domain
        .run(managed.fleet.read(TIMEOUT))
        .await??
        .revision())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn native_peer_inspection_binds_member_and_dormant_physical_state() -> Result<()> {
    let cluster = Cluster::open(true).await?;
    let receipt = cluster.write(0, 1, 3).await?;
    let revision = revision(&cluster).await?;
    // The Fleet lane authenticates exact boots without any namespace read grant.
    let member = cluster.inspect(0, 1, revision).await?;
    assert_eq!(member.view.node.identity.node, cluster.peers[1].node);
    assert_eq!(member.view.node.identity.process, cluster.peers[1].process);
    assert_eq!(member.view.node.groups, 2);
    let inventory = member.inventory.context("complete member inventory")?;
    assert_eq!(inventory.histories().len(), GROUPS);
    for birth in &cluster.births {
        let row = inventory
            .group_history(&birth.history)
            .context("registered group absent")?;
        assert_eq!(row.member(), MemberId::new(2)?);
        ensure!(row.in_configuration());
    }
    for (source, target) in [(0, 3), (3, 3)] {
        let dormant = cluster.inspect(source, target, revision).await?;
        assert_eq!(dormant.view.node.groups, 0);
        assert_eq!(dormant.view.node.reserved_bytes, 0);
        let inventory = dormant.inventory.context("complete standby inventory")?;
        ensure!(inventory.histories().is_empty() && inventory.initializing().is_none());
    }
    let from_standby = cluster.inspect(3, 0, revision).await?;
    assert_eq!(
        from_standby
            .inventory
            .context("standby observation")?
            .group_history(&cluster.births[0].history)
            .context("member inventory")?
            .member(),
        MemberId::new(1)?
    );
    for reader in 0..VOTERS {
        assert_eq!(
            cluster.read(reader, 0, &receipt).await?.result,
            3_i64.to_be_bytes()
        );
    }
    assert_eq!(
        cluster
            .calls
            .iter()
            .map(|calls| calls.load(Ordering::Acquire))
            .sum::<usize>(),
        1
    );
    cluster.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn native_peer_inspection_refreshes_process_and_joins_retained_waiters() -> Result<()> {
    let mut cluster = Cluster::open(true).await?;
    let previous = cluster.inspect(0, 1, revision(&cluster).await?).await?;
    cluster.restart(1).await?;
    let revision = revision(&cluster).await?;
    let current = cluster.inspect(0, 1, revision).await?;
    let old = previous.view.fleet_boot.context("original boot")?;
    let new = current.view.fleet_boot.context("retained boot")?;
    assert_ne!(old.process, new.process);
    assert_ne!(old.boot, new.boot);
    assert!(old.epoch < new.epoch);
    assert_eq!(old.disk, new.disk);
    assert_eq!(old.engine, new.engine);
    assert_eq!(previous.inventory, current.inventory);
    // Invocation admits the network work before the first response poll. Keep
    // these waiters unpolled while a real business write completes independently.
    let source = cluster.service(0)?.clone();
    let before = source.resource_usage().control;
    let retained = (0..16)
        .map(|_| {
            source.inspect_peer_physical_inventory(
                cluster.peers[1].node,
                revision,
                Duration::from_secs(30),
            )
        })
        .collect::<Vec<_>>();
    let charged = source.resource_usage().control;
    ensure!(charged.items > before.items && charged.bytes > before.bytes);
    assert_eq!(cluster.write(0, 1, 1).await?.result, 1_i64.to_be_bytes());
    tokio::time::timeout(TIMEOUT, cluster.close()).await??;
    for waiter in retained {
        let _ = tokio::time::timeout(TIMEOUT, waiter).await?;
    }
    ensure!(source.is_closed() && source.resource_usage().control.items == 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn native_complete_node_load_scan_covers_every_registered_history() -> Result<()> {
    let cluster = Cluster::open(true).await?;
    cluster.write(0, 1, 1).await?;
    cluster.write(1, 1, 1).await?;
    let mut expected = [0; VOTERS];
    for group in 0..GROUPS {
        expected[cluster.leader(group).await?] += 1;
    }
    let managed = cluster
        .managed
        .as_ref()
        .context("managed inspection fixture")?;
    let (revision, authorization) = managed
        .domain
        .run(async {
            let cut = managed.fleet.read(TIMEOUT).await?;
            let authorization = managed
                .fleet
                .authorize_group(&cut, &cluster.births[0].history, TIMEOUT)
                .await?;
            Ok::<_, anyhow::Error>((cut.revision(), authorization))
        })
        .await??;
    let observer = cluster.service(3)?;
    let deadline = Instant::now() + TIMEOUT;
    for (target, leaders) in expected.into_iter().enumerate() {
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            ensure!(
                !remaining.is_zero(),
                "node-wide leader count did not converge for node {target}"
            );
            let result = observer
                .observe_peer_fleet_membership(
                    cluster.peers[target].node,
                    authorization.clone(),
                    revision,
                    remaining.min(Duration::from_millis(500)),
                )
                .await;
            if let Ok(proof) = result {
                ensure!(
                    proof.progress().history == cluster.births[0].history
                        && proof.node().node.groups == 2
                );
                if let Some(load) = &proof.progress().leader_load {
                    assert_eq!(load.capacity, 2);
                    if load.leaders == leaders {
                        ensure!(load.sample_age_ms < 5000, "complete load sample was stale");
                        break;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    ensure!(cluster
        .inspect(0, 3, revision)
        .await?
        .inventory
        .context("standby inventory")?
        .histories()
        .is_empty());
    assert_eq!(
        cluster
            .calls
            .iter()
            .map(|calls| calls.load(Ordering::Acquire))
            .sum::<usize>(),
        2
    );
    cluster.close().await
}
