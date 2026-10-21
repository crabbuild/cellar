//! Physical RPCs exercise actual Node services, including an unclaimed standby.
use super::*;

async fn observe(
    cluster: &Cluster,
    source: usize,
    target: usize,
    group: usize,
) -> Result<CellInspection> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let result = cluster
            .cell(source, group)?
            .inspect_peer(cluster.peers[target].node, Duration::from_secs(1))
            .await;
        match result {
            Ok(view) if view.node.identity.process == cluster.peers[target].process => {
                return Ok(view)
            }
            Ok(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(10)).await
            }
            Ok(_) => anyhow::bail!("inspection never observed the current process"),
            Err(_) if tokio::time::Instant::now() < deadline => {
                // During restart the caller may still hold the old process's
                // directory view. Its stopped address really is unreachable;
                // wait for independently timed discovery of the new endpoint.
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn native_peer_inspection_binds_member_and_dormant_physical_state() -> Result<()> {
    let cluster = Cluster::start_with_inspection(true).await?;
    let receipt = cluster.write(0, 1, 3).await?;
    // No query-origin grant exists for source zero at node one.
    let member = observe(&cluster, 0, 1, 0).await?;
    assert_eq!(member.scope, cluster.registrations[0].birth.scope);
    assert_eq!(member.node.identity.node, cluster.peers[1].node);
    assert_eq!(member.node.identity.process, cluster.peers[1].process);
    assert_eq!(member.node.groups, 2);
    assert_eq!(
        member.occupancy,
        ReplicaOccupancy::Member(MemberId::new(2)?)
    );
    ensure!(member.member.is_some());
    let dormant = observe(&cluster, 0, 3, 0).await?;
    assert_eq!(dormant.occupancy, ReplicaOccupancy::Standby);
    assert!(dormant.member.is_none());
    assert_eq!(dormant.node.groups, 0);
    assert_eq!(dormant.node.reserved_bytes, 0);
    // A dormant physical source carries no invented member identity either.
    let from_standby = observe(&cluster, 3, 0, 0).await?;
    assert_eq!(
        from_standby.occupancy,
        ReplicaOccupancy::Member(MemberId::new(1)?)
    );
    assert_eq!(
        cluster
            .cell(3, 0)?
            .inspect_peer(cluster.peers[3].node, Duration::from_secs(1))
            .await?
            .occupancy,
        ReplicaOccupancy::Standby
    );
    // The independent read grant from node two to node zero cannot inspect.
    assert_eq!(
        cluster
            .cell(2, 0)?
            .inspect_peer(cluster.peers[0].node, Duration::from_secs(1))
            .await,
        Err(InspectionError::Refused)
    );
    for reader in 0..3 {
        assert_eq!(
            cluster
                .cell(reader, 0)?
                .query(
                    cluster.definitions[reader].get.clone(),
                    vec![],
                    QueryOptions {
                        consistency: QueryConsistency::AtLeast(receipt.version_token.clone()),
                        ..Default::default()
                    }
                )
                .await?
                .result,
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
    let mut cluster = Cluster::start_with_inspection(true).await?;
    let previous = observe(&cluster, 0, 1, 0).await?;
    cluster.restart(1).await?;
    let current = observe(&cluster, 0, 1, 0).await?;
    assert_ne!(
        previous.node.identity.process,
        current.node.identity.process
    );
    assert_eq!(previous.node.identity.disk, current.node.identity.disk);
    assert_eq!(
        previous.node.storage_instance,
        current.node.storage_instance
    );
    let leader = cluster.leader(0).await?;
    // Admission is synchronous; none of these futures needs to be polled for
    // the node to own and then cancel/join its actual network operations.
    let retained = (0..16)
        .map(|_| {
            cluster
                .cell(0, 0)
                .map(|cell| cell.inspect_peer(cluster.peers[1].node, Duration::from_secs(30)))
        })
        .collect::<Result<Vec<_>>>()?;
    // Business producers keep their independent admission while metadata
    // results remain retained by these unpolled control waiters.
    assert_eq!(
        cluster
            .cell(leader, 0)?
            .command(
                cluster.definitions[leader].increment.clone(),
                1_i64.to_be_bytes().to_vec(),
                public_node::options(1)?
            )
            .await?
            .result,
        1_i64.to_be_bytes()
    );
    tokio::time::timeout(Duration::from_secs(5), cluster.close()).await??;
    for waiter in retained {
        let _ = waiter.await;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn native_complete_node_load_scan_covers_every_registered_history() -> Result<()> {
    let cluster = Cluster::start_with_inspection(true).await?;
    cluster.write(0, 1, 1).await?;
    cluster.write(1, 1, 1).await?;
    let mut expected = [0; 4];
    for group in 0..2 {
        expected[cluster.leader(group).await?] += 1;
    }
    let caller = cluster.cell(0, 0)?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    for (target, leaders) in expected.into_iter().enumerate() {
        loop {
            let observed = caller
                .inspect_peer(cluster.peers[target].node, Duration::from_millis(500))
                .await;
            if let Ok(observed) = observed {
                if let Some(load) = observed.leader_load {
                    assert_eq!(load.capacity, 2);
                    if load.leaders == leaders {
                        ensure!(load.sample_age_ms < 5000, "complete load sample was stale");
                        break;
                    }
                }
            }
            ensure!(
                tokio::time::Instant::now() < deadline,
                "node-wide leader count did not converge for node {target}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
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
