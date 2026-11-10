// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Public SDK observation and guarded control of a shared physical group.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_group_observation_and_guarded_handoff() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(guarded_handoff(rooted)).await?;
    }
    Ok(())
}

async fn guarded_handoff(rooted: bool) -> Result<()> {
    let mut cluster = Box::pin(ManagedCluster::start(rooted, &[])).await?;
    let mut retained = None;
    let work = async {
        let seed = seed_cells(&cluster.hosts, cluster.history).await?;
        let initial = leader(&cluster.hosts, cluster.history).await?;
        let target = (initial + 1) % 3;
        for (i, host) in cluster.hosts.iter().enumerate() {
            let group = host.group(cluster.history)?;
            let observed: CellGroupObservation = group.observe(TIMEOUT).await?;
            ensure!(observed.replica.scope == cluster.history);
            ensure!(observed.replica.node == cluster.fixtures[i].spec.node);
            ensure!(observed.replica.member.get() == i as u64 + 1);
            ensure!(observed.cells == 2);
            ensure!(observed.checkpoint.index <= observed.status.applied.index);
            ensure!(observed.status.applied.index <= observed.status.committed);
            ensure!(observed.status.committed <= observed.persisted);
            ensure!(observed.membership_revision <= observed.status.applied.index);
            ensure!(
                observed.persisted.get() - observed.checkpoint.index.get()
                    == observed.retained_entries as u64
            );
            ensure!(
                observed.role
                    == if i == initial {
                        RaftReplicaRole::Leader
                    } else {
                        RaftReplicaRole::Voter
                    }
            );
            // Merely reading a publication must not turn it into coverage.
            let _: Option<CellGroupArchiveStatus> = group.archive_status()?;
        }
        let group = cluster.hosts[initial].group(cluster.history)?;
        let progress = group.replication_status(TIMEOUT).await?;
        let transfer = LeadershipTransfer {
            target: MemberId::new(target as u64 + 1)?,
            expected_term: progress.term,
            expected_revision: progress.membership_revision,
        };
        for timeout in [Duration::ZERO, Duration::from_secs(61)] {
            ensure!(matches!(
                group.observe(timeout).await,
                Err(RuntimeError::Invalid(_))
            ));
            ensure!(matches!(
                group.transfer_leadership(transfer, timeout).await,
                Err(RuntimeError::Invalid(_))
            ));
        }
        let unpolled = group.transfer_leadership(transfer, Duration::from_millis(1));
        let unpolled_observation = group.observe(Duration::from_millis(1));
        tokio::time::sleep(Duration::from_millis(20)).await;
        ensure!(matches!(
            unpolled.await,
            Err(RuntimeError::DeadlineExceeded)
        ));
        ensure!(matches!(
            unpolled_observation.await,
            Err(RuntimeError::DeadlineExceeded)
        ));
        for rejected in [
            LeadershipTransfer {
                expected_term: Term::new(progress.term.get() + 1),
                ..transfer
            },
            LeadershipTransfer {
                expected_revision: LogIndex::new(progress.membership_revision.get() + 1),
                ..transfer
            },
            LeadershipTransfer {
                target: MemberId::new(4)?,
                ..transfer
            },
        ] {
            ensure!(matches!(
                group.transfer_leadership(rejected, TIMEOUT).await,
                Err(RuntimeError::Invalid(_))
            ));
            let observed = group.observe(TIMEOUT).await?;
            ensure!(observed.status.leader == Some(initial as u64 + 1));
            ensure!(observed.status.term == progress.term);
            ensure!(observed.membership_revision == progress.membership_revision);
        }
        tokio::time::timeout(TIMEOUT, async {
            loop {
                match group.transfer_leadership(transfer, TIMEOUT).await {
                    Ok(()) => break Ok::<_, anyhow::Error>(()),
                    Err(RuntimeError::NotReady) => {
                        tokio::time::sleep(Duration::from_millis(10)).await
                    }
                    Err(error) => break Err(error.into()),
                }
            }
        })
        .await??;
        tokio::time::timeout(TIMEOUT, async {
            loop {
                let observed = cluster.hosts[target]
                    .group(cluster.history)?
                    .observe(TIMEOUT)
                    .await?;
                if observed.role == RaftReplicaRole::Leader && observed.status.ready {
                    break Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await??;
        verify_cells(&cluster.hosts[target], &seed, 2).await?;
        retained = Some((group.clone(), group.observe(TIMEOUT), transfer));
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let stopped = close(std::mem::take(&mut cluster.nodes), &cluster.hosts).await;
    if let Some((group, unpolled, transfer)) = retained {
        ensure!(matches!(unpolled.await, Err(RuntimeError::Closed)));
        ensure!(matches!(
            group.observe(TIMEOUT).await,
            Err(RuntimeError::Closed)
        ));
        ensure!(matches!(group.archive_status(), Err(RuntimeError::Closed)));
        ensure!(matches!(
            group.transfer_leadership(transfer, TIMEOUT).await,
            Err(RuntimeError::Closed)
        ));
    }
    cluster.fixtures[0].domain.shutdown().await;
    stopped?;
    work
}
