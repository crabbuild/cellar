// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Private requests may start an ordinary retained election, never grant authority.
use super::*;

async fn request(
    runtime: &CellGroupRuntime,
    group: GroupHistory,
    cell: &CellHistory,
    enrollment: bool,
) -> std::result::Result<(), RuntimeError> {
    if enrollment {
        runtime
            .enrollment_authority(group, cell.clone())
            .await
            .map(|_| ())
    } else {
        runtime
            .execution_authority(group, cell.clone())
            .await
            .map(|_| ())
    }
}

pub(crate) async fn retained_private_election(connected: bool) -> Result<()> {
    private_election(connected, true).await
}

pub(crate) async fn initial_private_election(connected: bool) -> Result<()> {
    private_election(connected, false).await
}

async fn private_election(connected: bool, retained: bool) -> Result<()> {
    for enrollment in [false, true] {
        let mut cluster = Cluster::new().await?;
        if retained {
            cluster.elect().await?;
        }
        let route = cluster.birth.route().clone();
        let prior = cluster.voters[0].owner.as_ref().unwrap().status()?.term;
        let cell = history(&route, "retained-private")?;
        // Attach real initial or retained disks with automatic ticks disabled.
        let mut fixture = RuntimeCluster::from_cluster(cluster, 256, 256, 1_000_000).await?;
        let result = futures_util::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(async {
            assert!(matches!(
                request(&fixture.runtimes[1], route.history, &cell, enrollment).await,
                Err(RuntimeError::NotLeader | RuntimeError::NotReady)
            ));
            assert!(matches!(
                request(&fixture.runtimes[0], route.history, &cell, enrollment).await,
                Err(RuntimeError::NotLeader | RuntimeError::NotReady)
            ));
            let mut votes = 0;
            tokio::time::timeout(Duration::from_secs(2), async {
                while votes < 2 {
                    fixture
                        .pump_with_network(&[], &mut BTreeMap::new(), |_, message| {
                            if message.get_msg_type() == MessageType::MsgRequestPreVote {
                                assert_eq!(message.from, 1, "prior followers must not campaign");
                                votes += 1;
                            }
                            connected
                        })
                        .await?;
                    tokio::task::yield_now().await;
                }
                Ok::<_, anyhow::Error>(())
            })
            .await
            .context("private request did not start the retained election")??;
            assert_eq!(votes, 2);
            if connected {
                tokio::time::timeout(Duration::from_secs(5), async {
                    loop {
                        fixture.pump(&[], &mut BTreeMap::new()).await?;
                        let status = fixture.runtimes[0].status(route.history).await?;
                        if status.ready {
                            assert!(status.term > prior);
                            let authority = fixture.runtimes[0]
                                .enrollment_authority(route.history, cell.clone())
                                .await?;
                            assert!(authority.state().record().is_none());
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                    Ok::<_, anyhow::Error>(())
                })
                .await
                .context("ordinary retained election did not apply its barrier")??;
            } else {
                for _ in 0..8 {
                    assert!(matches!(
                        request(&fixture.runtimes[0], route.history, &cell, enrollment).await,
                        Err(RuntimeError::NotLeader | RuntimeError::NotReady)
                    ));
                    fixture
                        .pump_with_network(&[], &mut BTreeMap::new(), |_, message| {
                            if message.get_msg_type() == MessageType::MsgRequestPreVote {
                                votes += 1;
                            }
                            false
                        })
                        .await?;
                    tokio::task::yield_now().await;
                }
                assert_eq!(votes, 2, "requests must not replenish the startup hint");
                for runtime in &fixture.runtimes {
                    let status = runtime.status(route.history).await?;
                    assert!(!status.ready && status.term == prior);
                }
            }
            Ok::<_, anyhow::Error>(())
        }))
        .await;
        // Keep the actual voter disks until every owner has joined, including
        // an assertion unwind. A failed regression must remain inspectable.
        let root = std::mem::replace(&mut fixture.cluster._root, tempfile::tempdir()?);
        let closed = fixture.close().await;
        if !matches!(&result, Ok(Ok(()))) || closed.is_err() {
            let root = root.keep();
            for voter in 1..=3 {
                eprintln!(
                    "retained private startup disk: {}",
                    root.join(format!("node-{voter}")).display()
                );
            }
        }
        match result {
            Ok(result) => result?,
            Err(panic) => std::panic::resume_unwind(panic),
        }
        closed?;
    }
    Ok(())
}
