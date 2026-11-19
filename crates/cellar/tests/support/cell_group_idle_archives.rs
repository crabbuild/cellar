// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Empty groups own no archive cut; first-cell apply must wake publication.

use super::*;

fn idle_attempts(cluster: &ManagedCluster) -> Result<Option<Vec<u64>>> {
    cluster
        .hosts
        .iter()
        .map(|host| {
            let Some(status) = host.group(cluster.history)?.archive_status()? else {
                return Ok(None);
            };
            ensure!(!status.stopped, "empty archive owner stopped");
            ensure!(
                status.published.is_none(),
                "empty group acquired a fictitious cut"
            );
            Ok((status.attempts != 0 && status.error.is_none()).then_some(status.attempts))
        })
        .collect()
}

pub async fn qualify(real: bool) -> Result<()> {
    for rooted in [false, true] {
        let store = Arc::new(if real {
            catalog_fault::Store::around(rustfs::store("idle-group-archive", rooted)?)
        } else {
            catalog_fault::Store::default()
        });
        let mut cluster = Box::pin(ManagedCluster::start_with_store(
            rooted,
            &[],
            ArchiveSetup::Initialized,
            store,
        ))
        .await?;
        let domain = cluster.fixtures[0].domain.clone();
        let work = domain
            .run(async {
                for host in &cluster.hosts {
                    host.group(cluster.history)?
                        .quorum_membership(TIMEOUT)
                        .await?;
                }
                let attempts = cellar::asyncrt::timeout(TIMEOUT, async {
                    let mut previous = None;
                    let mut stable = 0;
                    loop {
                        if let Some(attempts) = idle_attempts(&cluster)? {
                            stable = if previous.as_ref() == Some(&attempts) {
                                stable + 1
                            } else {
                                0
                            };
                            previous = Some(attempts.clone());
                            if stable == 3 {
                                return Ok::<_, anyhow::Error>(attempts);
                            }
                        } else {
                            previous = None;
                            stable = 0;
                        }
                        cellar::asyncrt::sleep(Duration::from_millis(100)).await;
                    }
                })
                .await
                .context("empty archive owners did not become idle")??;
                // Several ordinary background retry intervals pass without any
                // application request, archive output or fabricated image proof.
                cellar::asyncrt::sleep(Duration::from_secs(2)).await;
                ensure!(
                    idle_attempts(&cluster)? == Some(attempts),
                    "empty groups kept polling archive ownership"
                );
                let leader = managed_fixture::leader(&cluster.hosts, cluster.history).await?;
                let history = CellHistory {
                    cluster: cluster.history.cluster,
                    namespace: cluster.history.namespace,
                    cell: CellId::new("Counter:idle-archive-wakeup")?,
                    incarnation: IncarnationId::new([231; 16])?,
                };
                let cell = cluster.hosts[leader].cell(history)?;
                // A Quorum creation has no waiting archive output. Its apply event
                // alone must restart background publication on the sleeping owner.
                cell.enroll(
                    CellEnrollmentRequest {
                        creation: ProposalId::new([232; 16])?,
                        durability: Durability::Quorum,
                    },
                    TIMEOUT,
                )
                .await?;
                cellar::asyncrt::timeout(TIMEOUT, async {
                    loop {
                        if cluster.hosts[leader]
                            .group(cluster.history)?
                            .archive_status()?
                            .is_some_and(|status| status.published.is_some())
                        {
                            return Ok::<_, anyhow::Error>(());
                        }
                        cellar::asyncrt::sleep(Duration::from_millis(10)).await;
                    }
                })
                .await
                .context("first-cell apply lost the archive wakeup")??;
                // Advisory status never proves this later command. Its actual S3
                // receipt requires the ordinary independent coverage/authority gates.
                let mut options = CommandOptions::new(
                    CommandId::new(ClientId::new([233; 16])?, 1)?,
                    u64::try_from(cellar::asyncrt::wall_ms())? + 60_000,
                );
                options.durability = Durability::S3;
                let receipt = cell
                    .command(&cell.contract("Add")?, 3i64.to_be_bytes().to_vec(), options)
                    .await?;
                ensure!(
                    receipt.durability == Durability::S3 && receipt.result == 3i64.to_be_bytes()
                );
                Ok::<_, anyhow::Error>(())
            })
            .await
            .map_err(anyhow::Error::from)
            .and_then(std::convert::identity);
        let joined =
            managed_fixture::close_group_nodes(std::mem::take(&mut cluster.nodes), &cluster.hosts)
                .await;
        if work.is_err() || joined.is_err() {
            for root in std::mem::take(&mut cluster.roots) {
                eprintln!("retained idle-archive disk: {}", root.keep().display());
            }
        }
        domain.shutdown().await;
        work.with_context(|| format!("idle archive RustFS={real} extents={rooted}"))?;
        joined?;
        eprintln!("Idle archive RustFS={real} extents={rooted}: PASS");
    }
    Ok(())
}
