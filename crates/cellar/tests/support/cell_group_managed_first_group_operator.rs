// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Managed operator startup refuses readiness after losing quorum.

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_first_group_cli_admin_and_cold_quorum() -> Result<()> {
    let first = Fixture::with_class(false, counter_class()?).await?;
    let mut cluster = Box::pin(ManagedCluster::start_with_applications(
        first,
        &[],
        ArchiveSetup::Initialized,
        true,
        None,
        FirstGroupSetup::Operator,
    ))
    .await?;
    let work = async {
        for host in &cluster.hosts {
            let sample = host.performance();
            ensure!(sample.available && !sample.saturated);
            for name in [
                "first_group_archive",
                "first_group_attach",
                "first_group_quorum",
            ] {
                let phase = sample
                    .operations
                    .iter()
                    .find(|phase| phase.name == name)
                    .context("startup phase")?;
                ensure!(
                    phase.started == 1
                        && phase.succeeded == 1
                        && phase.failed == 0
                        && phase.abandoned == 0,
                    "initial startup phase {name}: {phase:?}"
                );
            }
        }
        seed_cells(&cluster.hosts, cluster.history).await?;
        let peers = cluster.nodes.split_off(1);
        close_group_nodes(peers, &cluster.hosts[1..]).await?;
        let survivor = &cluster.hosts[0];
        tokio::time::timeout(TIMEOUT, async {
            let group = survivor.group(cluster.history)?;
            loop {
                let status = group.status().await?;
                if !status.ready && status.leader.is_none() {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .context("observe first group quorum loss")??;
        let readiness = survivor
            .start_managed_namespace_first_group(
                cluster.history.cluster,
                cluster.history.namespace,
                TIMEOUT,
            )
            .await;
        ensure!(
            matches!(
                readiness,
                Err(CellGroupStartupError::Unconfirmed(
                    CellGroupControllerError::Runtime(RuntimeError::DeadlineExceeded)
                ))
            ),
            "retained attachment must refuse readiness without quorum: {readiness:?}"
        );
        ensure!(survivor.resource_usage().host.controller.groups == 1);
        let sample = survivor.performance();
        ensure!(sample.available && !sample.saturated);
        for name in [
            "first_group_archive",
            "first_group_attach",
            "first_group_quorum",
        ] {
            let phase = sample
                .operations
                .iter()
                .find(|phase| phase.name == name)
                .context("retained startup phase")?;
            let failed = u64::from(name == "first_group_quorum");
            ensure!(
                phase.started == 2
                    && phase.succeeded == 2 - failed
                    && phase.failed == failed
                    && phase.abandoned == 0,
                "retained startup phase {name}: {phase:?}"
            );
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let joined = close_group_nodes(cluster.nodes, &cluster.hosts).await;
    cluster.fixtures[0].domain.shutdown().await;
    work?;
    joined?;
    Ok(())
}
