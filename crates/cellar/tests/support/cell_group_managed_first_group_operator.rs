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
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let joined = close_group_nodes(cluster.nodes, &cluster.hosts).await;
    cluster.fixtures[0].domain.shutdown().await;
    work?;
    joined?;
    Ok(())
}
