// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A first mutation must repair cached source placement before submission.
use super::*;

pub(super) struct Cached {
    origin: usize,
    cells: Vec<(RaftCellHandle, CommandReceipt)>,
}

pub(super) async fn prime(cluster: &ManagedCluster, seeded: &SeededCells) -> Result<Cached> {
    let origin = leader(&cluster.hosts[..3], cluster.history).await?;
    let mut cells = Vec::new();
    for (index, (cell, receipt)) in seeded.cells.iter().zip(&seeded.receipts).enumerate() {
        let handle = cluster.hosts[origin].cell(cell.clone())?;
        let query = handle
            .query(
                ProposalId::new([154 + index as u8; 16])?,
                &handle.contract("Get")?,
                vec![],
                QueryOptions {
                    consistency: QueryConsistency::Linearizable,
                    ..Default::default()
                },
            )
            .await?;
        ensure!(query.witness.group == cluster.history);
        cells.push((handle, receipt.clone()));
    }
    Ok(Cached { origin, cells })
}

pub(super) async fn replay(cluster: &ManagedCluster, cached: Cached, retry: u64) -> Result<()> {
    let revision = cluster.fixtures[0].catalog.read(TIMEOUT).await?.revision();
    let peer = &cluster.peers[cached.origin];
    for host in &cluster.hosts[3..] {
        inspect_current(host, peer.node, revision).await?;
        host.grant_command_origin(NamespaceCommandOrigin {
            cluster: cluster.history.cluster,
            namespace: cluster.history.namespace,
            revision: 1,
            peer: peer.clone(),
        })?;
    }
    for (handle, original) in cached.cells {
        let replay = handle
            .command(
                &handle.contract("Add")?,
                original.result.clone(),
                CommandOptions::new(original.id, retry),
            )
            .await
            .context("first cached-source command after split must refresh before submission")?;
        ensure!(replay.id == original.id && replay.result == original.result);
    }
    Ok(())
}
