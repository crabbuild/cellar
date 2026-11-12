// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Recovery never replays the interrupted callback or completes its receipt.
use super::*;

pub async fn run(
    rooted: bool,
    durability: Durability,
    store: Arc<catalog_fault::Store>,
) -> Result<()> {
    let checkpoint = Arc::new(Notify::new());
    let mut cluster = Box::pin(bindings::start_with_checkpoint(
        rooted,
        store,
        checkpoint.clone(),
    ))
    .await?;
    let domain = cluster.fixtures[0].domain.clone();
    let work = domain
        .run(async {
            let group = cluster.history;
            let leader = managed_fixture::leader(&cluster.hosts, group).await?;
            let source = cluster.hosts[leader].cell(CellHistory {
                cluster: group.cluster,
                namespace: group.namespace,
                cell: bindings::identity("source")?,
                incarnation: IncarnationId::new([119; 16])?,
            })?;
            source
                .enroll(
                    CellEnrollmentRequest {
                        creation: ProposalId::new([120; 16])?,
                        durability,
                    },
                    TIMEOUT,
                )
                .await?;
            let original = bindings::options(1, durability)?;
            ensure!(original.timeout == TIMEOUT);
            let original_call = source
                .command(
                    &source.contract("Abandon")?,
                    b"{}".to_vec(),
                    original.clone(),
                )
                .boxed();
            let original_call = cellar::asyncrt::timeout(TIMEOUT, async {
                match futures_util::future::select(original_call, checkpoint.notified().boxed())
                    .await
                {
                    futures_util::future::Either::Left((result, _)) => {
                        anyhow::bail!("binding returned before its target checkpoint: {result:?}")
                    }
                    futures_util::future::Either::Right(((), call)) => Ok(call),
                }
            })
            .await
            .context("original five-second checkpoint deadline")??;
            let source_history = source.history().clone();
            // Stop immediately after the durable target checkpoint, while the
            // original RPC is still awaiting its response and before due recovery.
            Box::pin(bindings::restart(&mut cluster)).await?;
            let error = original_call
                .await
                .err()
                .context("interrupted call cannot complete")?;
            ensure!(
                error.ambiguous,
                "source intent must have committed: {error}"
            );
            drop(source);
            let leader = managed_fixture::leader(&cluster.hosts, group).await?;
            let source = cluster.hosts[leader].cell(source_history)?;
            let target_history = cluster.hosts[leader]
                .resolve_cell_history(
                    group.cluster,
                    group.namespace,
                    bindings::identity("held-target")?,
                    QueryOptions::default(),
                )
                .await?
                .context("interrupted binding enrolled its exact target")?;
            let target = cluster.hosts[leader].cell(target_history.clone())?;
            let value = target
                .query(
                    ProposalId::new([130; 16])?,
                    &target.contract("Get")?,
                    vec![],
                    QueryOptions::default(),
                )
                .await?;
            ensure!(
                value.result == b"1",
                "target checkpoint committed before the lost response"
            );
            drop((target, value));
            // Give the node-owned due worker time to act without any client retry.
            cellar::asyncrt::sleep(Duration::from_secs(2)).await;
            let end = cellar::asyncrt::mono_ms() + 60_000;
            let mut sequence = 2;
            loop {
                let probe = source
                    .command(
                        &source.contract("RecoveryProbe")?,
                        b"{}".to_vec(),
                        bindings::options(sequence, durability)?,
                    )
                    .await?;
                sequence += 1;
                if probe.result == b"true" {
                    break;
                }
                ensure!(probe.result == b"false");
                ensure!(
                    cellar::asyncrt::mono_ms() < end,
                    "autonomous recovery did not release the source obligation"
                );
                cellar::asyncrt::sleep(Duration::from_millis(200)).await;
            }
            let unresolved = source
                .command(&source.contract("Abandon")?, b"{}".to_vec(), original)
                .await;
            let unresolved = unresolved
                .err()
                .context("recovery must preserve the original parent's unknown result")?;
            ensure!(unresolved.ambiguous);
            let target = cluster.hosts[leader].cell(target_history)?;
            let value = target
                .query(
                    ProposalId::new([131; 16])?,
                    &target.contract("Get")?,
                    vec![],
                    QueryOptions::default(),
                )
                .await?;
            ensure!(
                value.result == b"1",
                "recovery must not replay the target mutation"
            );
            let source_value = source
                .query(
                    ProposalId::new([132; 16])?,
                    &source.contract("Get")?,
                    vec![],
                    QueryOptions::default(),
                )
                .await?;
            ensure!(
                source_value.result == b"0",
                "recovery probe must roll back every mutation"
            );
            let again = source
                .command(
                    &source.contract("Again")?,
                    b"{}".to_vec(),
                    bindings::options(sequence, durability)?,
                )
                .await?;
            ensure!(
                serde_json::from_slice::<serde_json::Value>(&again.result)?
                    == json!({"value":3,"name":"held-target"}),
                "next binding must advance beyond the sealed child sequence"
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
            eprintln!("retained binding recovery disk: {}", root.keep().display());
        }
    }
    cluster.fixtures[0].domain.shutdown().await;
    work?;
    joined
}
