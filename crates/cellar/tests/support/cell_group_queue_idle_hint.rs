// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Separate public requests may reuse only an exact positive receipt hint.

use super::*;

pub async fn qualify(rooted: bool) -> Result<()> {
    let s = Scenario::start(
        rooted,
        Arc::new(catalog_fault::Store::default()),
        false,
        5000,
        None,
        8,
        Durability::Quorum.into(),
    )
    .await?;
    let domain = s.cluster.fixtures[0].domain.clone();
    let outcome = domain
        .run(async {
            ready(&s.cluster, &[s.dead]).await?;
            let leader = managed_fixture::leader(&s.cluster.hosts, s.dead).await?;
            let group = s.cluster.hosts[leader].group(s.dead)?;
            let before = group.status().await?;
            let hints = || -> Result<u64> {
                s.cluster.hosts.iter().try_fold(0_u64, |sum, host| {
                    let snapshot = host.performance();
                    ensure!(snapshot.available && !snapshot.saturated);
                    let count = snapshot
                        .counters
                        .iter()
                        .find(|sample| sample.name == "cell_ingress_sequence_hints")
                        .context("ingress sequence observations")?
                        .value;
                    sum.checked_add(count)
                        .context("ingress observation overflow")
                })
            };
            // Each signed HTTP entrance has its own call scope. There are no
            // consumer/timer mutations in this storage-only Queue, and the
            // first complete response precedes the second request.
            let paused = admin(&s.cluster, DEAD, json!({"op":"pause"})).await?;
            ensure!(paused["paused"] == true);
            let hinted_before = hints()?;
            let resumed = admin(&s.cluster, DEAD, json!({"op":"resume"})).await?;
            ensure!(resumed["paused"] == false);
            let hinted_after = hints()?;
            let info = admin(&s.cluster, DEAD, json!({"op":"info"})).await?;
            ensure!(info["paused"] == false && info["backlogCount"] == 0.0);
            ready(&s.cluster, &[s.dead]).await?;
            let after = group.status().await?;
            ensure!(before.term == after.term && before.leader == after.leader);
            ensure!(
                before.committed.get().checked_add(2) == Some(after.committed.get()),
                "two public operator mutations did not retain their individual commits"
            );
            ensure!(after.applied.index == after.committed);
            Ok::<_, anyhow::Error>((hinted_before, hinted_after))
        })
        .await;
    let retain =
        !matches!(&outcome, Ok(Ok((before, after))) if after.checked_sub(*before) == Some(1));
    let cleanup = s.close(retain).await;
    let (before, after) = outcome??;
    cleanup?;
    let reused = after
        .checked_sub(before)
        .context("ingress observation wrapped")?;
    ensure!(
        reused == 1,
        "completed public Queue requests reused {reused} verified successors; expected 1"
    );
    Ok(())
}
