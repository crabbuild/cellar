// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! The node batch wakes ordinary replication while physical sync is pending.

use super::*;
use anyhow::ensure;

pub(crate) async fn append_during_sync() -> Result<()> {
    let mut fixture = RuntimeCluster::new().await?;
    let route = fixture.cluster.birth.route().clone();
    fixture.runtimes[0].campaign(route.history).await?;
    fixture.drive(None).await?;
    let before = fixture.cluster.voters[0].log.cell_groups()?[0].group_manifest()?;
    let bytes = images::image(
        None,
        "CREATE TABLE value(n); INSERT INTO value VALUES (41);",
    )?;
    let cut = images::Cut::new(&[], &bytes, 0, true, false);
    let gate = fixture.cluster.voters[0].gate.clone();
    let release = gate.arm(fault::HOLD_SYNC);
    let target = fixture.runtimes[0]
        .propose(
            route.history,
            request(&route, history(&route, "ready-runtime")?, 1, &cut)?,
        )
        .await?;
    let checked: Result<()> = async {
        tokio::time::timeout(Duration::from_secs(5), gate.entered.notified()).await?;
        // No status requests, manual ticks or new work wake the node here.
        // The runtime's normal domain timer remains enabled.
        let mut peers = Vec::new();
        for _ in 0..2 {
            let event = tokio::time::timeout(Duration::from_secs(5), fixture.events[0].recv())
                .await?
                .context("append event")?;
            let message = match event.effect().map_err(|e| anyhow::anyhow!(e.to_string()))? {
                CellGroupEffect::Message(message) => message.clone(),
                _ => anyhow::bail!("runtime published application/read authority before sync"),
            };
            ensure!(event.group() == route.history);
            ensure!(
                message.get_msg_type() == MessageType::MsgAppend
                    && message
                        .entries
                        .iter()
                        .any(|entry| entry.index == target.position.index.get()),
                "runtime did not release the pending append"
            );
            peers.push(message.to);
            fixture.network.push_back((route.history, message));
        }
        peers.sort_unstable();
        ensure!(peers == [2, 3], "append did not reach both peers");
        ensure!(
            fixture.cluster.voters[0].log.cell_groups()?[0].group_manifest()? == before,
            "runtime published a root before local sync"
        );
        Ok(())
    }
    .await;
    drop(release);
    let settled = fixture.drive(Some(target.position.index)).await;
    let closed = fixture.close().await;
    checked?;
    settled?;
    closed?;
    eprintln!("READY_RUNTIME_PIPELINE {{\"earlyAppendMessages\":2,\"leaderPublicationHeld\":true,\"appliedAfterSync\":true}}");
    Ok(())
}
