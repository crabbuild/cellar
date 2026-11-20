// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Protocol proposals can overlap disk; persistence-dependent replies cannot.

use super::*;
use anyhow::ensure;

fn messages(owner: &mut CellGroupOwner) -> Result<Vec<Message>> {
    let mut result = Vec::new();
    for _ in 0..32 {
        match owner.poll()? {
            Some(CellGroupEffect::Message(message)) => result.push(message),
            None => return Ok(result),
            _ => anyhow::bail!("application/read authority escaped a held append"),
        }
    }
    anyhow::bail!("unbounded append output")
}

async fn replicate(voter: &mut Voter, message: Message, hold: bool) -> Result<Vec<Message>> {
    voter.owner.as_mut().unwrap().step(message)?;
    let CellGroupEffect::Persist(work) = voter
        .owner
        .as_mut()
        .unwrap()
        .poll()?
        .context("follower append persistence")?
    else {
        anyhow::bail!("follower append did not require persistence")
    };
    if hold {
        let gate = voter.gate.clone();
        let release = gate.arm(fault::HOLD_SYNC);
        let mut write = Box::pin(work.run());
        let mut finished = false;
        let checked: Result<()> = async {
            match tokio::time::timeout(
                Duration::from_secs(5),
                futures_util::future::select(&mut write, Box::pin(gate.entered.notified())),
            )
            .await?
            {
                futures_util::future::Either::Left((done, _)) => {
                    finished = true;
                    voter.owner.as_mut().unwrap().complete_persist(done?)?;
                    anyhow::bail!("follower sync did not reach its hold")
                }
                futures_util::future::Either::Right(_) => (),
            }
            ensure!(
                voter.owner.as_mut().unwrap().poll()?.is_none(),
                "follower replied or applied before its own sync"
            );
            Ok(())
        }
        .await;
        drop(release);
        let settled: Result<()> = async {
            if !finished {
                let done = write.await?;
                voter.owner.as_mut().unwrap().complete_persist(done)?;
            }
            Ok(())
        }
        .await;
        checked?;
        settled?;
    } else {
        let done = work.run().await?;
        voter.owner.as_mut().unwrap().complete_persist(done)?;
    }
    let replies = messages(voter.owner.as_mut().unwrap())?;
    ensure!(
        replies
            .iter()
            .any(|message| message.get_msg_type() == MessageType::MsgAppendResponse),
        "synced follower append lacks its response"
    );
    Ok(replies)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn leader_replication_overlaps_sync_without_early_acknowledgement() -> Result<()> {
    let mut cluster = Cluster::new().await?;
    cluster.elect().await?;
    let route = cluster.birth.route().clone();
    let cell = history(&route, "ready-network")?;
    let bytes = images::image(
        None,
        "CREATE TABLE value(n); INSERT INTO value VALUES (37);",
    )?;
    let cut = images::Cut::new(&[], &bytes, 0, true, false);
    let before = cluster.voters[0]
        .owner
        .as_ref()
        .unwrap()
        .manifest()
        .marker()
        .clone();
    let proposal = cluster.voters[0].owner.as_mut().unwrap().propose(request(
        &route,
        cell.clone(),
        1,
        &cut,
    )?)?;
    let CellGroupEffect::Persist(work) = cluster.voters[0]
        .owner
        .as_mut()
        .unwrap()
        .poll()?
        .context("leader append persistence")?
    else {
        anyhow::bail!("leader append did not require persistence")
    };
    ensure!(
        cluster.voters[0].owner.as_mut().unwrap().poll()?.is_none(),
        "append escaped before physical admission"
    );
    let gate = cluster.voters[0].gate.clone();
    let mut release = Some(gate.arm(fault::HOLD_SYNC));
    let mut write = Box::pin(work.run());
    let mut finished = false;
    let mut early = 0;
    let mut held_leader_checked = false;
    let mut follower_checked = false;
    let captured: Result<()> = async {
        match tokio::time::timeout(
            Duration::from_secs(5),
            futures_util::future::select(&mut write, Box::pin(gate.entered.notified())),
        )
        .await?
        {
            futures_util::future::Either::Left((done, _)) => {
                finished = true;
                cluster.voters[0]
                    .owner
                    .as_mut()
                    .unwrap()
                    .complete_persist(done?)?;
                anyhow::bail!("leader sync did not reach its hold")
            }
            futures_util::future::Either::Right(_) => (),
        }
        let mut outgoing = messages(cluster.voters[0].owner.as_mut().unwrap())?;
        early = outgoing
            .iter()
            .filter(|message| {
                message.get_msg_type() == MessageType::MsgAppend
                    && message
                        .entries
                        .iter()
                        .any(|entry| entry.index == proposal.position.index.get())
            })
            .count();
        // RED still executes the follower proof control after releasing local
        // sync. GREEN leaves it held while both peer disks actually advance.
        if early == 0 {
            drop(release.take());
            let done = write.as_mut().await?;
            finished = true;
            cluster.voters[0]
                .owner
                .as_mut()
                .unwrap()
                .complete_persist(done)?;
            outgoing = messages(cluster.voters[0].owner.as_mut().unwrap())?;
        }
        let mut peers = 0;
        for message in outgoing {
            ensure!(
                message.get_msg_type() == MessageType::MsgAppend
                    && message
                        .entries
                        .iter()
                        .any(|entry| entry.index == proposal.position.index.get()),
                "unexpected protocol output in append fixture"
            );
            let target = usize::try_from(message.to)?
                .checked_sub(1)
                .context("peer target")?;
            ensure!((1..=2).contains(&target));
            let replies = replicate(&mut cluster.voters[target], message, peers == 0).await?;
            follower_checked = true;
            cluster.network.extend(replies);
            peers += 1;
        }
        ensure!(
            peers == 2,
            "replication did not reach both actual peer engines"
        );
        if !finished {
            ensure!(
                cluster.voters[0].owner.as_mut().unwrap().poll()?.is_none(),
                "leader applied or acknowledged without its exact sync completion"
            );
            ensure!(
                cluster.voters[0]
                    .owner
                    .as_ref()
                    .unwrap()
                    .manifest()
                    .marker()
                    == &before
            );
            ensure!(
                cluster.voters[0].log.cell_groups()?[0]
                    .group_manifest()?
                    .as_ref()
                    == Some(&before)
            );
            held_leader_checked = true;
        }
        Ok(())
    }
    .await;
    drop(release.take());
    let settled: Result<()> = async {
        if !finished {
            let done = write.await?;
            cluster.voters[0]
                .owner
                .as_mut()
                .unwrap()
                .complete_persist(done)?;
        }
        Ok(())
    }
    .await;
    let verified: Result<()> = async {
        if captured.is_ok() && settled.is_ok() {
            cluster.drive(false).await?;
            cluster.restart(2).await?;
            cluster.drive(false).await?;
            for voter in &cluster.voters {
                let (leaf, CellImagePin::Materialized(pin)) = voter
                    .owner
                    .as_ref()
                    .unwrap()
                    .manifest()
                    .pin_cell(&cell.cell)
                    .context("applied pipeline cell")?
                else {
                    anyhow::bail!("pipeline image mode")
                };
                ensure!(leaf.version.sequence.get() == 1 && images::read(&pin)? == bytes);
            }
        }
        Ok(())
    }
    .await;
    let closed = cluster.close().await;
    captured?;
    settled?;
    verified?;
    closed?;
    eprintln!(
        "READY_NETWORK_PIPELINE {}",
        serde_json::json!({"earlyAppendMessages":early,"peerDisks":2,"followerReplyHeld":follower_checked,"leaderPublicationHeld":held_leader_checked,"exactImageAfterRestart":true})
    );
    ensure!(
        early == 2 && held_leader_checked,
        "leader replication waited for local sync instead of overlapping it"
    );
    Ok(())
}
