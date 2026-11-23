// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Replication and applied observations during a pending physical sync.

use super::*;
use anyhow::ensure;

pub(crate) async fn observations_during_sync() -> Result<()> {
    let mut fixture = RuntimeCluster::new().await?;
    // Keep failed physical histories after the workers and domain have joined.
    let root = std::mem::replace(&mut fixture.cluster._root, tempfile::tempdir()?);
    let checked = check_observations_during_sync(&mut fixture).await;
    let closed = fixture.close().await;
    if checked.is_err() || closed.is_err() {
        let _ = root.keep();
    }
    closed?;
    checked
}

async fn check_observations_during_sync(fixture: &mut RuntimeCluster) -> Result<()> {
    let route = fixture.cluster.birth.route().clone();
    let runtime = fixture.runtimes[0].clone();
    runtime.campaign(route.history).await?;
    fixture.drive(None).await?;
    let known = history(&route, "observed-before-sync")?;
    let pending = history(&route, "unpublished-during-sync")?;
    let cold = history(&route, "initialize-after-sync")?;
    let bytes = images::image(
        None,
        "CREATE TABLE value(n); INSERT INTO value VALUES (41);",
    )?;
    let cut = images::Cut::new(&[], &bytes, 0, true, false);
    let initial = runtime
        .propose(route.history, request(&route, known.clone(), 1, &cut)?)
        .await?;
    fixture.drive(Some(initial.position.index)).await?;
    let version = runtime
        .reconciliation_authority(route.history, known.clone())
        .await?
        .cell()
        .version
        .clone();
    let record = runtime
        .enrollment_state(route.history, known.clone())
        .await?
        .record()
        .cloned();
    ensure!(record.is_some(), "initial enrollment missing");
    let before = fixture.cluster.voters[0].log.cell_groups()?[0].group_manifest()?;
    let gate = fixture.cluster.voters[0].gate.clone();
    let release = gate.arm(fault::HOLD_SYNC);
    let target = runtime
        .propose(route.history, request(&route, pending.clone(), 1, &cut)?)
        .await?;
    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified()).await?;

    let observations = async {
        let (proof, state, unpublished) = tokio::join!(
            runtime.reconciliation_authority(route.history, known.clone()),
            runtime.enrollment_state(route.history, known.clone()),
            runtime.reconciliation_authority(route.history, pending.clone()),
        );
        let proof = proof?;
        let state = state?;
        proof.check()?;
        state.check()?;
        Ok::<_, anyhow::Error>((
            proof.cell().version == version && state.record() == record.as_ref(),
            matches!(unpublished, Err(RuntimeError::NotFound)),
        ))
    };
    tokio::pin!(observations);
    let initialization = async {
        runtime
            .enrollment_authority(route.history, cold)
            .await
            .map(drop)
    };
    tokio::pin!(initialization);
    let mut early_initialization =
        tokio::time::timeout(Duration::from_millis(10), &mut initialization)
            .await
            .ok();
    let early_observations = tokio::time::timeout(Duration::from_secs(1), &mut observations)
        .await
        .ok();
    if early_initialization.is_none() {
        early_initialization = tokio::time::timeout(Duration::from_millis(10), &mut initialization)
            .await
            .ok();
    }
    let progressed = early_observations.is_some();
    let held: Result<()> = async {
        ensure!(
            !matches!(early_initialization, Some(Ok(()))),
            "initialization gained authority before sync"
        );
        let status =
            tokio::time::timeout(Duration::from_secs(1), runtime.status(route.history)).await??;
        ensure!(!status.ready, "pending sync became execution-ready");
        ensure!(
            fixture.cluster.voters[0].log.cell_groups()?[0].group_manifest()? == before,
            "unpersisted enrollment was published"
        );
        if let Some(observed) = &early_observations {
            ensure!(
                matches!(observed, Ok((true, true))),
                "observation did not select only the applied root"
            );
        }
        Ok(())
    }
    .await;
    drop(release);
    // A timeout only observes progress. Join the original requests after sync;
    // never reissue initialization or replace an uncertain proposal.
    let (observed, initialized, settled) = tokio::join!(
        async {
            match early_observations {
                Some(result) => result,
                None => observations.await,
            }
        },
        async {
            match early_initialization {
                Some(result) => result,
                None => initialization.await,
            }
        },
        fixture.drive(Some(target.position.index)),
    );
    held?;
    let (known_unchanged, _) = observed?;
    ensure!(
        known_unchanged,
        "unrelated enrollment changed the known cell"
    );
    // A refusal while the sync was held is safe; otherwise require success
    // after the original proposal has settled.
    if !matches!(initialized, Err(RuntimeError::NotReady)) {
        initialized?;
    }
    settled?;
    let published = runtime
        .reconciliation_authority(route.history, pending.clone())
        .await?;
    published.check()?;
    ensure!(published.cell().version.sequence.get() == 1);
    drop(published);
    ensure!(progressed, "applied observations waited for unrelated sync");
    Ok(())
}

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
