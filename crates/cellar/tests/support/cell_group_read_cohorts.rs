// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Actual RawNode barriers with queued arrivals, cancellation and separate pins.
use super::*;
use futures_util::FutureExt;

fn count(fixture: &RuntimeCluster, name: &str) -> Result<u64> {
    fixture.cluster.voters[0]
        .log
        .performance_snapshot()
        .counters
        .iter()
        .find(|counter| counter.name == name)
        .map(|counter| counter.value)
        .with_context(|| format!("missing cohort observation {name}"))
}

pub(crate) async fn run(rooted: bool) -> Result<()> {
    run_scenario(rooted, None).await
}

pub(crate) async fn invalidated(rooted: bool, configuration: bool) -> Result<()> {
    run_scenario(rooted, Some(configuration)).await
}

async fn run_scenario(rooted: bool, invalidation: Option<bool>) -> Result<()> {
    let (mut fixture, route, a, b) = enrolled_sql_cells(rooted).await?;
    let c = history(&route, "sql-c")?;
    let pool = pool(&fixture, 0)?;
    let oa = fixture.cell_options(&a).await?;
    let ob = fixture.cell_options(&b).await?;
    let oc = fixture.cell_options(&c).await?;
    let av = selected(&mut fixture, 0, &a, CellReadConsistency::Stale)
        .await?
        .cell()
        .version
        .clone();
    let cv = selected(&mut fixture, 0, &c, CellReadConsistency::Stale)
        .await?
        .cell()
        .version
        .clone();
    let runtime = fixture.runtimes[0].clone();
    let authority = runtime
        .execution_authority(route.history, b.clone())
        .await?;
    let CellSqlOutcome::Prepared(prepared) = pool
        .stage(ob, authority, cell_attempt(211)?, true, |tx| {
            tx.execute("UPDATE value SET n=n+1", [])?;
            Ok(b"cohort-queue-gate".to_vec())
        })
        .await?
    else {
        anyhow::bail!("cohort gate preparation")
    };
    let barriers = count(&fixture, "cell_linearizable_read_cohorts_started")?;
    let joins = count(&fixture, "cell_linearizable_read_cohort_joins")?;
    pool.mark_submitted(b.clone(), prepared.proposal()).await?;
    let gate = fixture.cluster.voters[0].gate.clone();
    let release = gate.arm(fault::HOLD_SYNC);
    let accepted = accepted_sql(&runtime, &prepared).await?;
    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified()).await?;
    let until = deadline(&fixture, 5000).await?;
    let mut first = Box::pin(runtime.read_authority(
        route.history,
        a.clone(),
        CellReadConsistency::Linearizable,
        until,
    ));
    let mut second = Box::pin(runtime.read_authority(
        route.history,
        c.clone(),
        CellReadConsistency::Linearizable,
        until,
    ));
    assert!(first.as_mut().now_or_never().is_none());
    let term = runtime.status(route.history).await?.term;
    let mut heartbeat = Message {
        from: 2,
        to: 1,
        term: term.get(),
        ..Default::default()
    };
    heartbeat.set_msg_type(MessageType::MsgHeartbeatResponse);
    runtime.admit_step(route.history, heartbeat).await?;
    assert!(second.as_mut().now_or_never().is_none());
    // Status is served while persistence is held on the same FIFO mailbox.
    // Its reply orders after both reads and the intervening peer frame.
    runtime.status(route.history).await?;
    assert_eq!(
        count(&fixture, "cell_linearizable_read_cohorts_started")?,
        barriers
    );
    assert_eq!(runtime.usage().read_pins.items, 2);
    drop(release);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let current = count(&fixture, "cell_linearizable_read_cohorts_started")?;
            assert!(current <= barriers + 1);
            if current == barriers + 1 {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await??;
    assert_eq!(
        count(&fixture, "cell_linearizable_read_cohort_joins")?,
        joins + 1
    );
    if let Some(configuration) = invalidation {
        // Apply the gating mutation without letting the old read quorum reply.
        // Append responses still flow, so this is a read barrier cut rather
        // than a stalled consensus or SQL preparation fixture.
        tokio::time::timeout(Duration::from_secs(5), async {
            while runtime.status(route.history).await?.applied.index < accepted.position.index {
                assert!(first.as_mut().now_or_never().is_none());
                assert!(second.as_mut().now_or_never().is_none());
                fixture
                    .pump_with_network(&[], &mut BTreeMap::new(), |_, message| {
                        message.to != 1
                            || message.get_msg_type() != MessageType::MsgHeartbeatResponse
                    })
                    .await?;
                tokio::task::yield_now().await;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await??;
        drop(
            pool.finish(
                runtime
                    .execution_authority(route.history, b.clone())
                    .await?,
            )
            .await?,
        );
        drop(prepared);
        let before = runtime.status(route.history).await?;
        let revision = fixture.cluster.voters[0]
            .log
            .cell_group(&route.history)?
            .group_manifest()?
            .context("cohort manifest before invalidation")?
            .membership_revision();
        if configuration {
            let group = fixture.cluster.voters[0].log.cell_group(&route.history)?;
            let change = super::super::membership::change(
                3,
                raft::prelude::ConfChangeType::AddLearnerNode,
                raft::prelude::ConfChangeTransition::Explicit,
            );
            let request =
                CellGroupMembershipChange::new(&route, &group.membership()?, change, &[])?;
            runtime.configure(route.history, request).await?;
        } else {
            runtime.transfer_leader(route.history, 3).await?;
        }
        let mut results = [None, None];
        tokio::time::timeout(Duration::from_secs(5), async {
            while results.iter().any(Option::is_none) {
                if results[0].is_none() {
                    results[0] = first.as_mut().now_or_never();
                }
                if results[1].is_none() {
                    results[1] = second.as_mut().now_or_never();
                }
                fixture
                    .pump_with_network(&[], &mut BTreeMap::new(), |_, message| {
                        message.to != 1
                            || message.get_msg_type() != MessageType::MsgHeartbeatResponse
                    })
                    .await?;
                tokio::task::yield_now().await;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await??;
        for result in results {
            assert!(matches!(result.unwrap(), Err(RuntimeError::NotLeader)));
        }
        let after = runtime.status(route.history).await?;
        let after_revision = fixture.cluster.voters[0]
            .log
            .cell_group(&route.history)?
            .group_manifest()?
            .context("cohort manifest after invalidation")?
            .membership_revision();
        if configuration {
            assert_eq!(after.term, before.term);
            assert!(after_revision > revision);
        } else {
            assert!(after.term > before.term);
        }
        assert_eq!(runtime.usage().read_pins.items, 0);
        pool.close().await?;
        return fixture.close().await;
    }
    // No peer responses have been pumped: the first barrier is still pending.
    let mut late = Box::pin(runtime.read_authority(
        route.history,
        a.clone(),
        CellReadConsistency::Linearizable,
        until,
    ));
    assert!(late.as_mut().now_or_never().is_none());
    runtime.status(route.history).await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        while count(&fixture, "cell_linearizable_read_cohorts_started")? < barriers + 2 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    assert_eq!(
        count(&fixture, "cell_linearizable_read_cohorts_started")?,
        barriers + 2
    );
    assert_eq!(
        count(&fixture, "cell_linearizable_read_cohort_joins")?,
        joins + 1
    );
    drop(first);
    tokio::time::timeout(Duration::from_secs(5), async {
        while runtime.usage().read_pins.items != 2 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await?;
    let mut both = Box::pin(async { Ok::<_, anyhow::Error>((second.await?, late.await?)) });
    let (second, late) = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(selected) = both.as_mut().now_or_never() {
                break selected;
            }
            // Finish each physical pump turn; observing a read must not drop
            // a partly delivered peer frame belonging to the earlier mutation.
            fixture.pump(&[], &mut BTreeMap::new()).await?;
            tokio::task::yield_now().await;
        }
    })
    .await??;
    assert_eq!(second.cell().version, cv);
    assert_eq!(late.cell().version, av);
    assert!(second.barrier().is_some() && late.barrier().is_some());
    assert_eq!(runtime.usage().read_pins.items, 2);
    fixture.drive(Some(accepted.position.index)).await?;
    let receipt = pool
        .finish(runtime.execution_authority(route.history, b).await?)
        .await?;
    drop(receipt);
    drop(prepared);
    let output = value(&pool, oc, second).await?;
    assert_eq!(output.result(), 0_i64.to_le_bytes());
    drop(output);
    assert_eq!(runtime.usage().read_pins.items, 1);
    let output = value(&pool, oa, late).await?;
    assert_eq!(output.result(), 0_i64.to_le_bytes());
    drop(output);
    assert_eq!(runtime.usage().read_pins.items, 0);
    pool.close().await?;
    fixture.close().await
}
