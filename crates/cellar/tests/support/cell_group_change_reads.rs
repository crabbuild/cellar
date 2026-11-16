// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A cell-change wait consumes no SQL/image work or unrelated read cohort.
use super::*;
use futures_util::FutureExt;

fn barriers(fixture: &RuntimeCluster) -> Result<u64> {
    fixture.cluster.voters[0]
        .log
        .performance_snapshot()
        .counters
        .iter()
        .find(|counter| counter.name == "cell_linearizable_read_barriers")
        .map(|counter| counter.value)
        .context("read barrier counter")
}

async fn mutate(
    fixture: &mut RuntimeCluster,
    pool: &CellGroupSqlPool,
    cell: &CellHistory,
    attempt: u8,
    hold_read_quorum: bool,
) -> Result<cellar_logic::consensus::LogIndex> {
    let group = fixture.cluster.birth.route().history;
    let authority = fixture.runtimes[0]
        .execution_authority(group, cell.clone())
        .await?;
    let options = fixture.cell_options(cell).await?;
    let CellSqlOutcome::Prepared(prepared) = pool
        .stage(options, authority, cell_attempt(attempt)?, true, |tx| {
            tx.execute("UPDATE value SET n=n+1", [])?;
            Ok(b"advanced".to_vec())
        })
        .await?
    else {
        anyhow::bail!("change preparation required");
    };
    pool.mark_submitted(cell.clone(), prepared.proposal())
        .await?;
    let accepted = accepted_sql(&fixture.runtimes[0], &prepared).await?;
    if hold_read_quorum {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let mut complete = true;
                for runtime in &fixture.runtimes {
                    complete &=
                        runtime.status(group).await?.applied.index >= accepted.position.index;
                }
                if complete {
                    return Ok::<_, anyhow::Error>(());
                }
                fixture
                    .pump_with_network(&[], &mut BTreeMap::new(), |_, message| {
                        message.to != 1
                            || !matches!(
                                message.get_msg_type(),
                                raft::prelude::MessageType::MsgHeartbeatResponse
                                    | raft::prelude::MessageType::MsgReadIndexResp
                            )
                    })
                    .await?;
                tokio::task::yield_now().await;
            }
        })
        .await??;
    } else {
        fixture.drive(Some(accepted.position.index)).await?;
    }
    drop(
        pool.finish(
            fixture.runtimes[0]
                .execution_authority(group, cell.clone())
                .await?,
        )
        .await?,
    );
    Ok(accepted.position.index)
}

pub(crate) async fn run(rooted: bool) -> Result<()> {
    let (mut fixture, route, a, b) = enrolled_sql_cells(rooted).await?;
    let pool = pool(&fixture, 0)?;
    let work = async {
        let runtime = fixture.runtimes[0].clone();
        let observed = selected(&mut fixture, 0, &a, CellReadConsistency::Stale)
            .await?
            .cell()
            .version
            .clone();
        let count = barriers(&fixture)?;
        let mut changed = Box::pin(runtime.read_authority(
            route.history,
            a.clone(),
            CellReadConsistency::LinearizableAfter(observed.clone()),
            deadline(&fixture, 5000).await?,
        ));
        assert!(changed.as_mut().now_or_never().is_none());
        runtime.status(route.history).await?;
        assert_eq!(
            barriers(&fixture)?,
            count,
            "unchanged cell issued ReadIndex"
        );
        assert_eq!(pool.usage()?.active_cells, 0);
        // An ordinary reader still completes while this cell's waiter is parked.
        drop(selected(&mut fixture, 0, &b, CellReadConsistency::Linearizable).await?);
        assert!(changed.as_mut().now_or_never().is_none());
        let count = barriers(&fixture)?;
        mutate(&mut fixture, &pool, &b, 231, false).await?;
        assert!(
            changed.as_mut().now_or_never().is_none(),
            "another cell released the wait"
        );
        assert_eq!(
            barriers(&fixture)?,
            count,
            "unrelated commit issued ReadIndex"
        );
        let changed_at = mutate(&mut fixture, &pool, &a, 232, true).await?;
        runtime.status(route.history).await?;
        anyhow::ensure!(
            changed.as_mut().now_or_never().is_none(),
            "cell change reused a quorum barrier from before the change"
        );
        let authority = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(result) = changed.as_mut().now_or_never() {
                    return result.map_err(anyhow::Error::from);
                }
                fixture.pump(&[], &mut BTreeMap::new()).await?;
                tokio::task::yield_now().await;
            }
        })
        .await??;
        assert!(authority.cell().version.sequence > observed.sequence);
        assert!(
            authority.barrier().is_some(),
            "changed read has no fresh quorum"
        );
        assert!(
            authority.barrier().unwrap().index >= changed_at,
            "change read selected an older quorum cut"
        );
        let current = authority.cell().version.clone();
        let options = fixture.cell_options(&a).await?;
        assert_eq!(
            value(&pool, options, authority).await?.result(),
            1_i64.to_le_bytes()
        );
        // Even an already advanced image cannot reuse an earlier quorum result.
        assert!(matches!(
            runtime
                .read_authority(
                    route.history,
                    a.clone(),
                    CellReadConsistency::LinearizableAfter(observed),
                    deadline(&fixture, 40).await?
                )
                .await,
            Err(RuntimeError::DeadlineExceeded)
        ));
        fixture.drive(None).await?;
        assert!(matches!(
            runtime
                .read_authority(
                    route.history,
                    a.clone(),
                    CellReadConsistency::LinearizableAfter(current.clone()),
                    deadline(&fixture, 40).await?
                )
                .await,
            Err(RuntimeError::DeadlineExceeded)
        ));
        let mut cancelled = Box::pin(runtime.read_authority(
            route.history,
            a.clone(),
            CellReadConsistency::LinearizableAfter(current),
            deadline(&fixture, 5000).await?,
        ));
        assert!(cancelled.as_mut().now_or_never().is_none());
        runtime.status(route.history).await?;
        drop(cancelled);
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                runtime.status(route.history).await?;
                if runtime.usage().read_pins.items == 0 {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await??;
        let wrong = selected(&mut fixture, 0, &b, CellReadConsistency::Stale)
            .await?
            .cell()
            .version
            .clone();
        assert!(matches!(
            runtime
                .read_authority(
                    route.history,
                    a.clone(),
                    CellReadConsistency::LinearizableAfter(wrong),
                    deadline(&fixture, 1000).await?
                )
                .await,
            Err(RuntimeError::Invalid(_))
        ));
        assert_eq!(pool.usage()?.active_cells, 0);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let closed = pool.close().await;
    if work.is_err() || closed.is_err() {
        let root = std::mem::replace(&mut fixture.cluster._root, tempfile::tempdir()?);
        eprintln!("retained cell-change disk: {}", root.keep().display());
    }
    let joined = fixture.close().await;
    work?;
    closed?;
    joined
}
