// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Actual three-voter SQL and checkpoint boundaries at a small entry ceiling.
//! Full-backend outage authority and physical file amplification are separate.
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

pub(crate) async fn retained_log_admission(rooted: bool) -> Result<()> {
    let mut retention = CellGroupLogRetentionLimits::default();
    retention.group.entries = 5;
    retention.progress_group.entries = 1;
    let cluster = Cluster::with_store_retention(
        RaftRuntimeOptions::default(),
        tempfile::tempdir()?,
        Arc::new(DirectFileSystem),
        [6; 32],
        Arc::new(object_store::memory::InMemory::new()),
        Durability::Quorum,
        retention,
    )
    .await?;
    let fixture = RuntimeCluster::from_cluster(cluster, 256, 256, 1_000_000).await?;
    let (mut fixture, route, a, b) =
        enroll_sql_cells(fixture, rooted, Arc::new(DirectFileSystem)).await?;
    let group = fixture.cluster.voters[0].log.cell_group(&route.history)?;
    assert_eq!(group.log_retention_usage()?.retained_entries, 4);
    let mut executor = fixture.private_cell(&a).await?;
    let (mut executor, prepared) = fixture
        .cluster
        .domain
        .blocking(move || -> Result<_> {
            let CellStageOutcome::Prepared(prepared) =
                executor.stage(cell_attempt(91)?, true, |tx| {
                    tx.execute("UPDATE value SET n=n+1", [])?;
                    Ok(b"retention-result".to_vec())
                })?
            else {
                anyhow::bail!("prepared A")
            };
            executor.mark_submitted(prepared.proposal())?;
            Ok((executor, prepared))
        })?
        .await??;
    let reserved = group.log_retention_usage()?;
    assert_eq!(
        (reserved.retained_entries, reserved.reserved_entries),
        (4, 1)
    );
    let calls = Arc::new(AtomicUsize::new(0));
    for committed in [false, true] {
        if committed {
            let accepted = accepted_private(&fixture.runtimes[0], &prepared).await?;
            fixture.drive(Some(accepted.position.index)).await?;
            assert_eq!(group.log_retention_usage()?.reserved_entries, 0);
            assert_eq!(group.log_retention_usage()?.retained_entries, 5);
        }
        let mut other = fixture.private_cell(&b).await?;
        let observed = calls.clone();
        fixture
            .cluster
            .domain
            .blocking(move || -> Result<()> {
                assert!(matches!(
                    other.stage(cell_attempt(92)?, true, move |_| {
                        observed.fetch_add(1, Ordering::SeqCst);
                        Ok(vec![])
                    }),
                    Err(ExecutionError::Refused(CommandFailure::Overloaded))
                ));
                other.close()?;
                Ok(())
            })?
            .await??;
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
    let authority = fixture.runtimes[0]
        .execution_authority(route.history, a.clone())
        .await?;
    fixture
        .cluster
        .domain
        .blocking(move || -> Result<()> {
            assert_eq!(
                executor.finish(authority)?.receipt.result(),
                b"retention-result"
            );
            executor.close()?;
            Ok(())
        })?
        .await??;
    // A known receipt still resolves when no new ordinary entry can fit.
    let mut duplicate = fixture.private_cell(&a).await?;
    fixture
        .cluster
        .domain
        .blocking(move || -> Result<()> {
            let CellStageOutcome::Duplicate(receipt) =
                duplicate.stage(cell_attempt(93)?, true, |_| {
                    panic!("duplicate callback must not run at the retention ceiling")
                })?
            else {
                anyhow::bail!("known receipt")
            };
            assert_eq!(receipt.receipt.result(), b"retention-result");
            duplicate.close()?;
            Ok(())
        })?
        .await??;
    let d = history(&route, "retention-new")?;
    let pool = CellGroupSqlPool::start(
        &fixture.cluster.voters[0].log,
        CellSqlLimits {
            workers: 1,
            queue_per_worker: 2,
            max_pending_cells: 2,
            group_pending_cells: 2,
            node_bytes: 512 * 1024 * 1024,
            group_bytes: 512 * 1024 * 1024,
        },
    )?;
    let authority = fixture.runtimes[0]
        .enrollment_authority(route.history, d.clone())
        .await?;
    let observed = calls.clone();
    let result = pool
        .initialize(
            fixture.cell_options(&d).await?,
            authority,
            CellEnrollmentAttempt {
                creation: ProposalId::new([94; 16])?,
                preview: ImageId::new([94; 16])?,
                inputs: cellar::storage::CommandInputs {
                    now_unix_millis: 10,
                    random: [17; 32],
                },
            },
            move |_| {
                observed.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        )
        .await;
    assert!(matches!(
        result,
        Err(ExecutionError::Refused(CommandFailure::Overloaded))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    pool.close().await?;
    drop(pool);
    // The saturated group must not consume the sibling's own entry allowance.
    let sibling = fixture.add_group(47).await?;
    fixture.runtimes[0].campaign(sibling).await?;
    fixture.settle(&[route.history, sibling]).await?;
    let other_route = fixture.extra[&sibling][0]
        .log
        .cell_group(&sibling)?
        .group_manifest()?
        .context("sibling root")?
        .route()
        .clone();
    let image = images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES(41);")?;
    let cut = images::Cut::new(&[], &image, 0, true, false);
    let bytes = request(&other_route, history(&other_route, "healthy")?, 1, &cut)?;
    fixture.runtimes[0].propose(sibling, bytes).await?;
    fixture.settle(&[route.history, sibling]).await?;
    assert_eq!(group.log_retention_usage()?.retained_entries, 5);
    // Only actual selected local checkpoints retire debt. Use the original
    // one-source pool and wait for each physical publication before recapture.
    let checkpoints = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    for node in 0..3 {
        let source = fixture.runtimes[node]
            .capture_checkpoint(route.history, &checkpoints)
            .await?;
        let prepared = fixture
            .cluster
            .domain
            .blocking(move || source.prepare_publication())?
            .await??;
        fixture.runtimes[node]
            .publish_checkpoint(route.history, prepared)
            .await?;
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                fixture.pump(&[], &mut BTreeMap::new()).await?;
                let usage = fixture.cluster.voters[node]
                    .log
                    .cell_group(&route.history)?
                    .log_retention_usage()?;
                if usage.retained_entries == 0
                    && fixture.runtimes[node].status(route.history).await?.ready
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await??;
    }
    assert_eq!(checkpoints.usage().sources, 0);
    let mut executor = fixture.private_cell(&b).await?;
    let (mut executor, next) = fixture
        .cluster
        .domain
        .blocking(move || -> Result<_> {
            let CellStageOutcome::Prepared(next) =
                executor.stage(cell_attempt(95)?, true, |tx| {
                    tx.execute("UPDATE value SET n=n+7", [])?;
                    Ok(b"after-checkpoint".to_vec())
                })?
            else {
                anyhow::bail!("new callback after actual reclaim")
            };
            executor.mark_submitted(next.proposal())?;
            Ok((executor, next))
        })?
        .await??;
    let accepted = accepted_private(&fixture.runtimes[0], &next).await?;
    fixture.drive(Some(accepted.position.index)).await?;
    let authority = fixture.runtimes[0]
        .execution_authority(route.history, b.clone())
        .await?;
    fixture
        .cluster
        .domain
        .blocking(move || -> Result<()> {
            assert_eq!(
                executor.finish(authority)?.receipt.result(),
                b"after-checkpoint"
            );
            executor.close()?;
            Ok(())
        })?
        .await??;
    assert_eq!(group.log_retention_usage()?.reserved_entries, 0);
    drop(prepared);
    drop(next);
    fixture.close().await
}
