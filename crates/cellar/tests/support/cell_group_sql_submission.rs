// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::reconciliation::apply_through;
use super::*;

async fn initial(
    fixture: &RuntimeCluster,
    pool: &CellGroupSqlPool,
    cell: &CellHistory,
    creation: u8,
    preview: u8,
) -> Result<CellSqlPrepared> {
    let authority = fixture.runtimes[0]
        .enrollment_authority(fixture.cluster.birth.route().history, cell.clone())
        .await?;
    let CellSqlOutcome::Prepared(prepared) = pool
        .initialize(
            fixture.cell_options(cell).await?,
            authority,
            CellEnrollmentAttempt {
                creation: ProposalId::new([creation; 16])?,
                preview: ImageId::new([preview; 16])?,
                inputs: cellar::storage::CommandInputs {
                    now_unix_millis: 10,
                    random: [17; 32],
                },
            },
            |tx| {
                tx.execute_batch("CREATE TABLE value(n); INSERT INTO value VALUES (0);")?;
                Ok(())
            },
        )
        .await?
    else {
        anyhow::bail!("new enrollment")
    };
    pool.mark_submitted(cell.clone(), prepared.proposal())
        .await?;
    Ok(prepared)
}

async fn mutation(
    fixture: &RuntimeCluster,
    pool: &CellGroupSqlPool,
    cell: &CellHistory,
    proposal: u8,
    preview: u8,
) -> Result<CellSqlPrepared> {
    let authority = fixture.runtimes[0]
        .execution_authority(fixture.cluster.birth.route().history, cell.clone())
        .await?;
    let mut attempt = cell_attempt(proposal)?;
    attempt.preview = ImageId::new([preview; 16])?;
    let CellSqlOutcome::Prepared(prepared) = pool
        .stage(
            fixture.cell_options(cell).await?,
            authority,
            attempt,
            true,
            |tx| {
                let value: i64 = tx.query_row("SELECT n FROM value", [], |row| row.get(0))?;
                assert_eq!(value, 0, "rejected private effects must not be reused");
                tx.execute("UPDATE value SET n=n+1", [])?;
                Ok(b"committed".to_vec())
            },
        )
        .await?
    else {
        anyhow::bail!("new mutation")
    };
    pool.mark_submitted(cell.clone(), prepared.proposal())
        .await?;
    Ok(prepared)
}

pub(super) async fn rejections(rooted: bool) -> Result<()> {
    let mut fixture = RuntimeCluster::with_limits(256, 1, 2).await?;
    if rooted {
        for (n, voter) in fixture.cluster.voters.iter_mut().enumerate() {
            voter.extents = Some(NodeExtentStore::open(
                fixture.cluster._root.path().join(format!("submission-{n}")),
                Arc::new(DirectFileSystem),
                Default::default(),
            )?);
        }
    }
    let route = fixture.cluster.birth.route().clone();
    fixture.runtimes[0].campaign(route.history).await?;
    fixture.drive(None).await?;
    let limits = CellSqlLimits {
        workers: 2,
        queue_per_worker: 4,
        max_pending_cells: 4,
        group_pending_cells: 4,
        node_bytes: 512 * 1024 * 1024,
        group_bytes: 512 * 1024 * 1024,
    };
    let pool = CellGroupSqlPool::start(&fixture.cluster.voters[0].log, limits)?;
    let a = history(&route, "submission-a")?;
    let b = history(&route, "submission-b")?;
    let c = history(&route, "submission-c")?;
    let pa = initial(&fixture, &pool, &a, 181, 181).await?;
    let pb = initial(&fixture, &pool, &b, 182, 182).await?;

    // Drop the acceptance response, then hold real engine sync so a queued
    // ReadIndex occupies the only ordinary request slot. Queue overload must
    // not produce a false owner rejection for B.
    let gate = fixture.cluster.voters[0].gate.clone();
    let release = gate.arm(fault::HOLD_SYNC);
    drop(pa.submit(&fixture.runtimes[0]).await?);
    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified()).await?;
    let mut waiting = Box::pin(fixture.runtimes[0].read_index(route.history, vec![77]));
    assert!(
        tokio::time::timeout(Duration::from_millis(30), waiting.as_mut())
            .await
            .is_err()
    );
    assert!(matches!(
        pb.submit(&fixture.runtimes[0]).await,
        Err(RuntimeError::Overloaded)
    ));
    assert!(pb.rejection()?.is_none());
    assert!(pa.rejection()?.is_none());
    assert_eq!(pool.usage()?.active_cells, 2);
    drop(release);
    tokio::time::timeout(Duration::from_secs(5), waiting).await??;
    let accepted_a = accepted_sql(&fixture.runtimes[0], &pa).await?;
    assert!(pa.rejection()?.is_none());
    let state = fixture.runtimes[0]
        .enrollment_state(route.history, b.clone())
        .await?;
    assert!(matches!(
        pool.resolve_enrollment(pb.proposal(), state).await,
        Err(ExecutionError::Uncertain)
    ));

    // The actual owner now refuses B against A's retained acceptance slot.
    // Discard the response and recover its irreversible seal from the producer.
    drop(pb.submit(&fixture.runtimes[0]).await?);
    let proof_b = pb.rejection()?.context("sealed B")?;
    assert!(pool.reject(a.clone(), proof_b.clone()).await.is_err());
    assert_eq!(pool.usage()?.active_cells, 2);
    let foreign = CellGroupSqlPool::start(&fixture.cluster.voters[1].log, limits)?;
    assert!(foreign.reject(b.clone(), proof_b.clone()).await.is_err());
    foreign.close().await?;
    assert!(matches!(
        pb.submit(&fixture.runtimes[1]).await,
        Err(RuntimeError::Invalid(_))
    ));
    pool.reject(b.clone(), proof_b.clone()).await?;
    assert_eq!(pool.usage()?.active_cells, 1);
    assert!(pool.usage()?.retained_bytes > 0);
    assert!(matches!(
        fixture.runtimes[0]
            .propose_at_term(route.history, pb.term(), pb.bytes().to_vec())
            .await,
        Err(RuntimeError::Invalid(_))
    ));
    apply_through(&mut fixture, accepted_a.position.index).await?;
    drop(super::enrollment::resolved(&fixture, &pool, 0, &a, pa.proposal()).await?);

    // Retrying after capacity returns must still reject every old clone. A fresh
    // producer of the same creation identity gets a distinct submission seal.
    let fresh_b = initial(&fixture, &pool, &b, 182, 183).await?;
    assert_eq!(pb.proposal(), fresh_b.proposal());
    assert!(pool.reject(b.clone(), proof_b.clone()).await.is_err());
    assert_eq!(pool.usage()?.active_cells, 1);
    assert!(matches!(
        pb.submit(&fixture.runtimes[0]).await?,
        CellSqlSubmission::Rejected(_)
    ));
    let accepted_b = accepted_sql(&fixture.runtimes[0], &fresh_b).await?;
    assert_eq!(
        accepted_b.position.index.get(),
        accepted_a.position.index.get() + 1
    );
    apply_through(&mut fixture, accepted_b.position.index).await?;
    drop(super::enrollment::resolved(&fixture, &pool, 0, &b, fresh_b.proposal()).await?);
    let pc = initial(&fixture, &pool, &c, 184, 184).await?;
    drop(pc.submit(&fixture.runtimes[0]).await?);
    pool.reject(c.clone(), pc.rejection()?.context("full group rejection")?)
        .await?;
    assert_eq!(pool.usage()?.active_cells, 0);
    assert!(fixture.runtimes[0]
        .enrollment_state(route.history, c)
        .await?
        .record()
        .is_none());

    // Completed/rejected producers release cell names, but their retained
    // outputs still consume all four operation reservations. Release this
    // phase's outputs before preparing the independent mutation phase.
    assert_eq!(pool.usage()?.retained_operations, 4);
    drop((pa, pb, fresh_b, pc, proof_b));
    assert_eq!(pool.usage()?.retained_operations, 0);
    assert_eq!(pool.usage()?.retained_bytes, 0);

    let ma = mutation(&fixture, &pool, &a, 185, 185).await?;
    let mb = mutation(&fixture, &pool, &b, 186, 186).await?;
    drop(ma.submit(&fixture.runtimes[0]).await?);
    let accepted_ma = accepted_sql(&fixture.runtimes[0], &ma).await?;
    drop(mb.submit(&fixture.runtimes[0]).await?);
    let proof_mb = mb.rejection()?.context("mutation seal")?;
    assert!(pool.reject(a.clone(), proof_mb.clone()).await.is_err());
    pool.reject(b.clone(), proof_mb.clone()).await?;
    apply_through(&mut fixture, accepted_ma.position.index).await?;
    let authority = fixture.runtimes[0]
        .execution_authority(route.history, a.clone())
        .await?;
    assert_eq!(
        pool.finish(authority).await?.receipt().receipt.result(),
        b"committed"
    );
    let cached = accepted_sql(&fixture.runtimes[0], &ma).await?;
    assert_eq!(cached.position, accepted_ma.position);
    assert_eq!(cached.digest, accepted_ma.digest);
    assert!(ma.rejection()?.is_none());
    let fresh_mb = mutation(&fixture, &pool, &b, 186, 187).await?;
    assert_eq!(mb.proposal(), fresh_mb.proposal());
    assert!(pool.reject(b.clone(), proof_mb.clone()).await.is_err());
    assert!(matches!(
        mb.submit(&fixture.runtimes[0]).await?,
        CellSqlSubmission::Rejected(_)
    ));
    let accepted_mb = accepted_sql(&fixture.runtimes[0], &fresh_mb).await?;
    assert_eq!(
        accepted_mb.position.index.get(),
        accepted_ma.position.index.get() + 1
    );
    apply_through(&mut fixture, accepted_mb.position.index).await?;
    let authority = fixture.runtimes[0]
        .execution_authority(route.history, b.clone())
        .await?;
    assert_eq!(
        pool.finish(authority).await?.receipt().receipt.result(),
        b"committed"
    );
    assert_eq!(pool.usage()?.active_cells, 0);
    for node in 0..3 {
        for cell in [&a, &b] {
            let proof = fixture.runtimes[node]
                .reconciliation_authority(route.history, cell.clone())
                .await?;
            assert_eq!(proof.cell().version.sequence.get(), 2);
        }
    }
    drop((ma, mb, fresh_mb, proof_mb));
    pool.close().await?;
    assert_eq!(pool.usage()?.retained_bytes, 0);
    assert_eq!(pool.usage()?.running_workers, 0);
    if rooted {
        assert_eq!(
            fixture.cluster.voters[0]
                .extents
                .as_ref()
                .context("extents")?
                .usage()?
                .workspaces,
            0
        );
    }
    fixture.close().await
}
