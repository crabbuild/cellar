// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

pub(super) async fn admission(rooted: bool) -> Result<()> {
    let (mut fixture, route, a, b) = enrolled_sql_cells(rooted).await?;
    let c = history(&route, "sql-c")?;
    let key = |cell: &CellHistory| -> u64 {
        u64::from_le_bytes(cell_image_history_digest(cell)[..8].try_into().unwrap())
    };
    let workers = (2..=8)
        .find(|n| key(&a) % n != key(&b) % n)
        .context("distinct workers")? as usize;
    let pool = CellGroupSqlPool::start(
        &fixture.cluster.voters[0].log,
        CellSqlLimits {
            workers,
            queue_per_worker: 2,
            max_pending_cells: 3,
            group_pending_cells: 2,
            node_bytes: 512 * 1024 * 1024,
            group_bytes: 256 * 1024 * 1024,
        },
    )?;
    assert!(CellGroupSqlPool::start(
        &fixture.cluster.voters[0].log,
        CellSqlLimits {
            workers: 1,
            queue_per_worker: 1,
            max_pending_cells: 1,
            group_pending_cells: 1,
            node_bytes: 128 * 1024 * 1024,
            group_bytes: 128 * 1024 * 1024,
        }
    )
    .is_err());
    let oa = fixture.cell_options(&a).await?;
    let ob = fixture.cell_options(&b).await?;
    let oc = fixture.cell_options(&c).await?;
    let foreign = CellGroupSqlPool::start(
        &fixture.cluster.voters[1].log,
        CellSqlLimits {
            workers: 1,
            queue_per_worker: 1,
            max_pending_cells: 1,
            group_pending_cells: 1,
            node_bytes: 128 * 1024 * 1024,
            group_bytes: 128 * 1024 * 1024,
        },
    )?;
    let ca = fixture.runtimes[0]
        .execution_authority(route.history, a.clone())
        .await?;
    assert!(matches!(
        foreign
            .stage(oa.clone(), ca, cell_attempt(90)?, true, |_| panic!(
                "cross-engine authority"
            ))
            .await,
        Err(ExecutionError::Invalid(_))
    ));
    assert_eq!(foreign.usage()?.active_cells, 0);
    foreign.close().await?;
    drop(foreign);
    let ca = fixture.runtimes[0]
        .execution_authority(route.history, a.clone())
        .await?;
    let (entered, started) = tokio::sync::oneshot::channel();
    let (release, wait) = std::sync::mpsc::sync_channel(1);
    let caller = pool.clone();
    let options = oa.clone();
    let attempt = cell_attempt(91)?;
    let pa = attempt.proposal;
    let waiter = fixture.cluster.domain.spawn(async move {
        caller
            .stage(options, ca, attempt, true, move |tx| {
                tx.execute("UPDATE value SET n=n+1", [])?;
                let _ = entered.send(());
                wait.recv_timeout(Duration::from_secs(10))?;
                Ok(b"a-pool".to_vec())
            })
            .await
    })?;
    tokio::time::timeout(Duration::from_secs(5), started).await??;
    waiter.abort();
    assert!(waiter.await.is_err());
    let retained = pool.usage()?;
    assert_eq!(retained.active_cells, 1);
    assert!(retained.retained_bytes > 0);
    let ca = fixture.runtimes[0]
        .execution_authority(route.history, a.clone())
        .await?;
    assert!(matches!(
        pool.stage(oa.clone(), ca, cell_attempt(92)?, true, |_| panic!(
            "cancelled waiter released cell ownership"
        ))
        .await,
        Err(ExecutionError::Busy)
    ));
    let cb = fixture.runtimes[0]
        .execution_authority(route.history, b.clone())
        .await?;
    let CellSqlOutcome::Prepared(pb) = tokio::time::timeout(
        Duration::from_secs(5),
        pool.stage(ob.clone(), cb, cell_attempt(93)?, true, |tx| {
            tx.execute("UPDATE value SET n=n+7", [])?;
            Ok(b"b-pool".to_vec())
        }),
    )
    .await??
    else {
        anyhow::bail!("prepared B")
    };
    assert_eq!(pool.usage()?.active_cells, 2);
    assert!(
        pool.usage()?.retained_bytes < 2 * retained.retained_bytes,
        "completed capture retained its full transient allocation"
    );
    let cc = fixture.runtimes[0]
        .execution_authority(route.history, c.clone())
        .await?;
    assert!(matches!(
        pool.stage(oc, cc, cell_attempt(94)?, true, |_| panic!(
            "group quota exceeded"
        ))
        .await,
        Err(ExecutionError::Refused(
            cellar_logic::consensus::CommandFailure::Overloaded
        ))
    ));
    // B completes through the same consensus group while A's physical SQL is held.
    pool.mark_submitted(b.clone(), pb.proposal()).await?;
    pool.mark_submitted(b.clone(), pb.proposal()).await?;
    let proposal = fixture.runtimes[0]
        .propose(route.history, pb.bytes().to_vec())
        .await?;
    fixture.drive(Some(proposal.position.index)).await?;
    let cb = fixture.runtimes[0]
        .execution_authority(route.history, b.clone())
        .await?;
    let rb = pool.finish(cb).await?;
    assert_eq!(rb.receipt().receipt.result(), b"b-pool");
    assert_eq!(pool.usage()?.active_cells, 1);
    drop(rb);
    drop(pb);
    assert_eq!(pool.usage()?.retained_bytes, retained.retained_bytes);
    release.send(())?;
    let pa = tokio::time::timeout(Duration::from_secs(5), pool.resume(a.clone(), pa)).await??;
    pool.mark_submitted(a.clone(), pa.proposal()).await?;
    let proposal = fixture.runtimes[0]
        .propose(route.history, pa.bytes().to_vec())
        .await?;
    fixture.drive(Some(proposal.position.index)).await?;
    let ca = fixture.runtimes[0]
        .execution_authority(route.history, a.clone())
        .await?;
    let before = ca.cell().version.clone();
    let ra = pool.finish(ca).await?;
    assert_eq!(ra.receipt().receipt.result(), b"a-pool");
    drop(ra);
    drop(pa);
    assert_eq!(pool.usage()?.active_cells, 0);
    assert_eq!(pool.usage()?.retained_bytes, 0);
    let cb = fixture.runtimes[0]
        .execution_authority(route.history, b.clone())
        .await?;
    let duplicate = pool
        .stage(ob, cb, cell_attempt(95)?, false, |_| {
            panic!("receipt replayed callback")
        })
        .await?;
    let CellSqlOutcome::Receipt(receipt) = duplicate else {
        anyhow::bail!("duplicate receipt")
    };
    assert_eq!(receipt.receipt().receipt.result(), b"b-pool");
    drop(receipt);
    // Cancelling close's waiter must not detach the physical callback or its lock.
    let ca = fixture.runtimes[0]
        .execution_authority(route.history, a.clone())
        .await?;
    let (entered, started) = tokio::sync::oneshot::channel();
    let (release, wait) = std::sync::mpsc::sync_channel(1);
    let mut attempt = cell_attempt(96)?;
    attempt.request.id = CommandId::new(ClientId::new([81; 16])?, 2)?;
    let caller = pool.clone();
    let waiter = fixture.cluster.domain.spawn(async move {
        caller
            .stage(oa, ca, attempt, true, move |tx| {
                tx.execute("UPDATE value SET n=n+100", [])?;
                let _ = entered.send(());
                wait.recv_timeout(Duration::from_secs(10))?;
                Ok(b"private-only".to_vec())
            })
            .await
    })?;
    tokio::time::timeout(Duration::from_secs(5), started).await??;
    waiter.abort();
    assert!(waiter.await.is_err());
    let closer = pool.clone();
    let mut closing = fixture
        .cluster
        .domain
        .spawn(async move { closer.close().await })?;
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut closing)
            .await
            .is_err()
    );
    closing.abort();
    assert!(closing.await.is_err());
    assert!(pool.usage()?.running_workers > 0);
    assert_eq!(pool.usage()?.active_cells, 1);
    assert!(CellGroupSqlPool::start(
        &fixture.cluster.voters[0].log,
        CellSqlLimits {
            workers: 1,
            queue_per_worker: 1,
            max_pending_cells: 1,
            group_pending_cells: 1,
            node_bytes: 128 * 1024 * 1024,
            group_bytes: 128 * 1024 * 1024,
        }
    )
    .is_err());
    assert!(
        tokio::time::timeout(Duration::from_millis(30), pool.close())
            .await
            .is_err()
    );
    release.send(())?;
    tokio::time::timeout(Duration::from_secs(5), pool.close()).await??;
    let usage = pool.usage()?;
    assert_eq!(usage.running_workers, 0);
    assert_eq!(usage.active_cells, 0);
    assert_eq!(usage.retained_bytes, 0);
    let after = fixture.runtimes[0]
        .execution_authority(route.history, a)
        .await?;
    assert_eq!(after.cell().version, before);
    drop(after);
    drop(pool);
    let replacement = CellGroupSqlPool::start(
        &fixture.cluster.voters[0].log,
        CellSqlLimits {
            workers: 1,
            queue_per_worker: 1,
            max_pending_cells: 1,
            group_pending_cells: 1,
            node_bytes: 128 * 1024 * 1024,
            group_bytes: 128 * 1024 * 1024,
        },
    )?;
    let cb = fixture.runtimes[0]
        .execution_authority(route.history, b)
        .await?;
    let options = fixture.cell_options(&cb.cell().version.history).await?;
    let receipt = replacement
        .stage(options, cb, cell_attempt(97)?, false, |_| {
            panic!("restart receipt replayed callback")
        })
        .await?;
    assert!(matches!(receipt, CellSqlOutcome::Receipt(_)));
    replacement.close().await?;
    drop(replacement);
    assert!(
        CellGroupSqlPool::start(
            &fixture.cluster.voters[0].log,
            CellSqlLimits {
                workers: 1,
                queue_per_worker: 1,
                max_pending_cells: 1,
                group_pending_cells: 1,
                node_bytes: 128 * 1024 * 1024,
                group_bytes: 128 * 1024 * 1024,
            }
        )
        .is_err(),
        "retained response released its node admission owner"
    );
    drop(receipt);
    let replacement = CellGroupSqlPool::start(
        &fixture.cluster.voters[0].log,
        CellSqlLimits {
            workers: 1,
            queue_per_worker: 1,
            max_pending_cells: 1,
            group_pending_cells: 1,
            node_bytes: 128 * 1024 * 1024,
            group_bytes: 128 * 1024 * 1024,
        },
    )?;
    replacement.close().await?;
    drop(replacement);
    fixture.close().await
}
