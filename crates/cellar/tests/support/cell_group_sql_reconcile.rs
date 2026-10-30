// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

async fn transfer(fixture: &mut RuntimeCluster, from: usize, to: usize) -> Result<()> {
    let history = fixture.cluster.birth.route().history;
    let term = fixture.runtimes[from].status(history).await?.term;
    let mut message = Message::default();
    // The current leader's transfer directive triggers a real higher-term
    // election. MsgTransferLeader itself is a local administration request and
    // is deliberately rejected by the authenticated peer-message boundary.
    message.set_msg_type(MessageType::MsgTimeoutNow);
    message.from = from as u64 + 1;
    message.to = to as u64 + 1;
    message.term = term.get();
    fixture.runtimes[to].step(history, message).await?;
    if from == 0 {
        let mut held = BTreeMap::new();
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                fixture.pump(&[history], &mut held).await?;
                if fixture.runtimes[to].status(history).await?.ready && held.contains_key(&history)
                {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::task::yield_now().await;
            }
        })
        .await??;
        let a = super::history(fixture.cluster.birth.route(), "sql-a")?;
        assert!(
            matches!(
                fixture.runtimes[0]
                    .reconciliation_authority(history, a)
                    .await,
                Err(RuntimeError::NotReady)
            ),
            "a persisted newer term without its applied barrier cannot retire a producer"
        );
        let work = held.remove(&history).context("held new-term barrier")?;
        let next = prepare(&mut fixture.cluster.voters[0], &work)?;
        fixture.runtimes[0]
            .complete_apply(history, work, next)
            .await?;
    }
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            fixture.pump(&[], &mut BTreeMap::new()).await?;
            let status = fixture.runtimes[to].status(history).await?;
            if status.ready
                && status.term > term
                && fixture.applied[&history]
                    .iter()
                    .all(|index| *index >= status.applied.index)
            {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await?
}

pub(super) async fn reconcile(rooted: bool) -> Result<()> {
    let (mut fixture, route, a, b) = enrolled_sql_cells(rooted).await?;
    let pool = CellGroupSqlPool::start(
        &fixture.cluster.voters[0].log,
        CellSqlLimits {
            workers: 2,
            queue_per_worker: 2,
            max_pending_cells: 3,
            group_pending_cells: 3,
            node_bytes: 512 * 1024 * 1024,
            group_bytes: 512 * 1024 * 1024,
        },
    )?;
    let mut prepared = Vec::new();
    for (cell, tag) in [(&a, 111), (&b, 112)] {
        let authority = fixture.runtimes[0]
            .execution_authority(route.history, cell.clone())
            .await?;
        let CellSqlOutcome::Prepared(command) = pool
            .stage(
                fixture.cell_options(cell).await?,
                authority,
                cell_attempt(tag)?,
                true,
                |tx| {
                    tx.execute("UPDATE value SET n=n+1", [])?;
                    Ok(b"reconciled".to_vec())
                },
            )
            .await?
        else {
            anyhow::bail!("prepared")
        };
        pool.mark_submitted(cell.clone(), command.proposal())
            .await?;
        prepared.push(command);
    }
    // A's submission response is unknown. The same-term applied prefix is not
    // enough to release it, even though its receipt is currently absent.
    let same_term = fixture.runtimes[0]
        .reconciliation_authority(route.history, a.clone())
        .await?;
    assert!(matches!(
        pool.reconcile(prepared[0].proposal(), same_term).await,
        Err(ExecutionError::Uncertain)
    ));
    assert_eq!(pool.usage()?.active_cells, 2);
    let stale_b = fixture.runtimes[0]
        .reconciliation_authority(route.history, b.clone())
        .await?;
    let submitted = fixture.runtimes[0]
        .propose(route.history, prepared[1].bytes().to_vec())
        .await?;
    fixture.drive(Some(submitted.position.index)).await?;
    assert!(
        stale_b.check().is_err(),
        "an old cell pin cannot prove absence after commit"
    );
    assert!(pool
        .reconcile(prepared[1].proposal(), stale_b)
        .await
        .is_err());

    // B committed; deliberately lose its exact-entry completion. Transfer real
    // leadership so node 0 must reconcile while it is a follower.
    transfer(&mut fixture, 0, 1).await?;
    assert!(fixture.runtimes[0]
        .execution_authority(route.history, a.clone())
        .await
        .is_err());
    // The new leader advances B again while the old node retains B's first
    // unresolved producer. Reconciliation must find the original receipt in a
    // later image, rather than demand that image equal the old private result.
    let successor = CellGroupSqlPool::start(
        &fixture.cluster.voters[1].log,
        CellSqlLimits {
            workers: 1,
            queue_per_worker: 2,
            max_pending_cells: 2,
            group_pending_cells: 2,
            node_bytes: 256 * 1024 * 1024,
            group_bytes: 256 * 1024 * 1024,
        },
    )?;
    let authority = fixture.runtimes[1]
        .execution_authority(route.history, b.clone())
        .await?;
    let mut attempt = cell_attempt(115)?;
    attempt.request.id = CommandId::new(ClientId::new([81; 16])?, 2)?;
    let CellSqlOutcome::Prepared(next) = successor
        .stage(
            fixture.cell_options_at(&b, 1).await?,
            authority,
            attempt,
            true,
            |tx| {
                tx.execute("UPDATE value SET n=n+10", [])?;
                Ok(b"later-b".to_vec())
            },
        )
        .await?
    else {
        anyhow::bail!("successor command")
    };
    successor.mark_submitted(b.clone(), next.proposal()).await?;
    let accepted = fixture.runtimes[1]
        .propose(route.history, next.bytes().to_vec())
        .await?;
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            fixture.pump(&[], &mut BTreeMap::new()).await?;
            if fixture.applied[&route.history]
                .iter()
                .all(|index| *index >= accepted.position.index)
            {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    let authority = fixture.runtimes[1]
        .execution_authority(route.history, b.clone())
        .await?;
    let later = successor.finish(authority).await?;
    assert_eq!(later.receipt().receipt.result(), b"later-b");
    drop(later);
    drop(next);
    successor.close().await?;
    drop(successor);
    let foreign = fixture.runtimes[1]
        .reconciliation_authority(route.history, a.clone())
        .await?;
    assert!(matches!(
        pool.reconcile(prepared[0].proposal(), foreign).await,
        Err(ExecutionError::Invalid(_))
    ));
    let wrong = fixture.runtimes[0]
        .reconciliation_authority(route.history, b.clone())
        .await?;
    assert!(matches!(
        pool.reconcile(prepared[0].proposal(), wrong).await,
        Err(ExecutionError::Invalid(_))
    ));
    assert_eq!(pool.usage()?.active_cells, 2);
    let proof = fixture.runtimes[0]
        .reconciliation_authority(route.history, a.clone())
        .await?;
    assert!(matches!(
        pool.reconcile(prepared[0].proposal(), proof).await?,
        CellSqlReconciliation::NotCommitted
    ));
    assert_eq!(pool.usage()?.active_cells, 1);
    let proof = fixture.runtimes[0]
        .reconciliation_authority(route.history, b.clone())
        .await?;
    let CellSqlReconciliation::Committed(receipt) =
        pool.reconcile(prepared[1].proposal(), proof).await?
    else {
        anyhow::bail!("committed receipt")
    };
    assert_eq!(receipt.receipt().receipt.result(), b"reconciled");
    assert_eq!(receipt.receipt().version.history, b);
    assert_eq!(receipt.receipt().version.sequence.get(), 3);
    assert_eq!(receipt.receipt().position, accepted.position);
    assert_eq!(pool.usage()?.active_cells, 0);
    assert!(
        pool.usage()?.retained_bytes > 0,
        "responses retain admission"
    );
    // Losing a reconciliation response cannot be reported as definite failure.
    let proof = fixture.runtimes[0]
        .reconciliation_authority(route.history, b.clone())
        .await?;
    assert!(matches!(
        pool.reconcile(prepared[1].proposal(), proof).await,
        Err(ExecutionError::Uncertain)
    ));
    assert!(matches!(
        pool.resume(b.clone(), prepared[1].proposal()).await,
        Err(ExecutionError::Uncertain)
    ));
    drop(receipt);
    drop(prepared);
    assert_eq!(pool.usage()?.retained_bytes, 0);

    transfer(&mut fixture, 1, 0).await?;
    // B retries through the committed receipt without invoking the callback.
    let authority = fixture.runtimes[0]
        .execution_authority(route.history, b.clone())
        .await?;
    let CellSqlOutcome::Receipt(receipt) = pool
        .stage(
            fixture.cell_options(&b).await?,
            authority,
            cell_attempt(113)?,
            false,
            |_| panic!("duplicate callback"),
        )
        .await?
    else {
        anyhow::bail!("retry receipt")
    };
    assert_eq!(receipt.receipt().receipt.result(), b"reconciled");
    drop(receipt);
    // A can reopen from its committed base and execute the request exactly once.
    let authority = fixture.runtimes[0]
        .execution_authority(route.history, a.clone())
        .await?;
    let CellSqlOutcome::Prepared(command) = pool
        .stage(
            fixture.cell_options(&a).await?,
            authority,
            cell_attempt(114)?,
            true,
            |tx| {
                let value: i64 = tx.query_row("SELECT n FROM value", [], |row| row.get(0))?;
                assert_eq!(value, 0, "abandoned private writes must not be reused");
                tx.execute("UPDATE value SET n=n+1", [])?;
                Ok(b"retry-a".to_vec())
            },
        )
        .await?
    else {
        anyhow::bail!("retry A")
    };
    pool.mark_submitted(a.clone(), command.proposal()).await?;
    let submitted = fixture.runtimes[0]
        .propose(route.history, command.bytes().to_vec())
        .await?;
    fixture.drive(Some(submitted.position.index)).await?;
    // A positive same-term receipt resolves without requiring a term change.
    let proof = fixture.runtimes[0]
        .reconciliation_authority(route.history, a.clone())
        .await?;
    let CellSqlReconciliation::Committed(receipt) =
        pool.reconcile(command.proposal(), proof).await?
    else {
        anyhow::bail!("same-term committed receipt")
    };
    assert_eq!(receipt.receipt().receipt.result(), b"retry-a");
    drop(receipt);
    drop(command);
    assert_eq!(pool.usage()?.retained_bytes, 0);
    pool.close().await?;
    drop(pool);
    fixture.close().await
}
