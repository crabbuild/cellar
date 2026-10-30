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

fn execution_limits(options: &mut CellSqlOptions) -> &mut ExecutionLimits {
    match options {
        CellSqlOptions::Materialized(o) => &mut o.limits,
        CellSqlOptions::Extents(o) => &mut o.limits,
    }
}

async fn apply_through(fixture: &mut RuntimeCluster, index: LogIndex) -> Result<()> {
    let group = fixture.cluster.birth.route().history;
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            fixture.pump(&[], &mut BTreeMap::new()).await?;
            if fixture.applied[&group]
                .iter()
                .all(|applied| *applied >= index)
            {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await?
}

async fn commit_on(
    fixture: &mut RuntimeCluster,
    pool: &CellGroupSqlPool,
    node: usize,
    cell: &CellHistory,
    prepared: CellSqlPrepared,
) -> Result<CellSqlReceipt> {
    let group = fixture.cluster.birth.route().history;
    pool.mark_submitted(cell.clone(), prepared.proposal())
        .await?;
    let accepted = fixture.runtimes[node]
        .propose(group, prepared.bytes().to_vec())
        .await?;
    apply_through(fixture, accepted.position.index).await?;
    let authority = fixture.runtimes[node]
        .execution_authority(group, cell.clone())
        .await?;
    Ok(pool.finish(authority).await?)
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
        let mut options = fixture.cell_options(cell).await?;
        execution_limits(&mut options).max_client_identities = 1;
        let CellSqlOutcome::Prepared(command) = pool
            .stage(options, authority, cell_attempt(tag)?, true, |tx| {
                tx.execute("UPDATE value SET n=n+1", [])?;
                Ok(b"reconciled".to_vec())
            })
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
    apply_through(&mut fixture, accepted.position.index).await?;
    let authority = fixture.runtimes[1]
        .execution_authority(route.history, b.clone())
        .await?;
    let later = successor.finish(authority).await?;
    assert_eq!(later.receipt().receipt.result(), b"later-b");
    drop(later);
    drop(next);
    // Fill A's client identity capacity with a different committed client.
    // Its changed image forces receipt lookup, not the unchanged-base shortcut.
    let authority = fixture.runtimes[1]
        .execution_authority(route.history, a.clone())
        .await?;
    let mut attempt = cell_attempt(116)?;
    attempt.request.id = CommandId::new(ClientId::new([99; 16])?, 1)?;
    let CellSqlOutcome::Prepared(next) = successor
        .stage(
            fixture.cell_options_at(&a, 1).await?,
            authority,
            attempt,
            true,
            |tx| {
                tx.execute("UPDATE value SET n=n+5", [])?;
                Ok(b"other-client".to_vec())
            },
        )
        .await?
    else {
        anyhow::bail!("other client command")
    };
    drop(commit_on(&mut fixture, &successor, 1, &a, next).await?);
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
    // Read-only absence did not relax fresh command admission at capacity.
    let authority = fixture.runtimes[0]
        .execution_authority(route.history, a.clone())
        .await?;
    let mut full = fixture.cell_options(&a).await?;
    execution_limits(&mut full).max_client_identities = 1;
    assert!(matches!(
        pool.stage(full, authority, cell_attempt(117)?, true, |_| panic!(
            "new client at capacity"
        ))
        .await,
        Err(ExecutionError::Refused(CommandFailure::Overloaded))
    ));
    assert_eq!(pool.usage()?.active_cells, 0);
    // With sufficient capacity, reopen from the committed image, not the old
    // private write. The other client's committed value must remain visible.
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
                assert_eq!(value, 5, "abandoned private writes must not be reused");
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

pub(super) async fn refusals(rooted: bool) -> Result<()> {
    let (mut fixture, route, a, b) = enrolled_sql_cells(rooted).await?;
    let limits = CellSqlLimits {
        workers: 2,
        queue_per_worker: 2,
        max_pending_cells: 3,
        group_pending_cells: 3,
        node_bytes: 512 * 1024 * 1024,
        group_bytes: 512 * 1024 * 1024,
    };
    let pool = CellGroupSqlPool::start(&fixture.cluster.voters[0].log, limits)?;
    let mut pending = Vec::new();
    for (cell, tag) in [(&a, 121), (&b, 122)] {
        let authority = fixture.runtimes[0]
            .execution_authority(route.history, cell.clone())
            .await?;
        let CellSqlOutcome::Prepared(prepared) = pool
            .stage(
                fixture.cell_options(cell).await?,
                authority,
                cell_attempt(tag)?,
                true,
                |tx| {
                    tx.execute("UPDATE value SET n=n+1", [])?;
                    Ok(b"original".to_vec())
                },
            )
            .await?
        else {
            anyhow::bail!("original command")
        };
        pool.mark_submitted(cell.clone(), prepared.proposal())
            .await?;
        pending.push(prepared);
    }
    // B commits but its response is lost; A's original proposal never commits.
    let accepted = fixture.runtimes[0]
        .propose(route.history, pending[1].bytes().to_vec())
        .await?;
    apply_through(&mut fixture, accepted.position.index).await?;
    transfer(&mut fixture, 0, 1).await?;
    let successor = CellGroupSqlPool::start(&fixture.cluster.voters[1].log, limits)?;
    let authority = fixture.runtimes[1]
        .execution_authority(route.history, a.clone())
        .await?;
    let mut conflict = cell_attempt(123)?;
    conflict.request.request_digest = [88; 32];
    let CellSqlOutcome::Prepared(prepared) = successor
        .stage(
            fixture.cell_options_at(&a, 1).await?,
            authority,
            conflict,
            true,
            |tx| {
                tx.execute("UPDATE value SET n=n+10", [])?;
                Ok(b"different-request".to_vec())
            },
        )
        .await?
    else {
        anyhow::bail!("conflicting identity winner")
    };
    drop(commit_on(&mut fixture, &successor, 1, &a, prepared).await?);
    // Preserve the latest receipt while replicated maintenance expires B's
    // first receipt. At command 3 the two-row ceiling triggers bounded pruning.
    for sequence in 2..=3 {
        let authority = fixture.runtimes[1]
            .execution_authority(route.history, b.clone())
            .await?;
        let mut options = fixture.cell_options_at(&b, 1).await?;
        execution_limits(&mut options).max_retained_receipts = 2;
        let mut attempt = cell_attempt(123 + sequence as u8)?;
        attempt.request.id = CommandId::new(ClientId::new([81; 16])?, sequence)?;
        if sequence == 3 {
            attempt.inputs.now_unix_millis = 2000;
            attempt.request.retry_until_ms = 3000;
        }
        let CellSqlOutcome::Prepared(prepared) = successor
            .stage(options, authority, attempt, true, |tx| {
                tx.execute("UPDATE value SET n=n+1", [])?;
                Ok(b"later".to_vec())
            })
            .await?
        else {
            anyhow::bail!("maintenance command")
        };
        drop(commit_on(&mut fixture, &successor, 1, &b, prepared).await?);
    }
    successor.close().await?;
    drop(successor);
    let proof = fixture.runtimes[0]
        .reconciliation_authority(route.history, a.clone())
        .await?;
    assert!(matches!(
        pool.reconcile(pending[0].proposal(), proof).await?,
        CellSqlReconciliation::Refused(CommandFailure::CommandConflict)
    ));
    let proof = fixture.runtimes[0]
        .reconciliation_authority(route.history, b.clone())
        .await?;
    assert!(matches!(
        pool.reconcile(pending[1].proposal(), proof).await?,
        CellSqlReconciliation::Refused(CommandFailure::CommandExpired)
    ));
    assert_eq!(pool.usage()?.active_cells, 0);
    assert!(pool.usage()?.retained_bytes > 0);
    drop(pending);
    assert_eq!(pool.usage()?.retained_bytes, 0);
    transfer(&mut fixture, 1, 0).await?;
    for cell in [&a, &b] {
        let authority = fixture.runtimes[0]
            .execution_authority(route.history, cell.clone())
            .await?;
        let error = pool
            .stage(
                fixture.cell_options(cell).await?,
                authority,
                cell_attempt(128)?,
                true,
                |_| panic!("refused receipt cannot replay the callback"),
            )
            .await;
        if cell == &a {
            assert!(matches!(
                error,
                Err(ExecutionError::Refused(CommandFailure::CommandConflict))
            ));
        } else {
            assert!(matches!(
                error,
                Err(ExecutionError::Refused(CommandFailure::CommandExpired))
            ));
        }
        assert_eq!(pool.usage()?.active_cells, 0);
        assert_eq!(pool.usage()?.retained_bytes, 0);
    }
    let authority = fixture.runtimes[0]
        .execution_authority(route.history, b.clone())
        .await?;
    let mut next = cell_attempt(129)?;
    next.request.id = CommandId::new(ClientId::new([81; 16])?, 4)?;
    next.inputs.now_unix_millis = 2000;
    next.request.retry_until_ms = 3000;
    let CellSqlOutcome::Prepared(prepared) = pool
        .stage(
            fixture.cell_options(&b).await?,
            authority,
            next,
            true,
            |tx| {
                let value: i64 = tx.query_row("SELECT n FROM value", [], |row| row.get(0))?;
                assert_eq!(value, 3);
                tx.execute("UPDATE value SET n=n+1", [])?;
                Ok(b"after-expiry".to_vec())
            },
        )
        .await?
    else {
        anyhow::bail!("next command after refused retry")
    };
    let receipt = commit_on(&mut fixture, &pool, 0, &b, prepared).await?;
    assert_eq!(receipt.receipt().version.sequence.get(), 5);
    assert_eq!(receipt.receipt().receipt.result(), b"after-expiry");
    drop(receipt);
    pool.close().await?;
    drop(pool);
    fixture.close().await
}
