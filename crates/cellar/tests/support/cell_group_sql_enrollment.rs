// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::reconciliation::{apply_through, commit_on, transfer};
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

fn attempt(tag: u8) -> Result<CellEnrollmentAttempt> {
    Ok(CellEnrollmentAttempt {
        creation: ProposalId::new([tag; 16])?,
        preview: ImageId::new([tag; 16])?,
        inputs: cellar::storage::CommandInputs {
            now_unix_millis: 10,
            random: [17; 32],
        },
    })
}

fn limits(workers: usize) -> CellSqlLimits {
    CellSqlLimits {
        workers,
        queue_per_worker: 4,
        max_pending_cells: 4,
        group_pending_cells: 4,
        node_bytes: 512 * 1024 * 1024,
        group_bytes: 512 * 1024 * 1024,
    }
}

async fn cluster(rooted: bool) -> Result<(RuntimeCluster, GroupRoute)> {
    let mut fixture = RuntimeCluster::new().await?;
    if rooted {
        for (n, voter) in fixture.cluster.voters.iter_mut().enumerate() {
            voter.extents = Some(NodeExtentStore::open(
                fixture.cluster._root.path().join(format!("enrollment-{n}")),
                Arc::new(DirectFileSystem),
                Default::default(),
            )?);
        }
    }
    let route = fixture.cluster.birth.route().clone();
    fixture.runtimes[0].campaign(route.history).await?;
    fixture.drive(None).await?;
    Ok((fixture, route))
}

async fn prepare_initial(
    fixture: &RuntimeCluster,
    pool: &CellGroupSqlPool,
    node: usize,
    cell: &CellHistory,
    tag: u8,
    value: i64,
) -> Result<CellSqlPrepared> {
    let authority = fixture.runtimes[node]
        .enrollment_authority(fixture.cluster.birth.route().history, cell.clone())
        .await?;
    let outcome = pool
        .initialize(
            fixture.cell_options_at(cell, node).await?,
            authority,
            attempt(tag)?,
            move |tx| {
                tx.execute_batch("CREATE TABLE value(n);")?;
                tx.execute("INSERT INTO value VALUES (?1)", [value])?;
                Ok(())
            },
        )
        .await?;
    let CellSqlOutcome::Prepared(prepared) = outcome else {
        anyhow::bail!("prepared enrollment")
    };
    Ok(prepared)
}

async fn submit(
    fixture: &mut RuntimeCluster,
    pool: &CellGroupSqlPool,
    node: usize,
    cell: &CellHistory,
    prepared: &CellSqlPrepared,
) -> Result<()> {
    let group = fixture.cluster.birth.route().history;
    pool.mark_submitted(cell.clone(), prepared.proposal())
        .await?;
    let accepted = fixture.runtimes[node]
        .propose_at_term(group, prepared.term(), prepared.bytes().to_vec())
        .await?;
    apply_through(fixture, accepted.position.index).await
}

async fn resolved(
    fixture: &RuntimeCluster,
    pool: &CellGroupSqlPool,
    node: usize,
    cell: &CellHistory,
    creation: ProposalId,
) -> Result<CellSqlEnrollment> {
    let state = fixture.runtimes[node]
        .enrollment_state(fixture.cluster.birth.route().history, cell.clone())
        .await?;
    let CellSqlEnrollmentResolution::Committed(record) =
        pool.resolve_enrollment(creation, state).await?
    else {
        anyhow::bail!("committed enrollment")
    };
    Ok(*record)
}

pub(super) async fn concurrent(rooted: bool) -> Result<()> {
    let (mut fixture, route) = cluster(rooted).await?;
    let a = history(&route, "sql-a")?;
    let b = history(&route, "sql-b")?;
    let c = history(&route, "sql-c")?;
    let key = |cell: &CellHistory| {
        u64::from_le_bytes(cell_image_history_digest(cell)[..8].try_into().unwrap())
    };
    let workers = (2..=8)
        .find(|n| key(&a) % n != key(&b) % n)
        .context("independent workers")? as usize;
    let pool = CellGroupSqlPool::start(&fixture.cluster.voters[0].log, limits(workers))?;
    assert!(fixture.runtimes[1]
        .enrollment_authority(route.history, a.clone())
        .await
        .is_err());
    let callbacks = Arc::new(AtomicUsize::new(0));
    let count = callbacks.clone();
    let (entered, started) = tokio::sync::oneshot::channel();
    let (release, wait) = std::sync::mpsc::sync_channel(1);
    let authority = fixture.runtimes[0]
        .enrollment_authority(route.history, a.clone())
        .await?;
    let options = fixture.cell_options(&a).await?;
    let caller = pool.clone();
    let waiter = fixture.cluster.domain.spawn(async move {
        caller
            .initialize(options, authority, attempt(151)?, move |tx| {
                count.fetch_add(1, Ordering::SeqCst);
                tx.execute_batch("CREATE TABLE value(n); INSERT INTO value VALUES (1);")?;
                let _ = entered.send(());
                wait.recv_timeout(Duration::from_secs(10))?;
                Ok(())
            })
            .await
            .map_err(anyhow::Error::new)
    })?;
    tokio::time::timeout(Duration::from_secs(5), started).await??;
    waiter.abort();
    assert!(waiter.await.is_err());
    let mut other = a.clone();
    other.incarnation = IncarnationId::new([152; 16])?;
    for cell in [&a, &other] {
        let authority = fixture.runtimes[0]
            .enrollment_authority(route.history, cell.clone())
            .await?;
        assert!(matches!(
            pool.initialize(
                fixture.cell_options(cell).await?,
                authority,
                attempt(152)?,
                |_| panic!("concurrent creation callback")
            )
            .await,
            Err(ExecutionError::Busy)
        ));
    }
    let pb = prepare_initial(&fixture, &pool, 0, &b, 153, 7).await?;
    submit(&mut fixture, &pool, 0, &b, &pb).await?;
    let rb = resolved(&fixture, &pool, 0, &b, pb.proposal()).await?;
    assert_eq!(rb.record().cell(), &b);
    drop(rb);
    drop(pb);
    assert_eq!(pool.usage()?.active_cells, 1);
    release.send(())?;
    let pa = tokio::time::timeout(
        Duration::from_secs(5),
        pool.resume(a.clone(), ProposalId::new([151; 16])?),
    )
    .await??;
    let state = fixture.runtimes[0]
        .enrollment_state(route.history, a.clone())
        .await?;
    assert!(matches!(
        pool.resolve_enrollment(pa.proposal(), state).await,
        Err(ExecutionError::Uncertain)
    ));
    submit(&mut fixture, &pool, 0, &a, &pa).await?;
    let ra = resolved(&fixture, &pool, 0, &a, pa.proposal()).await?;
    let original = ra.record().clone();
    assert_eq!(callbacks.load(Ordering::SeqCst), 1);
    drop(ra);
    drop(pa);
    let pc = prepare_initial(&fixture, &pool, 0, &c, 154, 9).await?;
    submit(&mut fixture, &pool, 0, &c, &pc).await?;
    drop(resolved(&fixture, &pool, 0, &c, pc.proposal()).await?);
    drop(pc);
    for voter in &fixture.cluster.voters {
        assert_eq!(
            voter.log.cell_groups()?.len(),
            1,
            "enrollment must not create a physical Raft group"
        );
        let marker = voter
            .log
            .cell_group(&route.history)?
            .group_manifest()?
            .context("root")?;
        for cell in [&a, &b, &c] {
            let leaf = voter
                .tree
                .get(&voter.pages, marker.root(), &cell.cell)?
                .context("enrolled cell")?;
            assert_eq!(leaf.version.sequence.get(), 1);
        }
    }
    // A real mutation must not erase or overwrite the immutable creation record.
    let authority = fixture.runtimes[0]
        .execution_authority(route.history, a.clone())
        .await?;
    let CellSqlOutcome::Prepared(command) = pool
        .stage(
            fixture.cell_options(&a).await?,
            authority,
            cell_attempt(155)?,
            true,
            |tx| {
                let value: i64 = tx.query_row("SELECT n FROM value", [], |row| row.get(0))?;
                assert_eq!(value, 1);
                tx.execute("UPDATE value SET n=n+10", [])?;
                Ok(b"after-init".to_vec())
            },
        )
        .await?
    else {
        anyhow::bail!("mutation")
    };
    let receipt = commit_on(&mut fixture, &pool, 0, &a, command).await?;
    let version = receipt.receipt().version.clone();
    drop(receipt);
    let authority = fixture.runtimes[0]
        .enrollment_authority(route.history, a.clone())
        .await?;
    let CellSqlOutcome::Enrollment(record) = pool
        .initialize(
            fixture.cell_options(&a).await?,
            authority,
            attempt(151)?,
            |_| panic!("committed creation must not initialize again"),
        )
        .await?
    else {
        anyhow::bail!("creation retry")
    };
    assert_eq!(record.record(), &original);
    drop(record);
    assert_eq!(
        fixture.runtimes[0]
            .execution_authority(route.history, a.clone())
            .await?
            .cell()
            .version,
        version
    );
    for (cell, tag) in [(&a, 156), (&other, 151)] {
        let authority = fixture.runtimes[0]
            .enrollment_authority(route.history, cell.clone())
            .await?;
        assert!(matches!(
            pool.initialize(
                fixture.cell_options(cell).await?,
                authority,
                attempt(tag)?,
                |_| panic!("conflicting creation")
            )
            .await,
            Err(ExecutionError::Refused(CommandFailure::CommandConflict))
        ));
    }
    assert_eq!(pool.usage()?.active_cells, 0);
    assert_eq!(pool.usage()?.retained_bytes, 0);
    // A cancelled initializer and cancelled close waiter cannot release the
    // physical callback or its logical-name reservation before the worker exits.
    let d = history(&route, "cancel-init")?;
    let authority = fixture.runtimes[0]
        .enrollment_authority(route.history, d.clone())
        .await?;
    let options = fixture.cell_options(&d).await?;
    let (entered, started) = tokio::sync::oneshot::channel();
    let (release, wait) = std::sync::mpsc::sync_channel(1);
    let caller = pool.clone();
    let waiter = fixture.cluster.domain.spawn(async move {
        caller
            .initialize(options, authority, attempt(157)?, move |tx| {
                tx.execute_batch("CREATE TABLE value(n); INSERT INTO value VALUES (99);")?;
                let _ = entered.send(());
                wait.recv_timeout(Duration::from_secs(10))?;
                Ok(())
            })
            .await
            .map_err(anyhow::Error::new)
    })?;
    tokio::time::timeout(Duration::from_secs(5), started).await??;
    waiter.abort();
    assert!(waiter.await.is_err());
    let caller = pool.clone();
    let mut closing = fixture
        .cluster
        .domain
        .spawn(async move { caller.close().await })?;
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut closing)
            .await
            .is_err()
    );
    closing.abort();
    assert!(closing.await.is_err());
    assert_eq!(pool.usage()?.active_cells, 1);
    assert!(pool.usage()?.running_workers > 0);
    assert!(
        tokio::time::timeout(Duration::from_millis(30), pool.close())
            .await
            .is_err()
    );
    release.send(())?;
    pool.close().await?;
    assert_eq!(pool.usage()?.active_cells, 0);
    assert_eq!(pool.usage()?.retained_bytes, 0);
    assert_eq!(pool.usage()?.running_workers, 0);
    if let Some(store) = &fixture.cluster.voters[0].extents {
        assert_eq!(store.usage()?.workspaces, 0);
    }
    assert!(fixture.runtimes[0]
        .enrollment_state(route.history, d)
        .await?
        .record()
        .is_none());
    drop(pool);
    fixture.close().await
}

pub(super) async fn recovery(rooted: bool) -> Result<()> {
    let (mut fixture, route) = cluster(rooted).await?;
    let pool = CellGroupSqlPool::start(&fixture.cluster.voters[0].log, limits(2))?;
    // The existing election fixture holds the sql-a barrier on the old leader.
    let a = history(&route, "sql-a")?;
    let initial = prepare_initial(&fixture, &pool, 0, &a, 161, 1).await?;
    submit(&mut fixture, &pool, 0, &a, &initial).await?;
    drop(resolved(&fixture, &pool, 0, &a, initial.proposal()).await?);
    drop(initial);
    let d = history(&route, "absent-init")?;
    let e = history(&route, "conflict-init")?;
    let f = history(&route, "committed-init")?;
    let pd = prepare_initial(&fixture, &pool, 0, &d, 162, 2).await?;
    let pe = prepare_initial(&fixture, &pool, 0, &e, 163, 3).await?;
    let pf = prepare_initial(&fixture, &pool, 0, &f, 164, 4).await?;
    pool.mark_submitted(d.clone(), pd.proposal()).await?;
    pool.mark_submitted(e.clone(), pe.proposal()).await?;
    submit(&mut fixture, &pool, 0, &f, &pf).await?;
    transfer(&mut fixture, 0, 1).await?;
    let successor = CellGroupSqlPool::start(&fixture.cluster.voters[1].log, limits(1))?;
    let mut new_e = e.clone();
    new_e.incarnation = IncarnationId::new([165; 16])?;
    let next = prepare_initial(&fixture, &successor, 1, &new_e, 165, 30).await?;
    submit(&mut fixture, &successor, 1, &new_e, &next).await?;
    drop(resolved(&fixture, &successor, 1, &new_e, next.proposal()).await?);
    drop(next);
    let foreign = fixture.runtimes[1]
        .enrollment_state(route.history, d.clone())
        .await?;
    assert!(matches!(
        pool.resolve_enrollment(pd.proposal(), foreign).await,
        Err(ExecutionError::Invalid(_))
    ));
    let state = fixture.runtimes[0]
        .enrollment_state(route.history, d.clone())
        .await?;
    assert!(matches!(
        pool.resolve_enrollment(pd.proposal(), state).await?,
        CellSqlEnrollmentResolution::NotCommitted
    ));
    let state = fixture.runtimes[0]
        .enrollment_state(route.history, e.clone())
        .await?;
    assert!(matches!(
        pool.resolve_enrollment(pe.proposal(), state).await?,
        CellSqlEnrollmentResolution::Conflict
    ));
    let authority = fixture.runtimes[1]
        .execution_authority(route.history, f.clone())
        .await?;
    let CellSqlOutcome::Prepared(command) = successor
        .stage(
            fixture.cell_options_at(&f, 1).await?,
            authority,
            cell_attempt(166)?,
            true,
            |tx| {
                tx.execute("UPDATE value SET n=n+1", [])?;
                Ok(b"later".to_vec())
            },
        )
        .await?
    else {
        anyhow::bail!("new leader mutation")
    };
    drop(commit_on(&mut fixture, &successor, 1, &f, command).await?);
    let record = resolved(&fixture, &pool, 0, &f, pf.proposal()).await?;
    assert_eq!(record.record().creation(), pf.proposal());
    assert_eq!(pool.usage()?.active_cells, 0);
    assert!(pool.usage()?.retained_bytes > 0);
    // Even after this node regains leadership, a retained old prepared response
    // cannot be proposed under its new term. Neither sample-before-queue nor
    // merely checking that the receiver is a leader would reject this race.
    transfer(&mut fixture, 1, 0).await?;
    assert!(matches!(
        fixture.runtimes[0]
            .propose_at_term(route.history, pe.term(), pe.bytes().to_vec())
            .await,
        Err(RuntimeError::StaleCompletion)
    ));
    assert_eq!(
        fixture.runtimes[0]
            .enrollment_state(route.history, new_e.clone())
            .await?
            .record()
            .context("winner")?
            .cell(),
        &new_e
    );
    drop(record);
    drop(pd);
    drop(pe);
    drop(pf);
    assert_eq!(pool.usage()?.retained_bytes, 0);
    successor.close().await?;
    drop(successor);
    pool.close().await?;
    drop(pool);
    fixture.close().await
}
