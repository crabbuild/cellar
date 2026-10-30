// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::reconciliation::apply_through;
use super::*;

async fn deadline(fixture: &RuntimeCluster, ms: u64) -> Result<u64> {
    fixture
        .cluster
        .domain
        .run(async { cellar::asyncrt::mono_ms() })
        .await?
        .checked_add(ms)
        .context("deadline")
}

fn pool(fixture: &RuntimeCluster, node: usize) -> Result<CellGroupSqlPool> {
    Ok(CellGroupSqlPool::start(
        &fixture.cluster.voters[node].log,
        CellSqlLimits {
            workers: 2,
            queue_per_worker: 4,
            max_pending_cells: 8,
            group_pending_cells: 8,
            node_bytes: 512 * 1024 * 1024,
            group_bytes: 512 * 1024 * 1024,
        },
    )?)
}

async fn selected(
    fixture: &mut RuntimeCluster,
    node: usize,
    cell: &CellHistory,
    consistency: CellReadConsistency,
) -> Result<CellReadAuthority> {
    let runtime = fixture.runtimes[node].clone();
    let read = runtime.read_authority(
        fixture.cluster.birth.route().history,
        cell.clone(),
        consistency,
        deadline(fixture, 5000).await?,
    );
    tokio::pin!(read);
    loop {
        tokio::select! {
            result = &mut read => return result.map_err(Into::into),
            result = async {
                fixture.pump(&[], &mut BTreeMap::new()).await?;
                tokio::task::yield_now().await;
                Ok::<_, anyhow::Error>(())
            } => result?,
        }
    }
}

async fn value(
    pool: &CellGroupSqlPool,
    options: CellSqlOptions,
    authority: CellReadAuthority,
) -> Result<CellSqlQueryResult> {
    Ok(pool
        .query(options, authority, |db, version| {
            let n: i64 = db.query_row("SELECT n FROM value", [], |row| row.get(0))?;
            assert!(db
                .connection()
                .execute("UPDATE value SET n=999", [])
                .is_err());
            assert!(version.sequence.get() > 0);
            Ok(n.to_le_bytes().to_vec())
        })
        .await?)
}

pub(crate) async fn coherent(rooted: bool) -> Result<()> {
    let (mut fixture, route, a, b) = enrolled_sql_cells(rooted).await?;
    let pool = pool(&fixture, 0)?;
    let oa = fixture.cell_options(&a).await?;
    let ob = fixture.cell_options(&b).await?;
    let original = selected(&mut fixture, 0, &a, CellReadConsistency::Stale).await?;
    let original_version = original.cell().version.clone();
    drop(value(&pool, oa.clone(), original).await?);

    // A ready leader without any new peer responses cannot reuse a cached cut.
    assert!(fixture.runtimes[0].status(route.history).await?.ready);
    assert!(matches!(
        fixture.runtimes[0]
            .read_authority(
                route.history,
                a.clone(),
                CellReadConsistency::Linearizable,
                deadline(&fixture, 50).await?
            )
            .await,
        Err(RuntimeError::DeadlineExceeded)
    ));
    fixture.drive(None).await?;
    assert!(fixture.runtimes[0]
        .read_index(route.history, b"cellar-cell-read-1\0forged".to_vec())
        .await
        .is_err());

    let authority = fixture.runtimes[0]
        .execution_authority(route.history, b.clone())
        .await?;
    let CellSqlOutcome::Prepared(prepared) = pool
        .stage(ob.clone(), authority, cell_attempt(201)?, true, |tx| {
            tx.execute("UPDATE value SET n=n+1", [])?;
            Ok(b"b".to_vec())
        })
        .await?
    else {
        anyhow::bail!("B preparation")
    };
    let private_b = selected(&mut fixture, 0, &b, CellReadConsistency::Stale).await?;
    assert_eq!(
        value(&pool, ob.clone(), private_b).await?.result(),
        0_i64.to_le_bytes()
    );
    let old_b = selected(&mut fixture, 0, &b, CellReadConsistency::Stale).await?;
    let old_b_version = old_b.cell().version.clone();
    let encoded = CellGroupEntry::decode(prepared.bytes(), &route, GroupEntryLimits::default())?;
    let GroupCommand::Mutation(mutation) = encoded.command else {
        anyhow::bail!("B mutation")
    };
    let expected_b = CellVersion {
        history: b.clone(),
        sequence: mutation.sequence,
        image: mutation.result.into(),
    };
    pool.mark_submitted(b.clone(), prepared.proposal()).await?;
    let accepted = accepted_sql(&fixture.runtimes[0], &prepared).await?;
    let mut held = BTreeMap::new();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            fixture.pump(&[route.history], &mut held).await?;
            if held.contains_key(&route.history) {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await??;
    // A has not changed, but a strong A read must still wait for B's earlier
    // committed entry to enter the contiguous group publication prefix.
    let runtime = fixture.runtimes[0].clone();
    let pending = runtime.read_authority(
        route.history,
        a.clone(),
        CellReadConsistency::Linearizable,
        deadline(&fixture, 5000).await?,
    );
    tokio::pin!(pending);
    let at_least = runtime.read_authority(
        route.history,
        b.clone(),
        CellReadConsistency::AtLeast(expected_b.clone()),
        deadline(&fixture, 5000).await?,
    );
    tokio::pin!(at_least);
    for _ in 0..30 {
        assert!(tokio::time::timeout(Duration::from_millis(2), &mut pending)
            .await
            .is_err());
        assert!(
            tokio::time::timeout(Duration::from_millis(2), &mut at_least)
                .await
                .is_err()
        );
        fixture.pump(&[route.history], &mut held).await?;
    }
    let work = held.remove(&route.history).context("held B apply")?;
    assert_eq!(work.entry.index, accepted.position.index.get());
    let next = prepare(&mut fixture.cluster.voters[0], &work)?;
    fixture.runtimes[0]
        .complete_apply(route.history, work, next)
        .await?;
    apply_through(&mut fixture, accepted.position.index).await?;
    let selected = tokio::time::timeout(Duration::from_secs(5), pending).await??;
    let at_least = tokio::time::timeout(Duration::from_secs(5), at_least).await??;
    assert_eq!(at_least.cell().version, expected_b);
    drop(value(&pool, ob.clone(), at_least).await?);
    let old_output = value(&pool, ob.clone(), old_b).await?;
    assert_eq!(old_output.result(), 0_i64.to_le_bytes());
    assert_eq!(old_output.version(), &old_b_version);
    drop(old_output);
    assert_eq!(selected.cell().version, original_version);
    assert!(selected.applied().index >= accepted.position.index);
    assert!(selected.barrier().context("fresh barrier")?.index >= accepted.position.index);
    assert_eq!(
        value(&pool, oa.clone(), selected).await?.result(),
        0_i64.to_le_bytes()
    );
    drop(
        pool.finish(
            fixture.runtimes[0]
                .execution_authority(route.history, b.clone())
                .await?,
        )
        .await?,
    );
    drop(prepared);

    let version_b = fixture.runtimes[0]
        .execution_authority(route.history, b.clone())
        .await?
        .cell()
        .version
        .clone();
    let ready_b = self::selected(
        &mut fixture,
        0,
        &b,
        CellReadConsistency::AtLeast(version_b.clone()),
    )
    .await?;
    let output = value(&pool, ob, ready_b).await?;
    assert_eq!(output.result(), 1_i64.to_le_bytes());
    assert_eq!(output.version(), &version_b);
    drop(output);
    assert!(fixture.runtimes[0]
        .read_authority(
            route.history,
            a.clone(),
            CellReadConsistency::AtLeast(version_b),
            deadline(&fixture, 1000).await?
        )
        .await
        .is_err());
    let mut future = original_version.clone();
    future.sequence = MutationSequence::new(2)?;
    assert!(matches!(
        fixture.runtimes[0]
            .read_authority(
                route.history,
                a.clone(),
                CellReadConsistency::AtLeast(future),
                deadline(&fixture, 30).await?
            )
            .await,
        Err(RuntimeError::DeadlineExceeded)
    ));
    let mut wrong = a.clone();
    wrong.incarnation = IncarnationId::new([202; 16])?;
    assert!(fixture.runtimes[0]
        .read_authority(
            route.history,
            wrong,
            CellReadConsistency::Stale,
            deadline(&fixture, 1000).await?
        )
        .await
        .is_err());

    let strong = self::selected(&mut fixture, 0, &a, CellReadConsistency::Linearizable).await?;
    super::reconciliation::transfer(&mut fixture, 0, 1).await?;
    assert!(pool
        .query(oa.clone(), strong, |_, _| panic!("old leader query"))
        .await
        .is_err());
    assert!(fixture.runtimes[0]
        .read_authority(
            route.history,
            a.clone(),
            CellReadConsistency::Linearizable,
            deadline(&fixture, 1000).await?
        )
        .await
        .is_err());
    let stale = self::selected(
        &mut fixture,
        0,
        &a,
        CellReadConsistency::AtLeast(original_version),
    )
    .await?;
    drop(value(&pool, oa, stale).await?);
    assert_eq!(pool.usage()?.active_cells, 0);
    assert_eq!(pool.usage()?.retained_bytes, 0);
    pool.close().await?;
    fixture.close().await
}

pub(crate) async fn lifecycle(rooted: bool) -> Result<()> {
    let (mut fixture, route, a, _) = enrolled_sql_cells(rooted).await?;
    let pool = pool(&fixture, 0)?;
    let oa = fixture.cell_options(&a).await?;
    let mut pins = Vec::new();
    for _ in 0..32 {
        pins.push(selected(&mut fixture, 0, &a, CellReadConsistency::Stale).await?);
    }
    assert!(matches!(
        fixture.runtimes[0]
            .read_authority(
                route.history,
                a.clone(),
                CellReadConsistency::Stale,
                deadline(&fixture, 1000).await?
            )
            .await,
        Err(RuntimeError::Overloaded)
    ));
    drop(pins);
    let authority = selected(&mut fixture, 0, &a, CellReadConsistency::Stale).await?;
    let foreign = self::pool(&fixture, 1)?;
    assert!(foreign
        .query(oa.clone(), authority, |_, _| panic!("foreign query"))
        .await
        .is_err());
    foreign.close().await?;

    let authority = fixture.runtimes[0]
        .read_authority(
            route.history,
            a.clone(),
            CellReadConsistency::Stale,
            deadline(&fixture, 100).await?,
        )
        .await?;
    assert!(pool.query(oa.clone(), authority, |db, _| {
        let n: i64 = db.query_row("WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n WHERE x<1000000000) SELECT sum(x) FROM n", [], |row| row.get(0))?;
        Ok(n.to_le_bytes().to_vec())
    }).await.is_err());
    let authority = selected(&mut fixture, 0, &a, CellReadConsistency::Stale).await?;
    drop(value(&pool, oa.clone(), authority).await?);
    let authority = selected(&mut fixture, 0, &a, CellReadConsistency::Stale).await?;
    assert!(pool
        .query(oa.clone(), authority, |_, _| panic!(
            "contained query panic"
        ))
        .await
        .is_err());
    let authority = selected(&mut fixture, 0, &a, CellReadConsistency::Stale).await?;
    drop(value(&pool, oa.clone(), authority).await?);

    let authority = selected(&mut fixture, 0, &a, CellReadConsistency::Stale).await?;
    let (entered, started) = tokio::sync::oneshot::channel();
    let (release, wait) = std::sync::mpsc::sync_channel(1);
    let caller = pool.clone();
    let waiter = fixture.cluster.domain.spawn(async move {
        caller
            .query(oa, authority, move |db, _| {
                let _ = entered.send(());
                wait.recv_timeout(Duration::from_secs(10))?;
                let n: i64 = db.query_row("SELECT n FROM value", [], |row| row.get(0))?;
                Ok(n.to_le_bytes().to_vec())
            })
            .await
    })?;
    tokio::time::timeout(Duration::from_secs(5), started).await??;
    waiter.abort();
    assert!(waiter.await.is_err());
    assert_eq!(pool.usage()?.active_cells, 0);
    assert_eq!(pool.usage()?.retained_operations, 1);
    assert!(
        tokio::time::timeout(Duration::from_millis(30), pool.close())
            .await
            .is_err()
    );
    release.send(())?;
    tokio::time::timeout(Duration::from_secs(5), pool.close()).await??;
    assert_eq!(pool.usage()?.retained_operations, 0);
    assert_eq!(pool.usage()?.retained_bytes, 0);
    assert_eq!(pool.usage()?.running_workers, 0);
    fixture.close().await
}
