// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Completed immutable replies retain bytes without occupying physical SQL work.
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

pub(crate) async fn run(rooted: bool, group_limited: bool) -> Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let class = Arc::new(RaftClass::new("Counter", 1, [51; 32])?.query(
        "Get",
        [53; 32],
        move |context, _| {
            counted.fetch_add(1, Ordering::AcqRel);
            let QueryVersion::Cell(observation) = context.version() else {
                anyhow::bail!("cell query received group observation");
            };
            let value: i64 = context.query_row("SELECT n FROM value", [], |row| row.get(0))?;
            let mut result = value.to_le_bytes().to_vec();
            result.extend_from_slice(&observation.version.sequence.get().to_le_bytes());
            result.extend_from_slice(&observation.witness.position.index.get().to_le_bytes());
            Ok(result)
        },
    )?);
    let (mut fixture, route, cell, _) = Box::pin(registered::setup(rooted, &class)).await?;
    // Keep these voter disks for independently audited failure evidence.
    fixture.cluster._root.disable_cleanup(true);
    let (old, applier) = fixture
        .appliers
        .remove(&(0, route.history))
        .context("SQL owner")?;
    let mut limits = old.usage()?.limits;
    limits.max_pending_cells = if group_limited { 2 } else { 1 };
    limits.group_pending_cells = 1;
    limits.node_bytes = if group_limited { 49_152 } else { 12_288 };
    limits.group_bytes = 12_288;
    old.close().await?;
    drop(old);
    let pool = CellGroupSqlPool::start(&fixture.cluster.voters[0].log, limits)?;
    fixture
        .appliers
        .insert((0, route.history), (pool.clone(), applier));
    let mut options = fixture.cell_options(&cell).await?;
    match &mut options {
        CellSqlOptions::Materialized(options) => options.limits.max_result_bytes = 1024,
        CellSqlOptions::Extents(options) => options.limits.max_result_bytes = 1024,
    }
    let contract = class.contract("Get").context("Get")?;
    let authority = reads::selected(&mut fixture, 0, &cell, CellReadConsistency::Stale).await?;
    let observation = authority.cell().observation();
    let first = pool
        .query_registered(
            options.clone(),
            authority,
            class.clone(),
            contract.clone(),
            vec![],
        )
        .await?;
    let bytes = first.result().to_vec();
    assert_eq!(bytes.len(), 24);
    assert_eq!(i64::from_le_bytes(bytes[..8].try_into()?), 0);
    reads::verify_observation(&first, route.schema_digest)?;
    assert_eq!(first.observation(), &observation);
    let replica = first.replica().clone();

    // The first reply remains undelivered. A second real callback must fit
    // the same original one-operation node or group bank.
    let authority = reads::selected(&mut fixture, 0, &cell, CellReadConsistency::Stale).await?;
    let second = pool
        .query_registered(
            options.clone(),
            authority,
            class.clone(),
            contract.clone(),
            vec![],
        )
        .await?;
    assert_eq!(calls.load(Ordering::Acquire), 2);
    assert_eq!(second.result(), bytes);
    assert_eq!(second.observation(), &observation);
    assert_eq!(second.replica(), &replica);
    reads::verify_observation(&second, route.schema_digest)?;
    assert_eq!(pool.usage()?.retained_operations, 0);
    assert_eq!(pool.usage()?.retained_bytes, 2 * (bytes.len() + 4096));

    // Neither retained reply loses its byte charge. The next full input/
    // result reservation cannot fit, and refusal never enters a callback.
    let authority = reads::selected(&mut fixture, 0, &cell, CellReadConsistency::Stale).await?;
    assert!(matches!(
        pool.query_registered(
            options.clone(),
            authority,
            class.clone(),
            contract.clone(),
            vec![]
        )
        .await,
        Err(ExecutionError::Refused(CommandFailure::Overloaded))
    ));
    assert_eq!(calls.load(Ordering::Acquire), 2);
    assert_eq!(pool.usage()?.retained_operations, 0);
    assert_eq!(pool.usage()?.retained_bytes, 2 * (bytes.len() + 4096));
    drop(first);
    let authority = reads::selected(&mut fixture, 0, &cell, CellReadConsistency::Stale).await?;
    let third = pool
        .query_registered(options, authority, class, contract, vec![])
        .await?;
    assert_eq!(calls.load(Ordering::Acquire), 3);
    assert_eq!(third.result(), bytes);
    assert_eq!(third.observation(), &observation);
    assert_eq!(third.replica(), &replica);
    reads::verify_observation(&third, route.schema_digest)?;

    // Closing physical workers does not allow a new pool to reuse the log
    // lease while immutable outputs still own their original reservations.
    pool.close().await?;
    let (owned, applier) = fixture
        .appliers
        .remove(&(0, route.history))
        .context("SQL owner")?;
    drop(owned);
    drop(pool);
    assert!(CellGroupSqlPool::start(&fixture.cluster.voters[0].log, limits).is_err());
    assert_eq!(second.result(), bytes);
    assert_eq!(third.result(), bytes);
    drop((second, third));
    let pool = CellGroupSqlPool::start(&fixture.cluster.voters[0].log, limits)?;
    assert_eq!(pool.usage()?.retained_operations, 0);
    assert_eq!(pool.usage()?.retained_bytes, 0);
    fixture.appliers.insert((0, route.history), (pool, applier));
    fixture.close().await?;
    Ok(())
}
