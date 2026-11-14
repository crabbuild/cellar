// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Refusal observations distinguish admission from entered physical execution.

use super::*;
use cellar::sdk::consensus::RaftPerformanceSnapshot;
use cellar_logic::consensus::CommandFailure;

fn count(snapshot: &RaftPerformanceSnapshot, name: &str) -> Result<u64> {
    Ok(snapshot
        .counters
        .iter()
        .find(|counter| counter.name == name)
        .with_context(|| format!("missing refusal observation: {name}"))?
        .value)
}

pub(super) async fn node_and_callback(rooted: bool) -> Result<()> {
    let (fixture, route, a, b) = enrolled_sql_cells(rooted).await?;
    let log = fixture.cluster.voters[0].log.clone();
    let pool = CellGroupSqlPool::start(
        &log,
        CellSqlLimits {
            workers: 1,
            queue_per_worker: 1,
            max_pending_cells: 1,
            group_pending_cells: 1,
            node_bytes: 128 * 1024 * 1024,
            group_bytes: 128 * 1024 * 1024,
        },
    )?;
    let before = log.performance_snapshot();
    let options = fixture.cell_options(&a).await?;
    let authority = fixture.runtimes[0]
        .execution_authority(route.history, a.clone())
        .await?;
    let (entered, started) = tokio::sync::oneshot::channel();
    let (release, wait) = std::sync::mpsc::sync_channel(1);
    let caller = pool.clone();
    let attempt = cell_attempt(110)?;
    let task = fixture.cluster.domain.spawn(async move {
        caller
            .stage(options, authority, attempt, true, move |tx| {
                tx.execute("UPDATE value SET n=n+1", [])?;
                let _ = entered.send(());
                wait.recv_timeout(Duration::from_secs(10))?;
                Err(anyhow::Error::new(CommandFailure::Overloaded))
            })
            .await
    })?;
    tokio::time::timeout(Duration::from_secs(5), started).await??;
    let retained = pool.usage()?;
    let authority = fixture.runtimes[0]
        .execution_authority(route.history, b.clone())
        .await?;
    let result = pool
        .stage(
            fixture.cell_options(&b).await?,
            authority,
            cell_attempt(111)?,
            true,
            |_| panic!("node refusal entered the callback"),
        )
        .await;
    assert!(matches!(
        result,
        Err(ExecutionError::Refused(CommandFailure::Overloaded))
    ));
    let during = log.performance_snapshot();
    assert_eq!(
        count(&during, "sql_node_admission_refusals")?
            - count(&before, "sql_node_admission_refusals")?,
        1
    );
    assert_eq!(
        count(&during, "sql_group_admission_refusals")?,
        count(&before, "sql_group_admission_refusals")?
    );
    assert_eq!(
        count(&during, "sql_stage_admission_refusals")?
            - count(&before, "sql_stage_admission_refusals")?,
        1
    );
    assert_eq!(
        count(&during, "sql_stage_execution_refusals")?,
        count(&before, "sql_stage_execution_refusals")?
    );
    assert_eq!(pool.usage()?.active_cells, retained.active_cells);
    assert_eq!(pool.usage()?.retained_bytes, retained.retained_bytes);
    release.send(())?;
    let result = tokio::time::timeout(Duration::from_secs(5), task).await??;
    assert!(matches!(
        result,
        Err(ExecutionError::Refused(CommandFailure::Overloaded))
    ));
    let after = log.performance_snapshot();
    assert_eq!(
        count(&after, "sql_stage_execution_refusals")?
            - count(&before, "sql_stage_execution_refusals")?,
        1
    );
    assert_eq!(
        count(&after, "sql_private_open_refusals")?,
        count(&before, "sql_private_open_refusals")?
    );
    assert_eq!(pool.usage()?.active_cells, 0);
    assert_eq!(pool.usage()?.retained_bytes, 0);
    pool.close().await?;
    drop(pool);
    drop(log);
    fixture.close().await
}

pub(super) async fn private_workspace() -> Result<()> {
    use cellar::sdk::consensus::verification::RootPrivateOptions;
    use cellar_ltx::internal::StoreError;
    let (fixture, route, a, _) = enrolled_sql_cells(true).await?;
    let log = fixture.cluster.voters[0].log.clone();
    let pool = CellGroupSqlPool::start(
        &log,
        CellSqlLimits {
            workers: 1,
            queue_per_worker: 1,
            max_pending_cells: 2,
            group_pending_cells: 2,
            node_bytes: 256 * 1024 * 1024,
            group_bytes: 256 * 1024 * 1024,
        },
    )?;
    let options = fixture.cell_options(&a).await?;
    let CellSqlOptions::Extents(rooted) = &options else {
        anyhow::bail!("rooted fixture");
    };
    let store = rooted.images.clone();
    let bounds = RootPrivateOptions::workspace_bounds(rooted.limits)?;
    let limit = store
        .observer()
        .capture()
        .context("extent inventory")?
        .1
        .max_workspaces;
    let mut held = Vec::new();
    let mut full = false;
    for ordinal in 0..=limit {
        let mut id = [248; 32];
        id[..8].copy_from_slice(&(ordinal as u64).to_be_bytes());
        match store.create_workspace(id, bounds) {
            Ok(workspace) => held.push(workspace),
            Err(StoreError::Limit) => {
                full = true;
                break;
            }
            Err(error) => return Err(error.into()),
        }
    }
    anyhow::ensure!(
        full && !held.is_empty(),
        "filled the original physical bank"
    );
    let before = log.performance_snapshot();
    let authority = fixture.runtimes[0]
        .execution_authority(route.history, a.clone())
        .await?;
    let result = pool
        .stage(options.clone(), authority, cell_attempt(112)?, true, |_| {
            panic!("workspace refusal entered callback")
        })
        .await;
    assert!(matches!(
        result,
        Err(ExecutionError::Refused(CommandFailure::Overloaded))
    ));
    let during = log.performance_snapshot();
    assert_eq!(
        count(&during, "sql_private_open_refusals")? - count(&before, "sql_private_open_refusals")?,
        1
    );
    assert_eq!(
        count(&during, "sql_stage_execution_refusals")?
            - count(&before, "sql_stage_execution_refusals")?,
        1
    );
    assert_eq!(
        count(&during, "sql_stage_admission_refusals")?,
        count(&before, "sql_stage_admission_refusals")?
    );
    assert_eq!(pool.usage()?.active_cells, 0);
    assert_eq!(pool.usage()?.retained_bytes, 0);
    drop(held);
    let authority = fixture.runtimes[0]
        .execution_authority(route.history, a)
        .await?;
    let result = pool
        .stage(options, authority, cell_attempt(112)?, true, |_| {
            Err(anyhow::Error::new(CommandFailure::Overloaded))
        })
        .await;
    assert!(matches!(
        result,
        Err(ExecutionError::Refused(CommandFailure::Overloaded))
    ));
    let after = log.performance_snapshot();
    assert_eq!(
        count(&after, "sql_private_open_refusals")?,
        count(&during, "sql_private_open_refusals")?
    );
    assert_eq!(
        count(&after, "sql_stage_execution_refusals")?
            - count(&during, "sql_stage_execution_refusals")?,
        1
    );
    assert_eq!(store.usage()?.workspaces, 0);
    assert!(!store.usage()?.quarantined);
    pool.close().await?;
    drop(pool);
    drop(store);
    drop(log);
    fixture.close().await
}
