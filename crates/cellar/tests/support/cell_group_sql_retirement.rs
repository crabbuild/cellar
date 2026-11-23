// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Preview retirement shares the node inventory, not the SQL callback lane.

use super::super::super::fault as files;
use super::*;
use futures_util::FutureExt as _;

pub(super) async fn progress(
    reconcile: bool,
    store: Arc<object_store::memory::InMemory>,
) -> Result<()> {
    let mut root = tempfile::tempdir()?;
    root.disable_cleanup(true);
    eprintln!(
        "retained preview-retirement disk: {}",
        root.path().display()
    );
    let gate = files::Gate::new(None);
    let filesystem = Arc::new(files::FaultFileSystem { gate: gate.clone() });
    let cluster = Cluster::with_store_birth(
        RaftRuntimeOptions::default(),
        root,
        filesystem.clone(),
        [6; 32],
        store.clone(),
        ClusterBirthOptions {
            group: GroupId::new([3; 16])?,
            image_storage: RaftImageStorage::Extents,
            minimum: Durability::Quorum,
            log: ClusterLogLimits::default(),
        },
    )
    .await?;
    let initial = RuntimeCluster::from_cluster(cluster, 256, 256, 1_000_000).await?;
    let (mut fixture, route, a, b) =
        enroll_sql_cells_with_admission(initial, true, filesystem, true).await?;
    for node in 0..3 {
        // One existing SQL worker; two operations admit the held command and
        // unrelated query. The ordinary Queue workload remains unchanged.
        let pool = CellGroupSqlPool::start(
            &fixture.cluster.voters[node].log,
            CellSqlLimits {
                workers: 1,
                queue_per_worker: 1,
                max_pending_cells: 2,
                group_pending_cells: 2,
                node_bytes: 1024 * 1024 * 1024,
                group_bytes: 512 * 1024 * 1024,
            },
        )?;
        apply::install_with_pool(&mut fixture, &a, node, pool).await?;
    }
    let pool = fixture.appliers[&(0, route.history)].0.clone();
    let mut physical = None;
    let outcome = std::panic::AssertUnwindSafe(async {
        let options_a = fixture.cell_options(&a).await?;
        let options_b = fixture.cell_options(&b).await?;
        let CellSqlOptions::Extents(rooted) = &options_a else {
            anyhow::bail!("rooted retirement fixture");
        };
        let inventory = rooted.images.clone();
        let authority = fixture.runtimes[0]
            .execution_authority(route.history, a.clone())
            .await?;
        let CellSqlOutcome::Prepared(command) = pool
            .stage(options_a, authority, cell_attempt(234)?, true, |tx| {
                tx.execute("UPDATE value SET n=n+7", [])?;
                Ok(b"retirement-result".to_vec())
            })
            .await?
        else {
            anyhow::bail!("prepared command");
        };
        pool.mark_submitted(a.clone(), command.proposal()).await?;
        let accepted = accepted_sql(&fixture.runtimes[0], &command).await?;
        fixture.drive(Some(accepted.position.index)).await?;
        let committed = fixture.runtimes[0]
            .execution_authority(route.history, a.clone())
            .await?;
        let observation = fixture.runtimes[0]
            .reconciliation_authority(route.history, a.clone())
            .await?;
        let selected_b = reads::selected(&mut fixture, 0, &b, CellReadConsistency::Stale).await?;
        let CellImagePin::Extents(image) = committed.image() else {
            anyhow::bail!("selected extent image");
        };
        let extent = image
            .manifest()
            .extent(0)
            .context("selected first extent")?;
        let before = pool.usage()?;
        anyhow::ensure!(before.active_cells == 1 && before.retained_bytes > 0);
        let workspace = inventory.usage()?;
        anyhow::ensure!(workspace.workspaces > 0 && workspace.workspace_bytes > 0);
        let release = gate.arm(files::HOLD_EXTENT_READ);
        let checking = inventory.clone();
        physical = Some(
            fixture
                .cluster
                .domain
                .blocking(move || checking.verify_extent(extent))?,
        );
        tokio::time::timeout(Duration::from_secs(5), gate.entered.notified())
            .await
            .context("inventory verification entered")?;
        anyhow::ensure!(inventory.usage()?.busy);
        let mut settlement = Box::pin(async {
            if reconcile {
                let CellSqlReconciliation::Committed(receipt) =
                    pool.reconcile(command.proposal(), observation).await?
                else {
                    anyhow::bail!("committed reconciliation");
                };
                Ok::<_, anyhow::Error>(*receipt)
            } else {
                Ok(pool.finish(committed).await?)
            }
        });
        anyhow::ensure!(futures_util::poll!(settlement.as_mut()).is_pending());
        let mut query = Box::pin(pool.query(options_b, selected_b, |db, _| {
            let value: i64 = db.query_row("SELECT n FROM value", [], |row| row.get(0))?;
            Ok(value.to_le_bytes().to_vec())
        }));
        // Observe these original operations without cancelling or retrying them.
        let early = tokio::time::timeout(Duration::from_secs(1), &mut query).await;
        let progressed = early.is_ok();
        let reply_withheld = futures_util::poll!(settlement.as_mut()).is_pending();
        let held = pool.usage()?;
        let physical_held = inventory.usage()?;
        drop(release);
        physical
            .take()
            .context("physical verification owner")?
            .await??;
        let output = match early {
            Ok(result) => result,
            Err(_) => query.await,
        }?;
        let receipt = settlement.await?;
        anyhow::ensure!(output.result() == 0_i64.to_le_bytes());
        anyhow::ensure!(receipt.receipt().receipt.result() == b"retirement-result");
        anyhow::ensure!(receipt.receipt().position == accepted.position);
        anyhow::ensure!(receipt.receipt().witness.proposal == command.proposal());
        drop(output);
        drop(receipt);
        drop(command);
        anyhow::ensure!(pool.usage()?.active_cells == 0 && pool.usage()?.retained_bytes == 0);
        anyhow::ensure!(
            held.active_cells == 1 && held.retained_bytes >= before.retained_bytes,
            "settlement released original admission before retirement joined"
        );
        anyhow::ensure!(
            physical_held.workspaces == workspace.workspaces
                && physical_held.workspace_bytes == workspace.workspace_bytes,
            "settlement refunded its physical workspace while retirement was blocked"
        );
        anyhow::ensure!(reply_withheld, "receipt preceded joined retirement");
        Ok::<_, anyhow::Error>(progressed)
    })
    .catch_unwind()
    .await;
    // An assertion or panic releases the injected hold before joining its owner.
    let joined = match physical.take() {
        Some(task) => task
            .await
            .map_err(anyhow::Error::from)
            .and_then(|result| result.map_err(Into::into)),
        None => Ok(()),
    };
    drop(pool);
    let closed = fixture.close().await;
    eprintln!(
        "PREVIEW_RETIREMENT_CLEANUP {{\"returnedOk\":{}}}",
        closed.is_ok()
    );
    let progressed = match outcome {
        Ok(result) => result?,
        Err(panic) => std::panic::resume_unwind(panic),
    };
    joined?;
    closed?;
    eprintln!(
        "PREVIEW_RETIREMENT {{\"reconcile\":{reconcile},\"unrelatedSqlProgress\":{progressed}}}"
    );
    anyhow::ensure!(progressed, "preview retirement blocked unrelated SQL");
    Ok(())
}
