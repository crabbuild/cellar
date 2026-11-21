// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A proven live producer need not reopen its already captured SQLite image.

use super::*;
use futures_util::FutureExt as _;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::super::super::fault as files;

pub(super) async fn reconcile(
    rooted: bool,
    store: Arc<object_store::memory::InMemory>,
) -> Result<()> {
    let mut root = tempfile::tempdir()?;
    root.disable_cleanup(true);
    eprintln!("retained receipt-reuse disk: {}", root.path().display());
    let gate = files::Gate::new(None);
    let filesystem = Arc::new(files::FaultFileSystem { gate: gate.clone() });
    let class = RaftClass::new("ReceiptReuse", 1, [41; 32])?;
    let cluster = Cluster::with_store_birth(
        RaftRuntimeOptions::default(),
        root,
        filesystem.clone(),
        class.schema_digest(),
        store.clone(),
        ClusterBirthOptions {
            group: GroupId::new([3; 16])?,
            image_storage: if rooted {
                RaftImageStorage::Extents
            } else {
                RaftImageStorage::Materialized
            },
            minimum: Durability::Quorum,
            log: ClusterLogLimits::default(),
        },
    )
    .await?;
    let (mut fixture, route, cell, _) = Box::pin(super::registered::setup_cluster(
        rooted, cluster, filesystem,
    ))
    .await?;
    let pool = fixture.appliers[&(0, route.history)].0.clone();
    let outcome = std::panic::AssertUnwindSafe(async {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let authority = fixture.runtimes[0]
            .execution_authority(route.history, cell.clone())
            .await?;
        let CellSqlOutcome::Prepared(command) = pool
            .stage(
                fixture.cell_options(&cell).await?,
                authority,
                cell_attempt(232)?,
                true,
                move |tx| {
                    observed.fetch_add(1, Ordering::SeqCst);
                    tx.execute("UPDATE value SET n=n+7", [])?;
                    Ok(b"captured-receipt".to_vec())
                },
            )
            .await?
        else {
            anyhow::bail!("prepared command")
        };
        pool.mark_submitted(cell.clone(), command.proposal())
            .await?;
        let same_term = fixture.runtimes[0]
            .reconciliation_authority(route.history, cell.clone())
            .await?;
        anyhow::ensure!(matches!(
            pool.reconcile(command.proposal(), same_term).await,
            Err(ExecutionError::Uncertain)
        ));
        let stale = fixture.runtimes[0]
            .reconciliation_authority(route.history, cell.clone())
            .await?;
        let accepted = accepted_sql(&fixture.runtimes[0], &command).await?;
        fixture.drive(Some(accepted.position.index)).await?;
        anyhow::ensure!(matches!(
            pool.reconcile(command.proposal(), stale).await,
            Err(ExecutionError::Uncertain)
        ));
        let proof = fixture.runtimes[0]
            .reconciliation_authority(route.history, cell.clone())
            .await?;
        if let CellImagePin::Materialized(pin) = proof.image() {
            *gate.image_read_paths.lock().unwrap() = vec![fixture
                .cluster
                ._root
                .path()
                .join("node-1/images")
                .join(format!("{}.sqlite", hex::encode(pin.id().bytes())))];
        }
        let release = gate.arm(if rooted {
            files::HOLD_EXTENT_READ
        } else {
            files::HOLD_IMAGE_READ
        });
        let mut pending = Box::pin(pool.reconcile(command.proposal(), proof));
        let start = std::time::Instant::now();
        // This observes the same owned operation; timeout neither cancels it
        // nor changes command admission. Always release and settle before fail.
        let early = tokio::time::timeout(Duration::from_secs(1), &mut pending).await;
        let before_release = early.is_ok();
        let elapsed = start.elapsed();
        let held = gate.entered.notified().now_or_never().is_some();
        drop(release);
        let result = match early {
            Ok(result) => result,
            Err(_) => pending.await,
        }?;
        let CellSqlReconciliation::Committed(receipt) = result else {
            anyhow::bail!("committed receipt")
        };
        anyhow::ensure!(receipt.receipt().receipt.result() == b"captured-receipt");
        anyhow::ensure!(receipt.receipt().position == accepted.position);
        anyhow::ensure!(receipt.receipt().witness.proposal == command.proposal());
        let version = receipt.receipt().version.clone();
        anyhow::ensure!(version.sequence.get() == 2);
        drop(receipt);
        drop(command);
        // Retry returns the exact replicated receipt without another callback.
        let authority = fixture.runtimes[0]
            .execution_authority(route.history, cell.clone())
            .await?;
        let CellSqlOutcome::Receipt(receipt) = pool
            .stage(
                fixture.cell_options(&cell).await?,
                authority,
                cell_attempt(233)?,
                false,
                |_| panic!("duplicate callback"),
            )
            .await?
        else {
            anyhow::bail!("duplicate receipt")
        };
        anyhow::ensure!(receipt.receipt().receipt.result() == b"captured-receipt");
        drop(receipt);
        for node in 0..3 {
            let version = version.clone();
            let authority =
                super::reads::selected(&mut fixture, node, &cell, CellReadConsistency::Stale)
                    .await?;
            let output = fixture.appliers[&(node, route.history)]
                .0
                .query(
                    fixture.cell_options_at(&cell, node).await?,
                    authority,
                    move |db, actual| {
                        anyhow::ensure!(actual == &version);
                        let value: i64 =
                            db.query_row("SELECT n FROM value", [], |row| row.get(0))?;
                        Ok(value.to_le_bytes().to_vec())
                    },
                )
                .await?;
            anyhow::ensure!(output.result() == 7_i64.to_le_bytes());
        }
        anyhow::ensure!(calls.load(Ordering::SeqCst) == 1);
        anyhow::ensure!(pool.usage()?.active_cells == 0);
        anyhow::ensure!(pool.usage()?.retained_operations == 0);
        anyhow::ensure!(pool.usage()?.retained_bytes == 0);
        Ok::<_, anyhow::Error>((before_release, held, elapsed))
    })
    .catch_unwind()
    .await;
    drop(pool);
    let closed = fixture.close().await;
    eprintln!(
        "RECEIPT_REUSE_CLEANUP {{\"returnedOk\":{}}}",
        closed.is_ok()
    );
    if let Err(error) = &closed {
        eprintln!("receipt-reuse cleanup error: {error:#}");
    }
    let (before_release, held, elapsed) = match outcome {
        Ok(result) => result?,
        Err(panic) => std::panic::resume_unwind(panic),
    };
    closed?;
    eprintln!("RECEIPT_REUSE {{\"rooted\":{rooted},\"beforeRelease\":{before_release},\"heldRead\":{held},\"observationMicros\":{}}}", elapsed.as_micros());
    anyhow::ensure!(
        before_release && !held,
        "reconciliation reopened its captured image"
    );
    Ok(())
}
