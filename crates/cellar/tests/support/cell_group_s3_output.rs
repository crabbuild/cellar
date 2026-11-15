// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A resolved SQL producer cannot be reconciled again during S3 output fencing.
use super::*;
use futures_util::FutureExt;

pub(crate) async fn output_during_apply(rooted: bool) -> Result<()> {
    for duplicate in [false, true] {
        Box::pin(run(rooted, duplicate)).await?;
    }
    Ok(())
}

async fn run(rooted: bool, duplicate: bool) -> Result<()> {
    let root = tempfile::tempdir()?;
    let faults = apply_faults::physical::Faults::new(root.path().join("node-1/pages"), None);
    let filesystem = Arc::new(apply_faults::physical::FaultFileSystem {
        faults: faults.clone(),
    });
    let h = Harness::configured_resources(
        rooted,
        Arc::new(object_store::memory::InMemory::new()),
        2,
        Durability::Quorum,
        None,
        None,
        Some(Resources::Storage { root, filesystem }),
    )
    .await?;
    let work = std::panic::AssertUnwindSafe(async {
        let publication = h.remote.hold_publication();

        struct Publication(Arc<archive_controller::archive_store::Gate>);

        impl Drop for Publication {
            fn drop(&mut self) {
                self.0.release.notify_one();
            }
        }
        let publication_release = Publication(publication.clone());
        if duplicate {
            // The duplicate enters S3 settlement from preparation's receipt path,
            // rather than reconciling a newly accepted SQL producer.
            let first = h
                .command(
                    1,
                    b"retained-output",
                    Durability::Quorum,
                    Duration::from_secs(5),
                )
                .await?
                .response()
                .await?;
            assert_eq!(
                first
                    .local_receipt()
                    .expect("local command receipt")
                    .receipt
                    .result(),
                b"retained-output"
            );
            drop(first);
        }
        let before_local = h.command_phase("cell_command_local")?;
        let before_coverage = h.command_phase("cell_command_s3_coverage")?;
        let before_output = h.command_phase("cell_command_output_check")?;
        let before_freshness = h.command_phase("cell_command_s3_freshness")?;
        let call = h
            .command(
                1,
                b"retained-output",
                Durability::S3,
                Duration::from_secs(5),
            )
            .await?;
        tokio::time::timeout(Duration::from_secs(5), publication.entered.notified()).await?;
        h.waiting().await?;
        let coverage = h.command_phase("cell_command_s3_coverage")?;
        assert_eq!(coverage.started, before_coverage.started + 1);
        assert_eq!(coverage.succeeded, before_coverage.succeeded);
        assert_eq!(coverage.failed, before_coverage.failed);
        assert_eq!(coverage.abandoned, before_coverage.abandoned);
        assert_eq!(
            h.command_phase("cell_command_output_check")?.started,
            before_output.started
        );
        assert_eq!(
            h.command_phase("cell_command_s3_freshness")?.started,
            before_freshness.started
        );
        assert_eq!(
            h.command_phase("cell_command_local")?.succeeded,
            before_local.succeeded
        );

        // The first producer has retired. Hold a later cell's real manifest sync,
        // without enlarging the one-item SQL admission or changing the old image.
        let other = history(&h.route, "registered-b")?;
        let authority = h.fixture.runtimes[0]
            .execution_authority(h.route.history, other.clone())
            .await?;
        let options = h.fixture.cell_options(&other).await?;
        let pool = h.fixture.appliers[&(0, h.route.history)].0.clone();
        let CellSqlOutcome::Prepared(private) = pool
            .stage(options, authority, cell_attempt(239)?, true, |tx| {
                tx.execute("UPDATE value SET n=n+1", [])?;
                Ok(b"blocking-result".to_vec())
            })
            .await?
        else {
            anyhow::bail!("private preparation")
        };
        pool.mark_submitted(other.clone(), private.proposal())
            .await?;
        let original = h.fixture.runtimes[0]
            .reconciliation_authority(h.route.history, h.cell.clone())
            .await?
            .cell()
            .last_mutation;
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, blocked) = std::sync::mpsc::sync_channel(1);

        struct Unblock(std::sync::mpsc::SyncSender<()>);

        impl Drop for Unblock {
            fn drop(&mut self) {
                let _ = self.0.try_send(());
            }
        }
        let unblock = Unblock(release);
        *faults.before_directory_sync.lock().unwrap() = Some(Box::new(move || {
            let _ = entered.send(());
            blocked
                .recv_timeout(Duration::from_secs(5))
                .expect("release real manifest sync");
        }));
        let accepted = accepted_sql(&h.fixture.runtimes[0], &private).await?;
        tokio::time::timeout(Duration::from_secs(5), started).await??;
        controller::wait_for(async || {
            let status = h.fixture.runtimes[0].status(h.route.history).await?;
            Ok(status.committed >= accepted.position.index
                && status.applied.index < accepted.position.index)
        })
        .await?;
        drop(publication_release);
        archive_controller::published(&h.controllers[0], h.route.history, original.index).await?;
        let mut response = Box::pin(call.response());
        assert!(
        tokio::time::timeout(Duration::from_millis(30), response.as_mut())
            .await
            .is_err(),
        "committed output must wait for fresh authority without revisiting its retired SQL producer"
    );
        assert_eq!(h.controllers[0].usage().quarantined_groups, 0);
        assert_eq!(h.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            h.command_phase("cell_command_local")?.succeeded,
            before_local.succeeded
        );
        drop(unblock);
        controller::applied(&h.fixture, h.route.history, accepted.position.index).await?;
        let authority = h.fixture.runtimes[0]
            .reconciliation_authority(h.route.history, other.clone())
            .await?;
        let CellSqlReconciliation::Committed(blocker_receipt) =
            pool.reconcile(private.proposal(), authority).await?
        else {
            anyhow::bail!("blocking command receipt")
        };
        assert_eq!(
            blocker_receipt.receipt().receipt.result(),
            b"blocking-result"
        );
        drop(blocker_receipt);
        drop(private);
        let receipt = response.await?;
        assert_eq!(receipt.durability(), Durability::S3);
        assert_eq!(
            receipt
                .local_receipt()
                .expect("local command receipt")
                .receipt
                .result(),
            b"retained-output"
        );
        assert_eq!(
            receipt
                .local_receipt()
                .expect("local command receipt")
                .version
                .sequence
                .get(),
            2
        );
        for before in [&before_local, &before_coverage] {
            let after = h.command_phase(before.name)?;
            assert_eq!(after.started, before.started + 1);
            assert_eq!(after.succeeded, before.succeeded + 1);
            assert_eq!(after.failed, before.failed);
            assert_eq!(after.abandoned, before.abandoned);
        }
        let output = h.command_phase("cell_command_output_check")?;
        assert!(output.started > before_output.started);
        assert_eq!(output.succeeded, before_output.succeeded + 1);
        assert_eq!(output.abandoned, before_output.abandoned);
        let freshness = h.command_phase("cell_command_s3_freshness")?;
        assert!(freshness.succeeded > before_freshness.succeeded);
        assert_eq!(freshness.failed, before_freshness.failed);
        assert_eq!(freshness.abandoned, before_freshness.abandoned);
        drop(receipt);
        let retry = h
            .command(
                1,
                b"retained-output",
                Durability::S3,
                Duration::from_secs(5),
            )
            .await?
            .response()
            .await?;
        assert_eq!(
            retry
                .local_receipt()
                .expect("local command receipt")
                .receipt
                .result(),
            b"retained-output"
        );
        assert_eq!(h.calls.load(Ordering::SeqCst), 1);
        drop(retry);
        for runtime in &h.fixture.runtimes {
            let cell = runtime
                .reconciliation_authority(h.route.history, other.clone())
                .await?;
            assert_eq!(cell.cell().version.sequence.get(), 2);
        }
        Ok::<_, anyhow::Error>(())
    })
    .catch_unwind()
    .await;
    let closed = h.close().await;
    match work {
        Ok(result) => result.and(closed),
        Err(panic) => {
            if let Err(error) = closed {
                eprintln!("S3 output fixture cleanup after panic failed: {error}");
            }
            std::panic::resume_unwind(panic)
        }
    }
}
