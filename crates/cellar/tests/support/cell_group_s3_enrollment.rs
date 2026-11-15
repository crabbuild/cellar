// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Original creation records survive delayed archival and later cell mutations.
use super::*;

impl Harness {
    async fn enroll(
        &self,
        dispatcher: &CellGroupDispatcher,
        creation: u8,
        durability: Durability,
        timeout: Duration,
    ) -> Result<CellEnrollmentCall> {
        let invocation = dispatcher
            .enroll(
                self.cell.clone(),
                CellEnrollmentRequest {
                    creation: ProposalId::new([creation; 16])?,
                    durability,
                },
                CellRoutingLimits {
                    timeout,
                    max_attempts: 4,
                },
                None,
            )
            .await?;
        Ok(self.controllers[0].enroll(invocation, self.fixture.cell_options(&self.cell).await?)?)
    }

    async fn creation(&self) -> Result<EnrollmentRecord> {
        Ok(self.fixture.runtimes[0]
            .enrollment_state(self.route.history, self.cell.clone())
            .await?
            .record()
            .context("committed creation")?
            .clone())
    }
}

/// A proven creation remains buffered while a later entry awaits apply. Neither
/// a transient serving fence nor expiry can recreate its retired SQL producer.
pub(crate) async fn enrollment_output_during_apply(
    rooted: bool,
    store: Arc<dyn ObjectStore>,
    expire: bool,
) -> Result<()> {
    let root = tempfile::tempdir()?;
    let faults = apply_faults::physical::Faults::new(root.path().join("node-1/pages"), None);
    let filesystem = Arc::new(apply_faults::physical::FaultFileSystem {
        faults: faults.clone(),
    });
    let mut h = Harness::configured_resources(
        rooted,
        store,
        2,
        Durability::Quorum,
        None,
        None,
        Some(Resources::Storage { root, filesystem }),
    )
    .await?;
    h.cell.cell = CellId::new("enrollment-output")?;
    let publication = h.remote.hold_publication();

    struct Publication(Arc<archive_controller::archive_store::Gate>);

    impl Drop for Publication {
        fn drop(&mut self) {
            self.0.release.notify_one();
        }
    }
    let publication_release = Publication(publication.clone());
    let work = async {
        let call = h
            .enroll(&h.dispatcher, 225, Durability::S3, Duration::from_secs(5))
            .await?;
        tokio::time::timeout(Duration::from_secs(5), publication.entered.notified()).await?;
        h.waiting().await?;
        let original = h.creation().await?;
        let position = h.fixture.runtimes[0]
            .reconciliation_authority(h.route.history, h.cell.clone())
            .await?
            .cell()
            .last_mutation;
        let other = history(&h.route, "registered-b")?;
        let runtime = &h.fixture.runtimes[0];
        let authority = runtime
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
            anyhow::bail!("private preparation");
        };
        pool.mark_submitted(other.clone(), private.proposal())
            .await?;
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, held) = std::sync::mpsc::sync_channel(1);

        struct Unblock(std::sync::mpsc::SyncSender<()>);

        impl Drop for Unblock {
            fn drop(&mut self) {
                let _ = self.0.try_send(());
            }
        }
        let unblock = Unblock(release);
        *faults.before_directory_sync.lock().unwrap() = Some(Box::new(move || {
            let _ = entered.send(());
            let _ = held.recv_timeout(Duration::from_secs(8));
        }));
        let accepted = accepted_sql(runtime, &private).await?;
        tokio::time::timeout(Duration::from_secs(5), started).await??;
        let status = runtime.status(h.route.history).await?;
        anyhow::ensure!(
            status.committed >= accepted.position.index
                && status.applied.index < accepted.position.index
        );
        drop(publication_release);
        archive_controller::published(&h.controllers[0], h.route.history, position.index).await?;
        let mut response = Box::pin(call.response());
        let early = tokio::time::timeout(Duration::from_millis(500), response.as_mut()).await;
        let buffered = early.is_err();
        let premature = early.map(|result| result.map(|_| ()));
        let expired = if buffered && expire {
            let error = response
                .as_mut()
                .await
                .err()
                .context("original enrollment deadline")?;
            anyhow::ensure!(
                error.ambiguous
                    && matches!(
                        error.cause.as_ref(),
                        CellDispatchError::Runtime(RuntimeError::DeadlineExceeded)
                    ),
                "{error:?}"
            );
            true
        } else {
            false
        };
        anyhow::ensure!(h.controllers[0].usage().quarantined_groups == 0);
        anyhow::ensure!(h.initializations.load(Ordering::SeqCst) == 1);
        drop(unblock);
        controller::applied(&h.fixture, h.route.history, accepted.position.index).await?;
        let authority = runtime
            .reconciliation_authority(h.route.history, other.clone())
            .await?;
        let CellSqlReconciliation::Committed(blocker) =
            pool.reconcile(private.proposal(), authority).await?
        else {
            anyhow::bail!("blocking command receipt");
        };
        anyhow::ensure!(blocker.receipt().receipt.result() == b"blocking-result");
        drop(blocker);
        drop(private);
        anyhow::ensure!(
            buffered,
            "proven enrollment returned before fresh serving authority: {premature:?}"
        );
        if !expired {
            let receipt = response.await?;
            anyhow::ensure!(
                receipt.record() == &original && receipt.durability() == Durability::S3
            );
            drop(receipt);
        } else {
            drop(response);
        }
        let retry = h
            .enroll(&h.dispatcher, 225, Durability::S3, Duration::from_secs(5))
            .await?
            .response()
            .await?;
        anyhow::ensure!(retry.record() == &original && retry.durability() == Durability::S3);
        anyhow::ensure!(
            h.initializations.load(Ordering::SeqCst) == 1,
            "receipt retry reran initializer"
        );
        drop(retry);
        controller::wait_for(async || {
            Ok(h.controllers[0].usage().retained_commands == 0
                && h.controllers[0].usage().retained_s3 == 0
                && pool.usage()?.retained_operations == 0)
        })
        .await?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    if work.is_err() {
        h.fixture.cluster._root.disable_cleanup(true);
        eprintln!(
            "retained enrollment-output disk: {}",
            h.fixture.cluster._root.path().display()
        );
    }
    let closed = h.close().await;
    work?;
    closed
}

pub(crate) async fn s3_enrollment(rooted: bool, store: Arc<dyn ObjectStore>) -> Result<()> {
    for minimum in [Durability::Quorum, Durability::S3] {
        let backend = Arc::new(object_store::prefix::PrefixStore::new(
            store.clone(),
            ObjectPath::from(if minimum == Durability::S3 {
                "namespace-s3"
            } else {
                "namespace-quorum"
            }),
        ));
        let mut h = Harness::with_minimum(rooted, backend, 2, minimum).await?;
        h.cell.cell = CellId::new("new-s3-enrollment")?;
        let requested = if minimum == Durability::S3 {
            Durability::Quorum
        } else {
            Durability::S3
        };
        let foreign = h.dispatcher.clone().with_s3(&h.controllers[1])?;
        let error = h
            .enroll(&foreign, 210, requested, Duration::from_secs(15))
            .await?
            .response()
            .await
            .err()
            .context("foreign enrollment runtime")?;
        assert!(!error.ambiguous);
        assert!(matches!(
            error.cause.as_ref(),
            CellDispatchError::Runtime(RuntimeError::Invalid(_))
        ));
        assert_eq!(h.initializations.load(Ordering::SeqCst), 0);
        h.remote.outage.store(true, Ordering::Release);
        let call = h
            .enroll(&h.dispatcher, 210, requested, Duration::from_secs(15))
            .await?;
        h.waiting().await?;
        let original = h.creation().await?;
        assert_eq!(original.creation(), ProposalId::new([210; 16])?);
        assert_eq!(h.initializations.load(Ordering::SeqCst), 1);
        assert_eq!(
            h.fixture.appliers[&(0, h.route.history)]
                .0
                .usage()?
                .retained_operations,
            0
        );
        if minimum == Durability::Quorum {
            let changed = h
                .command(
                    1,
                    b"after-initialization",
                    Durability::Quorum,
                    Duration::from_secs(15),
                )
                .await?
                .response()
                .await?;
            assert_eq!(
                changed
                    .local_receipt()
                    .expect("local command receipt")
                    .version
                    .sequence
                    .get(),
                2
            );
            drop(changed);
        }
        let gate = h.remote.hold_publication();
        h.remote.outage.store(false, Ordering::Release);
        tokio::time::timeout(Duration::from_secs(10), gate.entered.notified()).await?;
        let mut response = Box::pin(call.response());
        assert!(
            tokio::time::timeout(Duration::from_millis(30), response.as_mut())
                .await
                .is_err()
        );
        gate.release.notify_one();
        let response = response.await?;
        assert_eq!(response.durability(), Durability::S3);
        assert_eq!(response.record(), &original);
        assert_eq!(h.archive.tails.usage().operations, 1);
        assert_eq!(h.initializations.load(Ordering::SeqCst), 1);
        // Replaying this creation shares its live admitted verification. It
        // must succeed without another immutable upload or verifier workspace.
        h.remote
            .deny_generation_writes
            .store(true, Ordering::Release);
        let duplicate = h
            .enroll(&h.dispatcher, 210, Durability::S3, Duration::from_secs(15))
            .await?
            .response()
            .await?;
        controller::wait_for(async || Ok(h.controllers[0].usage().retained_s3 == 2)).await?;
        assert_eq!(duplicate.record(), &original);
        assert_eq!(duplicate.durability(), Durability::S3);
        assert_eq!(h.archive.tails.usage().operations, 1);
        assert_eq!(h.initializations.load(Ordering::SeqCst), 1);
        drop(response);
        controller::wait_for(async || Ok(h.controllers[0].usage().retained_s3 == 1)).await?;
        assert_eq!(h.archive.tails.usage().operations, 1);

        // Dropping the first consumer cannot free the last consumer's physical
        // admission. A new command is uncovered by the shared historical proof.
        h.remote
            .deny_generation_writes
            .store(false, Ordering::Release);
        let calls = h.calls.load(Ordering::SeqCst);
        let sequence = if minimum == Durability::Quorum { 2 } else { 1 };
        let mut successor = Box::pin(
            h.command(
                sequence,
                b"after-shared-enrollment",
                Durability::S3,
                Duration::from_secs(15),
            )
            .await?
            .response(),
        );
        controller::wait_for(async || {
            Ok(h.controllers[0].usage().retained_s3 == 2
                && h.calls.load(Ordering::SeqCst) == calls + 1
                && h.fixture.appliers[&(0, h.route.history)]
                    .0
                    .usage()?
                    .active_cells
                    == 0)
        })
        .await?;
        assert!(
            tokio::time::timeout(Duration::from_millis(30), successor.as_mut())
                .await
                .is_err()
        );
        assert_eq!(h.archive.tails.usage().operations, 1);
        drop(duplicate);
        let response = successor.await?;
        assert_eq!(response.durability(), Durability::S3);
        let receipt = response.local_receipt().context("successor S3 receipt")?;
        assert_eq!(receipt.receipt.result(), b"after-shared-enrollment");
        assert_eq!(receipt.version.sequence.get(), sequence + 1);
        assert_eq!(h.calls.load(Ordering::SeqCst), calls + 1);
        assert_eq!(h.initializations.load(Ordering::SeqCst), 1);
        drop(response);
        controller::wait_for(async || Ok(h.controllers[0].usage().retained_s3 == 0)).await?;
        h.remote
            .deny_generation_writes
            .store(false, Ordering::Release);
        let error = h
            .enroll(&h.dispatcher, 211, Durability::S3, Duration::from_secs(15))
            .await?
            .response()
            .await
            .err()
            .context("conflicting creation")?;
        assert!(matches!(
            error.cause.as_ref(),
            CellDispatchError::Execution(ExecutionError::Refused(CommandFailure::CommandConflict))
        ));
        assert_eq!(h.initializations.load(Ordering::SeqCst), 1);
        assert!(h.controllers[0].group_failure(h.route.history)?.is_none());
        h.close().await?;
    }
    Ok(())
}

pub(crate) async fn s3_enrollment_deadline(
    rooted: bool,
    store: Arc<dyn ObjectStore>,
    cancel: bool,
) -> Result<()> {
    let mut h = Harness::new(rooted, store, 1).await?;
    h.cell.cell = CellId::new("expired-s3-enrollment")?;
    h.remote.outage.store(true, Ordering::Release);
    let call = h
        .enroll(
            &h.dispatcher,
            212,
            Durability::S3,
            Duration::from_millis(800),
        )
        .await?;
    h.waiting().await?;
    let original = h.creation().await?;
    if cancel {
        drop(call);
    } else {
        let error = call
            .response()
            .await
            .err()
            .context("S3 enrollment deadline")?;
        assert!(error.ambiguous);
        assert!(matches!(
            error.cause.as_ref(),
            CellDispatchError::Runtime(RuntimeError::DeadlineExceeded)
        ));
    }
    controller::wait_for(async || {
        Ok(h.controllers[0].usage().retained_commands == 0
            && h.controllers[0].usage().retained_s3 == 0)
    })
    .await?;
    assert!(h.controllers[0].group_failure(h.route.history)?.is_none());
    assert_eq!(
        h.fixture.appliers[&(0, h.route.history)]
            .0
            .usage()?
            .retained_operations,
        0
    );
    let changed = h
        .command(
            1,
            b"after-deadline",
            Durability::Quorum,
            Duration::from_secs(15),
        )
        .await?
        .response()
        .await?;
    assert_eq!(
        changed
            .local_receipt()
            .expect("local command receipt")
            .version
            .sequence
            .get(),
        2
    );
    drop(changed);
    h.remote.outage.store(false, Ordering::Release);
    let response = h
        .enroll(&h.dispatcher, 212, Durability::S3, Duration::from_secs(15))
        .await?
        .response()
        .await?;
    assert_eq!(response.record(), &original);
    assert_eq!(response.durability(), Durability::S3);
    assert_eq!(h.initializations.load(Ordering::SeqCst), 1);
    drop(response);
    h.close().await
}
