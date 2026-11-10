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
        let duplicate = h
            .enroll(&h.dispatcher, 210, Durability::S3, Duration::from_secs(15))
            .await?;
        controller::wait_for(async || Ok(h.controllers[0].usage().retained_s3 == 2)).await?;
        let mut duplicate = Box::pin(duplicate.response());
        assert!(
            tokio::time::timeout(Duration::from_millis(30), duplicate.as_mut())
                .await
                .is_err()
        );
        drop(response);
        let response = duplicate.await?;
        assert_eq!(response.record(), &original);
        assert_eq!(response.durability(), Durability::S3);
        assert_eq!(h.initializations.load(Ordering::SeqCst), 1);
        drop(response);
        controller::wait_for(async || Ok(h.controllers[0].usage().retained_s3 == 0)).await?;
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
