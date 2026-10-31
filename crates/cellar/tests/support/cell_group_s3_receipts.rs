// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Read-only S3 results at a later cut, with bounded payload allocation.
use super::*;

impl Harness {
    fn receipt_request(
        &self,
        sequence: u64,
        input: &[u8],
        durability: Durability,
    ) -> Result<CellReceiptRequest> {
        Ok(CellReceiptRequest {
            id: CommandId::new(ClientId::new([188; 16])?, sequence)?,
            contract: self.class.contract("Add").context("Add")?,
            input: input.to_vec(),
            durability,
        })
    }

    async fn lookup(
        &self,
        request: CellReceiptRequest,
        timeout: Duration,
    ) -> Result<CellReceiptInvocation> {
        let mut invocation = self.dispatcher.lookup_receipt(
            self.cell.clone(),
            request,
            CellRoutingLimits {
                timeout,
                max_attempts: 4,
            },
            None,
        )?;
        invocation.begin().await?;
        Ok(invocation)
    }

    async fn now(&self) -> Result<u64> {
        Ok(self
            .fixture
            .cluster
            .domain
            .run(async { cellar::asyncrt::wall_ms().max(0) as u64 })
            .await?)
    }

    async fn token(&self, response: &CellReceiptResponse) -> Result<()> {
        let keys = VersionKeys::new(
            VersionSigningKey::new([89; 32], u64::MAX)?,
            vec![],
            Duration::from_secs(30),
        )?;
        assert_eq!(
            keys.verify_cell(
                response.token(),
                &self.cell,
                self.class.schema_digest(),
                self.now().await?
            )?,
            *response.observation()
        );
        Ok(())
    }

    async fn published_command(&self, sequence: u64, input: &[u8]) -> Result<()> {
        self.published_with_ceiling(sequence, input, None).await
    }

    async fn published_with_ceiling(
        &self,
        sequence: u64,
        input: &[u8],
        ceiling: Option<usize>,
    ) -> Result<()> {
        let mut options = self.fixture.cell_options(&self.cell).await?;
        if let Some(ceiling) = ceiling {
            match &mut options {
                CellSqlOptions::Materialized(o) => o.limits.max_retained_receipts = ceiling,
                CellSqlOptions::Extents(o) => o.limits.max_retained_receipts = ceiling,
            }
        }
        let response = self
            .command_using(
                sequence,
                input,
                Durability::Quorum,
                Duration::from_secs(15),
                options,
            )
            .await?
            .response()
            .await?;
        let index = response.receipt().position.index;
        drop(response);
        archive_controller::published(&self.controllers[0], self.route.history, index).await
    }
}

pub(crate) async fn s3_receipt_observations(
    rooted: bool,
    store: Arc<dyn ObjectStore>,
) -> Result<()> {
    let h = Harness::new(rooted, store, 1).await?;
    let pool = &h.fixture.appliers[&(0, h.route.history)].0;
    let runtime = &h.fixture.runtimes[0];
    let requested = h.receipt_request(1, b"late", Durability::S3)?;
    let mut unknown = h.lookup(requested.clone(), Duration::from_secs(15)).await?;
    let response = unknown
        .execute(runtime, pool, h.fixture.cell_options(&h.cell).await?)
        .await?;
    assert_eq!(response.durability(), Durability::S3);
    assert!(response.receipt().is_none());
    assert_eq!(response.observation().version.sequence.get(), 1);
    h.token(&response).await?;
    drop(response);
    drop(unknown);
    let mut lookup = h.lookup(requested.clone(), Duration::from_secs(15)).await?;
    let selection = lookup
        .select(runtime, pool, h.fixture.cell_options(&h.cell).await?)
        .await?;
    assert_eq!(h.controllers[0].usage().retained_s3, 1);
    assert_eq!(h.controllers[0].usage().s3_bytes, 8192);
    assert_eq!(pool.usage()?.retained_operations, 0);
    // The original SQL read observed None at version 1. The same one-slot node
    // can now commit and archive the command before proof submission.
    h.published_command(1, b"late").await?;
    let response = selection.response().await?;
    assert_eq!(response.durability(), Durability::S3);
    assert_eq!(
        response
            .receipt()
            .context("later archived receipt")?
            .result(),
        b"late"
    );
    assert_eq!(response.observation().version.sequence.get(), 2);
    assert_eq!(h.archive.tails.usage().operations, 1);
    assert_eq!(h.controllers[0].usage().s3_bytes, 8192 + 2 * b"late".len());
    h.token(&response).await?;
    drop(response);
    drop(lookup);
    // A retained result is exact across newer images, and its token identifies
    // the actually inspected archived image rather than the older SQL selection.
    let mut known = h.lookup(requested, Duration::from_secs(15)).await?;
    let selection = known
        .select(runtime, pool, h.fixture.cell_options(&h.cell).await?)
        .await?;
    h.published_command(2, b"").await?;
    let response = selection.response().await?;
    assert_eq!(
        response.receipt().context("retained result")?.result(),
        b"late"
    );
    assert_eq!(response.observation().version.sequence.get(), 3);
    h.token(&response).await?;
    drop(response);
    drop(known);
    let mut empty = h
        .lookup(
            h.receipt_request(2, b"", Durability::S3)?,
            Duration::from_secs(15),
        )
        .await?;
    let response = empty
        .execute(runtime, pool, h.fixture.cell_options(&h.cell).await?)
        .await?;
    assert_eq!(
        response.receipt().map(|row| row.result()),
        Some(b"".as_slice())
    );
    drop(response);
    drop(empty);
    let mut missing = h.receipt_request(1, b"missing", Durability::S3)?;
    missing.id = CommandId::new(ClientId::new([189; 16])?, 1)?;
    let mut missing = h.lookup(missing, Duration::from_secs(15)).await?;
    let response = missing
        .execute(runtime, pool, h.fixture.cell_options(&h.cell).await?)
        .await?;
    assert!(response.receipt().is_none());
    assert_eq!(response.durability(), Durability::S3);
    assert_eq!(response.observation().version.sequence.get(), 3);
    drop(response);
    drop(missing);
    // A provider in the same domain cannot lend a different runtime's proof.
    let foreign = h.dispatcher.clone().with_s3(&h.controllers[1])?;
    let mut foreign = foreign.lookup_receipt(
        h.cell.clone(),
        h.receipt_request(1, b"late", Durability::S3)?,
        CellRoutingLimits {
            timeout: Duration::from_secs(15),
            max_attempts: 4,
        },
        None,
    )?;
    foreign.begin().await?;
    assert!(matches!(
        foreign
            .select(runtime, pool, h.fixture.cell_options(&h.cell).await?)
            .await
            .err(),
        Some(CellDispatchError::Runtime(RuntimeError::Invalid(_)))
    ));
    drop(foreign);
    assert_eq!(h.calls.load(Ordering::SeqCst), 2);
    assert_eq!(h.initializations.load(Ordering::SeqCst), 0);
    assert_eq!(h.controllers[0].usage().retained_s3, 0);
    assert_eq!(pool.usage()?.retained_operations, 0);
    h.close().await
}

pub(crate) async fn s3_receipt_minimums(rooted: bool, store: Arc<dyn ObjectStore>) -> Result<()> {
    for minimum in [Durability::Quorum, Durability::S3] {
        let backend = Arc::new(object_store::prefix::PrefixStore::new(
            store.clone(),
            ObjectPath::from(if minimum == Durability::S3 {
                "namespace-s3"
            } else {
                "namespace-quorum"
            }),
        ));
        let h = Harness::with_minimum(rooted, backend, 1, minimum).await?;
        let pool = &h.fixture.appliers[&(0, h.route.history)].0;
        let runtime = &h.fixture.runtimes[0];
        let result = h
            .command(1, b"once", Durability::S3, Duration::from_secs(15))
            .await?
            .response()
            .await?;
        drop(result);
        let request = h.receipt_request(1, b"once", Durability::Quorum)?;
        let mut lookup = h.lookup(request, Duration::from_secs(15)).await?;
        let selection = lookup
            .select(runtime, pool, h.fixture.cell_options(&h.cell).await?)
            .await?;
        assert_eq!(h.controllers[0].usage().retained_s3, 1);
        assert_eq!(pool.usage()?.retained_operations, 0);
        drop(selection);
        assert_eq!(h.controllers[0].usage().retained_s3, 0);
        drop(lookup);
        let mut lookup = h
            .lookup(
                h.receipt_request(1, b"once", Durability::Quorum)?,
                Duration::from_secs(15),
            )
            .await?;
        let response = lookup
            .execute(runtime, pool, h.fixture.cell_options(&h.cell).await?)
            .await?;
        assert_eq!(response.durability(), Durability::S3);
        assert_eq!(
            response
                .receipt()
                .context("stored S3 minimum")?
                .minimum_durability(),
            Durability::S3
        );
        assert_eq!(response.receipt().unwrap().result(), b"once");
        h.token(&response).await?;
        drop(response);
        drop(lookup);
        if minimum == Durability::Quorum {
            let gateway = h.fixture.transports[0].install_cell_command_gateway(
                h.dispatcher.clone(),
                h.controllers[0].clone(),
                h.fixture.cell_options(&h.cell).await?,
            )?;
            let router = super::super::query_origins::Router::start(&h.fixture).await?;
            router
                .transport
                .install_cell_query_group(h.fixture.tcp_route(h.route.history)?)?;
            router.grant(&h.fixture, 1)?;
            h.fixture.transports[0].grant_command_origin(
                cellar::application::consensus::NamespaceCommandOrigin {
                    cluster: h.route.history.cluster,
                    namespace: h.route.history.namespace,
                    revision: 1,
                    peer: router.peer.clone(),
                },
            )?;
            // The gateway has a valid S3 provider, but remote proof delivery is
            // still guarded when a Quorum request discovers a stored S3 minimum.
            let mut remote = h
                .lookup(
                    h.receipt_request(1, b"once", Durability::Quorum)?,
                    Duration::from_secs(15),
                )
                .await?;
            assert!(matches!(
                remote
                    .execute_remote(&router.transport, MemberId::new(1)?)
                    .await
                    .err(),
                Some(CellDispatchError::Execution(ExecutionError::Refused(
                    CommandFailure::UnsupportedCapability
                )))
            ));
            drop(remote);
            let mut duplicate = h
                .dispatcher
                .command(
                    h.cell.clone(),
                    CellCommandRequest {
                        id: CommandId::new(ClientId::new([188; 16])?, 1)?,
                        contract: h.class.contract("Add").context("Add")?,
                        input: b"once".to_vec(),
                        durability: Durability::Quorum,
                        retry_until_ms: h.retry_until,
                    },
                    CellRoutingLimits {
                        timeout: Duration::from_secs(15),
                        max_attempts: 4,
                    },
                    None,
                )
                .await?;
            duplicate.begin().await?;
            let error = duplicate
                .execute_remote(&router.transport, MemberId::new(1)?)
                .await
                .err()
                .context("remote stored S3 command refused")?;
            assert!(error.ambiguous);
            assert!(matches!(
                error.cause.as_ref(),
                CellDispatchError::Execution(ExecutionError::Refused(
                    CommandFailure::UnsupportedCapability
                ))
            ));
            drop(duplicate);
            assert_eq!(h.controllers[0].usage().retained_s3, 0);
            assert!(h.controllers[0].group_failure(h.route.history)?.is_none());
            drop(gateway);
            router.close().await?;
        }
        let mut missing = h.receipt_request(1, b"missing", Durability::Quorum)?;
        missing.id = CommandId::new(ClientId::new([189; 16])?, 1)?;
        let mut missing = h.lookup(missing, Duration::from_secs(15)).await?;
        let response = missing
            .execute(runtime, pool, h.fixture.cell_options(&h.cell).await?)
            .await?;
        assert!(response.receipt().is_none());
        assert_eq!(response.durability(), minimum);
        drop(response);
        drop(missing);
        assert_eq!(h.calls.load(Ordering::SeqCst), 1);
        h.close().await?;
    }
    Ok(())
}

pub(crate) async fn s3_receipt_failures(rooted: bool, store: Arc<dyn ObjectStore>) -> Result<()> {
    for profile in ["conflict", "expiry", "allocation"] {
        let backend = Arc::new(object_store::prefix::PrefixStore::new(
            store.clone(),
            ObjectPath::from(profile),
        ));
        let mut h = Harness::configured(
            rooted,
            backend,
            1,
            Durability::Quorum,
            (profile == "allocation").then_some(40 * 1024),
        )
        .await?;
        let mut expiry = None;
        if profile == "expiry" {
            h.retry_until = h.now().await? + 2000;
            expiry = Some(h.retry_until);
            h.published_command(1, b"wanted").await?;
            h.retry_until = h.now().await? + 60_000;
            h.published_command(2, b"protected-latest").await?;
        }
        let pool = &h.fixture.appliers[&(0, h.route.history)].0;
        let runtime = &h.fixture.runtimes[0];
        let mut lookup = h
            .lookup(
                h.receipt_request(1, b"wanted", Durability::S3)?,
                Duration::from_secs(15),
            )
            .await?;
        let selection = lookup
            .select(runtime, pool, h.fixture.cell_options(&h.cell).await?)
            .await?;
        assert_eq!(pool.usage()?.retained_operations, 0);
        if profile == "expiry" {
            let expiry = expiry.context("original retry horizon")?;
            controller::wait_for(async || Ok(h.now().await? > expiry)).await?;
            // Only the retry horizon changes; the selected original operation,
            // inputs and deadline stay sealed in the lookup phase.
            h.retry_until = h.now().await? + 60_000;
            // Two retained rows fill this new scenario's ceiling. Maintenance
            // prunes only the expired prefix; the latest receipt stays protected.
            h.published_with_ceiling(3, b"next", Some(2)).await?;
        } else {
            h.published_command(
                1,
                if profile == "conflict" {
                    b"other"
                } else {
                    b"wanted"
                },
            )
            .await?;
        }
        let error = selection
            .response()
            .await
            .err()
            .context("typed archived refusal")?;
        assert!(
            matches!(error, CellDispatchError::Execution(ExecutionError::Refused(failure)) if match profile {
                "conflict" => failure == CommandFailure::CommandConflict,
                "expiry" => failure == CommandFailure::CommandExpired,
                _ => failure == CommandFailure::Overloaded,
            })
        );
        drop(lookup);
        assert_eq!(h.controllers[0].usage().retained_s3, 0);
        assert_eq!(h.controllers[0].usage().s3_bytes, 0);
        assert_eq!(pool.usage()?.retained_operations, 0);
        assert!(h.controllers[0].group_failure(h.route.history)?.is_none());
        if profile == "allocation" {
            // The legitimate 40 KiB result remains readable by Quorum. It must
            // be refused before payload allocation when the 64 KiB S3 budget
            // cannot retain the original and extra read bytes. No quota grows.
            let mut read = h
                .lookup(
                    h.receipt_request(1, b"wanted", Durability::Quorum)?,
                    Duration::from_secs(15),
                )
                .await?;
            let result = read
                .execute(runtime, pool, h.fixture.cell_options(&h.cell).await?)
                .await?;
            assert_eq!(
                result
                    .receipt()
                    .context("intact large result")?
                    .result()
                    .len(),
                40 * 1024
            );
            assert_eq!(result.durability(), Durability::Quorum);
            drop(result);
            drop(read);
        }
        assert_eq!(
            h.calls.load(Ordering::SeqCst),
            if profile == "expiry" { 3 } else { 1 }
        );
        h.close().await?;
    }
    Ok(())
}

pub(crate) async fn s3_receipt_deadline(
    rooted: bool,
    store: Arc<dyn ObjectStore>,
    cancel: bool,
) -> Result<()> {
    let h = Harness::new(rooted, store, 1).await?;
    let pool = &h.fixture.appliers[&(0, h.route.history)].0;
    let runtime = &h.fixture.runtimes[0];
    let request = h.receipt_request(1, b"after", Durability::S3)?;
    let mut lookup = h
        .lookup(request.clone(), Duration::from_millis(800))
        .await?;
    let selection = lookup
        .select(runtime, pool, h.fixture.cell_options(&h.cell).await?)
        .await?;
    h.remote.outage.store(true, Ordering::Release);
    let attempts = h.controllers[0]
        .archive_status(h.route.history)?
        .context("archive registration")?
        .attempts;
    let mut pending = Box::pin(selection.response());
    assert!(futures_util::poll!(pending.as_mut()).is_pending());
    controller::wait_for(async || {
        Ok(h.controllers[0]
            .archive_status(h.route.history)?
            .context("archive status")?
            .attempts
            > attempts
            && h.controllers[0].usage().retained_s3 == 1)
    })
    .await?;
    assert_eq!(pool.usage()?.retained_operations, 0);
    if !cancel {
        assert!(matches!(
            pending.as_mut().await.err(),
            Some(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded))
        ));
    }
    drop(pending);
    drop(lookup);
    controller::wait_for(async || Ok(h.controllers[0].usage().retained_s3 == 0)).await?;
    assert!(h.controllers[0].group_failure(h.route.history)?.is_none());
    let response = h
        .command(1, b"after", Durability::Quorum, Duration::from_secs(15))
        .await?
        .response()
        .await?;
    drop(response);
    h.remote.outage.store(false, Ordering::Release);
    let mut retry = h.lookup(request, Duration::from_secs(15)).await?;
    let response = retry
        .execute(runtime, pool, h.fixture.cell_options(&h.cell).await?)
        .await?;
    assert_eq!(response.durability(), Durability::S3);
    assert_eq!(
        response.receipt().context("original command")?.result(),
        b"after"
    );
    assert_eq!(h.calls.load(Ordering::SeqCst), 1);
    drop(response);
    drop(retry);
    assert_eq!(pool.usage()?.retained_operations, 0);
    h.close().await
}
