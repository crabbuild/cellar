// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real payload readers must return fitting credit to the original S3 request.
use super::*;
use futures_util::FutureExt;

pub(crate) async fn s3_payload_capacity_release(rooted: bool, background: bool) -> Result<()> {
    let mut root = tempfile::tempdir()?;
    root.disable_cleanup(true);
    eprintln!(
        "retained S3 payload capacity disk: {}",
        root.path().display()
    );
    let pool = CellGroupPayloadPool::new(Default::default())?;
    let h = Harness::configured_resources(
        rooted,
        Arc::new(object_store::memory::InMemory::new()),
        2,
        Durability::Quorum,
        None,
        None,
        Some(Resources::PayloadCapacity {
            root,
            filesystem: Arc::new(DirectFileSystem),
            pool: pool.clone(),
            background,
        }),
    )
    .await?;
    let mut held = Vec::new();
    let mut release = None;
    let checked = std::panic::AssertUnwindSafe(async {
        // Construction has already published an initial cut. Every following
        // publication must use that retained archive context's payload bank.
        let initial = h.controllers[0]
            .archive_status(h.route.history)?
            .context("initial archive status")?
            .published
            .context("initial verified cut")?;
        let catalog = Catalog::new(h.remote.clone(), Default::default())?;
        let namespace = h.fixture.cluster.domain.run(catalog.existing_namespace(
            h.archive.directory.namespace_config().clone(),
        )).await??;
        let payloads = namespace.cell_payloads(&pool)?;
        let cell = history(&h.route, "external-payload")?;
        let body = bytes::Bytes::from(vec![93; 4096]);
        let value = h.fixture.cluster.domain.run(payloads.publish(
            &cell, body.clone(), Duration::from_secs(5),
        )).await??;
        let reference = value.reference().clone();
        let objects = h.remote.inner.list(None).try_collect::<Vec<_>>().await?;
        let path = objects.iter().find(|object| {
            object.location.as_ref().contains("/cell-payloads-1/")
                && !object.location.as_ref().contains("/ownership/")
                && object.location.filename().is_some_and(|name| {
                    name.ends_with(&hex::encode(reference.digest()))
                })
        }).context("published external bytes")?.location.clone();
        let mut seeded = false;
        for sequence in 1..=if background { 1 } else { 2 } {
            tokio::time::timeout(Duration::from_secs(5), controller::wait_for(async || {
                Ok(pool.usage().operations == 0)
            })).await.context("prior payload work joined before pressure setup")??;
            let gets = h.remote.payload_gets.load(Ordering::Acquire);
            let gate = h.remote.hold(path.clone());
            release = Some(gate.clone());
            // Keep the ordinary two-operation bank. Both admitted readers
            // reach the real adapter before archive admission is exercised.
            for _ in 0..CellGroupPayloadLimits::default().max_operations {
                let payloads = payloads.clone();
                let cell = cell.clone();
                let reference = reference.clone();
                held.push(h.fixture.cluster.domain.spawn(async move {
                    payloads.get(&cell, &reference, Duration::from_secs(5)).await
                })?);
            }
            tokio::time::timeout(Duration::from_secs(5), controller::wait_for(async || {
                Ok(pool.usage().operations == CellGroupPayloadLimits::default().max_operations
                    && h.remote.payload_gets.load(Ordering::Acquire)
                        >= gets + CellGroupPayloadLimits::default().max_operations)
            })).await.context("real reads occupy ordinary payload admission")??;
            if !seeded {
                let encoded = hex::encode(serde_json::to_vec(&reference)?);
                let image = images::image(None, &format!(
                    "CREATE TABLE __sys_kv(name TEXT PRIMARY KEY,value BLOB,blob_id TEXT,size INTEGER NOT NULL); INSERT INTO __sys_kv VALUES('external',NULL,CAST(X'{encoded}' AS TEXT),{});", body.len(),
                ))?;
                let accepted = h.fixture.runtimes[0].propose(
                    h.route.history,
                    apply::enrollment_bytes(&h.route, &cell, &image, rooted)?,
                ).await?;
                controller::applied(&h.fixture, h.route.history, accepted.position.index).await?;
                seeded = true;
            }
            let before = h.controllers[0].archive_status(h.route.history)?.context("before call")?;
            let call = h.command(
                sequence,
                b"payload-credit",
                if background { Durability::Quorum } else { Durability::S3 },
                Duration::from_secs(5),
            ).await?;
            let mut response = Box::pin(call.response());
            if background {
                let result = response.as_mut().await?;
                assert_eq!(result.result(), b"payload-credit");
                assert_eq!(result.durability(), Durability::Quorum);
                drop(result);
            } else {
                h.waiting().await?;
            }
            tokio::time::timeout(Duration::from_secs(3), controller::wait_for(async || {
                let status = h.controllers[0].archive_status(h.route.history)?.context("blocked status")?;
                assert!(!status.stopped, "payload admission stopped its owner");
                Ok(status.attempts > before.attempts
                    && status.error.as_ref().is_some_and(|error| {
                        matches!(error.as_ref(), CellGroupArchiveControlError::Archive(
                            CellGroupArchiveError::Payload(CellGroupPayloadError::Overloaded)
                        )) && before.error.as_ref().is_none_or(|previous| !Arc::ptr_eq(previous, error))
                    }))
            })).await.context("actual archive payload refusal")??;
            assert_eq!(h.calls.load(Ordering::Acquire), sequence as usize);
            let applied = h.fixture.runtimes[0].status(h.route.history).await?.applied.index;
            let blocked = h.controllers[0].archive_status(h.route.history)?.context("uncovered cut")?;
            assert!(blocked.published.as_ref().is_some_and(|cut| cut.position().index < applied));
            assert!(applied > initial.position().index);
            if !background {
                assert!(tokio::time::timeout(Duration::from_millis(30), response.as_mut()).await.is_err(),
                    "occupied payload admission granted uncovered S3 output");
            }
            let attempts = h.controllers[0].archive_status(h.route.history)?.context("blocked attempts")?.attempts;
            h.remote.clear();
            gate.release_all_reads();
            for read in held.drain(..) {
                let actual = read.await??;
                assert_eq!(actual.as_ref(), body.as_ref());
                drop(actual);
            }
            release = None;
            // The archive may already have acquired a returned slot. Both
            // original readers joined, while the shared ceiling still applies.
            assert!(pool.usage().operations <= CellGroupPayloadLimits::default().max_operations);
            let result = if background {
                tokio::time::sleep(Duration::from_millis(100)).await;
                assert_eq!(h.controllers[0].archive_status(h.route.history)?.context("background cadence")?.attempts,
                    attempts, "payload release bypassed background coalescing");
                h.command(2, b"payload-credit", Durability::S3, Duration::from_secs(5)).await?.response().await?
            } else {
                response.await.context("returned payload credit did not wake original S3 output")?
            };
            assert_eq!(result.result(), b"payload-credit");
            assert_eq!(result.durability(), Durability::S3);
            let position = result.local_receipt().context("exact command receipt")?.position;
            drop(result);
            let coverage = archive_controller::verified_selected(
                &h.fixture, &h.archive, rooted, &format!("payload-credit-audit-{sequence}"),
            ).await?;
            assert_eq!(coverage.marker().position().index, position.index);
            assert!(coverage.recovery().manifest().pin_cell(&cell.cell).is_some());
            assert!(h.remote.payload_gets.load(Ordering::Acquire)
                > gets + CellGroupPayloadLimits::default().max_operations);
            assert_eq!(h.calls.load(Ordering::Acquire), if background { 2 } else { sequence as usize });
            assert!(h.controllers[0].group_failure(h.route.history)?.is_none());
            drop(coverage);
        }
        Ok::<_, anyhow::Error>(())
    }).catch_unwind().await;
    h.remote.clear();
    if let Some(gate) = release {
        gate.release_all_reads();
    }
    let mut joined = Ok(());
    for read in held {
        let result = read
            .await
            .map_err(anyhow::Error::from)
            .and_then(|result| result.map(drop).map_err(anyhow::Error::from));
        if joined.is_ok() {
            joined = result;
        }
    }
    let closed = h.close().await;
    match checked {
        Ok(result) => result?,
        Err(panic) => std::panic::resume_unwind(panic),
    }
    joined?;
    closed
}
