// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Returned physical credit wakes the original request without another arrival.
use super::*;

pub(crate) async fn s3_background_capacity_cadence(
    rooted: bool,
    expired_foreground: bool,
) -> Result<()> {
    let mut h = Harness::configured_resources(
        rooted,
        Arc::new(object_store::memory::InMemory::new()),
        2,
        Durability::Quorum,
        None,
        None,
        Some(Resources::BackgroundCoalescing),
    )
    .await?;
    let work = async {
        let source = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match h.fixture.runtimes[0]
                    .capture_checkpoint(h.route.history, &h.archive.checkpoints)
                    .await
                {
                    Ok(source) => break Ok::<_, anyhow::Error>(source),
                    Err(CellGroupCheckpointError::Busy | CellGroupCheckpointError::NotReady) => {
                        tokio::task::yield_now().await;
                    }
                    Err(error) => break Err(error.into()),
                }
            }
        })
        .await??;
        let call = h
            .command(
                1,
                b"background",
                if expired_foreground {
                    Durability::S3
                } else {
                    Durability::Quorum
                },
                Duration::from_secs(5),
            )
            .await?;
        let minimum_attempts = if expired_foreground {
            let error = call
                .response()
                .await
                .err()
                .context("occupied S3 deadline")?;
            anyhow::ensure!(error.ambiguous);
            anyhow::ensure!(matches!(
                error.cause.as_ref(),
                CellDispatchError::Runtime(RuntimeError::DeadlineExceeded)
            ));
            h.controllers[0]
                .archive_status(h.route.history)?
                .context("expired foreground status")?
                .attempts
                + 1
        } else {
            let result = call.response().await?;
            anyhow::ensure!(result.result() == b"background");
            drop(result);
            0
        };
        tokio::time::timeout(
            Duration::from_secs(3),
            controller::wait_for(async || {
                let status = h.controllers[0]
                    .archive_status(h.route.history)?
                    .context("archive status")?;
                Ok(status.attempts >= minimum_attempts
                    && (!expired_foreground || h.controllers[0].usage().retained_s3 == 0)
                    && status.error.as_deref().is_some_and(|error| {
                        matches!(
                            error,
                            CellGroupArchiveControlError::Archive(
                                CellGroupArchiveError::Checkpoint(CellGroupCheckpointError::Busy)
                            )
                        )
                    }))
            }),
        )
        .await??;
        let attempts = h.controllers[0]
            .archive_status(h.route.history)?
            .context("blocked background status")?
            .attempts;
        // A physical release is not a new foreground S3 request. Background
        // publication keeps its configured 500 ms minimum retry interval.
        drop(source);
        tokio::time::sleep(Duration::from_millis(100)).await;
        anyhow::ensure!(
            h.controllers[0]
                .archive_status(h.route.history)?
                .context("released background status")?
                .attempts
                == attempts,
            "background capacity release bypassed configured maintenance cadence"
        );
        // A new foreground request may bypass that cadence once. It must still
        // acquire real physical credit and return its exact S3-proved result.
        let result = h
            .command(2, b"foreground", Durability::S3, Duration::from_secs(5))
            .await?
            .response()
            .await?;
        anyhow::ensure!(result.result() == b"foreground");
        anyhow::ensure!(result.durability() == Durability::S3);
        anyhow::ensure!(h.calls.load(Ordering::Acquire) == 2);
        anyhow::ensure!(h.controllers[0].group_failure(h.route.history)?.is_none());
        drop(result);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    if work.is_err() {
        h.fixture.cluster._root.disable_cleanup(true);
        eprintln!(
            "retained background capacity disk: {}",
            h.fixture.cluster._root.path().display()
        );
    }
    let closed = h.close().await;
    work?;
    closed
}

pub(crate) async fn s3_capacity_release(
    rooted: bool,
    store: Arc<dyn ObjectStore>,
    checkpoint: bool,
) -> Result<()> {
    let mut h = Harness::configured_resources(
        rooted,
        store,
        2,
        Durability::Quorum,
        None,
        None,
        Some(Resources::SlowMaintenance),
    )
    .await?;
    let work = async {
        // Both physical banks retain their ordinary single slot. The long
        // existing maintenance interval makes a missing credit wake observable;
        // no operation, callback or lease deadline is extended.
        let (held_output, held_checkpoint, sequence) = if checkpoint {
            let source = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    match h.fixture.runtimes[0]
                        .capture_checkpoint(h.route.history, &h.archive.checkpoints)
                        .await
                    {
                        Ok(source) => break Ok::<_, anyhow::Error>(source),
                        Err(
                            CellGroupCheckpointError::Busy | CellGroupCheckpointError::NotReady,
                        ) => {
                            tokio::task::yield_now().await;
                        }
                        Err(error) => break Err(error.into()),
                    }
                }
            })
            .await??;
            anyhow::ensure!(h.archive.checkpoints.usage().sources == 1);
            (None, Some(source), 1)
        } else {
            let first = h
                .command(1, b"held-proof", Durability::S3, Duration::from_secs(5))
                .await?
                .response()
                .await?;
            anyhow::ensure!(first.result() == b"held-proof");
            anyhow::ensure!(first.durability() == Durability::S3);
            anyhow::ensure!(h.archive.tails.usage().operations == 1);
            anyhow::ensure!(h.archive.checkpoints.usage().sources == 1);
            (Some(first), None, 2)
        };
        let next = h
            .command(
                sequence,
                b"after-release",
                Durability::S3,
                Duration::from_secs(5),
            )
            .await?;
        let mut response = Box::pin(next.response());
        tokio::time::timeout(
            Duration::from_secs(3),
            controller::wait_for(async || {
                let status = h.controllers[0]
                    .archive_status(h.route.history)?
                    .context("archive status")?;
                let blocked = status.error.as_deref().is_some_and(|error| {
                    matches!(
                        error,
                        CellGroupArchiveControlError::Archive(CellGroupArchiveError::Checkpoint(
                            CellGroupCheckpointError::Busy
                        ))
                    )
                });
                Ok(blocked
                    && h.calls.load(Ordering::Acquire) == sequence as usize
                    && h.fixture.appliers[&(0, h.route.history)]
                        .0
                        .usage()?
                        .active_cells
                        == 0)
            }),
        )
        .await??;
        anyhow::ensure!(
            tokio::time::timeout(Duration::from_millis(30), response.as_mut())
                .await
                .is_err(),
            "an occupied physical bank cannot acknowledge an uncovered output"
        );
        // Release the last actual owner. No new application request or manual
        // timer advance may be needed to observe this returned capacity.
        drop((held_output, held_checkpoint));
        let result = response
            .await
            .context("returned physical credit did not wake S3 output")?;
        anyhow::ensure!(result.durability() == Durability::S3);
        anyhow::ensure!(result.result() == b"after-release");
        anyhow::ensure!(h.calls.load(Ordering::Acquire) == sequence as usize);
        anyhow::ensure!(h.controllers[0].group_failure(h.route.history)?.is_none());
        drop(result);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    if work.is_err() {
        h.fixture.cluster._root.disable_cleanup(true);
        eprintln!(
            "retained archive capacity disk: {}",
            h.fixture.cluster._root.path().display()
        );
    }
    let closed = h.close().await;
    work?;
    closed
}
