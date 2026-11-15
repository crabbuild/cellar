// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Returned physical credit wakes the original request without another arrival.
use super::*;

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
