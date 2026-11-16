// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Exact archive intent retries must reread external bytes before coverage.
use super::*;
use futures_util::FutureExt;

#[derive(Clone, Copy, Debug)]
pub(crate) enum PayloadFailure {
    Unavailable,
    Missing,
    Corrupt,
}

fn expected(error: &CellGroupArchiveControlError, failure: PayloadFailure) -> bool {
    matches!(
        (error, failure),
        (
            CellGroupArchiveControlError::Archive(CellGroupArchiveError::Payload(
                CellGroupPayloadError::Storage(object_store::Error::Generic { .. }),
            )),
            PayloadFailure::Unavailable,
        ) | (
            CellGroupArchiveControlError::Archive(CellGroupArchiveError::Payload(
                CellGroupPayloadError::Missing,
            )),
            PayloadFailure::Missing,
        ) | (
            CellGroupArchiveControlError::Archive(CellGroupArchiveError::Payload(
                CellGroupPayloadError::Invalid(_),
            )),
            PayloadFailure::Corrupt,
        )
    )
}

async fn pending(fixture: &RuntimeCluster) -> Result<serde_json::Value> {
    let directory = fixture
        .cluster
        ._root
        .path()
        .join("node-1/log/cell-group-archive-journals");
    fixture
        .cluster
        .domain
        .blocking(move || -> Result<_> {
            let states = std::fs::read_dir(directory)?.collect::<std::io::Result<Vec<_>>>()?;
            assert_eq!(states.len(), 1);
            let state: serde_json::Value =
                serde_json::from_slice(&std::fs::read(states[0].path().join("state"))?)?;
            let pending = state["record"]["pending"].clone();
            assert!(
                pending.get("Reserve").is_some(),
                "missing original exact reserve intent"
            );
            Ok(pending)
        })?
        .await?
}

pub(crate) async fn payload_read_failure(
    rooted: bool,
    backend: Arc<dyn ObjectStore>,
    failure: PayloadFailure,
) -> Result<()> {
    let mut root = tempfile::tempdir()?;
    root.disable_cleanup(true);
    eprintln!(
        "retained archive payload retry disk: {}",
        root.path().display()
    );
    let remote = archive_store::Store::around(backend);
    let filesystem = Arc::new(DirectFileSystem);
    let class = RaftClass::new("Counter", 1, [81; 32])?;
    let cluster = Cluster::with_store_birth(
        Default::default(),
        root,
        filesystem.clone(),
        class.schema_digest(),
        remote.clone(),
        ClusterBirthOptions {
            group: GroupId::new([3; 16])?,
            image_storage: if rooted {
                RaftImageStorage::Extents
            } else {
                RaftImageStorage::Materialized
            },
            minimum: Durability::Quorum,
            log: Default::default(),
        },
    )
    .await?;
    let (mut fixture, route, _, _) =
        Box::pin(registered::setup_cluster(rooted, cluster, filesystem)).await?;
    let (mut config, archive_remote) = registration(&fixture, &route, rooted).await?;
    let pool = CellGroupPayloadPool::new(Default::default())?;
    let catalog = Catalog::new(archive_remote, Default::default())?;
    let namespace = fixture
        .cluster
        .domain
        .run(catalog.existing_namespace(fixture.cluster.directory.namespace_config().clone()))
        .await??;
    let payloads = namespace.cell_payloads(&pool)?;
    let cell = history(&route, "external-payload")?;
    let body = bytes::Bytes::from(vec![93; 4096]);
    let value = fixture
        .cluster
        .domain
        .run(payloads.publish(&cell, body.clone(), Duration::from_secs(5)))
        .await??;
    let reference = value.reference().clone();
    let encoded = hex::encode(serde_json::to_vec(&reference)?);
    let image = images::image(None, &format!(
        "CREATE TABLE __sys_kv(name TEXT PRIMARY KEY,value BLOB,blob_id TEXT,size INTEGER NOT NULL); INSERT INTO __sys_kv VALUES('external',NULL,CAST(X'{encoded}' AS TEXT),{});", body.len(),
    ))?;
    let accepted = fixture.runtimes[0]
        .propose(
            route.history,
            apply::enrollment_bytes(&route, &cell, &image, rooted)?,
        )
        .await?;
    fixture.drive(Some(accepted.position.index)).await?;
    config.io = config.io.with_payloads(pool);
    let objects = remote.inner.list(None).try_collect::<Vec<_>>().await?;
    let path = objects
        .iter()
        .find(|object| {
            object.location.as_ref().contains("/cell-payloads-1/")
                && !object.location.as_ref().contains("/ownership/")
                && object
                    .location
                    .filename()
                    .is_some_and(|name| name.ends_with(&hex::encode(reference.digest())))
        })
        .context("published external bytes")?
        .location
        .clone();
    match failure {
        PayloadFailure::Unavailable => remote.deny_payload_reads.store(true, Ordering::Release),
        PayloadFailure::Missing => remote.inner.delete(&path).await?,
        PayloadFailure::Corrupt => {
            remote
                .inner
                .put(&path, bytes::Bytes::from(vec![0; body.len()]).into())
                .await?;
        }
    }
    let controllers = controller::start_with_limits(&mut fixture, controller::limits()).await?;
    let owner = &controllers[0];
    let checked = std::panic::AssertUnwindSafe(async {
        owner.archive(config.clone())?.response().await?;
        let first = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let status = owner
                    .archive_status(route.history)?
                    .context("archive status")?;
                if status
                    .error
                    .as_ref()
                    .is_some_and(|error| expected(error, failure))
                {
                    break Ok::<_, anyhow::Error>(status);
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .context("archive reached actual external-byte failure")??;
        assert!(
            first.published.is_none(),
            "unverified payload became coverage"
        );
        let original = pending(&fixture).await?;
        if matches!(failure, PayloadFailure::Unavailable) && !first.stopped {
            let denied = remote.denied_payload_reads.load(Ordering::Acquire);
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let status = owner
                        .archive_status(route.history)?
                        .context("retry status")?;
                    assert!(!status.stopped, "payload retry stopped its owner");
                    assert!(
                        status.published.is_none(),
                        "unavailable bytes became coverage"
                    );
                    if status.attempts > first.attempts
                        && remote.denied_payload_reads.load(Ordering::Acquire) > denied
                        && status
                            .error
                            .as_ref()
                            .is_some_and(|error| expected(error, failure))
                    {
                        break Ok::<_, anyhow::Error>(());
                    }
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            })
            .await
            .context("same owner retried the original intent")??;
            assert_eq!(
                pending(&fixture).await?,
                original,
                "payload retry replaced its journaled intent"
            );
            let gets = remote.payload_gets.load(Ordering::Acquire);
            remote.deny_payload_reads.store(false, Ordering::Release);
            published(owner, route.history, accepted.position.index).await?;
            let coverage =
                verified_current(&fixture, &config, rooted, "payload-retry-audit").await?;
            assert_eq!(coverage.marker().position().index, accepted.position.index);
            assert!(coverage
                .recovery()
                .manifest()
                .pin_cell(&cell.cell)
                .is_some());
            assert!(
                remote.payload_gets.load(Ordering::Acquire) > gets,
                "retry skipped actual external bytes"
            );
            let actual = fixture
                .cluster
                .domain
                .run(payloads.get(&cell, &reference, Duration::from_secs(5)))
                .await??;
            assert_eq!(actual.as_ref(), body.as_ref());
            drop(actual);
            drop(coverage);
        }
        Ok::<_, anyhow::Error>(first.stopped)
    })
    .catch_unwind()
    .await;
    remote.deny_payload_reads.store(false, Ordering::Release);
    for controller in &controllers {
        controller.close().await?;
    }
    drop(controllers);
    drop(config);
    fixture.close().await?;
    let stopped = match checked {
        Ok(result) => result?,
        Err(panic) => std::panic::resume_unwind(panic),
    };
    assert_eq!(
        stopped,
        !matches!(failure, PayloadFailure::Unavailable),
        "external-byte error has the wrong terminal classification: {failure:?}"
    );
    Ok(())
}
