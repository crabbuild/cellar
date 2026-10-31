// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Positive command output proofs on the actual controller and archive lane.
use super::*;
use futures_util::TryStreamExt;
use object_store::{path::Path as ObjectPath, ObjectStore};
use std::sync::atomic::{AtomicUsize, Ordering};

struct Harness {
    fixture: RuntimeCluster,
    route: GroupRoute,
    cell: CellHistory,
    controllers: Vec<CellGroupController>,
    dispatcher: CellGroupDispatcher,
    class: Arc<RaftClass>,
    archive: CellGroupArchiveRegistration,
    remote: Arc<archive_controller::archive_store::Store>,
    calls: Arc<AtomicUsize>,
    retry_until: u64,
}

impl Harness {
    async fn new(
        rooted: bool,
        store: Arc<dyn object_store::ObjectStore>,
        max_commands: usize,
    ) -> Result<Self> {
        Self::with_minimum(rooted, store, max_commands, Durability::Quorum).await
    }

    async fn with_minimum(
        rooted: bool,
        store: Arc<dyn object_store::ObjectStore>,
        max_commands: usize,
        minimum: Durability,
    ) -> Result<Self> {
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let class = Arc::new(RaftClass::new("Counter", 1, [81; 32])?.command(
            "Add",
            [82; 32],
            move |context, input| {
                count.fetch_add(1, Ordering::SeqCst);
                context.sql().execute("UPDATE value SET n=n+1", [])?;
                Ok(input.to_vec())
            },
        )?);
        let (mut fixture, route, cell, _) = Box::pin(registered::setup_store_minimum(
            rooted, &class, store, minimum,
        ))
        .await?;
        let (archive, remote) = archive_controller::registration(&fixture, &route, rooted).await?;
        let controllers = controller::start_with_limits(
            &mut fixture,
            CellGroupControllerLimits {
                max_commands,
                ..controller::limits()
            },
        )
        .await?;
        controllers[0].archive(archive.clone())?.response().await?;
        archive_controller::published(
            &controllers[0],
            route.history,
            fixture.runtimes[0]
                .status(route.history)
                .await?
                .applied
                .index,
        )
        .await?;
        let dispatcher = CellGroupDispatcher::new(
            &fixture.cluster.domain,
            fixture.cluster.directory.clone(),
            class.clone(),
            VersionKeys::new(
                VersionSigningKey::new([89; 32], u64::MAX)?,
                vec![],
                Duration::from_secs(30),
            )?,
            CellDispatchLimits {
                max_cached_routes: 32,
                route_cache_bytes: 256 * 1024,
                max_invocations: 8,
                retained_bytes: 1024 * 1024,
                max_input_bytes: 1024,
            },
        )?
        .with_s3(&controllers[0])?;
        let retry_until = fixture
            .cluster
            .domain
            .run(async { cellar::asyncrt::wall_ms() as u64 + 60_000 })
            .await?;
        Ok(Self {
            fixture,
            route,
            cell,
            controllers,
            dispatcher,
            class,
            archive,
            remote,
            calls,
            retry_until,
        })
    }

    async fn command(
        &self,
        sequence: u64,
        input: &[u8],
        durability: Durability,
        timeout: Duration,
    ) -> Result<CellCommandCall> {
        let invocation = self
            .dispatcher
            .command(
                self.cell.clone(),
                CellCommandRequest {
                    id: CommandId::new(ClientId::new([188; 16])?, sequence)?,
                    contract: self.class.contract("Add").context("Add")?,
                    input: input.to_vec(),
                    durability,
                    retry_until_ms: self.retry_until,
                },
                CellRoutingLimits {
                    timeout,
                    max_attempts: 4,
                },
                None,
            )
            .await?;
        Ok(
            self.controllers[0]
                .command(invocation, self.fixture.cell_options(&self.cell).await?)?,
        )
    }

    async fn waiting(&self) -> Result<()> {
        controller::wait_for(async || {
            Ok(self.controllers[0].usage().retained_s3 == 1
                && self.fixture.appliers[&(0, self.route.history)]
                    .0
                    .usage()?
                    .active_cells
                    == 0)
        })
        .await
    }

    async fn close(self) -> Result<()> {
        for controller in &self.controllers {
            controller.close().await?;
        }
        drop(self.dispatcher);
        drop(self.archive);
        self.fixture.close().await
    }
}

pub(crate) async fn s3_namespace_minimum(rooted: bool, store: Arc<dyn ObjectStore>) -> Result<()> {
    let h = Harness::with_minimum(rooted, store, 1, Durability::S3).await?;
    let gate = h.remote.hold_publication();
    let call = h
        .command(1, b"minimum", Durability::Quorum, Duration::from_secs(15))
        .await?;
    tokio::time::timeout(Duration::from_secs(10), gate.entered.notified()).await?;
    let mut waiting = Box::pin(call.response());
    assert!(
        tokio::time::timeout(Duration::from_millis(30), waiting.as_mut())
            .await
            .is_err()
    );
    gate.release.notify_one();
    let response = waiting.await?;
    assert_eq!(response.durability(), Durability::S3);
    assert_eq!(
        response.receipt().receipt.minimum_durability(),
        Durability::S3
    );
    assert_eq!(response.receipt().receipt.result(), b"minimum");
    assert_eq!(h.calls.load(Ordering::SeqCst), 1);
    drop(response);
    h.close().await
}

pub(crate) async fn s3_commands(
    rooted: bool,
    store: Arc<dyn object_store::ObjectStore>,
) -> Result<()> {
    let h = Harness::new(rooted, store, 2).await?;
    // A controller in the same execution domain still cannot lend another
    // runtime's archive authority to this producer.
    let foreign = h.dispatcher.clone().with_s3(&h.controllers[1])?;
    let invocation = foreign
        .command(
            h.cell.clone(),
            CellCommandRequest {
                id: CommandId::new(ClientId::new([188; 16])?, 1)?,
                contract: h.class.contract("Add").context("Add")?,
                input: b"first".to_vec(),
                durability: Durability::S3,
                retry_until_ms: h.retry_until,
            },
            CellRoutingLimits {
                timeout: Duration::from_secs(15),
                max_attempts: 4,
            },
            None,
        )
        .await?;
    let error = h.controllers[0]
        .command(invocation, h.fixture.cell_options(&h.cell).await?)?
        .response()
        .await
        .err()
        .context("foreign S3 runtime refused")?;
    assert!(!error.ambiguous);
    assert!(matches!(
        error.cause.as_ref(),
        CellDispatchError::Runtime(RuntimeError::Invalid(_))
    ));
    assert_eq!(h.calls.load(Ordering::SeqCst), 0);
    h.remote.outage.store(true, Ordering::Release);
    let call = h
        .command(1, b"first", Durability::S3, Duration::from_secs(15))
        .await?;
    h.waiting().await?;
    assert_eq!(
        h.fixture.appliers[&(0, h.route.history)]
            .0
            .usage()?
            .retained_operations,
        0
    );
    // The same cell commits its successor during the first S3 wait. The proof
    // must read the original receipt from that later independently replayed image.
    let next = h
        .command(2, b"second", Durability::Quorum, Duration::from_secs(15))
        .await?
        .response()
        .await?;
    assert_eq!(next.durability(), Durability::Quorum);
    assert_eq!(next.receipt().receipt.result(), b"second");
    drop(next);
    let gate = h.remote.hold_publication();
    h.remote.outage.store(false, Ordering::Release);
    tokio::time::timeout(Duration::from_secs(10), gate.entered.notified()).await?;
    let mut waiting = Box::pin(call.response());
    assert!(
        tokio::time::timeout(Duration::from_millis(30), waiting.as_mut())
            .await
            .is_err()
    );
    gate.release.notify_one();
    let response = waiting.await?;
    assert_eq!(response.durability(), Durability::S3);
    assert_eq!(response.receipt().receipt.result(), b"first");
    assert_eq!(response.receipt().version.sequence.get(), 2);
    assert_eq!(h.archive.tails.usage().operations, 1);
    assert_eq!(h.calls.load(Ordering::SeqCst), 2);
    // A Quorum duplicate still honors the stored S3 minimum. A retained response
    // keeps its physical proof and remote hold until the final consumer drops it.
    let mut duplicate = Box::pin(
        h.command(1, b"first", Durability::Quorum, Duration::from_secs(15))
            .await?
            .response(),
    );
    controller::wait_for(async || Ok(h.controllers[0].usage().retained_s3 == 2)).await?;
    assert!(
        tokio::time::timeout(Duration::from_millis(30), duplicate.as_mut())
            .await
            .is_err()
    );
    drop(response);
    let response = duplicate.await?;
    assert_eq!(response.durability(), Durability::S3);
    assert_eq!(response.receipt().receipt.result(), b"first");
    assert_eq!(response.receipt().version.sequence.get(), 3);
    assert_eq!(h.calls.load(Ordering::SeqCst), 2);
    drop(response);
    controller::wait_for(async || Ok(h.controllers[0].usage().retained_s3 == 0)).await?;
    assert!(h.controllers[0].group_failure(h.route.history)?.is_none());
    h.close().await
}

pub(crate) async fn s3_deadline(
    rooted: bool,
    store: Arc<dyn object_store::ObjectStore>,
    cancel: bool,
) -> Result<()> {
    let h = Harness::new(rooted, store, 1).await?;
    h.remote.outage.store(true, Ordering::Release);
    let call = h
        .command(1, b"first", Durability::S3, Duration::from_millis(800))
        .await?;
    h.waiting().await?;
    if cancel {
        drop(call);
    } else {
        let error = call.response().await.err().context("S3 deadline")?;
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
            .active_cells,
        0
    );
    let next = h
        .command(2, b"second", Durability::Quorum, Duration::from_secs(15))
        .await?
        .response()
        .await?;
    assert_eq!(next.receipt().receipt.result(), b"second");
    drop(next);
    h.remote.outage.store(false, Ordering::Release);
    let response = h
        .command(1, b"first", Durability::S3, Duration::from_secs(15))
        .await?
        .response()
        .await?;
    assert_eq!(response.durability(), Durability::S3);
    assert_eq!(response.receipt().receipt.result(), b"first");
    assert_eq!(h.calls.load(Ordering::SeqCst), 2);
    drop(response);
    h.close().await
}

pub(crate) async fn s3_missing_sibling(rooted: bool, store: Arc<dyn ObjectStore>) -> Result<()> {
    let h = Harness::new(rooted, store, 1).await?;
    let result = h
        .command(1, b"first", Durability::Quorum, Duration::from_secs(15))
        .await?
        .response()
        .await?;
    let index = result.receipt().position.index;
    drop(result);
    archive_controller::published(&h.controllers[0], h.route.history, index).await?;
    let marker = h.controllers[0]
        .archive_status(h.route.history)?
        .context("published archive")?
        .published
        .context("archive marker")?;
    let objects = h.remote.list(None).try_collect::<Vec<_>>().await?;
    let mut missing = None;
    for object in objects
        .iter()
        .filter(|object| object.location.as_ref().contains("/indexes/"))
    {
        let value: serde_json::Value =
            serde_json::from_slice(&h.remote.get(&object.location).await?.bytes().await?)?;
        let Some(chunks) = value.get("chunks").and_then(|v| v.as_array()) else {
            continue;
        };
        let selected: GroupManifest = serde_json::from_value(value["marker"].clone())?;
        if selected != marker {
            continue;
        }
        let prefix = object
            .location
            .as_ref()
            .split_once("/generations/")
            .context("generation prefix")?
            .0;
        let path = |chunk: &serde_json::Value| -> Result<ObjectPath> {
            let object = &chunk["object"];
            let digest: Vec<u8> = serde_json::from_value(object["digest"].clone())?;
            Ok(ObjectPath::from(format!(
                "{prefix}/generations/{}/chunks/{}-{}",
                object["generation"],
                object["length"],
                hex::encode(digest)
            )))
        };
        let a = chunks
            .iter()
            .filter(|chunk| chunk["part"]["cell"] == "registered-a")
            .map(&path)
            .collect::<Result<Vec<_>>>()?;
        for chunk in chunks
            .iter()
            .filter(|chunk| chunk["part"]["cell"] == "registered-b")
        {
            let candidate = path(chunk)?;
            if !a.contains(&candidate) {
                missing = Some(candidate);
                break;
            }
        }
        if missing.is_some() {
            break;
        }
    }
    let missing = missing.context("B dependency outside A closure")?;
    h.remote.inner.delete(&missing).await?;
    // A still has its committed receipt. A cached published marker must not
    // acknowledge it while any other cell's selected dependency is missing.
    let error = h
        .command(1, b"first", Durability::S3, Duration::from_secs(15))
        .await?
        .response()
        .await
        .err()
        .context("incomplete group must refuse S3")?;
    assert!(error.ambiguous);
    assert!(matches!(
        error.cause.as_ref(),
        CellDispatchError::Archive(_)
    ));
    assert!(h.controllers[0].group_failure(h.route.history)?.is_none());
    let result = h
        .command(2, b"second", Durability::Quorum, Duration::from_secs(15))
        .await?
        .response()
        .await?;
    assert_eq!(result.receipt().receipt.result(), b"second");
    assert_eq!(h.calls.load(Ordering::SeqCst), 2);
    drop(result);
    h.close().await
}
