// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! SQL output proofs on the actual controller and archive lane.
#[path = "cell_group_archive_dependencies.rs"]
mod dependencies;
pub(crate) use dependencies::archive_dependencies;

#[path = "cell_group_s3_output.rs"]
mod output;
pub(crate) use output::output_during_apply;

#[path = "cell_group_s3_outage.rs"]
mod outage;
pub(crate) use outage::full_backend_outage;

#[path = "cell_group_s3_enrollment.rs"]
mod enrollment;
#[path = "cell_group_s3_receipts.rs"]
mod receipts;
#[path = "cell_group_s3_remote.rs"]
mod remote;
use super::*;
pub(crate) use enrollment::{s3_enrollment, s3_enrollment_deadline};
use futures_util::TryStreamExt;
use object_store::{path::Path as ObjectPath, ObjectStore};
pub(crate) use receipts::{
    s3_receipt_deadline, s3_receipt_failures, s3_receipt_minimums, s3_receipt_observations,
};
pub(crate) use remote::{s3_remote_deadlines, s3_remote_outputs, s3_remote_slow_delivery};
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
    initializations: Arc<AtomicUsize>,
    retry_until: u64,
}

enum Resources {
    SlowMaintenance,
    Retention(CellGroupLogRetentionLimits),
    Storage {
        root: tempfile::TempDir,
        filesystem: Arc<dyn cellar_ltx::FileSystem>,
    },
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
        Self::configured(rooted, store, max_commands, minimum, None).await
    }

    async fn configured(
        rooted: bool,
        store: Arc<dyn object_store::ObjectStore>,
        max_commands: usize,
        minimum: Durability,
        result_size: Option<usize>,
    ) -> Result<Self> {
        Self::configured_transport(rooted, store, max_commands, minimum, result_size, None).await
    }

    async fn configured_transport(
        rooted: bool,
        store: Arc<dyn object_store::ObjectStore>,
        max_commands: usize,
        minimum: Durability,
        result_size: Option<usize>,
        send_buffer: Option<u32>,
    ) -> Result<Self> {
        Box::pin(Self::configured_retention(
            rooted,
            store,
            max_commands,
            minimum,
            result_size,
            send_buffer,
            None,
        ))
        .await
    }

    async fn configured_retention(
        rooted: bool,
        store: Arc<dyn object_store::ObjectStore>,
        max_commands: usize,
        minimum: Durability,
        result_size: Option<usize>,
        send_buffer: Option<u32>,
        retention: Option<CellGroupLogRetentionLimits>,
    ) -> Result<Self> {
        Self::configured_resources(
            rooted,
            store,
            max_commands,
            minimum,
            result_size,
            send_buffer,
            retention.map(Resources::Retention),
        )
        .await
    }

    async fn configured_resources(
        rooted: bool,
        store: Arc<dyn object_store::ObjectStore>,
        max_commands: usize,
        minimum: Durability,
        result_size: Option<usize>,
        send_buffer: Option<u32>,
        resources: Option<Resources>,
    ) -> Result<Self> {
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let initializations = Arc::new(AtomicUsize::new(0));
        let init_count = initializations.clone();
        let class = Arc::new(
            RaftClass::new("Counter", 1, [81; 32])?
                .initializer([83; 32], move |_, transaction| {
                    init_count.fetch_add(1, Ordering::SeqCst);
                    transaction
                        .execute_batch("CREATE TABLE value(n); INSERT INTO value VALUES(7);")?;
                    Ok(())
                })?
                .command(
                    "Add",
                    if result_size.is_some() {
                        [84; 32]
                    } else {
                        [82; 32]
                    },
                    move |context, input| {
                        count.fetch_add(1, Ordering::SeqCst);
                        context.sql().execute("UPDATE value SET n=n+1", [])?;
                        Ok(result_size.map_or_else(|| input.to_vec(), |length| vec![7; length]))
                    },
                )?,
        );
        let mut limits = controller::limits();
        limits.max_commands = max_commands;
        if matches!(resources, Some(Resources::SlowMaintenance)) {
            limits.retry_interval_ms = 60_000;
        }
        let (mut fixture, route, cell, _) = if let Some(Resources::Retention(retention)) = resources
        {
            let filesystem = Arc::new(DirectFileSystem);
            let cluster = Box::pin(Cluster::with_store_birth(
                RaftRuntimeOptions::default(),
                tempfile::tempdir()?,
                filesystem.clone(),
                class.schema_digest(),
                store,
                ClusterBirthOptions {
                    group: GroupId::new([3; 16])?,
                    image_storage: if rooted {
                        RaftImageStorage::Extents
                    } else {
                        RaftImageStorage::Materialized
                    },
                    minimum,
                    log: ClusterLogLimits {
                        retention,
                        ..Default::default()
                    },
                },
            ))
            .await?;
            Box::pin(registered::setup_cluster(rooted, cluster, filesystem)).await?
        } else if let Some(Resources::Storage { root, filesystem }) = resources {
            let cluster = Cluster::with_store_birth(
                RaftRuntimeOptions::default(),
                root,
                filesystem.clone(),
                class.schema_digest(),
                store,
                ClusterBirthOptions {
                    image_storage: if rooted {
                        RaftImageStorage::Extents
                    } else {
                        RaftImageStorage::Materialized
                    },
                    group: GroupId::new([3; 16])?,
                    minimum,
                    log: ClusterLogLimits::default(),
                },
            )
            .await?;
            Box::pin(registered::setup_cluster(rooted, cluster, filesystem)).await?
        } else {
            Box::pin(registered::setup_store_minimum(
                rooted, &class, store, minimum,
            ))
            .await?
        };
        let (archive, remote) =
            archive_controller::registration_minimum(&fixture, &route, rooted, minimum).await?;
        if send_buffer.is_some() {
            fixture.connect_tcp_with_query_buffer(send_buffer).await?;
        }
        let controllers = controller::start_with_limits(&mut fixture, limits).await?;
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
            initializations,
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
        self.command_using(
            sequence,
            input,
            durability,
            timeout,
            self.fixture.cell_options(&self.cell).await?,
        )
        .await
    }

    async fn command_using(
        &self,
        sequence: u64,
        input: &[u8],
        durability: Durability,
        timeout: Duration,
        options: CellSqlOptions,
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
        Ok(self.controllers[0].command(invocation, options)?)
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

pub(crate) async fn s3_replays_from_peer_archive(
    rooted: bool,
    store: Arc<dyn ObjectStore>,
) -> Result<()> {
    let h = Harness::new(rooted, store, 1).await?;
    let gate = h.remote.hold_publication();
    let written = h
        .command(
            1,
            b"peer-archive",
            Durability::Quorum,
            Duration::from_secs(5),
        )
        .await?
        .response()
        .await?;
    let at = written
        .local_receipt()
        .context("original receipt")?
        .position;
    drop(written);
    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified()).await?;
    controller::applied(&h.fixture, h.route.history, at.index).await?;

    let peer = CellGroupArchiveRegistration {
        store: archive_controller::verifier(&h.fixture, &h.route, rooted, "peer-archive").await?,
        ..h.archive.clone()
    };
    h.controllers[1].archive(peer)?.response().await?;
    archive_controller::published(&h.controllers[1], h.route.history, at.index).await?;
    // The peer's full cut is already durable. Disallow new data so the
    // waiting voter has to verify that cut instead of uploading it again.
    h.remote
        .deny_generation_writes
        .store(true, Ordering::Release);
    gate.release.notify_one();
    let replay = h
        .command(1, b"peer-archive", Durability::S3, Duration::from_secs(5))
        .await?
        .response()
        .await?;
    let receipt = replay.local_receipt().context("S3 replay receipt")?;
    assert_eq!(receipt.position, at);
    assert_eq!(receipt.receipt.result(), b"peer-archive");
    assert_eq!(replay.durability(), Durability::S3);
    assert_eq!(h.calls.load(Ordering::Acquire), 1);
    drop(replay);
    h.close().await
}

/// A follower can pin a published prefix while a newer local cut is not archived.
pub(crate) async fn follower_acquires_published_prefix(
    rooted: bool,
    store: Arc<dyn ObjectStore>,
) -> Result<()> {
    let h = Harness::new(rooted, store, 4).await?;
    let first = h
        .command(
            1,
            b"archived-prefix",
            Durability::S3,
            Duration::from_secs(5),
        )
        .await?
        .response()
        .await?;
    let at = first.local_receipt().context("archived receipt")?.position;
    drop(first);
    let gate = h.remote.hold_publication();
    let result = async {
        let next = h
            .command(
                2,
                b"unpublished-tail",
                Durability::Quorum,
                Duration::from_secs(5),
            )
            .await?
            .response()
            .await?;
        let tail = next.local_receipt().context("tail receipt")?.position;
        ensure!(tail.index > at.index);
        drop(next);
        tokio::time::timeout(Duration::from_secs(5), gate.entered.notified()).await?;
        controller::applied(&h.fixture, h.route.history, tail.index).await?;
        ensure!(h.fixture.runtimes[1].status(h.route.history).await?.leader == Some(1));
        // The leader's tail publication is held. An attempted follower upload
        // cannot succeed, but metadata acquisition and physical verification can.
        h.remote
            .deny_generation_writes
            .store(true, Ordering::Release);
        let peer = CellGroupArchiveRegistration {
            publisher: CellGroupArchivePublisher::GroupLeader,
            store: archive_controller::verifier(&h.fixture, &h.route, rooted, "follower-prefix")
                .await?,
            ..h.archive.clone()
        };
        h.controllers[1].archive(peer)?.response().await?;
        archive_controller::published(&h.controllers[1], h.route.history, at.index).await?;
        let prefix = h.controllers[1]
            .archive_status(h.route.history)?
            .and_then(|status| status.published)
            .context("independently verified follower prefix")?;
        ensure!(prefix.position() == at);
        ensure!(h.fixture.runtimes[1].status(h.route.history).await?.leader == Some(1));
        Ok::<_, anyhow::Error>(())
    }
    .await;
    h.remote
        .deny_generation_writes
        .store(false, Ordering::Release);
    gate.release.notify_one();
    let closed = h.close().await;
    result?;
    closed
}

pub(crate) async fn archive_progress_wakes_commands(rooted: bool) -> Result<()> {
    let h = Harness::configured_resources(
        rooted,
        Arc::new(object_store::memory::InMemory::new()),
        1,
        Durability::Quorum,
        None,
        None,
        Some(Resources::SlowMaintenance),
    )
    .await?;
    // Each response must observe its own verified publication. The entire
    // sequence fits inside one maintenance interval without polling archive
    // status or issuing a manual archive request between commands.
    tokio::time::timeout(Duration::from_secs(20), async {
        let mut previous = LogIndex::new(0);
        for sequence in 1..=8u64 {
            let payload = sequence.to_be_bytes();
            let response = h
                .command(sequence, &payload, Durability::S3, Duration::from_secs(20))
                .await?
                .response()
                .await?;
            assert_eq!(response.durability(), Durability::S3);
            let local = response.local_receipt().context("local S3 receipt")?;
            assert_eq!(local.receipt.result(), payload);
            assert!(local.position.index > previous);
            previous = local.position.index;
        }
        let published = h.controllers[0]
            .archive_status(h.route.history)?
            .context("archive registration")?
            .published
            .context("published archive marker")?;
        assert!(published.position().index >= previous);
        assert_eq!(h.calls.load(Ordering::Acquire), 8);
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("S3 completion waited for periodic maintenance")??;
    h.close().await
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
        response
            .local_receipt()
            .expect("local command receipt")
            .receipt
            .minimum_durability(),
        Durability::S3
    );
    assert_eq!(
        response
            .local_receipt()
            .expect("local command receipt")
            .receipt
            .result(),
        b"minimum"
    );
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
    assert_eq!(
        next.local_receipt()
            .expect("local command receipt")
            .receipt
            .result(),
        b"second"
    );
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
    assert_eq!(
        response
            .local_receipt()
            .expect("local command receipt")
            .receipt
            .result(),
        b"first"
    );
    assert_eq!(
        response
            .local_receipt()
            .expect("local command receipt")
            .version
            .sequence
            .get(),
        2
    );
    assert_eq!(h.archive.tails.usage().operations, 1);
    assert_eq!(h.calls.load(Ordering::SeqCst), 2);
    // A covered duplicate shares the already-admitted proof, including the
    // stored S3 minimum. Disallow another upload while both consumers retain it.
    h.remote
        .deny_generation_writes
        .store(true, Ordering::Release);
    let duplicate = h
        .command(1, b"first", Durability::Quorum, Duration::from_secs(15))
        .await?
        .response()
        .await?;
    controller::wait_for(async || Ok(h.controllers[0].usage().retained_s3 == 2)).await?;
    assert_eq!(h.archive.tails.usage().operations, 1);
    assert_eq!(duplicate.durability(), Durability::S3);
    assert_eq!(
        duplicate
            .local_receipt()
            .expect("local command receipt")
            .receipt
            .result(),
        b"first"
    );
    assert_eq!(
        duplicate
            .local_receipt()
            .expect("local command receipt")
            .version
            .sequence
            .get(),
        3
    );
    assert_eq!(h.calls.load(Ordering::SeqCst), 2);
    drop(response);
    controller::wait_for(async || Ok(h.controllers[0].usage().retained_s3 == 1)).await?;
    assert_eq!(h.archive.tails.usage().operations, 1);

    // An uncovered successor cannot borrow that historical proof or bypass the
    // single-operation verification pool. Dropping only the first consumer did
    // not release the shared admission; the final consumer must release it.
    h.remote
        .deny_generation_writes
        .store(false, Ordering::Release);
    let mut successor = Box::pin(
        h.command(3, b"third", Durability::S3, Duration::from_secs(15))
            .await?
            .response(),
    );
    controller::wait_for(async || {
        Ok(h.controllers[0].usage().retained_s3 == 2
            && h.calls.load(Ordering::SeqCst) == 3
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
    let successor = successor.await?;
    assert_eq!(successor.durability(), Durability::S3);
    let receipt = successor.local_receipt().context("successor S3 receipt")?;
    assert_eq!(receipt.receipt.result(), b"third");
    assert_eq!(receipt.version.sequence.get(), 4);
    assert_eq!(h.calls.load(Ordering::SeqCst), 3);
    drop(successor);
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
    assert_eq!(
        next.local_receipt()
            .expect("local command receipt")
            .receipt
            .result(),
        b"second"
    );
    drop(next);
    h.remote.outage.store(false, Ordering::Release);
    let response = h
        .command(1, b"first", Durability::S3, Duration::from_secs(15))
        .await?
        .response()
        .await?;
    assert_eq!(response.durability(), Durability::S3);
    assert_eq!(
        response
            .local_receipt()
            .expect("local command receipt")
            .receipt
            .result(),
        b"first"
    );
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
    let index = result
        .local_receipt()
        .expect("local command receipt")
        .position
        .index;
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
    assert_eq!(
        result
            .local_receipt()
            .expect("local command receipt")
            .receipt
            .result(),
        b"second"
    );
    assert_eq!(h.calls.load(Ordering::SeqCst), 2);
    drop(result);
    h.close().await
}
