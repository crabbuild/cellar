// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Complete-group coverage must include B even when only A is awaiting S3.
use super::*;

struct Base {
    h: Harness,
    registry: CellGroupArchiveRegistry,
    hold: CellGroupArchiveHold,
    dependency: ObjectPath,
    bytes: Vec<u8>,
}

pub(super) fn image_value(pin: CellImagePin, expected: u64) -> Result<()> {
    let bytes = match pin {
        CellImagePin::Empty => anyhow::bail!("expected a physical SQLite image"),
        CellImagePin::Materialized(pin) => {
            let mut reader = pin.open_reader()?;
            let mut bytes = Vec::new();
            while (bytes.len() as u64) < reader.length() {
                let count = (reader.length() - bytes.len() as u64).min(64 * 1024) as usize;
                bytes.extend(reader.read_exact_at(bytes.len() as u64, count)?);
            }
            bytes
        }
        CellImagePin::Extents(pin) => {
            let mut bytes = Vec::new();
            for ordinal in 0..pin.manifest().geometry().extent_count() {
                bytes.extend(
                    pin.view()
                        .read_extent(pin.manifest().extent(ordinal).context("SQLite extent")?)?,
                );
            }
            bytes
        }
    };
    let root = tempfile::tempdir()?;
    let path = root.path().join("value.sqlite");
    std::fs::write(&path, bytes)?;
    let db =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    assert_eq!(
        db.query_row("SELECT n FROM value", [], |r| r.get::<_, u64>(0))?,
        expected
    );
    Ok(())
}

async fn archive_values(h: &Harness, coverage: &CellGroupArchiveCoverage) -> Result<()> {
    let root = coverage.recovery().manifest().clone();
    h.fixture
        .cluster
        .domain
        .blocking(move || -> Result<()> {
            assert_eq!(root.pinned_images(), 2);
            for (name, expected) in [("registered-a", 0), ("registered-b", 1)] {
                let (leaf, pin) = root
                    .pin_cell(&CellId::new(name)?)
                    .context("archived cell")?;
                assert_eq!(leaf.version.sequence.get(), expected + 1);
                image_value(pin, expected)?;
            }
            Ok(())
        })?
        .await?
}

async fn local_values(h: &Harness, index: LogIndex) -> Result<()> {
    controller::applied(&h.fixture, h.route.history, index).await?;
    let b = history(&h.route, "registered-b")?;
    for runtime in &h.fixture.runtimes {
        for (cell, value) in [(&h.cell, 2), (&b, 1)] {
            let selected = runtime
                .reconciliation_authority(h.route.history, cell.clone())
                .await?;
            assert_eq!(selected.cell().version.sequence.get(), value + 1);
            let image = selected.image().clone();
            h.fixture
                .cluster
                .domain
                .blocking(move || image_value(image, value))?
                .await??;
        }
    }
    assert_eq!(h.calls.load(Ordering::SeqCst), 3);
    assert!(h.controllers[0].group_failure(h.route.history)?.is_none());
    Ok(())
}

async fn setup(rooted: bool, store: Arc<dyn ObjectStore>) -> Result<Base> {
    let h = Harness::new(rooted, store, 2).await?;
    // Give B different actual SQLite bytes, so the fault cannot hit a shared
    // empty/header chunk or any part of A's image.
    let b = history(&h.route, "registered-b")?;
    let invocation = h
        .dispatcher
        .command(
            b.clone(),
            CellCommandRequest {
                id: CommandId::new(ClientId::new([189; 16])?, 1)?,
                contract: h.class.contract("Add").context("Add")?,
                input: b"b-base".to_vec(),
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
    let response = h.controllers[0]
        .command(invocation, h.fixture.cell_options(&b).await?)?
        .response()
        .await?;
    assert_eq!(response.durability(), Durability::Quorum);
    assert_eq!(
        response
            .local_receipt()
            .expect("local command receipt")
            .receipt
            .result(),
        b"b-base"
    );
    let index = response
        .local_receipt()
        .expect("local command receipt")
        .position
        .index;
    drop(response);
    archive_controller::published(&h.controllers[0], h.route.history, index).await?;
    let coverage =
        archive_controller::verified_current(&h.fixture, &h.archive, rooted, "dependency-base")
            .await?;
    archive_values(&h, &coverage).await?;
    let marker = coverage.marker().clone();
    // The controller retains its durable hold. Release this completed receiver
    // before asking the same one-slot production pool to archive a new cut.
    drop(coverage);

    // A separate recovery client owns its own bounded two-operation I/O pool,
    // one checkpoint receiver and one tail operation. Its persistent hold is
    // independent of the controller's head retirement.
    let registry = h
        .archive
        .directory
        .open_group_archive(
            &h.fixture.cluster.domain,
            &CellGroupArchivePool::new(Default::default())?,
            &h.archive.birth,
        )
        .await?;
    let intent = registry.prepare_acquire(&h.fixture.cluster.domain).await?;
    let encoded = serde_json::to_vec(&intent)?;
    let path = h
        .fixture
        .cluster
        ._root
        .path()
        .join("dependency-recovery-intent");
    h.fixture
        .cluster
        .domain
        .blocking(move || -> Result<()> {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&path)?;
            file.write_all(&encoded)?;
            file.sync_all()?;
            std::fs::File::open(path.parent().context("intent parent")?)?.sync_all()?;
            Ok(())
        })?
        .await??;
    let hold = registry.acquire(&h.fixture.cluster.domain, &intent).await?;
    assert_eq!(hold.marker(), &marker);

    let objects = h.remote.list(None).try_collect::<Vec<_>>().await?;
    let mut dependency = None;
    for object in objects
        .iter()
        .filter(|o| o.location.as_ref().contains("/indexes/"))
    {
        let value: serde_json::Value =
            serde_json::from_slice(&h.remote.get(&object.location).await?.bytes().await?)?;
        let Some(chunks) = value.get("chunks").and_then(|v| v.as_array()) else {
            continue;
        };
        if serde_json::from_value::<GroupManifest>(value["marker"].clone())? != marker {
            continue;
        }
        let prefix = object
            .location
            .as_ref()
            .split_once("/generations/")
            .context("generation prefix")?
            .0;
        let path = |chunk: &serde_json::Value| -> Result<ObjectPath> {
            let reference = &chunk["object"];
            let digest: Vec<u8> = serde_json::from_value(reference["digest"].clone())?;
            Ok(ObjectPath::from(format!(
                "{prefix}/generations/{}/chunks/{}-{}",
                reference["generation"],
                reference["length"],
                hex::encode(digest)
            )))
        };
        let a = chunks
            .iter()
            .filter(|c| c["part"]["cell"] == "registered-a")
            .map(&path)
            .collect::<Result<Vec<_>>>()?;
        for chunk in chunks.iter().filter(|c| {
            c["part"]["cell"] == "registered-b"
                && c["part"]["kind"] == if rooted { "Extent" } else { "Materialized" }
        }) {
            let candidate = path(chunk)?;
            if !a.contains(&candidate) {
                dependency = Some(candidate);
                break;
            }
        }
        if dependency.is_some() {
            break;
        }
    }
    let dependency = dependency.context("B image bytes outside A's entire image closure")?;
    let bytes = h
        .remote
        .inner
        .get(&dependency)
        .await?
        .bytes()
        .await?
        .to_vec();
    assert!(!bytes.is_empty());
    assert!(dependency
        .as_ref()
        .ends_with(&hex::encode(Sha256::digest(&bytes))));
    assert_eq!(h.calls.load(Ordering::SeqCst), 1);
    Ok(Base {
        h,
        registry,
        hold,
        dependency,
        bytes,
    })
}

async fn selected_stays_before_a(base: &Base) -> Result<()> {
    // Read the actual registry selection, not the advisory controller status.
    // The uncommitted acquire intent below is only an observation.
    let intent = base
        .registry
        .prepare_acquire(&base.h.fixture.cluster.domain)
        .await?;
    let value = serde_json::to_value(intent)?;
    let selected: GroupManifest =
        serde_json::from_value(value["action"]["Acquire"]["root"]["marker"].clone())?;
    assert_eq!(&selected, base.hold.marker());
    Ok(())
}

async fn receiver(
    base: &Base,
    rooted: bool,
    name: &str,
    checkpoints: &CellGroupCheckpointPool,
) -> Result<CellGroupArchiveCheckpointReceiver> {
    let destination =
        archive_controller::verifier(&base.h.fixture, &base.h.route, rooted, name).await?;
    Ok(checkpoints.receive_archive(
        destination,
        ScratchPool::new(ScratchLimits {
            max_bytes: 32 * 1024 * 1024,
            max_files: 1,
        })?,
        base.h.fixture.cluster._root.path().into(),
    )?)
}

async fn exact_local_retry(h: &Harness) -> Result<()> {
    let mut request = cell_attempt(207)?;
    request.request.id = CommandId::new(ClientId::new([188; 16])?, 1)?;
    request.request.operation = "Add".into();
    request.request.contract_digest = [82; 32];
    request.request.request_digest = command_request_digest(b"later-a");
    request.request.durability = Durability::S3;
    request.request.retry_until_ms = h.retry_until;
    let authority = h.fixture.runtimes[0]
        .execution_authority(h.route.history, h.cell.clone())
        .await?;
    let CellSqlOutcome::Receipt(receipt) = h.fixture.appliers[&(0, h.route.history)]
        .0
        .stage_registered(
            h.fixture.cell_options(&h.cell).await?,
            authority,
            request,
            h.class.clone(),
            b"later-a".to_vec(),
            false,
        )
        .await?
    else {
        anyhow::bail!("known command must resolve without callback")
    };
    assert_eq!(receipt.receipt().receipt.result(), b"later-a");
    assert_eq!(
        receipt.receipt().receipt.minimum_durability(),
        Durability::S3
    );
    assert_eq!(h.calls.load(Ordering::SeqCst), 3);
    // This internal receipt carries no archive proof and is never emitted as an
    // S3 response. The stored minimum remains S3, including during repair.
    Ok(())
}

async fn delayed(rooted: bool, store: Arc<dyn ObjectStore>) -> Result<()> {
    let base = Box::pin(setup(rooted, store)).await?;
    let gate = base.h.remote.hold(base.dependency.clone());
    let call = base
        .h
        .command(1, b"later-a", Durability::S3, Duration::from_secs(15))
        .await?;
    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified()).await?;
    base.h.waiting().await?;
    let mut output = Box::pin(call.response());
    assert!(
        tokio::time::timeout(Duration::from_millis(30), output.as_mut())
            .await
            .is_err()
    );

    let next = base
        .h
        .command(
            2,
            b"quorum-successor",
            Durability::Quorum,
            Duration::from_secs(15),
        )
        .await?
        .response()
        .await?;
    assert_eq!(next.durability(), Durability::Quorum);
    assert_eq!(
        next.local_receipt()
            .expect("local command receipt")
            .receipt
            .result(),
        b"quorum-successor"
    );
    let index = next
        .local_receipt()
        .expect("local command receipt")
        .position
        .index;
    assert!(index > base.hold.marker().position().index);
    drop(next);
    local_values(&base.h, index).await?;
    selected_stays_before_a(&base).await?;

    let checkpoints = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    let tails = CellGroupArchiveTailPool::new(Default::default())?;
    let target = receiver(&base, rooted, "dependency-delayed", &checkpoints).await?;
    let mut coverage = Box::pin(base.registry.verify_hold(
        &base.h.fixture.cluster.domain,
        &base.hold,
        &tails,
        target,
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(30), coverage.as_mut())
            .await
            .is_err()
    );
    assert_eq!(checkpoints.usage().sources, 1);
    assert!(
        tokio::time::timeout(Duration::from_millis(30), output.as_mut())
            .await
            .is_err()
    );

    base.h.remote.clear();
    gate.release.notify_waiters();
    let recovered = coverage.await?;
    archive_values(&base.h, &recovered).await?;
    drop(recovered);
    assert_eq!(checkpoints.usage().sources, 0);
    assert_eq!(tails.usage().operations, 0);
    let response = output.await?;
    assert_eq!(response.durability(), Durability::S3);
    assert_eq!(
        response
            .local_receipt()
            .expect("local command receipt")
            .receipt
            .result(),
        b"later-a"
    );
    drop(response);
    archive_controller::published(&base.h.controllers[0], base.h.route.history, index).await?;
    exact_local_retry(&base.h).await?;
    base.registry
        .release(&base.h.fixture.cluster.domain, &base.hold)
        .await?;
    base.h.close().await
}

async fn corrupt(rooted: bool, store: Arc<dyn ObjectStore>) -> Result<()> {
    let base = Box::pin(setup(rooted, store)).await?;
    let mut damaged = base.bytes.clone();
    damaged[0] ^= 1;
    assert_ne!(Sha256::digest(&base.bytes), Sha256::digest(&damaged));
    base.h
        .remote
        .inner
        .put(&base.dependency, damaged.clone().into())
        .await?;
    assert_eq!(
        base.h
            .remote
            .inner
            .get(&base.dependency)
            .await?
            .bytes()
            .await?
            .as_ref(),
        damaged
    );
    let checkpoints = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    let tails = CellGroupArchiveTailPool::new(Default::default())?;
    let target = receiver(&base, rooted, "dependency-corrupt", &checkpoints).await?;
    let error = base
        .registry
        .verify_hold(&base.h.fixture.cluster.domain, &base.hold, &tails, target)
        .await
        .err()
        .context("corrupt B must refuse complete coverage")?;
    assert!(matches!(
        error,
        CellGroupArchiveError::Invalid("object digest/length")
    ));
    assert_eq!(checkpoints.usage().sources, 0);
    assert_eq!(tails.usage().operations, 0);

    let error = base
        .h
        .command(1, b"later-a", Durability::S3, Duration::from_secs(15))
        .await?
        .response()
        .await
        .err()
        .context("corrupt B must refuse A S3 acknowledgement")?;
    assert!(error.ambiguous);
    assert!(matches!(
        error.cause.as_ref(),
        CellDispatchError::Archive(_)
    ));
    let status = base.h.controllers[0]
        .archive_status(base.h.route.history)?
        .context("archive status")?;
    assert!(status.stopped);
    assert!(status
        .error
        .context("corruption failure")?
        .to_string()
        .contains("object digest/length"));
    selected_stays_before_a(&base).await?;
    let next = base
        .h
        .command(
            2,
            b"quorum-successor",
            Durability::Quorum,
            Duration::from_secs(15),
        )
        .await?
        .response()
        .await?;
    assert_eq!(next.durability(), Durability::Quorum);
    let index = next
        .local_receipt()
        .expect("local command receipt")
        .position
        .index;
    assert!(index > base.hold.marker().position().index);
    drop(next);
    local_values(&base.h, index).await?;

    // Repair only the fault-injected object in this isolated fixture. Recovery
    // uses a fresh physical receiver, while the failed archive owner stays stopped.
    base.h
        .remote
        .inner
        .put(&base.dependency, base.bytes.clone().into())
        .await?;
    let target = receiver(&base, rooted, "dependency-repaired", &checkpoints).await?;
    let recovered = base
        .registry
        .verify_hold(&base.h.fixture.cluster.domain, &base.hold, &tails, target)
        .await?;
    archive_values(&base.h, &recovered).await?;
    drop(recovered);
    exact_local_retry(&base.h).await?;
    assert!(
        base.h.controllers[0]
            .archive_status(base.h.route.history)?
            .context("archive status")?
            .stopped
    );
    base.registry
        .release(&base.h.fixture.cluster.domain, &base.hold)
        .await?;
    base.h.close().await
}

#[inline(never)]
pub(crate) fn archive_dependencies(
    rooted: bool,
    store: Arc<dyn ObjectStore>,
) -> std::pin::Pin<Box<impl std::future::Future<Output = Result<()>>>> {
    Box::pin(async move {
        // Each scenario creates real fixed-identity voter disks. Isolate their
        // catalogs and permanent claims instead of trying to reuse that birth.
        let delayed_store = Arc::new(object_store::prefix::PrefixStore::new(
            store.clone(),
            ObjectPath::from("delayed"),
        ));
        let corrupt_store = Arc::new(object_store::prefix::PrefixStore::new(
            store,
            ObjectPath::from("corrupt"),
        ));
        Box::pin(delayed(rooted, delayed_store)).await?;
        Box::pin(corrupt(rooted, corrupt_store)).await
    })
}
