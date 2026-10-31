// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Actual conditional object-store operations, three-voter engines and physical
//! SQLite/LTX verification. Publication is historical coverage, never serving.
use super::*;
use std::collections::BTreeMap;

struct Scenario {
    cluster: Cluster,
    expected: BTreeMap<String, Vec<u8>>,
    remote: Arc<archive_store::Store>,
    registry: CellGroupArchiveRegistry,
    upload: CellGroupArchiveUpload,
    owners: CellGroupArchiveUploadPool,
    tails: CellGroupArchiveTailPool,
    destination: ArchiveDestination,
    receivers: CellGroupCheckpointPool,
    archive_domain: ExecutionDomain,
    gc_domain: ExecutionDomain,
    gc_registry: CellGroupArchiveRegistry,
    gc_owners: CellGroupArchiveUploadPool,
    gc_tails: CellGroupArchiveTailPool,
}

impl Scenario {
    async fn new(rooted: bool) -> Result<Self> {
        Self::with_owner_limits(rooted, Default::default()).await
    }

    async fn with_owner_limits(rooted: bool, limits: CellGroupArchiveUploadLimits) -> Result<Self> {
        let (cluster, expected) =
            checkpoint_fixture::populate_images(rooted, true, false, 1, 8192).await?;
        let remote = archive_store::Store::new();
        let archive = CellGroupArchivePool::new(Default::default())?.for_group(
            remote.clone(),
            Path::from("protected-head"),
            cluster.birth.route().history,
        )?;
        let creation = archive
            .prepare_registry_creation(&cluster.domain, cluster.birth.route().clone())
            .await?;
        let saved = persist_archive_intent(&cluster, creation.intent()).await?;
        assert_eq!(creation.intent(), &saved);
        let registry = creation.commit(&cluster.domain).await?;
        let owners = CellGroupArchiveUploadPool::new(limits)?;
        let tails = CellGroupArchiveTailPool::new(Default::default())?;
        let upload = next_upload(&cluster, &registry, &owners, &tails).await?;
        let destination = destination(&cluster).await?;
        let receivers = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
        let archive_domain = ExecutionDomain::new(tokio::runtime::Handle::current());
        // The collector is a separate node/domain with its own admission pools.
        // Both nodes coordinate exclusively through real conditional registry I/O.
        let gc_domain = ExecutionDomain::new(tokio::runtime::Handle::current());
        let gc_archive = CellGroupArchivePool::new(Default::default())?.for_group(
            remote.clone(),
            Path::from("protected-head"),
            cluster.birth.route().history,
        )?;
        let gc_registry = gc_archive.open_registry(&gc_domain, &saved).await?;
        Ok(Self {
            cluster,
            expected,
            remote,
            registry,
            upload,
            owners,
            tails,
            destination,
            receivers,
            archive_domain,
            gc_domain,
            gc_registry,
            gc_owners: CellGroupArchiveUploadPool::new(Default::default())?,
            gc_tails: CellGroupArchiveTailPool::new(Default::default())?,
        })
    }

    async fn checkpoint(
        &self,
        upload: &CellGroupArchiveUpload,
    ) -> Result<CellGroupArchiveReference> {
        let sources = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
        Ok(upload
            .upload_checkpoint(&self.archive_domain, source(&sources, &self.cluster)?)
            .await?)
    }

    async fn tail(
        &self,
        upload: &CellGroupArchiveUpload,
        checkpoint: &CellGroupArchiveReference,
    ) -> Result<CellGroupArchiveUploadedTail> {
        let group = self.cluster.voters[0]
            .log
            .cell_group(&self.cluster.birth.route().history)?;
        let root = self.cluster.voters[0].owner.as_ref().unwrap().manifest();
        Ok(upload
            .upload_tail(&self.archive_domain, &self.tails, checkpoint, &group, root)
            .await?)
    }

    async fn publish(
        &self,
        upload: &CellGroupArchiveUpload,
        uploaded: &CellGroupArchiveUploadedTail,
    ) -> Result<CellGroupArchiveCoverage> {
        let prepared = upload
            .prepare_publication(
                &self.archive_domain,
                &self.tails,
                uploaded,
                receiver(&self.receivers, &self.destination, &self.cluster)?,
            )
            .await?;
        let saved = persist_archive_intent(&self.cluster, prepared.intent()).await?;
        assert_eq!(prepared.intent(), &saved);
        Ok(prepared.commit(&self.archive_domain).await?)
    }

    async fn collect(&self) -> Result<CellGroupArchiveCollectionReport> {
        Ok(self
            .gc_registry
            .collect(
                &self.gc_domain,
                &self.gc_owners,
                &self.gc_tails,
                Default::default(),
            )
            .await?)
    }

    async fn finish(self) -> Result<()> {
        self.archive_domain.shutdown().await;
        self.gc_domain.shutdown().await;
        self.cluster.close().await
    }
}

async fn next_upload(
    cluster: &Cluster,
    registry: &CellGroupArchiveRegistry,
    owners: &CellGroupArchiveUploadPool,
    tails: &CellGroupArchiveTailPool,
) -> Result<CellGroupArchiveUpload> {
    let intent = registry.prepare_upload(&cluster.domain).await?;
    let saved = persist_archive_intent(cluster, &intent).await?;
    let ticket = registry.reserve(&cluster.domain, &saved).await?;
    Ok(registry
        .open_upload(&cluster.domain, owners, &ticket, tails)
        .await?)
}

async fn control_path(remote: &dyn ObjectStore) -> Result<Path> {
    Ok(remote
        .list(None)
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .find(|m| m.location.as_ref().ends_with("/ownership/registry"))
        .context("registry path")?
        .location)
}

fn object_path(prefix: &str, kind: &str, object: &serde_json::Value) -> Result<Path> {
    let digest: Vec<u8> = serde_json::from_value(object["digest"].clone())?;
    Ok(Path::from(format!(
        "{prefix}/registries/{}/generations/{}/{kind}/{}-{}",
        hex::encode(serde_json::from_value::<Vec<u8>>(
            object["registry"].clone()
        )?),
        object["generation"],
        object["length"],
        hex::encode(digest)
    )))
}

async fn held_root_path(remote: &dyn ObjectStore, hold: &CellGroupArchiveHold) -> Result<Path> {
    let path = control_path(remote).await?;
    let prefix = path
        .as_ref()
        .strip_suffix("/ownership/registry")
        .context("registry namespace")?;
    object_path(
        prefix,
        "roots",
        &serde_json::to_value(hold)?["root"]["object"],
    )
}

pub(super) async fn complete_source_cut(rooted: bool) -> Result<()> {
    let mut s = Scenario::new(rooted).await?;
    let checkpoint = s.checkpoint(&s.upload).await?;
    let (_, original) = index(s.remote.as_ref(), &checkpoint).await?;
    checkpoint_fixture::advance_partition(
        &mut s.cluster,
        "alpha",
        &s.expected["alpha"],
        rooted,
        Some(2),
    )
    .await?;
    s.expected.insert(
        "alpha".into(),
        images::image(Some(&s.expected["alpha"]), "UPDATE value SET n=n+1")?,
    );
    let uploaded = s.tail(&s.upload, &checkpoint).await?;
    let (cp_path, _) = index(s.remote.as_ref(), &checkpoint).await?;
    let dependency = cell_objects(&cp_path, &original, "beta")[0].clone();
    // A reservation protects the whole in-progress generation before any head
    // exists. It cannot justify publishing a missing dependency.
    assert_eq!(s.collect().await?.deleted_objects, 0);
    let gate = s.remote.hold(dependency.clone());
    let mut pending = Box::pin(s.upload.prepare_publication(
        &s.archive_domain,
        &s.tails,
        &uploaded,
        receiver(&s.receivers, &s.destination, &s.cluster)?,
    ));
    let entered = Box::pin(tokio::time::timeout(
        Duration::from_secs(5),
        gate.entered.notified(),
    ));
    match futures_util::future::select(entered, pending.as_mut()).await {
        futures_util::future::Either::Left((result, _)) => result?,
        futures_util::future::Either::Right(_) => {
            anyhow::bail!("publication ended before delayed B")
        }
    }
    assert!(matches!(
        s.registry.prepare_acquire(&s.cluster.domain).await,
        Err(CellGroupArchiveError::Invalid("no selected archive"))
    ));
    // Source authority was captured with the tail, before the source advanced.
    // Another cell can commit without invalidating that immutable committed cut.
    checkpoint_fixture::advance_partition(
        &mut s.cluster,
        "beta",
        &s.expected["beta"],
        rooted,
        Some(2),
    )
    .await?;
    let later = s.cluster.voters[0]
        .owner
        .as_ref()
        .unwrap()
        .manifest()
        .marker()
        .clone();
    s.remote.clear();
    gate.release.notify_one();
    let prepared = pending.await?;
    let intent = persist_archive_intent(&s.cluster, prepared.intent()).await?;
    assert_eq!(prepared.intent(), &intent);
    let coverage = prepared.commit(&s.archive_domain).await?;
    assert!(coverage.marker().position().index < later.position().index);
    tail_fixture::check_images(&s.cluster, coverage.recovery().manifest(), &s.expected).await?;
    let hold = coverage.hold().clone();
    drop(coverage);
    // A later A cut still includes unchanged B. Corrupt B on the actual store,
    // and no new head or remote reference may be created.
    let next = next_upload(&s.cluster, &s.registry, &s.owners, &s.tails).await?;
    let extended = next
        .extend_tail(
            &s.archive_domain,
            &s.tails,
            uploaded.reference(),
            &s.cluster.voters[0]
                .log
                .cell_group(&s.cluster.birth.route().history)?,
            s.cluster.voters[0].owner.as_ref().unwrap().manifest(),
        )
        .await?;
    let before = s
        .remote
        .get(&control_path(s.remote.as_ref()).await?)
        .await?
        .bytes()
        .await?;
    let bytes = s.remote.inner.get(&dependency).await?.bytes().await?;
    let mut corrupt = bytes.to_vec();
    corrupt[0] ^= 0x80;
    s.remote.inner.put(&dependency, corrupt.into()).await?;
    assert!(next
        .prepare_publication(
            &s.archive_domain,
            &s.tails,
            &extended,
            receiver(&s.receivers, &s.destination, &s.cluster)?
        )
        .await
        .is_err());
    let after = s
        .remote
        .get(&control_path(s.remote.as_ref()).await?)
        .await?
        .bytes()
        .await?;
    assert_eq!(before, after);
    let deletes = s.remote.deletes.load(Ordering::Acquire);
    assert!(s.collect().await.is_err());
    assert_eq!(
        s.remote.deletes.load(Ordering::Acquire),
        deletes,
        "a partial mark must issue no DELETE"
    );
    s.remote.inner.put(&dependency, bytes.into()).await?;
    let restored = s
        .registry
        .verify_hold(
            &s.archive_domain,
            &hold,
            &s.tails,
            receiver(&s.receivers, &s.destination, &s.cluster)?,
        )
        .await?;
    tail_fixture::check_images(&s.cluster, restored.recovery().manifest(), &s.expected).await?;
    drop(restored);
    s.registry.abort(&s.archive_domain, &next.ticket()).await?;
    s.finish().await
}

pub(super) async fn holds_and_pending_parents(rooted: bool) -> Result<()> {
    let mut s = Scenario::new(rooted).await?;
    let first = s.checkpoint(&s.upload).await?;
    let first_tail = s.tail(&s.upload, &first).await?;
    let first_coverage = s.publish(&s.upload, &first_tail).await?;
    let first_hold = first_coverage.hold().clone();
    let first_root = held_root_path(s.remote.as_ref(), &first_hold).await?;
    drop(first_coverage);
    let intent = s.registry.prepare_acquire(&s.archive_domain).await?;
    let intent = persist_archive_intent(&s.cluster, &intent).await?;
    let reader = s.registry.acquire(&s.archive_domain, &intent).await?;
    // Keep a child reservation on the old head while a different publisher wins.
    let pending = next_upload(&s.cluster, &s.registry, &s.owners, &s.tails).await?;
    let newer = next_upload(&s.cluster, &s.registry, &s.owners, &s.tails).await?;
    checkpoint_fixture::advance_partition(
        &mut s.cluster,
        "alpha",
        &s.expected["alpha"],
        rooted,
        Some(2),
    )
    .await?;
    s.expected.insert(
        "alpha".into(),
        images::image(Some(&s.expected["alpha"]), "UPDATE value SET n=n+1")?,
    );
    let second = s.checkpoint(&newer).await?;
    let second_tail = s.tail(&newer, &second).await?;
    let second_coverage = s.publish(&newer, &second_tail).await?;
    let second_hold = second_coverage.hold().clone();
    drop(second_coverage);
    s.registry.release(&s.archive_domain, &first_hold).await?;
    s.collect().await?;
    assert!(
        s.remote.get(&first_root).await.is_ok(),
        "an independent reader hold protects the old root"
    );
    s.registry.release(&s.archive_domain, &reader).await?;
    s.collect().await?;
    assert!(
        s.remote.get(&first_root).await.is_ok(),
        "pending child retains its exact parent"
    );
    s.registry
        .abort(&s.archive_domain, &pending.ticket())
        .await?;
    // All old bytes still exist, but the current head's complete checkpoint
    // no longer owns the old checkpoint index. A new publisher cannot resurrect
    // that arbitrary old reference, even after successful physical replay.
    let unprotected = next_upload(&s.cluster, &s.registry, &s.owners, &s.tails).await?;
    let candidate = unprotected
        .extend_tail(
            &s.archive_domain,
            &s.tails,
            first_tail.reference(),
            &s.cluster.voters[0]
                .log
                .cell_group(&s.cluster.birth.route().history)?,
            s.cluster.voters[0].owner.as_ref().unwrap().manifest(),
        )
        .await?;
    let control = control_path(s.remote.as_ref()).await?;
    let before = s.remote.inner.get(&control).await?.bytes().await?;
    assert!(matches!(
        unprotected
            .prepare_publication(
                &s.archive_domain,
                &s.tails,
                &candidate,
                receiver(&s.receivers, &s.destination, &s.cluster)?
            )
            .await,
        Err(CellGroupArchiveError::Invalid(
            "unprotected inherited archive object"
        ))
    ));
    assert_eq!(s.remote.inner.get(&control).await?.bytes().await?, before);
    s.registry
        .abort(&s.archive_domain, &unprotected.ticket())
        .await?;
    drop(candidate);
    drop(unprotected);
    let report = s.collect().await?;
    assert!(report.deleted_objects > 0);
    assert!(matches!(
        s.remote.get(&first_root).await,
        Err(object_store::Error::NotFound { .. })
    ));
    let recovered = s
        .registry
        .verify_hold(
            &s.archive_domain,
            &second_hold,
            &s.tails,
            receiver(&s.receivers, &s.destination, &s.cluster)?,
        )
        .await?;
    tail_fixture::check_images(&s.cluster, recovered.recovery().manifest(), &s.expected).await?;
    drop(recovered);
    // Retired tokens and allocations do not resurrect an owner on retry.
    assert!(s
        .registry
        .acquire(&s.archive_domain, &intent)
        .await
        .is_err());
    assert!(s
        .registry
        .open_upload(&s.archive_domain, &s.owners, &pending.ticket(), &s.tails)
        .await
        .is_err());
    s.finish().await
}

pub(super) async fn uncertain_publication(rooted: bool) -> Result<()> {
    let mut s = Scenario::new(rooted).await?;
    let checkpoint = s.checkpoint(&s.upload).await?;
    let uploaded = s.tail(&s.upload, &checkpoint).await?;
    let prepared = s
        .upload
        .prepare_publication(
            &s.archive_domain,
            &s.tails,
            &uploaded,
            receiver(&s.receivers, &s.destination, &s.cluster)?,
        )
        .await?;
    let saved = persist_archive_intent(&s.cluster, prepared.intent()).await?;
    let path = control_path(s.remote.as_ref()).await?;
    let gate = s
        .remote
        .hold_after(path, s.remote.puts.load(Ordering::Acquire) + 1);
    s.remote.lose_put_response.store(true, Ordering::Release);
    let mut pending = Box::pin(prepared.commit(&s.archive_domain));
    let entered = Box::pin(tokio::time::timeout(
        Duration::from_secs(5),
        gate.entered.notified(),
    ));
    match futures_util::future::select(entered, pending.as_mut()).await {
        futures_util::future::Either::Left((result, _)) => result?,
        futures_util::future::Either::Right(_) => anyhow::bail!("commit ended before readback cut"),
    }
    drop(pending); // The conditional write landed, but the producer has no reply.
    s.remote.clear();
    gate.release.notify_one();
    assert_eq!(s.tails.usage().operations, 0);
    assert_eq!(s.receivers.usage().sources, 0);
    for voter in &mut s.cluster.voters {
        drop(voter.owner.take());
        drop(voter.held.take());
        voter.log.close().await?;
    }
    let puts = s.remote.puts.load(Ordering::Acquire);
    let coverage = s
        .registry
        .reconcile_publication(
            &s.archive_domain,
            &saved,
            &s.tails,
            receiver(&s.receivers, &s.destination, &s.cluster)?,
        )
        .await?;
    tail_fixture::check_images(&s.cluster, coverage.recovery().manifest(), &s.expected).await?;
    assert_eq!(
        s.remote.puts.load(Ordering::Acquire),
        puts,
        "reconciliation must not reallocate or republish"
    );
    assert!(
        s.registry.reserve(&s.archive_domain, &saved).await.is_err(),
        "serialized publication intents are not publishing authority"
    );
    let hold = coverage.hold().clone();
    drop(coverage);
    s.registry.release(&s.archive_domain, &hold).await?;
    assert!(s
        .registry
        .reconcile_publication(
            &s.archive_domain,
            &saved,
            &s.tails,
            receiver(&s.receivers, &s.destination, &s.cluster)?
        )
        .await
        .is_err());
    s.finish().await
}

pub(super) async fn delayed_delete(rooted: bool) -> Result<()> {
    let s = Scenario::new(rooted).await?;
    let first = s.checkpoint(&s.upload).await?;
    let (old_index, old_metadata) = index(s.remote.as_ref(), &first).await?;
    s.registry
        .abort(&s.archive_domain, &s.upload.ticket())
        .await?;
    let gate = s.remote.hold_delete(old_index.clone());
    let mut collection = Box::pin(s.gc_registry.collect(
        &s.gc_domain,
        &s.gc_owners,
        &s.gc_tails,
        Default::default(),
    ));
    let entered = Box::pin(tokio::time::timeout(
        Duration::from_secs(5),
        gate.entered.notified(),
    ));
    match futures_util::future::select(entered, collection.as_mut()).await {
        futures_util::future::Either::Left((result, _)) => result?,
        futures_util::future::Either::Right(_) => {
            anyhow::bail!("collector ended before held DELETE")
        }
    }
    let next = next_upload(&s.cluster, &s.registry, &s.owners, &s.tails).await?;
    let second = s.checkpoint(&next).await?;
    let (new_index, new_metadata) = index(s.remote.as_ref(), &second).await?;
    assert_ne!(first.generation(), second.generation());
    assert_ne!(old_index, new_index);
    assert_eq!(
        old_metadata["chunks"][0]["object"]["digest"],
        new_metadata["chunks"][0]["object"]["digest"]
    );
    assert_ne!(
        old_metadata["chunks"][0]["object"]["generation"],
        new_metadata["chunks"][0]["object"]["generation"]
    );
    let uploaded = s.tail(&next, &second).await?;
    let coverage = s.publish(&next, &uploaded).await?;
    let hold = coverage.hold().clone();
    drop(coverage);
    gate.release.notify_one();
    assert!(collection.await?.deleted_objects > 0);
    assert!(s.remote.get(&old_index).await.is_err());
    assert!(s.remote.get(&new_index).await.is_ok());
    let recovered = s
        .registry
        .verify_hold(
            &s.archive_domain,
            &hold,
            &s.tails,
            receiver(&s.receivers, &s.destination, &s.cluster)?,
        )
        .await?;
    tail_fixture::check_images(&s.cluster, recovered.recovery().manifest(), &s.expected).await?;
    drop(recovered);
    s.finish().await
}

pub(super) async fn registry_identity_and_intents(rooted: bool) -> Result<()> {
    let mut s = Scenario::new(rooted).await?;
    let first = s.registry.prepare_upload(&s.archive_domain).await?;
    let stale = s.registry.prepare_upload(&s.archive_domain).await?;
    assert_ne!(
        first, stale,
        "distinct callers must not share an allocation"
    );
    let saved = persist_archive_intent(&s.cluster, &first).await?;
    let ticket = s.registry.reserve(&s.archive_domain, &saved).await?;
    let puts = s.remote.puts.load(Ordering::Acquire);
    assert_eq!(s.registry.reserve(&s.archive_domain, &saved).await?, ticket);
    assert!(s.registry.reserve(&s.archive_domain, &stale).await.is_err());
    assert_eq!(s.remote.puts.load(Ordering::Acquire), puts);
    s.registry.abort(&s.archive_domain, &ticket).await?;
    let puts = s.remote.puts.load(Ordering::Acquire);
    assert!(s.registry.reserve(&s.archive_domain, &saved).await.is_err());
    assert_eq!(
        s.remote.puts.load(Ordering::Acquire),
        puts,
        "an aborted allocation never reappears"
    );
    let next = s.registry.prepare_upload(&s.archive_domain).await?;
    let saved = persist_archive_intent(&s.cluster, &next).await?;
    let newer = s.registry.reserve(&s.archive_domain, &saved).await?;
    assert!(newer.generation() > ticket.generation());

    // Opening requires the owner's persisted identity. Counter loss refuses and
    // cannot be repaired by creating a new identity in the same namespace.
    let path = control_path(s.remote.as_ref()).await?;
    let identity_path = Path::from(
        path.as_ref()
            .replace("/ownership/registry", "/ownership/identity"),
    );
    let identity = s.remote.inner.get(&identity_path).await?.bytes().await?;
    s.remote.inner.delete(&path).await?;
    assert!(s
        .registry
        .archive()
        .open_registry(&s.archive_domain, s.registry.creation())
        .await
        .is_err());
    let fresh = s
        .registry
        .archive()
        .prepare_registry_creation(&s.archive_domain, s.cluster.birth.route().clone())
        .await?;
    let saved = persist_archive_intent(&s.cluster, fresh.intent()).await?;
    assert_ne!(&saved, s.registry.creation());
    assert!(fresh.commit(&s.archive_domain).await.is_err());
    assert!(s.remote.inner.get(&path).await.is_err());
    assert_eq!(
        s.remote.inner.get(&identity_path).await?.bytes().await?,
        identity
    );
    // The unavailable S3 metadata does not stop the independent local quorum.
    checkpoint_fixture::advance_partition(
        &mut s.cluster,
        "alpha",
        &s.expected["alpha"],
        rooted,
        Some(2),
    )
    .await?;

    // Losing the identity-create response leaves no permission to initialize or
    // reset a registry on restart. A serialized intent is opening metadata only.
    let uncertain = CellGroupArchivePool::new(Default::default())?.for_group(
        s.remote.clone(),
        Path::from("uncertain-initialization"),
        s.cluster.birth.route().history,
    )?;
    let creation = uncertain
        .prepare_registry_creation(&s.archive_domain, s.cluster.birth.route().clone())
        .await?;
    let saved = persist_archive_intent(&s.cluster, creation.intent()).await?;
    s.remote.lose_put_response.store(true, Ordering::Release);
    assert!(matches!(
        creation.commit(&s.archive_domain).await,
        Err(CellGroupArchiveError::Unconfirmed)
    ));
    assert!(uncertain
        .open_registry(&s.archive_domain, &saved)
        .await
        .is_err());
    let controls: Vec<_> = s
        .remote
        .list(Some(&Path::from("uncertain-initialization")))
        .try_collect()
        .await?;
    assert_eq!(controls.len(), 1);
    assert!(controls[0]
        .location
        .as_ref()
        .ends_with("/ownership/identity"));
    s.finish().await
}

pub(super) async fn source_owner_lifetimes(rooted: bool) -> Result<()> {
    let mut s = Scenario::with_owner_limits(
        rooted,
        CellGroupArchiveUploadLimits {
            max_owners: 2,
            ..Default::default()
        },
    )
    .await?;
    assert_eq!(s.owners.usage().operations, 1);
    let checkpoint = s.checkpoint(&s.upload).await?;
    let first = s.tail(&s.upload, &checkpoint).await?;
    assert_eq!(s.owners.usage().operations, 2);
    let retained = first.clone();
    drop(first);
    assert_eq!(
        s.owners.usage().operations,
        2,
        "clones retain the source witness owner"
    );
    assert!(matches!(s.tail(&s.upload, &checkpoint).await,
        Err(error) if error.downcast_ref::<CellGroupArchiveError>().is_some_and(|e| matches!(e, CellGroupArchiveError::Busy))));
    assert_eq!(s.tails.usage().operations, 0);
    checkpoint_fixture::advance_partition(
        &mut s.cluster,
        "alpha",
        &s.expected["alpha"],
        rooted,
        Some(2),
    )
    .await?;
    s.expected.insert(
        "alpha".into(),
        images::image(Some(&s.expected["alpha"]), "UPDATE value SET n=n+1")?,
    );
    let _persistent_locator = retained.into_reference();
    assert_eq!(s.owners.usage().operations, 1);
    let latest = s.tail(&s.upload, &checkpoint).await?;
    assert_eq!(s.owners.usage().operations, 2);
    let coverage = s.publish(&s.upload, &latest).await?;
    tail_fixture::check_images(&s.cluster, coverage.recovery().manifest(), &s.expected).await?;
    drop(coverage);
    drop(latest);
    assert_eq!(s.owners.usage().operations, 1);
    s.finish().await
}
