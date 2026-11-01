// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real catalog CAS/archive I/O and voter disks, with a contract-test authority.
//! This is not Docker host fencing or fresh-incarnation serving qualification.
use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[path = "cell_group_disaster_activation.rs"]
mod activation;
#[path = "cell_group_disaster_seed.rs"]
mod seed;
pub(super) use activation::activate_quorum;
pub(super) use seed::seed_engine;

#[derive(Default)]
struct Authority {
    fences: AtomicUsize,
    verifications: AtomicUsize,
    permanently_stopped: AtomicBool,
    lose_response: AtomicBool,
    reject: AtomicBool,
}

impl DisasterFenceAuthority<CellGroupDisasterFenceRequest> for Authority {
    fn fence<'a>(
        &'a self,
        request: &'a CellGroupDisasterFenceRequest,
    ) -> futures_util::future::BoxFuture<'a, Result<Vec<u8>>> {
        Box::pin(async move {
            self.fences.fetch_add(1, Ordering::AcqRel);
            self.permanently_stopped.store(true, Ordering::Release);
            if self.lose_response.swap(false, Ordering::AcqRel) {
                anyhow::bail!("lost authority response after fence");
            }
            Ok(request.digest().to_vec())
        })
    }

    fn verify<'a>(
        &'a self,
        request: &'a CellGroupDisasterFenceRequest,
        evidence: &'a [u8],
    ) -> futures_util::future::BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.verifications.fetch_add(1, Ordering::AcqRel);
            anyhow::ensure!(
                self.permanently_stopped.load(Ordering::Acquire),
                "no external fence"
            );
            anyhow::ensure!(
                !self.reject.load(Ordering::Acquire),
                "independent verification rejected"
            );
            anyhow::ensure!(evidence == request.digest(), "foreign fence evidence");
            Ok(())
        })
    }
}

struct Scenario {
    cluster: Cluster,
    expected: std::collections::BTreeMap<String, Vec<u8>>,
    remote: Arc<archive_store::Store>,
    archives: CellGroupArchivePool,
    tails: CellGroupArchiveTailPool,
    receivers: CellGroupCheckpointPool,
    destination: ArchiveDestination,
    request: CellGroupRecoveryRequest,
    alternate: CellGroupRecoveryRequest,
    competing: DirectoryMutation,
    checkpoint: CellGroupArchiveReference,
    authority: Authority,
    original_head: [u8; 32],
}

impl Scenario {
    async fn new(rooted: bool, store: Arc<dyn object_store::ObjectStore>) -> Result<Self> {
        let remote = archive_store::Store::around(store);
        let mut cluster = Cluster::with_store(
            Default::default(),
            tempfile::tempdir()?,
            Arc::new(DirectFileSystem),
            [6; 32],
            remote.clone(),
        )
        .await?;
        cluster.elect().await?;
        let expected =
            checkpoint_fixture::enroll_images(&mut cluster, rooted, false, 0, 8192).await?;
        let archives = CellGroupArchivePool::new(Default::default())?;
        let binding = cluster
            .directory
            .prepare_group_archive_binding(&cluster.domain, &archives, &cluster.birth)
            .await?;
        let registry = binding.commit(&cluster.domain).await?;
        let owners = CellGroupArchiveUploadPool::new(Default::default())?;
        let tails = CellGroupArchiveTailPool::new(Default::default())?;
        let intent = registry.prepare_upload(&cluster.domain).await?;
        let ticket = registry.reserve(&cluster.domain, &intent).await?;
        let upload = registry
            .open_upload(&cluster.domain, &owners, &ticket, &tails)
            .await?;
        let sources = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
        let checkpoint = upload
            .upload_checkpoint(&cluster.domain, source(&sources, &cluster)?)
            .await?;
        let group = cluster.voters[0]
            .log
            .cell_group(&cluster.birth.route().history)?;
        let uploaded = upload
            .upload_tail(
                &cluster.domain,
                &tails,
                &checkpoint,
                &group,
                cluster.voters[0].owner.as_ref().unwrap().manifest(),
            )
            .await?;
        drop(group);
        let destination = destination(&cluster).await?;
        let receivers = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
        let coverage = upload
            .prepare_publication(
                &cluster.domain,
                &tails,
                &uploaded,
                receiver(&receivers, &destination, &cluster)?,
            )
            .await?
            .commit(&cluster.domain)
            .await?;
        let hold = coverage.hold().clone();
        drop(coverage);
        let route = cluster.birth.route();
        let target = cluster.directory.prepare_group_birth(CellGroupBirthSpec {
            history: GroupHistory {
                group: GroupId::new([70; 16])?,
                incarnation: IncarnationId::new([71; 16])?,
                ..route.history
            },
            range: route.range.clone(),
            epoch: RouteEpoch::new(2)?,
            creation: ProposalId::new([72; 16])?,
            voters: (11..=13)
                .map(|n| InitialMember {
                    member: MemberId::new(n).unwrap(),
                    node: NodeId::new([n as u8 + 100; 16]).unwrap(),
                })
                .collect(),
            learners: vec![],
        })?;
        let view = cluster.domain.run(cluster.directory.observe()).await??;
        let request = cluster.directory.prepare_group_recovery(
            &view,
            ProposalId::new([73; 16])?,
            &cluster.birth,
            &target,
            hold.clone(),
        )?;
        let alternate = cluster.directory.prepare_group_recovery(
            &view,
            ProposalId::new([74; 16])?,
            &cluster.birth,
            &target,
            hold,
        )?;
        let competing_birth = cluster.directory.prepare_group_birth(CellGroupBirthSpec {
            history: GroupHistory {
                group: GroupId::new([80; 16])?,
                incarnation: IncarnationId::new([81; 16])?,
                ..route.history
            },
            range: route.range.clone(),
            epoch: RouteEpoch::new(2)?,
            creation: ProposalId::new([82; 16])?,
            voters: target.voters().to_vec(),
            learners: vec![],
        })?;
        let competing = cluster.directory.prepare_replacement(
            &view,
            ProposalId::new([75; 16])?,
            std::slice::from_ref(route),
            std::slice::from_ref(competing_birth.route()),
        )?;
        cluster
            .domain
            .run(cluster.directory.reserve_group_birth(&competing_birth))
            .await??;
        let saved: Vec<u8> = persist_archive_intent(&cluster, &request.encode().to_vec()).await?;
        assert_eq!(saved, request.encode());
        let decoded = cluster.directory.decode_group_recovery(&saved)?;
        assert_eq!(decoded.digest(), request.digest());
        assert_eq!(decoded.hold(), request.hold());
        Ok(Self {
            cluster,
            expected,
            remote,
            archives,
            tails,
            receivers,
            destination,
            request,
            alternate,
            competing,
            checkpoint,
            authority: Default::default(),
            original_head: view.digest(),
        })
    }

    fn fence<'a>(
        &'a self,
        directory: &'a RangeDirectory,
        request: &'a CellGroupRecoveryRequest,
    ) -> futures_util::future::BoxFuture<'a, Result<FencedCellGroupRecovery, CellGroupRecoveryError>>
    {
        // Repeated by-value recovery futures made the debug caller's polling
        // frame exceed the default test-thread stack before its first poll.
        // Keep the complete admitted operation owned behind this one boundary.
        Box::pin(async move {
            directory
                .fence_group_recovery(
                    &self.cluster.domain,
                    &self.archives,
                    &self.tails,
                    request,
                    receiver(&self.receivers, &self.destination, &self.cluster).unwrap(),
                    &self.authority,
                )
                .await
        })
    }

    async fn reopen(&self) -> Result<RangeDirectory> {
        let route = self.cluster.birth.route();
        let catalog = Catalog::new(self.remote.clone(), Default::default())?;
        let namespace = self
            .cluster
            .domain
            .run(catalog.existing_namespace(NamespaceConfig::new(
                route.history.cluster,
                route.history.namespace,
                cellar_logic::NamespaceMode::Raft,
                route.schema_digest,
            )))
            .await??;
        Ok(RangeDirectory::new(namespace, Default::default())?)
    }

    async fn assert_closed(&self) -> Result<()> {
        let cluster = &self.cluster;
        assert!(matches!(
            cluster
                .domain
                .run(
                    cluster
                        .directory
                        .require_current_group_birth(&cluster.birth)
                )
                .await?,
            Err(CatalogError::Fenced)
        ));
        assert!(cluster
            .domain
            .run(
                cluster
                    .directory
                    .materialize_selected_group(cluster.birth.route())
            )
            .await?
            .is_err());
        assert!(cluster
            .domain
            .run(
                cluster
                    .directory
                    .bootstrap_group_member(&cluster.birth, &cluster.voters[0].log)
            )
            .await?
            .is_err());
        assert!(cluster
            .domain
            .run(
                cluster
                    .directory
                    .materialize_group_birth(self.request.replacement())
            )
            .await?
            .is_err());
        assert!(cluster
            .domain
            .run(cluster.directory.publish(&self.competing))
            .await?
            .is_err());
        let before = self.authority.fences.load(Ordering::Acquire);
        assert!(self
            .fence(&cluster.directory, &self.alternate)
            .await
            .is_err());
        assert_eq!(self.authority.fences.load(Ordering::Acquire), before);
        let route = cluster.birth.route();
        let catalog = Catalog::new(self.remote.clone(), Default::default())?;
        let namespace = cluster
            .domain
            .run(catalog.existing_namespace(NamespaceConfig::new(
                route.history.cluster,
                route.history.namespace,
                cellar_logic::NamespaceMode::Raft,
                route.schema_digest,
            )))
            .await??;
        assert!(matches!(
            cluster
                .domain
                .run(namespace.freeze_births(ProposalId::new([80; 16])?, Duration::from_secs(5)))
                .await?,
            Err(CatalogError::Conflict(
                "disaster namespace cannot enter managed ownership"
            ))
        ));
        Ok(())
    }

    async fn finish(self) -> Result<()> {
        assert_eq!(self.receivers.usage().sources, 0);
        assert_eq!(self.tails.usage().operations, 0);
        self.destination.store.close();
        self.cluster.close().await
    }
}

pub(super) async fn cancel_import(rooted: bool) -> Result<()> {
    let s = Box::new(Scenario::new(rooted, Arc::new(object_store::memory::InMemory::new())).await?);
    let gate = fault::Gate::new(None);
    let target = destination_at(
        &s.cluster,
        rooted,
        Arc::new(fault::FaultFileSystem { gate: gate.clone() }),
        s.cluster._root.path().join("recovery-cancel"),
        s.request.replacement().route().clone(),
    )
    .await?;
    let pool = CellGroupRecoveryPool::new(Default::default())?;
    let scratch = ScratchPool::new(ScratchLimits {
        max_bytes: 32 * 1024 * 1024,
        max_files: 1,
    })?;
    let proof = s.fence(&s.cluster.directory, &s.request).await?;
    let release = gate.arm(fault::HOLD_DEPENDENCY_SYNC);
    let mut work = Box::pin(pool.prepare(
        &s.cluster.domain,
        proof,
        target.store,
        scratch.clone(),
        s.cluster._root.path().into(),
    ));
    let entered = Box::pin(tokio::time::timeout(
        Duration::from_secs(15),
        gate.entered.notified(),
    ));
    match futures_util::future::select(entered, work.as_mut()).await {
        futures_util::future::Either::Left((result, _)) => result?,
        futures_util::future::Either::Right(_) => {
            anyhow::bail!("import skipped physical dependency sync")
        }
    }
    drop(work);
    assert_eq!(pool.usage().operations, 1);
    assert_eq!(s.receivers.usage().sources, 1);
    drop(release);
    let s = *s;
    let receivers = s.receivers.clone();
    s.cluster.close().await?;
    assert_eq!(pool.usage().operations, 0);
    assert_eq!(receivers.usage().sources, 0);
    assert_eq!(scratch.usage()?.reserved_bytes, 0);
    Ok(())
}

pub(super) async fn import(rooted: bool, store: Arc<dyn object_store::ObjectStore>) -> Result<()> {
    let s = Box::new(Scenario::new(rooted, store).await?);
    let route = s.request.replacement().route().clone();
    let target = destination_at(
        &s.cluster,
        rooted,
        Arc::new(DirectFileSystem),
        s.cluster._root.path().join("recovery-import"),
        route.clone(),
    )
    .await?;
    let pool = CellGroupRecoveryPool::new(Default::default())?;
    let scratch = ScratchPool::new(ScratchLimits {
        max_bytes: 32 * 1024 * 1024,
        max_files: 1,
    })?;
    let proof = s.fence(&s.cluster.directory, &s.request).await?;
    let retained_proof = proof.clone();
    let bounded = CellGroupRecoveryPool::new(CellGroupRecoveryLimits {
        max_image_bytes: 512,
        max_restore_bytes: 512,
        ..Default::default()
    })?;
    assert!(matches!(
        bounded
            .prepare(
                &s.cluster.domain,
                proof.clone(),
                target.store.clone(),
                scratch.clone(),
                s.cluster._root.path().into()
            )
            .await,
        Err(CellGroupArchiveError::Limit)
    ));
    assert_eq!(bounded.usage().operations, 0);
    let source = proof.coverage().recovery().manifest().clone();
    let preparation = pool
        .prepare(
            &s.cluster.domain,
            proof,
            target.store.clone(),
            scratch.clone(),
            s.cluster._root.path().into(),
        )
        .await?;
    let marker = preparation.manifest().marker();
    let expected_marker = marker.clone();
    let expected_commitment = preparation.commitment();
    assert_eq!(marker.route(), &route);
    assert_eq!(
        marker.position(),
        LogPosition {
            index: LogIndex::new(1),
            term: Term::new(1)
        }
    );
    assert_eq!(marker.membership_revision(), 1);
    assert_eq!(marker.recovery_request(), Some(s.request.digest()));
    assert!(!marker.is_active());
    assert_ne!(preparation.commitment(), [0; 32]);
    assert_eq!(preparation.request().encode(), s.request.encode());
    assert_eq!(pool.usage().operations, 1);
    assert_eq!(scratch.usage()?.reserved_bytes, 0);
    tail_fixture::check_images(&s.cluster, preparation.manifest(), &s.expected).await?;
    let keys = VersionKeys::new(
        VersionSigningKey::new([91; 32], 100_000)?,
        vec![],
        Duration::from_secs(10),
    )?;
    for name in s.expected.keys() {
        let cell = CellId::new(name)?;
        let (old, old_pin) = source.pin_cell(&cell).context("old leaf")?;
        let (new, new_pin) = preparation.manifest().pin_cell(&cell).context("new leaf")?;
        assert_eq!(new.version.history.incarnation, route.history.incarnation);
        assert_ne!(new.version.history, old.version.history);
        assert_eq!(new.version.sequence.get(), 1);
        assert_eq!(new.enrollment.cell(), &new.version.history);
        assert_eq!(new.enrollment.creation(), s.request.operation());
        assert_eq!(new.enrollment.initial(), new.version.image);
        assert_eq!(new.effects, old.effects);
        assert_eq!(new.geometry, old.geometry);
        assert_eq!(new.witness.group, route.history);
        assert_eq!(new.witness.position, marker.position());
        assert_eq!(new.witness.proposal, s.request.operation());
        assert_eq!(new.last_mutation, marker.position());
        match (old_pin, new_pin) {
            (CellImagePin::Materialized(old), CellImagePin::Materialized(new)) => {
                assert_ne!(old.id(), new.id());
                assert_eq!(old.state(), new.state());
            }
            (CellImagePin::Extents(old), CellImagePin::Extents(new)) => {
                assert_ne!(old.reference(), new.reference());
                assert_ne!(
                    old.manifest().binding().history,
                    new.manifest().binding().history
                );
                for n in 0..old.manifest().geometry().extent_count() {
                    assert_eq!(old.manifest().extent(n), new.manifest().extent(n));
                }
            }
            (CellImagePin::Empty, CellImagePin::Empty) => {}
            _ => anyhow::bail!("recovery changed image representation"),
        }
        let old_token = keys.issue_cell(&old.observation(), route.schema_digest, 10)?;
        assert_eq!(
            keys.verify_cell(&old_token, &new.version.history, route.schema_digest, 11),
            Err(VersionError::WrongHistory)
        );
    }
    // Holding the result owns its admission. Refusal precedes physical work;
    // the replacement still has no local Raft mapping or startup permission.
    assert!(matches!(
        pool.prepare(
            &s.cluster.domain,
            retained_proof,
            target.store.clone(),
            scratch.clone(),
            s.cluster._root.path().into()
        )
        .await,
        Err(CellGroupArchiveError::Busy)
    ));
    for voter in &s.cluster.voters {
        assert!(voter.log.cell_group(&route.history).is_err());
    }
    let retained = preparation.manifest().clone();
    drop(preparation);
    assert_eq!(pool.usage().operations, 1);
    drop(retained);
    assert_eq!(pool.usage().operations, 0);
    // Exact retry produces the same imported genesis/commitment. A wrong route
    // cannot turn the fenced old data owner into a replacement data owner.
    let proof = s.fence(&s.cluster.directory, &s.request).await?;
    assert!(pool
        .prepare(
            &s.cluster.domain,
            proof,
            s.destination.store.clone(),
            scratch.clone(),
            s.cluster._root.path().into()
        )
        .await
        .is_err());
    assert_eq!(pool.usage().operations, 0);
    let proof = s.fence(&s.cluster.directory, &s.request).await?;
    let retried = pool
        .prepare(
            &s.cluster.domain,
            proof,
            target.store,
            scratch,
            s.cluster._root.path().into(),
        )
        .await?;
    tail_fixture::check_images(&s.cluster, retried.manifest(), &s.expected).await?;
    assert!(!retried.manifest().marker().is_active());
    assert_eq!(retried.manifest().marker(), &expected_marker);
    assert_eq!(retried.commitment(), expected_commitment);
    drop(retried);
    drop(source);
    s.assert_closed().await?;
    assert_eq!(pool.usage().operations, 0);
    assert_eq!(s.receivers.usage().sources, 0);
    let s = *s;
    s.cluster.close().await
}

pub(super) async fn resume(rooted: bool, store: Arc<dyn object_store::ObjectStore>) -> Result<()> {
    let s = Box::pin(Scenario::new(rooted, store)).await?;
    let (path, index) = index(s.remote.as_ref(), &s.checkpoint).await?;
    let dependency = cell_objects(&path, &index, "beta")[0].clone();
    let bytes = s.remote.get(&dependency).await?.bytes().await?;
    let mut corrupt = bytes.to_vec();
    corrupt[0] ^= 1;
    s.remote.put(&dependency, corrupt.into()).await?;
    assert!(s.fence(&s.cluster.directory, &s.request).await.is_err());
    assert_eq!(s.authority.fences.load(Ordering::Acquire), 0);
    assert!(!s.authority.permanently_stopped.load(Ordering::Acquire));
    assert_eq!(
        s.cluster
            .domain
            .run(s.cluster.directory.observe())
            .await??
            .digest(),
        s.original_head
    );
    s.cluster
        .domain
        .run(
            s.cluster
                .directory
                .require_current_group_birth(&s.cluster.birth),
        )
        .await??;
    s.remote.put(&dependency, bytes.into()).await?;

    // Both the catalog CAS and the external stop can commit before their
    // responses are lost. Read back the exact lock, then resume that same fence.
    s.authority.lose_response.store(true, Ordering::Release);
    let head = s
        .remote
        .list(None)
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .find(|meta| meta.location.as_ref().ends_with("/cell-groups/head"))
        .context("directory head")?
        .location;
    let gate = s.remote.hold_put(head);
    let mut work = s.fence(&s.cluster.directory, &s.request);
    let entered = Box::pin(tokio::time::timeout(
        Duration::from_secs(15),
        gate.entered.notified(),
    ));
    match futures_util::future::select(entered, work.as_mut()).await {
        futures_util::future::Either::Left((result, _)) => result?,
        futures_util::future::Either::Right(_) => anyhow::bail!("recovery skipped topology lock"),
    }
    s.remote.lose_put_response.store(true, Ordering::Release);
    s.remote.clear_put();
    gate.release.notify_one();
    assert!(matches!(
        work.await,
        Err(CellGroupRecoveryError::Catalog(CatalogError::Fence(_)))
    ));
    assert!(!s.remote.lose_put_response.load(Ordering::Acquire));
    assert!(s.authority.permanently_stopped.load(Ordering::Acquire));
    assert_eq!(s.authority.fences.load(Ordering::Acquire), 1);
    s.assert_closed().await?;

    s.authority.reject.store(true, Ordering::Release);
    assert!(s.fence(&s.cluster.directory, &s.request).await.is_err());
    assert_eq!(s.authority.fences.load(Ordering::Acquire), 2);
    assert_eq!(s.authority.verifications.load(Ordering::Acquire), 1);
    s.assert_closed().await?;
    s.authority.reject.store(false, Ordering::Release);
    let proof = s.fence(&s.cluster.directory, &s.request).await?;
    assert_eq!(proof.request().encode(), s.request.encode());
    assert_eq!(proof.coverage().marker(), s.request.hold().marker());
    tail_fixture::check_images(
        &s.cluster,
        proof.coverage().recovery().manifest(),
        &s.expected,
    )
    .await?;
    for voter in &s.cluster.voters {
        assert_eq!(
            voter.owner.as_ref().unwrap().manifest().marker(),
            proof.coverage().marker()
        );
        assert!(voter
            .log
            .cell_group(&s.request.replacement().route().history)
            .is_err());
    }
    drop(proof);
    assert_eq!(s.authority.fences.load(Ordering::Acquire), 3);
    assert_eq!(s.authority.verifications.load(Ordering::Acquire), 3);
    s.assert_closed().await?;

    // Fresh catalog owner reconstructs intent from durable bytes and rechecks
    // complete physical data and the winning receipt, without calling fence.
    let directory = s.reopen().await?;
    let request = directory.decode_group_recovery(s.request.encode())?;
    s.authority.reject.store(true, Ordering::Release);
    assert!(s.fence(&directory, &request).await.is_err());
    assert_eq!(s.authority.fences.load(Ordering::Acquire), 3);
    s.authority.reject.store(false, Ordering::Release);
    let resumed = s.fence(&directory, &request).await?;
    tail_fixture::check_images(
        &s.cluster,
        resumed.coverage().recovery().manifest(),
        &s.expected,
    )
    .await?;
    assert_eq!(resumed.request().encode(), s.request.encode());
    drop(resumed);
    assert_eq!(s.authority.fences.load(Ordering::Acquire), 3);
    assert_eq!(s.authority.verifications.load(Ordering::Acquire), 5);
    s.finish().await
}

pub(super) async fn topology_wins(
    rooted: bool,
    store: Arc<dyn object_store::ObjectStore>,
) -> Result<()> {
    let s = Box::pin(Scenario::new(rooted, store)).await?;
    let head = s
        .remote
        .list(None)
        .try_collect::<Vec<_>>()
        .await?
        .into_iter()
        .find(|meta| meta.location.as_ref().ends_with("/cell-groups/head"))
        .context("directory head")?
        .location;
    let gate = s.remote.hold_put(head);
    let mut work = Box::pin(s.fence(&s.cluster.directory, &s.request));
    let entered = Box::pin(tokio::time::timeout(
        Duration::from_secs(15),
        gate.entered.notified(),
    ));
    match futures_util::future::select(entered, work.as_mut()).await {
        futures_util::future::Either::Left((result, _)) => result?,
        futures_util::future::Either::Right(_) => anyhow::bail!("recovery skipped topology CAS"),
    }
    s.remote.clear_put();
    s.cluster
        .domain
        .run(s.cluster.directory.publish(&s.competing))
        .await??;
    gate.release.notify_one();
    assert!(work.await.is_err());
    assert_eq!(s.authority.fences.load(Ordering::Acquire), 0);
    assert!(!s.authority.permanently_stopped.load(Ordering::Acquire));
    assert!(s.fence(&s.cluster.directory, &s.request).await.is_err());
    assert_eq!(s.authority.fences.load(Ordering::Acquire), 0);
    s.finish().await
}
