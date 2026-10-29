// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Fixed archive coverage through actual quorum cuts and physical SQLite replay.
//! Rooted capacity/permit inputs remain privileged fixture inputs.
#![allow(clippy::disallowed_methods)]
use super::retirement::{discover, reserve};
use super::*;
use crate::storage_fault as scratch_fault;
use cellar::application::consensus::verification::{
    ArchiveError, ArchiveLimits, ArchiveReference, ArchiveVerifier, CellArchive,
    VerifiedArchiveCoverage,
};
use cellar_ltx::generation::{GenerationError, GenerationLimits, GenerationStore, ImageId};
use cellar_ltx::{DirectFileSystem, LtxHost};
use futures_util::TryStreamExt;
use object_store::ObjectStore;

#[path = "raft_evacuation_verification.rs"]
mod evacuation_verification;

struct Started {
    f: Fixture,
    nodes: Vec<Running>,
    cells: Vec<RaftCellHandle>,
    _observer_root: tempfile::TempDir,
    observer_node: cellar::sdk::Node,
    observer: RaftCells,
    proposal: BirthProposal,
    authorization: FleetGroupAuthorization,
    archive: CellArchive,
    _scratch_root: tempfile::TempDir,
    scratch: GenerationStore,
    verifier: ArchiveVerifier,
}

impl Started {
    async fn new() -> Result<Self> {
        Box::pin(Self::with_storage(RaftImageStorage::Materialized)).await
    }

    async fn with_storage(image_storage: RaftImageStorage) -> Result<Self> {
        let f = Box::pin(Fixture::with_storage(
            true,
            true,
            FleetReplicaSeparation::PhysicalHosts,
            image_storage,
        ))
        .await?;
        let nodes = fleet(&f).await?;
        let (observer_root, observer_node, observer) = f.peer_host(7).await?;
        let (namespace, proposal, reservation) = reserved_birth(&f).await?;
        let birth = f
            .domain
            .run(namespace.resolve_managed_birth(&reservation, TIMEOUT))
            .await??;
        let authorization = confirm(&f, &namespace, &proposal, &reservation).await?;
        let cells = registered(&nodes, &authorization).await?;
        activate(&cells, &authorization).await?;
        discover(
            &f,
            &[&nodes[0].host, &nodes[1].host, &nodes[2].host, &observer],
        )
        .await?;
        let archive = f
            .domain
            .run(async {
                namespace.cell_archive(&birth, LtxHost::default(), ArchiveLimits::default())
            })
            .await??;
        let scratch_root = tempfile::tempdir()?;
        let scratch = GenerationStore::open(
            scratch_root.path().join("verify"),
            Arc::new(DirectFileSystem),
            GenerationLimits::default(),
        )?;
        let mut verifier = ArchiveVerifier::new(scratch.clone(), ImageId::new([91; 16])?)?;
        if image_storage == RaftImageStorage::Extents {
            verifier = verifier.with_extent_store(cellar_ltx::internal::NodeExtentStore::open(
                scratch_root.path().join("verify-extents"),
                Arc::new(DirectFileSystem),
                Default::default(),
            )?)?;
        }
        Ok(Self {
            f,
            nodes,
            cells,
            _observer_root: observer_root,
            observer_node,
            observer,
            proposal,
            authorization,
            archive,
            _scratch_root: scratch_root,
            scratch,
            verifier,
        })
    }

    async fn cut(&self) -> Result<FleetPublishedCut> {
        let p = primary(&self.f, &self.cells).await?;
        let revision = self.f.catalog.read(TIMEOUT).await?.revision();
        let observed = self
            .observer
            .observe_peer_fleet_membership(
                self.nodes[p].spec.node,
                self.authorization.clone(),
                revision,
                TIMEOUT,
            )
            .await?;
        Ok(observed
            .published_cut()
            .context("coherent quorum cut")?
            .clone())
    }

    async fn reference(&self, minimum: &FleetPublishedCut) -> Result<ArchiveReference> {
        self.f
            .domain
            .run(cellar::asyncrt::timeout(TIMEOUT, async {
                loop {
                    if let Some(metadata) = self.archive.recovery_metadata().await? {
                        if metadata.position().index >= minimum.applied().index
                            && metadata.membership().revision() == minimum.membership_revision()
                        {
                            return Ok::<_, anyhow::Error>(metadata.reference());
                        }
                    }
                    cellar::asyncrt::sleep(Duration::from_millis(10)).await;
                }
            }))
            .await??
    }

    async fn verify(
        &self,
        reference: &ArchiveReference,
        minimum: &FleetPublishedCut,
    ) -> Result<VerifiedArchiveCoverage> {
        Ok(self
            .f
            .domain
            .run(
                self.archive
                    .verify_published_at(reference, minimum, &self.verifier, TIMEOUT),
            )
            .await??)
    }

    async fn close(self) -> Result<()> {
        ensure!(self.scratch.usage()?.generations == 0);
        ensure!(self.observer.observe_node(TIMEOUT).await?.node.groups == 0);
        self.observer_node.shutdown(ShutdownMode::Preserve).await?;
        stop(self.nodes).await?;
        self.f.domain.shutdown().await;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_archive_coverage_empty_and_data() -> Result<()> {
    let s = Started::new().await?;
    Box::pin(empty_and_data(s)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_extent_archive_coverage_empty_data_and_stale_retry() -> Result<()> {
    let s = Box::pin(Started::with_storage(RaftImageStorage::Extents)).await?;
    Box::pin(empty_and_data(s)).await
}

async fn empty_and_data(s: Started) -> Result<()> {
    let empty = s.cut().await?;
    ensure!(empty.image().is_none());
    let empty_ref = s.reference(&empty).await?;
    let proof = s.verify(&empty_ref, &empty).await?;
    ensure!(proof.image_state().is_none() && proof.covers(&empty)?);
    ensure!(proof.reference() == &empty_ref);
    ensure!(proof.binding() == empty.binding());
    ensure!(proof.birth_digest() == empty.birth_digest());
    ensure!(proof.schema_digest() == empty.schema_digest());
    ensure!(proof.membership_revision() == empty.membership_revision());
    ensure!(s.f.calls.load(Ordering::Acquire) == 0);

    write_read(&s.f, &s.cells, 1, 4, 4).await?;
    let first = s.cut().await?;
    let first_ref = s.reference(&first).await?;
    let first_proof = s.verify(&first_ref, &first).await?;
    ensure!(first_proof.covers(&empty)?);
    ensure!(first_proof.image_state() == Some(first.image().unwrap().state()));
    if s.f.declaration.namespace.image_storage == RaftImageStorage::Materialized {
        ensure!(
            first_proof.image_state().unwrap().materialized()?.digest
                == first.image().unwrap().digest()?
        );
    } else {
        ensure!(matches!(
            first_proof.image_state().unwrap().commitment(),
            cellar::application::consensus::ImageCommitment::Extents(_)
        ));
        ensure!(first.image().unwrap().digest().is_err());
    }
    ensure!(s.verify(&empty_ref, &empty).await?.image_state().is_none());
    ensure!(!proof.covers(&first)?);

    write_read(&s.f, &s.cells, 2, 5, 9).await?;
    let newer = s.cut().await?;
    let newer_ref = s.reference(&newer).await?;
    ensure!(s.verify(&newer_ref, &newer).await?.covers(&first)?);
    ensure!(!first_proof.covers(&newer)?);
    ensure!(s.verify(&first_ref, &first).await?.reference() == &first_ref);
    let stale =
        s.f.domain
            .run(
                s.archive
                    .verify_published_at(&first_ref, &newer, &s.verifier, TIMEOUT),
            )
            .await?;
    ensure!(matches!(stale, Err(ArchiveError::Unconfirmed)));
    ensure!(s.verify(&newer_ref, &newer).await?.covers(&newer)?);
    ensure!(s.f.calls.load(Ordering::Acquire) == 2);
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_archive_coverage_configuration_and_replay_failure() -> Result<()> {
    let s = Started::new().await?;
    write_read(&s.f, &s.cells, 1, 4, 4).await?;
    let before = s.cut().await?;
    let reference = s.reference(&before).await?;
    let proof = s.verify(&reference, &before).await?;
    // Valid bytes and hashes do not bypass physical replay admission. This
    // workspace cannot hold even one page and must never issue a proof.
    let root = tempfile::tempdir()?;
    let small = GenerationStore::open(
        root.path().join("too-small"),
        Arc::new(DirectFileSystem),
        GenerationLimits {
            max_bytes: 1,
            ..Default::default()
        },
    )?;
    let verifier = ArchiveVerifier::new(small, ImageId::new([92; 16])?)?;
    let result =
        s.f.domain
            .run(
                s.archive
                    .verify_published_at(&reference, &before, &verifier, TIMEOUT),
            )
            .await?;
    ensure!(matches!(
        result,
        Err(ArchiveError::Images(GenerationError::Limit))
    ));
    let result =
        s.f.domain
            .run(
                s.archive
                    .verify_published_at(&reference, &before, &verifier, TIMEOUT),
            )
            .await?;
    ensure!(matches!(
        result,
        Err(ArchiveError::Invalid("archive verifier requires recovery"))
    ));

    Box::pin(configuration_and_restore(s, before, reference, proof)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_extent_archive_coverage_preserves_origin_through_configuration_and_restore(
) -> Result<()> {
    let s = Box::pin(Started::with_storage(RaftImageStorage::Extents)).await?;
    write_read(&s.f, &s.cells, 1, 4, 4).await?;
    let before = s.cut().await?;
    let reference = s.reference(&before).await?;
    let proof = s.verify(&reference, &before).await?;
    ensure!(
        proof.image_position()
            == s.f
                .domain
                .run(s.archive.recovery_metadata())
                .await??
                .context("published root metadata")?
                .published_image()?
                .map(|image| image.position())
    );
    Box::pin(configuration_and_restore(s, before, reference, proof)).await
}

async fn configuration_and_restore(
    mut s: Started,
    before: FleetPublishedCut,
    reference: ArchiveReference,
    proof: VerifiedArchiveCoverage,
) -> Result<()> {
    let p = primary(&s.f, &s.cells).await?;
    let (_, permit) = reserve(
        &s.f,
        &s.cells[p],
        &s.proposal.scope,
        1,
        160,
        MemberId::new(((p + 1) % 3) as u64 + 1)?,
        JoiningMember {
            member: MemberId::new(4)?,
            node: NodeId::new([7; 16])?,
            disk: DiskLineage::new([17; 16])?,
        },
    )
    .await?;
    s.cells[p].fence_fleet_permit(permit, TIMEOUT).await?;
    discover(
        &s.f,
        &[
            &s.nodes[0].host,
            &s.nodes[1].host,
            &s.nodes[2].host,
            &s.observer,
        ],
    )
    .await?;
    let after = s.cut().await?;
    ensure!(after.membership_revision() > before.membership_revision());
    ensure!(after.image() == before.image());
    ensure!(matches!(proof.covers(&after), Err(ArchiveError::Conflict)));
    let result =
        s.f.domain
            .run(
                s.archive
                    .verify_published_at(&reference, &after, &s.verifier, TIMEOUT),
            )
            .await?;
    ensure!(matches!(result, Err(ArchiveError::Conflict)));
    let current = s.reference(&after).await?;
    let verified = s.verify(&current, &after).await?;
    ensure!(verified.covers(&after)?);
    // Different producers may retain legitimate publication aliases of the
    // same image. Exact origin preservation is checked at the capture boundary;
    // this quorum comparison must preserve image/configuration coverage.
    let retained = stop(std::mem::take(&mut s.nodes)).await?;
    s.nodes = restart(&s.f, retained, &s.proposal).await?;
    s.cells = handles(&s.nodes, &s.proposal)?;
    discover(
        &s.f,
        &[
            &s.nodes[0].host,
            &s.nodes[1].host,
            &s.nodes[2].host,
            &s.observer,
        ],
    )
    .await?;
    let restored = s.cut().await?;
    ensure!(restored.image().unwrap().position().index >= after.applied().index);
    let reference = s.reference(&restored).await?;
    ensure!(s.verify(&reference, &restored).await?.covers(&after)?);
    ensure!(s.f.calls.load(Ordering::Acquire) == 1);
    s.close().await
}

struct Release(Arc<fault::Store>);

impl Drop for Release {
    fn drop(&mut self) {
        self.0.hold_archive_payloads.store(false, Ordering::Release);
        self.0.payload_window_resume.notify_waiters();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_archive_coverage_selected_reference_and_deadline() -> Result<()> {
    let s = Started::new().await?;
    Box::pin(selected_reference_and_deadline(s)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_extent_archive_coverage_selected_reference_deadline_and_retry() -> Result<()> {
    let s = Box::pin(Started::with_storage(RaftImageStorage::Extents)).await?;
    Box::pin(selected_reference_and_deadline(s)).await
}

async fn selected_reference_and_deadline(mut s: Started) -> Result<()> {
    let _release = Release(s.f.store.clone());
    write_read(&s.f, &s.cells, 1, 4, 4).await?;
    let before = s.cut().await?;
    let reference = s.reference(&before).await?;
    write_read(&s.f, &s.cells, 2, 5, 9).await?;
    let after = s.cut().await?;
    let newer = s.reference(&after).await?;
    ensure!(newer != reference);
    // Join all archive writers before deleting immutable material. The saved
    // cuts/proofs are historical and deliberately confer no current quorum.
    stop(std::mem::take(&mut s.nodes)).await?;
    for timeout in [
        Duration::ZERO,
        Duration::from_nanos(1),
        Duration::from_secs(60) + Duration::from_nanos(1),
    ] {
        let invalid =
            s.f.domain
                .run(
                    s.archive
                        .verify_published_at(&newer, &after, &s.verifier, timeout),
                )
                .await?;
        ensure!(matches!(
            invalid,
            Err(ArchiveError::Invalid("archive verification timeout"))
        ));
    }
    s.f.store
        .hold_archive_payloads
        .store(true, Ordering::Release);
    let result = s
        .f
        .domain
        .run(
            s.archive
                .verify_published_at(&newer, &after, &s.verifier, Duration::from_millis(150)),
        )
        .await?;
    ensure!(matches!(result, Err(ArchiveError::Timeout)));
    s.f.store
        .hold_archive_payloads
        .store(false, Ordering::Release);
    s.f.store.payload_window_resume.notify_waiters();
    ensure!(s.scratch.usage()?.generations == 0);
    ensure!(s.verify(&newer, &after).await?.covers(&after)?);

    s.f.domain
        .run(async {
            let mut pending = Box::pin(s.archive.verify_published_at(
                &newer,
                &after,
                &s.verifier,
                Duration::from_millis(150),
            ));
            ensure!(futures_util::poll!(pending.as_mut()).is_pending());
            cellar::asyncrt::sleep(Duration::from_millis(200)).await;
            ensure!(matches!(pending.await, Err(ArchiveError::Timeout)));
            Ok::<_, anyhow::Error>(())
        })
        .await??;

    let value = serde_json::to_value(&reference)?;
    let digest: [u8; 32] = serde_json::from_value(value["digest"].clone())?;
    let suffix = format!("-{}", hex::encode(digest));
    let objects = s.f.store.inner.list(None).try_collect::<Vec<_>>().await?;
    let selected = objects
        .iter()
        .filter(|object| object.location.as_ref().ends_with(&suffix))
        .collect::<Vec<_>>();
    ensure!(selected.len() == 1);
    s.f.store.inner.delete(&selected[0].location).await?;
    let result =
        s.f.domain
            .run(
                s.archive
                    .verify_published_at(&reference, &before, &s.verifier, TIMEOUT),
            )
            .await?;
    ensure!(matches!(
        result,
        Err(ArchiveError::Corrupt("missing archive object"))
    ));
    ensure!(s.verify(&newer, &after).await?.reference() == &newer);
    ensure!(s.f.calls.load(Ordering::Acquire) == 2);
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_extent_archive_coverage_requires_fresh_complete_remote_closure() -> Result<()> {
    let mut s = Box::pin(Started::with_storage(RaftImageStorage::Extents)).await?;
    write_read(&s.f, &s.cells, 1, 4, 4).await?;
    let minimum = s.cut().await?;
    let reference = s.reference(&minimum).await?;
    stop(std::mem::take(&mut s.nodes)).await?;
    ensure!(s.verify(&reference, &minimum).await?.covers(&minimum)?);
    let objects = s.f.store.inner.list(None).try_collect::<Vec<_>>().await?;
    let mut removed = Vec::new();
    for object in objects
        .iter()
        .filter(|o| o.location.as_ref().contains("/root-extents/"))
    {
        let bytes = s.f.store.inner.get(&object.location).await?.bytes().await?;
        s.f.store.inner.delete(&object.location).await?;
        removed.push((object.location.clone(), bytes));
    }
    ensure!(!removed.is_empty());
    let result =
        s.f.domain
            .run(
                s.archive
                    .verify_published_at(&reference, &minimum, &s.verifier, TIMEOUT),
            )
            .await?;
    ensure!(matches!(
        result,
        Err(ArchiveError::Corrupt("missing root archive extent"))
    ));
    // Restore fixture-owned immutable bytes only after observing refusal. The
    // next attempt must reconcile failed scratch and re-read the full closure.
    for (path, bytes) in removed {
        s.f.store.inner.put(&path, bytes.into()).await?;
    }
    ensure!(s.verify(&reference, &minimum).await?.covers(&minimum)?);
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_extent_archive_cancelled_waiter_keeps_scratch_until_entered_io_exits() -> Result<()>
{
    let mut s = Box::pin(Started::with_storage(RaftImageStorage::Extents)).await?;
    write_read(&s.f, &s.cells, 1, 4, 4).await?;
    let minimum = s.cut().await?;
    let reference = s.reference(&minimum).await?;
    stop(std::mem::take(&mut s.nodes)).await?;
    let gate = scratch_fault::Gate::new(None);
    let inventory = cellar_ltx::internal::NodeExtentStore::open(
        s._scratch_root.path().join("held-verifier"),
        Arc::new(scratch_fault::FaultFileSystem { gate: gate.clone() }),
        Default::default(),
    )?;
    let verifier = ArchiveVerifier::new(s.scratch.clone(), ImageId::new([94; 16])?)?
        .with_extent_store(inventory.clone())?;
    let release = gate.arm(scratch_fault::HOLD_EXTENT_READ);
    s.f.domain.run(async {
        let mut pending = Box::pin(s.archive.verify_published_at(&reference, &minimum, &verifier, TIMEOUT));
        cellar::asyncrt::select! {
            _ = gate.entered.notified() => {},
            result = pending.as_mut() => anyhow::bail!("verification finished before held physical read: {result:?}"),
        }
        drop(pending);
        let result = s.archive.verify_published_at(&reference, &minimum, &verifier, TIMEOUT).await;
        ensure!(matches!(result, Err(ArchiveError::Unconfirmed)), "scratch was reused during entered I/O: {result:?}");
        drop(release);
        cellar::asyncrt::timeout(TIMEOUT, async {
            loop {
                match s.archive.verify_published_at(&reference, &minimum, &verifier, TIMEOUT).await {
                    Ok(proof) => return Ok::<_, anyhow::Error>(proof),
                    Err(ArchiveError::Unconfirmed) => cellar::asyncrt::sleep(Duration::from_millis(10)).await,
                    Err(error) => return Err(error.into()),
                }
            }
        }).await??;
        let usage = inventory.usage()?;
        ensure!(usage.bytes == 0 && usage.roots == 0 && usage.objects == 0 && usage.workspaces == 0 && !usage.busy && !usage.quarantined);
        Ok::<_, anyhow::Error>(())
    }).await??;
    s.close().await
}
