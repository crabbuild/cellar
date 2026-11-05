// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use cellar_logic::consensus::LogPosition;
use cellar_ltx::{
    internal::NodeExtentStore,
    stream_snapshot::{ScratchLimits, ScratchPool},
};
use sha2::Digest;
use std::sync::atomic::Ordering;

#[path = "raft_catalog_fault.rs"]
#[allow(dead_code)]
mod catalog_fault;

struct Archives {
    pool: CellGroupArchivePool,
    tails: CellGroupArchiveTailPool,
    uploads: CellGroupArchiveUploadPool,
    checkpoints: CellGroupCheckpointPool,
    registry: CellGroupArchiveRegistry,
    rooted: bool,
}

impl Archives {
    async fn new(cluster: &Cluster, rooted: bool) -> Result<Self> {
        let pool = CellGroupArchivePool::new(Default::default())?;
        let registry = cluster
            .directory
            .prepare_group_archive_binding(&cluster.domain, &pool, &cluster.birth)
            .await?
            .commit(&cluster.domain)
            .await?;
        Ok(Self {
            pool,
            registry,
            rooted,
            tails: CellGroupArchiveTailPool::new(Default::default())?,
            uploads: CellGroupArchiveUploadPool::new(Default::default())?,
            checkpoints: CellGroupCheckpointPool::new(checkpoint_fixture::limits())?,
        })
    }

    fn receiver(
        &self,
        cluster: &Cluster,
        label: &str,
    ) -> Result<CellGroupArchiveCheckpointReceiver> {
        let (store, base) = self.store(cluster, cluster.birth.route().clone(), label)?;
        Ok(self.checkpoints.receive_archive(store, scratch()?, base)?)
    }

    fn store(
        &self,
        cluster: &Cluster,
        route: GroupRoute,
        label: &str,
    ) -> Result<(CellGroupArchiveStore, std::path::PathBuf)> {
        let base = cluster._root.path().join(label);
        let filesystem = Arc::new(DirectFileSystem);
        let pages = ManifestPageStore::open(
            &base.join("pages"),
            filesystem.clone(),
            1024,
            16 * 1024 * 1024,
            16,
        )?;
        let images =
            GenerationStore::open(base.join("images"), filesystem.clone(), Default::default())?;
        let extents = self
            .rooted
            .then(|| NodeExtentStore::open(base.join("extents"), filesystem, Default::default()))
            .transpose()?;
        let store = CellGroupArchiveStore::new(
            route,
            pages,
            checkpoint_receive::config(images, extents, &base)?,
            ManifestLimits::default(),
            GroupEntryLimits::default(),
            100,
        )?;
        Ok((store, base))
    }

    async fn publish(&self, fixture: &RuntimeCluster, label: &str) -> Result<CellGroupArchiveHold> {
        self.publish_group(
            fixture,
            &self.registry,
            fixture.cluster.birth.route(),
            label,
        )
        .await
    }

    async fn publish_group(
        &self,
        fixture: &RuntimeCluster,
        registry: &CellGroupArchiveRegistry,
        route: &GroupRoute,
        label: &str,
    ) -> Result<CellGroupArchiveHold> {
        let cluster = &fixture.cluster;
        let intent = registry.prepare_upload(&cluster.domain).await?;
        let ticket = registry.reserve(&cluster.domain, &intent).await?;
        let upload = registry
            .open_upload(&cluster.domain, &self.uploads, &ticket, &self.tails)
            .await?;
        let cut = fixture.runtimes[0]
            .capture_checkpoint(route.history, &self.checkpoints)
            .await?;
        let uploaded = upload.upload_cut(&cluster.domain, &self.tails, cut).await?;
        let (store, base) = self.store(cluster, route.clone(), label)?;
        let receiver = self.checkpoints.receive_archive(store, scratch()?, base)?;
        let verified = upload
            .prepare_publication(&cluster.domain, &self.tails, &uploaded, receiver)
            .await?
            .commit(&cluster.domain)
            .await?;
        Ok(verified.hold().clone())
    }

    fn source(
        &self,
        cluster: &Cluster,
        hold: CellGroupArchiveHold,
        label: &str,
    ) -> Result<CellGroupTransitionSource> {
        Ok(CellGroupTransitionSource {
            birth: cluster.birth.clone(),
            hold,
            receiver: self.receiver(cluster, label)?,
        })
    }
}

fn scratch() -> Result<ScratchPool> {
    Ok(ScratchPool::new(ScratchLimits {
        max_bytes: 32 * 1024 * 1024,
        max_files: 1,
    })?)
}

pub(crate) async fn sealed_source_archives(
    rooted: bool,
    store: Arc<dyn object_store::ObjectStore>,
) -> Result<()> {
    let store = Arc::new(catalog_fault::Store::around(store));
    let mut cluster = Box::pin(Cluster::with_store_birth(
        RaftRuntimeOptions::default(),
        tempfile::tempdir()?,
        Arc::new(DirectFileSystem),
        [6; 32],
        store.clone(),
        ClusterBirthOptions {
            group: GroupId::new([3; 16])?,
            minimum: Durability::Quorum,
            image_storage: if rooted {
                RaftImageStorage::Extents
            } else {
                RaftImageStorage::Materialized
            },
            log: Default::default(),
        },
    ))
    .await?;
    cluster.elect().await?;
    checkpoint_fixture::enroll_images(&mut cluster, rooted, false, 0, 4096).await?;
    let archives = Archives::new(&cluster, rooted).await?;
    let mut fixture = RuntimeCluster::from_cluster(cluster, 256, 256, 64).await?;
    source_seal::attach_source_appliers(&mut fixture)?;
    let scope = fixture.cluster.birth.route().history;
    fixture.runtimes[0].campaign(scope).await?;
    fixture.drive(None).await?;
    let unsealed = archives.publish(&fixture, "verify-unsealed").await?;
    let plan = source_seal::split_plan(&fixture.cluster).await?;
    let directory = fixture.cluster.directory.clone();
    let domain = &fixture.cluster.domain;
    domain.run(directory.plan_transition(&plan)).await??;
    domain
        .run(directory.prepare_transition(&plan, [15; 32]))
        .await??;
    domain
        .run(directory.authorize_transition_seal(&plan))
        .await??;
    let permit = domain
        .run(directory.prepare_source_seal(&plan, scope, &fixture.cluster.voters[0].log))
        .await??;
    let CellGroupSealOutcome::Accepted(seal) = fixture.runtimes[0].seal_source(permit).await?
    else {
        anyhow::bail!("first seal proposal");
    };
    tokio::time::timeout(Duration::from_secs(20), async {
        while !fixture.applied[&scope]
            .iter()
            .all(|index| *index >= seal.position.index)
        {
            fixture.pump(&[], &mut BTreeMap::new()).await?;
            tokio::task::yield_now().await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("replicate archived source seal")??;
    let installed = Box::pin(verify_sealed_sources(
        &fixture,
        &archives,
        store.clone(),
        &plan,
        unsealed,
        seal.position,
    ))
    .await?;
    let (committed, proofs) =
        Box::pin(verify_pending_child_raft(&mut fixture, &plan, installed)).await?;
    Box::pin(verify_installed_decision(
        &fixture,
        &plan,
        store.clone(),
        &committed,
        proofs,
    ))
    .await?;
    Box::pin(verify_transfer_archive(
        &fixture,
        &archives,
        &plan,
        store.clone(),
        &committed,
    ))
    .await?;
    let active = Box::pin(verify_transfer_activation(
        &mut fixture,
        &plan,
        &committed,
        store.clone(),
    ))
    .await?;
    let (retired, acknowledgement) =
        Box::pin(verify_parent_retirement(&mut fixture, &plan)).await?;
    anyhow::ensure!(matches!(
        directory
            .settle_transfer_retirement(
                &fixture.cluster.domain,
                &archives.pool,
                &plan,
                vec![acknowledgement.clone()],
            )
            .await,
        Err(CellGroupArchiveBindingError::Catalog(CatalogError::Fenced))
    ));
    Box::pin(verify_child_archive_coverage(
        &fixture,
        &archives,
        &plan,
        &active,
        store.clone(),
    ))
    .await?;
    Box::pin(verify_retirement_settlement(
        &fixture,
        &archives,
        &plan,
        acknowledgement,
        store,
    ))
    .await?;
    reopen_installed_children(&fixture, &plan, &active, &retired).await?;
    fixture.close().await
}

async fn verify_sealed_sources(
    fixture: &RuntimeCluster,
    archives: &Archives,
    store: Arc<catalog_fault::Store>,
    plan: &DirectoryMutation,
    unsealed: CellGroupArchiveHold,
    seal: LogPosition,
) -> Result<InstalledChildPartitions> {
    let directory = &fixture.cluster.directory;
    let scope = fixture.cluster.birth.route().history;
    let domain = &fixture.cluster.domain;
    let sealed = archives.publish(fixture, "verify-sealed").await?;
    anyhow::ensure!(
        sealed
            .marker()
            .source_seal()
            .context("archived seal")?
            .position()
            == seal
    );
    anyhow::ensure!(matches!(
        directory
            .verify_and_record_sealed_sources(domain, &archives.pool, &archives.tails, plan, vec![])
            .await,
        Err(CellGroupArchiveBindingError::Catalog(
            CatalogError::Invalid(_)
        ))
    ));
    anyhow::ensure!(matches!(
        directory
            .verify_and_record_sealed_sources(
                domain,
                &archives.pool,
                &archives.tails,
                plan,
                vec![archives.source(&fixture.cluster, unsealed, "reject-preseal")?]
            )
            .await,
        Err(CellGroupArchiveBindingError::Catalog(CatalogError::Fenced))
    ));
    let intent = archives.registry.prepare_acquire(domain).await?;
    let released = archives.registry.acquire(domain, &intent).await?;
    archives.registry.release(domain, &released).await?;
    anyhow::ensure!(matches!(
        directory
            .verify_and_record_sealed_sources(
                domain,
                &archives.pool,
                &archives.tails,
                plan,
                vec![archives.source(&fixture.cluster, released, "reject-released")?]
            )
            .await,
        Err(CellGroupArchiveBindingError::Archive(_))
    ));
    let (_, journal) = domain
        .run(directory.active_transition())
        .await??
        .context("authorized source")?;
    anyhow::ensure!(
        journal.phase() == TransitionPhase::SealAuthorized && journal.sealed_sources().is_none()
    );

    store.fail_head_confirmation.store(true, Ordering::Release);
    anyhow::ensure!(matches!(
        directory
            .verify_and_record_sealed_sources(
                domain,
                &archives.pool,
                &archives.tails,
                plan,
                vec![archives.source(&fixture.cluster, sealed.clone(), "verify-before-cas")?]
            )
            .await,
        Err(CellGroupArchiveBindingError::Catalog(
            CatalogError::Unconfirmed
        ))
    ));
    let catalog = Catalog::new(store.clone(), CatalogOptions::default())?;
    let namespace = domain
        .run(catalog.existing_namespace(directory.namespace_config().clone()))
        .await??;
    let reopened = RangeDirectory::new(namespace, DirectoryLimits::default())?;
    let (original, journal) = domain
        .run(reopened.active_transition())
        .await??
        .context("retained source set")?;
    anyhow::ensure!(
        original.encode() == plan.encode() && journal.phase() == TransitionPhase::Sealed
    );
    let mut holds = domain
        .run(reopened.sealed_source_holds(&original))
        .await??;
    anyhow::ensure!(holds == vec![sealed.clone()]);
    let birth = domain
        .run(reopened.materialize_selected_history(scope))
        .await??;
    // The exact verified source retains the only checkpoint receiver slot.
    // Exercise the alternate reference before pinning the selected source.
    let intent = archives.registry.prepare_acquire(domain).await?;
    let alternate = archives.registry.acquire(domain, &intent).await?;
    anyhow::ensure!(alternate != sealed);
    let mut source = archives.source(&fixture.cluster, alternate.clone(), "reject-alternate")?;
    source.birth = birth.clone();
    anyhow::ensure!(matches!(
        reopened
            .verify_and_record_sealed_sources(
                domain,
                &archives.pool,
                &archives.tails,
                &original,
                vec![source]
            )
            .await,
        Err(CellGroupArchiveBindingError::Catalog(
            CatalogError::Conflict(_)
        ))
    ));
    archives.registry.release(domain, &alternate).await?;
    let permit = domain
        .run(reopened.prepare_source_seal(&original, scope, &fixture.cluster.voters[0].log))
        .await??;
    anyhow::ensure!(matches!(
        fixture.runtimes[0].seal_source(permit).await?,
        CellGroupSealOutcome::Applied(_)
    ));
    let mut source = archives.source(
        &fixture.cluster,
        holds.pop().context("retained hold")?,
        "verify-reopened",
    )?;
    source.birth = birth;
    let verified = reopened
        .verify_and_record_sealed_sources(
            domain,
            &archives.pool,
            &archives.tails,
            &original,
            vec![source],
        )
        .await?;
    anyhow::ensure!(verified.journal() == &journal && verified.sources().len() == 1);
    anyhow::ensure!(verified.sources()[0].marker() == sealed.marker());
    anyhow::ensure!(verified.sources()[0].recovery().manifest().pinned_images() == 3);
    let installed = Box::pin(verify_partition(
        &fixture.cluster,
        archives,
        &original,
        verified,
    ))
    .await?;
    Ok(installed)
}

async fn verify_installed_decision(
    fixture: &RuntimeCluster,
    plan: &DirectoryMutation,
    store: Arc<catalog_fault::Store>,
    markers: &[GroupManifest],
    proofs: Vec<TransferChildQuorum>,
) -> Result<()> {
    let domain = &fixture.cluster.domain;
    let directory = &fixture.cluster.directory;
    anyhow::ensure!(domain
        .run(directory.installed_transfer_children(plan))
        .await??
        .is_none());
    let before = domain.run(directory.observe()).await??;
    anyhow::ensure!(matches!(
        domain
            .run(directory.record_transfer_child_quorums(plan, vec![proofs[0].clone()]))
            .await?,
        Err(CatalogError::Invalid(_))
    ));
    let mut reversed = proofs.clone();
    reversed.reverse();
    anyhow::ensure!(matches!(
        domain
            .run(directory.record_transfer_child_quorums(plan, reversed))
            .await?,
        Err(CatalogError::Fenced)
    ));
    store.fail_head_confirmation.store(true, Ordering::Release);
    anyhow::ensure!(matches!(
        domain
            .run(directory.record_transfer_child_quorums(plan, proofs.clone()))
            .await?,
        Err(CatalogError::Unconfirmed)
    ));
    let catalog = Catalog::new(store, CatalogOptions::default())?;
    let namespace = domain
        .run(catalog.existing_namespace(directory.namespace_config().clone()))
        .await??;
    let reopened = RangeDirectory::new(namespace, DirectoryLimits::default())?;
    let (original, decision) = domain
        .run(reopened.active_transition())
        .await??
        .context("retained installed decision")?;
    anyhow::ensure!(original.encode() == plan.encode());
    anyhow::ensure!(decision.phase() == TransitionPhase::Installed);
    anyhow::ensure!(decision.installed_children().is_some());
    anyhow::ensure!(
        domain
            .run(reopened.installed_transfer_children(&original))
            .await??
            .as_deref()
            == Some(markers)
    );
    anyhow::ensure!(
        domain
            .run(reopened.record_transfer_child_quorums(&original, proofs))
            .await??
            == decision
    );
    let after = domain.run(reopened.observe()).await??;
    anyhow::ensure!(after.revision() == before.revision() && after.groups() == before.groups());
    let cell = CellId::new("alpha")?;
    anyhow::ensure!(
        domain.run(reopened.lookup(&after, &cell)).await?? == *fixture.cluster.birth.route()
    );
    Ok(())
}

async fn verify_transfer_archive(
    fixture: &RuntimeCluster,
    archives: &Archives,
    plan: &DirectoryMutation,
    store: Arc<catalog_fault::Store>,
    markers: &[GroupManifest],
) -> Result<()> {
    let cluster = &fixture.cluster;
    let domain = &cluster.domain;
    let directory = &cluster.directory;
    let holds = domain.run(directory.sealed_source_holds(plan)).await??;
    let hold = holds.first().context("sealed source hold")?;
    anyhow::ensure!(domain
        .run(directory.transfer_archive(plan))
        .await??
        .is_none());

    anyhow::ensure!(matches!(
        directory
            .publish_transfer(domain, &archives.pool, plan)
            .await,
        Err(CellGroupArchiveBindingError::Catalog(CatalogError::Fenced))
    ));
    anyhow::ensure!(matches!(
        domain
            .run(directory.prepare_transfer_activation(
                plan,
                markers[0].route().history,
                &cluster.voters[0].log,
            ))
            .await?,
        Err(CatalogError::Fenced)
    ));
    let inputs = archive_inputs(archives, cluster, hold, markers, "closure-outage")?;
    store
        .deny_group_payload_reads
        .store(true, Ordering::Release);
    let denied = directory
        .verify_and_record_transfer_archive(domain, plan, inputs)
        .await;
    store
        .deny_group_payload_reads
        .store(false, Ordering::Release);
    anyhow::ensure!(matches!(
        denied,
        Err(CellGroupArchiveBindingError::Archive(_))
    ));
    anyhow::ensure!(store.denied_group_payload_reads.load(Ordering::Acquire) > 0);
    let (_, installed) = domain
        .run(directory.active_transition())
        .await??
        .context("installed decision after archive refusal")?;
    anyhow::ensure!(installed.phase() == TransitionPhase::Installed);
    anyhow::ensure!(installed.transfer_archive().is_none());

    let inputs = archive_inputs(archives, cluster, hold, markers, "closure-verify")?;
    store.fail_head_confirmation.store(true, Ordering::Release);
    anyhow::ensure!(matches!(
        directory
            .verify_and_record_transfer_archive(domain, plan, inputs)
            .await,
        Err(CellGroupArchiveBindingError::Catalog(
            CatalogError::Unconfirmed
        ))
    ));

    let catalog = Catalog::new(store.clone(), CatalogOptions::default())?;
    let namespace = domain
        .run(catalog.existing_namespace(directory.namespace_config().clone()))
        .await??;
    let reopened = RangeDirectory::new(namespace, DirectoryLimits::default())?;
    let (original, archived) = domain
        .run(reopened.active_transition())
        .await??
        .context("archived decision")?;
    anyhow::ensure!(original.encode() == plan.encode());
    anyhow::ensure!(archived.phase() == TransitionPhase::Archived);
    let closure = domain
        .run(reopened.transfer_archive(&original))
        .await??
        .context("retained transfer archive")?;
    anyhow::ensure!(archived.transfer_archive() == Some(closure.digest));
    anyhow::ensure!(closure.source_holds == holds && closure.children == markers);

    let mut inputs = archive_inputs(archives, cluster, hold, markers, "closure-replay")?;
    inputs.sources[0].birth = domain
        .run(reopened.materialize_selected_history(cluster.birth.route().history))
        .await??;
    anyhow::ensure!(
        reopened
            .verify_and_record_transfer_archive(domain, &original, inputs)
            .await?
            == archived
    );
    anyhow::ensure!(matches!(
        domain.run(reopened.publish(&original)).await?,
        Err(CatalogError::Fenced)
    ));
    let view = domain.run(reopened.observe()).await??;
    anyhow::ensure!(
        domain
            .run(reopened.lookup(&view, &CellId::new("alpha")?))
            .await??
            == *cluster.birth.route()
    );
    Box::pin(verify_transfer_publication(
        fixture, archives, &reopened, &original, view, store, &closure,
    ))
    .await
}

async fn verify_transfer_publication(
    fixture: &RuntimeCluster,
    archives: &Archives,
    directory: &RangeDirectory,
    plan: &DirectoryMutation,
    before: DirectoryView,
    store: Arc<catalog_fault::Store>,
    closure: &CellGroupTransferArchive,
) -> Result<()> {
    let cluster = &fixture.cluster;
    let domain = &cluster.domain;

    // The conditional update commits but its immediate read-back fails.
    store.fail_head_confirmation.store(true, Ordering::Release);
    let publication = directory
        .publish_transfer(domain, &archives.pool, plan)
        .await?;
    anyhow::ensure!(!store.fail_head_confirmation.load(Ordering::Acquire));
    anyhow::ensure!(publication.transfer_archive() == Some(closure.digest));
    anyhow::ensure!(publication.revision() == before.revision() + 1);

    let old = domain.run(directory.group_page(&before, None, 3)).await??;
    anyhow::ensure!(old.next.is_none() && old.routes.as_slice() == [cluster.birth.route().clone()]);
    let after = domain.run(directory.observe()).await??;
    let new = domain.run(directory.group_page(&after, None, 3)).await??;
    anyhow::ensure!(after.revision() == publication.revision() && new.next.is_none());
    let expected = closure.children.iter().map(GroupManifest::route);
    anyhow::ensure!(new.routes.iter().eq(expected));
    anyhow::ensure!(new.routes.len() == 2);
    let cell = CellId::new("alpha")?;
    let child = domain.run(directory.lookup(&after, &cell)).await??;
    anyhow::ensure!(new.routes.contains(&child));

    let catalog = Catalog::new(store, CatalogOptions::default())?;
    let namespace = domain
        .run(catalog.existing_namespace(directory.namespace_config().clone()))
        .await??;
    let reopened = RangeDirectory::new(namespace, DirectoryLimits::default())?;
    let (original, decision) = domain
        .run(reopened.active_transition())
        .await??
        .context("published transition")?;
    anyhow::ensure!(original.encode() == plan.encode());
    anyhow::ensure!(decision.phase() == TransitionPhase::Published);
    anyhow::ensure!(
        reopened
            .publish_transfer(domain, &archives.pool, &original)
            .await?
            == publication
    );
    let retained = domain
        .run(reopened.transfer_archive(&original))
        .await??
        .context("published transfer archive")?;
    anyhow::ensure!(retained.digest == closure.digest);
    anyhow::ensure!(
        retained.source_holds == closure.source_holds && retained.children == closure.children
    );

    anyhow::ensure!(matches!(
        domain
            .run(reopened.prepare_source_retirement(
                &original,
                cluster.birth.route().history,
                &cluster.voters[0].log,
            ))
            .await?,
        Err(CatalogError::Fenced)
    ));
    for route in &new.routes {
        let birth = domain
            .run(reopened.materialize_selected_history(route.history))
            .await??;
        let (bootstrap, reads) = domain
            .run(catalog_fault::count_reads(
                reopened.bootstrap_group_member(&birth, &cluster.voters[0].log),
            ))
            .await?;
        anyhow::ensure!(matches!(bootstrap, Err(CatalogError::Fenced)));
        anyhow::ensure!(!reads.keys().any(|key| key.contains("/transfer-installs/")));
        anyhow::ensure!(matches!(
            fixture.runtimes[0].propose(route.history, vec![1]).await,
            Err(RuntimeError::NotReady)
        ));
        anyhow::ensure!(matches!(
            fixture.runtimes[0].read_index(route.history, vec![1]).await,
            Err(RuntimeError::NotReady)
        ));
    }
    Ok(())
}

async fn verify_transfer_activation(
    fixture: &mut RuntimeCluster,
    plan: &DirectoryMutation,
    children: &[GroupManifest],
    store: Arc<catalog_fault::Store>,
) -> Result<Vec<GroupManifest>> {
    let directory = fixture.cluster.directory.clone();
    let domain = &fixture.cluster.domain;
    let log = fixture.cluster.voters[0].log.clone();
    let publication = domain
        .run(directory.publication(plan))
        .await??
        .context("activation publication")?;
    let mut positions = Vec::with_capacity(children.len());
    for child in children {
        let scope = child.route().history;
        let wrong_engine = domain
            .run(directory.prepare_transfer_activation(plan, scope, &fixture.cluster.voters[1].log))
            .await??;
        anyhow::ensure!(matches!(
            fixture.runtimes[0].activate_transfer(wrong_engine).await,
            Err(RuntimeError::StaleCompletion)
        ));
        let permit = domain
            .run(directory.prepare_transfer_activation(plan, scope, &log))
            .await??;
        let CellGroupTransferActivation::Accepted(activation) =
            fixture.runtimes[0].activate_transfer(permit).await?
        else {
            anyhow::bail!("expected the first transfer activation proposal");
        };
        let duplicate = domain
            .run(directory.prepare_transfer_activation(plan, scope, &log))
            .await??;
        let CellGroupTransferActivation::Accepted(retry) =
            fixture.runtimes[0].activate_transfer(duplicate).await?
        else {
            anyhow::bail!("activation cannot commit before messages are delivered");
        };
        anyhow::ensure!(retry.position == activation.position && retry.digest == activation.digest);
        anyhow::ensure!(matches!(
            fixture.runtimes[0].propose(scope, vec![1]).await,
            Err(RuntimeError::NotReady)
        ));
        positions.push((scope, activation.position.index));
    }
    await_applied(fixture, &positions)
        .await
        .context("commit transfer activations")?;

    let domain = &fixture.cluster.domain;
    let mut acknowledgements = Vec::with_capacity(children.len());
    let mut expected_activations = Vec::with_capacity(children.len());
    for (child, (scope, position)) in children.iter().zip(positions) {
        let retry = domain
            .run(directory.prepare_transfer_activation(plan, scope, &log))
            .await??;
        let CellGroupTransferActivation::Applied(applied) =
            fixture.runtimes[0].activate_transfer(retry).await?
        else {
            anyhow::bail!("transfer activation did not return durable acknowledgement");
        };
        anyhow::ensure!(applied.position().index == position);
        expected_activations.push((scope, applied.position()));
        acknowledgements.push(*applied);
        for voter in &fixture.cluster.voters {
            let marker = voter
                .log
                .cell_group(&scope)?
                .group_manifest()?
                .context("active child")?;
            anyhow::ensure!(marker.is_active() && marker.root() == child.root());
            let retained = serde_json::to_value(&marker)?;
            anyhow::ensure!(serde_json::from_value::<GroupManifest>(retained.clone())? == marker);
            anyhow::ensure!(
                retained["transfer"]["activation"]["publication"]
                    == serde_json::to_value(publication.journal_digest())?
            );
        }
    }

    Box::pin(verify_active_decision(
        &fixture.cluster,
        &directory,
        plan,
        acknowledgements,
        &expected_activations,
        store,
    ))
    .await?;
    let view = domain.run(directory.observe()).await??;
    let original_cell = CellId::new("alpha")?;
    let original_route = domain
        .run(directory.lookup(&view, &original_cell))
        .await??;
    let existing = history(&original_route, "alpha")?;
    let runtime = fixture.runtimes[0].clone();
    let deadline = domain
        .run(async { cellar::asyncrt::mono_ms() })
        .await?
        .checked_add(5_000)
        .context("activation read deadline")?;
    let read = runtime.read_authority(
        original_route.history,
        existing.clone(),
        CellReadConsistency::Linearizable,
        deadline,
    );
    tokio::pin!(read);
    let authority = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let std::task::Poll::Ready(result) = futures_util::poll!(&mut read) {
                return Ok::<_, anyhow::Error>(result?);
            }
            fixture.pump(&[], &mut BTreeMap::new()).await?;
            tokio::task::yield_now().await;
        }
    })
    .await
    .context("linearizable read after activation")??;
    anyhow::ensure!(authority.cell().version.history == existing && authority.barrier().is_some());
    drop(authority);

    let domain = &fixture.cluster.domain;
    let name = CellId::new("after-transfer")?;
    let route = domain.run(directory.lookup(&view, &name)).await??;
    let cell = history(&route, name.as_str())?;
    let encoded = CellGroupEntry {
        binding: GroupEntryBinding {
            group: route.history,
            route_epoch: route.epoch,
            birth_digest: route.birth_digest,
            schema_digest: route.schema_digest,
            proposal: ProposalId::new([91; 16])?,
        },
        command: GroupCommand::Enroll {
            cell: cell.clone(),
            image: LtxImage {
                epoch: 1,
                txid: 0,
                checksum: 0,
                commitment: ImageCommitment::Sha256(sha2::Sha256::digest([]).into()),
            },
            files: Vec::new(),
        },
    }
    .encode(&route, GroupEntryLimits::default())?;
    let before = log
        .cell_group(&route.history)?
        .group_manifest()?
        .context("child before write")?;
    let write = runtime.propose(route.history, encoded).await?;
    await_applied(fixture, &[(route.history, write.position.index)])
        .await
        .context("commit post-activation enrollment")?;
    let after = log
        .cell_group(&route.history)?
        .group_manifest()?
        .context("child after write")?;
    anyhow::ensure!(after.is_active() && after.root().cells() == before.root().cells() + 1);
    children
        .iter()
        .map(|child| {
            log.cell_group(&child.route().history)?
                .group_manifest()?
                .context("retained active child")
        })
        .collect()
}

async fn verify_active_decision(
    cluster: &Cluster,
    directory: &RangeDirectory,
    plan: &DirectoryMutation,
    acknowledgements: Vec<CellGroupTransferActivationAck>,
    positions: &[(GroupHistory, LogPosition)],
    store: Arc<catalog_fault::Store>,
) -> Result<()> {
    let domain = &cluster.domain;
    anyhow::ensure!(domain
        .run(directory.transfer_activations(plan))
        .await??
        .is_none());
    anyhow::ensure!(matches!(
        domain
            .run(directory.record_transfer_activations(plan, vec![acknowledgements[0].clone()],))
            .await?,
        Err(CatalogError::Invalid(_))
    ));
    let mut reversed = acknowledgements.clone();
    reversed.reverse();
    anyhow::ensure!(matches!(
        domain
            .run(directory.record_transfer_activations(plan, reversed))
            .await?,
        Err(CatalogError::Fenced)
    ));
    let (_, before) = domain
        .run(directory.active_transition())
        .await??
        .context("published transition before activation settlement")?;
    anyhow::ensure!(before.phase() == TransitionPhase::Published);
    store.fail_head_confirmation.store(true, Ordering::Release);
    anyhow::ensure!(matches!(
        domain
            .run(directory.record_transfer_activations(plan, acknowledgements.clone()))
            .await?,
        Err(CatalogError::Unconfirmed)
    ));

    let catalog = Catalog::new(store, CatalogOptions::default())?;
    let namespace = domain
        .run(catalog.existing_namespace(directory.namespace_config().clone()))
        .await??;
    let reopened = RangeDirectory::new(namespace, DirectoryLimits::default())?;
    let (original, active) = domain
        .run(reopened.active_transition())
        .await??
        .context("active transition after lost reply")?;
    anyhow::ensure!(original.encode() == plan.encode());
    anyhow::ensure!(
        active.phase() == TransitionPhase::Active && active.activated_children().is_some()
    );
    anyhow::ensure!(
        domain
            .run(reopened.transfer_activations(&original))
            .await??
            .as_deref()
            == Some(positions)
    );
    anyhow::ensure!(
        domain
            .run(reopened.record_transfer_activations(&original, acknowledgements))
            .await??
            == active
    );
    anyhow::ensure!(
        domain.run(reopened.publication(&original)).await??
            == domain.run(directory.publication(plan)).await??
    );
    Ok(())
}

async fn await_applied(
    fixture: &mut RuntimeCluster,
    positions: &[(GroupHistory, LogIndex)],
) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            fixture.pump(&[], &mut BTreeMap::new()).await?;
            if positions.iter().all(|(group, position)| {
                fixture.applied[group]
                    .iter()
                    .all(|applied| applied >= position)
            }) {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .context("replicate child entries")?
}

async fn verify_parent_retirement(
    fixture: &mut RuntimeCluster,
    plan: &DirectoryMutation,
) -> Result<(GroupManifest, CellGroupSourceRetirementAck)> {
    let directory = fixture.cluster.directory.clone();
    let scope = fixture.cluster.birth.route().history;
    let log = fixture.cluster.voters[0].log.clone();
    let domain = &fixture.cluster.domain;
    let before = log
        .cell_group(&scope)?
        .group_manifest()?
        .context("sealed parent")?;
    let published = domain
        .run(directory.publication(plan))
        .await??
        .context("retirement publication")?;
    let wrong = domain
        .run(directory.prepare_source_retirement(plan, scope, &fixture.cluster.voters[1].log))
        .await??;
    anyhow::ensure!(matches!(
        fixture.runtimes[0].retire_source(wrong).await,
        Err(RuntimeError::StaleCompletion)
    ));
    let permit = domain
        .run(directory.prepare_source_retirement(plan, scope, &log))
        .await??;
    let CellGroupSourceRetirement::Accepted(retirement) =
        fixture.runtimes[0].retire_source(permit).await?
    else {
        anyhow::bail!("expected first retirement proposal");
    };
    let retry = domain
        .run(directory.prepare_source_retirement(plan, scope, &log))
        .await??;
    let CellGroupSourceRetirement::Accepted(duplicate) =
        fixture.runtimes[0].retire_source(retry).await?
    else {
        anyhow::bail!("retirement cannot commit before Raft delivery");
    };
    anyhow::ensure!(
        duplicate.position == retirement.position && duplicate.digest == retirement.digest
    );
    await_applied(fixture, &[(scope, retirement.position.index)])
        .await
        .context("commit the sealed parent's tombstone")?;

    let domain = &fixture.cluster.domain;
    let retry = domain
        .run(directory.prepare_source_retirement(plan, scope, &log))
        .await??;
    let CellGroupSourceRetirement::Applied(acknowledgement) =
        fixture.runtimes[0].retire_source(retry).await?
    else {
        anyhow::bail!("retirement retry lost its applied tombstone");
    };
    let tombstone = acknowledgement.tombstone();
    anyhow::ensure!(tombstone.position() == retirement.position);
    anyhow::ensure!(tombstone.publication() == published.journal_digest());
    for (voter, runtime) in fixture.cluster.voters.iter().zip(&fixture.runtimes) {
        let marker = voter
            .log
            .cell_group(&scope)?
            .group_manifest()?
            .context("retired parent")?;
        anyhow::ensure!(!marker.is_active() && marker.root() == before.root());
        let seal = marker.source_seal().context("retired source seal")?;
        anyhow::ensure!(
            seal.position() == before.source_seal().context("original seal")?.position()
        );
        anyhow::ensure!(seal.retirement() == Some(tombstone));
        anyhow::ensure!(matches!(
            runtime.propose(scope, vec![1]).await,
            Err(RuntimeError::NotReady)
        ));
        anyhow::ensure!(matches!(
            runtime.read_index(scope, vec![1]).await,
            Err(RuntimeError::NotReady)
        ));
    }

    let pool = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    let snapshot = fixture.runtimes[0].capture_checkpoint(scope, &pool).await?;
    let selected = snapshot.marker().clone();
    anyhow::ensure!(selected.source_seal().and_then(|seal| seal.retirement()) == Some(tombstone));
    fixture.runtimes[0]
        .publish_checkpoint(scope, snapshot.prepare_publication()?)
        .await?;
    let group = log.cell_group(&scope)?;
    tokio::time::timeout(Duration::from_secs(20), async {
        while group.checkpoint_manifest()?.as_ref() != Some(&selected) {
            fixture.pump(&[], &mut BTreeMap::new()).await?;
            tokio::task::yield_now().await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("persist the parent tombstone checkpoint")??;
    let domain = &fixture.cluster.domain;
    let (_, journal) = domain
        .run(directory.active_transition())
        .await??
        .context("topology still owns parent archive holds")?;
    anyhow::ensure!(journal.phase() == TransitionPhase::Active);
    let hold = domain.run(directory.sealed_source_holds(plan)).await??;
    anyhow::ensure!(hold.len() == 1);
    let held = hold[0].marker();
    anyhow::ensure!(held.route() == fixture.cluster.birth.route());
    anyhow::ensure!(held.root() == before.root() && held.source_seal() == before.source_seal());
    Ok((selected, *acknowledgement))
}

async fn verify_child_archive_coverage(
    fixture: &RuntimeCluster,
    archives: &Archives,
    plan: &DirectoryMutation,
    active: &[GroupManifest],
    store: Arc<catalog_fault::Store>,
) -> Result<()> {
    let cluster = &fixture.cluster;
    let domain = &cluster.domain;
    let directory = &cluster.directory;
    let parent_holds = domain.run(directory.sealed_source_holds(plan)).await??;
    let mut holds = Vec::with_capacity(active.len());
    for (index, marker) in active.iter().enumerate() {
        let birth = domain
            .run(directory.materialize_selected_history(marker.route().history))
            .await??;
        let registry = directory
            .prepare_group_archive_binding(domain, &archives.pool, &birth)
            .await?
            .commit(domain)
            .await?;
        let hold = archives
            .publish_group(
                fixture,
                &registry,
                marker.route(),
                &format!("child-coverage-upload-{index}"),
            )
            .await?;
        anyhow::ensure!(hold.marker() == marker);
        holds.push(hold);
    }
    anyhow::ensure!(domain
        .run(directory.retirement_child_holds(plan))
        .await??
        .is_none());
    let incomplete = child_archive_inputs(
        cluster,
        archives,
        directory,
        &holds[..1],
        "coverage-incomplete",
    )
    .await?;
    anyhow::ensure!(matches!(
        directory
            .verify_and_record_child_archives(
                domain,
                &archives.pool,
                &archives.tails,
                plan,
                incomplete
            )
            .await,
        Err(CellGroupArchiveBindingError::Catalog(
            CatalogError::Invalid(_)
        ))
    ));
    let mut reversed =
        child_archive_inputs(cluster, archives, directory, &holds, "coverage-reversed").await?;
    reversed.reverse();
    anyhow::ensure!(matches!(
        directory
            .verify_and_record_child_archives(
                domain,
                &archives.pool,
                &archives.tails,
                plan,
                reversed
            )
            .await,
        Err(CellGroupArchiveBindingError::Catalog(CatalogError::Fenced))
    ));

    let outage =
        child_archive_inputs(cluster, archives, directory, &holds, "coverage-outage").await?;
    let denied_before = store.denied_group_payload_reads.load(Ordering::Acquire);
    store
        .deny_group_payload_reads
        .store(true, Ordering::Release);
    let refused = directory
        .verify_and_record_child_archives(domain, &archives.pool, &archives.tails, plan, outage)
        .await;
    store
        .deny_group_payload_reads
        .store(false, Ordering::Release);
    anyhow::ensure!(matches!(
        refused,
        Err(CellGroupArchiveBindingError::Archive(_))
    ));
    anyhow::ensure!(store.denied_group_payload_reads.load(Ordering::Acquire) > denied_before);
    anyhow::ensure!(domain
        .run(directory.retirement_child_holds(plan))
        .await??
        .is_none());

    let verified =
        child_archive_inputs(cluster, archives, directory, &holds, "coverage-verify").await?;
    store.fail_head_confirmation.store(true, Ordering::Release);
    anyhow::ensure!(matches!(
        directory
            .verify_and_record_child_archives(
                domain,
                &archives.pool,
                &archives.tails,
                plan,
                verified
            )
            .await,
        Err(CellGroupArchiveBindingError::Catalog(
            CatalogError::Unconfirmed
        ))
    ));
    let catalog = Catalog::new(store, CatalogOptions::default())?;
    let namespace = domain
        .run(catalog.existing_namespace(directory.namespace_config().clone()))
        .await??;
    let reopened = RangeDirectory::new(namespace, DirectoryLimits::default())?;
    let (original, decision) = domain
        .run(reopened.active_transition())
        .await??
        .context("active transition with child archive proof")?;
    anyhow::ensure!(original.encode() == plan.encode());
    anyhow::ensure!(
        decision.phase() == TransitionPhase::Active && decision.child_archives().is_some()
    );
    anyhow::ensure!(
        domain
            .run(reopened.retirement_child_holds(&original))
            .await??
            .as_deref()
            == Some(holds.as_slice())
    );
    let replay =
        child_archive_inputs(cluster, archives, &reopened, &holds, "coverage-replay").await?;
    anyhow::ensure!(
        reopened
            .verify_and_record_child_archives(
                domain,
                &archives.pool,
                &archives.tails,
                &original,
                replay,
            )
            .await?
            == decision
    );

    anyhow::ensure!(
        domain
            .run(reopened.sealed_source_holds(&original))
            .await??
            == parent_holds
    );
    let parent = parent_holds
        .first()
        .context("original parent archive hold")?;
    let held = archives
        .registry
        .verify_hold(
            domain,
            parent,
            &archives.tails,
            archives.receiver(cluster, "parent-hold-after-coverage")?,
        )
        .await?;
    anyhow::ensure!(held.marker() == parent.marker());
    Ok(())
}

async fn verify_retirement_settlement(
    fixture: &RuntimeCluster,
    archives: &Archives,
    plan: &DirectoryMutation,
    acknowledgement: CellGroupSourceRetirementAck,
    store: Arc<catalog_fault::Store>,
) -> Result<()> {
    let cluster = &fixture.cluster;
    let domain = &cluster.domain;
    let directory = &cluster.directory;
    let publication = domain.run(directory.publication(plan)).await??;
    let parent_holds = domain.run(directory.sealed_source_holds(plan)).await??;
    let child_holds = domain
        .run(directory.retirement_child_holds(plan))
        .await??
        .context("selected child holds")?;
    anyhow::ensure!(matches!(
        directory
            .release_retired_parent_holds(domain, &archives.pool, plan)
            .await,
        Err(CellGroupArchiveBindingError::Catalog(CatalogError::Fenced))
    ));
    store.fail_head_confirmation.store(true, Ordering::Release);
    anyhow::ensure!(matches!(
        directory
            .settle_transfer_retirement(domain, &archives.pool, plan, vec![acknowledgement.clone()])
            .await,
        Err(CellGroupArchiveBindingError::Catalog(
            CatalogError::Unconfirmed
        ))
    ));

    let catalog = Catalog::new(store.clone(), CatalogOptions::default())?;
    let namespace = domain
        .run(catalog.existing_namespace(directory.namespace_config().clone()))
        .await??;
    let reopened = RangeDirectory::new(namespace, DirectoryLimits::default())?;
    let (original, retired) = domain
        .run(reopened.active_transition())
        .await??
        .context("retired transition still owns topology")?;
    anyhow::ensure!(original.encode() == plan.encode());
    anyhow::ensure!(
        retired.phase() == TransitionPhase::Retired && retired.retired_parents().is_some()
    );
    anyhow::ensure!(!retired.parent_holds_released());
    let parent = parent_holds.first().context("retired parent hold")?;
    let retained = archives
        .registry
        .verify_hold(
            domain,
            parent,
            &archives.tails,
            archives.receiver(cluster, "retired-before-release")?,
        )
        .await?;
    anyhow::ensure!(retained.marker() == parent.marker());
    drop(retained);

    store.deny_registry_writes.store(true, Ordering::Release);
    let denied = reopened
        .release_retired_parent_holds(domain, &archives.pool, &original)
        .await;
    store.deny_registry_writes.store(false, Ordering::Release);
    anyhow::ensure!(matches!(
        denied,
        Err(CellGroupArchiveBindingError::Archive(_))
    ));
    let (_, pending) = domain
        .run(reopened.active_transition())
        .await??
        .context("release refusal keeps topology owned")?;
    anyhow::ensure!(pending == retired);

    store.fail_head_confirmation.store(true, Ordering::Release);
    let completed = reopened
        .release_retired_parent_holds(domain, &archives.pool, &original)
        .await?;
    anyhow::ensure!(!store.fail_head_confirmation.load(Ordering::Acquire));
    anyhow::ensure!(
        completed.phase() == TransitionPhase::Retired && completed.parent_holds_released()
    );
    anyhow::ensure!(domain.run(reopened.active_transition()).await??.is_none());
    anyhow::ensure!(
        reopened
            .release_retired_parent_holds(domain, &archives.pool, &original)
            .await?
            == completed
    );
    anyhow::ensure!(
        reopened
            .settle_transfer_retirement(domain, &archives.pool, &original, vec![acknowledgement],)
            .await?
            == completed
    );
    anyhow::ensure!(domain.run(reopened.publication(&original)).await?? == publication);
    anyhow::ensure!(
        domain
            .run(reopened.retirement_child_holds(&original))
            .await??
            .as_deref()
            == Some(child_holds.as_slice())
    );
    anyhow::ensure!(matches!(
        archives
            .registry
            .verify_hold(
                domain,
                parent,
                &archives.tails,
                archives.receiver(cluster, "retired-after-release")?,
            )
            .await,
        Err(CellGroupArchiveError::Expired)
    ));
    Ok(())
}

async fn child_archive_inputs(
    cluster: &Cluster,
    archives: &Archives,
    directory: &RangeDirectory,
    holds: &[CellGroupArchiveHold],
    label: &str,
) -> Result<Vec<CellGroupTransitionSource>> {
    let mut children = Vec::with_capacity(holds.len());
    for (index, hold) in holds.iter().enumerate() {
        let route = hold.marker().route();
        let birth = cluster
            .domain
            .run(directory.materialize_selected_history(route.history))
            .await??;
        let (store, base) =
            archives.store(cluster, route.clone(), &format!("{label}/child-{index}"))?;
        let pool = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
        let receiver = pool.receive_archive(store, scratch()?, base)?;
        children.push(CellGroupTransitionSource {
            birth,
            hold: hold.clone(),
            receiver,
        });
    }
    Ok(children)
}

fn archive_inputs(
    archives: &Archives,
    cluster: &Cluster,
    hold: &CellGroupArchiveHold,
    markers: &[GroupManifest],
    label: &str,
) -> Result<CellGroupTransitionPreparationInputs> {
    Ok(CellGroupTransitionPreparationInputs {
        archives: archives.pool.clone(),
        tails: archives.tails.clone(),
        restore: transfer_pool()?,
        sources: vec![archives.source(cluster, hold.clone(), &format!("{label}/source"))?],
        targets: transfer_targets(
            archives,
            cluster,
            markers.iter().map(GroupManifest::route),
            label,
        )?,
    })
}

async fn reopen_installed_children(
    fixture: &RuntimeCluster,
    plan: &DirectoryMutation,
    installed: &[GroupManifest],
    retired: &GroupManifest,
) -> Result<()> {
    let cluster = &fixture.cluster;
    fixture.runtimes[0].close().await?;
    cluster.voters[0].log.close().await?;
    let log = cluster
        .domain
        .run(ReplicaLog::open(
            ReplicaLogOptions::new(
                cluster._root.path().join("node-1/log"),
                NodeId::new([1; 16])?,
                DiskLineage::new([11; 16])?,
                ProcessGeneration::new([91; 16])?,
                LogOpenMode::Resume,
            ),
            Arc::new(DirectFileSystem),
        ))
        .await??;
    let result = async {
        let parent = log.cell_group(&retired.route().history)?;
        anyhow::ensure!(parent.group_manifest()?.as_ref() == Some(retired));
        anyhow::ensure!(parent.checkpoint_manifest()?.as_ref() == Some(retired));
        for marker in installed {
            let permit = cluster
                .domain
                .run(cluster.directory.bootstrap_pending_transfer_voter(
                    plan,
                    marker.route().history,
                    &log,
                ))
                .await??;
            let group = permit.group();
            anyhow::ensure!(
                group.group_manifest()?.as_ref() == Some(marker)
                    && group.identity().process == ProcessGeneration::new([91; 16])?
                    && marker.is_active()
            );
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let joined = log.close().await;
    result?;
    Ok(joined?)
}

struct InstalledChildPartitions {
    genesis: Vec<GroupManifest>,
    voters: Vec<(CellGroupRecoveryPool, PreparedCellGroupTransfer)>,
}

async fn verify_pending_child_raft(
    fixture: &mut RuntimeCluster,
    plan: &DirectoryMutation,
    installed: InstalledChildPartitions,
) -> Result<(Vec<GroupManifest>, Vec<TransferChildQuorum>)> {
    let cluster = &fixture.cluster;
    let directory = &cluster.directory;
    let domain = &cluster.domain;
    for (index, (pool, partition)) in installed.voters.into_iter().enumerate() {
        let log = &cluster.voters[index].log;
        let (apply_pool, _) = fixture
            .appliers
            .get(&(index, cluster.birth.route().history))
            .context("voter apply pool")?;
        let apply_pool = apply_pool.clone();
        for child in partition.children() {
            let history = child.route().history;
            let permit = domain
                .run(directory.bootstrap_pending_transfer_voter(plan, history, log))
                .await??;
            // The exact same physical content on another voter grants no right
            // to use this voter's page and image owner.
            let other = &cluster.voters[(index + 1) % cluster.voters.len()].log;
            let foreign = domain
                .run(directory.bootstrap_pending_transfer_voter(plan, history, other))
                .await??;
            anyhow::ensure!(matches!(
                child.installed_applier(&foreign),
                Err(RuntimeError::StaleCompletion)
            ));
            let applier = child.installed_applier(&permit)?;
            let recover = applier.clone();
            let root = domain
                .blocking(move || recover.recover_selected())?
                .await??;
            anyhow::ensure!(Some(root.marker()) == child.genesis().map(|genesis| genesis.marker()));
            fixture.runtimes[index].attach(permit, root).await?;
            fixture
                .appliers
                .insert((index, history), (apply_pool.clone(), applier));
            fixture
                .applied
                .entry(history)
                .or_insert_with(|| vec![LogIndex::new(1); 3]);
        }
        let retained = partition.children()[0]
            .genesis()
            .context("child genesis")?
            .clone();
        drop(partition);
        anyhow::ensure!(pool.usage().operations == 1);
        drop(retained);
        anyhow::ensure!(pool.usage().operations == 0);
    }

    for genesis in &installed.genesis {
        let history = genesis.route().history;
        anyhow::ensure!(matches!(
            fixture.runtimes[0].pending_transfer_quorum(history).await,
            Err(RuntimeError::NotLeader)
        ));
        fixture.runtimes[0].campaign(history).await?;
        anyhow::ensure!(matches!(
            fixture.runtimes[0].pending_transfer_quorum(history).await,
            Err(RuntimeError::NotLeader | RuntimeError::NotReady)
        ));
    }
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            fixture.pump(&[], &mut BTreeMap::new()).await?;
            if installed.genesis.iter().all(|marker| {
                fixture.applied[&marker.route().history]
                    .iter()
                    .all(|index| index.get() >= 2)
            }) {
                break;
            }
            tokio::task::yield_now().await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("commit pending child Raft barriers")??;

    let mut committed = Vec::with_capacity(installed.genesis.len());
    let mut proofs = Vec::with_capacity(installed.genesis.len());
    for genesis in installed.genesis {
        let history = genesis.route().history;
        let status = fixture.runtimes[0].status(history).await?;
        anyhow::ensure!(
            !status.ready
                && status.leader == Some(1)
                && status.term.get() > 1
                && status.applied.term == status.term
                && status.committed >= status.applied.index
        );
        for runtime in &fixture.runtimes {
            anyhow::ensure!(matches!(
                runtime.propose(history, vec![1]).await,
                Err(RuntimeError::NotReady)
            ));
            anyhow::ensure!(matches!(
                runtime.read_index(history, vec![1]).await,
                Err(RuntimeError::NotReady)
            ));
        }
        let marker = fixture.cluster.voters[0]
            .log
            .cell_group(&history)?
            .group_manifest()?
            .context("applied child root")?;
        anyhow::ensure!(
            marker.position() == status.applied
                && marker.root() == genesis.root()
                && marker.transfer_origin() == genesis.transfer_origin()
                && !marker.is_active()
        );
        proofs.push(fixture.runtimes[0].pending_transfer_quorum(history).await?);
        anyhow::ensure!(matches!(
            fixture.runtimes[1].pending_transfer_quorum(history).await,
            Err(RuntimeError::NotLeader)
        ));
        committed.push(marker);
    }
    Ok((committed, proofs))
}

async fn verify_partition(
    cluster: &Cluster,
    archives: &Archives,
    plan: &DirectoryMutation,
    verified: VerifiedCellGroupTransitionSources,
) -> Result<InstalledChildPartitions> {
    let sources = verified
        .journal()
        .sealed_sources()
        .context("sealed source set missing")?;
    let directory = &cluster.directory;
    let mut installed = Vec::new();
    let mut partitions = Vec::new();
    for (voter_index, voter) in cluster.voters.iter().enumerate() {
        let pool = transfer_pool()?;
        let targets = transfer_targets(
            archives,
            cluster,
            verified.children().iter(),
            &format!("node-{}", voter_index + 1),
        )?;
        let partition = pool
            .prepare_transfer_partition(&cluster.domain, verified.clone(), targets)
            .await?;
        anyhow::ensure!(pool.usage().operations == 1);
        if voter_index == 0 {
            verify_partition_data(archives, plan, &partition, sources)?;
        }
        for (child_index, child) in partition.children().iter().enumerate() {
            let marker = child.genesis().context("child genesis")?.marker();
            let history = child.route().history;
            let first = cluster
                .domain
                .run(directory.install_transfer_voter(plan, child, &voter.log))
                .await??;
            anyhow::ensure!(first.group_manifest()?.as_ref() == Some(marker));
            if voter_index == 0 {
                verify_install_retry(cluster, plan, child, &first).await?;
                installed.push(marker.clone());
            } else {
                anyhow::ensure!(&installed[child_index] == marker);
            }
            let quorum = cluster
                .domain
                .run(directory.observe_transfer_install_quorum(plan, history))
                .await??;
            if voter_index == 0 {
                anyhow::ensure!(quorum.is_none());
                anyhow::ensure!(matches!(
                    cluster
                        .domain
                        .run(directory.bootstrap_pending_transfer_voter(plan, history, &voter.log))
                        .await?,
                    Err(CatalogError::Unconfirmed)
                ));
            } else {
                let proof = quorum.context("missing physical install quorum")?;
                anyhow::ensure!(
                    proof.genesis == *marker
                        && proof.voters.len() == voter_index + 1
                        && proof
                            .voters
                            .iter()
                            .zip(&cluster.voters)
                            .all(|(receipt, expected)| {
                                let group = expected.log.cell_group(&history);
                                group.is_ok_and(|group| {
                                    group.identity().member == receipt.member
                                        && group.identity().node == receipt.node
                                        && group.identity().disk == receipt.disk
                                }) && expected
                                    .log
                                    .storage_instance()
                                    .is_ok_and(|engine| engine == receipt.engine)
                            })
                );
            }
        }
        partitions.push((pool, partition));
    }
    Ok(InstalledChildPartitions {
        genesis: installed,
        voters: partitions,
    })
}

fn transfer_pool() -> Result<CellGroupRecoveryPool> {
    Ok(CellGroupRecoveryPool::new(CellGroupRecoveryLimits {
        max_cells: 3,
        max_image_bytes: 1024 * 1024,
        max_restore_bytes: 4 * 1024 * 1024,
        ..Default::default()
    })?)
}

fn transfer_targets<'a>(
    archives: &Archives,
    cluster: &Cluster,
    routes: impl Iterator<Item = &'a GroupRoute>,
    label: &str,
) -> Result<Vec<CellGroupTransferTarget>> {
    routes
        .enumerate()
        .map(|(index, route)| {
            let (store, directory) =
                archives.store(cluster, route.clone(), &format!("{label}/child-{index}"))?;
            Ok(CellGroupTransferTarget {
                store,
                directory,
                scratch: scratch()?,
            })
        })
        .collect()
}

fn verify_partition_data(
    archives: &Archives,
    plan: &DirectoryMutation,
    partition: &PreparedCellGroupTransfer,
    sources: [u8; 32],
) -> Result<()> {
    anyhow::ensure!(partition.children().len() == 2);
    anyhow::ensure!(
        partition
            .children()
            .iter()
            .map(|child| child.root().cells())
            .sum::<u32>()
            == 3
    );
    for child in partition.children() {
        let genesis = child.genesis().context("missing final child genesis")?;
        let marker = genesis.marker();
        let origin = marker.transfer_origin().context("child origin missing")?;
        anyhow::ensure!(
            !marker.is_active()
                && marker.recovery_request().is_none()
                && marker.route() == child.route()
                && marker.root() == child.root()
                && marker.position().index.get() == 1
                && marker.position().term.get() == 1
                && marker.membership_revision() == 1
                && origin.operation() == plan.operation().bytes()
                && origin.plan() == plan.digest()
                && origin.sealed_sources() == sources
        );
        let mut encoded = serde_json::to_value(marker)?;
        anyhow::ensure!(serde_json::from_value::<GroupManifest>(encoded.clone())? == *marker);
        encoded["transfer"]["sealed_sources"] = serde_json::to_value([0u8; 32])?;
        anyhow::ensure!(serde_json::from_value::<GroupManifest>(encoded).is_err());
    }
    let source = partition.sources().sources()[0].recovery().manifest();
    for name in ["alpha", "beta", "empty"] {
        let cell = CellId::new(name)?;
        let (original, _) = source.pin_cell(&cell).context("sealed cell missing")?;
        let mut assigned = partition.children().iter().filter_map(|child| {
            child
                .pin_cell(&cell)
                .map(|(leaf, pin)| (child.route(), leaf, pin))
        });
        let (route, staged, pin) = assigned.next().context("staged cell missing")?;
        anyhow::ensure!(assigned.next().is_none());
        anyhow::ensure!(route
            .range
            .contains(&cell_routing_key(route.history.namespace, &cell)));
        let mut expected = original.clone();
        expected.last_mutation = LogPosition {
            index: LogIndex::new(1),
            term: Term::new(1),
        };
        anyhow::ensure!(staged == &expected);
        match pin {
            CellImagePin::Materialized(pin) if !archives.rooted && name != "empty" => {
                pin.verify_and_sync()?
            }
            CellImagePin::Extents(pin) if archives.rooted && name != "empty" => {
                anyhow::ensure!(
                    pin.reference().root().bytes()
                        == match staged.version.image.commitment {
                            CellImageCommitment::Extents { root, .. } => root,
                            _ => anyhow::bail!("staged extent image commitment"),
                        }
                );
            }
            CellImagePin::Empty if name == "empty" => (),
            _ => anyhow::bail!("wrong staged image representation"),
        }
    }
    Ok(())
}

async fn verify_install_retry(
    cluster: &Cluster,
    plan: &DirectoryMutation,
    child: &PreparedCellGroupTransferChild,
    first: &GroupLog<GroupHistory>,
) -> Result<()> {
    let directory = &cluster.directory;
    let log = &cluster.voters[0].log;
    let retry = cluster
        .domain
        .run(directory.install_transfer_voter(plan, child, log))
        .await??;
    anyhow::ensure!(retry.numeric_key() == first.numeric_key());
    let ordinary = cluster
        .domain
        .run(
            log.create_cell_group(CellGroupBootstrap {
                route: child.route().clone(),
                member: MemberId::new(1)?,
                voters: [1, 2, 3]
                    .into_iter()
                    .map(MemberId::new)
                    .collect::<Result<Vec<_>, _>>()?,
                learners: vec![],
            }),
        )
        .await?;
    anyhow::ensure!(matches!(ordinary, Err(LogError::WrongIdentity)));
    anyhow::ensure!(cluster
        .domain
        .run(directory.materialize_selected_history(child.route().history))
        .await?
        .is_err());
    Ok(())
}
