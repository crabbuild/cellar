// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use cellar_logic::consensus::LogPosition;
use cellar_ltx::{
    internal::NodeExtentStore,
    stream_snapshot::{ScratchLimits, ScratchPool},
};
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
            cluster.birth.route().clone(),
            pages,
            checkpoint_receive::config(images, extents, &base)?,
            ManifestLimits::default(),
            GroupEntryLimits::default(),
            100,
        )?;
        Ok(self.checkpoints.receive_archive(
            store,
            ScratchPool::new(ScratchLimits {
                max_bytes: 32 * 1024 * 1024,
                max_files: 1,
            })?,
            base,
        )?)
    }

    async fn publish(&self, fixture: &RuntimeCluster, label: &str) -> Result<CellGroupArchiveHold> {
        let cluster = &fixture.cluster;
        let intent = self.registry.prepare_upload(&cluster.domain).await?;
        let ticket = self.registry.reserve(&cluster.domain, &intent).await?;
        let upload = self
            .registry
            .open_upload(&cluster.domain, &self.uploads, &ticket, &self.tails)
            .await?;
        let cut = fixture.runtimes[0]
            .capture_checkpoint(cluster.birth.route().history, &self.checkpoints)
            .await?;
        let uploaded = upload.upload_cut(&cluster.domain, &self.tails, cut).await?;
        let verified = upload
            .prepare_publication(
                &cluster.domain,
                &self.tails,
                &uploaded,
                self.receiver(cluster, label)?,
            )
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
    Box::pin(verify_sealed_sources(
        &fixture,
        &archives,
        store,
        &plan,
        unsealed,
        seal.position,
    ))
    .await?;
    fixture.close().await
}

async fn verify_sealed_sources(
    fixture: &RuntimeCluster,
    archives: &Archives,
    store: Arc<catalog_fault::Store>,
    plan: &DirectoryMutation,
    unsealed: CellGroupArchiveHold,
    seal: LogPosition,
) -> Result<()> {
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
    let mut source = archives.source(
        &fixture.cluster,
        holds.pop().context("retained hold")?,
        "verify-reopened",
    )?;
    source.birth = birth.clone();
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
    drop(verified);
    let intent = archives.registry.prepare_acquire(domain).await?;
    let alternate = archives.registry.acquire(domain, &intent).await?;
    anyhow::ensure!(alternate != sealed);
    let mut source = archives.source(&fixture.cluster, alternate, "reject-alternate")?;
    source.birth = birth;
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
    let permit = domain
        .run(reopened.prepare_source_seal(&original, scope, &fixture.cluster.voters[0].log))
        .await??;
    anyhow::ensure!(matches!(
        fixture.runtimes[0].seal_source(permit).await?,
        CellGroupSealOutcome::Applied(_)
    ));
    Ok(())
}
