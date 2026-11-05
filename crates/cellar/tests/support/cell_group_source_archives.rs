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
        store,
        &plan,
        unsealed,
        seal.position,
    ))
    .await?;
    reopen_installed_children(&fixture, &installed).await?;
    fixture.close().await
}

async fn verify_sealed_sources(
    fixture: &RuntimeCluster,
    archives: &Archives,
    store: Arc<catalog_fault::Store>,
    plan: &DirectoryMutation,
    unsealed: CellGroupArchiveHold,
    seal: LogPosition,
) -> Result<Vec<GroupManifest>> {
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
    let installed = Box::pin(verify_partition(
        &fixture.cluster,
        archives,
        &original,
        verified,
    ))
    .await?;
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
    Ok(installed)
}

async fn reopen_installed_children(
    fixture: &RuntimeCluster,
    installed: &[GroupManifest],
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
        for marker in installed {
            let group = log.cell_group(&marker.route().history)?;
            anyhow::ensure!(
                group.group_manifest()?.as_ref() == Some(marker)
                    && group.identity().process == ProcessGeneration::new([91; 16])?
                    && !marker.is_active()
            );
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let joined = log.close().await;
    result?;
    Ok(joined?)
}

async fn verify_partition(
    cluster: &Cluster,
    archives: &Archives,
    plan: &DirectoryMutation,
    verified: VerifiedCellGroupTransitionSources,
) -> Result<Vec<GroupManifest>> {
    let targets = verified
        .children()
        .iter()
        .enumerate()
        .map(|(index, route)| {
            let (store, directory) =
                archives.store(cluster, route.clone(), &format!("node-1/child-{index}"))?;
            Ok(CellGroupTransferTarget {
                store,
                directory,
                scratch: scratch()?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let pool = CellGroupRecoveryPool::new(CellGroupRecoveryLimits {
        max_cells: 3,
        max_image_bytes: 1024 * 1024,
        max_restore_bytes: 4 * 1024 * 1024,
        ..Default::default()
    })?;
    let sources = verified
        .journal()
        .sealed_sources()
        .context("sealed source set missing")?;
    let partition = pool
        .prepare_transfer_partition(&cluster.domain, verified, targets)
        .await?;
    anyhow::ensure!(pool.usage().operations == 1);
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
    let directory = &cluster.directory;
    let log = &cluster.voters[0].log;
    let mut installed = Vec::new();
    for child in partition.children() {
        let marker = child.genesis().context("child genesis")?.marker();
        let first = cluster
            .domain
            .run(directory.install_transfer_voter(plan, child, log))
            .await??;
        anyhow::ensure!(first.group_manifest()?.as_ref() == Some(marker));
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
        installed.push(marker.clone());
    }
    let retained = partition.children()[0]
        .genesis()
        .context("child genesis")?
        .clone();
    drop(partition);
    anyhow::ensure!(pool.usage().operations == 1);
    drop(retained);
    anyhow::ensure!(pool.usage().operations == 0);
    Ok(installed)
}
