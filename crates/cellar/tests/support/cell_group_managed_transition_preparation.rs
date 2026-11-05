// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Rebuild a durable pre-copy from the same real managed archive hold.
use super::*;
use cellar::application::consensus::verification::cell_groups::{
    CellGroupApplyImages, CellGroupArchiveHold, CellGroupArchiveRegistry, CellGroupArchiveStore,
    CellGroupArchiveTailPool, CellGroupBirth, CellGroupCheckpointPool, CellGroupRecoveryLimits,
    CellGroupRecoveryPool, CellGroupTransferTarget, CellGroupTransitionPreparationInputs,
    CellGroupTransitionSource, GroupRoute, ManifestPageStore,
};
use cellar_ltx::{
    generation::{GenerationStore, ReadServices},
    internal::{CacheLimits, NodeExtentStore, EXTENT_BYTES},
    stream_snapshot::{ScratchLimits, ScratchPool},
    DirectFileSystem,
};
use rand::RngCore as _;

pub(super) async fn exercise(
    cluster: &ManagedCluster,
    controller: &FleetCatalog,
    directory: &RangeDirectory,
    transition: &ReopenedManagedCellGroupTransition,
    parent: &CellGroupBirth,
    children: &[GroupRoute],
    rooted: bool,
) -> Result<(CellGroupArchiveRegistry, CellGroupArchiveHold)> {
    let seeded = seed_cells(&cluster.hosts, cluster.history).await?;
    let domain = &cluster.fixtures[0].domain;
    let leader = leader(&cluster.hosts[..3], cluster.history).await?;
    let cell = &seeded.cells[0];
    let handle = cluster.hosts[leader].cell(cell.clone())?;
    let client = seeded.receipts[0].id.client;
    let mut options = CommandOptions::new(CommandId::new(client, 2)?, seeded.retry);
    options.durability = Durability::S3;
    let archived = handle
        .command(
            &handle.contract("Add")?,
            2i64.to_be_bytes().to_vec(),
            options,
        )
        .await?;
    ensure!(archived.durability == Durability::S3 && archived.result == 9i64.to_be_bytes());
    let archives = CellGroupArchivePool::new(Default::default())?;
    let registry = directory
        .open_group_archive(domain.as_ref(), &archives, parent)
        .await?;
    let intent = registry.prepare_acquire(domain.as_ref()).await?;
    let held = registry.acquire(domain.as_ref(), &intent).await?;
    let scratch = tempfile::tempdir()?;
    let mut selected = None;
    for stage in ["first", "reopened"] {
        let mut routes = vec![parent.route().clone()];
        routes.extend_from_slice(children);
        let root = scratch.path().join(stage);
        let mut stores = domain
            .blocking(move || {
                routes
                    .into_iter()
                    .enumerate()
                    .map(|(index, route)| store(root.join(index.to_string()), route, rooted))
                    .collect::<Result<Vec<_>>>()
            })?
            .await??
            .into_iter();
        let source = stores.next().context("missing source receiver")?;
        let receiver = CellGroupCheckpointPool::new(checkpoint_limits())?.receive_archive(
            source.store,
            source.scratch,
            source.directory,
        )?;
        let prepared = controller
            .prepare_managed_transition(
                &transition.request,
                directory,
                &transition.plan,
                CellGroupTransitionPreparationInputs {
                    archives: archives.clone(),
                    tails: CellGroupArchiveTailPool::new(Default::default())?,
                    restore: CellGroupRecoveryPool::new(CellGroupRecoveryLimits {
                        max_cells: 4,
                        max_image_bytes: 1024 * 1024,
                        max_restore_bytes: 4 * 1024 * 1024,
                        ..Default::default()
                    })?,
                    sources: vec![CellGroupTransitionSource {
                        birth: parent.clone(),
                        hold: held.clone(),
                        receiver,
                    }],
                    targets: stores.collect(),
                },
                TIMEOUT,
            )
            .await?;
        ensure!(prepared.journal().phase() == TransitionPhase::Prepared);
        let digest = prepared
            .journal()
            .preparation()
            .context("missing pre-copy digest")?;
        ensure!(selected.is_none_or(|original| original == digest));
        selected = Some(digest);
        ensure!(
            prepared
                .children()
                .iter()
                .map(|child| child.root().cells())
                .sum::<u32>()
                == 2
        );
        let mut staged = prepared
            .children()
            .iter()
            .filter_map(|child| child.pin_cell(&cell.cell));
        let (leaf, _) = staged.next().context("missing archived cell")?;
        ensure!(leaf.version == archived.version && leaf.witness == archived.witness);
        ensure!(staged.next().is_none());
        ensure!(
            domain
                .run(directory.managed_preparation_holds(&transition.plan))
                .await??
                == vec![held.clone()]
        );
        if stage == "first" {
            // The pre-copy must preserve its exact historical cut while the
            // parent continues accepting subsequent quorum commands.
            let next = handle
                .command(
                    &handle.contract("Add")?,
                    1i64.to_be_bytes().to_vec(),
                    CommandOptions::new(CommandId::new(client, 3)?, seeded.retry),
                )
                .await?;
            ensure!(next.result == 10i64.to_be_bytes() && next.version != archived.version);
        }
    }
    Ok((registry, held))
}

fn store(
    base: std::path::PathBuf,
    route: GroupRoute,
    rooted: bool,
) -> Result<CellGroupTransferTarget> {
    let filesystem = Arc::new(DirectFileSystem);
    let scratch = ScratchPool::new(ScratchLimits {
        max_bytes: 32 * 1024 * 1024,
        max_files: 1,
    })?;
    let services = ReadServices {
        now_unix_millis: Arc::new(cellar::asyncrt::wall_ms),
        fill_random: Arc::new(|bytes| cellar::asyncrt::rng("transition-precopy").fill_bytes(bytes)),
    };
    let images = if rooted {
        CellGroupApplyImages::Extents {
            images: NodeExtentStore::open(
                base.join("extents"),
                filesystem.clone(),
                Default::default(),
            )?,
            services,
            limits: Default::default(),
            cache_directory: base.join("cache"),
            cache_pool: scratch.clone(),
            cache: CacheLimits {
                max_cached_bytes: EXTENT_BYTES,
                max_cached_extents: 1,
            },
        }
    } else {
        CellGroupApplyImages::Materialized {
            images: GenerationStore::open(
                base.join("images"),
                filesystem.clone(),
                Default::default(),
            )?,
            services,
        }
    };
    let limits = limits();
    Ok(CellGroupTransferTarget {
        store: CellGroupArchiveStore::new(
            route,
            ManifestPageStore::open(
                &base.join("pages"),
                filesystem,
                limits.manifest_pages,
                limits.manifest_bytes,
                limits.retained_roots,
            )?,
            images,
            limits.manifest,
            limits.entries,
            limits.max_effect_rows,
        )?,
        scratch,
        directory: base,
    })
}
