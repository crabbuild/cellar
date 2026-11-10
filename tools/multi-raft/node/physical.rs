// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

pub(super) struct Physical {
    pub pages: ManifestPageStore,
    pub images: CellGroupApplyImages,
}

impl Physical {
    pub fn open(base: &std::path::Path, rooted: bool) -> Result<Self> {
        let filesystem: Arc<dyn cellar_ltx::FileSystem> = Arc::new(DirectFileSystem);
        let pages = ManifestPageStore::open(
            &base.join("pages"),
            filesystem.clone(),
            1024,
            16 * 1024 * 1024,
            16,
        )?;
        let services = ReadServices {
            now_unix_millis: Arc::new(cellar::asyncrt::wall_ms),
            fill_random: Arc::new(|bytes| bytes.fill(17)),
        };
        let images = if rooted {
            CellGroupApplyImages::Extents {
                images: NodeExtentStore::open(
                    base.join("extents"),
                    filesystem,
                    Default::default(),
                )?,
                services,
                limits: Default::default(),
                cache_directory: base.join("apply-cache"),
                cache_pool: scratch(1)?,
                cache: cache(),
            }
        } else {
            CellGroupApplyImages::Materialized {
                images: GenerationStore::open(base.join("images"), filesystem, Default::default())?,
                services,
            }
        };
        Ok(Self { pages, images })
    }

    pub fn archive(
        self,
        route: cellar::application::consensus::verification::cell_groups::GroupRoute,
    ) -> Result<CellGroupArchiveStore> {
        Ok(CellGroupArchiveStore::new(
            route,
            self.pages,
            self.images,
            Default::default(),
            Default::default(),
            100,
        )?)
    }

    pub fn recover(
        self,
        group: cellar::application::consensus::verification::GroupLog<GroupHistory>,
    ) -> Result<(CellGroupApplier, PreparedGroupManifest)> {
        Ok(CellGroupApplier::recover(
            group,
            self.pages,
            self.images,
            Default::default(),
            Default::default(),
            100,
        )?)
    }

    pub fn initial(
        mut self,
        group: cellar::application::consensus::verification::GroupLog<GroupHistory>,
    ) -> Result<(CellGroupApplier, PreparedGroupManifest)> {
        let marker = group.group_manifest()?.context("initial manifest")?;
        let tree = ManifestTree::new(marker.route().clone(), Default::default())?;
        let root = tree.prepare_complete(
            &group,
            &mut self.pages,
            marker.root(),
            marker.position(),
            marker.membership_revision(),
            |_| Err(ManifestError::Invalid("source genesis is not empty")),
        )?;
        let applier = CellGroupApplier::new(
            group,
            self.pages,
            self.images,
            Default::default(),
            Default::default(),
            100,
        )?;
        Ok((applier, root))
    }
}

pub(super) fn scratch(files: usize) -> Result<ScratchPool> {
    Ok(ScratchPool::new(ScratchLimits {
        max_bytes: files as u64 * EXTENT_BYTES,
        max_files: files,
    })?)
}

fn cache() -> CacheLimits {
    CacheLimits {
        max_cached_bytes: EXTENT_BYTES,
        max_cached_extents: 1,
    }
}

pub(super) fn sql_options(
    images: CellGroupApplyImages,
    base: PathBuf,
    schema: [u8; 32],
    source: bool,
) -> Result<CellSqlOptions> {
    let host = cellar_ltx::LtxHost::new(
        cellar::asyncrt::wall_ms,
        |_| Ok(Duration::ZERO),
        |path| async move { std::fs::read(path) },
        |job| async move {
            job();
            Ok(())
        },
    )
    .with_filesystem(Arc::new(DirectFileSystem));
    let limits = ExecutionLimits {
        envelope: EnvelopeLimits {
            max_payload_bytes: 1024 * 1024,
            ..Default::default()
        },
        max_retained_receipts: if source { 2 } else { 4096 },
        ..Default::default()
    };
    Ok(match images {
        CellGroupApplyImages::Materialized { images, services } => {
            CellSqlOptions::Materialized(PrivateOptions {
                directory: base,
                host,
                sqlite_vfs: None,
                images,
                query_services: services,
                application_digest: schema,
                limits,
            })
        }
        CellGroupApplyImages::Extents {
            images, services, ..
        } => CellSqlOptions::Extents(CellSqlRootOptions {
            host,
            sqlite_vfs: None,
            images,
            query_services: services,
            application_digest: schema,
            limits,
            cache_directory: base.with_extension("read-cache"),
            cache_pool: scratch(2)?,
            cache: cache(),
        }),
    })
}

pub(super) fn checkpoint_pool() -> Result<CellGroupCheckpointPool> {
    Ok(CellGroupCheckpointPool::new(CellGroupCheckpointLimits {
        max_sources: 1,
        max_retained_bytes: 32 * 1024 * 1024,
        max_cells: 64,
        max_transfer_bytes: 32 * 1024 * 1024,
        max_chunk_bytes: 4093,
    })?)
}
