// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Private replay inventory: serving image pins cannot satisfy terminal proof.
use super::*;
use cellar::application::consensus::verification::cell_groups::{
    CellGroupApplyImages, CellGroupArchivePool, CellGroupArchiveTailPool, CellGroupCheckpointPool,
};
use cellar_ltx::{
    generation::{GenerationStore, ReadServices},
    internal::{CacheLimits, NodeExtentStore, EXTENT_BYTES},
    stream_snapshot::{ScratchLimits, ScratchPool},
    DirectFileSystem,
};
use rand::RngCore as _;

pub(super) async fn verifier(
    fixture: &Fixture,
    root: &Path,
    rooted: bool,
) -> Result<FleetGroupArchiveVerifier> {
    let directory = root.to_owned();
    let base = directory.clone();
    let images = fixture
        .domain
        .blocking(move || {
            let filesystem = Arc::new(DirectFileSystem);
            let services = ReadServices {
                now_unix_millis: Arc::new(cellar::asyncrt::wall_ms),
                fill_random: Arc::new(|bytes| {
                    cellar::asyncrt::rng("managed-recovery-verifier").fill_bytes(bytes)
                }),
            };
            if rooted {
                Ok::<_, anyhow::Error>(CellGroupApplyImages::Extents {
                    images: NodeExtentStore::open(
                        base.join("extents"),
                        filesystem,
                        Default::default(),
                    )?,
                    services,
                    limits: Default::default(),
                    cache_directory: base.join("cache"),
                    cache_pool: ScratchPool::new(ScratchLimits {
                        max_bytes: 32 * 1024 * 1024,
                        max_files: 1,
                    })?,
                    cache: CacheLimits {
                        max_cached_bytes: EXTENT_BYTES,
                        max_cached_extents: 1,
                    },
                })
            } else {
                Ok(CellGroupApplyImages::Materialized {
                    images: GenerationStore::open(
                        base.join("images"),
                        filesystem,
                        Default::default(),
                    )?,
                    services,
                })
            }
        })?
        .await??;
    let limits = limits();
    Ok(FleetGroupArchiveVerifier::new(
        CellGroupArchivePool::new(Default::default())?,
        CellGroupArchiveTailPool::new(Default::default())?,
        CellGroupCheckpointPool::new(checkpoint_limits())?,
        ScratchPool::new(ScratchLimits {
            max_bytes: 32 * 1024 * 1024,
            max_files: 1,
        })?,
        FleetGroupArchiveReplayOptions {
            directory,
            filesystem: Arc::new(DirectFileSystem),
            materialized: (!rooted).then(|| images.clone()),
            extents: rooted.then_some(images),
            manifest: limits.manifest,
            entries: limits.entries,
            max_pages: limits.manifest_pages,
            max_page_bytes: limits.manifest_bytes,
            max_retained_roots: limits.retained_roots,
            max_effect_rows: limits.max_effect_rows,
        },
    )?)
}
