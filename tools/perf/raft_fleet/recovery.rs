// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Explicit owned recovery startup; evidence and physical archive replay remain mandatory.
use super::*;
use cellar::sdk::consensus::verification::cell_groups::{
    CellGroupApplyImages, CellGroupArchivePool, CellGroupArchiveTailPool, CellGroupCheckpointPool,
};
use cellar_ltx::{
    apply::ApplyLimits,
    generation::ReadServices,
    internal::{NodeExtentStore, EXTENT_BYTES},
    stream_snapshot::{ScratchLimits, ScratchPool},
    DirectFileSystem,
};
use rand::RngCore as _;

impl Api {
    pub(super) fn recovery_status(&self) -> Result<Value> {
        let guard = self
            .recovery
            .lock()
            .map_err(|_| anyhow::anyhow!("recovery handle poisoned"))?;
        let Some(service) = guard.as_ref() else {
            return Ok(Value::Null);
        };
        let status = service.status();
        Ok(
            json!({"running":status.running, "rounds":status.poll_rounds,
            "execution_attempts":status.execution_attempts, "settled_movements":status.settled_movements,
            "last_revision":status.last_revision,
            "last_failure":status.last_round_failure.map(|f| format!("{f:?}")),
            "detection":status.failure_detection.map(|d| json!({
                "revision":d.observed_revision,
                "nodes":d.nodes.iter().map(|n| json!({
                    "boot":n.boot, "health":format!("{:?}",n.health),
                    "consecutive_failures":n.consecutive_failures,
                    "last_error":n.last_error.as_ref().map(|e| format!("{e:?}")),
                    "sample_age_ms":n.sample_age_ms
                })).collect::<Vec<_>>()
            })),
            "recovery":status.failure_recovery.map(|r| json!({
                "phase":format!("{:?}",r.phase), "verified_histories":r.verified_histories,
                "total_histories":r.total_histories, "removed_revision":r.removed_revision
            }))}),
        )
    }

    pub(super) async fn start_recovery(&self) -> Result<Value> {
        if self
            .recovery
            .lock()
            .map_err(|_| anyhow::anyhow!("recovery handle poisoned"))?
            .is_some()
        {
            return self.recovery_status();
        }
        let cut = self.managed.catalog.read(TIMEOUT).await?;
        let host = self
            .managed
            .catalog
            .authorize_host(&cut, self.node, TIMEOUT)
            .await?;
        ensure!(
            host.state() == FleetNodeState::Active,
            "recovery requires active local capacity"
        );
        self.resources.observe(self.node, TIMEOUT).await?;
        self.managed
            .catalog
            .check_inventory(&cut, std::slice::from_ref(&self.managed.directory), TIMEOUT)
            .await?;
        // Replay storage is deployment-owned and retained across controller
        // recreation. Its configured ceilings must be included in the supplied
        // durable overhead policy; opening it grants no archive coverage.
        let config = self.managed.config.clone();
        let verifier = self
            .managed
            .domain
            .blocking(move || -> Result<_> {
                let filesystem: Arc<dyn cellar_ltx::FileSystem> = Arc::new(DirectFileSystem);
                let base = PathBuf::from("/data/fleet-replay");
                std::fs::create_dir_all(&base)?;
                std::fs::File::open(&base)?.sync_all()?;
                let images = NodeExtentStore::open(
                    base.join("extents"),
                    filesystem.clone(),
                    RaftExtentLimits {
                        max_bytes: config.replay_extent_bytes,
                        ..Default::default()
                    },
                )?;
                let mut limits = ApplyLimits::default();
                limits.decode.max_image_bytes = config.image_bytes;
                let cache = config.extent_options().cache;
                let cache_pool = ScratchPool::new(ScratchLimits {
                    max_bytes: config.storage.extent_cache_bytes,
                    max_files: usize::try_from(config.storage.extent_cache_bytes / EXTENT_BYTES)?,
                })?;
                let images = CellGroupApplyImages::Extents {
                    images,
                    limits,
                    services: ReadServices {
                        now_unix_millis: Arc::new(cellar::asyncrt::wall_ms),
                        fill_random: Arc::new(|bytes| {
                            cellar::asyncrt::rng("fleet-group-archive-read").fill_bytes(bytes)
                        }),
                    },
                    cache_directory: base.join("cache"),
                    cache_pool,
                    cache,
                };
                Ok(FleetGroupArchiveVerifier::new(
                    CellGroupArchivePool::new(CellGroupArchiveLimits::default())?,
                    CellGroupArchiveTailPool::new(CellGroupArchiveTailLimits {
                        max_cells: config.cells_per_group,
                        ..Default::default()
                    })?,
                    CellGroupCheckpointPool::new(CellGroupCheckpointLimits {
                        max_sources: 1,
                        max_retained_bytes: 32 * 1024 * 1024,
                        max_cells: config.cells_per_group,
                        max_transfer_bytes: 256 * 1024 * 1024,
                        max_chunk_bytes: 64 * 1024,
                    })?,
                    ScratchPool::new(ScratchLimits {
                        max_bytes: config.storage.snapshot_scratch_bytes,
                        max_files: 8,
                    })?,
                    FleetGroupArchiveReplayOptions {
                        directory: base,
                        filesystem,
                        materialized: None,
                        extents: Some(images),
                        manifest: ManifestLimits {
                            max_cells: config.cells_per_group,
                            max_depth: 32,
                        },
                        entries: Default::default(),
                        max_pages: 32768,
                        max_page_bytes: 64 * 1024 * 1024,
                        max_retained_roots: 16,
                        max_effect_rows: 1024,
                    },
                )?)
            })?
            .await??;
        let resources = Arc::new(self.resources.clone());
        let service = self
            .managed
            .catalog
            .start_failure_recovery_service(
                self.cells.clone(),
                resources.clone(),
                resources,
                Arc::new(self.isolation.clone()),
                FleetMovementServiceOptions::default(),
                FleetFailureRecoveryOptions {
                    detection: FleetFailureDetectionOptions::default(),
                    archive: verifier,
                },
            )
            .await?;
        *self
            .recovery
            .lock()
            .map_err(|_| anyhow::anyhow!("recovery handle poisoned"))? = Some(service);
        self.recovery_status()
    }
}
