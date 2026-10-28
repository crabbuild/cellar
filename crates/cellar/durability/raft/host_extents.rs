// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One node inventory, complete native retention recovery and joined GC work.
//! The node supplies physical policy; immutable namespaces select commitments.

use super::*;
use cellar_ltx::{
    internal::{ExtentRootReference, NodeExtentStore, EXTENT_BYTES},
    stream_snapshot::{ScratchLimits, ScratchPool},
};
use std::path::Path;

pub use cellar_ltx::internal::{
    CacheLimits as RaftExtentCacheLimits, NodeExtentLimits as RaftExtentLimits,
};

/// Shared physical admission for opt-in extent namespaces. Limits cover the
/// entire node, including closed roots, incoming data and private workspaces.
#[derive(Clone, Copy, Debug)]
pub struct RaftExtentOptions {
    pub inventory: RaftExtentLimits,
    pub cache: RaftExtentCacheLimits,
    pub cache_scratch: ScratchLimits,
    pub gc_interval_ms: u64,
    pub gc_batch_files: usize,
}

impl Default for RaftExtentOptions {
    fn default() -> Self {
        Self {
            inventory: RaftExtentLimits::default(),
            cache: RaftExtentCacheLimits::default(),
            cache_scratch: ScratchLimits {
                max_bytes: 256 * 1024 * 1024,
                max_files: 64,
            },
            gc_interval_ms: 1000,
            gc_batch_files: 64,
        }
    }
}

impl RaftExtentOptions {
    pub(super) fn validate(&self, execution: ExecutionLimits) -> anyhow::Result<()> {
        self.inventory.validate()?;
        self.cache_scratch.validate()?;
        let workspace = RootPrivateOptions::workspace_bounds(execution)?.reservation()?;
        let extents = execution.max_image_bytes.div_ceil(EXTENT_BYTES) as usize;
        anyhow::ensure!(
            self.inventory.max_bytes >= workspace.saturating_add(4 * execution.max_image_bytes)
                && self.inventory.max_roots >= 4
                && self.inventory.max_objects >= 4 * extents
                && self.inventory.max_root_extents >= 4 * extents
                && self.inventory.max_references >= 4,
            "extent inventory cannot admit current, snapshot, prepare and receive ownership"
        );
        anyhow::ensure!(
            self.cache.max_cached_bytes >= EXTENT_BYTES
                && self.cache.max_cached_extents > 0
                && self.cache_scratch.max_bytes >= 2 * EXTENT_BYTES
                && self.cache_scratch.max_files >= 2
                && (10..=60_000).contains(&self.gc_interval_ms)
                && (1..=4096).contains(&self.gc_batch_files),
            "invalid extent cache or reclamation bounds"
        );
        Ok(())
    }
}

pub(super) struct Owner {
    pub store: NodeExtentStore,
    cache: ScratchPool,
    options: RaftExtentOptions,
    next_gc: u64,
    busy: bool,
    failed: bool,
}

impl Owner {
    /// Startup is still parked. Rebuild retention from every durable engine
    /// group, including unregistered/closed groups, before allowing any GC.
    pub async fn open(
        log: &super::super::storage::ReplicaLog,
        directory: PathBuf,
        host: cellar_ltx::LtxHost,
        options: Option<RaftExtentOptions>,
        needed: bool,
    ) -> anyhow::Result<Option<Self>> {
        if !needed && options.is_none() {
            return Ok(None);
        }
        let options = options
            .ok_or_else(|| anyhow::anyhow!("extent namespace requires node extent storage"))?;
        let groups = log.groups()?;
        let owner = log
            .domain()
            .blocking(move || {
                let store = NodeExtentStore::open_serialized(
                    directory.join("extent-inventory"),
                    host.filesystem(),
                    options.inventory,
                )?;
                let mut retained = BTreeMap::<[u8; 32], ExtentRootReference>::new();
                for group in groups {
                    let applied = group.applied()?;
                    let snapshot_index = group.read()?.snapshot.get_metadata().index;
                    for (role, revision, image) in [
                        (1, applied.position.index.get(), applied.image),
                        (
                            2,
                            snapshot_index,
                            super::super::snapshot::retained_image(&group)?,
                        ),
                    ] {
                        let Some(image) = image else {
                            continue;
                        };
                        image.validate()?;
                        let crate::application::consensus::ImageCommitment::Extents(reference) =
                            image.commitment
                        else {
                            continue;
                        };
                        store.retain_reference(
                            super::super::rooted::marker::reference_key(&group, role),
                            revision,
                            reference,
                        )?;
                        retained.insert(reference.root().bytes(), reference);
                    }
                }
                store.retain_only(&retained.into_values().collect::<Vec<_>>())?;
                store.reclaim(options.gc_batch_files)?;
                Ok::<_, anyhow::Error>(Self {
                    store,
                    cache: ScratchPool::new(options.cache_scratch)?,
                    options,
                    next_gc: 0,
                    busy: false,
                    failed: false,
                })
            })?
            .await??;
        Ok(Some(owner))
    }

    pub fn cell_storage(&self, directory: &Path) -> sql::RootStorage {
        sql::RootStorage {
            store: self.store.clone(),
            cache_pool: self.cache.clone(),
            cache: self.options.cache,
            cache_directory: directory.join("extent-cache"),
        }
    }
}

impl Driver {
    pub(super) fn pump_extent_gc(&mut self) {
        let Some(owner) = &mut self.extents else {
            return;
        };
        let now = crate::asyncrt::mono_ms();
        if owner.busy || owner.failed {
            return;
        }
        let pressure = owner.store.usage().is_ok_and(|usage| {
            usage.root_extents >= owner.options.inventory.max_root_extents.saturating_mul(3) / 4
                || usage.roots >= owner.options.inventory.max_roots.saturating_mul(3) / 4
                || usage.bytes >= owner.options.inventory.max_bytes.saturating_mul(3) / 4
        });
        if now < owner.next_gc && !pressure {
            return;
        }
        owner.busy = true;
        owner.next_gc = now.saturating_add(owner.options.gc_interval_ms);
        let store = owner.store.clone();
        let limit = owner.options.gc_batch_files;
        let job = self.log.domain().blocking(move || store.reclaim(limit));
        self.jobs.push(Box::pin(async move {
            let result = match job {
                Ok(job) => job
                    .await
                    .map_err(anyhow::Error::from)
                    .and_then(|value| value.map_err(anyhow::Error::from)),
                Err(error) => Err(error.into()),
            };
            Job::ExtentGc(result)
        }));
    }

    pub(super) fn extent_gc_completed(&mut self, result: anyhow::Result<usize>) {
        if let Some(owner) = &mut self.extents {
            owner.busy = false;
            match result {
                Ok(removed) if removed == owner.options.gc_batch_files => owner.next_gc = 0,
                Ok(_) => (),
                Err(error) => {
                    owner.failed = true;
                    tracing::error!(event = "raft_extent_gc_failed", %error);
                }
            }
        }
    }
}
