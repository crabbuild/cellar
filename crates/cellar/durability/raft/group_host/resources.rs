// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One serving image inventory and one independent archive-verifier inventory
//! per node. Namespace/cell handles never open another physical store.
use super::*;
use crate::raft::{
    catalog::{NamespaceConfig, RaftImageStorage},
    execution::PrivateOptions,
    host::RaftHostOptions,
    storage::ReplicaLog,
};
use cellar_ltx::{
    generation::{GenerationStore, ReadServices},
    internal::NodeExtentStore,
    stream_snapshot::ScratchPool,
};
use rand::RngCore;
use std::path::Path;

#[derive(Clone)]
pub(super) struct Images {
    directory: PathBuf,
    materialized: Option<GenerationStore>,
    extents: Option<NodeExtentStore>,
    cache: Option<ScratchPool>,
    accounting: Option<cellar_ltx::generation::GenerationAccounting>,
}

/// Constructed only after this owner's complete reference recovery. The live
/// collector can delete eligible physical files, but cannot change references.
pub(super) struct CollectibleExtents(NodeExtentStore);

impl CollectibleExtents {
    pub(super) fn store(&self) -> &NodeExtentStore {
        &self.0
    }
}

impl Images {
    pub async fn open(
        log: &ReplicaLog,
        directory: PathBuf,
        options: &RaftHostOptions,
        namespaces: &[Namespace],
    ) -> anyhow::Result<Self> {
        let materialized = namespaces.iter().any(|n| {
            n.directory.namespace_config().image_storage == RaftImageStorage::Materialized
        });
        let extents = namespaces
            .iter()
            .any(|n| n.directory.namespace_config().image_storage == RaftImageStorage::Extents);
        let limits = options.images;
        let extent_options = options.extents;
        let scratch = log.snapshot_scratch();
        let fs = crate::asyncrt::fs();
        log.domain()
            .blocking(move || {
                let accounting =
                    materialized.then(cellar_ltx::generation::GenerationAccounting::default);
                let materialized = materialized
                    .then(|| {
                        GenerationStore::open_with_scratch(
                            directory.join("images"),
                            fs.clone(),
                            limits,
                            accounting.as_ref(),
                            scratch,
                        )
                    })
                    .transpose()?;
                let (extents, cache) = if extents {
                    let options = extent_options.ok_or_else(|| {
                        anyhow::anyhow!("extent namespace requires node inventory")
                    })?;
                    (
                        Some(NodeExtentStore::open_serialized(
                            directory.join("extent-inventory"),
                            fs,
                            options.inventory,
                        )?),
                        Some(ScratchPool::new(options.cache_scratch)?),
                    )
                } else {
                    (None, None)
                };
                Ok(Self {
                    directory,
                    materialized,
                    extents,
                    cache,
                    accounting,
                })
            })?
            .await?
    }

    /// Startup has recovered every physical group's current/checkpoint graph,
    /// and no peer or public route can enter yet. Those complete graph pins
    /// replace the conservative host references loaded from filenames. Keep
    /// the appliers alive through physical completion, including cancellation.
    pub async fn adopt_recovered(
        &self,
        log: &ReplicaLog,
        appliers: &[CellGroupApplier],
    ) -> anyhow::Result<Option<CollectibleExtents>> {
        let inventory = log.cell_groups()?;
        anyhow::ensure!(
            inventory.len() == appliers.len()
                && inventory.iter().all(|group| appliers
                    .iter()
                    .any(|a| a.history() == group.identity().scope)),
            "image adoption requires complete physical group inventory"
        );
        let images = self.clone();
        let retained = appliers.to_vec();
        log.domain()
            .blocking(move || -> anyhow::Result<_> {
                if let Some(store) = &images.materialized {
                    store.retain_only(&[])?;
                }
                if let Some(store) = &images.extents {
                    store.retain_only(&[])?;
                }
                let inventory = images.extents.map(CollectibleExtents);
                drop(retained);
                Ok(inventory)
            })?
            .await?
    }

    /// This separately locked verifier has no durable local selectors. Call
    /// before registering its first receive/replay owner. Remote holds still
    /// require fresh verification; cached filenames supply no recovery proof.
    pub async fn adopt_verifier(
        &self,
        log: &ReplicaLog,
    ) -> anyhow::Result<Option<CollectibleExtents>> {
        let images = self.clone();
        log.domain()
            .blocking(move || -> anyhow::Result<_> {
                if let Some(store) = &images.materialized {
                    store.retain_only(&[])?;
                }
                if let Some(store) = &images.extents {
                    store.retain_only(&[])?;
                }
                Ok(images.extents.map(CollectibleExtents))
            })?
            .await?
    }

    pub fn observer(&self) -> crate::application::consensus::group_client::resources::Images {
        crate::application::consensus::group_client::resources::Images {
            materialized: self.accounting.clone(),
            extents: self.extents.as_ref().map(NodeExtentStore::observer),
        }
    }

    pub fn sql(
        &self,
        namespace: &NamespaceConfig,
        options: &RaftHostOptions,
    ) -> anyhow::Result<CellSqlOptions> {
        let directory = self
            .directory
            .join("sql")
            .join(hex::encode(namespace.cluster.bytes()))
            .join(hex::encode(namespace.namespace.bytes()));
        Ok(match namespace.image_storage {
            RaftImageStorage::Materialized => CellSqlOptions::Materialized(PrivateOptions {
                directory,
                host: host(),
                sqlite_vfs: None,
                images: self
                    .materialized
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("missing materialized inventory"))?,
                query_services: services(),
                application_digest: namespace.schema_digest,
                limits: options.execution,
            }),
            RaftImageStorage::Extents => CellSqlOptions::Extents(CellSqlRootOptions {
                host: host(),
                sqlite_vfs: None,
                images: self
                    .extents
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("missing extent inventory"))?,
                query_services: services(),
                application_digest: namespace.schema_digest,
                limits: options.execution,
                cache_directory: directory,
                cache_pool: self
                    .cache
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("missing extent cache admission"))?,
                cache: options
                    .extents
                    .ok_or_else(|| anyhow::anyhow!("missing extent cache limits"))?
                    .cache,
            }),
        })
    }

    pub fn apply(
        &self,
        namespace: &NamespaceConfig,
        options: &RaftHostOptions,
    ) -> anyhow::Result<CellGroupApplyImages> {
        Ok(match self.sql(namespace, options)? {
            CellSqlOptions::Materialized(o) => CellGroupApplyImages::Materialized {
                images: o.images,
                services: o.query_services,
            },
            CellSqlOptions::Extents(o) => CellGroupApplyImages::Extents {
                images: o.images,
                services: o.query_services,
                limits: options.images.apply,
                cache_directory: o.cache_directory,
                cache_pool: o.cache_pool,
                cache: o.cache,
            },
        })
    }
}

pub(super) fn pages(path: &Path, limits: CellGroupNodeLimits) -> anyhow::Result<ManifestPageStore> {
    Ok(ManifestPageStore::open(
        path,
        crate::asyncrt::fs(),
        limits.manifest_pages,
        limits.manifest_bytes,
        limits.retained_roots,
    )?)
}

fn services() -> ReadServices {
    ReadServices {
        now_unix_millis: Arc::new(crate::asyncrt::wall_ms),
        fill_random: Arc::new(|b| crate::asyncrt::rng("cell-group-image-read").fill_bytes(b)),
    }
}

fn host() -> cellar_ltx::LtxHost {
    let fs = crate::asyncrt::fs();
    let age = fs.clone();
    let read = fs.clone();
    cellar_ltx::LtxHost::new(
        crate::asyncrt::wall_ms,
        move |path| {
            Ok(std::time::Duration::from_millis(
                crate::asyncrt::wall_ms()
                    .saturating_sub(age.metadata(path)?.modified_unix_millis)
                    .max(0) as u64,
            ))
        },
        move |path| {
            let fs = read.clone();
            async move {
                crate::asyncrt::try_blocking(move || fs.read(&path))
                    .map_err(|e| std::io::Error::other(e.to_string()))?
                    .await
                    .map_err(|e| std::io::Error::other(e.to_string()))?
            }
        },
        |job| async move {
            crate::asyncrt::try_blocking(job)
                .map_err(|e| cellar_ltx::HostTaskError::new(e.to_string()))?
                .await
                .map_err(|e| cellar_ltx::HostTaskError::new(e.to_string()))
        },
    )
    .with_filesystem(fs)
}

#[cfg(all(test, not(cellar_internal_tests)))]
#[path = "resources_tests.rs"]
mod tests;
