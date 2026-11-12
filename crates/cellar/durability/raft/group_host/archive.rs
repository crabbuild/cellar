// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Startup opens preprovisioned archive identities. An activated transfer may
//! still rely on its source archive closure while its own archive is pending.
use super::*;

pub(super) struct Pools {
    io: CellGroupArchivePool,
    owners: CellGroupArchiveUploadPool,
    tails: CellGroupArchiveTailPool,
    checkpoints: CellGroupCheckpointPool,
}

impl Pools {
    pub fn new(
        options: Option<CellGroupNodeArchiveOptions>,
        checkpoint: Option<CellGroupCheckpointPool>,
    ) -> anyhow::Result<Option<Self>> {
        options
            .map(|o| {
                Ok(Self {
                    io: CellGroupArchivePool::new(o.io)?,
                    owners: CellGroupArchiveUploadPool::new(o.uploads)?,
                    tails: CellGroupArchiveTailPool::new(o.tails)?,
                    checkpoints: checkpoint.ok_or_else(|| {
                        anyhow::anyhow!("archive requires complete group checkpoints")
                    })?,
                })
            })
            .transpose()
    }

    pub async fn register(
        &self,
        prepared: &Prepared,
        controller: &CellGroupController,
        log: &super::super::storage::ReplicaLog,
        directory: &std::path::Path,
        limits: CellGroupNodeLimits,
        options: &super::super::host::RaftHostOptions,
    ) -> anyhow::Result<(
        crate::application::consensus::group_client::resources::Images,
        Option<resources::CollectibleExtents>,
        CellGroupArchiveProvisioning,
    )> {
        // All verifier groups share separate node-wide physical inventories.
        // Serving pins or voter images cannot satisfy independent archive replay.
        // The checkpoint receiver creates anonymous image spools in this
        // node-owned directory. Its receive API expects an existing parent;
        // initialize and sync it before any archive job can publish admission.
        let scratch_directory = directory.join("archive-spools");
        let scratch = scratch_directory.clone();
        let filesystem = crate::asyncrt::fs();
        log.domain()
            .blocking(move || {
                cellar_ltx::host::create_dir_all_durable(filesystem.as_ref(), &scratch)
            })?
            .await??;
        let base = directory.join("archive-verifier");
        let images =
            resources::Images::open(log, base.clone(), options, &prepared.namespaces).await?;
        let collectible = images.adopt_verifier(log).await?;
        for assignment in prepared
            .groups
            .iter()
            .filter(|a| a.placement == RaftGroupPlacement::Replica)
        {
            // The replacement closure owns the sealed source archive. A
            // historical voter participates in Raft only for retirement.
            if assignment.sealed_transfer_source {
                continue;
            }
            let namespace = &prepared.namespaces[assignment.namespace];
            if assignment.pending_transfer_archive
                && !namespace
                    .directory
                    .has_group_archive_binding(&assignment.birth)
                    .await?
            {
                // The verified transfer closure still owns recovery. Leave
                // archive publication and explicit S3 coverage unavailable
                // until the operator creates this child's archive binding.
                continue;
            }
            namespace
                .directory
                .open_group_archive(&log.domain(), &self.io, &assignment.birth)
                .await?;
            let route = assignment.birth.route().clone();
            let path = ManifestPageStore::group_pages_path(&base, route.history);
            let backend = images.apply(namespace.directory.namespace_config(), options)?;
            let store = log
                .domain()
                .blocking(move || {
                    Ok::<_, anyhow::Error>(CellGroupArchiveStore::new(
                        route,
                        resources::pages(&path, limits)?,
                        backend,
                        limits.manifest,
                        limits.entries,
                        limits.max_effect_rows,
                    )?)
                })?
                .await??;
            controller
                .archive(CellGroupArchiveRegistration {
                    publisher: CellGroupArchivePublisher::GroupLeader,
                    directory: namespace.directory.clone(),
                    birth: assignment.birth.clone(),
                    io: self.io.clone(),
                    owners: self.owners.clone(),
                    tails: self.tails.clone(),
                    checkpoints: self.checkpoints.clone(),
                    store,
                    scratch: log.snapshot_scratch(),
                    scratch_directory: scratch_directory.clone(),
                })?
                .response()
                .await?;
        }
        let namespaces = prepared
            .namespaces
            .iter()
            .map(|namespace| {
                Ok(CellGroupProvisionNamespace {
                    directory: namespace.directory.clone(),
                    images: images.apply(namespace.directory.namespace_config(), options)?,
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok((
            images.observer(),
            collectible,
            CellGroupArchiveProvisioning {
                namespaces,
                io: self.io.clone(),
                owners: self.owners.clone(),
                tails: self.tails.clone(),
                checkpoints: self.checkpoints.clone(),
            },
        ))
    }
}
