// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Configured partition management shares the node's private verifier stores.
use super::*;
use crate::management::RaftFleetControl;
use crate::raft::fleet_catalog::{FleetGroupArchiveReplayOptions, FleetGroupArchiveVerifier};

pub(super) fn archive_verifier(
    archive: &CellGroupArchiveProvisioning,
    log: &super::super::storage::ReplicaLog,
    directory: &std::path::Path,
    limits: CellGroupNodeLimits,
) -> anyhow::Result<FleetGroupArchiveVerifier> {
    let mut materialized = None;
    let mut extents = None;
    for namespace in &archive.namespaces {
        match &namespace.images {
            CellGroupApplyImages::Materialized { .. } if materialized.is_none() => {
                materialized = Some(namespace.images.clone())
            }
            CellGroupApplyImages::Extents { .. } if extents.is_none() => {
                extents = Some(namespace.images.clone())
            }
            _ => (),
        }
    }
    Ok(FleetGroupArchiveVerifier::new(
        archive.io.clone(),
        archive.tails.clone(),
        archive.checkpoints.clone(),
        log.snapshot_scratch(),
        FleetGroupArchiveReplayOptions {
            directory: directory.join("partition-replay"),
            filesystem: crate::asyncrt::fs(),
            materialized,
            extents,
            manifest: limits.manifest,
            entries: limits.entries,
            max_pages: limits.manifest_pages,
            max_page_bytes: limits.manifest_bytes,
            max_retained_roots: limits.retained_roots,
            max_effect_rows: limits.max_effect_rows,
        },
    )?)
}

pub(super) fn prepare(
    prepared: &Prepared,
    archive: &CellGroupArchiveProvisioning,
    log: &super::super::storage::ReplicaLog,
    directory: &std::path::Path,
    limits: CellGroupNodeLimits,
    options: &super::super::host::RaftHostOptions,
) -> anyhow::Result<Option<RaftFleetControl>> {
    let Some(config) = &options.automatic_partitions else {
        return Ok(None);
    };
    let managed = options
        .managed
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("automatic partition enrollment"))?;
    let verifier = archive_verifier(archive, log, directory, limits)?;
    let catalog = managed
        .authorization
        .stop_catalog()
        .owned_client(log.domain());
    Ok(Some(
        RaftFleetControl::from_domain(
            log.domain(),
            catalog,
            prepared
                .namespaces
                .iter()
                .map(|n| n.directory.clone())
                .collect(),
        )?
        .with_archive_pool(archive.io.clone())
        .with_transition_preparer(verifier, CellGroupRecoveryPool::new(config.recovery)?),
    ))
}
