// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Complete physical group replay for managed lifecycle verification. The
//! supplied inventories belong to the verifier, separately from serving data.
use super::*;
use crate::application::consensus::group_protocol::GroupEntryLimits;
use crate::raft::catalog::{CellGroupArchiveBindingError, RaftImageStorage, RangeDirectory};
use crate::raft::cell_groups::{
    CellGroupApplyImages, CellGroupArchiveCheckpointReceiver, CellGroupArchivePool,
    CellGroupArchiveRegistry, CellGroupArchiveStore, CellGroupArchiveTailPool,
    CellGroupCheckpointError, CellGroupCheckpointPool, ManifestLimits, ManifestPageStore,
};
use cellar_logic::cell_group::GroupRoute;
use cellar_ltx::{stream_snapshot::ScratchPool, FileSystem};
use sha2::{Digest, Sha256};
use std::path::{Component, PathBuf};

/// Explicit verifier-owned physical stores and existing per-group page bounds.
/// Image, checkpoint, tail and scratch pools are shared across all histories.
/// Use private inventories: serving stores cannot substitute for S3 replay.
pub struct FleetGroupArchiveReplayOptions {
    pub directory: PathBuf,
    pub filesystem: Arc<dyn FileSystem>,
    pub materialized: Option<CellGroupApplyImages>,
    pub extents: Option<CellGroupApplyImages>,
    pub manifest: ManifestLimits,
    pub entries: GroupEntryLimits,
    pub max_pages: usize,
    pub max_page_bytes: u64,
    pub max_retained_roots: usize,
    pub max_effect_rows: u32,
}

/// Opens independent receivers on the caller's execution domain. This owns no
/// Raft log, application callback, admission authority or remote retention lease.
#[derive(Clone)]
pub struct FleetGroupArchiveVerifier {
    archives: CellGroupArchivePool,
    tails: CellGroupArchiveTailPool,
    checkpoints: CellGroupCheckpointPool,
    scratch: ScratchPool,
    options: Arc<FleetGroupArchiveReplayOptions>,
}

impl FleetGroupArchiveVerifier {
    pub fn new(
        archives: CellGroupArchivePool,
        tails: CellGroupArchiveTailPool,
        checkpoints: CellGroupCheckpointPool,
        scratch: ScratchPool,
        options: FleetGroupArchiveReplayOptions,
    ) -> Result<Self> {
        if !options.directory.is_absolute()
            || options.directory.as_os_str().len() > 3800
            || options
                .directory
                .components()
                .any(|p| matches!(p, Component::ParentDir))
            || options.materialized.is_none() && options.extents.is_none()
            || options
                .materialized
                .as_ref()
                .is_some_and(|i| !matches!(i, CellGroupApplyImages::Materialized { .. }))
            || options
                .extents
                .as_ref()
                .is_some_and(|i| !matches!(i, CellGroupApplyImages::Extents { .. }))
            || !(1..=1_000_000).contains(&options.max_effect_rows)
        {
            return Err(FleetCatalogError::Invalid("group archive replay storage"));
        }
        ManifestPageStore::validate_limits(
            options.max_pages,
            options.max_page_bytes,
            options.max_retained_roots,
        )
        .and_then(|()| options.manifest.validate())
        .map_err(|_| FleetCatalogError::Limit)?;
        options
            .entries
            .validate()
            .map_err(|_| FleetCatalogError::Invalid("group archive entry limits"))?;
        for images in options.materialized.iter().chain(options.extents.iter()) {
            images
                .validate()
                .map_err(|e| FleetCatalogError::Storage(e.into()))?;
        }
        Ok(Self {
            archives,
            tails,
            checkpoints,
            scratch,
            options: Arc::new(options),
        })
    }

    pub(crate) fn tails(&self) -> &CellGroupArchiveTailPool {
        &self.tails
    }

    /// Reopen the immutable namespace-owned registry for this exact rooted
    /// physical history. Never initialize counters or select a cell image.
    pub(crate) async fn open(
        &self,
        domain: &DomainToken,
        namespace: &RangeDirectory,
        history: &FleetHistory,
    ) -> Result<CellGroupArchiveRegistry> {
        domain
            .run(async {
                let scope = history
                    .binding()
                    .scope()
                    .map_err(|_| FleetCatalogError::Corrupt("archive history"))?;
                let birth = namespace.materialize_selected_history(scope).await?;
                if birth.route().history != scope
                    || birth.route().birth_digest != history.birth_digest()
                    || birth.route().schema_digest != history.schema_digest()
                {
                    return Err(FleetCatalogError::Conflict);
                }
                namespace
                    .open_group_archive(domain, &self.archives, &birth)
                    .await
                    .map_err(|e| match e {
                        CellGroupArchiveBindingError::Catalog(e) => FleetCatalogError::from(e),
                        CellGroupArchiveBindingError::Archive(e) => FleetCatalogError::from(e),
                    })
            })
            .await
            .map_err(|_| FleetCatalogError::Closed)?
    }

    /// Allocate a complete receiver under the existing checkpoint/scratch
    /// admissions. Deterministic group paths keep retries bounded; a prior
    /// entered worker retains its directory lock until physical work exits.
    pub(crate) async fn receiver(
        &self,
        domain: &DomainToken,
        route: &GroupRoute,
        storage: RaftImageStorage,
    ) -> Result<CellGroupArchiveCheckpointReceiver> {
        let route = route.clone();
        let mut images = match storage {
            RaftImageStorage::Materialized => self.options.materialized.as_ref(),
            RaftImageStorage::Extents => self.options.extents.as_ref(),
        }
        .ok_or(FleetCatalogError::Invalid(
            "missing group archive image inventory",
        ))?
        .clone();
        let mut digest = Sha256::new();
        digest.update(b"cellar-fleet-group-replay-directory-1");
        digest.update(route.history.cluster.bytes());
        digest.update(route.history.namespace.bytes());
        digest.update(route.history.group.bytes());
        digest.update(route.history.incarnation.bytes());
        let key = hex::encode(digest.finalize());
        if let CellGroupApplyImages::Extents {
            cache_directory, ..
        } = &mut images
        {
            *cache_directory = self.options.directory.join("cache").join(&key);
        }
        let options = self.options.clone();
        let pages = options.directory.join("groups").join(key).join("pages");
        let admission = self
            .checkpoints
            .prepare_archive_receive(options.max_pages)
            .map_err(checkpoint_error)?;
        let scratch = self.scratch.clone();
        domain
            .blocking(move || {
                let pages = ManifestPageStore::open(
                    &pages,
                    options.filesystem.clone(),
                    options.max_pages,
                    options.max_page_bytes,
                    options.max_retained_roots,
                )
                .map_err(|e| FleetCatalogError::Storage(e.into()))?;
                let store = CellGroupArchiveStore::new(
                    route,
                    pages,
                    images,
                    options.manifest,
                    options.entries,
                    options.max_effect_rows,
                )
                .map_err(|e| FleetCatalogError::Storage(e.into()))?;
                admission
                    .receive(store, scratch, options.directory.join("scratch"))
                    .map_err(checkpoint_error)
            })
            .map_err(|_| FleetCatalogError::Closed)?
            .await
            .map_err(|e| FleetCatalogError::Storage(e.into()))?
    }
}

fn checkpoint_error(error: CellGroupCheckpointError) -> FleetCatalogError {
    match error {
        CellGroupCheckpointError::Invalid(reason) => FleetCatalogError::Invalid(reason),
        CellGroupCheckpointError::Limit => FleetCatalogError::Limit,
        CellGroupCheckpointError::Busy => FleetCatalogError::Overloaded,
        error => FleetCatalogError::Storage(error.into()),
    }
}
