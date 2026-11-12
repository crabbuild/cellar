// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Dynamic groups reuse the node's independent verifier inventory and pools.
//! Preparing an owner opens existing archive identity; it never initializes one.
use super::*;
use crate::raft::cell_groups::controller::provision::CellGroupProvisioning;

pub(crate) struct Provisioning {
    pub namespaces: Vec<CellGroupProvisionNamespace>,
    pub io: CellGroupArchivePool,
    pub owners: CellGroupArchiveUploadPool,
    pub tails: CellGroupArchiveTailPool,
    pub checkpoints: CellGroupCheckpointPool,
}

impl Provisioning {
    pub(in crate::raft::cell_groups::controller) fn metadata(
        &self,
        settings: &CellGroupProvisioning,
    ) -> std::result::Result<usize, CellGroupControllerError> {
        let invalid = || CellGroupControllerError::Invalid("archive provisioning resources");
        if self.namespaces.len() != settings.namespaces.len() {
            return Err(invalid());
        }
        let mut bytes = seed::JOURNAL_WORKSPACE_BYTES;
        for (profile, serving) in self.namespaces.iter().zip(&settings.namespaces) {
            if !profile.directory.same_owner(&serving.directory)
                || std::mem::discriminant(&profile.images)
                    != std::mem::discriminant(&serving.images)
            {
                return Err(invalid());
            }
            profile
                .images
                .validate()
                .map_err(|e| CellGroupControllerError::Apply(Arc::new(e)))?;
            bytes = bytes
                .checked_add(profile.directory.cache_capacity_bytes())
                .and_then(|n| n.checked_add(8192))
                .ok_or_else(invalid)?;
        }
        Ok(bytes)
    }

    pub(in crate::raft::cell_groups::controller) async fn prepare(
        &self,
        directory: &RangeDirectory,
        birth: &CellGroupBirth,
        settings: &CellGroupProvisioning,
        runtime: &CellGroupRuntime,
        existing: Option<CellGroupArchiveRegistration>,
    ) -> std::result::Result<CellGroupArchiveRegistration, CellGroupControllerError> {
        let profile = self
            .namespaces
            .iter()
            .find(|p| p.directory.same_owner(directory))
            .ok_or(CellGroupControllerError::Invalid(
                "archive provisioning namespace owner",
            ))?;
        // Repeat identity/counter validation on retries, even when retaining the
        // same local store after an uncertain attachment. No absence fallback.
        directory
            .open_group_archive(&runtime.log().domain(), &self.io, birth)
            .await
            .map_err(|e| CellGroupControllerError::Archive(Arc::new(Error::Binding(e))))?;
        if let Some(existing) = existing {
            if existing.birth.route() != birth.route() || !existing.directory.same_owner(directory)
            {
                return Err(CellGroupControllerError::Invalid(
                    "retained archive provisioning owner",
                ));
            }
            return Ok(existing);
        }
        let route = birth.route().clone();
        let path = ManifestPageStore::group_pages_path(
            &settings.directory.join("archive-verifier"),
            route.history,
        );
        let filesystem = settings.filesystem.clone();
        let images = profile.images.clone();
        let (max_pages, max_bytes, roots) = (
            settings.max_pages,
            settings.max_page_bytes,
            settings.max_retained_roots,
        );
        let (manifest, entries, effects) = (
            settings.manifest,
            settings.entries,
            settings.max_effect_rows,
        );
        let store = runtime
            .log()
            .domain()
            .blocking(move || {
                let pages = ManifestPageStore::open(&path, filesystem, max_pages, max_bytes, roots)
                    .map_err(crate::raft::execution::storage)?;
                CellGroupArchiveStore::new(route, pages, images, manifest, entries, effects)
            })
            .map_err(|_| CellGroupControllerError::Closed)?
            .await
            .map_err(|_| CellGroupControllerError::Closed)?
            .map_err(|e| CellGroupControllerError::Apply(Arc::new(e)))?;
        Ok(CellGroupArchiveRegistration {
            publisher: CellGroupArchivePublisher::GroupLeader,
            directory: directory.clone(),
            birth: birth.clone(),
            io: self.io.clone(),
            owners: self.owners.clone(),
            tails: self.tails.clone(),
            checkpoints: self.checkpoints.clone(),
            store,
            scratch: runtime.log().snapshot_scratch(),
            scratch_directory: settings.directory.join("archive-spools"),
        })
    }
}
