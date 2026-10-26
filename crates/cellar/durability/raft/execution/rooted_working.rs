// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! The private command database on tracked, disposable extent residency.
//! Its SQL policy is the same CommandDatabase used by materialized execution.

use super::{
    images::{Images, Options, RootState},
    *,
};
use crate::application::consensus::{CellUpdate, ImageCommitment, LtxImage};
use crate::storage::CommandDatabase;
use anyhow::{ensure, Result};
use cellar_ltx::{
    internal::{
        db::{observe_extents, ExtentObservation},
        ExtentManifest, ExtentWorkspace, Geometry, ImageBinding, WorkspaceOptions, EXTENT_BYTES,
    },
    Db, TXID,
};
use std::sync::Arc;

pub(super) const WAL_BYTES: u64 = 512 * 1024 * 1024;
const RECYCLE_WAL_BYTES: u64 = WAL_BYTES / 2;

pub(super) struct RootWorking {
    pub database: CommandDatabase,
    capture: Db,
    workspace: ExtentWorkspace,
    observation: Option<ExtentObservation>,
    initial: RootState,
    frozen: Option<RootState>,
    history: [u8; 32],
}

impl RootWorking {
    pub fn open(
        options: &Options,
        base: Option<&crate::raft::rooted::RootPublishedImage>,
        history: [u8; 32],
    ) -> Result<Self> {
        let Images::Extents {
            store,
            workspace: admission,
            cache_directory,
            cache_pool,
            cache,
        } = &options.images
        else {
            anyhow::bail!("extent storage required");
        };
        ensure!(
            !cache_directory.starts_with(&options.directory),
            "read cache must be outside the private directory"
        );
        let filesystem = options.host.filesystem();
        let path = options.directory.join("working.sqlite");
        crate::raft::working::clean(filesystem.as_ref(), &options.directory, &path)?;
        let image = match base {
            Some(image) => image.image(),
            None => Arc::new(store.seal_image_root(
                ExtentManifest::new(
                    ImageBinding {
                        history,
                        schema: options.application_digest,
                        epoch: 1,
                        txid: 0,
                        checksum: 0,
                    },
                    Geometry::new(0, 0)?,
                    &[],
                )?,
                &mut vec![0; EXTENT_BYTES as usize],
            )?),
        };
        ensure!(
            image.manifest().binding().history == history
                && image.manifest().binding().schema == options.application_digest,
            "working extent history/schema differs"
        );
        let initial = RootState::of(&image);
        cellar_ltx::host::create_dir_all_durable(filesystem.as_ref(), cache_directory)?;
        let workspace = ExtentWorkspace::new(
            image,
            WorkspaceOptions {
                path: path.clone(),
                sqlite_vfs: options.sqlite_vfs.clone(),
                filesystem,
                max_image_bytes: options.limits.max_image_bytes,
                max_wal_bytes: WAL_BYTES,
                cache_directory: cache_directory.clone(),
                scratch: cache_pool.clone(),
                cache: *cache,
                retention: Some(admission.retention()),
            },
        )?;
        let mut capture =
            Db::open_disposable_with_host(&path, options.host.clone(), Some(workspace.vfs_name()))?;
        // Recycle a quiescent writer at a bounded WAL high-water mark. Do not
        // let capture's in-place checkpoint fallback build a full-image delta.
        // The VFS independently caps actual WAL writes before capture runs.
        capture.min_checkpoint_page_n = u32::MAX;
        capture.truncate_page_n = 0;
        capture.checkpoint_interval = std::time::Duration::ZERO;
        capture.disable_disposable_close_checkpoint()?;
        let database = CommandDatabase::open(
            &path,
            Some(workspace.vfs_name()),
            options.limits.max_image_bytes,
        )?;
        database.framework().set_db_config(
            rusqlite::config::DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE,
            true,
        )?;
        receipts::install(database.framework())?;
        if initial.image.txid != 0 {
            capture.seed_continuation(
                TXID(initial.image.txid),
                (initial.geometry.length() / u64::from(initial.geometry.page_size())) as u32,
            )?;
        }
        Ok(Self {
            database,
            capture,
            workspace,
            observation: None,
            initial,
            frozen: None,
            history,
        })
    }

    pub fn matches(
        &self,
        options: &Options,
        base: &crate::raft::rooted::RootPublishedImage,
    ) -> bool {
        self.frozen == Some(RootState::of(&base.image()))
            && options
                .host
                .metadata(&self.capture.wal_path())
                .is_ok_and(|metadata| metadata.len <= RECYCLE_WAL_BYTES)
    }

    pub fn begin_mutation(&mut self) {
        self.frozen = None;
    }

    pub fn observation_work(&self) -> Option<cellar_ltx::internal::db::ObservationWork> {
        self.observation.as_ref().map(ExtentObservation::work)
    }

    pub fn trim(
        &self,
        options: &Options,
        base: Option<&crate::raft::rooted::RootPublishedImage>,
    ) -> Result<()> {
        crate::raft::working::trim_capture(
            &self.capture,
            &options.host,
            options.limits.envelope.max_files,
            TXID(base.map_or(0, |base| base.image().manifest().binding().txid)),
        )
    }

    pub fn capture(
        &mut self,
        options: &Options,
        scope: &cellar_logic::consensus::GroupScope,
        proposal: ProposalId,
        base: Option<&crate::raft::rooted::RootPublishedImage>,
    ) -> Result<CellUpdate> {
        let before = base
            .map(|base| RootState::of(&base.image()))
            .unwrap_or(self.initial);
        let (position, files) = crate::raft::working::capture_update(
            &mut self.capture,
            &self.database,
            &options.limits,
            TXID(before.image.txid),
        )?;
        let (length, page_size) = self.database.geometry(options.limits.max_image_bytes)?;
        let history = self.history;
        let observed = observe_extents(
            &self.capture,
            self.observation.as_ref(),
            ImageBinding {
                history,
                schema: options.application_digest,
                epoch: before.image.epoch,
                txid: position.txid.0,
                checksum: position.post_apply_checksum,
            },
            length,
            WAL_BYTES,
        )?;
        ensure!(
            observed.manifest().geometry().page_size() == page_size,
            "root capture geometry changed"
        );
        let reference = cellar_ltx::internal::ExtentRootReference::new(
            observed.manifest().root(),
            u32::try_from(observed.manifest().encode()?.len())?,
        )?;
        let result = LtxImage {
            epoch: before.image.epoch,
            txid: position.txid.0,
            checksum: position.post_apply_checksum,
            commitment: ImageCommitment::Extents(reference),
        };
        self.frozen = Some(RootState {
            image: result,
            geometry: observed.manifest().geometry(),
        });
        self.observation = Some(observed);
        Ok(CellUpdate {
            scope: scope.clone(),
            application_digest: options.application_digest,
            proposal,
            base: before.image,
            result,
            files,
        })
    }

    pub fn close(self) -> Result<()> {
        drop(self.database);
        self.capture.close()?;
        drop(self.workspace);
        Ok(())
    }
}
