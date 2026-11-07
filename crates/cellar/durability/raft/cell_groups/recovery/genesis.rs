// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Catalog-authorized, fresh Raft genesis. The node writer owns the sync and
//! physical root publication; neither an archive owner nor an image is a grant.
use super::super::*;
use crate::raft::{cache::GroupLog, catalog::RecoverySeedPermit, storage::LogError};
use cellar_logic::{
    cell_group::GroupHistory,
    consensus::{EngineInstanceId, ReplicaIdentity},
};
use std::sync::{atomic::AtomicBool, Arc};

pub(crate) struct RecoverySeed {
    permit: RecoverySeedPermit,
    prepared: super::PreparedRecoveryData,
}

impl RecoverySeed {
    pub(crate) fn new(
        permit: RecoverySeedPermit,
        prepared: super::PreparedRecoveryData,
    ) -> Result<Self, LogError> {
        if prepared.manifest.marker().recovery_request() != Some(permit.request())
            || prepared.manifest.marker() != permit.genesis()
            || prepared.commitment != permit.commitment()
            || !prepared.store.owner.owns(&prepared.manifest)
        {
            return Err(LogError::WrongIdentity);
        }
        Ok(Self { permit, prepared })
    }

    pub(crate) fn bootstrap(&self) -> &crate::raft::storage::CellGroupBootstrap {
        self.permit.bootstrap()
    }

    pub(crate) fn genesis(&self) -> &GroupManifest {
        self.permit.genesis()
    }

    pub(crate) fn check(
        &self,
        owner: &Arc<()>,
        identity: &ReplicaIdentity<GroupHistory>,
        engine: EngineInstanceId,
    ) -> Result<(), LogError> {
        self.permit.check(owner, identity, engine)
    }

    /// Reverify the complete closure on this actual engine's blocking writer.
    /// Hold the source/import proof until the writer finishes even on a lost reply.
    pub(crate) fn managed_binding(
        &self,
        guard: Option<&crate::raft::fleet_stop::Guard>,
        now: u64,
    ) -> Result<Option<&crate::raft::member_state::FleetGroupBinding>, LogError> {
        self.permit.managed_binding(guard, now)
    }

    pub(crate) fn prepare(
        &self,
        closed: &Arc<AtomicBool>,
    ) -> Result<
        (
            PreparedGroupManifest,
            crate::raft::cell_groups::retention::PublicationPin,
        ),
        LogError,
    > {
        let mut owner = self
            .prepared
            .store
            .storage
            .try_lock()
            .map_err(|error| match error {
                std::sync::TryLockError::WouldBlock => LogError::Overloaded,
                std::sync::TryLockError::Poisoned(_) => {
                    LogError::Io("poisoned recovery store".into())
                }
            })?;
        let apply::Storage { tree, pages, .. } = &mut *owner;
        let root = tree
            .prepare_recovery_genesis(&self.permit, pages, &self.prepared.manifest)
            .map_err(preparation_error)?;
        let publication = root
            .begin_genesis(&self.permit, closed)
            .map_err(preparation_error)?;
        // Revoke further archive admission. Entered immutable I/O retains its
        // own permits/pins, while the engine now owns this page-store lifetime.
        self.prepared.store.close();
        Ok((root, publication))
    }
}

/// One synced physical seed and independently confirmed catalog receipt.
/// It has no startup, voting or application permission. The directory must
/// select the replacement, then startup and a fresh committed activation follow.
#[derive(Clone)]
pub struct SeededCellGroupRecovery {
    request: Arc<CellGroupRecoveryRequest>,
    group: GroupLog<GroupHistory>,
    applier: CellGroupApplier,
    root: PreparedGroupManifest,
    binding: [u8; 32],
}

impl SeededCellGroupRecovery {
    pub(crate) fn new(
        request: Arc<CellGroupRecoveryRequest>,
        group: GroupLog<GroupHistory>,
        applier: CellGroupApplier,
        root: PreparedGroupManifest,
        binding: [u8; 32],
    ) -> Self {
        Self {
            request,
            group,
            applier,
            root,
            binding,
        }
    }

    pub fn request(&self) -> &CellGroupRecoveryRequest {
        &self.request
    }

    pub fn group(&self) -> &GroupLog<GroupHistory> {
        &self.group
    }

    pub fn applier(&self) -> &CellGroupApplier {
        &self.applier
    }

    pub fn manifest(&self) -> &PreparedGroupManifest {
        &self.root
    }

    pub fn binding(&self) -> [u8; 32] {
        self.binding
    }
}

impl CellGroupApplier {
    /// Preserve the already opened physical owner during archive-to-engine
    /// transfer. Only a checked recovery grant invokes this boundary.
    pub(crate) fn recover_seeded(
        group: GroupLog<GroupHistory>,
        store: &CellGroupArchiveStore,
        genesis: &GroupManifest,
    ) -> Result<(Self, PreparedGroupManifest), crate::raft::execution::ExecutionError> {
        if group
            .group_manifest()
            .map_err(crate::raft::execution::storage)?
            .as_ref()
            != Some(genesis)
            || store.route() != genesis.route()
            || !genesis.is_recovery_genesis()
        {
            return Err(crate::raft::execution::ExecutionError::Invalid(
                "recovery seed selection",
            ));
        }
        store.close();
        let applier = Self {
            group,
            storage: store.storage.clone(),
        };
        let root = applier.recover_selected()?;
        // A same-process retry can encounter cached seals. Confirm bytes and
        // persistence independently before a permanent seed receipt is written.
        root.images.visit(&mut |_, _, pin, _, _| {
            match pin {
                CellImagePin::Materialized(pin) => pin
                    .verify_and_sync()
                    .map_err(crate::raft::execution::storage)?,
                CellImagePin::Extents(pin) => {
                    for index in 0..pin.manifest().geometry().extent_count() {
                        let extent = pin.manifest().extent(index).ok_or(
                            crate::raft::execution::ExecutionError::Invalid("recovery extent"),
                        )?;
                        pin.view()
                            .read_extent(extent)
                            .map_err(crate::raft::execution::storage)?;
                    }
                }
                CellImagePin::Empty => (),
            }
            Ok::<_, crate::raft::execution::ExecutionError>(())
        })?;
        Ok((applier, root))
    }
}

pub(super) fn preparation_error(error: ManifestError) -> LogError {
    match error {
        ManifestError::Limit => LogError::Overloaded,
        ManifestError::Conflict => LogError::WrongIdentity,
        ManifestError::Invalid(message) => LogError::Invalid(message.into()),
        ManifestError::Quarantined => LogError::LostStableState,
        ManifestError::Io(error) => LogError::Io(error.to_string()),
    }
}
