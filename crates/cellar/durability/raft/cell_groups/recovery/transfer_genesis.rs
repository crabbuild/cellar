// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Local physical preparation for an authorized, non-serving transfer child.
use super::super::*;
use crate::raft::{catalog::TransferSeedPermit, storage::LogError};
use cellar_logic::{
    cell_group::GroupHistory,
    consensus::{EngineInstanceId, ReplicaIdentity},
};
use std::sync::{atomic::AtomicBool, Arc, OnceLock};

pub(crate) struct TransferSeed {
    permit: TransferSeedPermit,
    prepared: PreparedGroupManifest,
    store: CellGroupArchiveStore,
    installed_engine: Arc<OnceLock<Arc<()>>>,
}

impl TransferSeed {
    pub(crate) fn new(
        permit: TransferSeedPermit,
        child: &PreparedCellGroupTransferChild,
    ) -> Result<Self, LogError> {
        let prepared = child.genesis().ok_or(LogError::WrongIdentity)?;
        let store = child.store();
        if prepared.marker() != &permit.genesis || !store.owner.owns(prepared) {
            return Err(LogError::WrongIdentity);
        }
        Ok(Self {
            permit,
            prepared: prepared.clone(),
            store: store.clone(),
            installed_engine: child
                .install_owner()
                .ok_or(LogError::WrongIdentity)?
                .clone(),
        })
    }

    pub(crate) fn bootstrap(&self) -> &crate::raft::storage::CellGroupBootstrap {
        &self.permit.bootstrap
    }

    pub(crate) fn genesis(&self) -> &GroupManifest {
        &self.permit.genesis
    }

    pub(crate) fn check(
        &self,
        owner: &Arc<()>,
        identity: &ReplicaIdentity<GroupHistory>,
        engine: EngineInstanceId,
    ) -> Result<(), LogError> {
        self.permit.check(owner, identity, engine)
    }

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
            super::super::retention::PublicationPin,
        ),
        LogError,
    > {
        self.store
            .require_live()
            .map_err(|error| LogError::Invalid(error.to_string()))?;
        let mut owner = self.store.storage.try_lock().map_err(|error| match error {
            std::sync::TryLockError::WouldBlock => LogError::Overloaded,
            std::sync::TryLockError::Poisoned(_) => LogError::Io("poisoned transfer store".into()),
        })?;
        self.store
            .require_live()
            .map_err(|error| LogError::Invalid(error.to_string()))?;
        let apply::Storage { tree, pages, .. } = &mut *owner;
        let root = tree
            .prepare_transfer_install(&self.permit, pages, &self.prepared)
            .map_err(super::genesis::preparation_error)?;
        let publication = root
            .begin_transfer_genesis(&self.permit, closed)
            .map_err(super::genesis::preparation_error)?;
        self.installed_engine
            .set(self.permit.owner.clone())
            .map_err(|_| LogError::WrongIdentity)?;
        self.store.close();
        Ok((root, publication))
    }
}
