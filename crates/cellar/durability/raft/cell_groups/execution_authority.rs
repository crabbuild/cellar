// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A private-execution observation, never an acknowledgement or query lease.

use super::{CellImagePin, CellManifestLeaf, PreparedGroupManifest};
use crate::application::consensus::cell_routing_key;
use crate::raft::{cache::GroupLog, completion::RuntimeError};
use cellar_logic::{cell_group::*, consensus::Term};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

/// Issued only by the actual leader after its current-term applied barrier.
/// It pins one cell, not every image in the group. Other cells may advance while
/// this cell prepares; its own selected base, route or leader term may not.
/// Proposal admission must still recheck authority after physical preparation.
pub struct CellExecutionAuthority {
    group: GroupLog<GroupHistory>,
    route: GroupRoute,
    term: Term,
    leader_term: Arc<AtomicU64>,
    leaf: CellManifestLeaf,
    image: CellImagePin,
}

impl CellExecutionAuthority {
    pub(crate) fn engine_owner(&self) -> &Arc<()> {
        &self.group.owner
    }

    pub(crate) fn accepts_reconciliation(&self, proof: &CellReconciliationAuthority) -> bool {
        Arc::ptr_eq(&self.group.owner, &proof.group.owner)
            && self.route == proof.route
            && self.leaf.version.history == proof.leaf.version.history
    }

    pub(super) fn new(
        group: GroupLog<GroupHistory>,
        current: &PreparedGroupManifest,
        cell: &CellHistory,
        term: Term,
        leader_term: Arc<AtomicU64>,
    ) -> Result<Self, RuntimeError> {
        let (leaf, image) = current.pin_cell(&cell.cell).ok_or(RuntimeError::NotFound)?;
        if &leaf.version.history != cell {
            return Err(RuntimeError::Invalid("cell execution history".into()));
        }
        let authority = Self {
            group,
            route: current.marker().route().clone(),
            term,
            leader_term,
            leaf: leaf.clone(),
            image,
        };
        authority.check()?;
        Ok(authority)
    }

    pub(crate) fn belongs_to_engine(&self, owner: &Arc<()>) -> bool {
        Arc::ptr_eq(&self.group.owner, owner)
    }

    pub fn route(&self) -> &GroupRoute {
        &self.route
    }

    pub fn term(&self) -> Term {
        self.term
    }

    pub fn cell(&self) -> &CellManifestLeaf {
        &self.leaf
    }

    pub fn image(&self) -> &CellImagePin {
        &self.image
    }

    pub(crate) fn check_committed_entry(&self, bytes: &[u8]) -> Result<(), RuntimeError> {
        self.check()?;
        let cache = self.group.read()?;
        let position = self.leaf.last_mutation;
        let entry = cache
            .entries
            .get(&position.index.get())
            .ok_or(RuntimeError::NotReady)?;
        if entry.term != position.term.get()
            || entry.data.as_ref() != bytes
            || cache.hard_state.commit < position.index.get()
        {
            return Err(RuntimeError::StaleCompletion);
        }
        Ok(())
    }

    pub fn check(&self) -> Result<(), RuntimeError> {
        if self.leader_term.load(Ordering::Acquire) != self.term.get() {
            return Err(RuntimeError::NotReady);
        }
        let cache = self.group.read()?;
        let current = cache
            .group_manifest
            .as_ref()
            .ok_or(RuntimeError::NotReady)?;
        let key = cell_routing_key(
            self.route.history.namespace,
            &self.leaf.version.history.cell,
        );
        if cache.hard_state.term != self.term.get()
            || !cache
                .conf_state
                .voters
                .iter()
                .chain(&cache.conf_state.voters_outgoing)
                .any(|id| *id == self.group.identity().member.get())
            || current.route() != &self.route
            || !cache.membership.permits_group(self.group.identity())
            || cache
                .manifest_pins
                .as_ref()
                .and_then(|pins| pins.get(&key))
                .is_none_or(|(leaf, _)| leaf != &self.leaf)
            || self.leader_term.load(Ordering::Acquire) != self.term.get()
        {
            return Err(RuntimeError::NotReady);
        }
        Ok(())
    }
}

/// A durable current-term applied cut, including on a follower. This permits
/// receipt inspection and retirement of older-term producers, never new SQL
/// execution, a linearizable query, or an S3 acknowledgement.
pub struct CellReconciliationAuthority {
    group: GroupLog<GroupHistory>,
    route: GroupRoute,
    term: Term,
    leaf: CellManifestLeaf,
    image: CellImagePin,
}

impl CellReconciliationAuthority {
    pub fn route(&self) -> &GroupRoute {
        &self.route
    }

    pub(super) fn new(
        group: GroupLog<GroupHistory>,
        current: &PreparedGroupManifest,
        cell: &CellHistory,
    ) -> Result<Self, RuntimeError> {
        let (leaf, image) = current.pin_cell(&cell.cell).ok_or(RuntimeError::NotFound)?;
        if &leaf.version.history != cell {
            return Err(RuntimeError::Invalid("cell reconciliation history".into()));
        }
        let proof = Self {
            group,
            route: current.marker().route().clone(),
            term: current.marker().position().term,
            leaf: leaf.clone(),
            image,
        };
        proof.check()?;
        Ok(proof)
    }

    pub(crate) fn belongs_to_engine(&self, owner: &Arc<()>) -> bool {
        Arc::ptr_eq(&self.group.owner, owner)
    }

    pub fn term(&self) -> Term {
        self.term
    }

    pub fn cell(&self) -> &CellManifestLeaf {
        &self.leaf
    }

    pub fn image(&self) -> &CellImagePin {
        &self.image
    }

    pub fn check(&self) -> Result<(), RuntimeError> {
        let cache = self.group.read()?;
        let current = cache
            .group_manifest
            .as_ref()
            .ok_or(RuntimeError::NotReady)?;
        let key = cell_routing_key(
            self.route.history.namespace,
            &self.leaf.version.history.cell,
        );
        if self.term.get() == 0
            || cache.hard_state.term != self.term.get()
            || current.position().term != self.term
            || current.position().index.get() > cache.hard_state.commit
            || current.route() != &self.route
            || !cache.membership.permits_group(self.group.identity())
            || cache
                .manifest_pins
                .as_ref()
                .and_then(|pins| pins.get(&key))
                .is_none_or(|(leaf, _)| leaf != &self.leaf)
        {
            return Err(RuntimeError::NotReady);
        }
        Ok(())
    }
}
