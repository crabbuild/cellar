// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A private-execution observation, never an acknowledgement or query lease.

use super::{CellImagePin, CellManifestLeaf, PreparedGroupManifest};
use crate::application::consensus::cell_routing_key;
use crate::raft::{cache::GroupLog, completion::RuntimeError};
use cellar_logic::{cell_group::*, consensus::Term};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, RwLock, Weak,
};

/// A weak cache fence, never execution, image or durability authority.
#[derive(Clone)]
pub(crate) struct CellActivationFence {
    owner: Weak<()>,
    closed: Weak<std::sync::atomic::AtomicBool>,
    reclaimed: Weak<std::sync::atomic::AtomicBool>,
    cache: Weak<RwLock<crate::raft::cache::CachedGroup>>,
    leader_term: Weak<AtomicU64>,
    identity: cellar_logic::consensus::ReplicaIdentity<GroupHistory>,
    route: GroupRoute,
    term: Term,
    version: CellVersion,
}

impl CellActivationFence {
    pub(crate) fn accepts(&self, authority: &CellExecutionAuthority) -> bool {
        self.owner.ptr_eq(&Arc::downgrade(&authority.group.owner))
            && self.route == authority.route
            && self.term == authority.term
            && self.version == authority.leaf.version
            && self.current()
    }

    pub(crate) fn current(&self) -> bool {
        let (Some(closed), Some(reclaimed), Some(leader), Some(cache)) = (
            self.closed.upgrade(),
            self.reclaimed.upgrade(),
            self.leader_term.upgrade(),
            self.cache.upgrade(),
        ) else {
            return false;
        };
        if self.owner.strong_count() == 0
            || closed.load(Ordering::Acquire)
            || reclaimed.load(Ordering::Acquire)
            || leader.load(Ordering::Acquire) != self.term.get()
        {
            return false;
        }
        let Ok(cache) = cache.read() else {
            return false;
        };
        let key = cell_routing_key(self.route.history.namespace, &self.version.history.cell);
        cache
            .group_manifest
            .as_ref()
            .is_some_and(|manifest| manifest.is_active() && manifest.route() == &self.route)
            && cache.hard_state.term == self.term.get()
            && cache.membership.permits_group(&self.identity)
            && cache
                .conf_state
                .voters
                .iter()
                .chain(&cache.conf_state.voters_outgoing)
                .any(|id| *id == self.identity.member.get())
            && cache
                .manifest_pins
                .as_ref()
                .and_then(|pins| pins.get(&key))
                .is_some_and(|(leaf, _)| leaf.version == self.version)
            && leader.load(Ordering::Acquire) == self.term.get()
    }
}

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
    pub(crate) fn activation_fence(&self) -> CellActivationFence {
        CellActivationFence {
            owner: Arc::downgrade(&self.group.owner),
            closed: Arc::downgrade(&self.group.closed),
            reclaimed: Arc::downgrade(&self.group.reclaimed),
            cache: Arc::downgrade(&self.group.cache),
            leader_term: Arc::downgrade(&self.leader_term),
            identity: self.group.identity().clone(),
            route: self.route.clone(),
            term: self.term,
            version: self.leaf.version.clone(),
        }
    }

    pub(crate) fn reserve_log(
        &self,
        envelope: crate::application::consensus::EnvelopeLimits,
    ) -> Result<Arc<super::log_retention::LogReservation>, RuntimeError> {
        self.group
            .log_retention()
            .ok_or_else(|| RuntimeError::Protocol("missing cell-group log retention".into()))?
            .reserve_execution(envelope)
            .map_err(|error| match error {
                crate::raft::storage::LogError::Overloaded => RuntimeError::Overloaded,
                other => RuntimeError::Storage(other),
            })
    }

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

    pub(crate) fn term_current(&self) -> bool {
        self.leader_term.load(Ordering::Acquire) == self.term.get()
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
        if !self.term_current() {
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
        if !current.is_active()
            || cache.hard_state.term != self.term.get()
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

/// Custody of an already committed outbox claim. It retains the selected
/// image and leader fence while unrelated cells or later business writes may
/// advance. It grants neither SQL execution nor an acknowledgement.
pub(crate) struct CellEffectAuthority {
    execution: CellExecutionAuthority,
    minimum: cellar_logic::cell_group::CellVersion,
}

impl CellExecutionAuthority {
    pub(crate) fn effect_authority(
        self,
        minimum: &cellar_logic::cell_group::CellVersion,
        term: Term,
    ) -> Result<CellEffectAuthority, RuntimeError> {
        // The execution observation was minted at a ready applied barrier.
        // Its own cell has since advanced through the exact committed claim;
        // checking equality with that older leaf would invalidate every claim.
        if term != self.term
            || !minimum
                .covers(&self.leaf.version)
                .map_err(|_| RuntimeError::Invalid("effect claim history".into()))?
        {
            return Err(RuntimeError::NotReady);
        }
        let authority = CellEffectAuthority {
            execution: self,
            minimum: minimum.clone(),
        };
        authority.check()?;
        Ok(authority)
    }
}

impl CellEffectAuthority {
    pub(crate) fn check(&self) -> Result<(), RuntimeError> {
        let execution = &self.execution;
        if execution.leader_term.load(Ordering::Acquire) != execution.term.get() {
            return Err(RuntimeError::NotReady);
        }
        let cache = execution.group.read()?;
        let current = cache
            .group_manifest
            .as_ref()
            .ok_or(RuntimeError::NotReady)?;
        let key = cell_routing_key(
            execution.route.history.namespace,
            &self.minimum.history.cell,
        );
        let (leaf, _pin) = cache
            .manifest_pins
            .as_ref()
            .and_then(|pins| pins.get(&key))
            .ok_or(RuntimeError::NotReady)?;
        if !current.is_active()
            || current.route() != &execution.route
            || cache.hard_state.term != execution.term.get()
            || !cache
                .conf_state
                .voters
                .iter()
                .chain(&cache.conf_state.voters_outgoing)
                .any(|id| *id == execution.group.identity().member.get())
            || !cache.membership.permits_group(execution.group.identity())
            || !leaf
                .version
                .covers(&self.minimum)
                .map_err(|_| RuntimeError::NotReady)?
            || execution.leader_term.load(Ordering::Acquire) != execution.term.get()
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
        if !current.is_active()
            || self.term.get() == 0
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
