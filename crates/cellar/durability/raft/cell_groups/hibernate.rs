// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Replicated park preparation evidence. Local park and wake authority
//! require separate durable records and acknowledgements from the voters.

use super::{GroupManifest, ManifestError};
use crate::raft::{
    cache::{CachedGroup, GroupLog, GroupWrite},
    storage::{LogError, ReplicaLog},
};
use cellar_logic::{
    cell_group::{GroupHistory, MembershipRevision, ParkGeneration},
    consensus::{LogIndex, LogPosition, MemberId, OperationId, ProcessGeneration, Term},
};
use serde::{Deserialize, Serialize};

/// Historical cut selected by applying a committed PreparePark entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupParkCut {
    generation: u64,
    term: u64,
    membership_revision: u64,
}

impl GroupParkCut {
    pub(super) fn prepare(
        generation: ParkGeneration,
        term: Term,
        membership: MembershipRevision,
        applied: LogPosition,
        applied_membership: u64,
    ) -> Result<Self, ManifestError> {
        // The committed index is the generation, so a successor leader
        // cannot commit a second prepare with an already committed identity.
        if generation.get() != applied.index.get()
            || term != applied.term
            || membership.get() != applied_membership
        {
            return Err(ManifestError::Invalid("park entry binding"));
        }
        let cut = Self {
            generation: generation.get(),
            term: term.get(),
            membership_revision: membership.get(),
        };
        if !cut.valid_at(applied, applied_membership) {
            return Err(ManifestError::Invalid("park cut"));
        }
        Ok(cut)
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn position(&self) -> LogPosition {
        LogPosition {
            index: LogIndex::new(self.generation),
            term: Term::new(self.term),
        }
    }

    pub fn membership_revision(&self) -> u64 {
        self.membership_revision
    }

    pub(super) fn valid_at(&self, applied: LogPosition, membership: u64) -> bool {
        self.membership_revision > 0
            && self.membership_revision < self.generation
            && self.membership_revision <= membership
            && self.position().validate().is_ok()
            && self.generation <= applied.index.get()
            && self.term <= applied.term.get()
            && (self.generation != applied.index.get()
                || (self.term == applied.term.get() && self.membership_revision == membership))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum LocalParkPhase {
    Acknowledged,
    Closed,
}

/// Engine-local acknowledgement and irreversible close floor. Snapshots
/// carry the replicated cut, never a different disk's acknowledgement.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LocalParkRecord {
    cut: GroupParkCut,
    process: [u8; 16],
    phase: LocalParkPhase,
}

#[derive(Clone)]
pub(crate) enum LocalParkChange {
    Acknowledge(GroupParkCut),
    Close(ParkGeneration),
}

impl LocalParkRecord {
    pub(crate) fn acknowledged(&self) -> bool {
        self.phase == LocalParkPhase::Acknowledged
    }

    fn closed(&self) -> Self {
        Self {
            phase: LocalParkPhase::Closed,
            ..self.clone()
        }
    }

    fn validate_history(&self, manifest: &GroupManifest) -> Result<(), LogError> {
        let latest = manifest.park_cut().ok_or(LogError::WrongIdentity)?;
        if ProcessGeneration::new(self.process).is_err()
            || !self
                .cut
                .valid_at(manifest.position(), manifest.membership_revision())
            || self.cut.generation() > latest.generation()
            || self.cut.position().term > latest.position().term
            || self.cut.membership_revision() > latest.membership_revision()
            || (self.cut.generation() == latest.generation() && &self.cut != latest)
        {
            return Err(LogError::WrongIdentity);
        }
        Ok(())
    }
}

impl CachedGroup {
    fn quiescent_park_cut(&self, cut: &GroupParkCut) -> bool {
        let at = cut.position();
        self.group_manifest.as_ref().is_some_and(|marker| {
            marker.is_active() && marker.park_cut() == Some(cut) && marker.position() == at
        }) && self.applied.position == at
            && self.last_index() == at.index.get()
            && self.hard_state.commit == at.index.get()
            && self.hard_state.term == at.term.get()
            && self.membership.revision().get() == cut.membership_revision()
            && self.conf_state.voters_outgoing.is_empty()
            && self.conf_state.learners.is_empty()
            && self.conf_state.learners_next.is_empty()
            && !self.conf_state.auto_leave
    }

    pub(crate) fn validate_local_park(&self) -> Result<(), LogError> {
        let Some(record) = &self.local_park else {
            return Ok(());
        };
        let manifest = self
            .group_manifest
            .as_ref()
            .ok_or(LogError::WrongIdentity)?;
        record.validate_history(manifest)?;
        if record.acknowledged() && !self.quiescent_park_cut(&record.cut) {
            return Err(LogError::Corrupt("unclosed park acknowledgement".into()));
        }
        Ok(())
    }

    pub(crate) fn update_local_park(
        &mut self,
        previous: &Self,
        change: Option<&LocalParkChange>,
        member: MemberId,
        process: ProcessGeneration,
    ) -> Result<(), LogError> {
        let current = previous.local_park.as_ref();
        match change {
            Some(LocalParkChange::Acknowledge(cut)) => {
                if !previous.quiescent_park_cut(cut)
                    || !self.quiescent_park_cut(cut)
                    || !self.conf_state.voters.contains(&member.get())
                    || self.hard_state.vote != previous.hard_state.vote
                {
                    return Err(LogError::WrongIdentity);
                }
                let record = LocalParkRecord {
                    cut: cut.clone(),
                    process: process.bytes(),
                    phase: LocalParkPhase::Acknowledged,
                };
                if current == Some(&record) {
                    return Ok(());
                }
                if current.is_some_and(|current| {
                    current.phase != LocalParkPhase::Closed
                        || current.cut.generation() >= cut.generation()
                }) {
                    return Err(LogError::WrongIdentity);
                }
                self.local_park = Some(record);
            }
            Some(LocalParkChange::Close(generation)) => {
                let record = current
                    .filter(|record| record.cut.generation() == generation.get())
                    .ok_or(LogError::WrongIdentity)?;
                self.local_park = Some(record.closed());
            }
            None => {
                if let Some(record) = current {
                    if record.acknowledged()
                        && (!self.quiescent_park_cut(&record.cut)
                            || self.hard_state.vote != previous.hard_state.vote)
                    {
                        self.local_park = Some(record.closed());
                    }
                }
            }
        }
        Ok(())
    }
}

impl ReplicaLog {
    /// Close an earlier process's local acknowledgement before starting any
    /// retained group owner. This never rewrites the selected Raft state.
    pub(crate) async fn close_retained_park(
        &self,
        group: &GroupLog<GroupHistory>,
    ) -> Result<(), LogError> {
        let acknowledged = group
            .read()?
            .local_park
            .as_ref()
            .filter(|record| record.acknowledged())
            .map(|record| record.cut.generation());
        if let Some(generation) = acknowledged {
            let generation =
                ParkGeneration::new(generation).map_err(|_| LogError::WrongIdentity)?;
            let mut write = GroupWrite::new(group);
            write.local_park = Some(LocalParkChange::Close(generation));
            self.persist_cells(OperationId::new(1), vec![write]).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "hibernate_tests.rs"]
mod tests;
