// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Replicated park preparation evidence. Local park and wake authority
//! require separate durable records and acknowledgements from the voters.

use super::{CellGroupProposal, GroupManifest, ManifestError, ParkCertificate};
use crate::raft::{
    cache::{CachedGroup, GroupLog, GroupWrite},
    storage::{LogError, ReplicaLog},
};
use cellar_logic::{
    cell_group::{GroupHistory, MembershipRevision, ParkGeneration},
    consensus::{
        LogIndex, LogPosition, MemberId, NodeIdentity, OperationId, ProcessGeneration, ProposalId,
        ReplicaIdentity, Term,
    },
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

pub(crate) const MAX_PARK_NEGOTIATION_MS: u64 = 5_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ParkPrepareRequest {
    pub proposal: ProposalId,
    pub term: Term,
    pub membership: MembershipRevision,
    pub expected_applied: LogPosition,
    pub deadline_ms: u64,
}

/// Leader-local admission, followed by ordinary Raft commitment and apply.
#[derive(Clone, Debug)]
pub(crate) struct ParkPreparation {
    pub proposal: CellGroupProposal,
    pub next_due_unix_millis: Option<u64>,
}

/// Physical evidence for one voter. Selection of a park certificate still
/// requires authenticated acknowledgements from the complete voter set.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ParkAcknowledgement {
    pub voter: ReplicaIdentity<GroupHistory>,
    pub leader: MemberId,
    pub cut: GroupParkCut,
    pub sync_sequence: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum LocalParkPhase {
    Acknowledged,
    Certified,
    Parked,
    Closed,
    WakeClosed,
}

/// Engine-local acknowledgement and irreversible close floor. Snapshots
/// carry the replicated cut, never a different disk's acknowledgement.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LocalParkRecord {
    cut: GroupParkCut,
    process: [u8; 16],
    phase: LocalParkPhase,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    certificate: Option<Box<ParkCertificate>>,
}

#[derive(Clone)]
pub(crate) enum LocalParkChange {
    Acknowledge(GroupParkCut),
    Certify(Box<ParkCertificate>),
    Park(Box<ParkCertificate>),
    Close(ParkGeneration),
}

impl LocalParkRecord {
    pub(crate) fn is_open(&self) -> bool {
        matches!(
            self.phase,
            LocalParkPhase::Acknowledged | LocalParkPhase::Certified | LocalParkPhase::Parked
        )
    }

    pub(super) fn matches_acknowledgement(
        &self,
        cut: &GroupParkCut,
        process: ProcessGeneration,
    ) -> bool {
        self.is_open() && &self.cut == cut && self.process == process.bytes()
    }

    fn closed(&self) -> Self {
        Self {
            cut: self.cut.clone(),
            process: self.process,
            phase: if self.phase == LocalParkPhase::Parked
                || self.phase == LocalParkPhase::WakeClosed
            {
                LocalParkPhase::WakeClosed
            } else {
                LocalParkPhase::Closed
            },
            certificate: None,
        }
    }

    pub(crate) fn reserved_bytes(&self) -> usize {
        self.certificate
            .as_ref()
            .map_or(0, |certificate| certificate.reserved_bytes())
    }

    pub(crate) fn certificate(&self) -> Option<&ParkCertificate> {
        self.certificate.as_deref()
    }

    pub(crate) fn matches_member(&self, member: u64) -> bool {
        self.certificate.as_ref().is_none_or(|certificate| {
            certificate.matches_journal(member, self.process)
                && (self.phase == LocalParkPhase::Parked
                    || certificate
                        .leader()
                        .is_ok_and(|leader| leader.get() == member))
        })
    }

    pub(crate) fn parked(&self) -> bool {
        self.phase == LocalParkPhase::Parked
    }

    pub(super) fn wake_floor(&self) -> Option<LogIndex> {
        (self.phase == LocalParkPhase::WakeClosed).then(|| self.cut.position().index)
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
    pub(crate) fn quiescent_park_cut(&self, cut: &GroupParkCut) -> bool {
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

    pub(super) fn admit_park_acknowledgement(
        &self,
        cut: &GroupParkCut,
        member: MemberId,
        process: ProcessGeneration,
    ) -> Result<(), LogError> {
        if !self.quiescent_park_cut(cut)
            || !self.conf_state.voters.contains(&member.get())
            || self.local_park.as_ref().is_some_and(|current| {
                if current.is_open() {
                    !current.matches_acknowledgement(cut, process)
                } else {
                    current.cut.generation() >= cut.generation()
                }
            })
        {
            return Err(LogError::WrongIdentity);
        }
        Ok(())
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
        if record.is_open() && !self.quiescent_park_cut(&record.cut) {
            return Err(LogError::Corrupt("unclosed park acknowledgement".into()));
        }
        match (&record.phase, record.certificate()) {
            (LocalParkPhase::Certified | LocalParkPhase::Parked, Some(certificate))
                if certificate.cut() == &record.cut =>
            {
                certificate.validate(manifest.route(), &self.conf_state, &self.membership)
            }
            (
                LocalParkPhase::Acknowledged | LocalParkPhase::Closed | LocalParkPhase::WakeClosed,
                None,
            ) => Ok(()),
            _ => Err(LogError::Corrupt("park certificate phase".into())),
        }
    }

    pub(super) fn admit_park_certificate(
        &self,
        certificate: &ParkCertificate,
        member: MemberId,
        identity: NodeIdentity,
    ) -> Result<(), LogError> {
        if !certificate.matches_leader(member, identity)
            || self
                .local_park
                .as_ref()
                .is_some_and(LocalParkRecord::parked)
        {
            return Err(LogError::WrongIdentity);
        }
        self.admit_park_selection(certificate, member, identity)
    }

    pub(super) fn admit_park(
        &self,
        certificate: &ParkCertificate,
        member: MemberId,
        identity: NodeIdentity,
    ) -> Result<(), LogError> {
        if certificate.leader()? == member
            && self
                .local_park
                .as_ref()
                .and_then(LocalParkRecord::certificate)
                != Some(certificate)
        {
            return Err(LogError::WrongIdentity);
        }
        self.admit_park_selection(certificate, member, identity)
    }

    fn admit_park_selection(
        &self,
        certificate: &ParkCertificate,
        member: MemberId,
        identity: NodeIdentity,
    ) -> Result<(), LogError> {
        let cut = certificate.cut();
        let manifest = self
            .group_manifest
            .as_ref()
            .ok_or(LogError::WrongIdentity)?;
        if !self.quiescent_park_cut(cut)
            || !certificate.matches_voter(member, identity)
            || !self.local_park.as_ref().is_some_and(|record| {
                record.matches_acknowledgement(cut, identity.process)
                    && record
                        .certificate()
                        .is_none_or(|current| current == certificate)
            })
        {
            return Err(LogError::WrongIdentity);
        }
        certificate.validate(manifest.route(), &self.conf_state, &self.membership)
    }

    pub(crate) fn update_local_park(
        &mut self,
        previous: &Self,
        change: Option<&LocalParkChange>,
        member: MemberId,
        identity: NodeIdentity,
    ) -> Result<(), LogError> {
        let process = identity.process;
        let current = previous.local_park.as_ref();
        match change {
            Some(LocalParkChange::Acknowledge(cut)) => {
                previous.admit_park_acknowledgement(cut, member, process)?;
                if !self.quiescent_park_cut(cut)
                    || !self.conf_state.voters.contains(&member.get())
                    || self.hard_state.vote != previous.hard_state.vote
                {
                    return Err(LogError::WrongIdentity);
                }
                if current.is_none_or(|record| !record.matches_acknowledgement(cut, process)) {
                    self.local_park = Some(LocalParkRecord {
                        cut: cut.clone(),
                        process: process.bytes(),
                        phase: LocalParkPhase::Acknowledged,
                        certificate: None,
                    });
                }
            }
            Some(LocalParkChange::Certify(certificate) | LocalParkChange::Park(certificate)) => {
                let phase = if matches!(change, Some(LocalParkChange::Park(_))) {
                    previous.admit_park(certificate, member, identity)?;
                    self.admit_park(certificate, member, identity)?;
                    LocalParkPhase::Parked
                } else {
                    previous.admit_park_certificate(certificate, member, identity)?;
                    self.admit_park_certificate(certificate, member, identity)?;
                    LocalParkPhase::Certified
                };
                if self.hard_state.vote != previous.hard_state.vote {
                    return Err(LogError::WrongIdentity);
                }
                self.local_park = Some(LocalParkRecord {
                    cut: certificate.cut().clone(),
                    process: process.bytes(),
                    phase,
                    certificate: Some(certificate.clone()),
                });
            }
            Some(LocalParkChange::Close(generation)) => {
                let record = current
                    .filter(|record| record.cut.generation() == generation.get())
                    .ok_or(LogError::WrongIdentity)?;
                self.local_park = Some(record.closed());
            }
            None => {
                if let Some(record) = current {
                    if record.is_open()
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
            .filter(|record| record.is_open())
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
