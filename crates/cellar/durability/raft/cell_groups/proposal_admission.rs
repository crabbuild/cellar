// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded, leader-local acceptance; never a commit or receipt proof.

use super::{CellGroupProposal, PreparedGroupManifest};
use crate::application::consensus::group_protocol::{CellGroupEntry, GroupCommand};
use crate::raft::budget::{Budget, Permit};
use crate::raft::completion::{entry_digest, entry_position, RuntimeError};
use cellar_logic::{
    cell_group::CellId,
    consensus::{LogIndex, LogPosition, ProposalId},
};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::{Arc, Weak};

pub(super) struct Candidate {
    cell: CellId,
    proposal: ProposalId,
    payload: [u8; 32],
    enrollment: bool,
}

impl Candidate {
    pub(super) fn mutation_cell(&self) -> Option<&CellId> {
        (!self.enrollment).then_some(&self.cell)
    }
}

struct Accepted {
    candidate: Candidate,
    acceptance: CellGroupProposal,
    _permit: Permit,
    prepared_image: Weak<crate::raft::execution::CellPreparedImage>,
}

pub(super) enum Admission {
    Existing(CellGroupProposal),
    New(Candidate, Permit),
}

/// At most one record per pending logical name, bounded by max_requests. No LTX
/// payload or image pin is retained; optional local preparations are weak.
/// Clearing on role loss is safe because new
/// admission requires a current-term applied barrier covering inherited entries.
#[derive(Default)]
pub(super) struct ProposalAdmission {
    cells: BTreeMap<CellId, Accepted>,
    positions: BTreeMap<LogIndex, CellId>,
    enrollments: u32,
}

impl ProposalAdmission {
    /// Only this leader's exact admitted mutation can certify an independent
    /// pending cell. Enrollment, control and inherited entries cannot do so.
    pub(super) fn independent_mutation(&self, entry: &raft::prelude::Entry, cell: &CellId) -> bool {
        if entry.get_entry_type() != raft::prelude::EntryType::EntryNormal || entry.data.is_empty()
        {
            return false;
        }
        let Some(other) = self.positions.get(&LogIndex::new(entry.index)) else {
            return false;
        };
        self.cells.get(other).is_some_and(|accepted| {
            accepted
                .candidate
                .mutation_cell()
                .is_some_and(|other| other != cell)
                && accepted.acceptance.position == entry_position(entry)
                && entry_digest(entry).is_ok_and(|digest| digest == accepted.acceptance.digest)
        })
    }

    pub(super) fn check(
        &self,
        entry: &CellGroupEntry<'_>,
        bytes: &[u8],
        current: &PreparedGroupManifest,
        max_cells: u32,
        max_pending: usize,
        budget: &Arc<Budget>,
    ) -> Result<Admission, RuntimeError> {
        let (cell, enrollment) = match &entry.command {
            GroupCommand::Enroll { cell, .. } => (&cell.cell, true),
            GroupCommand::Mutation(mutation) => (&mutation.cell.cell, false),
            _ => {
                return Err(RuntimeError::Invalid(
                    "group administration is not attached".into(),
                ))
            }
        };
        let candidate = Candidate {
            cell: cell.clone(),
            proposal: entry.binding.proposal,
            payload: Sha256::digest(bytes).into(),
            enrollment,
        };
        if let Some(accepted) = self.cells.get(cell) {
            if accepted.candidate.proposal != candidate.proposal {
                return Err(RuntimeError::NotReady);
            }
            if accepted.candidate.payload != candidate.payload {
                return Err(RuntimeError::Invalid(
                    "proposal identity reused with different bytes".into(),
                ));
            }
            return Ok(Admission::Existing(accepted.acceptance.clone()));
        }
        let existing = current.pin_cell(cell);
        match &entry.command {
            GroupCommand::Enroll { .. } => {
                // Once applied, the immutable creation record is the retry
                // authority. Do not append another enrollment, even at capacity.
                if existing.is_some() {
                    return Err(RuntimeError::StaleCompletion);
                }
                if current
                    .marker()
                    .root()
                    .cells()
                    .saturating_add(self.enrollments)
                    >= max_cells
                {
                    return Err(RuntimeError::Overloaded);
                }
            }
            GroupCommand::Mutation(mutation) => {
                let (leaf, _) = existing.ok_or(RuntimeError::StaleCompletion)?;
                mutation
                    .check_base(&leaf.version)
                    .map_err(|_| RuntimeError::StaleCompletion)?;
            }
            _ => unreachable!("validated data command"),
        }
        if self.cells.len() >= max_pending {
            return Err(RuntimeError::Overloaded);
        }
        // Three bounded cell-ID copies, both map nodes and acceptance metadata.
        // Shared across runtime groups, separately from Ready/completion budgets
        // so saturation cannot prevent accepted work from draining.
        let permit = budget.acquire(4096).ok_or(RuntimeError::Overloaded)?;
        Ok(Admission::New(candidate, permit))
    }

    pub(super) fn accept(
        &mut self,
        candidate: Candidate,
        acceptance: CellGroupProposal,
        permit: Permit,
    ) {
        self.enrollments += u32::from(candidate.enrollment);
        self.positions
            .insert(acceptance.position.index, candidate.cell.clone());
        self.cells.insert(
            candidate.cell.clone(),
            Accepted {
                candidate,
                acceptance,
                _permit: permit,
                prepared_image: Weak::new(),
            },
        );
    }

    pub(super) fn attach_prepared_image(
        &mut self,
        acceptance: &CellGroupProposal,
        image: Weak<crate::raft::execution::CellPreparedImage>,
    ) {
        let Some(cell) = self.positions.get(&acceptance.position.index) else {
            return;
        };
        let Some(accepted) = self.cells.get_mut(cell) else {
            return;
        };
        if accepted.acceptance.position == acceptance.position
            && accepted.acceptance.digest == acceptance.digest
            && accepted.acceptance.group == acceptance.group
        {
            accepted.prepared_image = image;
        }
    }

    pub(super) fn prepared_image(
        &self,
        position: LogPosition,
        bytes: &[u8],
    ) -> Option<Arc<crate::raft::execution::CellPreparedImage>> {
        let cell = self.positions.get(&position.index)?;
        let accepted = self.cells.get(cell)?;
        if accepted.acceptance.position != position
            || accepted.candidate.payload != <[u8; 32]>::from(Sha256::digest(bytes))
        {
            return None;
        }
        accepted.prepared_image.upgrade()
    }

    pub(super) fn applied(&mut self, position: LogPosition) {
        let Some(cell) = self.positions.get(&position.index) else {
            return;
        };
        if self
            .cells
            .get(cell)
            .is_none_or(|accepted| accepted.acceptance.position != position)
        {
            return;
        }
        let cell = self
            .positions
            .remove(&position.index)
            .expect("accepted position");
        let accepted = self.cells.remove(&cell).expect("accepted cell");
        self.enrollments -= u32::from(accepted.candidate.enrollment);
    }

    pub(super) fn clear(&mut self) {
        self.cells.clear();
        self.positions.clear();
        self.enrollments = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cellar_logic::{
        cell_group::GroupHistory,
        consensus::{ClusterId, GroupId, IncarnationId, NamespaceId},
    };
    use raft::prelude::{Entry, EntryType};

    #[test]
    fn pending_preparation_requires_exact_other_cell_mutation() {
        let other = CellId::new("Counter:other").unwrap();
        let target = CellId::new("Counter:target").unwrap();
        let entry = Entry {
            index: 3,
            term: 2,
            data: b"already validated mutation".to_vec().into(),
            ..Default::default()
        };
        let mut admission = ProposalAdmission::default();
        assert!(!admission.independent_mutation(&entry, &target));
        admission.accept(
            Candidate {
                cell: other.clone(),
                proposal: ProposalId::new([1; 16]).unwrap(),
                payload: Sha256::digest(&entry.data).into(),
                enrollment: false,
            },
            CellGroupProposal {
                group: GroupHistory {
                    cluster: ClusterId::new([1; 16]).unwrap(),
                    namespace: NamespaceId::new([2; 16]).unwrap(),
                    group: GroupId::new([3; 16]).unwrap(),
                    incarnation: IncarnationId::new([4; 16]).unwrap(),
                },
                position: entry_position(&entry),
                digest: entry_digest(&entry).unwrap(),
            },
            Budget::new(4096, 1).acquire(4096).unwrap(),
        );
        assert!(admission.independent_mutation(&entry, &target));
        assert!(!admission.independent_mutation(&entry, &other));
        for changed in [
            Entry {
                term: 3,
                ..entry.clone()
            },
            Entry {
                index: 4,
                ..entry.clone()
            },
            Entry {
                data: b"different mutation".to_vec().into(),
                ..entry.clone()
            },
            Entry {
                entry_type: EntryType::EntryConfChangeV2,
                ..entry.clone()
            },
            Entry {
                data: Vec::new().into(),
                ..entry.clone()
            },
        ] {
            assert!(!admission.independent_mutation(&changed, &target));
        }
        admission
            .cells
            .get_mut(&other)
            .unwrap()
            .candidate
            .enrollment = true;
        assert!(!admission.independent_mutation(&entry, &target));
        admission.clear();
        assert!(!admission.independent_mutation(&entry, &target));
    }
}
