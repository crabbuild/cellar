// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded, leader-local acceptance; never a commit or receipt proof.

use super::{CellGroupProposal, PreparedGroupManifest};
use crate::application::consensus::group_protocol::{CellGroupEntry, GroupCommand};
use crate::raft::budget::{Budget, Permit};
use crate::raft::completion::RuntimeError;
use cellar_logic::{
    cell_group::CellId,
    consensus::{LogIndex, LogPosition, ProposalId},
};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::Arc;

pub(super) struct Candidate {
    cell: CellId,
    proposal: ProposalId,
    payload: [u8; 32],
    enrollment: bool,
}

struct Accepted {
    candidate: Candidate,
    acceptance: CellGroupProposal,
    _permit: Permit,
}

pub(super) enum Admission {
    Existing(CellGroupProposal),
    New(Candidate, Permit),
}

/// At most one record per pending logical name, bounded by max_requests. No LTX
/// payload or image pin is retained. Clearing on role loss is safe because new
/// admission requires a current-term applied barrier covering inherited entries.
#[derive(Default)]
pub(super) struct ProposalAdmission {
    cells: BTreeMap<CellId, Accepted>,
    positions: BTreeMap<LogIndex, CellId>,
    enrollments: u32,
}

impl ProposalAdmission {
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
            },
        );
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
