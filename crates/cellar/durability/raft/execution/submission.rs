// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A prepared SQL attempt is accepted once or permanently sealed before append.

use crate::raft::{cell_groups::CellGroupProposal, completion::RuntimeError};
use cellar_logic::{
    cell_group::GroupHistory,
    consensus::{ProposalId, Term},
};
use std::sync::{Arc, Mutex};

#[derive(Default)]
enum Decision {
    #[default]
    Open,
    Submitting,
    Accepted(CellGroupProposal),
    Rejected,
}

struct Submission {
    engine: Arc<()>,
    decision: Mutex<Decision>,
}

#[derive(Clone)]
pub struct CellPreparedCommand {
    bytes: Arc<Vec<u8>>,
    proposal: ProposalId,
    term: Term,
    group: GroupHistory,
    submission: Arc<Submission>,
}

/// This preparation was sealed before append. It is not a claim about other
/// preparations of the same business request. All clones share the same seal.
#[derive(Clone)]
pub struct CellProposalRejection {
    submission: Arc<Submission>,
}

pub enum CellSubmissionOutcome {
    /// Acceptance remains provisional, never a commit or S3 acknowledgement.
    Accepted(CellGroupProposal),
    Rejected(CellProposalRejection),
}

impl CellPreparedCommand {
    pub(super) fn new(
        bytes: Vec<u8>,
        proposal: ProposalId,
        term: Term,
        group: GroupHistory,
        engine: Arc<()>,
    ) -> Self {
        Self {
            bytes: Arc::new(bytes),
            proposal,
            term,
            group,
            submission: Arc::new(Submission {
                engine,
                decision: Mutex::new(Decision::Open),
            }),
        }
    }

    pub fn term(&self) -> Term {
        self.term
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn proposal(&self) -> ProposalId {
        self.proposal
    }

    pub(crate) fn group(&self) -> GroupHistory {
        self.group
    }

    pub(crate) fn belongs_to(&self, engine: &Arc<()>, group: GroupHistory) -> bool {
        Arc::ptr_eq(engine, &self.submission.engine) && group == self.group
    }

    pub(crate) fn submit(
        &self,
        engine: &Arc<()>,
        group: GroupHistory,
        attempt: impl FnOnce() -> Result<Option<CellGroupProposal>, RuntimeError>,
    ) -> Result<CellSubmissionOutcome, RuntimeError> {
        if !self.belongs_to(engine, group) {
            return Err(RuntimeError::Invalid("prepared submission owner".into()));
        }
        let mut decision = self
            .submission
            .decision
            .lock()
            .map_err(|_| RuntimeError::Protocol("submission state poisoned".into()))?;
        match &*decision {
            Decision::Accepted(accepted) => {
                return Ok(CellSubmissionOutcome::Accepted(accepted.clone()))
            }
            Decision::Rejected => return Ok(CellSubmissionOutcome::Rejected(self.rejected())),
            Decision::Submitting => return Err(RuntimeError::NotReady),
            Decision::Open => (),
        }
        *decision = Decision::Submitting;
        drop(decision);
        // No lock spans decoding, hashing, RawNode work or async operations.
        // Only the owner's definitive pre-append capacity refusal returns None.
        let outcome = attempt();
        let mut decision = self
            .submission
            .decision
            .lock()
            .map_err(|_| RuntimeError::Protocol("submission state poisoned".into()))?;
        match outcome {
            Ok(Some(accepted)) => {
                *decision = Decision::Accepted(accepted.clone());
                Ok(CellSubmissionOutcome::Accepted(accepted))
            }
            Ok(None) => {
                *decision = Decision::Rejected;
                Ok(CellSubmissionOutcome::Rejected(self.rejected()))
            }
            Err(error) => {
                *decision = Decision::Open;
                Err(error)
            }
        }
    }

    fn rejected(&self) -> CellProposalRejection {
        CellProposalRejection {
            submission: self.submission.clone(),
        }
    }

    /// Recover a lost rejection response without asking a closed or replaced
    /// runtime. Open and accepted attempts never yield a rejection proof.
    pub fn rejection(&self) -> Result<Option<CellProposalRejection>, RuntimeError> {
        let decision = self
            .submission
            .decision
            .lock()
            .map_err(|_| RuntimeError::Protocol("submission state poisoned".into()))?;
        Ok(matches!(*decision, Decision::Rejected).then(|| self.rejected()))
    }

    pub(crate) fn matches_rejection(&self, rejection: &CellProposalRejection) -> bool {
        Arc::ptr_eq(&self.submission, &rejection.submission)
    }
}

impl CellProposalRejection {
    pub(crate) fn belongs_to_engine(&self, engine: &Arc<()>) -> bool {
        Arc::ptr_eq(engine, &self.submission.engine)
    }
}
