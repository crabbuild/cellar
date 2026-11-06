// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Replicated park preparation evidence. Local park and wake authority
//! require separate durable records and acknowledgements from the voters.

use super::ManifestError;
use cellar_logic::{
    cell_group::{MembershipRevision, ParkGeneration},
    consensus::{LogIndex, LogPosition, Term},
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

#[cfg(test)]
#[path = "hibernate_tests.rs"]
mod tests;
