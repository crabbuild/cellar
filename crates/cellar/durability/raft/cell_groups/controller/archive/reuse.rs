// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Admission boundaries for sharing one physically retained verification.

#[cfg(test)]
#[path = "reuse_tests.rs"]
mod tests;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Admission(u64);

#[derive(Default)]
pub(super) struct Admissions {
    last: u64,
}

impl Admissions {
    pub(super) fn admit(&mut self) -> Option<Admission> {
        self.last = self.last.checked_add(1)?;
        Some(Admission(self.last))
    }

    pub(super) fn current(&self) -> u64 {
        self.last
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct Eligibility {
    through: Option<u64>,
    proven_output: bool,
}

impl Eligibility {
    /// A new publication can include arrivals until its controller completion.
    pub(super) fn publication() -> Self {
        Self {
            through: None,
            proven_output: false,
        }
    }

    /// Revalidating an already published cut covers the admitted starting batch.
    /// Completing background maintenance cannot renew that admission boundary.
    pub(super) fn existing(admitted_through: u64) -> Self {
        Self {
            through: Some(admitted_through),
            proven_output: false,
        }
    }

    pub(super) fn seal(&mut self, admitted_through: u64) {
        if self.through.is_none() {
            self.through = Some(admitted_through);
        }
    }

    /// An exact S3 output has been proved using this physical verification.
    /// Its retained lifetime also covers lost/buffered replies and cleanup.
    pub(super) fn prove_output(&mut self) {
        self.proven_output = true;
    }

    pub(super) fn allows(&self, admission: Admission) -> bool {
        self.proven_output || self.through.is_none_or(|last| admission.0 <= last)
    }
}
