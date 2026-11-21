// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A weak physical-use index keeps retirement protection distinct from reuse.
use super::reuse::{Admission, Eligibility};
use super::{Error, Result};
use crate::raft::cell_groups::{
    archive::CoverageUse, CellGroupArchiveCoverage, CellGroupArchiveHold,
};
use std::sync::{Arc, Mutex};

struct TrackedUse {
    snapshot: CoverageUse,
    eligibility: Eligibility,
}

#[derive(Clone, Default)]
pub(super) struct Index(Arc<Mutex<Vec<TrackedUse>>>);

impl Index {
    pub(super) fn track(
        &self,
        coverage: &CellGroupArchiveCoverage,
        eligibility: Option<Eligibility>,
    ) -> Result<()> {
        let mut uses = self
            .0
            .lock()
            .map_err(|_| Error::Invalid("archive use lock"))?;
        uses.retain(|usage| usage.snapshot.live());
        let snapshot = coverage.retain_use();
        if !uses
            .iter()
            .any(|usage| usage.snapshot.same_snapshot(&snapshot))
        {
            uses.push(TrackedUse {
                snapshot,
                eligibility: eligibility
                    .ok_or(Error::Invalid("untracked retained verification"))?,
            });
        }
        Ok(())
    }

    pub(super) fn allows(
        &self,
        coverage: &CellGroupArchiveCoverage,
        admission: Admission,
    ) -> Result<bool> {
        let uses = self
            .0
            .lock()
            .map_err(|_| Error::Invalid("archive use lock"))?;
        let snapshot = coverage.retain_use();
        let usage = uses
            .iter()
            .find(|usage| usage.snapshot.same_snapshot(&snapshot))
            .ok_or(Error::Invalid("untracked archive verification"))?;
        Ok(usage.eligibility.allows(admission))
    }

    pub(super) fn seal(
        &self,
        coverage: &CellGroupArchiveCoverage,
        admitted_through: u64,
    ) -> Result<()> {
        let mut uses = self
            .0
            .lock()
            .map_err(|_| Error::Invalid("archive use lock"))?;
        let snapshot = coverage.retain_use();
        let usage = uses
            .iter_mut()
            .find(|usage| usage.snapshot.same_snapshot(&snapshot))
            .ok_or(Error::Invalid("untracked archive completion"))?;
        usage.eligibility.seal(admitted_through);
        Ok(())
    }

    pub(super) fn prove_output(&self, coverage: &CellGroupArchiveCoverage) -> Result<()> {
        let mut uses = self
            .0
            .lock()
            .map_err(|_| Error::Invalid("archive use lock"))?;
        let snapshot = coverage.retain_use();
        let usage = uses
            .iter_mut()
            .find(|usage| usage.snapshot.same_snapshot(&snapshot))
            .ok_or(Error::Invalid("untracked output verification"))?;
        usage.eligibility.prove_output();
        Ok(())
    }

    pub(super) fn live(&self, admission: Admission) -> Result<Option<CellGroupArchiveCoverage>> {
        let snapshot = {
            let mut uses = self
                .0
                .lock()
                .map_err(|_| Error::Invalid("archive use lock"))?;
            uses.retain(|usage| usage.snapshot.live());
            uses.iter()
                .filter(|usage| usage.eligibility.allows(admission))
                .max_by_key(|usage| usage.snapshot.marker().position().index)
                .map(|usage| usage.snapshot.clone())
        };
        Ok(snapshot.and_then(|snapshot| snapshot.retain_coverage()))
    }

    pub(super) fn retains(&self, hold: &CellGroupArchiveHold) -> Result<bool> {
        let mut uses = self
            .0
            .lock()
            .map_err(|_| Error::Invalid("archive use lock"))?;
        uses.retain(|usage| usage.snapshot.live());
        Ok(uses.iter().any(|usage| usage.snapshot.retains(hold)))
    }
}
