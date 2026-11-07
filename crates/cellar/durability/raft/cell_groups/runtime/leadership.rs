// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A complete node sample uses bounded owner turns and one roster revision.
use super::*;
use std::collections::BTreeMap;

pub(super) const BATCH_GROUPS: usize = 16;

pub(super) struct LeadershipBatch {
    pub revision: u64,
    pub population: usize,
    pub count: usize,
    pub leaders: u32,
    pub next: Option<GroupHistory>,
}

pub(crate) struct LeadershipSample {
    pub groups: usize,
    pub leaders: u32,
    pub started_at_ms: u64,
}

/// Only the bounded prefix invokes the physical owner callback. Looking ahead
/// for another key must not sample a seventeenth owner in the same turn.
pub(super) fn capture<T>(
    groups: &BTreeMap<GroupHistory, T>,
    revision: u64,
    after: Option<GroupHistory>,
    mut leader: impl FnMut(&T) -> Result<bool, RuntimeError>,
) -> Result<LeadershipBatch, RuntimeError> {
    use std::ops::Bound::{Excluded, Unbounded};
    let mut rows = groups.range((after.map_or(Unbounded, Excluded), Unbounded));
    let mut count = 0;
    let mut leaders = 0;
    let mut last = None;
    for _ in 0..BATCH_GROUPS {
        let Some((history, group)) = rows.next() else {
            break;
        };
        leaders += u32::from(leader(group)?);
        count += 1;
        last = Some(*history);
    }
    Ok(LeadershipBatch {
        revision,
        population: groups.len(),
        count,
        leaders,
        next: rows.next().and(last),
    })
}

struct Scan {
    started_at_ms: u64,
    max_groups: usize,
    population: Option<(u64, usize)>,
    after: Option<GroupHistory>,
    visited: usize,
    leaders: u32,
}

impl Scan {
    fn new(started_at_ms: u64, max_groups: usize) -> Self {
        Self {
            started_at_ms,
            max_groups,
            population: None,
            after: None,
            visited: 0,
            leaders: 0,
        }
    }

    fn accept(&mut self, batch: LeadershipBatch) -> Result<Option<LeadershipSample>, RuntimeError> {
        let current = (batch.revision, batch.population);
        if self.population.is_some_and(|expected| expected != current) {
            return Err(RuntimeError::NotReady);
        }
        self.population = Some(current);
        if batch.count > BATCH_GROUPS
            || batch.leaders as usize > batch.count
            || batch.population > self.max_groups
        {
            return Err(RuntimeError::Protocol("leadership batch bounds".into()));
        }
        self.visited = self
            .visited
            .checked_add(batch.count)
            .ok_or(RuntimeError::Overloaded)?;
        self.leaders = self
            .leaders
            .checked_add(batch.leaders)
            .ok_or(RuntimeError::Overloaded)?;
        if self.visited > batch.population {
            return Err(RuntimeError::Protocol("leadership population".into()));
        }
        match batch.next {
            Some(next) if batch.count > 0 && self.after.is_none_or(|previous| next > previous) => {
                self.after = Some(next);
                Ok(None)
            }
            Some(_) => Err(RuntimeError::Protocol("leadership cursor".into())),
            None if self.visited == batch.population => Ok(Some(LeadershipSample {
                groups: self.visited,
                leaders: self.leaders,
                started_at_ms: self.started_at_ms,
            })),
            None => Err(RuntimeError::NotReady),
        }
    }
}

impl CellGroupRuntime {
    /// Advisory node-wide physical-group leadership, never a serving or
    /// transfer grant. A failed owner, changed roster or incomplete scan
    /// refuses the sample instead of publishing partial headroom.
    pub(crate) async fn leadership_sample(
        &self,
        deadline: u64,
    ) -> Result<LeadershipSample, RuntimeError> {
        let domain = self.inner.log.domain();
        let started_at_ms = domain.mono_ms().map_err(|_| RuntimeError::Closed)?;
        let mut scan = Scan::new(started_at_ms, self.inner.options.max_groups);
        loop {
            if domain.mono_ms().map_err(|_| RuntimeError::Closed)? >= deadline {
                return Err(RuntimeError::DeadlineExceeded);
            }
            let timer = domain
                .sleep_until(deadline)
                .map_err(|_| RuntimeError::Closed)?;
            let response = crate::asyncrt::select! {
                _ = timer => return Err(RuntimeError::DeadlineExceeded),
                result = self.call(None, Command::LeadershipBatch(scan.after), 4096) => result?,
            };
            let Response::LeadershipBatch(batch) = response else {
                return Err(RuntimeError::Protocol("leadership batch reply".into()));
            };
            if let Some(sample) = scan.accept(batch)? {
                return Ok(sample);
            }
        }
    }
}

#[cfg(test)]
#[path = "leadership_tests.rs"]
mod tests;
