//! Node-wide leadership observations, collected in bounded owned batches.
//! A count is published only after every attached history was sampled.
use super::*;
use crate::raft::inspection::LeaderLoad;
use std::ops::Bound::{Excluded, Unbounded};

#[derive(Clone, Debug)]
pub struct RaftInspectionScanOptions {
    pub interval_ms: u64,
    pub timeout_ms: u64,
    pub groups_per_batch: usize,
}

impl Default for RaftInspectionScanOptions {
    fn default() -> Self {
        Self {
            interval_ms: 250,
            timeout_ms: 5000,
            groups_per_batch: 4,
        }
    }
}

impl RaftInspectionScanOptions {
    pub(super) fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            (10..=60_000).contains(&self.interval_ms)
                && (1..=60_000).contains(&self.timeout_ms)
                && (1..=16).contains(&self.groups_per_batch),
            "invalid inspection scan bounds"
        );
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Population {
    pub cells: usize,
    pub groups: usize,
}

struct Cycle {
    sequence: u64,
    population: Population,
    started: u64,
    deadline: u64,
    cursor: Option<GroupScope>,
    visited: usize,
    leaders: u32,
}

struct Cached {
    population: Population,
    started: u64,
    leaders: u32,
}

pub(super) struct Scanner {
    options: RaftInspectionScanOptions,
    cycle: Option<Cycle>,
    cached: Option<Cached>,
    sequence: u64,
    next_at: u64,
    busy: bool,
}

pub(super) struct Completed {
    sequence: u64,
    result: Vec<(ReplicaIdentity, Result<GroupStatus, RuntimeError>)>,
}

impl Scanner {
    pub(super) fn new(options: RaftInspectionScanOptions) -> Self {
        Self {
            options,
            cycle: None,
            cached: None,
            sequence: 0,
            next_at: 0,
            busy: false,
        }
    }

    fn failed(&mut self, now: u64) {
        self.cycle = None;
        self.busy = false;
        self.next_at = now.saturating_add(self.options.interval_ms);
        // Keep the prior complete sample at its original age. Failure cannot
        // refresh it or substitute a partial count (including an invented zero).
    }

    fn begin(&mut self, now: u64, population: Population, initializing: bool) -> bool {
        if self.busy {
            return false;
        }
        if initializing || population.groups < population.cells {
            self.failed(now);
            return false;
        }
        if self
            .cycle
            .as_ref()
            .is_some_and(|cycle| cycle.population != population || now >= cycle.deadline)
        {
            self.failed(now);
            return false;
        }
        if self.cycle.is_none() {
            if now < self.next_at {
                return false;
            }
            let Some(sequence) = self.sequence.checked_add(1) else {
                return false;
            };
            let Some(deadline) = now.checked_add(self.options.timeout_ms) else {
                return false;
            };
            self.sequence = sequence;
            self.cycle = Some(Cycle {
                sequence,
                population,
                started: now,
                deadline,
                cursor: None,
                visited: 0,
                leaders: 0,
            });
        }
        if population.cells == 0 {
            self.complete(now);
            return false;
        }
        true
    }

    fn complete(&mut self, now: u64) {
        if let Some(cycle) = self.cycle.take() {
            self.cached = Some(Cached {
                population: cycle.population,
                started: cycle.started,
                leaders: cycle.leaders,
            });
        }
        self.busy = false;
        self.next_at = now.saturating_add(self.options.interval_ms);
    }

    fn accept(
        &mut self,
        completed: Completed,
        now: u64,
        population: Population,
        initializing: bool,
    ) {
        let Some(cycle) = self.cycle.as_mut() else {
            return;
        };
        if cycle.sequence != completed.sequence {
            return;
        }
        self.busy = false;
        if initializing
            || cycle.population != population
            || now >= cycle.deadline
            || completed.result.is_empty()
            || completed.result.len() > self.options.groups_per_batch
        {
            self.failed(now);
            return;
        }
        for (identity, result) in completed.result {
            let status = match result {
                Ok(status)
                    if status.replica == identity
                        && cycle
                            .cursor
                            .as_ref()
                            .is_none_or(|cursor| *cursor < identity.scope) =>
                {
                    status
                }
                _ => {
                    self.failed(now);
                    return;
                }
            };
            cycle.cursor = Some(identity.scope);
            cycle.visited += 1;
            cycle.leaders += u32::from(status.is_leader);
        }
        if cycle.visited > population.cells {
            self.failed(now);
        } else if cycle.visited == population.cells {
            self.complete(now);
        }
    }

    pub(super) fn view(
        &self,
        now: u64,
        population: Population,
        initializing: bool,
        capacity: u32,
    ) -> Option<LeaderLoad> {
        let cached = self.cached.as_ref()?;
        if initializing || cached.population != population {
            return None;
        }
        Some(LeaderLoad {
            leaders: cached.leaders,
            capacity,
            sample_age_ms: now.checked_sub(cached.started)?,
        })
    }
}

impl Driver {
    pub(super) fn pump_leader_load(&mut self) {
        let Ok(now) = self.log.domain().mono_ms() else {
            return;
        };
        let Ok(groups) = self.log.group_count() else {
            return;
        };
        let population = Population {
            cells: self.cells.len(),
            groups,
        };
        let Some(scanner) = self.leader_load.as_mut() else {
            return;
        };
        if !scanner.begin(now, population, self.activating.is_some()) {
            return;
        }
        let Some(cycle) = scanner.cycle.as_ref() else {
            return;
        };
        let after = cycle.cursor.as_ref().map_or(Unbounded, Excluded);
        let batch: Vec<_> = self
            .cells
            .range((after, Unbounded))
            .take(scanner.options.groups_per_batch)
            .map(|(_, cell)| cell.group.identity().clone())
            .collect();
        if batch.is_empty() {
            scanner.failed(now);
            return;
        }
        let sequence = cycle.sequence;
        let deadline = cycle.deadline;
        scanner.busy = true;
        let runtime = self.runtime.clone();
        let domain = self.log.domain();
        self.jobs.push(Box::pin(async move {
            let observed = batch.into_iter().map(|identity| {
                let runtime = runtime.clone();
                let domain = domain.clone();
                async move {
                    let result = match domain.sleep_until(deadline) {
                        Ok(timer) => crate::asyncrt::select_biased! { "the original complete-scan deadline precedes a late status";
                            _ = timer => Err(RuntimeError::NotReady),
                            result = runtime.status(&identity.scope) => result,
                        },
                        Err(_) => Err(RuntimeError::Closed),
                    };
                    (identity, result)
                }
            });
            Job::LeaderLoad(Box::new(Completed { sequence, result: futures_util::future::join_all(observed).await }))
        }));
    }

    pub(super) fn finish_leader_load(&mut self, completed: Completed) {
        let Ok(now) = self.log.domain().mono_ms() else {
            return;
        };
        let Ok(groups) = self.log.group_count() else {
            return;
        };
        let population = Population {
            cells: self.cells.len(),
            groups,
        };
        if let Some(scanner) = self.leader_load.as_mut() {
            scanner.accept(completed, now, population, self.activating.is_some());
        }
    }
}

#[cfg(test)]
#[path = "host_leader_load_tests.rs"]
mod tests;
