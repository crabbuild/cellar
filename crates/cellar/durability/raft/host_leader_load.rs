// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Node-wide leadership observations, collected in bounded owned batches.
//! A count is published only after every attached history was sampled.
use super::*;
use crate::application::consensus::RaftGroupResources;
use crate::raft::inspection::LeaderLoad;

/// Metadata only: retaining an SDK handle cannot retain a group or engine.
#[derive(Default)]
pub(crate) struct GroupResourceObserver(Mutex<Option<(u64, RaftGroupResources)>>);

impl GroupResourceObserver {
    pub(crate) fn capture(&self, now: u64) -> Option<RaftGroupResources> {
        let (started, mut sample) = (*self.0.lock().ok()?)?;
        sample.sample_age_ms = now.checked_sub(started)?;
        Some(sample)
    }
}

fn accumulate(total: &mut RaftGroupResources, value: RaftGroupResources) -> Option<()> {
    for (sum, value) in [
        (&mut total.leaders, value.leaders),
        (&mut total.voters, value.voters),
        (&mut total.learners, value.learners),
        (&mut total.removed, value.removed),
        (&mut total.unknown, value.unknown),
        (&mut total.coherent_images, value.coherent_images),
        (&mut total.historical_queries, value.historical_queries),
        (
            &mut total.recent_source_queries,
            value.recent_source_queries,
        ),
        (&mut total.failed, value.failed),
        (&mut total.retired, value.retired),
        (&mut total.sql_busy, value.sql_busy),
        (&mut total.pending_commands, value.pending_commands),
        (&mut total.incoming_seeds, value.incoming_seeds),
        (&mut total.applying, value.applying),
        (&mut total.archive_pending, value.archive_pending),
        (&mut total.archive_current, value.archive_current),
        (&mut total.archive_unconfirmed, value.archive_unconfirmed),
    ] {
        *sum = sum.checked_add(value)?;
    }
    total.apply_lag_entries = total
        .apply_lag_entries
        .checked_add(value.apply_lag_entries)?;
    total.archive_lag_entries = total
        .archive_lag_entries
        .checked_add(value.archive_lag_entries)?;
    total.max_apply_lag_entries = total.max_apply_lag_entries.max(value.max_apply_lag_entries);
    total.max_archive_lag_entries = total
        .max_archive_lag_entries
        .max(value.max_archive_lag_entries);
    Some(())
}

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
    resources: RaftGroupResources,
}

struct Cached {
    population: Population,
    started: u64,
    leaders: u32,
    resources: RaftGroupResources,
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
                resources: RaftGroupResources {
                    durable_groups: population.groups,
                    attached_groups: population.cells,
                    parked_groups: population.groups - population.cells,
                    ..Default::default()
                },
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
                resources: cycle.resources,
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
        mut capture: impl FnMut(&GroupStatus) -> Option<RaftGroupResources>,
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
            if capture(&status)
                .and_then(|value| accumulate(&mut cycle.resources, value))
                .is_none()
            {
                self.failed(now);
                return;
            }
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

#[cfg(test)]
#[path = "host_leader_load_tests.rs"]
mod tests;
