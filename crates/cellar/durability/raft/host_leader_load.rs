// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Node-wide leadership observations, collected in bounded owned batches.
//! A count is published only after every attached history was sampled.
use super::*;
use crate::application::consensus::{RaftGroupResources, RaftReplicaRole};
use crate::raft::inspection::LeaderLoad;
use std::ops::Bound::{Excluded, Unbounded};

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
        let begins = scanner.begin(now, population, self.activating.is_some());
        if let Ok(mut source) = self.local.groups.0.lock() {
            *source = scanner
                .cached
                .as_ref()
                .filter(|cached| cached.population == population && self.activating.is_none())
                .map(|cached| (cached.started, cached.resources));
        }
        if !begins {
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
            scanner.accept(
                completed,
                now,
                population,
                self.activating.is_some(),
                |status| {
                    let cell = self.cells.get(&status.replica.scope)?;
                    if cell.group.identity() != &status.replica {
                        return None;
                    }
                    let registered = self
                        .local
                        .bindings
                        .get(&(
                            status.replica.scope.namespace,
                            status.replica.scope.cell.clone(),
                        ))
                        .is_some_and(|binding| binding.class.has_queries());
                    let (role, queries) = readiness::capture(
                        cell,
                        status,
                        now,
                        self.options.stale_source_timeout_ms,
                        self.local.admission.is_draining(),
                        registered,
                    )
                    .ok()?;
                    let apply_lag = status
                        .committed
                        .get()
                        .checked_sub(status.applied.index.get())?;
                    let archive_lag = status.committed.get().saturating_sub(cell.archived.get());
                    Some(RaftGroupResources {
                        leaders: usize::from(role == RaftReplicaRole::Leader),
                        voters: usize::from(role == RaftReplicaRole::Voter),
                        learners: usize::from(role == RaftReplicaRole::Learner),
                        removed: usize::from(role == RaftReplicaRole::Removed),
                        unknown: usize::from(role == RaftReplicaRole::Unknown),
                        coherent_images: usize::from(queries.coherent_image),
                        historical_queries: usize::from(queries.historical),
                        recent_source_queries: usize::from(queries.recent_source),
                        failed: usize::from(cell.failed),
                        retired: usize::from(cell.retired),
                        sql_busy: usize::from(cell.sql_busy),
                        pending_commands: usize::from(cell.pending.is_some()),
                        incoming_seeds: usize::from(cell.incoming.is_some()),
                        applying: usize::from(
                            cell.apply.is_some()
                                || cell.publication.is_some()
                                || cell.apply_inflight,
                        ),
                        archive_pending: usize::from(archive_lag > 0),
                        archive_current: usize::from(cell.archived.get() > 0 && archive_lag == 0),
                        archive_unconfirmed: usize::from(cell.archived.get() == 0),
                        apply_lag_entries: apply_lag,
                        max_apply_lag_entries: apply_lag,
                        archive_lag_entries: archive_lag,
                        max_archive_lag_entries: archive_lag,
                        ..Default::default()
                    })
                },
            );
            if let Ok(mut source) = self.local.groups.0.lock() {
                *source = scanner
                    .cached
                    .as_ref()
                    .filter(|cached| cached.population == population && self.activating.is_none())
                    .map(|cached| (cached.started, cached.resources));
            }
        }
    }
}

#[cfg(test)]
#[path = "host_leader_load_tests.rs"]
mod tests;
