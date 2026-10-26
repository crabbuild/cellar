// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded hints decide order only. Every attempt revalidates durable authority.
use super::*;

pub(super) struct Entry {
    pub pending: FleetPendingMovement,
    pub status: FleetMovementServiceEntry,
    pub next_at: u64,
}

#[derive(Default)]
pub(super) struct Schedule {
    pub entries: Vec<Entry>,
    cursor: usize,
    priority_streak: u8,
}

impl Schedule {
    pub fn refresh(&mut self, pending: Vec<FleetPendingMovement>) {
        // Do not forget disappeared work: its settlement reply may have been
        // lost. Resolve its permanent receipt before reclaiming a local slot.
        for movement in pending {
            if self.entries.len() == super::super::super::MAX_MOVES {
                break;
            }
            if self
                .entries
                .iter()
                .any(|entry| entry.pending.reservation == movement.reservation)
            {
                continue;
            }
            self.entries.push(Entry {
                status: FleetMovementServiceEntry {
                    reservation: movement.reservation,
                    phase: None,
                    consecutive_failures: 0,
                    last_failure: None,
                    needs_operator: false,
                },
                pending: movement,
                next_at: 0,
            });
        }
    }

    pub fn select(&mut self, now: u64) -> Option<usize> {
        let len = self.entries.len();
        let ready = |index: &usize| self.entries[*index].next_at <= now;
        let ordered = (0..len).map(|offset| (self.cursor + offset) % len);
        let first = ordered.clone().find(ready)?;
        let priority = ordered.filter(ready).find(|index| {
            matches!(
                self.entries[*index].status.phase,
                Some(
                    FleetMovementPhase::Joint
                        | FleetMovementPhase::Complete
                        | FleetMovementPhase::Fenced
                        | FleetMovementPhase::Aborted
                )
            )
        });
        let selected = if self.priority_streak < 3 {
            priority.unwrap_or(first)
        } else {
            first
        };
        self.priority_streak = if priority == Some(selected) {
            self.priority_streak.saturating_add(1)
        } else {
            0
        };
        if self.priority_streak >= 4 {
            self.priority_streak = 0;
        }
        self.cursor = (selected + 1) % len;
        Some(selected)
    }
}

pub(super) fn retry_delay(
    options: &FleetMovementServiceOptions,
    failures: u32,
    jitter: u32,
) -> u64 {
    let base = options.interval.as_millis() as u64;
    let max = options.retry_max_interval.as_millis() as u64;
    let exponential = base.saturating_mul(1_u64 << failures.min(16)).min(max);
    exponential
        .saturating_add(u64::from(jitter) % (exponential / 4 + 1))
        .min(max)
}

pub(super) fn classify(error: &FleetCatalogError) -> FleetMovementServiceFailure {
    match error {
        FleetCatalogError::Conflict => FleetMovementServiceFailure::Conflict,
        FleetCatalogError::Overloaded => FleetMovementServiceFailure::Overloaded,
        FleetCatalogError::Unconfirmed => FleetMovementServiceFailure::Unconfirmed,
        FleetCatalogError::Storage(_) => FleetMovementServiceFailure::Storage,
        FleetCatalogError::Observation(_) => FleetMovementServiceFailure::Observation,
        FleetCatalogError::Closed => FleetMovementServiceFailure::Closed,
        _ => FleetMovementServiceFailure::Rejected,
    }
}
