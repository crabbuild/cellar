// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded diagnostics for explicitly tagged, read-only fault probes.
//! These observations never establish a quorum, successful read or durability.
use anyhow::{ensure, Result};
use serde::Serialize;
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

const CAPACITY: usize = 64;

#[derive(Clone, Serialize)]
struct Observation {
    sequence: u64,
    probe_id: String,
    cell: usize,
    started_us: u64,
    status_finished_us: Option<u64>,
    replication_started_us: Option<u64>,
    replication_finished_us: Option<u64>,
    finished_us: Option<u64>,
    outcome: &'static str,
}

#[derive(Default)]
struct History {
    next: u64,
    evicted: u64,
    rows: VecDeque<(u64, Observation)>,
}

#[derive(Clone)]
pub(super) struct Probes {
    source: String,
    history: Arc<Mutex<History>>,
}

impl Probes {
    pub(super) fn new(source: String) -> Self {
        Self {
            source,
            history: Arc::new(Mutex::new(History::default())),
        }
    }

    // Called within the fixture-owned execution domain. The clock has its own
    // origin: compare durations, never subtract it from host/Python timestamps.
    pub(super) fn start(&self, id: &str, cell: usize) -> Result<Probe> {
        ensure!(
            id.len() <= 41
                && id.split(':').count() == 2
                && id
                    .split(':')
                    .all(|part| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit())),
            "invalid probe identity"
        );
        let mut history = self
            .history
            .lock()
            .map_err(|_| anyhow::anyhow!("poisoned probe history"))?;
        let sequence = history.next;
        history.next = sequence
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("probe sequence exhausted"))?;
        if history.rows.len() == CAPACITY {
            history.rows.pop_front();
            history.evicted = history
                .evicted
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("probe eviction count exhausted"))?;
        }
        history.rows.push_back((
            sequence,
            Observation {
                sequence,
                probe_id: id.into(),
                cell,
                started_us: cellar::asyncrt::mono_us(),
                status_finished_us: None,
                replication_started_us: None,
                replication_finished_us: None,
                finished_us: None,
                outcome: "pending",
            },
        ));
        Ok(Probe {
            history: self.history.clone(),
            sequence,
        })
    }

    pub(super) fn snapshot(&self) -> Result<serde_json::Value> {
        let history = self
            .history
            .lock()
            .map_err(|_| anyhow::anyhow!("poisoned probe history"))?;
        Ok(
            serde_json::json!({"source": self.source, "capacity": CAPACITY,
            "started": history.next, "evicted": history.evicted,
            "observations": history.rows.iter().map(|(_, row)| row).collect::<Vec<_>>() }),
        )
    }
}

pub(super) struct Probe {
    history: Arc<Mutex<History>>,
    sequence: u64,
}

impl Probe {
    fn update(&self, f: impl FnOnce(&mut Observation)) {
        if let Ok(mut history) = self.history.lock() {
            if let Some((_, row)) = history.rows.iter_mut().find(|(id, _)| *id == self.sequence) {
                f(row);
            }
        }
        // Poison is exposed by snapshot(); eviction is explicit in its counts.
    }

    pub(super) fn status_finished(&self) {
        let now = cellar::asyncrt::mono_us();
        self.update(|row| row.status_finished_us = Some(now));
    }

    pub(super) fn replication_started(&self) {
        let now = cellar::asyncrt::mono_us();
        self.update(|row| row.replication_started_us = Some(now));
    }

    pub(super) fn replication_finished(&self) {
        let now = cellar::asyncrt::mono_us();
        self.update(|row| row.replication_finished_us = Some(now));
    }

    pub(super) fn finish(&self, success: bool) {
        let now = cellar::asyncrt::mono_us();
        self.update(|row| {
            row.finished_us = Some(now);
            row.outcome = if success { "success" } else { "error" };
        });
    }
}

impl Drop for Probe {
    fn drop(&mut self) {
        // Drop may run outside the execution domain. Do not invent an end time
        // for a cancelled handler, or turn its partially observed work into a read.
        self.update(|row| {
            if row.outcome == "pending" {
                row.outcome = "cancelled";
            }
        });
    }
}

#[cfg(test)]
#[path = "raft_probe_tests.rs"]
mod tests;
