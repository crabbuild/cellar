// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded, payload-free timing evidence for the lifted-limit integration case.
#![allow(clippy::disallowed_methods)]
use anyhow::{ensure, Result};
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
    time::Instant,
};

pub(super) struct Observation {
    started: Instant,
    phases: Mutex<Vec<(&'static str, Instant)>>,
    blob_puts: AtomicUsize,
}

impl Observation {
    pub(super) fn new() -> Self {
        Self {
            started: Instant::now(),
            phases: Mutex::new(Vec::new()),
            blob_puts: AtomicUsize::new(0),
        }
    }

    pub(super) fn phase(&self, name: &'static str) -> Result<()> {
        let now = Instant::now();
        let mut phases = self.phases.lock().unwrap();
        ensure!(phases.len() < 32, "cell-limit phase observation overflow");
        phases.push((name, now));
        Ok(())
    }

    pub(super) fn blob_put_completed(&self) {
        self.blob_puts.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn snapshot(&self, pending_events: Option<usize>) -> Value {
        let now = Instant::now();
        let phases = self.phases.lock().unwrap();
        let elapsed = phases
            .iter()
            .enumerate()
            .map(|(index, (name, started))| {
                let ended = phases.get(index + 1).map_or(now, |(_, at)| *at);
                json!({"phase":name,"elapsed_ms":ended.duration_since(*started).as_millis(),
                    "complete":index + 1 < phases.len()})
            })
            .collect::<Vec<_>>();
        json!({"format":1,"elapsed_ms":now.duration_since(self.started).as_millis(),
            "phases":elapsed,"completed_blob_puts":self.blob_puts.load(Ordering::Relaxed),
            "pending_application_events":pending_events})
    }
}
