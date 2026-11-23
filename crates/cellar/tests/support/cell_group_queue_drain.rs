// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Observe the unchanged drain reads without adding requests or changing proof.

use super::*;
use serde_json::Value;

#[derive(Default, serde::Serialize)]
struct ReadCost {
    completed: u64,
    succeeded: u64,
    failed: u64,
    sum_ms: u64,
    max_ms: u64,
}

impl ReadCost {
    fn record(&mut self, elapsed_ms: u64, succeeded: bool) {
        self.completed += 1;
        self.succeeded += u64::from(succeeded);
        self.failed += u64::from(!succeeded);
        self.sum_ms = self.sum_ms.saturating_add(elapsed_ms);
        self.max_ms = self.max_ms.max(elapsed_ms);
    }
}

#[derive(Clone, Copy)]
enum Read {
    Info,
    Peek,
}

impl Read {
    fn name(self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Peek => "peek",
        }
    }
}

pub(super) struct Observation {
    started: u64,
    info: ReadCost,
    peek: ReadCost,
    pending: Option<(Read, u64)>,
}

impl Observation {
    pub(super) fn new() -> Self {
        Self {
            started: cellar::asyncrt::mono_ms(),
            info: ReadCost::default(),
            peek: ReadCost::default(),
            pending: None,
        }
    }

    pub(super) async fn info(&mut self, cluster: &ManagedCluster, queue: &str) -> Result<Value> {
        self.read(cluster, queue, Read::Info).await
    }

    pub(super) async fn peek(&mut self, cluster: &ManagedCluster, queue: &str) -> Result<Value> {
        self.read(cluster, queue, Read::Peek).await
    }

    async fn read(&mut self, cluster: &ManagedCluster, queue: &str, read: Read) -> Result<Value> {
        let started = cellar::asyncrt::mono_ms();
        self.pending = Some((read, started));
        let body = match read {
            Read::Info => json!({"op":"info"}),
            Read::Peek => json!({"op":"peek","limit":100}),
        };
        let result = admin(cluster, queue, body).await;
        let elapsed = cellar::asyncrt::mono_ms().saturating_sub(started);
        let cost = match read {
            Read::Info => &mut self.info,
            Read::Peek => &mut self.peek,
        };
        cost.record(elapsed, result.is_ok());
        self.pending = None;
        result
    }

    pub(super) fn finish(
        self,
        scenario: &Scenario,
        queue: &str,
        last: &Option<(Value, Value)>,
        succeeded: bool,
    ) {
        let at = cellar::asyncrt::mono_ms();
        let rooted = scenario.cluster.fixtures.first().map(|fixture| {
            fixture.declaration.namespace.image_storage == RaftImageStorage::Extents
        });
        let queue_kind = if queue == QUEUE {
            "primary"
        } else if queue == FAST {
            "fast"
        } else {
            "other"
        };
        let backlog = last
            .as_ref()
            .and_then(|(info, _)| info["backlogCount"].as_f64());
        let rows = last
            .as_ref()
            .and_then(|(_, peek)| peek["messages"].as_array());
        let mut states = BTreeMap::new();
        if let Some(rows) = rows {
            for row in rows {
                if let Some(state) = row["state"].as_str() {
                    *states.entry(state).or_insert(0usize) += 1;
                }
            }
        }
        let completed = scenario.signals.completed.lock().ok().map(|v| v.len());
        // The original timeout drops only the observing read future. A pending
        // call is reported separately, never counted as a terminal response.
        eprintln!(
            "QUEUE_DRAIN_OBSERVATION {}",
            json!({"extents":rooted,"queueKind":queue_kind,"succeeded":succeeded,
                "originalBudgetMs":30000,"elapsedMs":at.saturating_sub(self.started),
                "info":self.info,"peek":self.peek,
                "pendingRead":self.pending.map(|(kind,_)|kind.name()),
                "pendingElapsedMs":self.pending.map(|(_,started)|at.saturating_sub(started)),
                "lastInfoBacklogCount":backlog,"lastPeekRows":rows.map(Vec::len),
                "lastPeekStates":states,"consumerReportedCompletions":completed})
        );
        if !succeeded {
            // Capture local counters only after the unchanged drain has ended.
            // No extra RPC, timer, worker or retry participates in its oracle.
            for (node, host) in scenario.cluster.hosts.iter().enumerate() {
                eprintln!(
                    "QUEUE_DRAIN_FAILURE {}",
                    json!({"extents":rooted,"queueKind":queue_kind,"node":node,
                        "resources":host.resource_usage(),"performance":host.performance()})
                );
            }
        }
    }
}
