// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::raft::cell_groups::{CellGroupApply, CellGroupEffect, CellGroupPreparedApply};
use cellar_logic::{cell_group::GroupHistory, consensus::CommandFailure};
use futures_util::{future::BoxFuture, stream::FuturesUnordered, StreamExt};
use std::collections::{BTreeSet, VecDeque};

enum Work {
    Prepare(Box<CellGroupApply>),
    Publish(Box<CellGroupPreparedApply>),
}

struct Pending {
    group: GroupHistory,
    work: Work,
    retry_at: u64,
}

type Completed = (GroupHistory, Result<Option<Work>, CellGroupControllerError>);

pub(super) struct Worker {
    runtime: CellGroupRuntime,
    events: CellGroupRuntimeEvents,
    transport: RaftTransport,
    sql: CellGroupSqlPool,
    roster: BTreeMap<GroupHistory, CellGroupApplier>,
    limits: CellGroupControllerLimits,
    shared: Arc<Shared>,
    pending: VecDeque<Pending>,
    active: BTreeSet<GroupHistory>,
    jobs: FuturesUnordered<BoxFuture<'static, Completed>>,
    hints: FuturesUnordered<BoxFuture<'static, ()>>,
    requests: tokio::sync::mpsc::Receiver<commands::Request>,
    commands: FuturesUnordered<BoxFuture<'static, commands::Completed>>,
    failed_command: Option<commands::Completed>,
    progress: tokio::sync::watch::Sender<()>,
}

impl Worker {
    pub(super) fn new(
        runtime: CellGroupRuntime,
        events: CellGroupRuntimeEvents,
        transport: RaftTransport,
        sql: CellGroupSqlPool,
        roster: BTreeMap<GroupHistory, CellGroupApplier>,
        requests: tokio::sync::mpsc::Receiver<commands::Request>,
        shared: Arc<Shared>,
    ) -> Self {
        Self {
            runtime,
            events,
            transport,
            sql,
            roster,
            limits: shared.limits,
            shared,
            pending: VecDeque::new(),
            active: BTreeSet::new(),
            jobs: FuturesUnordered::new(),
            hints: FuturesUnordered::new(),
            requests,
            commands: FuturesUnordered::new(),
            failed_command: None,
            progress: tokio::sync::watch::channel(()).0,
        }
    }

    fn launch(&mut self, now: u64) {
        // Rotate the bounded roster queue. A busy group cannot monopolize the
        // next available apply slot, and successors never run concurrently.
        for _ in 0..self.pending.len() {
            if self.jobs.len() == self.limits.max_parallel_apply {
                break;
            }
            let Some(job) = self.pending.pop_front() else {
                break;
            };
            if job.retry_at > now || self.active.contains(&job.group) {
                self.pending.push_back(job);
                continue;
            }
            self.active.insert(job.group);
            let sql = self.sql.clone();
            let applier = self.roster[&job.group].clone();
            let runtime = self.runtime.clone();
            self.jobs.push(Box::pin(async move {
                let result = apply(sql, applier, runtime, job.work).await;
                (job.group, result)
            }));
        }
        self.shared
            .active_apply
            .store(self.jobs.len(), Ordering::Release);
        self.shared
            .pending_apply
            .store(self.pending.len(), Ordering::Release);
    }

    pub(super) async fn run(
        &mut self,
        domain: &crate::asyncrt::DomainToken,
    ) -> Result<(), CellGroupControllerError> {
        let mut wake_at = domain
            .mono_ms()
            .map_err(|_| CellGroupControllerError::Closed)?
            .saturating_add(self.limits.retry_interval_ms);
        loop {
            if self.shared.closing.load(Ordering::Acquire) {
                return Ok(());
            }
            let now = domain
                .mono_ms()
                .map_err(|_| CellGroupControllerError::Closed)?;
            self.launch(now);
            let timer = domain
                .sleep_until(wake_at)
                .map_err(|_| CellGroupControllerError::Closed)?;
            crate::asyncrt::select! {
                _ = self.shared.stop.notified() => return Ok(()),
                _ = domain.wait_for_work() => return Err(CellGroupControllerError::Closed),
                _ = timer => {
                    self.progress.send_replace(());
                    wake_at = domain.mono_ms().map_err(|_| CellGroupControllerError::Closed)?
                        .saturating_add(self.limits.retry_interval_ms);
                },
                Some(request) = self.requests.recv() => {
                    self.commands.push(Box::pin(commands::drive(request, self.runtime.clone(), self.sql.clone(), self.shared.clone(), self.progress.subscribe())));
                }
                Some(completed) = self.commands.next(), if !self.commands.is_empty() => {
                    if completed.invocation.unresolved() {
                        let error = completed.error.clone().map_or(CellGroupControllerError::Invalid("unresolved successful command"), CellGroupControllerError::Command);
                        // Retain the invocation and its permit through physical
                        // shutdown even after delivering the error to its caller.
                        self.failed_command = Some(completed);
                        return Err(error);
                    }
                }
                Some((group, result)) = self.jobs.next(), if !self.jobs.is_empty() => {
                    self.active.remove(&group);
                    if let Some(work) = result? {
                        self.shared.apply_retries.fetch_add(1, Ordering::Relaxed);
                        self.pending.push_back(Pending { group, work, retry_at: now.saturating_add(self.limits.retry_interval_ms) });
                    }
                }
                Some(()) = self.hints.next(), if !self.hints.is_empty() => (),
                event = self.events.recv() => {
                    let event = event.ok_or(CellGroupControllerError::Closed)?;
                    let group = event.group();
                    if !self.roster.contains_key(&group) {
                        return Err(CellGroupControllerError::Invalid("unregistered group event"));
                    }
                    match event.effect().map_err(Clone::clone)? {
                        CellGroupEffect::Message(message) => {
                            if let Err(error) = self.transport.submit_physical(&group, message.clone()) {
                                if !matches!(error, TransportError::Overloaded | TransportError::UnknownGroup | TransportError::StalePeer) {
                                    return Err(CellGroupControllerError::Transport(error));
                                }
                                self.shared.send_refusals.fetch_add(1, Ordering::Relaxed);
                                // A hint is advisory, never delivery or commit evidence.
                                // Drop excess hints; Raft's own ticks retry lost traffic.
                                if self.hints.len() < self.limits.max_groups {
                                    let runtime = self.runtime.clone();
                                    let member = message.to;
                                    self.hints.push(Box::pin(async move {
                                        let _ = runtime.report_unreachable(group, member).await;
                                    }));
                                }
                            }
                        }
                        CellGroupEffect::Apply(_) => {
                            // One pending successor per group is possible before a
                            // completed publication future has been polled here.
                            if self.pending.iter().any(|job| job.group == group) {
                                return Err(CellGroupControllerError::Invalid("duplicate group apply"));
                            }
                            let work = event.retain_apply().ok_or(CellGroupControllerError::Invalid("apply ticket"))?;
                            self.pending.push_back(Pending { group, work: Work::Prepare(Box::new(work)), retry_at: now });
                        }
                        CellGroupEffect::Applied { .. } => {
                            // Only this post-sync event advances the published count.
                            self.shared.applied.fetch_add(1, Ordering::Relaxed);
                            self.progress.send_replace(());
                        }
                        CellGroupEffect::ReadIndex { .. } => (), // Runtime-owned read authority.
                        CellGroupEffect::Persist(_) => return Err(CellGroupControllerError::Invalid("escaped persistence")),
                    }
                }
            }
        }
    }

    pub(super) async fn finish(&mut self) -> Result<(), CellGroupControllerError> {
        self.transport.close().await;
        let sql = self
            .sql
            .close()
            .await
            .map_err(|error| CellGroupControllerError::Apply(Arc::new(error)));
        // Retain the runtime's engine claim while an apply worker can still
        // write. A replacement runtime must not overlap physical preparation.
        let runtime = self
            .runtime
            .close()
            .await
            .map_err(CellGroupControllerError::Runtime);
        // Keep all unfinished tickets and appliers until these physical joins.
        sql.and(runtime)
    }
}

async fn apply(
    sql: CellGroupSqlPool,
    applier: CellGroupApplier,
    runtime: CellGroupRuntime,
    work: Work,
) -> Result<Option<Work>, CellGroupControllerError> {
    let prepared = match work {
        Work::Prepare(work) => match sql.prepare_apply(&applier, (*work).clone()).await {
            Ok(prepared) => Box::new(prepared),
            Err(ExecutionError::Busy | ExecutionError::Refused(CommandFailure::Overloaded)) => {
                return Ok(Some(Work::Prepare(work)))
            }
            Err(error) => return Err(CellGroupControllerError::Apply(Arc::new(error))),
        },
        Work::Publish(prepared) => prepared,
    };
    match (*prepared).clone().complete(&runtime).await {
        Ok(()) => Ok(None),
        Err(RuntimeError::Overloaded) => Ok(Some(Work::Publish(prepared))),
        Err(error) => Err(error.into()),
    }
}
