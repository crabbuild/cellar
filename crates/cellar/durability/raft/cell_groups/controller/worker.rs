// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::raft::cell_groups::{CellGroupApply, CellGroupEffect, CellGroupPreparedApply};
use cellar_logic::{cell_group::GroupHistory, consensus::CommandFailure};
use futures_util::{future::BoxFuture, stream::FuturesUnordered, StreamExt};
use std::collections::{BTreeSet, VecDeque};

#[derive(Clone)]
enum Work {
    Prepare(Box<CellGroupApply>),
    Publish(Box<CellGroupPreparedApply>),
}

struct Pending {
    group: GroupHistory,
    work: Work,
    retry_at: u64,
}

type Completed = (
    GroupHistory,
    Result<Option<Work>, CellGroupControllerError>,
    Work,
);

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
    requests: tokio::sync::mpsc::Receiver<requests::Request>,
    commands: FuturesUnordered<BoxFuture<'static, requests::Completed>>,
    failed_commands: Vec<requests::Completed>,
    quarantined: BTreeSet<GroupHistory>,
    retained_apply: Vec<Work>,
    provisioning: provision::Manager,
    registrations: FuturesUnordered<BoxFuture<'static, registration::Completed>>,
    routes: BTreeMap<GroupHistory, crate::raft::transport::PhysicalRegistration>,
    progress: tokio::sync::watch::Sender<()>,
    checkpoints: checkpoints::Manager,
    _checkpoint_routes: Vec<crate::raft::transport::CellGroupCheckpointRegistration>,
}

impl Worker {
    pub(super) fn new(
        runtime: CellGroupRuntime,
        events: CellGroupRuntimeEvents,
        transport: RaftTransport,
        sql: CellGroupSqlPool,
        roster: BTreeMap<GroupHistory, CellGroupApplier>,
        requests: tokio::sync::mpsc::Receiver<requests::Request>,
        shared: Arc<Shared>,
    ) -> Result<Self, CellGroupControllerError> {
        let mut checkpoint_routes = Vec::new();
        if shared.checkpoints.is_some() {
            for applier in roster.values() {
                checkpoint_routes.push(
                    transport
                        .register_group_checkpoint(applier.clone())
                        .map_err(CellGroupControllerError::Transport)?,
                );
            }
        }
        Ok(Self {
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
            failed_commands: Vec::new(),
            quarantined: BTreeSet::new(),
            retained_apply: Vec::new(),
            registrations: FuturesUnordered::new(),
            provisioning: provision::Manager::default(),
            routes: BTreeMap::new(),
            progress: tokio::sync::watch::channel(()).0,
            checkpoints: checkpoints::Manager::default(),
            _checkpoint_routes: checkpoint_routes,
        })
    }

    fn launch(&mut self, now: u64) -> Result<(), CellGroupControllerError> {
        // Rotate the bounded roster queue. A busy group cannot monopolize the
        // next available apply slot, and successors never run concurrently.
        for _ in 0..self.pending.len() {
            let Some(job) = self.pending.pop_front() else {
                break;
            };
            if self.quarantined.contains(&job.group) {
                self.retained_apply.push(job.work);
                continue;
            }
            if job.retry_at > now
                || self.active.contains(&job.group)
                || self.checkpoints.contains(&job.group)
            {
                self.pending.push_back(job);
                continue;
            }
            let checkpoint_started = match &job.work {
                Work::Prepare(work) => self.checkpoints.start(work, now, &self.shared)?,
                Work::Publish(_) => false,
            };
            if checkpoint_started {
                continue;
            }
            // Capture/publication has its own bounded capacity and requires
            // no SQL slot. Continue scanning when apply is full so a slow
            // physical apply cannot prevent another group's checkpoint.
            if self.jobs.len() == self.limits.max_parallel_apply {
                self.pending.push_back(job);
                continue;
            }
            self.active.insert(job.group);
            let sql = self.sql.clone();
            let applier = self.roster[&job.group].clone();
            let runtime = self.runtime.clone();
            self.jobs.push(Box::pin(async move {
                let retained = job.work.clone();
                let result = apply(sql, applier, runtime, job.work).await;
                (job.group, result, retained)
            }));
        }
        self.shared
            .active_apply
            .store(self.jobs.len(), Ordering::Release);
        self.shared
            .pending_apply
            .store(self.pending.len(), Ordering::Release);
        Ok(())
    }

    async fn quarantine(
        &mut self,
        group: GroupHistory,
        error: CellGroupControllerError,
    ) -> Result<(), CellGroupControllerError> {
        if failure::node_wide(&error) {
            return Err(error);
        }
        if self.quarantined.insert(group) {
            // Stop application admission first. Runtime revocation then bypasses
            // any waiting apply, fences captured leader authority and refuses
            // new direct/transport work without dropping the RawNode claim.
            self.shared.groups.quarantine(group, error)?;
            self.runtime.quarantine(group).await?;
            self.progress.send_replace(());
        }
        Ok(())
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
            self.launch(now)?;
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
                    match request {
                        requests::Request::Provision(request) => self.provisioning.request(*request, &self.shared),
                        requests::Request::Registration(request) => {
                            let group = request.registration.applier.history();
                            if self.roster.insert(group, request.registration.applier.clone()).is_some() {
                                return Err(CellGroupControllerError::Invalid("duplicate registered applier"));
                            }
                            self.registrations.push(Box::pin(registration::drive(*request, self.runtime.clone(), self.transport.clone())));
                        }
                        other => self.commands.push(Box::pin(requests::drive(other, self.runtime.clone(), self.sql.clone(), self.shared.clone(), self.progress.subscribe()))),
                    }
                }
                Some(done) = self.provisioning.next(), if self.provisioning.is_running() => {
                    self.provisioning.complete(done, &self.shared, &self.transport, &mut self.roster, &mut self.routes)?;
                }
                Some(completed) = self.registrations.next(), if !self.registrations.is_empty() => {
                    let group = completed.group;
                    let uncertain = completed.uncertain();
                    match completed.finish(false) {
                        Ok(Some(guard)) => { self.routes.insert(group, guard); }
                        Ok(None) => unreachable!("active registration completion"),
                        Err(error) => {
                            if !self.quarantined.contains(&group) {
                                self.roster.remove(&group);
                            }
                            if uncertain { return Err(error); }
                        }
                    }
                }
                Some(completed) = self.commands.next(), if !self.commands.is_empty() => {
                    if let Some(error) = completed.failure() {
                        // Retain the invocation and its permit through physical
                        // shutdown even after delivering the error to its caller.
                        let group = completed.group().ok_or(CellGroupControllerError::Invalid("unresolved command without group"))?;
                        self.failed_commands.push(completed);
                        self.quarantine(group, error).await?;
                    }
                }
                Some((group, result, retained)) = self.jobs.next(), if !self.jobs.is_empty() => {
                    self.active.remove(&group);
                    match result {
                        Err(error) => {
                            self.retained_apply.push(retained);
                            self.quarantine(group, error).await?;
                        }
                        Ok(Some(work)) if self.quarantined.contains(&group) => self.retained_apply.push(work),
                        Ok(Some(work)) => {
                            self.shared.apply_retries.fetch_add(1, Ordering::Relaxed);
                            self.pending.push_back(Pending { group, work, retry_at: now.saturating_add(self.limits.retry_interval_ms) });
                        }
                        Ok(None) => (),
                    }
                }
                completed = self.checkpoints.next(), if self.checkpoints.running() => {
                    match completed {
                    checkpoints::Completed::Captured((group, result)) => match result {
                        Ok(checkpoint) if !self.quarantined.contains(&group) => self.checkpoints.publish(group, checkpoint, &self.runtime)?,
                        Ok(_) => {
                            if let Some(work) = self.checkpoints.remove(group, &self.shared) { self.retained_apply.push(Work::Prepare(work)); }
                        }
                        Err(error) => self.checkpoint_failed(group, error, now).await?,
                    },
                    checkpoints::Completed::Admitted((group, result)) => match result {
                        Ok(()) => {
                            if let Some(work) = self.checkpoints.accept(group, &self.shared)? { self.resume_apply(group, work, now); }
                        }
                        Err(error) => self.checkpoint_failed(group, error.into(), now).await?,
                    },
                    }
                }
                Some(()) = self.hints.next(), if !self.hints.is_empty() => (),
                event = self.events.recv() => {
                    let event = event.ok_or(CellGroupControllerError::Closed)?;
                    let group = event.group();
                    if !self.roster.contains_key(&group) {
                        return Err(CellGroupControllerError::Invalid("unregistered group event"));
                    }
                    if self.quarantined.contains(&group) {
                        if let Some(work) = event.retain_apply() {
                            self.retained_apply.push(Work::Prepare(Box::new(work)));
                        }
                        continue;
                    }
                    let effect = match event.effect() {
                        Ok(effect) => effect,
                        Err(error) => {
                            self.quarantine(group, error.clone().into()).await?;
                            continue;
                        }
                    };
                    match effect {
                        CellGroupEffect::Message(message) => {
                            if let Err(error) = self.transport.submit_physical(&group, message.clone()) {
                                if !matches!(error, TransportError::Overloaded | TransportError::UnknownGroup | TransportError::StalePeer | TransportError::Refused) {
                                    return Err(CellGroupControllerError::Transport(error));
                                }
                                self.shared.send_refusals.fetch_add(1, Ordering::Relaxed);
                                // A hint is advisory, never delivery or commit evidence.
                                // Excess ordinary unreachable hints may be dropped.
                                let snapshot = message.get_msg_type() == ::raft::prelude::MessageType::MsgSnapshot;
                                // Snapshot failure cannot be dropped: Raft pauses
                                // that peer until this feedback. At most one can
                                // be pending per known peer; ordinary hints keep
                                // the stricter one-per-group aggregate cap.
                                if snapshot || self.hints.len() < self.limits.max_groups {
                                    let runtime = self.runtime.clone();
                                    let member = message.to;
                                    self.hints.push(Box::pin(async move {
                                        if snapshot {
                                            let _ = runtime.report_snapshot(group, member, false).await;
                                        } else {
                                            let _ = runtime.report_unreachable(group, member).await;
                                        }
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
                        CellGroupEffect::Applied { .. } | CellGroupEffect::CheckpointInstalled { .. } => {
                            // Only this post-sync event advances the published count.
                            self.shared.applied.fetch_add(1, Ordering::Relaxed);
                            self.progress.send_replace(());
                        }
                        CellGroupEffect::CheckpointPublished { manifest } => {
                            self.shared.checkpoints_published.fetch_add(1, Ordering::Relaxed);
                            if let Some(work) = self.checkpoints.observed(group, manifest.position().index.get(), &self.shared)? {
                                self.resume_apply(group, work, now);
                            }
                            // Local compaction does not advance applied state.
                            self.progress.send_replace(());
                        }
                        CellGroupEffect::ReadIndex { .. } => (), // Runtime-owned read authority.
                        CellGroupEffect::Persist(_) => return Err(CellGroupControllerError::Invalid("escaped persistence")),
                    }
                }
            }
        }
    }

    fn resume_apply(&mut self, group: GroupHistory, work: Box<CellGroupApply>, now: u64) {
        if self.quarantined.contains(&group) {
            self.retained_apply.push(Work::Prepare(work));
        } else {
            self.pending.push_back(Pending {
                group,
                work: Work::Prepare(work),
                retry_at: now,
            });
        }
    }

    async fn checkpoint_failed(
        &mut self,
        group: GroupHistory,
        error: CellGroupControllerError,
        now: u64,
    ) -> Result<(), CellGroupControllerError> {
        if checkpoints::retryable(&error) {
            if let Some(work) = self.checkpoints.retry(group, now, &self.shared) {
                self.resume_apply(group, work, now);
            }
        } else {
            if let Some(work) = self.checkpoints.remove(group, &self.shared) {
                self.retained_apply.push(Work::Prepare(work));
            }
            self.quarantine(group, error).await?;
        }
        Ok(())
    }

    pub(super) async fn finish(&mut self) -> Result<(), CellGroupControllerError> {
        self.requests.close();
        while self.requests.try_recv().is_ok() {}
        // Attachment may already own a RawNode/claim or be inside catalog I/O.
        // Poll it through completion before closing its physical dependencies.
        while let Some(completed) = self.registrations.next().await {
            let _ = completed.finish(true);
        }
        self.provisioning.join().await;
        self.checkpoints.join().await;
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
