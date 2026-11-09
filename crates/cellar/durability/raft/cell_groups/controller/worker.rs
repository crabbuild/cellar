// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::retirement as registration_retirement;
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
    archives: archive::Manager,
    late_archives: archive::late::Manager,
    effects: effects::Manager,
    _checkpoint_routes: Vec<crate::raft::transport::CellGroupCheckpointRegistration>,
    removed_after: Option<GroupHistory>,
    removed_at: u64,
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
        // Retained replicas serve peer and S3 seeds whenever the transport
        // pool is configured, independently of periodic local checkpoints.
        let serve_checkpoints = match transport.group_checkpoint_pool() {
            Ok(_) => true,
            Err(TransportError::Refused) => false,
            Err(error) => return Err(CellGroupControllerError::Transport(error)),
        };
        let mut checkpoint_routes = Vec::new();
        if serve_checkpoints {
            for applier in roster.values() {
                checkpoint_routes.push(
                    transport
                        .register_group_checkpoint(applier.clone())
                        .map_err(CellGroupControllerError::Transport)?,
                );
            }
        }
        let archives = archive::Manager::default();
        transport
            .enable_group_archive_seeds(archive::seed::Client::new(&shared))
            .map_err(CellGroupControllerError::Transport)?;
        let effects = effects::Manager::new(&roster)?;
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
            archives,
            late_archives: archive::late::Manager::default(),
            effects,
            _checkpoint_routes: checkpoint_routes,
            removed_after: None,
            removed_at: 0,
        })
    }

    async fn retire_source(
        &mut self,
        source: &registration_retirement::RetiredSource,
    ) -> Result<(), CellGroupControllerError> {
        source.check(self.runtime.log())?;
        let history = source.history();
        if self.shared.groups.begin_retirement(history)? {
            return Ok(());
        }
        let sql_idle = self
            .sql
            .retire_group(history)
            .map_err(|error| CellGroupControllerError::Apply(Arc::new(error)))?;
        let seeds_idle = self
            .shared
            .archive_seeds
            .retire(history)
            .map_err(|error| CellGroupControllerError::Archive(Arc::new(error)))?;
        let effects_idle = self.effects.retiring(history);
        let archive_idle = self.archives.retire(history, &self.shared);
        let discovery_idle = self.late_archives.retire(history, &self.shared);
        let provision_idle = self.provisioning.retire(history);
        // Freeze only this owner, then keep driving its persistence and apply
        // completions. A retry can detach it after all queued work settles.
        self.runtime.quiesce_retired_source(source.clone()).await?;
        if !sql_idle
            || !seeds_idle
            || !effects_idle
            || !archive_idle
            || !discovery_idle
            || !provision_idle
            || self.active.contains(&history)
            || self.pending.iter().any(|work| work.group == history)
            || self.checkpoints.contains(&history)
            || self
                .runtime
                .log()
                .cell_group(&history)
                .map_err(RuntimeError::from)?
                .consensus_claimed
                .load(Ordering::Acquire)
        {
            return Err(RuntimeError::NotReady.into());
        }
        // The physical engine history stays in the log and roster. Only active
        // dispatch, page/applier and checkpoint-registration owners leave here.
        self.transport
            .retire_physical_group(source)
            .map_err(CellGroupControllerError::Transport)?;
        self.routes.remove(&history);
        self._checkpoint_routes
            .retain(|route| route.history() != history);
        if let Some(applier) = self.roster.remove(&history) {
            self.shared
                .retired_sources
                .lock()
                .map_err(|_| CellGroupControllerError::Closed)?
                .insert(history, applier);
        }
        self.effects.remove(&history);
        self.shared
            .groups
            .finish_retirement(history, source.is_removed_member())?;
        self.progress.send_replace(());
        Ok(())
    }

    async fn retire_removed(&mut self, now: u64) -> Result<(), CellGroupControllerError> {
        if now < self.removed_at {
            return Ok(());
        }
        self.removed_at = now.saturating_add(self.limits.retry_interval_ms);
        // One local history per existing retry tick. No namespace/cell scan,
        // catalog CAS, additional worker or per-group work queue is introduced.
        let next = self
            .removed_after
            .and_then(|after| {
                self.roster
                    .range((std::ops::Bound::Excluded(after), std::ops::Bound::Unbounded))
                    .next()
                    .map(|(history, _)| *history)
            })
            .or_else(|| self.roster.keys().next().copied());
        self.removed_after = next;
        let Some(history) = next else {
            return Ok(());
        };
        if self.quarantined.contains(&history) {
            return Ok(());
        }
        let result = async {
            if let Some(source) = super::removed_members::source(&self.runtime, history)? {
                // Retain its applier/images, engine mapping and every physical
                // charge. Only the joined consensus/dispatch owner is withdrawn;
                // reclamation still needs its separate rooted authorization.
                Box::pin(self.retire_source(&source)).await?;
            }
            Ok(())
        }
        .await;
        match result {
            Ok(())
            | Err(CellGroupControllerError::Runtime(
                RuntimeError::NotReady | RuntimeError::Overloaded | RuntimeError::StaleCompletion,
            )) => Ok(()),
            Err(CellGroupControllerError::Runtime(RuntimeError::Storage(
                crate::raft::storage::LogError::WrongIdentity,
            ))) => Ok(()),
            Err(CellGroupControllerError::Apply(error))
                if matches!(
                    error.as_ref(),
                    ExecutionError::Busy | ExecutionError::Refused(CommandFailure::Overloaded)
                ) =>
            {
                Ok(())
            }
            Err(error) => self.quarantine(history, error).await,
        }
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
            Box::pin(self.retire_removed(now)).await?;
            self.launch(now)?;
            self.late_archives.launch(&self.archives, &self.shared)?;
            self.archives.launch(&self.runtime, &self.shared, now);
            self.shared.archive_seeds.maintain(&self.shared, now);
            self.effects.launch(&self.quarantined, &self.shared, now)?;
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
                        requests::Request::Reclamation(request) => {
                            let result = async {
                                let done = source_reclaim::drive(&self.shared, &request).await?;
                                if done {
                                    request.release_retirement(&self.shared, &self.sql).await?;
                                }
                                Ok(done)
                            }.await;
                            let fatal = result.as_ref().err().filter(|error| failure::node_wide(error)).cloned();
                            let _ = request.reply.send(super::calls::Delivery { result: result.map_err(Arc::new), _permit: request.memory });
                            if let Some(error) = fatal { return Err(error); }
                        }
                        requests::Request::Retirement(request) => {
                            let result = self.retire_source(&request.source).await.map_err(Arc::new);
                            let _ = request.reply.send(super::calls::Delivery { result, _permit: request.memory });
                        }
                        requests::Request::Archive(request) => self.archives.request(*request, &self.shared),
                        requests::Request::EnsureArchive(group) => self.late_archives.request(group, &self.archives, &self.shared)? ,
                        requests::Request::S3(request) => self.archives.settle(*request),
                        requests::Request::Provision(request) => self.provisioning.request(*request, &self.shared, &self.transport),
                        requests::Request::Registration(request) => {
                            let group = request.registration.applier.history();
                            if self.roster.insert(group, request.registration.applier.clone()).is_some() {
                                return Err(CellGroupControllerError::Invalid("duplicate registered applier"));
                            }
                            self.effects.register(&request.registration.applier)?;
                            self.registrations.push(Box::pin(registration::drive(*request, self.runtime.clone(), self.transport.clone())));
                        }
                        other => self.commands.push(Box::pin(requests::drive(other, self.runtime.clone(), self.sql.clone(), self.shared.clone(), self.progress.subscribe(), self.transport.clone()))),
                    }
                }
                Some(done) = self.provisioning.next(), if self.provisioning.is_running() => {
                    let group = done.group();
                    self.provisioning.complete(done, &self.shared, &self.transport, &mut self.roster, &mut self.routes, &mut self.archives)?;
                    if let Some(applier) = self.roster.get(&group) { self.effects.register(applier)?; }
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
                                self.effects.remove(&group);
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
                Some(completed) = self.effects.next(), if self.effects.running() => {
                    if let Some(applier) = self.roster.get(&completed.group) { self.effects.register(applier)?; }
                    self.effects.complete(completed, &self.shared);
                }
                completed = self.late_archives.next(), if self.late_archives.running() => {
                    self.late_archives.complete(completed, &mut self.archives, &self.shared);
                    self.progress.send_replace(());
                }
                completed = self.archives.next(), if self.archives.running() => {
                    self.archives.complete(completed, &self.shared, now, false);
                    self.progress.send_replace(());
                }
                event = self.events.recv() => {
                    let event = event.ok_or(CellGroupControllerError::Closed)?;
                    let group = event.group();
                    if !self.roster.contains_key(&group) && (self.shared.groups.retired(&group)
                        || self.runtime.log().source_reclamation(group).await.map_err(RuntimeError::from)?.is_some()) {
                        // Metadata/messages can have been delivered before the
                        // runtime detached. Apply tickets prevent physical join.
                        if event.retain_apply().is_some() { return Err(CellGroupControllerError::Invalid("retired source apply event")); }
                        continue;
                    }
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
                    let installed_checkpoint = matches!(&effect, CellGroupEffect::CheckpointInstalled { .. });
                    match effect {
                        CellGroupEffect::Message(message) => {
                            if let Err(error) = self.transport.submit_cell_group_event(&event) {
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
                                            let _ = runtime.admit_unreachable(group, member).await;
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
                        CellGroupEffect::Applied { manifest, .. } | CellGroupEffect::CheckpointInstalled { manifest } => {
                            // Configuration apply sets its revision to this
                            // entry's index. Ordinary cell writes do not scan routes.
                            if installed_checkpoint || manifest.membership_revision() == manifest.position().index.get() {
                                if let Err(error) = self.transport.reconcile_managed_physical(&group) {
                                    if !matches!(error, TransportError::Overloaded | TransportError::UnknownGroup | TransportError::StalePeer | TransportError::Refused) {
                                        return Err(CellGroupControllerError::Transport(error));
                                    }
                                }
                            }
                            self.archives.applied(group, manifest.position().index);
                            self.effects.observe(group, manifest.position().index, manifest.root().next_due_unix_millis())?;
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
        self.shared.archive_seeds.stop();
        self.requests.close();
        while self.requests.try_recv().is_ok() {}
        // Attachment may already own a RawNode/claim or be inside catalog I/O.
        // Poll it through completion before closing its physical dependencies.
        while let Some(completed) = self.registrations.next().await {
            let _ = completed.finish(true);
        }
        self.effects.join(&self.shared).await;
        self.provisioning.join().await;
        self.checkpoints.join().await;
        self.late_archives.join(&self.shared).await;
        self.archives.finish(&self.shared).await;
        // A seed producer waits for its transport-held handoff lease before
        // retiring the journaled generation. Drive both joins together.
        futures_util::future::join(self.transport.close(), self.shared.archive_seeds.join()).await;
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
