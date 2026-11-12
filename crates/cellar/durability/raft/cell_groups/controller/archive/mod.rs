// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One bounded, fair archive lane on the existing node controller. Published
//! markers are observations, never S3 command settlement or serving authority.
mod job;
mod journal;
pub(super) mod late;
mod provision;
pub(in crate::raft::cell_groups::controller) mod seed;
pub(crate) use provision::Provisioning;
pub(crate) use seed::{Client as ArchiveSeedClient, Descriptor as ArchiveSeedDescriptor};
pub(in crate::raft::cell_groups::controller) mod settlement;

use super::calls::Delivery;
use super::*;
use crate::raft::cell_groups::archive_compaction::ArchiveActivation;
use crate::raft::cell_groups::{archive::ControllerExecution, *};
use cellar_logic::{cell_group::GroupHistory, consensus::LogIndex};
use futures_util::future::BoxFuture;
use std::collections::VecDeque;
use tokio::sync::oneshot;

#[derive(Debug)]
pub enum CellGroupArchiveControlError {
    Invalid(&'static str),
    Runtime(RuntimeError),
    Archive(CellGroupArchiveError),
    Binding(CellGroupArchiveBindingError),
    Journal(std::io::Error),
    Worker(crate::asyncrt::TaskPanic),
    Stopped,
}

impl std::fmt::Display for CellGroupArchiveControlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "group archive controller: {self:?}")
    }
}

impl std::error::Error for CellGroupArchiveControlError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Runtime(error) => Some(error),
            Self::Archive(error) => Some(error),
            Self::Binding(error) => Some(error),
            Self::Journal(error) => Some(error),
            Self::Worker(error) => Some(error),
            _ => None,
        }
    }
}

impl From<CellGroupArchiveError> for CellGroupArchiveControlError {
    fn from(error: CellGroupArchiveError) -> Self {
        Self::Archive(error)
    }
}

impl From<CellGroupArchiveBindingError> for CellGroupArchiveControlError {
    fn from(error: CellGroupArchiveBindingError) -> Self {
        Self::Binding(error)
    }
}

impl From<std::io::Error> for CellGroupArchiveControlError {
    fn from(error: std::io::Error) -> Self {
        Self::Journal(error)
    }
}

impl From<crate::asyncrt::DomainClosed> for CellGroupArchiveControlError {
    fn from(_: crate::asyncrt::DomainClosed) -> Self {
        Self::Stopped
    }
}

type Error = CellGroupArchiveControlError;
type Result<T> = std::result::Result<T, Error>;

/// Selects who produces new cuts; existing journal intents always reconcile.
/// This scheduling choice grants no serving or archive authority.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CellGroupArchivePublisher {
    /// Managed groups configure archiving on every leader-eligible voter.
    /// Followers independently acquire and verify published prefixes.
    GroupLeader {
        /// Minimum shared-timer interval for blocked/background archive work.
        /// A newly waiting S3 request may initiate one immediate attempt.
        retry_interval_ms: u64,
    },
    /// An explicitly registered archiver publishes regardless of Raft role.
    /// Use when only a selected replica owns archive publication.
    LocalReplica,
}

impl CellGroupArchivePublisher {
    fn validate(self) -> bool {
        match self {
            Self::GroupLeader { retry_interval_ms } => (10..=60_000).contains(&retry_interval_ms),
            Self::LocalReplica => true,
        }
    }

    fn retry_interval_ms(self, ordinary: u64) -> u64 {
        match self {
            Self::GroupLeader { retry_interval_ms } => ordinary.max(retry_interval_ms),
            Self::LocalReplica => ordinary,
        }
    }
}

/// Share all admission pools across this node's registrations. The binding must
/// already be initialized in the namespace catalog. The controller owns and
/// closes the private verifier store after joining archive work. Use fresh stores
/// on restart. No application callback is needed for background publication.
#[derive(Clone)]
pub struct CellGroupArchiveRegistration {
    pub publisher: CellGroupArchivePublisher,
    pub directory: RangeDirectory,
    pub birth: CellGroupBirth,
    pub io: CellGroupArchivePool,
    pub owners: CellGroupArchiveUploadPool,
    pub tails: CellGroupArchiveTailPool,
    pub checkpoints: CellGroupCheckpointPool,
    pub store: CellGroupArchiveStore,
    pub scratch: cellar_ltx::stream_snapshot::ScratchPool,
    pub scratch_directory: std::path::PathBuf,
}

#[derive(Clone, Default)]
pub struct CellGroupArchiveStatus {
    /// Last complete cut verified/published by this owner. Advisory historical
    /// metadata only; a consumer still needs a retained physical coverage proof.
    pub published: Option<GroupManifest>,
    pub attempts: u64,
    pub error: Option<Arc<CellGroupArchiveControlError>>,
    pub stopped: bool,
}

pub struct CellGroupArchiveCall {
    reply: oneshot::Receiver<Delivery<(), CellGroupArchiveControlError>>,
}

impl CellGroupArchiveCall {
    /// Caller cancellation does not cancel the admitted archive owner.
    pub async fn response(self) -> std::result::Result<(), Arc<CellGroupArchiveControlError>> {
        self.reply
            .await
            .map_err(|_| Arc::new(Error::Stopped))?
            .result
    }
}

pub(super) struct Request {
    registration: CellGroupArchiveRegistration,
    activation: Arc<ArchiveActivation>,
    reply: RegistrationReply,
}

struct RegistrationReply {
    sender: oneshot::Sender<Delivery<(), CellGroupArchiveControlError>>,
    permit: Arc<crate::raft::budget::Permit>,
}

impl RegistrationReply {
    fn send(self, result: std::result::Result<(), Arc<CellGroupArchiveControlError>>) {
        let _ = self.sender.send(Delivery {
            result,
            _permit: self.permit,
        });
    }
}

impl CellGroupController {
    /// Install background archival for an already attached group. Admission is
    /// node-bounded and survives response cancellation. This never enables S3
    /// acknowledgements or authorizes local log reclamation.
    pub fn archive(
        &self,
        registration: CellGroupArchiveRegistration,
    ) -> Result<CellGroupArchiveCall> {
        let shared = &self.handle.0;
        if !registration.publisher.validate() {
            return Err(Error::Invalid("archive publisher retry interval"));
        }
        if shared.closing.load(Ordering::Acquire) {
            return Err(Error::Stopped);
        }
        let group = registration.birth.route().history;
        if !shared.groups.contains(&group).map_err(Error::Runtime)? {
            return Err(Error::Runtime(RuntimeError::NotFound));
        }
        let local = shared
            .runtime
            .log()
            .cell_group(&group)
            .map_err(|error| Error::Runtime(error.into()))?;
        if local
            .group_manifest()
            .map_err(|error| Error::Runtime(error.into()))?
            .as_ref()
            .is_none_or(|marker| marker.route() != registration.birth.route())
        {
            return Err(Error::Invalid("archive registration local group birth"));
        }
        if registration.store.route() != registration.birth.route()
            || !registration.scratch_directory.is_absolute()
            || registration.scratch_directory.as_os_str().len() > 4096
        {
            return Err(Error::Invalid("archive registration binding/path"));
        }
        let permit = shared
            .commands
            .acquire(8192)
            .ok_or(Error::Runtime(RuntimeError::Overloaded))?;
        let activation = local
            .archive_compaction
            .begin_activation()
            .map_err(|error| Error::Runtime(error.into()))?;
        let (reply, receiver) = oneshot::channel();
        shared
            .requests
            .try_send(requests::Request::Archive(Box::new(Request {
                registration,
                activation,
                reply: RegistrationReply {
                    sender: reply,
                    permit: Arc::new(permit),
                },
            })))
            .map_err(|error| match error {
                tokio::sync::mpsc::error::TrySendError::Closed(_) => Error::Stopped,
                tokio::sync::mpsc::error::TrySendError::Full(_) => {
                    Error::Runtime(RuntimeError::Overloaded)
                }
            })?;
        Ok(CellGroupArchiveCall { reply: receiver })
    }

    pub fn archive_status(
        &self,
        group: GroupHistory,
    ) -> std::result::Result<Option<CellGroupArchiveStatus>, RuntimeError> {
        Ok(self
            .handle
            .0
            .archives
            .lock()
            .map_err(|_| RuntimeError::Closed)?
            .get(&group)
            .cloned())
    }
}

struct Entry {
    execution: ControllerExecution,
    registration: CellGroupArchiveRegistration,
    activation: Option<Arc<ArchiveActivation>>,
    context: Option<job::Context>,
    reply: Option<RegistrationReply>,
    queued: bool,
    latest_applied: Option<LogIndex>,
    dirty: bool,
    fatal: bool,
    stopping: bool,
    retry_at: u64,
    waiters: Vec<settlement::Request>,
}

pub(super) struct Completed {
    group: GroupHistory,
    context: Option<job::Context>,
    result: Result<GroupManifest>,
    maintenance_pending: bool,
    observation: std::result::Result<LogIndex, RuntimeError>,
    waiters: Vec<settlement::Request>,
    coverage: Option<CellGroupArchiveCoverage>,
}

#[derive(Default)]
pub(super) struct Manager {
    entries: BTreeMap<GroupHistory, Entry>,
    queue: VecDeque<GroupHistory>,
    job: Option<BoxFuture<'static, Completed>>,
    active_group: Option<GroupHistory>,
    // At most one just-completed verification can cross the controller turn
    // for requests that arrived while it was running. Never cache idle groups:
    // the next launch drops this pin before another group's verification or an
    // uncovered successor needs the same bounded physical replay admission.
    handoff: Option<(GroupHistory, CellGroupArchiveCoverage)>,
}

impl Manager {
    pub(super) fn contains(&self, group: &GroupHistory) -> bool {
        self.entries.contains_key(group)
    }

    pub fn request(&mut self, request: Request, shared: &Shared) {
        let group = request.registration.birth.route().history;
        match shared.groups.contains(&group) {
            Ok(true) => (),
            result => {
                request.reply.send(Err(Arc::new(Error::Runtime(
                    result.err().unwrap_or(RuntimeError::NotFound),
                ))));
                return;
            }
        }
        if self.entries.contains_key(&group) || self.entries.len() >= shared.limits.max_groups {
            request.reply.send(Err(Arc::new(Error::Invalid(
                "duplicate/full archive roster",
            ))));
            return;
        }
        shared
            .archives
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(group, CellGroupArchiveStatus::default());
        self.entries.insert(
            group,
            Entry {
                execution: ControllerExecution::new(shared.domain.clone()),
                registration: request.registration,
                activation: Some(request.activation),
                context: None,
                reply: Some(request.reply),
                queued: true,
                latest_applied: None,
                dirty: true,
                fatal: false,
                stopping: false,
                retry_at: 0,
                waiters: Vec::new(),
            },
        );
        self.queue.push_back(group);
    }

    pub fn provision(
        &mut self,
        registration: CellGroupArchiveRegistration,
        shared: &Shared,
    ) -> std::result::Result<(), CellGroupControllerError> {
        let group = registration.birth.route().history;
        if self.entries.contains_key(&group) || self.entries.len() >= shared.limits.max_groups {
            return Err(CellGroupControllerError::Invalid(
                "duplicate/full provisioned archive roster",
            ));
        }
        let local = shared
            .runtime
            .log()
            .cell_group(&group)
            .map_err(RuntimeError::from)?;
        let activation = local
            .archive_compaction
            .begin_activation()
            .map_err(RuntimeError::from)?;
        shared
            .archives
            .lock()
            .map_err(|_| CellGroupControllerError::Closed)?
            .insert(group, CellGroupArchiveStatus::default());
        // The physical group/preparation already holds bounded admission. This
        // internal handoff consumes no second ordinary command slot or waiter.
        self.entries.insert(
            group,
            Entry {
                execution: ControllerExecution::new(shared.domain.clone()),
                registration,
                activation: Some(activation),
                context: None,
                reply: None,
                queued: true,
                latest_applied: None,
                dirty: true,
                fatal: false,
                stopping: false,
                retry_at: 0,
                waiters: Vec::new(),
            },
        );
        self.queue.push_back(group);
        Ok(())
    }

    pub fn applied(&mut self, group: GroupHistory, index: LogIndex) {
        if let Some(entry) = self.entries.get_mut(&group) {
            entry.latest_applied = Some(
                entry
                    .latest_applied
                    .map_or(index, |previous| previous.max(index)),
            );
            if !entry.fatal {
                entry.dirty = true;
                if !entry.queued {
                    entry.queued = true;
                    self.queue.push_back(group);
                }
            }
        }
    }

    pub fn settle(&mut self, request: settlement::Request) {
        let group = request.route.history;
        let Some(entry) = self.entries.get_mut(&group) else {
            request.fail(Arc::new(Error::Runtime(RuntimeError::NotReady)));
            return;
        };
        if entry.fatal || entry.registration.birth.route() != &request.route {
            request.fail(Arc::new(Error::Invalid(
                "S3 archive registration unavailable/changed",
            )));
            return;
        }
        // One newly waiting foreground proof bypasses background coalescing.
        // Further waiters share that attempt and its bounded retry schedule.
        if entry.waiters.is_empty() {
            entry.retry_at = 0;
        }
        entry.waiters.push(request);
        entry.dirty = true;
        if !entry.queued {
            entry.queued = true;
            self.queue.push_back(group);
        }
    }

    pub fn running(&self) -> bool {
        self.job.is_some()
    }

    pub async fn next(&mut self) -> Completed {
        self.job.as_mut().expect("active archive job").await
    }

    pub fn launch(&mut self, runtime: &CellGroupRuntime, shared: &Shared, now: u64) {
        if self.running() {
            return;
        }
        let handoff = self.handoff.take();
        for _ in 0..self.queue.len() {
            let group = self.queue.pop_front().expect("archive roster queue");
            let entry = self.entries.get_mut(&group).expect("archive roster entry");
            entry.queued = false;
            if !entry.dirty || entry.fatal || entry.stopping {
                continue;
            }
            if entry.retry_at > now {
                entry.queued = true;
                self.queue.push_back(group);
                continue;
            }
            entry.dirty = false;
            if let Some(status) = shared
                .archives
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .get_mut(&group)
            {
                status.attempts = status.attempts.saturating_add(1);
            }
            let registration = entry.registration.clone();
            let activation = entry.activation.clone();
            let mut context = entry.context.clone();
            let execution = entry.execution.clone();
            let runtime = runtime.clone();
            // Bound receipt SQL work per fair group turn, independently of the
            // node's potentially larger total command admission.
            let count = entry.waiters.len().min(32);
            let mut waiters = entry.waiters.drain(..count).collect();
            let handoff = handoff
                .filter(|(selected, _)| *selected == group)
                .map(|(_, coverage)| coverage);
            tracing::debug!(target: "cellar::raft::group_archive", node = ?runtime.log().limits().node,
                group = ?group.group, stage = "launch", waiters = count);
            settlement::prune(&mut waiters, now);
            self.active_group = Some(group);
            self.job = Some(Box::pin(async move {
                let mut maintenance_pending = false;
                let mut completed_coverage = None;
                let result = std::panic::AssertUnwindSafe(async {
                    if context.is_none() {
                        context = Some(
                            job::open(
                                &registration,
                                &runtime,
                                &execution,
                                activation.ok_or(Error::Invalid("missing archive activation"))?,
                            )
                            .await?,
                        );
                    }
                    // `prove` indexed this live verification before completion.
                    // Retain it only through the exact reuse check, which still
                    // validates current birth/journal ownership and each receipt.
                    let retained = if !waiters.is_empty() {
                        job::retained(
                            context.as_ref().expect("opened archive context"),
                            &execution,
                        )
                        .await?
                    } else {
                        None
                    };
                    drop(handoff);
                    if let Some(retained) = retained {
                        confirm_coverage(&runtime, group, &retained.coverage)?;
                        tracing::debug!(target: "cellar::raft::group_archive", node = ?runtime.log().limits().node,
                            group = ?group.group, stage = "reuse", index = retained.coverage.marker().position().index.get(), waiters = waiters.len());
                        waiters = settlement::prove(
                            std::mem::take(&mut waiters),
                            context.as_ref().expect("opened archive context"),
                            &registration,
                            &retained.coverage,
                            &execution,
                        )
                        .await;
                        if waiters.is_empty() {
                            maintenance_pending = retained.maintenance_pending;
                            completed_coverage = Some(retained.coverage);
                            return Ok(retained.published);
                        }
                        // Release this temporary reuse before asking for a new
                        // verification. Only real output owners may continue
                        // occupying the original pool admission.
                    }
                    let driven = job::drive(
                        context.as_ref().expect("opened archive context"),
                        &registration,
                        &runtime,
                        &execution,
                        !waiters.is_empty(),
                    )
                    .await?;
                    maintenance_pending = driven.maintenance_pending;
                    let coverage = driven.coverage;
                    confirm_coverage(&runtime, group, &coverage)?;
                    tracing::debug!(target: "cellar::raft::group_archive", node = ?runtime.log().limits().node,
                        group = ?group.group, stage = "coverage", index = coverage.marker().position().index.get());
                    waiters = settlement::prove(
                        std::mem::take(&mut waiters),
                        context.as_ref().expect("opened archive context"),
                        &registration,
                        &coverage,
                        &execution,
                    )
                    .await;
                    tracing::debug!(target: "cellar::raft::group_archive", node = ?runtime.log().limits().node,
                        group = ?group.group, stage = "settled", remaining = waiters.len());
                    completed_coverage = Some(coverage);
                    Ok(driven.published)
                })
                .catch_unwind()
                .await
                .unwrap_or(Err(Error::Invalid("archive job panicked")));
                // Reconciliation may have recovered an older head while all
                // newer apply events were already consumed before this job.
                // Observe the owner after the attempt so an idle final tail
                // cannot disappear when the dirty flag was cleared at launch.
                let observation = runtime
                    .status(group)
                    .await
                    .map(|status| status.applied.index);
                Completed {
                    group,
                    context,
                    result,
                    maintenance_pending,
                    observation,
                    waiters,
                    coverage: completed_coverage,
                }
            }));
            break;
        }
    }

    pub fn complete(&mut self, completed: Completed, shared: &Shared, now: u64, closing: bool) {
        self.job = None;
        self.active_group = None;
        self.handoff = None;
        let entry = self
            .entries
            .get_mut(&completed.group)
            .expect("completed archive owner");
        entry.context = completed.context;
        let retry_interval = entry
            .registration
            .publisher
            .retry_interval_ms(shared.limits.retry_interval_ms);
        let unexamined_waiters = !entry.waiters.is_empty();
        entry.waiters.extend(completed.waiters);
        settlement::prune(&mut entry.waiters, now);
        let mut statuses = shared
            .archives
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let status = statuses.get_mut(&completed.group).expect("archive status");
        match completed.result {
            Ok(marker) => {
                let covered = marker.position().index;
                status.published = Some(marker);
                status.error = None;
                if completed.maintenance_pending {
                    // Cached output does not complete durable publication or
                    // retirement work. Keep its bounded retry cadence even
                    // when no further commands or apply events arrive.
                    entry.retry_at = now.saturating_add(retry_interval);
                }
                match completed.observation {
                    Ok(applied) => {
                        let latest = entry
                            .latest_applied
                            .map_or(applied, |seen| seen.max(applied));
                        entry.dirty = latest > covered || completed.maintenance_pending;
                    }
                    Err(error) => {
                        let error = Error::Runtime(error);
                        entry.fatal = !retryable(&error);
                        status.stopped = entry.fatal;
                        entry.dirty = true;
                        entry.retry_at = now.saturating_add(retry_interval);
                        status.error = Some(Arc::new(error));
                    }
                }
            }
            Err(error) => {
                entry.fatal = !retryable(&error);
                status.stopped = entry.fatal;
                entry.dirty = true;
                entry.retry_at = now.saturating_add(retry_interval);
                status.error = Some(Arc::new(error));
            }
        }
        if entry.context.is_some() || entry.fatal {
            entry.activation.take();
        }
        if entry.fatal {
            let error = status
                .error
                .clone()
                .unwrap_or_else(|| Arc::new(Error::Stopped));
            for request in entry.waiters.drain(..) {
                request.fail(error.clone());
            }
        } else if !entry.waiters.is_empty() {
            entry.dirty = true;
            if !closing && unexamined_waiters {
                self.handoff = completed
                    .coverage
                    .map(|coverage| (completed.group, coverage));
            }
        }
        if entry.dirty
            && !entry.fatal
            && entry.waiters.is_empty()
            && matches!(
                entry.registration.publisher,
                CellGroupArchivePublisher::GroupLeader { .. }
            )
        {
            // Coalesce background applied cuts and remote-head polling. This
            // uses the controller's shared timer, never a task/timer per group.
            entry.retry_at = entry.retry_at.max(now.saturating_add(retry_interval));
        }
        if entry.dirty && !entry.fatal && !entry.queued {
            entry.queued = true;
            self.queue.push_back(completed.group);
        }
        if closing || entry.context.is_some() || entry.fatal {
            if let Some(reply) = entry.reply.take() {
                let result = if closing {
                    Err(Arc::new(Error::Stopped))
                } else if entry.context.is_some() {
                    Ok(())
                } else {
                    Err(status.error.clone().expect("failed archive initialization"))
                };
                reply.send(result);
            }
        }
    }

    pub(super) fn retire(&mut self, group: GroupHistory, shared: &Shared) -> bool {
        if self
            .handoff
            .as_ref()
            .is_some_and(|(selected, _)| *selected == group)
        {
            self.handoff = None;
        }
        if let Some(entry) = self.entries.get_mut(&group) {
            entry.stopping = true;
        }
        if self.active_group == Some(group)
            || self
                .entries
                .get(&group)
                .is_some_and(|entry| !entry.execution.is_idle())
        {
            return false;
        }
        self.queue.retain(|queued| *queued != group);
        if let Some(mut entry) = self.entries.remove(&group) {
            for request in entry.waiters.drain(..) {
                request.fail(Arc::new(Error::Stopped));
            }
            if let Some(reply) = entry.reply.take() {
                reply.send(Err(Arc::new(Error::Stopped)));
            }
            entry.registration.store.close();
        }
        if let Some(status) = shared
            .archives
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_mut(&group)
        {
            status.stopped = true;
        }
        true
    }

    pub async fn finish(&mut self, shared: &Shared) {
        self.handoff = None;
        if let Some(job) = self.job.take() {
            let completed = job.await;
            self.complete(completed, shared, 0, true);
        }
        for entry in self.entries.values_mut() {
            entry.execution.join().await;
            for request in entry.waiters.drain(..) {
                request.fail(Arc::new(Error::Stopped));
            }
            entry.registration.store.close();
            if let Some(reply) = entry.reply.take() {
                reply.send(Err(Arc::new(Error::Stopped)));
            }
        }
        self.entries.clear();
        for status in shared
            .archives
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .values_mut()
        {
            status.stopped = true;
        }
    }
}

fn confirm_coverage(
    runtime: &CellGroupRuntime,
    group: GroupHistory,
    coverage: &CellGroupArchiveCoverage,
) -> Result<()> {
    let local = runtime
        .log()
        .cell_group(&group)
        .map_err(|error| Error::Runtime(error.into()))?;
    local
        .archive_compaction
        .confirm(&local, coverage)
        .map_err(|error| Error::Runtime(error.into()))
}

fn archive_retryable(error: &CellGroupArchiveError) -> bool {
    matches!(
        error,
        CellGroupArchiveError::Busy
            | CellGroupArchiveError::Timeout
            | CellGroupArchiveError::Conflict
            | CellGroupArchiveError::Expired
            | CellGroupArchiveError::Unconfirmed
            | CellGroupArchiveError::Storage(_)
            | CellGroupArchiveError::Checkpoint(
                CellGroupCheckpointError::NotReady
                    | CellGroupCheckpointError::Busy
                    | CellGroupCheckpointError::Authority(
                        RuntimeError::NotReady | RuntimeError::Overloaded
                    )
            )
    )
}

fn retryable(error: &Error) -> bool {
    match error {
        Error::Archive(error) | Error::Binding(CellGroupArchiveBindingError::Archive(error)) => {
            archive_retryable(error)
        }
        Error::Binding(CellGroupArchiveBindingError::Catalog(error)) => matches!(
            error,
            crate::raft::catalog::CatalogError::Storage(_)
                | crate::raft::catalog::CatalogError::Unconfirmed
        ),
        Error::Runtime(RuntimeError::Overloaded | RuntimeError::NotReady) => true,
        _ => false,
    }
}
