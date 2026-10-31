// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One bounded, fair archive lane on the existing node controller. Published
//! markers are observations, never S3 command settlement or serving authority.
mod job;
mod journal;

use super::calls::Delivery;
use super::*;
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

type Error = CellGroupArchiveControlError;
type Result<T> = std::result::Result<T, Error>;

/// Share all admission pools across this node's registrations. The binding must
/// already be initialized in the namespace catalog. The controller owns and
/// closes the private verifier store after joining archive work. Use fresh stores
/// on restart. No application callback is needed for background publication.
#[derive(Clone)]
pub struct CellGroupArchiveRegistration {
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
        if shared.closing.load(Ordering::Acquire) {
            return Err(Error::Stopped);
        }
        let group = registration.birth.route().history;
        if !shared.groups.contains(&group).map_err(Error::Runtime)? {
            return Err(Error::Runtime(RuntimeError::NotFound));
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
        let (reply, receiver) = oneshot::channel();
        shared
            .requests
            .try_send(requests::Request::Archive(Box::new(Request {
                registration,
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
    registration: CellGroupArchiveRegistration,
    context: Option<job::Context>,
    reply: Option<RegistrationReply>,
    queued: bool,
    latest_applied: Option<LogIndex>,
    dirty: bool,
    fatal: bool,
    retry_at: u64,
}

pub(super) struct Completed {
    group: GroupHistory,
    context: Option<job::Context>,
    result: Result<GroupManifest>,
    observation: std::result::Result<LogIndex, RuntimeError>,
}

pub(super) struct Manager {
    entries: BTreeMap<GroupHistory, Entry>,
    queue: VecDeque<GroupHistory>,
    job: Option<BoxFuture<'static, Completed>>,
    execution: ControllerExecution,
}

impl Manager {
    pub fn new(domain: crate::asyncrt::DomainToken) -> Self {
        Self {
            entries: BTreeMap::new(),
            queue: VecDeque::new(),
            job: None,
            execution: ControllerExecution::new(domain),
        }
    }

    pub fn request(&mut self, request: Request, shared: &Shared) {
        let group = request.registration.birth.route().history;
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
                registration: request.registration,
                context: None,
                reply: Some(request.reply),
                queued: true,
                latest_applied: None,
                dirty: true,
                fatal: false,
                retry_at: 0,
            },
        );
        self.queue.push_back(group);
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
        for _ in 0..self.queue.len() {
            let group = self.queue.pop_front().expect("archive roster queue");
            let entry = self.entries.get_mut(&group).expect("archive roster entry");
            entry.queued = false;
            if !entry.dirty || entry.fatal {
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
            let mut context = entry.context.clone();
            let execution = self.execution.clone();
            let runtime = runtime.clone();
            self.job = Some(Box::pin(async move {
                let result = std::panic::AssertUnwindSafe(async {
                    if context.is_none() {
                        context = Some(job::open(&registration, &runtime, &execution).await?);
                    }
                    job::drive(
                        context.as_ref().expect("opened archive context"),
                        &registration,
                        &runtime,
                        &execution,
                    )
                    .await
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
                    observation,
                }
            }));
            break;
        }
    }

    pub fn complete(&mut self, completed: Completed, shared: &Shared, now: u64, closing: bool) {
        self.job = None;
        let entry = self
            .entries
            .get_mut(&completed.group)
            .expect("completed archive owner");
        entry.context = completed.context;
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
                match completed.observation {
                    Ok(applied) => {
                        let latest = entry
                            .latest_applied
                            .map_or(applied, |seen| seen.max(applied));
                        entry.dirty = latest > covered;
                    }
                    Err(error) => {
                        let error = Error::Runtime(error);
                        entry.fatal = !retryable(&error);
                        status.stopped = entry.fatal;
                        entry.dirty = true;
                        entry.retry_at = now.saturating_add(shared.limits.retry_interval_ms);
                        status.error = Some(Arc::new(error));
                    }
                }
            }
            Err(error) => {
                entry.fatal = !retryable(&error);
                status.stopped = entry.fatal;
                entry.dirty = true;
                entry.retry_at = now.saturating_add(shared.limits.retry_interval_ms);
                status.error = Some(Arc::new(error));
            }
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

    pub async fn finish(&mut self, shared: &Shared) {
        if let Some(job) = self.job.take() {
            let completed = job.await;
            self.complete(completed, shared, 0, true);
        }
        self.execution.join().await;
        for entry in self.entries.values_mut() {
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
