//! A fixed node-wide blocking pool owns SQLite and image directories. Domain
//! cancellation joins a running callback before these owners release locks.

use super::apply::{marker, ImageApplier, PublishedImage};
use super::archive::{ArchiveLimits, CommittedArchiveCut};
use super::budget::{Budget, Permit};
use super::cache::{CommittedImage, GroupLog};
use super::completion::{
    ApplyCompletion, ApplyTicket, GroupStatus, RuntimeEvent, RuntimeEventKind,
};
use super::execution::*;
use super::snapshot::{PreparedSnapshot, SnapshotCheckpoint, SnapshotLimits, SnapshotPackage};
use super::transport::snapshots::{ReceivedSnapshot, SourceReservation};
use crate::application::consensus::client::{AttemptControl, Work};
use crate::application::consensus::{OperationContract, RaftClass};
use cellar_logic::consensus::{CommandFailure, CommandIdentity, GroupScope};
use cellar_ltx::generation::{GenerationLimits, GenerationStore, ImageId, ReadServices};
use cellar_ltx::LtxHost;
use futures_util::future::BoxFuture;
use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    mpsc, Arc,
};
use std::time::Duration;
use tokio::sync::{oneshot, Semaphore};

#[cfg(test)]
#[path = "host_sql_tests.rs"]
mod tests;

#[derive(Clone)]
pub(crate) struct CellStorage {
    pub directory: PathBuf,
    pub host: LtxHost,
    pub services: ReadServices,
    pub images: GenerationLimits,
    pub execution: ExecutionLimits,
}

#[derive(Clone)]
pub(crate) struct ImageView {
    pub position: cellar_logic::consensus::LogPosition,
    pub image: CommittedImage,
}

impl ImageView {
    fn of(image: &PublishedImage) -> Self {
        Self {
            position: image.position(),
            image: marker(&image.pin()),
        }
    }
}

pub(crate) enum Action {
    SealArchiveSeed {
        received: Box<super::transport::seeds::ReceivedArchiveSeed>,
        storage: CellStorage,
    },
    SealSeed {
        received: Box<super::transport::snapshots::ReceivedSeed>,
        storage: CellStorage,
    },
    CaptureSeed {
        limits: SnapshotLimits,
        reservation: SourceReservation,
    },
    Initialize {
        group: GroupLog,
        class: Arc<RaftClass>,
        storage: CellStorage,
    },
    Stage {
        identity: CommandIdentity,
        status: Box<GroupStatus>,
        attempt: CommandAttempt,
        input: Vec<u8>,
        allow_new: bool,
        system: bool,
    },
    Submit {
        identity: CommandIdentity,
    },
    Discard {
        identity: CommandIdentity,
    },
    Retire {
        identity: CommandIdentity,
        barrier: cellar_logic::consensus::LogPosition,
    },
    Prepare {
        event: RuntimeEvent,
        image: ImageId,
    },
    Publish {
        ticket: ApplyTicket,
    },
    CaptureArchive {
        limits: ArchiveLimits,
    },
    ScanEffects {
        now: u64,
        prefer_delivery: bool,
        reservation: Permit,
    },
    CaptureCheckpoint {
        limits: SnapshotLimits,
        reservation: SourceReservation,
    },
    CaptureSnapshot {
        snapshot: Box<::raft::prelude::Snapshot>,
        limits: SnapshotLimits,
        reservation: SourceReservation,
    },
    PrepareSnapshot {
        ticket: ApplyTicket,
        received: Box<ReceivedSnapshot>,
    },
    PublishSnapshot {
        ticket: ApplyTicket,
    },
    Query {
        contract: OperationContract,
        input: Vec<u8>,
        minimum: super::query::Minimum,
        control: Arc<AttemptControl>,
        source_deadline: Option<u64>,
    },
}

pub(crate) enum Value {
    Seeded(Box<super::snapshot::PreparedSeed>),
    Initialized(Option<ImageView>),
    Staged(StageOutcome),
    Submitted,
    Discarded,
    Retired(cellar_logic::consensus::LogPosition),
    Prepared(ApplyCompletion),
    ArchiveCut(Option<Box<CommittedArchiveCut>>),
    EffectScan(Option<Box<super::effects::scan::EffectScan>>),
    Checkpoint(Box<SnapshotCheckpoint>),
    SnapshotSource {
        package: Box<SnapshotPackage>,
        reservation: SourceReservation,
    },
    SnapshotPrepared(Box<super::completion::SnapshotCompletion>),
    SnapshotPublished(Option<ImageView>),
    Published {
        view: ImageView,
        receipt: Option<(CommandIdentity, CommittedReceipt)>,
    },
    Queried(RegisteredQueryOutput),
}

impl Value {
    fn bytes(&self) -> usize {
        match self {
            Self::Staged(StageOutcome::Prepared(p)) => p.bytes().len(),
            Self::Staged(StageOutcome::Duplicate(r)) => r.receipt().result().len(),
            Self::Queried(q) => q.result.len(),
            Self::ArchiveCut(Some(cut)) => cut.memory_bytes(),
            Self::Published {
                receipt: Some((_, r)),
                ..
            } => r.receipt().result().len(),
            _ => 0,
        }
    }
}

pub(crate) struct Output {
    pub value: Value,
    pub _bytes: Option<Permit>,
}

struct Request {
    scope: GroupScope,
    action: Action,
    reply: oneshot::Sender<Result<Output, ExecutionError>>,
    _work: Option<Arc<Work>>,
    cancel: Option<Arc<AttemptControl>>,
    _slot: tokio::sync::OwnedSemaphorePermit,
}

struct RequestQueue {
    receiver: mpsc::Receiver<Request>,
    pending: VecDeque<Request>,
    capacity: usize,
    progress_streak: usize,
}

impl RequestQueue {
    fn new(receiver: mpsc::Receiver<Request>, capacity: usize) -> Self {
        Self {
            receiver,
            pending: VecDeque::new(),
            capacity,
            progress_streak: 0,
        }
    }

    fn next(&mut self) -> Result<Request, mpsc::RecvTimeoutError> {
        if self.pending.is_empty() {
            self.pending
                .push_back(self.receiver.recv_timeout(Duration::from_millis(10))?);
        }
        // All requests, including this local batch and the executing callback,
        // retain the same worker semaphore permits. Draining does not enlarge
        // admission, and never waits to assemble a batch.
        while self.pending.len() < self.capacity {
            match self.receiver.try_recv() {
                Ok(request) => self.pending.push_back(request),
                Err(_) => break,
            }
        }
        // Short progress work frees committed commands or archive retention.
        // Never pass earlier work for the same cell. After three selections,
        // serve the oldest request so ongoing progress cannot starve callbacks.
        let index = if self.progress_streak < 3 {
            self.pending
                .iter()
                .enumerate()
                .find(|(index, request)| {
                    request.action.progress_priority()
                        && !self
                            .pending
                            .iter()
                            .take(*index)
                            .any(|earlier| earlier.scope == request.scope)
                })
                .map_or(0, |(index, _)| index)
        } else {
            0
        };
        let request = self
            .pending
            .remove(index)
            .ok_or(mpsc::RecvTimeoutError::Disconnected)?;
        self.progress_streak = if request.action.progress_priority() {
            self.progress_streak.saturating_add(1).min(3)
        } else {
            0
        };
        Ok(request)
    }
}

impl Action {
    fn progress_priority(&self) -> bool {
        matches!(
            self,
            Self::Submit { .. }
                | Self::Discard { .. }
                | Self::Retire { .. }
                | Self::Publish { .. }
                | Self::PublishSnapshot { .. }
                | Self::CaptureArchive { .. }
        )
    }
}

pub(crate) struct Pool {
    domain: crate::asyncrt::DomainToken,
    senders: Vec<mpsc::SyncSender<Request>>,
    slots: Vec<Arc<Semaphore>>,
    workers: Vec<crate::asyncrt::TaskHandle<Result<(), ExecutionError>>>,
    stopped: Arc<AtomicBool>,
    running: Arc<AtomicUsize>,
    per_worker_capacity: usize,
}

/// Retains only semaphore/counter metadata, never a worker, image or connection.
pub(crate) struct ResourceObserver {
    slots: Vec<Arc<Semaphore>>,
    stopped: Arc<AtomicBool>,
    running: Arc<AtomicUsize>,
    per_worker_capacity: usize,
}

impl ResourceObserver {
    pub(crate) fn capture(&self) -> Option<crate::application::consensus::RaftSqlUsage> {
        let work_capacity = self.slots.len().checked_mul(self.per_worker_capacity)?;
        let work_reservations = self.slots.iter().try_fold(0usize, |sum, slots| {
            sum.checked_add(
                self.per_worker_capacity
                    .checked_sub(slots.available_permits())?,
            )
        })?;
        let running_workers = self.running.load(Ordering::Acquire);
        if running_workers > self.slots.len() {
            return None;
        }
        Some(crate::application::consensus::RaftSqlUsage {
            running_workers,
            worker_capacity: self.slots.len(),
            work_reservations,
            work_capacity,
            queue_closed: self.stopped.load(Ordering::Acquire),
        })
    }
}

struct Running(Arc<AtomicUsize>);

impl Running {
    fn enter(running: Arc<AtomicUsize>) -> Self {
        running.fetch_add(1, Ordering::AcqRel);
        Self(running)
    }
}

impl Drop for Running {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

pub(crate) struct Budgets {
    pub business: Arc<Budget>,
    pub archive_capture: Arc<Budget>,
    pub query_pins: Arc<Budget>,
}

/// A bounded port into one existing SQL owner. It owns no worker and cannot
/// outlive pool shutdown admission; dynamic initialization uses this same port.
#[derive(Clone)]
pub(crate) struct Worker {
    domain: crate::asyncrt::DomainToken,
    sender: mpsc::SyncSender<Request>,
    slots: Arc<Semaphore>,
    stopped: Arc<AtomicBool>,
}

impl Pool {
    pub fn start(
        domain: crate::asyncrt::DomainToken,
        count: usize,
        queue: usize,
        max_cells: usize,
        budgets: Budgets,
    ) -> Result<Self, ExecutionError> {
        let stopped = Arc::new(AtomicBool::new(false));
        let running = Arc::new(AtomicUsize::new(0));
        let mut pool = Self {
            domain: domain.clone(),
            senders: vec![],
            slots: vec![],
            workers: vec![],
            stopped: stopped.clone(),
            running: running.clone(),
            per_worker_capacity: queue,
        };
        for _ in 0..count {
            let (sender, receiver) = mpsc::sync_channel::<Request>(queue);
            let owner = domain.clone();
            let stopped = stopped.clone();
            let budget = budgets.business.clone();
            let archive_budget = budgets.archive_capture.clone();
            let query_pins = budgets.query_pins.clone();
            let running = running.clone();
            let task = domain
                .blocking(move || {
                    // This guard drops after cells/connections finish cleanup,
                    // including errors and callback unwinding. A waiter cannot
                    // retire this physical worker count.
                    let _running = Running::enter(running);
                    let mut cells = BTreeMap::<GroupScope, Cell>::new();
                    let mut requests = RequestQueue::new(receiver, queue);
                    let result = loop {
                        if stopped.load(Ordering::Acquire) || owner.mono_ms().is_err() {
                            break Ok(());
                        }
                        let request = match requests.next() {
                            Ok(request) => request,
                            Err(mpsc::RecvTimeoutError::Timeout) => continue,
                            Err(mpsc::RecvTimeoutError::Disconnected) => break Ok(()),
                        };
                        let Request {
                            scope,
                            action,
                            reply,
                            _work,
                            cancel,
                            _slot,
                        } = request;
                        if stopped.load(Ordering::Acquire) || owner.mono_ms().is_err() {
                            break Ok(());
                        }
                        if cancel
                            .as_ref()
                            .is_some_and(|control| control.cancelled() && !control.submitted())
                        {
                            let _ = reply
                                .send(Err(ExecutionError::Refused(CommandFailure::Unavailable)));
                            continue;
                        }
                        // Reserve the complete capture before opening the image
                        // or allocating its dependency closure. The returned cut
                        // retains this reservation through archive publication.
                        let mut capture = if let Action::CaptureArchive { limits } = &action {
                            match archive_budget.acquire(limits.max_capture_bytes) {
                                Some(permit) => Some(permit),
                                None => {
                                    let _ = reply.send(Err(ExecutionError::Refused(
                                        CommandFailure::Overloaded,
                                    )));
                                    continue;
                                }
                            }
                        } else {
                            None
                        };
                        let result =
                            Self::execute(&mut cells, max_cells, &scope, action, &query_pins)
                                .and_then(|value| {
                                    let bytes = value.bytes();
                                    let budget = if matches!(&value, Value::ArchiveCut(_)) {
                                        &archive_budget
                                    } else {
                                        &budget
                                    };
                                    let permit = if capture.is_some()
                                        && matches!(&value, Value::ArchiveCut(_))
                                    {
                                        capture.take()
                                    } else if bytes == 0 {
                                        None
                                    } else {
                                        match budget.acquire(bytes) {
                                            Some(permit) => Some(permit),
                                            None => {
                                                if matches!(
                                                    &value,
                                                    Value::Staged(StageOutcome::Prepared(_))
                                                ) {
                                                    if let Some(cell) = cells.get_mut(&scope) {
                                                        cell.discard(None)?;
                                                    }
                                                }
                                                return Err(ExecutionError::Refused(
                                                    CommandFailure::Overloaded,
                                                ));
                                            }
                                        }
                                    };
                                    Ok(Output {
                                        value,
                                        _bytes: permit,
                                    })
                                });
                        let _ = reply.send(result);
                    };
                    // Keep joining/closing every resource even when one reports an
                    // error. Their locks are released on this blocking owner.
                    let mut cleanup = Ok(());
                    for (_, mut cell) in cells {
                        if let Some(private) = cell.private.take() {
                            if let Err(error) = private.close() {
                                cleanup = Err(error);
                            }
                        }
                    }
                    result.and(cleanup)
                })
                .map_err(|_| ExecutionError::Refused(CommandFailure::Unavailable))?;
            pool.senders.push(sender);
            pool.slots.push(Arc::new(Semaphore::new(queue)));
            pool.workers.push(task);
        }
        Ok(pool)
    }

    pub(crate) fn resource_observer(&self) -> ResourceObserver {
        ResourceObserver {
            slots: self.slots.clone(),
            stopped: self.stopped.clone(),
            running: self.running.clone(),
            per_worker_capacity: self.per_worker_capacity,
        }
    }

    pub fn call(
        &self,
        worker: usize,
        scope: GroupScope,
        action: Action,
        work: Option<Arc<Work>>,
        cancel: Option<Arc<AttemptControl>>,
    ) -> BoxFuture<'static, Result<Output, ExecutionError>> {
        match self.worker(worker) {
            Ok(worker) => worker.call(scope, action, work, cancel),
            Err(error) => Box::pin(async { Err(error) }),
        }
    }

    pub fn worker(&self, worker: usize) -> Result<Worker, ExecutionError> {
        let sender = self
            .senders
            .get(worker)
            .cloned()
            .ok_or(ExecutionError::Invalid("SQL worker"))?;
        Ok(Worker {
            domain: self.domain.clone(),
            sender,
            slots: self.slots[worker].clone(),
            stopped: self.stopped.clone(),
        })
    }

    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
        for slots in &self.slots {
            slots.close();
        }
    }

    pub async fn join(&mut self) -> Result<(), ExecutionError> {
        self.stop();
        let mut result = Ok(());
        for worker in self.workers.drain(..) {
            match worker.await {
                Ok(Ok(())) => (),
                Ok(Err(error)) => result = Err(error),
                Err(error) => result = Err(ExecutionError::Storage(anyhow::Error::new(error))),
            }
        }
        result
    }

    fn execute(
        cells: &mut BTreeMap<GroupScope, Cell>,
        max: usize,
        scope: &GroupScope,
        action: Action,
        query_pins: &Arc<Budget>,
    ) -> Result<Value, ExecutionError> {
        if let Action::SealArchiveSeed { received, storage } = action {
            if cells.contains_key(scope) || cells.len() >= max || received.scope() != scope {
                return Err(ExecutionError::Invalid("seed cell owner"));
            }
            let images = GenerationStore::open(
                storage.directory.join("images"),
                storage.host.filesystem(),
                storage.images,
            )
            .map_err(super::execution::storage)?;
            // Caller verified an absent stable group and absent permanent claim.
            // Orphan files from a pre-fence attempt confer no authority.
            images.retain_only(&[]).map_err(super::execution::storage)?;
            images
                .reclaim(storage.images.max_generations)
                .map_err(super::execution::storage)?;
            let seed = received
                .finish(&images)
                .map_err(super::execution::storage)?;
            return Ok(Value::Seeded(Box::new(seed)));
        }
        if let Action::SealSeed { received, storage } = action {
            if cells.contains_key(scope) || cells.len() >= max || received.scope() != scope {
                return Err(ExecutionError::Invalid("seed cell owner"));
            }
            let images = GenerationStore::open(
                storage.directory.join("images"),
                storage.host.filesystem(),
                storage.images,
            )
            .map_err(super::execution::storage)?;
            // Caller verified an absent stable group and absent permanent claim.
            // Orphan files from a pre-fence attempt confer no authority.
            images.retain_only(&[]).map_err(super::execution::storage)?;
            images
                .reclaim(storage.images.max_generations)
                .map_err(super::execution::storage)?;
            let seed = received
                .finish(&images)
                .map_err(super::execution::storage)?;
            return Ok(Value::Seeded(Box::new(seed)));
        }
        if let Action::Initialize {
            group,
            class,
            storage,
        } = action
        {
            if cells.contains_key(scope) || cells.len() >= max || group.identity().scope != *scope {
                return Err(ExecutionError::Invalid("cell initialization"));
            }
            let images = GenerationStore::open(
                storage.directory.join("images"),
                storage.host.filesystem(),
                storage.images,
            )
            .map_err(super::execution::storage)?;
            let applier = ImageApplier::recover(
                group.clone(),
                images.clone(),
                class.schema_digest(),
                storage.execution.envelope,
            )
            .map_err(super::execution::storage)?;
            let view = applier.current().as_ref().map(ImageView::of);
            let options = PrivateOptions {
                directory: storage.directory.join("private"),
                host: storage.host,
                sqlite_vfs: None,
                images,
                query_services: storage.services,
                application_digest: class.schema_digest(),
                limits: storage.execution,
            };
            cells.insert(
                scope.clone(),
                Cell {
                    group,
                    class,
                    options,
                    applier,
                    snapshot: None,
                    private: None,
                    active: None,
                },
            );
            return Ok(Value::Initialized(view));
        }
        cells
            .get_mut(scope)
            .ok_or(ExecutionError::Invalid("unregistered cell"))?
            .run(action, query_pins)
    }
}

impl Worker {
    pub fn call(
        &self,
        scope: GroupScope,
        action: Action,
        work: Option<Arc<Work>>,
        cancel: Option<Arc<AttemptControl>>,
    ) -> BoxFuture<'static, Result<Output, ExecutionError>> {
        let Worker {
            domain,
            sender,
            slots,
            stopped,
        } = self.clone();
        Box::pin(async move {
            let slot = slots
                .acquire_owned()
                .await
                .map_err(|_| ExecutionError::Refused(CommandFailure::Unavailable))?;
            if stopped.load(Ordering::Acquire) || domain.mono_ms().is_err() {
                return Err(ExecutionError::Refused(CommandFailure::Unavailable));
            }
            let (reply, result) = oneshot::channel();
            sender
                .try_send(Request {
                    scope,
                    action,
                    reply,
                    _work: work,
                    cancel,
                    _slot: slot,
                })
                .map_err(|_| ExecutionError::Refused(CommandFailure::Unavailable))?;
            result
                .await
                .map_err(|_| ExecutionError::Refused(CommandFailure::Unavailable))?
        })
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        self.stop();
    }
}

struct Cell {
    group: GroupLog,
    class: Arc<RaftClass>,
    options: PrivateOptions,
    applier: ImageApplier,
    snapshot: Option<(ApplyTicket, PreparedSnapshot, Permit)>,
    private: Option<PrivateExecutor>,
    active: Option<CommandIdentity>,
}

impl Cell {
    fn discard(&mut self, identity: Option<&CommandIdentity>) -> Result<(), ExecutionError> {
        if self
            .active
            .as_ref()
            .zip(identity)
            .is_some_and(|(active, expected)| active != expected)
        {
            return Err(ExecutionError::Invalid("stale private discard"));
        }
        if let Some(private) = self.private.as_mut() {
            private.discard_unsubmitted()?;
        }
        if let Some(private) = self.private.take() {
            private.close()?;
        }
        self.active = None;
        Ok(())
    }

    fn run(&mut self, action: Action, query_pins: &Arc<Budget>) -> Result<Value, ExecutionError> {
        match action {
            Action::SealArchiveSeed { .. }
            | Action::SealSeed { .. }
            | Action::Initialize { .. } => Err(ExecutionError::Invalid("duplicate initialization")),
            Action::Stage {
                identity,
                status,
                attempt,
                input,
                allow_new,
                system,
            } => {
                if self.active.is_some()
                    || identity.authority.replica != *self.group.identity()
                    || attempt.proposal != identity.proposal
                {
                    return Err(ExecutionError::Invalid("private command identity"));
                }
                let authority = CommandAuthority::from_status(self.group.clone(), &status)?;
                match self.private.as_mut() {
                    Some(private) => {
                        if private.refresh_authority(authority.clone()).is_err() {
                            private.rebuild(self.applier.current(), authority)?;
                        }
                    }
                    None => {
                        self.private = Some(PrivateExecutor::open(
                            self.options.clone(),
                            authority,
                            self.applier.current(),
                        )?)
                    }
                }
                let executor = self
                    .private
                    .as_mut()
                    .ok_or(ExecutionError::RebuildRequired)?;
                let result = if system {
                    executor.stage_system_admitted(attempt, &self.class, &input, allow_new)
                } else {
                    executor.stage_registered_admitted(attempt, &self.class, &input, allow_new)
                };
                match result {
                    Ok(outcome) => {
                        if matches!(&outcome, StageOutcome::Prepared(_)) {
                            self.active = Some(identity);
                        }
                        Ok(Value::Staged(outcome))
                    }
                    Err(error) => {
                        self.discard(None)?;
                        Err(error)
                    }
                }
            }
            Action::Submit { identity } => {
                if self.active.as_ref() != Some(&identity) {
                    return Err(ExecutionError::Invalid("private submission identity"));
                }
                self.private
                    .as_mut()
                    .ok_or(ExecutionError::RebuildRequired)?
                    .mark_submitted(identity.proposal)?;
                Ok(Value::Submitted)
            }
            Action::Discard { identity } => {
                self.discard(Some(&identity))?;
                Ok(Value::Discarded)
            }
            Action::Retire { identity, barrier } => {
                if self
                    .active
                    .as_ref()
                    .is_some_and(|active| active != &identity)
                {
                    return Err(ExecutionError::Invalid("private retirement identity"));
                }
                if let Some(private) = self.private.as_mut() {
                    private.retire_after_barrier(self.applier.current().as_ref(), barrier)?;
                } else {
                    let cache = self
                        .group
                        .read()
                        .map_err(|e| super::execution::storage(anyhow::Error::new(e)))?;
                    if cache.applied.position != barrier
                        || cache.hard_state.term != barrier.term.get()
                        || barrier.term <= identity.authority.term
                    {
                        return Err(ExecutionError::Uncertain);
                    }
                }
                if let Some(private) = self.private.take() {
                    private.close()?;
                }
                self.active = None;
                Ok(Value::Retired(barrier))
            }
            Action::Prepare { event, image } => {
                let RuntimeEventKind::Apply { ticket, entry } = &event.kind else {
                    return Err(ExecutionError::Invalid("apply event"));
                };
                Ok(Value::Prepared(
                    self.applier
                        .prepare_private(ticket.clone(), entry, image, self.private.as_mut())
                        .map_err(|error| match error {
                            super::apply::ImageApplyError::Images(
                                cellar_ltx::generation::GenerationError::Limit,
                            ) => ExecutionError::Refused(CommandFailure::Overloaded),
                            error => super::execution::storage(error),
                        })?,
                ))
            }
            Action::Publish { ticket } => {
                let published = self
                    .applier
                    .publish(&ticket)
                    .map_err(super::execution::storage)?;
                let receipt = if let Some(identity) = &self.active {
                    if published.proposal() == Some(identity.proposal) {
                        match self
                            .private
                            .as_mut()
                            .ok_or(ExecutionError::RebuildRequired)?
                            .finish(published.clone())?
                        {
                            AppliedExecution::Command(receipt) => Some((identity.clone(), receipt)),
                            AppliedExecution::Pruned { .. } => {
                                return Err(ExecutionError::Invalid("unexpected pruning command"))
                            }
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };
                if receipt.is_some() {
                    self.active = None;
                }
                Ok(Value::Published {
                    view: ImageView::of(&published),
                    receipt,
                })
            }
            Action::CaptureArchive { limits } => {
                let cut = self
                    .applier
                    .queryable()
                    .map_err(super::execution::storage)?
                    .map(|image| {
                        let dependencies = super::effects::capture_dependencies(
                            &image,
                            self.class.blob_limits(),
                            self.options.query_services.clone(),
                            limits,
                        )
                        .map_err(super::execution::storage)?;
                        CommittedArchiveCut::capture(&self.group, image, dependencies, limits)
                            .map_err(super::execution::storage)
                    })
                    .transpose()?;
                Ok(Value::ArchiveCut(cut.map(Box::new)))
            }
            Action::ScanEffects {
                now,
                prefer_delivery,
                reservation,
            } => {
                let scan = self
                    .applier
                    .queryable()
                    .map_err(super::execution::storage)?
                    .map(|image| {
                        super::effects::scan::scan(
                            image,
                            &self.class,
                            self.options.query_services.clone(),
                            now,
                            prefer_delivery,
                            reservation,
                        )
                    })
                    .transpose()
                    .map_err(super::execution::storage)?;
                Ok(Value::EffectScan(scan.map(Box::new)))
            }
            Action::CaptureSeed {
                limits,
                reservation,
            } => {
                let package = match self
                    .applier
                    .queryable()
                    .map_err(super::execution::storage)?
                {
                    Some(image) => SnapshotPackage::capture(&self.group, image, limits),
                    None => SnapshotPackage::capture_empty(
                        &self.group,
                        self.class.schema_digest(),
                        limits,
                    ),
                }
                .map_err(super::execution::storage)?;
                Ok(Value::SnapshotSource {
                    package: Box::new(package),
                    reservation,
                })
            }
            Action::CaptureCheckpoint {
                limits,
                reservation,
            } => {
                let _reservation = reservation;
                let package = match self
                    .applier
                    .queryable()
                    .map_err(super::execution::storage)?
                {
                    Some(image) => SnapshotPackage::capture(&self.group, image, limits),
                    None => SnapshotPackage::capture_empty(
                        &self.group,
                        self.class.schema_digest(),
                        limits,
                    ),
                }
                .map_err(super::execution::storage)?;
                Ok(Value::Checkpoint(Box::new(package.checkpoint_material())))
            }
            Action::CaptureSnapshot {
                snapshot,
                limits,
                reservation,
            } => {
                let package = SnapshotPackage::retained(
                    &self.group,
                    &self.options.images,
                    self.class.schema_digest(),
                    &snapshot,
                    limits,
                )
                .map_err(super::execution::storage)?;
                Ok(Value::SnapshotSource {
                    package: Box::new(package),
                    reservation,
                })
            }
            Action::PrepareSnapshot { ticket, received } => {
                if self.snapshot.is_some() {
                    return Err(ExecutionError::Invalid("snapshot already prepared"));
                }
                let (prepared, reservation) =
                    received
                        .finish(&self.options.images)
                        .map_err(|error| match error {
                            super::snapshot::SnapshotError::Limit
                            | super::snapshot::SnapshotError::Images(
                                cellar_ltx::generation::GenerationError::Limit,
                            ) => ExecutionError::Refused(CommandFailure::Overloaded),
                            super::snapshot::SnapshotError::Invalid(_) => {
                                ExecutionError::Invalid("snapshot verification")
                            }
                            error => super::execution::storage(error),
                        })?;
                let completion = prepared
                    .completion(&ticket)
                    .map_err(super::execution::storage)?;
                self.snapshot = Some((ticket, prepared, reservation));
                Ok(Value::SnapshotPrepared(Box::new(completion)))
            }
            Action::PublishSnapshot { ticket } => {
                let Some((expected, prepared, _reservation)) = self.snapshot.take() else {
                    return Err(ExecutionError::Invalid("missing prepared snapshot"));
                };
                if expected != ticket {
                    return Err(ExecutionError::Invalid("snapshot publication ticket"));
                }
                let current = if self
                    .group
                    .applied()
                    .map_err(super::execution::storage)?
                    .image
                    .is_some()
                {
                    Some(ImageView::of(
                        &prepared
                            .publish(&mut self.applier)
                            .map_err(super::execution::storage)?,
                    ))
                } else {
                    prepared
                        .publish_empty(&mut self.applier)
                        .map_err(super::execution::storage)?;
                    None
                };
                Ok(Value::SnapshotPublished(current))
            }
            Action::Query {
                contract,
                input,
                minimum,
                control,
                source_deadline,
            } => {
                if control.cancelled()
                    || source_deadline.is_some_and(|deadline| !control.before(deadline))
                {
                    return Err(ExecutionError::Refused(CommandFailure::VersionUnavailable));
                }
                let published = self
                    .applier
                    .queryable()
                    .map_err(super::execution::storage)?
                    .ok_or(ExecutionError::Refused(CommandFailure::VersionUnavailable))?;
                let version = cellar_logic::consensus::CommittedVersion {
                    scope: published.scope().clone(),
                    position: published.position(),
                };
                if !minimum
                    .satisfied(&version, published.scope())
                    .map_err(ExecutionError::Refused)?
                {
                    return Err(ExecutionError::Refused(CommandFailure::VersionUnavailable));
                }
                // This stack belongs to the blocking SQL worker, not its async
                // waiter. The immutable database drops inside the call before
                // the reservation is released, including on error or unwind.
                let _query_pin = query_pins
                    .acquire(0)
                    .ok_or(ExecutionError::Refused(CommandFailure::Overloaded))?;
                Ok(Value::Queried(query_registered_controlled(
                    &published,
                    &self.class,
                    &contract,
                    &input,
                    self.options.query_services.clone(),
                    self.options.limits,
                    Some(control),
                )?))
            }
        }
    }
}
