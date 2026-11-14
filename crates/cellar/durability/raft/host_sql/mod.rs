// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A fixed node-wide blocking pool owns SQLite and image directories. Domain
//! cancellation joins a running callback before these owners release locks.

pub(crate) mod cell_groups;
use super::budget::{Budget, Permit};
use super::execution::ExecutionError;
use super::performance::Registry as Performance;
use cellar_logic::consensus::CommandFailure;
use std::collections::VecDeque;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    mpsc, Arc, OnceLock,
};
use std::time::Duration;
use tokio::sync::Semaphore;

trait Scheduled {
    fn same_cell(&self, other: &Self) -> bool;

    fn progress_priority(&self) -> bool;
}

enum Envelope {
    Shared(cell_groups::Request),
    GroupApply(Box<cell_groups::ApplyRequest>),
}

/// A readiness token wakes the existing physical owner without another queue
/// entry, semaphore reservation or per-cell task.
struct WorkerWake(OnceLock<std::thread::Thread>);

impl WorkerWake {
    fn wake(&self) {
        if let Some(thread) = self.0.get() {
            thread.unpark();
        }
    }
}

#[derive(Clone)]
struct Port {
    sender: mpsc::SyncSender<Envelope>,
    wake: Arc<WorkerWake>,
}

impl Port {
    fn try_send(&self, request: Envelope) -> Result<(), ExecutionError> {
        self.sender
            .try_send(request)
            .map_err(|_| ExecutionError::Refused(CommandFailure::Unavailable))?;
        self.wake.wake();
        Ok(())
    }
}

impl Scheduled for Envelope {
    fn same_cell(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Shared(a), Self::Shared(b)) => a.cell == b.cell,
            (Self::GroupApply(a), Self::GroupApply(b)) => a.group == b.group,
            _ => false,
        }
    }

    fn progress_priority(&self) -> bool {
        match self {
            Self::Shared(request) => request.progress_priority(),
            Self::GroupApply(_) => true,
        }
    }
}

struct RequestQueue<T> {
    receiver: mpsc::Receiver<T>,
    pending: VecDeque<T>,
    capacity: usize,
    progress_streak: usize,
}

impl<T: Scheduled> RequestQueue<T> {
    fn new(receiver: mpsc::Receiver<T>, capacity: usize) -> Self {
        Self {
            receiver,
            pending: VecDeque::new(),
            capacity,
            progress_streak: 0,
        }
    }

    fn next(&mut self, wait: Duration) -> Result<T, mpsc::RecvTimeoutError> {
        if self.pending.is_empty() {
            let request = match self.receiver.try_recv() {
                Ok(request) => request,
                Err(mpsc::TryRecvError::Disconnected) => {
                    return Err(mpsc::RecvTimeoutError::Disconnected)
                }
                Err(mpsc::TryRecvError::Empty) => {
                    // Both queue producers and callback wakers unpark this
                    // owner. A callback-only wake has no envelope: return to
                    // polling suspended work immediately. The park token
                    // also preserves a wake between try_recv and parking.
                    std::thread::park_timeout(wait);
                    self.receiver.try_recv().map_err(|error| match error {
                        mpsc::TryRecvError::Empty => mpsc::RecvTimeoutError::Timeout,
                        mpsc::TryRecvError::Disconnected => mpsc::RecvTimeoutError::Disconnected,
                    })?
                }
            };
            self.pending.push_back(request);
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
                    request.progress_priority()
                        && !self
                            .pending
                            .iter()
                            .take(*index)
                            .any(|earlier| earlier.same_cell(request))
                })
                .map_or(0, |(index, _)| index)
        } else {
            0
        };
        let request = self
            .pending
            .remove(index)
            .ok_or(mpsc::RecvTimeoutError::Disconnected)?;
        self.progress_streak = if request.progress_priority() {
            self.progress_streak.saturating_add(1).min(3)
        } else {
            0
        };
        Ok(request)
    }
}

pub(crate) struct Pool {
    activations: Arc<cell_groups::RpcCache>,
    performance: Arc<Performance>,
    domain: crate::asyncrt::DomainToken,
    senders: Vec<Port>,
    completion_slots: Vec<Arc<Semaphore>>,
    slots: Vec<Arc<Semaphore>>,
    workers: Vec<crate::asyncrt::TaskHandle<Result<(), ExecutionError>>>,
    stopped: Arc<AtomicBool>,
    running: Arc<AtomicUsize>,
    per_worker_capacity: usize,
}

/// Retains only semaphore/counter metadata, never a worker, image or connection.
pub(crate) struct ResourceObserver {
    activations: std::sync::Weak<cell_groups::RpcCache>,
    activation_capacity: usize,
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
        let (idle_activations, retained_activations, activation_capacity) =
            match self.activations.upgrade() {
                Some(cache) => cache.usage().ok()?,
                None if self.stopped.load(Ordering::Acquire) => (0, 0, self.activation_capacity),
                None => return None,
            };
        Some(crate::application::consensus::RaftSqlUsage {
            running_workers,
            worker_capacity: self.slots.len(),
            work_reservations,
            work_capacity,
            idle_activations,
            retained_activations,
            activation_capacity,
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

/// A bounded port into one existing SQL owner. It owns no worker and cannot
/// outlive pool shutdown admission; dynamic initialization uses this same port.
#[derive(Clone)]
pub(crate) struct Worker {
    performance: Arc<Performance>,
    domain: crate::asyncrt::DomainToken,
    sender: Port,
    completion_slots: Arc<Semaphore>,
    slots: Arc<Semaphore>,
}

impl Pool {
    pub fn start(
        domain: crate::asyncrt::DomainToken,
        count: usize,
        queue: usize,
        max_cells: usize,
        performance: Arc<Performance>,
    ) -> Result<Self, ExecutionError> {
        let stopped = Arc::new(AtomicBool::new(false));
        let running = Arc::new(AtomicUsize::new(0));
        let mut pool = Self {
            activations: cell_groups::RpcCache::new(max_cells),
            performance: performance.clone(),
            domain: domain.clone(),
            senders: vec![],
            completion_slots: vec![],
            slots: vec![],
            workers: vec![],
            stopped: stopped.clone(),
            running: running.clone(),
            per_worker_capacity: queue,
        };
        for _ in 0..count {
            let capacity = queue
                .checked_add(1)
                .ok_or(ExecutionError::Invalid("SQL queue capacity"))?;
            let (sender, receiver) = mpsc::sync_channel::<Envelope>(capacity);
            let owner = domain.clone();
            let performance = performance.clone();
            let stopped = stopped.clone();
            let running = running.clone();
            let activations = pool.activations.clone();
            let wake = Arc::new(WorkerWake(OnceLock::new()));
            let worker_wake = wake.clone();
            let task = domain
                .blocking(move || {
                    // This guard drops after cells/connections finish cleanup,
                    // including errors and callback unwinding. A waiter cannot
                    // retire this physical worker count.
                    let _running = Running::enter(running);
                    worker_wake.0.set(std::thread::current()).map_err(|_| {
                        ExecutionError::Invalid("SQL worker wake already installed")
                    })?;
                    let mut shared = cell_groups::Cells::new(activations, worker_wake);
                    let mut requests = RequestQueue::new(receiver, capacity);
                    loop {
                        if stopped.load(Ordering::Acquire) || owner.mono_ms().is_err() {
                            break;
                        }
                        let wait = if shared.poll_suspended(&owner, &stopped, &performance) {
                            // Give queued work its turn before trying the next
                            // ready continuation without the idle tick delay.
                            Duration::ZERO
                        } else {
                            Duration::from_millis(10)
                        };
                        let request = match requests.next(wait) {
                            Ok(request) => request,
                            Err(mpsc::RecvTimeoutError::Timeout) => continue,
                            Err(mpsc::RecvTimeoutError::Disconnected) => break,
                        };
                        match request {
                            Envelope::GroupApply(request) => {
                                request.execute(&owner, &stopped, &performance);
                            }
                            Envelope::Shared(request) => {
                                shared.execute(request, max_cells, &owner, &stopped, &performance);
                            }
                        }
                    }
                    shared.close()
                })
                .map_err(|_| ExecutionError::Refused(CommandFailure::Unavailable))?;
            pool.senders.push(Port { sender, wake });
            pool.slots.push(Arc::new(Semaphore::new(queue)));
            pool.completion_slots.push(Arc::new(Semaphore::new(1)));
            pool.workers.push(task);
        }
        Ok(pool)
    }

    pub(crate) fn resource_observer(&self) -> ResourceObserver {
        ResourceObserver {
            activations: Arc::downgrade(&self.activations),
            activation_capacity: self.activations.capacity(),
            slots: self.slots.clone(),
            stopped: self.stopped.clone(),
            running: self.running.clone(),
            per_worker_capacity: self.per_worker_capacity,
        }
    }

    pub fn worker(&self, worker: usize) -> Result<Worker, ExecutionError> {
        let sender = self
            .senders
            .get(worker)
            .cloned()
            .ok_or(ExecutionError::Invalid("SQL worker"))?;
        Ok(Worker {
            performance: self.performance.clone(),
            domain: self.domain.clone(),
            sender,
            completion_slots: self.completion_slots[worker].clone(),
            slots: self.slots[worker].clone(),
        })
    }

    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
        for slots in self.slots.iter().chain(&self.completion_slots) {
            slots.close();
        }
        for port in &self.senders {
            port.wake.wake();
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
}

impl Drop for Pool {
    fn drop(&mut self) {
        self.stop();
    }
}
