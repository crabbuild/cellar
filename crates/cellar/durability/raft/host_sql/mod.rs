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
    mpsc, Arc,
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

    fn next(&mut self) -> Result<T, mpsc::RecvTimeoutError> {
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
    performance: Arc<Performance>,
    domain: crate::asyncrt::DomainToken,
    senders: Vec<mpsc::SyncSender<Envelope>>,
    completion_slots: Vec<Arc<Semaphore>>,
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

/// A bounded port into one existing SQL owner. It owns no worker and cannot
/// outlive pool shutdown admission; dynamic initialization uses this same port.
#[derive(Clone)]
pub(crate) struct Worker {
    performance: Arc<Performance>,
    domain: crate::asyncrt::DomainToken,
    sender: mpsc::SyncSender<Envelope>,
    completion_slots: Arc<Semaphore>,
    slots: Arc<Semaphore>,
    stopped: Arc<AtomicBool>,
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
            let task = domain
                .blocking(move || {
                    // This guard drops after cells/connections finish cleanup,
                    // including errors and callback unwinding. A waiter cannot
                    // retire this physical worker count.
                    let _running = Running::enter(running);
                    let mut shared = cell_groups::Cells::default();
                    let mut requests = RequestQueue::new(receiver, capacity);
                    loop {
                        if stopped.load(Ordering::Acquire) || owner.mono_ms().is_err() {
                            break;
                        }
                        let request = match requests.next() {
                            Ok(request) => request,
                            Err(mpsc::RecvTimeoutError::Timeout) => continue,
                            Err(mpsc::RecvTimeoutError::Disconnected) => break,
                        };
                        match request {
                            Envelope::GroupApply(request) => {
                                request.execute(&owner, &stopped);
                            }
                            Envelope::Shared(request) => {
                                shared.execute(request, max_cells, &owner, &stopped, &performance);
                            }
                        }
                    }
                    shared.close()
                })
                .map_err(|_| ExecutionError::Refused(CommandFailure::Unavailable))?;
            pool.senders.push(sender);
            pool.slots.push(Arc::new(Semaphore::new(queue)));
            pool.completion_slots.push(Arc::new(Semaphore::new(1)));
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
            stopped: self.stopped.clone(),
        })
    }

    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
        for slots in self.slots.iter().chain(&self.completion_slots) {
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
}

impl Drop for Pool {
    fn drop(&mut self) {
        self.stop();
    }
}
