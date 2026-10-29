// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Modified for Cellar.

// Placement and deadlines remain host decisions across worker executors.
#![allow(clippy::disallowed_methods)]

//! Worker placement and serialized turns around `cellar_logic::isolate`.
//!
//! A shared host runtime owns sockets and timers. A slot's async permit grants
//! one worker backend exclusive access for a synchronous turn; suspended work
//! retains an affiliation while releasing that permit. Compiled Rust workers
//! install their original SQLite and invocation ownership on each turn. The
//! transitional JS backend additionally enters its isolate through a locker.
//!
//! This module owns workers and counters. Placement, growth, shedding and
//! retirement remain decisions of `cellar_logic::isolate`. The stable isolate
//! and heap identities retain their existing operator and policy meanings.

use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;

use anyhow::Result;
use cellar_logic::isolate::HeapId;
use cellar_logic::isolate::IsolateId;
use cellar_logic::isolate::IsolateLoad;
use cellar_logic::isolate::Placement;
use cellar_logic::isolate::PoolLimits;
use cellar_logic::isolate::PoolLoad;
use cellar_logic::isolate::Refusal;

use crate::worker::Worker;

#[cfg(test)]
#[path = "turn_scheduler_tests.rs"]
mod turn_scheduler_tests;

pub use crate::engine_api::AdmitError;
pub use crate::engine_api::PoolCensus;

/// Heap identity is process-wide because pressure policy compares cells from
/// every script pool and every draining generation. A counter inside `Pool`
/// would reproduce the collision this identity prevents. The core treats the
/// value as opaque, so its allocation order is not a deterministic decision.
static NEXT_HEAP_ID: AtomicU64 = AtomicU64::new(1);

fn next_heap_id() -> HeapId {
    let value = NEXT_HEAP_ID
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            value.checked_add(1)
        })
        .expect("worker heap identity space exhausted");
    HeapId::new(value)
}

/// Bound the producer replies that can wait for one Queue's shared commit and
/// durability proof. Four 64-call groups can overlap, and a stopped bucket
/// still cannot retain an unbounded number of promises or copied bodies.
pub(crate) const MAX_QUEUE_PRODUCER_EVENTS_PER_CELL: usize = 256;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum TurnLane {
    Stateless,
    Cell(String),
}

#[derive(Default)]
struct TurnSchedule {
    current: Option<TurnLane>,
    ready: std::collections::VecDeque<TurnLane>,
    waiting: std::collections::HashMap<
        TurnLane,
        std::collections::VecDeque<tokio::sync::oneshot::Sender<()>>,
    >,
}

#[derive(Default)]
struct TurnScheduler {
    schedule: Mutex<TurnSchedule>,
}

struct TurnPermit<'a>(&'a TurnScheduler);

struct PendingTurn<'a> {
    scheduler: &'a TurnScheduler,
    receive: tokio::sync::oneshot::Receiver<()>,
}

impl Drop for PendingTurn<'_> {
    fn drop(&mut self) {
        // Close before inspecting the grant: otherwise a racing release can
        // send successfully between try_recv and the receiver's destruction.
        self.receive.close();
        if self.receive.try_recv().is_ok() {
            // The lane was granted, but acquire was cancelled before consuming
            // the notification. No TurnPermit exists to release it yet.
            self.scheduler.release();
        }
    }
}

impl TurnScheduler {
    async fn acquire(&self, lane: TurnLane) -> TurnPermit<'_> {
        let receive = {
            let mut schedule = self.schedule.lock().expect("turn scheduler poisoned");
            if schedule.current.is_none() {
                schedule.current = Some(lane);
                None
            } else {
                let (send, receive) = tokio::sync::oneshot::channel();
                if !schedule.waiting.contains_key(&lane) && schedule.current.as_ref() != Some(&lane)
                {
                    schedule.ready.push_back(lane.clone());
                }
                schedule.waiting.entry(lane).or_default().push_back(send);
                Some(receive)
            }
        };
        if let Some(receive) = receive {
            let mut pending = PendingTurn {
                scheduler: self,
                receive,
            };
            (&mut pending.receive)
                .await
                .expect("a queued turn retains its sender until it is granted");
            // Receiving consumes the notification, so PendingTurn no longer
            // owns a buffered grant. Ownership passes to the returned permit.
        }
        TurnPermit(self)
    }

    fn release(&self) {
        loop {
            let send = {
                let mut schedule = self.schedule.lock().expect("turn scheduler poisoned");
                let current = schedule
                    .current
                    .take()
                    .expect("a released turn scheduler has a current lane");
                if schedule
                    .waiting
                    .get(&current)
                    .is_some_and(|waiters| !waiters.is_empty())
                {
                    schedule.ready.push_back(current);
                }
                let Some(lane) = schedule.ready.pop_front() else {
                    return;
                };
                let waiters = schedule
                    .waiting
                    .get_mut(&lane)
                    .expect("a ready turn lane has waiters");
                let send = waiters.pop_front().unwrap();
                if waiters.is_empty() {
                    schedule.waiting.remove(&lane);
                }
                schedule.current = Some(lane);
                send
            };
            // A wake can synchronously cancel the receiver and release its
            // grant. Publish ownership first, then notify outside the lock.
            if send.send(()).is_ok() {
                return;
            }
            // The receiver closed before the grant. Release that lane in the
            // next iteration, preserving queued same-lane turns and fairness.
        }
    }
}

impl Drop for TurnPermit<'_> {
    fn drop(&mut self) {
        self.0.release();
    }
}

pub(crate) struct QueueProducerPermit {
    slot: Arc<Slot>,
    scope: String,
}

impl Drop for QueueProducerPermit {
    fn drop(&mut self) {
        self.slot.release_queue_producer(&self.scope);
    }
}

/// One isolate, and everything the decision core needs to know about it.
pub struct Slot {
    /// The index used only inside this pool's placement snapshot and slot
    /// vector. Distinct pools can use the same value.
    pub id: IsolateId,
    /// This slot, for an op of one of its turns to reach it: a facet of the
    /// script's own class runs in the isolate of the object that holds it.
    me: std::sync::Weak<Slot>,
    /// The identity reported to node-wide Actor and pressure policy.
    heap_id: HeapId,
    /// The async gate serializes entry into the worker it guards.
    ///
    /// `None` once the isolate has been retired and freed. Retirement only
    /// frees a slot that is drained — no turns, no affiliations — and a
    /// retiring slot is filtered out of every placement decision, so nothing
    /// can arrive afterwards. Entering one would be a bug in this module.
    worker: tokio::sync::Mutex<Option<Worker>>,
    application_work: Arc<crate::host_services::ApplicationWork>,
    handler_budget: std::time::Duration,
    turn_scheduler: TurnScheduler,
    queue_producers: Mutex<std::collections::HashMap<String, usize>>,
    turns: AtomicUsize,
    requests: AtomicUsize,
    worker_pins: AtomicUsize,
    /// The pool's admission bell, cloned into every slot so an
    /// `Affiliation` drop can ring it without holding the pool.
    freed: Arc<tokio::sync::Notify>,
    /// The pool's reclaim bell, rung when a retiring isolate may have
    /// drained. `None` for a standalone slot: nothing frees or replaces it,
    /// so [`Slot::condemn`] cannot apply to it.
    reclaim: Option<Arc<tokio::sync::Notify>>,
    /// The slot holds a Worker Loader's Worker, so the facets it runs belong
    /// to the root that started them rather than to this isolate.
    loaded_worker: bool,
    /// Cells whose realm lives in this isolate. Not a count of live work: a
    /// cell stays until it is evicted or handed to another node, which is
    /// why `retire` refuses an isolate holding any.
    cells: AtomicUsize,
    // Reservations can adopt out of order; only completed adoptions count
    // toward the collection boundary. Residency drop retires both counts.
    adopted_cells: AtomicUsize,
    retiring: AtomicBool,
    #[cfg(cellar_internal_tests)]
    turn_observations: Mutex<Vec<String>>,
}

thread_local! {
    static CURRENT_SLOT: std::cell::RefCell<Option<std::sync::Weak<Slot>>> =
        const { std::cell::RefCell::new(None) };
}

/// The slot whose turn this thread runs, for an op that must reach its own
/// isolate again after the turn.
pub(crate) fn current_slot() -> Option<Arc<Slot>> {
    CURRENT_SLOT.with_borrow(|slot| slot.as_ref().and_then(std::sync::Weak::upgrade))
}

struct CurrentSlotGuard {
    previous: Option<std::sync::Weak<Slot>>,
    _thread: std::marker::PhantomData<std::rc::Rc<()>>,
}

impl CurrentSlotGuard {
    fn enter(slot: std::sync::Weak<Slot>) -> Self {
        Self {
            previous: CURRENT_SLOT.replace(Some(slot)),
            _thread: std::marker::PhantomData,
        }
    }
}

impl Drop for CurrentSlotGuard {
    fn drop(&mut self) {
        CURRENT_SLOT.set(self.previous.take());
    }
}

impl Slot {
    pub(crate) fn handler_budget(&self) -> std::time::Duration {
        self.handler_budget
    }

    fn observe(&self) -> IsolateLoad {
        IsolateLoad {
            turns: self.turns.load(Ordering::Relaxed),
            requests: self.requests.load(Ordering::Relaxed),
            cells: self.cells.load(Ordering::Relaxed),
            retiring: self.retiring.load(Ordering::Relaxed),
        }
    }

    /// Enter the isolate for one turn and run `f` inside it.
    ///
    /// Only the gate is awaited; `f` is synchronous by type, so holding the
    /// isolate across an await is unwritable rather than a rule a comment
    /// asks callers to remember.
    pub async fn turn<T>(&self, f: impl FnOnce(&mut Worker) -> T) -> T {
        self.turn_in(TurnLane::Stateless, f).await
    }

    /// Enter one cell turn through a round-robin lane for that cell. A hot
    /// cell can retain its next place, but it cannot take the next place from
    /// another cell that already waits on the same isolate.
    pub async fn turn_cell<T>(&self, scope: &str, f: impl FnOnce(&mut Worker) -> T) -> T {
        self.turn_in(TurnLane::Cell(scope.to_string()), f).await
    }

    async fn turn_in<T>(&self, lane: TurnLane, f: impl FnOnce(&mut Worker) -> T) -> T {
        struct Counted<'a>(&'a Slot);

        impl Drop for Counted<'_> {
            fn drop(&mut self) {
                self.0.turns.fetch_sub(1, Ordering::Relaxed);
            }
        }
        self.turns.fetch_add(1, Ordering::Relaxed);
        let _turn = Counted(self);
        let _scheduled = self.turn_scheduler.acquire(lane.clone()).await;
        let mut worker = self.worker.lock().await;
        let worker = match worker.as_mut() {
            Some(worker) => worker,
            None => panic!("entered isolate {} after it was freed", self.id),
        };
        #[cfg(cellar_internal_tests)]
        self.turn_observations.lock().unwrap().push(match lane {
            TurnLane::Stateless => "stateless".to_string(),
            TurnLane::Cell(scope) => scope,
        });
        let turn = || {
            let _installed = CurrentSlotGuard::enter(self.me.clone());
            f(worker)
        };
        // A native turn can synchronously open, write or sync SQLite. Holding
        // its permit is required; holding the host's async worker is not.
        #[cfg(not(cellar_internal_tests))]
        {
            crate::asyncrt::blocking_region(turn)
        }
        #[cfg(cellar_internal_tests)]
        {
            turn()
        }
    }

    /// Reserve one event through its complete lifetime, including the reply
    /// gate. Releasing at the gate would let stalled storage retain unbounded
    /// promises and copied bodies. Cell turn fairness is enforced separately.
    pub(crate) fn queue_producer(self: &Arc<Self>, scope: &str) -> Option<QueueProducerPermit> {
        let mut producers = self
            .queue_producers
            .lock()
            .expect("Queue producer admission poisoned");
        let outstanding = producers.entry(scope.to_string()).or_default();
        if *outstanding >= MAX_QUEUE_PRODUCER_EVENTS_PER_CELL {
            return None;
        }
        *outstanding += 1;
        Some(QueueProducerPermit {
            slot: self.clone(),
            scope: scope.to_string(),
        })
    }

    fn release_queue_producer(&self, scope: &str) {
        let mut producers = self
            .queue_producers
            .lock()
            .expect("Queue producer admission poisoned");
        let outstanding = producers
            .get_mut(scope)
            .expect("a Queue producer permit has an admission");
        *outstanding -= 1;
        if *outstanding == 0 {
            producers.remove(scope);
        }
    }

    /// Mark a request as living in this isolate. Held for the request's whole
    /// life, including while it is suspended, because its promise stays in
    /// this heap and every later turn must come back here.
    pub fn affiliate(self: &Arc<Self>) -> Affiliation {
        let work = self.application_work.enter();
        self.requests.fetch_add(1, Ordering::Relaxed);
        Affiliation {
            slot: self.clone(),
            _work: work,
        }
    }

    /// A slot holding a Worker Loader's Worker.
    pub(crate) fn loaded_worker(worker: Worker) -> Arc<Self> {
        Self::outside_pool(worker, true)
    }

    /// A slot holding a Worker outside a placement pool.
    ///
    /// A dynamic Worker owns exactly one isolate, so it needs no
    /// admission, growth, or retirement policy.
    fn outside_pool(worker: Worker, loaded_worker: bool) -> Arc<Self> {
        Arc::new_cyclic(|me| Slot {
            me: me.clone(),
            id: 0,
            heap_id: next_heap_id(),
            worker: tokio::sync::Mutex::new(Some(worker)),
            application_work: crate::asyncrt::services().application_work_registry(),
            handler_budget: crate::engine_api::handler_budget(),
            turn_scheduler: TurnScheduler::default(),
            queue_producers: Mutex::new(std::collections::HashMap::new()),
            turns: AtomicUsize::new(0),
            requests: AtomicUsize::new(0),
            worker_pins: AtomicUsize::new(0),
            freed: Arc::new(tokio::sync::Notify::new()),
            reclaim: None,
            loaded_worker,
            cells: AtomicUsize::new(0),
            adopted_cells: AtomicUsize::new(0),
            retiring: AtomicBool::new(false),
            #[cfg(cellar_internal_tests)]
            turn_observations: Mutex::new(Vec::new()),
        })
    }

    /// Whether this slot holds a Worker Loader's Worker.
    pub(crate) fn is_loaded_worker(&self) -> bool {
        self.loaded_worker
    }

    #[cfg(cellar_internal_tests)]
    pub fn for_test(worker: Worker) -> Arc<Self> {
        Self::outside_pool(worker, false)
    }

    #[cfg(all(test, cellar_internal_tests))]
    pub(crate) fn vacant_for_request_cancellation_test() -> Arc<Self> {
        Arc::new_cyclic(|me| Slot {
            me: me.clone(),
            id: 0,
            heap_id: HeapId::new(0),
            worker: tokio::sync::Mutex::new(None),
            handler_budget: crate::engine_api::handler_budget(),
            turn_scheduler: TurnScheduler::default(),
            queue_producers: Mutex::new(std::collections::HashMap::new()),
            turns: AtomicUsize::new(0),
            requests: AtomicUsize::new(0),
            worker_pins: AtomicUsize::new(0),
            freed: Arc::new(tokio::sync::Notify::new()),
            reclaim: None,
            loaded_worker: false,
            cells: AtomicUsize::new(0),
            adopted_cells: AtomicUsize::new(0),
            retiring: AtomicBool::new(false),
            turn_observations: Mutex::new(Vec::new()),
        })
    }

    #[cfg(cellar_internal_tests)]
    #[doc(hidden)]
    pub async fn hold_turn_for_test(&self) -> impl Drop + '_ {
        self.worker.lock().await
    }

    #[cfg(cellar_internal_tests)]
    #[doc(hidden)]
    pub fn queued_turns_for_test(&self) -> usize {
        self.turns.load(Ordering::Relaxed)
    }

    #[cfg(cellar_internal_tests)]
    #[doc(hidden)]
    pub fn reset_turn_observations_for_test(&self) {
        self.turn_observations.lock().unwrap().clear();
    }

    #[cfg(cellar_internal_tests)]
    #[doc(hidden)]
    pub fn turn_observations_for_test(&self) -> Vec<String> {
        self.turn_observations.lock().unwrap().clone()
    }

    /// Give this isolate a cell's realm. Held for as long as the cell lives
    /// here, which is what stops the isolate being retired underneath it.
    fn house(self: &Arc<Self>, max_cells: usize) -> Residency {
        let cells = self.cells.fetch_add(1, Ordering::Relaxed) + 1;
        debug_assert!(cells <= max_cells);
        Residency {
            slot: self.clone(),
            max_cells,
            adopted: false,
        }
    }

    pub fn is_retiring(&self) -> bool {
        self.retiring.load(Ordering::Relaxed)
    }

    /// Retire this isolate because its guest can no longer run in it, and
    /// ask the pool to free it once it drains. Returns whether the slot
    /// belongs to a pool that can do both.
    ///
    /// Placement already skips a retiring slot, so the next request grows a
    /// fresh isolate instead. Leaving the dead isolate to placement is the
    /// failure this prevents: it fails each request in about a millisecond,
    /// so it carries the fewest turns and `place` hands it nearly every
    /// request. [`reap`](Self::reap) does not know it is dead, and on a
    /// four-isolate node it retired the three healthy isolates first, one
    /// per 30 s pass, so one fatal Python error failed that node's traffic
    /// for 94 s.
    ///
    /// The bell, not the 30 s reap, frees the heap: a guest that fails on
    /// every start would otherwise hold one dead heap per failure until the
    /// next tick, and a Python heap is tens of megabytes.
    ///
    /// A cell pool has no task on the bell. A condemned cell isolate still
    /// takes no new cell, and frees when its cells leave, as retirement
    /// always has; Python Workers cannot declare cells.
    #[allow(dead_code)]
    pub(crate) fn condemn(&self, reason: &str) -> bool {
        let Some(reclaim) = &self.reclaim else {
            return false;
        };
        if !self.retiring.swap(true, Ordering::Relaxed) {
            tracing::warn!(isolate = %self.heap_id, slot = self.id, reason, "isolate condemned");
        }
        reclaim.notify_one();
        true
    }

    /// The node-wide identity of this slot's currently installed worker heap.
    pub fn heap_id(&self) -> HeapId {
        self.heap_id
    }

    /// Retain the executor through a foreign root's facet lifetime without
    /// consuming request admission. Acquired while a turn keeps it installed.
    pub(crate) fn retain_worker(self: &Arc<Self>) -> WorkerPin {
        self.worker_pins.fetch_add(1, Ordering::AcqRel);
        WorkerPin(self.clone())
    }

    fn is_reusable(&self) -> bool {
        if self.worker_pins.load(Ordering::Acquire) != 0 {
            return false;
        }
        let load = self.observe();
        if !cellar_logic::isolate::may_free(&load) || load.cells != 0 {
            return false;
        }
        self.worker.try_lock().is_ok_and(|worker| worker.is_none())
    }
}

/// A host lifetime holding a worker installed, distinct from a request.
pub(crate) struct WorkerPin(Arc<Slot>);

impl Drop for WorkerPin {
    fn drop(&mut self) {
        self.0.worker_pins.fetch_sub(1, Ordering::AcqRel);
        if self.0.is_retiring() {
            if let Some(reclaim) = &self.0.reclaim {
                reclaim.notify_one();
            }
        }
    }
}

/// A request's claim on the isolate that ran its first turn.
pub struct Affiliation {
    slot: Arc<Slot>,
    _work: crate::host_services::ApplicationWorkGuard,
}

impl Affiliation {
    pub fn slot(&self) -> &Arc<Slot> {
        &self.slot
    }
}

impl Drop for Affiliation {
    fn drop(&mut self) {
        self.slot.requests.fetch_sub(1, Ordering::Relaxed);
        // Exactly one admission waiter per freed slot: `notify_one` hands
        // the capacity to a single parked request, where waking them all
        // would stampede `admit` on every release.
        self.slot.freed.notify_one();
        // The last request out of a retiring isolate is what lets it free.
        // Freeing here would drop an application slot inside whatever turn dropped
        // this affiliation, so the pool's maintenance task does it.
        if self.slot.is_retiring() {
            if let Some(reclaim) = &self.slot.reclaim {
                reclaim.notify_one();
            }
        }
    }
}

/// A cell's claim on the isolate that holds its realm.
///
/// The counterpart of [`Affiliation`], and the difference between them is
/// the whole reason both exist: an affiliation lasts one request, a
/// residency lasts until the cell is evicted or moves node.
pub struct Residency {
    slot: Arc<Slot>,
    max_cells: usize,
    adopted: bool,
}

impl Residency {
    pub fn slot(&self) -> &Arc<Slot> {
        &self.slot
    }

    /// Open a reserved cell's storage and return its residency with its alarm snapshot.
    /// Collection follows the last completed adoption, because reservation
    /// order can differ from the order in which activation futures resume.
    pub(crate) async fn adopt(
        mut self,
        cell: &str,
        storage: crate::host_turn::CellStorage<'_>,
    ) -> Result<(Self, cellar_logic::wake::AlarmSnapshot)> {
        anyhow::ensure!(!self.adopted, "cell residency already adopted");
        let slot = self.slot.clone();
        let alarm = slot
            .turn(|worker| -> Result<cellar_logic::wake::AlarmSnapshot> {
                let alarm = worker.own_cell(cell, Some(storage))?;
                // There is no await between adoption and accounting. Cancellation
                // before this turn drops only the reservation; afterward the
                // residency owns both counters, including on an error unwind.
                self.adopted = true;
                let adopted = slot.adopted_cells.fetch_add(1, Ordering::Relaxed) + 1;
                debug_assert!(adopted <= self.max_cells);
                if adopted == self.max_cells {
                    worker.settle_heap();
                }
                Ok(alarm)
            })
            .await?;
        Ok((self, alarm))
    }
}

impl Drop for Residency {
    fn drop(&mut self) {
        if self.adopted {
            self.slot.adopted_cells.fetch_sub(1, Ordering::Relaxed);
        }
        self.slot.cells.fetch_sub(1, Ordering::Relaxed);
    }
}

type Build = Box<dyn Fn() -> Result<Worker> + Send + Sync>;

pub struct Pool {
    slots: RwLock<Vec<Arc<Slot>>>,
    application_work: Arc<crate::host_services::ApplicationWork>,
    limits: PoolLimits,
    build: Build,
    handler_budget: std::time::Duration,
    freed: Arc<tokio::sync::Notify>,
    /// Rung by a condemnation and by the last request out of a retiring
    /// isolate. See [`Pool::reclaim_bell`].
    reclaim: Arc<tokio::sync::Notify>,
    admission_wait: std::time::Duration,
    /// The whole pool belongs to a superseded generation. An isolate grown
    /// after this is set starts out retiring: a placement that snapshotted
    /// the old generation just before the flip can still land, but nothing
    /// it builds outlives the work it was built for.
    retired: AtomicBool,
    /// Keeps admission and cell placement from racing isolate retirement.
    /// Stateless admission takes a shared guard, while maintenance and cell
    /// placement take the exclusive guard.
    maintenance: RwLock<()>,
}

impl Pool {
    pub fn new(limits: PoolLimits, admission_wait: std::time::Duration, build: Build) -> Self {
        Pool {
            slots: RwLock::new(Vec::new()),
            application_work: crate::asyncrt::services().application_work_registry(),
            limits,
            build,
            handler_budget: crate::engine_api::handler_budget(),
            freed: Arc::new(tokio::sync::Notify::new()),
            reclaim: Arc::new(tokio::sync::Notify::new()),
            admission_wait,
            retired: AtomicBool::new(false),
            maintenance: RwLock::new(()),
        }
    }

    fn load(&self) -> PoolLoad {
        let slots = self.slots.read().expect("pool poisoned");
        PoolLoad {
            isolates: slots.iter().map(|s| s.observe()).collect(),
            limits: self.limits,
        }
    }

    pub fn len(&self) -> usize {
        self.slots.read().expect("pool poisoned").len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn get(&self, id: IsolateId) -> Arc<Slot> {
        self.slots.read().expect("pool poisoned")[id].clone()
    }

    /// Build an isolate and admit it to the pool.
    ///
    /// The isolate is constructed before the lock is taken: loading a script
    /// runs JS and can take milliseconds, and holding the pool lock across it
    /// would stall every other placement on the node.
    /// Build an isolate, unless a capped pool reached `cap` while this one
    /// was being built. Cell placement passes no cap because node memory is
    /// bounded before placement and its per-isolate limit must remain exact.
    ///
    /// `cap` is re-checked under the write lock because admission decides
    /// against a snapshot it does not hold. Without the re-check every
    /// request that saw the same pre-growth snapshot grows, so a cap of two
    /// produced sixteen isolates under load — the breach scaled with offered
    /// concurrency, and the configured limit was met only when serialised.
    fn grow(&self, cap: Option<usize>) -> Result<Arc<Slot>> {
        let worker = (self.build)()?;
        let mut slots = self.slots.write().expect("pool poisoned");
        let live = slots.iter().filter(|slot| !slot.is_retiring()).count();
        if cap.is_some_and(|cap| live >= cap) {
            if let Some(slot) = slots.iter().find(|slot| !slot.is_retiring()) {
                return Ok(slot.clone());
            }
        }
        // A freed slot has no worker, work, affiliations, or cells. Reuse its
        // stable index so placement scans scale with the current pool rather
        // than with every isolate created during the process lifetime.
        let reusable = slots.iter().position(|slot| slot.is_reusable());
        let id = reusable.unwrap_or(slots.len());
        let heap_id = next_heap_id();
        let slot = Arc::new_cyclic(|me| Slot {
            me: me.clone(),
            id,
            heap_id,
            worker: tokio::sync::Mutex::new(Some(worker)),
            application_work: self.application_work.clone(),
            handler_budget: self.handler_budget,
            turn_scheduler: TurnScheduler::default(),
            queue_producers: Mutex::new(std::collections::HashMap::new()),
            turns: AtomicUsize::new(0),
            requests: AtomicUsize::new(0),
            worker_pins: AtomicUsize::new(0),
            freed: self.freed.clone(),
            reclaim: Some(self.reclaim.clone()),
            loaded_worker: false,
            cells: AtomicUsize::new(0),
            adopted_cells: AtomicUsize::new(0),
            retiring: AtomicBool::new(self.retired.load(Ordering::Relaxed)),
            #[cfg(cellar_internal_tests)]
            turn_observations: Mutex::new(Vec::new()),
        });
        if let Some(id) = reusable {
            slots[id] = slot.clone();
        } else {
            slots.push(slot.clone());
        }
        tracing::info!(isolate = %heap_id, slot = id, total = slots.len(), "isolate started");
        Ok(slot)
    }

    fn resolve(&self, placement: Placement, cap: Option<usize>) -> Result<Arc<Slot>> {
        match placement {
            Placement::Existing(id) => Ok(self.get(id)),
            Placement::Grow => self.grow(cap),
        }
    }

    /// Build one isolate up front.
    ///
    /// The pool is otherwise lazy, and an idle node holding no isolates is the
    /// point. But the first isolate is built at startup for two reasons that
    /// laziness cannot serve: a node whose script does not load must fail
    /// *then*, not once per request forever after; and compiling the script
    /// inside the first request adds its whole cost to that request's latency,
    /// which a startup probe reads as a node that never came up.
    pub fn warm(&self) -> Result<()> {
        self.grow(Some(self.limits.max_stateless))?;
        Ok(())
    }

    /// Take a stateless request, or refuse it.
    ///
    /// One call against one snapshot: admission and placement are the same
    /// decision, so the node cannot admit a request it then fails to place.
    pub fn admit(&self, shedding: bool) -> Result<Affiliation, AdmitError> {
        // A configured request ceiling must remain exact under concurrent
        // admission, so serialize the check and reservation only in that
        // mode. The default unbounded path keeps concurrent shared access.
        if self.limits.max_requests.is_some() {
            let _maintenance = self.maintenance.write().expect("pool poisoned");
            self.admit_locked(shedding)
        } else {
            let _maintenance = self.maintenance.read().expect("pool poisoned");
            self.admit_locked(shedding)
        }
    }

    fn admit_locked(&self, shedding: bool) -> Result<Affiliation, AdmitError> {
        let placement = cellar_logic::isolate::admit(&self.load(), shedding)?;
        self.resolve(placement, Some(self.limits.max_stateless))
            .map(|slot| slot.affiliate())
            .map_err(AdmitError::Build)
    }

    /// [`admit`](Self::admit), except a request that finds the node full
    /// waits — bounded — for a slot instead of being refused at once.
    ///
    /// The wait is the middle gear between "admit" and "refuse". At
    /// overload the parse-and-503 path costs about half a served request,
    /// so a node refusing 200k times a second spends its cores saying no.
    /// A parked waiter costs nothing until an `Affiliation` drop hands it
    /// the slot, and the excess load queues in kernel socket buffers
    /// behind connections nobody is reading. `NodePressured` still
    /// refuses at once: pressure means give resources back, not hold
    /// callers.
    pub async fn admit_or_wait(&self, shedding: bool) -> Result<Affiliation, AdmitError> {
        let deadline_ms =
            crate::asyncrt::mono_ms().saturating_add(self.admission_wait.as_millis() as u64);
        loop {
            // Armed before the check: a slot freed between a failed admit
            // and the park would otherwise be a lost wakeup.
            let freed = self.freed.notified();
            match self.admit(shedding) {
                Err(AdmitError::Refused(Refusal::NodeFull)) => {
                    tokio::pin!(freed);
                    // A freed slot before the deadline: both arms end the
                    // wait, so declaring the order costs nothing and makes
                    // the branch replayable. Priority to the wakeup, because
                    // a slot that freed in the same instant the deadline
                    // expired is a slot the caller can have.
                    crate::asyncrt::select_biased! {
                        "a slot freed in the same instant as the deadline goes to the caller";
                        _ = &mut freed => {},
                        _ = crate::asyncrt::sleep_until(deadline_ms) => {
                            return Err(AdmitError::Refused(Refusal::NodeFull));
                        }
                    }
                }
                other => return other,
            }
        }
    }

    /// Give a cell a home, building an isolate if every one is full.
    ///
    /// The counterpart of [`admit`](Self::admit) and deliberately the same
    /// shape: one call against one snapshot. It cannot refuse — by the time
    /// a cell needs a realm, ownership has already agreed the node serves
    /// it, so `cellar_logic::isolate::place_cell` always answers.
    pub fn place_cell(&self) -> Result<Residency> {
        let _maintenance = self.maintenance.write().expect("pool poisoned");
        let placement = cellar_logic::isolate::place_cell(&self.load());
        // Cell isolates have no arbitrary total cap. The resident-cell and
        // RSS limits bound node memory before placement. The lock keeps this
        // reservation exact and prevents maintenance from retiring the slot.
        Ok(self.resolve(placement, None)?.house(self.limits.max_cells))
    }

    /// One maintenance pass: give back an isolate the pool no longer needs,
    /// and free whatever has finished draining.
    ///
    /// Without this the pool only grows. A burst takes it to `max_stateless`
    /// and every one of those worker heaps is held for the life of the process,
    /// which is the whole reason `cellar_logic::isolate::retire` exists —
    /// and, until this existed, the reason it was dead code hiding a leak.
    ///
    /// Retire at most one isolate. Stateless pools use this paced maintenance
    /// so a quiet interval does not discard every warm compiled Worker.
    pub fn reap(&self) {
        // Maintenance runs on a Tokio worker. Skip this pass when a blocking
        // cell start is compiling an isolate instead of blocking the worker.
        let Ok(_maintenance) = self.maintenance.try_write() else {
            return;
        };
        self.retire_one();
        self.free_retired();
    }

    /// The bell a maintenance task awaits to call
    /// [`free_drained`](Self::free_drained) between reap ticks.
    pub(crate) fn reclaim_bell(&self) -> Arc<tokio::sync::Notify> {
        self.reclaim.clone()
    }

    /// Free every retiring isolate that has drained, and retire nothing.
    /// Returns `false` when admission or cell placement holds the pool, so
    /// the caller retries rather than let the heap wait for the next reap.
    ///
    /// The guard is the one `reap` takes, for the same race: an admission
    /// that snapshotted this slot before it began retiring can still
    /// affiliate to it, and `may_free` only sees that request once the
    /// admission has released the guard.
    pub(crate) fn free_drained(&self) -> bool {
        let Ok(_maintenance) = self.maintenance.try_write() else {
            return false;
        };
        self.free_retired();
        true
    }

    /// Retire every empty isolate. Cell pools use this after eviction because
    /// an empty cell heap carries no warm request capacity worth preserving.
    pub fn reap_empty(&self) {
        let Ok(_maintenance) = self.maintenance.try_write() else {
            tracing::debug!(
                event = "isolate_reap_skipped",
                "a cell start holds the pool; empty isolates wait for the next pass"
            );
            return;
        };
        while self.retire_one() {}
        self.free_retired();
    }

    /// Retire every isolate, housed or not: the pool belongs to a superseded
    /// application generation and must take no new work. Stateless slots
    /// free as their affiliations drop; a slot hosting cells frees once the
    /// last of them has moved to a newer generation, which `may_free`
    /// enforces by refusing a housed isolate.
    pub fn retire_all(&self) {
        let _maintenance = self.maintenance.write().expect("pool poisoned");
        self.retired.store(true, Ordering::Relaxed);
        for slot in self.slots.read().expect("pool poisoned").iter() {
            if !slot.retiring.swap(true, Ordering::Relaxed) {
                tracing::info!(isolate = %slot.heap_id, slot = slot.id, "isolate retiring");
            }
        }
        self.free_retired();
    }

    /// Whether every isolate this pool ever built has been freed. True for a
    /// pool that never grew, so a generation whose scripts took no traffic
    /// drains at once.
    pub fn is_drained(&self) -> bool {
        self.slots
            .read()
            .expect("pool poisoned")
            .iter()
            .all(|slot| slot.worker.try_lock().is_ok_and(|worker| worker.is_none()))
    }

    fn retire_one(&self) -> bool {
        let Some(id) = cellar_logic::isolate::retire(&self.load()) else {
            return false;
        };
        let slot = self.get(id);
        if !slot.retiring.swap(true, Ordering::Relaxed) {
            tracing::info!(isolate = %slot.heap_id, slot = id, "isolate retiring");
        }
        true
    }

    fn free_retired(&self) {
        // A retiring isolate takes no new work, so once its counters reach
        // zero nothing can enter it again and its heap can go. `try_lock`
        // rather than blocking: if a turn holds it, it is not drained and
        // this pass has nothing to do.
        for slot in self.slots.read().expect("pool poisoned").iter() {
            if slot.worker_pins.load(Ordering::Acquire) != 0
                || !cellar_logic::isolate::may_free(&slot.observe())
            {
                continue;
            }
            let Ok(mut worker) = slot.worker.try_lock() else {
                continue;
            };
            if slot.worker_pins.load(Ordering::Acquire) != 0 {
                continue;
            }
            if worker.take().is_some() {
                tracing::info!(isolate = %slot.heap_id, slot = slot.id, "isolate freed");
            }
        }
    }

    /// Return the current live-isolate count.
    pub fn live(&self) -> usize {
        self.slots
            .read()
            .expect("pool poisoned")
            .iter()
            .filter(|slot| !slot.is_retiring())
            .count()
    }

    /// What the pool holds right now, for `/state`.
    ///
    /// The count an operator of a hibernation-heavy node needs and could
    /// not get: a dormant cell keeps about 160 KB more than its own records
    /// explain (GCE, 2026-09-03), and the one O(cells) structure
    /// that fits is the isolate. `live_empty` is what `reap_empty` retires
    /// on its next pass and `retiring` is what `may_free` still refuses, so
    /// either staying positive across passes names the leak.
    pub fn census(&self) -> PoolCensus {
        let mut census = PoolCensus::default();
        for slot in self.slots.read().expect("pool poisoned").iter() {
            let load = slot.observe();
            census.cells += load.cells;
            census.requests += load.requests;
            census.turns += load.turns;
            // A heap is gone once its worker was taken. A worker a turn
            // holds right now is installed, so a failed `try_lock` counts
            // as a heap and never as freed; its size is unknown this pass.
            // Holding the guard is the permit a turn holds, which is what
            // keeps the slot lock uncontended while sampling memory.
            match slot.worker.try_lock() {
                Ok(worker) if worker.is_none() => {
                    census.freed += 1;
                    continue;
                }
                Ok(worker) => {
                    if let Some(heap) = worker.as_ref().and_then(Worker::heap_bytes) {
                        census.heap_bytes += heap.physical;
                        census.external_bytes += heap.external;
                    }
                }
                Err(_) => {}
            }
            if load.retiring {
                census.retiring += 1;
            } else {
                census.live += 1;
                if load.cells == 0 {
                    census.live_empty += 1;
                }
            }
        }
        census
    }
}

/// The worker pool and its state can be reached from any Tokio worker.
/// All application backends must retain this Send/Sync contract.
///
/// A compile-time check rather than a comment, because losing it would show
/// up as a confusing error somewhere far away.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Pool>();
    assert_send_sync::<Slot>();
    assert_send_sync::<Affiliation>();
};
