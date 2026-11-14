// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One bounded node turn discovers committed work through the manifest index.
//! Cursors are hints per physical group; no cell owns a task, timer or cache.

mod turn;

use super::*;
use crate::application::consensus::RaftClass;
use crate::raft::budget::Budget;
use crate::raft::cell_groups::{CellGroupDispatcher, CellSqlOptions};
use crate::raft::effects::RaftEffectOptions;
use cellar_logic::{
    cell_group::{CellHistory, CellId, GroupHistory},
    consensus::{ClusterId, LogIndex, NamespaceId},
};
use futures_util::{future::BoxFuture, stream::FuturesUnordered, FutureExt, StreamExt};
use std::collections::BTreeSet;
use std::ops::Bound::{Excluded, Included, Unbounded};

type SqlFactory = Arc<dyn Fn(&CellHistory) -> Result<CellSqlOptions, ExecutionError> + Send + Sync>;

/// A compiled namespace's effect adapter and explicit execution resources.
/// The factory runs on the owning execution domain's blocking service. It must
/// derive private paths from the complete cell history and must not spawn work.
/// Its captured configuration must be bounded independently of cell count.
pub struct CellGroupEffectRegistration {
    dispatcher: CellGroupDispatcher,
    options: RaftEffectOptions,
    sql: SqlFactory,
}

impl CellGroupEffectRegistration {
    pub fn new(
        dispatcher: CellGroupDispatcher,
        options: RaftEffectOptions,
        sql: impl Fn(&CellHistory) -> Result<CellSqlOptions, ExecutionError> + Send + Sync + 'static,
    ) -> Result<Self, CellGroupControllerError> {
        if !(10..=60_000).contains(&options.interval_ms)
            || !(1..=60_000).contains(&options.operation_timeout_ms)
            || !(1..=300_000).contains(&options.callback_timeout_ms)
            || options
                .operation_timeout_ms
                .checked_add(100)
                .is_none_or(|n| n > options.retry_window_ms)
            || options.retry_window_ms > 600_000
            || !(64 * 1024..=8 * 1024 * 1024).contains(&options.metadata_bytes)
        {
            return Err(CellGroupControllerError::Invalid("effect scheduler bounds"));
        }
        Ok(Self {
            dispatcher,
            options,
            sql: Arc::new(sql),
        })
    }
}

struct Registration {
    dispatcher: CellGroupDispatcher,
    class: Arc<RaftClass>,
    schema: [u8; 32],
    options: RaftEffectOptions,
    memory: Arc<Budget>,
    sql: SqlFactory,
}

pub(super) struct Config {
    namespaces: BTreeMap<(ClusterId, NamespaceId), Arc<Registration>>,
}

impl CellGroupController {
    pub(crate) fn require_host_effects(
        &self,
        dispatcher: &CellGroupDispatcher,
    ) -> Result<(), crate::raft::cell_groups::CellDispatchError> {
        use crate::raft::cell_groups::CellDispatchError;
        let (_, class, _) = dispatcher.host_binding()?;
        if !class.has_scheduled_work() {
            return Ok(());
        }
        let effects = self
            .handle
            .0
            .effects
            .lock()
            .map_err(|_| CellDispatchError::Runtime(RuntimeError::Closed))?;
        if effects
            .as_ref()
            .and_then(|config| config.namespaces.get(&dispatcher.namespace()))
            .is_none_or(|entry| entry.schema != class.schema_digest())
        {
            return Err(CellDispatchError::Execution(ExecutionError::Refused(
                cellar_logic::consensus::CommandFailure::UnsupportedCapability,
            )));
        }
        Ok(())
    }
}

impl CellGroupController {
    /// Install the node's bounded compiled effect registrations once. All local
    /// groups in these namespaces, including later registrations, participate.
    /// Commands retain ordinary admission and S3 settlement. A scan or delivery
    /// waiter cannot retire a pending command's physical SQL producer.
    pub fn configure_effects(
        &self,
        registrations: Vec<CellGroupEffectRegistration>,
    ) -> Result<(), CellGroupControllerError> {
        let shared = &self.handle.0;
        if registrations.is_empty() || registrations.len() > shared.limits.max_groups {
            return Err(CellGroupControllerError::Invalid("effect registrations"));
        }
        let mut namespaces = BTreeMap::new();
        for registration in registrations {
            let (namespace, class) = registration
                .dispatcher
                .effect_binding(&shared.domain)
                .map_err(|_| CellGroupControllerError::Invalid("effect dispatcher owner/class"))?;
            // Alarm discovery retains only Fire{id}; the payload stays on the
            // selected SQLite image until the bounded SQL callback loads it.
            if class.outbox_definition().is_some()
                && registration.options.metadata_bytes < 512 * 1024
            {
                return Err(CellGroupControllerError::Invalid(
                    "effect metadata capacity",
                ));
            }
            let dispatcher = registration
                .dispatcher
                .with_s3(self)
                .map_err(|_| CellGroupControllerError::Invalid("effect archive owner"))?;
            let key = (namespace.cluster, namespace.namespace);
            let registration = Arc::new(Registration {
                dispatcher,
                class,
                schema: namespace.schema_digest,
                options: registration.options,
                memory: Budget::new(registration.options.metadata_bytes, 1),
                sql: registration.sql,
            });
            if namespaces.insert(key, registration).is_some() {
                return Err(CellGroupControllerError::Invalid(
                    "duplicate effect namespace",
                ));
            }
        }
        let mut slot = shared
            .effects
            .lock()
            .map_err(|_| CellGroupControllerError::Closed)?;
        if slot.is_some() || shared.closing.load(Ordering::Acquire) {
            return Err(CellGroupControllerError::Invalid(
                "effect owner already configured/closed",
            ));
        }
        *slot = Some(Arc::new(Config { namespaces }));
        Ok(())
    }
}

#[derive(Default)]
pub(super) struct Manager {
    group: Option<GroupHistory>,
    cells: BTreeMap<GroupHistory, CellId>,
    hints: BTreeMap<GroupHistory, (LogIndex, Option<u64>)>,
    waiting: BTreeSet<(u64, GroupHistory)>,
    ready: BTreeSet<GroupHistory>,
    next_at: u64,
    jobs: FuturesUnordered<BoxFuture<'static, Completed>>,
}

pub(super) struct Completed {
    pub(super) group: GroupHistory,
    cell: Option<CellId>,
    interval: u64,
    result: Result<(), turn::Error>,
}

impl Manager {
    pub fn new(
        roster: &BTreeMap<GroupHistory, CellGroupApplier>,
    ) -> Result<Self, CellGroupControllerError> {
        let mut manager = Self::default();
        for applier in roster.values() {
            manager.register(applier)?;
        }
        Ok(manager)
    }

    pub fn register(&mut self, applier: &CellGroupApplier) -> Result<(), CellGroupControllerError> {
        let (position, next) = applier
            .effect_hint()
            .map_err(|error| CellGroupControllerError::Apply(Arc::new(error)))?;
        self.observe(applier.history(), position, next)
    }

    pub fn observe(
        &mut self,
        group: GroupHistory,
        position: LogIndex,
        next: Option<u64>,
    ) -> Result<(), CellGroupControllerError> {
        if let Some((old_position, old_next)) = self.hints.get(&group) {
            if position < *old_position {
                return Ok(());
            }
            if position == *old_position && next != *old_next {
                return Err(CellGroupControllerError::Invalid(
                    "conflicting effect hint at one apply boundary",
                ));
            }
            if let Some(old_next) = old_next {
                self.waiting.remove(&(*old_next, group));
            }
        }
        self.ready.remove(&group);
        self.hints.insert(group, (position, next));
        if let Some(next) = next {
            self.waiting.insert((next, group));
        }
        Ok(())
    }

    pub fn remove(&mut self, group: &GroupHistory) {
        self.cells.remove(group);
        self.ready.remove(group);
        if let Some((_, Some(at))) = self.hints.remove(group) {
            self.waiting.remove(&(at, *group));
        }
    }

    pub fn retiring(&mut self, group: GroupHistory) -> bool {
        self.remove(&group);
        !self.running() || self.group != Some(group)
    }

    pub fn running(&self) -> bool {
        !self.jobs.is_empty()
    }

    pub fn launch(
        &mut self,
        quarantined: &BTreeSet<GroupHistory>,
        shared: &Arc<Shared>,
        now: u64,
    ) -> Result<(), CellGroupControllerError> {
        if self.running() || now < self.next_at || shared.closing.load(Ordering::Acquire) {
            return Ok(());
        }
        let Some(config) = shared
            .effects
            .lock()
            .map_err(|_| CellGroupControllerError::Closed)?
            .clone()
        else {
            return Ok(());
        };
        let Ok(wall) = u64::try_from(crate::asyncrt::wall_ms()) else {
            return Ok(());
        };
        // Work scales with due groups. Empty and future-only groups incur no
        // runtime RPC or SQLite open. Promotion is bounded per node turn.
        for _ in 0..64 {
            let Some((at, group)) = self.waiting.first().copied() else {
                break;
            };
            if at > wall {
                break;
            }
            self.waiting.remove(&(at, group));
            self.ready.insert(group);
        }
        let after = self.group.map_or(Unbounded, Excluded);
        let through = self.group.map_or(Unbounded, Included);
        // Rotate ready groups rather than repeatedly choosing the oldest due
        // timestamp. A hot cell/group cannot starve another due group.
        for group in self
            .ready
            .range((after, Unbounded))
            .chain(self.ready.range((Unbounded, through)))
            .take(4)
        {
            self.group = Some(*group);
            if quarantined.contains(group) || !shared.groups.contains(group)? {
                continue;
            }
            let Some(registration) = config.namespaces.get(&(group.cluster, group.namespace))
            else {
                continue;
            };
            let Some(reservation) = registration
                .memory
                .acquire(registration.options.metadata_bytes)
            else {
                continue;
            };
            // Serialize work visibility with the public maintenance/drain
            // barrier before scheduling a scan or invoking an effect adapter.
            let mut admission = match shared.registration() {
                Ok(admission) => admission,
                // The reversible refusal was captured under the admission
                // barrier. A concurrent resume cannot turn it into a failure.
                Err(RuntimeError::NotReady) => return Ok(()),
                Err(_) if shared.application_paused() || shared.closing.load(Ordering::Acquire) => {
                    return Ok(())
                }
                Err(error) => return Err(error.into()),
            };
            if shared.application_paused() {
                return Ok(());
            }
            let work = admission.work.take();
            let group = *group;
            let after = self.cells.get(&group).cloned();
            let registration = registration.clone();
            drop(admission);
            let shared = shared.clone();
            shared.active_effects.store(1, Ordering::Release);
            self.jobs.push(Box::pin(async move {
                let _work = work;
                let mut cell = None;
                let result = std::panic::AssertUnwindSafe(turn::run(
                    group,
                    after,
                    &registration,
                    &shared,
                    reservation,
                    &mut cell,
                ))
                .catch_unwind()
                .await
                .unwrap_or(Err(turn::Error::Invalid("effect adapter panicked")));
                Completed {
                    group,
                    cell,
                    interval: registration.options.interval_ms,
                    result,
                }
            }));
            return Ok(());
        }
        self.next_at = now.saturating_add(shared.limits.retry_interval_ms);
        Ok(())
    }

    pub async fn next(&mut self) -> Option<Completed> {
        self.jobs.next().await
    }

    pub fn complete(&mut self, completed: Completed, shared: &Shared) {
        if let Some(cell) = completed.cell {
            if self.hints.contains_key(&completed.group) {
                self.cells.insert(completed.group, cell);
            }
        }
        self.next_at = shared
            .domain
            .mono_ms()
            .unwrap_or(u64::MAX)
            .saturating_add(completed.interval);
        shared.active_effects.store(0, Ordering::Release);
        shared.effect_turns.fetch_add(1, Ordering::Relaxed);
        if let Err(error) = completed.result {
            shared.effect_retries.fetch_add(1, Ordering::Relaxed);
            tracing::debug!(group = ?completed.group, %error, "effect turn requires committed-state reconciliation");
        }
    }

    pub async fn join(&mut self, shared: &Shared) {
        // Closing stops admission; already admitted commands stay owned by the
        // controller. Poll scans/factories/sink waiters to joined completion
        // before the controller closes SQL and releases its physical claims.
        while let Some(completed) = self.next().await {
            self.complete(completed, shared);
        }
    }
}
