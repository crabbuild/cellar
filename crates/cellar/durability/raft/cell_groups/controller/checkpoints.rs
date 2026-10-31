// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Checkpoint the preceding applied cut while retaining its successor ticket.
//! One bounded slot spans capture, reserved admission and durable publication.
use super::*;
use crate::raft::cell_groups::{
    CellGroupApply, CellGroupCheckpointError, CellGroupCheckpointPool,
    PreparedLocalCellGroupCheckpoint,
};
use cellar_logic::cell_group::GroupHistory;
use futures_util::{future::BoxFuture, stream::FuturesUnordered, StreamExt};

#[derive(Clone, Copy, Debug)]
pub struct CellGroupCheckpointPolicy {
    /// Entries since the persisted checkpoint (birth index before the first).
    /// Selection occurs before a successor apply; an idle final tail is retained.
    pub interval_entries: u64,
    /// Node-wide captures, admissions and pending syncs combined.
    pub max_parallel: usize,
}

pub(super) struct Config {
    policy: CellGroupCheckpointPolicy,
    pool: CellGroupCheckpointPool,
}

impl Config {
    pub fn new(
        limits: CellGroupControllerLimits,
        transport: &RaftTransport,
    ) -> Result<Option<Self>, CellGroupControllerError> {
        let Some(policy) = limits.checkpoints else {
            return Ok(None);
        };
        if policy.interval_entries == 0
            || policy.max_parallel == 0
            || policy.max_parallel > limits.max_groups.min(256)
        {
            return Err(CellGroupControllerError::Invalid("checkpoint policy"));
        }
        Ok(Some(Self {
            policy,
            pool: transport
                .group_checkpoint_pool()
                .map_err(CellGroupControllerError::Transport)?,
        }))
    }
}

struct Entry {
    work: Box<CellGroupApply>,
    admitted: bool,
    published: bool,
}

type Captured = (
    GroupHistory,
    Result<PreparedLocalCellGroupCheckpoint, CellGroupControllerError>,
);
type Admitted = (GroupHistory, Result<(), RuntimeError>);

pub(super) enum Completed {
    Captured(Captured),
    Admitted(Admitted),
}

#[derive(Default)]
pub(super) struct Manager {
    entries: BTreeMap<GroupHistory, Entry>,
    retry_at: BTreeMap<GroupHistory, u64>,
    captures: FuturesUnordered<BoxFuture<'static, Captured>>,
    publications: FuturesUnordered<BoxFuture<'static, Admitted>>,
}

impl Manager {
    pub fn contains(&self, group: &GroupHistory) -> bool {
        self.entries.contains_key(group)
    }

    pub fn running(&self) -> bool {
        !self.captures.is_empty() || !self.publications.is_empty()
    }

    pub async fn next(&mut self) -> Completed {
        crate::asyncrt::select! {
            Some(done) = self.captures.next(), if !self.captures.is_empty() => Completed::Captured(done),
            Some(done) = self.publications.next(), if !self.publications.is_empty() => Completed::Admitted(done),
        }
    }

    pub fn start(
        &mut self,
        work: &CellGroupApply,
        now: u64,
        shared: &Shared,
    ) -> Result<bool, CellGroupControllerError> {
        let Some(config) = &shared.checkpoints else {
            return Ok(false);
        };
        let history = work.base.marker().route().history;
        if self.entries.len() >= config.policy.max_parallel
            || self.retry_at.get(&history).is_some_and(|at| *at > now)
        {
            return Ok(false);
        }
        let group = shared
            .runtime
            .log()
            .cell_group(&history)
            .map_err(RuntimeError::from)?;
        let previous = group
            .checkpoint_manifest()
            .map_err(RuntimeError::from)?
            .map_or(1, |root| root.position().index.get());
        let index = work.base.marker().position().index.get();
        if index.saturating_sub(previous) < config.policy.interval_entries {
            return Ok(false);
        }
        let pool = config.pool.clone();
        let root = work.base.clone();
        let task = shared
            .domain
            .blocking(move || pool.capture(&group, &root)?.prepare_publication())
            .map_err(|_| CellGroupControllerError::Closed)?;
        self.entries.insert(
            history,
            Entry {
                work: Box::new(work.clone()),
                admitted: false,
                published: false,
            },
        );
        self.captures.push(Box::pin(async move {
            (
                history,
                task.await
                    .map_err(|_| CellGroupControllerError::Closed)
                    .and_then(|result| {
                        result
                            .map_err(|error| CellGroupControllerError::Checkpoint(Arc::new(error)))
                    }),
            )
        }));
        shared
            .active_checkpoints
            .store(self.entries.len(), Ordering::Release);
        Ok(true)
    }

    pub fn publish(
        &mut self,
        group: GroupHistory,
        checkpoint: PreparedLocalCellGroupCheckpoint,
        runtime: &CellGroupRuntime,
    ) -> Result<(), CellGroupControllerError> {
        let entry = self
            .entries
            .get(&group)
            .ok_or(CellGroupControllerError::Invalid(
                "checkpoint capture ticket",
            ))?;
        let ticket = entry.work.ticket.clone();
        let runtime = runtime.clone();
        self.publications.push(Box::pin(async move {
            (
                group,
                runtime
                    .checkpoint_before_apply(group, ticket, checkpoint)
                    .await,
            )
        }));
        Ok(())
    }

    pub fn observed(
        &mut self,
        group: GroupHistory,
        index: u64,
        shared: &Shared,
    ) -> Result<Option<Box<CellGroupApply>>, CellGroupControllerError> {
        let Some(entry) = self.entries.get_mut(&group) else {
            return Ok(None);
        };
        if entry.work.base.marker().position().index.get() != index {
            return Err(CellGroupControllerError::Invalid(
                "checkpoint publication cut",
            ));
        }
        entry.published = true;
        Ok(if entry.admitted {
            self.remove(group, shared)
        } else {
            None
        })
    }

    pub fn accept(
        &mut self,
        group: GroupHistory,
        shared: &Shared,
    ) -> Result<Option<Box<CellGroupApply>>, CellGroupControllerError> {
        let entry = self
            .entries
            .get_mut(&group)
            .ok_or(CellGroupControllerError::Invalid(
                "checkpoint admission ticket",
            ))?;
        entry.admitted = true;
        Ok(if entry.published {
            self.remove(group, shared)
        } else {
            None
        })
    }

    pub fn retry(
        &mut self,
        group: GroupHistory,
        now: u64,
        shared: &Shared,
    ) -> Option<Box<CellGroupApply>> {
        self.retry_at
            .insert(group, now.saturating_add(shared.limits.retry_interval_ms));
        shared.checkpoint_retries.fetch_add(1, Ordering::Relaxed);
        self.remove(group, shared)
    }

    pub fn remove(&mut self, group: GroupHistory, shared: &Shared) -> Option<Box<CellGroupApply>> {
        let work = self.entries.remove(&group).map(|entry| entry.work);
        shared
            .active_checkpoints
            .store(self.entries.len(), Ordering::Release);
        work
    }

    /// A cancelled controller waiter cannot detach a physical capture. Join it
    /// before closing the runtime's engine claim. Admitted writes belong to the
    /// runtime and are joined by runtime.close(). Retain tickets until both joins.
    pub async fn join(&mut self) {
        while self.captures.next().await.is_some() {}
        self.publications.clear();
    }
}

pub(super) fn retryable(error: &CellGroupControllerError) -> bool {
    matches!(error, CellGroupControllerError::Checkpoint(error) if matches!(error.as_ref(), CellGroupCheckpointError::Busy))
        || matches!(
            error,
            CellGroupControllerError::Runtime(RuntimeError::Overloaded)
        )
}
