// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Retire one verified source without stopping unrelated node services.
use super::*;
use crate::raft::{
    cache::GroupLog,
    cell_groups::{DirectoryMutation, GroupManifest, RangeDirectory},
};
use cellar_logic::cell_group::GroupHistory;
use tokio::sync::oneshot;

/// Constructed only after the exact selected retirement set verifies the local
/// synced tombstone. No local marker alone authorizes dropping a voter.
#[derive(Clone)]
pub(crate) struct RetiredSource {
    group: GroupLog<GroupHistory>,
    marker: GroupManifest,
    member: Option<super::super::member_reclaim::Member>,
}

impl RetiredSource {
    pub(super) fn removed_member(
        log: &crate::raft::storage::ReplicaLog,
        member: super::super::member_reclaim::Member,
    ) -> Result<Self, RuntimeError> {
        let group = log.cell_group(&member.scope()?)?;
        member.check_group(&group)?;
        let marker = group.group_manifest()?.ok_or(RuntimeError::NotReady)?;
        Ok(Self {
            group,
            marker,
            member: Some(member),
        })
    }

    pub(crate) fn history(&self) -> GroupHistory {
        self.marker.route().history
    }

    pub(crate) fn check(&self, log: &crate::raft::storage::ReplicaLog) -> Result<(), RuntimeError> {
        let group = log.cell_group(&self.history())?;
        if let Some(member) = &self.member {
            member.check_group(&group)?;
            if !Arc::ptr_eq(&group.owner, &self.group.owner)
                || group.identity() != self.group.identity()
            {
                return Err(RuntimeError::StaleCompletion);
            }
            return Ok(());
        }
        let cache = group.read()?;
        let current = cache
            .group_manifest
            .as_ref()
            .ok_or(RuntimeError::NotReady)?;
        if !Arc::ptr_eq(&group.owner, &self.group.owner)
            || group.identity() != self.group.identity()
            || current.route() != self.marker.route()
            || current.source_seal() != self.marker.source_seal()
            || self
                .marker
                .source_seal()
                .and_then(|seal| seal.retirement())
                .is_none()
            || cache.hard_state.commit < current.position().index.get()
        {
            return Err(RuntimeError::StaleCompletion);
        }
        Ok(())
    }
}

pub(super) struct Request {
    pub(super) source: RetiredSource,
    pub(super) reply: oneshot::Sender<super::calls::Delivery<(), CellGroupControllerError>>,
    pub(super) memory: Arc<crate::raft::budget::Permit>,
}

impl CellGroupController {
    /// Internal handoff after the Fleet owner verified all final source members.
    /// Losing a waiter never reopens sealed admission or frees the retained slot.
    pub(crate) async fn quiesce_retired_source(
        &self,
        directory: &RangeDirectory,
        plan: &DirectoryMutation,
        history: GroupHistory,
    ) -> Result<(), CellGroupControllerError> {
        let shared = &self.handle.0;
        if shared.closing.load(Ordering::Acquire) {
            return Err(CellGroupControllerError::Closed);
        }
        let memory = Arc::new(
            shared
                .commands
                .acquire(32 * 1024)
                .ok_or(RuntimeError::Overloaded)?,
        );
        let group = shared
            .runtime
            .log()
            .cell_group(&history)
            .map_err(RuntimeError::from)?;
        let marker = group
            .group_manifest()
            .map_err(RuntimeError::from)?
            .ok_or(RuntimeError::NotReady)?;
        directory
            .park_managed_retired_source(plan, &group, &marker)
            .await
            .map_err(|error| CellGroupControllerError::Catalog(Arc::new(error)))?;
        let source = RetiredSource {
            group,
            marker,
            member: None,
        };
        source.check(shared.runtime.log())?;
        self.quiesce_source(source, memory).await
    }

    pub(super) async fn quiesce_removed_member(
        &self,
        member: super::super::member_reclaim::Member,
    ) -> Result<(), CellGroupControllerError> {
        let shared = &self.handle.0;
        if shared.closing.load(Ordering::Acquire) {
            return Err(CellGroupControllerError::Closed);
        }
        let memory = Arc::new(
            shared
                .commands
                .acquire(32 * 1024)
                .ok_or(RuntimeError::Overloaded)?,
        );
        let source = RetiredSource::removed_member(shared.runtime.log(), member)?;
        self.quiesce_source(source, memory).await
    }

    async fn quiesce_source(
        &self,
        source: RetiredSource,
        memory: Arc<crate::raft::budget::Permit>,
    ) -> Result<(), CellGroupControllerError> {
        let shared = &self.handle.0;
        let (reply, receive) = oneshot::channel();
        shared
            .requests
            .try_send(requests::Request::Retirement(Box::new(Request {
                source,
                reply,
                memory,
            })))
            .map_err(|error| match error {
                tokio::sync::mpsc::error::TrySendError::Full(_) => RuntimeError::Overloaded,
                tokio::sync::mpsc::error::TrySendError::Closed(_) => RuntimeError::Closed,
            })?;
        receive
            .await
            .map_err(|_| CellGroupControllerError::Closed)?
            .result
            .map_err(|error| (*error).clone())
    }
}
