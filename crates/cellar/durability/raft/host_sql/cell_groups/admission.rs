// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use cellar_logic::{
    cell_group::CellId,
    consensus::{ClusterId, NamespaceId},
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Mutex, Weak};

type LogicalCell = (ClusterId, NamespaceId, CellId);

pub(super) struct Admission {
    lease: Arc<crate::raft::storage::SqlPoolLease>,
    node: Arc<Budget>,
    limits: CellSqlLimits,
    state: Mutex<State>,
    max_retired_groups: usize,
}

#[derive(Default)]
struct State {
    cells: BTreeSet<LogicalCell>,
    groups: BTreeMap<GroupHistory, Weak<Budget>>,
    retired: BTreeSet<GroupHistory>,
}

pub(super) struct Bytes {
    _lease: Arc<crate::raft::storage::SqlPoolLease>,
    _node: Permit,
    _group: Permit,
}

impl Bytes {
    pub fn shrink(bytes: &mut Arc<Self>, retained: usize) -> Result<(), ExecutionError> {
        let unique =
            Arc::get_mut(bytes).ok_or(ExecutionError::Invalid("shared capture admission"))?;
        if !unique._node.shrink_to(retained) || !unique._group.shrink_to(retained) {
            return Err(ExecutionError::Invalid("retained SQL output bound"));
        }
        Ok(())
    }
}

pub(super) struct CellLease {
    owner: Arc<Admission>,
    cell: LogicalCell,
}

impl Drop for CellLease {
    fn drop(&mut self) {
        if let Ok(mut state) = self.owner.state.lock() {
            state.cells.remove(&self.cell);
        }
    }
}

impl Admission {
    pub fn limits(&self) -> CellSqlLimits {
        self.limits
    }

    pub fn new(
        limits: CellSqlLimits,
        lease: Arc<crate::raft::storage::SqlPoolLease>,
        max_retired_groups: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            lease,
            max_retired_groups,
            node: Budget::new(limits.node_bytes, limits.max_pending_cells),
            limits,
            state: Mutex::new(State::default()),
        })
    }

    pub fn reserve(
        self: &Arc<Self>,
        cell: &CellHistory,
        group: GroupHistory,
        bytes: usize,
    ) -> Result<(CellLease, Arc<Bytes>), ExecutionError> {
        let mut state = self.state.lock().map_err(|_| unavailable())?;
        let key = (cell.cluster, cell.namespace, cell.cell.clone());
        if state.cells.contains(&key) {
            return Err(ExecutionError::Busy);
        }
        let bytes = self.reserve_bytes(&mut state, group, bytes)?;
        state.cells.insert(key.clone());
        Ok((
            CellLease {
                owner: self.clone(),
                cell: key,
            },
            bytes,
        ))
    }

    pub fn reserve_read(
        &self,
        group: GroupHistory,
        bytes: usize,
    ) -> Result<Arc<Bytes>, ExecutionError> {
        let mut state = self.state.lock().map_err(|_| unavailable())?;
        self.reserve_bytes(&mut state, group, bytes)
    }

    fn reserve_bytes(
        &self,
        state: &mut State,
        group: GroupHistory,
        bytes: usize,
    ) -> Result<Arc<Bytes>, ExecutionError> {
        if state.retired.contains(&group) {
            return Err(ExecutionError::Refused(CommandFailure::Unavailable));
        }
        let node = self.node.acquire(bytes).ok_or_else(overloaded)?;
        // Reclaim metadata only, never authority or pending physical ownership.
        state.groups.retain(|_, budget| budget.strong_count() != 0);
        let group_budget = state
            .groups
            .get(&group)
            .and_then(Weak::upgrade)
            .unwrap_or_else(|| {
                Budget::new(self.limits.group_bytes, self.limits.group_pending_cells)
            });
        let group_permit = group_budget.acquire(bytes).ok_or_else(overloaded)?;
        state.groups.insert(group, Arc::downgrade(&group_budget));
        Ok(Arc::new(Bytes {
            _lease: self.lease.clone(),
            _node: node,
            _group: group_permit,
        }))
    }

    /// Seal this history's admission under the same lock as reservation. A
    /// retained response/producer stays charged after its caller disconnects.
    pub fn retire(&self, group: GroupHistory) -> Result<bool, ExecutionError> {
        let mut state = self.state.lock().map_err(|_| unavailable())?;
        if !state.retired.contains(&group) && state.retired.len() >= self.max_retired_groups {
            return Err(overloaded());
        }
        state.retired.insert(group);
        Ok(state
            .groups
            .get(&group)
            .is_none_or(|budget| budget.strong_count() == 0))
    }

    pub(super) fn resume_reclaimed_member(
        &self,
        group: GroupHistory,
    ) -> Result<(), ExecutionError> {
        let mut state = self.state.lock().map_err(|_| unavailable())?;
        if state
            .groups
            .get(&group)
            .is_some_and(|budget| budget.strong_count() != 0)
        {
            return Err(ExecutionError::Busy);
        }
        state.groups.remove(&group);
        state.retired.remove(&group);
        Ok(())
    }

    pub fn usage(&self) -> Result<(usize, usize, usize), ExecutionError> {
        let cells = self.state.lock().map_err(|_| unavailable())?.cells.len();
        let (bytes, reservations) = self.node.usage();
        Ok((cells, bytes, reservations))
    }
}

#[cfg(all(test, not(cellar_internal_tests)))]
mod tests {
    use super::*;
    use crate::raft::{
        options::{LogOpenMode, ReplicaLogOptions},
        storage::ReplicaLog,
    };
    use cellar_logic::consensus::{DiskLineage, GroupId, IncarnationId, NodeId, ProcessGeneration};

    #[tokio::test]
    #[allow(clippy::disallowed_methods)]
    async fn retirement_seals_late_sql_without_refunding_held_or_other_group_work(
    ) -> anyhow::Result<()> {
        let root = tempfile::tempdir()?;
        let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
        let log = domain
            .run(ReplicaLog::open(
                ReplicaLogOptions::new(
                    root.path().join("raft"),
                    NodeId::new([1; 16])?,
                    DiskLineage::new([2; 16])?,
                    ProcessGeneration::new([3; 16])?,
                    LogOpenMode::Create,
                ),
                Arc::new(cellar_ltx::DirectFileSystem),
            ))
            .await??;
        let limits = CellSqlLimits {
            workers: 1,
            queue_per_worker: 4,
            max_pending_cells: 4,
            group_pending_cells: 2,
            node_bytes: 16_384,
            group_bytes: 8192,
        };
        let admission = Admission::new(limits, log.claim_sql_pool()?, 2);
        let history = |id| -> anyhow::Result<GroupHistory> {
            Ok(GroupHistory {
                cluster: ClusterId::new([4; 16])?,
                namespace: NamespaceId::new([5; 16])?,
                group: GroupId::new([id; 16])?,
                incarnation: IncarnationId::new([id; 16])?,
            })
        };
        let first = history(6)?;
        let second = history(7)?;
        let response = admission.reserve_read(first, 1024)?;
        let physical = response.clone();
        assert!(!admission.retire(first)?);
        drop(response);
        assert!(!admission.retire(first)?);
        assert!(matches!(
            admission.reserve_read(first, 1024),
            Err(ExecutionError::Refused(CommandFailure::Unavailable))
        ));
        let unrelated = admission.reserve_read(second, 1024)?;
        drop(physical);
        assert!(admission.retire(first)?);
        assert_eq!(admission.usage()?.2, 1);
        assert!(!admission.retire(second)?);
        assert!(matches!(
            admission.retire(history(8)?),
            Err(ExecutionError::Refused(CommandFailure::Overloaded))
        ));
        drop(unrelated);
        assert!(admission.retire(second)?);
        drop(admission);
        log.close().await?;
        domain.shutdown().await;
        Ok(())
    }
}
