// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::raft::{
    options::{LogOpenMode, ReplicaLogOptions},
    storage::ReplicaLog,
};
use cellar_logic::consensus::{DiskLineage, GroupId, IncarnationId, NodeId, ProcessGeneration};
use futures_util::FutureExt;

struct Fixture {
    admission: Arc<Admission>,
    log: ReplicaLog,
    domain: crate::asyncrt::ExecutionDomain,
    _root: tempfile::TempDir,
}

impl Fixture {
    #[allow(clippy::disallowed_methods)]
    async fn new(node_items: usize, group_items: usize) -> anyhow::Result<Self> {
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
        let admission = Admission::new(
            CellSqlLimits {
                workers: 1,
                queue_per_worker: 4,
                max_pending_cells: node_items,
                group_pending_cells: group_items,
                node_bytes: 128,
                group_bytes: 64,
            },
            log.claim_sql_pool()?,
            2,
            log.performance(),
        );
        Ok(Self {
            _root: root,
            domain,
            log,
            admission,
        })
    }

    async fn close(self) -> anyhow::Result<()> {
        assert_eq!(self.admission.usage()?, (0, 0, 0));
        drop(self.admission);
        self.log.close().await?;
        self.domain.shutdown().await;
        Ok(())
    }
}

fn group(id: u8) -> anyhow::Result<GroupHistory> {
    Ok(GroupHistory {
        cluster: ClusterId::new([4; 16])?,
        namespace: NamespaceId::new([5; 16])?,
        group: GroupId::new([id; 16])?,
        incarnation: IncarnationId::new([id; 16])?,
    })
}

fn cell(group: GroupHistory, name: &str) -> anyhow::Result<CellHistory> {
    Ok(CellHistory {
        cluster: group.cluster,
        namespace: group.namespace,
        cell: CellId::new(name)?,
        incarnation: group.incarnation,
    })
}

fn refused(fixture: &Fixture, cell: &CellHistory, group: GroupHistory, bytes: usize) -> Failure {
    fixture
        .admission
        .reserve_stage(cell, group, bytes)
        .err()
        .expect("reservation refused")
}

#[tokio::test]
async fn capacity_wait_wakes_after_shrink_and_last_retained_owner() -> anyhow::Result<()> {
    let f = Fixture::new(4, 4).await?;
    let group = group(6)?;
    let (lease, mut bytes) = f.admission.reserve(&cell(group, "a")?, group, 64)?;
    let capacity = refused(&f, &cell(group, "b")?, group, 16)
        .wait
        .expect("group capacity proof");
    drop(lease);
    assert_eq!(f.admission.usage()?, (0, 64, 1));
    {
        let mut wait = std::pin::pin!(capacity.released());
        assert!(wait.as_mut().now_or_never().is_none());
        Bytes::shrink(&mut bytes, 48)?;
        assert!(wait.as_mut().now_or_never().is_some());
    }
    let retained = bytes.clone();
    let (other, other_bytes) = f.admission.reserve(&cell(group, "b")?, group, 16)?;
    let final_credit = refused(&f, &cell(group, "c")?, group, 16)
        .wait
        .expect("retained output proof");
    {
        let mut wait = std::pin::pin!(final_credit.released());
        assert!(wait.as_mut().now_or_never().is_none());
        drop(bytes);
        assert!(
            wait.as_mut().now_or_never().is_none(),
            "one output clone still owns the credit"
        );
        drop(retained);
        assert!(wait.as_mut().now_or_never().is_some());
    }
    drop((other, other_bytes, capacity, final_credit));
    f.close().await
}

#[tokio::test]
async fn item_pressure_ignores_byte_shrink_until_owner_exits() -> anyhow::Result<()> {
    let f = Fixture::new(2, 1).await?;
    let group = group(6)?;
    let (lease, mut bytes) = f.admission.reserve(&cell(group, "a")?, group, 32)?;
    let capacity = refused(&f, &cell(group, "b")?, group, 1)
        .wait
        .expect("item capacity proof");
    {
        let mut wait = std::pin::pin!(capacity.released());
        assert!(wait.as_mut().now_or_never().is_none());
        drop(lease);
        Bytes::shrink(&mut bytes, 0)?;
        assert_eq!(f.admission.usage()?, (0, 0, 1));
        assert!(wait.as_mut().now_or_never().is_none());
        drop(bytes);
        assert!(wait.as_mut().now_or_never().is_some());
    }
    drop(capacity);
    f.close().await
}

#[tokio::test]
async fn late_fitting_waiters_recheck_after_a_consumed_signal() -> anyhow::Result<()> {
    let f = Fixture::new(4, 4).await?;
    let group = group(6)?;
    let (a, mut bytes) = f.admission.reserve(&cell(group, "a")?, group, 64)?;
    let first = refused(&f, &cell(group, "b")?, group, 8)
        .wait
        .expect("first wait");
    let second = refused(&f, &cell(group, "c")?, group, 16)
        .wait
        .expect("second wait");
    Bytes::shrink(&mut bytes, 40)?;
    assert!(first
        .bank
        .upgrade()
        .expect("live retained bank")
        .wake
        .notified()
        .now_or_never()
        .is_some());
    assert!(first.released().now_or_never().is_some());
    assert!(
        second.released().now_or_never().is_some(),
        "credit can fit multiple late waiters"
    );
    let b = f.admission.reserve(&cell(group, "b")?, group, 8)?;
    let c = f.admission.reserve(&cell(group, "c")?, group, 16)?;
    assert_eq!(f.admission.usage()?.1, 64);
    drop((a, bytes, b, c, first, second));
    f.close().await
}

#[tokio::test]
async fn nonfitting_waiter_preserves_fitting_cell_and_other_group_progress() -> anyhow::Result<()> {
    let f = Fixture::new(4, 4).await?;
    let hot = group(6)?;
    let cold = group(7)?;
    let (a, mut bytes) = f.admission.reserve(&cell(hot, "a")?, hot, 64)?;
    let large = refused(&f, &cell(hot, "b")?, hot, 32)
        .wait
        .expect("large wait");
    let small = refused(&f, &cell(hot, "c")?, hot, 8)
        .wait
        .expect("small wait");
    let other_group = f.admission.reserve(&cell(cold, "d")?, cold, 32)?;
    {
        let mut large_wait = std::pin::pin!(large.released());
        let mut small_wait = std::pin::pin!(small.released());
        assert!(large_wait.as_mut().now_or_never().is_none());
        assert!(small_wait.as_mut().now_or_never().is_none());
        Bytes::shrink(&mut bytes, 56)?;
        assert!(large_wait.as_mut().now_or_never().is_none());
        assert!(small_wait.as_mut().now_or_never().is_some());
    }
    let fitting = f.admission.reserve(&cell(hot, "c")?, hot, 8)?;
    assert_eq!(f.admission.usage()?.1, 96);
    drop((a, bytes, fitting, other_group, large, small));
    f.close().await
}

#[tokio::test]
async fn oversized_and_retired_reservations_never_grant_wait_proofs() -> anyhow::Result<()> {
    let f = Fixture::new(4, 4).await?;
    let group = group(6)?;
    for bytes in [65, 129] {
        let failure = refused(&f, &cell(group, "a")?, group, bytes);
        assert!(matches!(
            failure.error,
            ExecutionError::Refused(CommandFailure::Overloaded)
        ));
        assert!(failure.wait.is_none());
        assert_eq!(f.admission.usage()?, (0, 0, 0));
    }
    assert!(f.admission.retire(group)?);
    let retired = refused(&f, &cell(group, "a")?, group, 1);
    assert!(matches!(
        retired.error,
        ExecutionError::Refused(CommandFailure::Unavailable)
    ));
    assert!(retired.wait.is_none());
    f.close().await
}

#[tokio::test]
async fn retirement_wakes_waiter_without_refunding_physical_work() -> anyhow::Result<()> {
    let f = Fixture::new(4, 4).await?;
    let group = group(6)?;
    let physical = f.admission.reserve(&cell(group, "a")?, group, 64)?;
    let capacity = refused(&f, &cell(group, "b")?, group, 8)
        .wait
        .expect("wait proof");
    let mut progress = f.admission.writer_progress();
    {
        let mut wait = std::pin::pin!(capacity.released());
        assert!(wait.as_mut().now_or_never().is_none());
        assert!(!f.admission.retire(group)?);
        assert!(progress.changed().now_or_never().is_some());
        assert_eq!(f.admission.usage()?.1, 64);
        drop(physical);
        assert!(wait.as_mut().now_or_never().is_some());
    }
    assert!(f.admission.retire(group)?);
    assert!(
        capacity.released().now_or_never().is_some(),
        "an unpolled proof does not retain the retired owner"
    );
    drop(capacity);
    f.admission.release_reclaimed_group(group)?;
    f.close().await
}

#[tokio::test]
#[allow(clippy::disallowed_methods)]
async fn writer_release_wakes_waiters_without_raft_apply() -> anyhow::Result<()> {
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
    let group = GroupHistory {
        cluster: ClusterId::new([4; 16])?,
        namespace: NamespaceId::new([5; 16])?,
        group: GroupId::new([6; 16])?,
        incarnation: IncarnationId::new([7; 16])?,
    };
    let cell = CellHistory {
        cluster: group.cluster,
        namespace: group.namespace,
        cell: cellar_logic::cell_group::CellId::new("writer")?,
        incarnation: group.incarnation,
    };
    let admission = Admission::new(
        CellSqlLimits {
            workers: 1,
            queue_per_worker: 4,
            max_pending_cells: 4,
            group_pending_cells: 4,
            node_bytes: 16_384,
            group_bytes: 8192,
        },
        log.claim_sql_pool()?,
        2,
        log.performance(),
    );
    let mut progress = admission.writer_progress();
    let (mut first, first_bytes) = admission.reserve(&cell, group, 1024)?;
    assert!(matches!(
        admission.reserve(&cell, group, 1024),
        Err(ExecutionError::Busy)
    ));
    // No group exists and no Raft apply can wake this waiter. Parking must
    // publish local progress even while the original physical bytes stay held.
    first.park()?;
    domain
        .run(crate::asyncrt::timeout(
            std::time::Duration::from_secs(1),
            progress.changed(),
        ))
        .await???;
    let (second, second_bytes) = admission.reserve(&cell, group, 1024)?;
    assert!(matches!(first.resume(), Err(ExecutionError::Busy)));
    drop(second);
    domain
        .run(crate::asyncrt::timeout(
            std::time::Duration::from_secs(1),
            progress.changed(),
        ))
        .await???;
    first.resume()?;
    drop(first);
    domain
        .run(crate::asyncrt::timeout(
            std::time::Duration::from_secs(1),
            progress.changed(),
        ))
        .await???;
    assert_eq!(admission.usage()?.0, 0);
    assert_eq!(
        admission.usage()?.2,
        2,
        "physical ownership must stay charged after writer release"
    );
    drop((first_bytes, second_bytes, admission));
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}

#[tokio::test]
#[allow(clippy::disallowed_methods)]
async fn retirement_seals_late_sql_without_refunding_held_or_other_group_work() -> anyhow::Result<()>
{
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
    let admission = Admission::new(limits, log.claim_sql_pool()?, 2, log.performance());
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
