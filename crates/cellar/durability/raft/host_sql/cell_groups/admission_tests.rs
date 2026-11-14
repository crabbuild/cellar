// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::raft::{
    options::{LogOpenMode, ReplicaLogOptions},
    storage::ReplicaLog,
};
use cellar_logic::consensus::{DiskLineage, GroupId, IncarnationId, NodeId, ProcessGeneration};
use futures_util::FutureExt;

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

#[tokio::test]
#[allow(clippy::disallowed_methods)]
async fn queued_writers_precede_callback_resume_and_cancel_without_refunding_physical_work(
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
    let admission = Admission::new(
        CellSqlLimits {
            workers: 1,
            queue_per_worker: 4,
            max_pending_cells: 4,
            group_pending_cells: 3,
            node_bytes: 16_384,
            group_bytes: 8192,
        },
        log.claim_sql_pool()?,
        2,
    );
    let group = GroupHistory {
        cluster: ClusterId::new([4; 16])?,
        namespace: NamespaceId::new([5; 16])?,
        group: GroupId::new([6; 16])?,
        incarnation: IncarnationId::new([7; 16])?,
    };
    let cell = CellHistory {
        cluster: group.cluster,
        namespace: group.namespace,
        cell: CellId::new("workflow:one")?,
        incarnation: group.incarnation,
    };
    let (mut callback, physical) = admission.reserve(&cell, group, 1024)?;
    let mut control = Box::pin(admission.reserve_queued(&cell, group, 1024));
    assert!(control.as_mut().now_or_never().is_none());
    assert!(callback.contended());
    assert_eq!(admission.usage()?, (1, 2048, 2));
    // Queued requests consume exactly the ordinary item and byte budgets.
    let mut cancelled = Box::pin(admission.reserve_queued(&cell, group, 1024));
    assert!(cancelled.as_mut().now_or_never().is_none());
    assert!(matches!(
        admission.reserve_read(group, 1024),
        Err(ExecutionError::Refused(CommandFailure::Overloaded))
    ));
    drop(cancelled);
    assert_eq!(admission.usage()?, (1, 2048, 2));
    callback.park()?;
    // A ready callback cannot jump ahead of the control already in the FIFO.
    let mut resumed = Box::pin(std::future::poll_fn(|cx| callback.resume(cx)));
    assert!(resumed.as_mut().now_or_never().is_none());
    assert!(matches!(
        admission.reserve(&cell, group, 1024),
        Err(ExecutionError::Busy)
    ));
    let (control, control_bytes) = control.await?;
    assert!(resumed.as_mut().now_or_never().is_none());
    drop(control);
    resumed.await?;
    drop(control_bytes);
    assert_eq!(admission.usage()?, (1, 1024, 1));
    // Closing wakes a queued writer and refuses late requests; it does not
    // refund a callback or its retained output before physical ownership ends.
    let mut waiting = Box::pin(admission.reserve_queued(&cell, group, 1024));
    assert!(waiting.as_mut().now_or_never().is_none());
    admission.close();
    assert!(matches!(
        waiting.await,
        Err(ExecutionError::Refused(CommandFailure::Unavailable))
    ));
    assert!(matches!(
        admission.reserve_read(group, 1024),
        Err(ExecutionError::Refused(CommandFailure::Unavailable))
    ));
    assert_eq!(admission.usage()?, (1, 1024, 1));
    drop(callback);
    assert_eq!(admission.usage()?, (0, 1024, 1));
    assert!(admission.state.lock().unwrap().writers.is_empty());
    drop(physical);
    assert_eq!(admission.usage()?, (0, 0, 0));
    drop(admission);
    log.close().await?;
    domain.shutdown().await;
    Ok(())
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
    assert!(std::future::poll_fn(|cx| first.resume(cx))
        .now_or_never()
        .is_none());
    drop(second);
    domain
        .run(crate::asyncrt::timeout(
            std::time::Duration::from_secs(1),
            progress.changed(),
        ))
        .await???;
    std::future::poll_fn(|cx| first.resume(cx)).await?;
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
