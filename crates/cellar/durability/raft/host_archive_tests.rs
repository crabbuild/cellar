// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#![allow(clippy::disallowed_methods)]
use super::{next_scope, PhysicalBudget, RaftArchiveOptions};
use crate::raft::performance::{Operation, Registry};
use cellar_logic::consensus::{ClusterId, GroupId, GroupScope, IncarnationId, NamespaceId};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, watch};

async fn physical_fixture() -> anyhow::Result<(
    crate::asyncrt::ExecutionDomain,
    cellar_ltx::LtxHost,
    Arc<Registry>,
)> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let (token, performance) = domain
        .run(async {
            let token = crate::asyncrt::services().domain_token().unwrap();
            let performance = Registry::new(token.clone());
            (token, performance)
        })
        .await?;
    let host = cellar_ltx::LtxHost::new(
        || 0,
        |_| Err(std::io::ErrorKind::Unsupported.into()),
        |_| async { Err(std::io::ErrorKind::Unsupported.into()) },
        move |job| {
            let token = token.clone();
            async move {
                token
                    .blocking(job)
                    .map_err(|error| cellar_ltx::HostTaskError::new(error.to_string()))?
                    .await
                    .map_err(|error| cellar_ltx::HostTaskError::new(error.to_string()))
            }
        },
    );
    Ok((domain, host, performance))
}

async fn wait_physical_admissions(performance: &Arc<Registry>, count: u64) -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if performance.snapshot().operations[Operation::ArchivePhysicalAdmission as usize]
                .started
                >= count
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;
    Ok(())
}

type PhysicalTask = tokio::task::JoinHandle<Result<i32, cellar_ltx::HostTaskError>>;

async fn held_physical_job(
    host: cellar_ltx::LtxHost,
) -> anyhow::Result<(PhysicalTask, std::sync::mpsc::SyncSender<()>)> {
    let (entered, started) = oneshot::channel();
    let (release, held) = std::sync::mpsc::sync_channel(1);
    let task = tokio::spawn(async move {
        host.run_blocking(move || {
            let _ = entered.send(());
            let _ = held.recv();
            41
        })
        .await
    });
    tokio::time::timeout(Duration::from_secs(5), started).await??;
    Ok((task, release))
}

#[tokio::test]
async fn node_wide_physical_limit_leaves_foreground_host_unqueued() -> anyhow::Result<()> {
    let (domain, foreground, performance) = physical_fixture().await?;
    let (_stop, cancellation) = watch::channel(false);
    let budget = PhysicalBudget::new(1, cancellation, performance.clone())?;
    let first_host = budget.host(foreground.clone());
    let other_cell_host = budget.host(foreground.clone());
    assert!(Arc::ptr_eq(
        &foreground.filesystem(),
        &first_host.filesystem()
    ));
    let (first, release) = held_physical_job(first_host).await?;
    let second = tokio::spawn(async move { other_cell_host.run_blocking(|| 42).await });
    wait_physical_admissions(&performance, 2).await?;
    let snapshot = performance.snapshot();
    assert_eq!(
        snapshot.operations[Operation::ArchivePhysicalAdmission as usize].succeeded,
        1,
        "another cell's archive bypassed the full node-wide physical limit"
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), foreground.run_blocking(|| 7)).await??,
        7
    );
    release.send(())?;
    assert_eq!(first.await??, 41);
    assert_eq!(second.await??, 42);
    assert_eq!(budget.slots.available_permits(), 1);
    let snapshot = performance.snapshot();
    for operation in [
        Operation::ArchivePhysicalAdmission,
        Operation::ArchivePhysicalQueue,
    ] {
        assert_eq!(snapshot.operations[operation as usize].succeeded, 2);
    }
    domain.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn physical_shutdown_refuses_waiters_without_refunding_entered_work() -> anyhow::Result<()> {
    let (domain, foreground, performance) = physical_fixture().await?;
    let (stop, cancellation) = watch::channel(false);
    let budget = PhysicalBudget::new(1, cancellation, performance.clone())?;
    let (first, release) = held_physical_job(budget.host(foreground.clone())).await?;
    let queued_host = budget.host(foreground.clone());
    let queued_ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let ran = queued_ran.clone();
    let second = tokio::spawn(async move {
        queued_host
            .run_blocking(move || ran.store(true, std::sync::atomic::Ordering::Release))
            .await
    });
    wait_physical_admissions(&performance, 2).await?;
    stop.send_replace(true);
    assert!(tokio::time::timeout(Duration::from_secs(5), second)
        .await??
        .is_err());
    assert!(!queued_ran.load(std::sync::atomic::Ordering::Acquire));
    assert_eq!(budget.slots.available_permits(), 0);
    assert!(budget
        .host(foreground.clone())
        .run_blocking(|| 0)
        .await
        .is_err());
    assert_eq!(foreground.run_blocking(|| 7).await?, 7);
    release.send(())?;
    assert_eq!(first.await??, 41);
    assert_eq!(budget.slots.available_permits(), 1);
    let snapshot = performance.snapshot();
    let admission = &snapshot.operations[Operation::ArchivePhysicalAdmission as usize];
    assert_eq!(
        (admission.started, admission.succeeded, admission.failed),
        (3, 1, 2)
    );
    domain.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn canceled_waiter_keeps_physical_work_charged_until_domain_shutdown_joins_it(
) -> anyhow::Result<()> {
    let (domain, foreground, performance) = physical_fixture().await?;
    let (stop, cancellation) = watch::channel(false);
    let budget = PhysicalBudget::new(1, cancellation, performance.clone())?;
    let (first, release) = held_physical_job(budget.host(foreground.clone())).await?;
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    assert_eq!(budget.slots.available_permits(), 0);
    let queued_host = budget.host(foreground);
    let second = tokio::spawn(async move { queued_host.run_blocking(|| 42).await });
    wait_physical_admissions(&performance, 2).await?;
    stop.send_replace(true);
    assert!(tokio::time::timeout(Duration::from_secs(5), second)
        .await??
        .is_err());
    let (closing, closed) = oneshot::channel();
    let mut shutdown = tokio::spawn(async move {
        domain.close();
        let _ = closing.send(());
        domain.shutdown().await;
    });
    tokio::time::timeout(Duration::from_secs(5), closed).await??;
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut shutdown)
            .await
            .is_err(),
        "shutdown completed before entered physical work joined"
    );
    assert_eq!(budget.slots.available_permits(), 0);
    release.send(())?;
    tokio::time::timeout(Duration::from_secs(5), shutdown).await??;
    assert_eq!(budget.slots.available_permits(), 1);
    Ok(())
}

#[tokio::test]
async fn dropped_archive_owner_cancels_queued_jobs_and_nodes_have_independent_limits(
) -> anyhow::Result<()> {
    let (domain, foreground, performance) = physical_fixture().await?;
    let (owner, cancellation) = watch::channel(false);
    let budget = PhysicalBudget::new(1, cancellation, performance.clone())?;
    let (_other_owner, cancellation) = watch::channel(false);
    let other_node = PhysicalBudget::new(1, cancellation, performance.clone())?;
    let (first, release) = held_physical_job(budget.host(foreground.clone())).await?;
    assert_eq!(
        other_node
            .host(foreground.clone())
            .run_blocking(|| 7)
            .await?,
        7
    );
    let queued_host = budget.host(foreground);
    let second = tokio::spawn(async move { queued_host.run_blocking(|| 42).await });
    wait_physical_admissions(&performance, 3).await?;
    drop(owner);
    assert!(tokio::time::timeout(Duration::from_secs(5), second)
        .await??
        .is_err());
    assert_eq!(budget.slots.available_permits(), 0);
    release.send(())?;
    assert_eq!(first.await??, 41);
    assert_eq!(budget.slots.available_permits(), 1);
    domain.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn failed_physical_job_releases_its_slot_after_the_owner_returns() -> anyhow::Result<()> {
    let (domain, foreground, performance) = physical_fixture().await?;
    let (_owner, cancellation) = watch::channel(false);
    let budget = PhysicalBudget::new(1, cancellation, performance)?;
    let archive = budget.host(foreground);
    assert!(archive
        .run_blocking(|| panic!("injected physical task failure"))
        .await
        .is_err());
    assert_eq!(budget.slots.available_permits(), 1);
    assert_eq!(archive.run_blocking(|| 42).await?, 42);
    domain.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn abandoned_admission_never_dispatches_the_job_or_leaks_a_slot() -> anyhow::Result<()> {
    let (domain, foreground, performance) = physical_fixture().await?;
    let (_owner, cancellation) = watch::channel(false);
    let budget = PhysicalBudget::new(1, cancellation, performance.clone())?;
    let (first, release) = held_physical_job(budget.host(foreground.clone())).await?;
    let queued_host = budget.host(foreground.clone());
    let second = tokio::spawn(async move {
        queued_host
            .run_blocking(|| panic!("cancelled admission ran"))
            .await
    });
    wait_physical_admissions(&performance, 2).await?;
    second.abort();
    assert!(second.await.unwrap_err().is_cancelled());
    assert_eq!(budget.slots.available_permits(), 0);
    let snapshot = performance.snapshot();
    assert_eq!(
        snapshot.operations[Operation::ArchivePhysicalAdmission as usize].abandoned,
        1
    );
    assert_eq!(
        snapshot.operations[Operation::ArchivePhysicalQueue as usize].started,
        1
    );
    release.send(())?;
    assert_eq!(first.await??, 41);
    assert_eq!(budget.host(foreground).run_blocking(|| 42).await?, 42);
    assert_eq!(budget.slots.available_permits(), 1);
    domain.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn executor_queue_owns_the_slot_after_its_waiter_is_aborted() -> anyhow::Result<()> {
    type Submitted = (Box<dyn FnOnce() + Send>, oneshot::Sender<()>);
    let (domain, foreground, performance) = physical_fixture().await?;
    let (_owner, cancellation) = watch::channel(false);
    let budget = PhysicalBudget::new(1, cancellation, performance.clone())?;
    let (submitted, mut jobs) = tokio::sync::mpsc::unbounded_channel::<Submitted>();
    let executor = cellar_ltx::LtxHost::new(
        || 0,
        |_| Err(std::io::ErrorKind::Unsupported.into()),
        |_| async { Err(std::io::ErrorKind::Unsupported.into()) },
        move |job| {
            let submitted = submitted.clone();
            async move {
                let (done, joined) = oneshot::channel();
                submitted
                    .send((job, done))
                    .map_err(|_| cellar_ltx::HostTaskError::new("test executor closed"))?;
                joined
                    .await
                    .map_err(|_| cellar_ltx::HostTaskError::new("test executor stopped"))
            }
        },
    );
    let host = budget.host(executor);
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let work_calls = calls.clone();
    let waiting = tokio::spawn(async move {
        host.run_blocking(move || work_calls.fetch_add(1, std::sync::atomic::Ordering::AcqRel))
            .await
    });
    let (job, done) = tokio::time::timeout(Duration::from_secs(5), jobs.recv())
        .await?
        .unwrap();
    waiting.abort();
    assert!(waiting.await.unwrap_err().is_cancelled());
    assert_eq!(budget.slots.available_permits(), 0);
    let snapshot = performance.snapshot();
    let queued = &snapshot.operations[Operation::ArchivePhysicalQueue as usize];
    assert_eq!(
        (queued.started, queued.succeeded, queued.abandoned),
        (1, 0, 0)
    );
    foreground
        .run_blocking(move || {
            job();
            let _ = done.send(());
        })
        .await?;
    assert_eq!(calls.load(std::sync::atomic::Ordering::Acquire), 1);
    assert_eq!(budget.slots.available_permits(), 1);
    let snapshot = performance.snapshot();
    let queued = &snapshot.operations[Operation::ArchivePhysicalQueue as usize];
    assert_eq!(
        (queued.started, queued.succeeded, queued.abandoned),
        (1, 1, 0)
    );
    domain.shutdown().await;
    Ok(())
}

#[test]
fn archive_physical_job_admission_is_explicit_and_bounded() {
    for max_blocking_jobs in [0, 5, usize::MAX] {
        assert!(RaftArchiveOptions {
            max_blocking_jobs,
            ..Default::default()
        }
        .validate()
        .is_err());
    }
    for max_blocking_jobs in 1..=4 {
        assert!(RaftArchiveOptions {
            max_blocking_jobs,
            ..Default::default()
        }
        .validate()
        .is_ok());
    }
}

fn scope(index: u16) -> GroupScope {
    GroupScope {
        cluster: ClusterId::new([1; 16]).unwrap(),
        namespace: NamespaceId::new([2; 16]).unwrap(),
        cell: format!("Counter:{index:04}"),
        group: GroupId::new([3; 16]).unwrap(),
        incarnation: IncarnationId::new([4; 16]).unwrap(),
    }
}

#[test]
fn archive_selection_does_not_inspect_cells_before_the_cursor() {
    let cells = (0..1500)
        .map(|i| (scope(i), ()))
        .collect::<BTreeMap<_, _>>();
    let cursor = scope(1200);
    let mut inspected = Vec::new();
    let selected = next_scope(&cells, Some(&cursor), |key, _| {
        inspected.push(key.clone());
        true
    });
    assert_eq!(selected, Some(&scope(1201)));
    assert_eq!(
        inspected.len(),
        1,
        "eligibility scanned the preceding cells"
    );
    assert_eq!(inspected, vec![scope(1201)]);
}

#[test]
fn archive_selection_wraps_once_and_preserves_cyclic_eligibility() {
    let cells = (0..6).map(|i| (scope(i), ())).collect::<BTreeMap<_, _>>();
    for cursor in [
        None,
        Some(scope(0)),
        Some(scope(3)),
        Some(scope(5)),
        Some(scope(9)),
    ] {
        for mask in 0..64 {
            let expected = cells
                .keys()
                .filter(|key| cursor.as_ref().is_none_or(|cursor| *key > cursor))
                .chain(
                    cells
                        .keys()
                        .filter(|key| cursor.as_ref().is_some_and(|cursor| *key <= cursor)),
                )
                .find(|key| {
                    let index: u16 = key.cell[8..].parse().unwrap();
                    mask & (1 << index) != 0
                });
            let mut inspected = Vec::new();
            let selected = next_scope(&cells, cursor.as_ref(), |key, _| {
                inspected.push(key.clone());
                let index: u16 = key.cell[8..].parse().unwrap();
                mask & (1 << index) != 0
            });
            assert_eq!(selected, expected);
            let mut unique = inspected.clone();
            unique.sort();
            unique.dedup();
            assert_eq!(inspected.len(), unique.len(), "a cell was inspected twice");
        }
    }
    assert!(next_scope::<()>(&BTreeMap::new(), None, |_, _| panic!("empty map")).is_none());
}

#[test]
fn archive_producers_require_a_bounded_nonempty_window() {
    for max_producers in [0, 5, usize::MAX] {
        assert!(RaftArchiveOptions {
            max_producers,
            ..Default::default()
        }
        .validate()
        .is_err());
    }
    for max_producers in 1..=4 {
        assert!(RaftArchiveOptions {
            max_producers,
            ..Default::default()
        }
        .validate()
        .is_ok());
    }
}

#[test]
fn publication_scheduling_grace_is_bounded() {
    for publication_stale_ms in [0, 99, 60_001, u64::MAX] {
        assert!(RaftArchiveOptions {
            publication_stale_ms,
            ..Default::default()
        }
        .validate()
        .is_err());
    }
    for publication_stale_ms in [100, 5000, 60_000] {
        assert!(RaftArchiveOptions {
            publication_stale_ms,
            ..Default::default()
        }
        .validate()
        .is_ok());
    }
}

#[test]
fn publication_owner_distinguishes_node_disk_and_process() {
    use cellar_logic::consensus::{DiskLineage, NodeId, NodeIdentity, ProcessGeneration};
    let identity = NodeIdentity {
        node: NodeId::new([1; 16]).unwrap(),
        disk: DiskLineage::new([2; 16]).unwrap(),
        process: ProcessGeneration::new([3; 16]).unwrap(),
    };
    let owner = super::publication_owner(identity);
    assert_ne!(owner, [0; 32]);
    for changed in [
        NodeIdentity {
            node: NodeId::new([4; 16]).unwrap(),
            ..identity
        },
        NodeIdentity {
            disk: DiskLineage::new([4; 16]).unwrap(),
            ..identity
        },
        NodeIdentity {
            process: ProcessGeneration::new([4; 16]).unwrap(),
            ..identity
        },
    ] {
        assert_ne!(owner, super::publication_owner(changed));
    }
}
