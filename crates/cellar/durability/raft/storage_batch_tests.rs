// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::asyncrt::ExecutionDomain;
use crate::raft::storage_fault as fault;
use ::raft::Storage as _;
use cellar_logic::{cell_group::*, consensus::*};

#[path = "storage_batch_test_fs.rs"]
mod filesystem;

fn bootstrap(id: u8) -> CellGroupBootstrap {
    CellGroupBootstrap {
        route: GroupRoute {
            history: GroupHistory {
                cluster: ClusterId::new([1; 16]).unwrap(),
                namespace: NamespaceId::new([2; 16]).unwrap(),
                group: GroupId::new([id; 16]).unwrap(),
                incarnation: IncarnationId::new([4; 16]).unwrap(),
            },
            range: CellRange::all(),
            epoch: RouteEpoch::new(1).unwrap(),
            birth_digest: [9; 32],
            schema_digest: [8; 32],
        },
        member: MemberId::new(1).unwrap(),
        voters: vec![MemberId::new(1).unwrap()],
        learners: vec![],
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queued_disjoint_cell_groups_share_one_physical_fence() -> anyhow::Result<()> {
    let mut root = tempfile::tempdir()?;
    root.disable_cleanup(true);
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let gate = fault::Gate::new(None);
    let barriers = Arc::new(filesystem::Barriers::default());
    let filesystem = Arc::new(filesystem::Filesystem {
        barriers: barriers.clone(),
        inner: Arc::new(fault::FaultFileSystem { gate: gate.clone() }),
    });
    let mut options = ReplicaLogOptions::new(
        root.path().join("log"),
        NodeId::new([1; 16])?,
        DiskLineage::new([2; 16])?,
        ProcessGeneration::new([3; 16])?,
        LogOpenMode::Create,
    );
    options.purge_interval_ms = None;
    let log = domain.run(ReplicaLog::open(options, filesystem)).await??;
    let mut groups = Vec::new();
    for id in 5..=7 {
        groups.push(log.create_cell_group(bootstrap(id)).await?);
    }
    // Queue all three requests behind a real held engine fence. No timer or
    // batch-fill delay is used to make the writes happen to arrive together.
    let release = gate.arm(fault::HOLD_SYNC);
    let owned = log.clone();
    let leading = domain.spawn(async move { owned.reserve_node_revision().await })?;
    domain
        .run(crate::asyncrt::timeout(
            Duration::from_secs(5),
            gate.entered.notified(),
        ))
        .await??;
    barriers.reset();
    let mut pending = Vec::new();
    for (slot, group) in groups.iter().enumerate() {
        let mut write = GroupWrite::new(group);
        let mut hard = group.initial_state()?.hard_state;
        hard.term = 2;
        hard.vote = 1;
        write.hard_state = Some(hard);
        let mut request =
            Box::pin(log.persist_cells(OperationId::new(slot as u64 + 2), vec![write]));
        assert!(futures_util::poll!(request.as_mut()).is_pending());
        pending.push(request);
    }
    let held_invisible = groups
        .iter()
        .all(|group| group.initial_state().unwrap().hard_state.term == 1);
    drop(release);
    leading.await??;
    let results = futures_util::future::join_all(pending).await;
    let mut sequences = Vec::new();
    for (slot, result) in results.into_iter().enumerate() {
        let receipt = result?;
        assert_eq!(receipt.operation, OperationId::new(slot as u64 + 2));
        assert_eq!(receipt.groups, vec![groups[slot].identity().clone()]);
        assert_eq!(groups[slot].initial_state()?.hard_state.term, 2);
        sequences.push(receipt.sequence);
    }
    let observed = barriers.counts();
    log.close().await?;
    domain.shutdown().await;
    eprintln!("CELLAR_PERSIST_BATCH {{\"requests\":3,\"fileSyncs\":{},\"directorySyncs\":{},\"queuedHeldInvisible\":{held_invisible},\"exactReceipts\":true,\"joined\":true}}", observed.0, observed.1);
    assert!(
        held_invisible,
        "queued persistence escaped a held physical fence"
    );
    assert_eq!(
        observed,
        (1, 0),
        "already-queued disjoint groups must share one physical fence"
    );
    assert!(sequences.iter().all(|sequence| *sequence == sequences[0]));
    Ok(())
}

struct Fixture {
    _root: tempfile::TempDir,
    domain: ExecutionDomain,
    log: ReplicaLog,
    gate: Arc<fault::Gate>,
    barriers: Arc<filesystem::Barriers>,
    groups: Vec<GroupLog<GroupHistory>>,
}

impl Fixture {
    async fn new(queue_capacity: usize, batch_bytes: usize) -> anyhow::Result<Self> {
        let mut root = tempfile::tempdir()?;
        root.disable_cleanup(true);
        let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
        let gate = fault::Gate::new(None);
        let barriers = Arc::new(filesystem::Barriers::default());
        let filesystem = Arc::new(filesystem::Filesystem {
            barriers: barriers.clone(),
            inner: Arc::new(fault::FaultFileSystem { gate: gate.clone() }),
        });
        let mut options = ReplicaLogOptions::new(
            root.path().join("log"),
            NodeId::new([1; 16])?,
            DiskLineage::new([2; 16])?,
            ProcessGeneration::new([3; 16])?,
            LogOpenMode::Create,
        );
        options.queue_capacity = queue_capacity;
        options.max_batch_bytes = batch_bytes;
        options.purge_interval_ms = None;
        let log = domain.run(ReplicaLog::open(options, filesystem)).await??;
        let mut groups = Vec::new();
        for id in 5..=7 {
            groups.push(log.create_cell_group(bootstrap(id)).await?);
        }
        Ok(Self {
            _root: root,
            domain,
            log,
            gate,
            barriers,
            groups,
        })
    }

    fn write(&self, group: usize, term: u64) -> GroupWrite<GroupHistory> {
        let mut write = GroupWrite::new(&self.groups[group]);
        let mut hard = self.groups[group].initial_state().unwrap().hard_state;
        hard.term = term;
        hard.vote = 1;
        write.hard_state = Some(hard);
        write
    }

    fn terms(&self) -> Vec<u64> {
        self.groups
            .iter()
            .map(|group| group.initial_state().unwrap().hard_state.term)
            .collect()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coalesced_sync_keeps_payloads_and_items_after_waiter_abandonment() -> anyhow::Result<()> {
    let f = Fixture::new(3, 32 * 1024 * 1024).await?;
    let work = async {
        let release = f.gate.arm(fault::HOLD_SYNC);
        let owned = f.log.clone();
        let leading = f
            .domain
            .spawn(async move { owned.reserve_node_revision().await })?;
        f.domain
            .run(crate::asyncrt::timeout(
                Duration::from_secs(5),
                f.gate.entered.notified(),
            ))
            .await??;
        f.barriers.reset();
        let mut pending = Vec::new();
        for slot in 0..3 {
            let mut request = Box::pin(
                f.log
                    .persist_cells(OperationId::new(slot as u64 + 2), vec![f.write(slot, 2)]),
            );
            assert!(futures_util::poll!(request.as_mut()).is_pending());
            pending.push(request);
        }
        let (held, shared_release) = f.barriers.hold_next_sync();
        drop(release);
        leading.await??;
        f.domain
            .run(crate::asyncrt::timeout(
                Duration::from_secs(5),
                held.entered.notified(),
            ))
            .await??;
        let before_bytes = f.log.inner.pending.load(Ordering::Acquire);
        let before_items = f.log.inner.pending_items.load(Ordering::Acquire);
        let held_terms = f.terms();
        drop(pending.pop());
        let abandoned_bytes = f.log.inner.pending.load(Ordering::Acquire);
        let abandoned_items = f.log.inner.pending_items.load(Ordering::Acquire);
        let mut tail = Box::pin(f.log.reserve_node_revision());
        assert!(futures_util::poll!(tail.as_mut()).is_pending());
        let full_items = f.log.inner.pending_items.load(Ordering::Acquire);
        let mut rejected_waiter = Box::pin(f.log.reserve_node_revision());
        let rejected = futures_util::poll!(rejected_waiter.as_mut());
        drop(rejected_waiter);
        drop(shared_release);
        let results = futures_util::future::join_all(pending).await;
        let mut shared_sequence = None;
        for (slot, result) in results.into_iter().enumerate() {
            let receipt = result?;
            assert_eq!(receipt.operation, OperationId::new(slot as u64 + 2));
            assert_eq!(receipt.groups, vec![f.groups[slot].identity().clone()]);
            assert!(shared_sequence.is_none_or(|sequence| sequence == receipt.sequence));
            shared_sequence = Some(receipt.sequence);
        }
        let tail_sequence = tail.await?;
        Ok::<_, anyhow::Error>((
            before_bytes,
            before_items,
            abandoned_bytes,
            abandoned_items,
            held_terms,
            full_items,
            rejected,
            tail_sequence,
            shared_sequence.unwrap(),
            f.barriers.counts(),
            f.terms(),
        ))
    }
    .await;
    let closed = f.log.close().await;
    f.domain.shutdown().await;
    let (
        before_bytes,
        before_items,
        abandoned_bytes,
        abandoned_items,
        held_terms,
        full_items,
        rejected,
        tail_sequence,
        shared_sequence,
        barriers,
        terms,
    ) = work?;
    closed?;
    assert_eq!(held_terms, vec![1, 1, 1]);
    assert!(before_bytes > 0);
    assert_eq!(before_bytes, abandoned_bytes);
    assert_eq!((before_items, abandoned_items, full_items), (3, 3, 4));
    assert_eq!(rejected, std::task::Poll::Ready(Err(LogError::Overloaded)));
    assert!(tail_sequence > shared_sequence);
    assert_eq!(barriers, (2, 0));
    assert_eq!(terms, vec![2, 2, 2]);
    assert_eq!(f.log.inner.pending.load(Ordering::Acquire), 0);
    assert_eq!(f.log.inner.pending_items.load(Ordering::Acquire), 0);
    Ok(())
}

async fn separate_fences(
    groups: &[usize],
    terms: &[u64],
    batch_bytes: usize,
) -> anyhow::Result<()> {
    let f = Fixture::new(32, batch_bytes).await?;
    let work = async {
        let release = f.gate.arm(fault::HOLD_SYNC);
        let owned = f.log.clone();
        let leading = f
            .domain
            .spawn(async move { owned.reserve_node_revision().await })?;
        f.domain
            .run(crate::asyncrt::timeout(
                Duration::from_secs(5),
                f.gate.entered.notified(),
            ))
            .await??;
        f.barriers.reset();
        let mut pending = Vec::new();
        for (slot, (&group, &term)) in groups.iter().zip(terms).enumerate() {
            let mut request = Box::pin(f.log.persist_cells(
                OperationId::new(slot as u64 + 2),
                vec![f.write(group, term)],
            ));
            assert!(futures_util::poll!(request.as_mut()).is_pending());
            pending.push(request);
        }
        drop(release);
        leading.await??;
        let results = futures_util::future::join_all(pending).await;
        let mut previous = 0;
        for (slot, result) in results.into_iter().enumerate() {
            let receipt = result?;
            assert_eq!(receipt.operation, OperationId::new(slot as u64 + 2));
            assert_eq!(
                receipt.groups,
                vec![f.groups[groups[slot]].identity().clone()]
            );
            assert!(receipt.sequence > previous);
            previous = receipt.sequence;
        }
        Ok::<_, anyhow::Error>((f.barriers.counts(), f.terms()))
    }
    .await;
    let closed = f.log.close().await;
    f.domain.shutdown().await;
    let (barriers, final_terms) = work?;
    closed?;
    assert_eq!(barriers, (groups.len(), 0));
    for (&group, &term) in groups.iter().zip(terms).rev() {
        if !groups
            .iter()
            .zip(terms)
            .any(|(&other, &later)| other == group && later > term)
        {
            assert_eq!(final_terms[group], term);
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queued_overlapping_groups_keep_ordered_separate_fences() -> anyhow::Result<()> {
    separate_fences(&[0, 0], &[2, 3], 32 * 1024 * 1024).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coalescing_preserves_original_batch_byte_limit() -> anyhow::Result<()> {
    separate_fences(&[0, 1], &[2, 2], 3072).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serialized_overload_refuses_before_sync_or_cache_publication() -> anyhow::Result<()> {
    let f = Fixture::new(32, 4096).await?;
    f.barriers.reset();
    let result = f
        .log
        // Exercise the final worker bound independently of the earlier cost
        // estimate, which now correctly refuses this cohort before enqueue.
        .request(1024, |reply| {
            Work::PersistCells(
                OperationId::new(2),
                (0..3).map(|slot| f.write(slot, 2)).collect(),
                reply,
            )
        })
        .await;
    let observed = f.barriers.counts();
    let terms = f.terms();
    // A refused original request remains atomic. A later independent request
    // proves admission stays usable; this does not retry an unknown outcome.
    let later = f
        .log
        .persist_cells(OperationId::new(3), vec![f.write(0, 3)])
        .await;
    let closed = f.log.close().await;
    f.domain.shutdown().await;
    assert!(matches!(result, Err(LogError::Overloaded)));
    assert_eq!(observed, (0, 0));
    assert_eq!(terms, vec![1, 1, 1]);
    assert_eq!(f.log.inner.pending.load(Ordering::Acquire), 0);
    assert_eq!(f.log.inner.pending_items.load(Ordering::Acquire), 0);
    let receipt = later?;
    assert_eq!(receipt.operation, OperationId::new(3));
    assert_eq!(receipt.groups, vec![f.groups[0].identity().clone()]);
    closed?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_neighbor_does_not_refuse_other_group_writes() -> anyhow::Result<()> {
    let f = Fixture::new(32, 32 * 1024 * 1024).await?;
    let work = async {
        let release = f.gate.arm(fault::HOLD_SYNC);
        let owned = f.log.clone();
        let leading = f
            .domain
            .spawn(async move { owned.reserve_node_revision().await })?;
        f.domain
            .run(crate::asyncrt::timeout(
                Duration::from_secs(5),
                f.gate.entered.notified(),
            ))
            .await??;
        f.barriers.reset();
        let mut pending = Vec::new();
        for slot in 0..3 {
            let mut write = f.write(slot, 2);
            if slot == 1 {
                write.hard_state.as_mut().unwrap().commit = 2;
            }
            let mut request = Box::pin(
                f.log
                    .persist_cells(OperationId::new(slot as u64 + 2), vec![write]),
            );
            assert!(futures_util::poll!(request.as_mut()).is_pending());
            pending.push(request);
        }
        drop(release);
        leading.await??;
        let results = futures_util::future::join_all(pending).await;
        Ok::<_, anyhow::Error>((results, f.barriers.counts(), f.terms()))
    }
    .await;
    let closed = f.log.close().await;
    f.domain.shutdown().await;
    let (results, barriers, terms) = work?;
    closed?;
    let mut previous = 0;
    for (slot, result) in results.into_iter().enumerate() {
        if slot == 1 {
            assert!(matches!(result, Err(LogError::Invalid(_))));
            continue;
        }
        let receipt = result?;
        assert_eq!(receipt.operation, OperationId::new(slot as u64 + 2));
        assert_eq!(receipt.groups, vec![f.groups[slot].identity().clone()]);
        assert!(receipt.sequence > previous);
        previous = receipt.sequence;
    }
    assert_eq!(barriers, (2, 0));
    assert_eq!(terms, vec![2, 1, 2]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn uncertain_coalesced_sync_never_retries_or_publishes_cache() -> anyhow::Result<()> {
    let f = Fixture::new(32, 32 * 1024 * 1024).await?;
    let metadata_before: Vec<_> = f
        .groups
        .iter()
        .map(|group| group.metadata_bytes.load(Ordering::Acquire))
        .collect();
    let work = async {
        let release = f.gate.arm(fault::HOLD_SYNC);
        let owned = f.log.clone();
        let leading = f
            .domain
            .spawn(async move { owned.reserve_node_revision().await })?;
        f.domain
            .run(crate::asyncrt::timeout(
                Duration::from_secs(5),
                f.gate.entered.notified(),
            ))
            .await??;
        f.barriers.reset();
        let mut pending = Vec::new();
        for slot in 0..3 {
            let mut request = Box::pin(
                f.log
                    .persist_cells(OperationId::new(slot as u64 + 2), vec![f.write(slot, 2)]),
            );
            assert!(futures_util::poll!(request.as_mut()).is_pending());
            pending.push(request);
        }
        f.barriers
            .before_next_sync(|| Err(std::io::Error::other("injected shared engine sync failure")));
        drop(release);
        leading.await??;
        let results = futures_util::future::join_all(pending).await;
        Ok::<_, anyhow::Error>(results)
    }
    .await;
    let closed = f.log.close().await;
    f.domain.shutdown().await;
    let results = work?;
    // Include engine destruction in the no-retry assertion. A failed worker
    // can send its replies before the engine's file writers finish dropping.
    let barriers = f.barriers.counts();
    assert!(results
        .into_iter()
        .all(|result| matches!(result, Err(LogError::Io(_)))));
    assert!(matches!(closed, Err(LogError::Io(_))));
    assert_eq!(barriers, (1, 0));
    assert_eq!(f.barriers.after_failure.load(Ordering::Acquire), 0);
    // read() refuses after node close; the cache itself must still contain only
    // the prior durable state, never the failed cohort's candidate metadata.
    for (group, metadata_bytes) in f.groups.iter().zip(metadata_before) {
        assert_eq!(group.cache.read().unwrap().hard_state.term, 1);
        assert_eq!(group.metadata_bytes.load(Ordering::Acquire), metadata_bytes);
    }
    assert_eq!(f.log.inner.pending.load(Ordering::Acquire), 0);
    assert_eq!(f.log.inner.pending_items.load(Ordering::Acquire), 0);
    Ok(())
}
