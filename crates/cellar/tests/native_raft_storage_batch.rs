// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use cellar::sdk::consensus::verification::*;
use cellar::sdk::ExecutionDomain;
use std::sync::Arc;
use std::time::Duration;
#[path = "support/raft_storage_fault.rs"]
pub mod fault;
use ::raft::Storage as _;
use cellar_logic::cell_group::GroupRoute;
use cellar_logic::{cell_group::*, consensus::*};

#[path = "../durability/raft/storage_batch_test_fs.rs"]
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
#[allow(clippy::disallowed_methods)] // Host-side timeout for the real I/O fault gate.
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
        .run(tokio::time::timeout(
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
    let (shared, shared_release) = barriers.hold_next_sync();
    drop(release);
    leading.await??;
    domain
        .run(tokio::time::timeout(
            Duration::from_secs(5),
            shared.entered.notified(),
        ))
        .await??;
    let shared_invisible = groups
        .iter()
        .all(|group| group.initial_state().unwrap().hard_state.term == 1);
    drop(shared_release);
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
        shared_invisible,
        "candidate metadata escaped the shared physical sync"
    );
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::disallowed_methods)] // Host-side timeout for the real I/O fault gate.
async fn metadata_heavy_cohorts_obey_serialized_batch_budget() -> anyhow::Result<()> {
    let mut root = tempfile::tempdir()?;
    root.disable_cleanup(true);
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let gate = fault::Gate::new(None);
    let barriers = Arc::new(filesystem::Barriers::default());
    let fs = Arc::new(filesystem::Filesystem {
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
    options.max_batch_bytes = 6400;
    options.purge_interval_ms = None;
    let limit = options.max_batch_bytes;
    let log = domain.run(ReplicaLog::open(options, fs)).await??;
    let mut groups = Vec::new();
    // Valid maximum membership makes serialized cached metadata dominate the
    // tiny incoming HardState updates. No voter processes are started here.
    for id in 5..=7 {
        let mut initial = bootstrap(id);
        initial.voters = (1..=32).map(|id| MemberId::new(id).unwrap()).collect();
        groups.push(log.create_cell_group(initial).await?);
    }
    let work = async {
        let release = gate.arm(fault::HOLD_SYNC);
        let owned = log.clone();
        let leading = domain.spawn(async move { owned.reserve_node_revision().await })?;
        domain
            .run(tokio::time::timeout(
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
        Ok::<_, anyhow::Error>((
            barriers.counts(),
            barriers
                .max_write_bytes
                .load(std::sync::atomic::Ordering::SeqCst),
            sequences,
        ))
    }
    .await;
    let closed = log.close().await;
    domain.shutdown().await;
    let (observed, max_write_bytes, sequences) = work?;
    closed?;
    eprintln!("CELLAR_PERSIST_ENCODED_BUDGET {{\"requests\":3,\"votersPerGroup\":32,\"maxBatchBytes\":{limit},\"fileSyncs\":{},\"directorySyncs\":{},\"maxPhysicalWriteBytes\":{max_write_bytes},\"exactReceipts\":true,\"joined\":true}}", observed.0, observed.1);
    assert_eq!(
        observed,
        (3, 0),
        "metadata-heavy requests must split before exceeding the serialized batch budget"
    );
    assert!(
        max_write_bytes <= limit,
        "physical write exceeded the admitted serialized batch bound"
    );
    assert!(sequences.windows(2).all(|pair| pair[0] < pair[1]));
    Ok(())
}

#[tokio::test]
#[allow(clippy::disallowed_methods)] // Host-side bounded observation of real worker progress.
async fn metadata_heavy_ready_batches_make_durable_progress() -> anyhow::Result<()> {
    let mut root = tempfile::tempdir()?;
    root.disable_cleanup(true);
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let gate = fault::Gate::new(None);
    let barriers = Arc::new(filesystem::Barriers::default());
    let fs = Arc::new(filesystem::Filesystem {
        barriers: barriers.clone(),
        inner: Arc::new(fault::FaultFileSystem { gate }),
    });
    let mut options = ReplicaLogOptions::new(
        root.path().join("log"),
        NodeId::new([1; 16])?,
        DiskLineage::new([2; 16])?,
        ProcessGeneration::new([3; 16])?,
        LogOpenMode::Create,
    );
    options.max_batch_bytes = 3328;
    options.purge_interval_ms = None;
    let limit = options.max_batch_bytes;
    let log = domain.run(ReplicaLog::open(options, fs)).await??;
    let mut scopes = Vec::new();
    let mut groups = Vec::new();
    for id in 5..=6 {
        let scope = GroupScope {
            cluster: ClusterId::new([1; 16])?,
            namespace: NamespaceId::new([2; 16])?,
            cell: format!("Counter:{id}:{}", "x".repeat(1014)),
            group: GroupId::new([id; 16])?,
            incarnation: IncarnationId::new([4; 16])?,
        };
        assert_eq!(scope.cell.len(), 1024);
        groups.push(
            log.create_group(GroupBootstrap {
                scope: scope.clone(),
                member: MemberId::new(1)?,
                birth_digest: [9; 32],
                // Fits the unchanged runtime's default membership bound. These
                // are metadata voters, not additional processes or a benchmark.
                voters: (1..=7).map(|id| MemberId::new(id).unwrap()).collect(),
                learners: vec![],
            })
            .await?,
        );
        scopes.push(scope);
    }
    let (runtime, _events) = domain
        .run(RaftRuntime::start(
            log.clone(),
            RaftRuntimeOptions {
                tick_interval_ms: None,
                election_seed: Some([7; 32]),
                ..Default::default()
            },
        ))
        .await??;
    barriers.reset();
    let work = domain
        .run(async {
            // On this current-thread executor every future enters the mailbox
            // before the production worker runs. No batch-fill delay is introduced.
            futures_util::future::try_join_all(scopes.iter().map(|scope| {
                let mut message = ::raft::prelude::Message {
                    from: 2,
                    to: 1,
                    term: 2,
                    commit: 1,
                    ..Default::default()
                };
                message.set_msg_type(::raft::prelude::MessageType::MsgHeartbeat);
                runtime.step(scope, message)
            }))
            .await?;
            let progressed = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if groups.iter().all(|group| {
                        group
                            .initial_state()
                            .is_ok_and(|state| state.hard_state.term == 2)
                    }) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .is_ok();
            Ok::<_, anyhow::Error>((
                progressed,
                barriers.counts(),
                barriers
                    .max_write_bytes
                    .load(std::sync::atomic::Ordering::SeqCst),
            ))
        })
        .await;
    let runtime_closed = runtime.close().await;
    let log_closed = log.close().await;
    domain.shutdown().await;
    let (progressed, observed, max_write_bytes) = work??;
    eprintln!("CELLAR_READY_METADATA_BUDGET {{\"groups\":2,\"votersPerGroup\":7,\"cellKeyBytes\":1024,\"maxBatchBytes\":{limit},\"durableProgress\":{progressed},\"fileSyncs\":{},\"directorySyncs\":{},\"maxPhysicalWriteBytes\":{max_write_bytes},\"runtimeClosedOk\":{},\"joined\":true}}", observed.0, observed.1, runtime_closed.is_ok());
    assert!(
        progressed,
        "metadata-heavy Ready batches must not retry the same oversized cohort forever"
    );
    runtime_closed?;
    log_closed?;
    assert_eq!(observed, (2, 0));
    assert!(max_write_bytes <= limit);
    Ok(())
}
