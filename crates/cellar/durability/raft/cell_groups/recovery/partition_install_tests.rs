// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Consumed import preparation must join before its completion becomes ready.
//! This tests the engine boundary; catalog and RF3 serving grants are separate.

use super::*;
use crate::asyncrt::ExecutionDomain;
use crate::raft::{
    catalog::TransferSeedPermit,
    options::{LogOpenMode, ReplicaLogOptions},
    storage::{CellGroupBootstrap, ReplicaLog},
    storage_fault as fault,
};
use anyhow::ensure;
use cellar_logic::consensus::ProposalId;
use cellar_ltx::generation::{GenerationStore, ReadServices};
use std::future::Future;
use std::sync::atomic::AtomicUsize;
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;
use tokio::sync::Notify;

struct CompletionWake {
    recovery: CellGroupRecoveryPool,
    observed_operations: AtomicUsize,
    observed_bytes: AtomicUsize,
    wakes: AtomicUsize,
    changed: Notify,
}

impl Wake for CompletionWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        // oneshot::Sender notifies synchronously. Capture the state at that
        // boundary, before the engine can continue beyond reply.send().
        let usage = self.recovery.usage();
        if self
            .observed_operations
            .compare_exchange(
                usize::MAX,
                usage.operations,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            self.observed_bytes
                .store(usage.retained_bytes, Ordering::Release);
        }
        self.wakes.fetch_add(1, Ordering::AcqRel);
        self.changed.notify_one();
    }
}

fn empty_preparation(
    root: &std::path::Path,
    route: GroupRoute,
    recovery: &CellGroupRecoveryPool,
) -> anyhow::Result<PreparedCellGroupTransferChild> {
    let files = Arc::new(cellar_ltx::DirectFileSystem);
    let pages = ManifestPageStore::open(&root.join("pages"), files.clone(), 128, 1024 * 1024, 16)?;
    let images = CellGroupApplyImages::Materialized {
        images: GenerationStore::open(root.join("images"), files, Default::default())?,
        services: ReadServices {
            now_unix_millis: Arc::new(|| 1),
            fill_random: Arc::new(|bytes| bytes.fill(17)),
        },
    };
    let store = CellGroupArchiveStore::new(
        route.clone(),
        pages,
        images,
        Default::default(),
        Default::default(),
        100,
    )?;
    let marker = GroupManifest::transfer_genesis(
        route,
        ManifestRoot::default(),
        GroupTransferOrigin::new(ProposalId::new([28; 16])?, [29; 32], [30; 32]),
    )?;
    let mut prepared = {
        let mut physical = store.storage.lock().unwrap();
        let apply::Storage { tree, pages, .. } = &mut *physical;
        tree.prepare_archive_complete(&store.owner, pages, marker, &mut |_| {
            Err(ManifestError::Invalid("empty transfer has no image"))
        })?
    };
    // The production partitioner installs this exact ownership relation.
    prepared.recovery_admission = Some(recovery.admit()?);
    Ok(PreparedCellGroupTransferChild {
        data: ChildData::Sealed {
            genesis: Box::new(prepared),
            engine: Arc::new(OnceLock::new()),
        },
        store,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn imported_seed_releases_its_slot_before_completion_wakes() -> anyhow::Result<()> {
    use cellar_logic::{cell_group::*, consensus::*};

    let mut root = tempfile::tempdir()?;
    root.disable_cleanup(true);
    eprintln!("retained import completion disk: {}", root.path().display());
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let gate = fault::Gate::new(None);
    let files = Arc::new(fault::FaultFileSystem { gate: gate.clone() });
    let mut options = ReplicaLogOptions::new(
        root.path().join("engine"),
        NodeId::new([21; 16])?,
        DiskLineage::new([22; 16])?,
        ProcessGeneration::new([23; 16])?,
        LogOpenMode::Create,
    );
    let log = domain
        .run(ReplicaLog::open(options.clone(), files.clone()))
        .await??;
    let recovery = CellGroupRecoveryPool::new(Default::default())?;
    let observation = Arc::new(CompletionWake {
        recovery: recovery.clone(),
        observed_operations: AtomicUsize::new(usize::MAX),
        observed_bytes: AtomicUsize::new(usize::MAX),
        wakes: AtomicUsize::new(0),
        changed: Notify::new(),
    });
    let work = async {
        let route = GroupRoute {
            history: GroupHistory {
                cluster: ClusterId::new([24; 16])?,
                namespace: NamespaceId::new([25; 16])?,
                group: GroupId::new([26; 16])?,
                incarnation: IncarnationId::new([27; 16])?,
            },
            range: CellRange::all(),
            epoch: RouteEpoch::new(2)?,
            birth_digest: [31; 32],
            schema_digest: [32; 32],
        };
        let preparing_directory = root.path().to_path_buf();
        let preparing_route = route.clone();
        let preparing_recovery = recovery.clone();
        let child = domain
            .blocking(move || {
                empty_preparation(&preparing_directory, preparing_route, &preparing_recovery)
            })?
            .await??;
        let marker = child.genesis().unwrap().marker().clone();
        let permit = TransferSeedPermit::for_unmanaged_engine_test(
            &log,
            CellGroupBootstrap {
                route: route.clone(),
                member: MemberId::new(1)?,
                voters: (1..=3)
                    .map(MemberId::new)
                    .collect::<std::result::Result<_, _>>()?,
                learners: vec![],
            },
            marker.clone(),
        )?;
        let seed = TransferSeed::new(permit, &child)?;
        drop(child);
        let charged = recovery.usage();
        ensure!(charged.operations == 1 && charged.retained_bytes > 0);
        let release = gate.arm(fault::HOLD_SYNC);
        let mut install = Box::pin(log.install_transfer_seed(seed));
        let waker = Waker::from(observation.clone());
        ensure!(matches!(
            install.as_mut().poll(&mut Context::from_waker(&waker)),
            Poll::Pending
        ));
        domain
            .run(crate::asyncrt::timeout(
                Duration::from_secs(5),
                gate.entered.notified(),
            ))
            .await??;
        ensure!(matches!(
            log.cell_group(&route.history),
            Err(crate::raft::storage::LogError::NotFound)
        ));
        ensure!(
            recovery.usage().operations == 1
                && recovery.usage().retained_bytes == charged.retained_bytes
        );
        ensure!(
            observation.wakes.load(Ordering::Acquire) == 0
                && observation.observed_operations.load(Ordering::Acquire) == usize::MAX
                && observation.observed_bytes.load(Ordering::Acquire) == usize::MAX,
            "completion woke before the held engine sync completed"
        );
        drop(release);
        domain
            .run(crate::asyncrt::timeout(
                Duration::from_secs(5),
                observation.changed.notified(),
            ))
            .await??;
        let group = install.await?;
        ensure!(observation.wakes.load(Ordering::Acquire) == 1);
        ensure!(group.group_manifest()? == Some(marker.clone()));
        ensure!(!marker.is_active());
        Ok::<_, anyhow::Error>((route.history, marker, charged.retained_bytes))
    }
    .await;
    let closed = log.close().await;
    let joined_usage = recovery.usage();
    let released = joined_usage.operations == 0 && joined_usage.retained_bytes == 0;
    let reopened = async {
        let (history, marker, _) = work.as_ref().map_err(|e| anyhow::anyhow!("{e:#}"))?;
        options.mode = LogOpenMode::Resume;
        options.process = ProcessGeneration::new([33; 16])?;
        let resumed = domain.run(ReplicaLog::open(options, files)).await??;
        let selected = resumed.cell_group(history)?.group_manifest()?;
        let closed = resumed.close().await;
        ensure!(selected.as_ref() == Some(marker) && !marker.is_active());
        closed?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    domain.shutdown().await;
    let observed = observation.observed_operations.load(Ordering::Acquire);
    let observed_bytes = observation.observed_bytes.load(Ordering::Acquire);
    let charged_bytes = work.as_ref().ok().map(|(_, _, bytes)| *bytes);
    eprintln!(
        "IMPORT_COMPLETION_OBSERVATION {}",
        serde_json::json!({"operationsAtWake":observed,"retainedBytesAtWake":observed_bytes,"heldBytesBeforeSync":charged_bytes,"wakes":observation.wakes.load(Ordering::Acquire),"preSyncAndMappingVerified":work.is_ok(),"noPrematureWakeVerified":work.is_ok(),"joinedClose":closed.is_ok(),"joinedSlotReleased":released,"durableReopenVerified":reopened.is_ok()})
    );
    work?;
    closed?;
    reopened?;
    ensure!(released, "joined import retained recovery admission");
    ensure!(
        observed == 0 && observed_bytes == 0,
        "completed import woke its caller while retaining {observed} recovery slot(s) and {observed_bytes} bytes"
    );
    Ok(())
}
