// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Public node ownership across cells, closed registrations and joined restart.
#![allow(clippy::disallowed_methods)]
#[path = "support/raft_node.rs"]
#[allow(dead_code)]
mod public_node;

use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{consensus::*, Node, ShutdownMode};
use futures_util::TryStreamExt as _;
use object_store::ObjectStore as _;
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

fn registrations(schema: [u8; 32]) -> Result<Vec<RaftCellRegistration>> {
    let mut namespace = NamespaceConfig::new(
        ClusterId::new([1; 16])?,
        NamespaceId::new([2; 16])?,
        NamespaceMode::Raft,
        schema,
    );
    namespace.voters = VoterPolicy::SingleVoterDevelopment;
    namespace.image_storage = RaftImageStorage::Extents;
    (1..=2)
        .map(|index| {
            Ok(RaftCellRegistration {
                namespace: namespace.clone(),
                placement: RaftCellPlacement::Replica,
                class: "Counter".into(),
                birth: BirthProposal {
                    scope: GroupScope {
                        cluster: namespace.cluster,
                        namespace: namespace.namespace,
                        cell: format!("Counter:{index}"),
                        group: GroupId::new([index; 16])?,
                        incarnation: IncarnationId::new([3; 16])?,
                    },
                    voters: vec![InitialMember {
                        member: MemberId::new(1)?,
                        node: NodeId::new([1; 16])?,
                    }],
                    learners: vec![],
                },
            })
        })
        .collect()
}

fn host(max_roots: usize) -> RaftHostOptions {
    RaftHostOptions {
        max_groups: 2,
        checkpoint_entries: 1,
        extents: Some(RaftExtentOptions {
            inventory: RaftExtentLimits {
                max_roots,
                max_root_extents: 256,
                ..Default::default()
            },
            gc_interval_ms: 10,
            gc_batch_files: 2,
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[derive(Default)]
struct Callbacks {
    by_cell: Mutex<BTreeMap<String, usize>>,
    refused: AtomicUsize,
}

impl Callbacks {
    fn record(&self, scope: &GroupScope) {
        *self
            .by_cell
            .lock()
            .unwrap()
            .entry(scope.cell.clone())
            .or_default() += 1;
    }

    fn count(&self, scope: &GroupScope) -> usize {
        self.by_cell
            .lock()
            .unwrap()
            .get(&scope.cell)
            .copied()
            .unwrap_or(0)
    }

    fn total(&self) -> usize {
        self.by_cell.lock().unwrap().values().sum()
    }
}

async fn write(
    cell: &RaftCellHandle,
    contract: &OperationContract,
    sequence: u64,
    amount: i64,
    callbacks: &Callbacks,
) -> Result<CommandReceipt> {
    let mut options = public_node::archived_options(sequence)?;
    let deadline = tokio::time::Instant::now() + options.timeout;
    loop {
        options.timeout = deadline.saturating_duration_since(tokio::time::Instant::now());
        ensure!(
            !options.timeout.is_zero(),
            "extent command admission deadline"
        );
        let before = callbacks.count(cell.scope());
        let result = cell
            .command(
                contract.clone(),
                amount.to_be_bytes().to_vec(),
                options.clone(),
            )
            .await;
        let executed = callbacks.count(cell.scope()) - before;
        ensure!(
            executed <= 1,
            "one command attempt executed its callback more than once"
        );
        match result {
            Err(CommandFailure::Overloaded) => {
                // Capture may exhaust physical roots after the private SQL
                // callback. This definite pre-proposal refusal discards that
                // execution; retrying the same identity may run it again.
                callbacks.refused.fetch_add(executed, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            result => {
                return result
                    .with_context(|| format!("extent write sequence={sequence}, amount={amount}"))
            }
        }
    }
}

async fn close(node: Node, service: RaftCells) -> Result<()> {
    let closed = service.close().await;
    let shutdown = node.shutdown(ShutdownMode::Preserve).await;
    ensure!(
        service.resource_usage().extents.is_none(),
        "retained SDK handle owns extent storage after close"
    );
    ensure!(closed?.unfinished.is_empty());
    shutdown?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn extent_activation_requires_physical_admission_before_catalog_mutation() -> Result<()> {
    let root = tempfile::tempdir()?;
    let (gate, _entered) = public_node::Gate::new();
    gate.release();
    let definition = public_node::counter(Arc::new(AtomicUsize::new(0)), gate)?;
    let node = public_node::node(root.path(), definition.application.clone()).await?;
    let store = Arc::new(object_store::memory::InMemory::new());
    let result = public_node::cells(
        &node,
        definition.application,
        store.clone(),
        registrations(definition.schema)?.remove(0),
        false,
    )
    .await;
    node.shutdown(ShutdownMode::Preserve).await?;
    ensure!(
        result.is_err(),
        "extent namespace activated without physical admission"
    );
    ensure!(
        store.list(None).try_collect::<Vec<_>>().await?.is_empty(),
        "failed preflight wrote catalog state"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn node_extent_gc_preserves_closed_groups_and_releases_the_shared_inventory_on_shutdown(
) -> Result<()> {
    exercise(12, None).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn extent_gc_retries_preserve_exact_committed_callbacks_under_root_pressure() -> Result<()> {
    exercise(8, None).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn append_segments_preserve_closed_groups_receipts_and_shutdown_recovery() -> Result<()> {
    exercise(128, Some(32 * 1024)).await
}

async fn exercise(max_roots: usize, segment_bytes: Option<u64>) -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("cellar=debug")
        .with_test_writer()
        .try_init();
    let root = tempfile::tempdir()?;
    // The shared counter signals its second command even after this gate is
    // released. Retain the receiver for the lifetime of all three node runs.
    let (gate, _entered) = public_node::Gate::new();
    gate.release();
    let calls = Arc::new(Callbacks::default());
    let observed = calls.clone();
    let definition = public_node::counter_observed(
        move |scope| {
            observed.record(scope);
            Ok(())
        },
        gate,
    )?;
    let registrations = registrations(definition.schema)?;
    let store = Arc::new(object_store::memory::InMemory::new());
    let node = public_node::node(root.path(), definition.application.clone()).await?;
    let service = public_node::cells_many(
        &node,
        definition.application.clone(),
        store.clone(),
        registrations.clone(),
        false,
        {
            let mut options = host(max_roots);
            options.extents.as_mut().unwrap().append_segment_bytes = segment_bytes;
            options
        },
        Default::default(),
    )
    .await?;
    let result = async {
        let a = service.cell(
            registrations[0].namespace.namespace,
            &registrations[0].birth.scope.cell,
        )?;
        let b = service.cell(
            registrations[1].namespace.namespace,
            &registrations[1].birth.scope.cell,
        )?;
        public_node::ready(&a).await?;
        public_node::ready(&b).await?;
        let mut retained = None;
        for sequence in 1..=16 {
            let (left, right) = futures_util::future::join(
                write(&a, &definition.increment, sequence, 1, &calls),
                write(&b, &definition.increment, sequence, 3, &calls),
            )
            .await;
            ensure!(left?.result == i64::try_from(sequence)?.to_be_bytes());
            let right = right?;
            ensure!(right.result == (3 * i64::try_from(sequence)?).to_be_bytes());
            retained = Some(right);
        }
        let usage = service
            .resource_usage()
            .extents
            .context("missing shared node inventory")?;
        ensure!(!usage.quarantined && usage.roots <= max_roots && usage.workspaces <= 2);
        let text = service.resource_usage().prometheus();
        let samples = text
            .lines()
            .filter(|line| line.starts_with("cellar_raft_extents_"))
            .collect::<Vec<_>>();
        ensure!(samples.len() == 14 && samples.iter().all(|line| !line.contains('{')));
        ensure!(text.contains("cellar_raft_extents_sample_available 1\n"));
        retained.context("missing retained receipt")
    }
    .await;
    let closed = close(node, service).await;
    let retained = result?;
    closed?;

    // Register only A. GC must also preserve B's engine-selected current and
    // checkpoint roots, even though no B SQL owner exists in this process.
    let node = public_node::node(root.path(), definition.application.clone()).await?;
    let service = public_node::cells_many(
        &node,
        definition.application.clone(),
        store.clone(),
        vec![registrations[0].clone()],
        true,
        {
            let mut options = host(max_roots);
            options.extents.as_mut().unwrap().append_segment_bytes = segment_bytes;
            options
        },
        Default::default(),
    )
    .await?;
    let result = async {
        let a = service.cell(
            registrations[0].namespace.namespace,
            &registrations[0].birth.scope.cell,
        )?;
        public_node::ready(&a).await?;
        for sequence in 17..=32 {
            ensure!(
                write(&a, &definition.increment, sequence, 1, &calls)
                    .await?
                    .result
                    == i64::try_from(sequence)?.to_be_bytes()
            );
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let closed = close(node, service).await;
    result?;
    closed?;

    let node = public_node::node(root.path(), definition.application.clone()).await?;
    let service = public_node::cells_many(
        &node,
        definition.application.clone(),
        store,
        registrations.clone(),
        true,
        {
            let mut options = host(max_roots);
            options.extents.as_mut().unwrap().append_segment_bytes = segment_bytes;
            options
        },
        Default::default(),
    )
    .await?;
    let result = async {
        let b = service.cell(
            registrations[1].namespace.namespace,
            &registrations[1].birth.scope.cell,
        )?;
        public_node::ready(&b).await?;
        let historical = b
            .query(
                definition.get.clone(),
                vec![],
                QueryOptions {
                    consistency: QueryConsistency::AtLeast(retained.version_token.clone()),
                    ..Default::default()
                },
            )
            .await?;
        ensure!(historical.result == 48_i64.to_be_bytes());
        let before = calls.total();
        let duplicate = write(&b, &definition.increment, 16, 3, &calls).await?;
        ensure!(duplicate.id == retained.id && duplicate.result == retained.result);
        ensure!(
            calls.total() == before,
            "restored receipt reran the callback"
        );
        ensure!(
            write(&b, &definition.increment, 17, 3, &calls)
                .await?
                .result
                == 51_i64.to_be_bytes()
        );
        let refused = calls.refused.load(Ordering::SeqCst);
        ensure!(
            calls.total().checked_sub(refused) == Some(49),
            "expected 49 committed callbacks; total={}, pre-proposal refusals={refused}",
            calls.total()
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let closed = close(node, service).await;
    result?;
    closed
}
