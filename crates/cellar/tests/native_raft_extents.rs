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
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
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

fn host() -> RaftHostOptions {
    RaftHostOptions {
        max_groups: 2,
        checkpoint_entries: 1,
        extents: Some(RaftExtentOptions {
            inventory: RaftExtentLimits {
                max_roots: 12,
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

async fn write(
    cell: &RaftCellHandle,
    contract: &OperationContract,
    sequence: u64,
    amount: i64,
) -> Result<CommandReceipt> {
    let mut options = public_node::archived_options(sequence)?;
    let deadline = tokio::time::Instant::now() + options.timeout;
    loop {
        options.timeout = deadline.saturating_duration_since(tokio::time::Instant::now());
        ensure!(
            !options.timeout.is_zero(),
            "extent command admission deadline"
        );
        match cell
            .command(
                contract.clone(),
                amount.to_be_bytes().to_vec(),
                options.clone(),
            )
            .await
        {
            Err(CommandFailure::Overloaded) => tokio::time::sleep(Duration::from_millis(5)).await,
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
    let _ = tracing_subscriber::fmt()
        .with_env_filter("cellar=debug")
        .with_test_writer()
        .try_init();
    let root = tempfile::tempdir()?;
    // The shared counter signals its second command even after this gate is
    // released. Retain the receiver for the lifetime of all three node runs.
    let (gate, _entered) = public_node::Gate::new();
    gate.release();
    let calls = Arc::new(AtomicUsize::new(0));
    let definition = public_node::counter(calls.clone(), gate)?;
    let registrations = registrations(definition.schema)?;
    let store = Arc::new(object_store::memory::InMemory::new());
    let node = public_node::node(root.path(), definition.application.clone()).await?;
    let service = public_node::cells_many(
        &node,
        definition.application.clone(),
        store.clone(),
        registrations.clone(),
        false,
        host(),
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
                write(&a, &definition.increment, sequence, 1),
                write(&b, &definition.increment, sequence, 3),
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
        ensure!(!usage.quarantined && usage.roots <= 12 && usage.workspaces <= 2);
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
        host(),
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
                write(&a, &definition.increment, sequence, 1).await?.result
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
        host(),
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
        let before = calls.load(Ordering::SeqCst);
        let duplicate = write(&b, &definition.increment, 16, 3).await?;
        ensure!(duplicate.id == retained.id && duplicate.result == retained.result);
        ensure!(
            calls.load(Ordering::SeqCst) == before,
            "restored receipt reran the callback"
        );
        ensure!(write(&b, &definition.increment, 17, 3).await?.result == 51_i64.to_be_bytes());
        ensure!(calls.load(Ordering::SeqCst) == 49);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let closed = close(node, service).await;
    result?;
    closed
}
