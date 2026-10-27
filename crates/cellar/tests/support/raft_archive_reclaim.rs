// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Short archived histories compact independently; failed publication retains logs.
use super::{bundle_faults as faults, bundle_images as images, public_node};
use anyhow::{ensure, Context as _, Result};
use cellar::application::consensus::verification::Catalog;
use cellar::sdk::{consensus::*, ShutdownMode};
use cellar_ltx::{
    generation::{GenerationLimits, GenerationStore},
    DirectFileSystem, LtxHost,
};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

struct Release(Arc<faults::Store>);

impl Drop for Release {
    fn drop(&mut self) {
        self.0.archive_hang.store(false, Ordering::Release);
        self.0.archive_resume.notify_waiters();
        self.0.archive_heads_hang.store(false, Ordering::Release);
        self.0.archive_heads_resume.notify_waiters();
    }
}

fn host() -> RaftHostOptions {
    RaftHostOptions {
        // Neither entry-count nor cache-pressure policy can explain compaction.
        checkpoint_entries: u64::MAX,
        archive_checkpoint_ms: 20,
        ..Default::default()
    }
}

pub async fn run() -> Result<()> {
    let (gate, _entered) = public_node::Gate::new();
    gate.release();
    let calls = Arc::new(AtomicUsize::new(0));
    let definition = public_node::counter(calls.clone(), gate)?;
    let mut namespace = NamespaceConfig::new(
        ClusterId::new([1; 16])?,
        NamespaceId::new([2; 16])?,
        NamespaceMode::Raft,
        definition.schema,
    );
    namespace.voters = VoterPolicy::SingleVoterDevelopment;
    let registrations = (10..13)
        .map(|id| {
            Ok(RaftCellRegistration {
                placement: RaftCellPlacement::Replica,
                namespace: namespace.clone(),
                class: "Counter".into(),
                birth: BirthProposal {
                    scope: GroupScope {
                        cluster: namespace.cluster,
                        namespace: namespace.namespace,
                        cell: format!("Counter:reclaim-{id}"),
                        group: GroupId::new([id; 16])?,
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
        .collect::<Result<Vec<_>>>()?;
    let root = tempfile::tempdir()?;
    let store = Arc::new(faults::Store::default());
    let release = Release(store.clone());
    store.archive_heads_hang.store(true, Ordering::Release);
    let node = public_node::node(root.path(), definition.application.clone()).await?;
    let service = public_node::cells_many(
        &node,
        definition.application.clone(),
        store.clone(),
        registrations.clone(),
        false,
        host(),
        TransportOptions::default(),
    )
    .await?;
    let result = async {
        let cells = registrations
            .iter()
            .map(|r| service.cell(namespace.namespace, &r.birth.scope.cell))
            .collect::<Result<Vec<_>, _>>()?;
        for cell in &cells {
            public_node::ready(cell).await?;
        }
        for sequence in 1..=2 {
            let mut previous = Vec::new();
            let mut receipts = Vec::new();
            for cell in &cells {
                previous.push(cell.status().await?.checkpoint);
                receipts.push(
                    cell.command(
                        definition.increment.clone(),
                        1_i64.to_be_bytes().to_vec(),
                        public_node::options(sequence)?,
                    )
                    .await?,
                );
            }
            if sequence == 1 {
                tokio::time::timeout(Duration::from_secs(5), async {
                    while store.archive_heads_waiting.load(Ordering::Acquire) < 2 {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                })
                .await
                .context("uploaded recovery sets did not reach held head confirmation")?;
            }
            // Round one has uploaded objects and written heads, but holds their
            // confirmations. Round two holds object I/O completely. Neither
            // may establish the local archive proof required by this policy.
            tokio::time::sleep(Duration::from_millis(100)).await;
            for ((cell, checkpoint), receipt) in cells.iter().zip(&previous).zip(&receipts) {
                let status = cell.status().await?;
                ensure!(status.checkpoint == *checkpoint);
                ensure!(status.archived < receipt.version.position.index);
            }
            store.archive_hang.store(false, Ordering::Release);
            store.archive_resume.notify_waiters();
            store.archive_heads_hang.store(false, Ordering::Release);
            store.archive_heads_resume.notify_waiters();
            for cell in &cells {
                let durable = cell
                    .command(
                        definition.increment.clone(),
                        1_i64.to_be_bytes().to_vec(),
                        public_node::archived_options(sequence)?,
                    )
                    .await?;
                ensure!(durable.result == (sequence as i64).to_be_bytes());
            }
            tokio::time::timeout(Duration::from_secs(5), async {
                for (cell, receipt) in cells.iter().zip(&receipts) {
                    loop {
                        let status = cell.status().await?;
                        if status.checkpoint.index >= receipt.version.position.index {
                            ensure!(status.checkpoint.index <= status.archived);
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                }
                Ok::<_, anyhow::Error>(())
            })
            .await
            .context("short archived histories did not checkpoint fairly")??;
            store.archive_hang.store(true, Ordering::Release);
        }
        ensure!(
            calls.load(Ordering::Acquire) == 6,
            "durability retries executed callbacks twice"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    drop(release);
    let closed = service.close().await;
    let stopped = node.shutdown(ShutdownMode::Preserve).await;
    result?;
    ensure!(closed?.unfinished.is_empty());
    stopped?;

    // Independent object-store-only restore proves compaction kept the complete
    // recovery set, including command receipts stored in the SQL image.
    let catalog = Catalog::new(
        store.clone(),
        CatalogOptions {
            prefix: "host/test".into(),
            ..Default::default()
        },
    )?;
    let selected = catalog.existing_namespace(namespace.clone()).await?;
    for registration in &registrations {
        let birth = selected
            .birth(&registration.birth.scope.cell)
            .await?
            .context("birth")?;
        let archive =
            selected.cell_archive(&birth, LtxHost::default(), ArchiveLimits::default())?;
        let set = archive
            .recovery_set()
            .await?
            .context("confirmed recovery set")?;
        let target = tempfile::tempdir()?;
        let generations = GenerationStore::open(
            target.path().join("images"),
            Arc::new(DirectFileSystem),
            GenerationLimits::default(),
        )?;
        let pin = set.restore(&generations, images::id(91))?;
        let query = images::query(pin)?;
        ensure!(
            query
                .connection()
                .query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))?
                == 2
        );
    }
    let node = public_node::node(root.path(), definition.application.clone()).await?;
    let service = public_node::cells_many(
        &node,
        definition.application.clone(),
        store,
        registrations.clone(),
        true,
        host(),
        TransportOptions::default(),
    )
    .await?;
    let result = async {
        for registration in &registrations {
            let cell = service.cell(namespace.namespace, &registration.birth.scope.cell)?;
            public_node::ready(&cell).await?;
            let receipt = cell
                .command(
                    definition.increment.clone(),
                    1_i64.to_be_bytes().to_vec(),
                    public_node::archived_options(2)?,
                )
                .await?;
            ensure!(receipt.result == 2_i64.to_be_bytes());
        }
        ensure!(
            calls.load(Ordering::Acquire) == 6,
            "recovered compacted receipts re-executed callbacks"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let closed = service.close().await;
    let stopped = node.shutdown(ShutdownMode::Preserve).await;
    result?;
    ensure!(closed?.unfinished.is_empty());
    stopped?;
    Ok(())
}
