//! Two real SDK histories share an immutable object, never a head or proof.
#![allow(clippy::disallowed_methods)]
use super::{bundle_faults as faults, bundle_images as images, public_node};
use anyhow::{ensure, Context as _, Result};
use cellar::application::consensus::verification::{ArchiveRecoverySet, Catalog};
use cellar::sdk::{consensus::*, ShutdownMode};
use cellar_ltx::{
    generation::{GenerationLimits, GenerationStore},
    DirectFileSystem, LtxHost,
};
use futures_util::TryStreamExt;
use object_store::ObjectStore;
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

fn restore(set: &ArchiveRecoverySet, expected: i64) -> Result<()> {
    let root = tempfile::tempdir()?;
    let images = GenerationStore::open(
        root.path().join("restore"),
        Arc::new(DirectFileSystem),
        GenerationLimits::default(),
    )?;
    let pin = set.restore(&images, images::id(91))?;
    let query = images::query(pin)?;
    ensure!(
        query
            .connection()
            .query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))?
            == expected
    );
    Ok(())
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
    let registrations = (10..12)
        .map(|id| {
            Ok(RaftCellRegistration {
                placement: RaftCellPlacement::Replica,
                namespace: namespace.clone(),
                class: "Counter".into(),
                birth: BirthProposal {
                    scope: GroupScope {
                        cluster: namespace.cluster,
                        namespace: namespace.namespace,
                        cell: format!("Counter:bundle-{id}"),
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
    let node = public_node::node(root.path(), definition.application.clone()).await?;
    let store = Arc::new(faults::Store::default());
    let release = Release(store.clone());
    store.archive_hang.store(true, Ordering::Release);
    let service = public_node::cells_many(
        &node,
        definition.application.clone(),
        store.clone(),
        registrations.clone(),
        false,
        RaftHostOptions {
            archive: RaftArchiveOptions {
                max_batch_groups: 2,
                ..Default::default()
            },
            ..Default::default()
        },
        TransportOptions::default(),
    )
    .await?;
    let result = async {
        let cells = registrations
            .iter()
            .map(|registration| service.cell(namespace.namespace, &registration.birth.scope.cell))
            .collect::<Result<Vec<_>, _>>()?;
        for cell in &cells {
            public_node::ready(cell).await?;
        }
        let first = public_node::options(1)?;
        cells[0]
            .command(
                definition.increment.clone(),
                3_i64.to_be_bytes().to_vec(),
                first.clone(),
            )
            .await?;
        // Freeze each producer after capture, then finish the old heads together.
        // Concurrent publishers otherwise complete their physical replay at
        // different times and need not batch the next cuts.
        tokio::time::timeout(Duration::from_secs(5), async {
            while store.archive_attempts.load(Ordering::Acquire) == 0 {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .context("captured first archive cut")?;
        cells[1]
            .command(
                definition.increment.clone(),
                7_i64.to_be_bytes().to_vec(),
                first,
            )
            .await?;
        tokio::time::timeout(Duration::from_secs(5), async {
            while store.archive_attempts.load(Ordering::Acquire) < 2 {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .context("captured both old archive cuts")?;
        let second = public_node::options(2)?;
        for (cell, value) in cells.iter().zip([3_i64, 7]) {
            cell.command(
                definition.increment.clone(),
                value.to_be_bytes().to_vec(),
                second.clone(),
            )
            .await?;
        }
        store.fail_bundle_after.store(true, Ordering::Release);
        store.archive_heads_hang.store(true, Ordering::Release);
        store.archive_hang.store(false, Ordering::Release);
        store.archive_resume.notify_waiters();
        tokio::time::timeout(Duration::from_secs(5), async {
            while store.archive_heads_waiting.load(Ordering::Acquire) < 2 {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .context("both old archive heads reached publication")?;
        drop(release);
        // Upgrade retries to S3 without changing command identity, input or
        // deadline. The lost bundle PUT response must be reconciled by readback.
        let mut durable = second;
        durable.durability = Durability::S3;
        durable.timeout = Duration::from_secs(10);
        let mut receipts = Vec::new();
        for (cell, value) in cells.iter().zip([3_i64, 7]) {
            let receipt = cell
                .command(
                    definition.increment.clone(),
                    value.to_be_bytes().to_vec(),
                    durable.clone(),
                )
                .await?;
            ensure!(
                receipt.result == (2 * value).to_be_bytes() && receipt.durability == Durability::S3
            );
            receipts.push(receipt);
        }
        ensure!(
            !store.fail_bundle_after.load(Ordering::Acquire),
            "native producer never wrote a bundle"
        );
        ensure!(
            calls.load(Ordering::Acquire) == 4,
            "S3 retry repeated a business callback"
        );
        let performance = service.performance();
        ensure!(performance.available && !performance.saturated);
        let counter = |name| {
            performance
                .counters
                .iter()
                .find(|counter| counter.name == name)
                .unwrap()
                .value
        };
        ensure!(counter("object_put_requested_bytes") > counter("object_put_confirmed_bytes"));
        ensure!(counter("object_get_body_bytes") > 0);
        ensure!(counter("archive_bundle_read_owner") > 0);
        ensure!(
            counter("archive_bundle_read_owner") + counter("archive_bundle_read_joined") >= 2,
            "native bundle members did not enter the observed read context"
        );
        ensure!(
            performance
                .operations
                .iter()
                .find(|op| op.name == "object_put_create")
                .unwrap()
                .failed
                > 0,
            "lost bundle PUT reply was incorrectly recorded as a confirmed request"
        );
        let catalog = Catalog::new(
            store.clone(),
            CatalogOptions {
                prefix: "host/test".into(),
                ..Default::default()
            },
        )?;
        let selected = catalog.existing_namespace(namespace.clone()).await?;
        let mut archives = Vec::new();
        for (registration, receipt) in registrations.iter().zip(receipts) {
            let birth = selected
                .birth(&registration.birth.scope.cell)
                .await?
                .context("selected birth")?;
            let archive =
                selected.cell_archive(&birth, LtxHost::default(), ArchiveLimits::default())?;
            let metadata = archive
                .recovery_metadata()
                .await?
                .context("published head")?;
            ensure!(
                metadata.scope() == &registration.birth.scope
                    && metadata.position().index >= receipt.version.position.index
            );
            archives.push((archive, metadata.reference()));
        }
        Ok::<_, anyhow::Error>(archives)
    }
    .await;
    store.archive_hang.store(false, Ordering::Release);
    store.archive_resume.notify_waiters();
    let close = service.close().await?;
    ensure!(close.unfinished.is_empty());
    node.shutdown(ShutdownMode::Preserve).await?;
    let archives = result?;
    let objects = store.inner.list(None).try_collect::<Vec<_>>().await?;
    let bundles: Vec<_> = objects
        .iter()
        .filter(|object| object.location.as_ref().contains("/archives/bundles/"))
        .collect();
    ensure!(
        bundles.len() == 1,
        "expected one shared bundle, observed {}",
        bundles.len()
    );
    ensure!(
        objects
            .iter()
            .filter(|object| object.location.as_ref().contains("/tails/"))
            .count()
            == 0,
        "the new tail must reside inside the shared bundle"
    );
    for ((archive, reference), value) in archives.iter().zip([6, 14]) {
        let set = archive.recovery_set_at(reference).await?;
        restore(&set, value)?;
    }
    ensure!(
        archives[0].0.recovery_set_at(&archives[1].1).await.is_err(),
        "foreign manifest supplied a history"
    );
    let path = &bundles[0].location;
    let bytes = store.inner.get(path).await?.bytes().await?;
    let mut corrupt = bytes.to_vec();
    *corrupt.last_mut().context("nonempty bundle")? ^= 1;
    store.inner.put(path, corrupt.into()).await?;
    for (archive, reference) in &archives {
        ensure!(
            archive.recovery_set_at(reference).await.is_err(),
            "corrupt bundle supplied successful recovery"
        );
    }
    store.inner.put(path, bytes.into()).await?;
    for ((archive, reference), value) in archives.iter().zip([6, 14]) {
        restore(&archive.recovery_set_at(reference).await?, value)?;
    }
    cellar::note!("native bundle: two exact histories, one shared snapshot/tail object, confirmed lost PUT response, isolated post-close restores, corruption refusal, four callbacks");
    Ok(())
}
