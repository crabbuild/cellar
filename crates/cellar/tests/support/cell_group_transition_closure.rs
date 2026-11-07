// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Lose every local inventory immediately after publication, before activation.
use super::*;
use cellar::application::consensus::verification::cell_groups::{
    CellGroupArchiveError, CellGroupArchivePool, CellGroupArchiveTailLimits,
};
use std::{
    cell::RefCell,
    collections::BTreeSet,
    path::{Path, PathBuf},
};

tokio::task_local! { static CASE: RefCell<Scene>; }

#[derive(Default)]
struct Scene {
    inventories: BTreeSet<PathBuf>,
    acknowledged: Option<CommandReceipt>,
    published: Option<Expected>,
    refused: usize,
    restored: bool,
}

struct Expected {
    operation: ProposalId,
    plan: [u8; 32],
    publication: [u8; 32],
    directory: [u8; 32],
    fleet_revision: u64,
}

pub(crate) async fn qualify(rooted: bool, store: Arc<fault::Store>) -> Result<()> {
    CASE.scope(RefCell::new(Scene::default()), async {
        Box::pin(effects::qualify_publication(rooted, store)).await?;
        CASE.with(|scene| {
            let scene = scene.borrow();
            ensure!(
                scene.refused == 2 && scene.published.is_some() && scene.restored,
                "incomplete publication loss schedule"
            );
            Ok(())
        })
    })
    .await
}

pub(super) fn inventory(path: &Path) {
    let _ = CASE.try_with(|scene| {
        scene.borrow_mut().inventories.insert(path.to_owned());
    });
}

pub(super) fn acknowledged(receipt: &CommandReceipt) {
    let _ = CASE.try_with(|scene| scene.borrow_mut().acknowledged = Some(receipt.clone()));
}

pub(super) async fn before_publication(
    cluster: &ManagedCluster,
    fleet: &FleetCatalog,
    directory: &RangeDirectory,
    transition: &ReopenedManagedCellGroupTransition,
    archives: &CellGroupArchivePool,
) -> Result<()> {
    if CASE.try_with(|_| ()).is_err() {
        return Ok(());
    }
    let f = &cluster.fixtures[0];
    let before = f.domain.run(directory.observe()).await??;
    let revision = fleet.read(TIMEOUT).await?.revision();
    let (closure, reads) =
        fault::count_reads(fleet.reopen_managed_transfer_archive(transition, directory, TIMEOUT))
            .await;
    ensure!(
        closure?
            .context("selected remote closure")?
            .source_holds
            .len()
            == 1
    );
    let closure_key = reads
        .keys()
        .find(|key| key.contains("/transfer-archive-closures/"))
        .context("immutable closure was actually fetched")?
        .clone();
    let birth = f
        .domain
        .run(directory.materialize_selected_history(cluster.history))
        .await??;
    let registry = directory
        .open_group_archive(f.domain.as_ref(), archives, &birth)
        .await?;
    let (intent, reads) = fault::count_reads(registry.prepare_acquire(f.domain.as_ref())).await;
    intent?; // Read-only intent preparation allocates no remote hold.
    let registry_key = reads
        .keys()
        .find(|key| key.ends_with("/ownership/registry"))
        .context("retained source registry was actually fetched")?
        .clone();
    for (index, key) in [closure_key, registry_key].into_iter().enumerate() {
        let (result, reads) = fault::count_reads(fault::missing_read(
            key.clone(),
            fleet.publish_managed_transfer(transition, directory, archives, TIMEOUT),
        ))
        .await;
        ensure!(
            reads.get(&key).is_some_and(|count| *count > 0),
            "missing dependency was not exercised"
        );
        if index == 0 {
            ensure!(
                matches!(
                    result,
                    Err(FleetCatalogError::Corrupt(
                        "missing transfer archive closure"
                    ))
                ),
                "missing closure must fail closed: {result:?}"
            );
        } else {
            ensure!(
                matches!(result, Err(FleetCatalogError::Storage(ref error))
                if matches!(error.downcast_ref::<CellGroupArchiveError>(), Some(CellGroupArchiveError::MissingRegistry))),
                "missing retained source registry must fail closed: {result:?}"
            );
        }
        ensure!(fleet.read(TIMEOUT).await?.revision() == revision);
        ensure!(f.domain.run(directory.observe()).await??.digest() == before.digest());
        ensure!(f
            .domain
            .run(directory.publication(&transition.plan))
            .await??
            .is_none());
        CASE.with(|scene| scene.borrow_mut().refused += 1);
    }
    Ok(())
}

pub(super) async fn published(
    cluster: &ManagedCluster,
    directory: &RangeDirectory,
    transition: &ReopenedManagedCellGroupTransition,
) -> Result<bool> {
    if CASE.try_with(|_| ()).is_err() {
        return Ok(false);
    }
    let f = &cluster.fixtures[0];
    ensure!(
        transition
            .journal
            .as_ref()
            .context("publication journal")?
            .phase()
            == TransitionPhase::Published
    );
    let publication = f
        .domain
        .run(directory.publication(&transition.plan))
        .await??
        .context("publication")?;
    let view = f.domain.run(directory.observe()).await??;
    let routes = f.domain.run(directory.group_page(&view, None, 3)).await??;
    ensure!(routes.routes.len() == 2 && routes.next.is_none());
    for host in &cluster.hosts[3..] {
        for route in &routes.routes {
            ensure!(
                !host.group(route.history)?.status().await?.ready,
                "a child activated before erasure"
            );
        }
    }
    let expected = Expected {
        operation: transition.plan.operation(),
        plan: transition.plan.digest(),
        publication: publication.journal_digest(),
        directory: view.digest(),
        fleet_revision: f.catalog.read(TIMEOUT).await?.revision(),
    };
    CASE.with(|scene| scene.borrow_mut().published = Some(expected));
    Ok(true)
}

pub(super) async fn erase_and_restore(mut cluster: ManagedCluster) -> Result<()> {
    ensure!(
        cluster.nodes.is_empty(),
        "voter node owners must join before erasure"
    );
    let store = cluster.fixtures[0].store.clone();
    let namespace = cluster.fixtures[0].declaration.namespace.clone();
    let history = cluster.history;
    let disks = std::mem::take(&mut cluster.roots);
    ensure!(disks.len() == 6);
    let paths = CASE.with(|scene| scene.borrow().inventories.clone());
    ensure!(
        paths.len() == 2,
        "every pre-copy and final verifier inventory must be tracked"
    );
    effects::discard_endpoint();
    drop(cluster); // Drop every closed host, old directory and private inventory cache.

    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = async {
        let root = domain
            .blocking(move || {
                let disk_paths = disks
                    .iter()
                    .map(|disk| disk.path().to_owned())
                    .collect::<Vec<_>>();
                ensure!(disk_paths.iter().all(|path| path.exists()));
                for disk in disks {
                    disk.close()?;
                }
                for path in paths.iter() {
                    if path.exists() {
                        std::fs::remove_dir_all(path)?;
                    }
                }
                ensure!(
                    disk_paths.iter().chain(&paths).all(|path| !path.exists()),
                    "an old local copy survived"
                );
                let root = tempfile::Builder::new()
                    .prefix("published-closure-replay-")
                    .tempdir()?
                    .keep();
                ensure!(disk_paths
                    .iter()
                    .chain(&paths)
                    .all(|path| !root.starts_with(path)));
                Ok::<_, anyhow::Error>(root)
            })?
            .await??;
        let fleet = Fixture::open_catalog(&domain, store.clone(), history.cluster)?;
        let catalog = Catalog::new(
            store.clone(),
            CatalogOptions {
                prefix: "managed/".into(),
                ..Default::default()
            },
        )?;
        let namespace = domain.run(catalog.existing_namespace(namespace)).await??;
        let directory = RangeDirectory::new(namespace, DirectoryLimits::default())?;
        let (operation, plan, publication, digest, revision, receipt) = CASE.with(|scene| {
            let scene = scene.borrow();
            let expected = scene
                .published
                .as_ref()
                .context("missing publication metadata")?;
            Ok::<_, anyhow::Error>((
                expected.operation,
                expected.plan,
                expected.publication,
                expected.directory,
                expected.fleet_revision,
                scene.acknowledged.clone().context("S3 ledger")?,
            ))
        })?;
        let current = fleet
            .reopen_managed_transition(&directory, operation, TIMEOUT)
            .await?
            .context("remote transition")?;
        ensure!(
            current.plan.digest() == plan
                && current.journal.as_ref().context("remote journal")?.phase()
                    == TransitionPhase::Published
        );
        ensure!(
            domain
                .run(directory.publication(&current.plan))
                .await??
                .context("remote certificate")?
                .journal_digest()
                == publication
        );
        ensure!(
            domain.run(directory.observe()).await??.digest() == digest
                && fleet.read(TIMEOUT).await?.revision() == revision
        );
        ensure!(
            domain
                .run(directory.materialize_selected_history(history))
                .await?
                .is_err(),
            "ordinary parent startup became unfenced"
        );
        let rooted = directory.namespace_config().image_storage == RaftImageStorage::Extents;
        // Each attempt has new private stores. None can borrow a former voter's data.
        let denied = crate::managed_fixture::verifier::open_for_domain(
            &domain,
            &root.join("denied"),
            rooted,
            CellGroupArchiveTailLimits::default(),
        )
        .await?;
        let before = store.denied_group_payload_reads.load(Ordering::Acquire);
        store
            .deny_group_payload_reads
            .store(true, Ordering::Release);
        let refused = fleet
            .replay_managed_transfer_baseline(
                &directory,
                operation,
                &denied,
                preparation::recovery_pool()?,
                TIMEOUT,
            )
            .await;
        store
            .deny_group_payload_reads
            .store(false, Ordering::Release);
        ensure!(
            refused.is_err() && store.denied_group_payload_reads.load(Ordering::Acquire) > before,
            "archive replay succeeded without reading remote payload"
        );
        drop(denied);
        let verifier = crate::managed_fixture::verifier::open_for_domain(
            &domain,
            &root.join("verified"),
            rooted,
            CellGroupArchiveTailLimits::default(),
        )
        .await?;
        let restored = fleet
            .replay_managed_transfer_baseline(
                &directory,
                operation,
                &verifier,
                preparation::recovery_pool()?,
                TIMEOUT,
            )
            .await?;
        effects::verify_publication_baseline(&domain, &restored, &root, receipt).await?;
        ensure!(
            domain.run(directory.observe()).await??.digest() == digest
                && fleet.read(TIMEOUT).await?.revision() == revision,
            "baseline replay changed remote topology"
        );
        drop(restored);
        drop(verifier);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    domain.shutdown().await;
    result?;
    CASE.with(|scene| scene.borrow_mut().restored = true);
    Ok(())
}
