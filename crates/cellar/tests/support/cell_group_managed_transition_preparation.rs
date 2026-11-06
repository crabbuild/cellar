// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Rebuild a durable pre-copy from the same real managed archive hold.
use super::*;
use cellar::application::consensus::verification::cell_groups::{
    CellGroupApplyImages, CellGroupArchiveError, CellGroupArchiveHold, CellGroupArchiveRegistry,
    CellGroupArchiveStore, CellGroupArchiveTailLimits, CellGroupArchiveTailPool, CellGroupBirth,
    CellGroupCheckpointPool, CellGroupRecoveryLimits, CellGroupRecoveryPool,
    CellGroupTransferTarget, CellGroupTransitionPreparationInputs, CellGroupTransitionSource,
    DirectoryMutation, GroupRoute, ManifestPageStore, TransitionJournal,
};
use cellar::application::consensus::verification::FleetHistoryState;
use cellar::sdk::fleet_operations::{
    RaftFleetControl, RaftFleetFailureCode, RaftFleetTransitionActivationRequest,
    RaftFleetTransitionPrepareRequest, RaftFleetTransitionPublishRequest,
};
use cellar_ltx::{
    generation::{GenerationStore, ReadServices},
    internal::{CacheLimits, NodeExtentStore, EXTENT_BYTES},
    stream_snapshot::{ScratchLimits, ScratchPool},
    DirectFileSystem,
};
use rand::RngCore as _;

pub(super) async fn exercise(
    cluster: &ManagedCluster,
    controller: &FleetCatalog,
    directory: &RangeDirectory,
    transition: &ReopenedManagedCellGroupTransition,
    parent: &CellGroupBirth,
    children: &[GroupRoute],
    rooted: bool,
) -> Result<(CellGroupArchiveRegistry, CellGroupArchiveHold, SeededCells)> {
    let seeded = seed_cells(&cluster.hosts, cluster.history).await?;
    let fixture = &cluster.fixtures[0];
    let domain = &fixture.domain;
    let leader = leader(&cluster.hosts[..3], cluster.history).await?;
    let cell = &seeded.cells[0];
    let handle = cluster.hosts[leader].cell(cell.clone())?;
    let client = seeded.receipts[0].id.client;
    let mut options = CommandOptions::new(CommandId::new(client, 2)?, seeded.retry);
    options.durability = Durability::S3;
    let archived = handle
        .command(
            &handle.contract("Add")?,
            2i64.to_be_bytes().to_vec(),
            options,
        )
        .await?;
    ensure!(archived.durability == Durability::S3 && archived.result == 9i64.to_be_bytes());
    let archives = CellGroupArchivePool::new(Default::default())?;
    let registry = directory
        .open_group_archive(domain.as_ref(), &archives, parent)
        .await?;
    let operation = transition.plan.operation();
    let before = controller
        .reopen_managed_transition(directory, operation, TIMEOUT)
        .await?
        .context("planned transition before acquire")?;
    let denied_before = fixture.store.denied_registry_writes.load(Ordering::Acquire);
    fixture
        .store
        .deny_registry_writes
        .store(true, Ordering::Release);
    let denied = controller
        .allocate_managed_preparation_holds(directory, operation, &archives, TIMEOUT)
        .await;
    fixture
        .store
        .deny_registry_writes
        .store(false, Ordering::Release);
    ensure!(
        denied.is_err()
            && fixture.store.denied_registry_writes.load(Ordering::Acquire) > denied_before
    );
    let reopened = Fixture::open_catalog(domain, fixture.store.clone(), cluster.history.cluster)?;
    let after = reopened
        .reopen_managed_transition(directory, operation, TIMEOUT)
        .await?
        .context("admitted acquire after denied registry update")?;
    ensure!(after.journal != before.journal);
    ensure!(reopened
        .staged_managed_preparation_holds(directory, operation, TIMEOUT)
        .await?
        .is_none());

    let staged = allocate_preparation_holds(&reopened, directory, operation, &archives).await?;
    ensure!(staged.phase() == TransitionPhase::Planned && staged.preparation().is_none());
    let [held]: [CellGroupArchiveHold; 1] = reopened
        .staged_managed_preparation_holds(directory, operation, TIMEOUT)
        .await?
        .context("missing operator-owned staged hold")?
        .try_into()
        .map_err(|_| anyhow::anyhow!("split must own one preparation hold"))?;
    ensure!(
        reopened
            .allocate_managed_preparation_holds(directory, operation, &archives, TIMEOUT)
            .await?
            == staged
    );
    ensure!(matches!(
        reopened
            .release_cancelled_preparation_holds(directory, operation, &archives, TIMEOUT)
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    let alternate = acquire_hold(domain, &registry).await?;
    ensure!(alternate != held);
    ensure!(matches!(
        controller
            .stage_managed_preparation_holds(
                transition,
                directory,
                &archives,
                std::slice::from_ref(&alternate),
                TIMEOUT,
            )
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    registry.release(domain.as_ref(), &alternate).await?;

    let scratch = tempfile::tempdir()?;
    ensure!(
        reopened
            .staged_managed_preparation_holds(directory, operation, TIMEOUT)
            .await?
            == Some(vec![held.clone()])
    );
    let verifier = crate::managed_fixture::verifier::open(
        fixture,
        &scratch.path().join("denied"),
        rooted,
        CellGroupArchiveTailLimits::default(),
    )
    .await?;
    let denied_before = fixture
        .store
        .denied_group_payload_reads
        .load(Ordering::Acquire);
    fixture
        .store
        .deny_group_payload_reads
        .store(true, Ordering::Release);
    let denied = reopened
        .resume_managed_preparation(directory, operation, &verifier, recovery_pool()?, TIMEOUT)
        .await;
    fixture
        .store
        .deny_group_payload_reads
        .store(false, Ordering::Release);
    ensure!(
        denied.is_err()
            && fixture
                .store
                .denied_group_payload_reads
                .load(Ordering::Acquire)
                > denied_before
    );
    let after_denial = reopened
        .reopen_managed_transition(directory, operation, TIMEOUT)
        .await?
        .context("admitted transition after pre-copy outage")?;
    ensure!(after_denial.journal.as_ref() == Some(&staged));
    drop(verifier);

    let control = RaftFleetControl::new(domain, reopened.clone(), vec![directory.clone()])?;
    let request = RaftFleetTransitionPrepareRequest {
        catalog_id: control.catalog_id().into(),
        namespace_id: hex::encode(cluster.history.namespace.bytes()),
        operation_id: hex::encode(operation.bytes()),
        request_id: hex::encode([176; 16]),
        plan_digest: hex::encode(transition.plan.digest()),
    };
    let refused = control
        .prepare_transition(
            &transition.request.principal,
            &request.namespace_id,
            &request.operation_id,
            &request,
            TIMEOUT,
        )
        .await
        .err()
        .context("preparation without a configured verifier")?;
    ensure!(refused.code == RaftFleetFailureCode::Unsupported);
    let verifier = crate::managed_fixture::verifier::open(
        fixture,
        &scratch.path().join("operator"),
        rooted,
        CellGroupArchiveTailLimits::default(),
    )
    .await?;
    let prepared = crate::managed_fixture::operator::prepare_transition(
        fixture,
        directory,
        &transition.request.principal,
        &request,
        Some((verifier, recovery_pool()?)),
    )
    .await?;
    ensure!(prepared.phase == TransitionPhase::Prepared);
    // The selected cut survives a later quorum write to the active parent.
    let next = handle
        .command(
            &handle.contract("Add")?,
            1i64.to_be_bytes().to_vec(),
            CommandOptions::new(CommandId::new(client, 3)?, seeded.retry),
        )
        .await?;
    ensure!(next.result == 10i64.to_be_bytes() && next.version != archived.version);
    let replay = crate::managed_fixture::operator::prepare_transition(
        fixture,
        directory,
        &transition.request.principal,
        &request,
        None,
    )
    .await?;
    ensure!(replay == prepared);

    let verifier = crate::managed_fixture::verifier::open(
        fixture,
        &scratch.path().join("reopened"),
        rooted,
        CellGroupArchiveTailLimits::default(),
    )
    .await?;
    let reopened = Fixture::open_catalog(domain, fixture.store.clone(), cluster.history.cluster)?;
    let replay = reopened
        .resume_managed_preparation(directory, operation, &verifier, recovery_pool()?, TIMEOUT)
        .await?;
    ensure!(replay
        .children()
        .iter()
        .map(|child| child.route())
        .eq(children));
    ensure!(replay.journal().phase() == TransitionPhase::Prepared);
    ensure!(replay
        .children()
        .iter()
        .all(|child| child.genesis().is_none()));
    let digest = replay
        .journal()
        .preparation()
        .context("missing pre-copy digest")?;
    ensure!(prepared.preparation_digest == hex::encode(digest));
    ensure!(
        replay
            .children()
            .iter()
            .map(|child| child.root().cells())
            .sum::<u32>()
            == 2
    );
    let mut staged = replay
        .children()
        .iter()
        .filter_map(|child| child.pin_cell(&cell.cell));
    let (leaf, _) = staged.next().context("missing archived cell")?;
    ensure!(leaf.version == archived.version && leaf.witness == archived.witness);
    ensure!(staged.next().is_none());
    ensure!(
        domain
            .run(directory.managed_preparation_holds(&transition.plan))
            .await??
            == vec![held.clone()]
    );
    Ok((registry, held, seeded))
}

pub(super) async fn allocate_preparation_holds(
    fleet: &FleetCatalog,
    directory: &RangeDirectory,
    operation: ProposalId,
    archives: &CellGroupArchivePool,
) -> Result<TransitionJournal> {
    let journal = tokio::time::timeout(TIMEOUT, async {
        loop {
            match fleet
                .allocate_managed_preparation_holds(directory, operation, archives, TIMEOUT)
                .await
            {
                Ok(journal) => return Ok(journal),
                Err(FleetCatalogError::Conflict | FleetCatalogError::Unconfirmed) => {
                    tokio::task::yield_now().await;
                }
                Err(error) => return Err(error),
            }
        }
    })
    .await??;
    Ok(journal)
}

pub(super) fn recovery_pool() -> Result<CellGroupRecoveryPool> {
    Ok(CellGroupRecoveryPool::new(CellGroupRecoveryLimits {
        max_cells: 4,
        max_image_bytes: 1024 * 1024,
        max_restore_bytes: 4 * 1024 * 1024,
        ..Default::default()
    })?)
}

pub(super) fn store(
    base: std::path::PathBuf,
    route: GroupRoute,
    rooted: bool,
) -> Result<CellGroupTransferTarget> {
    let filesystem = Arc::new(DirectFileSystem);
    let scratch = ScratchPool::new(ScratchLimits {
        max_bytes: 32 * 1024 * 1024,
        max_files: 1,
    })?;
    let services = ReadServices {
        now_unix_millis: Arc::new(cellar::asyncrt::wall_ms),
        fill_random: Arc::new(|bytes| cellar::asyncrt::rng("transition-precopy").fill_bytes(bytes)),
    };
    let images = if rooted {
        CellGroupApplyImages::Extents {
            images: NodeExtentStore::open(
                base.join("extents"),
                filesystem.clone(),
                Default::default(),
            )?,
            services,
            limits: Default::default(),
            cache_directory: base.join("cache"),
            cache_pool: scratch.clone(),
            cache: CacheLimits {
                max_cached_bytes: EXTENT_BYTES,
                max_cached_extents: 1,
            },
        }
    } else {
        CellGroupApplyImages::Materialized {
            images: GenerationStore::open(
                base.join("images"),
                filesystem.clone(),
                Default::default(),
            )?,
            services,
        }
    };
    let limits = limits();
    Ok(CellGroupTransferTarget {
        store: CellGroupArchiveStore::new(
            route,
            ManifestPageStore::open(
                &base.join("pages"),
                filesystem,
                limits.manifest_pages,
                limits.manifest_bytes,
                limits.retained_roots,
            )?,
            images,
            limits.manifest,
            limits.entries,
            limits.max_effect_rows,
        )?,
        scratch,
        directory: base,
    })
}

pub(super) async fn acquire_hold(
    domain: &ExecutionDomain,
    registry: &CellGroupArchiveRegistry,
) -> Result<CellGroupArchiveHold> {
    Ok(tokio::time::timeout(TIMEOUT, async {
        let mut intent = registry.prepare_acquire(domain).await?;
        loop {
            match registry.acquire(domain, &intent).await {
                Ok(hold) => return Ok(hold),
                // A newer registry revision has fenced this exact intent.
                Err(CellGroupArchiveError::Expired) => {
                    intent = registry.prepare_acquire(domain).await?;
                }
                // Reconcile the same allocation until a hold or definite
                // expiry is observed. The archive publisher is still live.
                Err(CellGroupArchiveError::Conflict | CellGroupArchiveError::Unconfirmed) => {
                    tokio::task::yield_now().await;
                }
                Err(error) => return Err(error),
            }
        }
    })
    .await??)
}

pub(super) async fn verify_sealed_cut(
    cluster: &mut ManagedCluster,
    fleet: &FleetCatalog,
    directory: &RangeDirectory,
    transition: &ReopenedManagedCellGroupTransition,
    parent: &CellGroupBirth,
    old_hold: CellGroupArchiveHold,
    seal: &GroupSourceSeal,
) -> Result<()> {
    let domain = cluster.fixtures[0].domain.clone();
    let archives = CellGroupArchivePool::new(Default::default())?;
    let tails = CellGroupArchiveTailPool::new(Default::default())?;
    let registry = directory
        .open_group_archive(domain.as_ref(), &archives, parent)
        .await?;
    let scratch = tempfile::tempdir()?;
    let rooted = directory.namespace_config().image_storage == RaftImageStorage::Extents;
    let source = |label: &str, hold: CellGroupArchiveHold| {
        let base = scratch.path().join(label);
        let route = parent.route().clone();
        let domain = domain.clone();
        async move {
            let receiver_store = domain
                .blocking(move || store(base, route, rooted))?
                .await??;
            let receiver = CellGroupCheckpointPool::new(checkpoint_limits())?.receive_archive(
                receiver_store.store,
                receiver_store.scratch,
                receiver_store.directory,
            )?;
            Ok::<_, anyhow::Error>(CellGroupTransitionSource {
                birth: parent.clone(),
                hold,
                receiver,
            })
        }
    };
    ensure!(matches!(
        fleet
            .verify_managed_sealed_sources(
                transition,
                directory,
                &archives,
                &tails,
                vec![source("pre-copy", old_hold).await?],
                TIMEOUT,
            )
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    tokio::time::timeout(TIMEOUT, async {
        loop {
            for host in &cluster.hosts[..3] {
                let group = host.group(parent.route().history)?;
                if group
                    .archive_status()?
                    .and_then(|status| status.published)
                    .is_some_and(|marker| marker.source_seal() == Some(seal))
                {
                    return Ok::<_, anyhow::Error>(());
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("managed archiver did not publish the sealed cut")??;
    let held = acquire_hold(&domain, &registry).await?;
    ensure!(held.marker().source_seal() == Some(seal));

    let first = fleet
        .verify_managed_sealed_sources(
            transition,
            directory,
            &archives,
            &tails,
            vec![source("verify", held.clone()).await?],
            TIMEOUT,
        )
        .await?;
    ensure!(first.journal().phase() == TransitionPhase::Sealed);
    ensure!(first.sources().len() == 1 && first.sources()[0].marker().source_seal() == Some(seal));
    let journal = first.journal().clone();
    let marker = first.sources()[0].marker().clone();
    ensure!(tails.usage().operations == 1);
    drop(first);
    ensure!(tails.usage().operations == 0);
    let reopened = domain
        .run(directory.sealed_source_holds(&transition.plan))
        .await??;
    ensure!(reopened == vec![held.clone()]);
    let replay = fleet
        .verify_managed_sealed_sources(
            transition,
            directory,
            &archives,
            &tails,
            vec![
                source(
                    "reopen",
                    reopened
                        .into_iter()
                        .next()
                        .context("selected source hold")?,
                )
                .await?,
            ],
            TIMEOUT,
        )
        .await?;
    ensure!(replay.journal() == &journal);
    ensure!(replay.sources()[0].marker() == &marker);

    let root = scratch.path().join("final");
    let routes = replay.children().to_vec();
    let targets = domain
        .blocking(move || {
            routes
                .into_iter()
                .enumerate()
                .map(|(index, route)| store(root.join(index.to_string()), route, rooted))
                .collect::<Result<Vec<_>>>()
        })?
        .await??;
    let prepared = recovery_pool()?
        .prepare_transfer_partition(domain.as_ref(), replay.clone(), targets)
        .await?;
    ensure!(prepared.children().len() == 2);
    ensure!(prepared
        .children()
        .iter()
        .all(|child| child.genesis().is_some()));
    ensure!(
        prepared
            .children()
            .iter()
            .map(|child| child.root().cells())
            .sum::<u32>()
            == 2
    );
    let first = prepared
        .children()
        .iter()
        .find(|child| child.root().cells() > 0)
        .context("retained restart requires a nonempty child")?
        .route()
        .history;
    drop(prepared);
    let transition = Arc::new(
        fleet
            .reopen_managed_transition(directory, transition.plan.operation(), TIMEOUT)
            .await?
            .context("reopen managed sealed transition for install")?,
    );
    let recovery = recovery_pool()?;
    ensure!(matches!(
        cluster.hosts[0]
            .install_managed_transfer_child(
                transition.clone(),
                replay.clone(),
                first,
                recovery.clone(),
                TIMEOUT,
            )
            .await,
        Err(CellGroupControllerError::Catalog(_))
    ));
    ensure!(matches!(
        cluster.hosts[0].group(first),
        Err(RuntimeError::NotFound)
    ));
    let mut retained = None;
    for route in replay.children() {
        let mut foreign_route = route.clone();
        foreign_route.epoch = RouteEpoch::new(route.epoch.get() + 1)?;
        ensure!(matches!(
            fleet
                .observe_managed_transfer_install_quorum(
                    &transition,
                    directory,
                    &foreign_route,
                    TIMEOUT,
                )
                .await,
            Err(FleetCatalogError::Conflict)
        ));
        ensure!(fleet
            .observe_managed_transfer_install_quorum(&transition, directory, route, TIMEOUT)
            .await?
            .is_none());
        let mut installed = Vec::new();
        for (index, host) in cluster.hosts[3..].iter().enumerate() {
            let receipt = host
                .install_managed_transfer_child(
                    transition.clone(),
                    replay.clone(),
                    route.history,
                    recovery.clone(),
                    TIMEOUT,
                )
                .await
                .with_context(|| format!("install pending voter {}", index + 1))?;
            ensure!(receipt.member.get() == index as u64 + 1);
            ensure!(receipt.node == host.identity().node && receipt.disk == host.identity().disk);
            ensure!(
                recovery.usage().operations == 0,
                "a completed install retained the recovery slot"
            );
            installed.push(receipt.clone());
            let observed = fleet
                .observe_managed_transfer_install_quorum(&transition, directory, route, TIMEOUT)
                .await?;
            if index == 0 {
                ensure!(observed.is_none());
                ensure!(matches!(
                    host.attach_managed_transfer_child(transition.clone(), route.clone(), TIMEOUT)
                        .await,
                    Err(CellGroupControllerError::Catalog(_))
                ));
            } else {
                let observed = observed.context("managed install majority")?;
                ensure!(observed.genesis.route() == route && observed.voters == installed);
            }
            let repeated = host
                .install_managed_transfer_child(
                    transition.clone(),
                    replay.clone(),
                    route.history,
                    recovery.clone(),
                    TIMEOUT,
                )
                .await?;
            ensure!(repeated == receipt);
            if route.history == first && index == 0 {
                retained = Some(receipt);
            }
            ensure!(matches!(
                host.group(route.history)?.status().await,
                Err(RuntimeError::NotFound)
            ));
        }
    }
    Box::pin(restart_destination(
        cluster,
        directory,
        transition.clone(),
        replay.clone(),
        first,
        recovery,
        retained.context("first destination receipt")?,
    ))
    .await?;
    let voters = &cluster.hosts[DESTINATION..];
    let mut proofs = Vec::with_capacity(replay.children().len());
    for route in replay.children() {
        if route.history != first {
            for voter in voters {
                voter
                    .attach_managed_transfer_child(transition.clone(), route.clone(), TIMEOUT)
                    .await
                    .context("attach second pending child")?;
            }
        }
        proofs.push(wait_pending_child(voters, route.history).await?);
    }
    verify_managed_installed_decision(cluster, fleet, directory, &transition, parent, proofs)
        .await?;
    let routes = replay.children().to_vec();
    drop(replay);
    ensure!(tails.usage().operations == 0);
    let root = scratch.path().join("archive");
    let target_routes = routes.clone();
    let targets = domain
        .blocking(move || {
            target_routes
                .into_iter()
                .enumerate()
                .map(|(index, route)| store(root.join(index.to_string()), route, rooted))
                .collect::<Result<Vec<_>>>()
        })?
        .await??;
    let inputs = CellGroupTransitionPreparationInputs {
        archives,
        tails,
        restore: recovery_pool()?,
        sources: vec![source("archive-source", held).await?],
        targets,
    };
    Box::pin(verify_managed_archive_decision(
        cluster,
        fleet,
        directory,
        &transition,
        parent,
        &routes,
        inputs,
    ))
    .await
}

async fn verify_managed_archive_decision(
    cluster: &ManagedCluster,
    fleet: &FleetCatalog,
    directory: &RangeDirectory,
    transition: &ReopenedManagedCellGroupTransition,
    parent: &CellGroupBirth,
    children: &[GroupRoute],
    inputs: CellGroupTransitionPreparationInputs,
) -> Result<()> {
    let fixture = &cluster.fixtures[0];
    let domain = &fixture.domain;
    let archives = inputs.archives.clone();
    let hold = inputs.sources[0].hold.clone();
    let before = domain.run(directory.observe()).await??;
    ensure!(fleet
        .reopen_managed_transfer_archive(transition, directory, TIMEOUT)
        .await?
        .is_none());

    ensure!(matches!(
        fleet
            .publish_managed_transfer(transition, directory, &archives, TIMEOUT)
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    fixture
        .store
        .fail_head_confirmation
        .store(true, Ordering::Release);
    ensure!(matches!(
        fleet
            .verify_managed_transfer_archive(transition, directory, inputs, TIMEOUT)
            .await,
        Err(FleetCatalogError::Unconfirmed)
    ));
    let reopened = fleet
        .reopen_managed_transition(directory, transition.plan.operation(), TIMEOUT)
        .await?
        .context("archived plan")?;
    let archived = reopened.journal.as_ref().context("archived journal")?;
    ensure!(archived.phase() == TransitionPhase::Archived);
    let closure = fleet
        .reopen_managed_transfer_archive(&reopened, directory, TIMEOUT)
        .await?
        .context("selected managed archive closure")?;
    ensure!(archived.transfer_archive() == Some(closure.digest));
    ensure!(closure.source_holds == vec![hold]);
    ensure!(closure.children.len() == children.len());
    ensure!(closure
        .children
        .iter()
        .zip(children)
        .all(|(marker, route)| { marker.route() == route && !marker.is_active() }));
    ensure!(
        closure
            .children
            .iter()
            .map(|marker| marker.root().cells())
            .sum::<u32>()
            == 2
    );
    let after = domain.run(directory.observe()).await??;
    ensure!(after.revision() == before.revision() && after.groups() == before.groups());
    ensure!(
        domain
            .run(directory.materialize_selected_history(parent.route().history))
            .await??
            .route()
            == parent.route()
    );
    Box::pin(verify_managed_publication(
        cluster,
        fleet,
        directory,
        &reopened,
        &archives,
        children,
        closure.digest,
    ))
    .await
}

async fn verify_managed_publication(
    cluster: &ManagedCluster,
    fleet: &FleetCatalog,
    directory: &RangeDirectory,
    transition: &ReopenedManagedCellGroupTransition,
    archives: &CellGroupArchivePool,
    children: &[GroupRoute],
    closure: [u8; 32],
) -> Result<()> {
    let fixture = &cluster.fixtures[0];
    let domain = &fixture.domain;
    let voters = &cluster.hosts[DESTINATION..];
    let before = domain.run(directory.observe()).await??;
    ensure!(matches!(
        voters[0]
            .group(children[0].history)?
            .activate_managed_transfer_child(transition, TIMEOUT)
            .await,
        Err(CellGroupControllerError::Catalog(_))
    ));

    let controller = Fixture::open_catalog(domain, fixture.store.clone(), cluster.history.cluster)?;
    let control = RaftFleetControl::new(domain, controller, vec![directory.clone()])?;
    let namespace = hex::encode(cluster.history.namespace.bytes());
    let operation = hex::encode(transition.plan.operation().bytes());
    let principal = &transition.request.principal;
    let status = control
        .transition_status(&namespace, &operation, TIMEOUT)
        .await?;
    ensure!(status.phase == Some(TransitionPhase::Archived));
    let archive_digest = status
        .transfer_archive_digest
        .context("archived operator cut")?;
    ensure!(archive_digest == hex::encode(closure));
    let publish = RaftFleetTransitionPublishRequest {
        catalog_id: status.catalog_id,
        namespace_id: namespace.clone(),
        operation_id: operation.clone(),
        request_id: hex::encode([218; 16]),
        plan_digest: status.plan_digest,
        archive_digest,
    };
    let wrong_archive = RaftFleetTransitionPublishRequest {
        archive_digest: hex::encode([33; 32]),
        ..publish.clone()
    };
    let refused = control
        .publish_transition(principal, &namespace, &operation, &wrong_archive, TIMEOUT)
        .await;
    ensure!(matches!(refused, Err(failure) if failure.code == RaftFleetFailureCode::Conflict));
    ensure!(domain.run(directory.observe()).await??.revision() == before.revision());

    fixture
        .store
        .fail_head_confirmation
        .store(true, Ordering::Release);
    let published = crate::managed_fixture::operator::publish_transition(
        fixture, directory, principal, &publish,
    )
    .await?;
    ensure!(!fixture.store.fail_head_confirmation.load(Ordering::Acquire));
    let publication = domain
        .run(directory.publication(&transition.plan))
        .await??
        .context("operator publication certificate")?;
    ensure!(
        publication.transfer_archive() == Some(closure)
            && published.directory_revision == before.revision() + 1
            && published.directory_revision == publication.revision()
            && published.publication_digest == hex::encode(publication.journal_digest())
            && Some(published.tree_digest.as_str())
                == publication.tree_digest().map(hex::encode).as_deref()
    );
    ensure!(
        control
            .publish_transition(principal, &namespace, &operation, &publish, TIMEOUT)
            .await?
            == published
    );
    let after = domain.run(directory.observe()).await??;
    let selected = domain.run(directory.group_page(&after, None, 3)).await??;
    ensure!(after.revision() == publication.revision());
    ensure!(selected.next.is_none() && selected.routes == children);
    let reopened = fleet
        .reopen_managed_transition(directory, transition.plan.operation(), TIMEOUT)
        .await?
        .context("published managed transfer")?;
    ensure!(
        reopened
            .journal
            .as_ref()
            .context("publication journal")?
            .phase()
            == TransitionPhase::Published
    );
    ensure!(
        fleet
            .publish_managed_transfer(&reopened, directory, archives, TIMEOUT)
            .await?
            == publication
    );
    ensure!(matches!(
        cluster.hosts[0]
            .group(cluster.history)?
            .retire_managed_source(&reopened, TIMEOUT)
            .await,
        Err(CellGroupControllerError::Catalog(_))
    ));
    ensure!(matches!(
        fleet
            .prepare_managed_transfer_child_archive(
                &reopened,
                directory,
                children[0].history,
                archives,
                TIMEOUT
            )
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    for route in children {
        for voter in voters {
            ensure!(!voter.group(route.history)?.status().await?.ready);
        }
    }

    let request = |index: usize| RaftFleetTransitionActivationRequest {
        catalog_id: control.catalog_id().to_owned(),
        namespace_id: namespace.clone(),
        operation_id: operation.clone(),
        request_id: hex::encode([219 + index as u8; 16]),
        plan_digest: hex::encode(reopened.plan.digest()),
        group_id: hex::encode(children[index].history.group.bytes()),
        incarnation_id: hex::encode(children[index].history.incarnation.bytes()),
    };
    let first = request(0);
    let leader = tokio::time::timeout(TIMEOUT, async {
        loop {
            for (index, voter) in voters.iter().enumerate() {
                if voter.group(children[0].history)?.status().await?.leader
                    == Some(index as u64 + 1)
                {
                    return Ok::<_, anyhow::Error>(voter);
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("operator needs a pending child leader")??;
    let applied = crate::managed_fixture::operator::activate_transition_child(
        fixture, directory, principal, leader, &first,
    )
    .await?;
    ensure!(applied.phase == TransitionPhase::Published && applied.applied.is_some());
    ensure!(
        control
            .activate_transition_child(principal, &namespace, &operation, &first, TIMEOUT)
            .await?
            == applied
    );
    for voter in voters {
        ensure!(!voter.group(children[1].history)?.status().await?.ready);
    }
    ensure!(matches!(
        cluster.hosts[0]
            .group(cluster.history)?
            .retire_managed_source(&reopened, TIMEOUT)
            .await,
        Err(CellGroupControllerError::Catalog(_))
    ));

    let mut acknowledgements = Vec::with_capacity(children.len());
    for route in children {
        acknowledgements.push(wait_managed_activation(voters, &reopened, route.history).await?);
    }
    ensure!(matches!(
        fleet
            .record_managed_transfer_activations(
                &reopened,
                directory,
                vec![acknowledgements[0].clone()],
                TIMEOUT
            )
            .await,
        Err(FleetCatalogError::Invalid(_))
    ));
    let mut reversed = acknowledgements.clone();
    reversed.reverse();
    ensure!(matches!(
        fleet
            .record_managed_transfer_activations(&reopened, directory, reversed, TIMEOUT)
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    let second = request(1);
    let second_leader = crate::managed_fixture::leader(voters, children[1].history).await?;
    let voter_control = control
        .clone()
        .with_node_observer(voters[second_leader].clone());
    fixture
        .store
        .fail_head_confirmation
        .store(true, Ordering::Release);
    let lost = voter_control
        .activate_transition_child(principal, &namespace, &operation, &second, TIMEOUT)
        .await;
    ensure!(matches!(lost, Err(failure) if failure.code == RaftFleetFailureCode::Unavailable));
    ensure!(!fixture.store.fail_head_confirmation.load(Ordering::Acquire));
    let confirmed = control
        .activate_transition_child(principal, &namespace, &operation, &second, TIMEOUT)
        .await?;
    let position = acknowledgements[1].position();
    ensure!(
        confirmed.phase == TransitionPhase::Active
            && confirmed
                .applied
                .is_some_and(|applied| applied.index == position.index.get()
                    && applied.term == position.term.get())
    );
    let active = fleet
        .reopen_managed_transition(directory, transition.plan.operation(), TIMEOUT)
        .await?
        .context("active managed transfer")?;
    let journal = active.journal.as_ref().context("activation journal")?;
    ensure!(journal.phase() == TransitionPhase::Active && journal.activated_children().is_some());
    ensure!(
        fleet
            .record_managed_transfer_activations(
                &active,
                directory,
                acknowledgements.clone(),
                TIMEOUT
            )
            .await?
            == *journal
    );

    let positions = children
        .iter()
        .zip(&acknowledgements)
        .map(|(route, ack)| (route.history, ack.position().index))
        .collect::<Vec<_>>();
    wait_transfer_applied(voters, &positions).await
}

pub(super) async fn wait_transfer_applied(
    voters: &[RaftCells],
    positions: &[(GroupHistory, LogIndex)],
) -> Result<()> {
    tokio::time::timeout(TIMEOUT, async {
        loop {
            let mut applied = true;
            for (history, index) in positions {
                for voter in voters {
                    applied &= voter.group(*history)?.status().await?.applied.index >= *index;
                }
            }
            if applied {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("transfer voter apply did not reach the committed position")?
}

pub(super) async fn wait_managed_activation(
    voters: &[RaftCells],
    transition: &ReopenedManagedCellGroupTransition,
    history: GroupHistory,
) -> Result<CellGroupTransferActivationAck> {
    use CellGroupTransferActivation::{Accepted, Applied};
    let end = std::time::Instant::now() + TIMEOUT;
    loop {
        for voter in voters {
            let remaining = end.saturating_duration_since(std::time::Instant::now());
            ensure!(
                !remaining.is_zero(),
                "managed transfer child did not activate"
            );
            match voter
                .group(history)?
                .activate_managed_transfer_child(transition, remaining)
                .await
            {
                Ok(Applied(acknowledgement)) => return Ok(*acknowledgement),
                Ok(Accepted(_))
                | Err(CellGroupControllerError::Runtime(
                    RuntimeError::NotLeader | RuntimeError::NotReady,
                )) => (),
                Err(error) => return Err(error.into()),
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

pub(super) async fn verify_transferred_sql(
    cluster: &mut ManagedCluster,
    directory: &RangeDirectory,
    seeded: SeededCells,
) -> Result<()> {
    let domain = cluster.fixtures[0].domain.clone();
    let view = domain.run(directory.observe()).await??;
    let hosts = &cluster.hosts[DESTINATION..];
    let mut positions = Vec::with_capacity(seeded.cells.len());
    for (index, (cell, original)) in seeded
        .cells
        .iter()
        .cloned()
        .zip(seeded.receipts)
        .enumerate()
    {
        let route = domain.run(directory.lookup(&view, &cell.cell)).await??;
        let leader = leader(hosts, route.history).await?;
        let handle = hosts[leader].cell(cell)?;
        let get = handle.contract("Get")?;
        let query = handle
            .query(
                ProposalId::new([190 + index as u8; 16])?,
                &get,
                vec![],
                QueryOptions::default(),
            )
            .await?;
        let expected = [10i64, 11][index];
        ensure!(query.result == expected.to_be_bytes());
        let add = handle.contract("Add")?;
        let replay = handle
            .command(
                &add,
                original.result.clone(),
                CommandOptions::new(original.id, seeded.retry),
            )
            .await?;
        ensure!(replay.id == original.id && replay.result == original.result);
        let next = handle
            .command(
                &add,
                1i64.to_be_bytes().to_vec(),
                CommandOptions::new(
                    CommandId::new(original.id.client, [4, 2][index])?,
                    seeded.retry,
                ),
            )
            .await?;
        ensure!(next.result == (expected + 1).to_be_bytes() && next.witness.group == route.history);
        positions.push((route.history, next.witness.position.index));
    }
    wait_transfer_applied(hosts, &positions).await?;
    reopen_transfer_destination(cluster, 111, 2, true).await?;
    let restarted = &cluster.hosts[DESTINATION];
    let view = domain.run(directory.observe()).await??;
    for (index, cell) in seeded.cells.into_iter().enumerate() {
        let route = domain.run(directory.lookup(&view, &cell.cell)).await??;
        let group = restarted.group(route.history)?;
        ensure!(group.archive_status()?.is_none());
        let handle = restarted.cell(cell)?;
        let query = handle
            .query(
                ProposalId::new([194 + index as u8; 16])?,
                &handle.contract("Get")?,
                vec![],
                QueryOptions {
                    routing: QueryRouting::Local,
                    ..Default::default()
                },
            )
            .await?;
        ensure!(query.result == [11i64, 12][index].to_be_bytes());
        ensure!(query.served_by_node == restarted.identity());
    }
    Ok(())
}

pub(super) async fn initialize_transfer_archive(
    fleet: &FleetCatalog,
    directory: &RangeDirectory,
    transition: &ReopenedManagedCellGroupTransition,
    history: GroupHistory,
    archives: &CellGroupArchivePool,
    path: &std::path::Path,
    domain: &ExecutionDomain,
) -> Result<()> {
    use std::io::Write as _;

    let init = fleet
        .prepare_managed_transfer_child_archive(transition, directory, history, archives, TIMEOUT)
        .await?;
    let mut intent = std::fs::File::create_new(path)?;
    let encoded = init.encoded_intent().to_vec();
    intent.write_all(&encoded)?;
    intent.sync_all()?;
    std::fs::File::open(path.parent().context("child archive intent directory")?)?.sync_all()?;
    init.commit(domain).await?;
    fleet
        .verify_managed_transfer_child_archive(
            transition, directory, history, archives, &encoded, TIMEOUT,
        )
        .await?;
    ensure!(matches!(
        fleet
            .prepare_managed_transfer_child_archive(
                transition, directory, history, archives, TIMEOUT
            )
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    Ok(())
}

pub(super) async fn wait_active_archive(
    host: &RaftCells,
    history: GroupHistory,
) -> Result<LogIndex> {
    let group = host.group(history)?;
    let applied = group.status().await?.applied.index;
    tokio::time::timeout(TIMEOUT, async {
        loop {
            if group
                .archive_status()?
                .and_then(|status| status.published)
                .is_some_and(|marker| marker.position().index >= applied && marker.is_active())
            {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("child archiver did not publish its active cut")??;
    Ok(applied)
}

pub(super) async fn verify_child_archive_coverage(
    cluster: &mut ManagedCluster,
    directory: &RangeDirectory,
    transition: &ReopenedManagedCellGroupTransition,
    tombstone: CellGroupSourceRetirementAck,
    cells: Vec<CellHistory>,
) -> Result<()> {
    let domain = cluster.fixtures[0].domain.clone();
    let fleet = cluster.fixtures[0].catalog.clone();
    let archives = CellGroupArchivePool::new(Default::default())?;
    ensure!(matches!(
        fleet
            .settle_managed_transfer_retirement(
                transition,
                directory,
                &archives,
                vec![tombstone.clone()],
                TIMEOUT
            )
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    let tails = CellGroupArchiveTailPool::new(Default::default())?;
    let scratch = tempfile::tempdir()?;
    let rooted = directory.namespace_config().image_storage == RaftImageStorage::Extents;
    let view = domain.run(directory.observe()).await??;
    let children = domain
        .run(directory.group_page(&view, None, 3))
        .await??
        .routes;
    ensure!(children.len() == 2);
    for (index, route) in children.iter().enumerate() {
        initialize_transfer_archive(
            &fleet,
            directory,
            transition,
            route.history,
            &archives,
            &scratch.path().join(format!("child-{index}.intent")),
            &domain,
        )
        .await?;
    }

    reopen_transfer_destination(cluster, 113, 2, true).await?;
    let host = &cluster.hosts[DESTINATION];
    let mut sources = Vec::with_capacity(children.len());
    let mut holds = Vec::with_capacity(children.len());
    for (index, route) in children.into_iter().enumerate() {
        let applied = wait_active_archive(host, route.history).await?;
        let birth = domain
            .run(directory.materialize_selected_history(route.history))
            .await??;
        let registry = directory
            .open_group_archive(domain.as_ref(), &archives, &birth)
            .await?;
        let hold = acquire_hold(&domain, &registry).await?;
        ensure!(hold.marker().position().index >= applied);
        let base = scratch.path().join(format!("child-verifier-{index}"));
        let stored = domain
            .blocking(move || store(base, route, rooted))?
            .await??;
        let receiver = CellGroupCheckpointPool::new(checkpoint_limits())?.receive_archive(
            stored.store,
            stored.scratch,
            stored.directory,
        )?;
        holds.push(hold.clone());
        sources.push(CellGroupTransitionSource {
            birth,
            hold,
            receiver,
        });
    }
    ensure!(fleet
        .reopen_managed_transfer_child_holds(transition, directory, TIMEOUT)
        .await?
        .is_none());
    cluster.fixtures[0]
        .store
        .fail_head_confirmation
        .store(true, Ordering::Release);
    let recorded = fleet
        .verify_managed_transfer_child_archives(
            transition, directory, &archives, &tails, sources, TIMEOUT,
        )
        .await;
    ensure!(
        matches!(recorded, Err(FleetCatalogError::Unconfirmed)),
        "child archive CAS result: {recorded:?}"
    );
    let reopened = fleet
        .reopen_managed_transition(directory, transition.plan.operation(), TIMEOUT)
        .await?
        .context("managed child archive decision")?;
    let journal = reopened.journal.as_ref().context("child archive journal")?;
    ensure!(
        journal.phase() == TransitionPhase::Active
            && journal.child_archives().is_some()
            && !journal.parent_holds_released()
    );
    ensure!(
        fleet
            .reopen_managed_transfer_child_holds(&reopened, directory, TIMEOUT)
            .await?
            .as_deref()
            == Some(holds.as_slice())
    );
    let reserved = fleet.read(TIMEOUT).await?;
    let early = FleetRequestIdentity {
        operation: reopened.request.operation,
        ..request(237, reserved.revision())
    };
    ensure!(matches!(
        fleet
            .confirm_managed_transfer_children(
                &reserved, &early, &reopened, directory, &archives, TIMEOUT
            )
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    ensure!(matches!(
        fleet
            .settle_managed_transfer_retirement(
                &reopened,
                directory,
                &archives,
                Vec::new(),
                TIMEOUT
            )
            .await,
        Err(FleetCatalogError::Invalid(_))
    ));
    cluster.fixtures[0]
        .store
        .fail_head_confirmation
        .store(true, Ordering::Release);
    let lost = fleet
        .settle_managed_transfer_retirement(
            &reopened,
            directory,
            &archives,
            vec![tombstone.clone()],
            TIMEOUT,
        )
        .await;
    ensure!(
        matches!(lost, Err(FleetCatalogError::Unconfirmed)),
        "retirement CAS result: {lost:?}"
    );
    let retired = fleet
        .settle_managed_transfer_retirement(
            &reopened,
            directory,
            &archives,
            vec![tombstone],
            TIMEOUT,
        )
        .await?;
    ensure!(
        retired.phase() == TransitionPhase::Retired
            && retired.retired_parents().is_some()
            && retired.child_archives() == journal.child_archives()
            && !retired.parent_holds_released()
    );
    ensure!(
        domain
            .run(directory.active_transition())
            .await??
            .context("retirement retains topology ownership")?
            .1
            == retired
    );
    ensure!(matches!(
        cluster.hosts[DESTINATION]
            .group(holds[0].marker().route().history)?
            .activate_managed_transfer_child(&reopened, TIMEOUT)
            .await,
        Err(CellGroupControllerError::Catalog(_))
    ));
    reopen_transfer_destination(cluster, 114, 2, true)
        .await
        .context("reopen activated child voters during retirement cleanup")?;
    Box::pin(verify_live_transfer_children(
        cluster, directory, &reopened, &archives, &holds, cells,
    ))
    .await
}

async fn verify_live_transfer_children(
    cluster: &mut ManagedCluster,
    directory: &RangeDirectory,
    transition: &ReopenedManagedCellGroupTransition,
    archives: &CellGroupArchivePool,
    holds: &[CellGroupArchiveHold],
    cells: Vec<CellHistory>,
) -> Result<()> {
    let domain = cluster.fixtures[0].domain.clone();
    let fleet = cluster.fixtures[0].catalog.clone();
    let reserved = fleet.read(TIMEOUT).await?;
    let confirmation = FleetRequestIdentity {
        operation: transition.request.operation,
        ..request(238, reserved.revision())
    };
    for hold in holds {
        let history = hold.marker().route().history;
        ensure!(
            *fleet.history(&reserved, &history, TIMEOUT).await?.state()
                == FleetHistoryState::Reserved
        );
    }
    cluster.fixtures[0]
        .store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    let lost = fleet
        .confirm_managed_transfer_children(
            &reserved,
            &confirmation,
            transition,
            directory,
            archives,
            TIMEOUT,
        )
        .await;
    ensure!(
        matches!(lost, Err(FleetCatalogError::Storage(_))),
        "promotion CAS result: {:?}",
        lost.as_ref().err()
    );
    let live = fleet.read(TIMEOUT).await?;
    ensure!(live.revision() == reserved.revision() + 1);
    for hold in holds {
        let history = hold.marker().route().history;
        ensure!(*fleet.history(&live, &history, TIMEOUT).await?.state() == FleetHistoryState::Live);
    }
    ensure!(
        fleet
            .confirm_managed_transfer_children(
                &live,
                &confirmation,
                transition,
                directory,
                archives,
                TIMEOUT,
            )
            .await?
            .applied_revision
            == live.revision()
    );
    ensure!(domain.run(directory.active_transition()).await??.is_some());
    reopen_transfer_destination(cluster, 115, 2, true)
        .await
        .context("reopen Live transfer children using permanent proofs")?;
    let view = domain.run(directory.observe()).await??;
    let voters = &cluster.hosts[DESTINATION..];
    let retry = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?
    .checked_add(60_000)
    .context("retry deadline")?;
    for (index, cell) in cells.into_iter().enumerate() {
        let route = domain.run(directory.lookup(&view, &cell.cell)).await??;
        let reopened = &voters[0];
        ensure!(reopened.group(route.history)?.archive_status()?.is_some());
        let handle = reopened.cell(cell.clone())?;
        let query = handle
            .query(
                ProposalId::new([196 + index as u8; 16])?,
                &handle.contract("Get")?,
                vec![],
                QueryOptions {
                    routing: QueryRouting::Local,
                    ..Default::default()
                },
            )
            .await?;
        ensure!(query.result == [11i64, 12][index].to_be_bytes());
        ensure!(query.served_by_node == reopened.identity());
        let leader = &voters[leader(voters, route.history).await?];
        let handle = leader.cell(cell)?;
        let receipt = handle
            .command(
                &handle.contract("Add")?,
                1i64.to_be_bytes().to_vec(),
                CommandOptions::new(CommandId::new(ClientId::new([201; 16])?, 1)?, retry),
            )
            .await?;
        ensure!(receipt.result == [12i64, 13][index].to_be_bytes());
        ensure!(receipt.witness.group == route.history);
    }
    Ok(())
}

pub(super) async fn verify_released_source_holds(
    cluster: &ManagedCluster,
    directory: &RangeDirectory,
    plan: &DirectoryMutation,
    registry: &CellGroupArchiveRegistry,
    preparation: CellGroupArchiveHold,
) -> Result<()> {
    let domain = &cluster.fixtures[0].domain;
    let mut holds = domain.run(directory.sealed_source_holds(plan)).await??;
    ensure!(holds.len() == 1);
    holds.push(preparation);
    verify_expired_holds(domain, directory, registry, holds).await
}

pub(super) async fn verify_expired_holds(
    domain: &ExecutionDomain,
    directory: &RangeDirectory,
    registry: &CellGroupArchiveRegistry,
    holds: Vec<CellGroupArchiveHold>,
) -> Result<()> {
    let scratch = tempfile::tempdir()?;
    let rooted = directory.namespace_config().image_storage == RaftImageStorage::Extents;
    let tails = CellGroupArchiveTailPool::new(Default::default())?;
    for (index, hold) in holds.into_iter().enumerate() {
        let base = scratch.path().join(format!("released-source-{index}"));
        let route = hold.marker().route().clone();
        let stored = domain
            .blocking(move || store(base, route, rooted))?
            .await??;
        let receiver = CellGroupCheckpointPool::new(checkpoint_limits())?.receive_archive(
            stored.store,
            stored.scratch,
            stored.directory,
        )?;
        ensure!(matches!(
            registry.verify_hold(domain, &hold, &tails, receiver).await,
            Err(CellGroupArchiveError::Expired)
        ));
    }
    Ok(())
}

async fn verify_managed_installed_decision(
    cluster: &ManagedCluster,
    fleet: &FleetCatalog,
    directory: &RangeDirectory,
    transition: &ReopenedManagedCellGroupTransition,
    parent: &CellGroupBirth,
    proofs: Vec<TransferChildQuorum>,
) -> Result<()> {
    let fixture = &cluster.fixtures[0];
    let domain = &fixture.domain;
    let before = domain.run(directory.observe()).await??;
    ensure!(matches!(
        fleet
            .record_managed_transfer_child_quorums(
                transition,
                directory,
                vec![proofs[0].clone()],
                TIMEOUT
            )
            .await,
        Err(FleetCatalogError::Invalid(_))
    ));
    let mut reversed = proofs.clone();
    reversed.reverse();
    ensure!(matches!(
        fleet
            .record_managed_transfer_child_quorums(transition, directory, reversed, TIMEOUT)
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    let operation = transition.plan.operation();
    let prior = fleet
        .reopen_managed_transition(directory, operation, TIMEOUT)
        .await?
        .context("sealed plan")?;
    ensure!(prior.journal.context("sealed journal")?.phase() == TransitionPhase::Sealed);
    fixture
        .store
        .fail_head_confirmation
        .store(true, Ordering::Release);
    ensure!(matches!(
        fleet
            .record_managed_transfer_child_quorums(transition, directory, proofs.clone(), TIMEOUT)
            .await,
        Err(FleetCatalogError::Unconfirmed)
    ));
    let reopened = fleet
        .reopen_managed_transition(directory, operation, TIMEOUT)
        .await?
        .context("installed plan")?;
    let installed = reopened.journal.as_ref().context("installed journal")?;
    ensure!(
        installed.phase() == TransitionPhase::Installed && installed.installed_children().is_some()
    );
    ensure!(
        fleet
            .record_managed_transfer_child_quorums(&reopened, directory, proofs, TIMEOUT)
            .await?
            == *installed
    );
    let after = domain.run(directory.observe()).await??;
    ensure!(after.revision() == before.revision() && after.groups() == before.groups());
    ensure!(
        domain
            .run(directory.materialize_selected_history(parent.route().history))
            .await??
            .route()
            == parent.route()
    );
    Ok(())
}

async fn restart_destination(
    cluster: &mut ManagedCluster,
    directory: &RangeDirectory,
    transition: Arc<ReopenedManagedCellGroupTransition>,
    sources: VerifiedCellGroupTransitionSources,
    child: GroupHistory,
    recovery: CellGroupRecoveryPool,
    expected: InstalledTransferVoter,
) -> Result<()> {
    reopen_transfer_destination(cluster, 109, 0, false).await?;
    let fixture = &cluster.fixtures[DESTINATION];
    let host = &cluster.hosts[DESTINATION];
    for route in sources.children() {
        ensure!(matches!(
            host.group(route.history)?.status().await,
            Err(RuntimeError::NotFound)
        ));
    }
    ensure!(matches!(
        host.install_managed_transfer_child(
            transition.clone(),
            sources.clone(),
            child,
            recovery.clone(),
            TIMEOUT,
        )
        .await,
        Err(CellGroupControllerError::Catalog(_))
    ));
    let route = sources
        .children()
        .iter()
        .find(|route| route.history == child)
        .cloned()
        .context("retained child route")?;
    let historical = fixture
        .catalog
        .observe_managed_transfer_install_quorum(&transition, directory, &route, TIMEOUT)
        .await?
        .context("durable receipts survive an ordinary boot")?;
    ensure!(historical.voters.len() == 3 && historical.voters.contains(&expected));
    ensure!(matches!(
        host.attach_managed_transfer_child(transition.clone(), route.clone(), TIMEOUT)
            .await,
        Err(CellGroupControllerError::Catalog(_))
    ));
    admit_capacity(fixture, host, directory, 225).await?;
    let reopened = host
        .install_managed_transfer_child(
            transition.clone(),
            sources.clone(),
            child,
            recovery.clone(),
            TIMEOUT,
        )
        .await?;
    ensure!(reopened == expected);
    ensure!(matches!(
        host.group(child)?.status().await,
        Err(RuntimeError::NotFound)
    ));
    let voters = &cluster.hosts[DESTINATION..];
    // Boot and capacity are rooted before peer discovery installs the new
    // endpoint. The transport fences the old process during that interval.
    let revision = fixture.catalog.read(TIMEOUT).await?.revision();
    for voter in &voters[1..] {
        inspect_current(voter, host.identity().node, revision)
            .await
            .context("discover restarted pending voter before peer attachment")?;
    }
    voters[0]
        .attach_managed_transfer_child(transition.clone(), route.clone(), TIMEOUT)
        .await
        .context("attach restarted pending voter before its peers")?;
    ensure!(matches!(
        voters[0]
            .group(child)?
            .managed_pending_transfer_quorum(TIMEOUT)
            .await,
        Err(CellGroupControllerError::Runtime(
            RuntimeError::NotLeader | RuntimeError::NotReady
        ))
    ));
    for voter in &voters[1..] {
        voter
            .attach_managed_transfer_child(transition.clone(), route.clone(), TIMEOUT)
            .await
            .context("attach peer of restarted pending voter")?;
    }
    wait_pending_child(voters, child).await?;
    for voter in voters {
        voter
            .attach_managed_transfer_child(transition.clone(), route.clone(), TIMEOUT)
            .await
            .context("reconcile an attached pending voter")?;
    }
    reopen_transfer_destination(cluster, 110, 1, false).await?;
    let fixture = &cluster.fixtures[DESTINATION];
    let host = &cluster.hosts[DESTINATION];
    ensure!(matches!(
        host.group(child)?.status().await,
        Err(RuntimeError::NotFound)
    ));
    ensure!(matches!(
        host.attach_managed_transfer_child(transition.clone(), route.clone(), TIMEOUT)
            .await,
        Err(CellGroupControllerError::Catalog(_))
    ));
    admit_capacity(fixture, host, directory, 227).await?;
    ensure!(
        host.install_managed_transfer_child(transition.clone(), sources, child, recovery, TIMEOUT)
            .await?
            == expected
    );
    host.attach_managed_transfer_child(transition, route, TIMEOUT)
        .await
        .context("reattach retained pending voter after the fresh term")?;
    wait_pending_child(&cluster.hosts[DESTINATION..], child).await?;
    Ok(())
}

const DESTINATION: usize = 3;

async fn reopen_transfer_destination(
    cluster: &mut ManagedCluster,
    generation: u8,
    pending_quorums: usize,
    activated: bool,
) -> Result<()> {
    let old = cluster.nodes.remove(DESTINATION);
    let old_host = cluster.hosts.remove(DESTINATION);
    let previous = old_host.identity();
    stop_transfer_destination(old, &old_host, 2, pending_quorums, activated).await?;
    let fixture = &cluster.fixtures[DESTINATION];
    let node = start_node(
        cluster.roots[DESTINATION].path(),
        fixture.application.clone(),
        &fixture.spec.label,
    )
    .await?;
    let startup = async {
        let (mut config, services) = fixture
            .config(&node, LogOpenMode::Resume, generation)
            .await?;
        crate::managed_fixture::archives::configure(&mut config);
        node.start_raft_cells(config, services).await
    }
    .await;
    let host = match startup {
        Ok(host) => host,
        Err(error) => {
            let stopped = node.shutdown(ShutdownMode::Preserve).await;
            return Err(error.context(format!("retained transfer startup, shutdown={stopped:?}")));
        }
    };
    cluster.nodes.insert(DESTINATION, node);
    cluster.hosts.insert(DESTINATION, host);
    let host = &cluster.hosts[DESTINATION];
    ensure!(host.identity().process != previous.process);
    ensure!(host.observe_node(TIMEOUT).await?.node.groups == 2);
    Ok(())
}

pub(super) async fn wait_pending_child(
    voters: &[RaftCells],
    child: GroupHistory,
) -> Result<TransferChildQuorum> {
    let mut last = vec![None; voters.len()];
    tokio::time::timeout(TIMEOUT, async {
        loop {
            let mut applied = true;
            let mut witness = None;
            for (index, voter) in voters.iter().enumerate() {
                let group = voter.group(child)?;
                let status = group.status().await?;
                ensure!(!status.ready, "a pending child served before publication");
                applied &= status.applied.index.get() > 1;
                last[index] = Some((status, false));
                match group.managed_pending_transfer_quorum(TIMEOUT).await {
                    Ok(proof) => {
                        if let Some((_, observed)) = &mut last[index] {
                            *observed = true;
                        }
                        witness = Some(proof);
                    }
                    Err(CellGroupControllerError::Runtime(
                        RuntimeError::NotLeader | RuntimeError::NotReady,
                    )) => (),
                    Err(error) => return Err(error.into()),
                }
            }
            if let (true, Some(witness)) = (applied, witness) {
                return Ok::<_, anyhow::Error>(witness);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .with_context(|| format!("pending transfer child {child:?} must commit a fresh term, last voter status and proof: {last:?}"))?
}
