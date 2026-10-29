// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Public startup and catalog boundary tests. The separate Docker qualification
//! supplies real infrastructure fencing; this fixture models controller outcomes.
use super::*;
use futures_util::future::BoxFuture;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::time::Duration;

struct Fence {
    stopped: AtomicBool,
    calls: AtomicUsize,
}

impl Fence {
    fn new(stopped: bool) -> Self {
        Self {
            stopped: AtomicBool::new(stopped),
            calls: AtomicUsize::new(0),
        }
    }
}

impl DisasterFenceAuthority for Fence {
    fn fence<'a>(&'a self, request: &'a DisasterFenceRequest) -> BoxFuture<'a, Result<Vec<u8>>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            ensure!(
                self.stopped.load(Ordering::SeqCst),
                "controller cannot confirm old deployment stopped"
            );
            Ok(request.digest().to_vec())
        })
    }

    fn verify<'a>(
        &'a self,
        request: &'a DisasterFenceRequest,
        evidence: &'a [u8],
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            ensure!(
                self.stopped.load(Ordering::SeqCst) && evidence == request.digest(),
                "fence receipt mismatch"
            );
            Ok(())
        })
    }
}

fn request(source: &GroupBirth, archive: ArchiveReference) -> Result<DisasterRecoveryRequest> {
    let mut scope = source.scope().clone();
    scope.group = GroupId::new([91; 16])?;
    scope.incarnation = IncarnationId::new([92; 16])?;
    Ok(DisasterRecoveryRequest {
        source: source.scope().clone(),
        archive,
        replacement: BirthProposal {
            scope,
            voters: vec![InitialMember {
                member: MemberId::new(1)?,
                node: NodeId::new([21; 16])?,
            }],
            learners: vec![],
        },
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disaster_and_fleet_import_share_an_exclusive_admission_boundary() -> Result<()> {
    for freeze_first in [false, true] {
        let mut fixture = Fixture::new(ArchiveLimits::default()).await?;
        let cut = fixture.commit(7, vec![]).await?;
        let reference = fixture.publish(cut).await?.reference();
        let request = request(&fixture.birth, reference)?;
        let fence = Fence::new(true);
        let operation = cellar_logic::consensus::ProposalId::new([81; 16])?;
        if freeze_first {
            fixture
                .domain
                .run(
                    fixture
                        .namespace
                        .freeze_births(operation, Duration::from_secs(5)),
                )
                .await??;
            ensure!(matches!(
                fixture
                    .namespace
                    .disaster_fence_request(request.clone())
                    .await,
                Err(CatalogError::BirthsFrozen)
            ));
            ensure!(matches!(
                fixture
                    .namespace
                    .recover_from_disaster(
                        request,
                        &fence,
                        LtxHost::default(),
                        ArchiveLimits::default()
                    )
                    .await,
                Err(CatalogError::BirthsFrozen)
            ));
            ensure!(fence.calls.load(Ordering::SeqCst) == 0);
            ensure!(
                fixture
                    .namespace
                    .birth(fixture.birth.scope().cell.as_str())
                    .await?
                    .context("birth")?
                    .digest()
                    == fixture.birth.digest()
            );
        } else {
            let successor = fixture
                .namespace
                .recover_from_disaster(
                    request.clone(),
                    &fence,
                    LtxHost::default(),
                    ArchiveLimits::default(),
                )
                .await?;
            ensure!(successor.scope() == &request.replacement.scope);
            ensure!(matches!(
                fixture
                    .domain
                    .run(
                        fixture
                            .namespace
                            .freeze_births(operation, Duration::from_secs(5))
                    )
                    .await?,
                Err(CatalogError::Conflict(
                    "disaster namespace cannot enter managed ownership"
                ))
            ));
            let retry = fixture
                .namespace
                .recover_from_disaster(
                    request,
                    &fence,
                    LtxHost::default(),
                    ArchiveLimits::default(),
                )
                .await?;
            ensure!(retry.digest() == successor.digest());
        }
        fixture.close().await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disaster_concurrent_freeze_has_one_admission_winner() -> Result<()> {
    let mut fixture = Fixture::new(ArchiveLimits::default()).await?;
    let cut = fixture.commit(7, vec![]).await?;
    let reference = fixture.publish(cut).await?.reference();
    let request = request(&fixture.birth, reference)?;
    let fence = Fence::new(true);
    let operation = cellar_logic::consensus::ProposalId::new([82; 16])?;
    let (freeze, recovery) = fixture
        .domain
        .run(async {
            tokio::join!(
                fixture
                    .namespace
                    .freeze_births(operation, Duration::from_secs(5)),
                fixture.namespace.recover_from_disaster(
                    request,
                    &fence,
                    LtxHost::default(),
                    ArchiveLimits::default()
                )
            )
        })
        .await?;
    match (freeze, recovery) {
        (Ok(_), Err(CatalogError::BirthsFrozen)) => {
            ensure!(fence.calls.load(Ordering::SeqCst) == 0);
        }
        (
            Err(CatalogError::Conflict("disaster namespace cannot enter managed ownership")),
            Ok(_),
        ) => (),
        (freeze, recovery) => anyhow::bail!(
            "expected exactly one admission winner: freeze={:?}, recovery={recovery:?}",
            freeze.map(|_| ())
        ),
    }
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disaster_refuses_managed_namespace_before_fencing() -> Result<()> {
    let mut fixture = Fixture::new(ArchiveLimits::default()).await?;
    let cut = fixture.commit(7, vec![]).await?;
    let reference = fixture.publish(cut).await?.reference();
    let store = Arc::new(faults::Store::default());
    let catalog = Catalog::new(store.clone(), CatalogOptions::default())?;
    let mut config = fixture.namespace.config().clone();
    config.voters = VoterPolicy::ThreeVoters;
    config.managed_fleet = Some(serde_json::from_value(
        serde_json::json!({"fleet": vec![71; 16], "catalog": vec![72; 32]}),
    )?);
    let namespace = catalog.open_namespace(config).await?;
    let before = store.inner.list(None).try_collect::<Vec<_>>().await?;
    let fence = Fence::new(true);
    let request = request(&fixture.birth, reference)?;
    ensure!(matches!(
        namespace.disaster_fence_request(request.clone()).await,
        Err(CatalogError::ManagedAdmissionRequired)
    ));
    ensure!(matches!(
        namespace
            .recover_from_disaster(
                request,
                &fence,
                LtxHost::default(),
                ArchiveLimits::default()
            )
            .await,
        Err(CatalogError::ManagedAdmissionRequired)
    ));
    ensure!(fence.calls.load(Ordering::SeqCst) == 0);
    ensure!(store.inner.list(None).try_collect::<Vec<_>>().await? == before);
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disaster_wrong_representation_refuses_before_intent_or_fence() -> Result<()> {
    let mut fixture = Fixture::new(ArchiveLimits::default()).await?;
    let cut = fixture.commit(7, vec![]).await?;
    let reference = fixture.publish(cut).await?.reference();
    let store = Arc::new(faults::Store::default());
    let catalog = Catalog::new(store.clone(), CatalogOptions::default())?;
    let mut config = fixture.namespace.config().clone();
    config.image_storage = RaftImageStorage::Extents;
    let namespace = catalog.open_namespace(config).await?;
    let birth = namespace
        .resolve_birth(BirthProposal {
            scope: fixture.birth.scope().clone(),
            voters: fixture.birth.voters().to_vec(),
            learners: vec![],
        })
        .await?;
    // Copy the materialized archive itself, without the incompatible catalog.
    // The refusal must be representation validation, not a missing object.
    for object in fixture
        .store
        .inner
        .list(None)
        .try_collect::<Vec<_>>()
        .await?
    {
        if object.location.as_ref().contains("/archives/") {
            let bytes = fixture
                .store
                .inner
                .get(&object.location)
                .await?
                .bytes()
                .await?;
            store.inner.put(&object.location, bytes.into()).await?;
        }
    }
    let before = store.inner.list(None).try_collect::<Vec<_>>().await?;
    let fence = Fence::new(true);
    let request = request(&birth, reference)?;
    namespace.disaster_fence_request(request.clone()).await?;
    ensure!(matches!(
        namespace
            .recover_from_disaster(
                request,
                &fence,
                LtxHost::default(),
                ArchiveLimits::default()
            )
            .await,
        Err(CatalogError::Archive(ArchiveError::Corrupt(
            "archive image representation"
        )))
    ));
    ensure!(fence.calls.load(Ordering::SeqCst) == 0);
    ensure!(store.inner.list(None).try_collect::<Vec<_>>().await? == before);
    ensure!(
        namespace
            .birth(&birth.scope().cell)
            .await?
            .context("birth")?
            .digest()
            == birth.digest()
    );
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disaster_fence_failure_is_closed_and_same_intent_resumes() -> Result<()> {
    let mut fixture = Fixture::new(ArchiveLimits::default()).await?;
    let cut = fixture.commit_payload(7, vec![], false).await?;
    fixture.publish(cut).await?;
    let reference = fixture
        .archive
        .recovery_metadata()
        .await?
        .context("head")?
        .reference();
    let request = request(&fixture.birth, reference)?;
    let fence = Fence::new(false);
    ensure!(matches!(
        fixture
            .namespace
            .recover_from_disaster(
                request.clone(),
                &fence,
                LtxHost::default(),
                ArchiveLimits::default()
            )
            .await,
        Err(CatalogError::Fence(_))
    ));
    ensure!(matches!(
        fixture.namespace.birth(&request.source.cell).await,
        Err(CatalogError::Fenced)
    ));
    ensure!(matches!(
        fixture
            .namespace
            .bootstrap_member(&fixture.birth, &fixture.log)
            .await,
        Err(CatalogError::Fenced)
    ));
    let mut conflict = request.clone();
    conflict.replacement.scope.incarnation = IncarnationId::new([93; 16])?;
    ensure!(matches!(
        fixture
            .namespace
            .recover_from_disaster(
                conflict,
                &fence,
                LtxHost::default(),
                ArchiveLimits::default()
            )
            .await,
        Err(CatalogError::Conflict(_))
    ));
    ensure!(fence.calls.load(Ordering::SeqCst) == 1);
    fence.stopped.store(true, Ordering::SeqCst);
    let (a, b) = tokio::join!(
        fixture.namespace.recover_from_disaster(
            request.clone(),
            &fence,
            LtxHost::default(),
            ArchiveLimits::default()
        ),
        fixture.namespace.recover_from_disaster(
            request.clone(),
            &fence,
            LtxHost::default(),
            ArchiveLimits::default()
        ),
    );
    ensure!(a?.digest() == b?.digest());
    let count = fence.calls.load(Ordering::SeqCst);
    let birth = fixture
        .namespace
        .recover_from_disaster(
            request.clone(),
            &fence,
            LtxHost::default(),
            ArchiveLimits::default(),
        )
        .await?;
    ensure!(
        birth.scope() == &request.replacement.scope && fence.calls.load(Ordering::SeqCst) == count
    );
    fence.stopped.store(false, Ordering::SeqCst);
    ensure!(
        fixture
            .namespace
            .recover_from_disaster(
                request,
                &fence,
                LtxHost::default(),
                ArchiveLimits::default()
            )
            .await
            .is_err(),
        "resume must reverify authority evidence"
    );
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disaster_missing_pinned_manifest_never_falls_back_or_fences() -> Result<()> {
    let mut fixture = Fixture::new(ArchiveLimits::default()).await?;
    let cut = fixture.commit(7, vec![]).await?;
    let first = fixture.publish(cut).await?;
    let objects = fixture
        .store
        .inner
        .list(None)
        .try_collect::<Vec<_>>()
        .await?;
    let selected = objects
        .into_iter()
        .find(|o| o.location.as_ref().contains("/manifests/"))
        .context("manifest")?;
    let cut = fixture.commit(9, vec![]).await?;
    fixture.publish(cut).await?;
    fixture.store.inner.delete(&selected.location).await?;
    let fence = Fence::new(true);
    let request = request(&fixture.birth, first.reference())?;
    ensure!(matches!(
        fixture
            .namespace
            .recover_from_disaster(
                request,
                &fence,
                LtxHost::default(),
                ArchiveLimits::default()
            )
            .await,
        Err(CatalogError::Archive(_))
    ));
    ensure!(fence.calls.load(Ordering::SeqCst) == 0);
    ensure!(
        fixture
            .namespace
            .birth(&fixture.birth.scope().cell)
            .await?
            .context("birth")?
            .scope()
            == fixture.birth.scope()
    );
    fixture.close().await
}

struct LostConfirmation(Arc<faults::Store>);

impl DisasterFenceAuthority for LostConfirmation {
    fn fence<'a>(&'a self, request: &'a DisasterFenceRequest) -> BoxFuture<'a, Result<Vec<u8>>> {
        Box::pin(async move {
            self.0.fail_after.store(true, Ordering::SeqCst);
            self.0.fail_confirmation.store(true, Ordering::SeqCst);
            Ok(request.digest().to_vec())
        })
    }

    fn verify<'a>(
        &'a self,
        request: &'a DisasterFenceRequest,
        evidence: &'a [u8],
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            ensure!(evidence == request.digest(), "receipt binding");
            Ok(())
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disaster_lost_completion_response_reconciles_exact_decision() -> Result<()> {
    let mut fixture = Fixture::new(ArchiveLimits::default()).await?;
    let cut = fixture.commit(7, vec![]).await?;
    let set = fixture.publish(cut).await?;
    let request = request(&fixture.birth, set.reference())?;
    let fence = LostConfirmation(fixture.store.clone());
    ensure!(matches!(
        fixture
            .namespace
            .recover_from_disaster(
                request.clone(),
                &fence,
                LtxHost::default(),
                ArchiveLimits::default()
            )
            .await,
        Err(CatalogError::Storage(_))
    ));
    let birth = fixture
        .namespace
        .birth(&request.source.cell)
        .await?
        .context("durable completed decision")?;
    ensure!(birth.scope() == &request.replacement.scope);
    let recovered = fixture
        .namespace
        .recover_from_disaster(
            request,
            &fence,
            LtxHost::default(),
            ArchiveLimits::default(),
        )
        .await?;
    ensure!(recovered.digest() == birth.digest());
    fixture.close().await
}

async fn fresh_cells(
    node: &cellar::sdk::Node,
    native: (&public_node::Counter, &Arc<faults::Store>, &RaftHostOptions),
    registration: RaftCellRegistration,
    resume: bool,
) -> Result<RaftCells> {
    use cellar::sdk::NodeRaftListeners;
    let (definition, store, options) = native;
    node.start_raft_cells(
        RaftCellsConfig {
            infrastructure: cellar::sdk::NodeRaftConfig {
                log: ReplicaLogOptions::new(
                    node.info().data_dir.join("raft"),
                    NodeId::new([21; 16])?,
                    DiskLineage::new([31; 16])?,
                    ProcessGeneration::new([if resume { 42 } else { 41 }; 16])?,
                    if resume {
                        LogOpenMode::Resume
                    } else {
                        LogOpenMode::Create
                    },
                ),
                consensus: RaftRuntimeOptions {
                    tick_interval_ms: Some(10),
                    ..Default::default()
                },
                transport: TransportOptions::default(),
            },
            catalog: CatalogOptions {
                prefix: "host/test".into(),
                ..Default::default()
            },
            cells: vec![registration],
            peers: vec![],
            query_origins: vec![],
            host: options.clone(),
        },
        RaftCellsServices {
            listeners: NodeRaftListeners {
                control: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
                data: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
                query: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
            },
            tls: public_node::tls()?,
            store: store.clone(),
            application: definition.application.clone(),
            versions: public_node::version_keys()?,
        },
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disaster_restores_exact_older_cut_after_disk_loss_and_preserves_receipts() -> Result<()> {
    restore_exact_cut(RaftHostOptions::default(), false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disaster_extent_restores_exact_cut_receipts_and_restart() -> Result<()> {
    restore_exact_cut(
        RaftHostOptions {
            extents: Some(RaftExtentOptions::default()),
            ..Default::default()
        },
        false,
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disaster_extent_archive_lag_loses_only_the_unarchived_suffix() -> Result<()> {
    restore_exact_cut(
        RaftHostOptions {
            extents: Some(RaftExtentOptions::default()),
            ..Default::default()
        },
        true,
    )
    .await
}

async fn restore_exact_cut(options: RaftHostOptions, unarchived_suffix: bool) -> Result<()> {
    let native = Native::start(
        Arc::new(faults::Store::default()),
        options,
        Durability::Quorum,
    )
    .await?;
    native.retry(1, Durability::S3).await?;
    let acknowledged = native.retry(2, Durability::S3).await?;
    let archive = native.archive().await?;
    let selected = archive
        .recovery_metadata()
        .await?
        .context("selected cut")?
        .reference();
    if unarchived_suffix {
        native
            .store
            .archive_unavailable
            .store(true, Ordering::Release);
        let newer = native.retry(3, Durability::Quorum).await?;
        ensure!(newer.durability == Durability::Quorum);
        let status = native.cell()?.status().await?;
        ensure!(status.archive.readiness == ArchiveReadiness::Lagging);
        ensure!(status.archive.lag_entries > 0);
    } else {
        native.retry(3, Durability::S3).await?;
        ensure!(
            archive
                .recovery_metadata()
                .await?
                .context("newer head")?
                .reference()
                != selected
        );
    }
    native.service.close().await?;
    native
        .node
        .shutdown(cellar::sdk::ShutdownMode::Preserve)
        .await?;
    native
        .store
        .archive_unavailable
        .store(false, Ordering::Release);
    // Only this test's voter directory is removed, after all owners have joined.
    std::fs::remove_dir_all(native.root.path().join("raft"))?;
    std::fs::remove_dir_all(native.root.path().join("raft-cells"))?;
    let namespace = Catalog::new(
        native.store.clone(),
        CatalogOptions {
            prefix: "host/test".into(),
            ..Default::default()
        },
    )?
    .existing_namespace(native.registration.namespace.clone())
    .await?;
    let old = namespace
        .birth(&native.registration.birth.scope.cell)
        .await?
        .context("old birth")?;
    let request = request(&old, selected)?;
    let replacement = request.replacement.clone();
    let operator = S3DisasterRecovery::open(
        native.store.clone(),
        CatalogOptions {
            prefix: "host/test".into(),
            ..Default::default()
        },
        native.registration.namespace.clone(),
        LtxHost::default(),
        ArchiveLimits::default(),
    )
    .await?;
    let planned = operator.fence_request(request.clone()).await?;
    ensure!(planned.recovery().source == request.source);
    operator.recover(request, &Fence::new(true)).await?;
    ensure!(
        namespace
            .resolve_birth(native.registration.birth.clone())
            .await
            .is_err(),
        "old admission must stay fenced"
    );
    let root = tempfile::tempdir()?;
    let mut registration = native.registration.clone();
    registration.birth = replacement.clone();
    let node = public_node::node(root.path(), native.definition.application.clone()).await?;
    let service = fresh_cells(
        &node,
        (&native.definition, &native.store, &native.options),
        registration.clone(),
        false,
    )
    .await?;
    let cell = service.cell(registration.namespace.namespace, &replacement.scope.cell)?;
    public_node::ready(&cell).await?;
    let query = cell
        .query(
            native.definition.get.clone(),
            vec![],
            QueryOptions {
                consistency: QueryConsistency::Linearizable,
                ..Default::default()
            },
        )
        .await?;
    ensure!(
        query.result == 2_i64.to_be_bytes(),
        "must restore the pinned cut, not newer head"
    );
    ensure!(query.version.scope == replacement.scope);
    ensure!(
        native.calls.load(Ordering::SeqCst) == 3,
        "restore must not execute callbacks"
    );
    let stale = cell
        .query(
            native.definition.get.clone(),
            vec![],
            QueryOptions {
                consistency: QueryConsistency::AtLeast(acknowledged.version_token),
                ..Default::default()
            },
        )
        .await;
    ensure!(
        matches!(stale, Err(CommandFailure::WrongIncarnation)),
        "old version token accepted: {stale:?}"
    );
    let duplicate = cell
        .command(
            native.definition.increment.clone(),
            1_i64.to_be_bytes().to_vec(),
            public_node::archived_options(2)?,
        )
        .await?;
    ensure!(
        duplicate.result == acknowledged.result && duplicate.version.scope == replacement.scope
    );
    ensure!(
        native.calls.load(Ordering::SeqCst) == 3,
        "duplicate reexecuted"
    );
    let next = cell
        .command(
            native.definition.increment.clone(),
            1_i64.to_be_bytes().to_vec(),
            public_node::archived_options(3)?,
        )
        .await?;
    ensure!(next.result == 3_i64.to_be_bytes() && native.calls.load(Ordering::SeqCst) == 4);
    service.close().await?;
    node.shutdown(cellar::sdk::ShutdownMode::Preserve).await?;
    // Ordinary restart must resume new stable voting state, without reseeding.
    let node = public_node::node(root.path(), native.definition.application.clone()).await?;
    let service = fresh_cells(
        &node,
        (&native.definition, &native.store, &native.options),
        registration.clone(),
        true,
    )
    .await?;
    let cell = service.cell(registration.namespace.namespace, &replacement.scope.cell)?;
    public_node::ready(&cell).await?;
    let state = cell
        .query(
            native.definition.get.clone(),
            vec![],
            QueryOptions {
                consistency: QueryConsistency::AtLeast(next.version_token),
                ..Default::default()
            },
        )
        .await?;
    ensure!(state.result == 3_i64.to_be_bytes());
    service.close().await?;
    node.shutdown(cellar::sdk::ShutdownMode::Preserve).await?;
    std::fs::remove_dir_all(root.path().join("raft"))?;
    std::fs::remove_dir_all(root.path().join("raft-cells"))?;
    let node = public_node::node(root.path(), native.definition.application.clone()).await?;
    ensure!(
        fresh_cells(
            &node,
            (&native.definition, &native.store, &native.options),
            registration,
            false
        )
        .await
        .is_err(),
        "new incarnation cannot reset a claimed lost disk"
    );
    node.shutdown(cellar::sdk::ShutdownMode::Preserve).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn disaster_extent_missing_and_corrupt_objects_refuse_before_fence() -> Result<()> {
    for corrupt in [false, true] {
        let native = Native::start(
            Arc::new(faults::Store::default()),
            RaftHostOptions {
                extents: Some(RaftExtentOptions::default()),
                ..Default::default()
            },
            Durability::Quorum,
        )
        .await?;
        native.retry(1, Durability::S3).await?;
        let reference = native
            .archive()
            .await?
            .recovery_metadata()
            .await?
            .context("selected cut")?
            .reference();
        native.service.close().await?;
        native
            .node
            .shutdown(cellar::sdk::ShutdownMode::Preserve)
            .await?;
        let namespace = Catalog::new(
            native.store.clone(),
            CatalogOptions {
                prefix: "host/test".into(),
                ..Default::default()
            },
        )?
        .existing_namespace(native.registration.namespace.clone())
        .await?;
        let birth = namespace
            .birth(&native.registration.birth.scope.cell)
            .await?
            .context("birth")?;
        let objects = native
            .store
            .inner
            .list(None)
            .try_collect::<Vec<_>>()
            .await?;
        let object = objects
            .iter()
            .find(|object| object.location.as_ref().contains("/root-extents/"))
            .context("extent object")?;
        if corrupt {
            let mut bytes = native
                .store
                .inner
                .get(&object.location)
                .await?
                .bytes()
                .await?
                .to_vec();
            bytes[0] ^= 1;
            native
                .store
                .inner
                .put(&object.location, bytes.into())
                .await?;
        } else {
            native.store.inner.delete(&object.location).await?;
        }
        let fence = Fence::new(true);
        let before = native
            .store
            .inner
            .list(None)
            .try_collect::<Vec<_>>()
            .await?;
        ensure!(namespace
            .recover_from_disaster(
                request(&birth, reference)?,
                &fence,
                LtxHost::default(),
                ArchiveLimits::default()
            )
            .await
            .is_err());
        ensure!(fence.calls.load(Ordering::SeqCst) == 0);
        ensure!(
            native
                .store
                .inner
                .list(None)
                .try_collect::<Vec<_>>()
                .await?
                == before
        );
        ensure!(
            namespace
                .birth(&birth.scope().cell)
                .await?
                .context("old birth")?
                .digest()
                == birth.digest()
        );
    }
    Ok(())
}
