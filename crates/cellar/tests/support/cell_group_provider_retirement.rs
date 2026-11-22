// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Retirement is not allowed to erase autonomous proof-confirmation work.
use super::*;

#[derive(Clone, Copy, Debug)]
enum Cut {
    LostArchiveProof,
    AutonomousDelete,
}

pub(super) async fn qualify(real: bool) -> Result<()> {
    qualify_cut(real, Cut::LostArchiveProof).await
}

pub(super) async fn qualify_order(real: bool) -> Result<()> {
    qualify_cut(real, Cut::AutonomousDelete).await
}

async fn qualify_cut(real: bool, cut: Cut) -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("cellar::raft::checkpoint=debug")
        .with_ansi(false)
        .try_init();
    for rooted in [false, true] {
        let peer = Arc::new(Peer::default());
        let root = tempfile::tempdir()?.keep();
        eprintln!("retained retirement-confirmation state: {}", root.display());
        let controller_domain = Arc::new(cellar::sdk::ExecutionDomain::new(
            tokio::runtime::Handle::current(),
        ));
        let controller = ExecutionController::create(
            controller_domain.clone(),
            ExecutionControllerOptions {
                directory: root.join("controller"),
                controller_id: [0; 16],
                backend: peer.identity(),
                limits: ExecutionControllerLimits::default(),
            },
            peer.clone(),
            [245; 32],
        )
        .await?;
        let store = Arc::new(if real {
            catalog_fault::Store::around(rustfs::store("provider-retirement-confirmation", rooted)?)
        } else {
            catalog_fault::Store::default()
        });
        let route = Arc::new(Route {
            provider: Arc::new(RwLock::new(Arc::new(ControllerProvider::new(
                controller.clone(),
            )?))),
            factory_refused: AtomicBool::new(false),
            retired: AtomicBool::new(false),
            changed: Notify::new(),
            staged: AtomicUsize::new(0),
            store,
            old_context: Mutex::new(None),
            wait: Arc::new(lifecycle_wait::Wait::default()),
        });
        let mut cluster = start(
            rooted,
            Durability::Quorum,
            route.clone(),
            Scenario::Interrupted,
        )
        .await?;
        let domain = cluster.fixtures[0].domain.clone();
        let result = domain.run(async {
            let group = cluster.history;
            let history = CellHistory {
                cluster: group.cluster,
                namespace: group.namespace,
                cell: CellId::new("Counter:controller-provider")?,
                incarnation: IncarnationId::new([246; 16])?,
            };
            let leader = managed_fixture::leader(&cluster.hosts, group).await?;
            let cell = cluster.hosts[leader].cell(history.clone())?;
            cell.enroll(CellEnrollmentRequest { creation: ProposalId::new([247; 16])?, durability: Durability::S3 }, TIMEOUT).await?;
            call(&cluster, &history, Durability::S3, 1, "Create").await?;
            let denied_before = route.store.denied_group_archive_writes.load(Ordering::Acquire);
            let fences_before = peer.retirement_fences.load(Ordering::Acquire);
            peer.hold_retirement_fence.store(true, Ordering::Release);
            let (original, staged) = match cut {
                Cut::LostArchiveProof => {
                    *peer.refuse_retirement_archive.lock().unwrap() = Some(route.store.clone());
                    let failure = call(&cluster, &history, Durability::S3, 2, "MaintainRetirement").await
                        .err().context("unarchived retirement returned success")?;
                    let original = bindings::options(2, Durability::S3)?.id;
                    ensure!(failure.downcast_ref::<Arc<CellCommandError>>().is_some_and(|e| e.ambiguous && e.command == original), "retirement lost its original uncertain command: {failure:#}");
                    ensure!(peer.deletes.load(Ordering::Acquire) == 1 && !peer.resource.lock().unwrap().exists, "retirement cut did not follow the physical Delete");
                    ensure!(route.store.denied_group_archive_writes.load(Ordering::Acquire) > denied_before, "retirement cut refused no archive write");
                    ensure!(!route.retired.load(Ordering::Acquire), "retirement was reported before its S3 proof");
                    (Some(original), None)
                }
                Cut::AutonomousDelete => {
                    // Persist only the desired intent. The shared scheduler
                    // must perform Delete, prove its result, then separately
                    // activate the retired confirmation fence without traffic.
                    let staged = call(&cluster, &history, Durability::S3, 2, "StageDeleteIntent").await;
                    eprintln!("retirement_stage_request_completed {}", staged.is_ok());
                    // Preserve this original response requirement, but first
                    // collect the independent fence outcome so that an entered
                    // cleanup cannot hide an ordering failure behind timeout.
                    (None, Some(staged))
                }
            };
            route.factory_refused.store(true, Ordering::Release);
            route.store.deny_group_archive_writes.store(false, Ordering::Release);
            cellar::asyncrt::timeout(Duration::from_secs(60), async {
                loop {
                    let changed = peer.retirement_fence_changed.notified();
                    if peer.retirement_fences.load(Ordering::Acquire) > fences_before { break; }
                    changed.await;
                }
                ensure!(!route.retired.load(Ordering::Acquire), "retirement confirmation preceded independent fence activation");
                ensure!(peer.active.lock().unwrap().as_ref().is_some_and(|claim| !claim.checkpoint().record.is_retired()), "held retirement fence unexpectedly became active");
                peer.hold_retirement_fence.store(false, Ordering::Release);
                peer.retirement_fence_changed.notify_waiters();
                loop {
                    let changed = route.changed.notified();
                    if route.retired.load(Ordering::Acquire) { break; }
                    changed.await;
                }
                Ok::<_,anyhow::Error>(())
            }).await.context("retired generation lost autonomous S3 confirmation")??;
            ensure!(peer.creates.load(Ordering::Acquire) == 1 && peer.deletes.load(Ordering::Acquire) == 1, "confirmation repeated a provider mutation");
            // The independent controller must also refuse retired execution,
            // even when a caller bypasses the SDK's dispatch admission.
            let claim = peer.active.lock().unwrap().clone().context("confirmation claim missing")?;
            ensure!(claim.checkpoint().record.is_retired());
            let payload = serde_json::to_vec(&Request { format: 1, operation: Operation::Running.name().into(), input: json!({}) })?;
            let operation = ExecutionOperation::new([37;16], ExecutionOperationKind::Inspect, &payload, u64::try_from(cellar::asyncrt::wall_ms())? + 1000)?;
            let before = peer.calls.load(Ordering::Acquire);
            let refused = controller.execute(claim, operation, payload).await;
            ensure!(refused.err().is_some_and(|error| error.downcast_ref::<ExecutionControllerFailure>() == Some(&ExecutionControllerFailure::StaleClaim)) && peer.calls.load(Ordering::Acquire) == before, "controller admitted retired provider execution");
            if let Some(staged) = staged {
                staged.context("DeleteIntent did not finish within its original request deadline")?;
            }
            // Framework confirmation cannot turn a partly executed business
            // callback into a successful original result.
            let leader = managed_fixture::leader(&cluster.hosts, group).await?;
            let cell = cluster.hosts[leader].cell(history.clone())?;
            if let Some(original) = original {
                let receipt = cell.receipt(CellReceiptRequest { id: original, contract: cell.contract("Run")?, input: serde_json::to_vec("MaintainRetirement")?, durability: Durability::S3 }, TIMEOUT).await;
                ensure!(matches!(receipt.as_ref().err().map(AsRef::as_ref), Some(CellDispatchError::Execution(ExecutionError::Uncertain))), "framework confirmation fabricated a business completion");
            }
            route.factory_refused.store(false, Ordering::Release);
            let method = match cut { Cut::LostArchiveProof => "ReadRetirement", Cut::AutonomousDelete => "Read" };
            let read = call(&cluster, &history, Durability::S3, 3, method).await?;
            ensure!(read.durability >= Durability::S3);
            eprintln!("retirement_confirmation_observations {}", json!({"extents":rooted,"minimum":"S3","creates":peer.creates.load(Ordering::Acquire),"deletes":peer.deletes.load(Ordering::Acquire),"denied_archive_writes":route.store.denied_group_archive_writes.load(Ordering::Acquire)-denied_before,"business_callback_replayed":false,"confirmation_fence_observed":true}));
            Ok::<_,anyhow::Error>(())
        }).await.map_err(anyhow::Error::from).and_then(std::convert::identity);
        peer.hold_retirement_fence.store(false, Ordering::Release);
        peer.retirement_fence_changed.notify_waiters();
        route
            .store
            .deny_group_archive_writes
            .store(false, Ordering::Release);
        let closed =
            managed_fixture::close_group_nodes(std::mem::take(&mut cluster.nodes), &cluster.hosts)
                .await;
        for root in std::mem::take(&mut cluster.roots) {
            eprintln!(
                "retained retirement-confirmation voter disk: {}",
                root.keep().display()
            );
        }
        domain.shutdown().await;
        controller.shutdown().await?;
        controller_domain.shutdown().await;
        result.with_context(|| format!("retirement confirmation extents={rooted} cut={cut:?}"))?;
        closed?;
    }
    Ok(())
}
