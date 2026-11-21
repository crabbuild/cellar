// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Retirement is not allowed to erase autonomous proof-confirmation work.
use super::*;

pub(super) async fn qualify(real: bool) -> Result<()> {
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
            *peer.refuse_retirement_archive.lock().unwrap() = Some(route.store.clone());
            let failure = call(&cluster, &history, Durability::S3, 2, "MaintainRetirement").await
                .err().context("unarchived retirement returned success")?;
            let original = bindings::options(2, Durability::S3)?.id;
            ensure!(failure.downcast_ref::<Arc<CellCommandError>>().is_some_and(|e| e.ambiguous && e.command == original), "retirement lost its original uncertain command: {failure:#}");
            ensure!(peer.deletes.load(Ordering::Acquire) == 1 && !peer.resource.lock().unwrap().exists, "retirement cut did not follow the physical Delete");
            ensure!(route.store.denied_group_archive_writes.load(Ordering::Acquire) > denied_before, "retirement cut refused no archive write");
            ensure!(!route.retired.load(Ordering::Acquire), "retirement was reported before its S3 proof");
            route.factory_refused.store(true, Ordering::Release);
            route.store.deny_group_archive_writes.store(false, Ordering::Release);
            cellar::asyncrt::timeout(Duration::from_secs(60), async {
                loop {
                    let changed = route.changed.notified();
                    if route.retired.load(Ordering::Acquire) { break; }
                    changed.await;
                }
            }).await.context("retired generation lost autonomous S3 confirmation")?;
            ensure!(peer.creates.load(Ordering::Acquire) == 1 && peer.deletes.load(Ordering::Acquire) == 1, "confirmation repeated a provider mutation");
            // The independent controller must also refuse retired execution,
            // even when a caller bypasses the SDK's dispatch admission.
            let claim = peer.active.lock().unwrap().clone().context("confirmation claim missing")?;
            ensure!(claim.checkpoint().record.is_retired());
            let payload = serde_json::to_vec(&Request { format: 1, operation: Operation::Running.name().into(), input: json!({}) })?;
            let operation = ExecutionOperation::new([37;16], ExecutionOperationKind::Read, &payload, u64::try_from(cellar::asyncrt::wall_ms())? + 1000)?;
            let before = peer.calls.load(Ordering::Acquire);
            let refused = controller.execute(claim, operation, payload).await;
            ensure!(refused.err().is_some_and(|error| error.downcast_ref::<ExecutionControllerFailure>() == Some(&ExecutionControllerFailure::StaleClaim)) && peer.calls.load(Ordering::Acquire) == before, "controller admitted retired provider execution");
            // Framework confirmation cannot turn a partly executed business
            // callback into a successful original result.
            let leader = managed_fixture::leader(&cluster.hosts, group).await?;
            let cell = cluster.hosts[leader].cell(history.clone())?;
            let receipt = cell.receipt(CellReceiptRequest { id: original, contract: cell.contract("Run")?, input: serde_json::to_vec("MaintainRetirement")?, durability: Durability::S3 }, TIMEOUT).await;
            ensure!(matches!(receipt.as_ref().err().map(AsRef::as_ref), Some(CellDispatchError::Execution(ExecutionError::Uncertain))), "framework confirmation fabricated a business completion");
            route.factory_refused.store(false, Ordering::Release);
            let read = call(&cluster, &history, Durability::S3, 3, "ReadRetirement").await?;
            ensure!(read.durability >= Durability::S3);
            eprintln!("retirement_confirmation_observations {}", json!({"extents":rooted,"minimum":"S3","creates":peer.creates.load(Ordering::Acquire),"deletes":peer.deletes.load(Ordering::Acquire),"denied_archive_writes":route.store.denied_group_archive_writes.load(Ordering::Acquire)-denied_before,"business_callback_replayed":false}));
            Ok::<_,anyhow::Error>(())
        }).await.map_err(anyhow::Error::from).and_then(std::convert::identity);
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
        result.with_context(|| format!("retirement confirmation extents={rooted}"))?;
        closed?;
    }
    Ok(())
}
