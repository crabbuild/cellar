// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A proven deletion intent can precede any controller Delete receipt.
use super::*;

pub(super) async fn qualify(real: bool, maintenance: bool) -> Result<()> {
    for rooted in [false, true] {
        for minimum in [Durability::Quorum, Durability::S3] {
            let peer = Arc::new(Peer::default());
            let root = tempfile::tempdir()?.keep();
            eprintln!("retained delete-intent controller: {}", root.display());
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
                catalog_fault::Store::around(rustfs::store(
                    match (maintenance, minimum) {
                        (true, Durability::S3) => "controller-delete-maint-s3",
                        (true, _) => "controller-delete-maint-quorum",
                        (false, Durability::S3) => "controller-delete-reconcile-s3",
                        (false, _) => "controller-delete-reconcile-quorum",
                    },
                    rooted,
                )?)
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
            // No background scheduler: the fresh public invocation below drives
            // the actual maintenance adapter once, making the missing-Delete
            // branch deterministic rather than waiting for a timer race.
            let mut cluster = start(rooted, minimum, route, Scenario::Basic).await?;
            let domain = cluster.fixtures[0].domain.clone();
            let result = domain.run(async {
                let group = cluster.history;
                let history = CellHistory {cluster: group.cluster, namespace: group.namespace, cell: CellId::new("Counter:controller-provider")?, incarnation: IncarnationId::new([246;16])?};
                let leader = managed_fixture::leader(&cluster.hosts, group).await?;
                cluster.hosts[leader].cell(history.clone())?.enroll(CellEnrollmentRequest {
                    creation: ProposalId::new([247;16])?, durability: minimum,
                }, TIMEOUT).await?;
                call(&cluster, &history, minimum, 1, "Create").await?;
                let staged = call(&cluster, &history, minimum, 2, "StageDeleteIntent").await?;
                ensure!(staged.durability >= minimum);
                let staged: serde_json::Value = serde_json::from_slice(&staged.result)?;
                ensure!(staged["record"]["intent"] == "deleted" && staged["record"]["observation"] == "pending");
                ensure!(peer.creates.load(Ordering::Acquire) == 1 && peer.deletes.load(Ordering::Acquire) == 0);
                let completed = call(&cluster, &history, minimum, 3, if maintenance {"MaintainDeleteIntent"} else {"Reconcile"}).await?;
                ensure!(completed.durability >= minimum);
                let completed: serde_json::Value = serde_json::from_slice(&completed.result)?;
                ensure!(completed["record"]["intent"] == "deleted" && completed["record"]["observation"] == "confirmed", "maintenance left a proven deletion intent pending when no Delete had been submitted");
                ensure!(peer.creates.load(Ordering::Acquire) == 1 && peer.deletes.load(Ordering::Acquire) == 1 && !peer.resource.lock().unwrap().exists);
                // An already retired generation must remain an observation.
                call(&cluster, &history, minimum, 4, "Reconcile").await?;
                ensure!(peer.deletes.load(Ordering::Acquire) == 1, "retired maintenance replayed Delete");
                eprintln!("controller_delete_intent_observations {}", json!({"extents":rooted,"minimum":format!("{minimum:?}"),"creates":1,"deletes":1,"recovered_without_delete_receipt":true,"maintenance":maintenance}));
                Ok::<_,anyhow::Error>(())
            }).await;
            let closed = managed_fixture::close_group_nodes(
                std::mem::take(&mut cluster.nodes),
                &cluster.hosts,
            )
            .await;
            for voter in std::mem::take(&mut cluster.roots) {
                eprintln!("retained delete-intent voter: {}", voter.keep().display());
            }
            domain.shutdown().await;
            controller.shutdown().await?;
            controller_domain.shutdown().await;
            result??;
            closed?;
        }
    }
    Ok(())
}
