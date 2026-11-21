// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Private preparation must share a source proof with business changes, the
//! unfinished command, binding intent and execution claim before controller I/O.
use super::*;

pub(super) async fn qualify(real: bool, outage: bool) -> Result<()> {
    for rooted in [false, true] {
        for minimum in [Durability::Quorum, Durability::S3] {
            let peer = Arc::new(Peer::default());
            let root = tempfile::tempdir()?.keep();
            eprintln!("retained admission controller: {}", root.display());
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
                    if outage {
                        "controller-admission-outage"
                    } else {
                        "controller-admission"
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
                store: store.clone(),
                old_context: Mutex::new(None),
                wait: Arc::new(lifecycle_wait::Wait::default()),
            });
            let mut cluster = start(rooted, minimum, route.clone(), Scenario::Basic).await?;
            let domain = cluster.fixtures[0].domain.clone();
            let result = domain.run(async {
                let group = cluster.history;
                let history = CellHistory { cluster: group.cluster, namespace: group.namespace, cell: CellId::new("Counter:controller-provider")?, incarnation: IncarnationId::new([246;16])? };
                let leader = managed_fixture::leader(&cluster.hosts, group).await?;
                let cell = cluster.hosts[leader].cell(history.clone())?;
                cell.enroll(CellEnrollmentRequest { creation: ProposalId::new([247;16])?, durability: minimum }, TIMEOUT).await?;
                let before = cell.query(ProposalId::new([248;16])?, &cell.contract("Version")?, vec![], QueryOptions::default()).await?;
                store.deny_group_archive_writes.store(outage, Ordering::Release);
                let created = call(&cluster, &history, minimum, 1, "Create").await;
                if outage && minimum == Durability::S3 {
                    let error = created.err().context("S3 archive outage admitted provider creation")?;
                    let original = bindings::options(1, minimum)?.id;
                    ensure!(error.downcast_ref::<Arc<CellCommandError>>().is_some_and(|error| error.ambiguous && error.command == original));
                    ensure!(peer.calls.load(Ordering::Acquire) == 0 && peer.creates.load(Ordering::Acquire) == 0 && peer.active.lock().unwrap().is_none(), "controller or provider entered before the required S3 proof");
                } else {
                    let receipt = created?;
                    ensure!(receipt.durability >= minimum);
                    ensure!(receipt.version.sequence.get() == before.version.sequence.get() + 2,
                        "provider preparation requires one combined entry/intent/fence proof and one result proof: before={} actual={}", before.version.sequence.get(), receipt.version.sequence.get());
                    ensure!(peer.creates.load(Ordering::Acquire) == 1 && peer.resource.lock().unwrap().exists);
                    let read = call(&cluster, &history, minimum, 2, "Read").await?;
                    ensure!(read.durability >= minimum);
                    let value: serde_json::Value = serde_json::from_slice(&read.result)?;
                    ensure!(value["record"]["intent"] == "running" && value["record"]["observation"] == "confirmed");
                }
                eprintln!("controller_admission_observations {}", json!({"extents":rooted,"minimum":format!("{minimum:?}"),"archive_outage":outage,"creates":peer.creates.load(Ordering::Acquire),"required_proof_before_controller":true}));
                Ok::<_, anyhow::Error>(())
            }).await;
            let closed = managed_fixture::close_group_nodes(
                std::mem::take(&mut cluster.nodes),
                &cluster.hosts,
            )
            .await;
            for voter in std::mem::take(&mut cluster.roots) {
                eprintln!("retained admission voter: {}", voter.keep().display());
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
