// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Contention must park the writer using its exact original command proof.
use super::*;

#[derive(Default)]
pub(super) struct Wait {
    held: Notify,
    waiting: Notify,
    release: Notify,
}

pub(super) async fn hold(
    context: &Context,
    journal: &BindingJournal,
    route: &Arc<Route>,
) -> Result<()> {
    let guard = journal.lock_lifecycle(context).await?;
    let grant = guard.fence_execution(context, route.authority()).await?;
    let wait = route.wait.clone();
    grant
        .dispatch(context, move |_| async move {
            wait.held.notify_one();
            wait.release.notified().await;
            Ok(())
        })
        .await
}

pub(super) async fn wait(
    context: &Context,
    journal: &BindingJournal,
    route: &Arc<Route>,
    dirty: bool,
) -> Result<()> {
    // Entry/read authority cannot make a writer resumable. Establish this
    // exact command's unfinished receipt first, then optionally dirty its SQL.
    context
        .container_provider_authority()?
        .before_effect(context)
        .await?;
    if dirty {
        context.storage()?.put_json("wait-work", &json!(true))?;
    }
    let mut acquiring = journal.lock_lifecycle(context);
    let _guard = futures_util::future::poll_fn(|cx| {
        let result = acquiring.as_mut().poll(cx);
        if result.is_pending() {
            // This future has attempted the original lock and selected the
            // contended path. No sleeps or unrelated writer stand in for it.
            route.wait.waiting.notify_one();
        }
        result
    })
    .await?;
    Ok(())
}

async fn contend(
    cluster: &ManagedCluster,
    history: &CellHistory,
    minimum: Durability,
    route: &Arc<Route>,
    dirty: bool,
) -> Result<()> {
    let holder = call(cluster, history, minimum, 2, "HoldLifecycle");
    tokio::pin!(holder);
    cellar::asyncrt::select_biased! {
        "wait for lifecycle holder admission";
        result = &mut holder => anyhow::bail!("holder ended before its owned I/O: {result:?}"),
        _ = route.wait.held.notified() => (),
    }
    let leader = managed_fixture::leader(&cluster.hosts, cluster.history).await?;
    let cell = cluster.hosts[leader].cell(history.clone())?;
    let before = cell
        .query(
            ProposalId::new([249; 16])?,
            &cell.contract("Version")?,
            vec![],
            QueryOptions::default(),
        )
        .await?;
    let mut options = bindings::options(1, minimum)?;
    options.id = CommandId::new(ClientId::new([250; 16])?, 1)?;
    let operation = if dirty {
        "WaitDirtyLifecycle"
    } else {
        "WaitLifecycle"
    };
    let contract = cell.contract("Run")?;
    let waiter = async {
        Ok::<_, anyhow::Error>(
            cell.command(&contract, serde_json::to_vec(operation)?, options)
                .await?,
        )
    };
    tokio::pin!(waiter);
    cellar::asyncrt::select_biased! {
        "observe contended lifecycle admission";
        result = &mut holder => anyhow::bail!("holder ended before contended admission: {result:?}"),
        result = &mut waiter => anyhow::bail!("waiter bypassed the original lifecycle lock: {result:?}"),
        _ = route.wait.waiting.notified() => (),
    }
    route.wait.release.notify_one();
    let (held, waited) = futures_util::future::try_join(holder, waiter).await?;
    ensure!(held.durability >= minimum && waited.durability >= minimum);
    let actual = held
        .version
        .sequence
        .get()
        .max(waited.version.sequence.get());
    let expected = before.version.sequence.get() + if dirty { 4 } else { 3 };
    ensure!(actual == expected, "contended lifecycle wait changed proof count: dirty={dirty} before={} expected={expected} actual={actual}", before.version.sequence.get());
    eprintln!(
        "controller_lifecycle_wait_observations {}",
        json!({"dirty":dirty,"minimum":format!("{minimum:?}"),"before":before.version.sequence.get(),"final":actual,"extra_checkpoint":dirty})
    );
    Ok(())
}

pub(super) async fn qualify(real: bool, dirty: bool) -> Result<()> {
    for rooted in [false, true] {
        for minimum in [Durability::Quorum, Durability::S3] {
            let peer = Arc::new(Peer::default());
            let root = tempfile::tempdir()?.keep();
            eprintln!("retained lifecycle-wait controller: {}", root.display());
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
                    if dirty {
                        "lifecycle-wait-dirty"
                    } else {
                        "lifecycle-wait-clean"
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
                wait: Arc::new(Wait::default()),
            });
            let mut cluster = start(rooted, minimum, route.clone(), Scenario::Basic).await?;
            let domain = cluster.fixtures[0].domain.clone();
            let result = domain
                .run(async {
                    let group = cluster.history;
                    let history = CellHistory {
                        cluster: group.cluster,
                        namespace: group.namespace,
                        cell: CellId::new("Counter:controller-provider")?,
                        incarnation: IncarnationId::new([246; 16])?,
                    };
                    let leader = managed_fixture::leader(&cluster.hosts, group).await?;
                    cluster.hosts[leader]
                        .cell(history.clone())?
                        .enroll(
                            CellEnrollmentRequest {
                                creation: ProposalId::new([247; 16])?,
                                durability: minimum,
                            },
                            TIMEOUT,
                        )
                        .await?;
                    call(&cluster, &history, minimum, 1, "Create").await?;
                    let result = contend(&cluster, &history, minimum, &route, dirty).await;
                    route.wait.release.notify_one();
                    result?;
                    call(&cluster, &history, minimum, 3, "Destroy").await?;
                    ensure!(
                        peer.creates.load(Ordering::Acquire) == 1
                            && peer.deletes.load(Ordering::Acquire) == 1
                    );
                    Ok::<_, anyhow::Error>(())
                })
                .await
                .map_err(anyhow::Error::from)
                .and_then(std::convert::identity);
            route.wait.release.notify_one();
            let closed = managed_fixture::close_group_nodes(
                std::mem::take(&mut cluster.nodes),
                &cluster.hosts,
            )
            .await;
            for root in std::mem::take(&mut cluster.roots) {
                eprintln!("retained lifecycle-wait voter: {}", root.keep().display());
            }
            domain.shutdown().await;
            controller.shutdown().await?;
            controller_domain.shutdown().await;
            result.with_context(|| {
                format!("lifecycle wait dirty={dirty} extents={rooted} minimum={minimum:?}")
            })?;
            closed?;
        }
    }
    Ok(())
}
