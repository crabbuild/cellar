// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A late foreground waiter must precede inspection without cancelling fencing.
use super::*;

#[derive(Default)]
pub(super) struct Gate {
    enabled: AtomicBool,
    armed: AtomicBool,
    started: AtomicBool,
    sequence: AtomicUsize,
    activated: AtomicUsize,
    acquired: AtomicUsize,
    maintained: AtomicUsize,
    inspected: AtomicUsize,
    claim: Mutex<Option<ExecutionClaim>>,
    peer: Mutex<std::sync::Weak<Peer>>,
    entered: Notify,
    waiting: Notify,
    release: Notify,
    changed: Notify,
}

impl Gate {
    pub(super) fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Acquire)
    }

    pub(super) fn hold(&self, claim: &ExecutionClaim) -> bool {
        let record = &claim.checkpoint().record;
        record.intent == BindingIntent::Running
            && record.observation == BindingObservation::Confirmed
            && record.resource_id.is_some()
            && self.armed.swap(false, Ordering::AcqRel)
    }

    pub(super) fn activate(
        self: Arc<Self>,
        authority: Arc<dyn ExecutionFenceAuthority>,
        claim: ExecutionClaim,
    ) -> HandlerFuture<()> {
        Box::pin(async move {
            *self.claim.lock().unwrap() = Some(claim.clone());
            self.started.store(true, Ordering::Release);
            self.entered.notify_one();
            self.release.notified().await;
            authority.activate(claim.clone()).await?;
            let peer = self
                .peer
                .lock()
                .unwrap()
                .upgrade()
                .context("inspection peer was released")?;
            ensure!(
                peer.active.lock().unwrap().as_ref() == Some(&claim),
                "independent controller did not retain the entered activation"
            );
            self.activated.store(self.next(), Ordering::Release);
            self.changed.notify_waiters();
            Ok(())
        })
    }

    fn next(&self) -> usize {
        self.sequence.fetch_add(1, Ordering::AcqRel) + 1
    }

    pub(super) fn maintaining(&self) {
        if self.started.load(Ordering::Acquire) {
            let order = self.next();
            let _ = self
                .maintained
                .compare_exchange(0, order, Ordering::AcqRel, Ordering::Acquire);
            self.changed.notify_waiters();
        }
    }

    pub(super) fn inspected(&self) {
        if self.started.load(Ordering::Acquire) {
            let order = self.next();
            let _ = self
                .inspected
                .compare_exchange(0, order, Ordering::AcqRel, Ordering::Acquire);
            self.changed.notify_waiters();
        }
    }
}

pub(super) async fn wait(
    context: &Context,
    journal: &BindingJournal,
    gate: &Arc<Gate>,
) -> Result<()> {
    // Unlike the unchanged-proof control, this request reaches contention
    // before proving its first unfinished receipt. The real shared lock path
    // must register that wait, prove the original request and park its writer.
    let mut acquiring = journal.lock_lifecycle(context);
    let _guard = futures_util::future::poll_fn(|cx| {
        let result = acquiring.as_mut().poll(cx);
        if result.is_pending() {
            gate.waiting.notify_one();
        }
        result
    })
    .await?;
    gate.acquired.store(gate.next(), Ordering::Release);
    gate.changed.notify_waiters();
    Ok(())
}

async fn contend(
    cluster: &ManagedCluster,
    history: &CellHistory,
    rooted: bool,
    minimum: Durability,
    route: &Arc<Route>,
    peer: &Arc<Peer>,
) -> Result<()> {
    let gate = &route.wait.inspection;
    gate.armed.store(true, Ordering::Release);
    cellar::asyncrt::timeout(Duration::from_secs(5), gate.entered.notified())
        .await
        .context("healthy framework activation never reached the held I/O boundary")?;
    let held = gate
        .claim
        .lock()
        .unwrap()
        .clone()
        .context("held activation claim missing")?;
    ensure!(
        held.checkpoint().record.intent == BindingIntent::Running
            && held.checkpoint().record.observation == BindingObservation::Confirmed
            && held.checkpoint().record.resource_id.is_some()
            && held.checkpoint().record.generation == 1,
        "held activation was not a healthy inspection"
    );
    ensure!(
        gate.maintained.load(Ordering::Acquire) == 0 && gate.inspected.load(Ordering::Acquire) == 0,
        "inspection entered while activation was held"
    );
    let foreground = call(cluster, history, minimum, 2, "WaitInspectionLifecycle");
    tokio::pin!(foreground);
    cellar::asyncrt::select_biased! {
        "wait for the original foreground lifecycle reservation";
        result = &mut foreground => anyhow::bail!("foreground ended before registering behind the held framework activation: {result:?}"),
        _ = gate.waiting.notified() => (),
    }
    // The original activation must complete. A late foreground waiter cannot
    // cancel an entered fence or turn its unresolved result into no effect.
    route.factory_refused.store(true, Ordering::Release);
    gate.release.notify_one();
    let receipt = foreground.await.with_context(|| {
        format!(
            "inspection foreground failed: activated={} acquired={} maintained={} inspected={}",
            gate.activated.load(Ordering::Acquire),
            gate.acquired.load(Ordering::Acquire),
            gate.maintained.load(Ordering::Acquire),
            gate.inspected.load(Ordering::Acquire)
        )
    })?;
    ensure!(receipt.durability >= minimum);
    let activated = gate.activated.load(Ordering::Acquire);
    let acquired = gate.acquired.load(Ordering::Acquire);
    let maintained = gate.maintained.load(Ordering::Acquire);
    ensure!(
        activated > 0 && acquired > activated,
        "foreground admission cancelled or overtook the entered activation"
    );
    ensure!(maintained == 0 || maintained > acquired,
        "routine inspection overtook the original foreground reservation: activated={activated} maintained={maintained} acquired={acquired}");
    cellar::asyncrt::timeout(Duration::from_secs(60), async {
        loop {
            let changed = gate.changed.notified();
            let maintained = gate.maintained.load(Ordering::Acquire);
            let inspected = gate.inspected.load(Ordering::Acquire);
            if maintained > acquired && inspected > maintained {
                break;
            }
            changed.await;
        }
    })
    .await
    .context("deferred inspection lost autonomous discovery")?;
    ensure!(
        peer.creates.load(Ordering::Acquire) == 1 && peer.deletes.load(Ordering::Acquire) == 0,
        "inspection changed the resource generation"
    );
    eprintln!(
        "controller_inspection_handoff_observations {}",
        json!({
            "minimum":format!("{minimum:?}"), "extents":rooted, "activated":activated,
            "foreground_acquired":acquired,
            "maintenance":gate.maintained.load(Ordering::Acquire),
            "inspection":gate.inspected.load(Ordering::Acquire),
            "entered_fence_completed":true, "autonomous_successor":true,
            "business_constructor_refused":true
        })
    );
    Ok(())
}

pub(super) async fn qualify(real: bool) -> Result<()> {
    let mut failures = Vec::new();
    for rooted in [false, true] {
        for minimum in [Durability::Quorum, Durability::S3] {
            let wait = Arc::new(lifecycle_wait::Wait::default());
            wait.inspection.enabled.store(true, Ordering::Release);
            let peer = Arc::new(Peer {
                inspection_gate: Some(wait.inspection.clone()),
                ..Peer::default()
            });
            *wait.inspection.peer.lock().unwrap() = Arc::downgrade(&peer);
            let root = tempfile::tempdir()?.keep();
            eprintln!("retained inspection-handoff controller: {}", root.display());
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
                    if minimum == Durability::S3 {
                        "inspection-handoff-s3"
                    } else {
                        "inspection-handoff-quorum"
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
                wait,
            });
            // This new profile isolates healthy inspection. Existing workload
            // lifetimes stay unchanged; requests/attempts/recovery keep 5/3/60s.
            let started = start_with_policy(
                rooted,
                minimum,
                route.clone(),
                Some(MaintenancePolicy {
                    inspect_interval_ms: 100,
                    maximum_lifetime_ms: 60_000,
                    attempt_timeout_ms: 3000,
                    idle_timeout_ms: None,
                }),
            )
            .await;
            let mut cluster = match started {
                Ok(cluster) => cluster,
                Err(error) => {
                    route.wait.inspection.release.notify_one();
                    let stopped = controller.shutdown().await;
                    controller_domain.shutdown().await;
                    failures.push(format!(
                        "inspection startup extents={rooted} minimum={minimum:?}: {error:#}"
                    ));
                    if let Err(error) = stopped {
                        failures.push(format!("controller close: {error:#}"));
                    }
                    continue;
                }
            };
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
                    contend(&cluster, &history, rooted, minimum, &route, &peer).await?;
                    route.factory_refused.store(false, Ordering::Release);
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
            route.wait.inspection.release.notify_one();
            route.factory_refused.store(false, Ordering::Release);
            let closed = managed_fixture::close_group_nodes(
                std::mem::take(&mut cluster.nodes),
                &cluster.hosts,
            )
            .await;
            for root in std::mem::take(&mut cluster.roots) {
                eprintln!(
                    "retained inspection-handoff voter: {}",
                    root.keep().display()
                );
            }
            domain.shutdown().await;
            let stopped = controller.shutdown().await;
            controller_domain.shutdown().await;
            for result in [result, closed, stopped] {
                if let Err(error) = result {
                    failures.push(format!(
                        "inspection handoff extents={rooted} minimum={minimum:?}: {error:#}"
                    ));
                }
            }
        }
    }
    ensure!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}
