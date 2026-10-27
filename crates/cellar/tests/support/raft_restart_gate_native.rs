// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real reversible public/SDK admission; no readiness or shutdown from a gate.
use super::*;
use cellar::application::consensus::verification::{
    FleetRestartGateAdmission, FleetRestartGateCancelRequest, FleetRestartGateRequest,
    FleetRestartRequest,
};
use cellar::engine_api::HttpResponse;
use cellar::sdk::{Application, Cell, CellIdentity, Context, HandlerFuture, Invocation, Output};
use std::sync::Mutex;
use tokio::sync::Semaphore;

type BackgroundResult = std::result::Result<Vec<u8>, String>;

struct Signals {
    entered: tokio::sync::mpsc::UnboundedSender<()>,
    release: Semaphore,
    command: Mutex<Option<(RaftCellHandle, OperationContract)>>,
    completed: Mutex<Option<tokio::sync::oneshot::Sender<BackgroundResult>>>,
}

impl Signals {
    async fn write(&self) -> Result<Vec<u8>> {
        let (cell, contract) = self
            .command
            .lock()
            .map_err(|_| anyhow::anyhow!("command pin"))?
            .clone()
            .context("missing command")?;
        Ok(cell
            .command(
                contract,
                9i64.to_be_bytes().to_vec(),
                public_node::options(2)?,
            )
            .await?
            .result)
    }
}

struct HeldApplication {
    inner: Arc<dyn Application>,
    signals: Arc<Signals>,
}

impl Application for HeldApplication {
    fn supports_cell(&self, class: &str) -> bool {
        self.inner.supports_cell(class)
    }

    fn create_cell(&self, context: Context, identity: &CellIdentity) -> Result<Arc<dyn Cell>> {
        self.inner.create_cell(context, identity)
    }

    fn resolve_raft_class(
        &self,
        class: &str,
    ) -> std::result::Result<Arc<RaftClass>, RegistrationError> {
        self.inner.resolve_raft_class(class)
    }

    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = event else {
                return self.inner.clone().invoke(context, event).await;
            };
            let value = if request.url.ends_with("/hold") {
                self.signals.entered.send(())?;
                self.signals.release.acquire().await?.forget();
                self.signals.write().await?
            } else if request.url.ends_with("/background") {
                let signals = self.signals.clone();
                context.wait_until(async move {
                    signals.entered.send(())?;
                    signals.release.acquire().await?.forget();
                    let reply = signals
                        .completed
                        .lock()
                        .map_err(|_| anyhow::anyhow!("completion pin"))?
                        .take()
                        .context("missing background completion")?;
                    let value = signals.write().await;
                    reply
                        .send(value.as_ref().cloned().map_err(ToString::to_string))
                        .map_err(|_| anyhow::anyhow!("background completion dropped"))?;
                    value.map(|_| ())
                })?;
                b"accepted".to_vec()
            } else {
                b"ready".to_vec()
            };
            Ok(Output::Http(HttpResponse {
                status: 200,
                body: value,
                headers: vec![],
                stream: None,
                websocket: None,
                write_position: None,
                observed_position: None,
            }))
        })
    }
}

async fn held() -> Result<(
    Setup,
    Arc<Signals>,
    tokio::sync::mpsc::UnboundedReceiver<()>,
)> {
    let (entered, receive) = tokio::sync::mpsc::unbounded_channel();
    let signals = Arc::new(Signals {
        entered,
        release: Semaphore::new(0),
        command: Mutex::new(None),
        completed: Mutex::new(None),
    });
    let injected = signals.clone();
    let s = Box::pin(Setup::wrapped(move |inner| {
        Arc::new(HeldApplication {
            inner,
            signals: injected,
        })
    }))
    .await?;
    Ok((s, signals, receive))
}

async fn admit(
    s: &Setup,
    p: usize,
) -> Result<(
    FleetRestartRequest,
    FleetRestartGateRequest,
    FleetRestartGateAdmission,
)> {
    let boot = s.nodes[p]
        .host
        .observe_node(TIMEOUT)
        .await?
        .fleet_boot
        .context("boot")?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let preparing = FleetRestartRequest {
        identity: request(210, root.revision()),
        node: s.nodes[p].spec.node,
        expected_process: ProcessGeneration::new(boot.process)?,
        grace_seconds: 120,
    };
    let committed =
        s.f.catalog
            .begin_restart_maintenance(
                &root,
                &preparing,
                std::slice::from_ref(&s.namespace),
                TIMEOUT,
            )
            .await
            .context("prepare exact running boot")?;
    let mut identity = request(212, committed.snapshot.revision());
    identity.operation = preparing.identity.operation;
    let gate = FleetRestartGateRequest {
        identity,
        node: preparing.node,
        expected_process: preparing.expected_process,
        gate: ProposalId::new([213; 16])?,
    };
    let admission =
        s.f.catalog
            .admit_restart_gate(&committed.snapshot, &gate, TIMEOUT)
            .await
            .context("root reversible gate epoch")?;
    let current =
        s.f.remote(&s.spare, gate.node, admission.observed_revision())
            .await
            .context("install authenticated gate revision")?;
    ensure!(
        current
            .view
            .current_enrollment
            .context("gate enrollment")?
            .epoch
            == admission.enrollment_epoch()
    );
    Ok((preparing, gate, admission))
}

async fn paused(s: &Setup, client: &reqwest::Client, url: &str) -> Result<()> {
    s.f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            loop {
                if client.get(format!("{url}/probe")).send().await?.status() == 503 {
                    return Ok::<_, anyhow::Error>(());
                }
                cellar::asyncrt::sleep(Duration::from_millis(5)).await;
            }
        }))
        .await??
}

fn cancel(
    gate: &FleetRestartGateRequest,
    revision: u64,
    id: u8,
) -> Result<FleetRestartGateCancelRequest> {
    let mut identity = request(id, revision);
    identity.operation = gate.identity.operation;
    Ok(FleetRestartGateCancelRequest {
        identity,
        node: gate.node,
        expected_process: gate.expected_process,
        fence: ProposalId::new([219; 16])?,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_gate_drains_accepted_http_and_preserves_consensus() -> Result<()> {
    let (s, signals, mut entered) = held().await?;
    let p = primary(&s.f, &s.cells).await?;
    *signals
        .command
        .lock()
        .map_err(|_| anyhow::anyhow!("command pin"))? =
        Some((s.cells[p].clone(), s.f.definition.increment.clone()));
    let client = reqwest::Client::builder().no_proxy().build()?;
    let url = format!("http://{}", s.nodes[p].node.info().public_address);
    let caller = client.clone();
    let endpoint = format!("{url}/hold");
    let response = s.f.domain.spawn(async move {
        let response = caller.post(endpoint).send().await?;
        Ok::<_, anyhow::Error>((response.status(), response.bytes().await?.to_vec()))
    })?;
    s.f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, entered.recv()))
        .await??
        .context("HTTP not entered")?;
    let (preparing, gate, admitted) = admit(&s, p).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let metadata = s.f.catalog.clone();
    let cut = root.clone();
    let intent = gate.clone();
    let observer = s.spare.clone();
    let mut quiesce = s.f.domain.spawn(async move {
        metadata
            .quiesce_restart(&cut, &intent, &observer, TIMEOUT)
            .await
    })?;
    if let Err(error) = paused(&s, &client, &url).await {
        return match quiesce.await? {
            Err(cause) => {
                Err(anyhow::Error::new(cause).context("quiesce refused before public closure"))
            }
            Ok(_) => Err(error.context("quiesce acknowledged without public closure")),
        };
    }
    ensure!(s
        .f
        .domain
        .run(cellar::asyncrt::timeout(
            Duration::from_millis(30),
            &mut quiesce
        ))
        .await?
        .is_err());
    ensure!(!s.nodes[p].node.is_closed());
    s.nodes[p].host.observe_node(TIMEOUT).await?;
    signals.release.add_permits(1);
    let (status, bytes) = response.await??;
    ensure!(
        status == 200
            && i64::from_be_bytes(
                bytes
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("HTTP result"))?
            ) == 16
    );
    let accepted = quiesce.await??;
    ensure!(accepted.applied_revision() == admitted.applied_revision());
    ensure!(matches!(
        s.cells[p]
            .command(
                s.f.definition.increment.clone(),
                1i64.to_be_bytes().to_vec(),
                public_node::options(3)?
            )
            .await,
        Err(CommandFailure::Unavailable)
    ));
    let mut forged = serde_json::to_value(admitted.quiesce_request()?)?;
    forged["action"] = serde_json::json!("stop");
    let forged: FleetStopRequest = serde_json::from_value(forged)?;
    ensure!(s.nodes[p]
        .node
        .request_fleet_stop(forged, TIMEOUT)
        .await
        .is_err());
    ensure!(!s.nodes[p].node.is_closed());
    let state = s.cells[p].inspect().await?.member.context("member")?;
    let q = (p + 1) % 3;
    s.cells[p]
        .transfer_leadership(
            LeadershipTransfer {
                expected_term: state.term,
                expected_revision: state.membership_revision,
                target: MemberId::new(q as u64 + 1)?,
            },
            TIMEOUT,
        )
        .await?;
    s.f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            loop {
                if s.cells[q].status().await.is_ok_and(|v| v.command_ready) {
                    return Ok::<_, anyhow::Error>(());
                }
                cellar::asyncrt::sleep(Duration::from_millis(5)).await;
            }
        }))
        .await???;
    let written = s.cells[q]
        .command(
            s.f.definition.increment.clone(),
            5i64.to_be_bytes().to_vec(),
            public_node::options(3)?,
        )
        .await?;
    ensure!(
        i64::from_be_bytes(
            written
                .result
                .try_into()
                .map_err(|_| anyhow::anyhow!("write result"))?
        ) == 21
    );
    let root = s.f.catalog.read(TIMEOUT).await?;
    let mut early = preparing.clone();
    early.identity.id = [217; 16];
    early.identity.expected_revision = root.revision();
    ensure!(matches!(
        s.f.catalog
            .cancel_restart_preparation(&root, &early, TIMEOUT)
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    let cancelled =
        s.f.catalog
            .cancel_restart_gate(
                &root,
                &cancel(&gate, root.revision(), 218)?,
                Some(&s.spare),
                TIMEOUT,
            )
            .await?;
    ensure!(
        s.f.catalog
            .authorize_host(&cancelled.snapshot, gate.node, TIMEOUT)
            .await?
            .state()
            == FleetNodeState::Ready
    );
    ensure!(s.nodes[p]
        .node
        .request_fleet_stop(admitted.quiesce_request()?, TIMEOUT)
        .await
        .is_err());
    ensure!(client.get(format!("{url}/probe")).send().await?.status() == 200);
    write_read(&s.f, &s.cells, 4, 7, 28).await?;
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_gate_cancel_defeats_a_timed_out_quiesce() -> Result<()> {
    let (s, signals, mut entered) = held().await?;
    let p = primary(&s.f, &s.cells).await?;
    *signals
        .command
        .lock()
        .map_err(|_| anyhow::anyhow!("command pin"))? =
        Some((s.cells[p].clone(), s.f.definition.increment.clone()));
    let client = reqwest::Client::builder().no_proxy().build()?;
    let url = format!("http://{}", s.nodes[p].node.info().public_address);
    let caller = client.clone();
    let endpoint = format!("{url}/hold");
    let response = s.f.domain.spawn(async move {
        let response = caller.post(endpoint).send().await?;
        Ok::<_, anyhow::Error>((response.status(), response.bytes().await?.to_vec()))
    })?;
    s.f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, entered.recv()))
        .await??
        .context("HTTP not entered")?;
    let (_, gate, admitted) = admit(&s, p).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let metadata = s.f.catalog.clone();
    let cut = root.clone();
    let intent = gate.clone();
    let observer = s.spare.clone();
    let quiesce = s.f.domain.spawn(async move {
        metadata
            .quiesce_restart(&cut, &intent, &observer, Duration::from_millis(250))
            .await
    })?;
    if let Err(error) = paused(&s, &client, &url).await {
        return match quiesce.await? {
            Err(cause) => {
                Err(anyhow::Error::new(cause)
                    .context("timed quiesce refused before public closure"))
            }
            Ok(_) => Err(error.context("timed quiesce acknowledged without public closure")),
        };
    }
    ensure!(quiesce.await?.is_err());
    let root = s.f.catalog.read(TIMEOUT).await?;
    s.f.catalog
        .cancel_restart_gate(
            &root,
            &cancel(&gate, root.revision(), 218)?,
            Some(&s.spare),
            TIMEOUT,
        )
        .await?;
    signals.release.add_permits(1);
    let (status, bytes) = response.await??;
    ensure!(
        status == 200
            && i64::from_be_bytes(
                bytes
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("HTTP result"))?
            ) == 16
    );
    ensure!(s.nodes[p]
        .node
        .request_fleet_stop(admitted.quiesce_request()?, TIMEOUT)
        .await
        .is_err());
    ensure!(client.get(format!("{url}/probe")).send().await?.status() == 200);
    write_read(&s.f, &s.cells, 3, 5, 21).await?;
    s.close().await
}

async fn cancelled_birth(s: &Setup, catalog: &FleetCatalog, excluded: usize) -> Result<()> {
    let mut proposal = s.proposal.clone();
    proposal.scope.group = GroupId::new([225; 16])?;
    proposal.scope.cell = "Counter:maintenance-cancelled".into();
    let mut nodes = s
        .nodes
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != excluded)
        .map(|(_, n)| n.spec.node)
        .collect::<Vec<_>>();
    nodes.push(NodeId::new([7; 16])?);
    proposal.voters = nodes
        .into_iter()
        .enumerate()
        .map(|(i, node)| {
            Ok(InitialMember {
                member: MemberId::new(i as u64 + 1)?,
                node,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let root = catalog.read(TIMEOUT).await?;
    let reserved = catalog
        .reserve_birth(
            &root,
            &request(225, root.revision()),
            &s.namespace,
            proposal,
            TIMEOUT,
        )
        .await?;
    s.f.domain
        .run(s.namespace.cancel_managed_birth(
            &reserved.authorization,
            ProposalId::new([227; 16])?,
            TIMEOUT,
        ))
        .await??;
    let root = catalog.read(TIMEOUT).await?;
    catalog
        .confirm_birth(
            &root,
            &request(228, root.revision()),
            &s.namespace,
            &reserved.authorization,
            TIMEOUT,
        )
        .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_gate_cancel_reconciles_synced_fence_and_root_churn() -> Result<()> {
    let s = Box::pin(Setup::new()).await?;
    let p = primary(&s.f, &s.cells).await?;
    let (_, gate, admitted) = admit(&s, p).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    s.f.catalog
        .quiesce_restart(&root, &gate, &s.spare, TIMEOUT)
        .await
        .context("quiesce exact rooted gate")?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    for change in 0..3 {
        let mut rebound = gate.clone();
        match change {
            0 => rebound.gate = ProposalId::new([214; 16])?,
            1 => rebound.expected_process = ProcessGeneration::new([215; 16])?,
            _ => rebound.identity.principal = "another principal".into(),
        }
        ensure!(matches!(
            s.f.catalog
                .admit_restart_gate(&root, &rebound, TIMEOUT)
                .await,
            Err(FleetCatalogError::PayloadConflict)
        ));
    }
    let cancellation = cancel(&gate, root.revision(), 220)?;
    s.f.store.fleet_cas_hang.store(true, Ordering::Release);
    s.f.store.hold_next_fleet_cas.store(true, Ordering::Release);
    let metadata = s.f.catalog.clone();
    let cut = root.clone();
    let intent = cancellation.clone();
    let observer = s.spare.clone();
    let pending = s.f.domain.spawn(async move {
        metadata
            .cancel_restart_gate(&cut, &intent, Some(&observer), TIMEOUT)
            .await
    })?;
    s.waiting().await?;
    let recreated = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    cancelled_birth(&s, &recreated, p).await?;
    s.f.store.fleet_cas_hang.store(false, Ordering::Release);
    s.f.store.fleet_cas_resume.notify_waiters();
    ensure!(matches!(pending.await?, Err(FleetCatalogError::Conflict)));
    ensure!(s.nodes[p]
        .node
        .request_fleet_stop(admitted.quiesce_request()?, TIMEOUT)
        .await
        .is_err());
    write_read(&s.f, &s.cells, 2, 5, 12).await?;
    let now = recreated.read(TIMEOUT).await?;
    s.f.remote(&s.spare, gate.node, now.revision())
        .await
        .context("install cancellation retry revision")?;
    let mut rebound = cancellation.clone();
    rebound.identity.expected_revision = now.revision();
    ensure!(matches!(
        recreated
            .cancel_restart_gate(&now, &rebound, Some(&s.spare), TIMEOUT)
            .await,
        Err(FleetCatalogError::PayloadConflict)
    ));
    let mut wrong = cancel(&gate, now.revision(), 230)?;
    wrong.fence = ProposalId::new([231; 16])?;
    ensure!(recreated
        .cancel_restart_gate(&now, &wrong, Some(&s.spare), TIMEOUT)
        .await
        .is_err());
    ensure!(recreated.read(TIMEOUT).await?.revision() == now.revision());
    let retry = cancel(&gate, now.revision(), 232)?;
    s.f.store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    ensure!(recreated
        .cancel_restart_gate(&now, &retry, Some(&s.spare), TIMEOUT)
        .await
        .is_err());
    s.f.store.fail_fleet_read.store(false, Ordering::Release);
    let restarted = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let now = restarted.read(TIMEOUT).await?;
    let completed = restarted
        .cancel_restart_gate(&now, &retry, None, TIMEOUT)
        .await?;
    ensure!(
        restarted
            .authorize_host(&now, gate.node, TIMEOUT)
            .await?
            .state()
            == FleetNodeState::Ready
    );
    ensure!(matches!(
        restarted.authorize_capacity(&now, gate.node, TIMEOUT).await,
        Err(FleetCatalogError::Conflict)
    ));
    let next = request(234, now.revision());
    let next = restarted
        .begin_evacuation(
            &now,
            &next,
            NodeId::new([7; 16])?,
            std::slice::from_ref(&s.namespace),
            TIMEOUT,
        )
        .await?;
    let historical = restarted
        .cancel_restart_gate(&next.snapshot, &retry, None, TIMEOUT)
        .await?;
    ensure!(
        historical.applied_revision == completed.applied_revision
            && historical.snapshot.revision() == next.snapshot.revision()
    );
    let mut blocked = request(236, next.snapshot.revision());
    blocked.operation = [237; 16];
    ensure!(matches!(
        restarted
            .begin_evacuation(
                &next.snapshot,
                &blocked,
                s.nodes[(p + 1) % 3].spec.node,
                &[],
                TIMEOUT
            )
            .await,
        Err(FleetCatalogError::DisruptionInProgress { .. })
    ));
    write_read(&s.f, &s.cells, 3, 9, 21).await?;
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_gate_drains_post_response_background_sdk_writes() -> Result<()> {
    let (s, signals, mut entered) = held().await?;
    let p = primary(&s.f, &s.cells).await?;
    *signals
        .command
        .lock()
        .map_err(|_| anyhow::anyhow!("command pin"))? =
        Some((s.cells[p].clone(), s.f.definition.increment.clone()));
    let (completed, written) = tokio::sync::oneshot::channel();
    *signals
        .completed
        .lock()
        .map_err(|_| anyhow::anyhow!("completion pin"))? = Some(completed);
    let client = reqwest::Client::builder().no_proxy().build()?;
    let url = format!("http://{}", s.nodes[p].node.info().public_address);
    let accepted = client.post(format!("{url}/background")).send().await?;
    ensure!(accepted.status() == 200 && accepted.bytes().await?.as_ref() == b"accepted");
    s.f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, entered.recv()))
        .await??
        .context("background not entered")?;
    let (_, gate, admitted) = admit(&s, p).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let metadata = s.f.catalog.clone();
    let cut = root.clone();
    let intent = gate.clone();
    let observer = s.spare.clone();
    let mut quiesce = s.f.domain.spawn(async move {
        metadata
            .quiesce_restart(&cut, &intent, &observer, TIMEOUT)
            .await
    })?;
    paused(&s, &client, &url).await?;
    ensure!(
        s.f.domain
            .run(cellar::asyncrt::timeout(
                Duration::from_millis(30),
                &mut quiesce
            ))
            .await?
            .is_err(),
        "quiescence acknowledged while accepted background work was held"
    );
    signals.release.add_permits(1);
    let bytes =
        s.f.domain
            .run(cellar::asyncrt::timeout(TIMEOUT, written))
            .await???
            .map_err(anyhow::Error::msg)
            .context("accepted background SDK write")?;
    ensure!(
        i64::from_be_bytes(
            bytes
                .try_into()
                .map_err(|_| anyhow::anyhow!("background result"))?
        ) == 16
    );
    ensure!(quiesce.await??.applied_revision() == admitted.applied_revision());
    ensure!(matches!(
        s.cells[p]
            .command(
                s.f.definition.increment.clone(),
                1i64.to_be_bytes().to_vec(),
                public_node::options(3)?
            )
            .await,
        Err(CommandFailure::Unavailable)
    ));
    let root = s.f.catalog.read(TIMEOUT).await?;
    s.f.catalog
        .cancel_restart_gate(
            &root,
            &cancel(&gate, root.revision(), 218)?,
            Some(&s.spare),
            TIMEOUT,
        )
        .await?;
    write_read(&s.f, &s.cells, 3, 5, 21).await?;
    s.close().await
}
