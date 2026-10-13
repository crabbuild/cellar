// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Adapted for Cellar from Celld's production node loop.

//! Production dispatch and bounded drain without signals or process exits.

use super::*;
use crate::node_lifecycle::{FenceReport, NodeOutcome, ShutdownPolicy, ShutdownReport};

pub(super) struct RunPolicy {
    pub shutdown: ShutdownPolicy,
    pub local_cache_max_bytes: Option<u64>,
    pub local_cache_prune_period: std::time::Duration,
}

pub(super) struct Channels {
    pub do_call_rx: mpsc::UnboundedReceiver<DoCallReq>,
    pub gate_rx: mpsc::UnboundedReceiver<crate::host_channels::GateReq>,
    pub facet_rx: mpsc::UnboundedReceiver<crate::host_channels::FacetReq>,
    pub rpc_call_rx: mpsc::UnboundedReceiver<RpcCallReq>,
    pub service_call_rx: mpsc::UnboundedReceiver<SvcCallReq>,
    pub service_rpc_rx: mpsc::UnboundedReceiver<SvcRpcReq>,
    pub queue_dispatch_rx: mpsc::UnboundedReceiver<QueueDispatchReq>,
    pub asset_call_rx: mpsc::UnboundedReceiver<AssetCallReq>,
    pub websocket_rx: mpsc::UnboundedReceiver<WebSocketFuture>,
    pub outbound_ws_rx: mpsc::UnboundedReceiver<crate::ws_registry::OutboundWsReq>,
}

impl Channels {
    /// Install dispatch once in the owning domain, after predecessor recovery.
    pub(super) fn install(websocket_rx: mpsc::UnboundedReceiver<WebSocketFuture>) -> Self {
        let (tx, do_call_rx) = mpsc::unbounded_channel();
        crate::host_channels::set_do_call_tx(tx);
        let (tx, gate_rx) = mpsc::unbounded_channel();
        crate::host_channels::set_gate_tx(tx);
        let (tx, facet_rx) = mpsc::unbounded_channel();
        crate::host_channels::set_facet_tx(tx);
        let (tx, rpc_call_rx) = mpsc::unbounded_channel();
        crate::host_channels::set_rpc_call_tx(tx);
        let (tx, service_call_rx) = mpsc::unbounded_channel();
        crate::host_channels::set_svc_call_tx(tx);
        let (tx, service_rpc_rx) = mpsc::unbounded_channel();
        crate::host_channels::set_svc_rpc_tx(tx);
        let (tx, queue_dispatch_rx) = mpsc::unbounded_channel();
        crate::host_channels::set_queue_dispatch_tx(tx);
        let (tx, asset_call_rx) = mpsc::unbounded_channel();
        crate::host_channels::set_asset_call_tx(tx);
        let (tx, outbound_ws_rx) = mpsc::unbounded_channel();
        crate::host_channels::set_outbound_ws_tx(tx);
        Self {
            do_call_rx,
            gate_rx,
            facet_rx,
            rpc_call_rx,
            service_call_rx,
            service_rpc_rx,
            queue_dispatch_rx,
            asset_call_rx,
            outbound_ws_rx,
            websocket_rx,
        }
    }
}

pub(super) struct PreparedDriver {
    pub app: AppHandle,
    pub listener: tokio::net::TcpListener,
    pub internal_listener: tokio::net::TcpListener,
    pub channels: Channels,
    pub fence_rx: mpsc::UnboundedReceiver<FenceReport>,
    pub actor_exit_rx: mpsc::UnboundedReceiver<Result<(), String>>,
    pub ready_ownership: Option<Arc<BucketOwnership>>,
    pub fleet_bucket: Option<Bucket>,
    pub clean_reload_node: String,
    pub process_generation: String,
    pub data_dir: std::path::PathBuf,
    pub durability_owner: Option<crate::node_log::DurabilityOwner>,
    pub policy: RunPolicy,
    #[cfg(all(test, cellar_internal_tests))]
    pub shutdown_accept_failure_test_active: bool,
}

struct AdmissionFence(Arc<std::sync::atomic::AtomicBool>);

impl Drop for AdmissionFence {
    fn drop(&mut self) {
        self.0.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

fn fence_outcome(report: Option<FenceReport>) -> NodeOutcome {
    match report {
        Some(FenceReport { code, reason }) => NodeOutcome::Fenced { code, reason },
        None => NodeOutcome::CoreFailed {
            error: "the core fencing report channel closed without an authority report".into(),
        },
    }
}

/// Shared production dispatch and drain path for node adapters.
/// The caller supplies a stop future and supervises the returned outcome.
pub(super) fn run<F>(
    prepared: PreparedDriver,
    stop: F,
) -> impl std::future::Future<Output = anyhow::Result<NodeOutcome>> + Send
where
    F: std::future::Future<Output = ShutdownMode> + Send,
{
    // Capture before the first poll, so dropping an unstarted driver also
    // closes admission. Its host still owns joining the execution domain.
    let admission_fence = AdmissionFence(prepared.app.draining.clone());
    async move {
        let _admission_fence = admission_fence;
        run_inner(prepared, stop).await
    }
}

fn bounded_stall_deadline(
    window: std::time::Duration,
    absolute: tokio::time::Instant,
) -> tokio::time::Instant {
    tokio::time::Instant::now()
        .checked_add(window)
        .unwrap_or(absolute)
        .min(absolute)
}

async fn run_inner<F>(prepared: PreparedDriver, stop: F) -> anyhow::Result<NodeOutcome>
where
    F: std::future::Future<Output = ShutdownMode> + Send,
{
    let PreparedDriver {
        app,
        listener,
        internal_listener,
        channels,
        mut fence_rx,
        mut actor_exit_rx,
        ready_ownership,
        fleet_bucket,
        clean_reload_node,
        process_generation,
        data_dir,
        mut durability_owner,
        policy,
        #[cfg(all(test, cellar_internal_tests))]
        shutdown_accept_failure_test_active,
    } = prepared;
    policy.shutdown.validate()?;
    anyhow::ensure!(
        tokio::time::Instant::now()
            .checked_add(std::time::Duration::from_millis(policy.shutdown.total_ms))
            .is_some(),
        "shutdown total_ms exceeds the host clock range"
    );
    anyhow::ensure!(
        !policy.local_cache_prune_period.is_zero(),
        "local cache prune period must be positive"
    );
    anyhow::ensure!(
        tokio::time::Instant::now()
            .checked_add(policy.local_cache_prune_period)
            .is_some(),
        "local cache prune period exceeds the host clock range"
    );
    let local_cache_replication = app
        .runtime
        .as_ref()
        .and_then(|runtime| runtime.replication());
    let local_cache_max_bytes = policy.local_cache_max_bytes;
    let Channels {
        mut do_call_rx,
        mut gate_rx,
        mut facet_rx,
        mut rpc_call_rx,
        mut service_call_rx,
        mut service_rpc_rx,
        mut queue_dispatch_rx,
        mut asset_call_rx,
        mut websocket_rx,
        mut outbound_ws_rx,
    } = channels;
    let mut stop = std::pin::pin!(stop);
    let (shutdown_tx, mut shutdown_rx) = mpsc::unbounded_channel();
    #[cfg(all(test, cellar_internal_tests))]
    if shutdown_accept_failure_test_active {
        let _ = shutdown_tx.send(ShutdownMode::Handoff);
    }
    // Caller-directed stops and authenticated operator stops use the same
    // bounded ownership and durability drain.
    let (connection_drain_tx, connection_drain) = watch::channel(false);
    let drain_ms = policy.shutdown.no_progress_ms;
    let shutdown_total_ms = policy.shutdown.total_ms;
    let drain_token_wait_ms = policy.shutdown.token_wait_ms;
    // A hung connection must not consume the whole preserve budget: the
    // semantic drain and the clean-reload certificate come out of the same
    // deadline.
    let connection_grace =
        CONNECTION_DRAIN_GRACE.min(std::time::Duration::from_millis(drain_ms / 4));
    let mut connections: FuturesUnordered<ConnectionFuture> = FuturesUnordered::new();
    let mut do_calls: FuturesUnordered<DoCallFuture> = FuturesUnordered::new();
    let mut gate_calls: FuturesUnordered<DoCallFuture> = FuturesUnordered::new();
    let mut service_calls: FuturesUnordered<DoCallFuture> = FuturesUnordered::new();
    let mut queue_calls: FuturesUnordered<DoCallFuture> = FuturesUnordered::new();
    let mut facet_calls: FuturesUnordered<DoCallFuture> = FuturesUnordered::new();
    let mut asset_calls: FuturesUnordered<AssetCallFuture> = FuturesUnordered::new();
    let application_work = crate::asyncrt::services().application_work_registry();
    // Queued requests are work too: a channel receive and the future it
    // creates must not expose an empty interval to the shutdown barrier.
    macro_rules! pending_dispatches {
        () => {
            do_calls.len()
                + gate_calls.len()
                + service_calls.len()
                + queue_calls.len()
                + facet_calls.len()
                + asset_calls.len()
                + do_call_rx.len()
                + gate_rx.len()
                + rpc_call_rx.len()
                + service_call_rx.len()
                + service_rpc_rx.len()
                + queue_dispatch_rx.len()
                + facet_rx.len()
                + asset_call_rx.len()
        };
    }
    let mut websockets: FuturesUnordered<WebSocketFuture> = FuturesUnordered::new();
    let mut cache_prunes: FuturesUnordered<CachePruneFuture> = FuturesUnordered::new();
    let mut replication_health = tokio::time::interval(std::time::Duration::from_millis(250));
    let mut local_cache_prune = tokio::time::interval_at(
        tokio::time::Instant::now() + policy.local_cache_prune_period,
        policy.local_cache_prune_period,
    );
    local_cache_prune.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let shutdown_mode = loop {
        crate::asyncrt::select! {
            connection = listener.accept() => {
                let (stream, _) = connection?;
                connections.push(serve_http_connection(
                    stream,
                    HttpSurface::Public,
                    app.clone(),
                    shutdown_tx.clone(),
                    connection_drain.clone(),
                    connection_grace,
                ));
            }
            connection = internal_listener.accept() => {
                let (stream, _) = connection?;
                connections.push(serve_http_connection(
                    stream,
                    HttpSurface::Internal,
                    app.clone(),
                    shutdown_tx.clone(),
                    connection_drain.clone(),
                    connection_grace,
                ));
            }
            Some(()) = connections.next(), if !connections.is_empty() => {}
            call = do_call_rx.recv() => {
                let Some(call) = call else {
                    anyhow::bail!("Durable Object call channel closed");
                };
                do_calls.push(Box::pin(dispatch_do_call(app.clone(), call)));
            }
            Some(()) = do_calls.next(), if !do_calls.is_empty() => {}
            req = gate_rx.recv() => {
                let Some(req) = req else {
                    anyhow::bail!("output-gate channel closed");
                };
                gate_calls.push(Box::pin(dispatch_gate(app.clone(), req)));
            }
            Some(()) = gate_calls.next(), if !gate_calls.is_empty() => {}
            call = service_call_rx.recv() => {
                let Some(call) = call else {
                    anyhow::bail!("service call channel closed");
                };
                service_calls.push(Box::pin(dispatch_service_call(app.clone(), call)));
            }
            call = service_rpc_rx.recv() => {
                let Some(call) = call else {
                    anyhow::bail!("service RPC channel closed");
                };
                service_calls.push(Box::pin(dispatch_service_rpc(app.clone(), call)));
            }
            Some(()) = service_calls.next(), if !service_calls.is_empty() => {}
            facet = facet_rx.recv() => {
                let Some(facet) = facet else {
                    anyhow::bail!("facet dispatch channel closed");
                };
                facet_calls.push(Box::pin(dispatch_facet(app.clone(), facet)));
            }
            Some(()) = facet_calls.next(), if !facet_calls.is_empty() => {}
            call = queue_dispatch_rx.recv() => {
                let Some(call) = call else {
                    anyhow::bail!("Queue dispatch channel closed");
                };
                queue_calls.push(Box::pin(dispatch_queue_batch(app.clone(), call)));
            }
            Some(()) = queue_calls.next(), if !queue_calls.is_empty() => {}
            call = rpc_call_rx.recv() => {
                let Some(call) = call else {
                    anyhow::bail!("Durable Object RPC channel closed");
                };
                do_calls.push(Box::pin(dispatch_rpc_call(app.clone(), call)));
            }
            call = asset_call_rx.recv() => {
                let Some(call) = call else {
                    anyhow::bail!("asset call channel closed");
                };
                asset_calls.push(Box::pin(dispatch_asset_call(app.clone(), call)));
            }
            Some(()) = asset_calls.next(), if !asset_calls.is_empty() => {}
            socket = websocket_rx.recv() => {
                let Some(socket) = socket else {
                    anyhow::bail!("WebSocket channel closed");
                };
                websockets.push(socket);
            }
            Some(()) = websockets.next(), if !websockets.is_empty() => {}
            _ = local_cache_prune.tick(), if local_cache_replication.is_some()
                && local_cache_max_bytes.is_some() && cache_prunes.is_empty() => {
                let replication = local_cache_replication.clone().unwrap();
                let max_bytes = local_cache_max_bytes.unwrap();
                cache_prunes.push(Box::pin(async move {
                    let result = crate::asyncrt::blocking(move || {
                        replication.prune_local_cache(max_bytes)
                    }).await;
                    (max_bytes, result)
                }));
            }
            Some((max_bytes, result)) = cache_prunes.next(), if !cache_prunes.is_empty() => {
                match result {
                    Ok(Ok((kept, evicted, bytes))) if evicted > 0 => {
                        tracing::info!(
                            event = "local_cache_pruned",
                            kept,
                            evicted,
                            bytes,
                            max_bytes,
                            "pruned least-recently-used eviction snapshots"
                        );
                    }
                    Ok(Ok(_)) => {}
                    Ok(Err(error)) => {
                        tracing::warn!(%error, "local cache inventory failed");
                    }
                    Err(error) => {
                        tracing::warn!(%error, "local cache pruning task failed");
                    }
                }
            }
            outbound = outbound_ws_rx.recv() => {
                let Some(outbound) = outbound else {
                    anyhow::bail!("outbound WebSocket channel closed");
                };
                let app = app.clone();
                websockets.push(Box::pin(async move {
                    if let Err(error) = outbound_websocket_task(app, outbound).await {
                        tracing::warn!("cellar outbound WebSocket failed: {error:#}");
                    }
                }));
            }
            mode = shutdown_rx.recv() => break mode.unwrap_or(ShutdownMode::Handoff),
            mode = &mut stop => break mode,
            report = fence_rx.recv() => {
                return Ok(fence_outcome(report));
            }
            actor_exit = actor_exit_rx.recv() => {
                let error = match actor_exit {
                    Some(Err(error)) => error,
                    Some(Ok(())) => "the core actor stopped unexpectedly".to_string(),
                    None => "the core actor thread panicked".to_string(),
                };
                tracing::error!(
                    event = "core_actor_exit",
                    %error,
                    "SELF-FENCE: the core actor exited unexpectedly"
                );
                return Ok(NodeOutcome::CoreFailed { error });
            }
            _ = replication_health.tick() => {
                if let Some(runtime) = &app.runtime {
                    match runtime.replication_status() {
                        Ok(None) => {}
                        Ok(Some(status)) => {
                            tracing::warn!("SELF-FENCE: replication process exited unexpectedly: {status}");
                            return Ok(NodeOutcome::ReplicationFailed { error: status.to_string() });
                        }
                        Err(error) => {
                            tracing::warn!("SELF-FENCE: replication process health check failed: {error}");
                            return Ok(NodeOutcome::ReplicationFailed { error: error.to_string() });
                        }
                    }
                }
            }
        }
    };
    // This deadline starts at the stop signal and covers token acquisition,
    // request drain, ownership handoff, transport flush, and local durability
    // shutdown. Progress can extend only the inner stall deadline. The
    // absolute bound leaves the orchestrator time to observe an orderly exit
    // instead of replacing a progressing donor with SIGKILL.
    let shutdown_started = tokio::time::Instant::now();
    let process_deadline = shutdown_started
        .checked_add(std::time::Duration::from_millis(shutdown_total_ms))
        .context("shutdown budget exceeds the host clock range")?;
    // A donor normally spends its complete budget making safe handoff
    // progress. Reserve at most one second, and never more than ten percent of
    // a short configured budget, for the token CAS. Otherwise a deadline-cut
    // donor leaves the replacement behind the 120-second token TTL and turns
    // serialization into artificial rollout recovery.
    let token_release_reserve_ms = if shutdown_mode == ShutdownMode::Handoff
        && fleet_bucket.is_some()
        && drain_token_wait_ms > 0
    {
        (shutdown_total_ms / 10).min(1_000)
    } else {
        0
    };
    let handoff_deadline =
        process_deadline - std::time::Duration::from_millis(token_release_reserve_ms);
    // Stop public admission immediately. Before either stop mode changes cell
    // lifecycle, finish admitted responses, complete application event drivers
    // (including waitUntil), and their queued SDK dispatches. A cell call
    // reaches its output gate only after it writes, so
    // beginning the final durability cut first would let that call cross the
    // cut without a core activity pin. Peer connections stay open:
    // cells outside the current batch must remain reachable during handoff.
    app.draining
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let stall_window = std::time::Duration::from_millis(drain_ms);
    // New drain-time connections need a fresh watch version. A receiver cloned
    // from `connection_drain` after its signal would close immediately, before
    // it could serve a health response or authenticated peer request.
    let (drain_connection_tx, drain_connection) = watch::channel(false);
    let mut drain_connections: FuturesUnordered<ConnectionFuture> = FuturesUnordered::new();
    // Each acceptor waits until this loop receives its previous connection.
    // One surface therefore cannot occupy both slots and block the other.
    let (shutdown_connection_tx, mut shutdown_connection_rx) = mpsc::channel(2);
    #[cfg(all(test, cellar_internal_tests))]
    let mut shutdown_accept_failure_recovered_for_test = !shutdown_accept_failure_test_active;
    #[cfg(all(test, cellar_internal_tests))]
    shutdown_accept_failure_test_checkpoint(
        shutdown_accept_failure_test_active,
        "listener failure armed",
    );
    #[cfg(all(test, cellar_internal_tests))]
    {
        spawn_shutdown_acceptor(
            listener,
            HttpSurface::Public,
            shutdown_connection_tx.clone(),
            shutdown_accept_failure_test_active,
        );
        spawn_shutdown_acceptor(
            internal_listener,
            HttpSurface::Internal,
            shutdown_connection_tx,
            false,
        );
    }
    #[cfg(not(all(test, cellar_internal_tests)))]
    {
        spawn_shutdown_acceptor(
            listener,
            HttpSurface::Public,
            shutdown_connection_tx.clone(),
        );
        spawn_shutdown_acceptor(
            internal_listener,
            HttpSurface::Internal,
            shutdown_connection_tx,
        );
    }
    let mut drain_token_hold: Option<crate::drain_token::Hold> = None;
    let (admitted_work_drained, ingress_drained_before_cut) = {
        // Serialize donors: claim the fleet drain token beside the
        // admitted-request drain, so simultaneous stop signals hand off one
        // node at a time. A waiting donor keeps serving peers and refuses
        // new handoffs because it is already draining. The wait has its own
        // bound inside the task; the pre-drain deadline never covers it.
        let drain_token: Arc<std::sync::Mutex<Option<crate::drain_token::Outcome>>> =
            Arc::new(std::sync::Mutex::new(None));
        match &fleet_bucket {
            Some(bucket) if shutdown_mode == ShutdownMode::Handoff && drain_token_wait_ms > 0 => {
                let bucket = bucket.clone();
                let slot = drain_token.clone();
                let token_node = clean_reload_node.clone();
                let baseline_ownership = ready_ownership.clone();
                let wait = std::time::Duration::from_millis(drain_token_wait_ms)
                    .min(handoff_deadline.saturating_duration_since(tokio::time::Instant::now()));
                crate::asyncrt::spawn(async move {
                    let now_ms = crate::ownership_store::now_ms();
                    let restoration_baseline = match baseline_ownership {
                        Some(ownership) => match ownership.read_capacity_peers().await {
                            Ok(peers) => {
                                let mut baseline = peers
                                    .into_iter()
                                    .filter(|peer| peer.expires_ms > now_ms)
                                    .map(|peer| cellar_logic::drain::RestorationBaseline {
                                        node: peer.node,
                                        restoring: peer.restoring,
                                    })
                                    .collect::<Vec<_>>();
                                baseline.sort_by(|left, right| left.node.cmp(&right.node));
                                baseline
                            }
                            Err(error) => {
                                tracing::warn!(
                                    event = "drain_restoration_baseline_unavailable",
                                    %error,
                                    "the donor could not snapshot the pre-drain restore baseline"
                                );
                                Vec::new()
                            }
                        },
                        None => Vec::new(),
                    };
                    tracing::info!(
                        event = "drain_restoration_baseline",
                        nodes = restoration_baseline.len(),
                        maximum = restoration_baseline
                            .iter()
                            .map(|baseline| baseline.restoring)
                            .max()
                            .unwrap_or(0),
                        "captured the pre-drain restore baseline"
                    );
                    let outcome = crate::drain_token::acquire(
                        &bucket,
                        &token_node,
                        wait,
                        restoration_baseline,
                    )
                    .await;
                    *slot.lock().expect("drain token slot") = Some(outcome);
                })
                .detach();
            }
            _ => {
                *drain_token.lock().expect("drain token slot") =
                    Some(crate::drain_token::Outcome::Disabled);
            }
        }
        let mut pre_drain_deadline = bounded_stall_deadline(stall_window, handoff_deadline);
        let mut pre_drain_tick = tokio::time::interval(std::time::Duration::from_millis(5));
        let mut admitted = None;
        let mut token_finished = false;
        let mut previous_work = application_work.snapshot();
        let mut previous_public = app.public_in_flight();
        let mut previous_dispatches = pending_dispatches!();
        let admitted_drained = loop {
            if !token_finished {
                let outcome = drain_token.lock().expect("drain token slot").take();
                if let Some(outcome) = outcome {
                    token_finished = true;
                    if let crate::drain_token::Outcome::Acquired(hold) = outcome {
                        drain_token_hold = Some(hold);
                    }
                }
            }
            // Event progress can keep this phase alive past the claim TTL.
            // Renew while draining accepted work as well as during handoff.
            if drain_token_hold.as_ref().is_some_and(|hold| {
                crate::drain_token::renew_due(hold, crate::ownership_store::now_ms())
            }) {
                if let (Some(bucket), Some(hold)) = (&fleet_bucket, drain_token_hold.as_mut()) {
                    let Some(renewed) = before_process_deadline(
                        handoff_deadline,
                        crate::drain_token::renew(bucket, &clean_reload_node, hold),
                    )
                    .await
                    else {
                        break false;
                    };
                    if !renewed {
                        drain_token_hold = None;
                    }
                }
            }
            let work = application_work.snapshot();
            let public = app.public_in_flight();
            let dispatches = pending_dispatches!();
            if work.retired_events != previous_work.retired_events
                || public < previous_public
                || dispatches < previous_dispatches
            {
                pre_drain_deadline = bounded_stall_deadline(stall_window, handoff_deadline);
            }
            previous_work = work;
            previous_public = public;
            previous_dispatches = dispatches;
            let quiescent = public == 0 && work.pending_events == 0 && dispatches == 0;
            // Peers remain served while waiting for the drain token. Recheck
            // quiescence if they introduce work after the prior idle sample.
            if admitted == Some(true) && !quiescent {
                admitted = None;
                pre_drain_deadline = bounded_stall_deadline(stall_window, handoff_deadline);
            }
            if admitted.is_none() && quiescent {
                admitted = Some(true);
            }
            if let Some(admitted_drained) = admitted {
                let token_ready = token_finished;
                #[cfg(all(test, cellar_internal_tests))]
                let token_ready = token_ready && shutdown_accept_failure_recovered_for_test;
                if token_ready {
                    break admitted_drained;
                }
            }
            crate::asyncrt::select! {
                _ = tokio::time::sleep_until(pre_drain_deadline), if admitted.is_none() => {
                    admitted = Some(false);
                }
                _ = tokio::time::sleep_until(handoff_deadline) => break false,
                _ = pre_drain_tick.tick() => {}
                connection = shutdown_connection_rx.recv() => {
                    let Some(connection) = connection else {
                        anyhow::bail!("shutdown listener tasks stopped during pre-drain");
                    };
                    drain_connections.push(serve_http_connection(
                        connection.stream,
                        connection.surface,
                        app.clone(),
                        shutdown_tx.clone(),
                        drain_connection.clone(),
                        connection_grace,
                    ));
                    #[cfg(all(test, cellar_internal_tests))]
                    if connection.recovered_after_error {
                        shutdown_accept_failure_recovered_for_test = true;
                    }
                    let _ = connection.received.send(());
                }
                Some(_) = drain_connections.next(), if !drain_connections.is_empty() => {}
                Some(_) = connections.next(), if !connections.is_empty() => {}
                call = do_call_rx.recv() => {
                    let Some(call) = call else {
                        anyhow::bail!("Durable Object call channel closed during shutdown pre-drain");
                    };
                    do_calls.push(Box::pin(dispatch_do_call(app.clone(), call)));
                }
                Some(_) = do_calls.next(), if !do_calls.is_empty() => {}
                req = gate_rx.recv() => {
                    let Some(req) = req else {
                        anyhow::bail!("output-gate channel closed during shutdown pre-drain");
                    };
                    gate_calls.push(Box::pin(dispatch_gate(app.clone(), req)));
                }
                Some(_) = gate_calls.next(), if !gate_calls.is_empty() => {}
                call = service_call_rx.recv() => {
                    let Some(call) = call else {
                        anyhow::bail!("service call channel closed during shutdown pre-drain");
                    };
                    service_calls.push(Box::pin(dispatch_service_call(app.clone(), call)));
                }
                call = service_rpc_rx.recv() => {
                    let Some(call) = call else {
                        anyhow::bail!("service RPC channel closed during shutdown pre-drain");
                    };
                    service_calls.push(Box::pin(dispatch_service_rpc(app.clone(), call)));
                }
                Some(_) = service_calls.next(), if !service_calls.is_empty() => {}
                facet = facet_rx.recv() => {
                    let Some(facet) = facet else {
                        anyhow::bail!("facet dispatch channel closed");
                    };
                    facet_calls.push(Box::pin(dispatch_facet(app.clone(), facet)));
                }
                Some(()) = facet_calls.next(), if !facet_calls.is_empty() => {}
                call = queue_dispatch_rx.recv() => {
                    let Some(call) = call else {
                        anyhow::bail!("Queue dispatch channel closed during shutdown pre-drain");
                    };
                    queue_calls.push(Box::pin(dispatch_queue_batch(app.clone(), call)));
                }
                Some(_) = queue_calls.next(), if !queue_calls.is_empty() => {}
                call = rpc_call_rx.recv() => {
                    let Some(call) = call else {
                        anyhow::bail!("Durable Object RPC channel closed during shutdown pre-drain");
                    };
                    do_calls.push(Box::pin(dispatch_rpc_call(app.clone(), call)));
                }
                call = asset_call_rx.recv() => {
                    let Some(call) = call else {
                        anyhow::bail!("asset call channel closed during shutdown pre-drain");
                    };
                    asset_calls.push(Box::pin(dispatch_asset_call(app.clone(), call)));
                }
                Some(_) = asset_calls.next(), if !asset_calls.is_empty() => {}
                socket = websocket_rx.recv() => {
                    let Some(socket) = socket else {
                        anyhow::bail!("WebSocket channel closed during shutdown pre-drain");
                    };
                    websockets.push(socket);
                }
                outbound = outbound_ws_rx.recv() => {
                    let Some(outbound) = outbound else {
                        anyhow::bail!("outbound WebSocket channel closed during shutdown pre-drain");
                    };
                    let app = app.clone();
                    websockets.push(Box::pin(async move {
                        if let Err(error) = outbound_websocket_task(app, outbound).await {
                            tracing::warn!("cellar outbound WebSocket failed: {error:#}");
                        }
                    }));
                }
                Some(_) = websockets.next(), if !websockets.is_empty() => {}
                report = fence_rx.recv() => return Ok(fence_outcome(report)),
                actor_exit = actor_exit_rx.recv() => {
                    let error = match actor_exit {
                        Some(Err(error)) => error,
                        Some(Ok(())) => "the core actor stopped unexpectedly".to_string(),
                        None => "the core actor thread panicked".to_string(),
                    };
                    tracing::error!(event = "core_actor_exit", %error, "SELF-FENCE during shutdown pre-drain");
                    return Ok(NodeOutcome::CoreFailed { error });
                }
            }
        };
        if !admitted_drained {
            tracing::warn!(
                event = "shutdown_pre_drain_forced",
                public_in_flight = app.public_in_flight(),
                application_events = application_work.snapshot().pending_events,
                do_calls = do_calls.len(),
                gate_calls = gate_calls.len(),
                service_calls = service_calls.len(),
                queue_calls = queue_calls.len(),
                asset_calls = asset_calls.len(),
                grace_ms = stall_window.as_millis(),
                "shutdown reached its accepted-work deadline before changing cell lifecycle"
            );
        }
        // Capture response completion before connection shutdown can cancel
        // an unfinished request and retire its ingress count.
        let ingress_drained = app.public_in_flight() == 0;
        if admitted_drained {
            let message = match shutdown_mode {
                ShutdownMode::Handoff => Message::ReleaseAll,
                ShutdownMode::Preserve => Message::BeginPreserve,
            };
            let _ = app.tx.send(message);
        }
        if shutdown_mode == ShutdownMode::Preserve {
            let _ = connection_drain_tx.send(true);
        }
        (admitted_drained, ingress_drained)
    };
    // Progress extends the stall deadline, but never the complete process
    // deadline. A deadline-cut cell keeps its durable owner record and follows
    // the same lease-expiry recovery path as an abrupt process loss.
    let mut stall_deadline = bounded_stall_deadline(stall_window, handoff_deadline);
    let mut handed_off = 0;
    let mut process_deadline_fired = false;
    let mut handoff = tokio::time::interval(std::time::Duration::from_millis(50));
    let mut containers_reaped = false;
    let drained = if !admitted_work_drained {
        false
    } else {
        loop {
            let requests_drained = connections.is_empty()
                && pending_dispatches!() == 0
                && application_work.snapshot().pending_events == 0;
            let shell_drained = requests_drained && websockets.is_empty();
            // The actor can be busy driving an immediate-effect failure loop, so
            // a status request is not itself allowed to bypass the drain deadline.
            let status = before_process_deadline(
                handoff_deadline,
                tokio::time::timeout(std::time::Duration::from_millis(50), app.drain_status()),
            )
            .await
            .and_then(Result::ok);
            if shutdown_mode == ShutdownMode::Handoff
                && status.is_some_and(|status| status.handed_off > handed_off)
            {
                handed_off = status.expect("checked drain status").handed_off;
                stall_deadline = bounded_stall_deadline(stall_window, handoff_deadline);
            }
            // A drain that makes progress can outlive the token TTL, so the
            // holder renews and only a dead holder lets the token lapse.
            let renew_due = drain_token_hold.as_ref().is_some_and(|hold| {
                crate::drain_token::renew_due(hold, crate::ownership_store::now_ms())
            });
            if renew_due {
                let mut lost = false;
                if let (Some(bucket), Some(hold)) = (&fleet_bucket, drain_token_hold.as_mut()) {
                    let Some(renewed) = before_process_deadline(
                        handoff_deadline,
                        crate::drain_token::renew(bucket, &clean_reload_node, hold),
                    )
                    .await
                    else {
                        process_deadline_fired = true;
                        break false;
                    };
                    lost = !renewed;
                }
                if lost {
                    drain_token_hold = None;
                }
            }
            let core_drained = status.is_some_and(|status| match shutdown_mode {
                ShutdownMode::Handoff => {
                    status.occupied == 0
                        && status.activating == 0
                        && status.quiescing == 0
                        && status.evicting == 0
                        && status.releasing == 0
                        && status.adopting == 0
                }
                ShutdownMode::Preserve => status.activating == 0 && status.evicting == 0,
            });
            let completely_drained = match shutdown_mode {
                ShutdownMode::Handoff => {
                    core_drained
                        && pending_dispatches!() == 0
                        && application_work.snapshot().pending_events == 0
                }
                ShutdownMode::Preserve => shell_drained && core_drained,
            };
            if completely_drained {
                break true;
            }
            // A preserve drain keeps its objects resident, so a socket an
            // object holds into its container never ends on its own and
            // the drain would sit out its whole deadline on it (the Sandbox
            // SDK's RPC transport is one such socket). The containers are
            // destroyed at exit anyway; destroying them once every request
            // has answered ends those sockets now.
            if !containers_reaped
                && shutdown_mode == ShutdownMode::Preserve
                && requests_drained
                && core_drained
            {
                containers_reaped = true;
                before_process_deadline(handoff_deadline, crate::container::shutdown()).await;
                continue;
            }
            crate::asyncrt::select! {
                _ = tokio::time::sleep_until(stall_deadline) => {
                    process_deadline_fired = tokio::time::Instant::now() >= handoff_deadline;
                    break false;
                },
                _ = handoff.tick() => {}
                connection = shutdown_connection_rx.recv() => {
                    let Some(connection) = connection else {
                        anyhow::bail!("shutdown listener tasks stopped during drain");
                    };
                    drain_connections.push(serve_http_connection(
                        connection.stream,
                        connection.surface,
                        app.clone(),
                        shutdown_tx.clone(),
                        drain_connection.clone(),
                        connection_grace,
                    ));
                    let _ = connection.received.send(());
                }
                Some(_) = drain_connections.next(), if !drain_connections.is_empty() => {}
                Some(_) = connections.next(), if !connections.is_empty() => {}
                call = do_call_rx.recv() => {
                    let Some(call) = call else {
                        anyhow::bail!("Durable Object call channel closed during shutdown");
                    };
                    do_calls.push(Box::pin(dispatch_do_call(app.clone(), call)));
                }
                Some(_) = do_calls.next(), if !do_calls.is_empty() => {}
                req = gate_rx.recv() => {
                    let Some(req) = req else {
                        anyhow::bail!("output-gate channel closed during shutdown");
                    };
                    gate_calls.push(Box::pin(dispatch_gate(app.clone(), req)));
                }
                Some(_) = gate_calls.next(), if !gate_calls.is_empty() => {}
                call = service_call_rx.recv() => {
                    let Some(call) = call else {
                        anyhow::bail!("service call channel closed during shutdown");
                    };
                    service_calls.push(Box::pin(dispatch_service_call(app.clone(), call)));
                }
                call = service_rpc_rx.recv() => {
                    let Some(call) = call else {
                        anyhow::bail!("service RPC channel closed during shutdown");
                    };
                    service_calls.push(Box::pin(dispatch_service_rpc(app.clone(), call)));
                }
                Some(_) = service_calls.next(), if !service_calls.is_empty() => {}
                facet = facet_rx.recv() => {
                    let Some(facet) = facet else {
                        anyhow::bail!("facet dispatch channel closed");
                    };
                    facet_calls.push(Box::pin(dispatch_facet(app.clone(), facet)));
                }
                Some(()) = facet_calls.next(), if !facet_calls.is_empty() => {}
                call = queue_dispatch_rx.recv() => {
                    let Some(call) = call else {
                        anyhow::bail!("Queue dispatch channel closed during shutdown");
                    };
                    queue_calls.push(Box::pin(dispatch_queue_batch(app.clone(), call)));
                }
                Some(_) = queue_calls.next(), if !queue_calls.is_empty() => {}
                call = rpc_call_rx.recv() => {
                    let Some(call) = call else {
                        anyhow::bail!("Durable Object RPC channel closed during shutdown");
                    };
                    do_calls.push(Box::pin(dispatch_rpc_call(app.clone(), call)));
                }
                call = asset_call_rx.recv() => {
                    let Some(call) = call else {
                        anyhow::bail!("asset call channel closed during shutdown");
                    };
                    asset_calls.push(Box::pin(dispatch_asset_call(app.clone(), call)));
                }
                Some(_) = asset_calls.next(), if !asset_calls.is_empty() => {}
                socket = websocket_rx.recv() => {
                    let Some(socket) = socket else {
                        anyhow::bail!("WebSocket channel closed during shutdown");
                    };
                    websockets.push(socket);
                }
                outbound = outbound_ws_rx.recv() => {
                    let Some(outbound) = outbound else {
                        anyhow::bail!("outbound WebSocket channel closed during shutdown");
                    };
                    let app = app.clone();
                    websockets.push(Box::pin(async move {
                        if let Err(error) = outbound_websocket_task(app, outbound).await {
                            tracing::warn!("cellar outbound WebSocket failed: {error:#}");
                        }
                    }));
                }
                Some(_) = websockets.next(), if !websockets.is_empty() => {}
                report = fence_rx.recv() => return Ok(fence_outcome(report)),
                actor_exit = actor_exit_rx.recv() => {
                    let error = match actor_exit {
                        Some(Err(error)) => error,
                        Some(Ok(())) => "the core actor stopped unexpectedly".to_string(),
                        None => "the core actor thread panicked".to_string(),
                    };
                    tracing::error!(event = "core_actor_exit", %error, "SELF-FENCE during shutdown");
                    return Ok(NodeOutcome::CoreFailed { error });
                }
            }
        }
    };
    let _ = connection_drain_tx.send(true);
    let _ = drain_connection_tx.send(true);
    // Release the token whether or not the drain completed: the process is
    // about to exit either way, and the next donor must not wait out the TTL.
    // The connection flush below needs nothing from the token, so releasing
    // first keeps the next donor off the TTL during the flush grace.
    if let (Some(bucket), Some(hold)) = (&fleet_bucket, drain_token_hold.take()) {
        if before_process_deadline(
            process_deadline,
            crate::drain_token::release(bucket, &clean_reload_node, hold),
        )
        .await
        .is_none()
        {
            tracing::warn!(
                event = "drain_token_release_deadline",
                "the process deadline skipped or cancelled the drain token release; the TTL bounds the residue"
            );
        }
    }
    #[cfg(all(test, cellar_internal_tests))]
    shutdown_accept_failure_test_checkpoint(
        shutdown_accept_failure_test_active,
        "drain token release finished",
    );
    if !admitted_work_drained {
        tracing::error!(
            event = "shutdown_handoff_not_started",
            drain_ms,
            admitted_public = app.public_in_flight(),
            "the shutdown pre-drain did not finish"
        );
    } else if !drained && shutdown_mode == ShutdownMode::Handoff {
        let drain_state = before_process_deadline(
            process_deadline,
            tokio::time::timeout(std::time::Duration::from_millis(50), app.snapshot()),
        )
        .await
        .and_then(Result::ok)
        .unwrap_or_else(|| "{\"error\":\"process_deadline\"}".to_string());
        match before_process_deadline(
            process_deadline,
            tokio::time::timeout(
                std::time::Duration::from_millis(50),
                app.drain_status(),
            ),
        )
        .await
        .and_then(Result::ok)
        {
            Some(status) => tracing::error!(
                event = "shutdown_handoff_stalled",
                drain_ms,
                shutdown_total_ms,
                process_deadline_fired,
                occupied = status.occupied,
                activating = status.activating,
                quiescing = status.quiescing,
                evicting = status.evicting,
                releasing = status.releasing,
                adopting = status.adopting,
                handed_off = status.handed_off,
                drain_state,
                "the shutdown handoff made no progress"
            ),
            None => tracing::error!(
                event = "shutdown_handoff_stalled",
                drain_ms,
                shutdown_total_ms,
                process_deadline_fired,
                drain_state,
                "the shutdown handoff made no observable progress because the core status was unavailable"
            ),
        }
    } else if !drained {
        tracing::warn!(
            "cellar preserve drain reached its {drain_ms}ms deadline: connections={} do_calls={} gate_calls={} service_calls={} queue_calls={} asset_calls={} websockets={}",
            connections.len(),
            do_calls.len(),
            gate_calls.len(),
            service_calls.len(),
            queue_calls.len(),
            asset_calls.len(),
            websockets.len(),
        );
    }
    // Let accepted connections finish their current responses before returning
    // to the host supervisor. A quiet node can drain in milliseconds; without
    // this wait, the supervisor could close the domain while a diagnostic
    // response or WebSocket close frame is still being written. The watches
    // above already told each
    // connection to close after the response it is serving, so this drains
    // fast; the grace only bounds a stuck peer. The dispatch sets (do_calls,
    // gate_calls, service_calls, asset_calls) are NOT polled here: a
    // connection whose response depends on one stalls for the grace and is
    // then cut, so a drain-window internal route must never dispatch into a
    // cell. Today none does — /state, /health, and /peer/abort answer from the
    // shell.
    // The backstop for a handoff cut by its deadline: a cell left behind
    // must not leave its container running until the next start of this
    // node reaps it. Idempotent after the preserve drain's own call.
    before_process_deadline(process_deadline, crate::container::shutdown()).await;
    let flush_connections = async {
        while drain_connections.next().await.is_some() {}
        while connections.next().await.is_some() {}
        while websockets.next().await.is_some() {}
    };
    flush_shutdown_connections(
        process_deadline,
        crate::ws_registry::ws_await_all_flushes(),
        flush_connections,
    )
    .await;
    // The graceful-shutdown drain point: seal our node-log record so the
    // next incarnation starts with no gather. Guarded inside — it seals
    // only when every shipped frame is bucket-covered.
    let durability_quiesced = if let Some(owner) = &mut durability_owner {
        let remaining = process_deadline.saturating_duration_since(tokio::time::Instant::now());
        let completed = if remaining.is_zero() {
            owner.stop_local_now();
            false
        } else {
            owner.quiesce_and_seal_within(remaining).await
        };
        if !completed {
            tracing::warn!("cellar durability quiesce exceeded the process deadline");
        }
        completed
    } else {
        true
    };
    #[cfg(all(test, cellar_internal_tests))]
    shutdown_accept_failure_test_checkpoint(
        shutdown_accept_failure_test_active,
        "durability quiesce finished",
    );
    // Preparation captures and uploads each stopped cell's exact database
    // position. A failed proof abandons the marker and leaves normal startup
    // to reconcile the preserved files against bucket coverage.
    let mut clean_reload_prepared = false;
    if clean_reload_is_eligible(durability_quiesced, drained, shutdown_mode) {
        let presence = before_process_deadline(process_deadline, app.presence()).await;
        let prepared = match (&app.runtime, presence) {
            (Some(runtime), Some(Some(presence))) => {
                before_process_deadline(
                    process_deadline,
                    runtime.prepare_clean_reload(&presence.cells),
                )
                .await
            }
            (_, Some(None)) | (None, Some(Some(_))) => Some(Err(anyhow::anyhow!(
                "clean reload requires a runtime and a resident snapshot"
            ))),
            (_, None) => None,
        };
        match prepared {
            Some(Ok(pruned)) => {
                match before_process_deadline(process_deadline, app.healthy()).await {
                    Some(true) => match crate::runtime::write_clean_reload_marker(
                        &data_dir,
                        &clean_reload_node,
                        &process_generation,
                    ) {
                        Ok(()) => {
                            clean_reload_prepared = true;
                            tracing::info!(
                                event = "clean_reload_prepared",
                                stale_live_databases_pruned = pruned,
                                "prepared local cells for an exact-generation reload"
                            );
                        }
                        Err(error) => tracing::warn!(
                            event = "clean_reload_abandoned",
                            %error,
                            "could not publish the clean local reload certificate"
                        ),
                    },
                    Some(false) => tracing::warn!(
                        event = "clean_reload_abandoned",
                        "node authority was lost while local cells were closing"
                    ),
                    None => tracing::warn!(
                        event = "clean_reload_abandoned",
                        "the process deadline skipped the final authority check"
                    ),
                }
            }
            Some(Err(error)) => tracing::warn!(
                event = "clean_reload_abandoned",
                %error,
                "local reload preparation failed; replacement will use normal recovery"
            ),
            None => tracing::warn!(
                event = "clean_reload_abandoned",
                "local reload preparation exceeded the shutdown deadline"
            ),
        }
    }
    let local_durability_closed = if let Some(owner) = &mut durability_owner {
        let remaining = process_deadline.saturating_duration_since(tokio::time::Instant::now());
        let completed = if remaining.is_zero() {
            owner.stop_local_now();
            false
        } else {
            owner.shutdown_local_within(remaining).await
        };
        if !completed {
            tracing::warn!("cellar local durability shutdown exceeded the node deadline");
        }
        completed
    } else {
        true
    };
    #[cfg(all(test, cellar_internal_tests))]
    shutdown_accept_failure_test_checkpoint(
        shutdown_accept_failure_test_active,
        "local durability shutdown finished",
    );
    // Return the observed proof phases to the host supervisor. A cell cut off
    // by its deadline retains its owner record for ordinary recovery. The
    // enclosing host still owns stopping and joining its execution domain.
    #[cfg(all(test, cellar_internal_tests))]
    shutdown_accept_failure_test_checkpoint(
        shutdown_accept_failure_test_active,
        "final log flush reached",
    );
    let remaining_work = application_work.snapshot();
    Ok(NodeOutcome::Stopped(ShutdownReport {
        mode: shutdown_mode,
        ingress_drained: ingress_drained_before_cut && app.public_in_flight() == 0,
        application_work_drained: admitted_work_drained
            && drained
            && remaining_work.pending_events == 0,
        pending_application_events: remaining_work.pending_events,
        cells_drained: drained,
        pending_dispatches: pending_dispatches!(),
        log_quiesced: durability_quiesced,
        local_durability_closed,
        clean_reload_prepared,
        drain_timed_out: !admitted_work_drained || !drained,
        deadline_elapsed: process_deadline_fired || tokio::time::Instant::now() >= process_deadline,
    }))
}

#[cfg(all(test, not(cellar_internal_tests)))]
mod tests;
