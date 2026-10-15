//! Process boundary: the caller owns Tokio, sockets, and shutdown signals.
#![allow(clippy::disallowed_methods)] // Process-owned server tasks, clock and stop deadline.

use anyhow::{ensure, Result};
use axum::{routing::get, Router};
use cellar::sdk::*;
use cellar_axum::CellarService;
use std::{sync::Arc, time::Duration};

#[tokio::main]
async fn main() -> Result<()> {
    let address = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:9876".into());
    let directory = std::env::args()
        .nth(2)
        .unwrap_or_else(|| ".cellar/axum-counter".into());
    let listener = tokio::net::TcpListener::bind(address).await?;
    let public_address = listener.local_addr()?;
    let internal_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let internal_address = internal_listener.local_addr()?;
    let signer = Arc::new(PeerProbeSigner::generate());
    let actor = ActorOptions {
        core: ActorPolicy {
            max_resident: 64,
            max_cached_routes: 256,
            max_activations: 16,
            max_evictions: 4,
            max_releases: 4,
            max_outbound_websockets: 16,
            ownership_on_evict: OwnershipOnEvict::Sticky,
            require_node_lease: true,
            peer_protocol: cellar::peer_auth::PROTOCOL_VERSION,
            operation_deadline_ms: Some(30_000),
            owner_log_recovery_backoff_ms: 50,
            owner_log_recovery_attempts: 3,
            alarm_resident_ms: 100,
            idle_evict_ms: None,
            pressure: Default::default(),
        },
        lease_ttl_ms: 10_000,
        placement_weight: 1,
        generation: signer.public_key_hex(),
        resume_generation: None,
    };
    let mut config = NodeConfig::new(
        ActorIdentity {
            node: "axum-counter".into(),
            region: "local".into(),
            advertise: internal_address.to_string(),
        },
        actor,
        directory.into(),
    );
    config.shutdown = ShutdownPolicy {
        total_ms: 5_000,
        token_wait_ms: 0,
        no_progress_ms: 2_000,
    };
    let executor = tokio::runtime::Handle::current();
    let (mut node, ingress) = Node::start_external(
        config,
        ExternalNodeServices {
            wall_clock: None,
            http_client: None,
            host_executor: executor.clone(),
            core_executor: executor,
            public_address,
            ingress_capacity: 256,
            internal_listener,
            peer_http: reqwest::Client::new(),
            peer_auth: Arc::new(PeerAuth::new([11; 32], "axum-counter")?),
            probe_signer: signer,
            fleet: None,
            containers: None,
            metrics: None,
        },
        cellar_axum_counter::graph(),
        CompiledApplications::default().register("axum-counter", "v1", || {
            cellar_axum_counter::definition()?.instantiate()
        })?,
    )
    .await?;
    let app = Router::new()
        .route("/outside", get(|| async { "ordinary Axum route" }))
        .fallback_service(CellarService::new(ingress.clone()));
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let mut server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
    });
    cellar::note!("Axum counter listening on http://{public_address}");

    enum End {
        Signal(std::io::Result<()>),
        Node(Result<NodeOutcome>),
        Server(Result<std::io::Result<()>, tokio::task::JoinError>),
    }
    let end = cellar::asyncrt::select! {
        result = tokio::signal::ctrl_c() => End::Signal(result),
        result = node.finished() => End::Node(result),
        result = &mut server => End::Server(result),
    };
    ingress.close_admission();
    let _ = stop.send(());
    // One absolute deadline covers both the runtime drain and the HTTP server.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    let outcome = node.shutdown(ShutdownMode::Preserve).await?;
    if !matches!(end, End::Server(_)) {
        match tokio::time::timeout_at(deadline, &mut server).await {
            Ok(result) => result??,
            Err(_) => {
                server.abort();
                let _ = server.await;
                anyhow::bail!("HTTP shutdown deadline expired; node outcome: {outcome:?}");
            }
        }
    }
    match end {
        End::Signal(result) => result?,
        End::Node(result) => {
            result?;
        }
        End::Server(result) => result??,
    }
    let NodeOutcome::Stopped(report) = outcome else {
        anyhow::bail!("node stopped: {outcome:?}");
    };
    ensure!(
        !report.drain_timed_out
            && report.ingress_drained
            && report.cells_drained
            && report.application_work_drained,
        "incomplete shutdown: {report:?}"
    );
    Ok(())
}
