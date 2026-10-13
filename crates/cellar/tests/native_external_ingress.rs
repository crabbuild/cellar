//! Application-owned HTTP transports use the production node's admission and drain.
#![allow(clippy::disallowed_methods)]
#[allow(dead_code)]
#[path = "support/html.rs"]
mod fixture;

use anyhow::{ensure, Result};
use bytes::Bytes;
use cellar::sdk::*;
use futures_util::StreamExt;
use http_body_util::{BodyExt, Full};
use hyper::Request;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, Semaphore};

struct Signals {
    entered: Notify,
    release: Semaphore,
}

impl Default for Signals {
    fn default() -> Self {
        Self {
            entered: Notify::new(),
            release: Semaphore::new(0),
        }
    }
}

struct Counter;

impl Cell for Counter {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = event else {
                anyhow::bail!("fetch required");
            };
            let storage = context.storage()?;
            let mut count = storage
                .get_json("count")?
                .and_then(|value| value.as_u64())
                .unwrap_or(0);
            if request.method == "POST" {
                count += 1;
                storage.put_json("count", &serde_json::json!(count))?;
            }
            Ok(Output::Http(
                http::Response::new(200, count.to_string(), Vec::new()).into_http(&context)?,
            ))
        })
    }
}

fn applications(signals: Arc<Signals>) -> Result<CompiledApplications> {
    CompiledApplications::default().register("external", "test", move || {
        let signals = signals.clone();
        Ok(Arc::new(
            Registry::new(move |context, event| {
                let signals = signals.clone();
                Box::pin(async move {
                    let Invocation::Fetch(request) = event else {
                        anyhow::bail!("fetch required");
                    };
                    let path = request.parsed_url()?.pathname();
                    if path == "/count" {
                        return Ok(Output::Http(
                            context
                                .namespace("COUNTERS")?
                                .get_by_name("one")
                                .fetch(&context, request)
                                .await?,
                        ));
                    }
                    if path == "/hold" {
                        signals.entered.notify_one();
                        signals.release.acquire().await?.forget();
                    }
                    let mut body = context.take_body(request.body)?;
                    let mut bytes = Vec::new();
                    while let Some(chunk) = body.next().await {
                        bytes.extend(chunk.map_err(anyhow::Error::msg)?);
                    }
                    Ok(Output::Http(
                        http::Response::new(
                            200,
                            bytes,
                            vec![
                                ("set-cookie".into(), "a=1".into()),
                                ("set-cookie".into(), "b=2".into()),
                            ],
                        )
                        .into_http(&context)?,
                    ))
                })
            })
            .cell("Counter", |_, _| Ok(Arc::new(Counter)))?,
        ))
    })
}

struct Harness {
    _directory: tempfile::TempDir,
    node: Node,
    ingress: IngressHandle,
    _listener: tokio::net::TcpListener,
}

async fn start(capacity: usize, max_body: usize, signals: Arc<Signals>) -> Result<Harness> {
    let directory = tempfile::tempdir()?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let internal_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let signer = Arc::new(PeerProbeSigner::generate());
    let options = ActorOptions {
        core: ActorPolicy {
            max_resident: 16,
            max_cached_routes: 0,
            max_activations: 8,
            max_evictions: 4,
            max_releases: 4,
            max_outbound_websockets: 8,
            ownership_on_evict: OwnershipOnEvict::Sticky,
            require_node_lease: true,
            peer_protocol: cellar::peer_auth::PROTOCOL_VERSION,
            operation_deadline_ms: Some(3_000),
            owner_log_recovery_backoff_ms: 50,
            owner_log_recovery_attempts: 3,
            alarm_resident_ms: 100,
            idle_evict_ms: None,
            pressure: Default::default(),
        },
        lease_ttl_ms: 1_000,
        placement_weight: 1,
        generation: signer.public_key_hex(),
        resume_generation: None,
    };
    let mut config = NodeConfig::new(
        ActorIdentity {
            node: "external".into(),
            region: "test".into(),
            advertise: internal_listener.local_addr()?.to_string(),
        },
        options,
        directory.path().to_path_buf(),
    );
    config.max_request_body_bytes = max_body;
    config.shutdown = ShutdownPolicy {
        total_ms: 5_000,
        token_wait_ms: 0,
        no_progress_ms: 2_000,
    };
    config.maintenance.rebalance = false;
    let services = ExternalNodeServices {
        http_client: None,
        host_executor: tokio::runtime::Handle::current(),
        core_executor: tokio::runtime::Handle::current(),
        public_address: listener.local_addr()?,
        ingress_capacity: capacity,
        internal_listener,
        peer_http: reqwest::Client::new(),
        peer_auth: Arc::new(PeerAuth::new([11; 32], "external")?),
        probe_signer: signer,
        fleet: None,
        containers: None,
        metrics: None,
    };
    let mut graph = fixture::graph("external", "", vec!["Counter".into()]);
    graph.primary.options.bindings = vec![("COUNTERS".into(), "Counter".into())];
    let (node, ingress) =
        Node::start_external(config, services, graph, applications(signals)?).await?;
    ensure!(
        node.info().public_address == listener.local_addr()?,
        "wrong public address"
    );
    Ok(Harness {
        _directory: directory,
        node,
        ingress,
        _listener: listener,
    })
}

fn request(path: &str, method: &str, bytes: impl Into<Bytes>) -> Result<Request<Full<Bytes>>> {
    Ok(Request::builder()
        .uri(format!("http://example{path}"))
        .method(method)
        .body(Full::new(bytes.into()))?)
}

async fn text(response: IngressResponse) -> Result<String> {
    Ok(String::from_utf8(
        response.into_body().collect().await?.to_bytes().to_vec(),
    )?)
}

async fn stop(node: Node) -> Result<()> {
    let outcome = tokio::time::timeout(
        Duration::from_secs(8),
        node.shutdown(ShutdownMode::Preserve),
    )
    .await??;
    let NodeOutcome::Stopped(report) = outcome else {
        anyhow::bail!("unexpected outcome: {outcome:?}");
    };
    ensure!(
        report.ingress_drained
            && report.application_work_drained
            && report.cells_drained
            && report.pending_dispatches == 0
            && !report.drain_timed_out,
        "incomplete drain: {report:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_ingress_reuses_body_limits_headers_and_cell_storage() -> Result<()> {
    let harness = start(8, 8, Arc::new(Signals::default())).await?;
    let response = harness
        .ingress
        .call(request("/echo", "POST", "hello")?)
        .await;
    ensure!(response.status() == 200);
    ensure!(response.headers().get_all("set-cookie").iter().count() == 2);
    ensure!(text(response).await? == "hello");
    let response = harness
        .ingress
        .call(request("/echo", "POST", "too many bytes")?)
        .await;
    ensure!(response.status() == 413, "body limit was bypassed");
    drop(response);
    ensure!(text(harness.ingress.call(request("/count", "POST", "")?).await).await? == "1");
    ensure!(text(harness.ingress.call(request("/count", "GET", "")?).await).await? == "1");
    stop(harness.node).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn external_capacity_is_retained_until_body_completion_or_drop() -> Result<()> {
    let harness = start(1, 1024, Arc::new(Signals::default())).await?;
    let response = harness
        .ingress
        .call(request("/echo", "POST", "held body")?)
        .await;
    ensure!(response.status() == 200);
    let refused = harness
        .ingress
        .call(request("/echo", "POST", "second")?)
        .await;
    ensure!(refused.status() == 503 && refused.headers().get("retry-after").is_some());
    drop(response);
    let next = harness
        .ingress
        .call(request("/echo", "POST", "next")?)
        .await;
    ensure!(next.status() == 200);
    ensure!(text(next).await? == "next");
    stop(harness.node).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queued_calls_survive_admission_closure_and_clones_reject_new_calls() -> Result<()> {
    let harness = start(8, 1024, Arc::new(Signals::default())).await?;
    let accepted = harness.ingress.call(request("/echo", "POST", "accepted")?);
    let clone = harness.ingress.clone();
    harness.ingress.close_admission();
    let refused = clone.call(request("/echo", "POST", "new")?).await;
    ensure!(refused.status() == 503);
    ensure!(text(accepted.await).await? == "accepted");
    stop(harness.node).await?;
    ensure!(clone.is_closed());
    ensure!(
        clone
            .call(request("/echo", "POST", "closed")?)
            .await
            .status()
            == 503
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_waits_for_accepted_external_work_and_response_body() -> Result<()> {
    let signals = Arc::new(Signals::default());
    let harness = start(8, 1024, signals.clone()).await?;
    let calling = tokio::spawn(harness.ingress.call(request("/hold", "POST", "accepted")?));
    tokio::time::timeout(Duration::from_secs(3), signals.entered.notified()).await?;
    harness.ingress.close_admission();
    let mut stopping = tokio::spawn(stop(harness.node));
    ensure!(
        tokio::time::timeout(Duration::from_millis(50), &mut stopping)
            .await
            .is_err()
    );
    signals.release.add_permits(1);
    let response = calling.await?;
    ensure!(
        tokio::time::timeout(Duration::from_millis(50), &mut stopping)
            .await
            .is_err()
    );
    ensure!(text(response).await? == "accepted");
    stopping.await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_external_call_retires_work_and_releases_admission() -> Result<()> {
    let signals = Arc::new(Signals::default());
    let harness = start(1, 1024, signals.clone()).await?;
    let calling = tokio::spawn(harness.ingress.call(request("/hold", "POST", "cancelled")?));
    tokio::time::timeout(Duration::from_secs(3), signals.entered.notified()).await?;
    calling.abort();
    let _ = calling.await;
    // Join through the node's existing work barrier instead of assuming that
    // cancelling the transport future immediately retires the application.
    stop(harness.node).await
}
