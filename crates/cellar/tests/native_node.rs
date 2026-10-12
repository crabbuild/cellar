//! Full public node bootstrap on caller-owned executors and infrastructure.
#![allow(clippy::disallowed_methods)]
#[allow(dead_code)]
#[path = "support/html.rs"]
mod fixture;
use anyhow::{ensure, Result};
use cellar::bucket::Bucket;
use cellar::engine_api::HttpResponse;
use cellar::sdk::*;
use std::{
    path::Path,
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};
use tokio::sync::{mpsc, Semaphore};

struct Executor(Option<tokio::runtime::Runtime>);
impl Executor {
    fn new(label: &str) -> Result<Self> {
        Ok(Self(Some(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .thread_name(label)
                .enable_all()
                .build()?,
        )))
    }
    fn handle(&self) -> tokio::runtime::Handle {
        self.0.as_ref().unwrap().handle().clone()
    }
}
impl Drop for Executor {
    fn drop(&mut self) {
        self.0.take().unwrap().shutdown_background();
    }
}

struct Signals {
    entered: mpsc::UnboundedSender<()>,
    release: Semaphore,
}
struct ReleaseFactoryOnDrop(Arc<(Mutex<bool>, Condvar)>);
impl Drop for ReleaseFactoryOnDrop {
    fn drop(&mut self) {
        *self.0 .0.lock().unwrap() = true;
        self.0 .1.notify_all();
    }
}
struct Account {
    label: &'static str,
    signals: Option<Arc<Signals>>,
}
fn response(text: impl ToString) -> Output {
    Output::Http(HttpResponse {
        status: 200,
        body: text.to_string().into_bytes(),
        headers: Vec::new(),
        stream: None,
        websocket: None,
        write_position: None,
        observed_position: None,
    })
}
impl Cell for Account {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = event else {
                return Ok(Output::Done);
            };
            if request.url.ends_with("/hold") {
                let signals = self.signals.as_ref().unwrap();
                signals.entered.send(())?;
                signals.release.acquire().await?.forget();
            }
            let storage = context.storage()?;
            let mut count = storage
                .get_json("count")?
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            if request.method == "POST" {
                count += 1;
                storage.put_json("count", &serde_json::json!(count))?;
            }
            Ok(response(format!("{}:{count}", self.label)))
        })
    }
}
fn applications(
    label: &'static str,
    signals: Option<Arc<Signals>>,
) -> Result<CompiledApplications> {
    CompiledApplications::default().register("embedded-node", "test", move || {
        let signals = signals.clone();
        Ok(Arc::new(
            Registry::new(|context, event| {
                Box::pin(async move {
                    let Invocation::Fetch(request) = event else {
                        return Ok(Output::Done);
                    };
                    let cell = context
                        .namespace("ACCOUNTS")?
                        .get_by_name("same-business-id");
                    Ok(Output::Http(cell.fetch(&context, request).await?))
                })
            })
            .cell("Account", move |_, _| {
                Ok(Arc::new(Account {
                    label,
                    signals: signals.clone(),
                }))
            })?,
        ))
    })
}
fn graph() -> DeploymentGraph {
    let mut graph = fixture::graph("embedded-node", "", vec!["Account".into()]);
    graph.primary.options.bindings = vec![("ACCOUNTS".into(), "Account".into())];
    graph
}
fn actor(generation: String) -> ActorOptions {
    ActorOptions {
        core: ActorPolicy {
            max_resident: 16,
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
        generation,
        resume_generation: None,
    }
}
async fn inputs(
    name: &str,
    directory: &Path,
    core: &Executor,
    bucket: Option<Bucket>,
    metrics: u64,
) -> Result<(NodeConfig, NodeServices)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let internal_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let probe_signer = Arc::new(PeerProbeSigner::generate());
    let peer_auth = Arc::new(PeerAuth::new([11; 32], name)?);
    let peer_http = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(2))
        .tcp_nodelay(true)
        .build()?;
    let fleet = bucket
        .map(|bucket| -> Result<NodeFleet> {
            let storage = ReplicaStorage::new(
                bucket.store.clone(),
                ReplicaStorageOptions {
                    prefix: bucket.prefix.clone(),
                    ..Default::default()
                },
            )?;
            Ok(NodeFleet {
                lease_bucket: bucket.clone(),
                bucket,
                replica_storage: storage,
                replication: ReplicationOptions {
                    flush_interval: Duration::ZERO,
                    compaction: None,
                    ..Default::default()
                },
                log_transport: Arc::new(SignedPeerTransport::from_clients(
                    peer_http.clone(),
                    peer_http.clone(),
                    peer_auth.clone(),
                )),
                log: LogOptions {
                    record_publish_timeout: Duration::from_secs(3),
                    ..Default::default()
                },
                fleet_durability: true,
            })
        })
        .transpose()?;
    let mut config = NodeConfig::new(
        ActorIdentity {
            node: name.into(),
            advertise: internal_listener.local_addr()?.to_string(),
            region: "test".into(),
        },
        actor(probe_signer.public_key_hex()),
        directory.to_path_buf(),
    );
    config.shutdown = ShutdownPolicy {
        total_ms: 8_000,
        token_wait_ms: 0,
        no_progress_ms: 5_000,
    };
    config.startup.bootstrap_timeout = Duration::from_secs(20);
    config.startup.recovery_attempt_timeout = Duration::from_secs(4);
    config.startup.authority_timeout = Duration::from_secs(4);
    config.maintenance.rebalance = false;
    config.maintenance.fleet_sample_period = Duration::from_millis(50);
    config.maintenance.load_sample_period = Duration::from_millis(50);
    config.generation_max_age = Duration::ZERO;
    let services = NodeServices {
        host_executor: tokio::runtime::Handle::current(),
        core_executor: core.handle(),
        listener,
        internal_listener,
        peer_http,
        peer_auth,
        probe_signer,
        fleet,
        containers: None,
        metrics: Some(Arc::new(move || HostMetricsSample {
            rss_bytes: metrics,
            in_use_bytes: metrics,
            fd_limit: 1024,
            ..Default::default()
        })),
    };
    Ok((config, services))
}
async fn start(
    name: &str,
    label: &'static str,
    directory: &Path,
    core: &Executor,
    bucket: Option<Bucket>,
    signals: Option<Arc<Signals>>,
    metrics: u64,
) -> Result<Node> {
    let (config, services) = inputs(name, directory, core, bucket, metrics).await?;
    Node::start(config, services, graph(), applications(label, signals)?).await
}
fn client() -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()?)
}
async fn call(
    client: &reqwest::Client,
    node: &Node,
    method: reqwest::Method,
    path: &str,
) -> Result<String> {
    let response = client
        .request(
            method,
            format!("http://{}{path}", node.info().public_address),
        )
        .send()
        .await?;
    ensure!(
        response.status() == 200,
        "public node status {}",
        response.status()
    );
    Ok(response.text().await?)
}
async fn ready(node: &Node) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !node.is_ready().await? {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await?
}
async fn stopped(node: Node) -> Result<()> {
    let services = node.services();
    let outcome = tokio::time::timeout(
        Duration::from_secs(12),
        node.run_until(async { ShutdownMode::Handoff }),
    )
    .await??;
    let NodeOutcome::Stopped(report) = outcome else {
        anyhow::bail!("unexpected shutdown {outcome:?}");
    };
    ensure!(
        report.ingress_drained
            && report.cells_drained
            && report.log_quiesced
            && report.local_durability_closed
            && report.pending_dispatches == 0
            && !report.drain_timed_out
            && !report.deadline_elapsed,
        "incomplete drain {report:?}"
    );
    ensure!(
        services
            .configure_metrics(Arc::new(HostMetricsSample::default))
            .is_err(),
        "completed node retained authority"
    );
    Ok(())
}

fn unhex(value: &str) -> Result<Vec<u8>> {
    ensure!(value.len().is_multiple_of(2), "invalid hex length");
    (0..value.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&value[i..i + 2], 16).map_err(Into::into))
        .collect()
}
async fn probe(client: &reqwest::Client, node: &Node) -> Result<()> {
    use ed25519_dalek::{Signature, VerifyingKey};
    let auth = PeerAuth::new([11; 32], "diagnostics")?;
    let challenge = "07".repeat(32);
    let request = auth.sign(
        client.get(format!(
            "http://{}/peer/probe",
            node.info().internal_address
        )),
        "GET",
        "/peer/probe",
        &[],
        &node.info().node,
    )?;
    let response = request
        .header("x-cells-probe-challenge", &challenge)
        .send()
        .await?;
    ensure!(
        response.status() == 200,
        "signed probe status {}",
        response.status()
    );
    let body: serde_json::Value = response.json().await?;
    ensure!(
        body["node"] == node.info().node
            && body["advertise"] == node.info().advertise
            && body["challenge"] == challenge,
        "probe identity crossed hosts"
    );
    let payload = format!(
        "cells-peer-probe-v1\n{}\n{}\n{challenge}",
        node.info().node,
        node.info().advertise
    );
    let key: [u8; 32] = unhex(&node.info().lease_generation)?.try_into().unwrap();
    let signature: [u8; 64] = unhex(body["signature"].as_str().unwrap())?
        .try_into()
        .unwrap();
    VerifyingKey::from_bytes(&key)?
        .verify_strict(payload.as_bytes(), &Signature::from_bytes(&signature))?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_nodes_drain_admitted_writes_isolate_fleets_and_leave_caller_executors_running(
) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let mut bucket_a = Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    bucket_a.prefix = "fleets/a/".into();
    let mut bucket_b = bucket_a.clone();
    bucket_b.prefix = "fleets/b/".into();
    let core_a = Executor::new("application-core-a")?;
    let core_b = Executor::new("application-core-b")?;
    let (entered, mut events) = mpsc::unbounded_channel();
    let signals = Arc::new(Signals {
        entered,
        release: Semaphore::new(0),
    });
    let a = start(
        "same-node",
        "a",
        &directory.path().join("a"),
        &core_a,
        Some(bucket_a.clone()),
        Some(signals.clone()),
        101,
    )
    .await?;
    let b = start(
        "same-node",
        "b",
        &directory.path().join("b"),
        &core_b,
        Some(bucket_b),
        None,
        202,
    )
    .await?;
    ready(&a).await?;
    ready(&b).await?;
    ensure!(
        a.services().sample_metrics().rss_bytes == 101
            && b.services().sample_metrics().rss_bytes == 202,
        "caller metrics were not isolated"
    );
    let client = client()?;
    ensure!(
        a.info().lease_generation != b.info().lease_generation,
        "independent hosts shared a signing generation"
    );
    probe(&client, &a).await?;
    probe(&client, &b).await?;
    ensure!(
        call(&client, &b, reqwest::Method::POST, "/write").await? == "b:1",
        "fleet B state crossed"
    );
    let hold_client = client.clone();
    let address = a.info().public_address;
    let held = tokio::spawn(async move {
        hold_client
            .post(format!("http://{address}/hold"))
            .send()
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await?
        .unwrap();
    let shutdown = tokio::spawn(stopped(a));
    ensure!(
        call(&client, &b, reqwest::Method::POST, "/write").await? == "b:2",
        "sibling stopped during drain"
    );
    signals.release.add_permits(1);
    let response = held.await??;
    ensure!(
        response.status() == 200 && response.text().await? == "a:1",
        "admitted write was cut off"
    );
    shutdown.await??;
    ensure!(
        core_a.handle().spawn(async { 7 }).await? == 7,
        "caller core executor stopped"
    );
    let reopened = start(
        "same-node",
        "a",
        &directory.path().join("a"),
        &core_a,
        Some(bucket_a),
        None,
        101,
    )
    .await?;
    ready(&reopened).await?;
    ensure!(
        call(&client, &reopened, reqwest::Method::POST, "/write").await? == "a:2",
        "committed fleet state was not restored"
    );
    stopped(reopened).await?;
    ensure!(
        call(&client, &b, reqwest::Method::POST, "/write").await? == "b:3",
        "sibling was closed"
    );
    stopped(b).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_nodes_adopt_compiled_generations_and_force_close_without_process_exit() -> Result<()>
{
    let directory = tempfile::tempdir()?;
    let core = Executor::new("application-core")?;
    let mut a = start("a", "a", directory.path(), &core, None, None, 0).await?;
    let client = client()?;
    ensure!(
        call(&client, &a, reqwest::Method::POST, "/write").await? == "a:1",
        "initial generation did not execute"
    );
    let (config, services) = inputs("other", directory.path(), &core, None, 0).await?;
    ensure!(
        Node::start(config, services, graph(), applications("other", None)?)
            .await
            .is_err(),
        "a live directory was shared by two nodes"
    );
    let failed = CompiledApplications::default().register("embedded-node", "test", || {
        anyhow::bail!("invalid replacement")
    })?;
    ensure!(
        a.adopt(graph(), failed).await.is_err(),
        "failed generation was adopted"
    );
    ensure!(
        call(&client, &a, reqwest::Method::GET, "/read").await? == "a:1",
        "failed adoption changed state"
    );
    ensure!(
        a.adopt(graph(), applications("new", None)?).await? == 2,
        "generation identity was reused"
    );
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let body = call(&client, &a, reqwest::Method::GET, "/read").await?;
            if body.starts_with("new:") {
                return Ok::<_, anyhow::Error>(body);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    ensure!(result == "new:1", "adoption lost committed state");
    let services = a.services();
    ensure!(
        a.cancel().await? == NodeOutcome::Cancelled,
        "forced stop claimed graceful completion"
    );
    ensure!(
        services
            .configure_peer_probe(Arc::new(PeerProbeSigner::generate()))
            .is_err(),
        "cancelled host retained authority"
    );
    let reopened = start("a", "new", directory.path(), &core, None, None, 0).await?;
    ensure!(
        call(&client, &reopened, reqwest::Method::GET, "/read").await? == "new:1",
        "forced closure lost SQLite state"
    );
    stopped(reopened).await?;
    ensure!(
        core.handle().spawn(async { 9 }).await? == 9,
        "caller executor was closed"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_bootstrap_retains_directory_until_started_application_factory_has_joined(
) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let core = Executor::new("application-core")?;
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let _release_on_failure = ReleaseFactoryOnDrop(gate.clone());
    let (entered, received) = std::sync::mpsc::channel();
    let factory_gate = gate.clone();
    let apps = CompiledApplications::default().register("embedded-node", "test", move || {
        entered.send(())?;
        let (lock, notify) = &*factory_gate;
        let mut released = lock.lock().unwrap();
        while !*released {
            released = notify.wait(released).unwrap();
        }
        Ok(Arc::new(
            Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).cell(
                "Account",
                |_, _| {
                    Ok(Arc::new(Account {
                        label: "discarded",
                        signals: None,
                    }))
                },
            )?,
        ))
    })?;
    let (config, services) = inputs("cancelled", directory.path(), &core, None, 0).await?;
    let startup = tokio::spawn(Node::start(config, services, graph(), apps));
    tokio::task::spawn_blocking(move || received.recv_timeout(Duration::from_secs(5))).await??;
    startup.abort();
    ensure!(startup.await.is_err(), "startup was not cancelled");
    let (config, services) = inputs("replacement", directory.path(), &core, None, 0).await?;
    ensure!(
        Node::start(
            config,
            services,
            graph(),
            applications("replacement", None)?
        )
        .await
        .is_err(),
        "cancelled startup released a directory with running work"
    );
    *gate.0.lock().unwrap() = true;
    gate.1.notify_all();
    let restarted = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match start(
                "replacement",
                "replacement",
                directory.path(),
                &core,
                None,
                None,
                0,
            )
            .await
            {
                Ok(node) => return Ok::<_, anyhow::Error>(node),
                Err(error) if error.to_string().contains("directory is already owned") => {
                    tokio::time::sleep(Duration::from_millis(10)).await
                }
                Err(error) => return Err(error),
            }
        }
    })
    .await??;
    stopped(restarted).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replacement_public_node_recovers_predecessor_before_replacing_its_lease() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let bucket = Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let core = Executor::new("application-core")?;
    let a = start(
        "leader",
        "leader",
        &directory.path().join("leader"),
        &core,
        Some(bucket.clone()),
        None,
        0,
    )
    .await?;
    let b = start(
        "follower",
        "follower",
        &directory.path().join("follower"),
        &core,
        Some(bucket.clone()),
        None,
        0,
    )
    .await?;
    ready(&a).await?;
    ready(&b).await?;
    let client = client()?;
    ensure!(
        call(&client, &a, reqwest::Method::POST, "/write").await? == "leader:1",
        "leader did not commit"
    );
    ensure!(
        call(&client, &b, reqwest::Method::GET, "/read").await? == "leader:1",
        "peer forwarding did not reach owner"
    );
    let old_generation = a.info().lease_generation.clone();
    a.cancel().await?;
    let recovered = start(
        "leader",
        "recovered",
        &directory.path().join("fresh-leader"),
        &core,
        Some(bucket.clone()),
        None,
        0,
    )
    .await?;
    ready(&recovered).await?;
    ensure!(
        recovered.info().lease_generation != old_generation,
        "replacement reused the old signing generation"
    );
    ensure!(
        call(&client, &recovered, reqwest::Method::GET, "/read").await? == "recovered:1",
        "replacement failed to recover a committed business cell"
    );
    stopped(recovered).await?;
    stopped(b).await?;
    Ok(())
}

#[test]
fn explicit_public_node_policy_ignores_invalid_process_configuration() -> Result<()> {
    let output = std::process::Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "public_nodes_drain_admitted_writes_isolate_fleets_and_leave_caller_executors_running",
            "--nocapture",
        ])
        .env("CELLAR_TTL_MS", "unusable")
        .env("CELLAR_OPERATION_DEADLINE_MS", "unusable")
        .env("CELLAR_SHUTDOWN_TOTAL_MS", "unusable")
        .env("CELLAR_ACTIVATIONS", "unusable")
        .env("CELLAR_LOADER_MAX_WORKERS", "unusable")
        .env("CELLAR_MAX_STATELESS_REQUESTS", "unusable")
        .env("CELLAR_DURABILITY", "unusable")
        .env("CELLAR_REBALANCE_INTERVAL_MS", "unusable")
        .env("CELLAR_READY_FLEET_GATE_MS", "unusable")
        .env("CELLAR_LOG_PIPELINE", "unusable")
        .output()?;
    ensure!(
        output.status.success(),
        "embedded node consulted process configuration:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_predecessor_recovery_preserves_the_predecessor_generation_and_releases_the_directory(
) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let bucket = Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let writer = ExecutionDomain::new(tokio::runtime::Handle::current());
    let prior = PeerProbeSigner::generate().public_key_hex();
    let record = serde_json::json!({
        "node":"unrecovered", "generation": prior, "probe_public_key":prior,
        "expires_ms":0, "addr":"127.0.0.1:9", "peer_protocol":cellar::peer_auth::PROTOCOL_VERSION,
        "log":{"state":"open", "epoch":2, "ensemble":["missing-follower"], "tiered":0, "may_have_fleet_acks":true, "claimant":null, "claimed_ms":null}
    });
    writer
        .run(async {
            cellar::wake_format::ensure_ready(&bucket).await?;
            bucket
                .put("nodes/unrecovered.json", serde_json::to_vec(&record)?)
                .await
        })
        .await??;
    let core = Executor::new("application-core")?;
    let path = directory.path().join("unrecovered");
    let (mut config, services) =
        inputs("unrecovered", &path, &core, Some(bucket.clone()), 0).await?;
    config.startup.recovery_attempts = 1;
    config.startup.recovery_attempt_timeout = Duration::from_millis(100);
    let result = Node::start(
        config,
        services,
        graph(),
        applications("unrecovered", None)?,
    )
    .await;
    ensure!(
        result.is_err(),
        "an unrecovered predecessor was overwritten"
    );
    let error = result.err().unwrap();
    ensure!(
        format!("{error:#}").contains("unrecovered predecessor"),
        "startup did not enforce the recovery gate: {error:#}"
    );
    let (body, _) = writer
        .run(async { bucket.get("nodes/unrecovered.json").await })
        .await??
        .unwrap();
    let preserved: serde_json::Value = serde_json::from_slice(&body)?;
    ensure!(
        preserved["generation"] == prior && preserved["addr"] == "127.0.0.1:9",
        "failed recovery installed a successor lease"
    );
    // A failed recovery must join its runtime before local storage can be reused.
    let local = start("local", "local", &path, &core, None, None, 0).await?;
    stopped(local).await?;
    writer.shutdown().await;
    Ok(())
}
