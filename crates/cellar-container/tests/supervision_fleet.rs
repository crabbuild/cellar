// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real fleet wake discovery and replication, with local cloud protocol peers.
#![allow(clippy::disallowed_methods)]
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{containers::*, *};
use cellar_container::{Adapter, Limits, Operation, Provider};
use serde_json::{json, Value};
use std::{
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::Notify;
#[path = "support/e2b.rs"]
mod e2b;
#[path = "../../cellar/tests/support/html.rs"]
#[allow(dead_code)]
mod graph;
#[path = "support/vercel.rs"]
mod vercel;

const USER_ALARM: i64 = 9_000_000_000_000;

#[derive(Default)]
struct Witness {
    retired: Mutex<Option<BindingCheckpoint>>,
    completed: Notify,
}

struct Observed {
    backend: Arc<dyn MaintenanceBackend>,
    witness: Arc<Witness>,
}

impl MaintenanceBackend for Observed {
    fn identity(&self) -> BackendIdentity {
        self.backend.identity()
    }

    fn delete_safety(&self) -> DeleteSafety {
        self.backend.delete_safety()
    }

    fn maintain(
        self: Arc<Self>,
        context: Context,
        expected: BindingCheckpoint,
        action: MaintenanceAction,
    ) -> HandlerFuture<()> {
        Box::pin(async move {
            self.backend
                .clone()
                .maintain(context.clone(), expected.clone(), action)
                .await?;
            let checkpoint = BindingJournal::open(&context, &self.identity().binding)?
                .load(&context)?
                .context("maintenance lost its binding")?;
            if checkpoint.record.is_retired() {
                ensure!(checkpoint.record.generation == expected.record.generation);
                ensure!(checkpoint.activation_epoch == context.identity().unwrap().epoch);
                *self.witness.retired.lock().unwrap() = Some(checkpoint);
                self.witness.completed.notify_one();
            }
            Ok(())
        })
    }
}

#[derive(Default)]
struct Calls {
    application: AtomicUsize,
    factories: AtomicUsize,
}

struct Root(Adapter);

impl Cell for Root {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = event else {
                anyhow::bail!("maintenance called the business cell");
            };
            if request.url.ends_with("/start") || request.url.ends_with("/lost-start") {
                context
                    .storage()?
                    .put_json("business", &json!("preserved"))?;
                context.storage()?.set_alarm(USER_ALARM)?;
                let result = self
                    .0
                    .session()
                    .execute(
                        &context,
                        Operation::Start,
                        json!({"options":{"env":[["HELLO","world"]]}}),
                        &Limits {
                            max_body_bytes: 65536,
                        },
                    )
                    .await;
                if request.url.ends_with("/lost-start") {
                    ensure!(result.is_err());
                } else {
                    result?;
                }
            }
            let checkpoint = BindingJournal::open(&context, "container")?
                .load(&context)?
                .context("binding missing")?;
            let checkpoint = json!({
                "record": checkpoint.record,
                "revision": checkpoint.revision,
                "activation_epoch": checkpoint.activation_epoch
            });
            let alarm = context.storage()?.get_alarm()?;
            let business = context.storage()?.get_json("business")?;
            let value = json!({"checkpoint":checkpoint,"alarm":alarm,"business":business});
            Ok(Output::Http(
                http::Response::from_json(&value)?.into_http(&context)?,
            ))
        })
    }
}

struct App {
    adapter: Adapter,
    deployed: bool,
    calls: Arc<Calls>,
}

impl Application for App {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        self.calls.application.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            ensure!(self.deployed, "removed application was invoked");
            let Invocation::Fetch(request) = event else {
                anyhow::bail!("unexpected application event");
            };
            Ok(Output::Http(
                context
                    .namespace("COUNTERS")?
                    .get_by_name("fleet-managed")
                    .fetch(&context, request)
                    .await?,
            ))
        })
    }

    fn supports_cell(&self, class: &str) -> bool {
        self.deployed && class == "Counter"
    }

    fn create_cell(&self, _: Context, _: &CellIdentity) -> Result<Arc<dyn Cell>> {
        self.calls.factories.fetch_add(1, Ordering::SeqCst);
        ensure!(self.deployed, "removed cell factory was invoked");
        Ok(Arc::new(Root(self.adapter.clone())))
    }
}

enum Peer {
    E2b(e2b::Peer),
    Vercel(vercel::Peer),
}

impl Peer {
    async fn start(vercel: bool) -> Result<Self> {
        Ok(if vercel {
            Self::Vercel(vercel::Peer::start().await?)
        } else {
            Self::E2b(e2b::Peer::start().await?)
        })
    }

    fn provider(&self) -> Result<Arc<dyn Provider>> {
        match self {
            Self::E2b(p) => p.provider(),
            Self::Vercel(p) => p.provider(),
        }
    }

    fn creates(&self) -> usize {
        match self {
            Self::E2b(p) => p.state.creates.load(Ordering::SeqCst),
            Self::Vercel(p) => p.state.creates.load(Ordering::SeqCst),
        }
    }

    fn deletes(&self) -> usize {
        match self {
            Self::E2b(p) => p.state.deletes.load(Ordering::SeqCst),
            Self::Vercel(p) => p.state.deletes.load(Ordering::SeqCst),
        }
    }

    fn lose_create(&self) {
        match self {
            Self::E2b(p) => p.state.lost_create.store(true, Ordering::SeqCst),
            Self::Vercel(p) => p.state.lost_create.store(true, Ordering::SeqCst),
        }
    }

    fn create_gate(&self) -> (&std::sync::atomic::AtomicBool, &Notify, &Notify) {
        match self {
            Self::E2b(p) => (
                &p.state.hold_create,
                &p.state.create_entered,
                &p.state.create_release,
            ),
            Self::Vercel(p) => (
                &p.state.hold_create,
                &p.state.create_entered,
                &p.state.create_release,
            ),
        }
    }

    async fn stop(self) -> Result<()> {
        match self {
            Self::E2b(p) => p.stop().await,
            Self::Vercel(p) => p.stop().await,
        }
    }
}

fn application(
    provider: Arc<dyn Provider>,
    deployed: bool,
    witness: Arc<Witness>,
    calls: Arc<Calls>,
) -> Result<Arc<dyn Application>> {
    let mut supervision = Supervision::default();
    supervision.register(
        Arc::new(Observed {
            backend: provider.maintenance_backend().context("backend missing")?,
            witness,
        }),
        MaintenancePolicy {
            inspect_interval_ms: 250,
            maximum_lifetime_ms: 5000,
            attempt_timeout_ms: 1000,
            idle_timeout_ms: None,
        },
    )?;
    Ok(supervise(
        Arc::new(App {
            adapter: Adapter::new(provider),
            deployed,
            calls,
        }),
        supervision,
    ))
}

async fn start_node(
    path: &Path,
    name: &str,
    bucket: cellar::bucket::Bucket,
    core: tokio::runtime::Handle,
    application: Arc<dyn Application>,
    deployed: bool,
) -> Result<Node> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let internal_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let signer = Arc::new(PeerProbeSigner::generate());
    let peer_auth = Arc::new(PeerAuth::new([37; 32], name)?);
    let peer_http = reqwest::Client::builder().no_proxy().build()?;
    let mut config = NodeConfig::new(
        ActorIdentity {
            node: name.into(),
            region: "test".into(),
            advertise: internal_listener.local_addr()?.to_string(),
        },
        ActorOptions {
            core: ActorPolicy {
                max_resident: 16,
                max_cached_routes: 16,
                max_activations: 8,
                max_evictions: 4,
                max_releases: 4,
                max_outbound_websockets: 8,
                ownership_on_evict: OwnershipOnEvict::Sticky,
                require_node_lease: true,
                peer_protocol: cellar::peer_auth::PROTOCOL_VERSION,
                operation_deadline_ms: Some(5000),
                owner_log_recovery_backoff_ms: 50,
                owner_log_recovery_attempts: 3,
                alarm_resident_ms: 100,
                idle_evict_ms: None,
                pressure: Default::default(),
            },
            lease_ttl_ms: 1000,
            placement_weight: 1,
            generation: signer.public_key_hex(),
            resume_generation: None,
        },
        path.into(),
    );
    config.runtime.handler_budget = Duration::from_secs(5);
    config.startup.bootstrap_timeout = Duration::from_secs(20);
    config.startup.recovery_attempt_timeout = Duration::from_secs(4);
    config.startup.authority_timeout = Duration::from_secs(4);
    config.maintenance.rebalance = false;
    config.maintenance.wake_period = Duration::from_millis(100);
    config.maintenance.fleet_sample_period = Duration::from_millis(50);
    config.shutdown = ShutdownPolicy {
        total_ms: 8000,
        token_wait_ms: 0,
        no_progress_ms: 5000,
    };
    let fleet = NodeFleet {
        replica_storage: ReplicaStorage::new(
            bucket.store.clone(),
            ReplicaStorageOptions {
                prefix: bucket.prefix.clone(),
                ..Default::default()
            },
        )?,
        lease_bucket: bucket.clone(),
        bucket,
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
    };
    let services = NodeServices {
        wall_clock: None,
        http_client: None,
        host_executor: tokio::runtime::Handle::current(),
        core_executor: core,
        listener,
        internal_listener,
        peer_http,
        peer_auth,
        probe_signer: signer,
        fleet: Some(fleet),
        containers: None,
        metrics: None,
    };
    let mut graph = graph::graph(
        "sandbox-fleet",
        "",
        if deployed {
            vec!["Counter".into()]
        } else {
            vec![]
        },
    );
    if deployed {
        graph.primary.options.bindings = vec![("COUNTERS".into(), "Counter".into())];
    }
    let compiled =
        CompiledApplications::default()
            .register("sandbox-fleet", "test", move || Ok(application.clone()))?;
    let node = Node::start(config, services, graph, compiled).await?;
    let ready = tokio::time::timeout(Duration::from_secs(5), async {
        while !node.is_ready().await? {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("fleet node did not become ready")
    .and_then(|ready| ready);
    if let Err(error) = ready {
        let _ = node.shutdown(ShutdownMode::Preserve).await;
        return Err(error);
    }
    Ok(node)
}

async fn stop(node: Node, mode: ShutdownMode) -> Result<()> {
    let name = node.info().node.clone();
    let outcome = node.shutdown(mode).await?;
    let NodeOutcome::Stopped(report) = outcome else {
        anyhow::bail!("{name}: unexpected stop outcome {outcome:?}");
    };
    ensure!(
        report.mode == mode
            && report.cells_drained
            && report.application_work_drained
            && report.ingress_drained
            && report.pending_application_events == 0
            && report.pending_dispatches == 0
            && report.log_quiesced
            && report.local_durability_closed
            && !report.drain_timed_out
            && !report.deadline_elapsed
            && (mode != ShutdownMode::Preserve || report.clean_reload_prepared),
        "{name}: fleet node did not join cleanly: {report:?}"
    );
    Ok(())
}

async fn call(client: &reqwest::Client, node: &Node, route: &str) -> Result<Value> {
    let response = client
        .get(format!("http://{}/{route}", node.info().public_address))
        .send()
        .await?;
    let status = response.status();
    let body = response.text().await?;
    ensure!(status.is_success(), "{route}: {status}: {body}");
    Ok(serde_json::from_str(&body)?)
}

async fn recovery(
    peer: &Peer,
    removed: bool,
    directory: &Path,
    core: tokio::runtime::Handle,
) -> Result<()> {
    let bucket = cellar::bucket::Bucket::open_dev(&directory.join("fleet.sqlite"))?;
    let provider = peer.provider()?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(6))
        .build()?;
    let first_witness = Arc::new(Witness::default());
    let first = start_node(
        &directory.join("before"),
        "provider-owner",
        bucket.clone(),
        core.clone(),
        application(
            provider.clone(),
            true,
            first_witness.clone(),
            Arc::new(Calls::default()),
        )?,
        true,
    )
    .await?;
    if removed {
        peer.lose_create();
    }
    let before = call(
        &client,
        &first,
        if removed { "lost-start" } else { "start" },
    )
    .await;
    let stopped = stop(first, ShutdownMode::Preserve).await;
    let before = before?;
    stopped?;
    ensure!(
        peer.creates() == 1 && peer.deletes() == 0,
        "resource retired before recovery case began"
    );
    ensure!(first_witness.retired.lock().unwrap().is_none());
    let witness = Arc::new(Witness::default());
    let calls = Arc::new(Calls::default());
    // A fresh directory excludes local SQLite and owner-log cache assistance.
    let second = start_node(
        &directory.join("after"),
        if removed {
            "provider-successor"
        } else {
            "provider-owner"
        },
        bucket.clone(),
        core.clone(),
        application(provider.clone(), !removed, witness.clone(), calls.clone())?,
        !removed,
    )
    .await?;
    let result = async {
        // No application, operator or cell-state request can activate this cell.
        tokio::time::timeout(Duration::from_secs(15), witness.completed.notified())
            .await
            .context("fleet discovery did not retire the retained provider resource")?;
        ensure!(
            calls.application.load(Ordering::SeqCst) == 0
                && calls.factories.load(Ordering::SeqCst) == 0,
            "recovery invoked business code"
        );
        let retired = witness
            .retired
            .lock()
            .unwrap()
            .clone()
            .context("retirement evidence missing")?;
        ensure!(retired.record.generation == 1 && retired.record.is_retired());
        ensure!(
            serde_json::to_value(&retired.record)?["request_id"]
                == before["checkpoint"]["record"]["request_id"]
        );
        ensure!(
            peer.creates() == 1 && peer.deletes() == 1,
            "recovery replayed creation or cleanup"
        );
        Ok::<_, anyhow::Error>(retired)
    }
    .await;
    let retired = match result {
        Ok(retired) => retired,
        Err(error) => {
            let _ = stop(second, ShutdownMode::Preserve).await;
            return Err(error);
        }
    };
    let third = start_node(
        &directory.join("verified"),
        "provider-verifier",
        bucket,
        core,
        application(
            provider,
            true,
            Arc::new(Witness::default()),
            Arc::new(Calls::default()),
        )?,
        true,
    )
    .await;
    let third = match third {
        Ok(third) => third,
        Err(error) => {
            let _ = stop(second, ShutdownMode::Preserve).await;
            return Err(error);
        }
    };
    // A handoff requires a live receiver. Keep the cold verifier ready before
    // releasing the owner which has completed the provider cleanup.
    if let Err(error) = stop(second, ShutdownMode::Handoff).await {
        let _ = stop(third, ShutdownMode::Preserve).await;
        return Err(error);
    }
    let state = call(&client, &third, "state").await;
    let stopped = stop(third, ShutdownMode::Preserve).await;
    let state = state?;
    stopped?;
    ensure!(state["alarm"] == USER_ALARM && state["business"] == "preserved");
    ensure!(state["checkpoint"]["record"] == serde_json::to_value(retired.record)?);
    ensure!(peer.creates() == 1 && peer.deletes() == 1);
    Ok(())
}

fn exercise(removed: bool) -> Result<()> {
    let host = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?;
    let core = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_name("provider-recovery-core")
        .enable_all()
        .build()?;
    host.block_on(async {
        for vercel in [false, true] {
            let peer = Peer::start(vercel).await?;
            let directory = tempfile::tempdir()?;
            let result = recovery(&peer, removed, directory.path(), core.handle().clone()).await;
            let closed = peer.stop().await;
            result?;
            closed?;
        }
        Ok(())
    })
}

#[test]
fn fleet_restart_retires_resources_without_requests_or_local_cell_files() -> Result<()> {
    exercise(false)
}

#[test]
fn removed_class_recovers_an_uncertain_create_and_retires_without_business_code() -> Result<()> {
    exercise(true)
}

async fn accepted_create_owner_loss(
    peer: &Peer,
    directory: &Path,
    core: tokio::runtime::Handle,
) -> Result<()> {
    let bucket = cellar::bucket::Bucket::open_dev(&directory.join("fleet.sqlite"))?;
    let provider = peer.provider()?;
    let original = Arc::new(Witness::default());
    let first = start_node(
        &directory.join("before"),
        "provider-owner",
        bucket.clone(),
        core.clone(),
        application(
            provider.clone(),
            true,
            original.clone(),
            Arc::new(Calls::default()),
        )?,
        true,
    )
    .await?;
    let owner = first.services();
    let (hold, entered, release) = peer.create_gate();
    hold.store(true, Ordering::SeqCst);
    let url = format!("http://{}/start", first.info().public_address);
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(6))
        .build()?;
    let request = tokio::spawn(async move { client.get(url).send().await });
    let accepted = tokio::time::timeout(Duration::from_secs(3), entered.notified()).await;
    if let Err(error) = accepted {
        release.notify_one();
        let _ = stop(first, ShutdownMode::Preserve).await;
        request.abort();
        let _ = request.await;
        return Err(error).context("provider never accepted the held create");
    }
    ensure!(peer.creates() == 1 && peer.deletes() == 0);
    let (paused, entered_pause) = tokio::sync::oneshot::channel();
    // The core has one worker, matching the existing native expiry fixture.
    // Renewal cannot grant work after resuming beyond two 1 s lease lifetimes.
    let suspended = core.spawn(async move {
        let _ = paused.send(());
        std::thread::sleep(Duration::from_millis(2500));
    });
    entered_pause.await?;
    let outcome = tokio::time::timeout(
        Duration::from_secs(10),
        first.run_until(std::future::pending()),
    )
    .await;
    suspended.await?;
    release.notify_one();
    let reply = request.await?;
    ensure!(
        !matches!(&reply,Ok(response) if response.status().is_success()),
        "expired owner acknowledged an accepted create"
    );
    ensure!(
        outcome??
            == NodeOutcome::Fenced {
                code: 3,
                reason: FenceReason::NodeLeaseExpired
            },
        "suspended owner did not report lease expiry"
    );
    ensure!(
        owner
            .configure_metrics(Arc::new(HostMetricsSample::default))
            .is_err(),
        "fencing returned before domain closure"
    );
    ensure!(
        peer.creates() == 1 && peer.deletes() == 0 && original.retired.lock().unwrap().is_none(),
        "remote acceptance was incorrectly retired by the fenced owner"
    );
    let witness = Arc::new(Witness::default());
    let calls = Arc::new(Calls::default());
    let second = start_node(
        &directory.join("after"),
        "provider-successor",
        bucket,
        core,
        application(provider, false, witness.clone(), calls.clone())?,
        false,
    )
    .await?;
    let result = async {
        tokio::time::timeout(Duration::from_secs(15), witness.completed.notified())
            .await
            .context("successor lost the accepted create obligation")?;
        let checkpoint = witness
            .retired
            .lock()
            .unwrap()
            .clone()
            .context("retirement evidence missing")?;
        ensure!(checkpoint.record.generation == 1 && checkpoint.record.is_retired());
        ensure!(
            calls.application.load(Ordering::SeqCst) == 0
                && calls.factories.load(Ordering::SeqCst) == 0
        );
        ensure!(
            peer.creates() == 1 && peer.deletes() == 1,
            "successor replayed creation or deleted another generation"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let stopped = stop(second, ShutdownMode::Preserve).await;
    result?;
    stopped?;
    Ok(())
}

#[test]
fn expired_owner_cannot_acknowledge_an_accepted_create_and_successor_cleans_it() -> Result<()> {
    let host = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()?;
    let core = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_name("provider-expiry-core")
        .enable_all()
        .build()?;
    host.block_on(async {
        for vercel in [false, true] {
            let peer = Peer::start(vercel).await?;
            let directory = tempfile::tempdir()?;
            let result =
                accepted_create_owner_loss(&peer, directory.path(), core.handle().clone()).await;
            peer.create_gate().2.notify_one();
            let closed = peer.stop().await;
            result?;
            closed?;
        }
        Ok(())
    })
}
