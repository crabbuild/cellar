// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Production actors embedded in application-owned runtimes and infrastructure.
#![allow(clippy::disallowed_methods)]
#[path = "support/html.rs"]
mod fixture;
#[path = "support/actor_lease_trace.rs"]
mod lease_trace;
use anyhow::{Context as _, Result};
use cellar::actor::{AppHandle, DrainPinRegistry, Message};
use cellar::engine_api::{HttpResponse, RuntimeFetch};
use cellar::generation::Generation;
use cellar::peer_auth::PeerAuth;
use cellar::runtime::{Replication, RuntimeManager};
use cellar::sdk::{
    Actor, ActorIdentity, ActorOptions, ActorPolicy, ActorServices, ExecutionDomain,
    OwnershipOnEvict, ReplicaStorage, ReplicaStorageOptions, ReplicationOptions, RuntimePolicy,
};
use cellar::sdk::{Application, Cell, Context, HandlerFuture, Invocation, Output, Registry};
use cellar_logic::Route;
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize},
    Arc,
};
use std::time::Duration;
use tokio::sync::mpsc;

#[tokio::test]
async fn actor_closure_during_a_poll_does_not_panic_or_stop_the_caller_executor() -> Result<()> {
    use futures_util::FutureExt;
    let core = Executor::new("closing-actor-core")?;
    let domain = Arc::new(ExecutionDomain::new(tokio::runtime::Handle::current()));
    let closing = Arc::downgrade(&domain);
    let sampled = Arc::new(tokio::sync::Notify::new());
    let signal = sampled.clone();
    let refused = Arc::new(AtomicBool::new(false));
    let rejection = refused.clone();
    domain.services().configure_metrics(Arc::new(move || {
        // Actor::start samples metrics in its current poll. Abort cannot
        // preempt that poll: a prequeued mailbox input is still ready when
        // the actor reaches its next selector/step boundary.
        closing.upgrade().unwrap().close();
        rejection.store(
            cellar::asyncrt::try_spawn(async { panic!("a refused task was polled") }).is_err(),
            std::sync::atomic::Ordering::Release,
        );
        signal.notify_one();
        cellar::sdk::HostMetricsSample::default()
    }))?;
    let (fence, _) = mpsc::unbounded_channel();
    let actor = domain
        .run(Actor::new(
            options("closing", 1_000, 2),
            fence,
            services(None)?,
            identity(),
        ))
        .await??;
    let (mailbox, receive) = mpsc::unbounded_channel();
    mailbox.send(Message::SampleLoad(
        cellar::sdk::HostMetricsSample::default(),
    ))?;
    let panicked = Arc::new(AtomicBool::new(false));
    let observed = panicked.clone();
    let task = domain.spawn_on(core.handle(), async move {
        let result = std::panic::AssertUnwindSafe(actor.run(receive))
            .catch_unwind()
            .await;
        observed.store(result.is_err(), std::sync::atomic::Ordering::Release);
    })?;
    tokio::time::timeout(Duration::from_secs(5), sampled.notified()).await?;
    tokio::time::timeout(Duration::from_secs(5), domain.shutdown()).await?;
    let _ = task.await;
    assert!(refused.load(std::sync::atomic::Ordering::Acquire));
    assert!(
        !panicked.load(std::sync::atomic::Ordering::Acquire),
        "actor panicked after its own domain closed within its current poll"
    );
    assert!(
        core.handle().spawn(async { true }).await?,
        "caller core executor stopped"
    );
    Ok(())
}

// The application's executor owner also cleans up on a failed assertion.
struct Executor(Option<tokio::runtime::Runtime>);

impl Executor {
    fn new(name: &str) -> Result<Self> {
        Ok(Self(Some(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .thread_name(name)
                .enable_all()
                .build()?,
        )))
    }

    fn handle(&self) -> &tokio::runtime::Handle {
        self.0.as_ref().unwrap().handle()
    }
}

impl Drop for Executor {
    fn drop(&mut self) {
        self.0.take().unwrap().shutdown_background();
    }
}

fn options(label: &str, ttl: u64, capacity: usize) -> ActorOptions {
    ActorOptions {
        core: ActorPolicy {
            max_resident: capacity,
            max_cached_routes: 0,
            max_activations: capacity,
            max_evictions: 2,
            max_releases: 2,
            max_outbound_websockets: capacity,
            ownership_on_evict: OwnershipOnEvict::Sticky,
            require_node_lease: true,
            peer_protocol: cellar::peer_auth::PROTOCOL_VERSION,
            operation_deadline_ms: Some(if capacity == 7 { 6_000 } else { 3_000 }),
            owner_log_recovery_backoff_ms: 50,
            owner_log_recovery_attempts: 3,
            alarm_resident_ms: capacity as u64 * 100,
            idle_evict_ms: None,
            pressure: Default::default(),
        },
        lease_ttl_ms: ttl,
        placement_weight: capacity as u64,
        generation: format!("{label}-process"),
        resume_generation: None,
    }
}

fn identity() -> ActorIdentity {
    ActorIdentity {
        node: "test".into(),
        advertise: "127.0.0.1:1234".into(),
        region: "test".into(),
    }
}

fn services(runtime: Option<RuntimeManager>) -> Result<ActorServices> {
    Ok(ActorServices {
        runtime,
        drain_pins: DrainPinRegistry::default(),
        ownership: None,
        peer_http: reqwest::Client::new(),
        peer_auth: Arc::new(PeerAuth::new([1; 32], "test")?),
    })
}

struct Account(&'static str);

impl Cell for Account {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(_) = event else {
                return Ok(Output::Done);
            };
            let storage = context.storage()?;
            let count = storage
                .get_json("count")?
                .and_then(|n| n.as_u64())
                .unwrap_or(0)
                + 1;
            storage.put_json("count", &serde_json::json!(count))?;
            Ok(Output::Http(HttpResponse {
                status: 200,
                body: format!("{}:{count}", self.0).into_bytes(),
                headers: Vec::new(),
                stream: None,
                websocket: None,
                write_position: None,
                observed_position: None,
            }))
        })
    }
}

async fn build(
    domain: &ExecutionDomain,
    label: &'static str,
    path: std::path::PathBuf,
    actor_options: ActorOptions,
) -> Result<(
    Actor,
    AppHandle,
    mpsc::UnboundedReceiver<Message>,
    Replication,
    mpsc::UnboundedReceiver<cellar::sdk::FenceReport>,
)> {
    domain
        .run(async move {
            domain.services().configure_actor(&actor_options)?;
            domain.services().configure_runtime(RuntimePolicy {
                max_stateless_workers: actor_options.core.max_resident,
                max_stateless_requests: Some(actor_options.core.max_resident),
                max_cell_requests: actor_options.core.max_resident,
                handler_budget: Duration::from_secs(actor_options.core.max_resident as u64),
                ..Default::default()
            })?;
            let registry = Registry::new(|_, _| Box::pin(async { Ok(Output::Done) }))
                .cell("Account", move |_, _| Ok(Arc::new(Account(label))))?;
            let generation = Generation::build_native(
                1,
                fixture::graph("embedded-actor", "", vec!["Account".into()]),
                fixture::generation_options(),
                HashMap::from([(
                    "embedded-actor".into(),
                    Arc::new(registry) as Arc<dyn Application>,
                )]),
            )?;
            std::fs::create_dir_all(&path)?;
            let bucket = cellar::bucket::Bucket::open_dev(&path.join("replica.sqlite"))?;
            let replication = Replication::start(
                &path.join("cells"),
                ReplicaStorage::new(bucket.store.clone(), ReplicaStorageOptions::default())?,
                ReplicationOptions {
                    sync_concurrency: actor_options.core.max_resident,
                    restore_concurrency: actor_options.core.max_resident,
                    ..Default::default()
                },
            )?;
            let mut runtime_options = fixture::runtime_options(path.join("cells"));
            runtime_options.replication = Some(replication.clone());
            runtime_options.bucket = Some(bucket);
            let runtime = RuntimeManager::start(generation, runtime_options)?;
            let actor_services = services(Some(runtime.clone()))?;
            let drain_pins = actor_services.drain_pins.clone();
            let peer_http = actor_services.peer_http.clone();
            let peer_auth = actor_services.peer_auth.clone();
            let (fence, fenced) = mpsc::unbounded_channel();
            let max_outbound_websockets = actor_options.core.max_outbound_websockets;
            let operation_deadline_ms = actor_options.core.operation_deadline_ms.unwrap();
            let actor = Actor::new(actor_options, fence, actor_services, identity()).await?;
            let (tx, rx) = mpsc::unbounded_channel();
            let (reload, _) = cellar::generation::reload_channel();
            let (websockets, _) = mpsc::unbounded_channel();
            let app = AppHandle {
                tx,
                runtime: Some(runtime),
                reload,
                peer_http,
                peer_auth,
                advertise: identity().advertise,
                websockets,
                max_outbound_websockets,
                draining: Arc::new(AtomicBool::new(false)),
                fleet_ready: Arc::new(AtomicBool::new(true)),
                public_in_flight: Arc::new(AtomicUsize::new(0)),
                drain_pins,
                follower: None,
                trust_forwarded_headers: false,
                max_request_body_bytes: 1024,
                operation_deadline_ms,
            };
            Ok((actor, app, rx, replication, fenced))
        })
        .await?
}

async fn fetch(app: &AppHandle) -> Result<HttpResponse> {
    let cell = "Account:shared-id".to_string();
    let routed = app
        .request(cell.clone())
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))
        .context("actor route request failed")?;
    assert_eq!(routed.route, Route::Local);
    let completion = app
        .local_request(routed.request, cell.clone(), None, "embedded-test")
        .run(app.runtime.as_ref().unwrap().fetch_cell(
            cell,
            None,
            RuntimeFetch {
                url: "http://application/count".into(),
                method: "POST".into(),
                body: Vec::new().into(),
                headers: Vec::new(),
                request_id: None,
                order: None,
                parent: None,
            },
            None,
        ))
        .await;
    let response = completion
        .result
        .map_err(|e| anyhow::anyhow!("{e:?}"))
        .context("actor invocation or output proof failed")?;
    assert!(
        response.write_position.is_some(),
        "the real output gate must receive a SQLite position"
    );
    Ok(response)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn embedded_production_actors_route_and_prove_writes_on_caller_executors() -> Result<()> {
    let lease_trace = lease_trace::install();
    // These executors and their lifetime belong to the application.
    let first_core = Executor::new("application-core-a")?;
    let second_core = Executor::new("application-core-b")?;
    let first = ExecutionDomain::new(tokio::runtime::Handle::current());
    let second = ExecutionDomain::new(tokio::runtime::Handle::current());
    let directory = tempfile::tempdir()?;
    let (actor_a, app_a, rx_a, replica_a, mut fences_a) = build(
        &first,
        "first",
        directory.path().join("first"),
        options("first", 700, 3),
    )
    .await?;
    let (actor_b, app_b, rx_b, replica_b, mut fences_b) = build(
        &second,
        "second",
        directory.path().join("second"),
        options("second", 1_100, 7),
    )
    .await?;
    assert_eq!(actor_a.lease_spec.ttl_ms, 700);
    assert_eq!(actor_b.lease_spec.ttl_ms, 1_100);
    assert_eq!(actor_a.lease_spec.generation, "first-process");
    assert_eq!(actor_b.lease_spec.generation, "second-process");
    assert_eq!(
        first
            .run(async { cellar::actor::operation_deadline_ms() })
            .await??,
        3_000
    );
    assert_eq!(
        second
            .run(async { cellar::actor::operation_deadline_ms() })
            .await??,
        6_000
    );
    let mut task_a = actor_a.spawn(&first, first_core.handle(), rx_a)?;
    let mut task_b = actor_b.spawn(&second, second_core.handle(), rx_b)?;
    lease_trace.mark("both actor tasks spawned");
    let mut joined_a = false;
    let mut joined_b = false;
    let result = tokio::time::timeout(Duration::from_secs(15), async {
        for (phase, domain, app) in [
            ("first actor healthy", &first, &app_a),
            ("second actor healthy", &second, &app_b),
        ] {
            while !domain.run(app.healthy()).await? {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            lease_trace.mark(phase);
        }
        for count in 1..=3 {
            lease_trace.mark(&format!("starting fetch {count}"));
            let (a, b) = tokio::join!(first.run(fetch(&app_a)), second.run(fetch(&app_b)));
            assert_eq!(
                a.context("first actor fetch domain closed")?
                    .with_context(|| format!("first actor fetch {count}"))?
                    .body,
                format!("first:{count}").as_bytes()
            );
            assert_eq!(
                b.context("second actor fetch domain closed")?
                    .with_context(|| format!("second actor fetch {count}"))?
                    .body,
                format!("second:{count}").as_bytes()
            );
        }
        // Restoration reads completed LTX objects from the supplied store.
        // A fake gate acknowledgement cannot satisfy this check.
        for (domain, replica) in [(&first, &replica_a), (&second, &replica_b)] {
            let snapshot = domain
                .run(replica.restore_snapshot("Account:shared-id"))
                .await??
                .context("gated writes must have a restorable replica")?;
            let connection = rusqlite::Connection::open(snapshot.path())?;
            let json: String = connection.query_row(
                "SELECT v FROM __sys_cell_kv WHERE k = 'count'",
                [],
                |row| row.get(0),
            )?;
            assert_eq!(serde_json::from_str::<u64>(&json)?, 3);
        }
        // Core timers must renew beyond each actor's first published TTL.
        tokio::time::sleep(Duration::from_millis(1_200)).await;
        assert!(first.run(app_a.healthy()).await?);
        assert!(second.run(app_b.healthy()).await?);
        anyhow::ensure!(
            lease_trace.applied_renewal("application-core-a"),
            "fixture did not observe the first core's successful renewal"
        );
        anyhow::ensure!(
            lease_trace.applied_renewal("application-core-b"),
            "fixture did not observe the second core's successful renewal"
        );
        first.shutdown().await;
        let finished = (&mut task_a).await;
        joined_a = true;
        assert!(finished.is_err());
        assert_eq!(
            second
                .run(fetch(&app_b))
                .await?
                .context("second actor fetch after first domain shutdown")?
                .body,
            b"second:4"
        );
        assert_eq!(tokio::spawn(async { 42 }).await?, 42);
        second.shutdown().await;
        let finished = (&mut task_b).await;
        joined_b = true;
        assert!(finished.is_err());
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("embedded actor operation budget")
    .and_then(|result| result);
    if let Err(error) = &result {
        use futures_util::FutureExt as _;
        let first_task = (!joined_a).then(|| (&mut task_a).now_or_never());
        let second_task = (!joined_b).then(|| (&mut task_b).now_or_never());
        eprintln!(
            "native actor failure: {error:#}; first_fence={:?}; second_fence={:?}; \
             first_domain_open={}; second_domain_open={}; first_task={first_task:?}; \
             second_task={second_task:?}",
            fences_a.try_recv(),
            fences_b.try_recv(),
            !first.is_closed(),
            !second.is_closed(),
        );
        eprintln!(
            "native actor lease observations: {:#?}",
            lease_trace.snapshot()
        );
    }
    // A returned error must not abandon either domain's
    // owned work before the fixture directory and caller executors are dropped.
    first.shutdown().await;
    second.shutdown().await;
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_actor_rejects_foreign_authority_and_inconsistent_policy() -> Result<()> {
    let first = ExecutionDomain::new(tokio::runtime::Handle::current());
    let second = ExecutionDomain::new(tokio::runtime::Handle::current());
    let directory = tempfile::tempdir()?;
    let (actor, app, receive, _, _fenced) =
        build(&first, "a", directory.path().into(), options("a", 1_000, 2)).await?;
    assert!(actor
        .spawn(&second, &tokio::runtime::Handle::current(), receive)
        .is_err());
    let (fence, _) = mpsc::unbounded_channel();
    assert!(second
        .run(Actor::new(
            options("b", 1_000, 2),
            fence,
            services(app.runtime.clone())?,
            identity()
        ))
        .await?
        .is_err());
    let (fence, _) = mpsc::unbounded_channel();
    let actor = first
        .run(Actor::new(
            options("a", 1_000, 2),
            fence,
            services(None)?,
            identity(),
        ))
        .await??;
    let (_, receive) = mpsc::unbounded_channel();
    assert!(second.run(actor.run(receive)).await?.is_err());
    for case in 0..5 {
        let mut policy = options("a", 1_000, 2);
        match case {
            0 => policy.core.require_node_lease = false,
            1 => policy.core.ownership_on_evict = OwnershipOnEvict::Release,
            2 => policy.core.operation_deadline_ms = None,
            3 => policy.lease_ttl_ms = 0,
            _ => policy.core.peer_protocol += 1,
        }
        let (fence, _) = mpsc::unbounded_channel();
        assert!(first
            .run(Actor::new(policy, fence, services(None)?, identity()))
            .await?
            .is_err());
    }
    // Changing a policy after application queues sampled it must be rejected.
    let mut changed = options("a", 1_000, 2);
    changed.core.operation_deadline_ms = Some(6_000);
    assert!(first.services().configure_actor(&changed).is_err());
    first.shutdown().await;
    second.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn supplied_executor_keeps_effects_and_joined_cancellation_in_the_owning_domain() -> Result<()>
{
    let core = Executor::new("application-core")?;
    let application = tokio::runtime::Handle::current().id();
    let core_id = core.handle().id();
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let services = domain.services();
    let (entered, receive) = tokio::sync::oneshot::channel();
    let task = domain.spawn_on(core.handle(), async move {
        assert_eq!(tokio::runtime::Handle::current().id(), core_id);
        assert!(Arc::ptr_eq(&services, &cellar::asyncrt::services()));
        let effect = cellar::asyncrt::spawn(async move {
            assert_eq!(tokio::runtime::Handle::current().id(), application);
        });
        effect.await.unwrap();
        tokio::time::sleep(Duration::from_millis(1)).await;
        entered.send(()).unwrap();
        std::future::pending::<()>().await;
    })?;
    tokio::time::timeout(Duration::from_secs(2), receive).await??;
    domain.shutdown().await;
    assert!(task.await.is_err());
    assert!(domain.spawn_on(core.handle(), async {}).is_err());
    assert_eq!(tokio::spawn(async { 42 }).await?, 42);
    Ok(())
}

// Poison only a child process: parallel test hosts never share an environment mutation.
#[test]
fn default_actor_deadline_ignores_process_environment() -> Result<()> {
    const CHILD: &str = "CELLAR_DEFAULT_DEADLINE_CHILD";
    if std::env::var_os(CHILD).is_some() {
        let runner = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        return runner.block_on(async {
            let default = ExecutionDomain::new(tokio::runtime::Handle::current());
            let configured = ExecutionDomain::new(tokio::runtime::Handle::current());
            let explicit = options("explicit-deadline", 1_000, 7);
            configured.services().configure_actor(&explicit)?;
            // Generation construction samples Queue settlement policy before
            // there is an actor, even for a script without Queue consumers.
            for domain in [&default, &configured] {
                domain
                    .run(async {
                        let registry = Registry::new(|_, _| Box::pin(async { Ok(Output::Done) }));
                        let _generation = Generation::build_native(
                            1,
                            fixture::graph("deadline-policy", "", Vec::new()),
                            fixture::generation_options(),
                            HashMap::from([(
                                "deadline-policy".into(),
                                Arc::new(registry) as Arc<dyn Application>,
                            )]),
                        )?;
                        Ok::<_, anyhow::Error>(())
                    })
                    .await??;
            }
            assert_eq!(
                default
                    .run(async { cellar::actor::operation_deadline_ms() })
                    .await??,
                15_000,
                "library default sampled the process deadline"
            );
            assert_eq!(
                configured
                    .run(async { cellar::actor::operation_deadline_ms() })
                    .await??,
                6_000
            );
            let mut matching = options("default-deadline", 1_000, 2);
            matching.core.operation_deadline_ms = Some(15_000);
            default.services().configure_actor(&matching)?;
            matching.core.operation_deadline_ms = Some(6_000);
            assert!(default.services().configure_actor(&matching).is_err());
            default.shutdown().await;
            assert_eq!(
                configured
                    .run(async { cellar::actor::operation_deadline_ms() })
                    .await??,
                6_000
            );
            configured.shutdown().await;
            Ok(())
        });
    }
    for process_deadline in ["37", "unusable", "0"] {
        let output = std::process::Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "default_actor_deadline_ignores_process_environment",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .env("CELLAR_OPERATION_DEADLINE_MS", process_deadline)
            .output()?;
        anyhow::ensure!(
            output.status.success(),
            "library default depended on process deadline {process_deadline:?}:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}

#[test]
fn explicit_actor_policy_survives_unusable_process_environment() -> Result<()> {
    let mut command = std::process::Command::new(std::env::current_exe()?);
    command.args([
        "--exact",
        "embedded_production_actors_route_and_prove_writes_on_caller_executors",
        "--nocapture",
    ]);
    for setting in [
        "CELLAR_TTL_MS",
        "CELLAR_OPERATION_DEADLINE_MS",
        "CELLAR_RECOVERY_RETRY_MS",
        "CELLAR_RECOVERY_RETRIES",
        "CELLAR_IDLE_EVICT_S",
        "CELLAR_ALARM_RESIDENT_MS",
        "CELLAR_MAX_OUTBOUND_WEBSOCKETS",
        "CELLAR_RSS_HIGH_MB",
        "CELLAR_MAX_STATELESS_ISOLATES",
        "CELLAR_MAX_REQUESTS",
        "CELLAR_MAX_CELL_REQUESTS",
        "CELLAR_ADMISSION_WAIT_MS",
        "CELLAR_HANDLER_BUDGET_S",
        "CELLAR_LOG_FLUSH_MS",
        "CELLAR_LTX_DURABILITY_TIMEOUT_SECS",
        "CELLAR_LTX_COMPACTION",
        "CELLAR_LTX_COMPACTIONS",
        "CELLAR_LTX_COMPACTION_MIN_MB",
        "CELLAR_LTX_COMPACTION_MIN_TXIDS",
        "CELLAR_LTX_HYDRATE_MBPS",
        "CELLAR_LTX_PAGED",
        "CELLAR_LTX_PAGED_MIN_MB",
        "CELLAR_LTX_RETENTION_SECS",
        "CELLAR_LTX_TRUNCATE_PAGES",
    ] {
        command.env(setting, "unusable-process-setting");
    }
    let output = command.output()?;
    anyhow::ensure!(
        output.status.success(),
        "explicit actor policy child failed under unusable process settings:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}
