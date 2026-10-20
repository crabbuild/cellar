//! Same real node/fleet policy for both measured application implementations.
use super::*;
use cellar::sdk::*;

struct Controlled {
    application: Arc<dyn Application>,
    stop: Arc<tokio::sync::Notify>,
    memory: Arc<Mutex<Value>>,
    observe: Observation,
    origin: Instant,
}

impl Application for Controlled {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        if let Invocation::Fetch(request) = &event {
            if request.url.ends_with("/native-observations") {
                let memory = self.memory.lock().unwrap().clone();
                let observation = (self.observe)();
                return Box::pin(async move {
                    Ok(Output::Http(http::Response::from_json(&json!({"memory":memory, "runtime":observation, "observed_elapsed_ns":self.origin.elapsed().as_nanos() as u64}))?.into_http(&context)?))
                });
            }
            if request.url.ends_with("/native-stop") && request.method == "POST" {
                self.stop.notify_one();
                return Box::pin(async move {
                    Ok(Output::Http(
                        http::Response::new(200, Vec::new(), Vec::new()).into_http(&context)?,
                    ))
                });
            }
        }
        self.application.clone().invoke(context, event)
    }

    fn supports_cell(&self, class: &str) -> bool {
        self.application.supports_cell(class)
    }

    fn create_cell(&self, context: Context, identity: &CellIdentity) -> Result<Arc<dyn Cell>> {
        self.application.create_cell(context, identity)
    }

    fn validate_deployment(&self, deployment: &cellar::fleet::LoadedDeployment) -> Result<()> {
        self.application.validate_deployment(deployment)
    }
}

pub(super) async fn serve(
    arguments: &Arguments,
    application: Arc<dyn Application>,
    observe: Observation,
) -> Result<Value> {
    Ok(drive(arguments, application, observe).await?.1)
}

pub(super) async fn drive(
    arguments: &Arguments,
    application: Arc<dyn Application>,
    observe: Observation,
) -> Result<(NodeOutcome, Value)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let internal_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let internal = internal_listener.local_addr()?;
    let name = format!("baseline-{}", arguments.role);
    let signer = Arc::new(PeerProbeSigner::generate());
    let actor = ActorOptions {
        core: ActorPolicy {
            max_resident: 64,
            max_cached_routes: 64,
            max_activations: 16,
            max_evictions: 8,
            max_releases: 8,
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
        lease_ttl_ms: 3000,
        placement_weight: 1,
        generation: signer.public_key_hex(),
        resume_generation: None,
    };
    let mut config = NodeConfig::new(
        ActorIdentity {
            node: name.clone(),
            region: "baseline".into(),
            advertise: internal.to_string(),
        },
        actor,
        arguments.directory.join("disk"),
    );
    config.maintenance.rebalance = false;
    config.runtime.handler_budget = Duration::from_secs(5);
    config.startup.recovery_retry_backoff = Duration::from_secs(3);
    let executor = tokio::runtime::Handle::current();
    let peer_http = reqwest::Client::builder().no_proxy().build()?;
    let peer_auth = Arc::new(PeerAuth::new([61; 32], &name)?);
    let bucket = cellar::bucket::Bucket::open_dev(&arguments.directory.join("fleet.sqlite"))?;
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
            durability_timeout: Duration::from_secs(2),
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
        fleet_durability: false,
    };
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let origin = Instant::now();
    let samples = Arc::new(Mutex::new(
        json!({"sample":memory(), "sampled_elapsed_ns":0}),
    ));
    let app: Arc<dyn Application> = Arc::new(Controlled {
        application,
        stop: shutdown.clone(),
        memory: samples.clone(),
        observe,
        origin,
    });
    let applications =
        CompiledApplications::default().register("perf-cells", "v1", move || Ok(app.clone()))?;
    let node = Node::start(
        config,
        NodeServices {
            wall_clock: None,
            http_client: None,
            host_executor: executor.clone(),
            core_executor: executor,
            listener,
            internal_listener,
            peer_http,
            peer_auth,
            probe_signer: signer,
            fleet: Some(fleet),
            containers: None,
            metrics: None,
        },
        deployment::graph(),
        applications,
    )
    .await?;
    std::fs::write(
        arguments.directory.join("ready.json"),
        serde_json::to_vec(&json!({
            "url":format!("http://{address}"), "mode":arguments.mode, "role":arguments.role,
            "before_runtime":arguments.before_runtime,
            "embed_compiled":cfg!(feature="embed"), "debug_assertions":cfg!(debug_assertions),
            "os":std::env::consts::OS, "arch":std::env::consts::ARCH, "cellar_version":env!("CARGO_PKG_VERSION"),
            "lease_ttl_ms":3000, "host_threads":4, "python_poll_interval_ms":if cfg!(feature="embed") {Some(1)} else {None},
            "durability":"bucket", "store":"private SQLite development bucket", "memory_sample_interval_ms":200,
        }))?,
    )?;
    let serving = node.run_until(async move {
        shutdown.notified().await;
        ShutdownMode::Preserve
    });
    tokio::pin!(serving);
    let mut interval = tokio::time::interval(Duration::from_millis(200));
    let outcome = loop {
        cellar::asyncrt::select! {
            outcome = &mut serving => break outcome?,
            _ = interval.tick() => {
                // The application supervisor samples outside node execution
                // domains and the interpreter loop; requests read only a cache.
                let value = json!({"sample":memory(), "sampled_elapsed_ns":origin.elapsed().as_nanos() as u64});
                *samples.lock().unwrap() = value;
            }
        }
    };
    let NodeOutcome::Stopped(ref report) = outcome else {
        anyhow::bail!("baseline node did not stop: {outcome:?}");
    };
    ensure!(
        report.ingress_drained
            && report.application_work_drained
            && report.pending_application_events == 0
            && report.cells_drained
            && report.pending_dispatches == 0
            && report.log_quiesced
            && report.local_durability_closed
            && report.clean_reload_prepared
            && !report.drain_timed_out
            && !report.deadline_elapsed,
        "baseline node failed complete drain: {report:?}"
    );
    let value = json!({"complete":true, "mode":"preserve", "ingress_drained":report.ingress_drained,
        "application_work_drained":report.application_work_drained, "pending_application_events":report.pending_application_events,
        "cells_drained":report.cells_drained, "pending_dispatches":report.pending_dispatches, "log_quiesced":report.log_quiesced,
        "local_durability_closed":report.local_durability_closed, "clean_reload_prepared":report.clean_reload_prepared,
        "drain_timed_out":report.drain_timed_out, "deadline_elapsed":report.deadline_elapsed});
    Ok((outcome, value))
}
