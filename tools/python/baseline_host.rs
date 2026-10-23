//! Same real node/fleet policy for both measured application implementations.
use super::*;
use cellar::sdk::*;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Controlled {
    application: Arc<dyn Application>,
    direct_cells: bool,
    stop: Arc<tokio::sync::Notify>,
    memory: Arc<Mutex<Value>>,
    observe: Observation,
    origin: Instant,
    resident: Option<Arc<AtomicUsize>>,
}

struct CountedCell {
    inner: Arc<dyn Cell>,
    resident: Arc<AtomicUsize>,
}

impl Cell for CountedCell {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move { self.inner.clone().invoke(context, event).await })
    }
}

impl Drop for CountedCell {
    fn drop(&mut self) {
        self.resident.fetch_sub(1, Ordering::AcqRel);
    }
}

impl Application for Controlled {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        if let Invocation::Fetch(request) = &event {
            if request.url.ends_with("/native-observations") {
                let memory = self.memory.lock().unwrap().clone();
                let observation = (self.observe)();
                let resident = self
                    .resident
                    .as_ref()
                    .map(|count| count.load(Ordering::Acquire));
                return Box::pin(async move {
                    Ok(Output::Http(http::Response::from_json(&json!({"memory":memory, "runtime":observation, "native_resident_cells":resident, "observed_elapsed_ns":self.origin.elapsed().as_nanos() as u64}))?.into_http(&context)?))
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
        if self.direct_cells {
            if let Invocation::Fetch(request) = event {
                // Diagnostic topology only: the same namespace fetch, executed
                // natively instead of by the Python forwarding-only handler.
                let path = match http::Url::new(&request.url, None) {
                    Ok(url) => url.pathname().to_owned(),
                    Err(error) => return Box::pin(async move { Err(error.into()) }),
                };
                if let Some(name) = path.strip_prefix("/cells/").filter(|name| !name.is_empty()) {
                    let name = name.to_owned();
                    return Box::pin(async move {
                        Ok(Output::Http(
                            context
                                .namespace("ENTITIES")?
                                .get_by_name(&name)
                                .fetch(&context, request)
                                .await?,
                        ))
                    });
                }
                return self
                    .application
                    .clone()
                    .invoke(context, Invocation::Fetch(request));
            }
        }
        self.application.clone().invoke(context, event)
    }

    fn supports_cell(&self, class: &str) -> bool {
        self.application.supports_cell(class)
    }

    fn create_cell(&self, context: Context, identity: &CellIdentity) -> Result<Arc<dyn Cell>> {
        let inner = self.application.create_cell(context, identity)?;
        if let Some(resident) = &self.resident {
            resident.fetch_add(1, Ordering::AcqRel);
            Ok(Arc::new(CountedCell {
                inner,
                resident: resident.clone(),
            }))
        } else {
            Ok(inner)
        }
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
    let max_resident = if arguments.residency() { 4096 } else { 64 };
    let actor = ActorOptions {
        core: ActorPolicy {
            max_resident,
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
    #[cfg(feature = "embed")]
    if arguments.mode == "python-defaults" {
        // Isolate Python admission from the native pool's lower default cap.
        // Python Options remain unchanged; record the native fixture policy.
        config.runtime.max_stateless_requests =
            Some(2 * cellar_python::Options::default().max_invocations);
        config.runtime.admission_wait = Duration::ZERO;
    }
    let runtime_policy = config.runtime;
    config.startup.recovery_retry_backoff = Duration::from_secs(3);
    let executor = tokio::runtime::Handle::current();
    let peer_http = reqwest::Client::builder().no_proxy().build()?;
    let peer_auth = Arc::new(PeerAuth::new([61; 32], &name)?);
    let fleet = if arguments.residency() {
        None
    } else {
        let bucket = cellar::bucket::Bucket::open_dev(&arguments.directory.join("fleet.sqlite"))?;
        Some(NodeFleet {
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
        })
    };
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let origin = Instant::now();
    let samples = Arc::new(Mutex::new(
        json!({"sample":memory(), "sampled_elapsed_ns":0}),
    ));
    let app: Arc<dyn Application> = Arc::new(Controlled {
        direct_cells: arguments.mode.contains("-direct"),
        application,
        stop: shutdown.clone(),
        memory: samples.clone(),
        observe,
        origin,
        resident: arguments.residency().then(|| Arc::new(AtomicUsize::new(0))),
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
            fleet,
            containers: None,
            metrics: None,
        },
        deployment::graph(),
        applications,
    )
    .await?;
    #[cfg(feature = "embed")]
    let python_poll_interval_ms = Some(cellar_python::Options::default().poll_interval.as_millis());
    #[cfg(not(feature = "embed"))]
    let python_poll_interval_ms: Option<u128> = None;
    std::fs::write(
        arguments.directory.join("ready.json"),
        serde_json::to_vec(&json!({
            "url":format!("http://{address}"), "mode":arguments.mode, "role":arguments.role,
            "before_runtime":arguments.before_runtime,
            "embed_compiled":cfg!(feature="embed"), "debug_assertions":cfg!(debug_assertions),
            "os":std::env::consts::OS, "arch":std::env::consts::ARCH, "cellar_version":env!("CARGO_PKG_VERSION"),
            "lease_ttl_ms":3000, "host_threads":4, "python_poll_interval_ms":python_poll_interval_ms,
            "durability":if arguments.residency() { "local-only" } else { "bucket" },
            "store":if arguments.residency() { "no fleet store" } else { "private SQLite development bucket" }, "memory_sample_interval_ms":200,
            "native_max_stateless_requests":runtime_policy.max_stateless_requests,
            "native_admission_wait_ms":runtime_policy.admission_wait.as_millis(),
            "handler_budget_ms":runtime_policy.handler_budget.as_millis(), "max_resident":max_resident,
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
    // Local-only residency cannot issue a fleet clean-reload certificate.
    // Every drain/join check remains mandatory in both fixture modes.
    ensure!(
        report.ingress_drained
            && report.application_work_drained
            && report.pending_application_events == 0
            && report.cells_drained
            && report.pending_dispatches == 0
            && report.log_quiesced
            && report.local_durability_closed
            && (report.clean_reload_prepared || arguments.residency())
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
