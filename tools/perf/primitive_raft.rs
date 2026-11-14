// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Three independent RF3 voters serving the ordinary primitive bindings.
#![allow(clippy::disallowed_methods)]
use super::*;
use cellar::sdk::consensus::verification::RootPrivateOptions;

fn namespace(layout: &raft_groups::Layout, schema: [u8; 32]) -> Result<RaftNamespaceConfig> {
    let mut result = raft_setup::namespace_with_storage(layout, schema, RaftImageStorage::Extents)?;
    result.namespace.minimum_durability = match std::env::var("CELLAR_PERF_DURABILITY")?.as_str() {
        "raft-quorum" => Durability::Quorum,
        "raft-s3" => Durability::S3,
        _ => anyhow::bail!("invalid primitive Raft durability"),
    };
    Ok(result)
}

pub async fn run() -> Result<()> {
    let family = primitive_app::family()?;
    let application = primitive_app::application(true)?;
    let class = primitive_app::class(&family);
    let schema = application.resolve_raft_class(class)?.schema_digest();
    // Keep the actual binding scopes; Queue includes its declared dead-letter
    // cells. Every family has three groups, and all groups have three voters.
    let mut identities = Vec::new();
    for (ordinal, dead) in
        (0..primitive_app::ENTITIES).flat_map(|ordinal| [false, true].map(|dead| (ordinal, dead)))
    {
        if dead && family != "queue" {
            continue;
        }
        let id = primitive_app::identity(&family, ordinal, dead);
        let scope = match family.as_str() {
            "sql" => cellar::engine_api::sql_cell_scope(&id),
            "kv" => cellar::engine_api::kv_cell_scope(&id, 0),
            _ => cellar::engine_api::queue_cell_scope(&id),
        };
        identities.push(CellId::new(scope)?);
    }
    let occupancy = identities.len() / 3;
    let layout = Arc::new(raft_groups::Layout::from_cells(
        identities, occupancy, class, 0,
    )?);
    let placement = Placement::new(3)?;
    let i: u8 = std::env::var("RAFT_NODE")?.parse()?;
    ensure!((1..=3).contains(&i), "invalid voter");
    let network = std::env::var("RAFT_NETWORK_PREFIX")?;
    let peers = peer_directory(placement, &network, 0)?;
    let tls = RaftTls::new(
        vec![std::fs::read("/config/ca.der")?.into()],
        vec![std::fs::read("/config/server.der")?.into()],
        rustls::pki_types::PrivateKeyDer::try_from(std::fs::read("/config/key.der")?)
            .map_err(|e| anyhow::anyhow!(e))?,
    )?;
    let root = PathBuf::from("/data");
    let bucket_name = std::env::var("CELLAR_PERF_BUCKET")?;
    let builder = AmazonS3Builder::new()
        .with_bucket_name(&bucket_name)
        .with_region("us-east-1")
        .with_endpoint("http://rustfs:9000")
        .with_allow_http(true)
        .with_virtual_hosted_style_request(false)
        .with_access_key_id(std::env::var("AWS_ACCESS_KEY_ID")?)
        .with_secret_access_key(std::env::var("AWS_SECRET_ACCESS_KEY")?)
        .with_conditional_put(S3ConditionalPut::ETagMatch);
    let ordinary = Arc::new(builder.clone().build()?);
    let conditional = Arc::new(
        builder
            .with_retry(object_store::RetryConfig {
                max_retries: 0,
                ..Default::default()
            })
            .build()?,
    );
    let bucket = Bucket::from_clients(
        ordinary.clone(),
        conditional,
        ordinary,
        StorageBackend::S3,
        bucket_name,
        String::new(),
    )?;
    let store = bucket.raft_store();
    let mut host = RaftHostOptions {
        max_groups: 3,
        blocking_workers: 1,
        worker_queue: 32,
        max_requests: 128,
        max_input_bytes: 16 * 1024,
        effects: Some(RaftEffectOptions {
            callback_timeout_ms: 5_000,
            ..Default::default()
        }),
        ..Default::default()
    };
    host.images.max_bytes = 128 * 1024 * 1024;
    // Current, private, apply, incoming and snapshot owners can retain eight
    // copies. Keep every admitted image inside the unchanged shared bank.
    host.execution.max_image_bytes = host.images.max_bytes / 8;
    host.extents = Some(raft_limits::extents(&host)?);
    // The public local inventory API requires an explicit inspection profile.
    // This fixed cohort does not advertise managed fleet capacity qualification.
    host.node_profile = Some(RaftNodeProfile {
        pool: "primitive-static".into(),
        region: "us-east-1".into(),
        zone: "shared-colima".into(),
        group_capacity: 3,
        byte_capacity: 3 * host.images.max_bytes,
        bytes_per_group: host.images.max_bytes,
        leader_capacity: 3,
        draining: false,
    });
    host.close_report.max_groups = 3;
    host.archive.max_producers = 1;
    host.archive.max_blocking_jobs = 1;
    host.archive.max_batch_groups = 1;
    let mut shared = raft_limits::shared(&host, 3, occupancy)?;
    if family == "kv" {
        shared.payloads = Some(CellGroupPayloadLimits {
            max_operations: 2,
            max_retained_bytes: 600 * 1024 * 1024,
            max_payload_bytes: 64 * 1024 * 1024 + 4096,
            io_timeout: Duration::from_secs(10),
        });
    }
    // Record the pre-existing production harness page budget. This is a new
    // resource profile, not a repair of the separate 1,024-page fixture failure.
    let profile = serde_json::json!({"family":family,"logicalCells":layout.total,"groups":3,"votersPerGroup":3,
        "cellsPerGroup":occupancy,"manifestPages":shared.manifest_pages,"manifestBytes":shared.manifest_bytes,
        "manifestRetainedRoots":shared.retained_roots,"sqlWorkers":1,"actorOperationMs":5000,"actorLeaseMs":5000,
        "callbackMs":5000,"imageBytes":host.images.max_bytes,"perCellImageBytes":host.execution.max_image_bytes,
        "imageOwnershipCopies":8,"manifestBudgetScope":"per physical group page store","images":"extents",
        "privateWorkspaceBytes":RootPrivateOptions::workspace_bounds(host.execution)?.reservation()?,"layout":layout.json(placement)});
    let initialize = std::env::var("RAFT_INITIALIZE").as_deref() == Ok("1");
    ensure!(!initialize || i == 1, "initialization owner");
    raft_setup::prepare(
        &root,
        store.clone(),
        namespace(&layout, schema)?,
        &layout,
        &peers,
        initialize,
        shared,
    )
    .await?;
    if initialize {
        return cellar::cli_output::Output::new(cellar::cli_output::Format::Json)
            .line(format_args!("{profile}"));
    }
    let core = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .thread_name("primitive-core")
        .enable_all()
        .build()?;
    let result = serve(
        i,
        network,
        root,
        application,
        bucket,
        store,
        layout,
        peers,
        tls,
        host,
        shared,
        schema,
        core.handle().clone(),
        profile,
    )
    .await;
    tokio::task::spawn_blocking(move || core.shutdown_timeout(Duration::from_secs(30))).await?;
    result
}

#[allow(clippy::too_many_arguments)]
async fn serve(
    i: u8,
    network: String,
    root: PathBuf,
    application: Arc<dyn Application>,
    bucket: Bucket,
    store: Arc<dyn object_store::ObjectStore>,
    layout: Arc<raft_groups::Layout>,
    peers: Vec<RaftPeerNode>,
    tls: RaftTls,
    host: RaftHostOptions,
    shared: CellGroupNodeLimits,
    schema: [u8; 32],
    core: tokio::runtime::Handle,
    profile: serde_json::Value,
) -> Result<()> {
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let signer = Arc::new(PeerProbeSigner::generate());
    let label = format!("raft-node{i}");
    let peer_http = reqwest::Client::builder()
        .no_proxy()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(10))
        .build()?;
    let peer_auth = Arc::new(PeerAuth::new([19; 32], &label)?);
    let fleet = NodeFleet {
        replica_storage: ReplicaStorage::new(
            bucket.store.clone(),
            ReplicaStorageOptions::default(),
        )?,
        lease_bucket: bucket.clone(),
        bucket,
        replication: ReplicationOptions::default(),
        log: LogOptions::default(),
        fleet_durability: false,
        log_transport: Arc::new(SignedPeerTransport::from_clients(
            peer_http.clone(),
            reqwest::Client::builder().no_proxy().build()?,
            peer_auth.clone(),
        )),
    };
    let mut config = NodeConfig::new(
        ActorIdentity {
            node: label.clone(),
            advertise: format!("{network}.{}:8081", i + 10),
            region: "us-east-1".into(),
        },
        ActorOptions {
            core: ActorPolicy {
                max_resident: 128,
                max_cached_routes: 256,
                max_activations: 64,
                max_evictions: 4,
                max_releases: 32,
                max_outbound_websockets: 32,
                ownership_on_evict: OwnershipOnEvict::Release,
                require_node_lease: true,
                peer_protocol: cellar::peer_auth::PROTOCOL_VERSION,
                operation_deadline_ms: Some(5000),
                owner_log_recovery_backoff_ms: 1000,
                owner_log_recovery_attempts: 240,
                alarm_resident_ms: 3_600_000,
                idle_evict_ms: None,
                pressure: Default::default(),
            },
            lease_ttl_ms: 5000,
            placement_weight: 1,
            generation: signer.public_key_hex(),
            resume_generation: None,
        },
        root.join("node"),
    );
    config.maintenance.rebalance = false;
    config.runtime.max_stateless_requests = Some(128);
    let app = application.clone();
    let node = Node::start(
        config,
        NodeServices {
            wall_clock: None,
            http_client: None,
            host_executor: tokio::runtime::Handle::current(),
            core_executor: core,
            listener: tokio::net::TcpListener::bind("0.0.0.0:8080").await?,
            internal_listener: tokio::net::TcpListener::bind("0.0.0.0:8081").await?,
            peer_http,
            peer_auth,
            probe_signer: signer,
            fleet: Some(fleet),
            containers: None,
            metrics: None,
        },
        primitive_app::graph()?,
        CompiledApplications::default().register(
            primitive_app::SCRIPT,
            primitive_app::VERSION,
            move || Ok(app.clone()),
        )?,
    )
    .await?;
    let result = async {
        let peer = peers[usize::from(i)-1].clone();
        // These declared frontends may invoke this fixture's one namespace.
        // RF3 membership itself deliberately grants no command permission.
        let cluster = ClusterId::new([1; 16])?;
        let namespace_id = NamespaceId::new([2; 16])?;
        let query_origins = peers.iter().filter(|origin| origin.node != peer.node)
            .map(|origin| NamespaceQueryOrigin { cluster, namespace: namespace_id,
                revision: 1, peer: origin.clone() }).collect();
        let command_origins = peers.iter().filter(|origin| origin.node != peer.node)
            .map(|origin| NamespaceCommandOrigin { cluster, namespace: namespace_id,
                revision: 1, peer: origin.clone() }).collect();
        let mut raft = RaftCellsConfig { query_origins, command_origins, shared,
            infrastructure:NodeRaftConfig { log:ReplicaLogOptions::new(node.info().data_dir.join("raft"),peer.node,peer.disk,peer.process,LogOpenMode::Create),
                consensus:RaftRuntimeOptions { tick_interval_ms:Some(100),..Default::default() },transport:TransportOptions::default() },
            catalog:raft_setup::catalog_options(),namespaces:vec![namespace(&layout,schema)?],
            groups:layout.groups.iter().map(|group|RaftGroupAssignment{history:group.history,placement:RaftGroupPlacement::Replica}).collect(), peers,host };
        raft.infrastructure.log.max_groups=3;raft.infrastructure.consensus.max_groups=3;
        let service = node.start_raft_cells(raft,RaftCellsServices {recovery:None,listeners:bind_raft_listeners(&peer).await?,tls,store,application,
            versions:VersionKeys::new(VersionSigningKey::new([29;32],u64::MAX)?,vec![],Duration::from_secs(86400))? }).await?;
        raft_setup::enroll(&service,&layout,Placement::new(3)?,i,Duration::from_secs(180)).await?;
        let balancing = service.clone(); let balancing_layout = layout.clone();
        let observed = service.clone(); let observed_layout = layout.clone();
        let token = std::env::var("CELLAR_PERF_TOKEN")?;
        let balance_token = token.clone();
        let router = axum::Router::new().route("/balance/{group}", axum::routing::post(move |axum::extract::Path(ordinal):axum::extract::Path<usize>, headers:axum::http::HeaderMap| {
            let service=balancing.clone();let layout=balancing_layout.clone();let token=balance_token.clone();
            async move {
                if headers.get("authorization").and_then(|v|v.to_str().ok())!=Some(format!("Bearer {token}").as_str()) {
                    return (axum::http::StatusCode::UNAUTHORIZED,axum::Json(serde_json::json!({"error":"unauthorized"})));
                }
                let result:Result<()> = async {
                    let selected=layout.groups.get(ordinal).context("group outside population")?;
                    let group=service.group(selected.history)?;
                    let observed=group.observe(Duration::from_secs(5)).await?;
                    let target=MemberId::new((ordinal % 3 + 1) as u64)?;
                    ensure!(observed.status.ready && observed.status.leader==Some(u64::from(i)),"only ready local leader can balance");
                    if observed.status.leader!=Some(target.get()) {
                        group.transfer_leadership(LeadershipTransfer{target,expected_term:observed.status.term,expected_revision:observed.membership_revision},Duration::from_secs(5)).await?;
                    }
                    Ok(())
                }.await;
                match result {Ok(())=>(axum::http::StatusCode::OK,axum::Json(serde_json::json!(true))),Err(error)=>(axum::http::StatusCode::SERVICE_UNAVAILABLE,axum::Json(serde_json::json!({"error":format!("{error:#}")})))}
            }
        })).route("/state",axum::routing::get(move |headers:axum::http::HeaderMap| {
            let service=observed.clone();let layout=observed_layout.clone();let token=token.clone();let profile=profile.clone();
            async move {
                if headers.get("authorization").and_then(|v|v.to_str().ok())!=Some(format!("Bearer {token}").as_str()) {
                    return (axum::http::StatusCode::UNAUTHORIZED,axum::Json(serde_json::json!({"error":"unauthorized"})));
                }
                let result:Result<_> = async {
                    let mut groups=Vec::new();
                    for group in &layout.groups {
                        let status=service.group(group.history)?.observe(Duration::from_secs(5)).await?;
                        groups.push(serde_json::json!({"leader":status.status.leader,"ready":status.status.ready,"cells":status.cells,"term":status.status.term.get()}));
                    }
                    Ok(serde_json::json!({"node":i,"profile":profile,"groups":groups,"resources":resource_snapshot(&service).await?,"performance":service.performance()}))
                }.await;
                match result {Ok(value)=>(axum::http::StatusCode::OK,axum::Json(value)),Err(error)=>(axum::http::StatusCode::SERVICE_UNAVAILABLE,axum::Json(serde_json::json!({"error":format!("{error:#}")})))}
            }
        }));
        let listener=tokio::net::TcpListener::bind("0.0.0.0:8082").await?;
        let admin=tokio::spawn(async move {axum::serve(listener,router).await});
        cellar::cli_output::Output::new(cellar::cli_output::Format::Json).line(format_args!("{}",serde_json::json!({"event":"primitive_raft_ready","node":i})))?;
        Ok::<_, anyhow::Error>((service, admin))
    }.await;
    let (service, admin) = match result {
        Ok(owners) => owners,
        Err(error) => {
            node.shutdown(ShutdownMode::Preserve).await?;
            return Err(error);
        }
    };
    let outcome = node
        .run_until(async {
            cellar::asyncrt::select! {_=term.recv()=>{},_=tokio::signal::ctrl_c()=>{}}
            ShutdownMode::Preserve
        })
        .await;
    admin.abort();
    let _ = admin.await;
    let report = service
        .close()
        .await
        .map_err(|error| anyhow::anyhow!("{error:?}"))?;
    let outcome = outcome?;
    ensure!(
        matches!(outcome, NodeOutcome::Stopped(_)),
        "primitive node stopped: {outcome:?}"
    );
    ensure!(
        report.groups.iter().all(|g| !g.failed),
        "failed group at close"
    );
    Ok(())
}
