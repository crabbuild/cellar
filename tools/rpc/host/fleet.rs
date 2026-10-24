// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real authenticated fleet inputs for the all-language RPC qualification.
use anyhow::{ensure, Context as _, Result};
use cellar::{bucket::Bucket, sdk::*};
use std::{path::Path, sync::Arc, time::Duration};

use super::native::fixture as deployment;

pub const CLASSES: [(&str, &str, &str); 4] = [
    ("rust", "RUST", "RustCounter"),
    ("python", "PYTHON", "PythonCounter"),
    ("v8", "V8", "V8Counter"),
    ("node", "NODE", "NodeCounter"),
];

pub async fn start(
    path: &Path,
    application: Arc<dyn Application>,
    name: &str,
    bucket: Bucket,
) -> Result<(Node, String)> {
    start_controlled(path, application, name, bucket, None).await
}

pub async fn start_controlled(
    path: &Path,
    application: Arc<dyn Application>,
    name: &str,
    bucket: Bucket,
    controls: Option<super::proof::Controls>,
) -> Result<(Node, String)> {
    start_configured(path, application, name, bucket, controls, |_, _| Ok(())).await
}

pub async fn start_configured(
    path: &Path,
    application: Arc<dyn Application>,
    name: &str,
    bucket: Bucket,
    controls: Option<super::proof::Controls>,
    configure: impl FnOnce(&mut NodeConfig, &mut NodeServices) -> Result<()>,
) -> Result<(Node, String)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let internal_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let internal = internal_listener.local_addr()?;
    let signer = Arc::new(PeerProbeSigner::generate());
    let actor = ActorOptions {
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
            operation_deadline_ms: Some(10_000),
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
            node: name.into(),
            region: "test".into(),
            advertise: internal.to_string(),
        },
        actor,
        path.to_path_buf(),
    );
    config.runtime.handler_budget = Duration::from_secs(15);
    config.startup.bootstrap_timeout = Duration::from_secs(20);
    config.startup.recovery_attempt_timeout = Duration::from_secs(4);
    config.startup.authority_timeout = Duration::from_secs(4);
    config.maintenance.rebalance = false;
    config.maintenance.fleet_sample_period = Duration::from_millis(50);
    config.maintenance.load_sample_period = Duration::from_millis(50);
    config.shutdown = ShutdownPolicy {
        total_ms: 8_000,
        token_wait_ms: 0,
        no_progress_ms: 5_000,
    };
    let peer_http = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(2))
        .tcp_nodelay(true)
        .build()?;
    let peer_auth = Arc::new(PeerAuth::new([31; 32], name)?);
    let mut fleet = NodeFleet {
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
            durability_timeout: Duration::from_secs(5),
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
    if let Some(controls) = controls {
        controls.install(&mut fleet)?;
    }
    let executor = tokio::runtime::Handle::current();
    let mut services = NodeServices {
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
    };
    let mut graph = deployment::graph(
        "rpc-qualification",
        "",
        CLASSES
            .iter()
            .map(|(_, _, class)| (*class).into())
            .collect(),
    );
    graph.primary.options.bindings = CLASSES
        .iter()
        .map(|(_, binding, class)| ((*binding).into(), (*class).into()))
        .collect();
    graph.primary.options.vars = vec![("DOMAIN_MARKER".into(), name.into())];
    let applications =
        CompiledApplications::default()
            .register("rpc-qualification", "test", move || Ok(application.clone()))?;
    configure(&mut config, &mut services)?;
    let node = Node::start(config, services, graph, applications)
        .await
        .with_context(|| format!("start fleet RPC node {name}"))?;
    tokio::time::timeout(Duration::from_secs(20), async {
        while !node
            .is_ready()
            .await
            .with_context(|| format!("poll fleet RPC node {name} readiness"))?
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .with_context(|| format!("fleet RPC node {name} readiness timed out"))??;
    Ok((node, format!("http://{address}")))
}

pub async fn stop(node: Node, mode: ShutdownMode) -> Result<()> {
    let label = format!("shutdown fleet RPC node {}", node.info().node);
    let outcome = node.shutdown(mode).await.context(label)?;
    let NodeOutcome::Stopped(proof) = outcome else {
        anyhow::bail!("fleet RPC node failed: {outcome:?}");
    };
    ensure!(
        proof.ingress_drained
            && proof.application_work_drained
            && proof.cells_drained
            && !proof.drain_timed_out,
        "fleet RPC node did not drain"
    );
    Ok(())
}
