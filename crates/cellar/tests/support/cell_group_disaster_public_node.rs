// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Public recovered-node fixture with the ordinary Node lifecycle and limits.
use anyhow::Result;
use cellar::sdk::{consensus::*, *};
use std::{path::Path, sync::Arc, time::Duration};
#[path = "html.rs"]
mod graph;

pub(super) async fn node_named(
    directory: &Path,
    application: Arc<dyn Application>,
    label: &str,
) -> Result<Node> {
    let classes = vec![];
    let filesystem = Arc::new(cellar_ltx::DirectFileSystem);
    let shutdown = ShutdownPolicy::default();
    let bucket: Option<cellar::bucket::Bucket> = None;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let internal_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let probe_signer = Arc::new(PeerProbeSigner::generate());
    let mut config = NodeConfig::new(
        ActorIdentity {
            node: label.into(),
            advertise: internal_listener.local_addr()?.to_string(),
            region: "test".into(),
        },
        ActorOptions {
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
                operation_deadline_ms: Some(3000),
                owner_log_recovery_backoff_ms: 50,
                owner_log_recovery_attempts: 3,
                alarm_resident_ms: 100,
                idle_evict_ms: None,
                pressure: Default::default(),
            },
            lease_ttl_ms: 1000,
            placement_weight: 1,
            generation: probe_signer.public_key_hex(),
            resume_generation: None,
        },
        directory.into(),
    );
    config.shutdown = shutdown;
    let peer_http = reqwest::Client::builder().no_proxy().build()?;
    let peer_auth = Arc::new(PeerAuth::new([11; 32], label)?);
    let fleet = bucket
        .map(|bucket| -> Result<NodeFleet> {
            Ok(NodeFleet {
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
                log: LogOptions::default(),
                fleet_durability: true,
            })
        })
        .transpose()?;
    let services = NodeServices {
        wall_clock: None,
        http_client: None,
        host_executor: tokio::runtime::Handle::current(),
        core_executor: tokio::runtime::Handle::current(),
        listener,
        internal_listener,
        peer_http,
        peer_auth,
        probe_signer,
        fleet,
        containers: None,
        metrics: None,
    };
    let compiled =
        CompiledApplications::default().register(label, "test", move || Ok(application.clone()))?;
    Node::start_with_filesystem(
        config,
        services,
        graph::graph(label, "", classes),
        compiled,
        filesystem,
    )
    .await
}

pub(super) fn limits() -> CellGroupNodeLimits {
    CellGroupNodeLimits {
        namespaces: CellGroupHostLimits {
            max_namespaces: 1,
            namespace_bytes: 16 * 1024 * 1024,
        },
        controller: CellGroupControllerLimits {
            max_groups: 2,
            max_parallel_apply: 2,
            retry_interval_ms: 5,
            max_commands: 2,
            max_command_bytes: 64 * 1024,
            checkpoints: None,
        },
        sql: CellSqlLimits {
            workers: 2,
            queue_per_worker: 4,
            max_pending_cells: 8,
            group_pending_cells: 8,
            node_bytes: 512 * 1024 * 1024,
            group_bytes: 512 * 1024 * 1024,
        },
        provisioning: CellGroupProvisionLimits {
            max_preparations: 1,
            max_preparation_bytes: 512 * 1024 * 1024,
            max_parallel_preparations: 1,
            max_waiters: 2,
            peer_revision: 1,
        },
        directory: DirectoryLimits::default(),
        dispatch: CellDispatchLimits {
            max_cached_routes: 32,
            route_cache_bytes: 256 * 1024,
            max_invocations: 8,
            retained_bytes: 1024 * 1024,
            max_input_bytes: 1024,
        },
        manifest: ManifestLimits {
            max_cells: 32,
            max_depth: 16,
        },
        entries: Default::default(),
        manifest_pages: 1024,
        manifest_bytes: 16 * 1024 * 1024,
        retained_roots: 16,
        max_effect_rows: 100,
        checkpoints: None,
        archive: None,
    }
}
