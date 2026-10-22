//! Isolated one-process-per-container native multi-Raft measurement node.
#![allow(clippy::disallowed_methods)]
#[path = "../../examples/raft-counter/counter.rs"]
mod counter;
mod raft_workload;
mod reference_workload;
use anyhow::{ensure, Context, Result};
use cellar::bucket::{Bucket, StorageBackend};
use cellar::engine_api::{Compat, WorkerConfigOptions};
use cellar::fleet::LoadedDeployment;
use cellar::sdk::{consensus::*, *};
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use std::{path::PathBuf, sync::Arc, time::Duration};

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn graph(script: &str) -> DeploymentGraph {
    DeploymentGraph::single(LoadedDeployment {
        event_streams: vec![],
        native: None,
        script_name: script.into(),
        version: "example".into(),
        prefix: "raft-counter".into(),
        options: WorkerConfigOptions {
            src: String::new(),
            script_name: script.into(),
            do_classes: vec![],
            bindings: vec![],
            blob_bindings: vec![],
            sql_bindings: vec![],
            kv_bindings: vec![],
            queue_bindings: vec![],
            stream_bindings: vec![],
            queue_consumers: vec![],
            workflow_bindings: vec![],
            vars: vec![],
            node: script.into(),
            modules: vec![],
            compat: Compat::default(),
        },
        asset_binding: None,
        loader_bindings: vec![],
        assets: None,
        services: vec![],
        crons: vec![],
        cron_revision: None,
        containers: vec![],
        fence_image: None,
        container_images: vec![],
    })
}

#[derive(Clone, Copy)]
struct Placement {
    nodes: u8,
}

impl Placement {
    fn new(nodes: u8) -> Result<Self> {
        ensure!(
            (3..=5).contains(&nodes),
            "local topology requires three to five nodes"
        );
        Ok(Self { nodes })
    }

    fn voters(self, cell: usize) -> [u8; 3] {
        let first = cell % usize::from(self.nodes);
        std::array::from_fn(|offset| ((first + offset) % usize::from(self.nodes) + 1) as u8)
    }

    fn local_count(self, total: usize, node: u8) -> Result<usize> {
        ensure!((1..=self.nodes).contains(&node), "invalid local node");
        let nodes = usize::from(self.nodes);
        Ok((0..3)
            .map(|offset| {
                let leader = (usize::from(node) - 1 + nodes - offset) % nodes;
                total / nodes + usize::from(leader < total % nodes)
            })
            .sum())
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 1)]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_ansi(false)
        .init();
    let i: u8 = std::env::var("RAFT_NODE")?.parse()?;
    let total: usize = std::env::var("RAFT_CELLS")?.parse()?;
    let tick_ms: u64 = std::env::var("RAFT_TICK_MS")
        .unwrap_or_else(|_| "100".into())
        .parse()?;
    let placement = Placement::new(
        std::env::var("RAFT_NODES")
            .unwrap_or_else(|_| "3".into())
            .parse()?,
    )?;
    ensure!((1..=1_000_000).contains(&total), "invalid cell count");
    let local_count = placement.local_count(total, i)?;
    ensure!(
        (1..=4096).contains(&local_count),
        "invalid local replica count"
    );
    let queue_items: usize = std::env::var("RAFT_QUEUE_ITEMS")
        .unwrap_or_else(|_| "256".into())
        .parse()?;
    let workload = match std::env::var("RAFT_WORKLOAD").as_deref() {
        Ok("reference") => true,
        Ok("counter") | Err(_) => false,
        _ => anyhow::bail!("invalid workload"),
    };
    let durability = match std::env::var("RAFT_DURABILITY").as_deref() {
        Ok("s3") => Durability::S3,
        Ok("quorum") | Err(_) => Durability::Quorum,
        _ => anyhow::bail!("invalid durability"),
    };
    ensure!(
        workload || durability == Durability::Quorum,
        "counter fixture requires quorum durability"
    );
    let network = std::env::var("RAFT_NETWORK_PREFIX").unwrap_or_else(|_| "172.29.81".into());
    let address: std::net::Ipv4Addr = format!("{network}.1").parse()?;
    ensure!(address.is_private(), "private workload network required");
    let primary_only = std::env::var("RAFT_PRIMARY_ONLY").as_deref() == Ok("1");
    ensure!(
        !primary_only || placement.nodes == 3,
        "primary-only fixture requires three nodes"
    );
    let root = PathBuf::from("/data");
    let secret = std::fs::read_to_string("/config/token")?.trim().to_owned();
    let tls = RaftTls::new(
        vec![std::fs::read("/config/ca.der")?.into()],
        vec![std::fs::read("/config/server.der")?.into()],
        rustls::pki_types::PrivateKeyDer::try_from(std::fs::read("/config/key.der")?)
            .map_err(|e| anyhow::anyhow!(e))?,
    )?;
    let builder = AmazonS3Builder::new()
        .with_bucket_name("raft-perf")
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
        "raft-perf".into(),
        String::new(),
    )?;
    let store = bucket.raft_store();
    let namespace = NamespaceId::new([2; 16])?;
    let cluster = ClusterId::new([1; 16])?;
    let label = format!("raft-node{i}");
    let class = if workload {
        raft_workload::definition()?
    } else {
        counter::definition()?
    };
    let schema = class.schema_digest();
    let class_name = if workload { "Workload" } else { "Counter" };
    let reference = if workload {
        Some(raft_workload::Api::new(
            secret.clone(),
            namespace,
            &class,
            durability,
        )?)
    } else {
        None
    };
    let api = if workload {
        None
    } else {
        Some(counter::CounterApi::new(secret.clone(), namespace, &class)?)
    };
    let application = match &api {
        Some(api) => counter::application(api.clone(), class)?,
        None => raft_workload::application(class)?,
    };
    let public = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
    let internal = tokio::net::TcpListener::bind("0.0.0.0:8081").await?;
    let signer = Arc::new(PeerProbeSigner::generate());
    let config = NodeConfig::new(
        ActorIdentity {
            node: label.clone(),
            advertise: format!("{network}.{}:8081", i + 10),
            region: "local-demo".into(),
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
                operation_deadline_ms: Some(5000),
                owner_log_recovery_backoff_ms: 100,
                owner_log_recovery_attempts: 3,
                alarm_resident_ms: 100,
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
    let app = application.clone();
    let node = Node::start(
        config,
        NodeServices {
            wall_clock: None,
            http_client: None,
            host_executor: tokio::runtime::Handle::current(),
            core_executor: tokio::runtime::Handle::current(),
            listener: public,
            internal_listener: internal,
            peer_http: reqwest::Client::builder().no_proxy().build()?,
            peer_auth: Arc::new(PeerAuth::new([19; 32], &label)?),
            probe_signer: signer,
            fleet: None,
            containers: None,
            metrics: None,
        },
        graph(&label),
        CompiledApplications::default().register(&label, "example", move || Ok(app.clone()))?,
    )
    .await?;
    let mut peers = Vec::new();
    for n in 1..=placement.nodes {
        let address = |port| format!("{network}.{}:{port}", n + 10).parse();
        peers.push(RaftPeerNode {
            node: NodeId::new([n; 16])?,
            disk: DiskLineage::new([n + 10; 16])?,
            process: ProcessGeneration::new(
                std::fs::read(format!("/config/process{n}"))?
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("process identity length"))?,
            )?,
            label: format!("raft-node{n}"),
            server_name: "localhost".into(),
            control_address: address(9100)?,
            data_address: address(9101)?,
            query_address: address(9102)?,
        });
    }
    // Fresh process identity on every restart; peers authenticate physical routes.
    // The runner persists the peer directory per run and replaces all processes together.
    let mut config = NamespaceConfig::new(cluster, namespace, NamespaceMode::Raft, schema);
    config.voters = VoterPolicy::ThreeVoters;
    let cells = (0..total)
        .filter(|cell| placement.voters(*cell).contains(&i))
        .map(|c| -> Result<_> {
            let mut group = [5; 16];
            group[..8].copy_from_slice(&(c as u64 + 1).to_be_bytes());
            Ok(RaftCellRegistration {
                placement: RaftCellPlacement::Replica,
                namespace: config.clone(),
                class: class_name.into(),
                birth: BirthProposal {
                    scope: GroupScope {
                        cluster,
                        namespace,
                        cell: format!("{class_name}:{c}"),
                        group: GroupId::new(group)?,
                        incarnation: IncarnationId::new([4; 16])?,
                    },
                    voters: peers
                        .iter()
                        .enumerate()
                        .filter(|(p, _)| placement.voters(c).contains(&(*p as u8 + 1)))
                        .map(|(p, peer)| {
                            Ok(InitialMember {
                                node: peer.node,
                                member: MemberId::new(p as u64 + 1)?,
                            })
                        })
                        .collect::<Result<Vec<_>>>()?,
                    learners: vec![],
                },
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let log_dir = node.info().data_dir.join("raft");
    let mode = if log_dir.exists() {
        LogOpenMode::Resume
    } else {
        LogOpenMode::Create
    };
    let peer = &peers[i as usize - 1];
    let mut host = RaftHostOptions {
        max_groups: local_count,
        max_requests: 256,
        ..Default::default()
    };
    host.close_report.max_groups = local_count;
    let service = node
        .start_raft_cells(
            RaftCellsConfig {
                query_origins: vec![],
                infrastructure: NodeRaftConfig {
                    log: ReplicaLogOptions::new(log_dir, peer.node, peer.disk, peer.process, mode),
                    consensus: RaftRuntimeOptions {
                        tick_interval_ms: Some(tick_ms),
                        max_requests: queue_items,
                        max_events: queue_items,
                        ..Default::default()
                    },
                    transport: TransportOptions {
                        queue_items,
                        peer_control_bytes: 16 * 1024 * 1024,
                        node_control_bytes: 32 * 1024 * 1024,
                        ..Default::default()
                    },
                },
                catalog: CatalogOptions {
                    prefix: "perf/multi-raft".into(),
                    ..Default::default()
                },
                cells,
                peers,
                host,
            },
            RaftCellsServices {
                listeners: NodeRaftListeners {
                    control: tokio::net::TcpListener::bind("0.0.0.0:9100").await?,
                    data: tokio::net::TcpListener::bind("0.0.0.0:9101").await?,
                    query: tokio::net::TcpListener::bind("0.0.0.0:9102").await?,
                },
                tls,
                store,
                application,
                versions: VersionKeys::new(
                    VersionSigningKey::new([29; 32], u64::MAX)?,
                    vec![],
                    Duration::from_secs(86400),
                )?,
            },
        )
        .await?;
    let router = if let Some(api) = api {
        *api.cells
            .lock()
            .map_err(|_| anyhow::anyhow!("poisoned binding"))? = Some(service.clone());
        counter::router(api)
    } else {
        let api = reference.ok_or_else(|| anyhow::anyhow!("missing workload API"))?;
        *api.cells
            .lock()
            .map_err(|_| anyhow::anyhow!("poisoned binding"))? = Some(service.clone());
        raft_workload::router(api)
    };
    let metrics = service.clone();
    let metrics_secret = secret.clone();
    let router = router.route("/metrics", axum::routing::get(move |headers: axum::http::HeaderMap| {
        let metrics = metrics.clone();
        let secret = metrics_secret.clone();
        async move {
            if headers.get("authorization").and_then(|value| value.to_str().ok()) != Some(format!("Bearer {secret}").as_str()) {
                return (axum::http::StatusCode::UNAUTHORIZED, axum::Json(serde_json::json!({"error":"unauthorized"})));
            }
            (axum::http::StatusCode::OK, axum::Json(serde_json::json!({"resources":metrics.resource_usage(), "performance":metrics.performance()})))
        }
    }));
    let admin = service.clone();
    let router = router.route("/admin/{id}", axum::routing::any(
        move |axum::extract::Path(id): axum::extract::Path<usize>, request: axum::extract::Request| {
            let service = admin.clone(); let secret = secret.clone();
            async move {
                let authorized = request.headers().get("authorization").and_then(|v| v.to_str().ok()) == Some(format!("Bearer {secret}").as_str());
                let transfer = request.method() == axum::http::Method::POST;
                drop(request);
                let result = async {
                    ensure!(authorized, "unauthorized");
                    ensure!(id < total, "invalid cell");
                    let cell = match service.cell(namespace, &format!("{class_name}:{id}")) {
                        Ok(cell) => cell,
                        Err(error) => {
                            ensure!(!transfer && !service.is_closed(), "unavailable registration: {error:?}");
                            return Ok(serde_json::json!({"cell":id, "node":i, "registered":false}));
                        }
                    };
                    let status = cell.status().await.map_err(|e| anyhow::anyhow!("{e:?}"))?;
                    if transfer && status.command_ready {
                        let target = MemberId::new(if primary_only { 1 } else { u64::from(placement.voters(id)[0]) })?;
                        if status.leader != Some(target) {
                            cell.transfer_leadership(LeadershipTransfer {target,
                                expected_term: status.term, expected_revision: status.membership_revision}, Duration::from_secs(5))
                                .await.map_err(|e| anyhow::anyhow!("{e:?}"))?;
                        }
                    }
                    let voters = if status.role == RaftReplicaRole::Leader && !transfer {
                        let progress = cell.replication_status().await.map_err(|e| anyhow::anyhow!("{e:?}"))?;
                        ensure!(progress.term == status.term && progress.membership_revision == status.membership_revision
                            && progress.replica.member.get() == u64::from(i), "changed replication observation");
                        ensure!(progress.members.iter().all(|member| member.voter), "unexpected learner");
                        let mut voters: Vec<_> = progress.members.iter().map(|member| member.member.get()).collect();
                        voters.sort_unstable();
                        Some(voters)
                    } else { None };
                    Ok::<_,anyhow::Error>(serde_json::json!({"cell":id, "node":i, "registered":true, "voters":voters, "leader":status.leader.map(MemberId::get),
                        "ready": status.command_ready, "role":format!("{:?}",status.role),
                        "term":status.term.get(), "committed":status.committed.get(), "applied":status.applied.index.get(), "archived":status.archived.get()}))
                }.await;
                match result { Ok(v) => (axum::http::StatusCode::OK, v.to_string()),
                    Err(e) => (axum::http::StatusCode::SERVICE_UNAVAILABLE, e.to_string()) }
            }
        }));
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8082").await?;
    cellar::note!(
        "raft_perf_ready node={} groups={} tick_ms={} queue_items={} durability={:?} archive=rustfs",
        i,
        local_count,
        tick_ms,
        queue_items,
        durability
    );
    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            let mut term =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("signal");
            cellar::asyncrt::select! { _ = tokio::signal::ctrl_c() => {}, _ = term.recv() => {} }
        })
        .await?;
    node.shutdown(ShutdownMode::Preserve)
        .await
        .context("node shutdown")?;
    let report = service
        .close()
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))?;
    cellar::note!("raft_perf_closed {:?}", report);
    Ok(())
}

#[cfg(test)]
mod placement_tests {
    use super::*;

    #[test]
    fn three_voters_are_balanced_without_registering_other_nodes() -> Result<()> {
        for nodes in 3..=5 {
            let placement = Placement::new(nodes)?;
            for total in [1, 3, 5, 17, 4096, 6825] {
                let mut actual = vec![0; usize::from(nodes)];
                for cell in 0..total {
                    let voters = placement.voters(cell);
                    assert_eq!(voters[0], (cell % usize::from(nodes) + 1) as u8);
                    assert_ne!(voters[0], voters[1]);
                    assert_ne!(voters[0], voters[2]);
                    assert_ne!(voters[1], voters[2]);
                    for voter in voters {
                        actual[usize::from(voter) - 1] += 1;
                    }
                }
                for node in 1..=nodes {
                    assert_eq!(
                        placement.local_count(total, node)?,
                        actual[usize::from(node) - 1]
                    );
                }
                assert_eq!(actual.iter().sum::<usize>(), total * 3);
            }
        }
        let five = Placement::new(5)?;
        assert_eq!(five.voters(4), [5, 1, 2]);
        assert_eq!(five.local_count(15, 5)?, 9);
        assert!(five.local_count(15, 0).is_err());
        assert!(five.local_count(15, 6).is_err());
        assert!(Placement::new(2).is_err());
        assert!(Placement::new(6).is_err());
        Ok(())
    }
}
