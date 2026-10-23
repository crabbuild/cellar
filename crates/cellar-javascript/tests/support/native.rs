#![allow(clippy::disallowed_methods)]
use anyhow::Result;
use cellar::sdk::*;
use std::{path::Path, sync::Arc, time::Duration};
#[path = "../../../cellar/tests/support/html.rs"]
#[allow(dead_code)]
mod fixture;

pub async fn start(path: &Path, application: Arc<dyn Application>) -> Result<(Node, String)> {
    let name = "javascript-test";
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
            operation_deadline_ms: Some(5_000),
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
    config.maintenance.rebalance = false;
    config.runtime.handler_budget = Duration::from_millis(250);
    let executor = tokio::runtime::Handle::current();
    let peer_http = reqwest::Client::new();
    let peer_auth = Arc::new(PeerAuth::new([31; 32], name)?);
    let services = NodeServices {
        wall_clock: None,
        http_client: None,
        host_executor: executor.clone(),
        core_executor: executor,
        listener,
        internal_listener,
        peer_http,
        peer_auth,
        probe_signer: signer,
        fleet: None,
        containers: None,
        metrics: None,
    };
    let mut graph = fixture::graph("javascript-test", "", vec!["Counter".into()]);
    graph.primary.options.bindings = vec![("COUNTERS".into(), "Counter".into())];
    graph.primary.options.vars = vec![("DOMAIN_MARKER".into(), name.into())];
    let applications =
        CompiledApplications::default()
            .register("javascript-test", "test", move || Ok(application.clone()))?;
    Ok((
        Node::start(config, services, graph, applications).await?,
        format!("http://{address}"),
    ))
}
