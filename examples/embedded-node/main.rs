//! A standalone embedding application: caller-owned runtimes, clients and stop policy.
use anyhow::{ensure, Result};
use cellar::engine_api::{Compat, WorkerConfigOptions};
use cellar::fleet::LoadedDeployment;
use cellar::sdk::{
    ActorIdentity, ActorOptions, ActorPolicy, Cell, CompiledApplications, Context, DeploymentGraph,
    HandlerFuture, Invocation, Node, NodeConfig, NodeOutcome, NodeServices, Output,
    OwnershipOnEvict, PeerAuth, PeerProbeSigner, Registry, ShutdownMode,
};
use std::{path::PathBuf, sync::Arc, time::Duration};
struct Counter;
impl Cell for Counter {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = event else {
                return Ok(Output::Done);
            };
            let storage = context.storage()?;
            let mut count = storage
                .get_json("count")?
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            if request.method == "POST" {
                count = count
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("counter overflow"))?;
                storage.put_json("count", &serde_json::json!(count))?;
            }
            Ok(Output::Http(
                cellar::sdk::http::Response::from_json(&serde_json::json!({"count":count}))?
                    .into_http(&context)?,
            ))
        })
    }
}

fn graph() -> DeploymentGraph {
    DeploymentGraph::single(LoadedDeployment {
        native: None,
        script_name: "embedded-example".into(),
        version: "v1".into(),
        prefix: "embedded-example/v1".into(),
        options: WorkerConfigOptions {
            src: String::new(),
            script_name: "embedded-example".into(),
            do_classes: vec!["Counter".into()],
            bindings: vec![("COUNTERS".into(), "Counter".into())],
            sql_bindings: Vec::new(),
            kv_bindings: Vec::new(),
            blob_bindings: Vec::new(),
            queue_bindings: Vec::new(),
            queue_consumers: Vec::new(),
            workflow_bindings: Vec::new(),
            vars: Vec::new(),
            node: String::new(),
            modules: Vec::new(),
            compat: Compat::default(),
        },
        asset_binding: None,
        loader_bindings: Vec::new(),
        assets: None,
        services: Vec::new(),
        crons: Vec::new(),
        containers: Vec::new(),
        fence_image: None,
        container_images: Vec::new(),
    })
}
fn main() -> Result<()> {
    let data = PathBuf::from(
        std::env::args()
            .nth(1)
            .unwrap_or_else(|| "examples/.cellar/embedded".into()),
    );
    // The application owns these executors and keeps them alive through the node's join.
    let host = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let core = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()?;
    host.block_on(async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:9876").await?;
        let internal_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let signer = Arc::new(PeerProbeSigner::generate());
        let mut config = NodeConfig::new(ActorIdentity {
            node:"embedded-example".into(), advertise:internal_listener.local_addr()?.to_string(), region:"local".into(),
        }, ActorOptions {
            core:ActorPolicy {
                max_resident:128,max_cached_routes:256,max_activations:16,max_evictions:4,max_releases:8,
                max_outbound_websockets:16,ownership_on_evict:OwnershipOnEvict::Sticky,
                require_node_lease:true,peer_protocol:cellar::peer_auth::PROTOCOL_VERSION,
                operation_deadline_ms:Some(10000),owner_log_recovery_backoff_ms:1000,
                owner_log_recovery_attempts:10,alarm_resident_ms:1000,idle_evict_ms:None,pressure:Default::default(),
            },lease_ttl_ms:10000,placement_weight:1,generation:signer.public_key_hex(),resume_generation:None,
        }, data);
        config.runtime.max_stateless_requests = Some(128);
        config.runtime.max_cell_requests = 16;
        config.runtime.handler_budget = Duration::from_secs(10);
        config.maintenance.rebalance = false;
        let mut key = [0;32];
        getrandom::fill(&mut key)?;
        let services = NodeServices {
            http_client: None,
            host_executor:host.handle().clone(),core_executor:core.handle().clone(),listener,internal_listener,
            peer_http:reqwest::Client::builder().connect_timeout(Duration::from_secs(5)).build()?,
            peer_auth:Arc::new(PeerAuth::new(key,"embedded-example")?),probe_signer:signer,
            fleet:None,containers:None,metrics:None,
        };
        let applications = CompiledApplications::default().register("embedded-example","v1",|| {
            Ok(Arc::new(Registry::new(|context,event| Box::pin(async move {
                let Invocation::Fetch(request) = event else { return Ok(Output::Done); };
                let url = url::Url::parse(&request.url)?;
                let name = url.path().strip_prefix("/counters/").filter(|name| !name.is_empty());
                let Some(name) = name else {
                    return Ok(Output::Http(cellar::sdk::http::Response::new(404,"use /counters/NAME",Vec::new()).into_http(&context)?));
                };
                if !matches!(request.method.as_str(), "GET" | "POST") {
                    return Ok(Output::Http(cellar::sdk::http::Response::new(405,"use GET or POST",Vec::new()).into_http(&context)?));
                }
                Ok(Output::Http(context.namespace("COUNTERS")?.get_by_name(name).fetch(&context,request).await?))
            })).cell("Counter",|_,_| Ok(Arc::new(Counter)))?))
        })?;
        #[cfg(unix)]
        let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        let node = Node::start(config,services,graph(),applications).await?;
        cellar::note!("embedded example ready at http://127.0.0.1:9876");
        let outcome = node.run_until(async {
            #[cfg(unix)]
            cellar::asyncrt::select! { _=terminate.recv()=>{}, _=tokio::signal::ctrl_c()=>{} }
            #[cfg(not(unix))]
            let _ = tokio::signal::ctrl_c().await;
            ShutdownMode::Preserve
        }).await?;
        let NodeOutcome::Stopped(report) = outcome else { anyhow::bail!("node failed: {outcome:?}"); };
        ensure!(report.ingress_drained && report.application_work_drained && report.cells_drained
            && report.pending_dispatches == 0 && !report.drain_timed_out,"incomplete shutdown: {report:?}");
        cellar::cli_output::Output::new(cellar::cli_output::Format::Text).line(format_args!("{}",serde_json::json!({
            "outcome":"stopped","ingress_drained":report.ingress_drained,
            "application_work_drained":report.application_work_drained,"cells_drained":report.cells_drained,
        })))?;
        Ok(())
    })
}
