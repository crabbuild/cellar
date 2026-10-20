//! Public embedded-node fixtures for registered Raft command serving.
#![allow(clippy::disallowed_methods)]
#[allow(dead_code)]
#[path = "html.rs"]
mod graph;
use anyhow::Result;
use cellar::sdk::consensus::*;
use cellar::sdk::*;
use cellar_logic::consensus::{DiskLineage, NodeId, ProcessGeneration};
use object_store::ObjectStore;
use std::{path::Path, sync::Arc, time::Duration};

pub async fn node(directory: &Path, application: Arc<dyn Application>) -> Result<Node> {
    node_named(directory, application, "raft-host").await
}

pub async fn node_named(
    directory: &Path,
    application: Arc<dyn Application>,
    label: &str,
) -> Result<Node> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let internal_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let probe_signer = Arc::new(PeerProbeSigner::generate());
    let config = NodeConfig::new(
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
    let services = NodeServices {
        wall_clock: None,
        http_client: None,
        host_executor: tokio::runtime::Handle::current(),
        core_executor: tokio::runtime::Handle::current(),
        listener,
        internal_listener,
        peer_http: reqwest::Client::builder().no_proxy().build()?,
        peer_auth: Arc::new(PeerAuth::new([11; 32], label)?),
        probe_signer,
        fleet: None,
        containers: None,
        metrics: None,
    };
    let compiled =
        CompiledApplications::default().register(label, "test", move || Ok(application.clone()))?;
    Node::start(config, services, graph::graph(label, "", vec![]), compiled).await
}

pub fn tls() -> Result<RaftTls> {
    Ok(RaftTls::new(
        vec![include_bytes!("../../transport/tcp/fixtures/ca.der")
            .to_vec()
            .into()],
        vec![include_bytes!("../../transport/tcp/fixtures/server.der")
            .to_vec()
            .into()],
        rustls::pki_types::PrivatePkcs8KeyDer::from(
            include_bytes!("../../transport/tcp/fixtures/key.der").to_vec(),
        )
        .into(),
    )?)
}

pub async fn cells(
    node: &Node,
    application: Arc<dyn Application>,
    store: Arc<dyn ObjectStore>,
    registration: RaftCellRegistration,
    recover: bool,
) -> Result<RaftCells> {
    let tls = tls()?;
    node.start_raft_cells(
        RaftCellsConfig {
            infrastructure: NodeRaftConfig {
                log: ReplicaLogOptions::new(
                    node.info().data_dir.join("raft"),
                    NodeId::new([1; 16])?,
                    DiskLineage::new([10; 16])?,
                    ProcessGeneration::new([if recover { 12 } else { 11 }; 16])?,
                    if recover {
                        LogOpenMode::Resume
                    } else {
                        LogOpenMode::Create
                    },
                ),
                consensus: RaftRuntimeOptions {
                    tick_interval_ms: Some(10),
                    ..Default::default()
                },
                transport: TransportOptions::default(),
            },
            catalog: CatalogOptions {
                prefix: "host/test".into(),
                ..Default::default()
            },
            cells: vec![registration],
            peers: vec![],
            host: RaftHostOptions::default(),
        },
        RaftCellsServices {
            listeners: NodeRaftListeners {
                control: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
                data: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
            },
            tls,
            store,
            application,
        },
    )
    .await
}

pub async fn ready(cell: &RaftCellHandle) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if cell.status().await.is_ok_and(|status| status.command_ready) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    Ok(())
}

pub fn options(sequence: u64) -> Result<CommandOptions> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis() as u64;
    Ok(CommandOptions::new(
        CommandId::new(ClientId::new([7; 16])?, sequence)?,
        now + 60_000,
    ))
}

pub struct Gate {
    open: std::sync::Mutex<bool>,
    changed: std::sync::Condvar,
    entered: tokio::sync::mpsc::UnboundedSender<()>,
}

impl Gate {
    pub fn new() -> (Arc<Self>, tokio::sync::mpsc::UnboundedReceiver<()>) {
        let (entered, receive) = tokio::sync::mpsc::unbounded_channel();
        (
            Arc::new(Self {
                open: std::sync::Mutex::new(false),
                changed: std::sync::Condvar::new(),
                entered,
            }),
            receive,
        )
    }

    fn wait(&self) -> Result<()> {
        self.entered.send(())?;
        let mut open = self.open.lock().unwrap_or_else(|e| e.into_inner());
        while !*open {
            open = self.changed.wait(open).unwrap_or_else(|e| e.into_inner());
        }
        Ok(())
    }

    pub fn release(&self) {
        *self.open.lock().unwrap_or_else(|e| e.into_inner()) = true;
        self.changed.notify_all();
    }
}

pub struct Release(pub Arc<Gate>);

impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}

pub struct Counter {
    pub application: Arc<dyn Application>,
    pub schema: [u8; 32],
    pub increment: OperationContract,
    pub get: OperationContract,
}

pub fn counter(calls: Arc<std::sync::atomic::AtomicUsize>, gate: Arc<Gate>) -> Result<Counter> {
    let class=RaftClass::new("Counter",1,[21;32])?
        .command("Increment",[22;32],move |context,input|{
            calls.fetch_add(1,std::sync::atomic::Ordering::SeqCst);
            let delta=i64::from_be_bytes(input.try_into()?);
            context.sql().execute_batch("CREATE TABLE IF NOT EXISTS counter(value INTEGER); INSERT INTO counter SELECT 0 WHERE NOT EXISTS(SELECT 1 FROM counter)")?;
            context.sql().execute("UPDATE counter SET value=value+?1",[delta])?;
            if context.command().sequence()==2 {gate.wait()?;}
            let value:i64=context.sql().query_row("SELECT value FROM counter",[],|row|row.get(0))?;
            Ok(value.to_be_bytes().to_vec())
        })?
        .query("Get",[23;32],|context,_|{
            let value:i64=context.query_row("SELECT value FROM counter",[],|row|row.get(0))?;
            Ok(value.to_be_bytes().to_vec())
        })?;
    let schema = class.schema_digest();
    let increment = class.contract("Increment").unwrap();
    let get = class.contract("Get").unwrap();
    let application =
        Arc::new(Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_class(class)?);
    Ok(Counter {
        application,
        schema,
        increment,
        get,
    })
}
