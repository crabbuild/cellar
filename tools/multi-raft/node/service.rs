// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use axum::{
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use cellar::peer_auth::PeerAuth;
use serde::Deserialize;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::net::TcpListener;

pub(super) struct Application {
    pub class: Arc<RaftClass>,
    commands: Arc<AtomicUsize>,
    initialized: Arc<AtomicUsize>,
}

pub(super) fn application() -> Result<Application> {
    let commands = Arc::new(AtomicUsize::new(0));
    let initialized = Arc::new(AtomicUsize::new(0));
    let c = commands.clone();
    let i = initialized.clone();
    let class = Arc::new(RaftClass::new("DockerCounter", 1, [84;32])?
        .initializer([87;32], move |_,tx| { i.fetch_add(1,Ordering::AcqRel); tx.execute_batch("CREATE TABLE value(n); INSERT INTO value VALUES(0); CREATE TABLE padding(bytes); INSERT INTO padding VALUES(zeroblob(8192));")?; Ok(()) })?
        .command("Add",[85;32],move |context,input| {
            c.fetch_add(1,Ordering::AcqRel);
            ensure!(input == [1], "bounded increment input");
            context.sql().execute("UPDATE value SET n=n+1",[])?;
            let value:i64=context.sql().query_row("SELECT n FROM value",[],|r|r.get(0))?;
            Ok(serde_json::to_vec(&json!({"cell":context.identity().cell.as_str(),"incarnation":hex::encode(context.identity().incarnation.bytes()),"command":context.command().sequence(),"value":value}))?)
        })?
        .query("Value",[86;32],|context,_| { let value:i64=context.query_row("SELECT n FROM value",[],|r|r.get(0))?; Ok(serde_json::to_vec(&value)?) })?);
    Ok(Application {
        class,
        commands,
        initialized,
    })
}

struct Node {
    domain: Arc<ExecutionDomain>,
    config: Arc<Config>,
    generation: usize,
    ordinal: usize,
    data: PathBuf,
    app: Application,
    directory: RangeDirectory,
    birth: CellGroupBirth,
    log: ReplicaLog,
    runtime: CellGroupRuntime,
    sql: CellGroupSqlPool,
    controller: CellGroupController,
    images: CellGroupApplyImages,
}

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
enum Action {
    Status,
    Archive,
    Activate,
    Plan,
    Enroll {
        cell: String,
    },
    Command {
        cell: String,
        sequence: u64,
        input: Vec<u8>,
        retry_until_ms: u64,
        s3: bool,
    },
    Query {
        cell: String,
        token: Option<String>,
        #[serde(default)]
        stale: bool,
    },
}

pub(super) async fn run(
    domain: Arc<ExecutionDomain>,
    config: Arc<Config>,
    options: &Options,
) -> Result<()> {
    let app = application()?;
    let directory = config.directory(&domain, app.class.schema_digest()).await?;
    let request = directory.prepare_group_birth(config.spec(options.generation)?)?;
    let birth = directory.materialize_group_birth(&request).await?;
    let mode = if options.retained {
        LogOpenMode::Resume
    } else {
        LogOpenMode::Create
    };
    let log = ReplicaLog::open(
        config.log_options(options, mode)?,
        Arc::new(DirectFileSystem),
    )
    .await?;
    let permit = if options.generation == 0 {
        directory.bootstrap_group_member(&birth, &log).await?
    } else {
        directory
            .bootstrap_recovered_group_member(
                &directory.decode_group_recovery(&config.recovery)?,
                &log,
                Arc::new(recovery::fence_authority(&config)?),
            )
            .await?
    };
    let group = permit.group().clone();
    let base = options.data.join("state");
    let rooted = config.rooted;
    let retained = options.retained;
    let (applier, root, images) = domain
        .blocking(move || -> Result<_> {
            let physical = physical::Physical::open(&base, rooted)?;
            let images = physical.images.clone();
            let (applier, root) = if retained {
                physical.recover(group)?
            } else {
                physical.initial(group)?
            };
            Ok((applier, root, images))
        })?
        .await??;
    let (runtime, events) = CellGroupRuntime::start(
        log.clone(),
        RaftRuntimeOptions {
            tick_interval_ms: Some(100),
            election_seed: Some([17 + options.node as u8; 32]),
            ..Default::default()
        },
    )
    .await?;
    runtime.attach(permit, root).await?;
    let n = options.node as u16;
    let bind: std::net::Ipv4Addr = config.peers[options.generation][options.node]
        .host
        .parse()?;
    let transport = RaftTransport::start_shared(
        runtime.clone(),
        Arc::new(PeerAuth::new([17; 32], format!("node-{}", n + 1))?),
        tls()?,
        TcpListener::bind((bind, 7100 + n * 3)).await?,
        TcpListener::bind((bind, 7101 + n * 3)).await?,
        TcpListener::bind((bind, 7102 + n * 3)).await?,
        Default::default(),
    )
    .await?;
    transport.install_physical_group(cellar::application::consensus::verification::GroupRoute {
        scope: config::history(options.generation)?,
        schema_digest: app.class.schema_digest(),
        revision: 1,
        peers: (0..3)
            .map(|node| -> Result<_> {
                let p = &config.peers[options.generation][node];
                let n = node as u16;
                Ok(RaftPeer {
                    replica: config.replica(options.generation, node)?,
                    label: format!("node-{}", n + 1),
                    server_name: "localhost".into(),
                    control_address: format!("{}:{}", p.host, 7100 + n * 3).parse()?,
                    data_address: format!("{}:{}", p.host, 7101 + n * 3).parse()?,
                    query_address: format!("{}:{}", p.host, 7102 + n * 3).parse()?,
                })
            })
            .collect::<Result<_>>()?,
    })?;
    let sql = CellGroupSqlPool::start(
        &log,
        CellSqlLimits {
            workers: 1,
            queue_per_worker: 4,
            max_pending_cells: 8,
            group_pending_cells: 8,
            node_bytes: 128 * 1024 * 1024,
            group_bytes: 64 * 1024 * 1024,
        },
    )?;
    let controller = CellGroupController::start(
        runtime.clone(),
        events,
        transport,
        sql.clone(),
        vec![applier],
        CellGroupControllerLimits {
            max_groups: 1,
            max_parallel_apply: 1,
            retry_interval_ms: 5,
            max_commands: 2,
            max_command_bytes: 64 * 1024,
            checkpoints: None,
        },
    )
    .await?;
    let node = Arc::new(Node {
        domain: domain.clone(),
        config,
        generation: options.generation,
        ordinal: options.node,
        data: options.data.clone(),
        app,
        directory,
        birth,
        log,
        runtime,
        sql,
        controller,
        images,
    });
    let router = Router::new()
        .route("/health", get(|| async { Json(json!({"live":true})) }))
        .route("/control", post(control))
        .layer(DefaultBodyLimit::max(64 * 1024))
        .with_state(node.clone());
    let listener = TcpListener::bind(("0.0.0.0", 8080 + n)).await?;
    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            #[cfg(unix)]
            {
                if let Ok(mut signal) =
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                {
                    signal.recv().await;
                }
            }
            #[cfg(not(unix))]
            {
                let _ = tokio::signal::ctrl_c().await;
            }
        })
        .await?;
    node.controller.close().await?;
    node.log.close().await?;
    Ok(())
}

async fn control(
    State(node): State<Arc<Node>>,
    headers: HeaderMap,
    Json(action): Json<Action>,
) -> impl IntoResponse {
    if headers.get("authorization").and_then(|v| v.to_str().ok())
        != Some(format!("Bearer {}", node.config.token).as_str())
    {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error":"unauthorized"})),
        );
    }
    let result = node.domain.run(node.call(action)).await;
    match result {
        Ok(Ok(value)) => (StatusCode::OK, Json(value)),
        Ok(Err(error)) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error":format!("{error:#}")})),
        ),
        Err(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error":"domain closed"})),
        ),
    }
}

impl Node {
    fn dispatcher(&self, s3: bool) -> Result<CellGroupDispatcher> {
        let dispatcher = CellGroupDispatcher::new(
            &self.domain,
            self.directory.clone(),
            self.app.class.clone(),
            keys()?,
            CellDispatchLimits {
                max_cached_routes: 4,
                route_cache_bytes: 256 * 1024,
                max_invocations: 4,
                retained_bytes: 1024 * 1024,
                max_input_bytes: 1024,
            },
        )?;
        Ok(if s3 {
            dispatcher.with_s3(&self.controller)?
        } else {
            dispatcher
        })
    }

    fn options(&self, name: &str) -> Result<CellSqlOptions> {
        ensure!(
            ["alpha", "beta", "gamma"].contains(&name),
            "unknown synthetic cell"
        );
        physical::sql_options(
            self.images.clone(),
            self.data.join(format!("sql-{name}")),
            self.app.class.schema_digest(),
            self.generation == 0,
        )
    }

    async fn call(&self, action: Action) -> Result<Value> {
        let history = config::history(self.generation)?;
        match action {
            Action::Status => {
                let s = self.runtime.status(history).await?;
                Ok(
                    json!({"member":config::member(self.generation,self.ordinal)?.get(),"leader":s.leader,"ready":s.ready,"term":s.term.get(),"applied":s.applied.index.get(),"commands":self.app.commands.load(Ordering::Acquire),"initializations":self.app.initialized.load(Ordering::Acquire)}),
                )
            }
            Action::Archive => self.archive().await,
            Action::Activate => {
                ensure!(self.generation == 1, "target activation only");
                let request = self
                    .directory
                    .decode_group_recovery(&self.config.recovery)?;
                let authority = recovery::fence_authority(&self.config)?;
                let permit = self
                    .directory
                    .prepare_group_recovery_activation(&request, &self.log, &authority)
                    .await?;
                match self.runtime.activate_recovery(permit).await? {
                    CellGroupRecoveryActivation::Accepted(p) => {
                        Ok(json!({"activated":false,"accepted":p.position.index.get()}))
                    }
                    CellGroupRecoveryActivation::Applied(ack) => {
                        self.directory
                            .complete_group_recovery(&request, &ack, &authority)
                            .await?;
                        Ok(json!({"activated":true}))
                    }
                }
            }
            Action::Enroll { cell } => {
                let options = self.options(&cell)?;
                let invocation = self
                    .dispatcher(true)?
                    .enroll(
                        config::cell(self.generation, &cell)?,
                        CellEnrollmentRequest {
                            creation: ProposalId::new([100 + cell.as_bytes()[0]; 16])?,
                            durability: Durability::S3,
                        },
                        bounds(),
                        None,
                    )
                    .await?;
                let result = self
                    .controller
                    .enroll(invocation, options)?
                    .response()
                    .await?;
                Ok(json!({"enrolled":true,"s3":result.durability()==Durability::S3}))
            }
            Action::Command {
                cell,
                sequence,
                input,
                retry_until_ms,
                s3,
            } => {
                let invocation = self
                    .dispatcher(true)?
                    .command(
                        config::cell(self.generation, &cell)?,
                        CellCommandRequest {
                            id: CommandId::new(ClientId::new([188; 16])?, sequence)?,
                            contract: self.app.class.contract("Add").context("Add")?,
                            input,
                            durability: if s3 {
                                Durability::S3
                            } else {
                                Durability::Quorum
                            },
                            retry_until_ms,
                        },
                        bounds(),
                        None,
                    )
                    .await?;
                let result = self
                    .controller
                    .command(invocation, self.options(&cell)?)?
                    .response()
                    .await?;
                let local = result
                    .local_receipt()
                    .context("command oracle requires a local receipt")?;
                Ok(
                    json!({"result":serde_json::from_slice::<Value>(result.result())?,"s3":result.durability()==Durability::S3,"minimum_s3":local.receipt.minimum_durability()==Durability::S3,"version":local.version.sequence.get(),"token":result.token().as_str()}),
                )
            }
            Action::Query { cell, token, stale } => {
                ensure!(
                    !stale || token.is_none(),
                    "stale inspection cannot carry an AtLeast token"
                );
                let consistency = match token {
                    Some(token) => {
                        CellQueryConsistency::AtLeast(CellVersionToken::from_encoded(&token)?)
                    }
                    None if stale => CellQueryConsistency::Stale,
                    None => CellQueryConsistency::Linearizable,
                };
                let mut invocation = self.dispatcher(false)?.query(
                    config::cell(self.generation, &cell)?,
                    CellQueryRequest {
                        invocation: ProposalId::new([180; 16])?,
                        contract: self.app.class.contract("Value").context("Value")?,
                        input: vec![],
                        consistency,
                    },
                    bounds(),
                    None,
                )?;
                invocation.begin().await?;
                let response = invocation
                    .execute(&self.runtime, &self.sql, self.options(&cell)?)
                    .await?;
                Ok(
                    json!({"value":serde_json::from_slice::<Value>(response.result())?,"token":response.token().as_str(),"version":response.observation().version.sequence.get(),"incarnation":hex::encode(response.observation().version.history.incarnation.bytes())}),
                )
            }
            Action::Plan => self.plan().await,
        }
    }

    async fn archive(&self) -> Result<Value> {
        let history = config::history(self.generation)?;
        if self.controller.archive_status(history)?.is_some() {
            return Ok(json!({"registered":true}));
        }
        let base = self.data.join("archive-verifier");
        let rooted = self.config.rooted;
        let route = self.birth.route().clone();
        let store = self
            .domain
            .blocking(move || physical::Physical::open(&base, rooted)?.archive(route))?
            .await??;
        self.controller
            .archive(CellGroupArchiveRegistration {
                publisher: CellGroupArchivePublisher::LocalReplica,
                directory: self.directory.clone(),
                birth: self.birth.clone(),
                io: CellGroupArchivePool::new(Default::default())?,
                owners: CellGroupArchiveUploadPool::new(Default::default())?,
                tails: CellGroupArchiveTailPool::new(Default::default())?,
                checkpoints: physical::checkpoint_pool()?,
                store,
                scratch: physical::scratch(8)?,
                scratch_directory: self.data.clone(),
            })?
            .response()
            .await?;
        Ok(json!({"registered":true}))
    }

    async fn plan(&self) -> Result<Value> {
        ensure!(self.generation == 0, "source plan only");
        let path = self.data.join("log/cell-group-archive-journals");
        let hold = self
            .domain
            .blocking(move || -> Result<CellGroupArchiveHold> {
                let entries = std::fs::read_dir(path)?.collect::<std::io::Result<Vec<_>>>()?;
                ensure!(
                    entries.len() == 1,
                    "exactly one group archive journal required"
                );
                let value: Value =
                    serde_json::from_slice(&std::fs::read(entries[0].path().join("state"))?)?;
                ensure!(
                    value["record"]["pending"] == "None" && value["record"]["retire"].is_null(),
                    "archive journal still advancing"
                );
                Ok(serde_json::from_value(value["record"]["head"].clone())?)
            })?
            .await??;
        ensure!(
            hold.marker().position().index
                >= self
                    .runtime
                    .status(config::history(0)?)
                    .await?
                    .applied
                    .index,
            "archive behind selected cut"
        );
        let target = self.directory.prepare_group_birth(self.config.spec(1)?)?;
        let view = self.directory.observe().await?;
        let request = self.directory.prepare_group_recovery(
            &view,
            ProposalId::new([73; 16])?,
            &self.birth,
            &target,
            hold,
        )?;
        Ok(json!({"recovery":request.encode(),"fence_digest":hex::encode(request.digest())}))
    }
}

fn tls() -> Result<RaftTls> {
    Ok(RaftTls::new(
        vec![
            include_bytes!("../../../crates/cellar/transport/tcp/fixtures/ca.der")
                .to_vec()
                .into(),
        ],
        vec![
            include_bytes!("../../../crates/cellar/transport/tcp/fixtures/server.der")
                .to_vec()
                .into(),
        ],
        rustls::pki_types::PrivatePkcs8KeyDer::from(
            include_bytes!("../../../crates/cellar/transport/tcp/fixtures/key.der").to_vec(),
        )
        .into(),
    )?)
}
