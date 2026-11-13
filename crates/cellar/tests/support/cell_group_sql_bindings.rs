// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Public SQL bindings across separately managed RF3 source/database groups.

use super::*;

#[path = "cell_group_sql_operator.rs"]
mod operator;
#[path = "cell_group_sql_recovery.rs"]
mod recovery;

pub use recovery::extent_error_progress;
pub use recovery::qualify as qualify_recovery;

const SCRIPT: &str = "sql-binding-service";
const DATABASE: &str = "shared/orders";
const MIGRATIONS: &str = "__sys_sql_migrations";

struct Caller(cellar::sdk::sql::Database);

fn sql_failure<T>(
    result: std::result::Result<T, cellar::sdk::sql::Error>,
    messages: &[&str],
) -> Result<serde_json::Value> {
    let error = result.err().context("expected the SQL statement failure")?;
    let message = error.to_string();
    ensure!(
        messages.iter().any(|expected| message.contains(expected)),
        "unexpected SQL failure: {message}"
    );
    Ok(json!(true))
}

impl Cell for Caller {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::CellRpc { method, args } = invocation else {
                anyhow::bail!("RPC required")
            };
            let RpcData::Json(input) = args else {
                anyhow::bail!("JSON required")
            };
            let input: serde_json::Value = serde_json::from_str(&input)?;
            let database = &self.0;
            let result = match method.as_str() {
                "Setup" => {
                    database.migrate(&context, "initial", "CREATE TABLE data(id INTEGER PRIMARY KEY, v TEXT UNIQUE); INSERT INTO data VALUES(1,'one');", MIGRATIONS).await?;
                    json!(true)
                }
                "Batch" => {
                    let results = database.batch(&context, &[
                        database.prepare("CREATE INDEX data_v ON data(v)"),
                        database.prepare("INSERT INTO data VALUES(?,?)").bind_json([json!(2), json!("two")])?,
                        database.prepare("INSERT INTO data VALUES(?,?)").bind_json([json!(3), json!("three")])?,
                    ]).await?;
                    ensure!(results.len() == 3);
                    ensure!(results[0].meta.changed_db, "SQL DDL metadata: {:?}", results[0].meta);
                    ensure!(results.iter().all(|result| result.success)
                        && results[1..].iter().all(|result| result.meta.changes == 1));
                    json!(true)
                }
                "Read" => {
                    let session = database.with_session(Some("first-primary"))?;
                    let result = session.prepare("SELECT id,v FROM data ORDER BY id").raw(&context, true).await?;
                    ensure!(session.get_bookmark().as_deref() == Some("cellar:primary"));
                    json!(result)
                }
                "FailedExec" => sql_failure(database.exec(&context, "INSERT INTO data VALUES(99,'must-roll-back'); SELECT * FROM missing_table;").await, &["no such table: missing_table"] )?,
                "FailedBatch" => sql_failure(database.batch(&context, &[
                    database.prepare("INSERT INTO data VALUES(99,'must-roll-back')"),
                    database.prepare("INSERT INTO data VALUES(1,'duplicate')"),
                ]).await, &["UNIQUE constraint failed: data.id"] )?,
                "FailedMigration" => sql_failure(database.migrate(&context, "failed", "CREATE TABLE transient(value); INSERT INTO data VALUES(99,'must-roll-back'); SELECT * FROM missing_table;", MIGRATIONS).await, &["no such table: missing_table"] )?,
                "History" => json!(database.migrations(&context, MIGRATIONS).await?),
                "Transient" => json!(database.prepare("SELECT count(*) AS n FROM sqlite_master WHERE name='transient'").first(&context, Some("n")).await?),
                "Reject" => sql_failure(database.exec(&context, input.as_str().context("SQL")?).await, &["not authorized", "prohibited"] )?,
                "Undeclared" => {
                    ensure!(context.sql("MISSING").is_err());
                    json!(context.rpc_cell(
                        cellar::engine_api::sql_cell_scope("undeclared/database"),
                        Some("undeclared/database".into()),
                        "__sqlExec".into(),
                        RpcData::Json("[\"CREATE TABLE forbidden(value)\"]".into()),
                    ).await.is_err())
                }
                _ => anyhow::bail!("unknown SQL test operation"),
            };
            Ok(Output::Rpc(RpcData::Json(result.to_string())))
        })
    }
}

fn application() -> Result<(Arc<Registry>, cellar::engine_api::WorkerConfigOptions)> {
    let mut class =
        RaftClass::new("Counter", 1, [191; 32])?.initializer([203; 32], |_, _| Ok(()))?;
    for (index, method) in [
        "Setup",
        "Batch",
        "Read",
        "FailedExec",
        "FailedBatch",
        "FailedMigration",
        "History",
        "Transient",
        "Reject",
        "Undeclared",
    ]
    .into_iter()
    .enumerate()
    {
        class = class.json_cell_rpc(method, [u8::try_from(index + 192)?; 32])?;
    }
    let application = Arc::new(
        Registry::new(|context, invocation| Box::pin(http_request(context, invocation)))
            .raft_cell(class, |context, _| Ok(Arc::new(Caller(context.sql("DB")?))))?,
    );
    let mut declared = configuration::options(SCRIPT);
    declared.do_classes = vec!["Counter".into(), cellar::deploy::SQL_CLASS.into()];
    declared.sql_bindings = vec![("DB".into(), DATABASE.into())];
    declared
        .sql_bindings
        .push(("RACE".into(), "shared/sql-enrollment-race".into()));
    application.validate_deployment(&configuration::deployment(declared.clone()))?;
    Ok((application, declared))
}

async fn http_request(context: Context, invocation: Invocation) -> Result<Output> {
    use futures_util::TryStreamExt as _;
    let Invocation::Fetch(request) = invocation else {
        anyhow::bail!("HTTP required");
    };
    ensure!(context.identity().is_none(), "public SQL must be stateless");
    let operation = request
        .url
        .rsplit('/')
        .next()
        .context("HTTP operation")?
        .to_string();
    let input = context
        .take_body(request.body)?
        .try_concat()
        .await
        .map_err(anyhow::Error::msg)?;
    let database = context.sql("DB")?;
    let result: Result<serde_json::Value> = async {
        match operation.as_str() {
            "IngressRace" => {
                let database = context.sql("RACE")?;
                database.exec(&context, "CREATE TABLE IF NOT EXISTS race(v INTEGER);").await?;
                let value: i64 = serde_json::from_slice(&input)?;
                database.prepare("INSERT INTO race VALUES(?)").bind([value.into()]).run(&context).await?;
                Ok(json!(true))
            }
            "IngressRaceCount" => {
                Ok(json!(context.sql("RACE")?.prepare("SELECT count(*) AS n FROM race").first(&context, Some("n")).await?))
            }
            "IngressSetup" => {
                database.exec(&context, "CREATE TABLE ingress(v INTEGER PRIMARY KEY); INSERT INTO ingress VALUES(7);").await?;
                Ok(json!(true))
            }
            "IngressWrite" => {
                let value: i64 = serde_json::from_slice(&input)?;
                let result = database.prepare("INSERT INTO ingress VALUES(?)")
                    .bind([value.into()]).run(&context).await?;
                ensure!(result.meta.changes == 1);
                Ok(json!(true))
            }
            "IngressRead" => Ok(json!(database.prepare("SELECT v FROM ingress ORDER BY v").raw(&context, false).await?)),
            "IngressRollback" => {
                sql_failure(database.batch(&context, &[
                    database.prepare("INSERT INTO ingress VALUES(99)"),
                    database.prepare("SELECT * FROM missing_table"),
                ]).await, &["no such table: missing_table"])
            }
            "IngressUnknown" => {
                let error = context.rpc_cell(
                    cellar::engine_api::sql_cell_scope("undeclared/http"),
                    Some("undeclared/http".into()), "__sqlExec".into(),
                    RpcData::Json("[\"CREATE TABLE forbidden(v)\"]".into()),
                ).await.err().context("undeclared stateless SQL was admitted")?;
                ensure!(error.downcast_ref::<CommandFailure>() == Some(&CommandFailure::UnsupportedCapability));
                Ok(json!(true))
            }
            "IngressOperatorRefused" => {
                let error = context.rpc_cell(
                    cellar::engine_api::sql_cell_scope(DATABASE), Some(DATABASE.into()),
                    "__sqlOperator".into(), RpcData::Json(json!([{"exec":{"sql":"CREATE TABLE operator_escape(v)","rows":true}}]).to_string()),
                ).await.err().context("ordinary binding entered the operator contract")?;
                ensure!(format!("{error:#}").contains("UnknownOperation"));
                Ok(json!(true))
            }
            _ => {
                let result = Arc::new(Caller(database)).invoke(context, Invocation::CellRpc {
                    method: operation, args: RpcData::Json(String::from_utf8(input)?),
                }).await?;
                let Output::Rpc(RpcData::Json(result)) = result else { anyhow::bail!("JSON response required"); };
                Ok(serde_json::from_str(&result)?)
            }
        }
    }.await;
    let (status, body) = match result {
        Ok(value) => (200, serde_json::to_vec(&value)?),
        Err(error) => (
            503,
            serde_json::to_vec(&json!({"error": format!("{error:#}")}))?,
        ),
    };
    Ok(Output::Http(cellar::engine_api::HttpResponse {
        status,
        headers: vec![],
        body,
        stream: None,
        websocket: None,
        write_position: None,
        observed_position: None,
    }))
}

async fn public_sql(
    cluster: &ManagedCluster,
    node: usize,
    method: &str,
    input: serde_json::Value,
) -> Result<serde_json::Value> {
    let started = cellar::asyncrt::mono_ms();
    let response = reqwest::Client::builder()
        .no_proxy()
        .timeout(TIMEOUT + Duration::from_secs(1))
        .build()?
        .post(format!(
            "http://{}/{method}",
            cluster.nodes[node].info().public_address
        ))
        .body(serde_json::to_vec(&input)?)
        .send()
        .await?;
    let status = response.status();
    let body = response.text().await?;
    eprintln!(
        "public SQL {method} node={node}: elapsed_ms={} status={status}",
        cellar::asyncrt::mono_ms().saturating_sub(started)
    );
    ensure!(
        status == 200,
        "public SQL {method} node {node}: {status} {body}"
    );
    Ok(serde_json::from_str(&body)?)
}

fn applied_hints(cluster: &ManagedCluster) -> Result<u64> {
    let mut total = 0;
    for host in &cluster.hosts {
        let sample = host.performance();
        ensure!(sample.available && !sample.saturated);
        total += sample
            .counters
            .iter()
            .find(|counter| counter.name == "cell_binding_applied_history_hints")
            .context("missing applied binding history observations")?
            .value;
    }
    Ok(total)
}

async fn database_group(cluster: &ManagedCluster) -> Result<GroupHistory> {
    let fixture = &cluster.fixtures[0];
    let declaration = &fixture.additional_namespaces[0];
    let adapter = Catalog::new(
        fixture.store.clone(),
        CatalogOptions {
            prefix: "managed/".into(),
            ..Default::default()
        },
    )?;
    let standby = fixture
        .catalog
        .observe_managed_standby(&adapter, declaration, DirectoryLimits::default(), TIMEOUT)
        .await?;
    let history = GroupHistory {
        cluster: declaration.namespace.cluster,
        namespace: declaration.namespace.namespace,
        group: GroupId::new([181; 16])?,
        incarnation: IncarnationId::new([182; 16])?,
    };
    let cut = fixture.catalog.read(TIMEOUT).await?;
    let reserved = fixture
        .catalog
        .reserve_managed_first_group(
            &adapter,
            declaration,
            RaftManagedFirstGroupRequest {
                reservation: managed_fixture::request(210, cut.revision()),
                standby,
                publication: ProposalId::new([183; 16])?,
                group: CellGroupBirthSpec {
                    history,
                    range: CellRange::all(),
                    epoch: RouteEpoch::new(1)?,
                    creation: ProposalId::new([184; 16])?,
                    voters: cluster
                        .fixtures
                        .iter()
                        .enumerate()
                        .map(|(index, fixture)| {
                            Ok(InitialMember {
                                member: MemberId::new(index as u64 + 1)?,
                                node: fixture.spec.node,
                            })
                        })
                        .collect::<Result<_>>()?,
                    learners: vec![],
                },
            },
            DirectoryLimits::default(),
            TIMEOUT,
        )
        .await?;
    let cut = fixture.catalog.read(TIMEOUT).await?;
    let authorization = fixture
        .catalog
        .publish_managed_first_group(
            &reserved,
            &managed_fixture::request(211, cut.revision()),
            TIMEOUT,
        )
        .await?;
    let mut database_fixture = fixture.clone();
    database_fixture.declaration = declaration.clone();
    let intent_root = cluster.roots[0].path().join("sql-archive");
    let filesystem = fixture.filesystem.clone();
    let directory = intent_root.clone();
    fixture
        .domain
        .blocking(move || {
            cellar_ltx::host::create_dir_all_durable(filesystem.as_ref(), &directory)
        })?
        .await??;
    managed_fixture::archives::initialize(&database_fixture, &adapter, history, &intent_root)
        .await?;
    for host in &cluster.hosts {
        host.activate_managed_initial_group(authorization.clone(), TIMEOUT)
            .await?;
    }
    grant_bindings(cluster).await?;
    managed_fixture::leader(&cluster.hosts, history).await?;
    Ok(history)
}

async fn grant_bindings(cluster: &ManagedCluster) -> Result<()> {
    let fixture = &cluster.fixtures[0];
    let cut = fixture.catalog.read(TIMEOUT).await?;
    for (index, host) in cluster.hosts.iter().enumerate() {
        for (peer_index, peer) in cluster.peers.iter().enumerate() {
            managed_fixture::inspect_current(host, peer.node, cut.revision()).await?;
            if index == peer_index {
                continue;
            }
            // Explicit trusted namespace permissions, separate from RF3
            // membership. Refresh only after verifying this boot's endpoints.
            for declaration in
                std::iter::once(&fixture.declaration).chain(&fixture.additional_namespaces)
            {
                host.grant_query_origin(NamespaceQueryOrigin {
                    cluster: declaration.namespace.cluster,
                    namespace: declaration.namespace.namespace,
                    revision: 1,
                    peer: peer.clone(),
                })?;
                host.grant_command_origin(NamespaceCommandOrigin {
                    cluster: declaration.namespace.cluster,
                    namespace: declaration.namespace.namespace,
                    revision: 1,
                    peer: peer.clone(),
                })?;
            }
        }
    }
    Ok(())
}

async fn restart(cluster: &mut ManagedCluster) -> Result<()> {
    let old_hosts = std::mem::take(&mut cluster.hosts);
    managed_fixture::close_group_nodes_with_groups(
        std::mem::take(&mut cluster.nodes),
        &old_hosts,
        2,
    )
    .await?;
    cluster.peers.clear();
    for (index, fixture) in cluster.fixtures.iter().enumerate() {
        let node = fixture.start_node(cluster.roots[index].path()).await?;
        let started = async {
            let (mut config, services) = fixture
                .config(&node, LogOpenMode::Resume, 104 + index as u8)
                .await?;
            managed_fixture::archives::configure(&mut config);
            let peer = config.peers[0].clone();
            Ok::<_, anyhow::Error>((node.start_raft_cells(config, services).await?, peer))
        }
        .await;
        let (host, peer) = match started {
            Ok(started) => started,
            Err(error) => {
                node.shutdown(cellar::sdk::ShutdownMode::Preserve).await?;
                return Err(error);
            }
        };
        cluster.nodes.push(node);
        cluster.hosts.push(host);
        cluster.peers.push(peer);
    }
    grant_bindings(cluster).await?;
    Ok(())
}

pub async fn qualify(real_rustfs: bool) -> Result<()> {
    qualify_host(real_rustfs, false).await
}

pub async fn qualify_axum(real_rustfs: bool) -> Result<()> {
    qualify_host(real_rustfs, true).await
}

async fn qualify_host(real_rustfs: bool, external: bool) -> Result<()> {
    let mut failures = Vec::new();
    for rooted in [false, true] {
        let result = async {
            let backend = real_rustfs
                .then(|| {
                    rustfs::StoreConfig::load(
                        if external {
                            "sql-bindings-axum"
                        } else {
                            "sql-bindings"
                        },
                        rooted,
                    )
                })
                .transpose()?;
            let store = if let Some(backend) = &backend {
                Arc::new(catalog_fault::Store::around(backend.open(None)?))
            } else {
                Arc::new(catalog_fault::Store::default())
            };
            Box::pin(run(rooted, store, backend.as_ref(), external)).await
        }
        .await;
        match result {
            Ok(()) => eprintln!(
                "SQL binding RustFS={real_rustfs} extents={rooted}: PASS host={}",
                if external { "Axum" } else { "native" }
            ),
            Err(error) => {
                eprintln!(
                    "SQL binding RustFS={real_rustfs} extents={rooted}: FAILED host={}: {error:#}",
                    if external { "Axum" } else { "native" }
                );
                failures.push(format!("extents={rooted}: {error:#}"));
            }
        }
    }
    ensure!(
        failures.is_empty(),
        "SQL binding qualification: {}",
        failures.join("; ")
    );
    Ok(())
}

async fn run(
    rooted: bool,
    store: Arc<catalog_fault::Store>,
    backend: Option<&rustfs::StoreConfig>,
    external: bool,
) -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("cellar::raft::query_connection=debug,cellar::raft::sql_ingress=debug")
        .with_test_writer()
        .try_init();
    let (application, declared) = application()?;
    let mut fixture = Fixture::with_namespaces(
        rooted,
        &["Counter", cellar::deploy::SQL_CLASS],
        application,
        store,
    )
    .await?;
    fixture.configuration = Some(declared);
    let http = external.then(|| Arc::new(managed_fixture::http::Owner::default()));
    fixture.external_http = http.clone();
    fixture.sql_workers = 1;
    fixture.max_input_bytes = 16 * 1024;
    // The old per-call client allocation fails this public drill. The fixed
    // ingress stream must share four permanent slots with ordinary bindings.
    fixture.max_client_identities = 4;
    let started = Box::pin(ManagedCluster::start_with_applications(
        fixture,
        &[],
        ArchiveSetup::Initialized,
        false,
        None,
        FirstGroupSetup::Normal,
    ))
    .await;
    let mut cluster = match started {
        Ok(cluster) => cluster,
        Err(error) => {
            if let Some(http) = http {
                http.close()
                    .await
                    .context(format!("SQL node startup: {error:#}"))?;
            }
            return Err(error);
        }
    };
    let domain = cluster.fixtures[0].domain.clone();
    let work = domain.run(async {
        let sql_group = database_group(&cluster).await?;
        let group = cluster.history;
        let leader = managed_fixture::leader(&cluster.hosts, group).await?;
        let sql_leader = managed_fixture::leader(&cluster.hosts, sql_group).await?;
        let target = (leader + 1) % 3;
        if sql_leader != target {
            let handle = cluster.hosts[sql_leader].group(sql_group)?;
            let status = handle.replication_status(TIMEOUT).await?;
            handle.transfer_leadership(LeadershipTransfer {
                target: MemberId::new(target as u64 + 1)?,
                expected_term: status.term,
                expected_revision: status.membership_revision,
            }, TIMEOUT).await?;
        }
        cellar::asyncrt::timeout(TIMEOUT, async {
            while managed_fixture::leader(&cluster.hosts, sql_group).await? != target {
                cellar::asyncrt::sleep(Duration::from_millis(10)).await;
            }
            Ok::<_, anyhow::Error>(())
        }).await??;
        operator::before(&cluster, backend, sql_group).await?;
        eprintln!("SQL operator admission RustFS={} extents={rooted} host={}: PASS", backend.is_some(), if external { "Axum" } else { "native" });
        let public_hints_before = applied_hints(&cluster)?;
        let raced = futures_util::try_join!(
            public_sql(&cluster, 0, "IngressRace", json!(1)),
            public_sql(&cluster, 2, "IngressRace", json!(1)),
        );
        if raced.is_err() {
            for (node, host) in cluster.hosts.iter().enumerate() {
                eprintln!("SQL race performance node={node}: {}", serde_json::to_string(&host.performance())?);
            }
        }
        let raced = raced?;
        ensure!(raced == (json!(true), json!(true)));
        ensure!(public_sql(&cluster, 1, "IngressRaceCount", json!(null)).await? == json!(2));
        // Public stateless calls share the operator's canonical database;
        // every voter must expose the same committed history.
        ensure!(public_sql(&cluster, 0, "IngressSetup", json!(null)).await? == json!(true));
        ensure!(public_sql(&cluster, 1, "IngressWrite", json!(8)).await? == json!(true));
        ensure!(public_sql(&cluster, 2, "IngressRollback", json!(null)).await? == json!(true));
        ensure!(public_sql(&cluster, 0, "IngressUnknown", json!(null)).await? == json!(true));
        for node in 0..3 {
            ensure!(public_sql(&cluster, node, "IngressRead", json!(null)).await? == json!([[7],[8]]));
        }
        ensure!(applied_hints(&cluster)? > public_hints_before,
            "stateless SQL did not exercise the applied history locator");
        let namespace = cellar_native::cell_id::Namespace::new("Counter", format!("cells:v1:{}:{SCRIPT}:Counter", SCRIPT.len()));
        let source = cluster.hosts[leader].cell(CellHistory { cluster: group.cluster, namespace: group.namespace, cell: CellId::new(namespace.id_from_name("source").scope())?, incarnation: IncarnationId::new([185; 16])? })?;
        source.enroll(CellEnrollmentRequest { creation: ProposalId::new([186; 16])?, durability: Durability::Quorum }, TIMEOUT).await?;
        let expected = json!([["id","v"],[1,"one"],[2,"two"],[3,"three"]]);
        let cases = [
            ("Setup", json!(null), json!(true)), ("Batch", json!(null), json!(true)),
            ("Read", json!(null), expected.clone()), ("FailedExec", json!(null), json!(true)),
            ("FailedBatch", json!(null), json!(true)), ("FailedMigration", json!(null), json!(true)),
            ("Read", json!(null), expected.clone()), ("History", json!(null), json!(["initial"])),
            ("Transient", json!(null), json!(0)),
            ("Reject", json!("INSERT INTO data VALUES(99,'must-roll-back'); COMMIT;"), json!(true)),
            ("Reject", json!("INSERT INTO data VALUES(99,'must-roll-back'); SELECT * FROM __sys_raft_binding_intents;"), json!(true)),
            ("Undeclared", json!(null), json!(true)), ("Read", json!(null), expected.clone()),
        ];
        let mut source_sequence = 1;
        let initial_hints = applied_hints(&cluster)?;
        for (index, (method, input, expected)) in cases.iter().enumerate() {
            let hints_before = applied_hints(&cluster)?;
            let started = cellar::asyncrt::mono_ms();
            let result = source.command(&source.contract(method)?, serde_json::to_vec(input)?, bindings::options(index as u64 + 1, Durability::Quorum)?).await;
            eprintln!("SQL binding {method}: elapsed_ms={} completed={}", cellar::asyncrt::mono_ms()-started, result.is_ok());
            let result = result.with_context(|| format!("SQL binding {method}"))?;
            ensure!(serde_json::from_slice::<serde_json::Value>(&result.result)? == *expected, "SQL binding {method}: {:?}", result.result);
            // Eligible local metadata supplies only a locator: one source
            // checkpoint and one final result remain required. A cold locator
            // retains ordinary enrollment/selection; an undeclared call emits
            // no child. Observe actual eligibility, not follower catchup timing.
            let hints = applied_hints(&cluster)?.checked_sub(hints_before)
                .context("binding observations regressed")?;
            ensure!(hints <= 1, "SQL binding {method}: duplicated locator selection");
            let mutations = if *method == "Undeclared" { ensure!(hints == 0); 1 }
                else if hints == 1 { 2 } else { 3 };
            ensure!(result.version.sequence.get() == source_sequence + mutations,
                "SQL binding {method}: redundant source mutations: {} -> {}",
                source_sequence, result.version.sequence.get());
            eprintln!("SQL binding {method}: applied_hints={hints} source_mutations={mutations}");
            source_sequence = result.version.sequence.get();
        }
        ensure!(applied_hints(&cluster)? > initial_hints,
            "SQL fixture did not exercise the applied history locator");
        ensure!(cluster.hosts[leader].resolve_cell_history(
            sql_group.cluster, sql_group.namespace,
            CellId::new(cellar::engine_api::sql_cell_scope("undeclared/database"))?,
            QueryOptions::default(),
        ).await?.is_none(), "undeclared SQL call must not enroll a database");
        let history = source.history().clone();
        let batch_options = bindings::options(14, Durability::Quorum)?;
        let duplicate = source.command(&source.contract("FailedBatch")?, b"null".to_vec(), batch_options.clone()).await?;
        restart(&mut cluster).await?;
        managed_fixture::leader(&cluster.hosts, sql_group).await?;
        let leader = managed_fixture::leader(&cluster.hosts, group).await?;
        let source = cluster.hosts[leader].cell(history)?;
        operator::after_restart(&cluster).await?;
        ensure!(public_sql(&cluster, 2, "IngressRead", json!(null)).await? == json!([[7],[8]]));
        ensure!(public_sql(&cluster, 1, "IngressWrite", json!(9)).await? == json!(true));
        ensure!(public_sql(&cluster, 0, "IngressRead", json!(null)).await? == json!([[7],[8],[9]]));
        let replay = source.command(&source.contract("FailedBatch")?, b"null".to_vec(), batch_options).await?;
        ensure!(replay.result == duplicate.result);
        let result = source.command(&source.contract("Read")?, b"null".to_vec(), bindings::options(15, Durability::Quorum)?).await?;
        ensure!(serde_json::from_slice::<serde_json::Value>(&result.result)? == expected);
        let result = source.command(&source.contract("History")?, b"null".to_vec(), bindings::options(16, Durability::Quorum)?).await?;
        ensure!(result.result == b"[\"initial\"]");
        // A public SQL read still requires the source quorum after two voter
        // owners stop; it cannot fall back to a local or bucket result.
        let stopped = cluster.nodes.split_off(1);
        managed_fixture::close_group_nodes_with_groups(stopped, &cluster.hosts[1..], 2).await?;
        cluster.hosts.truncate(1);
        operator::quorum_refused(&cluster).await?;
        let response = reqwest::Client::builder().no_proxy().timeout(TIMEOUT + Duration::from_secs(1)).build()?
            .post(format!("http://{}/IngressWrite", cluster.nodes[0].info().public_address))
            .body("10").send().await?;
        let status = response.status();
        let refused = response.text().await?;
        ensure!(status == 503 && (refused.contains("DeadlineExceeded") || refused.contains("Unavailable")
            || refused.contains("NotReady") || refused.contains("Timeout")), "quorum SQL refusal: {status} {refused}");
        let source = cluster.hosts[0].cell(source.history().clone())?;
        let options = bindings::options(17, Durability::Quorum)?;
        let command = options.id;
        let failure = source.command(&source.contract("Read")?, b"null".to_vec(), options).await
            .err().context("SQL binding must refuse without a quorum")?;
        ensure!(failure.command == command);
        Ok::<_, anyhow::Error>(())
    }).await.map_err(anyhow::Error::from).and_then(std::convert::identity);
    let joined = managed_fixture::close_group_nodes_with_groups(
        std::mem::take(&mut cluster.nodes),
        &cluster.hosts,
        2,
    )
    .await;
    let http_joined = match http {
        Some(http) => http.close().await,
        None => Ok(()),
    };
    if work.is_err() || joined.is_err() || http_joined.is_err() {
        for root in std::mem::take(&mut cluster.roots) {
            eprintln!("retained SQL binding disk: {}", root.keep().display());
        }
    }
    domain.shutdown().await;
    work?;
    joined?;
    http_joined
}
