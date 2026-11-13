// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Ordinary KV APIs, large immutable values and operator edits on managed RF3.
use super::*;
use cellar::sdk::kv::{PutOptions, ReadType, ReadValue};

#[path = "cell_group_kv_cli.rs"]
mod cli;

const SCRIPT: &str = "kv-binding-service";
const NAMESPACE: &str = "shared/cache";

struct Caller(Arc<AtomicUsize>);

async fn operation(context: &Context, method: &str) -> Result<serde_json::Value> {
    let namespace = context.kv("KV")?;
    match method {
        "Contract" => kv_contract::basic(context, &namespace).await?,
        "Large" => kv_contract::large(context, &namespace).await?,
        "ReadLarge" => {
            let Some(ReadValue::Bytes(value)) =
                namespace.get(context, "large", ReadType::Bytes).await?
            else {
                anyhow::bail!("large KV value absent")
            };
            ensure!(value.len() == cellar_logic::kv::MAX_VALUE_BYTES);
            ensure!(value
                .iter()
                .enumerate()
                .all(|(index, byte)| *byte == (index % 251) as u8));
        }
        "Boundary" => {
            let value = vec![127; cellar_logic::kv::MAX_INLINE_VALUE_BYTES];
            namespace
                .put_bytes(
                    context,
                    "inline-boundary",
                    value.clone(),
                    PutOptions::default(),
                )
                .await?;
            let Some(ReadValue::Bytes(actual)) = namespace
                .get(context, "inline-boundary", ReadType::Bytes)
                .await?
            else {
                anyhow::bail!("inline boundary value absent")
            };
            ensure!(actual == value);
        }
        "Write" => {
            namespace
                .put_text(context, "warm", "quorum", PutOptions::default())
                .await?
        }
        "Read" => {
            ensure!(
                matches!(namespace.get(context, "warm", ReadType::Text).await?, Some(ReadValue::Text(value)) if value == "quorum")
            );
        }
        "Delete" => {
            namespace
                .delete_many(context, &["A/one".into(), "missing".into()])
                .await?;
            ensure!(namespace
                .get(context, "A/one", ReadType::Text)
                .await?
                .is_none());
        }
        "Isolation" => {
            let side = context.kv("SIDE")?;
            ensure!(side.get(context, "warm", ReadType::Text).await?.is_none());
            side.put_text(context, "warm", "other namespace", PutOptions::default())
                .await?;
            ensure!(
                matches!(namespace.get(context, "warm", ReadType::Text).await?, Some(ReadValue::Text(value)) if value == "quorum")
            );
        }
        "Refused" => {
            ensure!(context.kv("MISSING").is_err());
            for (method, args) in [
                ("__kvOperator", json!([{"op":"delete","keys":["warm"]}])),
                (
                    "__kvExternalInput",
                    json!([{"method":"__kvOperator","input":"forbidden"}]),
                ),
                (
                    "__kvExternalInput",
                    json!([{"method":"__kvPrepareWire","input":"forbidden"}]),
                ),
            ] {
                let result = context
                    .rpc_cell(
                        cellar::engine_api::kv_cell_scope(NAMESPACE, 0),
                        Some(format!("{NAMESPACE}/0")),
                        method.into(),
                        RpcData::Json(args.to_string()),
                    )
                    .await;
                let refused = match result {
                    Err(_) => true,
                    Ok(reply) => cellar::application::decode_rpc_result(&reply).is_err(),
                };
                ensure!(refused, "public KV tunneled private method {method}");
            }
            ensure!(context
                .rpc_cell(
                    cellar::engine_api::kv_cell_scope("undeclared", 0),
                    Some("undeclared/0".into()),
                    "__kvMetrics".into(),
                    RpcData::Json("[{\"now\":0}]".into())
                )
                .await
                .is_err());
        }
        _ => anyhow::bail!("unknown KV test operation {method}"),
    }
    Ok(json!(true))
}

impl Cell for Caller {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::CellRpc { method, .. } = invocation else {
                anyhow::bail!("RPC required")
            };
            self.0.fetch_add(1, Ordering::AcqRel);
            Ok(Output::Rpc(RpcData::Json(
                operation(&context, &method).await?.to_string(),
            )))
        })
    }
}

fn application(
    calls: Arc<AtomicUsize>,
) -> Result<(Arc<Registry>, cellar::engine_api::WorkerConfigOptions)> {
    let mut class = RaftClass::new("Counter", 1, [31; 32])?.initializer([32; 32], |_, _| Ok(()))?;
    for (index, method) in [
        "Contract",
        "Large",
        "ReadLarge",
        "Boundary",
        "Write",
        "Read",
        "Delete",
        "Isolation",
        "Refused",
    ]
    .into_iter()
    .enumerate()
    {
        class = class.json_cell_rpc(method, [u8::try_from(index + 33)?; 32])?;
    }
    let application = Arc::new(
        Registry::new(|context, invocation| {
            Box::pin(async move {
                let Invocation::Fetch(request) = invocation else {
                    anyhow::bail!("HTTP required")
                };
                ensure!(context.identity().is_none());
                let method = request.url.rsplit('/').next().context("HTTP operation")?;
                let result = operation(&context, method).await;
                let (status, body) = match result {
                    Ok(value) => (200, value.to_string().into_bytes()),
                    Err(error) => (503, format!("{error:#}").into_bytes()),
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
            })
        })
        .raft_cell(class, move |_, _| Ok(Arc::new(Caller(calls.clone()))))?,
    );
    let mut declared = configuration::options(SCRIPT);
    declared.do_classes = vec!["Counter".into(), cellar::deploy::KV_CLASS.into()];
    declared.kv_bindings = vec![
        ("KV".into(), NAMESPACE.into()),
        ("SIDE".into(), "shared/other-cache".into()),
    ];
    application.validate_deployment(&configuration::deployment(declared.clone()))?;
    Ok((application, declared))
}

async fn public(cluster: &ManagedCluster, node: usize, method: &str) -> Result<(u16, String)> {
    let start = cellar::asyncrt::mono_ms();
    let response = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(60))
        .build()?
        .post(format!(
            "http://{}/{method}",
            cluster.nodes[node].info().public_address
        ))
        .send()
        .await?;
    let status = response.status().as_u16();
    let body = response.text().await?;
    eprintln!(
        "public KV {method} node={node}: elapsed_ms={} status={status}",
        cellar::asyncrt::mono_ms() - start
    );
    Ok((status, body))
}

async fn public_ok(cluster: &ManagedCluster, node: usize, method: &str) -> Result<()> {
    let (status, body) = public(cluster, node, method).await?;
    ensure!(
        status == 200 && body == "true",
        "public KV {method}: {status}: {body}"
    );
    Ok(())
}

async fn admin(cluster: &ManagedCluster, body: serde_json::Value) -> Result<serde_json::Value> {
    let info = cluster.nodes[0].info();
    let path = format!(
        "/runtime/{}?name={}",
        cellar::engine_api::kv_cell_scope(NAMESPACE, 0),
        url::form_urlencoded::byte_serialize(format!("{NAMESPACE}/0").as_bytes())
            .collect::<String>()
    );
    let payload = serde_json::to_vec(&body)?;
    let request = reqwest::Client::builder()
        .no_proxy()
        .timeout(TIMEOUT + Duration::from_secs(1))
        .build()?
        .post(format!("http://{}{path}", info.internal_address))
        .body(payload.clone());
    let auth = cellar::peer_auth::PeerAuth::new([11; 32], "kv-operator-fixture")?;
    let response = auth
        .sign(request, "POST", &path, &payload, &info.node)?
        .send()
        .await?;
    let status = response.status();
    let body = response.text().await?;
    ensure!(status == 200, "KV operator: {status}: {body}");
    serde_json::from_str::<serde_json::Value>(&body)?["result"]
        .as_object()
        .cloned()
        .map(serde_json::Value::Object)
        .context("KV operator result")
}

async fn operator_contract(cluster: &ManagedCluster) -> Result<serde_json::Value> {
    let original = admin(cluster, json!({"op":"get","key":"warm"})).await?;
    let token = original["revision"].as_str().context("KV revision token")?;
    ensure!(
        token.starts_with("cg1:") && token.len() > 37,
        "incarnation-qualified revision: {original}"
    );
    let edit = |revision| json!({"op":"edit","key":"warm","condition":{"revision":revision},"value":"cXVvcnVt","tag":"bytes","metadata":"null","expiration_mode":{"mode":"never"}});
    ensure!(admin(cluster, edit(json!(token))).await?["ok"] == true);
    ensure!(admin(cluster, edit(json!(token))).await?["conflict"] == true);
    let current = admin(cluster, json!({"op":"get","key":"warm"})).await?;
    let (_, sequence) = current["revision"]
        .as_str()
        .context("current revision")?
        .rsplit_once(':')
        .context("revision sequence")?;
    let foreign = format!("cg1:{}:{sequence}", "01".repeat(16));
    ensure!(
        admin(cluster, edit(json!(foreign))).await?["conflict"] == true,
        "another incarnation reused the same counter"
    );
    // Existing operator expiration has no minimum public TTL. Leave the cell
    // alone until due; storage metrics distinguish physical removal from reads
    // merely filtering an expired row.
    ensure!(admin(cluster, json!({"op":"edit","key":"autonomous-expiry","condition":{"revision":null},"value":"eA==","tag":"bytes","metadata":"null","expiration_mode":{"mode":"at","timestamp_ms":cellar::ownership_store::now_ms()+1500}})).await?["ok"] == true);
    let before = admin(cluster, json!({"op":"info"})).await?;
    cellar::asyncrt::sleep(Duration::from_millis(2100)).await;
    let after = admin(cluster, json!({"op":"info"})).await?;
    ensure!(
        after["stored"]
            .as_u64()
            .zip(before["stored"].as_u64())
            .is_some_and(|(after, before)| after + 1 == before),
        "KV alarm did not reclaim expired row: before={before} after={after}"
    );
    Ok(current["revision"].clone())
}

pub async fn qualify(real_rustfs: bool, external: bool) -> Result<()> {
    let mut failures = Vec::new();
    for rooted in [false, true] {
        let result = async {
            let backend = real_rustfs
                .then(|| {
                    rustfs::StoreConfig::load(
                        if external {
                            "kv-bindings-axum"
                        } else {
                            "kv-bindings"
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
            Ok(()) => {
                eprintln!("KV binding RustFS={real_rustfs} extents={rooted} Axum={external}: PASS")
            }
            Err(error) => {
                eprintln!("KV binding RustFS={real_rustfs} extents={rooted} Axum={external}: FAILED: {error:#}");
                failures.push(format!("extents={rooted}: {error:#}"));
            }
        }
    }
    ensure!(
        failures.is_empty(),
        "KV binding qualification: {}",
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
    let calls = Arc::new(AtomicUsize::new(0));
    let (application, declared) = application(calls.clone())?;
    let mut fixture = Fixture::with_namespaces(
        rooted,
        &["Counter", cellar::deploy::KV_CLASS],
        application,
        store,
    )
    .await?;
    fixture.configuration = Some(declared);
    let http = external.then(|| Arc::new(managed_fixture::http::Owner::default()));
    fixture.external_http = http.clone();
    fixture.sql_workers = 1;
    fixture.max_input_bytes = 16 * 1024;
    fixture.max_client_identities = 4;
    fixture.payloads = Some(CellGroupPayloadLimits {
        max_operations: 2,
        max_retained_bytes: 600 * 1024 * 1024,
        max_payload_bytes: 64 * 1024 * 1024 + 4096,
        io_timeout: Duration::from_secs(10),
    });
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
                http.close().await?;
            }
            return Err(error);
        }
    };
    let domain = cluster.fixtures[0].domain.clone();
    let work = domain
        .run(async {
            let kv_group = named_service::activate(&cluster, "kv-archive").await?;
            let group = cluster.history;
            let leader = managed_fixture::leader(&cluster.hosts, group).await?;
            let kv_leader = managed_fixture::leader(&cluster.hosts, kv_group).await?;
            let target = (leader + 1) % 3;
            if kv_leader != target {
                let handle = cluster.hosts[kv_leader].group(kv_group)?;
                let status = handle.replication_status(TIMEOUT).await?;
                handle
                    .transfer_leadership(
                        LeadershipTransfer {
                            target: MemberId::new(target as u64 + 1)?,
                            expected_term: status.term,
                            expected_revision: status.membership_revision,
                        },
                        TIMEOUT,
                    )
                    .await?;
            }
            cellar::asyncrt::timeout(TIMEOUT, async {
                while managed_fixture::leader(&cluster.hosts, kv_group).await? != target {
                    cellar::asyncrt::sleep(Duration::from_millis(10)).await;
                }
                Ok::<_, anyhow::Error>(())
            })
            .await??;
            public_ok(&cluster, 0, "Contract").await?;
            public_ok(&cluster, 2, "Write").await?;
            public_ok(&cluster, 1, "Refused").await?;
            public_ok(&cluster, 0, "Boundary").await?;
            if let Some(backend) = backend {
                cli::run(&cluster, backend).await?;
            }
            let token = operator_contract(&cluster).await?;
            let namespace = cellar_native::cell_id::Namespace::new(
                "Counter",
                format!("cells:v1:{}:{SCRIPT}:Counter", SCRIPT.len()),
            );
            let history = CellHistory {
                cluster: group.cluster,
                namespace: group.namespace,
                cell: CellId::new(namespace.id_from_name("source").scope())?,
                incarnation: IncarnationId::new([51; 16])?,
            };
            let source = cluster.hosts[leader].cell(history.clone())?;
            source
                .enroll(
                    CellEnrollmentRequest {
                        creation: ProposalId::new([52; 16])?,
                        durability: Durability::Quorum,
                    },
                    TIMEOUT,
                )
                .await?;
            let mut sequence = 1;
            for method in [
                "Contract",
                "Boundary",
                "Large",
                "Isolation",
                "Refused",
                "Delete",
            ] {
                let mut options = bindings::options(sequence, Durability::Quorum)?;
                // This profile admits a compound conformance operation consisting
                // of dozens of independent KV API calls; it changes no SQL gate.
                options.timeout = Duration::from_secs(20);
                let start = cellar::asyncrt::mono_ms();
                let reply = source
                    .command(&source.contract(method)?, b"null".to_vec(), options)
                    .await
                    .with_context(|| format!("source KV {method}"))?;
                eprintln!(
                    "source KV {method}: elapsed_ms={}",
                    cellar::asyncrt::mono_ms() - start
                );
                ensure!(reply.result == b"true");
                sequence += 1;
            }
            for host in &cluster.hosts {
                let usage = host
                    .resource_usage()
                    .host
                    .payloads
                    .context("installed KV pool")?;
                ensure!(
                    usage.operations == 0 && usage.retained_bytes == 0,
                    "KV transport leaked payload admission: {usage:?}"
                );
            }
            let duplicate_options = bindings::options(sequence, Durability::Quorum)?;
            let duplicate = source
                .command(
                    &source.contract("Read")?,
                    b"null".to_vec(),
                    duplicate_options.clone(),
                )
                .await?;
            sequence += 1;
            named_service::restart(&mut cluster).await?;
            managed_fixture::leader(&cluster.hosts, kv_group).await?;
            let leader = managed_fixture::leader(&cluster.hosts, group).await?;
            let source = cluster.hosts[leader].cell(history)?;
            let before = calls.load(Ordering::Acquire);
            let replay = source
                .command(
                    &source.contract("Read")?,
                    b"null".to_vec(),
                    duplicate_options,
                )
                .await?;
            ensure!(
                replay.result == duplicate.result && calls.load(Ordering::Acquire) == before,
                "retained replay executed the parent again"
            );
            ensure!(admin(&cluster, json!({"op":"get","key":"warm"})).await?["revision"] == token);
            public_ok(&cluster, 2, "ReadLarge").await?;
            public_ok(&cluster, 1, "Read").await?;
            let denied = cluster.fixtures[0]
                .store
                .denied_group_archive_writes
                .load(Ordering::Acquire);
            cluster.fixtures[0]
                .store
                .deny_group_archive_writes
                .store(true, Ordering::Release);
            source
                .command(
                    &source.contract("Write")?,
                    b"null".to_vec(),
                    bindings::options(sequence, Durability::Quorum)?,
                )
                .await?;
            sequence += 1;
            public_ok(&cluster, 0, "Read").await?;
            cellar::asyncrt::timeout(TIMEOUT, async {
                while cluster.fixtures[0]
                    .store
                    .denied_group_archive_writes
                    .load(Ordering::Acquire)
                    == denied
                {
                    cellar::asyncrt::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .context("archive outage did not reach the actual PUT adapter")?;
            cluster.fixtures[0]
                .store
                .deny_group_archive_writes
                .store(false, Ordering::Release);
            let stopped = cluster.nodes.split_off(1);
            managed_fixture::close_group_nodes_with_groups(stopped, &cluster.hosts[1..], 2).await?;
            cluster.hosts.truncate(1);
            for method in ["Write", "Read"] {
                let (status, body) = public(&cluster, 0, method).await?;
                ensure!(
                    status == 503,
                    "KV without quorum: {method}: {status}: {body}"
                );
            }
            let source = cluster.hosts[0].cell(source.history().clone())?;
            ensure!(source
                .command(
                    &source.contract("Write")?,
                    b"null".to_vec(),
                    bindings::options(sequence, Durability::Quorum)?
                )
                .await
                .is_err());
            Ok::<_, anyhow::Error>(())
        })
        .await
        .map_err(anyhow::Error::from)
        .and_then(std::convert::identity);
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
            eprintln!("retained KV binding disk: {}", root.keep().display());
        }
    }
    domain.shutdown().await;
    work?;
    joined?;
    http_joined
}
