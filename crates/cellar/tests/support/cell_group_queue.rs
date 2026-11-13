// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Ordinary Queue producer, callback, settlement and autonomous restart on RF3.

use super::*;
use cellar::sdk::queue::{self, Body, SendOptions};
use futures_util::TryStreamExt;

const SCRIPT: &str = "quorum-queue-service";
const QUEUE: &str = "quorum-jobs";
const DEAD: &str = "quorum-dead";

struct Caller;

impl Cell for Caller {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::CellRpc { method, args } = invocation else {
                anyhow::bail!("RPC required")
            };
            let RpcData::Json(args) = args else {
                anyhow::bail!("JSON required")
            };
            let args: serde_json::Value = serde_json::from_str(&args)?;
            let value = match method.as_str() {
                "Send" => {
                    let producer = context.queue("Q")?;
                    let send_options = SendOptions {
                        delay_seconds: args["delay"].as_u64().map(u32::try_from).transpose()?,
                    };
                    if let Some(batch) = args["batch"].as_array() {
                        let entries = batch
                            .iter()
                            .map(|v| {
                                Ok(queue::BatchEntry {
                                    body: Body::Text(v.as_str().context("batch message")?.into()),
                                    delay_seconds: None,
                                })
                            })
                            .collect::<Result<Vec<_>>>()?;
                        producer.send_batch(&context, entries, send_options).await?;
                    } else {
                        producer
                            .send(
                                &context,
                                Body::Text(args["body"].as_str().context("message body")?.into()),
                                send_options,
                            )
                            .await?;
                    }
                    json!(true)
                }
                "Metrics" => {
                    let metrics = context
                        .queue(args.as_str().unwrap_or("Q"))?
                        .metrics(&context)
                        .await?;
                    json!({"count":metrics.backlog_count,"bytes":metrics.backlog_bytes})
                }
                "Record" => {
                    let storage = context.storage()?;
                    let id = args["id"].as_str().context("message id")?;
                    if storage.get_json(id)?.is_none() {
                        storage.put_json(id, &args)?;
                        let count = storage
                            .get_json("count")?
                            .and_then(|v| v.as_u64())
                            .unwrap_or(0);
                        storage.put_json("count", &json!(count + 1))?;
                    }
                    json!(true)
                }
                _ => anyhow::bail!("unknown Queue test method"),
            };
            Ok(Output::Rpc(RpcData::Json(value.to_string())))
        })
    }
}

#[derive(Default)]
struct Observed {
    calls: Mutex<Vec<(String, u16)>>,
    changed: Notify,
    abandoned: Mutex<Option<queue::Event>>,
    allow_dead: std::sync::atomic::AtomicBool,
}

impl Observed {
    async fn wait(&self, text: &str, attempt: u16) -> Result<()> {
        self.wait_within(text, attempt, Duration::from_secs(20))
            .await
    }

    async fn wait_within(&self, text: &str, attempt: u16, timeout: Duration) -> Result<()> {
        cellar::asyncrt::timeout(timeout, async {
            loop {
                let wake = self.changed.notified();
                if self
                    .calls
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|(body, n)| body == text && *n == attempt)
                {
                    return;
                }
                wake.await;
            }
        })
        .await
        .with_context(|| format!("autonomous Queue callback {text} attempt {attempt} deadline"))?;
        Ok(())
    }
}

fn application(
    observed: Arc<Observed>,
) -> Result<(Arc<Registry>, cellar::engine_api::WorkerConfigOptions)> {
    let class = RaftClass::new("Counter", 1, [41; 32])?
        .initializer([42; 32], |_, _| Ok(()))?
        .json_cell_rpc("Send", [43; 32])?
        .json_cell_rpc("Metrics", [44; 32])?
        .json_cell_rpc("Record", [45; 32])?
        .query("Count", [46; 32], |query, _| {
            Ok(serde_json::to_vec(&query.storage().get_json("count")?)?)
        })?;
    let application = Arc::new(
        Registry::new(move |context, invocation| {
            let observed = observed.clone();
            Box::pin(async move {
                let invocation = match invocation {
                    Invocation::Fetch(request) => {
                        ensure!(context.identity().is_none());
                        let body = context
                            .take_body(request.body)?
                            .try_concat()
                            .await
                            .map_err(anyhow::Error::msg)?;
                        let text = String::from_utf8(body)?;
                        let result = context
                            .queue("Q")?
                            .send(&context, Body::Text(text), SendOptions::default())
                            .await;
                        return Ok(Output::Http(cellar::engine_api::HttpResponse {
                            status: if result.is_ok() { 200 } else { 503 },
                            headers: vec![],
                            body: result
                                .map(|_| b"true".to_vec())
                                .unwrap_or_else(|e| e.to_string().into_bytes()),
                            stream: None,
                            websocket: None,
                            write_position: None,
                            observed_position: None,
                        }));
                    }
                    invocation => invocation,
                };
                let Invocation::Queue(batch) = invocation else {
                    anyhow::bail!("Queue callback required")
                };
                ensure!(
                    context.identity().is_none() && context.storage().is_err(),
                    "Queue callback exposed broker storage"
                );
                eprintln!(
                    "Queue consumer entry: {:?}",
                    batch
                        .messages
                        .iter()
                        .map(|m| (&m.id, m.attempts))
                        .collect::<Vec<_>>()
                );
                let output = queue::dispatch(batch, move |event| async move {
                    if event.messages().iter().any(|m| {
                        matches!(&m.body,Body::Text(text) if text=="interrupted") && m.attempts == 1
                    }) {
                        *observed.abandoned.lock().unwrap() = Some(event.clone());
                        observed
                            .calls
                            .lock()
                            .unwrap()
                            .push(("interrupted".into(), 1));
                        observed.changed.notify_waiters();
                        return std::future::pending::<Result<()>>().await;
                    }
                    for message in event.messages() {
                        let Body::Text(text) = &message.body else {
                            anyhow::bail!("text message required")
                        };
                        if (text == "dead" && !observed.allow_dead.load(Ordering::Acquire))
                            || text == "dead-purge"
                            || (text == "retry" && message.attempts == 1)
                        {
                            observed
                                .calls
                                .lock()
                                .unwrap()
                                .push((text.clone(), message.attempts));
                            observed.changed.notify_waiters();
                            message.retry(Some(1))?;
                            continue;
                        }
                        context
                            .namespace("COUNTERS")?
                            .get_by_name("sink")
                            .rpc(
                                &context,
                                "Record",
                                RpcData::Json(json!({"id":message.id,"text":text}).to_string()),
                            )
                            .await?;
                        message.ack()?;
                        observed
                            .calls
                            .lock()
                            .unwrap()
                            .push((format!("acked:{text}"), message.attempts));
                        observed
                            .calls
                            .lock()
                            .unwrap()
                            .push((text.clone(), message.attempts));
                        observed.changed.notify_waiters();
                    }
                    Ok(())
                })
                .await?;
                if let Output::Queue(result) = &output {
                    eprintln!(
                        "Queue consumer result: outcome={:?} error={:?}",
                        result.outcome, result.error
                    );
                }
                Ok(output)
            })
        })
        .raft_cell(class, |_, _| Ok(Arc::new(Caller)))?,
    );
    let mut declared = configuration::options(SCRIPT);
    declared.do_classes = vec!["Counter".into(), cellar::deploy::QUEUE_CLASS.into()];
    declared.bindings = vec![("COUNTERS".into(), "Counter".into())];
    declared.queue_bindings = vec![
        cellar::engine_api::QueueBinding {
            environment: "Q".into(),
            queue: QUEUE.into(),
            delivery_delay: 0,
        },
        cellar::engine_api::QueueBinding {
            environment: "DLQ".into(),
            queue: DEAD.into(),
            delivery_delay: 0,
        },
    ];
    declared.queue_consumers = vec![cellar::protocol::QueueConsumerConfig {
        queue: QUEUE.into(),
        max_batch_size: 2,
        max_batch_timeout: 0,
        max_retries: 1,
        dead_letter_queue: Some(DEAD.into()),
        max_concurrency: Some(2),
        retry_delay: Some(1),
    }];
    application.validate_configuration(&declared)?;
    Ok((application, declared))
}

fn identity(name: &str) -> Result<CellId> {
    let namespace = cellar_native::cell_id::Namespace::new(
        "Counter",
        format!("cells:v1:{}:{SCRIPT}:Counter", SCRIPT.len()),
    );
    Ok(CellId::new(namespace.id_from_name(name).scope())?)
}

fn options(sequence: u64) -> Result<CommandOptions> {
    Ok(CommandOptions::new(
        CommandId::new(ClientId::new([47; 16])?, sequence)?,
        u64::try_from(cellar::asyncrt::wall_ms())? + 60_000,
    ))
}

pub async fn qualify(real_rustfs: bool) -> Result<()> {
    qualify_host(real_rustfs, false).await
}

pub async fn qualify_axum() -> Result<()> {
    qualify_host(true, true).await
}

async fn qualify_host(real_rustfs: bool, external: bool) -> Result<()> {
    for rooted in [false, true] {
        let backend = if real_rustfs {
            Some(rustfs::StoreConfig::load(
                if external {
                    "queue-kernel-axum"
                } else {
                    "queue-kernel"
                },
                rooted,
            )?)
        } else {
            None
        };
        let store = match &backend {
            Some(backend) => Arc::new(catalog_fault::Store::around(backend.open(None)?)),
            None => Arc::new(catalog_fault::Store::default()),
        };
        Box::pin(run(rooted, store, backend.as_ref(), external))
            .await
            .context(format!("Queue RustFS={real_rustfs} extents={rooted}"))?;
        eprintln!("Queue RustFS={real_rustfs} extents={rooted}: PASS");
    }
    Ok(())
}

async fn run(
    rooted: bool,
    store: Arc<catalog_fault::Store>,
    backend: Option<&rustfs::StoreConfig>,
    external: bool,
) -> Result<()> {
    let _=tracing_subscriber::fmt().with_env_filter("cellar::raft::cell_groups::controller::effects=debug,cellar::raft::cell_groups::dispatch=debug").with_test_writer().try_init();
    let observed = Arc::new(Observed::default());
    let (application, declared) = application(observed.clone())?;
    let mut fixture = Fixture::with_namespaces(
        rooted,
        &["Counter", cellar::deploy::QUEUE_CLASS],
        application,
        store,
    )
    .await?;
    fixture.configuration = Some(declared);
    let http = external.then(|| Arc::new(managed_fixture::http::Owner::default()));
    fixture.external_http = http.clone();
    fixture.sql_workers = 1;
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
                    .context(format!("Queue startup: {error:#}"))?;
            }
            return Err(error);
        }
    };
    let domain = cluster.fixtures[0].domain.clone();
    let work = domain
        .run(async {
            let queue_group = queue_group(&cluster).await?;
            for host in &cluster.hosts {
                let usage = host.resource_usage().host.controller;
                ensure!(
                    usage.ordinary.max_items == 2 && usage.callbacks.max_items == 1,
                    "callback admission must preserve the original ordinary quota"
                );
            }

            let group = cluster.history;
            let leader = managed_fixture::leader(&cluster.hosts, group).await?;
            let source = cluster.hosts[leader].cell(CellHistory {
                cluster: group.cluster,
                namespace: group.namespace,
                cell: identity("source")?,
                incarnation: IncarnationId::new([48; 16])?,
            })?;
            source
                .enroll(
                    CellEnrollmentRequest {
                        creation: ProposalId::new([49; 16])?,
                        durability: Durability::Quorum,
                    },
                    TIMEOUT,
                )
                .await?;
            let response = reqwest::Client::builder()
                .no_proxy()
                .timeout(TIMEOUT + Duration::from_secs(1))
                .build()?
                .post(format!(
                    "http://{}/queue",
                    cluster.nodes[2].info().public_address
                ))
                .body("public")
                .send()
                .await?;
            let status = response.status();
            let body = response.text().await?;
            ensure!(status == 200, "public Queue send: {status} {body}");
            if let Err(error) = observed.wait("public", 1).await {
                eprintln!(
                    "Queue failed delivery info: {:?}",
                    admin(&cluster, QUEUE, json!({"op":"info"})).await
                );
                eprintln!(
                    "Queue failed delivery peek: {:?}",
                    admin(&cluster, QUEUE, json!({"op":"peek","limit":10})).await
                );
                eprintln!(
                    "Queue callback observations: {:?}",
                    observed.calls.lock().unwrap()
                );
                for (index, host) in cluster.hosts.iter().enumerate() {
                    eprintln!(
                        "Queue host {index} controller: {:?}",
                        host.resource_usage().host.controller
                    );
                }
                return Err(error);
            }
            let paused = admin(&cluster, QUEUE, json!({"op":"pause"})).await?;
            ensure!(paused["paused"] == true);
            ensure!(admin(&cluster, QUEUE, json!({"op":"info"})).await?["paused"] == true);
            admin(&cluster, QUEUE, json!({"op":"resume"})).await?;
            let send = source.contract("Send")?;
            let mut sequence = 1;
            let batch = serde_json::to_vec(&json!({"batch":["ack","retry","dead","dead-purge"]}))?;
            let original = options(sequence)?;
            sequence += 1;
            let accepted = source
                .command(&send, batch.clone(), original.clone())
                .await?;
            let replay = source.command(&send, batch, original).await?;
            ensure!(accepted.result == replay.result);
            observed.wait("ack", 1).await?;
            observed.wait("retry", 2).await?;
            observed.wait("dead", 2).await?;
            let metrics = source.contract("Metrics")?;
            cellar::asyncrt::timeout(Duration::from_secs(20), async {
                loop {
                    let receipt = source
                        .command(&metrics, b"\"DLQ\"".to_vec(), options(sequence)?)
                        .await?;
                    sequence += 1;
                    let value: serde_json::Value = serde_json::from_slice(&receipt.result)?;
                    if value["count"] == 2.0 {
                        break Ok::<_, anyhow::Error>(());
                    }
                    cellar::asyncrt::sleep(Duration::from_millis(20)).await;
                }
            })
            .await??;
            if let Some(backend) = backend {
                cli(&cluster, backend).await?;
            }
            let peek = admin(&cluster, DEAD, json!({"op":"peek","limit":10})).await?;
            ensure!(peek.to_string().contains("deadLetterSource"));
            observed.allow_dead.store(true, Ordering::Release);
            let redrive = admin(&cluster, DEAD, json!({"op":"redrive","limit":1})).await?;
            ensure!(redrive["redriven"] == 1);
            observed.wait("acked:dead", 1).await?;
            let purged = admin(&cluster, DEAD, json!({"op":"purge"})).await?;
            ensure!(purged["deleted"] == 1);
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
                    &send,
                    serde_json::to_vec(&json!({"body":"archive-outage"}))?,
                    options(sequence)?,
                )
                .await?;
            sequence += 1;
            observed.wait("archive-outage", 1).await?;
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
            .context("archive outage must reach the real write adapter")?;
            cluster.fixtures[0]
                .store
                .deny_group_archive_writes
                .store(false, Ordering::Release);
            // A delayed message survives joined stop. The new owner discovers it
            // from committed manifest metadata without another producer request.
            let delayed = serde_json::to_vec(&json!({"body":"after-restart","delay":30}))?;
            let original = options(sequence)?;
            sequence += 1;
            let sent = source
                .command(&send, delayed.clone(), original.clone())
                .await?;
            let replay = source.command(&send, delayed, original).await?;
            ensure!(sent.result == replay.result);
            source
                .command(
                    &send,
                    serde_json::to_vec(&json!({"body":"interrupted"}))?,
                    options(sequence)?,
                )
                .await?;
            sequence += 1;
            observed.wait("interrupted", 1).await?;
            let id = observed
                .abandoned
                .lock()
                .unwrap()
                .as_ref()
                .context("interrupted event")?
                .messages()[0]
                .id
                .clone();
            let rows = admin(&cluster, QUEUE, json!({"op":"peek","limit":10})).await?;
            let leased = rows["messages"]
                .as_array()
                .context("Queue peek")?
                .iter()
                .find(|m| m["id"] == id)
                .context("committed interrupted lease")?;
            let until = leased["lease"]["expiresAt"]
                .as_u64()
                .context("Queue lease expiry")?;
            let now = u64::try_from(cellar::asyncrt::wall_ms())?;
            ensure!(
                until > now && until - now < 120_000,
                "Raft callback inherited the Fleet lease: {} ms",
                until.saturating_sub(now)
            );
            eprintln!("Queue interrupted lease remaining_ms={}", until - now);

            restart(&mut cluster)
                .await
                .context("retained Queue restart")?;
            observed
                .wait_within("after-restart", 1, Duration::from_secs(40))
                .await?;
            observed
                .wait_within("interrupted", 2, Duration::from_secs(120))
                .await?;
            ensure!(
                observed
                    .abandoned
                    .lock()
                    .unwrap()
                    .as_ref()
                    .context("old Queue event")?
                    .ack_all()
                    .is_err(),
                "interrupted event retained live settlement handles"
            );
            let leader = managed_fixture::leader(&cluster.hosts, group)
                .await
                .context("source leader after restart")?;
            let source = cluster.hosts[leader].cell(source.history().clone())?;
            let sink = cluster.hosts[leader]
                .resolve_cell_history(
                    group.cluster,
                    group.namespace,
                    identity("sink")?,
                    QueryOptions {
                        consistency: QueryConsistency::Linearizable,
                        ..Default::default()
                    },
                )
                .await
                .context("resolve consumer sink after restart")?
                .context("consumer sink")?;
            let sink = cluster.hosts[leader].cell(sink)?;
            let result = sink
                .query(
                    ProposalId::new([50; 16])?,
                    &sink.contract("Count")?,
                    vec![],
                    QueryOptions {
                        consistency: QueryConsistency::Linearizable,
                        ..Default::default()
                    },
                )
                .await
                .context("linearizable sink count after restart")?;
            ensure!(serde_json::from_slice::<serde_json::Value>(&result.result)? == json!(7));
            eprintln!("Queue retained restart and sink count verified");
            // Transfer the whole Queue group and verify a fresh public send/consume.
            let before = managed_fixture::leader(&cluster.hosts, queue_group).await?;
            cellar::asyncrt::timeout(TIMEOUT, async {
                loop {
                    let status = cluster.hosts[before]
                        .group(queue_group)?
                        .replication_status(TIMEOUT)
                        .await?;
                    match cluster.hosts[before]
                        .group(queue_group)?
                        .transfer_leadership(
                            LeadershipTransfer {
                                target: MemberId::new(((before + 1) % 3) as u64 + 1)?,
                                expected_term: status.term,
                                expected_revision: status.membership_revision,
                            },
                            TIMEOUT,
                        )
                        .await
                    {
                        Ok(()) => break,
                        // A callback settlement can still be applying. This is
                        // a proven pre-admission refusal, not an unknown handoff.
                        Err(RuntimeError::NotReady) => {
                            cellar::asyncrt::sleep(Duration::from_millis(10)).await;
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
                loop {
                    if managed_fixture::leader(&cluster.hosts, queue_group).await? != before {
                        break Ok::<_, anyhow::Error>(());
                    }
                    cellar::asyncrt::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .context("Queue leader transfer deadline")?
            .context("transfer Queue leader")?;
            source
                .command(
                    &send,
                    serde_json::to_vec(&json!({"body":"new-leader"}))?,
                    options(sequence)?,
                )
                .await?;
            sequence += 1;
            observed
                .wait("new-leader", 1)
                .await
                .context("Queue delivery after leader transfer")?;
            // No successful write can escape through Fleet after losing RF3 quorum.
            let stopped = cluster.nodes.split_off(1);
            for node in stopped {
                node.shutdown(cellar::sdk::ShutdownMode::Preserve).await?;
            }
            let source = cluster.hosts[0].cell(source.history().clone())?;
            let mut refused = options(sequence)?;
            refused.timeout = Duration::from_millis(500);
            ensure!(source
                .command(
                    &send,
                    serde_json::to_vec(&json!({"body":"must-refuse"}))?,
                    refused
                )
                .await
                .is_err());
            // A stateless public producer has no source-cell write that could
            // mask a Queue fallback. It must refuse on Queue quorum loss too.
            let response = reqwest::Client::builder()
                .no_proxy()
                .timeout(TIMEOUT + Duration::from_secs(1))
                .build()?
                .post(format!(
                    "http://{}/queue",
                    cluster.nodes[0].info().public_address
                ))
                .body("must-refuse-public")
                .send()
                .await?;
            let status = response.status();
            let body = response.text().await?;
            ensure!(
                status.is_server_error(),
                "public Queue quorum refusal: {status} {body}"
            );
            Ok::<_, anyhow::Error>(())
        })
        .await;
    let mut failures = Vec::new();
    for node in std::mem::take(&mut cluster.nodes) {
        if let Err(e) = node.shutdown(cellar::sdk::ShutdownMode::Preserve).await {
            failures.push(e.to_string());
        }
    }
    for host in &cluster.hosts {
        if let Err(e) = host.close().await {
            failures.push(e.to_string());
        }
    }
    if let Some(http) = http {
        if let Err(error) = http.close().await {
            failures.push(error.to_string());
        }
    }
    work??;
    ensure!(failures.is_empty(), "Queue joined cleanup: {failures:?}");
    Ok(())
}

async fn queue_group(cluster: &ManagedCluster) -> Result<GroupHistory> {
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
    let intent_root = cluster.roots[0].path().join("queue-archive");
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

/// Exercise the same signed operator entrance used by `cellar queue`.
async fn admin(
    cluster: &ManagedCluster,
    queue: &str,
    body: serde_json::Value,
) -> Result<serde_json::Value> {
    let info = cluster.nodes[1].info();
    let path = format!(
        "/runtime/{}?name={}",
        cellar::engine_api::queue_cell_scope(queue),
        url::form_urlencoded::byte_serialize(queue.as_bytes()).collect::<String>()
    );
    let payload = serde_json::to_vec(&body)?;
    let auth = cellar::peer_auth::PeerAuth::new([11; 32], "queue-operator-fixture")?;
    let request = reqwest::Client::builder()
        .no_proxy()
        .timeout(TIMEOUT + Duration::from_secs(1))
        .build()?
        .post(format!("http://{}{path}", info.internal_address))
        .body(payload.clone());
    let response = auth
        .sign(request, "POST", &path, &payload, &info.node)?
        .send()
        .await?;
    let status = response.status();
    let body = response.text().await?;
    ensure!(status == 200, "Queue operator {queue}: {status} {body}");
    let body: serde_json::Value = serde_json::from_str(&body)?;
    body.get("result")
        .cloned()
        .context("Queue operator result envelope")
}

#[allow(clippy::disallowed_methods)]
async fn cli(cluster: &ManagedCluster, backend: &rustfs::StoreConfig) -> Result<()> {
    use object_store::ObjectStore as _;
    use tokio::io::AsyncReadExt as _;
    cluster.fixtures[0]
        .store
        .put(
            &object_store::path::Path::from("fleet/peer-auth.json"),
            serde_json::to_vec(&json!({"version":1,"key":"0b".repeat(32)}))?.into(),
        )
        .await?;
    let executable = std::env::var_os("CELLAR_CELL_GROUPS_QUEUE_CLI")
        .unwrap_or_else(|| env!("CARGO_BIN_EXE_cellar").into());
    let mut command = tokio::process::Command::new(executable);
    command.args([
        "queue",
        "info",
        DEAD,
        "--bucket",
        &format!("s3://{}/{}", backend.bucket, backend.prefix),
        "--endpoint",
        &backend.endpoint,
        "--region",
        "us-east-1",
        "--json",
    ]);
    for node in &cluster.nodes {
        let info = node.info();
        command.args(["--entrance", &format!("{}={}", info.node, info.advertise)]);
    }
    command
        .kill_on_drop(true)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    let mut child = command.spawn()?;
    let mut out = child.stdout.take().context("Queue CLI stdout")?;
    let mut err = child.stderr.take().context("Queue CLI stderr")?;
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let result = cellar::asyncrt::timeout(Duration::from_secs(20), async {
        tokio::try_join!(
            child.wait(),
            out.read_to_end(&mut stdout),
            err.read_to_end(&mut stderr)
        )
    })
    .await;
    let status = match result {
        Ok(Ok(result)) => result.0,
        _ => {
            let _ = child.start_kill();
            child.wait().await?;
            anyhow::bail!("Queue CLI did not complete and was joined");
        }
    };
    let error = String::from_utf8_lossy(&stderr)
        .replace(&backend.access, "[redacted]")
        .replace(&backend.secret, "[redacted]");
    ensure!(status.success(), "Queue CLI {status}: {error}");
    let output: serde_json::Value = serde_json::from_slice(&stdout)?;
    ensure!(output["backlogCount"] == 2.0, "Queue CLI reply: {output}");
    Ok(())
}
