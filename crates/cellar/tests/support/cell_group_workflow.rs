// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! The ordinary Workflow API and replay contract over managed RF3 namespaces.

use super::workflow_contract as contract;
use super::*;
use cellar::sdk::workflow::{Event, Step, WaitOptions};
use cellar_native::clone::StructuredValue;

fn application(
    signals: Arc<contract::Signals>,
    scheduled: bool,
) -> Result<(Arc<Registry>, cellar::engine_api::WorkerConfigOptions)> {
    let worker_signals = signals.clone();
    let workflow_signals = signals.clone();
    let sink = RaftClass::new("Sink", 1, [90; 32])?
        .initializer([91; 32], |_, _| Ok(()))?
        .cell_rpc("note", [92; 32])?
        .cell_rpc("readWorkflow", [95; 32])?
        .query("ReadState", [96; 32], |q, _| {
            let storage = q.storage();
            Ok(serde_json::to_vec(&json!({
                "started": storage.get_json("workflow_read_started")?,
                "after_status": storage.get_json("after_status")?,
                "after_first_chunk": storage.get_json("after_first_chunk")?,
                "bytes": storage.get_json("workflow_read_bytes")?,
            }))?)
        })?
        .query("Effects", [93; 32], |q, _| {
            Ok(serde_json::to_vec(&q.storage().get_json("effects")?)?)
        })?;
    let app = Arc::new(
        Registry::new(move |ctx, invocation| {
            Box::pin(worker(ctx, invocation, worker_signals.clone()))
        })
        .raft_cell(sink, |ctx, _| {
            Ok(Arc::new(Sink(contract::Sink(ctx.storage()?))))
        })?
        .raft_workflow("Orders", [94; 32], move |ctx, event, step| {
            Box::pin(workflow(ctx, event, step, workflow_signals.clone()))
        })?,
    );
    let mut options = configuration::options(contract::SCRIPT);
    options.do_classes = vec![cellar::deploy::workflow_class(contract::SCRIPT)];
    if !scheduled {
        options.do_classes.insert(0, "Sink".into());
    }
    options.vars = vec![("SETTING".into(), "native".into())];
    options.workflow_bindings = vec![cellar::engine_api::WorkflowBinding {
        environment: "WF".into(),
        workflow: "orders".into(),
        class: "Orders".into(),
        settings: Default::default(),
    }];
    if scheduled {
        options.workflow_bindings[0].settings.schedules = vec!["* * * * *".into()];
    }
    let mut deployment = managed_fixture::graph::graph(contract::SCRIPT, "", Vec::new()).primary;
    deployment.options = options.clone();
    app.validate_deployment(&deployment)?;
    Ok((app, options))
}

struct Sink(contract::Sink);

impl Cell for Sink {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        if matches!(&invocation,Invocation::CellRpc {method,..} if method=="readWorkflow") {
            return Box::pin(async move {
                self.0 .0.put_json("workflow_read_started", &json!(true))?;
                let count = read_stream(&context).await?;
                self.0 .0.put_json("workflow_read_bytes", &json!(count))?;
                Ok(Output::Rpc(cellar::application::encode_rpc(
                    &StructuredValue::from_json(json!(count))?,
                )?))
            });
        }
        Arc::new(contract::Sink(self.0 .0.clone())).invoke(context, invocation)
    }
}

async fn read_stream(context: &Context) -> Result<usize> {
    let instance = context.workflow("WF")?.get(context, "stream").await?;
    context.storage()?.put_json("after_status", &json!(true))?;
    let mut stream = instance
        .step_stream(context, cellar::sdk::workflow::StepSelector::new("bytes"))
        .await?;
    let mut count = 0;
    while let Some(bytes) = stream.next(context).await? {
        ensure!(bytes.iter().all(|byte| *byte == 7));
        count += bytes.len();
        if count == bytes.len() {
            context
                .storage()?
                .put_json("after_first_chunk", &json!(true))?;
        }
    }
    ensure!(stream.size() == count as u64);
    Ok(count)
}

async fn worker(
    context: Context,
    invocation: Invocation,
    signals: Arc<contract::Signals>,
) -> Result<Output> {
    if matches!(&invocation,Invocation::Fetch(request) if url::Url::parse(&request.url).is_ok_and(|url|url.path()=="/source-read/stream"))
    {
        let reply = context
            .rpc_cell(
                contract::sink_scope(),
                None,
                "readWorkflow".into(),
                cellar::application::encode_rpc(&StructuredValue::array([])?)?,
            )
            .await?;
        let count = cellar::application::decode_rpc_result(&reply)?.to_json()?;
        return Ok(Output::Http(cellar::engine_api::HttpResponse {
            status: 200,
            body: serde_json::to_vec(&count)?,
            stream: None,
            headers: vec![],
            websocket: None,
            write_position: None,
            observed_position: None,
        }));
    }
    contract::worker(context, invocation, signals).await
}

async fn stream_version(
    cluster: &ManagedCluster,
    group: GroupHistory,
    invocation: u8,
) -> Result<CellVersion> {
    let leader = managed_fixture::leader(&cluster.hosts, group).await?;
    let history = cluster.hosts[leader]
        .resolve_cell_history(
            group.cluster,
            group.namespace,
            CellId::new(cellar::engine_api::workflow_cell_scope(
                contract::SCRIPT,
                "orders",
                "stream",
            ))?,
            QueryOptions::default(),
        )
        .await?
        .context("Workflow stream history")?;
    let cell = cluster.hosts[leader].cell(history)?;
    // This existing read-only observation also carries the exact logical cell
    // version. Check that downloading chunks does not append command receipts.
    Ok(cell
        .query(
            ProposalId::new([invocation; 16])?,
            &cell.contract("__sqlIngressSequence")?,
            0_u64.to_be_bytes().to_vec(),
            QueryOptions::default(),
        )
        .await?
        .version)
}

async fn source_read_state(
    cluster: &ManagedCluster,
    invocation: u8,
) -> Result<Option<QueryResult>> {
    let group = cluster.history;
    let leader = managed_fixture::leader(&cluster.hosts, group).await?;
    let Some(history) = cluster.hosts[leader]
        .resolve_cell_history(
            group.cluster,
            group.namespace,
            CellId::new(contract::sink_scope())?,
            QueryOptions::default(),
        )
        .await?
    else {
        return Ok(None);
    };
    let cell = cluster.hosts[leader].cell(history)?;
    Ok(Some(
        cell.query(
            ProposalId::new([invocation; 16])?,
            &cell.contract("ReadState")?,
            vec![],
            QueryOptions::default(),
        )
        .await?,
    ))
}

async fn source_download(cluster: &ManagedCluster) -> Result<()> {
    let before = source_read_state(cluster, 98)
        .await?
        .map_or(1, |query| query.version.sequence.get());
    let bytes = 2 * 1024 * 1024 + 17;
    ensure!(call(cluster, "source-read", "stream").await? == json!(bytes));
    let after = source_read_state(cluster, 99)
        .await?
        .context("source read state")?;
    ensure!(
        serde_json::from_slice::<serde_json::Value>(&after.result)?
            == json!({
                "started":true,"after_status":true,"after_first_chunk":true,"bytes":bytes,
            })
    );
    // Three dirty roots must be proven before their respective remote reads;
    // clean chunk reads reuse them. The final result is a fourth mutation.
    ensure!(
        after.version.sequence.get() == before + 4,
        "source sequence {} expected {}",
        after.version.sequence.get(),
        before + 4
    );
    Ok(())
}

async fn workflow(
    ctx: Context,
    event: Event,
    steps: Step,
    signals: Arc<contract::Signals>,
) -> Result<StructuredValue> {
    if let Some(schedule) = event.schedule {
        let scheduled = signals.clone();
        let output = steps
            .run("schedule", move |_| async move {
                scheduled.first.fetch_add(1, Ordering::SeqCst);
                scheduled
                    .extra_cells
                    .lock()
                    .unwrap()
                    .push(event.instance_id);
                StructuredValue::from_json(serde_json::to_value(schedule)?)
            })
            .await?;
        steps
            .wait_for_event(
                "scheduled-approval",
                WaitOptions::new("approve").timeout("10 minutes"),
            )
            .await?;
        return Ok(output);
    }
    if event.instance_id == "delete-held" {
        return steps
            .run("held", move |_| async move {
                struct Dropped(Arc<contract::Signals>);

                impl Drop for Dropped {
                    fn drop(&mut self) {
                        self.0.hung_dropped.store(true, Ordering::SeqCst);
                    }
                }
                let _owned = Dropped(signals.clone());
                signals.hung_started.notify_one();
                signals.release_race.notified().await;
                contract::note(&ctx, "forbidden-after-delete").await?;
                StructuredValue::from_json(json!(true))
            })
            .await;
    }
    if event.instance_id == "archive-outage" {
        return steps
            .run("archive", |_| async {
                StructuredValue::from_json(json!(true))
            })
            .await;
    }
    if event.instance_id == "recovery" {
        let first = signals.clone();
        let context = ctx.clone();
        steps
            .run("durable", move |_| async move {
                first.first.fetch_add(1, Ordering::SeqCst);
                contract::note(&context, "recovery-first").await?;
                StructuredValue::from_json(json!("saved"))
            })
            .await?;
        steps
            .wait_for_event(
                "approval",
                WaitOptions::new("approve").timeout("10 minutes"),
            )
            .await?;
        return steps
            .run("finish", move |_| async move {
                signals.last.fetch_add(1, Ordering::SeqCst);
                StructuredValue::from_json(json!("recovered"))
            })
            .await;
    }
    contract::workflow(ctx, event, steps, signals).await
}

async fn call(cluster: &ManagedCluster, action: &str, id: &str) -> Result<serde_json::Value> {
    let response = reqwest::Client::builder()
        .no_proxy()
        .timeout(TIMEOUT + Duration::from_secs(1))
        .build()?
        .get(format!(
            "http://{}/{action}/{id}",
            cluster.nodes[0].info().public_address
        ))
        .send()
        .await?;
    let status = response.status();
    let body = response.text().await?;
    ensure!(
        status.is_success(),
        "Workflow {action}/{id}: {status} {body}"
    );
    Ok(serde_json::from_str(&body)?)
}

async fn operator(
    cluster: &ManagedCluster,
    id: &str,
    command: serde_json::Value,
    generation: Option<&str>,
    signed: bool,
) -> Result<(reqwest::StatusCode, serde_json::Value)> {
    let info = cluster.nodes[1].info();
    let scope = cellar::engine_api::workflow_cell_scope(contract::SCRIPT, "orders", id);
    let name = format!("orders/{id}");
    let path = format!(
        "/runtime/{scope}?name={}",
        url::form_urlencoded::byte_serialize(name.as_bytes()).collect::<String>()
    );
    let payload = serde_json::to_vec(&json!({"binding":"WF", "instance":id,
        "command":command, "expected_generation":generation}))?;
    let request = reqwest::Client::builder()
        .no_proxy()
        .timeout(TIMEOUT + Duration::from_secs(1))
        .build()?
        .post(format!("http://{}{path}", info.internal_address))
        .body(payload.clone());
    let request = if signed {
        cellar::peer_auth::PeerAuth::new([11; 32], "workflow-operator-fixture")?
            .sign(request, "POST", &path, &payload, &info.node)?
    } else {
        request
    };
    let response = request.send().await?;
    let status = response.status();
    let bytes = response.bytes().await?;
    let body =
        serde_json::from_slice(&bytes).unwrap_or_else(|_| json!(String::from_utf8_lossy(&bytes)));
    Ok((status, body))
}

async fn state(cluster: &ManagedCluster, id: &str, expected: &str) -> Result<serde_json::Value> {
    cellar::asyncrt::timeout(Duration::from_secs(30), async {
        loop {
            let value = call(cluster, "status", id)
                .await
                .with_context(|| format!("Workflow {id} awaiting {expected}"))?;
            if value["state"] == expected {
                return Ok(value);
            }
            ensure!(
                value["state"] != "errored" || expected == "errored",
                "Workflow {id}: {value}"
            );
            cellar::asyncrt::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .with_context(|| format!("Workflow {id} did not reach {expected}"))?
}

pub async fn qualify(real: bool, external: bool) -> Result<()> {
    for extents in [false, true] {
        Box::pin(case(real, external, extents, false, false))
            .await
            .with_context(|| format!("Workflow RustFS={real} Axum={external} extents={extents}"))?;
    }
    Ok(())
}

pub async fn stream_controls() -> Result<()> {
    Box::pin(case(false, false, false, true, false)).await
}

pub async fn schedules(real: bool, external: bool) -> Result<()> {
    for extents in [false, true] {
        Box::pin(case(real, external, extents, false, true))
            .await
            .with_context(|| {
                format!("scheduled Workflow RustFS={real} Axum={external} extents={extents}")
            })?;
    }
    Ok(())
}

async fn scheduled_case(cluster: &mut ManagedCluster, signals: &contract::Signals) -> Result<()> {
    // No public request enrolls the cron owner or creates this instance.
    let id = cellar::asyncrt::timeout(Duration::from_secs(75), async {
        loop {
            if let Some(id) = signals.extra_cells.lock().unwrap().first().cloned() {
                return id;
            }
            cellar::asyncrt::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    state(cluster, &id, "waiting").await?;
    let leader = managed_fixture::leader(&cluster.hosts, cluster.history).await?;
    let history = cluster.hosts[leader]
        .resolve_cell_history(
            cluster.history.cluster,
            cluster.history.namespace,
            CellId::new(cellar_logic::cron::reserved_cell(contract::SCRIPT))?,
            QueryOptions {
                consistency: QueryConsistency::Linearizable,
                ..Default::default()
            },
        )
        .await?
        .context("autonomous Workflow cron enrollment")?;
    let cell = cluster.hosts[leader].cell(history)?;
    let occurrence = cellar::asyncrt::timeout(TIMEOUT, async {
        loop {
            let result = cell
                .query(
                    ProposalId::new([209; 16])?,
                    &cell.contract("__cronHistory")?,
                    b"{}".to_vec(),
                    QueryOptions {
                        consistency: QueryConsistency::Linearizable,
                        ..Default::default()
                    },
                )
                .await?;
            let page: serde_json::Value = serde_json::from_slice(&result.result)?;
            if let Some(row) = page["items"]
                .as_array()
                .context("cron history items")?
                .iter()
                .find(|row| row["workflow_instance"] == id && row["status"] == "workflow_accepted")
            {
                ensure!(row["workflow_binding"] == "WF" && row["attempt"] == 1);
                return Ok::<_, anyhow::Error>(
                    json!({"cron": row["expression"], "scheduled_ms":row["scheduled_at"]}),
                );
            }
            cellar::asyncrt::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    ensure!(signals.first.load(Ordering::SeqCst) == 1);
    named_service::restart(cluster).await?;
    state(cluster, &id, "waiting").await?;
    call(cluster, "event", &id).await?;
    let completed = state(cluster, &id, "complete").await?;
    ensure!(
        completed["output"] == occurrence,
        "scheduled Workflow result: {completed}"
    );
    ensure!(
        signals.first.load(Ordering::SeqCst) == 1,
        "completed scheduled step replayed after retained RF3 restart"
    );
    Ok(())
}

async fn case(
    real: bool,
    external: bool,
    extents: bool,
    stream_only: bool,
    scheduled: bool,
) -> Result<()> {
    let backend = real
        .then(|| {
            rustfs::StoreConfig::load(
                if external {
                    if scheduled {
                        "workflow-scheduled-axum"
                    } else {
                        "workflow-axum"
                    }
                } else {
                    if scheduled {
                        "workflow-scheduled"
                    } else {
                        "workflow"
                    }
                },
                extents,
            )
        })
        .transpose()?;
    let store = Arc::new(match &backend {
        Some(backend) => catalog_fault::Store::around(backend.open(None)?),
        None => catalog_fault::Store::default(),
    });
    let signals = Arc::new(contract::Signals::default());
    let (app, config) = application(signals.clone(), scheduled)?;
    let mut fixture = Fixture::with_namespaces(
        extents,
        &[
            if scheduled {
                cellar_logic::cron::RESERVED_CLASS
            } else {
                "Sink"
            },
            &cellar::deploy::workflow_class(contract::SCRIPT),
        ],
        app,
        store,
    )
    .await?;
    fixture.configuration = Some(config);
    fixture.sql_workers = 1;
    fixture.max_input_bytes = 16 * 1024;
    fixture.cell_limits.workflow.max_stream_bytes = 3 * 1024 * 1024;
    fixture.effects = Some(RaftEffectOptions {
        callback_timeout_ms: 30_000,
        ..Default::default()
    });
    let http = external.then(|| Arc::new(managed_fixture::http::Owner::default()));
    fixture.external_http = http.clone();
    let domain = fixture.domain.clone();
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
        Ok(c) => c,
        Err(e) => {
            if let Some(http) = http {
                http.close().await?;
            }
            domain.shutdown().await;
            return Err(e);
        }
    };
    let work = domain
        .run(async {
            let group = named_service::activate(&cluster, "workflow").await?;
            named_service::grant(&cluster).await?;
            managed_fixture::leader(&cluster.hosts, group).await?;
            if scheduled {
                scheduled_case(&mut cluster, &signals).await?;
                return Ok(());
            }
            if stream_only {
                call(&cluster, "create", "stream").await?;
                cellar::asyncrt::timeout(TIMEOUT, async {
                    while signals.stream_runs.load(Ordering::SeqCst) == 0 {
                        cellar::asyncrt::sleep(Duration::from_millis(1)).await;
                    }
                })
                .await?;
                state(&cluster, "stream", "waiting").await?;
                let before = stream_version(&cluster, group, 96).await?;
                ensure!(
                    call(&cluster, "read-stream", "stream").await? == json!(2 * 1024 * 1024 + 17)
                );
                source_download(&cluster).await?;
                ensure!(
                    stream_version(&cluster, group, 97).await? == before,
                    "resource reads changed the Workflow target version"
                );
                call(&cluster, "terminate", "stream").await?;
                state(&cluster, "stream", "terminated").await?;
                return Ok(());
            }
            call(&cluster, "create", "recovery").await?;
            state(&cluster, "recovery", "waiting").await?;
            ensure!(signals.first.load(Ordering::SeqCst) == 1);
            let (status, inspected) = operator(
                &cluster,
                "recovery",
                json!({"operation":"inspect"}),
                None,
                true,
            )
            .await?;
            ensure!(status == 200, "Workflow inspect: {status} {inspected}");
            let generation = inspected["result"]["generation"]
                .as_str()
                .context("Workflow generation")?;
            let (status, body) = operator(
                &cluster,
                "recovery",
                json!({"operation":"pause"}),
                Some("stale"),
                true,
            )
            .await?;
            ensure!(
                status == 400 && body.to_string().contains("generation"),
                "Workflow stale operator: {status} {body}"
            );
            state(&cluster, "recovery", "waiting").await?;
            let (status, body) = operator(
                &cluster,
                "recovery",
                json!({"operation":"pause"}),
                Some(generation),
                false,
            )
            .await?;
            ensure!(
                matches!(status.as_u16(), 401 | 403),
                "Workflow unsigned operator: {status} {body}"
            );
            let (status, body) = operator(
                &cluster,
                "recovery",
                json!({"operation":"pause"}),
                Some(generation),
                true,
            )
            .await?;
            ensure!(status == 200, "Workflow pause operator: {status} {body}");
            state(&cluster, "recovery", "paused").await?;
            let (status, body) = operator(
                &cluster,
                "recovery",
                json!({"operation":"resume"}),
                Some(generation),
                true,
            )
            .await?;
            ensure!(status == 200, "Workflow resume operator: {status} {body}");
            state(&cluster, "recovery", "waiting").await?;
            eprintln!("WORKFLOW_PHASE retained-restart");
            named_service::restart(&mut cluster).await?;
            state(&cluster, "recovery", "waiting").await?;
            eprintln!("WORKFLOW_PHASE retained-replay-observed");
            ensure!(
                signals.first.load(Ordering::SeqCst) == 1,
                "completed step replayed after retained RF3 restart"
            );
            call(&cluster, "event", "recovery").await?;
            state(&cluster, "recovery", "complete").await?;
            ensure!(signals.last.load(Ordering::SeqCst) == 1);

            for (id, expected) in [
                ("retry", "complete"),
                ("configured", "complete"),
                ("nested", "errored"),
                ("parallel", "complete"),
                ("capped", "errored"),
            ] {
                call(&cluster, "create", id).await?;
                state(&cluster, id, expected).await?;
            }
            ensure!(*signals.retries.lock().unwrap() == [1, 2, 3]);
            ensure!(*signals.delays.lock().unwrap() == [1, 2]);
            ensure!(signals.capped.load(Ordering::SeqCst) == 1);
            call(&cluster, "create", "permanent").await?;
            state(&cluster, "permanent", "waiting").await?;
            call(&cluster, "event", "permanent").await?;
            state(&cluster, "permanent", "complete").await?;
            ensure!(signals.permanent.load(Ordering::SeqCst) == 1);

            call(&cluster, "create", "timeout").await?;
            cellar::asyncrt::timeout(TIMEOUT, signals.timeout_started.notified()).await?;
            state(&cluster, "timeout", "complete").await?;
            ensure!(!signals.timeout_finished.load(Ordering::SeqCst));
            signals.release_timeout.notify_one();
            cellar::asyncrt::timeout(TIMEOUT, async {
                while !signals.timeout_finished.load(Ordering::SeqCst) {
                    cellar::asyncrt::sleep(Duration::from_millis(10)).await;
                }
            })
            .await?;
            state(&cluster, "timeout", "complete").await?;

            call(&cluster, "create", "rollback").await?;
            let rolled = state(&cluster, "rollback", "errored").await?;
            ensure!(rolled["rollback"]["outcome"] == "complete");
            let history = call(&cluster, "history", "rollback").await?;
            ensure!(history
                .as_array()
                .is_some_and(|rows| rows.iter().any(|e| e["type"] == "rollback_completed")));
            call(&cluster, "create", "stream").await?;
            eprintln!("WORKFLOW_PHASE stream-capture");
            state(&cluster, "stream", "waiting").await?;
            let before = stream_version(&cluster, group, 96).await?;
            ensure!(call(&cluster, "read-stream", "stream").await? == json!(2 * 1024 * 1024 + 17));
            source_download(&cluster).await?;
            eprintln!("WORKFLOW_PHASE stream-source-read");
            ensure!(
                stream_version(&cluster, group, 97).await? == before,
                "Workflow chunk reads mutated the target journal"
            );
            call(&cluster, "event", "stream").await?;
            state(&cluster, "stream", "complete").await?;
            ensure!(signals.stream_runs.load(Ordering::SeqCst) == 1);

            call(&cluster, "create", "delete-held").await?;
            cellar::asyncrt::timeout(TIMEOUT, signals.hung_started.notified()).await?;
            call(&cluster, "delete", "delete-held").await?;
            cellar::asyncrt::timeout(TIMEOUT, async {
                while !signals.hung_dropped.load(Ordering::SeqCst) {
                    cellar::asyncrt::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .context("delete left an old Workflow callback alive")?;
            signals.release_race.notify_one();
            let leader = managed_fixture::leader(&cluster.hosts, cluster.history).await?;
            let sink = cluster.hosts[leader]
                .resolve_cell_history(
                    cluster.history.cluster,
                    cluster.history.namespace,
                    CellId::new(contract::sink_scope())?,
                    QueryOptions {
                        consistency: QueryConsistency::Linearizable,
                        ..Default::default()
                    },
                )
                .await?
                .context("Workflow sink")?;
            let sink = cluster.hosts[leader].cell(sink)?;
            let effects = sink
                .query(
                    ProposalId::new([98; 16])?,
                    &sink.contract("Effects")?,
                    vec![],
                    QueryOptions {
                        consistency: QueryConsistency::Linearizable,
                        ..Default::default()
                    },
                )
                .await?;
            ensure!(
                !serde_json::from_slice::<serde_json::Value>(&effects.result)?
                    .as_array()
                    .is_some_and(|values| values.contains(&json!("forbidden-after-delete")))
            );

            call(&cluster, "delete", "recovery").await?;
            call(&cluster, "create", "recovery").await?;
            state(&cluster, "recovery", "waiting").await?;
            call(&cluster, "pause", "recovery").await?;
            state(&cluster, "recovery", "paused").await?;
            call(&cluster, "event", "recovery").await?;
            call(&cluster, "resume", "recovery").await?;
            state(&cluster, "recovery", "complete").await?;

            let denied = cluster.fixtures[0]
                .store
                .denied_group_archive_writes
                .load(Ordering::Acquire);
            cluster.fixtures[0]
                .store
                .deny_group_archive_writes
                .store(true, Ordering::Release);
            call(&cluster, "create", "archive-outage").await?;
            state(&cluster, "archive-outage", "complete").await?;
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
            .context("Workflow archive outage was not observed")?;
            cluster.fixtures[0]
                .store
                .deny_group_archive_writes
                .store(false, Ordering::Release);
            let stopped = cluster.nodes.split_off(1);
            for node in stopped {
                node.shutdown(cellar::sdk::ShutdownMode::Preserve).await?;
            }
            let response = reqwest::Client::builder()
                .no_proxy()
                .timeout(TIMEOUT + Duration::from_secs(1))
                .build()?
                .get(format!(
                    "http://{}/create/no-quorum",
                    cluster.nodes[0].info().public_address
                ))
                .send()
                .await?;
            ensure!(
                response.status().is_server_error(),
                "Workflow escaped to Fleet without RF3 quorum"
            );
            let response = reqwest::Client::builder()
                .no_proxy()
                .timeout(TIMEOUT + Duration::from_secs(1))
                .build()?
                .get(format!(
                    "http://{}/read-stream/stream",
                    cluster.nodes[0].info().public_address
                ))
                .send()
                .await?;
            ensure!(
                response.status().is_server_error(),
                "Workflow resource read escaped quorum refusal"
            );
            Ok::<_, anyhow::Error>(())
        })
        .await
        .map_err(anyhow::Error::from)
        .and_then(std::convert::identity);
    if work.is_err() {
        for (node, host) in cluster.hosts.iter().enumerate() {
            eprintln!(
                "Workflow failure resources: {}",
                json!({"node":node,"resources":host.resource_usage()})
            );
        }
    }
    let closed = managed_fixture::close_group_nodes_with_groups(
        std::mem::take(&mut cluster.nodes),
        &cluster.hosts,
        2,
    )
    .await;
    if work.is_err() || closed.is_err() {
        for root in std::mem::take(&mut cluster.roots) {
            eprintln!("retained Workflow disk: {}", root.keep().display());
        }
    }
    if let Some(http) = http {
        http.close().await?;
    }
    domain.shutdown().await;
    work?;
    closed?;
    eprintln!("Workflow RustFS={real} Axum={external} extents={extents}: PASS");
    Ok(())
}
