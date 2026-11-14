// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Public publication, leased delivery and source/inbox recovery on shared RF3.

use super::*;
use cellar::sdk::event_stream::{self as events, BatchOptions, Event, Stream};
use cellar::sdk::OutboxOptions;
use serde::Serialize;

const SCRIPT: &str = "quorum-event-stream";

#[derive(Serialize)]
struct Order {
    id: String,
    value: String,
}

impl Event for Order {
    const FAMILY: &'static str = "orders";
    const VERSION: u32 = 1;

    fn key(&self) -> &str {
        &self.id
    }
}

fn binding() -> events::Binding {
    events::Binding {
        environment: "ORDERS".into(),
        resource: events::Resource {
            tenant: "tenant".into(),
            application: SCRIPT.into(),
            id: "01010101010101010101010101010101".into(),
            name: "orders".into(),
            family: Order::FAMILY.into(),
        },
        principal: SCRIPT.into(),
        grant: "orders-owner".into(),
        publish: true,
        read: true,
        subscriptions: vec!["fulfillment".into()],
        administer: true,
    }
}

fn definition() -> events::Definition {
    events::Definition {
        resource: binding().resource,
        revision: 1,
        retention: events::Retention::Required,
        max_bytes: 1024 * 1024,
        max_records: 1000,
        max_age_ms: 3_600_000,
        grants: vec![events::Grant::from_binding(&binding())],
        subscriptions: vec![events::SubscriptionDefinition {
            name: "fulfillment".into(),
            start: events::Start::Earliest,
            required: true,
        }],
    }
}

pub fn registration() -> Result<()> {
    let owner = Registry::new(|_, _| Box::pin(async { Ok(Output::Done) }))
        .raft_event_stream(definition())?;
    let mut declared = configuration::options(SCRIPT);
    declared.do_classes = vec![
        cellar::deploy::EVENT_STREAM_CLASS.into(),
        cellar::deploy::EVENT_CATALOG_CLASS.into(),
    ];
    owner.validate_configuration(&declared)?;
    for name in &declared.do_classes {
        owner.resolve_raft_class(name)?;
    }
    ensure!(owner.raft_event_stream(definition()).is_err());
    let (_, valid) = application()?;
    for fault in ["name", "principal", "duplicate"] {
        let (candidate, _) = application()?;
        let mut invalid = valid.clone();
        match fault {
            "name" => invalid.stream_bindings[0].resource.id = None,
            "principal" => invalid.stream_bindings[0].principal = "other-application".into(),
            "duplicate" => invalid
                .stream_bindings
                .push(invalid.stream_bindings[0].clone()),
            _ => unreachable!(),
        }
        ensure!(
            candidate.validate_configuration(&invalid).is_err(),
            "admitted {fault}"
        );
    }
    Ok(())
}

struct Caller(Stream<Order>);

impl Cell for Caller {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::CellRpc { method, args } = invocation else {
                anyhow::bail!("framework work entered application code")
            };
            let RpcData::Json(args) = args else {
                anyhow::bail!("JSON required")
            };
            let args: serde_json::Value = serde_json::from_str(&args)?;
            let storage = context.storage()?;
            let value = match method.as_str() {
                "Publish" | "PublishS3" | "Rollback" | "Oversized" => {
                    let old = storage
                        .get_json("business")?
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);
                    let result: Result<()> = storage.tx(|tx| {
                        tx.put_json("business", &json!(old + 1))?;
                        tx.emit(
                            &self.0,
                            Order {
                                id: format!("order-{old}"),
                                value: if method == "Oversized" {
                                    "x".repeat(20 * 1024)
                                } else {
                                    args.as_str().context("event value")?.into()
                                },
                            },
                        )?;
                        ensure!(
                            tx.sql("SELECT random()", &[]).is_err(),
                            "event emission replaced the command authorizer"
                        );
                        if method == "Rollback" {
                            anyhow::bail!("rollback event and business state")
                        }
                        Ok(())
                    });
                    if method == "Publish" || method == "PublishS3" {
                        result?;
                    } else {
                        let error = result.err().context("expected emission refusal")?;
                        if method == "Oversized" {
                            ensure!(
                                error
                                    .downcast_ref::<events::DeliveryError>()
                                    .is_some_and(|e| e.code == events::ErrorCode::PayloadTooLarge),
                                "{error:#}"
                            );
                        }
                        ensure!(
                            storage
                                .get_json("business")?
                                .and_then(|v| v.as_u64())
                                .unwrap_or(0)
                                == old
                        );
                    }
                    json!(storage.get_json("business")?)
                }
                "Inspect" => json!(storage.outbox(&OutboxOptions::default())?),
                "Effects" => json!(storage.get_json("effects")?),
                "Read" => {
                    let page = self
                        .0
                        .read(&context, None, events::ReadOptions::default())
                        .await?;
                    json!(page
                        .records
                        .iter()
                        .map(|record| &record.event)
                        .collect::<Vec<_>>())
                }
                "Consume" => {
                    let subscription = self.0.subscription("fulfillment")?;
                    let Some(batch) = subscription
                        .next_with_id(
                            &context,
                            args["fetch"].as_str().context("fetch id")?,
                            BatchOptions {
                                wait_ms: 0,
                                ..Default::default()
                            },
                        )
                        .await?
                    else {
                        return Ok(Output::Rpc(RpcData::Json("null".into())));
                    };
                    storage.tx(|tx| {
                        for record in &batch.records {
                            if tx.inbox("fulfillment")?.accept(record)? {
                                let count = tx
                                    .get_json("effects")?
                                    .and_then(|v| v.as_u64())
                                    .unwrap_or(0);
                                tx.put_json("effects", &json!(count + 1))?;
                            }
                        }
                        Ok(())
                    })?;
                    // No acknowledgement can precede local Quorum/S3 proof.
                    storage.sync().await?;
                    let receipt = batch.receipt().encode()?;
                    if args["ack"].as_bool() == Some(true) {
                        batch.ack(&context).await?;
                    }
                    json!({"receipt":receipt,"effects":storage.get_json("effects")?,"records":batch.records.iter().map(|r|&r.event).collect::<Vec<_>>()})
                }
                "Ack" => {
                    let receipt = events::Receipt::decode(args.as_str().context("receipt")?)?;
                    json!(format!(
                        "{:?}",
                        self.0
                            .subscription("fulfillment")?
                            .ack(&context, &receipt)
                            .await?
                    ))
                }
                "Extend" => {
                    let receipt =
                        events::Receipt::decode(args["receipt"].as_str().context("receipt")?)?;
                    json!(
                        self.0
                            .subscription("fulfillment")?
                            .extend_until(
                                &context,
                                &receipt,
                                args["expires"].as_i64().context("expiry")?
                            )
                            .await?
                    )
                }
                "Retry" => {
                    let receipt = events::Receipt::decode(args.as_str().context("receipt")?)?;
                    self.0
                        .subscription("fulfillment")?
                        .retry(&context, &receipt, 0)
                        .await?;
                    json!(true)
                }
                "Change" => {
                    let change: events::Change = serde_json::from_value(args)?;
                    json!(
                        self.0
                            .subscription("fulfillment")?
                            .change(&context, &change)
                            .await?
                    )
                }
                "Status" => json!(self.0.subscription("fulfillment")?.status(&context).await?),
                "Configuration" => json!(self.0.configuration_status(&context).await?),
                "Seal" => {
                    json!(storage.tx(|tx| tx.seal(&self.0, args.as_str().context("operation")?))?)
                }
                "Untrusted" => {
                    let scope = cellar_native::cell_id::for_name(
                        "cells:v1:event-stream:__EventStream",
                        &serde_json::to_string(&(
                            binding().resource.tenant,
                            binding().resource.application,
                            binding().resource.id,
                            0u32,
                        ))?,
                    );
                    let scope = format!(
                        "{}:{}",
                        cellar::deploy::EVENT_STREAM_CLASS,
                        cellar_native::cell_id::encode_hex(&scope)
                    );
                    json!(context
                        .rpc_cell(
                            scope,
                            None,
                            "__streamAppend".into(),
                            RpcData::Json("[{\"authority\":{\"principal\":\"forged\"}}]".into())
                        )
                        .await
                        .is_err())
                }
                _ => anyhow::bail!("unknown stream test operation"),
            };
            Ok(Output::Rpc(RpcData::Json(value.to_string())))
        })
    }
}

fn application() -> Result<(Arc<Registry>, cellar::engine_api::WorkerConfigOptions)> {
    let mut class =
        RaftClass::new("Counter", 1, [171; 32])?.initializer([172; 32], |_, _| Ok(()))?;
    for (index, name) in [
        "Publish",
        "PublishS3",
        "Rollback",
        "Oversized",
        "Inspect",
        "Read",
        "Consume",
        "Ack",
        "Status",
        "Configuration",
        "Seal",
        "Untrusted",
        "Extend",
        "Retry",
        "Change",
        "Effects",
    ]
    .into_iter()
    .enumerate()
    {
        class = class.json_cell_rpc(name, [u8::try_from(index + 173)?; 32])?;
    }
    let app = Arc::new(
        Registry::new(|context, invocation| {
            Box::pin(async move {
                let Invocation::Fetch(request) = invocation else {
                    anyhow::bail!("HTTP required")
                };
                ensure!(context.identity().is_none());
                let result: Result<serde_json::Value> = async {
                    let stream = context.stream::<Order>("ORDERS")?;
                    match request.url.rsplit('/').next().context("HTTP operation")? {
                        "Read" => Ok(json!(stream
                            .read(&context, None, events::ReadOptions::default())
                            .await?
                            .records
                            .iter()
                            .map(|record| &record.event)
                            .collect::<Vec<_>>())),
                        "Empty" => Ok(json!(stream
                            .subscription("fulfillment")?
                            .next(
                                &context,
                                BatchOptions {
                                    wait_ms: 200,
                                    ..Default::default()
                                },
                            )
                            .await?
                            .is_none())),
                        _ => anyhow::bail!("unknown HTTP stream operation"),
                    }
                }
                .await;
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
        .raft_event_stream(definition())?
        .raft_cell(class, |context, _| {
            Ok(Arc::new(Caller(context.stream("ORDERS")?)))
        })?,
    );
    let mut options = configuration::options(SCRIPT);
    options.do_classes = vec!["Counter".into()];
    options.stream_bindings = vec![binding().into()];
    app.validate_configuration(&options)?;
    Ok((app, options))
}

async fn call(
    cluster: &ManagedCluster,
    history: &CellHistory,
    sequence: &mut std::collections::BTreeMap<String, u64>,
    method: &str,
    input: serde_json::Value,
) -> Result<CommandReceipt> {
    Ok(call_options(cluster, history, sequence, method, input)
        .await?
        .0)
}

async fn call_options(
    cluster: &ManagedCluster,
    history: &CellHistory,
    sequence: &mut std::collections::BTreeMap<String, u64>,
    method: &str,
    input: serde_json::Value,
) -> Result<(CommandReceipt, CommandOptions)> {
    let node = managed_fixture::leader(&cluster.hosts, cluster.history).await?;
    let cell = cluster.hosts[node].cell(history.clone())?;
    let next = sequence
        .entry(history.cell.as_str().to_owned())
        .or_insert(1);
    let id = CommandId::new(ClientId::new([190; 16])?, *next)?;
    *next += 1;
    let mut options = CommandOptions::new(id, u64::try_from(cellar::asyncrt::wall_ms())? + 600_000);
    if method == "PublishS3" || method == "Consume" {
        options.durability = Durability::S3;
    }
    let response = cell
        .command(
            &cell.contract(method)?,
            serde_json::to_vec(&input)?,
            options.clone(),
        )
        .await;
    Ok((
        response.with_context(|| format!("{method} for {}", history.cell.as_str()))?,
        options,
    ))
}

pub async fn qualify(real_rustfs: bool, external: bool) -> Result<()> {
    for rooted in [false, true] {
        eprintln!("EventStream RustFS={real_rustfs} extents={rooted} Axum={external}: START");
        let store = Arc::new(if real_rustfs {
            catalog_fault::Store::around(rustfs::store(
                if external {
                    "event-stream-axum"
                } else {
                    "event-stream"
                },
                rooted,
            )?)
        } else {
            catalog_fault::Store::default()
        });
        let (app, options) = application()?;
        let definitions = [
            "Counter",
            cellar::deploy::EVENT_STREAM_CLASS,
            cellar::deploy::EVENT_CATALOG_CLASS,
        ]
        .into_iter()
        .map(|name| {
            Ok((
                app.resolve_raft_class(name)?,
                if name == "Counter" {
                    Durability::Quorum
                } else {
                    Durability::S3
                },
            ))
        })
        .collect::<Result<Vec<_>>>()?;
        let mut fixture =
            Fixture::with_definition_policies(rooted, definitions, app, store.clone()).await?;
        fixture.configuration = Some(options);
        let http = external.then(|| Arc::new(managed_fixture::http::Owner::default()));
        fixture.external_http = http.clone();
        fixture.sql_workers = 1;
        fixture.max_input_bytes = 16 * 1024;
        fixture.effects = Some(RaftEffectOptions {
            callback_timeout_ms: 5000,
            ..Default::default()
        });
        let domain = fixture.domain.clone();
        let mut cluster = Box::pin(ManagedCluster::start_with_applications(
            fixture,
            &[],
            ArchiveSetup::Initialized,
            false,
            None,
            FirstGroupSetup::Normal,
        ))
        .await?;
        let work = domain
            .run(async {
                named_service::activate_at(&cluster, "stream-archive", 0).await?;
                named_service::activate_at(&cluster, "catalog-archive", 1).await?;
                let source = CellHistory {
                    cluster: cluster.history.cluster,
                    namespace: cluster.history.namespace,
                    cell: CellId::new("Counter:event-source")?,
                    incarnation: IncarnationId::new([191; 16])?,
                };
                let sink = CellHistory {
                    cell: CellId::new("Counter:event-sink")?,
                    incarnation: IncarnationId::new([192; 16])?,
                    ..source.clone()
                };
                for (index, history) in [&source, &sink].into_iter().enumerate() {
                    let node = managed_fixture::leader(&cluster.hosts, cluster.history).await?;
                    cluster.hosts[node]
                        .cell(history.clone())?
                        .enroll(
                            CellEnrollmentRequest {
                                creation: ProposalId::new([193 + index as u8; 16])?,
                                durability: Durability::Quorum,
                            },
                            TIMEOUT,
                        )
                        .await?;
                }
                let mut sequence = std::collections::BTreeMap::new();
                let mut configuration = serde_json::Value::Null;
                cellar::asyncrt::timeout(Duration::from_secs(20), async {
                    loop {
                        let configured = call(
                            &cluster,
                            &source,
                            &mut sequence,
                            "Configuration",
                            json!(null),
                        )
                        .await?;
                        let configured: serde_json::Value =
                            serde_json::from_slice(&configured.result)?;
                        configuration = configured.clone();
                        if configured["state"] == "active" {
                            return Ok::<_, anyhow::Error>(());
                        }
                        cellar::asyncrt::sleep(Duration::from_millis(100)).await;
                    }
                })
                .await
                .with_context(|| {
                    format!("autonomous catalog provisioning deadline: {configuration}")
                })??;
                call(
                    &cluster,
                    &source,
                    &mut sequence,
                    "Rollback",
                    json!("uncommitted"),
                )
                .await?;
                call(&cluster, &source, &mut sequence, "Oversized", json!(null)).await?;
                let before = call(&cluster, &source, &mut sequence, "Inspect", json!(null)).await?;
                ensure!(
                    serde_json::from_slice::<serde_json::Value>(&before.result)?["pending_rows"]
                        == "0"
                );
                let refused =
                    call(&cluster, &sink, &mut sequence, "Untrusted", json!(null)).await?;
                ensure!(
                    serde_json::from_slice::<bool>(&refused.result)?,
                    "application arguments supplied event authority"
                );
                store
                    .deny_group_archive_writes
                    .store(true, Ordering::Release);
                let (published, publish_options) = call_options(
                    &cluster,
                    &source,
                    &mut sequence,
                    "Publish",
                    json!("committed"),
                )
                .await?;
                ensure!(serde_json::from_slice::<serde_json::Value>(&published.result)? == 1);
                cellar::asyncrt::timeout(Duration::from_secs(20), async {
                    while store.denied_group_archive_writes.load(Ordering::Acquire) == 0 {
                        cellar::asyncrt::sleep(Duration::from_millis(50)).await;
                    }
                })
                .await
                .context("archive refusal was not observed")?;
                let pending =
                    call(&cluster, &source, &mut sequence, "Inspect", json!(null)).await?;
                ensure!(
                    serde_json::from_slice::<serde_json::Value>(&pending.result)?["pending_rows"]
                        == "1",
                    "unproved destination settled the source"
                );
                // Retain the original source obligation across a complete node restart.
                named_service::restart_after_close(&mut cluster, || {
                    store
                        .deny_group_archive_writes
                        .store(false, Ordering::Release)
                })
                .await?;
                let records = cellar::asyncrt::timeout(Duration::from_secs(20), async {
                    loop {
                        let observed =
                            call(&cluster, &sink, &mut sequence, "Read", json!(null)).await?;
                        let records: serde_json::Value = serde_json::from_slice(&observed.result)?;
                        if records.as_array().is_some_and(|r| r.len() == 1) {
                            return Ok::<_, anyhow::Error>(records);
                        }
                        cellar::asyncrt::sleep(Duration::from_millis(100)).await;
                    }
                })
                .await
                .context("autonomous event recovery deadline")??;
                ensure!(
                    records[0]["payload"]["value"] == "committed" && records[0]["sequence"] == "1"
                );
                call(
                    &cluster,
                    &sink,
                    &mut sequence,
                    "PublishS3",
                    json!("independent"),
                )
                .await?;
                cellar::asyncrt::timeout(Duration::from_secs(20), async {
                    loop {
                        let observed =
                            call(&cluster, &sink, &mut sequence, "Read", json!(null)).await?;
                        let records: serde_json::Value = serde_json::from_slice(&observed.result)?;
                        if records.as_array().is_some_and(|r| r.len() == 2) {
                            ensure!(
                                records[0]["lane"] != records[1]["lane"]
                                    && records[0]["sequence"] == "1"
                                    && records[1]["sequence"] == "1",
                                "logical sources share lane identity"
                            );
                            return Ok::<_, anyhow::Error>(());
                        }
                        cellar::asyncrt::sleep(Duration::from_millis(100)).await;
                    }
                })
                .await
                .context("S3 source delivery deadline")??;
                let public = reqwest::Client::builder()
                    .no_proxy()
                    .timeout(Duration::from_secs(10))
                    .build()?;
                let observed = public
                    .get(format!(
                        "http://{}/Read",
                        cluster.nodes[1].info().public_address
                    ))
                    .send()
                    .await?;
                let status = observed.status();
                let body = observed.text().await?;
                ensure!(status == 200, "public EventStream read: {status}: {body}");
                ensure!(serde_json::from_str::<Vec<serde_json::Value>>(&body)?.len() == 2);
                let first = call(
                    &cluster,
                    &sink,
                    &mut sequence,
                    "Consume",
                    json!({"fetch":"02020202020202020202020202020202","ack":false}),
                )
                .await?;
                let first: serde_json::Value = serde_json::from_slice(&first.result)?;
                ensure!(first["effects"] == 2);
                let duplicate = call(
                    &cluster,
                    &sink,
                    &mut sequence,
                    "Consume",
                    json!({"fetch":"02020202020202020202020202020202","ack":false}),
                )
                .await?;
                let duplicate: serde_json::Value = serde_json::from_slice(&duplicate.result)?;
                ensure!(
                    duplicate == first,
                    "leased replay changed receipt or repeated inbox effects"
                );
                let lease = call(&cluster, &sink, &mut sequence, "Status", json!(null)).await?;
                let lease: serde_json::Value = serde_json::from_slice(&lease.result)?;
                let expiry = lease["lease"]["expires_ms"]
                    .as_str()
                    .context("lease expiry")?
                    .parse::<i64>()?
                    + 1000;
                for _ in 0..2 {
                    let extended = call(
                        &cluster,
                        &sink,
                        &mut sequence,
                        "Extend",
                        json!({"receipt":first["receipt"],"expires":expiry}),
                    )
                    .await?;
                    ensure!(serde_json::from_slice::<i64>(&extended.result)? == expiry);
                }
                call(
                    &cluster,
                    &sink,
                    &mut sequence,
                    "Retry",
                    first["receipt"].clone(),
                )
                .await?;
                let redelivered = call(
                    &cluster,
                    &sink,
                    &mut sequence,
                    "Consume",
                    json!({"fetch":"04040404040404040404040404040404","ack":false}),
                )
                .await?;
                let redelivered: serde_json::Value = serde_json::from_slice(&redelivered.result)?;
                ensure!(
                    redelivered["effects"] == 2
                        && redelivered["records"] == first["records"]
                        && redelivered["receipt"] != first["receipt"],
                    "retry did not issue a new lease over deduplicated effects"
                );
                let first = redelivered;
                named_service::restart(&mut cluster).await?;
                let effects = call(&cluster, &sink, &mut sequence, "Effects", json!(null)).await?;
                ensure!(
                    serde_json::from_slice::<u64>(&effects.result)? == 2,
                    "proved inbox effects did not survive retained restart"
                );
                let ack = call(
                    &cluster,
                    &sink,
                    &mut sequence,
                    "Ack",
                    first["receipt"].clone(),
                )
                .await?;
                ensure!(serde_json::from_slice::<String>(&ack.result)? == "Acknowledged");
                let replay = call(
                    &cluster,
                    &sink,
                    &mut sequence,
                    "Ack",
                    first["receipt"].clone(),
                )
                .await?;
                ensure!(serde_json::from_slice::<String>(&replay.result)? == "AlreadyAcknowledged");
                let status = call(&cluster, &sink, &mut sequence, "Status", json!(null)).await?;
                let subscription_status: serde_json::Value =
                    serde_json::from_slice(&status.result)?;
                ensure!(subscription_status["checkpoint"] == "2");
                let start = cellar::asyncrt::mono_ms();
                let empty = public
                    .get(format!(
                        "http://{}/Empty",
                        cluster.nodes[2].info().public_address
                    ))
                    .send()
                    .await?;
                let status = empty.status();
                let body = empty.text().await?;
                ensure!(
                    status == 200 && body == "true",
                    "public subscription poll: {status}: {body}"
                );
                ensure!(
                    cellar::asyncrt::mono_ms() - start >= 200,
                    "empty poll did not honor its wait"
                );
                for (operation, action, expected_state) in [
                    (
                        "05050505050505050505050505050505",
                        events::ChangeAction::Pause,
                        "paused",
                    ),
                    (
                        "06060606060606060606060606060606",
                        events::ChangeAction::Resume,
                        "active",
                    ),
                ] {
                    let identity: events::SubscriptionIdentity =
                        serde_json::from_value(subscription_status["identity"].clone())?;
                    let change = events::Change {
                        operation: operation.into(),
                        expected: identity,
                        action,
                        reason: "shared Raft lifecycle qualification".into(),
                    };
                    let changed =
                        call(&cluster, &sink, &mut sequence, "Change", json!(change)).await?;
                    let replayed =
                        call(&cluster, &sink, &mut sequence, "Change", json!(change)).await?;
                    let changed: serde_json::Value = serde_json::from_slice(&changed.result)?;
                    ensure!(
                        changed["state"] == expected_state
                            && serde_json::from_slice::<serde_json::Value>(&replayed.result)?
                                == changed
                    );
                }
                let node = managed_fixture::leader(&cluster.hosts, cluster.history).await?;
                let original = cluster.hosts[node].cell(source.clone())?;
                let retry = original
                    .command(
                        &original.contract("Publish")?,
                        serde_json::to_vec(&json!("committed"))?,
                        publish_options,
                    )
                    .await?;
                ensure!(retry.result == published.result && retry.id == published.id);
                cellar::asyncrt::timeout(Duration::from_secs(20), async {
                    loop {
                        let observed =
                            call(&cluster, &source, &mut sequence, "Inspect", json!(null)).await?;
                        let observed: serde_json::Value = serde_json::from_slice(&observed.result)?;
                        if observed["pending_rows"] == "0" {
                            return Ok::<_, anyhow::Error>(());
                        }
                        cellar::asyncrt::sleep(Duration::from_millis(100)).await;
                    }
                })
                .await
                .context("outbox settlement deadline")??;
                call(
                    &cluster,
                    &source,
                    &mut sequence,
                    "Seal",
                    json!("03030303030303030303030303030303"),
                )
                .await?;
                cellar::asyncrt::timeout(Duration::from_secs(20), async {
                    loop {
                        let observed =
                            call(&cluster, &source, &mut sequence, "Inspect", json!(null)).await?;
                        let observed: serde_json::Value = serde_json::from_slice(&observed.result)?;
                        if observed["lanes"][0]["seal_state"] == "accepted" {
                            return Ok::<_, anyhow::Error>(());
                        }
                        cellar::asyncrt::sleep(Duration::from_millis(100)).await;
                    }
                })
                .await
                .context("terminal seal delivery deadline")??;
                let survivor = cluster.hosts[0].cell(sink)?;
                for node in cluster.nodes.split_off(1) {
                    node.shutdown(cellar::sdk::ShutdownMode::Preserve).await?;
                }
                ensure!(
                    survivor
                        .command(
                            &survivor.contract("Publish")?,
                            b"\"refused\"".to_vec(),
                            CommandOptions::new(
                                CommandId::new(ClientId::new([195; 16])?, 1)?,
                                u64::try_from(cellar::asyncrt::wall_ms())? + 60_000
                            )
                        )
                        .await
                        .is_err(),
                    "event emission bypassed quorum"
                );
                Ok::<_, anyhow::Error>(())
            })
            .await
            .map_err(anyhow::Error::from)
            .and_then(std::convert::identity);
        store
            .deny_group_archive_writes
            .store(false, Ordering::Release);
        let mut cleanup = Vec::new();
        for node in std::mem::take(&mut cluster.nodes) {
            if let Err(error) = node.shutdown(cellar::sdk::ShutdownMode::Preserve).await {
                cleanup.push(error.to_string());
            }
        }
        for host in &cluster.hosts {
            if let Err(error) = host.close().await {
                cleanup.push(error.to_string());
            }
        }
        if let Some(http) = http {
            if let Err(error) = http.close().await {
                cleanup.push(error.to_string());
            }
        }
        if work.is_err() || !cleanup.is_empty() {
            for root in std::mem::take(&mut cluster.roots) {
                eprintln!("retained event-stream disk: {}", root.keep().display());
            }
        }
        domain.shutdown().await;
        work?;
        ensure!(cleanup.is_empty(), "event cleanup: {cleanup:?}");
        eprintln!("EventStream RustFS={real_rustfs} extents={rooted} Axum={external}: PASS");
    }
    Ok(())
}
