// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Ordinary named EventStream activation, outbox, public reads and inbox/ack.

use super::*;
use cellar::sdk::event_stream::{self as events, Event};
use serde::{Deserialize, Serialize};

const SCRIPT: &str = "shared-events";

#[derive(Serialize, Deserialize)]
struct Order {
    key: String,
}

impl Event for Order {
    const FAMILY: &'static str = "orders";
    const VERSION: u32 = 1;

    fn key(&self) -> &str {
        &self.key
    }
}

fn binding() -> events::Binding {
    events::Binding {
        environment: "EVENTS".into(),
        resource: events::Resource {
            tenant: "tenant".into(),
            application: "shop".into(),
            id: "00000000000000000000000000000001".into(),
            name: "orders".into(),
            family: "orders".into(),
        },
        principal: SCRIPT.into(),
        grant: "owner".into(),
        publish: true,
        read: true,
        subscriptions: vec!["consumer".into()],
        administer: true,
    }
}

fn definition() -> events::Definition {
    events::Definition {
        resource: binding().resource,
        revision: 1,
        retention: events::Retention::Bounded,
        max_bytes: 64 << 10,
        max_records: 100,
        max_age_ms: 3_600_000,
        grants: vec![events::Grant::from_binding(&binding())],
        subscriptions: vec![events::SubscriptionDefinition {
            name: "consumer".into(),
            start: events::Start::Earliest,
            required: false,
        }],
    }
}

struct Producer;

impl Cell for Producer {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::CellRpc { method, .. } = invocation else {
                anyhow::bail!("RPC required")
            };
            let stream = context.stream::<Order>("EVENTS")?;
            let storage = context.storage()?;
            let result = match method.as_str() {
                "Emit" => {
                    storage.tx(|tx| {
                        let n = tx
                            .get_json("emitted")?
                            .and_then(|value| value.as_u64())
                            .unwrap_or(0)
                            + 1;
                        tx.put_json("emitted", &json!(n))?;
                        tx.emit(
                            &stream,
                            Order {
                                key: format!("order-{n}"),
                            },
                        )?;
                        Ok(())
                    })?;
                    json!(true)
                }
                "Consume" => {
                    let subscription = stream.subscription("consumer")?;
                    let batch = subscription
                        .next_with_id(
                            &context,
                            "00000000000000000000000000000011",
                            events::BatchOptions {
                                max_records: 10,
                                max_bytes: 4096,
                                wait_ms: 0,
                            },
                        )
                        .await?
                        .context("pending event batch")?;
                    storage.tx(|tx| {
                        let inbox = tx.inbox("orders")?;
                        for record in &batch.records {
                            ensure!(inbox.accept(record)?, "first delivery was already present");
                            ensure!(!inbox.accept(record)?, "inbox repeated the business effect");
                        }
                        tx.put_json("consumed", &json!(batch.records.len()))?;
                        Ok(())
                    })?;
                    storage.sync().await?;
                    ensure!(batch.ack(&context).await? == events::Acknowledgment::Acknowledged);
                    ensure!(
                        batch.ack(&context).await? == events::Acknowledgment::AlreadyAcknowledged
                    );
                    json!(batch.records.len())
                }
                _ => anyhow::bail!("unknown event test operation"),
            };
            Ok(Output::Rpc(RpcData::Json(result.to_string())))
        })
    }
}

fn application() -> Result<(Arc<Registry>, cellar::engine_api::WorkerConfigOptions)> {
    let class = RaftClass::new("Counter", 1, [221; 32])?
        .initializer([222; 32], |_, _| Ok(()))?
        .json_cell_rpc("Emit", [223; 32])?
        .json_cell_rpc("Consume", [224; 32])?
        .query("Counts", [225; 32], |query, _| {
            let storage = query.storage();
            Ok(serde_json::to_vec(&json!([
                storage.get_json("emitted")?,
                storage.get_json("consumed")?
            ]))?)
        })?;
    let app = Arc::new(
        Registry::new(|context, invocation| {
            Box::pin(async move {
                let Invocation::Fetch(_) = invocation else {
                    anyhow::bail!("HTTP required")
                };
                ensure!(context.identity().is_none());
                let stream = context.stream::<Order>("EVENTS")?;
                let page = stream
                    .read(
                        &context,
                        None,
                        events::ReadOptions {
                            max_records: 10,
                            max_bytes: 4096,
                        },
                    )
                    .await?;
                Ok(Output::Http(cellar::engine_api::HttpResponse {
                    status: 200,
                    headers: vec![],
                    body: serde_json::to_vec(&json!(page
                        .records
                        .iter()
                        .map(|record| &record.event)
                        .collect::<Vec<_>>()))?,
                    stream: None,
                    websocket: None,
                    write_position: None,
                    observed_position: None,
                }))
            })
        })
        .raft_cell(class, |_, _| Ok(Arc::new(Producer)))?,
    );
    let mut options = configuration::options(SCRIPT);
    options.do_classes = vec!["Counter".into()];
    let mut named: events::BindingConfig = binding().into();
    named.resource.id = None;
    options.stream_bindings = vec![named];
    let mut deployment = configuration::deployment(options.clone());
    deployment.event_streams = vec![definition()];
    app.validate_deployment(&deployment)?;
    Ok((app, options))
}

async fn read(cluster: &ManagedCluster, node: usize) -> Result<serde_json::Value> {
    let response = reqwest::Client::builder()
        .no_proxy()
        .timeout(TIMEOUT)
        .build()?
        .get(format!(
            "http://{}/events",
            cluster.nodes[node].info().public_address
        ))
        .send()
        .await?;
    let status = response.status();
    let body = response.text().await?;
    ensure!(status == 200, "public EventStream read: {status} {body}");
    Ok(serde_json::from_str(&body)?)
}

pub async fn qualify(real_rustfs: bool) -> Result<()> {
    for rooted in [false, true] {
        let store = Arc::new(if real_rustfs {
            catalog_fault::Store::around(rustfs::store("events", rooted)?)
        } else {
            catalog_fault::Store::default()
        });
        let (application, declared) = application()?;
        let mut fixture = Fixture::with_namespaces(
            rooted,
            &[
                "Counter",
                cellar::deploy::EVENT_STREAM_CLASS,
                cellar::deploy::EVENT_CATALOG_CLASS,
            ],
            application,
            store,
        )
        .await?;
        fixture.configuration = Some(declared);
        fixture.event_streams = vec![definition()];
        fixture.sql_workers = 1;
        fixture.max_input_bytes = 512 << 10;
        fixture.cell_limits.events.max_event_bytes = 2048;
        fixture.cell_limits.events.max_batch_bytes = 4096;
        fixture.cell_limits.events.max_outbox_bytes = 64 << 10;
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
                named_service::activate_namespace(&cluster, 0, "stream-archive").await?;
                named_service::activate_namespace(&cluster, 1, "catalog-archive").await?;
                for node in &cluster.nodes {
                    node.activate_event_resources().await?;
                }
                let group = cluster.history;
                let leader = managed_fixture::leader(&cluster.hosts, group).await?;
                let scope = cellar_native::cell_id::Namespace::new(
                    "Counter",
                    cellar::engine_api::namespace_key(SCRIPT, "Counter"),
                )
                .id_from_name("producer")
                .scope();
                let history = CellHistory {
                    cluster: group.cluster,
                    namespace: group.namespace,
                    cell: CellId::new(scope)?,
                    incarnation: IncarnationId::new([226; 16])?,
                };
                let cell = cluster.hosts[leader].cell(history.clone())?;
                cell.enroll(
                    CellEnrollmentRequest {
                        creation: ProposalId::new([227; 16])?,
                        durability: Durability::Quorum,
                    },
                    TIMEOUT,
                )
                .await?;
                let retry = u64::try_from(cellar::asyncrt::wall_ms())? + 60_000;
                let options =
                    CommandOptions::new(CommandId::new(ClientId::new([228; 16])?, 1)?, retry);
                let receipt = cell
                    .command(&cell.contract("Emit")?, b"[]".to_vec(), options.clone())
                    .await?;
                let replay = cell
                    .command(&cell.contract("Emit")?, b"[]".to_vec(), options.clone())
                    .await?;
                ensure!(replay.result == receipt.result && replay.id == receipt.id);
                cellar::asyncrt::timeout(Duration::from_secs(20), async {
                    loop {
                        let value = read(&cluster, (leader + 1) % 3).await?;
                        if value.as_array().is_some_and(|records| records.len() == 1) {
                            return Ok::<_, anyhow::Error>(());
                        }
                        cellar::asyncrt::sleep(Duration::from_millis(25)).await;
                    }
                })
                .await
                .context("autonomous shared outbox delivery deadline")??;
                let consumed = cell
                    .command(
                        &cell.contract("Consume")?,
                        b"[]".to_vec(),
                        CommandOptions::new(CommandId::new(ClientId::new([229; 16])?, 1)?, retry),
                    )
                    .await?;
                ensure!(serde_json::from_slice::<usize>(&consumed.result)? == 1);
                named_service::restart_with_groups(&mut cluster, 3).await?;
                for node in &cluster.nodes {
                    node.activate_event_resources().await?;
                }
                let leader = managed_fixture::leader(&cluster.hosts, group).await?;
                let cell = cluster.hosts[leader].cell(history)?;
                let replay = cell
                    .command(&cell.contract("Emit")?, b"[]".to_vec(), options)
                    .await?;
                ensure!(replay.result == receipt.result && replay.id == receipt.id);
                let counts = cell
                    .query(
                        ProposalId::new([230; 16])?,
                        &cell.contract("Counts")?,
                        vec![],
                        QueryOptions::default(),
                    )
                    .await?;
                ensure!(
                    serde_json::from_slice::<serde_json::Value>(&counts.result)? == json!([1, 1])
                );
                ensure!(
                    read(&cluster, (leader + 1) % 3)
                        .await?
                        .as_array()
                        .context("event array")?
                        .len()
                        == 1
                );
                Ok::<_, anyhow::Error>(())
            })
            .await
            .map_err(anyhow::Error::from)
            .and_then(std::convert::identity);
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
        if work.is_err() || !cleanup.is_empty() {
            for root in std::mem::take(&mut cluster.roots) {
                eprintln!("retained EventStream disk: {}", root.keep().display());
            }
        }
        domain.shutdown().await;
        work?;
        ensure!(cleanup.is_empty(), "EventStream cleanup: {cleanup:?}");
    }
    Ok(())
}
