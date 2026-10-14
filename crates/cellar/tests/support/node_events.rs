//! Actual node routing and proof gates, including staged adoption and inbox
//! redelivery. No synthetic successful-gate adapter is installed here.

#[path = "peer_reply_fault.rs"]
mod peer_reply_fault;

#[path = "source_cleanup_fault.rs"]
mod source_cleanup_fault;

use super::{call, client, inputs, stopped, Executor};
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{event_stream::*, *};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Serialize, Deserialize)]
struct Changed {
    value: i64,
}

impl Event for Changed {
    const FAMILY: &'static str = "counter-changed";
    const VERSION: u32 = 1;

    fn key(&self) -> &str {
        "counter"
    }
}

fn resource() -> Resource {
    Resource {
        tenant: "tenant".into(),
        application: "embedded-node".into(),
        id: "00000000000000000000000000000001".into(),
        name: "counter".into(),
        family: Changed::FAMILY.into(),
    }
}

fn binding(admin: bool) -> Binding {
    Binding {
        environment: if admin { "ADMIN_EVENTS" } else { "EVENTS" }.into(),
        resource: resource(),
        principal: "embedded-node".into(),
        grant: if admin { "management" } else { "application" }.into(),
        publish: !admin,
        read: !admin,
        subscriptions: if admin {
            Vec::new()
        } else {
            vec!["projection".into()]
        },
        administer: admin,
    }
}

fn graph(revision: i64, missing: bool) -> DeploymentGraph {
    let mut graph = super::graph();
    graph.primary.event_streams = vec![Definition {
        resource: resource(),
        revision,
        retention: Retention::Bounded,
        max_bytes: 1 << 20,
        max_records: 1000,
        max_age_ms: 86400000,
        grants: vec![
            Grant::from_binding(&binding(true)),
            Grant::from_binding(&binding(false)),
        ],
        subscriptions: vec![SubscriptionDefinition {
            name: "projection".into(),
            start: Start::Earliest,
            required: false,
        }],
    }];
    let mut application = binding(false);
    if missing {
        application.resource.id = "00000000000000000000000000000002".into();
        application.resource.name = "missing".into();
    }
    graph.primary.options.stream_bindings = vec![application.into(), binding(true).into()];
    graph
}

struct Counter(&'static str);

impl Cell for Counter {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = invocation else {
                return Ok(Output::Done);
            };
            let path = url::Url::parse(&request.url)?.path().to_owned();
            if path == "/source-facet" || path == "/nested-facet" {
                let class = context.exported_cell_class("SourceFacetCounter")?;
                let child = context
                    .facets()?
                    .get(&context, "persistent-child", move |_| {
                        Box::pin(
                            async move { Ok(cellar::application::facets::Startup::new(class)) },
                        )
                    })?;
                return Ok(Output::Http(child.fetch(&context, request).await?));
            }
            let db = context.storage()?;
            if path == "/" && request.method == "GET" {
                let count = db
                    .get_json("count")?
                    .and_then(|value| value.as_i64())
                    .unwrap_or(0);
                return Ok(super::response(format!("{}:{count}", self.0)));
            }
            let events = context.stream::<Changed>("EVENTS")?;
            let body = if path == "/outbox-pending" {
                db.outbox(&OutboxOptions::default())?
                    .pending_rows
                    .to_string()
            } else if path == "/source-inventory" {
                serde_json::to_string(
                    &context
                        .stream::<Changed>("ADMIN_EVENTS")?
                        .sources(&context, SourceListOptions::default())
                        .await?,
                )?
            } else if path == "/subscription-inventory" {
                serde_json::to_string(
                    &context
                        .stream::<Changed>("ADMIN_EVENTS")?
                        .subscriptions(&context, SubscriptionListOptions::default())
                        .await?,
                )?
            } else if path == "/hold-projection" {
                let batch = events
                    .subscription("projection")?
                    .next(
                        &context,
                        BatchOptions {
                            wait_ms: 0,
                            ..Default::default()
                        },
                    )
                    .await?;
                format!(
                    "leased:{}",
                    batch.map(|batch| batch.records.len()).unwrap_or(0)
                )
            } else if path == "/revoke-publish"
                || path == "/restore-publish"
                || path == "/revoke-admin"
            {
                let revision = if path == "/revoke-publish" { 2 } else { 3 };
                let mut desired = graph(revision, false).primary.event_streams[0].clone();
                if revision == 2 {
                    desired
                        .grants
                        .iter_mut()
                        .find(|grant| grant.id == "application")
                        .unwrap()
                        .publish = false;
                }
                if path == "/revoke-admin" {
                    desired.revision = 4;
                    desired
                        .grants
                        .iter_mut()
                        .find(|grant| grant.id == "management")
                        .unwrap()
                        .administer = false;
                    let mut replacement = Grant::from_binding(&binding(true));
                    replacement.id = "replacement".into();
                    desired.grants.push(replacement);
                }
                let admin = context.stream::<Changed>("ADMIN_EVENTS")?;
                admin
                    .configure(
                        &context,
                        &desired,
                        if revision == 2 {
                            "00000000000000000000000000000088"
                        } else {
                            "00000000000000000000000000000089"
                        },
                    )
                    .await?
                    .state
            } else if path == "/seal" {
                serde_json::to_string(
                    &db.tx(|tx| tx.seal(&events, "00000000000000000000000000000066"))?,
                )?
            } else if path == "/reset-source" {
                db.delete_all()?;
                "reset".into()
            } else if path == "/forge-source" {
                let stub = context.namespace("ACCOUNTS")?.get_by_name("events");
                let failure = stub
                    .rpc(
                        &context,
                        "__outboxControl",
                        cellar::engine_api::RpcData::Json(
                            serde_json::json!({"op":"inspect"}).to_string(),
                        ),
                    )
                    .await
                    .err()
                    .expect("ordinary RPC must refuse source authority");
                failure.to_string()
            } else if path == "/events" {
                events
                    .read(&context, None, ReadOptions::default())
                    .await?
                    .records
                    .len()
                    .to_string()
            } else if path == "/processed" {
                db.get_json("processed")?
                    .and_then(|value| value.as_i64())
                    .unwrap_or(0)
                    .to_string()
            } else if path == "/ack-stored" {
                let receipt: Receipt = serde_json::from_value(
                    db.get_json("ack_receipt")?
                        .context("stored acknowledgment receipt missing")?,
                )?;
                format!(
                    "{:?}",
                    events
                        .subscription("projection")?
                        .ack(&context, &receipt)
                        .await?
                )
            } else if path == "/process-retry"
                || path == "/process-ack"
                || path == "/process-lost-ack"
            {
                let subscription = events.subscription("projection")?;
                if let Some(batch) = subscription
                    .next(
                        &context,
                        BatchOptions {
                            wait_ms: 0,
                            ..Default::default()
                        },
                    )
                    .await?
                {
                    for record in &batch.records {
                        db.tx(|tx| {
                            if tx.inbox("projection")?.accept(record)? {
                                let value = record.event.decode::<Changed>()?.value;
                                let prior = tx
                                    .get_json("processed")?
                                    .and_then(|value| value.as_i64())
                                    .unwrap_or(0);
                                tx.put_json("processed", &serde_json::json!(prior + value))?;
                            }
                            if path == "/process-lost-ack" {
                                tx.put_json(
                                    "ack_receipt",
                                    &serde_json::to_value(batch.receipt())?,
                                )?;
                            }
                            Ok(())
                        })?;
                    }
                    db.sync().await?;
                    if path == "/process-lost-ack" {
                        let result = match batch.ack(&context).await {
                            Ok(value) => format!("{value:?}"),
                            Err(error) => format!(
                                "{:?}",
                                error
                                    .downcast_ref::<DeliveryError>()
                                    .context("lost acknowledgment lacked a typed delivery outcome")?
                                    .code
                            ),
                        };
                        return Ok(super::response(result));
                    }
                    if path == "/process-retry" {
                        batch.retry(&context, 0).await?;
                    } else {
                        batch.ack(&context).await?;
                    }
                }
                db.get_json("processed")?
                    .and_then(|value| value.as_i64())
                    .unwrap_or(0)
                    .to_string()
            } else if path == "/retire" {
                let admin = context.stream::<Changed>("ADMIN_EVENTS")?;
                admin
                    .retire(
                        &context,
                        &Retirement {
                            operation: "00000000000000000000000000000077".into(),
                            revision: 2,
                            reason: "test stale deployment refusal".into(),
                            mode: RetirementMode::Force {
                                data_loss_acknowledgment: "discard remaining event history".into(),
                            },
                        },
                    )
                    .await?
                    .state
            } else {
                let count = db
                    .get_json("count")?
                    .and_then(|value| value.as_i64())
                    .unwrap_or(0);
                let count = if request.method == "POST" {
                    db.tx(|tx| {
                        tx.put_json("count", &serde_json::json!(count + 1))?;
                        tx.emit(&events, Changed { value: count + 1 })
                    })?;
                    count + 1
                } else {
                    count
                };
                format!("{}:{count}", self.0)
            };
            Ok(super::response(body))
        })
    }
}

fn applications(label: &'static str) -> Result<CompiledApplications> {
    applications_with_probe(label, None)
}

fn applications_with_probe(
    label: &'static str,
    probe: Option<Arc<AtomicUsize>>,
) -> Result<CompiledApplications> {
    CompiledApplications::default().register("embedded-node", "test", move || {
        let probe = probe.clone();
        Ok(Arc::new(
            Registry::new(|context, invocation| {
                Box::pin(async move {
                    let Invocation::Fetch(request) = invocation else {
                        return Ok(Output::Done);
                    };
                    let url = url::Url::parse(&request.url)?;
                    let source = url
                        .query_pairs()
                        .find(|(name, _)| name == "source")
                        .map(|(_, source)| source.into_owned())
                        .unwrap_or_else(|| "events".into());
                    let account = context.namespace("ACCOUNTS")?.get_by_name(&source);
                    Ok(Output::Http(account.fetch(&context, request).await?))
                })
            })
            .cell("Account", move |_, _| {
                if let Some(probe) = &probe {
                    probe.fetch_add(1, Ordering::SeqCst);
                }
                Ok(Arc::new(Counter(label)))
            })?
            .cell("SourceFacetCounter", |_, _| {
                Ok(Arc::new(SourceFacetCounter))
            })?,
        ))
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_activates_declarations_checks_adoption_and_processes_redelivery() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let core = Executor::new("event-node-core")?;
    let data = directory.path().join("node");
    let (config, services) = inputs("events", &data, &core, None, 0).await?;
    let error = match Node::start(
        config,
        services,
        graph(1, false),
        applications("unsupported")?,
    )
    .await
    {
        Ok(node) => {
            drop(node);
            anyhow::bail!("event deployment admitted without recoverable discovery");
        }
        Err(error) => error,
    };
    ensure!(
        format!("{error:#}").contains("replicated SQLite and durable wake discovery"),
        "unexpected startup failure: {error:#}"
    );
    // Reuse the directory immediately: a failed activation must join its domain
    // and release the directory lock, not merely return an error to the caller.
    let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let (config, services) = inputs("events", &data, &core, Some(bucket), 0).await?;
    let mut node =
        Node::start(config, services, graph(1, false), applications("original")?).await?;
    // Resource activation and the independent fleet observer both contribute
    // to readiness; the first observer sample can precede lease publication.
    super::ready(&node).await?;
    ensure!(
        node.is_ready().await?,
        "resource activation did not finish before startup returned"
    );
    let client = client()?;
    ensure!(call(&client, &node, reqwest::Method::POST, "/write").await? == "original:1");
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if call(&client, &node, reqwest::Method::GET, "/events").await? == "1" {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    ensure!(call(&client, &node, reqwest::Method::POST, "/process-retry").await? == "1");
    ensure!(
        call(&client, &node, reqwest::Method::POST, "/process-ack").await? == "1",
        "redelivery repeated the committed local effect"
    );
    ensure!(
        node.adopt(graph(2, true), applications("invalid")?)
            .await
            .is_err(),
        "unresolved resource binding was adopted"
    );
    ensure!(
        call(&client, &node, reqwest::Method::GET, "/read").await? == "original:1",
        "failed resource activation replaced the serving generation"
    );
    node.adopt(graph(2, false), applications("replacement")?)
        .await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if call(&client, &node, reqwest::Method::GET, "/read").await? == "replacement:1" {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    ensure!(call(&client, &node, reqwest::Method::POST, "/retire").await? == "retiring");
    let mut external = graph(2, false);
    external.primary.event_streams.clear();
    ensure!(
        node.adopt(external, applications("retiring-external")?)
            .await
            .is_err(),
        "external binding ignored the catalog retirement lifecycle"
    );
    ensure!(
        node.adopt(graph(1, false), applications("obsolete")?)
            .await
            .is_err(),
        "historical active configuration receipt revived a retired resource"
    );
    ensure!(call(&client, &node, reqwest::Method::GET, "/read").await? == "replacement:1");
    stopped(node).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn node_refuses_unsupported_storage_and_peer_protocol_before_admission() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let core = Executor::new("event-cutover-core")?;
    let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let previous = br#"{"system":"cellar","format":2}"#.to_vec();
    let predecessor = serde_json::to_vec(
        &serde_json::json!({"node":"events","addr":"127.0.0.1:1","expires_ms":0,
        "peer_protocol":5,"paced_handoff":true,"probe_public_key":"prior-key","generation":"prior-generation"}),
    )?;
    let operator = ExecutionDomain::new(tokio::runtime::Handle::current());
    operator
        .run(async {
            bucket.put("wake/format.json", previous.clone()).await?;
            bucket.put("nodes/events.json", predecessor.clone()).await?;
            Ok::<_, anyhow::Error>(())
        })
        .await??;
    let data = directory.path().join("node");
    let (config, services) = inputs("events", &data, &core, Some(bucket.clone()), 0).await?;
    let error = match Node::start(
        config,
        services,
        graph(1, false),
        applications("not-admitted")?,
    )
    .await
    {
        Ok(node) => {
            drop(node);
            anyhow::bail!("prior fleet format admitted new event semantics");
        }
        Err(error) => error,
    };
    ensure!(
        format!("{error:#}").contains("unsupported fleet format"),
        "unexpected startup failure: {error:#}"
    );
    operator
        .run(async {
            ensure!(bucket.get("wake/format.json").await?.unwrap().0.as_ref() == previous);
            ensure!(
                bucket.get("nodes/events.json").await?.unwrap().0.as_ref() == predecessor,
                "format refusal overwrote predecessor authority"
            );
            bucket.delete("wake/format.json").await?;
            bucket.delete("nodes/events.json").await?;
            cellar::wake_format::ensure_ready(&bucket).await?;
            Ok::<_, anyhow::Error>(())
        })
        .await??;
    operator.shutdown().await;
    let (mut config, services) = inputs("events", &data, &core, Some(bucket.clone()), 0).await?;
    config.actor.core.peer_protocol = 2;
    ensure!(
        Node::start(
            config,
            services,
            graph(1, false),
            applications("bad-policy")?
        )
        .await
        .is_err(),
        "actor advertised an incompatible peer protocol"
    );
    let (config, services) = inputs("events", &data, &core, Some(bucket), 0).await?;
    let node = Node::start(config, services, graph(1, false), applications("fresh")?).await?;
    let sender = cellar::peer_auth::PeerAuth::new([11; 32], "previous-peer")?;
    let mut headers = sender.signed_headers("GET", "/peer/probe", &[], "events")?;
    headers.insert(cellar::peer_auth::RESPONSE_VERSION_HEADER, "6".parse()?);
    let reply = client()?
        .get(format!(
            "http://{}/peer/probe",
            node.info().internal_address
        ))
        .headers(headers)
        .send()
        .await?;
    ensure!(
        reply.status() == reqwest::StatusCode::UPGRADE_REQUIRED,
        "previous peer admitted: {}",
        reply.status()
    );
    ensure!(reply.headers()[cellar::peer_auth::RESPONSE_VERSION_HEADER] == "1");
    // An otherwise authenticated peer with an unsupported tunnel version
    // is refused by the tunnel gate independently.
    let headers = sender.signed_headers("GET", "/peer/tunnel", &[], "events")?;
    let reply = client()?
        .get(format!(
            "http://{}/peer/tunnel",
            node.info().internal_address
        ))
        .headers(headers)
        .header("Upgrade", "cellar-tunnel")
        .header("Connection", "upgrade")
        .header("x-cells-tunnel-version", "5")
        .header("x-cells-tunnel-kind", "rpc")
        .send()
        .await?;
    ensure!(
        reply.status() == reqwest::StatusCode::UPGRADE_REQUIRED,
        "previous tunnel version admitted: {}",
        reply.status()
    );
    stopped(node).await?;
    Ok(())
}

async fn operator(
    client: &reqwest::Client,
    node: &Node,
    scope: &str,
    body: &serde_json::Value,
    caller: &str,
) -> Result<serde_json::Value> {
    let path = format!("/outbox/{scope}");
    let body = serde_json::to_vec(body)?;
    let auth = PeerAuth::new([11; 32], caller)?;
    let request = auth.sign(
        client
            .post(format!("http://{}{path}", node.info().internal_address))
            .body(body.clone()),
        "POST",
        &path,
        &body,
        &node.info().node,
    )?;
    let response = request.send().await?;
    ensure!(
        response.status() == 200,
        "operator response {}: {}",
        response.status(),
        response.text().await?
    );
    Ok(response.json().await?)
}

fn stream_scope() -> Result<String> {
    let resource = resource();
    let name = serde_json::to_string(&(resource.tenant, resource.application, resource.id, 0u32))?;
    let id = cellar_native::cell_id::for_name("cells:v1:event-stream:__EventStream", &name);
    Ok(format!(
        "__EventStream:{}",
        cellar_native::cell_id::encode_hex(&id)
    ))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retirement_operator_coordinates_or_forces_and_recovers_terminal_receipts() -> Result<()> {
    use serde_json::json;
    for force in [false, true] {
        let directory = tempfile::tempdir()?;
        let core = Executor::new("retirement-operator-core")?;
        let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
        let (config, services) = inputs(
            "retirement-owner",
            &directory.path().join("owner"),
            &core,
            Some(bucket.clone()),
            0,
        )
        .await?;
        let owner = Node::start(config, services, graph(1, false), applications("owner")?).await?;
        super::ready(&owner).await?;
        let client = client()?;
        ensure!(call(&client, &owner, reqwest::Method::POST, "/write").await? == "owner:1");
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if call(&client, &owner, reqwest::Method::GET, "/events").await? == "1" {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await??;
        let (config, services) = inputs(
            "retirement-entrance",
            &directory.path().join("entrance"),
            &core,
            Some(bucket),
            0,
        )
        .await?;
        let mut external = graph(1, false);
        external.primary.event_streams.clear();
        let mut entrance =
            Node::start(config, services, external, applications("entrance")?).await?;
        super::ready(&entrance).await?;
        let admin = binding(true);
        let operation = "00000000000000000000000000000080";
        let coordinated = Retirement {
            operation: operation.into(),
            revision: 1,
            reason: "operator retirement qualification".into(),
            mode: RetirementMode::Coordinated {
                procedure: "all producers quiesced including never-contacted sources".into(),
            },
        };
        let command = json!({"kind":"retire","request":coordinated});
        ensure!(
            stream_operator(
                &client,
                &entrance,
                &admin,
                command.clone(),
                "events-operator"
            )
            .await?["error"]["code"]
                == "PendingDelivery"
        );
        let status = json!({"kind":"retirement_status","operation":operation});
        ensure!(
            stream_operator(
                &client,
                &entrance,
                &admin,
                status.clone(),
                "events-operator"
            )
            .await?["error"]["code"]
                == "OperationUnknown"
        );
        let mut stale = command.clone();
        stale["request"]["revision"] = "2".into();
        ensure!(
            stream_operator(&client, &entrance, &admin, stale, "events-operator").await?["error"]
                ["code"]
                == "ConfigConflict"
        );
        let mut forged = binding(false);
        forged.administer = true;
        ensure!(
            stream_operator(
                &client,
                &entrance,
                &forged,
                command.clone(),
                "events-operator"
            )
            .await?["error"]["code"]
                == "Forbidden"
        );
        let command = if force {
            let mut forced = coordinated.clone();
            forced.mode = RetirementMode::Force {
                data_loss_acknowledgment: "discard history and block outstanding obligations"
                    .into(),
            };
            json!({"kind":"retire","request":forced})
        } else {
            // Producer seal is a separate source transaction, not an outbox
            // drain inferred from the stream's observed producer list.
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let response = client
                        .post(format!("http://{}/seal", owner.info().public_address))
                        .send()
                        .await?;
                    if response.status() == 200 {
                        let seal: serde_json::Value =
                            serde_json::from_str(&response.text().await?)?;
                        if seal["state"] == "accepted" {
                            return Ok::<_, anyhow::Error>(());
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await??;
            ensure!(
                stream_operator(
                    &client,
                    &entrance,
                    &admin,
                    command.clone(),
                    "events-operator"
                )
                .await?["error"]["code"]
                    == "PendingDelivery",
                "undrained subscription permitted retirement"
            );
            ensure!(call(&client, &owner, reqwest::Method::POST, "/process-ack").await? == "1");
            command
        };
        let accepted = stream_operator(
            &client,
            &entrance,
            &admin,
            command.clone(),
            "events-operator",
        )
        .await?;
        ensure!(
            accepted["result"]["resource"] == resource().id
                && matches!(
                    accepted["result"]["state"].as_str(),
                    Some("retiring" | "deleted")
                ),
            "retirement not accepted: {accepted}"
        );
        ensure!(
            stream_operator(
                &client,
                &entrance,
                &admin,
                command.clone(),
                "other-operator"
            )
            .await?["error"]["code"]
                == "Forbidden"
        );
        let mut conflict = command.clone();
        conflict["request"]["reason"] = "different intent".into();
        ensure!(
            stream_operator(&client, &entrance, &admin, conflict, "events-operator").await?
                ["error"]["code"]
                == "ConfigConflict"
        );
        stopped(owner).await?;
        let terminal = tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                let receipt = stream_operator(
                    &client,
                    &entrance,
                    &admin,
                    status.clone(),
                    "events-operator",
                )
                .await?;
                if receipt["result"]["state"] == "deleted" {
                    return Ok::<_, anyhow::Error>(receipt);
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await??;
        ensure!(terminal["result"]["operator"] == "events-operator");
        // Bounded retention can reclaim an acknowledged event before closure.
        // The terminal range always corresponds to actual retained records.
        let records: i64 = terminal["result"]["records"].as_str().unwrap().parse()?;
        if records == 0 {
            ensure!(terminal["result"]["affected"].is_null());
        } else {
            ensure!(
                records == 1 && terminal["result"]["affected"] == json!({"first":"1","last":"1"})
            );
        }
        if force {
            ensure!(records == 1, "forced retirement lost its captured range");
        }
        ensure!(
            stream_operator(
                &client,
                &entrance,
                &admin,
                command.clone(),
                "events-operator"
            )
            .await?
                == terminal
        );
        ensure!(
            stream_operator(&client, &entrance, &admin, status, "other-operator").await?["error"]
                ["code"]
                == "Forbidden"
        );
        ensure!(
            stream_operator(
                &client,
                &entrance,
                &binding(false),
                json!({"kind":"read","cursor":null,"limit":1,"max_bytes":4096,"payload":false}),
                "events-operator"
            )
            .await?["error"]["code"]
                == "DestinationDeleted"
        );
        // Terminal resource state cannot be revived by a historic deployment.
        ensure!(entrance
            .adopt(graph(1, false), applications("stale")?)
            .await
            .is_err());
        stopped(entrance).await?;
    }
    Ok(())
}

async fn stream_operator(
    client: &reqwest::Client,
    node: &Node,
    binding: &Binding,
    command: serde_json::Value,
    caller: &str,
) -> Result<serde_json::Value> {
    let scope = if matches!(
        command["kind"].as_str(),
        Some("retire" | "retirement_status")
    ) {
        let resource = resource();
        let name = serde_json::to_string(&(resource.tenant, resource.application))?;
        let id = cellar_native::cell_id::for_name("cells:v1:event-catalog:__EventCatalog", &name);
        format!("__EventCatalog:{}", cellar_native::cell_id::encode_hex(&id))
    } else {
        stream_scope()?
    };
    let path = format!("/streams/{scope}");
    let body = serde_json::to_vec(&serde_json::json!({"binding":binding,"command":command}))?;
    let auth = PeerAuth::new([11; 32], caller)?;
    let request = auth.sign(
        client
            .post(format!("http://{}{path}", node.info().internal_address))
            .body(body.clone()),
        "POST",
        &path,
        &body,
        &node.info().node,
    )?;
    let response = request.send().await?;
    ensure!(
        response.status() == 200,
        "stream operator response {}: {}",
        response.status(),
        response.text().await?
    );
    Ok(response.json().await?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_operator_routes_controls_preserves_receipts_and_checks_revocation() -> Result<()> {
    use serde_json::json;
    let directory = tempfile::tempdir()?;
    let core = Executor::new("stream-operator-core")?;
    let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let (config, services) = inputs(
        "stream-owner",
        &directory.path().join("owner"),
        &core,
        Some(bucket.clone()),
        0,
    )
    .await?;
    let owner = Node::start(config, services, graph(1, false), applications("owner")?).await?;
    super::ready(&owner).await?;
    let client = client()?;
    let path = format!("/streams/{}", stream_scope()?);
    ensure!(
        client
            .post(format!("http://{}{path}", owner.info().internal_address))
            .json(&json!({}))
            .send()
            .await?
            .status()
            == 401
    );
    ensure!(
        client
            .post(format!("http://{}{path}", owner.info().internal_address))
            .body("x".repeat((16 << 10) + 1))
            .send()
            .await?
            .status()
            == 413
    );
    ensure!(call(&client, &owner, reqwest::Method::POST, "/write").await? == "owner:1");
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if call(&client, &owner, reqwest::Method::GET, "/events").await? == "1" {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    let (config, services) = inputs(
        "stream-entrance",
        &directory.path().join("entrance"),
        &core,
        Some(bucket),
        0,
    )
    .await?;
    let mut external = graph(1, false);
    external.primary.event_streams.clear();
    let entrance = Node::start(config, services, external, applications("entrance")?).await?;
    super::ready(&entrance).await?;
    let admin = binding(true);
    let reader = binding(false);
    let described = stream_operator(
        &client,
        &entrance,
        &admin,
        json!({"kind":"describe"}),
        "streams",
    )
    .await?;
    ensure!(described["result"]["rows"] == "1");
    ensure!(described["result"]["observation"]["protected_bytes"] == "0");
    ensure!(described["result"]["observation"]["active_leases"] == "0");
    ensure!(
        described["result"]["observation"]["sqlite_allocated_bytes"]
            .as_str()
            .unwrap()
            .parse::<i64>()?
            > 0
    );
    ensure!(described["result"]["observation"]["oldest_retained_age_ms"].is_string());
    let read = json!({"kind":"read","cursor":null,"limit":1,"max_bytes":4096,"payload":false});
    let page = stream_operator(&client, &entrance, &reader, read.clone(), "streams").await?;
    ensure!(page["result"]["records"][0]["event"]["payload"].is_null());
    ensure!(
        page["result"]["records"][0]["envelope"].is_null(),
        "encoded envelope leaked payload"
    );
    let mut include = read.clone();
    include["payload"] = true.into();
    ensure!(
        stream_operator(&client, &entrance, &reader, include, "streams").await?["result"]
            ["records"][0]["event"]["payload"]["value"]
            == 1
    );
    let mut resumed = read;
    resumed["cursor"] = page["result"]["cursor"].clone();
    ensure!(
        stream_operator(&client, &entrance, &reader, resumed, "streams").await?["result"]
            ["records"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let mut forged = reader.clone();
    forged.administer = true;
    ensure!(
        stream_operator(
            &client,
            &entrance,
            &forged,
            json!({"kind":"describe"}),
            "streams"
        )
        .await?["error"]["code"]
            == "Forbidden"
    );
    let status = json!({"kind":"subscription","name":"projection"});
    let original = stream_operator(&client, &entrance, &admin, status.clone(), "subscriptions")
        .await?["result"]["identity"]
        .clone();
    let pause = json!({"kind":"change","change":{"operation":"00000000000000000000000000000070","expected":original,"action":{"kind":"pause"},"reason":"operator test pause"}});
    let paused =
        stream_operator(&client, &entrance, &admin, pause.clone(), "subscriptions").await?;
    ensure!(
        paused["result"]["state"] == "paused" && paused["result"]["operator"] == "subscriptions"
    );
    ensure!(
        stream_operator(&client, &entrance, &admin, pause.clone(), "subscriptions").await?
            == paused
    );
    ensure!(
        stream_operator(&client, &entrance, &admin, pause.clone(), "other-operator").await?
            ["error"]["code"]
            == "Forbidden"
    );
    let mut conflict = pause.clone();
    conflict["change"]["reason"] = "other intent".into();
    ensure!(
        stream_operator(&client, &entrance, &admin, conflict, "subscriptions").await?["error"]
            ["code"]
            == "ConfigConflict"
    );
    let reset = json!({"kind":"change","change":{"operation":"00000000000000000000000000000071","expected":original,"action":{"kind":"reset","cursor":{"resource":resource().id,"partition":0,"next":"2"}},"reason":"explicit test skip"}});
    let reset = stream_operator(&client, &entrance, &admin, reset, "subscriptions").await?;
    ensure!(reset["result"]["checkpoint"] == "1" && reset["result"]["skipped"]["first"] == "1");
    let mut stale = pause.clone();
    stale["change"]["operation"] = "00000000000000000000000000000072".into();
    ensure!(
        stream_operator(&client, &entrance, &admin, stale, "subscriptions").await?["error"]["code"]
            == "SubscriptionReset"
    );
    let resume = json!({"kind":"change","change":{"operation":"00000000000000000000000000000072","expected":reset["result"]["current"],"action":{"kind":"resume"},"reason":"resume after reset"}});
    let resumed = stream_operator(&client, &entrance, &admin, resume, "subscriptions").await?;
    ensure!(resumed["result"]["state"] == "active");
    let audit = stream_operator(
        &client,
        &entrance,
        &admin,
        json!({"kind":"audit","after":"0","limit":1}),
        "streams",
    )
    .await?;
    ensure!(
        audit["result"]["records"].as_array().unwrap().len() == 1
            && audit["result"]["has_more"] == true
    );
    let next = stream_operator(
        &client,
        &entrance,
        &admin,
        json!({"kind":"audit","after":audit["result"]["next"],"limit":1}),
        "streams",
    )
    .await?;
    ensure!(
        next["result"]["records"][0]["receipt"]["operation"] == "00000000000000000000000000000071"
    );
    let operation = json!({"kind":"status","operation":"00000000000000000000000000000070"});
    ensure!(
        stream_operator(
            &client,
            &entrance,
            &admin,
            operation.clone(),
            "subscriptions"
        )
        .await?
            == paused
    );
    ensure!(
        stream_operator(
            &client,
            &entrance,
            &admin,
            json!({"kind":"status","operation":"00000000000000000000000000000099"}),
            "subscriptions"
        )
        .await?["error"]["code"]
            == "OperationUnknown"
    );
    stopped(owner).await?;
    ensure!(stream_operator(&client, &entrance, &admin, pause, "subscriptions").await? == paused);
    ensure!(
        stream_operator(
            &client,
            &entrance,
            &admin,
            operation.clone(),
            "subscriptions"
        )
        .await?
            == paused
    );
    let deletion = json!({"kind":"change","change":{"operation":"00000000000000000000000000000073","expected":resumed["result"]["current"],"action":{"kind":"delete","force":false},"reason":"explicit test deletion"}});
    let deleted = stream_operator(
        &client,
        &entrance,
        &admin,
        deletion.clone(),
        "subscriptions",
    )
    .await?;
    ensure!(deleted["result"]["state"] == "deleted");
    ensure!(
        stream_operator(&client, &entrance, &admin, deletion, "subscriptions").await? == deleted
    );
    ensure!(
        stream_operator(&client, &entrance, &admin, status, "subscriptions").await?["error"]
            ["code"]
            == "SubscriptionReset"
    );
    ensure!(call(&client, &entrance, reqwest::Method::POST, "/revoke-admin").await? == "active");
    ensure!(
        stream_operator(&client, &entrance, &admin, operation, "subscriptions").await?["error"]
            ["code"]
            == "Forbidden"
    );
    stopped(entrance).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn subscription_inventory_pages_tombstones_and_recovers_owner_handoff() -> Result<()> {
    use serde_json::{json, Value};
    let directory = tempfile::tempdir()?;
    let core = Executor::new("subscription-inventory-core")?;
    let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let (config, services) = inputs(
        "inventory-owner",
        &directory.path().join("owner"),
        &core,
        Some(bucket.clone()),
        0,
    )
    .await?;
    let mut declarations = graph(1, false);
    for name in ["audit", "mail"] {
        declarations.primary.event_streams[0]
            .subscriptions
            .push(SubscriptionDefinition {
                name: name.into(),
                start: Start::Earliest,
                required: false,
            });
    }
    let owner = Node::start(config, services, declarations, applications("owner")?).await?;
    super::ready(&owner).await?;
    let client = client()?;
    ensure!(call(&client, &owner, reqwest::Method::POST, "/write").await? == "owner:1");
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if call(&client, &owner, reqwest::Method::GET, "/events").await? == "1" {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    ensure!(call(&client, &owner, reqwest::Method::GET, "/hold-projection").await? == "leased:1");
    let sdk: SubscriptionPage = serde_json::from_str(
        &call(
            &client,
            &owner,
            reqwest::Method::GET,
            "/subscription-inventory",
        )
        .await?,
    )?;
    ensure!(sdk.records.len() == 3 && sdk.cursor.is_none());
    let leased = sdk
        .records
        .iter()
        .find(|record| record.identity.name == "projection")
        .unwrap();
    ensure!(leased
        .lease
        .as_ref()
        .is_some_and(|lease| lease.first == 1 && lease.last == 1));
    ensure!(leased.observation.lag_records == 1 && leased.observation.retained_lag_records == 1);
    ensure!(leased.observation.lease_active && !leased.observation.history_expired);
    ensure!(leased.observation.oldest_retained_age_ms.is_some());
    let (config, services) = inputs(
        "inventory-entrance",
        &directory.path().join("entrance"),
        &core,
        Some(bucket),
        0,
    )
    .await?;
    let mut external = graph(1, false);
    external.primary.event_streams.clear();
    let entrance = Node::start(config, services, external, applications("entrance")?).await?;
    super::ready(&entrance).await?;
    let admin = binding(true);
    let command = json!({"kind":"subscriptions","cursor":null,"limit":1});
    let first = stream_operator(
        &client,
        &entrance,
        &admin,
        command.clone(),
        "events-operator",
    )
    .await?;
    let mut records = first["result"]["records"].as_array().unwrap().clone();
    ensure!(records.len() == 1);
    let saved_cursor = first["result"]["cursor"].clone();
    ensure!(saved_cursor.is_string());
    stopped(owner).await?;
    let mut cursor = saved_cursor.clone();
    for _ in 0..3 {
        let page = stream_operator(
            &client,
            &entrance,
            &admin,
            json!({"kind":"subscriptions","cursor":cursor,"limit":1}),
            "events-operator",
        )
        .await?;
        let rows = page["result"]["records"].as_array().unwrap();
        ensure!(rows.len() <= 1);
        records.extend(rows.iter().cloned());
        cursor = page["result"]["cursor"].clone();
        if cursor.is_null() {
            break;
        }
    }
    ensure!(cursor.is_null() && records.len() == 3);
    let ids: Vec<_> = records
        .iter()
        .map(|record| record["identity"]["id"].as_str().unwrap())
        .collect();
    ensure!(ids.windows(2).all(|pair| pair[0] < pair[1]));
    ensure!(
        ids == sdk
            .records
            .iter()
            .map(|record| record.identity.id.as_str())
            .collect::<Vec<_>>()
    );
    for record in &records {
        ensure!(record["observation"]["lag_records"] == "1");
        ensure!(record["observation"]["retained_lag_records"] == "1");
        ensure!(record["observation"]["oldest_retained_age_ms"].is_string());
        ensure!(record["observation"]["history_expired"] == false);
    }
    let encoded = serde_json::to_string(&records)?;
    ensure!(
        !encoded.contains("token") && !encoded.contains("payload") && !encoded.contains("envelope")
    );
    for (index, record) in records.iter().enumerate() {
        let receipt = stream_operator(&client,&entrance,&admin,json!({"kind":"change","change":{"operation":format!("{:032x}",160+index),"expected":record["identity"],"action":{"kind":"delete","force":true},"reason":"inventory tombstone inspection"}}),"events-operator").await?;
        ensure!(receipt["result"]["state"] == "deleted");
    }
    let tombstones = stream_operator(
        &client,
        &entrance,
        &admin,
        json!({"kind":"subscriptions","cursor":null,"limit":100}),
        "events-operator",
    )
    .await?;
    let rows = tombstones["result"]["records"].as_array().unwrap();
    ensure!(
        rows.len() == 3
            && rows
                .iter()
                .all(|row| row["state"] == "deleted" && row["lease"].is_null())
    );
    ensure!(tombstones["result"]["cursor"] == Value::Null);
    ensure!(call(&client, &entrance, reqwest::Method::POST, "/revoke-admin").await? == "active");
    ensure!(
        stream_operator(
            &client,
            &entrance,
            &admin,
            json!({"kind":"subscriptions","cursor":saved_cursor,"limit":1}),
            "events-operator"
        )
        .await?["error"]["code"]
            == "Forbidden"
    );
    stopped(entrance).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_inventory_preserves_observed_watermarks_seals_and_owner_handoff() -> Result<()> {
    use serde_json::{json, Value};
    let directory = tempfile::tempdir()?;
    let core = Executor::new("source-inventory-core")?;
    let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let (config, services) = inputs(
        "source-inventory-owner",
        &directory.path().join("owner"),
        &core,
        Some(bucket.clone()),
        0,
    )
    .await?;
    let mut declarations = graph(1, false);
    declarations.primary.event_streams[0].max_age_ms = 500;
    let owner = Node::start(config, services, declarations, applications("owner")?).await?;
    super::ready(&owner).await?;
    let client = client()?;
    let empty: SourcePage = serde_json::from_str(
        &call(&client, &owner, reqwest::Method::GET, "/source-inventory").await?,
    )?;
    ensure!(empty.records.is_empty() && empty.cursor.is_none());
    for name in ["alpha", "bravo", "charlie"] {
        ensure!(
            call(
                &client,
                &owner,
                reqwest::Method::POST,
                &format!("/write?source={name}")
            )
            .await?
                == "owner:1"
        );
    }
    for name in ["alpha", "bravo", "charlie"] {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if call(
                    &client,
                    &owner,
                    reqwest::Method::GET,
                    &format!("/outbox-pending?source={name}"),
                )
                .await?
                    == "0"
                {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await??;
    }
    call(&client, &owner, reqwest::Method::POST, "/seal?source=alpha").await?;
    let sdk = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let page: SourcePage = serde_json::from_str(
                &call(&client, &owner, reqwest::Method::GET, "/source-inventory").await?,
            )?;
            if page.records.len() == 3 && page.records.iter().filter(|row| row.sealed).count() == 1
            {
                return Ok::<_, anyhow::Error>(page);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    ensure!(
        sdk.records
            .iter()
            .filter(|row| row.accepted_through == 1)
            .count()
            == 3
    );
    ensure!(sdk
        .records
        .iter()
        .all(|row| row.principal == "embedded-node" && row.grant == "application"));
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if call(&client, &owner, reqwest::Method::GET, "/events").await? == "0" {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    let retained: SourcePage = serde_json::from_str(
        &call(&client, &owner, reqwest::Method::GET, "/source-inventory").await?,
    )?;
    ensure!(serde_json::to_value(&retained.records)? == serde_json::to_value(&sdk.records)?);
    let (config, services) = inputs(
        "source-inventory-entrance",
        &directory.path().join("entrance"),
        &core,
        Some(bucket),
        0,
    )
    .await?;
    let mut external = graph(1, false);
    external.primary.event_streams.clear();
    let entrance = Node::start(config, services, external, applications("entrance")?).await?;
    super::ready(&entrance).await?;
    let admin = binding(true);
    let first = stream_operator(
        &client,
        &entrance,
        &admin,
        json!({"kind":"sources","cursor":null,"limit":1}),
        "events-operator",
    )
    .await?;
    let mut records = first["result"]["records"].as_array().unwrap().clone();
    ensure!(records.len() == 1);
    let saved_cursor = first["result"]["cursor"].clone();
    ensure!(saved_cursor.is_string());
    let mut false_admin = binding(false);
    false_admin.administer = true;
    ensure!(
        stream_operator(
            &client,
            &entrance,
            &false_admin,
            json!({"kind":"sources","cursor":null,"limit":1}),
            "events-operator"
        )
        .await?["error"]["code"]
            == "Forbidden"
    );
    stopped(owner)
        .await
        .context("source inventory owner handoff")?;
    let mut cursor = saved_cursor.clone();
    for _ in 0..4 {
        let page = stream_operator(
            &client,
            &entrance,
            &admin,
            json!({"kind":"sources","cursor":cursor,"limit":1}),
            "events-operator",
        )
        .await?;
        let rows = page["result"]["records"].as_array().unwrap();
        ensure!(rows.len() <= 1);
        records.extend(rows.iter().cloned());
        cursor = page["result"]["cursor"].clone();
        if cursor.is_null() {
            break;
        }
    }
    ensure!(cursor.is_null() && records.len() == 3);
    let expected = serde_json::to_value(&sdk.records)?;
    ensure!(
        Value::Array(records.clone()) == expected,
        "source inventory changed during owner handoff"
    );
    let encoded = serde_json::to_string(&records)?;
    ensure!(
        !encoded.contains("payload") && !encoded.contains("envelope") && !encoded.contains("token")
    );
    let path = format!("/streams/{}", stream_scope()?);
    let body = serde_json::to_vec(
        &json!({"binding":admin,"command":{"kind":"sources","cursor":"subs1.%%%","limit":1}}),
    )?;
    let auth = PeerAuth::new([11; 32], "events-operator")?;
    let response = auth
        .sign(
            client
                .post(format!("http://{}{path}", entrance.info().internal_address))
                .body(body.clone()),
            "POST",
            &path,
            &body,
            &entrance.info().node,
        )?
        .send()
        .await?;
    ensure!(response.status() == reqwest::StatusCode::BAD_REQUEST);
    ensure!(response.text().await?.contains("InvalidCursor"));
    ensure!(call(&client, &entrance, reqwest::Method::POST, "/revoke-admin").await? == "active");
    ensure!(
        stream_operator(
            &client,
            &entrance,
            &admin,
            json!({"kind":"sources","cursor":saved_cursor,"limit":1}),
            "events-operator"
        )
        .await?["error"]["code"]
            == "Forbidden"
    );
    stopped(entrance)
        .await
        .context("source inventory entrance shutdown")?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_operator_routes_to_owner_replays_receipts_and_recovers_handoff() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let core = Executor::new("source-operator-core")?;
    let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let constructors = Arc::new(AtomicUsize::new(0));
    let (config, services) = inputs(
        "source-owner",
        &directory.path().join("owner"),
        &core,
        Some(bucket.clone()),
        0,
    )
    .await?;
    let owner = Node::start(
        config,
        services,
        graph(1, false),
        applications_with_probe("owner", Some(constructors.clone()))?,
    )
    .await?;
    super::ready(&owner).await?;
    let client = client()?;
    let script = "embedded-node";
    let namespace = format!("cells:v1:{}:{script}:Account", script.len());
    let cell_id = cellar_native::cell_id::for_name(&namespace, "events");
    let scope = format!("Account:{}", cellar_native::cell_id::encode_hex(&cell_id));
    let before = constructors.load(Ordering::SeqCst);
    let unsigned = client
        .post(format!(
            "http://{}/outbox/{scope}",
            owner.info().internal_address
        ))
        .json(&serde_json::json!({"op":"inspect"}))
        .send()
        .await?;
    ensure!(
        unsigned.status() == 401,
        "unsigned source inspection admitted"
    );
    let oversized = client
        .post(format!(
            "http://{}/outbox/{scope}",
            owner.info().internal_address
        ))
        .body("x".repeat((16 << 10) + 1))
        .send()
        .await?;
    ensure!(
        oversized.status() == 413,
        "source ingress did not enforce its byte budget"
    );
    let empty = operator(
        &client,
        &owner,
        &scope,
        &serde_json::json!({"op":"inspect"}),
        "outbox",
    )
    .await?;
    ensure!(empty["result"]["incarnation"].is_null());
    ensure!(
        constructors.load(Ordering::SeqCst) == before,
        "framework inspection invoked the application constructor"
    );
    ensure!(
        call(&client, &owner, reqwest::Method::GET, "/forge-source")
            .await?
            .contains("framework RPC requires host authority"),
        "ordinary application RPC forged source authority"
    );
    ensure!(call(&client, &owner, reqwest::Method::POST, "/revoke-publish").await? == "active");
    ensure!(call(&client, &owner, reqwest::Method::POST, "/write").await? == "owner:1");
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let inspected = operator(
                &client,
                &owner,
                &scope,
                &serde_json::json!({"op":"inspect"}),
                "outbox",
            )
            .await?;
            if inspected["result"]["lanes"][0]["status"] == "blocked" {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    // Restore the deployment's accepted definition before admitting a second
    // node. The durable source obligation remains blocked until explicit retry.
    ensure!(call(&client, &owner, reqwest::Method::POST, "/restore-publish").await? == "active");
    let (config, services) = inputs(
        "source-entrance",
        &directory.path().join("entrance"),
        &core,
        Some(bucket),
        0,
    )
    .await?;
    let mut external = graph(3, false);
    external.primary.event_streams.clear();
    let entrance = Node::start(config, services, external, applications("entrance")?).await?;
    super::ready(&entrance).await?;
    let page = operator(
        &client,
        &entrance,
        &scope,
        &serde_json::json!({"op":"inspect","limit":1}),
        "outbox",
    )
    .await?;
    ensure!(page["result"]["pending_rows"] == "1");
    ensure!(page["result"]["lanes"][0]["blocked_reason"] == "Forbidden");
    ensure!(page["result"]["observation"]["oldest_pending_age_ms"].is_string());
    let reasons = page["result"]["observation"]["blocked_lanes"]
        .as_array()
        .unwrap();
    ensure!(
        reasons.len() == 7
            && reasons
                .iter()
                .find(|count| count["reason"] == "Forbidden")
                .unwrap()["lanes"]
                == "1"
    );
    let expected = page["result"]["lanes"][0]["revision"].clone();
    ensure!(expected["source"] == scope);
    let request = serde_json::json!({"op":"retry","change":{"operation":"00000000000000000000000000000055","reason":"retry after restoring destination publication","expected":expected}});
    let first = operator(&client, &entrance, &scope, &request, "outbox").await?;
    ensure!(
        first["result"]["rearmed"] == true,
        "owner-routed retry did not rearm pending delivery: {first}"
    );
    // Treat the first reply as lost. Replaying the same operation cannot run
    // the retry again even after dispatch has changed its observed revision.
    ensure!(operator(&client, &entrance, &scope, &request, "outbox").await? == first);
    let mut changed = request.clone();
    changed["change"]["reason"] = "changed intent".into();
    ensure!(
        operator(&client, &entrance, &scope, &changed, "outbox").await?["error"]["code"]
            == "ConfigConflict"
    );
    ensure!(
        operator(&client, &entrance, &scope, &request, "other-operator").await?["error"]["code"]
            == "Forbidden"
    );
    let status = serde_json::json!({"op":"status","operation":"00000000000000000000000000000055"});
    ensure!(operator(&client, &entrance, &scope, &status, "outbox").await? == first);
    let mut stale = request.clone();
    stale["change"]["operation"] = "00000000000000000000000000000056".into();
    ensure!(
        operator(&client, &entrance, &scope, &stale, "outbox").await?["error"]["code"]
            == "ConfigConflict"
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let page = operator(
                &client,
                &entrance,
                &scope,
                &serde_json::json!({"op":"inspect"}),
                "outbox",
            )
            .await?;
            if page["result"]["pending_rows"] == "0" {
                return Ok::<_, anyhow::Error>(page);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    ensure!(call(&client, &entrance, reqwest::Method::GET, "/events").await? == "1");
    let page = operator(
        &client,
        &entrance,
        &scope,
        &serde_json::json!({"op":"inspect"}),
        "outbox",
    )
    .await?;
    ensure!(page["result"]["observation"]["oldest_pending_age_ms"].is_null());
    ensure!(page["result"]["observation"]["blocked_lanes"]
        .as_array()
        .unwrap()
        .iter()
        .all(|count| count["lanes"] == "0"));
    let settled = serde_json::json!({"op":"retry","change":{"operation":"00000000000000000000000000000057","reason":"record settled producer observation","expected":page["result"]["lanes"][0]["revision"]}});
    ensure!(
        operator(&client, &entrance, &scope, &settled, "outbox").await?["result"]["rearmed"]
            == false
    );
    let audit = operator(
        &client,
        &entrance,
        &scope,
        &serde_json::json!({"op":"audit","limit":1}),
        "outbox",
    )
    .await?;
    ensure!(audit["result"]["records"].as_array().unwrap().len() == 1);
    let next = audit["result"]["next"].clone();
    ensure!(next == "1");
    let rest = operator(
        &client,
        &entrance,
        &scope,
        &serde_json::json!({"op":"audit","limit":1,"after":next}),
        "outbox",
    )
    .await?;
    ensure!(
        rest["result"]["records"][0]["receipt"]["operation"] == "00000000000000000000000000000057"
    );
    ensure!(rest["result"]["next"].is_null());
    stopped(owner).await?;
    let recovered = operator(&client, &entrance, &scope, &request, "outbox").await?;
    ensure!(
        recovered == first,
        "ownership handoff lost the immutable source operation receipt"
    );
    let page = operator(
        &client,
        &entrance,
        &scope,
        &serde_json::json!({"op":"inspect"}),
        "outbox",
    )
    .await?;
    ensure!(
        page["result"]["pending_rows"] == "0"
            && page["result"]["incarnation"] == expected["incarnation"]
    );
    ensure!(call(&client, &entrance, reqwest::Method::POST, "/reset-source").await? == "reset");
    ensure!(
        operator(&client, &entrance, &scope, &status, "outbox").await? == first,
        "business reset discarded source operation receipts"
    );
    let unknown = operator(
        &client,
        &entrance,
        &scope,
        &serde_json::json!({"op":"status","operation":"ffffffffffffffffffffffffffffffff"}),
        "outbox",
    )
    .await?;
    ensure!(unknown["error"]["code"] == "OperationUnknown");
    stopped(entrance).await?;
    Ok(())
}

fn recovery_graph() -> DeploymentGraph {
    let mut deployment = graph(3, false);
    deployment.primary.event_streams.clear();
    deployment.primary.options.do_classes.clear();
    deployment.primary.options.bindings.clear();
    deployment.primary.options.stream_bindings[0].publish = false;
    deployment
}

fn recovery_applications() -> Result<CompiledApplications> {
    CompiledApplications::default().register("embedded-node", "test", || {
        Ok(Arc::new(Registry::new(|context, invocation| {
            Box::pin(async move {
                let Invocation::Fetch(request) = invocation else {
                    anyhow::bail!("reader only serves HTTP");
                };
                let path = url::Url::parse(&request.url)?.path().to_owned();
                let body = if path == "/restore-publish" {
                    context
                        .stream::<Changed>("ADMIN_EVENTS")?
                        .configure(
                            &context,
                            &graph(3, false).primary.event_streams[0],
                            "00000000000000000000000000000089",
                        )
                        .await?
                        .state
                } else if path == "/events" {
                    context
                        .stream::<Changed>("EVENTS")?
                        .read(&context, None, ReadOptions::default())
                        .await?
                        .records
                        .len()
                        .to_string()
                } else {
                    "reader".into()
                };
                Ok(super::response(body))
            })
        })))
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn removed_source_class_recovers_delivery_and_control_without_application_code() -> Result<()>
{
    let directory = tempfile::tempdir()?;
    let core = Executor::new("removed-source-core")?;
    let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let (config, services) = inputs(
        "source-before-removal",
        &directory.path().join("before"),
        &core,
        Some(bucket.clone()),
        0,
    )
    .await?;
    let owner = Node::start(config, services, graph(1, false), applications("original")?).await?;
    super::ready(&owner).await?;
    let client = client()?;
    ensure!(call(&client, &owner, reqwest::Method::POST, "/revoke-publish").await? == "active");
    ensure!(call(&client, &owner, reqwest::Method::POST, "/write").await? == "original:1");
    let scope = format!(
        "Account:{}",
        cellar_native::cell_id::encode_hex(&cellar_native::cell_id::for_name(
            "cells:v1:13:embedded-node:Account",
            "events"
        ))
    );
    let before = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let state = operator(
                &client,
                &owner,
                &scope,
                &serde_json::json!({"op":"inspect"}),
                "outbox",
            )
            .await?;
            if state["result"]["lanes"][0]["status"] == "blocked" {
                return Ok::<_, anyhow::Error>(state);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    stopped(owner).await?;
    // This executable registers no Account class or cell factory at all.
    let (config, services) = inputs(
        "source-after-removal",
        &directory.path().join("after"),
        &core,
        Some(bucket),
        0,
    )
    .await?;
    let mut node =
        Node::start(config, services, recovery_graph(), recovery_applications()?).await?;
    super::ready(&node).await?;
    let state = operator(
        &client,
        &node,
        &scope,
        &serde_json::json!({"op":"inspect"}),
        "outbox",
    )
    .await?;
    ensure!(
        state["result"]["incarnation"] == before["result"]["incarnation"]
            && state["result"]["pending_rows"] == "1",
        "removed source identity/backlog changed"
    );
    ensure!(state["result"]["lanes"][0]["blocked_reason"] == "Forbidden");
    ensure!(state["result"]["observation"]["oldest_pending_age_ms"].is_string());
    ensure!(
        state["result"]["observation"]["blocked_lanes"]
            == before["result"]["observation"]["blocked_lanes"]
    );
    let denied = client
        .get(format!(
            "http://{}/do/{scope}",
            node.info().internal_address
        ))
        .send()
        .await?;
    ensure!(
        !denied.status().is_success()
            && denied
                .text()
                .await?
                .contains("source application class is not deployed"),
        "recovery executor served business code"
    );
    let fresh = client
        .get(format!(
            "http://{}/do/Account:never-created",
            node.info().internal_address
        ))
        .send()
        .await?;
    ensure!(
        !fresh.status().is_success(),
        "unknown fresh class was admitted as a source"
    );
    ensure!(call(&client, &node, reqwest::Method::POST, "/restore-publish").await? == "active");
    let retry = serde_json::json!({"op":"retry","change":{"operation":"00000000000000000000000000000060","reason":"recover retained producer after removing application code","expected":state["result"]["lanes"][0]["revision"]}});
    let receipt = operator(&client, &node, &scope, &retry, "outbox").await?;
    ensure!(receipt["result"]["rearmed"] == true);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let state = operator(
                &client,
                &node,
                &scope,
                &serde_json::json!({"op":"inspect"}),
                "outbox",
            )
            .await?;
            if state["result"]["pending_rows"] == "0" {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    ensure!(call(&client, &node, reqwest::Method::GET, "/events").await? == "1");
    ensure!(operator(&client, &node, &scope, &retry, "outbox").await? == receipt);
    let mut restored = graph(3, false);
    restored.primary.event_streams.clear();
    node.adopt(restored, applications("restored")?).await?;
    ensure!(
        call(&client, &node, reqwest::Method::GET, "/read").await? == "restored:1",
        "restored application could not resume business state"
    );
    ensure!(
        operator(&client, &node, &scope, &retry, "outbox").await? == receipt,
        "restoring the class changed the source receipt"
    );
    stopped(node).await?;
    Ok(())
}

async fn catalog_operator(
    client: &reqwest::Client,
    node: &Node,
    cursor: serde_json::Value,
    limit: usize,
) -> Result<serde_json::Value> {
    let resource = resource();
    let name = serde_json::to_string(&(&resource.tenant, &resource.application))?;
    let id = cellar_native::cell_id::for_name("cells:v1:event-catalog:__EventCatalog", &name);
    let path = format!(
        "/catalogs/__EventCatalog:{}",
        cellar_native::cell_id::encode_hex(&id)
    );
    let body = serde_json::to_vec(
        &serde_json::json!({"namespace":{"tenant":resource.tenant,"application":resource.application},"cursor":cursor,"limit":limit}),
    )?;
    let auth = PeerAuth::new([11; 32], "catalogs-operator")?;
    let response = auth
        .sign(
            client
                .post(format!("http://{}{path}", node.info().internal_address))
                .body(body.clone()),
            "POST",
            &path,
            &body,
            &node.info().node,
        )?
        .send()
        .await?;
    ensure!(
        response.status() == 200,
        "catalog operator response {}: {}",
        response.status(),
        response.text().await?
    );
    Ok(response.json().await?)
}

async fn catalog_retirement_proof(
    client: &reqwest::Client,
    node: &Node,
    resource: &Resource,
) -> Result<serde_json::Value> {
    let name = serde_json::to_string(&(&resource.tenant, &resource.application))?;
    let id = cellar_native::cell_id::for_name("cells:v1:event-catalog:__EventCatalog", &name);
    let path = format!(
        "/catalogs/__EventCatalog:{}",
        cellar_native::cell_id::encode_hex(&id)
    );
    let body = serde_json::to_vec(
        &serde_json::json!({"namespace":{"tenant":resource.tenant,"application":resource.application},"cursor":null,"limit":1,"retired_resource":resource}),
    )?;
    let auth = PeerAuth::new([11; 32], "catalogs-operator")?;
    let response = auth
        .sign(
            client
                .post(format!("http://{}{path}", node.info().internal_address))
                .body(body.clone()),
            "POST",
            &path,
            &body,
            &node.info().node,
        )?
        .send()
        .await?;
    ensure!(
        response.status() == 200,
        "retirement evidence response {}",
        response.status()
    );
    Ok(response.json().await?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catalog_inventory_pages_tombstones_and_recovers_owner_handoff() -> Result<()> {
    use serde_json::{json, Value};
    let directory = tempfile::tempdir()?;
    let core = Executor::new("catalog-inventory-core")?;
    let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let (config, services) = inputs(
        "catalog-inventory-owner",
        &directory.path().join("owner"),
        &core,
        Some(bucket.clone()),
        0,
    )
    .await?;
    let mut declarations = graph(1, false);
    for ordinal in 2..=3 {
        let mut definition = declarations.primary.event_streams[0].clone();
        definition.resource.id = format!("{ordinal:032x}");
        definition.resource.name = format!("extra-{ordinal}");
        declarations.primary.event_streams.push(definition);
    }
    let mut changing = graph(1, false);
    changing.primary.event_streams = declarations.primary.event_streams.clone();
    let mut owner = Node::start(config, services, declarations, applications("owner")?).await?;
    super::ready(&owner).await?;
    let (config, services) = inputs(
        "catalog-inventory-entrance",
        &directory.path().join("entrance"),
        &core,
        Some(bucket),
        0,
    )
    .await?;
    let mut external = graph(1, false);
    external.primary.event_streams.clear();
    let entrance = Node::start(config, services, external, applications("entrance")?).await?;
    super::ready(&entrance).await?;
    let client = client()?;
    let local = catalog_operator(&client, &owner, Value::Null, 100)
        .await
        .context("catalog inventory direct owner")?;
    ensure!(
        local["result"]["records"]
            .as_array()
            .is_some_and(|rows| rows.len() == 3),
        "catalog direct reply: {local}"
    );
    let first = catalog_operator(&client, &entrance, Value::Null, 1)
        .await
        .context("catalog inventory routed first page")?;
    ensure!(first["result"]["records"].as_array().unwrap().len() == 1);
    ensure!(first["result"]["records"][0]["accepted_revision"] == "1");
    let saved = first["result"]["cursor"].clone();
    ensure!(saved.is_string());
    let mut retired = binding(true);
    retired.resource.id = format!("{:032x}", 3);
    retired.resource.name = "extra-3".into();
    // Preserve an admin overlap for revision 2, then revoke both original
    // resource grants. Platform retirement evidence must not depend on them.
    let target = changing
        .primary
        .event_streams
        .iter_mut()
        .find(|definition| definition.resource.id == retired.resource.id)
        .unwrap();
    target.revision = 2;
    let mut replacement = target
        .grants
        .iter()
        .find(|grant| grant.administer)
        .unwrap()
        .clone();
    replacement.id = "replacement-management".into();
    target.grants.push(replacement);
    let mut overlap = graph(1, false);
    overlap.primary.event_streams = changing.primary.event_streams.clone();
    owner.adopt(overlap, applications("owner")?).await?;
    let target = changing
        .primary
        .event_streams
        .iter_mut()
        .find(|definition| definition.resource.id == retired.resource.id)
        .unwrap();
    target.revision = 3;
    target
        .grants
        .retain(|grant| grant.id == "replacement-management");
    owner.adopt(changing, applications("owner")?).await?;
    let original = retired.clone();
    retired.grant = "replacement-management".into();
    ensure!(
        catalog_retirement_proof(&client, &entrance, &retired.resource).await?["error"]["code"]
            == "PendingDelivery"
    );
    let request = Retirement {
        operation: "00000000000000000000000000000099".into(),
        revision: 3,
        reason: "catalog tombstone qualification".into(),
        mode: RetirementMode::Force {
            data_loss_acknowledgment: "explicitly retire this empty test stream".into(),
        },
    };
    let accepted = stream_operator(
        &client,
        &entrance,
        &retired,
        json!({"kind":"retire","request":request}),
        "catalogs-operator",
    )
    .await?;
    ensure!(
        accepted["error"].is_null(),
        "catalog fixture retirement failed: {accepted}"
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let page = catalog_operator(&client, &entrance, Value::Null, 100).await?;
            if page["result"]["records"]
                .as_array()
                .unwrap()
                .iter()
                .any(|entry| entry["state"] == "deleted")
            {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    let proof = catalog_retirement_proof(&client, &entrance, &retired.resource).await?;
    ensure!(proof["result"]["state"] == "deleted" && proof["result"]["revision"] == "3");
    ensure!(proof["result"]["operation"] == "00000000000000000000000000000099");
    ensure!(
        stream_operator(
            &client,
            &entrance,
            &original,
            json!({"kind":"retirement_status","operation":"00000000000000000000000000000099"}),
            "catalogs-operator"
        )
        .await?["error"]["code"]
            == "Forbidden"
    );
    stopped(owner)
        .await
        .context("catalog inventory owner handoff")?;
    let mut records = first["result"]["records"].as_array().unwrap().clone();
    let mut cursor = saved;
    for _ in 0..4 {
        let page = catalog_operator(&client, &entrance, cursor, 1).await?;
        let rows = page["result"]["records"].as_array().unwrap();
        ensure!(rows.len() <= 1);
        records.extend(rows.iter().cloned());
        cursor = page["result"]["cursor"].clone();
        if cursor.is_null() {
            break;
        }
    }
    ensure!(cursor.is_null() && records.len() == 3);
    ensure!(
        catalog_retirement_proof(&client, &entrance, &retired.resource).await? == proof,
        "owner handoff changed terminal retirement proof"
    );
    ensure!(records[0]["resource"]["id"] == resource().id);
    ensure!(
        records[2]["resource"]["id"] == format!("{:032x}", 3) && records[2]["state"] == "deleted"
    );
    ensure!(records.iter().all(|entry| entry["accepted_revision"]
        == if entry["resource"]["id"] == retired.resource.id {
            "3"
        } else {
            "1"
        }
        && entry["desired_revision"].is_null()));
    let encoded = serde_json::to_string(&records)?;
    ensure!(
        !encoded.contains("grants")
            && !encoded.contains("payload")
            && !encoded.contains("definition")
            && !encoded.contains("token")
    );
    let resource = resource();
    let name = serde_json::to_string(&(&resource.tenant, &resource.application))?;
    let id = cellar_native::cell_id::for_name("cells:v1:event-catalog:__EventCatalog", &name);
    let path = format!(
        "/catalogs/__EventCatalog:{}",
        cellar_native::cell_id::encode_hex(&id)
    );
    let body = serde_json::to_vec(
        &json!({"namespace":{"tenant":resource.tenant,"application":resource.application},"cursor":null,"limit":1}),
    )?;
    let unsigned = client
        .post(format!("http://{}{path}", entrance.info().internal_address))
        .body(body.clone())
        .send()
        .await?;
    ensure!(unsigned.status() == 401);
    let mut wrong: Value = serde_json::from_slice(&body)?;
    wrong["namespace"]["application"] = "foreign".into();
    let wrong = serde_json::to_vec(&wrong)?;
    let auth = PeerAuth::new([11; 32], "catalogs-operator")?;
    let response = auth
        .sign(
            client
                .post(format!("http://{}{path}", entrance.info().internal_address))
                .body(wrong.clone()),
            "POST",
            &path,
            &wrong,
            &entrance.info().node,
        )?
        .send()
        .await?;
    ensure!(response.status() == 400);
    stopped(entrance)
        .await
        .context("catalog inventory entrance shutdown")?;
    Ok(())
}

fn metric(snapshot: &cellar::sdk::EventMetricsSnapshot, name: &str) -> u64 {
    snapshot
        .counters
        .iter()
        .find(|counter| counter.name == name)
        .unwrap()
        .value
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn event_metrics_export_tracks_real_work_and_keeps_hosts_isolated() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let core = Executor::new("event-metrics-core")?;
    let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let (config, services) = inputs(
        "event-metrics-owner",
        &directory.path().join("owner"),
        &core,
        Some(bucket),
        0,
    )
    .await?;
    let owner = Node::start(config, services, graph(1, false), applications("owner")?).await?;
    super::ready(&owner).await?;
    let sibling_bucket =
        cellar::bucket::Bucket::open_dev(&directory.path().join("sibling-fleet.sqlite"))?;
    let (config, services) = inputs(
        "event-metrics-sibling",
        &directory.path().join("sibling"),
        &core,
        Some(sibling_bucket),
        0,
    )
    .await?;
    let sibling = Node::start(config, services, graph(1, false), applications("sibling")?).await?;
    super::ready(&sibling).await?;
    let client = client()?;
    ensure!(owner
        .services()
        .event_metrics()
        .counters
        .iter()
        .all(|counter| counter.value == 0));
    ensure!(call(&client, &owner, reqwest::Method::POST, "/write").await? == "owner:1");
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = owner.services().event_metrics();
            if metric(&snapshot, "cellar_event_dispatch_rpc_succeeded_total") > 0
                && metric(&snapshot, "cellar_event_appended_records_total") == 1
            {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    ensure!(call(&client, &owner, reqwest::Method::POST, "/process-ack").await? == "1");
    let snapshot = owner.services().event_metrics();
    ensure!(
        metric(&snapshot, "cellar_event_ack_calls_total") == 1
            && metric(&snapshot, "cellar_event_ack_processed_total") == 1
    );
    ensure!(
        snapshot
            .histograms
            .iter()
            .find(|histogram| histogram.name == "cellar_event_source_to_append_seconds")
            .unwrap()
            .count
            == 1
    );
    ensure!(sibling
        .services()
        .event_metrics()
        .counters
        .iter()
        .all(|counter| counter.value == 0));
    let path = "/event-metrics?format=json";
    let url = format!("http://{}{path}", owner.info().internal_address);
    ensure!(client.get(&url).send().await?.status() == 401);
    let auth = PeerAuth::new([11; 32], "metrics-operator")?;
    let response = auth
        .sign(client.get(&url), "GET", path, &[], &owner.info().node)?
        .send()
        .await?;
    ensure!(response.status() == 200);
    let exported: serde_json::Value = response.json().await?;
    ensure!(exported["counters"].as_array().unwrap().len() == snapshot.counters.len());
    ensure!(exported["counters"]
        .as_array()
        .unwrap()
        .iter()
        .all(|counter| counter["value"].is_string()));
    let path = "/event-metrics";
    let response = auth
        .sign(
            client.get(format!("http://{}{path}", owner.info().internal_address)),
            "GET",
            path,
            &[],
            &owner.info().node,
        )?
        .send()
        .await?;
    ensure!(
        response.status() == 200
            && response.headers()["content-type"]
                .to_str()?
                .starts_with("text/plain")
    );
    let text = response.text().await?;
    ensure!(
        text.contains("cellar_event_appended_records_total 1")
            && text.contains("cellar_event_ack_processed_total 1")
    );
    ensure!(
        !text.contains("scope=") && !text.contains("subscription=") && !text.contains("resource=")
    );
    stopped(owner)
        .await
        .context("event metrics owner shutdown")?;
    stopped(sibling)
        .await
        .context("event metrics sibling shutdown")?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_destruction_requires_retired_destinations_and_recovers_closed_admission(
) -> Result<()> {
    use serde_json::json;
    use tokio::time::Instant;
    let directory = tempfile::tempdir()?;
    let core = Executor::new("source-destruction-core")?;
    let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let (config, services) = inputs(
        "source-destruction-owner",
        &directory.path().join("owner"),
        &core,
        Some(bucket.clone()),
        0,
    )
    .await?;
    let owner = Node::start(config, services, graph(1, false), applications("owner")?).await?;
    super::ready(&owner).await?;
    let script = "embedded-node";
    let cell = cellar_native::cell_id::for_name(
        &format!("cells:v1:{}:{script}:Account", script.len()),
        "events",
    );
    let scope = format!("Account:{}", cellar_native::cell_id::encode_hex(&cell));
    let cleanup_fault = source_cleanup_fault::CleanupFault::new(
        bucket.store.clone(),
        format!("{}cells/{scope}/facets/", bucket.prefix),
    );
    let (config, mut services) = inputs(
        "source-destruction-entrance",
        &directory.path().join("entrance"),
        &core,
        Some(bucket.clone()),
        0,
    )
    .await?;
    services.fleet.as_mut().unwrap().replica_storage = ReplicaStorage::new(
        cleanup_fault.clone(),
        ReplicaStorageOptions {
            prefix: bucket.prefix.clone(),
            ..Default::default()
        },
    )?;
    let mut external = graph(1, false);
    external.primary.event_streams.clear();
    let mut entrance = Node::start(config, services, external, applications("entrance")?).await?;
    super::ready(&entrance).await?;
    let client = client()?;
    ensure!(call(&client, &owner, reqwest::Method::POST, "/revoke-publish").await? == "active");
    ensure!(call(&client, &owner, reqwest::Method::POST, "/").await? == "owner:1");
    let inspected = operator(&client, &owner, &scope, &json!({"op":"inspect"}), "outbox").await?;
    ensure!(inspected["result"]["pending_rows"] == "1");
    let incarnation = inspected["result"]["incarnation"].clone();
    ensure!(call(&client, &owner, reqwest::Method::POST, "/source-facet").await? == "1");
    ensure!(call(&client, &owner, reqwest::Method::POST, "/nested-facet").await? == "1");
    let command = json!({"op":"destroy","change":{"source":scope,"incarnation":incarnation,
        "operation":"000000000000000000000000000000aa","reason":"source lifecycle qualification",
        "data_loss_acknowledgment":"discard this proven obligation only after its exact destination is retired"}});
    let denied = operator(&client, &owner, &scope, &command, "outbox").await?;
    ensure!(
        denied["error"]["code"] == "PendingDelivery",
        "live destination destruction reply: {denied}"
    );
    let status = operator(
        &client,
        &owner,
        &scope,
        &json!({"op":"status","operation":"000000000000000000000000000000aa"}),
        "outbox",
    )
    .await?;
    ensure!(status["result"]["state"] == "checking" && status["result"]["discarded_rows"] == "0");
    let refused = client
        .post(format!("http://{}/", owner.info().public_address))
        .send()
        .await?;
    ensure!(
        !refused.status().is_success(),
        "source serving authority remained open"
    );
    let after = operator(&client, &owner, &scope, &json!({"op":"inspect"}), "outbox").await?;
    ensure!(
        after["result"]["pending_rows"] == "1" && after["result"]["incarnation"] == incarnation
    );
    stopped(owner)
        .await
        .context("source closure owner handoff")?;
    let recovered = operator(
        &client,
        &entrance,
        &scope,
        &json!({"op":"inspect"}),
        "outbox",
    )
    .await?;
    ensure!(
        recovered["result"]["incarnation"] == incarnation
            && recovered["result"]["pending_rows"] == "1"
    );
    cleanup_fault.armed.store(true, Ordering::Release);
    let retired=stream_operator(&client,&entrance,&binding(true),json!({"kind":"retire","request":{
        "operation":"000000000000000000000000000000ab","revision":"2","reason":"retire blocked source destination",
        "mode":{"mode":"force","data_loss_acknowledgment":"close this test stream before source destruction"}}}),"events-operator").await?;
    ensure!(
        retired.get("result").is_some(),
        "retirement failed: {retired}"
    );
    let fault_deadline = Instant::now() + Duration::from_secs(10);
    while cleanup_fault.deleted.lock().unwrap().is_empty() {
        let reply = operator(&client, &entrance, &scope, &command, "outbox").await?;
        ensure!(
            Instant::now() < fault_deadline,
            "source cleanup never reached the deletion fault: {reply}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let interrupted = operator(
        &client,
        &entrance,
        &scope,
        &json!({"op":"status","operation":"000000000000000000000000000000aa"}),
        "outbox",
    )
    .await?;
    ensure!(
        interrupted["result"]["state"] == "cleaning"
            && interrupted["result"]["discarded_rows"] == "1"
            && interrupted["result"]["discarded_bytes"] == inspected["result"]["pending_bytes"],
        "deletion reply loss changed progress: {interrupted}"
    );
    let refused = client
        .post(format!("http://{}/", entrance.info().public_address))
        .send()
        .await?;
    ensure!(
        !refused.status().is_success(),
        "business admission reopened during cleanup"
    );
    let premature = operator(
        &client,
        &entrance,
        &scope,
        &json!({"op":"recreate","change":{"source":scope,"incarnation":incarnation,
        "operation":"000000000000000000000000000000ad","reason":"cannot skip incomplete cleanup"}}),
        "outbox",
    )
    .await?;
    ensure!(
        premature["error"]["code"] == "ConfigConflict",
        "incomplete cleanup recreated source: {premature}"
    );
    stopped(entrance)
        .await
        .context("interrupted cleanup owner complete shutdown")?;
    let deleted_objects = cleanup_fault.deleted.lock().unwrap().clone();
    ensure!(!deleted_objects.is_empty());
    let audit_domain = cellar::asyncrt::ExecutionDomain::new(core.handle());
    let audit_result = audit_domain
        .run(async {
            for path in &deleted_objects {
                ensure!(
                    matches!(
                        bucket.store.head(path).await,
                        Err(object_store::Error::NotFound { .. })
                    ),
                    "lost deletion reply had no durable deletion at {path}"
                );
            }
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    audit_domain.shutdown().await;
    audit_result?;
    cleanup_fault.armed.store(false, Ordering::Release);
    let (config, mut services) = inputs(
        "source-cleanup-successor",
        &directory.path().join("successor"),
        &core,
        Some(bucket.clone()),
        0,
    )
    .await?;
    services.fleet.as_mut().unwrap().replica_storage = ReplicaStorage::new(
        cleanup_fault.clone(),
        ReplicaStorageOptions {
            prefix: bucket.prefix.clone(),
            ..Default::default()
        },
    )?;
    let mut external = graph(1, false);
    external.primary.event_streams.clear();
    external.primary.options.stream_bindings.clear();
    entrance = Node::start(config, services, external, applications("entrance")?).await?;
    super::ready(&entrance).await?;
    let recovered_cleanup = operator(
        &client,
        &entrance,
        &scope,
        &json!({"op":"status","operation":"000000000000000000000000000000aa"}),
        "outbox",
    )
    .await?;
    ensure!(
        matches!(
            recovered_cleanup["result"]["state"].as_str(),
            Some("cleaning" | "deleted")
        ) && recovered_cleanup["result"]["incarnation"] == incarnation
            && recovered_cleanup["result"]["discarded_rows"] == "1"
            && recovered_cleanup["result"]["discarded_bytes"]
                == inspected["result"]["pending_bytes"],
        "recovery changed the durable closure before replay: {recovered_cleanup}"
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    let deleted = loop {
        let reply = operator(&client, &entrance, &scope, &command, "outbox").await?;
        let status = operator(
            &client,
            &entrance,
            &scope,
            &json!({"op":"status","operation":"000000000000000000000000000000aa"}),
            "outbox",
        )
        .await?;
        if status["result"]["state"] == "deleted" {
            break status["result"].clone();
        }
        ensure!(
            Instant::now() < deadline,
            "source destruction did not finish: {reply} {status}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    ensure!(
        deleted["discarded_rows"] == "1"
            && deleted["discarded_bytes"] == inspected["result"]["pending_bytes"]
    );
    ensure!(
        operator(&client, &entrance, &scope, &command, "outbox").await?["result"] == deleted,
        "destruction receipt replay changed"
    );
    ensure!(
        operator(&client, &entrance, &scope, &command, "other-operator").await?["error"]["code"]
            == "Forbidden"
    );
    let mut changed = command.clone();
    changed["change"]["reason"] = json!("different");
    ensure!(
        operator(&client, &entrance, &scope, &changed, "outbox").await?["error"]["code"]
            == "ConfigConflict"
    );
    let recreated = operator(
        &client,
        &entrance,
        &scope,
        &json!({"op":"recreate","change":{"source":scope,"incarnation":incarnation,
        "operation":"000000000000000000000000000000ac","reason":"fresh source lifecycle"}}),
        "outbox",
    )
    .await?;
    ensure!(
        recreated["result"]["state"] == "live" && recreated["result"]["incarnation"] != incarnation
    );
    let fresh = operator(
        &client,
        &entrance,
        &scope,
        &json!({"op":"inspect"}),
        "outbox",
    )
    .await?;
    ensure!(
        fresh["result"]["incarnation"] == recreated["result"]["incarnation"]
            && fresh["result"]["pending_rows"] == "0"
            && fresh["result"]["lanes"] == json!([])
    );
    ensure!(
        call(&client, &entrance, reqwest::Method::GET, "/").await? == "entrance:0",
        "business state survived source destruction"
    );
    ensure!(
        operator(&client, &entrance, &scope, &command, "outbox").await?["result"] == deleted,
        "old destroy replay targeted successor"
    );
    ensure!(
        call(&client, &entrance, reqwest::Method::GET, "/source-facet").await? == "0",
        "cold source cleanup restored an old child"
    );
    ensure!(
        call(&client, &entrance, reqwest::Method::GET, "/nested-facet").await? == "0",
        "cold source cleanup restored an old descendant"
    );
    let old = operator(
        &client,
        &entrance,
        &scope,
        &json!({"op":"status","operation":"000000000000000000000000000000aa"}),
        "outbox",
    )
    .await?;
    ensure!(old["result"] == deleted);
    let audit = operator(&client, &entrance, &scope, &json!({"op":"audit"}), "outbox").await?;
    ensure!(audit["result"]["records"]
        .as_array()
        .is_some_and(|rows| rows.len() == 2));
    stopped(entrance)
        .await
        .context("source lifecycle final complete shutdown")?;
    Ok(())
}

#[derive(Default)]
struct SourceLifetimeProbe {
    saved: std::sync::Mutex<Option<Storage>>,
    started: std::sync::atomic::AtomicBool,
    dropped: std::sync::atomic::AtomicBool,
}

struct SourceLifetimeDrop(Arc<SourceLifetimeProbe>);

impl Drop for SourceLifetimeDrop {
    fn drop(&mut self) {
        self.0.dropped.store(true, Ordering::Release);
    }
}

struct SourceFacetCounter;

impl Cell for SourceFacetCounter {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = invocation else {
                return Ok(Output::Done);
            };
            if url::Url::parse(&request.url)?.path() == "/nested-facet" {
                let class = context.exported_cell_class("SourceFacetCounter")?;
                let facet = context.facets()?.get(&context, "nested", move |_| {
                    Box::pin(async move { Ok(cellar::application::facets::Startup::new(class)) })
                })?;
                let mut child = request;
                child.url = "http://source-facet/value".into();
                return Ok(Output::Http(facet.fetch(&context, child).await?));
            }
            let db = context.storage()?;
            let value = db
                .get_json("facet_count")?
                .and_then(|value| value.as_i64())
                .unwrap_or(0);
            let value = if request.method == "POST" {
                db.put_json("facet_count", &serde_json::json!(value + 1))?;
                value + 1
            } else {
                value
            };
            Ok(super::response(value))
        })
    }
}

struct SourceLifetimeCell(Arc<SourceLifetimeProbe>);

impl Cell for SourceLifetimeCell {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            if let Invocation::Fetch(request) = &invocation {
                match url::Url::parse(&request.url)?.path() {
                    "/source-facet" | "/nested-facet" => {
                        let class = context.exported_cell_class("SourceFacetCounter")?;
                        let facet =
                            context
                                .facets()?
                                .get(&context, "persistent-child", move |_| {
                                    Box::pin(async move {
                                        Ok(cellar::application::facets::Startup::new(class))
                                    })
                                })?;
                        let Invocation::Fetch(request) = invocation else {
                            unreachable!("matched fetch invocation")
                        };
                        return Ok(Output::Http(facet.fetch(&context, request).await?));
                    }
                    "/remember-storage" => {
                        *self.0.saved.lock().unwrap() = Some(context.storage()?);
                        return Ok(super::response("saved".to_owned()));
                    }
                    "/old-storage" => {
                        let old = self.0.saved.lock().unwrap().clone().unwrap();
                        let failure = old
                            .get_json("count")
                            .err()
                            .context("retired storage became valid again")?;
                        return Ok(super::response(failure.to_string()));
                    }
                    "/hold-source" => {
                        let _guard = SourceLifetimeDrop(self.0.clone());
                        self.0.started.store(true, Ordering::Release);
                        return std::future::pending::<Result<Output>>().await;
                    }
                    _ => {}
                }
            }
            Arc::new(Counter("lifetime"))
                .invoke(context, invocation)
                .await
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_destruction_fences_suspended_turns_retained_handles_and_preserves_live_watermarks(
) -> Result<()> {
    use serde_json::json;
    use sha2::Digest;
    let directory = tempfile::tempdir()?;
    let core = Executor::new("source-lifetime-core")?;
    let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let (mut config, services) = inputs(
        "source-lifetime",
        &directory.path().join("node"),
        &core,
        Some(bucket),
        0,
    )
    .await?;
    config.actor.core.idle_evict_ms = None;
    let probe = Arc::new(SourceLifetimeProbe::default());
    let constructors = probe.clone();
    let apps = CompiledApplications::default().register("embedded-node", "test", move || {
        let constructors = constructors.clone();
        Ok(Arc::new(
            Registry::new(|context, invocation| {
                Box::pin(async move {
                    let Invocation::Fetch(request) = invocation else {
                        return Ok(Output::Done);
                    };
                    Ok(Output::Http(
                        context
                            .namespace("ACCOUNTS")?
                            .get_by_name("events")
                            .fetch(&context, request)
                            .await?,
                    ))
                })
            })
            .cell("Account", move |_, _| {
                Ok(Arc::new(SourceLifetimeCell(constructors.clone())))
            })?
            .cell("SourceFacetCounter", |_, _| {
                Ok(Arc::new(SourceFacetCounter))
            })?,
        ))
    })?;
    let declarations = graph(1, false);

    let node = Node::start(config, services, declarations, apps).await?;
    super::ready(&node).await?;
    let client = client()?;
    let script = "embedded-node";
    let id = cellar_native::cell_id::for_name(
        &format!("cells:v1:{}:{script}:Account", script.len()),
        "events",
    );
    let scope = format!("Account:{}", cellar_native::cell_id::encode_hex(&id));
    ensure!(
        call(&client, &node, reqwest::Method::POST, "/")
            .await
            .context("source lifetime initial emission")?
            == "lifetime:1"
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let page = operator(&client, &node, &scope, &json!({"op":"inspect"}), "outbox").await?;
            if page["result"]["pending_rows"] == "0" {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let status: serde_json::Value = serde_json::from_str(
                &call(&client, &node, reqwest::Method::POST, "/seal")
                    .await
                    .context("source lifetime seal after confirmed settlement")?,
            )?;
            if status["state"] == "accepted" {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    ensure!(call(&client, &node, reqwest::Method::POST, "/source-facet").await? == "1");
    ensure!(call(&client, &node, reqwest::Method::POST, "/nested-facet").await? == "1");
    ensure!(call(&client, &node, reqwest::Method::GET, "/remember-storage").await? == "saved");
    let inventory = stream_operator(
        &client,
        &node,
        &binding(true),
        json!({"kind":"sources","cursor":null,"limit":100}),
        "events-operator",
    )
    .await?;
    ensure!(inventory["result"]["records"][0]["sealed"] == true);
    let before = operator(&client, &node, &scope, &json!({"op":"inspect"}), "outbox").await?;
    let incarnation = before["result"]["incarnation"].clone();
    let held_client = client.clone();
    let address = node.info().public_address;
    let held = tokio::spawn(async move {
        held_client
            .post(format!("http://{address}/hold-source"))
            .send()
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while !probe.started.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    let command = json!({"op":"destroy","change":{"source":scope,"incarnation":incarnation,
        "operation":"000000000000000000000000000000ba","reason":"sealed live source lifetime test",
        "data_loss_acknowledgment":"destroy this entire test source while retaining its sealed destination watermark"}});
    let accepted = operator(&client, &node, &scope, &command, "outbox").await?;
    ensure!(
        accepted.get("result").is_some(),
        "sealed source destruction failed: {accepted}"
    );
    let terminal = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let status = operator(
                &client,
                &node,
                &scope,
                &json!({"op":"status","operation":"000000000000000000000000000000ba"}),
                "outbox",
            )
            .await?;
            if status["result"]["state"] == "deleted" {
                return Ok::<_, anyhow::Error>(status);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    ensure!(
        terminal["result"]["discarded_rows"] == "0" && probe.dropped.load(Ordering::Acquire),
        "source closure did not drop the suspended handler"
    );
    ensure!(!tokio::time::timeout(Duration::from_secs(5), held)
        .await???
        .status()
        .is_success());
    let old_facet = sha2::Sha256::digest(b"persistent-child");
    let old_path = directory
        .path()
        .join("node")
        .join(&scope)
        .join("facets")
        .join(hex::encode(&old_facet[..16]));
    ensure!(
        !old_path.exists(),
        "source cleanup retained its old local facet family"
    );
    let after = stream_operator(
        &client,
        &node,
        &binding(true),
        json!({"kind":"sources","cursor":null,"limit":100}),
        "events-operator",
    )
    .await?;
    ensure!(
        after == inventory,
        "source destruction changed live destination watermarks"
    );
    let recreated = operator(
        &client,
        &node,
        &scope,
        &json!({"op":"recreate","change":{"source":scope,"incarnation":incarnation,
        "operation":"000000000000000000000000000000bb","reason":"fresh same-activation source"}}),
        "outbox",
    )
    .await?;
    ensure!(recreated["result"]["incarnation"] != incarnation);
    ensure!(
        call(&client, &node, reqwest::Method::GET, "/old-storage")
            .await?
            .contains("source business admission"),
        "old storage was not fenced by source generation"
    );
    ensure!(call(&client, &node, reqwest::Method::GET, "/").await? == "lifetime:0");
    ensure!(
        call(&client, &node, reqwest::Method::GET, "/source-facet").await? == "0",
        "recreated source reopened old facet state"
    );
    ensure!(
        call(&client, &node, reqwest::Method::GET, "/nested-facet").await? == "0",
        "recreated source reopened nested facet state"
    );
    stopped(node)
        .await
        .context("source lifetime complete shutdown")?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_proven_append_and_ack_replies_preserve_exact_acceptance_and_effects() -> Result<()> {
    use base64::Engine;
    use serde_json::json;
    let directory = tempfile::tempdir()?;
    let core = Executor::new("append-reply-core")?;
    let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let (mut config, services) = inputs(
        "append-destination",
        &directory.path().join("destination"),
        &core,
        Some(bucket.clone()),
        0,
    )
    .await?;
    let proxy = peer_reply_fault::Proxy::start(services.internal_listener.local_addr()?).await?;
    config.identity.advertise = proxy.address.to_string();
    let destination_constructors = Arc::new(AtomicUsize::new(0));
    let destination = Node::start(
        config,
        services,
        graph(1, false),
        applications_with_probe("destination", Some(destination_constructors.clone()))?,
    )
    .await?;
    super::ready(&destination).await?;
    let client = client()?;
    let empty = stream_operator(
        &client,
        &destination,
        &binding(false),
        json!({"kind":"read","cursor":null,"limit":10,"max_bytes":4096,"payload":true}),
        "audit",
    )
    .await?;
    ensure!(
        empty["result"]["records"] == json!([]),
        "destination was not empty: {empty}"
    );
    ensure!(destination_constructors.load(Ordering::Acquire) == 0);
    let (config, services) = inputs(
        "append-producer",
        &directory.path().join("producer"),
        &core,
        Some(bucket.clone()),
        0,
    )
    .await?;
    let producer_constructors = Arc::new(AtomicUsize::new(0));
    let mut external = graph(1, false);
    external.primary.event_streams.clear();
    let producer = Node::start(
        config,
        services,
        external,
        applications_with_probe("producer", Some(producer_constructors.clone()))?,
    )
    .await?;
    super::ready(&producer).await?;
    proxy.capture.arm("__streamAppend");
    ensure!(call(&client, &producer, reqwest::Method::POST, "/write").await? == "producer:1");
    tokio::time::timeout(Duration::from_secs(5), proxy.capture.entered.acquire())
        .await??
        .forget();
    ensure!(
        producer_constructors.load(Ordering::Acquire) == 1
            && destination_constructors.load(Ordering::Acquire) == 0,
        "source was not independently hosted"
    );
    let captured = proxy.capture.exchanges.lock().unwrap().clone();
    ensure!(captured.len() == 1 && captured[0].dropped && captured[0].scope == stream_scope()?);
    let request = cellar::application::decode_rpc(&cellar::engine_api::RpcData::Structured(
        captured[0].request.clone().into(),
    ))?
    .to_json()?;
    let reply = cellar::application::decode_rpc_result(&cellar::engine_api::RpcData::Structured(
        captured[0].response.clone().into(),
    ))?
    .to_json()?;
    ensure!(request[0]["records"]
        .as_array()
        .is_some_and(|records| records.len() == 1));
    let envelope: serde_json::Value = serde_json::from_slice(
        &base64::engine::general_purpose::STANDARD
            .decode(request[0]["records"][0].as_str().unwrap())?,
    )?;
    ensure!(
        reply["ok"]["protocol"] == 1
            && reply["ok"]["accepted_through"] == "1"
            && reply["ok"]["resource"] == resource().id
            && reply["ok"]["lane"] == request[0]["lane"],
        "captured append reply did not prove the exact prefix: {reply}"
    );
    ensure!(
        envelope["payload"] == json!({"value":1})
            && envelope["sequence"] == "1"
            && envelope["lane"] == request[0]["lane"]
            && envelope["family"] == Changed::FAMILY
            && envelope["version"] == 1
            && request[0]["binding"]["resource"]["id"] == resource().id,
        "append changed the independently issued command: {envelope}"
    );
    let scope = envelope["source"]
        .as_str()
        .context("envelope source missing")?;
    let pending = operator(&client, &producer, scope, &json!({"op":"inspect"}), "audit").await?;
    ensure!(
        pending["result"]["pending_rows"] == "1"
            && pending["result"]["incarnation"] == envelope["source_incarnation"]
            && pending["result"]["lanes"][0]["revision"]["next_sequence"] == "2"
            && pending["result"]["lanes"][0]["revision"]["settled_through"] == "0",
        "source settled before receiving the reply: {pending}"
    );
    let accepted = stream_operator(
        &client,
        &destination,
        &binding(false),
        json!({"kind":"read","cursor":null,"limit":10,"max_bytes":4096,"payload":true}),
        "audit",
    )
    .await?;
    ensure!(
        accepted["result"]["records"]
            .as_array()
            .is_some_and(|records| records.len() == 1)
            && accepted["result"]["records"][0]["offset"] == "1"
            && accepted["result"]["records"][0]["event"] == envelope,
        "destination acceptance differed before reply loss: {accepted}"
    );
    // Audit the acknowledged command and accepted envelope before permitting
    // network loss/retry or any consumer to hide a missing obligation.
    ensure!(call(&client, &producer, reqwest::Method::GET, "/read").await? == "producer:1");
    proxy.capture.release.add_permits(1);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let state = operator(&client, &producer, scope, &json!({"op":"inspect"}), "audit").await?;
        if state["result"]["pending_rows"] == "0" {
            break;
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "append retry did not settle: {state}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let attempts = proxy.capture.exchanges.lock().unwrap().clone();
    ensure!(
        attempts.len() >= 2 && attempts.iter().filter(|exchange| exchange.dropped).count() == 1
    );
    for attempt in &attempts {
        ensure!(
            attempt.scope == captured[0].scope
                && attempt.request == captured[0].request
                && attempt.response == captured[0].response,
            "append retry changed bytes or acceptance"
        );
    }
    let after = stream_operator(
        &client,
        &destination,
        &binding(false),
        json!({"kind":"read","cursor":null,"limit":10,"max_bytes":4096,"payload":true}),
        "audit",
    )
    .await?;
    ensure!(
        after["result"]["records"] == accepted["result"]["records"],
        "lost reply duplicated acceptance"
    );
    let (config, services) = inputs(
        "append-consumer",
        &directory.path().join("consumer"),
        &core,
        Some(bucket),
        0,
    )
    .await?;
    let consumer_constructors = Arc::new(AtomicUsize::new(0));
    let mut external = graph(1, false);
    external.primary.event_streams.clear();
    let consumer = Node::start(
        config,
        services,
        external,
        applications_with_probe("consumer", Some(consumer_constructors.clone()))?,
    )
    .await?;
    super::ready(&consumer).await?;
    ensure!(
        call(
            &client,
            &consumer,
            reqwest::Method::POST,
            "/process-retry?source=projection"
        )
        .await?
            == "1"
    );
    let before_ack = stream_operator(
        &client,
        &destination,
        &binding(true),
        json!({"kind":"subscription","name":"projection"}),
        "audit",
    )
    .await?;
    ensure!(before_ack["result"]["checkpoint"] == "0");
    proxy.capture.arm("__streamAck");
    let mut acknowledging = Box::pin(call(
        &client,
        &consumer,
        reqwest::Method::POST,
        "/process-lost-ack?source=projection",
    ));
    cellar::asyncrt::select! {
        result = &mut acknowledging => anyhow::bail!("acknowledgment finished before the proxy proof boundary: {result:?}"),
        entered = tokio::time::timeout(Duration::from_secs(5), proxy.capture.entered.acquire()) => entered??.forget(),
    }
    let lost_ack = proxy
        .capture
        .exchanges
        .lock()
        .unwrap()
        .iter()
        .filter(|exchange| exchange.method == "__streamAck")
        .cloned()
        .collect::<Vec<_>>();
    ensure!(lost_ack.len() == 1 && lost_ack[0].dropped && lost_ack[0].scope == stream_scope()?);
    let ack_request = cellar::application::decode_rpc(&cellar::engine_api::RpcData::Structured(
        lost_ack[0].request.clone().into(),
    ))?
    .to_json()?;
    let receipt = Receipt::decode(
        ack_request[0]["receipt"]
            .as_str()
            .context("captured acknowledgment receipt missing")?,
    )?;
    let receipt_identity = serde_json::to_value(&receipt)?;
    ensure!(
        receipt_identity["resource"] == resource().id
            && receipt_identity["name"] == "projection"
            && receipt_identity["first"] == "1"
            && receipt_identity["last"] == "1"
    );
    let ack_reply = cellar::application::decode_rpc_result(
        &cellar::engine_api::RpcData::Structured(lost_ack[0].response.clone().into()),
    )?
    .to_json()?;
    ensure!(
        ack_reply["ok"]["status"] == "acknowledged",
        "lost ack was not committed/proven: {ack_reply}"
    );
    let proven_ack = stream_operator(
        &client,
        &destination,
        &binding(true),
        json!({"kind":"subscription","name":"projection"}),
        "audit",
    )
    .await?;
    ensure!(
        proven_ack["result"]["checkpoint"] == "1" && proven_ack["result"]["lease"].is_null(),
        "acknowledgment was not durable before losing its reply: {proven_ack}"
    );
    proxy.capture.release.add_permits(1);
    let outcome = tokio::time::timeout(Duration::from_secs(5), &mut acknowledging).await??;
    drop(acknowledging);
    ensure!(
        outcome == "Ambiguous",
        "unexpected ack loss outcome: {outcome}"
    );
    // Inspect the original proven effect before deliberately retrying its saved
    // receipt. No next-batch call or fresh local effect can conceal ack loss.
    ensure!(
        call(
            &client,
            &consumer,
            reqwest::Method::GET,
            "/processed?source=projection"
        )
        .await?
            == "1"
    );
    ensure!(
        call(
            &client,
            &consumer,
            reqwest::Method::POST,
            "/ack-stored?source=projection"
        )
        .await?
            == "AlreadyAcknowledged"
    );
    let ack_attempts = proxy
        .capture
        .exchanges
        .lock()
        .unwrap()
        .iter()
        .filter(|exchange| exchange.method == "__streamAck")
        .cloned()
        .collect::<Vec<_>>();
    ensure!(
        ack_attempts.len() >= 2
            && ack_attempts
                .iter()
                .filter(|exchange| exchange.dropped)
                .count()
                == 1
    );
    for attempt in &ack_attempts {
        ensure!(
            attempt.scope == lost_ack[0].scope && attempt.request == lost_ack[0].request,
            "acknowledgment retry replaced the original receipt or changed its bytes"
        );
    }
    let replayed =
        cellar::application::decode_rpc_result(&cellar::engine_api::RpcData::Structured(
            ack_attempts.last().unwrap().response.clone().into(),
        ))?
        .to_json()?;
    ensure!(replayed["ok"]["status"] == "already_acknowledged");
    ensure!(
        call(
            &client,
            &consumer,
            reqwest::Method::POST,
            "/process-ack?source=projection"
        )
        .await?
            == "1"
    );

    ensure!(
        consumer_constructors.load(Ordering::Acquire) == 1,
        "consumer was not independently hosted"
    );
    let subscription = stream_operator(
        &client,
        &destination,
        &binding(true),
        json!({"kind":"subscription","name":"projection"}),
        "audit",
    )
    .await?;
    ensure!(
        subscription["result"]["checkpoint"] == "1",
        "consumer skipped or lost the accepted prefix: {subscription}"
    );
    stopped(consumer)
        .await
        .context("append fault consumer complete shutdown")?;
    stopped(producer)
        .await
        .context("append fault producer complete shutdown")?;
    stopped(destination)
        .await
        .context("append fault destination complete shutdown")?;
    proxy
        .stop()
        .await
        .context("append fault proxy joined every connection")?;
    Ok(())
}

struct NamedCounter {
    label: &'static str,
    retained: Arc<std::sync::Mutex<Option<Stream<Changed>>>>,
}

impl Cell for NamedCounter {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            if let Invocation::Fetch(request) = &invocation {
                let path = url::Url::parse(&request.url)?.path().to_owned();
                match path.as_str() {
                    "/pin-stream" => {
                        let stream = context.stream::<Changed>("EVENTS")?;
                        let id = stream.resource().id.clone();
                        *self.retained.lock().unwrap() = Some(stream);
                        return Ok(super::response(id));
                    }
                    "/binding-id" => {
                        return Ok(super::response(
                            context.stream::<Changed>("EVENTS")?.resource().id.clone(),
                        ))
                    }
                    "/retained-id" => {
                        return Ok(super::response(
                            self.retained
                                .lock()
                                .unwrap()
                                .as_ref()
                                .unwrap()
                                .resource()
                                .id
                                .clone(),
                        ))
                    }
                    "/emit-retained" => {
                        let stream = self.retained.lock().unwrap().as_ref().unwrap().clone();
                        let result = context
                            .storage()?
                            .tx(|tx| tx.emit(&stream, Changed { value: 99 }));
                        let code = result.err().and_then(|error|error.downcast::<DeliveryError>().ok())
                            .context("retained handle unexpectedly emitted or failed without delivery classification")?.code;
                        return Ok(super::response(format!("{code:?}")));
                    }
                    "/event-value" => {
                        let page = context
                            .stream::<Changed>("EVENTS")?
                            .read(&context, None, ReadOptions::default())
                            .await?;
                        return Ok(super::response(
                            page.records
                                .first()
                                .map(|record| {
                                    record.event.decode::<Changed>().map(|event| event.value)
                                })
                                .transpose()?
                                .unwrap_or(0),
                        ));
                    }
                    _ => {}
                }
            }
            Arc::new(Counter(self.label))
                .invoke(context, invocation)
                .await
        })
    }
}

fn named_applications(
    label: &'static str,
    retained: Arc<std::sync::Mutex<Option<Stream<Changed>>>>,
) -> Result<CompiledApplications> {
    CompiledApplications::default().register("embedded-node", "test", move || {
        let retained = retained.clone();
        Ok(Arc::new(
            Registry::new(|context, event| {
                Box::pin(async move {
                    let Invocation::Fetch(request) = event else {
                        return Ok(Output::Done);
                    };
                    Ok(Output::Http(
                        context
                            .namespace("ACCOUNTS")?
                            .get_by_name("events")
                            .fetch(&context, request)
                            .await?,
                    ))
                })
            })
            .cell("Account", move |_, _| {
                Ok(Arc::new(NamedCounter {
                    label,
                    retained: retained.clone(),
                }))
            })?,
        ))
    })
}

fn named_graph() -> DeploymentGraph {
    let mut graph = graph(1, false);
    graph.primary.event_streams.clear();
    for binding in &mut graph.primary.options.stream_bindings {
        binding.resource.id = None;
    }
    graph
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn named_bindings_resolve_grants_and_pin_uuid_across_recreation() -> Result<()> {
    use serde_json::json;
    let directory = tempfile::tempdir()?;
    let core = Executor::new("named-bindings-core")?;
    let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let (config, services) = inputs(
        "name-owner",
        &directory.path().join("owner"),
        &core,
        Some(bucket.clone()),
        0,
    )
    .await?;
    let owner = Node::start(config, services, graph(1, false), applications("owner")?).await?;
    super::ready(&owner).await?;
    let retained = Arc::new(std::sync::Mutex::new(None));
    let (config, services) = inputs(
        "name-client",
        &directory.path().join("client"),
        &core,
        Some(bucket),
        0,
    )
    .await?;
    let mut client_node = Node::start(
        config,
        services,
        named_graph(),
        named_applications("named", retained.clone())?,
    )
    .await?;
    super::ready(&client_node).await?;
    let client = client()?;
    ensure!(
        call(&client, &client_node, reqwest::Method::GET, "/pin-stream").await? == resource().id
    );
    ensure!(call(&client, &client_node, reqwest::Method::POST, "/write").await? == "named:1");
    tokio::time::timeout(Duration::from_secs(5), async {
        while call(&client, &client_node, reqwest::Method::GET, "/event-value").await? != "1" {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    for mismatch in ["missing-name", "revoked-grant", "wrong-family"] {
        let mut candidate = named_graph();
        match mismatch {
            "missing-name" => {
                candidate.primary.options.stream_bindings[0].resource.name = "absent".into()
            }
            "revoked-grant" => candidate.primary.options.stream_bindings[0].grant = "absent".into(),
            _ => {
                candidate.primary.options.stream_bindings[0].resource.family = "other-family".into()
            }
        }
        ensure!(
            client_node
                .adopt(candidate, named_applications("invalid", retained.clone())?)
                .await
                .is_err(),
            "invalid name binding was adopted: {mismatch}"
        );
        ensure!(call(&client, &client_node, reqwest::Method::GET, "/read").await? == "named:1");
    }
    ensure!(
        call(
            &client,
            &client_node,
            reqwest::Method::POST,
            "/revoke-publish"
        )
        .await?
            == "active"
    );
    ensure!(client_node
        .adopt(
            named_graph(),
            named_applications("revoked", retained.clone())?
        )
        .await
        .is_err());
    ensure!(
        call(
            &client,
            &client_node,
            reqwest::Method::POST,
            "/restore-publish"
        )
        .await?
            == "active"
    );
    let retired=stream_operator(&client, &owner, &binding(true), json!({"kind":"retire","request":{
        "operation":"000000000000000000000000000000f1","revision":"3","reason":"name reuse qualification",
        "mode":{"mode":"force","data_loss_acknowledgment":"discard the old stream payload for this isolated name test"}}}), "names-audit").await?;
    ensure!(
        retired.get("result").is_some(),
        "retirement failed: {retired}"
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let state = stream_operator(
            &client,
            &owner,
            &binding(true),
            json!({"kind":"retirement_status","operation":"000000000000000000000000000000f1"}),
            "names-audit",
        )
        .await?;
        if state["result"]["state"] == "deleted" {
            break;
        }
        ensure!(
            tokio::time::Instant::now() < deadline,
            "retirement did not finish: {state}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    ensure!(client_node
        .adopt(
            named_graph(),
            named_applications("deleted", retained.clone())?
        )
        .await
        .is_err());
    let mut successor = named_graph();
    let mut definition = graph(1, false).primary.event_streams.remove(0);
    definition.resource.id = "00000000000000000000000000000002".into();
    successor.primary.event_streams.push(definition);
    client_node
        .adopt(
            successor,
            named_applications("successor", retained.clone())?,
        )
        .await?;
    ensure!(
        call(&client, &client_node, reqwest::Method::GET, "/binding-id").await?
            == "00000000000000000000000000000002"
    );
    ensure!(
        call(&client, &client_node, reqwest::Method::GET, "/retained-id").await? == resource().id
    );
    ensure!(
        call(
            &client,
            &client_node,
            reqwest::Method::POST,
            "/emit-retained"
        )
        .await?
            == "Forbidden"
    );
    ensure!(call(&client, &client_node, reqwest::Method::POST, "/write").await? == "successor:2");
    tokio::time::timeout(Duration::from_secs(5), async {
        while call(&client, &client_node, reqwest::Method::GET, "/event-value").await? != "2" {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    stopped(client_node)
        .await
        .context("named binding client complete shutdown")?;
    stopped(owner)
        .await
        .context("named binding owner complete shutdown")?;
    Ok(())
}
