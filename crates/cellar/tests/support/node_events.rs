//! Actual node routing and proof gates, including staged adoption and inbox
//! redelivery. No synthetic successful-gate adapter is installed here.

use super::{call, client, inputs, stopped, Executor};
use anyhow::{ensure, Result};
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
    graph.primary.options.stream_bindings = vec![application, binding(true)];
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
            let db = context.storage()?;
            let events = context.stream::<Changed>("EVENTS")?;
            let body = if path == "/revoke-publish"
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
            } else if path == "/process-retry" || path == "/process-ack" {
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
                            Ok(())
                        })?;
                    }
                    db.sync().await?;
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
                    let account = context.namespace("ACCOUNTS")?.get_by_name("events");
                    Ok(Output::Http(account.fetch(&context, request).await?))
                })
            })
            .cell("Account", move |_, _| {
                if let Some(probe) = &probe {
                    probe.fetch_add(1, Ordering::SeqCst);
                }
                Ok(Arc::new(Counter(label)))
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
async fn node_cutover_refuses_previous_storage_and_peer_protocol_before_admission() -> Result<()> {
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
            let upgrade = cellar::wake_format::Upgrade {
                operation: "00000000000000000000000000000088".into(),
                reason: "coordinated test upgrade".into(),
                confirmed_stopped_writers: true,
            };
            cellar::wake_format::upgrade(&bucket, &upgrade).await?;
            Ok::<_, anyhow::Error>(())
        })
        .await??;
    operator.shutdown().await;
    let (mut config, services) = inputs("events", &data, &core, Some(bucket.clone()), 0).await?;
    config.actor.core.peer_protocol = 5;
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
    let node = Node::start(config, services, graph(1, false), applications("upgraded")?).await?;
    let sender = cellar::peer_auth::PeerAuth::new([11; 32], "previous-peer")?;
    let mut headers = sender.signed_headers("GET", "/peer/probe", &[], "events")?;
    headers.insert(cellar::peer_auth::RESPONSE_VERSION_HEADER, "5".parse()?);
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
    ensure!(reply.headers()[cellar::peer_auth::RESPONSE_VERSION_HEADER] == "6");
    // An otherwise authenticated current peer cannot negotiate the previous
    // tunnel layout. This exercises the tunnel's own version gate separately.
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

async fn stream_operator(
    client: &reqwest::Client,
    node: &Node,
    binding: &Binding,
    command: serde_json::Value,
    caller: &str,
) -> Result<serde_json::Value> {
    let path = format!("/streams/{}", stream_scope()?);
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
