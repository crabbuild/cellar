//! Actual node routing and proof gates, including staged adoption and inbox
//! redelivery. No synthetic successful-gate adapter is installed here.

use super::{call, client, inputs, stopped, Executor};
use anyhow::{ensure, Result};
use cellar::sdk::{event_stream::*, *};
use serde::{Deserialize, Serialize};
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
            let body = if path == "/events" {
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
    CompiledApplications::default().register("embedded-node", "test", move || {
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
            .cell("Account", move |_, _| Ok(Arc::new(Counter(label))))?,
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
