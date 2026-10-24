// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Independent Python consumer matrix against the native durable queue broker.
use super::*;

pub(super) struct Observed(pub(super) Arc<PythonApplication>);

impl Application for Observed {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        if matches!(&event, Invocation::Fetch(request) if request.url.ends_with("/metrics")) {
            return Box::pin(async move {
                let metrics = context.queue("JOBS")?.metrics(&context).await?;
                Ok(Output::Http(
                    http::Response::new(
                        200,
                        serde_json::to_vec(
                            &json!({"count":metrics.backlog_count,"bytes":metrics.backlog_bytes}),
                        )?,
                        vec![("content-type".into(), "application/json".into())],
                    )
                    .into_http(&context)?,
                ))
            });
        }
        self.0.clone().invoke(context, event)
    }

    fn supports_cell(&self, class: &str) -> bool {
        self.0.supports_cell(class)
    }

    fn create_cell(&self, context: Context, identity: &CellIdentity) -> Result<Arc<dyn Cell>> {
        self.0.create_cell(context, identity)
    }

    fn validate_deployment(&self, deployment: &cellar::fleet::LoadedDeployment) -> Result<()> {
        self.0.validate_deployment(deployment)
    }
}

async fn value(client: &reqwest::Client, url: &str, route: &str) -> Result<Value> {
    Ok(client
        .get(format!("{url}{route}"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}

async fn drained(client: &reqwest::Client, url: &str) -> Result<Value> {
    let result = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let metrics = value(client, url, "/metrics").await?;
            if metrics["count"].as_f64() == Some(0.0) && metrics["bytes"].as_f64() == Some(0.0) {
                return value(client, url, "/state").await;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    match result {
        Ok(result) => result,
        Err(error) => anyhow::bail!(
            "queue did not settle: {error}: metrics={} state={}",
            value(client, url, "/metrics").await?,
            value(client, url, "/state").await?
        ),
    }
}

pub(super) fn child() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let report = run(
        Options::default(),
        Source::new(include_str!("delivery.py"), "delivery.py")
            .events([EventKind::Fetch, EventKind::Queue]),
        move |application, monitor| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .enable_all()
                .build()?;
            runtime.block_on(async move {
            let application: Arc<dyn Application> = Arc::new(Observed(application));
            let configure = |config: &mut NodeConfig, _: &mut NodeServices, graph: &mut DeploymentGraph| {
                config.runtime.handler_budget = Duration::from_secs(2);
                config.actor.lease_ttl_ms = 5_000;
                config.startup.recovery_retry_backoff = Duration::from_secs(5);
                graph.primary.options.bindings.clear();
                graph.primary.options.do_classes = vec![cellar::deploy::SQL_CLASS.into(), cellar::deploy::QUEUE_CLASS.into()];
                graph.primary.options.sql_bindings.push(("DB".into(), "delivery-db".into()));
                graph.primary.options.queue_bindings.push(cellar::engine_api::QueueBinding {
                    environment: "JOBS".into(), queue: "delivery-main".into(), delivery_delay: 0,
                });
                graph.primary.options.queue_consumers.push(cellar::protocol::QueueConsumerConfig {
                    queue: "delivery-main".into(), max_batch_size: 1, max_batch_timeout: 0,
                    max_retries: 2, dead_letter_queue: None, max_concurrency: Some(1), retry_delay: Some(0),
                });
            };
            let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
            let data = directory.path().join("node");
            let (node, url) = start_graph(&data, application.clone(), "delivery", Some(bucket.clone()), configure)
                .await.context("delivery initial node admission")?;
            let client = reqwest::Client::builder().timeout(Duration::from_secs(10)).build()?;
            ensure!(value(&client, &url, "/setup").await? == true);
            ensure!(value(&client, &url, "/wrong-event").await? == true);
            let cases = [("success",1), ("error",2), ("poison",3), ("ack_error",1), ("retry_ack",2), ("ack_retry",1),
                         ("ackall_retry",1), ("retryall_ack",2), ("invalid",1), ("cancel",2), ("hold",1), ("burst",1)];
            for (kind, expected) in cases {
                if kind == "burst" {
                    ensure!(value(&client, &url, "/burst").await? == json!({"accepted":1,"refused":31}));
                } else {
                    client.post(format!("{url}/send")).json(&json!({"kind":kind})).send().await?.error_for_status()?;
                }
                if kind == "hold" {
                    tokio::time::timeout(Duration::from_secs(1), async {
                        loop {
                            if value(&client, &url, "/state").await?["holding"] == true { return Ok::<_, anyhow::Error>(()); }
                            tokio::task::yield_now().await;
                        }
                    }).await.context("delivery held consumer admission deadline")??;
                    ensure!(value(&client, &url, "/cross").await? == true);
                    ensure!(value(&client, &url, "/release").await? == true);
                }
                let state = drained(&client, &url).await?;
                let attempts = state["attempts"].as_array().context("attempt rows")?.iter()
                    .filter(|row| row["command"] == kind).collect::<Vec<_>>();
                ensure!(attempts.len() == expected, "{kind}: expected {expected} deliveries: {state}");
                for (index, row) in attempts.iter().enumerate() {
                    ensure!(row["attempt"] == index + 1 && row["message"] == attempts[0]["message"], "unstable delivery identity: {state}");
                }
                ensure!(state["effects"].as_array().context("effect rows")?.iter().filter(|row| row["command"] == kind).count() == 1,
                    "duplicate business effect: {state}");
                if kind == "cancel" { ensure!(state["cancelled"] == json!(["cancel"]), "cancelled turn did not retire: {state}"); }
            }
            ensure!(value(&client, &url, "/expired").await? == true);
            let before = drained(&client, &url).await?;
            stop(node, &monitor).await.context("delivery first preserving shutdown")?;
            let (node, url) = start_graph(&data, application, "delivery", Some(bucket), configure)
                .await.context("delivery restart admission")?;
            let after = drained(&client, &url).await?;
            ensure!(before == after, "settled queue or receipts changed across preserving restart: {before} -> {after}");
            stop(node, &monitor).await.context("delivery replacement shutdown")
        })
        },
    )?;
    ensure!(
        report.complete()
            && report.native_outcomes.len() == 2
            && report.bridge.operations.settled(),
        "delivery leaked work: {report:?}"
    );
    Ok(())
}
