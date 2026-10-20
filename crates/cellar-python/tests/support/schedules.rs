//! Actual native cron alarm, Python retry decisions and persisted history.
use super::*;

const EXPRESSIONS: [&str; 4] = ["* * * * *", "*/1 * * * *", "* 0-23 * * *", "* * 1-31 * *"];

struct Observed(Arc<PythonApplication>);

impl Application for Observed {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        if matches!(&event, Invocation::Fetch(request) if request.url.ends_with("/history")) {
            return Box::pin(async move {
                let data = context
                    .rpc_cell(
                        cellar_logic::cron::reserved_cell("python-test"),
                        None,
                        "__cronHistory".into(),
                        cellar::engine_api::RpcData::Json("{}".into()),
                    )
                    .await?;
                let cellar::engine_api::RpcData::Json(body) = data else {
                    anyhow::bail!("cron history was not JSON");
                };
                Ok(Output::Http(
                    http::Response::new(
                        200,
                        body.into_bytes(),
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

pub(super) fn child() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let report = run(
        Options::default(),
        Source::new(include_str!("schedules.py"), "schedules.py")
            .events([EventKind::Fetch, EventKind::Scheduled]),
        move |application, monitor| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .enable_all()
                .build()?;
            runtime.block_on(async move {
                // Advance at real speed without jumping ownership leases. Start in
                // the preceding minute: native alarm admission independently checks
                // real host UTC and must never see this fixture's future timestamp.
                let origin = std::time::Instant::now();
                let utc = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)?
                    .as_millis() as i64;
                let base = utc / 60_000 * 60_000 - 5_000;
                let clock: Arc<WallClock> =
                    Arc::new(move || base + origin.elapsed().as_millis() as i64);
                let application: Arc<dyn Application> = Arc::new(Observed(application));
                let configure = |config: &mut NodeConfig,
                                 services: &mut NodeServices,
                                 graph: &mut DeploymentGraph| {
                    config.runtime.handler_budget = Duration::from_secs(5);
                    config.actor.lease_ttl_ms = 5_000;
                    config.startup.recovery_retry_backoff = Duration::from_secs(5);
                    services.wall_clock = Some(clock.clone());
                    graph.primary.options.bindings.clear();
                    graph.primary.options.do_classes = vec![cellar::deploy::SQL_CLASS.into()];
                    graph
                        .primary
                        .options
                        .sql_bindings
                        .push(("DB".into(), "scheduled-db".into()));
                    graph.primary.crons = EXPRESSIONS.map(String::from).to_vec();
                };
                let bucket =
                    cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
                let data = directory.path().join("node");
                let (node, url) = start_graph(
                    &data,
                    application.clone(),
                    "schedules",
                    Some(bucket.clone()),
                    configure,
                )
                .await?;
                let client = reqwest::Client::builder()
                    .timeout(Duration::from_secs(10))
                    .build()?;
                ensure!(value(&client, &url, "/setup").await? == true);
                let entered = tokio::time::timeout(Duration::from_secs(10), async {
                    loop {
                        if value(&client, &url, "/state").await?["holding"] == true {
                            return Ok::<_, anyhow::Error>(());
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                })
                .await;
                match entered {
                    Ok(result) => result?,
                    Err(error) => anyhow::bail!(
                        "cron did not enter Python: {error}: state={} history={}",
                        value(&client, &url, "/state").await?,
                        value(&client, &url, "/history").await?
                    ),
                }
                ensure!(value(&client, &url, "/cross").await? == true);
                ensure!(value(&client, &url, "/release").await? == true);
                let mut history = tokio::time::timeout(Duration::from_secs(12), async {
                    loop {
                        let history = value(&client, &url, "/history").await?;
                        if history["items"].as_array().is_some_and(|items| {
                            items.len() == 5 && items.iter().all(|row| row["ended_at"].is_number())
                        }) {
                            return Ok::<_, anyhow::Error>(history);
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                })
                .await
                .context("cron retries did not settle")??;
                let mut state = value(&client, &url, "/state").await?;
                state.as_object_mut().context("cron state")?.remove("now");
                ensure!(
                    state["effects"].as_array().context("cron effects")?.len() == 4,
                    "occurrence effects duplicated: {state}"
                );
                for expression in EXPRESSIONS {
                    let rows = state["attempts"]
                        .as_array()
                        .context("cron attempts")?
                        .iter()
                        .filter(|row| row["cron"] == expression)
                        .collect::<Vec<_>>();
                    let expected = if expression == EXPRESSIONS[1] { 2 } else { 1 };
                    ensure!(rows.len() == expected, "wrong occurrence attempts: {state}");
                    for (index, row) in rows.iter().enumerate() {
                        ensure!(
                            row["attempt"] == index + 1
                                && row["occurrence"] == rows[0]["occurrence"],
                            "retry changed occurrence: {state}"
                        );
                    }
                }
                let items = history["items"].as_array().context("cron history")?;
                ensure!(
                    items
                        .iter()
                        .filter(|row| row["expression"] == EXPRESSIONS[2]
                            && row["status"] == "failed_no_retry"
                            && row["retry_requested"] == false)
                        .count()
                        == 1,
                    "no_retry lost: {history}"
                );
                ensure!(
                    items
                        .iter()
                        .filter(|row| row["expression"] == EXPRESSIONS[1]
                            && row["status"] == "failed"
                            && row["retry_requested"] == true)
                        .count()
                        == 1,
                    "failed cron was not retried: {history}"
                );
                ensure!(
                    items
                        .iter()
                        .filter(|row| row["status"] == "completed")
                        .count()
                        == 3,
                    "wrong settled cron history: {history}"
                );
                ensure!(value(&client, &url, "/expired").await? == true);
                stop(node, &monitor).await?;
                let (node, url) =
                    start_graph(&data, application, "schedules", Some(bucket), configure).await?;
                let mut restored = value(&client, &url, "/state").await?;
                restored
                    .as_object_mut()
                    .context("restored cron state")?
                    .remove("now");
                ensure!(restored == state, "cron receipts changed across restart");
                let mut restored_history = value(&client, &url, "/history").await?;
                // Inspection time is sampled per read; execution records and the
                // active durable alarm must remain identical.
                history
                    .as_object_mut()
                    .context("cron history page")?
                    .remove("observed_at");
                restored_history
                    .as_object_mut()
                    .context("restored cron history page")?
                    .remove("observed_at");
                ensure!(
                    restored_history == history,
                    "cron history changed across restart: {history} -> {restored_history}"
                );
                stop(node, &monitor).await
            })
        },
    )?;
    ensure!(
        report.complete()
            && report.native_outcomes.len() == 2
            && report.bridge.operations.settled(),
        "scheduled work leaked: {report:?}"
    );
    Ok(())
}
