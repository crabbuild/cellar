// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Ordinary alarm APIs, generation visibility and autonomous RF3 restart.

use super::*;
use futures_util::TryStreamExt;
use std::sync::atomic::AtomicBool;

const SCRIPT: &str = "quorum-alarm-service";

#[derive(Default)]
struct Observed {
    factories: AtomicUsize,
    entries: Mutex<Vec<String>>,
    changed: Notify,
    interrupt: AtomicBool,
    previous: Mutex<Option<Context>>,
}

struct AlarmCell {
    storage: Storage,
    observed: Arc<Observed>,
}

fn snapshot(storage: &Storage) -> Result<serde_json::Value> {
    Ok(
        json!({"alarm":storage.get_alarm()?,"calls":storage.get_json("calls")?.unwrap_or(json!([]))}),
    )
}

impl Cell for AlarmCell {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let storage = context.storage()?;
            match invocation {
                Invocation::Alarm { scheduled_ms } => {
                    ensure!(
                        storage.get_alarm()?.is_none(),
                        "active alarm exposed its lease"
                    );
                    ensure!(
                        self.storage.get_alarm()?.is_none(),
                        "cached Storage exposed its lease"
                    );
                    if let Some(previous) = self.observed.previous.lock().unwrap().replace(context)
                    {
                        ensure!(
                            previous.storage().is_err(),
                            "old alarm Context entered a new invocation"
                        );
                    }
                    let label = storage.get_json("label")?.context("alarm label")?;
                    let label = label.as_str().context("text alarm label")?.to_owned();
                    self.observed.entries.lock().unwrap().push(label.clone());
                    self.observed.changed.notify_waiters();
                    if label == "interrupt" && self.observed.interrupt.swap(false, Ordering::AcqRel)
                    {
                        return std::future::pending().await;
                    }
                    let mut calls = storage.get_json("calls")?.unwrap_or(json!([]));
                    calls
                        .as_array_mut()
                        .context("alarm calls")?
                        .push(json!({"label":label,"at":scheduled_ms}));
                    storage.put_json("calls", &calls)?;
                    if label == "rearm" {
                        storage.put_json("label", &json!("replacement"))?;
                        // Identical deadlines still create distinct installations.
                        storage.set_alarm(scheduled_ms)?;
                        ensure!(storage.get_alarm()? == Some(scheduled_ms));
                        ensure!(self.storage.get_alarm()? == Some(scheduled_ms));
                    }
                    Ok(Output::Done)
                }
                Invocation::CellRpc { method, args } => {
                    let RpcData::Json(args) = args else {
                        anyhow::bail!("JSON required")
                    };
                    let args: serde_json::Value = serde_json::from_str(&args)?;
                    match method.as_str() {
                        "Arm" => {
                            let at = cellar::asyncrt::wall_ms()
                                .checked_add(args["after"].as_i64().context("alarm delay")?)
                                .context("alarm deadline overflow")?;
                            storage.put_json("label", &args["label"])?;
                            storage.set_alarm(at)?;
                            ensure!(self.storage.get_alarm()? == Some(at));
                        }
                        "Cancel" => storage.delete_alarm()?,
                        "Rollback" => {
                            let before = storage.get_alarm()?;
                            let rolled: Result<()> = storage.transaction_sync(|tx| {
                                tx.set_alarm(1)?;
                                anyhow::bail!("rollback alarm installation")
                            });
                            ensure!(rolled.is_err());
                            ensure!(storage.get_alarm()? == before);
                        }
                        "Read" => (),
                        _ => anyhow::bail!("unknown alarm operation"),
                    }
                    Ok(Output::Rpc(RpcData::Json(snapshot(&storage)?.to_string())))
                }
                _ => anyhow::bail!("alarm or RPC required"),
            }
        })
    }
}

fn application(
    observed: Arc<Observed>,
) -> Result<(Arc<Registry>, cellar::engine_api::WorkerConfigOptions)> {
    ensure!(RaftClass::new("ReservedCheck", 1, [70; 32])?
        .query("__sqlIngressSequence", [71; 32], |_, _| Ok(vec![]))
        .is_err());
    let mut class = RaftClass::new("Counter", 1, [61; 32])?
        .initializer([62; 32], |_, _| Ok(()))?
        .durable_callbacks()?
        .query("Snapshot", [63; 32], |query, _| {
            Ok(serde_json::to_vec(&snapshot(&query.storage())?)?)
        })?;
    for (index, method) in ["Arm", "Cancel", "Rollback", "Read"]
        .into_iter()
        .enumerate()
    {
        class = class.cell_rpc(method, [64 + u8::try_from(index)?; 32])?;
    }
    let application = Arc::new(
        Registry::new(|context, invocation| {
            Box::pin(async move {
                let Invocation::Fetch(request) = invocation else {
                    anyhow::bail!("HTTP required")
                };
                let body = context
                    .take_body(request.body)?
                    .try_concat()
                    .await
                    .map_err(anyhow::Error::msg)?;
                let request: serde_json::Value = serde_json::from_slice(&body)?;
                let result = context
                    .exported_namespace("Counter")?
                    .get_by_name("alarm")
                    .rpc(
                        &context,
                        request["method"].as_str().context("method")?,
                        RpcData::Json(request["args"].to_string()),
                    )
                    .await;
                let (status, body) = match result {
                    Ok(RpcData::Json(body)) => (200, body.into_bytes()),
                    Ok(_) => anyhow::bail!("JSON reply required"),
                    Err(error) => (503, error.to_string().into_bytes()),
                };
                Ok(Output::Http(cellar::engine_api::HttpResponse {
                    status,
                    body,
                    headers: vec![],
                    stream: None,
                    websocket: None,
                    write_position: None,
                    observed_position: None,
                }))
            })
        })
        .raft_cell(class, move |context, _| {
            observed.factories.fetch_add(1, Ordering::AcqRel);
            Ok(Arc::new(AlarmCell {
                storage: context.storage()?,
                observed: observed.clone(),
            }))
        })?
        .raft_class(RaftClass::new("Other", 1, [72; 32])?.initializer([73; 32], |_, _| Ok(()))?)?,
    );
    let mut options = configuration::options(SCRIPT);
    options.do_classes.push("Counter".into());
    application.validate_deployment(&configuration::deployment(options.clone()))?;
    Ok((application, options))
}

async fn call(
    cluster: &ManagedCluster,
    method: &str,
    args: serde_json::Value,
) -> Result<(reqwest::StatusCode, serde_json::Value)> {
    let response = reqwest::Client::builder()
        .no_proxy()
        .timeout(TIMEOUT + Duration::from_secs(1))
        .build()?
        .post(format!(
            "http://{}/alarm",
            cluster.nodes[0].info().public_address
        ))
        .json(&json!({"method":method,"args":args}))
        .send()
        .await?;
    let status = response.status();
    let body = response.text().await?;
    let value = if status.is_success() {
        serde_json::from_str(&body)?
    } else {
        json!(body)
    };
    Ok((status, value))
}

async fn request(
    cluster: &ManagedCluster,
    method: &str,
    args: serde_json::Value,
) -> Result<serde_json::Value> {
    let (status, body) = call(cluster, method, args).await?;
    ensure!(
        status.is_success(),
        "public alarm {method}: {status} {body}"
    );
    Ok(body)
}

async fn inspect(cluster: &ManagedCluster, history: CellHistory) -> Result<serde_json::Value> {
    let cell = cluster.hosts[0].cell(history)?;
    let result = cell
        .query(
            ProposalId::new([69; 16])?,
            &cell.contract("Snapshot")?,
            vec![],
            QueryOptions::default(),
        )
        .await?;
    Ok(serde_json::from_slice(&result.result)?)
}

async fn wait_calls(
    cluster: &ManagedCluster,
    history: &CellHistory,
    count: usize,
) -> Result<serde_json::Value> {
    cellar::asyncrt::timeout(Duration::from_secs(20), async {
        loop {
            let value = inspect(cluster, history.clone()).await?;
            if value["calls"]
                .as_array()
                .is_some_and(|calls| calls.len() == count)
                && value["alarm"].is_null()
            {
                return Ok(value);
            }
            cellar::asyncrt::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .context("autonomous alarm completion deadline")?
}

pub async fn qualify_admission() -> Result<()> {
    for ambiguous in [false, true] {
        let observed = Arc::new(Observed::default());
        let (application, declared) = application(observed.clone())?;
        let definitions = if ambiguous {
            let class = application.resolve_raft_class("Counter")?;
            vec![
                (class.clone(), Durability::Quorum),
                (class, Durability::Quorum),
            ]
        } else {
            vec![(application.resolve_raft_class("Other")?, Durability::Quorum)]
        };
        let mut fixture = Fixture::with_definition_policies(
            false,
            definitions,
            application,
            Arc::new(catalog_fault::Store::default()),
        )
        .await?;
        fixture.configuration = Some(declared);
        fixture.sql_workers = 1;
        fixture.effects = Some(RaftEffectOptions {
            callback_timeout_ms: 5000,
            ..Default::default()
        });
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
            Ok(cluster) => cluster,
            Err(error) => {
                domain.shutdown().await;
                return Err(error);
            }
        };
        let work = domain
            .run(async {
                let (status, body) = call(&cluster, "Read", json!(null)).await?;
                let reason = if ambiguous {
                    "declared native namespace is ambiguous"
                } else {
                    "declared native namespace is missing"
                };
                ensure!(
                    status.is_server_error()
                        && body.as_str().is_some_and(|body| body.contains(reason)),
                    "native admission: {status} {body}"
                );
                ensure!(
                    observed.factories.load(Ordering::Acquire) == 0,
                    "refused native request constructed a cell"
                );
                ensure!(observed.entries.lock().unwrap().is_empty());
                Ok::<_, anyhow::Error>(())
            })
            .await
            .map_err(anyhow::Error::from)
            .and_then(std::convert::identity);
        let closed =
            managed_fixture::close_group_nodes(std::mem::take(&mut cluster.nodes), &cluster.hosts)
                .await;
        if work.is_err() || closed.is_err() {
            for root in std::mem::take(&mut cluster.roots) {
                eprintln!("retained native admission disk: {}", root.keep().display());
            }
        }
        domain.shutdown().await;
        work?;
        closed?;
    }
    Ok(())
}

pub async fn qualify(real_rustfs: bool, external: bool) -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("cellar::raft::sql_ingress=debug")
        .with_test_writer()
        .try_init();
    let mut failures = vec![];
    for rooted in [false, true] {
        for minimum in [Durability::Quorum, Durability::S3] {
            let result = case(real_rustfs, external, rooted, minimum).await;
            eprintln!("Alarm RustFS={real_rustfs} Axum={external} extents={rooted} minimum={minimum:?}: {}", if result.is_ok() { "PASS" } else { "FAILED" });
            if let Err(error) = result {
                failures.push(format!("extents={rooted} minimum={minimum:?}: {error:#}"));
            }
        }
    }
    ensure!(
        failures.is_empty(),
        "ordinary alarms: {}",
        failures.join("; ")
    );
    Ok(())
}

async fn case(real_rustfs: bool, external: bool, rooted: bool, minimum: Durability) -> Result<()> {
    let backend_case = match (external, minimum == Durability::S3) {
        (false, false) => "alarms-quorum",
        (false, true) => "alarms-s3",
        (true, false) => "alarms-axum-quorum",
        (true, true) => "alarms-axum-s3",
    };
    let backend = real_rustfs
        .then(|| rustfs::StoreConfig::load(backend_case, rooted))
        .transpose()?;
    let store = Arc::new(match &backend {
        Some(config) => catalog_fault::Store::around(config.open(None)?),
        None => catalog_fault::Store::default(),
    });
    let observed = Arc::new(Observed::default());
    let (application, declared) = application(observed.clone())?;
    let class = application.resolve_raft_class("Counter")?;
    let mut fixture =
        Fixture::with_definition_policies(rooted, vec![(class, minimum)], application, store)
            .await?;
    fixture.configuration = Some(declared);
    fixture.sql_workers = 1;
    fixture.effects = Some(RaftEffectOptions {
        callback_timeout_ms: 5000,
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
        Ok(cluster) => cluster,
        Err(error) => {
            if let Some(http) = http {
                http.close().await?;
            }
            domain.shutdown().await;
            return Err(error);
        }
    };
    let work = domain
        .run(async {
            let first = request(&cluster, "Read", json!(null)).await?;
            ensure!(first == json!({"alarm":null,"calls":[]}));
            for method in ["__sqlIngressSequence", "__sys_raft_native_work"] {
                let (status, _) = call(&cluster, method, json!(null)).await?;
                ensure!(
                    status.is_server_error(),
                    "public caller entered a private operation"
                );
            }
            let namespace = cellar_native::cell_id::Namespace::new(
                "Counter",
                format!("cells:v1:{}:{SCRIPT}:Counter", SCRIPT.len()),
            );
            let group = cluster.history;
            let history = cluster.hosts[0]
                .resolve_cell_history(
                    group.cluster,
                    group.namespace,
                    CellId::new(namespace.id_from_name("alarm").scope())?,
                    QueryOptions::default(),
                )
                .await?
                .context("public alarm enrollment")?;
            ensure!(
                inspect(&cluster, history.clone()).await? == first,
                "immutable alarm read differs"
            );
            request(&cluster, "Arm", json!({"label":"cancelled","after":60_000})).await?;
            request(&cluster, "Rollback", json!(null)).await?;
            request(&cluster, "Cancel", json!(null)).await?;
            ensure!(inspect(&cluster, history.clone()).await?["alarm"].is_null());
            request(&cluster, "Arm", json!({"label":"rearm","after":100})).await?;
            let completed = wait_calls(&cluster, &history, 2).await?;
            ensure!(
                completed["calls"][0]["label"] == "rearm"
                    && completed["calls"][1]["label"] == "replacement"
            );
            ensure!(
                completed["calls"][0]["at"] == completed["calls"][1]["at"],
                "same-deadline replacement was lost"
            );

            observed.interrupt.store(true, Ordering::Release);
            request(&cluster, "Arm", json!({"label":"interrupt","after":100})).await?;
            cellar::asyncrt::timeout(TIMEOUT, async {
                loop {
                    let changed = observed.changed.notified();
                    if observed
                        .entries
                        .lock()
                        .unwrap()
                        .iter()
                        .any(|label| label == "interrupt")
                    {
                        break;
                    }
                    changed.await;
                }
            })
            .await
            .context("interrupted alarm did not enter")?;
            bindings::restart(&mut cluster).await?;
            // No client request is needed to rediscover the persisted retry.
            cellar::asyncrt::timeout(Duration::from_secs(20), async {
                loop {
                    let changed = observed.changed.notified();
                    if observed
                        .entries
                        .lock()
                        .unwrap()
                        .iter()
                        .filter(|label| *label == "interrupt")
                        .count()
                        >= 2
                    {
                        break;
                    }
                    changed.await;
                }
            })
            .await
            .context("retained alarm did not resume autonomously")?;
            let completed = wait_calls(&cluster, &history, 3).await?;
            ensure!(completed["calls"][2]["label"] == "interrupt");
            ensure!(observed
                .entries
                .lock()
                .unwrap()
                .iter()
                .all(|label| label != "cancelled"));

            let stopped = cluster.nodes.split_off(1);
            for node in stopped {
                node.shutdown(cellar::sdk::ShutdownMode::Preserve).await?;
            }
            let (status, _) =
                call(&cluster, "Arm", json!({"label":"must-refuse","after":100})).await?;
            ensure!(
                status.is_server_error(),
                "public alarm write bypassed quorum"
            );
            ensure!(!observed
                .entries
                .lock()
                .unwrap()
                .iter()
                .any(|label| label == "must-refuse"));
            Ok::<_, anyhow::Error>(())
        })
        .await
        .map_err(anyhow::Error::from)
        .and_then(std::convert::identity);
    if work.is_err() {
        for (index, host) in cluster.hosts.iter().enumerate() {
            if let Ok(group) = host.group(cluster.history) {
                eprintln!(
                    "alarm failure node={index} status={:?}",
                    group.replication_status(TIMEOUT).await
                );
            }
        }
    }
    let mut cleanup = vec![];
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
            eprintln!("retained alarm disk: {}", root.keep().display());
        }
    }
    domain.shutdown().await;
    work?;
    ensure!(cleanup.is_empty(), "alarm joined cleanup: {cleanup:?}");
    Ok(())
}
