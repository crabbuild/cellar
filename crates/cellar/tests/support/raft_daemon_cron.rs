// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A cron-only application through compiled provisioning and three daemon voters.
use super::*;
use std::sync::{atomic::AtomicU64, Mutex};

const SCRIPT: &str = "raft-cli";
const EXPRESSION: &str = "* * * * *";

pub(crate) fn member(mode: &str) -> Option<&str> {
    mode.strip_prefix("cron-resume-")
        .or_else(|| mode.strip_prefix("cron-"))
}

fn deployment() -> cellar::generation::DeploymentGraph {
    let mut graph = graph::graph(SCRIPT, "", vec![]);
    graph.primary.crons = vec![EXPRESSION.into()];
    graph
        .primary
        .options
        .vars
        .push(("CRON_VALUE".into(), "admitted".into()));
    graph
}

fn minimum() -> Result<Durability> {
    match std::env::var("CELLAR_RAFT_CRON_MINIMUM")?.as_str() {
        "quorum" => Ok(Durability::Quorum),
        "s3" => Ok(Durability::S3),
        _ => anyhow::bail!("invalid cron namespace durability"),
    }
}

#[derive(Default)]
struct Api {
    cells: Mutex<Option<RaftCells>>,
    next: AtomicU64,
}

impl Api {
    async fn page(&self) -> Result<Value> {
        let cells = self
            .cells
            .lock()
            .map_err(|_| anyhow::anyhow!("cron binding poisoned"))?
            .clone()
            .context("cron service not installed")?;
        // One outer budget includes name resolution and the immutable read.
        tokio::time::timeout(Duration::from_secs(3), async {
            let group = history()?;
            let options = QueryOptions {
                consistency: QueryConsistency::Linearizable,
                timeout: Duration::from_secs(3),
                ..Default::default()
            };
            let Some(history) = cells
                .resolve_cell_history(
                    group.cluster,
                    group.namespace,
                    CellId::new(cellar_logic::cron::reserved_cell(SCRIPT))?,
                    options.clone(),
                )
                .await?
            else {
                return Ok(serde_json::json!({"items":[]}));
            };
            let cell = cells.cell(history)?;
            let serial = self
                .next
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                    value.checked_add(1)
                })
                .map_err(|_| anyhow::anyhow!("query identity exhausted"))?;
            let mut nonce = [0; 16];
            nonce[..8].copy_from_slice(&(serial + 1).to_be_bytes());
            nonce[8..12].copy_from_slice(&std::process::id().to_be_bytes());
            let page = cell
                .query(
                    ProposalId::new(nonce)?,
                    &cell.contract("__cronHistory")?,
                    b"{}".to_vec(),
                    options,
                )
                .await?;
            Ok(serde_json::from_slice(&page.result)?)
        })
        .await?
    }

    async fn reply(&self) -> (u16, Vec<u8>) {
        match self.page().await {
            Ok(value) => (200, serde_json::to_vec(&value).unwrap()),
            Err(error) => (503, format!("cron query refused: {error:#}").into_bytes()),
        }
    }
}

fn application(api: Arc<Api>, retained: bool) -> Result<Arc<dyn Application>> {
    Ok(Arc::new(
        Registry::new(move |context, invocation| {
            let api = api.clone();
            Box::pin(async move {
                match invocation {
                    Invocation::Scheduled(scheduled) => {
                        ensure!(context.identity().is_none() && context.storage().is_err());
                        ensure!(context.var("CRON_VALUE") == Some("admitted"));
                        ensure!(scheduled.cron == EXPRESSION);
                        cellar::note!(
                            "{}",
                            serde_json::json!({"event":"cron_handler_entered",
                        "scheduled_at":scheduled.scheduled_ms,"retained":retained})
                        );
                        if !retained {
                            std::future::pending::<()>().await;
                        }
                        Ok(Output::Done)
                    }
                    Invocation::Fetch(request) => {
                        ensure!(request.method == "GET");
                        let (status, body) = api.reply().await;
                        Ok(Output::Http(
                            cellar::sdk::http::Response::new(
                                status,
                                body,
                                vec![("content-type".into(), "application/json".into())],
                            )
                            .into_http(&context)?,
                        ))
                    }
                    _ => anyhow::bail!("unexpected cron-only invocation"),
                }
            })
        })
        .raft_primitives()?,
    ))
}

fn definition() -> Result<Arc<RaftClass>> {
    let application = application(Arc::new(Api::default()), false)?;
    application.validate_deployment(&deployment().primary)?;
    Ok(application.resolve_raft_class(cellar_logic::cron::RESERVED_CLASS)?)
}

fn operator(domain: Arc<ExecutionDomain>, root: &Path) -> Result<Operator> {
    let class = definition()?;
    let mut operator = Operator::open_for_class(domain, root, class.as_ref())?;
    operator.namespace.minimum_durability = minimum()?;
    Ok(operator)
}

async fn initialize(root: &Path) -> Result<()> {
    let domain = Arc::new(ExecutionDomain::new(tokio::runtime::Handle::current()));
    let result = domain.run(async {
        let operator = operator(domain.clone(), root)?;
        let snapshot = operator.fleet.initialize(TIMEOUT).await?;
        let identity = request(10, snapshot.revision());
        let plan = serde_json::json!({
            "catalog_id":hex::encode(operator.fleet.catalog_id()?),
            "cluster_id":hex::encode(operator.namespace.cluster.bytes()),
            "incarnation_id":hex::encode([81;16]),"coordination_prefix":"daemon-control/",
            "catalog_prefix":"daemon-managed/","namespace_id":hex::encode(operator.namespace.namespace.bytes()),
            "script":SCRIPT,"version":"test","class":cellar_logic::cron::RESERVED_CLASS,
            "principal":identity.principal,"request_id":hex::encode(identity.id),
            "operation_id":hex::encode(identity.operation),"expected_revision":identity.expected_revision,
            "image_storage":std::env::var("CELLAR_RAFT_PROCESS_IMAGE_MODE")?,
            "minimum_durability":std::env::var("CELLAR_RAFT_CRON_MINIMUM")?,
            "configuration":{"vars":[["CRON_VALUE","admitted"]],"crons":[EXPRESSION],
                "cron_history":{"prefix":"test/","deployment_version":"test","schedule_revision":null}}
        });
        for (field, value) in [("minimum_durability", Value::from("unsafe")),
            ("configuration", serde_json::json!({"crons":[EXPRESSION]}))] {
            let mut invalid = plan.clone(); invalid[field] = value;
            let refused = provision_call_for(root, &invalid, "provision-cron").await?;
            ensure!(!refused.status.success(), "invalid cron plan admitted");
            ensure!(operator.fleet.read(TIMEOUT).await?.revision() == identity.expected_revision);
        }
        let admitted = provision_call_for(root, &plan, "provision-cron").await?;
        ensure!(admitted.status.success(), "cron provisioning: {}", String::from_utf8_lossy(&admitted.stderr));
        let output: Value = serde_json::from_slice(&admitted.stdout)?;
        ensure!(output["class"] == cellar_logic::cron::RESERVED_CLASS
            && output["schema_digest"] == hex::encode(operator.namespace.schema_digest)
            && output["minimum_durability"] == plan["minimum_durability"]);
        let replay = provision_call_for(root, &plan, "provision-cron").await?;
        ensure!(replay.status.success() && replay.stdout == admitted.stdout);
        let declaration = operator.declaration()?;
        let capabilities = RaftManagedHostOptions::capability_digest(&[declaration])?;
        for index in 0..3 {
            let snapshot = operator.fleet.read(TIMEOUT).await?;
            operator.fleet.register_node(&snapshot, &request(11+index, snapshot.revision()),
                &node_spec(index, capabilities)?, TIMEOUT).await?;
        }
        Ok::<_,anyhow::Error>(())
    }).await;
    domain.shutdown().await;
    result?
}

pub(crate) fn child(root: PathBuf, mode: &str) -> Result<()> {
    let api = Arc::new(Api::default());
    let registered = api.clone();
    let retained = mode.starts_with("cron-resume-");
    let host = Host::local(deployment()).register(SCRIPT, "test", move || {
        application(registered.clone(), retained)
    })?;
    if mode == "provision-cron" {
        return host.run();
    }
    let index: u8 = member(mode).context("cron member absent")?.parse()?;
    ensure!(index < 3);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()?;
    let domain = Arc::new(ExecutionDomain::new(runtime.handle().clone()));
    let operator = Arc::new(operator(domain.clone(), &root)?);
    let prepared = operator.clone();
    let group = history()?;
    let preparing_root = root.clone();
    let auxiliary = runtime.handle().clone();
    let (stop, mut stopped) = tokio::sync::watch::channel(false);
    let (started, joined) = tokio::sync::oneshot::channel();
    let fixture = operator.clone();
    let host = host.raft_cells_with_startup(
        SCRIPT,
        move |native| async move {
            let store = prepared.bucket.raft_store();
            let mut cells = prepared
                .prepare(native, index, retained, false, store)
                .await?;
                cells.config.host.effects = Some(RaftEffectOptions {
                    callback_timeout_ms: 30_000,
                    ..Default::default()
                });
            cells.config.shared.sql.workers = 1;
            Ok(cells)
        },
        move |service| async move {
            if index == 0 && !retained {
                provision_first_group(&fixture, &service, &preparing_root, group).await?;
            }
            let selected = service
                .start_managed_namespace_first_group(
                    group.cluster,
                    group.namespace,
                    Duration::from_secs(20),
                )
                .await?;
            ensure!(selected == group);
            *api.cells.lock().unwrap() = Some(service);
            let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
            listener.set_nonblocking(true)?;
            let address = listener.local_addr()?;
            let router = axum::Router::new().route(
                "/cron",
                axum::routing::get(move || {
                    let api = api.clone();
                    async move {
                        let (status, body) = api.reply().await;
                        (axum::http::StatusCode::from_u16(status).unwrap(), body)
                    }
                }),
            );
            let task = auxiliary.spawn(async move {
                let listener = tokio::net::TcpListener::from_std(listener)?;
                cellar::note!("cellar axum listening on {address}");
                axum::serve(listener, router)
                    .with_graceful_shutdown(async move {
                        let _ = stopped.changed().await;
                    })
                    .await
            });
            started.send(task).map_err(|task| {
                task.abort();
                anyhow::anyhow!("cron server join dropped")
            })?;
            Ok(())
        },
    )?;
    let result = host.run_until(std::future::pending());
    let _ = stop.send(true);
    let closed = runtime.block_on(async {
        let result = match joined.await {
            Ok(task) => task.await?.map_err(anyhow::Error::from),
            Err(_) => Ok(()),
        };
        domain.shutdown().await;
        result
    });
    result.and(closed)
}

async fn shutdown(
    client: &reqwest::Client,
    processes: &mut [Process],
    addresses: &[ManagedIngress],
) -> Result<()> {
    futures_util::future::try_join_all(addresses.iter().map(|address| async {
        client
            .post(format!("{}/shutdown", address.internal))
            .send()
            .await?
            .error_for_status()?;
        Ok::<_, anyhow::Error>(())
    }))
    .await?;
    for process in processes {
        process.exited(true).await?;
    }
    Ok(())
}

async fn pages(client: &reqwest::Client, addresses: &[ManagedIngress]) -> Result<Vec<Value>> {
    let mut pages = Vec::new();
    for address in addresses {
        for entrance in [&address.native, &address.axum] {
            pages.push(
                client
                    .get(format!("{entrance}/cron"))
                    .send()
                    .await?
                    .error_for_status()?
                    .json()
                    .await?,
            );
        }
    }
    Ok(pages)
}

pub(crate) async fn qualify() -> Result<()> {
    ensure!(
        std::env::var_os(RUSTFS_ENDPOINT).is_some(),
        "cron process gate requires owned RustFS"
    );
    ensure!(
        !docker::enabled(),
        "cron native process gate does not qualify Docker voters"
    );
    let root = tempfile::tempdir()?;
    let mut processes = Vec::new();
    let result = async {
        initialize(root.path()).await?;
        for index in 0..3 {
            let disk = root.path().join(format!("disk-{index}"));
            processes.push(Process::spawn(root.path(), &format!("cron-{index}"),
                &[std::ffi::OsStr::new("--data-dir"), disk.as_os_str()], false)?);
        }
        let mut addresses = Vec::new();
        for process in &mut processes { addresses.push(process.managed_addresses().await?); }
        let client = reqwest::Client::builder().no_proxy().timeout(TIMEOUT).build()?;
        let first = tokio::time::timeout(Duration::from_secs(75), async {
            loop {
                let page = pages(&client, &addresses).await?;
                if let Some(item) = page[0]["items"].as_array().and_then(|rows|rows.first()) {
                    if item["status"] == "started" { break Ok::<_,anyhow::Error>(item.clone()); }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }).await??;
        for page in pages(&client,&addresses).await? {
            ensure!(page["items"].as_array().context("cron pages")?.iter().any(|v|v["scheduled_at"]==first["scheduled_at"]&&v["status"]=="started"));
        }
        shutdown(&client, &mut processes, &addresses).await?;
        let entries = processes.iter().map(Process::logs).collect::<Result<Vec<_>>>()?.iter()
            .map(|log|log.lines().filter(|line|serde_json::from_str::<Value>(line).is_ok_and(|v|v["event"]=="cron_handler_entered")).count()).sum::<usize>();
        ensure!(entries == 1, "old quorum executed more than one callback before joined stop: {entries}");
        let old_pids = processes.iter().map(Process::pid).collect::<Result<Vec<_>>>()?;
        processes.clear();
        for index in 0..3 {
            let disk = root.path().join(format!("disk-{index}"));
            processes.push(Process::spawn(root.path(), &format!("cron-resume-{index}"),
                &[std::ffi::OsStr::new("--data-dir"), disk.as_os_str()], false)?);
        }
        addresses.clear();
        for (index, process) in processes.iter_mut().enumerate() {
            ensure!(process.pid()? != old_pids[index]);
            addresses.push(process.managed_addresses().await?);
        }
        let completed = tokio::time::timeout(Duration::from_secs(45), async {
            loop {
                let observed = pages(&client,&addresses).await?;
                if observed.iter().all(|page|page["items"].as_array().is_some_and(|rows| rows.iter().any(|v|v["scheduled_at"]==first["scheduled_at"]&&v["status"]=="completed"&&v["attempt"]==2))) {
                    break Ok::<_,anyhow::Error>(observed);
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }).await??;
        shutdown(&client, &mut processes[..2], &addresses[..2]).await?;
        for entrance in [&addresses[2].native,&addresses[2].axum] {
            let reply = client.get(format!("{entrance}/cron")).send().await?;
            ensure!(reply.status()==503, "cron query fell back without quorum");
        }
        shutdown(&client, &mut processes[2..], &addresses[2..]).await?;
        cellar::note!("{}",serde_json::json!({"event":"cron_daemon_qualification_passed",
            "independent_voter_processes":3,"image":std::env::var("CELLAR_RAFT_PROCESS_IMAGE_MODE")?,
            "minimum":std::env::var("CELLAR_RAFT_CRON_MINIMUM")?,"first":first,
            "completed_pages":completed,"old_pids":old_pids,"quorum_refusal":true}));
        Ok::<_,anyhow::Error>(())
    }.await;
    drop(processes);
    if let Err(error) = result {
        let kept = root.keep();
        return Err(error.context(format!(
            "cron process disks/logs retained at {}",
            kept.display()
        )));
    }
    Ok(())
}
