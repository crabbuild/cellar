// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Explicit, manual RF3 public-binding measurements. All voters share a process;
//! this fixture does not establish independently constrained node performance.
#![allow(clippy::disallowed_methods)]

use super::*;
use futures_util::TryStreamExt as _;
use std::{future::Future, time::Instant};

pub struct Settings {
    pub rooted: bool,
    pub seconds: u64,
    pub repetitions: usize,
}

impl Settings {
    pub fn load() -> Result<Self> {
        let rooted = match std::env::var("CELLAR_PRIMITIVES_PERF_IMAGE")?.as_str() {
            "materialized" => false,
            "extents" => true,
            _ => anyhow::bail!("explicit materialized/extents image required"),
        };
        let seconds = std::env::var("CELLAR_PRIMITIVES_PERF_SECONDS")?.parse()?;
        let repetitions = std::env::var("CELLAR_PRIMITIVES_PERF_REPETITIONS")?.parse()?;
        ensure!((1..=120).contains(&seconds) && (1..=5).contains(&repetitions));
        Ok(Self {
            rooted,
            seconds,
            repetitions,
        })
    }

    pub fn matrix(&self) -> Vec<(usize, usize)> {
        (0..self.repetitions)
            .flat_map(|repeat| {
                let order = if repeat % 2 == 0 { [1, 4] } else { [4, 1] };
                order
                    .into_iter()
                    .map(move |concurrency| (repeat, concurrency))
            })
            .collect()
    }
}

pub struct Window {
    pub report: serde_json::Value,
    pub successes: Vec<u64>,
    pub errors: usize,
}

struct UnusedCounter;

impl Cell for UnusedCounter {
    fn invoke(self: Arc<Self>, _: Context, _: Invocation) -> HandlerFuture<Output> {
        Box::pin(async { anyhow::bail!("benchmark uses stateless bindings") })
    }
}

/// Closed-loop service latency includes the entire HTTP request and response.
/// A failed worker stops: a possibly committed mutation is never retried with a
/// fresh identity to manufacture an error-free measurement.
pub async fn window<F, Fut>(seconds: u64, concurrency: usize, operation: F) -> Window
where
    F: Fn(usize, u64) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let start = Instant::now();
    let deadline = start + Duration::from_secs(seconds);
    let workers = (0..concurrency).map(|worker| {
        let operation = &operation;
        async move {
            let mut successes = 0;
            let mut latencies = Vec::new();
            let mut errors = Vec::new();
            while Instant::now() < deadline {
                let entered = Instant::now();
                let result = operation(worker, successes + 1).await;
                latencies.push(entered.elapsed().as_micros() as u64);
                match result {
                    Ok(()) => successes += 1,
                    Err(error) => {
                        errors.push(format!(
                            "worker={worker} operation={}: {error:#}",
                            successes + 1
                        ));
                        break;
                    }
                }
            }
            (successes, latencies, errors)
        }
    });
    let results = futures_util::future::join_all(workers).await;
    let elapsed = start.elapsed().as_secs_f64();
    let successes: Vec<_> = results.iter().map(|r| r.0).collect();
    let mut latencies: Vec<_> = results.iter().flat_map(|r| r.1.iter().copied()).collect();
    let errors: Vec<_> = results.into_iter().flat_map(|r| r.2).collect();
    latencies.sort_unstable();
    let percentile = |p: usize| {
        latencies
            .get((latencies.len() * p).div_ceil(100).saturating_sub(1))
            .map(|v| *v as f64 / 1000.0)
    };
    let total: u64 = successes.iter().sum();
    Window {
        report: json!({"requestedSeconds":seconds,"elapsedSeconds":elapsed,"concurrency":concurrency,
            "attempts":latencies.len(),"successfulRequests":total,"errors":errors.len(),
            "errorSamples":errors,"requestsPerSecond":total as f64 / elapsed,
            "p50Ms":percentile(50),"p95Ms":percentile(95),"p99Ms":percentile(99),
            "maxMs":latencies.last().map(|v| *v as f64 / 1000.0),"latencyMicros":latencies}),
        successes,
        errors: errors.len(),
    }
}

pub fn emit(
    kind: &str,
    settings: &Settings,
    repeat: usize,
    phase: &str,
    result: &Window,
    verification: &Result<()>,
    resources: serde_json::Value,
) {
    eprintln!(
        "PRIMITIVES_PERF {}",
        json!({"primitive":kind,"imageMode":if settings.rooted {"extents"}else{"materialized"},
        "repeat":repeat,"phase":phase,"durability":"quorum","voters":3,"voterProcesses":1,
        "basePayloadBytes":256,"verified":verification.is_ok(),"verificationError":verification.as_ref().err().map(|e|format!("{e:#}")),
        "measurement":result.report,"resources":resources})
    );
}

pub(super) async fn request(
    client: &reqwest::Client,
    url: &str,
    input: serde_json::Value,
) -> Result<serde_json::Value> {
    let response = client.post(url).json(&input).send().await?;
    let status = response.status();
    let body = response.bytes().await?;
    ensure!(
        status == 200,
        "HTTP {status}: {}",
        String::from_utf8_lossy(&body)
    );
    Ok(serde_json::from_slice(&body)?)
}

async fn handler(context: Context, invocation: Invocation) -> Result<Output> {
    let Invocation::Fetch(request) = invocation else {
        anyhow::bail!("stateless HTTP required")
    };
    let bytes = context
        .take_body(request.body)?
        .try_concat()
        .await
        .map_err(anyhow::Error::msg)?;
    let input: serde_json::Value = serde_json::from_slice(&bytes)?;
    let key = input["key"].as_str().unwrap_or("setup");
    let value = input["value"].as_str().unwrap_or("");
    let operation = request.url.rsplit('/').next().context("operation")?;
    let result: Result<serde_json::Value> = async {
        if request.url.contains("/sql/") {
            let database = context.sql("DB")?;
            match operation {
                "setup" => { database.exec(&context, "CREATE TABLE perf(id TEXT PRIMARY KEY, value TEXT NOT NULL)").await?; Ok(json!(true)) }
                "write" => {
                    let result = database.prepare("INSERT INTO perf VALUES(?,?) ON CONFLICT(id) DO UPDATE SET value=excluded.value")
                        .bind_json([json!(key),json!(value)])?.run(&context).await?;
                    ensure!(result.meta.changes == 1); Ok(json!(true))
                }
                "read" => Ok(json!(database.prepare("SELECT value FROM perf WHERE id=?")
                    .bind_json([json!(key)])?.first(&context, Some("value")).await?)),
                _ => anyhow::bail!("unknown SQL operation"),
            }
        } else {
            let namespace = context.kv("KV")?;
            match operation {
                "write" => { namespace.put_text(&context,key,value,cellar::sdk::kv::PutOptions::default()).await?; Ok(json!(true)) }
                "read" => match namespace.get(&context,key,cellar::sdk::kv::ReadType::Text).await? {
                    Some(cellar::sdk::kv::ReadValue::Text(value)) => Ok(json!(value)),
                    None => Ok(json!(null)),
                    _ => anyhow::bail!("non-text KV result"),
                },
                _ => anyhow::bail!("unknown KV operation"),
            }
        }
    }.await;
    let (status, body) = match result {
        Ok(value) => (200, serde_json::to_vec(&value)?),
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
}

pub async fn bindings(kind: &str) -> Result<()> {
    let settings = Settings::load()?;
    let backend = rustfs::StoreConfig::load(&format!("perf-{kind}"), settings.rooted)?;
    let store = Arc::new(catalog_fault::Store::around(backend.open(None)?));
    let application = Arc::new(
        Registry::new(|context, invocation| Box::pin(handler(context, invocation))).raft_cell(
            RaftClass::new("Counter", 1, [191; 32])?
                .initializer([203; 32], |_, _| Ok(()))?
                .json_cell_rpc("Unused", [192; 32])?,
            |_, _| Ok(Arc::new(UnusedCounter)),
        )?,
    );
    let class = if kind == "sql" {
        cellar::deploy::SQL_CLASS
    } else {
        cellar::deploy::KV_CLASS
    };
    let mut declared = configuration::options("primitives-perf");
    declared.do_classes = vec!["Counter".into(), class.into()];
    if kind == "sql" {
        declared.sql_bindings = vec![("DB".into(), "perf/database".into())];
    } else {
        declared.kv_bindings = vec![("KV".into(), "perf/cache".into())];
    }
    application.validate_configuration(&declared)?;
    let mut fixture =
        Fixture::with_namespaces(settings.rooted, &["Counter", class], application, store).await?;
    fixture.configuration = Some(declared);
    fixture.sql_workers = 1;
    fixture.max_input_bytes = 16 * 1024;
    fixture.max_client_identities = 4;
    if kind == "kv" {
        // Match the ordinary KV fixture's explicit node-owned payload bank.
        fixture.payloads = Some(CellGroupPayloadLimits {
            max_operations: 2,
            max_retained_bytes: 600 * 1024 * 1024,
            max_payload_bytes: 64 * 1024 * 1024 + 4096,
            io_timeout: Duration::from_secs(10),
        });
    }
    let cluster = Box::pin(ManagedCluster::start_with_applications(
        fixture,
        &[],
        ArchiveSetup::Initialized,
        false,
        None,
        FirstGroupSetup::Normal,
    ))
    .await?;
    let domain = cluster.fixtures[0].domain.clone();
    let outcome = domain.run(async {
        named_service::activate(&cluster,"perf-archive").await?;
        let client = reqwest::Client::builder().no_proxy().timeout(TIMEOUT + Duration::from_secs(1)).build()?;
        let base = format!("http://{}/{kind}",cluster.nodes[0].info().public_address);
        if kind == "sql" { request(&client,&format!("{base}/setup"),json!({})).await?; }
        let payload = "x".repeat(256);
        let mut failed = false;
        for (repeat, concurrency) in settings.matrix() {
            let mut last = vec![String::new();concurrency];
            for (phase, seconds) in [("warmup",2),("write",settings.seconds),("read",settings.seconds)] {
                let write = phase != "read";
                let prefix = format!("{repeat}-{concurrency}-{phase}");
                let result = window(seconds,concurrency,|worker,sequence| {
                    let client = &client; let base = &base; let payload = &payload; let last = &last; let prefix = &prefix;
                    async move {
                        let key = format!("worker-{worker}");
                        if write {
                            let value = format!("{prefix}/{sequence}/{payload}");
                            ensure!(request(client,&format!("{base}/write"),json!({"key":key,"value":value})).await? == json!(true));
                        } else {
                            ensure!(request(client,&format!("{base}/read"),json!({"key":key})).await? == json!(last[worker]),"read value mismatch");
                        }
                        Ok(())
                    }
                }).await;
                let verification = async {
                    if write {
                        for (worker, last_value) in last.iter_mut().enumerate() {
                            if result.successes[worker] > 0 {
                                *last_value = format!("{prefix}/{}/{payload}",result.successes[worker]);
                                ensure!(request(&client,&format!("{base}/read"),json!({"key":format!("worker-{worker}")})).await? == json!(last_value),"final acknowledged value mismatch");
                            }
                        }
                    }
                    ensure!(result.errors == 0,"workload recorded request errors");
                    Ok(())
                }.await;
                let resources = json!(cluster.hosts.iter().map(|host|json!({"usage":host.resource_usage(),"performance":host.performance()})).collect::<Vec<_>>());
                emit(kind,&settings,repeat,phase,&result,&verification,resources);
                if verification.is_err() { failed = true; break; }
            }
        }
        ensure!(!failed,"one or more measured windows failed");
        Ok::<_,anyhow::Error>(())
    }).await;
    let cleanup =
        managed_fixture::close_group_nodes_with_groups(cluster.nodes, &cluster.hosts, 2).await;
    if !matches!(&outcome, Ok(Ok(()))) || cleanup.is_err() {
        for root in cluster.roots {
            eprintln!(
                "retained failed performance disk: {}",
                root.keep().display()
            );
        }
    }
    cleanup?;
    outcome??;
    Ok(())
}
