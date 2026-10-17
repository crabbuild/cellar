//! Independent open-loop client. Its executors, clocks and files belong to the
//! measuring process, outside the Cellar application execution domain.
#![allow(clippy::disallowed_methods)]

use anyhow::{ensure, Context, Result};
use fleetcare::contracts::{Command, Receipt};
use futures_util::{stream, StreamExt, TryStreamExt};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter},
    sync::{Mutex, Semaphore},
};

#[path = "../client_attachments.rs"]
mod attachments;
#[path = "../client_checks.rs"]
mod checks;
#[path = "../client_subscriptions.rs"]
mod subscriptions;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    action: String,
    urls: Vec<String>,
    token: String,
    manifest: PathBuf,
    ledger: PathBuf,
    report: PathBuf,
    #[serde(default)]
    phase: String,
    #[serde(default = "default_seconds")]
    seconds: u64,
    #[serde(default = "default_rate")]
    rate: u64,
    #[serde(default = "default_concurrency")]
    concurrency: usize,
    #[serde(default = "default_bytes")]
    bytes: usize,
    #[serde(default = "default_mix")]
    mix: String,
    #[serde(default)]
    seed: u64,
    #[serde(default)]
    skew: String,
}

fn default_seconds() -> u64 {
    30
}

fn default_rate() -> u64 {
    25
}

fn default_concurrency() -> usize {
    128
}

fn default_bytes() -> usize {
    1024
}

fn default_mix() -> String {
    "core".into()
}

#[derive(Clone, Deserialize, Serialize)]
struct Entry {
    kind: String,
    tenant: usize,
    id: usize,
    scope: String,
    path: String,
}

#[derive(Deserialize, Serialize)]
struct Manifest {
    profile: String,
    count: usize,
    cells: Vec<Entry>,
}

#[derive(Deserialize, Serialize)]
struct Attempt {
    phase: String,
    index: u64,
    operation: String,
    path: String,
    ingress: usize,
    scheduled_us: u64,
    sent_us: Option<u64>,
    finished_us: u64,
    status: String,
    bytes: usize,
    command: Option<Command>,
    receipt: Option<Receipt>,
    scope: Option<String>,
    owner: Option<String>,
}

// Relative precision is at least 1/64 above 64us. Each recorded value maps to
// an inclusive upper bound, so percentile reporting cannot round it downward.
fn bucket(us: u64) -> u64 {
    if us < 64 {
        return us;
    }
    let shift = (63 - us.leading_zeros()).saturating_sub(6);
    let width = 1u64 << shift;
    (us / width + 1) * width - 1
}

fn micros(elapsed: Duration) -> u64 {
    elapsed.as_micros().min(u64::MAX as u128) as u64
}

fn outcome_category(status: &str) -> String {
    if !status.starts_with("error:") {
        return status.into();
    }
    if let Some(http) = status.strip_prefix("error:HTTP ") {
        return format!(
            "error:http_{}",
            http.split_whitespace()
                .next()
                .unwrap_or("unknown")
                .trim_end_matches(':')
        );
    }
    if status.starts_with("error:protocol:") {
        return "error:protocol".into();
    }
    if status.contains("timed out") || status.contains("timeout") {
        return "error:timeout".into();
    }
    "error:transport_or_application".into()
}

#[derive(Default, Serialize)]
struct Metrics {
    attempts: u64,
    sent: u64,
    successful: u64,
    acknowledged_commands: u64,
    bytes: u64,
    outcomes: BTreeMap<String, u64>,
    latency_us: BTreeMap<u64, u64>,
    success_latency_us: BTreeMap<u64, u64>,
    outcome_latency_us: BTreeMap<String, BTreeMap<u64, u64>>,
    service_us: BTreeMap<u64, u64>,
    scheduling_debt_us: BTreeMap<u64, u64>,
}

impl Metrics {
    fn record(&mut self, a: &Attempt) {
        self.attempts += 1;
        self.sent += u64::from(a.sent_us.is_some());
        self.successful += u64::from(a.status == "ok");
        self.acknowledged_commands += u64::from(a.receipt.as_ref().is_some_and(|r| r.accepted));
        self.bytes += a.bytes as u64;
        let outcome = outcome_category(&a.status);
        *self.outcomes.entry(outcome.clone()).or_default() += 1;
        *self
            .latency_us
            .entry(bucket(a.finished_us.saturating_sub(a.scheduled_us)))
            .or_default() += 1;
        let latency = bucket(a.finished_us.saturating_sub(a.scheduled_us));
        *self
            .outcome_latency_us
            .entry(outcome)
            .or_default()
            .entry(latency)
            .or_default() += 1;
        if a.status == "ok" {
            *self.success_latency_us.entry(latency).or_default() += 1;
        }
        if let Some(sent) = a.sent_us {
            *self
                .service_us
                .entry(bucket(a.finished_us.saturating_sub(sent)))
                .or_default() += 1;
            *self
                .scheduling_debt_us
                .entry(bucket(sent.saturating_sub(a.scheduled_us)))
                .or_default() += 1;
        }
    }
}

async fn fetch(
    client: &Client,
    config: &Config,
    ingress: usize,
    path: &str,
    body: Option<&Value>,
) -> Result<Value> {
    let builder = if let Some(body) = body {
        client
            .post(format!("{}{path}", config.urls[ingress]))
            .json(body)
    } else {
        client.get(format!("{}{path}", config.urls[ingress]))
    };
    let response = builder.bearer_auth(&config.token).send().await?;
    let status = response.status();
    let bytes = response.bytes().await?;
    ensure!(
        status.is_success(),
        "HTTP {status} at {path} via ingress {ingress}: {}",
        String::from_utf8_lossy(&bytes)
            .chars()
            .take(256)
            .collect::<String>()
    );
    serde_json::from_slice(&bytes).context("invalid application JSON")
}

async fn append(writer: &Mutex<BufWriter<tokio::fs::File>>, value: &Attempt) -> Result<()> {
    let mut bytes = serde_json::to_vec(value)?;
    bytes.push(b'\n');
    writer.lock().await.write_all(&bytes).await?;
    Ok(())
}

async fn seed(client: &Client, config: &Config) -> Result<Value> {
    let value = fetch(client, config, 0, "/manifest", None).await?;
    let manifest: Manifest = serde_json::from_value(value.clone())?;
    ensure!(
        manifest.count == manifest.cells.len(),
        "manifest length mismatch"
    );
    let unique: std::collections::BTreeSet<_> = manifest.cells.iter().map(|c| &c.scope).collect();
    ensure!(
        unique.len() == manifest.count,
        "manifest contains duplicate scope"
    );
    tokio::fs::write(&config.manifest, serde_json::to_vec_pretty(&manifest)?).await?;
    // Migrate reporting partitions and start dispatchers before publishing
    // business outboxes. Every declared binding gets a real operation.
    for kind in ["sql", "kv", "workflow", "queue", "dlq", "cron"] {
        stream::iter(manifest.cells.iter().filter(|e| e.kind == kind).enumerate())
            .map(Ok::<_, anyhow::Error>)
            .try_for_each_concurrent(config.concurrency.min(16), |(i, e)| async move {
                let body = if kind == "kv" {
                    json!({"version": 1, "threshold": 80})
                } else {
                    json!({"action": "seed", "effect_id": format!("seed-{}-{}", e.kind, e.id)})
                };
                fetch(client, config, i % config.urls.len(), &e.path, Some(&body)).await?;
                Ok(())
            })
            .await?;
    }
    let writer = Mutex::new(BufWriter::new(
        tokio::fs::File::create(&config.ledger).await?,
    ));
    let started = Instant::now();
    stream::iter(
        manifest
            .cells
            .iter()
            .filter(|e| is_business(&e.kind))
            .enumerate(),
    )
    .map(Ok::<_, anyhow::Error>)
    .try_for_each_concurrent(config.concurrency.min(32), |(i, e)| {
        let writer = &writer;
        async move {
            let sent = micros(started.elapsed());
            let command = Command {
                command_id: "fixture-v1".into(),
                action: "seed".into(),
                payload: json!({"schema": 1}),
                expected_version: Some(0),
            };
            let value = fetch(
                client,
                config,
                i % config.urls.len(),
                &e.path,
                Some(&serde_json::to_value(&command)?),
            )
            .await?;
            let receipt: Receipt = serde_json::from_value(value["receipt"].clone())?;
            ensure!(
                receipt.accepted && receipt.version == 1 && receipt.ordinal == 1,
                "fixture receipt rejected"
            );
            ensure!(
                receipt.digest == command.digest(&e.scope)?,
                "fixture digest mismatch"
            );
            append(
                writer,
                &Attempt {
                    phase: "seed".into(),
                    index: i as u64,
                    operation: "seed".into(),
                    path: e.path.clone(),
                    ingress: i % config.urls.len(),
                    scheduled_us: sent,
                    sent_us: Some(sent),
                    finished_us: micros(started.elapsed()),
                    status: "ok".into(),
                    bytes: 0,
                    command: Some(command),
                    receipt: Some(receipt),
                    scope: Some(e.scope.clone()),
                    owner: value["owner"].as_str().map(str::to_owned),
                },
            )
            .await
        }
    })
    .await?;
    writer.lock().await.flush().await?;
    Ok(
        json!({"action": "seed", "count": manifest.count, "profile": manifest.profile, "elapsed_s": started.elapsed().as_secs_f64()}),
    )
}

fn is_business(kind: &str) -> bool {
    matches!(kind, "devices" | "books" | "inventory")
}

async fn audit(client: &Client, config: &Config, manifest: &Manifest) -> Result<Value> {
    let started = Instant::now();
    let owners = Mutex::new(BTreeMap::<String, usize>::new());
    let pending = Mutex::new(0u64);
    stream::iter(manifest.cells.iter().enumerate())
        .map(Ok::<_, anyhow::Error>)
        .try_for_each_concurrent(config.concurrency.min(32), |(i, e)| {
            let owners = &owners;
            let pending = &pending;
            async move {
                let path = if is_business(&e.kind) {
                    format!("{}/audit", e.path)
                } else {
                    e.path.clone()
                };
                let v = fetch(client, config, i % config.urls.len(), &path, None).await?;
                if is_business(&e.kind) {
                    ensure!(
                        v["scope"] == e.scope
                            && v["state"]["version"].as_u64().is_some_and(|n| n >= 1),
                        "unseeded or wrong entity"
                    );
                    let owner = v["owner"].as_str().context("missing authoritative owner")?;
                    *owners.lock().await.entry(owner.into()).or_default() += 1;
                    *pending.lock().await += v["pending_outbox"]
                        .as_u64()
                        .context("missing outbox count")?;
                }
                Ok(())
            }
        })
        .await?;
    Ok(
        json!({"action": "audit", "count": manifest.count, "business_owners": owners.into_inner(),
        "pending_outbox": pending.into_inner(), "elapsed_s": started.elapsed().as_secs_f64()}),
    )
}

async fn receipts(client: &Client, config: &Config, replay: bool) -> Result<Value> {
    use std::sync::atomic::{AtomicU64, Ordering};
    let lines = BufReader::new(tokio::fs::File::open(&config.ledger).await?).lines();
    let acknowledgements = AtomicU64::new(0);
    let ambiguous = AtomicU64::new(0);
    let acknowledgements = &acknowledgements;
    let ambiguous = &ambiguous;
    stream::try_unfold(lines, |mut lines| async move {
        Ok::<_, anyhow::Error>(lines.next_line().await?.map(|line| (line, lines)))
    })
    .try_for_each_concurrent(config.concurrency.min(32), |line| async move {
        let a: Attempt = serde_json::from_str(&line)?;
        if a.sent_us.is_none() {
            return Ok(());
        }
        let Some(command) = &a.command else {
            return Ok(());
        };
        let value = fetch(
            client,
            config,
            a.ingress % config.urls.len(),
            &format!("{}/receipts/{}", a.path, command.command_id),
            None,
        )
        .await?;
        let saved: Option<Receipt> = serde_json::from_value(value["receipt"].clone())?;
        if let Some(receipt) = &a.receipt {
            ensure!(
                saved.as_ref() == Some(receipt),
                "acknowledged receipt lost/changed: {} {}",
                a.path,
                command.command_id
            );
            if command.action == "attach" && receipt.accepted {
                let key = command.payload["key"]
                    .as_str()
                    .context("attachment key absent")?;
                let components: Vec<_> = key.split('/').collect();
                ensure!(components.len() == 4, "invalid attachment key");
                let path = format!(
                    "/attachments/{}/{}/{}/{}",
                    components[0].trim_start_matches("tenant-"),
                    components[1].trim_start_matches("book-"),
                    components[2],
                    components[3]
                );
                let response = client
                    .get(format!(
                        "{}{path}",
                        config.urls[a.ingress % config.urls.len()]
                    ))
                    .bearer_auth(&config.token)
                    .send()
                    .await?;
                ensure!(response.status() == 200, "acknowledged attachment lost");
                let bytes = response.bytes().await?;
                use sha2::{Digest, Sha256};
                ensure!(
                    command.payload["bytes"] == bytes.len()
                        && command.payload["sha256"] == format!("{:x}", Sha256::digest(&bytes)),
                    "acknowledged attachment content changed"
                );
            }
            acknowledgements.fetch_add(1, Ordering::Relaxed);
        } else {
            ambiguous.fetch_add(1, Ordering::Relaxed);
        }
        if replay {
            let response = fetch(
                client,
                config,
                a.ingress % config.urls.len(),
                &a.path,
                Some(&serde_json::to_value(command)?),
            )
            .await?;
            let receipt: Receipt = serde_json::from_value(response["receipt"].clone())?;
            if let Some(saved) = saved {
                ensure!(
                    receipt == saved && response["replay"] == true,
                    "replay changed durable receipt"
                );
            }
        }
        Ok(())
    })
    .await?;
    Ok(json!({"action":if replay {"replay"} else {"receipts"},
        "acknowledgements":acknowledgements.load(Ordering::Relaxed), "ambiguous":ambiguous.load(Ordering::Relaxed)}))
}

// SplitMix64 specifies a stable algorithm independent of rand crate releases.
fn random(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e3779b97f4a7c15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    z ^ (z >> 31)
}

type Targets<'a> = BTreeMap<&'a str, Vec<&'a Entry>>;

fn target<'a>(config: &Config, targets: &Targets<'a>, kind: &str, state: &mut u64) -> &'a Entry {
    let choices = &targets[kind];
    let n = if config.skew == "hot" && random(state).is_multiple_of(5) {
        1
    } else if config.skew == "80-20" && !random(state).is_multiple_of(5) {
        (choices.len() / 5).max(1)
    } else {
        choices.len()
    };
    choices[(random(state) as usize) % n]
}

fn operation(
    config: &Config,
    targets: &Targets<'_>,
    index: u64,
    state: &mut u64,
) -> (String, String, Option<Command>, Option<Value>) {
    if config.mix == "attachments" {
        // Admission can drop this arrival. Classify it before acquiring a
        // permit so unsent requests retain the same operation as sent ones.
        return (
            if index.is_multiple_of(2) {
                "attachment_write"
            } else {
                "attachment_read"
            }
            .into(),
            String::new(),
            None,
            None,
        );
    }
    if config.mix == "capabilities" {
        let cases = [
            "service", "assets", "loader", "web", "tcp", "outbound", "tls",
        ];
        let name = cases[(random(state) as usize) % cases.len()];
        return (
            format!("capability_{name}"),
            format!("/diagnostics/{name}"),
            None,
            None,
        );
    }
    let roll = random(state) % 100;
    let (kind, write) = match config.mix.as_str() {
        "read" => (
            if roll < 60 {
                "devices"
            } else if roll < 80 {
                "books"
            } else {
                "inventory"
            },
            false,
        ),
        "write" => (
            if roll < 70 {
                "devices"
            } else if roll < 90 {
                "books"
            } else {
                "inventory"
            },
            true,
        ),
        _ => {
            if roll < 30 {
                ("devices", false)
            } else if roll < 40 {
                ("books", false)
            } else if roll < 50 {
                ("inventory", false)
            } else if roll < 60 {
                ("sql", false)
            } else if roll < 80 {
                ("devices", true)
            } else if roll < 90 {
                ("books", true)
            } else if roll < 95 {
                ("inventory", true)
            } else {
                ("kv", true)
            }
        }
    };
    let e = target(config, targets, kind, state);
    let id = format!("{}-{index}", config.phase);
    if kind == "kv" {
        return (
            "kv_write".into(),
            e.path.clone(),
            None,
            Some(json!({"version": index, "policy": "standard"})),
        );
    }
    if !write {
        return (format!("{kind}_read"), e.path.clone(), None, None);
    }
    let (action, payload) = match kind {
        "devices" => (
            "telemetry",
            json!({"temperature": 20+(index%75), "sample": "x".repeat(config.bytes.saturating_sub(96))}),
        ),
        "books" => (
            "create",
            json!({"order_id": id, "inventory_id": 0, "saga": index.is_multiple_of(4)}),
        ),
        _ => ("replenish", json!({"qty": 1})),
    };
    let command = Command {
        command_id: id,
        action: action.into(),
        payload,
        expected_version: None,
    };
    (format!("{kind}_write"), e.path.clone(), Some(command), None)
}

async fn load(client: Client, config: Arc<Config>, manifest: Manifest) -> Result<Value> {
    let writer = Arc::new(Mutex::new(BufWriter::new(
        tokio::fs::File::create(&config.ledger).await?,
    )));
    let metrics = Arc::new(Mutex::new(BTreeMap::<String, Metrics>::new()));
    let permits = Arc::new(Semaphore::new(config.concurrency));
    let mut tasks = tokio::task::JoinSet::new();
    let started = Instant::now();
    let count = config
        .rate
        .checked_mul(config.seconds)
        .context("arrival count overflow")?;
    let mut rng = config.seed;
    let mut targets: Targets<'_> = BTreeMap::new();
    for entry in &manifest.cells {
        targets.entry(&entry.kind).or_default().push(entry);
    }
    let scopes: BTreeMap<_, _> = manifest.cells.iter().map(|e| (&e.path, &e.scope)).collect();
    let attachment_targets: Vec<_> = manifest
        .cells
        .iter()
        .filter(|e| e.kind == "books" && e.id == 0)
        .cloned()
        .collect();
    let attachment_bytes = Arc::new(vec![42; config.bytes]);
    for index in 0..count {
        let scheduled_us = index * 1_000_000 / config.rate;
        tokio::time::sleep_until(tokio::time::Instant::from_std(
            started + Duration::from_micros(scheduled_us),
        ))
        .await;
        let (operation, mut path, command, other_body) =
            operation(&config, &targets, index, &mut rng);
        let expected_scope = scopes.get(&path).map(|s| (*s).clone());
        let attachment_target =
            attachment_targets[(random(&mut rng) as usize) % attachment_targets.len()].clone();
        if config.mix == "attachments" {
            path = attachment_target.path.clone();
        }
        let attachment_bytes = attachment_bytes.clone();
        let ingress = (random(&mut rng) as usize) % config.urls.len();
        let permit = permits.clone().try_acquire_owned();
        let metrics = metrics.clone();
        let writer = writer.clone();
        let client = client.clone();
        let config = config.clone();
        tasks.spawn(async move {
            let mut a = Attempt {
                phase: config.phase.clone(),
                index,
                operation,
                path,
                ingress,
                scheduled_us,
                sent_us: None,
                finished_us: 0,
                status: "generator_saturated".into(),
                bytes: 0,
                command,
                receipt: None,
                scope: None,
                owner: None,
            };
            if let Ok(_permit) = permit {
                a.sent_us = Some(micros(started.elapsed()));
                let result = async {
                    if config.mix == "attachments" {
                        if index.is_multiple_of(2) {
                            let value = attachments::upload(
                                &client,
                                &config,
                                &writer,
                                &attachment_target,
                                &format!("{}-{index}", config.phase),
                                &attachment_bytes,
                            )
                            .await?;
                            a.bytes = serde_json::to_vec(&value)?.len();
                        } else {
                            a.bytes = attachments::read(
                                &client,
                                &config,
                                &attachment_target,
                                "fixture",
                                &attachment_bytes,
                            )
                            .await?;
                        }
                        a.status = "ok".into();
                        return Ok::<_, anyhow::Error>(());
                    }
                    let body = if let Some(c) = &a.command {
                        Some(serde_json::to_value(c)?)
                    } else {
                        other_body
                    };
                    let v = fetch(&client, &config, ingress, &a.path, body.as_ref()).await?;
                    if config.mix == "capabilities" {
                        ensure!(
                            v["passed"].as_array().is_some_and(|p| !p.is_empty()),
                            "protocol: capability returned no assertions"
                        );
                    }
                    a.bytes = serde_json::to_vec(&v)?.len();
                    a.scope = v["scope"].as_str().map(str::to_owned);
                    if is_business(a.operation.split('_').next().unwrap_or("")) {
                        ensure!(
                            a.scope == expected_scope,
                            "protocol: wrong authoritative scope"
                        );
                    }
                    a.owner = v["owner"].as_str().map(str::to_owned);
                    if let Some(command) = &a.command {
                        let receipt: Receipt = serde_json::from_value(v["receipt"].clone())?;
                        ensure!(
                            receipt.command_id == command.command_id
                                && receipt.digest
                                    == command.digest(
                                        a.scope.as_deref().context("missing receipt scope")?
                                    )?,
                            "protocol: invalid command acknowledgement"
                        );
                        a.status = if receipt.accepted {
                            "ok"
                        } else {
                            "business_rejected"
                        }
                        .into();
                        a.receipt = Some(receipt);
                    } else {
                        a.status = "ok".into();
                    }
                    Ok::<_, anyhow::Error>(())
                }
                .await;
                if let Err(error) = result {
                    a.status = format!(
                        "error:{}",
                        error.to_string().chars().take(200).collect::<String>()
                    );
                }
            }
            a.finished_us = micros(started.elapsed());
            metrics
                .lock()
                .await
                .entry(a.operation.clone())
                .or_default()
                .record(&a);
            append(&writer, &a).await
        });
        while let Some(result) = tasks.try_join_next() {
            result??;
        }
    }
    while let Some(result) = tasks.join_next().await {
        result??;
    }
    writer.lock().await.flush().await?;
    Ok(
        json!({"action": "load", "phase": config.phase, "offered_rps": config.rate,
        "arrival_seconds": config.seconds, "elapsed_s": started.elapsed().as_secs_f64(),
        "arrival_model": "fixed_rate", "random_algorithm": "splitmix64-v1",
        "operations": &*metrics.lock().await}),
    )
}

#[tokio::main(worker_threads = 2)]
async fn main() -> Result<()> {
    let path = std::env::args()
        .nth(1)
        .context("usage: fleetcare-load CONFIG.json")?;
    let config: Config = serde_json::from_slice(&tokio::fs::read(path).await?)?;
    ensure!(
        matches!(config.urls.len(), 1 | 3)
            && config.concurrency > 0
            && config.rate > 0
            && config.seconds > 0
            && config.bytes <= 60_000
            && config.phase.len() <= 80
            && matches!(
                config.mix.as_str(),
                "read" | "write" | "core" | "attachments" | "capabilities"
            )
            && matches!(config.skew.as_str(), "" | "uniform" | "80-20" | "hot"),
        "invalid workload configuration"
    );
    ensure!(
        config
            .phase
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b)),
        "invalid phase ID"
    );
    let client = Client::builder()
        .timeout(Duration::from_secs(60))
        .tcp_nodelay(true)
        .pool_max_idle_per_host(config.concurrency)
        .build()?;
    let manifest = if config.action != "seed" {
        Some(serde_json::from_slice::<Manifest>(
            &tokio::fs::read(&config.manifest).await?,
        )?)
    } else {
        None
    };
    let report_path = config.report.clone();
    let report = match config.action.as_str() {
        "seed" => seed(&client, &config).await?,
        "audit" => audit(&client, &config, &manifest.context("manifest absent")?).await?,
        "receipts" => receipts(&client, &config, false).await?,
        "replay" => receipts(&client, &config, true).await?,
        "probes" => checks::probes(&client, &config).await?,
        "attachment_seed" => {
            attachments::prepare(&client, &config, &manifest.context("manifest absent")?).await?
        }
        "projections" => {
            checks::projections(&client, &config, &manifest.context("manifest absent")?).await?
        }
        "load" => {
            load(
                client,
                Arc::new(config),
                manifest.context("manifest absent")?,
            )
            .await?
        }
        _ => anyhow::bail!("unknown client action"),
    };
    tokio::fs::write(report_path, serde_json::to_vec_pretty(&report)?).await?;
    cellar::cli_output::Output::new(cellar::cli_output::Format::Text).line(format_args!("{report}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_never_rounds_down_and_has_bounded_relative_error() {
        for value in [0, 1, 63, 64, 65, 100, 1000, 9999, 1_000_000, 60_000_000] {
            let upper = bucket(value);
            assert!(upper >= value);
            assert!(upper - value <= (value / 64).max(1));
        }
    }

    #[test]
    fn deterministic_random_protocol() {
        let mut state = 0;
        assert_eq!(random(&mut state), 0xe220a8397b1dcdaf);
        assert_eq!(random(&mut state), 0x6e789e6aa1b965f4);
    }

    #[test]
    fn attachment_arrivals_are_classified_before_in_flight_admission() {
        let config: Config = serde_json::from_value(json!({
            "action":"load", "urls":["http://unused"], "token":"fixture",
            "manifest":"", "ledger":"", "report":"", "mix":"attachments"
        }))
        .unwrap();
        let entries: Vec<_> = ["devices", "books", "inventory", "sql", "kv"]
            .into_iter()
            .map(|kind| Entry {
                kind: kind.into(),
                tenant: 0,
                id: 0,
                scope: kind.into(),
                path: format!("/{kind}"),
            })
            .collect();
        let targets: Targets<'_> = entries.iter().map(|e| (e.kind.as_str(), vec![e])).collect();
        for (index, expected) in [(0, "attachment_write"), (1, "attachment_read")] {
            let (name, _, command, body) = operation(&config, &targets, index, &mut 0);
            assert_eq!(name, expected);
            assert!(command.is_none() && body.is_none());
        }
    }
}
