// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Cross-deployment ownership, callback lifetimes and bounded public load.
use super::*;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::AtomicUsize;

const FAST: &str = "quorum-fast";

/// Timed healthy traffic, separate from the unchanged fault/load qualification.
pub async fn qualify_perf() -> Result<()> {
    use crate::primitives_perf::{emit, request, window, Settings};
    let settings = Settings::load()?;
    let backend = rustfs::StoreConfig::load("perf-queue", settings.rooted)?;
    let store = Arc::new(catalog_fault::Store::around(backend.open(None)?));
    let s = Scenario::start(
        settings.rooted,
        store,
        false,
        5000,
        None,
        8,
        Durability::Quorum,
    )
    .await?;
    let domain = s.cluster.fixtures[0].domain.clone();
    let outcome = domain.run(async {
        let client = reqwest::Client::builder().no_proxy().timeout(TIMEOUT + Duration::from_secs(1)).build()?;
        let base = format!("http://{}",s.cluster.nodes[0].info().public_address);
        let payload = "x".repeat(256);
        let mut failed = false;
        for (repeat,concurrency) in settings.matrix() {
            for (phase,seconds) in [("warmup",2),("enqueue",settings.seconds)] {
                let prefix = format!("perf-{repeat}-{concurrency}-{phase}-");
                let started = std::time::Instant::now();
                let mut result = window(seconds,concurrency,|worker,sequence| {
                    let client = &client; let base = &base; let prefix = &prefix; let payload = &payload;
                    async move {
                        let values: Vec<_> = (0..8).map(|item|format!("{prefix}{worker}-{sequence}-{item}-{payload}")).collect();
                        let endpoint = if worker % 2 == 0 { "queue" } else { "fast" };
                        ensure!(request(client,&format!("{base}/{endpoint}"),json!(values)).await? == json!(true));
                        Ok(())
                    }
                }).await;
                let expected: BTreeSet<_> = result.successes.iter().enumerate().flat_map(|(worker,count)| {
                    let prefix = &prefix; let payload = &payload;
                    (1..=*count).flat_map(move |sequence|(0..8).map(move |item|format!("{prefix}{worker}-{sequence}-{item}-{payload}")))
                }).collect();
                let verification = async {
                    s.empty(QUEUE).await?;
                    s.empty(FAST).await?;
                    let completed = s.signals.completed.lock().unwrap();
                    let actual: BTreeSet<_> = completed.iter().filter(|body|body.starts_with(&prefix)).cloned().collect();
                    ensure!(actual == expected,"Queue acknowledgements and consumer completions differ: expected={} actual={}",expected.len(),actual.len());
                    let delivered = s.signals.deliveries.lock().unwrap();
                    let ids: BTreeSet<_> = delivered.iter().filter(|(body,_,_)|body.starts_with(&prefix)).map(|(_,_,id)|id).collect();
                    ensure!(ids.len() == expected.len(),"Queue message identity count differs");
                    ensure!(result.errors == 0,"enqueue request errors");
                    Ok(())
                }.await;
                let drained = started.elapsed().as_secs_f64();
                result.report["batchMessages"] = json!(8);
                result.report["acknowledgedMessages"] = json!(expected.len());
                result.report["enqueueMessagesPerSecond"] = json!(expected.len() as f64 / result.report["elapsedSeconds"].as_f64().context("measured elapsed seconds")?);
                result.report["enqueueAndDrainSeconds"] = json!(drained);
                result.report["verifiedMessagesPerSecondIncludingDrain"] = if verification.is_ok() {json!(expected.len() as f64 / drained)}else{json!(null)};
                let resources = json!(s.cluster.hosts.iter().map(|host|json!({"usage":host.resource_usage(),"performance":host.performance()})).collect::<Vec<_>>());
                emit("queue",&settings,repeat,phase,&result,&verification,resources);
                if verification.is_err() { failed = true; break; }
            }
        }
        ensure!(!failed,"one or more Queue measured windows failed");
        Ok::<_,anyhow::Error>(())
    }).await;
    let retain = !matches!(&outcome, Ok(Ok(())));
    s.close(retain).await?;
    outcome??;
    Ok(())
}

#[derive(Default)]
struct Signals {
    deliveries: Mutex<Vec<(String, u16, String)>>,
    completed: Mutex<BTreeSet<String>>,
    held: Mutex<BTreeMap<String, queue::Event>>,
    changed: Notify,
    release: Notify,
}

impl Signals {
    async fn wait(
        &self,
        body: &str,
        attempt: u16,
        completed: bool,
        budget: Duration,
    ) -> Result<()> {
        cellar::asyncrt::timeout(budget, async {
            loop {
                let wake = self.changed.notified();
                let entered = self
                    .deliveries
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|(b, n, _)| b == body && *n == attempt);
                if entered && (!completed || self.completed.lock().unwrap().contains(body)) {
                    break;
                }
                wake.await;
            }
        })
        .await
        .with_context(|| format!("Queue {body} attempt {attempt} completed={completed}"))?;
        Ok(())
    }
}

fn counter() -> Result<RaftClass> {
    Ok(RaftClass::new("Counter", 1, [41; 32])?
        .initializer([42; 32], |_, _| Ok(()))?
        .json_cell_rpc("Send", [43; 32])?
        .json_cell_rpc("Metrics", [44; 32])?)
}

fn binding(environment: &str, queue: &str) -> cellar::engine_api::QueueBinding {
    cellar::engine_api::QueueBinding {
        environment: environment.into(),
        queue: queue.into(),
        delivery_delay: 0,
    }
}

struct Deployments {
    application: Arc<dyn Application>,
    options: cellar::engine_api::WorkerConfigOptions,
    definitions: Vec<(Arc<RaftClass>, Durability)>,
    cohosted: Vec<(
        cellar::engine_api::WorkerConfigOptions,
        Arc<dyn Application>,
    )>,
}

fn applications(signals: Arc<Signals>, dead_minimum: Durability) -> Result<Deployments> {
    let producer = Arc::new(
        Registry::new(|context, invocation| {
            Box::pin(async move {
                let Invocation::Fetch(request) = invocation else {
                    anyhow::bail!("producer received a consumer callback")
                };
                ensure!(context.var("role") == Some("producer"));
                let queue = if request.url.contains("/fast") {
                    "FAST"
                } else {
                    "Q"
                };
                let bytes = context
                    .take_body(request.body)?
                    .try_concat()
                    .await
                    .map_err(anyhow::Error::msg)?;
                let values: Vec<String> = serde_json::from_slice(&bytes)?;
                let entries: Vec<_> = values
                    .into_iter()
                    .map(|text| queue::BatchEntry {
                        body: Body::Text(text),
                        delay_seconds: None,
                    })
                    .collect();
                let result = context
                    .queue(queue)?
                    .send_batch(&context, entries, SendOptions::default())
                    .await;
                Ok(Output::Http(cellar::engine_api::HttpResponse {
                    status: if result.is_ok() { 200 } else { 503 },
                    headers: vec![],
                    body: result
                        .map(|_| b"true".to_vec())
                        .unwrap_or_else(|error| format!("{error:#}").into_bytes()),
                    stream: None,
                    websocket: None,
                    write_position: None,
                    observed_position: None,
                }))
            })
        })
        .raft_cell(counter()?, |_, _| Ok(Arc::new(Caller)))?,
    );
    let mut producer_options = configuration::options(SCRIPT);
    producer_options.do_classes = vec!["Counter".into()];
    producer_options.queue_bindings = vec![
        binding("Q", QUEUE),
        binding("FAST", FAST),
        binding("DLQ", DEAD),
    ];
    producer_options.vars = vec![("role".into(), "producer".into())];
    producer.validate_configuration(&producer_options)?;

    let consumer = Arc::new(
        Registry::new(move |context, invocation| {
            let signals = signals.clone();
            Box::pin(async move {
                ensure!(context.var("role") == Some("consumer"));
                ensure!(context.identity().is_none() && context.storage().is_err());
                let Invocation::Queue(batch) = invocation else {
                    anyhow::bail!("consumer callback required")
                };
                queue::dispatch(batch, move |event| async move {
                    for message in event.messages() {
                        let Body::Text(text) = &message.body else {
                            anyhow::bail!("text body")
                        };
                        signals.deliveries.lock().unwrap().push((
                            text.clone(),
                            message.attempts,
                            message.id.clone(),
                        ));
                        signals.changed.notify_waiters();
                        if text == "late" && message.attempts == 1 {
                            signals
                                .held
                                .lock()
                                .unwrap()
                                .insert(text.clone(), event.clone());
                            return std::future::pending::<Result<()>>().await;
                        }
                        if text == "purge" || (text == "late" && message.attempts == 2) {
                            signals
                                .held
                                .lock()
                                .unwrap()
                                .insert(text.clone(), event.clone());
                            signals.release.notified().await;
                        }
                        if text == "long" {
                            cellar::asyncrt::sleep(Duration::from_secs(65)).await;
                        }
                        if text == "deadfault"
                            || text == "purge"
                            || (text == "retry" && message.attempts == 1)
                        {
                            message.retry(Some(1))?;
                        } else {
                            message.ack()?;
                            signals.completed.lock().unwrap().insert(text.clone());
                        }
                        signals.changed.notify_waiters();
                    }
                    Ok(())
                })
                .await
            })
        })
        .raft_cell(counter()?, |_, _| Ok(Arc::new(Caller)))?,
    );
    let mut consumer_options = configuration::options("queue-consumer-deployment");
    consumer_options.do_classes = Vec::new();
    consumer_options.vars = vec![("role".into(), "consumer".into())];
    consumer_options.queue_consumers = [QUEUE, FAST]
        .into_iter()
        .map(|name| cellar::protocol::QueueConsumerConfig {
            queue: name.into(),
            max_batch_size: 8,
            max_batch_timeout: 0,
            max_retries: 1,
            dead_letter_queue: Some(DEAD.into()),
            max_concurrency: Some(2),
            retry_delay: Some(1),
        })
        .collect();
    consumer.validate_configuration(&consumer_options)?;

    let storage = Arc::new(
        Registry::new(|_, _| {
            Box::pin(async { anyhow::bail!("storage-only Queue cannot invoke a consumer") })
        })
        .raft_cell(counter()?, |_, _| Ok(Arc::new(Caller)))?,
    );
    let mut storage_options = configuration::options("queue-dead-letter-storage");
    storage_options.do_classes = Vec::new();
    storage_options.queue_bindings = vec![binding("DLQ", DEAD)];
    storage.validate_configuration(&storage_options)?;
    let definitions = vec![
        (producer.resolve_raft_class("Counter")?, Durability::Quorum),
        (
            consumer.resolve_raft_class(cellar::deploy::QUEUE_CLASS)?,
            Durability::Quorum,
        ),
        (
            storage.resolve_raft_class(cellar::deploy::QUEUE_CLASS)?,
            dead_minimum,
        ),
    ];
    let application = Arc::new(RaftApplications::new(
        producer.clone(),
        [
            consumer.clone() as Arc<dyn Application>,
            storage.clone() as Arc<dyn Application>,
        ],
    )?);
    Ok(Deployments {
        application,
        options: producer_options,
        definitions,
        cohosted: vec![(consumer_options, consumer), (storage_options, storage)],
    })
}

struct Scenario {
    cluster: ManagedCluster,
    signals: Arc<Signals>,
    http: Option<Arc<managed_fixture::http::Owner>>,
    queue: GroupHistory,
    dead: GroupHistory,
}

impl Scenario {
    async fn start(
        rooted: bool,
        store: Arc<catalog_fault::Store>,
        external: bool,
        callback_ms: u64,
        retention_ms: Option<i64>,
        ordinary_slots: usize,
        dead_minimum: Durability,
    ) -> Result<Self> {
        let signals = Arc::new(Signals::default());
        let Deployments {
            application,
            options,
            definitions,
            cohosted,
        } = applications(signals.clone(), dead_minimum)?;
        let mut fixture =
            Fixture::with_definition_policies(rooted, definitions, application, store).await?;
        fixture.configuration = Some(options);
        fixture.cohosted = cohosted;
        fixture.effects = Some(RaftEffectOptions {
            callback_timeout_ms: callback_ms,
            ..Default::default()
        });
        fixture.sql_workers = 1;
        if ordinary_slots != 2 {
            fixture.ordinary_admission = Some((ordinary_slots, 128 * 1024));
        }
        fixture.image_bytes = 128 * 1024 * 1024;
        fixture.max_input_bytes = 16 * 1024;
        fixture.cell_limits.queue.retention_ms = retention_ms;
        let http = external.then(|| Arc::new(managed_fixture::http::Owner::default()));
        fixture.external_http = http.clone();
        let started = Box::pin(ManagedCluster::start_with_applications(
            fixture,
            &[],
            ArchiveSetup::Initialized,
            false,
            None,
            FirstGroupSetup::Normal,
        ))
        .await;
        let cluster = match started {
            Ok(c) => c,
            Err(error) => {
                if let Some(http) = http {
                    http.close().await?;
                }
                return Err(error);
            }
        };
        let domain = cluster.fixtures[0].domain.clone();
        let initialized = domain
            .run(async {
                let queue = install_group(&cluster, 0, 181).await?;
                let dead = install_group(&cluster, 1, 185).await?;
                ready(&cluster, &[cluster.history, queue, dead]).await?;
                admin(&cluster, QUEUE, json!({"op":"info"}))
                    .await
                    .context("warm consumer Queue")?;
                admin(&cluster, FAST, json!({"op":"info"}))
                    .await
                    .context("warm fast Queue")?;
                archive_ready(&cluster, &[cluster.history, queue, dead]).await?;
                let warmed = admin(&cluster, DEAD, json!({"op":"info"})).await;
                if warmed.is_err() {
                    snapshot(&cluster, &[queue, dead]).await;
                }
                warmed.context("warm DLQ at its declared minimum")?;
                Ok::<_, anyhow::Error>((queue, dead))
            })
            .await;
        match initialized {
            Ok(Ok((queue, dead))) => Ok(Self {
                cluster,
                signals,
                http,
                queue,
                dead,
            }),
            result => {
                for root in cluster.roots {
                    eprintln!(
                        "retained failed Queue setup disk: {}",
                        root.keep().display()
                    );
                }
                let mut errors = Vec::new();
                for node in cluster.nodes {
                    if let Err(error) = node.shutdown(cellar::sdk::ShutdownMode::Preserve).await {
                        errors.push(error.to_string());
                    }
                }
                for host in cluster.hosts {
                    if let Err(error) = host.close().await {
                        errors.push(error.to_string());
                    }
                }
                if let Some(http) = http {
                    if let Err(error) = http.close().await {
                        errors.push(error.to_string());
                    }
                }
                if !errors.is_empty() {
                    eprintln!("Queue failed-setup cleanup: {errors:?}");
                }
                result??;
                unreachable!()
            }
        }
    }

    async fn close(self, retain: bool) -> Result<()> {
        let mut errors = Vec::new();
        for node in self.cluster.nodes {
            if let Err(e) = node.shutdown(cellar::sdk::ShutdownMode::Preserve).await {
                errors.push(e.to_string());
            }
        }
        for host in self.cluster.hosts {
            if let Err(e) = host.close().await {
                errors.push(e.to_string());
            }
        }
        if let Some(http) = self.http {
            if let Err(e) = http.close().await {
                errors.push(e.to_string());
            }
        }
        if retain || !errors.is_empty() {
            for root in self.cluster.roots {
                eprintln!(
                    "retained failed Queue qualification disk: {}",
                    root.keep().display()
                );
            }
        }
        ensure!(
            errors.is_empty(),
            "Queue qualification joined cleanup: {errors:?}"
        );
        Ok(())
    }

    async fn empty(&self, queue: &str) -> Result<()> {
        cellar::asyncrt::timeout(Duration::from_secs(30), async {
            loop {
                let value = admin(&self.cluster, queue, json!({"op":"info"}))
                    .await
                    .with_context(|| format!("empty {queue}"))?;
                let rows = admin(&self.cluster, queue, json!({"op":"peek","limit":100})).await?;
                if value["backlogCount"] == 0.0
                    && rows["messages"].as_array().is_some_and(Vec::is_empty)
                {
                    return Ok::<_, anyhow::Error>(());
                }
                cellar::asyncrt::sleep(Duration::from_millis(30)).await;
            }
        })
        .await??;
        Ok(())
    }
}

async fn send(cluster: &ManagedCluster, queue: &str, values: &[String]) -> Result<u128> {
    let start = cellar::asyncrt::mono_ms();
    let response = reqwest::Client::builder()
        .no_proxy()
        .timeout(TIMEOUT + Duration::from_secs(1))
        .build()?
        .post(format!(
            "http://{}/{}",
            cluster.nodes[0].info().public_address,
            if queue == FAST { "fast" } else { "queue" }
        ))
        .json(values)
        .send()
        .await?;
    let status = response.status();
    let body = response.text().await?;
    ensure!(status == 200, "Queue public producer {status}: {body}");
    Ok(u128::from(cellar::asyncrt::mono_ms() - start))
}

async fn ready(cluster: &ManagedCluster, groups: &[GroupHistory]) -> Result<()> {
    cellar::asyncrt::timeout(TIMEOUT, async {
        loop {
            let mut caught_up = true;
            for group in groups {
                let leader = managed_fixture::leader(&cluster.hosts, *group).await?;
                let term = cluster.hosts[leader].group(*group)?.status().await?.term;
                for host in &cluster.hosts {
                    let status = host.group(*group)?.status().await?;
                    caught_up &= status.leader == Some(leader as u64 + 1)
                        && status.applied.term == term
                        && status.applied.index == status.committed;
                }
            }
            if caught_up {
                return Ok::<_, anyhow::Error>(());
            }
            cellar::asyncrt::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("fresh quorum readiness")??;
    Ok(())
}

// Observe the pre-workload archive cut; this is not a command/response proof.
// S3-minimum enrollment and every operator call still use their original budget.
async fn archive_ready(cluster: &ManagedCluster, groups: &[GroupHistory]) -> Result<()> {
    let mut cuts = Vec::new();
    for group in groups {
        let leader = managed_fixture::leader(&cluster.hosts, *group).await?;
        let handle = cluster.hosts[leader].group(*group)?;
        let cut = handle.status().await?.applied.index;
        cuts.push((handle, cut));
    }
    let result = cellar::asyncrt::timeout(TIMEOUT, async {
        loop {
            let mut ready = true;
            for (handle, cut) in &cuts {
                ready &= handle
                    .archive_status()?
                    .and_then(|s| s.published)
                    .is_some_and(|m| m.position().index >= *cut);
            }
            if ready {
                return Ok::<_, anyhow::Error>(());
            }
            cellar::asyncrt::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    if !matches!(result, Ok(Ok(()))) {
        snapshot(cluster, groups).await;
    }
    result.context("initial Queue group archive readiness")??;
    Ok(())
}

async fn snapshot(cluster: &ManagedCluster, groups: &[GroupHistory]) {
    for (node, host) in cluster.hosts.iter().enumerate() {
        eprintln!(
            "QUEUE_FAILURE {}",
            json!({"node":node,"resources":host.resource_usage(),"performance":host.performance()})
        );
        for group in groups {
            if let Ok(handle) = host.group(*group) {
                let status = cellar::asyncrt::timeout(TIMEOUT, handle.status()).await;
                eprintln!("QUEUE_FAILURE node={node} history={group:?} status={status:?}");
                if let Ok(Some(archive)) = handle.archive_status() {
                    eprintln!("QUEUE_FAILURE node={node} archive attempts={} published={:?} stopped={} error={:?}",archive.attempts,archive.published.as_ref().map(|m|m.position()),archive.stopped,archive.error);
                }
            }
        }
    }
}

async fn move_leader(cluster: &ManagedCluster, group: GroupHistory, target: usize) -> Result<()> {
    cellar::asyncrt::timeout(TIMEOUT, async {
        loop {
            let current = managed_fixture::leader(&cluster.hosts, group).await?;
            if current == target {
                return Ok::<_, anyhow::Error>(());
            }
            let handle = cluster.hosts[current].group(group)?;
            let status = handle.replication_status(TIMEOUT).await?;
            match handle
                .transfer_leadership(
                    LeadershipTransfer {
                        target: MemberId::new(target as u64 + 1)?,
                        expected_term: status.term,
                        expected_revision: status.membership_revision,
                    },
                    TIMEOUT,
                )
                .await
            {
                Ok(()) | Err(RuntimeError::NotReady) => (),
                Err(error) => return Err(error.into()),
            }
            cellar::asyncrt::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    Ok(())
}

pub async fn qualify_faults(real: bool, external: bool) -> Result<()> {
    for rooted in [false, true] {
        let backend = real
            .then(|| {
                rustfs::StoreConfig::load(
                    if external {
                        "queue-faults-axum"
                    } else {
                        "queue-faults"
                    },
                    rooted,
                )
            })
            .transpose()?;
        let store = match &backend {
            Some(b) => Arc::new(catalog_fault::Store::around(b.open(None)?)),
            None => Arc::new(catalog_fault::Store::default()),
        };
        let mut s = Scenario::start(rooted, store, external, 5000, None, 2, Durability::S3).await?;
        let domain = s.cluster.fixtures[0].domain.clone();
        let outcome = domain
            .run(async {
                eprintln!("QUEUE_FAULT partial");
                send(&s.cluster, QUEUE, &["good".into(), "retry".into()]).await?;
                s.signals.wait("good", 1, true, TIMEOUT).await?;
                s.signals
                    .wait("retry", 2, true, Duration::from_secs(20))
                    .await?;
                s.empty(QUEUE).await?;

                eprintln!("QUEUE_FAULT late generation");
                send(&s.cluster, QUEUE, &["late".into()]).await?;
                s.signals.wait("late", 1, false, TIMEOUT).await?;
                let obsolete = s
                    .signals
                    .held
                    .lock()
                    .unwrap()
                    .get("late")
                    .context("held generation")?
                    .clone();
                let leader = managed_fixture::leader(&s.cluster.hosts, s.queue).await?;
                move_leader(&s.cluster, s.queue, (leader + 1) % 3).await?;
                s.signals
                    .wait("late", 2, false, Duration::from_secs(25))
                    .await?;
                ensure!(
                    obsolete.ack_all().is_err(),
                    "obsolete consumer could settle a later generation"
                );
                s.signals.release.notify_waiters();
                s.signals.wait("late", 2, true, TIMEOUT).await?;
                s.empty(QUEUE).await?;

                eprintln!("QUEUE_FAULT active purge");
                send(&s.cluster, QUEUE, &["purge".into()]).await?;
                s.signals.wait("purge", 1, false, TIMEOUT).await?;
                let purged = admin(&s.cluster, QUEUE, json!({"op":"purge"})).await?;
                ensure!(purged["leased"] == 1, "active purge: {purged}");
                s.signals.release.notify_waiters();
                s.empty(QUEUE).await?;
                ensure!(s.signals.held.lock().unwrap()["purge"].ack_all().is_err());

                // A different Raft group accepts the DLQ mutation with an S3 minimum.
                // Refuse archive PUTs, observe target quorum apply, and retire the
                // original source host before its target completion can be returned.
                eprintln!("QUEUE_FAULT DLQ reply loss");
                let source = managed_fixture::leader(&s.cluster.hosts, s.queue).await?;
                move_leader(&s.cluster, s.dead, (source + 1) % 3).await?;
                ready(&s.cluster, &[s.queue, s.dead]).await?;
                archive_ready(&s.cluster, &[s.dead]).await?;
                let destination = managed_fixture::leader(&s.cluster.hosts, s.dead).await?;
                ensure!(
                    destination != source
                        && managed_fixture::leader(&s.cluster.hosts, s.queue).await? == source,
                    "DLQ fault requires stable, different source and destination leaders"
                );
                let before = s.cluster.hosts[destination]
                    .group(s.dead)?
                    .status()
                    .await?
                    .applied
                    .index;
                let store = s.cluster.fixtures[0].store.clone();
                let denied = store.denied_group_archive_writes.load(Ordering::Acquire);
                store
                    .deny_group_archive_writes
                    .store(true, Ordering::Release);
                send(&s.cluster, QUEUE, &["deadfault".into()]).await?;
                s.signals
                    .wait("deadfault", 2, false, Duration::from_secs(20))
                    .await?;
                cellar::asyncrt::timeout(TIMEOUT, async {
                    loop {
                        let status = s.cluster.hosts[destination].group(s.dead)?.status().await?;
                        if status.applied.index > before
                            && store.denied_group_archive_writes.load(Ordering::Acquire) > denied
                        {
                            break Ok::<_, anyhow::Error>(());
                        }
                        cellar::asyncrt::sleep(Duration::from_millis(20)).await;
                    }
                })
                .await??;
                let id = s
                    .signals
                    .deliveries
                    .lock()
                    .unwrap()
                    .iter()
                    .find(|(b, _, _)| b == "deadfault")
                    .context("original message")?
                    .2
                    .clone();
                let closed = s.cluster.hosts[source].close().await;
                store
                    .deny_group_archive_writes
                    .store(false, Ordering::Release);
                closed?;
                restart(&mut s.cluster).await?;
                ready(&s.cluster, &[s.cluster.history, s.queue, s.dead]).await?;
                archive_ready(&s.cluster, &[s.cluster.history, s.queue, s.dead]).await?;
                s.empty(QUEUE).await?;
                let dead = admin(&s.cluster, DEAD, json!({"op":"peek","limit":100})).await?;
                let messages = dead["messages"].as_array().context("DLQ rows")?;
                ensure!(
                    messages.len() == 1 && messages[0]["id"] == id,
                    "DLQ lost-reply duplicate/loss: {dead}"
                );
                ensure!(messages[0]["deadLetterSource"] == QUEUE);
                Ok::<_, anyhow::Error>(())
            })
            .await;
        let retain = !matches!(&outcome, Ok(Ok(())));
        let cleanup = s.close(retain).await;
        outcome??;
        cleanup?;
        // Retention has its own explicit host policy; source command receipts
        // outlive expired Queue bodies and cannot recreate them on replay.
        let store = match &backend {
            Some(_) => Arc::new(catalog_fault::Store::around(
                rustfs::StoreConfig::load(
                    if external {
                        "queue-retention-axum"
                    } else {
                        "queue-retention"
                    },
                    rooted,
                )?
                .open(None)?,
            )),
            None => Arc::new(catalog_fault::Store::default()),
        };
        let s = Scenario::start(
            rooted,
            store,
            external,
            5000,
            Some(500),
            2,
            Durability::Quorum,
        )
        .await?;
        let domain = s.cluster.fixtures[0].domain.clone();
        let outcome = domain
            .run(async {
                admin(&s.cluster, QUEUE, json!({"op":"pause"})).await?;
                let group = s.cluster.history;
                let leader = managed_fixture::leader(&s.cluster.hosts, group).await?;
                let source = s.cluster.hosts[leader].cell(CellHistory {
                    cluster: group.cluster,
                    namespace: group.namespace,
                    cell: identity("retention")?,
                    incarnation: IncarnationId::new([111; 16])?,
                })?;
                source
                    .enroll(
                        CellEnrollmentRequest {
                            creation: ProposalId::new([112; 16])?,
                            durability: Durability::Quorum,
                        },
                        TIMEOUT,
                    )
                    .await?;
                let body = serde_json::to_vec(&json!({"body":"expired","delay":2}))?;
                let original = options(1)?;
                let contract = source.contract("Send")?;
                let accepted = source
                    .command(&contract, body.clone(), original.clone())
                    .await?;
                cellar::asyncrt::sleep(Duration::from_millis(800)).await;
                let replay = source.command(&contract, body, original).await?;
                ensure!(accepted.result == replay.result);
                admin(&s.cluster, QUEUE, json!({"op":"resume"})).await?;
                s.empty(QUEUE).await?;
                cellar::asyncrt::sleep(Duration::from_secs(2)).await;
                ensure!(
                    s.signals.deliveries.lock().unwrap().is_empty(),
                    "expired enqueue replay recreated a message"
                );
                Ok::<_, anyhow::Error>(())
            })
            .await;
        let retain = !matches!(&outcome, Ok(Ok(())));
        let cleanup = s.close(retain).await;
        outcome??;
        cleanup?;
    }
    Ok(())
}

pub async fn qualify_long(real: bool) -> Result<()> {
    for rooted in [false, true] {
        let backend = real
            .then(|| rustfs::StoreConfig::load("queue-long", rooted))
            .transpose()?;
        let store = match &backend {
            Some(b) => Arc::new(catalog_fault::Store::around(b.open(None)?)),
            None => Arc::new(catalog_fault::Store::default()),
        };
        let s = Scenario::start(rooted, store, false, 90_000, None, 2, Durability::Quorum).await?;
        let domain = s.cluster.fixtures[0].domain.clone();
        let outcome = domain
            .run(async {
                let start = cellar::asyncrt::mono_ms();
                send(&s.cluster, QUEUE, &["long".into()]).await?;
                s.signals.wait("long", 1, false, TIMEOUT).await?;
                send(&s.cluster, FAST, &["fast".into()]).await?;
                s.signals.wait("fast", 1, true, TIMEOUT).await?;
                ensure!(
                    !s.signals.completed.lock().unwrap().contains("long"),
                    "long callback did not remain in flight"
                );
                s.signals
                    .wait("long", 1, true, Duration::from_secs(80))
                    .await?;
                ensure!(cellar::asyncrt::mono_ms() - start >= 65_000);
                s.empty(QUEUE).await?;
                s.empty(FAST).await?;
                Ok::<_, anyhow::Error>(())
            })
            .await;
        let retain = !matches!(&outcome, Ok(Ok(())));
        let cleanup = s.close(retain).await;
        outcome??;
        cleanup?;
    }
    Ok(())
}

pub async fn qualify_load(real: bool, per_queue_in_flight: usize) -> Result<()> {
    ensure!((1..=2).contains(&per_queue_in_flight));
    for rooted in [false, true] {
        let backend = real
            .then(|| rustfs::StoreConfig::load("queue-load", rooted))
            .transpose()?;
        let store = match &backend {
            Some(b) => Arc::new(catalog_fault::Store::around(b.open(None)?)),
            None => Arc::new(catalog_fault::Store::default()),
        };
        let s = Scenario::start(rooted, store, false, 5000, None, 8, Durability::Quorum).await?;
        let domain = s.cluster.fixtures[0].domain.clone();
        let outcome=domain.run(async {
            let store=s.cluster.fixtures[0].store.clone();
            let denied=store.denied_group_archive_writes.load(Ordering::Acquire);
            store.deny_group_archive_writes.store(true,Ordering::Release);
            let outage=async {
                for (n,queue) in [QUEUE,FAST].into_iter().enumerate() {
                    let bodies=(0..8).map(|item|format!("outage-{n}-{item}")).collect::<Vec<_>>();
                    send(&s.cluster,queue,&bodies).await?;
                }
                cellar::asyncrt::timeout(TIMEOUT,async {
                    while store.denied_group_archive_writes.load(Ordering::Acquire)==denied {
                        cellar::asyncrt::sleep(Duration::from_millis(10)).await;
                    }
                }).await.context("archive outage did not reach the real store adapter")?;
                Ok::<_,anyhow::Error>(())
            }.await;
            store.deny_group_archive_writes.store(false,Ordering::Release);
            outage?;
            s.empty(QUEUE).await?;s.empty(FAST).await?;
            let start=cellar::asyncrt::mono_ms();
            let credits=[tokio::sync::Semaphore::new(per_queue_in_flight),tokio::sync::Semaphore::new(per_queue_in_flight)];
            let active=AtomicUsize::new(0);
            let peak=AtomicUsize::new(0);
            let producers=(0..4).map(|client| { let cluster=&s.cluster; let credits=&credits; let active=&active; let peak=&peak; async move {
                let mut latency=Vec::new();
                for batch in 0..16 {
                    let bodies=(0..8).map(|item|format!("load-{client}-{batch}-{item}")).collect::<Vec<_>>();
                    let _credit=credits[client%2].acquire().await?;
                    let entered=active.fetch_add(1,Ordering::AcqRel)+1;
                    peak.fetch_max(entered,Ordering::AcqRel);
                    let sent=send(cluster,if client%2==0 {QUEUE}else{FAST},&bodies).await;
                    active.fetch_sub(1,Ordering::AcqRel);
                    latency.push(sent?);
                }Ok::<_,anyhow::Error>(latency)
            }});
            let results=futures_util::future::join_all(producers).await;
            if results.iter().any(Result::is_err) {
                eprintln!("QUEUE_LOAD_FAILURE {}",json!({"realRustFS":real,"extents":rooted,"inFlightRequests":2*per_queue_in_flight,"perQueueInFlight":per_queue_in_flight,"peakInFlightRequests":peak.load(Ordering::Acquire),"activeCalls":active.load(Ordering::Acquire),"failedProducers":results.iter().filter(|r|r.is_err()).count(),"ordinarySlots":8,"ordinaryBytes":131072,"callbackSlots":2}));
                snapshot(&s.cluster,&[s.queue,s.dead]).await;
            }
            let mut latencies=Vec::new();for result in results {latencies.extend(result?);}
            ensure!(active.load(Ordering::Acquire)==0);
            let peak=peak.load(Ordering::Acquire);
            ensure!(peak==2*per_queue_in_flight,"offered Queue concurrency did not reach the selected envelope: {peak}");
            s.empty(QUEUE).await?;s.empty(FAST).await?;

            let completed=s.signals.completed.lock().unwrap();ensure!(completed.len()==528,"acknowledged load lost messages: {}",completed.len());
            let delivered=s.signals.deliveries.lock().unwrap();
            let unique=delivered.iter().map(|(_,_,id)|id).collect::<BTreeSet<_>>();ensure!(unique.len()==528);
            latencies.sort_unstable();
            eprintln!("QUEUE_LOAD {}",json!({"realRustFS":real,"extents":rooted,"producers":4,"inFlightRequests":2*per_queue_in_flight,"perQueueInFlight":per_queue_in_flight,"peakInFlightRequests":peak,"ordinarySlots":8,"ordinaryBytes":131072,"callbackSlots":2,"queues":2,"batchMessages":8,"steadyMessages":512,"archiveOutageMessages":16,"delivered":delivered.len(),"uniqueMessageIds":unique.len(),"elapsedMs":cellar::asyncrt::mono_ms()-start,"enqueueBatchP50Ms":latencies[latencies.len()/2],"enqueueBatchP99Ms":latencies[(latencies.len()*99/100).min(latencies.len()-1)]}));
            Ok::<_,anyhow::Error>(())
        }).await;
        let retain = !matches!(&outcome, Ok(Ok(())));
        let cleanup = s.close(retain).await;
        outcome??;
        cleanup?;
    }
    Ok(())
}
