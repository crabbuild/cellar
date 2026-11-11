// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Automatic preferences retain the destination's actual cell-version proof.
use super::*;
use futures_util::{future::BoxFuture, FutureExt};
use std::sync::atomic::AtomicU64;

#[path = "cell_group_routing_read_preference_faults.rs"]
mod faults;

#[derive(Clone, Copy, Debug)]
enum Ingress {
    Sdk,
    Native,
    Axum,
}

#[derive(Clone, Copy, Debug)]
enum Mode {
    Strong,
    AtLeast,
    Historical,
    RecentSource,
}

struct Drill<'a> {
    cluster: &'a nodes::Cluster,
    callbacks: &'a Arc<read_gate::Callbacks>,
    client: reqwest::Client,
    queries: AtomicU64,
    filesystem: &'a Arc<storage_fault::Gate>,
    origin: usize,
}

fn now() -> Result<u64> {
    Ok(u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?)
}

impl Drill<'_> {
    async fn drained(&self) -> Result<()> {
        tokio::time::timeout(TIMEOUT, async {
            loop {
                self.callbacks.clean()?;
                let mut drained = true;
                for host in &self.cluster.managed.hosts {
                    let usage = host.resource_usage();
                    let ordinary = usage.host.controller.ordinary;
                    ensure!(ordinary.max_items == 2 && ordinary.max_bytes == 65536);
                    drained &= ordinary.items == 0 && usage.runtime.read_pins.items == 0;
                }
                if drained {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .context("preference query physical owners drained")?
    }

    fn barriers(&self) -> Result<Vec<u64>> {
        self.cluster
            .managed
            .hosts
            .iter()
            .map(|host| {
                let sample = host.performance();
                ensure!(sample.available && !sample.saturated);
                sample
                    .counters
                    .into_iter()
                    .find(|counter| counter.name == "cell_linearizable_read_barriers")
                    .map(|counter| counter.value)
                    .context("physical read barrier counter")
            })
            .collect()
    }

    fn run_id(&self) -> String {
        format!(
            "extents={}",
            self.cluster.managed.fixtures[0]
                .declaration
                .namespace
                .image_storage
                == RaftImageStorage::Extents
        )
    }

    fn read_call(
        &self,
        ingress: Ingress,
        mode: Mode,
        minimum: &CommandReceipt,
    ) -> (ProposalId, BoxFuture<'static, Result<serde_json::Value>>) {
        let ordinal = self.queries.fetch_add(1, Ordering::AcqRel) + 1;
        let invocation = ProposalId::new((5000_u128 + u128::from(ordinal)).to_be_bytes()).unwrap();
        let group = self.cluster.managed.history;
        let cell = counter::history(group.cluster, group.namespace, "one").and_then(|history| {
            self.cluster.managed.hosts[self.origin]
                .cell(history)
                .map_err(Into::into)
        });
        let url = self
            .cluster
            .web()
            .map(|web| web.url(self.origin, matches!(ingress, Ingress::Axum), "one"));
        let client = self.client.clone();
        let minimum = minimum.clone();
        let run = self.run_id();
        emit(
            &run,
            serde_json::json!({"event": "request", "invocation": hex::encode(invocation.bytes()),
            "ingress": format!("{ingress:?}"), "origin": self.origin, "mode": format!("{mode:?}"), "timeout_ms": TIMEOUT.as_millis(),
            "max_attempts": QueryOptions::default().max_attempts, "minimum": if matches!(mode, Mode::AtLeast) { Some(format!("{:?}", minimum.version)) } else { None }}),
        );
        let future = async move {
            let result = async {
                if matches!(ingress, Ingress::Sdk) {
                    let options = QueryOptions {
                        consistency: match mode {
                            Mode::Strong => QueryConsistency::Linearizable,
                            Mode::AtLeast => QueryConsistency::AtLeast(minimum.version_token.clone()),
                            Mode::Historical | Mode::RecentSource => QueryConsistency::Stale,
                        },
                        stale_policy: if matches!(mode, Mode::Historical) { StaleReadPolicy::DisconnectedHistorical } else { StaleReadPolicy::RecentSource },
                        timeout: TIMEOUT,
                        ..QueryOptions::default()
                    };
                    let cell = cell?;
                    let reply = cell.query(invocation, &cell.contract("Get")?, vec![], options).await?;
                    return Ok(serde_json::json!({
                        "cell": reply.version.history.cell.as_str(), "value": i64::from_be_bytes(reply.result.as_slice().try_into()?),
                        "sequence": reply.version.sequence.get(), "version_token": reply.version_token.as_str(), "served_by": reply.served_by.get(),
                    }));
                }
                let mut request = client.get(url?).header("x-query-id", hex::encode(invocation.bytes()));
                request = match mode {
                    Mode::Strong => request,
                    Mode::AtLeast => request.header("x-read-consistency", "at-least").header("x-min-version", minimum.version_token.as_str()),
                    Mode::Historical => request.header("x-read-consistency", "stale").header("x-read-policy", "disconnected-historical"),
                    Mode::RecentSource => request.header("x-read-consistency", "stale").header("x-read-policy", "recent-source"),
                };
                http::Web::send(request, format!("automatic preference {ingress:?} {mode:?} invocation={invocation:?}"))
                    .await?.success()
            }.await;
            match &result {
                Ok(reply) => emit(&run, serde_json::json!({"event": "selected", "invocation": hex::encode(invocation.bytes()), "served_by": reply["served_by"], "value": reply["value"]})),
                Err(error) => emit(&run, serde_json::json!({"event": "error", "invocation": hex::encode(invocation.bytes()), "error": format!("{error:?}")})),
            }
            result.map(|mut reply| { reply["invocation"] = hex::encode(invocation.bytes()).into(); reply })
        }.boxed();
        (invocation, future)
    }

    async fn query(
        &self,
        ingress: Ingress,
        mode: Mode,
        expected: &CommandReceipt,
    ) -> Result<serde_json::Value> {
        self.read_call(ingress, mode, expected).1.await
    }

    async fn wait(&self, label: &str, mut ready: impl FnMut() -> Result<bool>) -> Result<()> {
        tokio::time::timeout(TIMEOUT, async {
            while !ready()? {
                self.callbacks.clean()?;
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await
        .with_context(|| label.to_owned())?
    }

    fn verify(
        &self,
        reply: &serde_json::Value,
        expected: &CommandReceipt,
        member: u64,
    ) -> Result<()> {
        ensure!(reply["cell"].as_str() == Some(expected.version.history.cell.as_str()));
        ensure!(
            reply["value"].as_i64()
                == Some(i64::from_be_bytes(expected.result.as_slice().try_into()?))
        );
        ensure!(reply["sequence"].as_u64() == Some(expected.version.sequence.get()));
        ensure!(
            reply["served_by"].as_u64() == Some(member),
            "automatic read expected member {member}, got {reply}"
        );
        let token = CellVersionToken::from_encoded(
            reply["version_token"]
                .as_str()
                .context("query version token")?,
        )?;
        let observed = Fixture::version_keys()?.verify_cell(
            &token,
            &expected.version.history,
            self.cluster.managed.fixtures[0]
                .declaration
                .namespace
                .schema_digest,
            now()?,
        )?;
        ensure!(
            observed.version == expected.version
                && observed.witness.group == expected.witness.group
        );
        ensure!(observed.witness.position.index >= expected.witness.position.index);
        emit(
            &self.run_id(),
            serde_json::json!({"event": "verified_result", "invocation": reply["invocation"],
            "served_by": reply["served_by"], "version": format!("{:?}", observed.version), "witness": format!("{:?}", observed.witness), "value": reply["value"]}),
        );
        Ok(())
    }
}

fn emit(run: &str, mut event: serde_json::Value) {
    event["run"] = run.into();
    event["case"] = "read_preferences_preserve_proof".into();
    event["schema"] = 1.into();
    eprintln!("READ_PREFERENCE_EVIDENCE {event}");
}

pub(super) async fn run() -> Result<()> {
    for rooted in [false, true] {
        let callbacks = Arc::new(read_gate::Callbacks::default());
        let on_command = callbacks.clone();
        let on_query = callbacks.clone();
        let filesystem = storage_fault::Gate::new(None);
        let mut cluster = Box::pin(nodes::Cluster::start_with_read_control(
            rooted,
            Arc::new(fault::Store::default()),
            Arc::new(storage_fault::FaultFileSystem {
                gate: filesystem.clone(),
            }),
            Arc::new(move || {
                on_command.commands.fetch_add(1, Ordering::AcqRel);
            }),
            Arc::new(move |cell| on_query.query(cell)),
        ))
        .await?;
        let work = async {
            cluster.authorize_commands()?;
            cluster.authorize_ingress_reads()?;
            cluster.bind()?;
            let current = cluster.leader().await?;
            let group = cluster.managed.history;
            if current != 1 {
                let source = cluster.managed.hosts[current].group(group)?;
                let status = source.replication_status(TIMEOUT).await?;
                source
                    .transfer_leadership(
                        LeadershipTransfer {
                            target: MemberId::new(2)?,
                            expected_term: status.term,
                            expected_revision: status.membership_revision,
                        },
                        TIMEOUT,
                    )
                    .await?;
            }
            tokio::time::timeout(TIMEOUT, async {
                loop {
                    let mut ready = true;
                    for (index, host) in cluster.managed.hosts[..3].iter().enumerate() {
                        let status = host.group(group)?.status().await?;
                        ready &= status.leader == Some(2);
                        if index == 1 {
                            ready &= status.ready && status.applied.term == status.term;
                        }
                    }
                    if ready {
                        return Ok::<_, anyhow::Error>(());
                    }
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            })
            .await
            .context("public preference leader observed by every voter")??;
            let history = counter::history(group.cluster, group.namespace, "one")?;
            let cell = cluster.managed.hosts[1].cell(history)?;
            cell.enroll(
                CellEnrollmentRequest {
                    creation: ProposalId::new([181; 16])?,
                    durability: Durability::Quorum,
                },
                TIMEOUT,
            )
            .await?;
            let original_options = CommandOptions {
                durability: Durability::S3,
                timeout: TIMEOUT,
                ..CommandOptions::new(
                    CommandId::new(ClientId::new([215; 16])?, 1)?,
                    now()? + 60_000,
                )
            };
            let mut expected = cell
                .command(
                    &cell.contract("Increment")?,
                    1_i64.to_be_bytes().to_vec(),
                    original_options.clone(),
                )
                .await?;
            ensure!(
                expected.durability == Durability::S3 && expected.result == 1_i64.to_be_bytes()
            );
            tokio::time::timeout(TIMEOUT, async {
                loop {
                    let mut ready = true;
                    for host in &cluster.managed.hosts[..3] {
                        ready &= host.group(group)?.status().await?.applied.index
                            >= expected.witness.position.index;
                    }
                    if ready {
                        return Ok::<_, anyhow::Error>(());
                    }
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            })
            .await??;
            let mut drill = Drill {
                cluster: &cluster,
                callbacks: &callbacks,
                client: reqwest::Client::builder()
                    .no_proxy()
                    .timeout(TIMEOUT + Duration::from_secs(1))
                    .build()?,
                queries: AtomicU64::new(0),
                filesystem: &filesystem,
                origin: 0,
            };
            drill.drained().await?;
            for origin in [0, nodes::INGRESS] {
                drill.origin = origin;
                for ingress in [Ingress::Sdk, Ingress::Native, Ingress::Axum] {
                    for mode in [
                        Mode::Strong,
                        Mode::AtLeast,
                        Mode::Historical,
                        Mode::RecentSource,
                    ] {
                        if origin == nodes::INGRESS && matches!(mode, Mode::Strong) {
                            // A real authenticated replay warms leader advice at
                            // this ingress. No hint manufactures membership.
                            let remote = cluster.managed.hosts[origin]
                                .cell(expected.version.history.clone())?;
                            let replayed = remote
                                .command(
                                    &remote.contract("Increment")?,
                                    1_i64.to_be_bytes().to_vec(),
                                    original_options.clone(),
                                )
                                .await?;
                            ensure!(
                                replayed.id == expected.id
                                    && replayed.result == expected.result
                                    && replayed.version == expected.version
                                    && replayed.witness == expected.witness
                                    && replayed.durability == Durability::S3
                            );
                            ensure!(callbacks.commands.load(Ordering::Acquire) == 1);
                        }
                        let before = drill.barriers()?;
                        let reply = drill.query(ingress, mode, &expected).await?;
                        drill
                            .verify(
                                &reply,
                                &expected,
                                if matches!(mode, Mode::Strong) { 2 } else { 1 },
                            )
                            .with_context(|| {
                                format!(
                                    "preference origin={origin} ingress={ingress:?} mode={mode:?}"
                                )
                            })?;
                        drill.drained().await?;
                        let mut target = before;
                        if matches!(mode, Mode::Strong) {
                            target[1] += 1;
                        }
                        ensure!(
                            drill.barriers()? == target,
                            "preference changed required barrier work"
                        );
                    }
                }
            }
            drill.origin = 0;
            ensure!(
                callbacks.commands.load(Ordering::Acquire) == 1
                    && callbacks.queries.load(Ordering::Acquire) == 24
            );
            let other = cluster.managed.hosts[1].cell(counter::history(
                group.cluster,
                group.namespace,
                "two",
            )?)?;
            other
                .enroll(
                    CellEnrollmentRequest {
                        creation: ProposalId::new([182; 16])?,
                        durability: Durability::Quorum,
                    },
                    TIMEOUT,
                )
                .await?;
            let foreign = other
                .command(
                    &other.contract("Increment")?,
                    2_i64.to_be_bytes().to_vec(),
                    CommandOptions {
                        durability: Durability::S3,
                        timeout: TIMEOUT,
                        ..CommandOptions::new(
                            CommandId::new(ClientId::new([216; 16])?, 1)?,
                            now()? + 60_000,
                        )
                    },
                )
                .await?;
            for (ordinal, ingress) in [Ingress::Sdk, Ingress::Native, Ingress::Axum]
                .into_iter()
                .enumerate()
            {
                drill.foreign_minimum(ingress, &foreign).await?;
                expected = drill
                    .lagged_minimum(ingress, ordinal as u8, &expected)
                    .await?;
                drill.cancelled_fallback_pins(ingress, &expected).await?;
            }
            ensure!(
                callbacks.commands.load(Ordering::Acquire) == 5
                    && callbacks.queries.load(Ordering::Acquire) == 36
            );
            drill.drained().await?;
            Ok::<_, anyhow::Error>(())
        }
        .await;
        callbacks.resume();
        let joined = cluster.close().await;
        if work.is_err() || joined.is_err() {
            for root in std::mem::take(&mut cluster.managed.roots) {
                eprintln!("retained read preference disk: {}", root.keep().display());
            }
        }
        cluster.managed.fixtures[0].domain.shutdown().await;
        work.with_context(|| format!("public read preferences extents={rooted}"))?;
        joined?;
    }
    Ok(())
}
