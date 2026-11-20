// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A committed child Ack does not make an interrupted parent's result known.
use super::*;

#[derive(Default)]
pub(super) struct AckCut {
    pub(super) committed: Notify,
    pub(super) receipt: Mutex<Option<String>>,
    pub(super) calls: AtomicUsize,
}

pub(super) async fn qualify(real: bool, minimum: Durability) -> Result<()> {
    let mut failures = Vec::new();
    for rooted in [false, true] {
        for retained in [false, true] {
            let result = run(real, rooted, retained, minimum).await;
            match result {
                Ok(()) => eprintln!("EVENT_ACK_RECOVERY RustFS={real} extents={rooted} retained={retained} source={minimum:?}: PASS"),
                Err(error) => {
                    eprintln!("EVENT_ACK_RECOVERY RustFS={real} extents={rooted} retained={retained} source={minimum:?}: FAILED: {error:#}");
                    failures.push(error);
                }
            }
        }
    }
    if let Some(error) = failures.into_iter().next() {
        return Err(error.context("committed Ack / lost parent qualification failed"));
    }
    Ok(())
}

fn options(sequence: u64, minimum: Durability) -> Result<CommandOptions> {
    let mut options = CommandOptions::new(
        CommandId::new(ClientId::new([228; 16])?, sequence)?,
        u64::try_from(cellar::asyncrt::wall_ms())? + 600_000,
    );
    ensure!(options.timeout == TIMEOUT);
    options.durability = minimum;
    Ok(options)
}

async fn invoke(
    cluster: &ManagedCluster,
    history: &CellHistory,
    sequence: u64,
    minimum: Durability,
    method: &str,
    input: serde_json::Value,
) -> Result<CommandReceipt> {
    let leader = managed_fixture::leader(&cluster.hosts, cluster.history).await?;
    let cell = cluster.hosts[leader].cell(history.clone())?;
    cell.command(
        &cell.contract(method)?,
        serde_json::to_vec(&input)?,
        options(sequence, minimum)?,
    )
    .await
    .with_context(|| format!("Ack recovery {method} sequence={sequence} source={minimum:?}"))
}

async fn run(real: bool, rooted: bool, retained: bool, minimum: Durability) -> Result<()> {
    let purpose = match (retained, minimum) {
        (false, Durability::Quorum) => "ack-live-quorum",
        (true, Durability::Quorum) => "ack-retained-quorum",
        (false, Durability::S3) => "ack-live-s3",
        (true, Durability::S3) => "ack-retained-s3",
    };
    let store = Arc::new(if real {
        catalog_fault::Store::around(rustfs::store(purpose, rooted)?)
    } else {
        catalog_fault::Store::default()
    });
    let cut = Arc::new(AckCut::default());
    let (app, configuration) = application_with_ack_cut(Some(cut.clone()))?;
    let definitions = [
        "Counter",
        cellar::deploy::EVENT_STREAM_CLASS,
        cellar::deploy::EVENT_CATALOG_CLASS,
    ]
    .into_iter()
    .map(|name| {
        Ok((
            app.resolve_raft_class(name)?,
            if name == "Counter" {
                Durability::Quorum
            } else {
                Durability::S3
            },
        ))
    })
    .collect::<Result<Vec<_>>>()?;
    let mut fixture = Fixture::with_definition_policies(rooted, definitions, app, store).await?;
    fixture.configuration = Some(configuration);
    fixture.sql_workers = 1;
    fixture.max_input_bytes = 16 * 1024;
    fixture.effects = Some(RaftEffectOptions {
        callback_timeout_ms: 5000,
        ..Default::default()
    });
    let domain = fixture.domain.clone();
    let mut cluster = Box::pin(ManagedCluster::start_with_applications(
        fixture,
        &[],
        ArchiveSetup::Initialized,
        false,
        None,
        FirstGroupSetup::Normal,
    ))
    .await?;
    let work = domain
        .run(async {
            named_service::activate_at(&cluster, "stream-archive", 0).await?;
            named_service::activate_at(&cluster, "catalog-archive", 1).await?;
            for node in &cluster.nodes {
                node.activate_event_resources().await?;
            }
            let producer = CellHistory {
                cluster: cluster.history.cluster,
                namespace: cluster.history.namespace,
                cell: CellId::new("Counter:ack-producer")?,
                incarnation: IncarnationId::new([211; 16])?,
            };
            let sink = CellHistory {
                cell: CellId::new("Counter:ack-sink")?,
                incarnation: IncarnationId::new([212; 16])?,
                ..producer.clone()
            };
            let leader = managed_fixture::leader(&cluster.hosts, cluster.history).await?;
            for (index, history) in [&producer, &sink].into_iter().enumerate() {
                cluster.hosts[leader]
                    .cell(history.clone())?
                    .enroll(
                        CellEnrollmentRequest {
                            creation: ProposalId::new([213 + index as u8; 16])?,
                            durability: if index == 0 {
                                Durability::Quorum
                            } else {
                                minimum
                            },
                        },
                        TIMEOUT,
                    )
                    .await
                    .context("Ack recovery source enrollment")?;
            }
            let mut sequence = std::collections::BTreeMap::new();
            cellar::asyncrt::timeout(Duration::from_secs(20), async {
                loop {
                    let response = call(
                        &cluster,
                        &producer,
                        &mut sequence,
                        "Configuration",
                        json!(null),
                    )
                    .await?;
                    let value: serde_json::Value = serde_json::from_slice(&response.result)?;
                    if value["state"] == "active" {
                        return Ok::<_, anyhow::Error>(());
                    }
                    cellar::asyncrt::sleep(Duration::from_millis(100)).await;
                }
            })
            .await
            .context("Ack recovery resource activation")??;
            call(
                &cluster,
                &producer,
                &mut sequence,
                "PublishS3",
                json!("ack-cut"),
            )
            .await?;
            cellar::asyncrt::timeout(Duration::from_secs(20), async {
                loop {
                    let response =
                        call(&cluster, &producer, &mut sequence, "Read", json!(null)).await?;
                    let values: Vec<serde_json::Value> = serde_json::from_slice(&response.result)?;
                    if values.len() == 1 {
                        return Ok::<_, anyhow::Error>(());
                    }
                    cellar::asyncrt::sleep(Duration::from_millis(100)).await;
                }
            })
            .await
            .context("Ack recovery publication")??;
            let initial: Vec<_> = cluster.hosts.iter().map(RaftCells::performance).collect();
            let leader = managed_fixture::leader(&cluster.hosts, cluster.history).await?;
            let source = cluster.hosts[leader].cell(sink.clone())?;
            let contract = source.contract("HoldAck")?;
            let input = serde_json::to_vec(
                &json!({"fetch":"08080808080808080808080808080808","ack":true}),
            )?;
            let original = options(1, minimum)?;
            let call = source
                .command(&contract, input.clone(), original.clone())
                .boxed();
            let call = cellar::asyncrt::timeout(TIMEOUT, async {
                match futures_util::future::select(call, cut.committed.notified().boxed()).await {
                    futures_util::future::Either::Left((result, _)) => {
                        anyhow::bail!("parent completed before committed Ack cut: {result:?}")
                    }
                    futures_util::future::Either::Right(((), call)) => Ok(call),
                }
            })
            .await
            .context("child Ack within original parent deadline")??;
            let result = if retained {
                let (joined, result) = futures_util::future::join(
                    Box::pin(named_service::restart(&mut cluster)),
                    call,
                )
                .await;
                joined.context("joined retained restart after committed Ack")?;
                result
            } else {
                call.await
            };
            ensure!(
                result
                    .err()
                    .context("held parent must remain unresolved")?
                    .ambiguous
            );
            let receipt = cut
                .receipt
                .lock()
                .unwrap()
                .clone()
                .context("actual committed Ack receipt")?;
            // No application request or receipt lookup may drive recovery here.
            cellar::asyncrt::timeout(Duration::from_secs(60), async {
                loop {
                    let mut settled = false;
                    for (host, before) in cluster.hosts.iter().zip(&initial) {
                        let after = host.performance();
                        ensure!(
                            before.available
                                && after.available
                                && !before.saturated
                                && !after.saturated
                        );
                        if !retained {
                            ensure!(before.source == after.source, "a live voter restarted");
                        }
                        let count = |snapshot: &RaftPerformanceSnapshot| {
                            snapshot
                                .counters
                                .iter()
                                .find(|c| c.name == "cell_binding_recovery_settlements")
                                .map_or(0, |c| c.value)
                        };
                        settled |= if retained {
                            count(&after) > 0
                        } else {
                            count(&after) > count(before)
                        };
                    }
                    if settled {
                        return Ok::<_, anyhow::Error>(());
                    }
                    cellar::asyncrt::sleep(Duration::from_millis(100)).await;
                }
            })
            .await
            .context("autonomous Ack obligation settlement without traffic")??;
            let ready = invoke(&cluster, &sink, 2, minimum, "RecoveryReady", json!(null)).await?;
            ensure!(
                ready.result == b"true",
                "settlement left an unresolved binding"
            );
            let leader = managed_fixture::leader(&cluster.hosts, cluster.history).await?;
            let recovered = cluster.hosts[leader].cell(sink.clone())?;
            let error = recovered
                .command(&recovered.contract("HoldAck")?, input, original)
                .await
                .err()
                .context("original parent must stay unknown")?;
            ensure!(
                error.ambiguous && cut.calls.load(Ordering::Acquire) == 1,
                "unknown parent was replayed or resolved"
            );
            let ack = invoke(&cluster, &sink, 3, minimum, "Ack", json!(receipt)).await?;
            ensure!(
                serde_json::from_slice::<String>(&ack.result)? == "AlreadyAcknowledged",
                "fresh Ack did not advance beyond the retired child sequence"
            );
            let effects = invoke(&cluster, &sink, 4, minimum, "Effects", json!(null)).await?;
            ensure!(
                serde_json::from_slice::<u64>(&effects.result)? == 1,
                "consumer effects were lost or repeated"
            );
            let status = invoke(&cluster, &sink, 5, minimum, "Status", json!(null)).await?;
            ensure!(
                serde_json::from_slice::<serde_json::Value>(&status.result)?["checkpoint"] == "1",
                "Ack changed the committed checkpoint"
            );
            ensure!(cut.calls.load(Ordering::Acquire) == 1 && cluster.nodes.len() == 3);
            Ok::<_, anyhow::Error>(())
        })
        .await
        .map_err(anyhow::Error::from)
        .and_then(std::convert::identity);
    let joined =
        managed_fixture::close_group_nodes(std::mem::take(&mut cluster.nodes), &cluster.hosts)
            .await;
    if work.is_err() || joined.is_err() {
        for root in std::mem::take(&mut cluster.roots) {
            eprintln!(
                "retained event-ack-recovery disk: {}",
                root.keep().display()
            );
        }
    }
    domain.shutdown().await;
    work?;
    joined
}
