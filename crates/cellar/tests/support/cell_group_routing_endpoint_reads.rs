// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A selected read repairs a new boot before entering the query producer.
use super::*;

struct HeldView {
    store: Arc<fault::Store>,
    gate: Arc<fault::RegistryGate>,
}

impl Drop for HeldView {
    fn drop(&mut self) {
        *self.store.denied_prefix.lock().unwrap() = None;
        *self.store.routing_read_after.lock().unwrap() = None;
        self.gate.armed.store(false, Ordering::Release);
        self.gate.resume.notify_one();
    }
}

fn barriers(cluster: &nodes::Cluster) -> Result<u64> {
    cluster.managed.hosts[..INGRESS]
        .iter()
        .map(|host| {
            let sample = host.performance();
            ensure!(sample.available && !sample.saturated);
            sample
                .counters
                .into_iter()
                .find(|counter| counter.name == "cell_linearizable_read_barriers")
                .map(|counter| counter.value)
                .context("endpoint read barrier counter")
        })
        .sum()
}

pub(super) async fn run(make: Option<RoutingStoreFactory>) -> Result<()> {
    for rooted in [false, true] {
        let commands = Arc::new(AtomicUsize::new(0));
        let queries = Arc::new(AtomicUsize::new(0));
        let on_command = commands.clone();
        let on_query = queries.clone();
        let store = Arc::new(match make {
            Some(make) => fault::Store::around(make("routing-endpoint-read-first", rooted)?),
            None => fault::Store::default(),
        });
        let filesystem = storage_fault::Gate::new(None);
        let mut cluster = Box::pin(nodes::Cluster::start_with_read_control(
            rooted,
            store,
            Arc::new(storage_fault::FaultFileSystem { gate: filesystem }),
            Arc::new(move || {
                on_command.fetch_add(1, Ordering::AcqRel);
            }),
            Arc::new(move |_| {
                on_query.fetch_add(1, Ordering::AcqRel);
            }),
        ))
        .await?;
        let result = Box::pin(exercise(&mut cluster, &commands, &queries, rooted)).await;
        if result.is_err() {
            crate::managed_fixture::diagnostics::record(&cluster.managed, "endpoint-read-first")
                .await;
        }
        let joined = cluster.close().await;
        if result.is_err() || joined.is_err() {
            for root in std::mem::take(&mut cluster.managed.roots) {
                eprintln!("retained endpoint read disk: {}", root.keep().display());
            }
        }
        cluster.managed.fixtures[0].domain.shutdown().await;
        result.with_context(|| format!("endpoint read first extents={rooted}"))?;
        joined?;
    }
    Ok(())
}

async fn exercise(
    cluster: &mut nodes::Cluster,
    commands: &AtomicUsize,
    queries: &AtomicUsize,
    rooted: bool,
) -> Result<()> {
    cluster.authorize_commands()?;
    cluster.authorize_ingress_reads()?;
    cluster.bind()?;
    lead_on_zero(cluster).await?;
    let history = cluster.managed.history;
    let logical = counter::history(history.cluster, history.namespace, "one")?;
    cluster.managed.hosts[0]
        .cell(logical.clone())?
        .enroll(
            CellEnrollmentRequest {
                creation: ProposalId::new([231; 16])?,
                durability: Durability::S3,
            },
            TIMEOUT,
        )
        .await?;
    let cell = cluster.managed.hosts[INGRESS].cell(logical.clone())?;
    let anchor = cell
        .command(
            &cell.contract("Increment")?,
            1_i64.to_be_bytes().to_vec(),
            options(1)?,
        )
        .await?;
    ensure!(
        anchor.result == 1_i64.to_be_bytes()
            && anchor.durability == Durability::S3
            && commands.load(Ordering::Acquire) == 1
    );
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(TIMEOUT + Duration::from_secs(1))
        .build()?;
    let fixture = &cluster.managed.fixtures[0];
    let catalog = Catalog::new(
        fixture.store.clone(),
        CatalogOptions {
            prefix: "managed/".into(),
            ..Default::default()
        },
    )?;
    let namespace = fixture
        .domain
        .run(catalog.existing_namespace(fixture.declaration.namespace.clone()))
        .await??;
    let suffix = format!("{}/", hex::encode(history.namespace.bytes()));
    let prefix = namespace
        .data_prefix()
        .strip_suffix(suffix.as_str())
        .context("endpoint read namespace prefix")?;
    let mut ordinal = 0_u8;
    for (caller, adapter) in [("Sdk", None), ("Native", Some(false)), ("Axum", Some(true))] {
        for strong in [true, false] {
            let before_view = cluster.managed.fixtures[0]
                .domain
                .run(cluster.managed.directory.observe())
                .await??;
            let before_route = cluster.managed.fixtures[0]
                .domain
                .run(
                    cluster
                        .managed
                        .directory
                        .lookup(&before_view, &logical.cell),
                )
                .await??;
            let configuration = cluster.managed.hosts[0]
                .group(history)?
                .quorum_membership(TIMEOUT)
                .await?;
            let revision = configuration.membership().revision();
            let voters = configuration.configuration().clone();
            drop(configuration);
            let old = cluster.managed.peers[0].clone();
            let path = object_store::path::Path::from(format!(
                "{prefix}nodes/{}/view",
                hex::encode(old.node.bytes())
            ));
            let store = cluster.managed.fixtures[INGRESS].store.clone();
            let gate = Arc::new(fault::RegistryGate::default());
            let _held = HeldView {
                store: store.clone(),
                gate: gate.clone(),
            };
            let denied = store.denied_prefix_reads.load(Ordering::Acquire);
            *store.denied_prefix.lock().unwrap() = Some(path.clone());
            tokio::time::timeout(TIMEOUT, async {
                while store.denied_prefix_reads.load(Ordering::Acquire) == denied {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            })
            .await
            .context("read endpoint metadata denial entered")?;
            cluster.restart_voter(0, 160 + ordinal).await?;
            lead_on_zero(cluster).await?;
            let current = cluster.managed.peers[0].clone();
            ensure!(
                old.node == current.node
                    && old.disk == current.disk
                    && old.process != current.process
            );
            let cut = cluster.managed.fixtures[0].catalog.read(TIMEOUT).await?;
            tokio::time::timeout(TIMEOUT, async {
                loop {
                    let observed = cluster.managed.hosts[INGRESS].observe_node(TIMEOUT).await?;
                    if observed
                        .fleet_trust_revision
                        .is_some_and(|r| r >= cut.revision())
                    {
                        return Ok::<_, anyhow::Error>(());
                    }
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            })
            .await
            .context("read ingress installs current boot trust")??;
            let old_inspection = cluster.managed.hosts[INGRESS]
                .inspect_peer_node(old.node, cut.revision(), TIMEOUT)
                .await;
            ensure!(
                matches!(
                    old_inspection,
                    Err(InspectionError::Refused | InspectionError::StalePeer)
                ),
                "old read endpoint inspection={old_inspection:?}"
            );
            // Hold the real new NodeView response before acceptance. The query
            // keeps one selected member and its original five-second ticket.
            gate.armed.store(true, Ordering::Release);
            *store.routing_read_after.lock().unwrap() = Some((path, gate.clone()));
            *store.denied_prefix.lock().unwrap() = None;
            tokio::time::timeout(TIMEOUT, gate.entered.notified())
                .await
                .context("new endpoint response held before read admission")?;
            let before = work(cluster)?;
            let before_queries = queries.load(Ordering::Acquire);
            let before_stages = sql_stages(cluster)?;
            let before_barriers = barriers(cluster)?;
            let invocation = ProposalId::new([190 + ordinal; 16])?;
            let deadline = tokio::time::Instant::now() + TIMEOUT;
            let pending = async {
                if let Some(axum) = adapter {
                    let mut request = client
                        .get(cluster.web()?.url(INGRESS, axum, "one"))
                        .header("x-query-id", hex::encode(invocation.bytes()))
                        .header("x-query-members", "1")
                        .header("x-query-attempt-ms", TIMEOUT.as_millis().to_string());
                    if !strong {
                        request = request
                            .header("x-read-consistency", "at-least")
                            .header("x-min-version", anchor.version_token.as_str());
                    }
                    return http::Web::send(
                        request,
                        format!("read-first endpoint {caller} strong={strong}"),
                    )
                    .await?
                    .success();
                }
                let result = cell
                    .query(
                        invocation,
                        &cell.contract("Get")?,
                        vec![],
                        QueryOptions {
                            consistency: if strong {
                                QueryConsistency::Linearizable
                            } else {
                                QueryConsistency::AtLeast(anchor.version_token.clone())
                            },
                            timeout: TIMEOUT,
                            routing: QueryRouting::Replicas {
                                members: vec![MemberId::new(1)?],
                                attempt_timeout: TIMEOUT,
                                leader_fallback: false,
                            },
                            ..Default::default()
                        },
                    )
                    .await?;
                ensure!(
                    matches!(result.version.covers(&anchor.version), Ok(true))
                        && result.witness.group == history
                );
                Ok::<_, anyhow::Error>(serde_json::json!({
                    "cell": result.version.history.cell.as_str(), "value": i64::from_be_bytes(result.result.as_slice().try_into()?),
                    "served_by": result.served_by.get(), "version_token": result.version_token.as_str(),
                }))
            };
            let release = async {
                tokio::time::timeout_at(deadline, async {
                    while work(cluster)?[4] == before[4] {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    ensure!(
                        queries.load(Ordering::Acquire) == before_queries
                            && commands.load(Ordering::Acquire) == 1
                            && sql_stages(cluster)? == before_stages
                    );
                    gate.resume.notify_one();
                    Ok::<_, anyhow::Error>(())
                })
                .await
                .context("selected read requests endpoint repair before producer admission")?
            };
            let (result, released) = tokio::join!(pending, release);
            let reply = result.with_context(|| format!("read-first {caller} strong={strong}; release={released:?}; before={before:?}, after={:?}", work(cluster)))?;
            released?;
            ensure!(tokio::time::Instant::now() < deadline);
            let after = work(cluster)?;
            let delta: Vec<_> = after.iter().zip(before).map(|(a, b)| a - b).collect();
            ensure!(
                delta[..4] == [0, 0, 0, 0] && delta[4] == 1 && delta[5] == 1 && delta[6] == 0,
                "read endpoint work={delta:?}"
            );
            ensure!(
                commands.load(Ordering::Acquire) == 1
                    && queries.load(Ordering::Acquire) == before_queries + 1
            );
            ensure!(
                reply["cell"] == logical.cell.as_str()
                    && reply["value"] == 1
                    && reply["served_by"] == 1
            );
            let token = CellVersionToken::from_encoded(
                reply["version_token"]
                    .as_str()
                    .context("read-first version token")?,
            )?;
            let now = u64::try_from(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)?
                    .as_millis(),
            )?;
            let observed = Fixture::version_keys()?.verify_cell(
                &token,
                &logical,
                cluster.managed.fixtures[0]
                    .declaration
                    .namespace
                    .schema_digest,
                now,
            )?;
            ensure!(
                matches!(observed.version.covers(&anchor.version), Ok(true))
                    && observed.witness.group == history
                    && observed.witness.position.index >= anchor.witness.position.index
            );
            let after_barriers = barriers(cluster)?;
            ensure!(if strong {
                after_barriers > before_barriers
            } else {
                after_barriers == before_barriers
            });
            tokio::time::timeout(TIMEOUT, async {
                loop {
                    if cluster.managed.hosts.iter().all(|host| {
                        let usage = host.resource_usage();
                        usage.host.controller.ordinary.items == 0
                            && usage.runtime.read_pins.items == 0
                    }) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            })
            .await
            .context("read-first physical owners drain")?;
            let after_view = cluster.managed.fixtures[0]
                .domain
                .run(cluster.managed.directory.observe())
                .await??;
            let after_route = cluster.managed.fixtures[0]
                .domain
                .run(cluster.managed.directory.lookup(&after_view, &logical.cell))
                .await??;
            ensure!(after_view.digest() == before_view.digest() && before_route == after_route);
            let configuration = cluster.managed.hosts[0]
                .group(history)?
                .quorum_membership(TIMEOUT)
                .await?;
            ensure!(
                configuration.membership().revision() == revision
                    && configuration.configuration() == &voters
            );
            drop(configuration);
            ensure!(
                cluster.managed.hosts[INGRESS]
                    .observe_node(TIMEOUT)
                    .await?
                    .node
                    .groups
                    == 0
            );
            eprintln!(
                "ENDPOINT_READ_EVIDENCE {}",
                serde_json::json!({
                    "schema": 1, "case": "read_first_refreshes_endpoint", "event": "verified", "caller": caller, "extents": rooted,
                    "consistency": if strong { "Strong" } else { "AtLeast" }, "invocation": hex::encode(invocation.bytes()),
                    "node": hex::encode(current.node.bytes()), "disk": hex::encode(current.disk.bytes()),
                    "old_process": hex::encode(old.process.bytes()), "current_process": hex::encode(current.process.bytes()),
                    "directory_before": hex::encode(before_view.digest()), "directory_after": hex::encode(after_view.digest()),
                    "membership_revision": format!("{revision:?}"), "routing_before": before, "routing_after": after,
                    "queries_before": before_queries, "queries_after": queries.load(Ordering::Acquire), "commands": commands.load(Ordering::Acquire),
                    "barriers_before": before_barriers, "barriers_after": after_barriers, "timeout_ms": TIMEOUT.as_millis(),
                    "maximum_attempts": QueryOptions::default().max_attempts, "selected_member": 1, "leader_fallback": false,
                    "minimum": format!("{:?}", anchor.version), "version": format!("{:?}", observed.version), "witness": format!("{:?}", observed.witness), "reply": reply,
                })
            );
            ordinal += 1;
        }
    }
    Ok(())
}
