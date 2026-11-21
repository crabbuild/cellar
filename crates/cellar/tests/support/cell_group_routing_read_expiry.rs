// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A newer heartbeat cannot renew an entered query's source proof or free its image.
use super::*;

const SOURCE_AGE: Duration = Duration::from_millis(500);

fn read(
    drill: &Drill<'_>,
    ingress: Ingress,
    member: MemberId,
    expected: &CommandReceipt,
    must_expire: bool,
) -> (
    ProposalId,
    BoxFuture<'static, Result<Option<serde_json::Value>>>,
) {
    let ordinal = drill.queries.fetch_add(1, Ordering::AcqRel) + 1;
    let invocation = ProposalId::new((9000_u128 + u128::from(ordinal)).to_be_bytes()).unwrap();
    let cell = drill.cluster.managed.hosts[nodes::INGRESS].cell(expected.version.history.clone());
    let url = drill
        .cluster
        .web()
        .map(|web| web.url(nodes::INGRESS, matches!(ingress, Ingress::Axum), "one"));
    let client = drill.client.clone();
    let future = async move {
        let mut value = if matches!(ingress, Ingress::Sdk) {
            let cell = cell?;
            let result = cell
                .query(
                    invocation,
                    &cell.contract("Get")?,
                    vec![],
                    QueryOptions {
                        consistency: QueryConsistency::Stale,
                        stale_policy: StaleReadPolicy::RecentSource,
                        timeout: TIMEOUT,
                        max_attempts: 1,
                        routing: QueryRouting::Replicas {
                            members: vec![member],
                            attempt_timeout: TIMEOUT,
                            leader_fallback: false,
                        },
                    },
                )
                .await;
            if must_expire {
                let error = result.err().context("expired source query must refuse")?;
                ensure!(
                    error.refusal() == Some(&CommandFailure::VersionUnavailable),
                    "expired source returned {error:?}"
                );
                return Ok(None);
            }
            let reply = result?;
            serde_json::json!({
                "cell": reply.version.history.cell.as_str(),
                "value": i64::from_be_bytes(reply.result.as_slice().try_into()?),
                "sequence": reply.version.sequence.get(),
                "version_token": reply.version_token.as_str(),
                "served_by": reply.served_by.get(),
            })
        } else {
            let response = http::Web::send(
                client
                    .get(url?)
                    .header("x-query-id", hex::encode(invocation.bytes()))
                    .header("x-read-consistency", "stale")
                    .header("x-read-policy", "recent-source")
                    .header("x-query-members", member.get().to_string())
                    .header("x-query-attempt-ms", TIMEOUT.as_millis().to_string())
                    .header("x-leader-fallback", "false"),
                format!("source expiry ingress={ingress:?} invocation={invocation:?}"),
            )
            .await?;
            if must_expire {
                response.refused(503, "VersionUnavailable")?;
                return Ok(None);
            }
            response.success()?
        };
        value["invocation"] = hex::encode(invocation.bytes()).into();
        Ok(Some(value))
    }
    .boxed();
    (invocation, future)
}

async fn caught_up(host: &RaftCells, receipt: &CommandReceipt) -> Result<()> {
    tokio::time::timeout(TIMEOUT, async {
        loop {
            let status = host.group(receipt.witness.group)?.status().await?;
            if status.applied.index >= receipt.witness.position.index {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .context("source follower applied the exact command receipt")?
}

async fn increment(cluster: &nodes::Cluster, sequence: u64) -> Result<CommandReceipt> {
    let group = cluster.managed.history;
    let leader = cluster.leader().await?;
    let cell = cluster.managed.hosts[leader].cell(counter::history(
        group.cluster,
        group.namespace,
        "one",
    )?)?;
    let receipt = cell
        .command(
            &cell.contract("Increment")?,
            1_i64.to_be_bytes().to_vec(),
            CommandOptions {
                durability: Durability::S3,
                timeout: TIMEOUT,
                ..CommandOptions::new(
                    CommandId::new(ClientId::new([244; 16])?, sequence)?,
                    now()? + 60_000,
                )
            },
        )
        .await?;
    ensure!(
        receipt.durability == Durability::S3
            && receipt.result == i64::try_from(sequence)?.to_be_bytes()
    );
    Ok(receipt)
}

pub(super) async fn run() -> Result<()> {
    for rooted in [false, true] {
        let callbacks = Arc::new(read_gate::Callbacks::default());
        let on_command = callbacks.clone();
        let on_query = callbacks.clone();
        let filesystem = storage_fault::Gate::new(None);
        let mut cluster = Box::pin(nodes::Cluster::start_with_source_age(
            rooted,
            Arc::new(fault::Store::default()),
            Arc::new(storage_fault::FaultFileSystem {
                gate: filesystem.clone(),
            }),
            Arc::new(move || {
                on_command.commands.fetch_add(1, Ordering::AcqRel);
            }),
            Arc::new(move |cell| on_query.query(cell)),
            SOURCE_AGE,
        ))
        .await?;
        let work = async {
            cluster.authorize_commands()?;
            cluster.authorize_ingress_reads()?;
            cluster.bind()?;
            let group = cluster.managed.history;
            let leader = cluster.leader().await?;
            let cell = cluster.managed.hosts[leader]
                .cell(counter::history(group.cluster, group.namespace, "one")?)?;
            cell.enroll(CellEnrollmentRequest {
                creation: ProposalId::new([243; 16])?,
                durability: Durability::Quorum,
            }, TIMEOUT).await?;
            let mut expected = increment(&cluster, 1).await?;
            let drill = Drill {
                cluster: &cluster,
                callbacks: &callbacks,
                client: reqwest::Client::builder().no_proxy()
                    .timeout(TIMEOUT + Duration::from_secs(1)).build()?,
                queries: AtomicU64::new(0),
                filesystem: &filesystem,
                origin: nodes::INGRESS,
            };
            for ingress in [Ingress::Sdk, Ingress::Native, Ingress::Axum] {
                drill.drained().await?;
                let leader = cluster.leader().await?;
                let follower = (leader + 1) % nodes::INGRESS;
                let member = MemberId::new(follower as u64 + 1)?;
                let host = &cluster.managed.hosts[follower];
                caught_up(host, &expected).await?;
                let warm = read(&drill, ingress, member, &expected, false).1.await?
                    .context("fresh source result")?;
                drill.verify(&warm, &expected, member.get())?;
                drill.drained().await?;
                let status = host.group(group)?.status().await?;
                ensure!(status.leader == Some(leader as u64 + 1));
                let barriers = drill.barriers()?;
                let before = callbacks.queries.load(Ordering::Acquire);
                let hold = callbacks.arm();
                let mut readers = tokio::task::JoinSet::new();
                let started = std::time::Instant::now();
                let (expired_invocation, pending) = read(&drill, ingress, member, &expected, true);
                readers.spawn(pending);
                drill.wait("actual recent-source SQL callback owns its image", || {
                    Ok(callbacks.queries.load(Ordering::Acquire) == before + 1
                        && host.resource_usage().runtime.read_pins.items == 1)
                }).await?;
                ensure!(readers.try_join_next().is_none(), "held source query completed early");
                let joined = tokio::time::timeout(SOURCE_AGE * 4, readers.join_next())
                    .await.context("source expiry must precede the original invocation deadline")?
                    .context("one expired query task")?;
                let result = joined.context("joined expired query task")??;
                ensure!(result.is_none() && started.elapsed() < TIMEOUT);
                let elapsed_ms = started.elapsed().as_millis();
                let retained = host.resource_usage();
                ensure!(retained.runtime.read_pins.items == 1
                    && retained.host.controller.ordinary.items == 1);
                ensure!(callbacks.queries.load(Ordering::Acquire) == before + 1);

                // The actual source keeps sending heartbeats while the entered
                // proof expires. A new committed image cannot free this reader.
                let commands = callbacks.commands.load(Ordering::Acquire);
                let next = increment(&cluster, expected.id.sequence() + 1).await?;
                ensure!(callbacks.commands.load(Ordering::Acquire) == commands + 1);
                ensure!(next.version.sequence > expected.version.sequence);
                ensure!(host.resource_usage().runtime.read_pins.items == 1);
                let current = host.group(group)?.status().await?;
                ensure!(current.term == status.term && current.leader == status.leader);
                ensure!(drill.barriers()? == barriers);
                drop(hold);
                drill.drained().await?;
                caught_up(host, &next).await?;
                let fresh = read(&drill, ingress, member, &next, false).1.await?
                    .context("new source proof after the held physical reader exits")?;
                drill.verify(&fresh, &next, member.get())?;
                drill.drained().await?;
                ensure!(callbacks.queries.load(Ordering::Acquire) == before + 2);
                ensure!(drill.barriers()? == barriers);
                let fresh_status = host.group(group)?.status().await?;
                ensure!(fresh_status.term == status.term && fresh_status.leader == status.leader);
                cellar::note!("READ_SOURCE_EXPIRY_EVIDENCE {}", serde_json::json!({
                    "schema": 1, "case": "recent_source_expiry_keeps_physical_image",
                    "extents": rooted, "ingress": format!("{ingress:?}"),
                    "source_age_ms": SOURCE_AGE.as_millis(), "request_timeout_ms": TIMEOUT.as_millis(),
                    "expired_query": hex::encode(expired_invocation.bytes()), "elapsed_ms": elapsed_ms,
                    "member": member.get(), "term": status.term.get(),
                    "expired_failure": "VersionUnavailable", "held_read_pins": retained.runtime.read_pins.items,
                    "held_ordinary": retained.host.controller.ordinary.items,
                    "released_read_pins": host.resource_usage().runtime.read_pins.items,
                    "released_ordinary": host.resource_usage().host.controller.ordinary.items,
                    "old_version": format!("{:?}", expected.version),
                    "new_version": format!("{:?}", next.version), "command": format!("{:?}", next.id),
                    "result_be": hex::encode(&next.result), "receipt": format!("{:?}", next.witness),
                    "required_durability": "S3", "achieved_durability": "S3",
                    "callbacks_before": before, "callbacks_after": callbacks.queries.load(Ordering::Acquire),
                    "commands_before": commands, "commands_after": callbacks.commands.load(Ordering::Acquire),
                    "barriers_before": barriers, "barriers_after": drill.barriers()?,
                    "fresh_query": fresh["invocation"], "source_term_unchanged": true,
                }));
                expected = next;
            }
            Ok::<_, anyhow::Error>(())
        }.await;
        callbacks.resume();
        let joined = cluster.close().await;
        if work.is_err() || joined.is_err() {
            for root in std::mem::take(&mut cluster.managed.roots) {
                cellar::note!("retained source expiry disk: {}", root.keep().display());
            }
        }
        cluster.managed.fixtures[0].domain.shutdown().await;
        work?;
        joined?;
    }
    Ok(())
}
