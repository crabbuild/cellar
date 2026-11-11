// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A verified new boot repairs its endpoint without changing cell placement.
use super::*;
use object_store::ObjectStore as _;

const WORK: [&str; 7] = [
    "cell_command_rpc_attempts",
    "cell_command_leader_probes",
    "cell_command_directory_lookups",
    "cell_membership_demand_refreshes",
    "cell_endpoint_demand_refreshes",
    "cell_endpoint_discovery_owners",
    "cell_endpoint_refresh_joins",
];

fn work(cluster: &nodes::Cluster) -> Result<[u64; 7]> {
    let sample = cluster.managed.hosts[INGRESS].performance();
    ensure!(sample.available && !sample.saturated);
    let mut result = [0; 7];
    for (i, name) in WORK.iter().enumerate() {
        result[i] = sample
            .counters
            .iter()
            .find(|counter| counter.name == *name)
            .with_context(|| format!("endpoint observation {name}"))?
            .value;
    }
    Ok(result)
}

fn sql_stages(cluster: &nodes::Cluster) -> Result<u64> {
    let mut started = 0;
    for host in &cluster.managed.hosts[..INGRESS] {
        let sample = host.performance();
        ensure!(sample.available && !sample.saturated);
        started += sample
            .operations
            .iter()
            .find(|operation| operation.name == "sql_stage")
            .context("endpoint SQL stage observation")?
            .started;
    }
    Ok(started)
}

fn options(sequence: u64) -> Result<CommandOptions> {
    let retry = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?
    .checked_add(60_000)
    .context("endpoint command retry horizon")?;
    Ok(CommandOptions {
        durability: Durability::S3,
        timeout: TIMEOUT,
        ..CommandOptions::new(CommandId::new(ClientId::new([233; 16])?, sequence)?, retry)
    })
}

struct Metadata(Arc<fault::Store>);

impl Drop for Metadata {
    fn drop(&mut self) {
        *self.0.denied_prefix.lock().unwrap() = None;
    }
}

async fn lead_on_zero(cluster: &nodes::Cluster) -> Result<()> {
    let current = cluster.leader().await?;
    if current != 0 {
        let group = cluster.managed.hosts[current].group(cluster.managed.history)?;
        let prior = group.observe(TIMEOUT).await?;
        group
            .transfer_leadership(
                LeadershipTransfer {
                    target: MemberId::new(1)?,
                    expected_term: prior.status.term,
                    expected_revision: prior.membership_revision,
                },
                TIMEOUT,
            )
            .await?;
    }
    tokio::time::timeout(TIMEOUT, async {
        loop {
            let status = cluster.managed.hosts[0]
                .group(cluster.managed.history)?
                .status()
                .await?;
            if status.ready && status.leader == Some(1) {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .context("endpoint fixture leader on original member")??;
    Ok(())
}

fn request(
    cluster: &nodes::Cluster,
    client: &reqwest::Client,
    axum: bool,
    command: &CommandOptions,
) -> Result<reqwest::RequestBuilder> {
    Ok(client
        .post(format!(
            "{}?delta=1",
            cluster.web()?.url(INGRESS, axum, "one")
        ))
        .header(
            "x-command-id",
            format!(
                "{}:{}",
                hex::encode(command.id.client.bytes()),
                command.id.sequence()
            ),
        )
        .header("x-retry-until-ms", command.retry_until_ms)
        .header("x-write-durability", "s3"))
}

fn http_receipt(reply: &serde_json::Value, receipt: &CommandReceipt, value: i64) -> Result<()> {
    http::value(reply, "one", value)?;
    ensure!(
        reply["durability"] == "s3"
            && reply["sequence"] == receipt.version.sequence.get()
            && reply["index"] == receipt.witness.position.index.get()
            && reply["term"] == receipt.witness.position.term.get()
            && reply["group"] == hex::encode(receipt.witness.group.group.bytes())
            && reply["group_incarnation"] == hex::encode(receipt.witness.group.incarnation.bytes())
    );
    Ok(())
}

pub(super) async fn exercise(
    cluster: &mut nodes::Cluster,
    callbacks: &AtomicUsize,
    rooted: bool,
) -> Result<()> {
    cluster.authorize_commands()?;
    cluster.authorize_ingress_reads()?;
    cluster.bind()?;
    lead_on_zero(cluster).await?;
    let group = cluster.managed.history;
    let logical = counter::history(group.cluster, group.namespace, "one")?;
    cluster.managed.hosts[0]
        .cell(logical.clone())?
        .enroll(
            CellEnrollmentRequest {
                creation: ProposalId::new([232; 16])?,
                durability: Durability::S3,
            },
            TIMEOUT,
        )
        .await?;
    let cell = cluster.managed.hosts[INGRESS].cell(logical.clone())?;
    let increment = cell.contract("Increment")?;
    let mut selected = options(1)?;
    let mut latest = cell
        .command(&increment, 1_i64.to_be_bytes().to_vec(), selected.clone())
        .await?;
    ensure!(latest.result == 1_i64.to_be_bytes() && latest.durability == Durability::S3);
    let original = latest.clone();
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
    let suffix = format!("{}/", hex::encode(group.namespace.bytes()));
    let prefix = namespace
        .data_prefix()
        .strip_suffix(suffix.as_str())
        .context("managed namespace data prefix")?;
    for (ordinal, (caller, adapter)) in
        [("Sdk", None), ("Native", Some(false)), ("Axum", Some(true))]
            .into_iter()
            .enumerate()
    {
        lead_on_zero(cluster).await?;
        let warmed = cell
            .command(&increment, 1_i64.to_be_bytes().to_vec(), selected.clone())
            .await?;
        ensure!(
            warmed.id == latest.id
                && warmed.result == latest.result
                && warmed.durability == Durability::S3
        );
        let before_callbacks = callbacks.load(Ordering::Acquire);
        ensure!(before_callbacks == ordinal + 1);
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
            .group(group)?
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
        // Validate the actual existing object before using its exact path as a fault cut.
        tokio::time::timeout(TIMEOUT, async { store.get(&path).await?.bytes().await })
            .await
            .context("existing endpoint object within original metadata bound")??;
        let _metadata = Metadata(store.clone());
        let denied = store.denied_prefix_reads.load(Ordering::Acquire);
        *store.denied_prefix.lock().unwrap() = Some(path);
        tokio::time::timeout(TIMEOUT, async {
            while store.denied_prefix_reads.load(Ordering::Acquire) == denied {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .context("ingress endpoint reads denied before new boot")?;
        cluster.restart_voter(0, 140 + ordinal as u8).await?;
        lead_on_zero(cluster).await?;
        let current = cluster.managed.peers[0].clone();
        ensure!(
            old.node == current.node && old.disk == current.disk && old.process != current.process
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
        .context("ingress installs new boot trust while its endpoint stays denied")??;
        let refused = cluster.managed.hosts[INGRESS]
            .inspect_peer_node(old.node, cut.revision(), TIMEOUT)
            .await;
        ensure!(
            matches!(
                refused,
                Err(InspectionError::Refused | InspectionError::StalePeer)
            ),
            "old process inspection={refused:?}"
        );
        ensure!(callbacks.load(Ordering::Acquire) == before_callbacks);
        let before = work(cluster)?;
        let before_stages = sql_stages(cluster)?;
        selected = options(ordinal as u64 + 2)?;
        let deadline = tokio::time::Instant::now() + TIMEOUT;
        enum Reply {
            Sdk(Box<CommandReceipt>),
            Http(serde_json::Value),
        }
        let pending = async {
            match adapter {
                Some(axum) => Ok::<_, anyhow::Error>(Reply::Http(
                    http::Web::send(
                        request(cluster, &client, axum, &selected)?,
                        format!("endpoint command {caller}"),
                    )
                    .await?
                    .success()?,
                )),
                None => Ok(Reply::Sdk(Box::new(
                    cell.command(&increment, 1_i64.to_be_bytes().to_vec(), selected.clone())
                        .await?,
                ))),
            }
        };
        let release = async {
            tokio::time::timeout_at(deadline, async {
                while work(cluster)?[4] == before[4] {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                ensure!(
                    callbacks.load(Ordering::Acquire) == before_callbacks
                        && sql_stages(cluster)? == before_stages
                );
                *store.denied_prefix.lock().unwrap() = None;
                Ok::<_, anyhow::Error>(before_stages)
            })
            .await
            .context("new command selects endpoint repair within original deadline")?
        };
        let (result, released) = tokio::join!(pending, release);
        let result = result.with_context(|| {
            format!(
                "endpoint {caller} command {:?}; metadata release={released:?}; routing before={before:?}, after={:?}",
                selected.id, work(cluster)
            )
        })?;
        let release_stages = released.with_context(|| {
            format!(
                "endpoint {caller} command completed without observed metadata demand; routing before={before:?}, after={:?}",
                work(cluster)
            )
        })?;
        ensure!(tokio::time::Instant::now() < deadline);
        let after = work(cluster)?;
        let delta: Vec<_> = after.iter().zip(before).map(|(a, b)| a - b).collect();
        ensure!(
            delta[0] == 1
                && delta[1] > 0
                && delta[1] <= 4
                && delta[2] == 0
                && delta[3] == 0
                && (1..=4).contains(&delta[4])
                && (1..=4).contains(&delta[5])
        );
        let (receipt, reply) = match result {
            Reply::Sdk(receipt) => (*receipt, None),
            Reply::Http(reply) => (
                cell.command(&increment, 1_i64.to_be_bytes().to_vec(), selected.clone())
                    .await?,
                Some(reply),
            ),
        };
        ensure!(
            receipt.id == selected.id
                && receipt.result == (ordinal as i64 + 2).to_be_bytes()
                && receipt.durability == Durability::S3
                && receipt.witness.group == group
                && matches!(receipt.version.covers(&original.version), Ok(true))
                && callbacks.load(Ordering::Acquire) == before_callbacks + 1
        );
        if let Some(reply) = &reply {
            http_receipt(reply, &receipt, ordinal as i64 + 2)?;
        }
        let replay = cell
            .command(&increment, 1_i64.to_be_bytes().to_vec(), selected.clone())
            .await?;
        ensure!(
            replay.id == receipt.id
                && replay.result == receipt.result
                && replay.version == receipt.version
                && replay.witness == receipt.witness
                && replay.durability == Durability::S3
        );
        if let Some(axum) = adapter {
            let replay = http::Web::send(
                request(cluster, &client, axum, &selected)?,
                format!("endpoint exact replay {caller}"),
            )
            .await?
            .success()?;
            http_receipt(&replay, &receipt, ordinal as i64 + 2)?;
        }
        ensure!(callbacks.load(Ordering::Acquire) == before_callbacks + 1);
        let read = cell
            .query(
                ProposalId::new([150 + ordinal as u8; 16])?,
                &cell.contract("Get")?,
                vec![],
                QueryOptions {
                    consistency: QueryConsistency::AtLeast(receipt.version_token.clone()),
                    timeout: TIMEOUT,
                    ..Default::default()
                },
            )
            .await?;
        ensure!(read.result == receipt.result && read.witness == receipt.witness);
        let after_view = cluster.managed.fixtures[0]
            .domain
            .run(cluster.managed.directory.observe())
            .await??;
        let after_route = cluster.managed.fixtures[0]
            .domain
            .run(cluster.managed.directory.lookup(&after_view, &logical.cell))
            .await??;
        ensure!(after_view.digest() == before_view.digest() && after_route == before_route);
        let configuration = cluster.managed.hosts[0]
            .group(group)?
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
            "ENDPOINT_ROUTING_EVIDENCE {}",
            serde_json::json!({
                "schema": 1, "case": "endpoint_refresh_keeps_membership", "caller": caller, "extents": rooted, "event": "verified",
                "old_peer": format!("{old:?}"), "current_peer": format!("{current:?}"), "membership_revision": format!("{revision:?}"),
                "node": hex::encode(current.node.bytes()), "disk": hex::encode(current.disk.bytes()),
                "old_process": hex::encode(old.process.bytes()), "current_process": hex::encode(current.process.bytes()),
                "directory_before": hex::encode(before_view.digest()), "directory_after": hex::encode(after_view.digest()),
                "sql_stages_before": before_stages, "sql_stages_at_release": release_stages,
                "route": format!("{after_route:?}"), "routing_before": before, "routing_after": after,
                "command": format!("{:?}", selected.id), "retry_until_ms": selected.retry_until_ms, "required": "S3", "achieved": "S3",
                "result_be": hex::encode(&receipt.result), "version": format!("{:?}", receipt.version), "witness": format!("{:?}", receipt.witness),
                "callbacks_before": before_callbacks, "callbacks_after": callbacks.load(Ordering::Acquire), "http": reply,
            })
        );
        latest = receipt;
    }
    Ok(())
}
