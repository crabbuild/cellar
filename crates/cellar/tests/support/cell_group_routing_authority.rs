// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Cached advice cannot replace a current namespace command-origin grant.
use super::*;
use cellar::application::consensus::verification::TransportError;

#[derive(Clone, Copy, Debug)]
enum Caller {
    Sdk,
    Native,
    Axum,
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
            .context("SQL stage observation")?
            .started;
    }
    Ok(started)
}

fn outcomes(host: &RaftCells) -> Result<[u64; 2]> {
    let sample = host.performance();
    ensure!(sample.available && !sample.saturated);
    let mut values = [0; 2];
    for (index, name) in [
        "cell_command_pre_admission_refusals",
        "cell_command_unconfirmed_replies",
    ]
    .into_iter()
    .enumerate()
    {
        values[index] = sample
            .counters
            .iter()
            .find(|counter| counter.name == name)
            .with_context(|| format!("mutation outcome observation {name}"))?
            .value;
    }
    Ok(values)
}

fn options(
    client: ClientId,
    sequence: u64,
    retry: u64,
    durability: Durability,
) -> Result<CommandOptions> {
    Ok(CommandOptions {
        durability,
        timeout: TIMEOUT,
        ..CommandOptions::new(CommandId::new(client, sequence)?, retry)
    })
}

fn request(
    cluster: &nodes::Cluster,
    client: &reqwest::Client,
    caller: Caller,
    command: &CommandOptions,
) -> Result<reqwest::RequestBuilder> {
    let url = cluster
        .web()?
        .url(INGRESS, matches!(caller, Caller::Axum), "one");
    Ok(client
        .post(format!("{url}?delta=1"))
        .header(
            "x-command-id",
            format!(
                "{}:{}",
                hex::encode(command.id.client.bytes()),
                command.id.sequence()
            ),
        )
        .header("x-retry-until-ms", command.retry_until_ms)
        .header(
            "x-write-durability",
            match command.durability {
                Durability::Quorum => "quorum",
                Durability::S3 => "s3",
            },
        ))
}

async fn warm(
    cluster: &nodes::Cluster,
    cell: &RaftCellHandle,
    client: &reqwest::Client,
    caller: Caller,
    command: CommandOptions,
) -> Result<()> {
    let before = sample(cluster)?;
    match caller {
        Caller::Sdk => {
            let receipt = cell
                .command(
                    &cell.contract("Increment")?,
                    1_i64.to_be_bytes().to_vec(),
                    command.clone(),
                )
                .await?;
            ensure!(receipt.id == command.id && receipt.result == 1_i64.to_be_bytes());
        }
        Caller::Native | Caller::Axum => {
            let receipt = http::Web::send(
                request(cluster, client, caller, &command)?,
                format!("authority warm caller={caller:?}"),
            )
            .await?
            .success()?;
            http::value(&receipt, "one", 1)?;
        }
    }
    direct(cluster, before, "authority cached original receipt")
}

pub(super) async fn exercise(
    cluster: &nodes::Cluster,
    callbacks: &AtomicUsize,
    rooted: bool,
) -> Result<()> {
    cluster.authorize_commands()?;
    cluster.authorize_ingress_reads()?;
    cluster.bind()?;
    let group = cluster.managed.history;
    let history = counter::history(group.cluster, group.namespace, "one")?;
    let leader = cluster.leader().await?;
    cluster.managed.hosts[leader]
        .cell(history.clone())?
        .enroll(
            CellEnrollmentRequest {
                creation: ProposalId::new([232; 16])?,
                durability: Durability::Quorum,
            },
            TIMEOUT,
        )
        .await?;
    let ingress = &cluster.managed.hosts[INGRESS];
    let cell = ingress.cell(history.clone())?;
    let client_id = ClientId::new([231; 16])?;
    let now = cluster.managed.fixtures[0]
        .domain
        .run(async { cellar::asyncrt::wall_ms() })
        .await?;
    let retry = u64::try_from(now)?
        .checked_add(60_000)
        .context("authority retry horizon")?;
    let original_options = options(client_id, 1, retry, Durability::Quorum)?;
    let original = cell
        .command(
            &cell.contract("Increment")?,
            1_i64.to_be_bytes().to_vec(),
            original_options.clone(),
        )
        .await?;
    ensure!(original.result == 1_i64.to_be_bytes() && callbacks.load(Ordering::Acquire) == 1);
    let mut minimum = original.version_token.clone();
    let http_client = reqwest::Client::builder()
        .no_proxy()
        .timeout(TIMEOUT + Duration::from_secs(1))
        .build()?;
    let origin = cluster.managed.peers[INGRESS].clone();

    for (ordinal, caller) in [Caller::Sdk, Caller::Native, Caller::Axum]
        .into_iter()
        .enumerate()
    {
        let expected_before = ordinal + 1;
        warm(
            cluster,
            &cell,
            &http_client,
            caller,
            original_options.clone(),
        )
        .await?;
        ensure!(callbacks.load(Ordering::Acquire) == expected_before);
        let revision = 2 + ordinal as u64 * 2;
        for host in &cluster.managed.hosts[..INGRESS] {
            host.revoke_command_origin(group.cluster, group.namespace, origin.node, revision)?;
        }
        let pending = options(client_id, 2 + ordinal as u64, retry, Durability::Quorum)?;
        let before = sample(cluster)?;
        let stages_before = sql_stages(cluster)?;
        let outcomes_before = outcomes(ingress)?;
        let ambiguous = match caller {
            Caller::Sdk => {
                let refused = cell
                    .command(
                        &cell.contract("Increment")?,
                        1_i64.to_be_bytes().to_vec(),
                        pending.clone(),
                    )
                    .await
                    .err()
                    .context("revoked SDK origin must refuse")?;
                ensure!(refused.command == pending.id && refused.ambiguous);
                Some(refused.ambiguous)
            }
            Caller::Native | Caller::Axum => {
                let expected = format!(
                    "{:?}; cause={:?}",
                    CommandFailure::Ambiguous {
                        command: pending.id
                    },
                    CommandFailure::Unavailable
                );
                http::Web::send(
                    request(cluster, &http_client, caller, &pending)?,
                    format!("revoked cached origin caller={caller:?}"),
                )
                .await?
                .refused(503, &expected)?;
                None
            }
        };
        direct(
            cluster,
            before,
            "cached advice cannot authorize revoked origin",
        )?;
        let routing_after = sample(cluster)?;
        let outcomes_after = outcomes(ingress)?;
        let stages_after = sql_stages(cluster)?;
        let callbacks_after_refusal = callbacks.load(Ordering::Acquire);
        ensure!(
            callbacks_after_refusal == expected_before && stages_after == stages_before,
            "revoked origin entered SQL staging or the application callback"
        );

        // A retained read grant is independent. Observe the actual cell while
        // write permission is absent, without restoring the revoked grant.
        let visible = cell
            .query(
                ProposalId::new([233 + ordinal as u8; 16])?,
                &cell.contract("Get")?,
                vec![],
                QueryOptions {
                    consistency: QueryConsistency::AtLeast(minimum.clone()),
                    timeout: TIMEOUT,
                    ..Default::default()
                },
            )
            .await?;
        ensure!(visible.result == (expected_before as i64).to_be_bytes());

        for host in &cluster.managed.hosts[..INGRESS] {
            let mut grant = NamespaceCommandOrigin {
                cluster: group.cluster,
                namespace: group.namespace,
                revision: revision - 1,
                peer: origin.clone(),
            };
            ensure!(
                matches!(
                    host.grant_command_origin(grant.clone()),
                    Err(CellDispatchError::Transport(TransportError::StalePeer))
                ),
                "stale grant crossed revocation tombstone"
            );
            grant.revision = revision + 1;
            grant.peer.process = ProcessGeneration::new([250; 16])?;
            // Either installed trust boundary can reject this unverified boot.
            ensure!(
                matches!(
                    host.grant_command_origin(grant),
                    Err(CellDispatchError::Transport(
                        TransportError::Refused | TransportError::StalePeer
                    ))
                ),
                "unverified process obtained an origin grant"
            );
            host.grant_command_origin(NamespaceCommandOrigin {
                cluster: group.cluster,
                namespace: group.namespace,
                revision: revision + 1,
                peer: origin.clone(),
            })?;
        }
        // The refused identity is reused exactly; stronger S3 acknowledgement
        // must contain this command's receipt and the current covering proof.
        let restored = cell
            .command(
                &cell.contract("Increment")?,
                1_i64.to_be_bytes().to_vec(),
                CommandOptions {
                    durability: Durability::S3,
                    ..pending.clone()
                },
            )
            .await?;
        ensure!(
            restored.id == pending.id
                && restored.result == (expected_before as i64 + 1).to_be_bytes()
                && restored.durability == Durability::S3
        );
        ensure!(
            matches!(restored.version.covers(&original.version), Ok(true))
                && callbacks.load(Ordering::Acquire) == expected_before + 1
        );
        minimum = restored.version_token.clone();
        let replay = cell
            .command(
                &cell.contract("Increment")?,
                1_i64.to_be_bytes().to_vec(),
                CommandOptions {
                    durability: Durability::S3,
                    ..pending.clone()
                },
            )
            .await?;
        ensure!(
            replay.id == restored.id
                && replay.result == restored.result
                && replay.version == restored.version
                && replay.witness == restored.witness
                && replay.durability == Durability::S3
        );
        ensure!(callbacks.load(Ordering::Acquire) == expected_before + 1);
        let http_replay = match caller {
            Caller::Sdk => None,
            Caller::Native | Caller::Axum => {
                let receipt = http::Web::send(
                    request(
                        cluster,
                        &http_client,
                        caller,
                        &CommandOptions {
                            durability: Durability::S3,
                            ..pending.clone()
                        },
                    )?,
                    format!("restored exact S3 replay caller={caller:?}"),
                )
                .await?
                .success()?;
                http::value(&receipt, "one", expected_before as i64 + 1)?;
                ensure!(
                    receipt["durability"] == "s3"
                        && receipt["sequence"] == restored.version.sequence.get()
                        && receipt["index"] == restored.witness.position.index.get()
                        && receipt["term"] == restored.witness.position.term.get()
                        && receipt["group"] == hex::encode(restored.witness.group.group.bytes())
                        && receipt["group_incarnation"]
                            == hex::encode(restored.witness.group.incarnation.bytes())
                );
                Some(receipt)
            }
        };
        ensure!(callbacks.load(Ordering::Acquire) == expected_before + 1);
        eprintln!(
            "AUTHORITY_ROUTING_EVIDENCE {}",
            serde_json::json!({
                "schema": 1, "case": "cached_advice_respects_origin_revocation", "caller": format!("{caller:?}"), "extents": rooted, "event": "verified",
                "group": format!("{group:?}"), "origin": format!("{:?}", ingress.identity()), "revocation_revision": revision,
                "command": format!("{:?}", pending.id), "sdk_ambiguous": ambiguous, "ambiguous": true,
                "routing_before": before, "routing_after_refusal": routing_after,
                "proven_rejections": outcomes_after[0] - outcomes_before[0],
                "unconfirmed_replies": outcomes_after[1] - outcomes_before[1],
                "callbacks_before": expected_before, "callbacks_after": callbacks.load(Ordering::Acquire),
                "callbacks_after_refusal": callbacks_after_refusal,
                "sql_stages_before": stages_before, "sql_stages_after_refusal": stages_after,
                "required": "S3", "achieved": format!("{:?}", restored.durability), "result_be": hex::encode(&restored.result),
                "version": format!("{:?}", restored.version), "witness": format!("{:?}", restored.witness),
                "http_replay": http_replay,
                "ingress_ordinary": ingress.resource_usage().host.controller.ordinary,
                "ingress_control": ingress.resource_usage().control,
            })
        );
    }
    ensure!(
        ingress.resource_usage().host.controller.groups == 0
            && callbacks.load(Ordering::Acquire) == 4
    );
    for (index, host) in cluster.managed.hosts[..INGRESS].iter().enumerate() {
        let local = host.cell(history.clone())?;
        let read = local
            .query(
                ProposalId::new([236 + index as u8; 16])?,
                &local.contract("Get")?,
                vec![],
                QueryOptions {
                    consistency: QueryConsistency::AtLeast(minimum.clone()),
                    routing: QueryRouting::Local,
                    timeout: TIMEOUT,
                    ..Default::default()
                },
            )
            .await?;
        ensure!(read.result == 4_i64.to_be_bytes());
    }
    Ok(())
}
