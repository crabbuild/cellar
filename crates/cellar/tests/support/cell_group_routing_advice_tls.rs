// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A certificate outside enrolled trust cannot become routing advice.
use super::*;

fn observation(server: &Server) -> Result<(u64, Vec<String>, usize)> {
    let state = server
        .state
        .lock()
        .map_err(|_| anyhow::anyhow!("wire state poisoned"))?;
    Ok((
        state.untrusted_attempts,
        state.untrusted_rejections.clone(),
        state.commands.len(),
    ))
}

pub(super) async fn exercise(drill: &TlsDrill<'_>) -> Result<()> {
    let cluster = drill.cluster;
    let ingress = &cluster.managed.hosts[INGRESS];
    let origin = cluster.managed.peers[INGRESS].clone();
    let group = cluster.managed.history;
    for (ordinal, caller) in [Caller::Sdk, Caller::Native, Caller::Axum]
        .into_iter()
        .enumerate()
    {
        seed(drill.cell, drill.server, ordinal).await?;
        let revision = 2 + u64::try_from(ordinal)? * 2;
        for (index, host) in cluster.managed.hosts[..INGRESS].iter().enumerate() {
            if index != drill.issuer {
                host.revoke_command_origin(group.cluster, group.namespace, origin.node, revision)?;
            }
        }
        let command = options(u64::try_from(ordinal)? + 2)?;
        let before = sample(cluster)?;
        let stages = authority::sql_stages(cluster)?;
        let callbacks = drill.callbacks.load(Ordering::Acquire);
        let rejected = counter(ingress, "cell_command_pre_admission_refusals")?;
        let (attempts, rejections, commands) = observation(drill.server)?;
        // Fully join the cached trusted connection. The next handshake uses
        // the same enrolled node/disk/process/address but an unknown issuer.
        drill.server.trust(false).await?;
        let deadline = tokio::time::Instant::now() + TIMEOUT;
        let failure =
            tokio::time::timeout_at(deadline, call(cluster, caller, drill.cell, &command, None))
                .await??;
        let after = sample(cluster)?;
        // The client's certificate refusal can complete before the peer reads
        // its TLS alert. Observe that physical result within the same request
        // deadline instead of racing the server's handshake task.
        let (attempts_after, rejections_after, commands_after) =
            tokio::time::timeout_at(deadline, async {
                loop {
                    let observed = observation(drill.server)?;
                    if observed.1.len() > rejections.len() {
                        return Ok::<_, anyhow::Error>(observed);
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .with_context(|| format!("peer did not observe UnknownCA within the original request deadline: rooted={} caller={caller:?}, before=({attempts}, {}), after={:?}, routing={before:?}->{after:?}, failure={failure}", drill.rooted, rejections.len(), observation(drill.server)))??;
        let callbacks_refused = drill.callbacks.load(Ordering::Acquire);
        let stages_refused = authority::sql_stages(cluster)?;
        let rejected_after = counter(ingress, "cell_command_pre_admission_refusals")?;
        ensure!(
            attempts_after > attempts && rejections_after.len() > rejections.len(),
            "the real client must reject the presented certificate with UnknownCA"
        );
        ensure!(
            commands_after == commands,
            "untrusted TLS must not reach RPC decoding"
        );
        ensure!(
            after[0] > before[0] && after[4] > before[4],
            "the warm command route was not exercised"
        );
        ensure!(callbacks_refused == callbacks && stages_refused == stages);
        ensure!(rejected_after == rejected && failure["ambiguous"] == true);
        drill.server.trust(true).await?;
        for (index, host) in cluster.managed.hosts[..INGRESS].iter().enumerate() {
            if index != drill.issuer {
                host.grant_command_origin(NamespaceCommandOrigin {
                    cluster: group.cluster,
                    namespace: group.namespace,
                    revision: revision + 1,
                    peer: origin.clone(),
                })?;
            }
        }
        let expected = i64::try_from(ordinal)? + 2;
        let recovered = call(cluster, caller, drill.cell, &command, Some(expected)).await?;
        let receipt = drill
            .cell
            .command(
                &drill.cell.contract("Increment")?,
                1_i64.to_be_bytes().to_vec(),
                command.clone(),
            )
            .await?;
        ensure!(
            receipt.id == command.id
                && receipt.durability == Durability::S3
                && receipt.result == expected.to_be_bytes()
        );
        ensure!(drill.callbacks.load(Ordering::Acquire) == callbacks + 1);
        let current = cluster.managed.hosts[drill.leader]
            .group(group)?
            .observe(TIMEOUT)
            .await?;
        ensure!(
            current.status.term == drill.term
                && current.status.leader == Some(drill.leader as u64 + 1)
                && current.membership_revision == drill.revision
        );
        cellar::note!(
            "WIRE_TLS_AUTHORITY_EVIDENCE {}",
            json!({
                "schema": 1, "case": "untrusted_tls_advice_cannot_grant_authority",
                "extents": drill.rooted, "ingress": format!("{caller:?}"), "history": format!("{group:?}"),
                "issuer": peer_record(&cluster.managed.peers[drill.issuer]), "origin": peer_record(&origin),
                "untrusted_certificate_sha256": hex::encode(drill.server.untrusted_fingerprint),
                "command": {"client": hex::encode(command.id.client.bytes()), "sequence": command.id.sequence()},
                "request_timeout_ms": TIMEOUT.as_millis(), "retry_until_ms": command.retry_until_ms,
                "failure": failure, "tls_attempts_before": attempts, "tls_attempts_after": attempts_after,
                "certificate_rejections": &rejections_after[rejections.len()..],
                "rpc_requests_before": commands, "rpc_requests_after_refusal": commands_after,
                "routing_before": before, "routing_after_refusal": after,
                "callbacks_before": callbacks, "callbacks_after_refusal": callbacks_refused,
                "callbacks_after_replay": drill.callbacks.load(Ordering::Acquire),
                "sql_stages_before": stages, "sql_stages_after_refusal": stages_refused,
                "proven_rejections_before": rejected, "proven_rejections_after_refusal": rejected_after,
                "required_durability": "S3", "achieved_durability": format!("{:?}", receipt.durability),
                "result_be": hex::encode(&receipt.result), "configuration_digest": hex::encode(drill.configuration),
                "membership_revision": current.membership_revision.get(), "term": current.status.term.get(),
                "version": format!("{:?}", receipt.version), "witness": format!("{:?}", receipt.witness),
                "version_token": receipt.version_token.as_str(), "recovered": recovered,
            })
        );
        drill
            .server
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("wire state poisoned"))?
            .receipt = receipt;
    }
    Ok(())
}
