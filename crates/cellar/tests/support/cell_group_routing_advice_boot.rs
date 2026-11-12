// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! An actual issuer restart fences its held, authenticated old-boot reply.
use super::*;

fn boot_record(boot: &FleetBootBinding) -> serde_json::Value {
    json!({"catalog": hex::encode(boot.catalog), "fleet": hex::encode(boot.fleet),
        "node": hex::encode(boot.node), "disk": hex::encode(boot.disk),
        "engine": hex::encode(boot.engine), "boot": hex::encode(boot.boot),
        "process": hex::encode(boot.process), "epoch": boot.epoch,
        "join_key_sha256": hex::encode(sha2::Sha256::digest(boot.join_key))})
}

fn stages(cluster: &nodes::Cluster, replaced: usize) -> Result<u64> {
    let mut stages = 0;
    // The replaced host has a new registry. Compare only unchanged processes;
    // the application callback separately covers every process generation.
    for (index, host) in cluster.managed.hosts[..INGRESS].iter().enumerate() {
        if index == replaced {
            continue;
        }
        let sample = host.performance();
        ensure!(sample.available && !sample.saturated);
        stages += sample
            .operations
            .iter()
            .find(|item| item.name == "sql_stage")
            .context("old-boot SQL stages")?
            .started;
    }
    Ok(stages)
}

fn routing(ingress: &RaftCells) -> Result<[u64; 6]> {
    let mut values = [0; 6];
    for (index, name) in COUNTERS.iter().enumerate() {
        values[index] = counter(ingress, name)?;
    }
    Ok(values)
}

pub(super) async fn exercise(cluster: &mut nodes::Cluster, drill: &BootDrill<'_>) -> Result<()> {
    let group = cluster.managed.history;
    let old = cluster.managed.peers[drill.issuer].clone();
    let origin = cluster.managed.peers[INGRESS].clone();
    // Prime the old member at its real pre-transfer term, so a later genuine
    // owner observation can warm the new cache without an invented term floor.
    seed_at(drill.cell, drill.server, 0, Some(drill.old_term.get())).await?;
    for (index, host) in cluster.managed.hosts[..INGRESS].iter().enumerate() {
        if index != drill.issuer {
            host.revoke_command_origin(group.cluster, group.namespace, origin.node, 2)?;
        }
    }
    let held = Arc::new(HeldReply::default());
    {
        let mut state = drill
            .server
            .state
            .lock()
            .map_err(|_| anyhow::anyhow!("wire state poisoned"))?;
        // A legal current member and a higher term would poison an unguarded
        // cache. Only this reply's actual superseded physical issuer fences it.
        state.advice.term = drill.term.get() + 100;
        state.advice.leader = drill.issuer as u64 + 1;
        state.held = Some(held.clone());
    }
    let command = options(2)?;
    let request = command_request(cluster, drill.caller, &command)?;
    let ingress = cluster.managed.hosts[INGRESS].clone();
    let before = routing(&ingress)?;
    let callbacks = drill.callbacks.load(Ordering::Acquire);
    let sql = stages(cluster, drill.issuer)?;
    let rejected = counter(&ingress, "cell_command_pre_admission_refusals")?;
    ensure!(callbacks == 1);
    let started = tokio::time::Instant::now();
    let deadline = started + TIMEOUT;
    let pending = submit(drill.cell.clone(), command.clone(), request, None);
    let change = async {
        tokio::time::timeout_at(deadline, async {
            held.entered.notified().await;
            ensure!(held.prefix_bytes.load(Ordering::Acquire) > 0);
            ensure!(drill.callbacks.load(Ordering::Acquire) == callbacks);
            // The replacement must remain unable to execute remote commands
            // throughout startup, including before it becomes inspectable.
            cluster
                .restart_voter_without_command_grants(drill.issuer, 190)
                .await?;
            let current = cluster.managed.peers[drill.issuer].clone();
            ensure!(
                old.node == current.node
                    && old.disk == current.disk
                    && old.process != current.process
            );
            let cut = cluster.managed.fixtures[0].catalog.read(TIMEOUT).await?;
            inspect_current(
                &cluster.managed.hosts[INGRESS],
                current.node,
                cut.revision(),
            )
            .await?;
            let inspected = cluster.managed.hosts[INGRESS]
                .inspect_peer_node(current.node, cut.revision(), TIMEOUT)
                .await?;
            ensure!(inspected
                .view
                .fleet_trust_revision
                .is_some_and(|revision| revision >= cut.revision()));
            ensure!(
                inspected.view.node.identity.node == current.node
                    && inspected.view.node.identity.disk == current.disk
                    && inspected.view.node.identity.process == current.process
            );
            let boot = inspected
                .view
                .fleet_boot
                .context("inspected current physical boot")?;
            let previous = &drill.old_boot;
            ensure!(
                boot.catalog == previous.catalog
                    && boot.fleet == previous.fleet
                    && boot.node == previous.node
                    && boot.disk == previous.disk
                    && boot.engine == previous.engine
                    && boot.epoch > previous.epoch
                    && boot.boot != previous.boot
                    && boot.process != previous.process
                    && boot.join_key != previous.join_key
            );
            let read = drill
                .cell
                .query(
                    ProposalId::new([246; 16])?,
                    &drill.cell.contract("Get")?,
                    vec![],
                    QueryOptions {
                        consistency: QueryConsistency::Linearizable,
                        timeout: TIMEOUT,
                        max_attempts: 1,
                        routing: QueryRouting::Replicas {
                            members: vec![MemberId::new(drill.leader as u64 + 1)?],
                            attempt_timeout: TIMEOUT,
                            leader_fallback: false,
                        },
                        ..Default::default()
                    },
                )
                .await?;
            ensure!(
                read.served_by == MemberId::new(drill.leader as u64 + 1)?
                    && read.result == 1_i64.to_be_bytes()
            );
            ensure!(
                drill.callbacks.load(Ordering::Acquire) == callbacks
                    && stages(cluster, drill.issuer)? == sql
            );
            // The current boot is installed and a genuine owner has replied.
            // Record whether the old tail is delivered or its socket is fenced.
            held.release.notify_one();
            Ok::<_, anyhow::Error>((current, boot, cut.revision()))
        })
        .await
        .context("actual boot change inside original command deadline")?
    };
    let (failure, changed) = tokio::join!(pending, change);
    held.release.notify_one();
    let (current, current_boot, trust_revision) = changed?;
    let failure = failure?;
    let elapsed = started.elapsed();
    let callbacks_refused = drill.callbacks.load(Ordering::Acquire);
    let sql_refused = stages(cluster, drill.issuer)?;
    ensure!(elapsed < TIMEOUT && failure["ambiguous"] == true);
    ensure!(callbacks_refused == callbacks && sql_refused == sql);
    tokio::time::timeout(TIMEOUT, async {
        loop {
            if held
                .result
                .lock()
                .map_err(|_| anyhow::anyhow!("held reply poisoned"))?
                .is_some()
            {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .context("old reply writer completion")??;
    let rejected_after = counter(&ingress, "cell_command_pre_admission_refusals")?;
    ensure!(rejected_after == rejected);
    let after = routing(&ingress)?;
    ensure!(after[0] > before[0] && after[4] > before[4]);
    for host in &cluster.managed.hosts[..INGRESS] {
        host.grant_command_origin(NamespaceCommandOrigin {
            cluster: group.cluster,
            namespace: group.namespace,
            revision: 3,
            peer: origin.clone(),
        })?;
    }
    let before_recovery = routing(&ingress)?;
    let recovered = call(cluster, drill.caller, drill.cell, &command, Some(2)).await?;
    let after_recovery = routing(&ingress)?;
    ensure!(
        after_recovery[..5]
            .iter()
            .zip(before_recovery)
            .map(|(a, b)| a - b)
            .collect::<Vec<_>>()
            == [1, 0, 0, 0, 1],
        "old-boot completion changed the first recovered command route"
    );
    let before_replay = counter(&ingress, "cell_command_rpc_attempts")?;
    let replay_before = routing(&ingress)?;
    let probes = counter(&ingress, "cell_command_leader_probes")?;
    let directories = counter(&ingress, "cell_command_directory_lookups")?;
    let membership = counter(&ingress, "cell_membership_demand_refreshes")?;
    let hits = counter(&ingress, "cell_leader_advice_hits")?;
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
            && receipt.result == 2_i64.to_be_bytes()
            && receipt.durability == Durability::S3
    );
    ensure!(drill.callbacks.load(Ordering::Acquire) == callbacks + 1);
    let replay_after = routing(&ingress)?;
    ensure!(
        counter(&ingress, "cell_command_rpc_attempts")? == before_replay + 1
            && counter(&ingress, "cell_command_leader_probes")? == probes
            && counter(&ingress, "cell_command_directory_lookups")? == directories
            && counter(&ingress, "cell_membership_demand_refreshes")? == membership
            && counter(&ingress, "cell_leader_advice_hits")? == hits + 1,
        "the old reply changed the warm current route"
    );
    let observation = cluster.managed.hosts[drill.leader]
        .group(group)?
        .observe(TIMEOUT)
        .await?;
    ensure!(
        observation.status.term == drill.term
            && observation.status.leader == Some(drill.leader as u64 + 1)
            && observation.membership_revision == drill.revision
    );
    let old_wire = drill
        .server
        .state
        .lock()
        .map_err(|_| anyhow::anyhow!("wire state poisoned"))?
        .commands
        .clone();
    ensure!(old_wire.len() == 1, "exact held old-boot command attempt");
    let tail = held
        .result
        .lock()
        .map_err(|_| anyhow::anyhow!("held reply poisoned"))?
        .clone();
    let mut evidence = serde_json::Map::new();
    for chunk in [
        json!({
            "schema": 1, "case": "superseded_boot_advice_cannot_change_route", "ingress": format!("{:?}", drill.caller),
            "extents": drill.rooted, "old_issuer": peer_record(&old), "current_issuer": peer_record(&current),
            "issuer": peer_record(&old), "old_boot": boot_record(&drill.old_boot), "current_boot": boot_record(&current_boot),
            "origin": peer_record(&origin), "trust_revision": trust_revision,
            "command": {"client": hex::encode(command.id.client.bytes()), "sequence": command.id.sequence()},
            "request_timeout_ms": TIMEOUT.as_millis(), "elapsed_ms": elapsed.as_millis(),
            "retry_until_ms": command.retry_until_ms,
            "held_prefix_bytes": held.prefix_bytes.load(Ordering::Acquire), "old_tail_result": tail,
            "wire_requests": old_wire, "failure": failure, "recovered": recovered,
        }),
        json!({
            "routing_before": before, "routing_after_refusal": after,
            "routing_before_recovery": before_recovery, "routing_after_recovery": after_recovery,
            "routing_before_replay": replay_before, "routing_after_replay": replay_after,
            "callbacks_before": callbacks, "callbacks_after_refusal": callbacks_refused,
            "callbacks_after_replay": drill.callbacks.load(Ordering::Acquire),
            "unchanged_process_sql_stages_before": sql, "unchanged_process_sql_stages_after_refusal": sql_refused,
            "sql_stages_before": sql, "sql_stages_after_refusal": sql_refused,
            "proven_rejections_before": rejected, "proven_rejections_after_refusal": rejected_after,
            "required_durability": "S3", "achieved_durability": format!("{:?}", receipt.durability),
        }),
        json!({
            "fault": "Control", "seed_term": drill.term.get() + 100, "seed_member": drill.issuer as u64 + 1,
            "configuration_digest": hex::encode(drill.configuration), "term": observation.status.term.get(),
            "membership_revision": observation.membership_revision.get(),
            "durability": format!("{:?}", receipt.durability), "result_be": hex::encode(&receipt.result),
            "version": format!("{:?}", receipt.version), "witness": format!("{:?}", receipt.witness),
            "token": receipt.version_token.as_str(), "warm_replay_one_rpc": true,
        }),
    ] {
        let serde_json::Value::Object(fields) = chunk else {
            anyhow::bail!("fixed boot evidence object");
        };
        evidence.extend(fields);
    }
    cellar::note!(
        "WIRE_BOOT_AUTHORITY_EVIDENCE {}",
        serde_json::Value::Object(evidence)
    );
    Ok(())
}
