// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Current authenticated advice cannot reinstall an actually removed voter.
use super::*;
use cellar::application::consensus::NamespaceQueryOrigin;

fn stages(cluster: &ManagedCluster) -> Result<u64> {
    let mut started = 0;
    for host in &cluster.hosts {
        let sample = host.performance();
        ensure!(sample.available && !sample.saturated);
        started += sample
            .operations
            .iter()
            .find(|item| item.name == "sql_stage")
            .context("removed-member SQL stages")?
            .started;
    }
    Ok(started)
}

pub(super) async fn exercise(
    cluster: &ManagedCluster,
    web: &http::Web,
    nodes: &mut [Option<Node>],
    callbacks: &AtomicUsize,
    rooted: bool,
    caller: Caller,
) -> Result<()> {
    let group = cluster.history;
    let source = leader(&cluster.hosts[..3], group).await?;
    let obsolete = (source + 1) % 3;
    let removed_member = MemberId::new(obsolete as u64 + 1)?;
    let logical = counter::history(group.cluster, group.namespace, "one")?;
    cluster.hosts[source]
        .cell(logical.clone())?
        .enroll(
            CellEnrollmentRequest {
                creation: ProposalId::new([245; 16])?,
                durability: Durability::S3,
            },
            TIMEOUT,
        )
        .await?;
    let ingress = &cluster.hosts[INGRESS];
    let cell = ingress.cell(logical.clone())?;
    let increment = cell.contract("Increment")?;
    let original_options = options(1)?;
    let original = cell
        .command(
            &increment,
            1_i64.to_be_bytes().to_vec(),
            original_options.clone(),
        )
        .await?;
    ensure!(
        original.result == 1_i64.to_be_bytes()
            && original.durability == Durability::S3
            && callbacks.load(Ordering::Acquire) == 1
    );
    let directory = &cluster.directory;
    let domain = &cluster.fixtures[0].domain;
    let before_view = domain.run(directory.observe()).await??;
    let before_range = domain
        .run(directory.lookup(&before_view, &logical.cell))
        .await??;
    let previous = cluster.hosts[source]
        .group(group)?
        .quorum_membership(TIMEOUT)
        .await?;
    let previous_voters = previous.configuration().voters.clone();
    let previous_revision = previous.membership().revision();
    ensure!(previous_voters == [1, 2, 3]);
    drop(previous);

    // Reuse the real replacement's original eight turns, settlement and physical
    // inventory proof. A made-up non-member ID cannot satisfy this setup.
    ranges::replace_member_for_advice(cluster, source, obsolete)
        .await
        .context("removed-member replacement and handoff")?;
    let membership = cluster.hosts[4]
        .group(group)?
        .quorum_membership(TIMEOUT)
        .await?;
    let configuration_state = membership.configuration().clone();
    let mut voters = configuration_state.voters.clone();
    voters.sort_unstable();
    let expected: Vec<_> = (1..=5)
        .filter(|member| *member != removed_member.get() && *member != 4)
        .collect();
    let revision = membership.membership().revision();
    let term = membership.status().term;
    let current_leader = membership
        .status()
        .leader
        .context("replacement current leader")?;
    ensure!(
        voters == expected
            && !voters.contains(&removed_member.get())
            && revision > previous_revision
            && membership.configuration().voters_outgoing.is_empty()
            && membership.configuration().learners.is_empty()
            && membership.configuration().learners_next.is_empty()
            && !membership.configuration().auto_leave
            && voters.contains(&current_leader)
            && current_leader == 5
    );
    let leader_index = usize::try_from(current_leader - 1)?;
    drop(membership);
    let issuer = (0..3)
        .find(|index| *index != obsolete && *index != leader_index)
        .context("still-current non-leader issuer")?;
    ensure!(voters.contains(&(issuer as u64 + 1)));
    let cut = cluster.fixtures[0].catalog.read(TIMEOUT).await?;
    inspect_current(ingress, cluster.peers[leader_index].node, cut.revision())
        .await
        .context("current ingress trust before membership inspection")?;
    let authorization = cluster.fixtures[0]
        .catalog
        .authorize_group(&cut, &group, TIMEOUT)
        .await?;
    let observed = ingress
        .observe_peer_fleet_membership(
            cluster.peers[leader_index].node,
            authorization,
            cut.revision(),
            TIMEOUT,
        )
        .await
        .context("current membership after replacement")?;
    ensure!(observed.membership().membership_revision() == revision);
    let configuration = observed.membership().configuration_digest();
    drop(observed);
    inspect_current(
        &cluster.hosts[leader_index],
        cluster.peers[obsolete].node,
        cut.revision(),
    )
    .await?;
    let physical = cluster.hosts[leader_index]
        .inspect_peer_physical_inventory(cluster.peers[obsolete].node, cut.revision(), TIMEOUT)
        .await
        .context("retired replica physical inventory")?;
    let inventory = physical
        .inventory
        .context("removed member physical inventory")?;
    let retained = inventory
        .group_history(&group)
        .context("removed member retained history")?;
    let in_configuration = retained.in_configuration();
    let retained_member = retained.member();
    ensure!(!in_configuration && retained_member == removed_member);

    let before_lookup = callbacks.load(Ordering::Acquire);
    let discover_removed_name = || {
        cluster.hosts[obsolete].resolve_cell_history(
            logical.cluster,
            logical.namespace,
            logical.cell.clone(),
            QueryOptions::default(),
        )
    };
    let denied_name = discover_removed_name().await;
    ensure!(
        denied_name.is_err(),
        "former membership granted namespace read access"
    );
    ensure!(callbacks.load(Ordering::Acquire) == before_lookup);
    for member in &voters {
        let index = usize::try_from(*member - 1)?;
        cluster.hosts[index].grant_query_origin(NamespaceQueryOrigin {
            cluster: group.cluster,
            namespace: group.namespace,
            revision: 1,
            peer: cluster.peers[obsolete].clone(),
        })?;
    }
    let removed_name = discover_removed_name()
        .await
        .context("removed member must discover names through its current ingress route")?;
    ensure!(removed_name.as_ref() == Some(&logical));
    ensure!(callbacks.load(Ordering::Acquire) == before_lookup);

    // Leadership moved to member 5, absent from the original configuration.
    // Public receipt replay and a query explicitly served by that new voter
    // establish current client membership before injecting a role-only fault.
    let replay = cell
        .command(&increment, 1_i64.to_be_bytes().to_vec(), original_options)
        .await?;
    ensure!(
        replay.version == original.version
            && replay.witness == original.witness
            && replay.result == original.result
            && replay.durability == original.durability
            && callbacks.load(Ordering::Acquire) == 1
    );
    let replacement = cell
        .query(
            ProposalId::new([244; 16])?,
            &cell.contract("Get")?,
            vec![],
            QueryOptions {
                consistency: QueryConsistency::Linearizable,
                timeout: TIMEOUT,
                max_attempts: 1,
                routing: QueryRouting::Replicas {
                    members: vec![MemberId::new(5)?],
                    attempt_timeout: TIMEOUT,
                    leader_fallback: false,
                },
                ..Default::default()
            },
        )
        .await?;
    ensure!(
        replacement.served_by == MemberId::new(5)?
            && replacement.result == original.result
            && replacement.version == original.version
            && callbacks.load(Ordering::Acquire) == 1
    );
    let issuer_peer = cluster.peers[issuer].clone();
    let node = nodes
        .get_mut(issuer)
        .and_then(Option::take)
        .context("owned current issuer node")?;
    movement::close(vec![node], std::slice::from_ref(&cluster.hosts[issuer])).await?;
    let server = Server::start(
        issuer_peer.query_address,
        Advice {
            history: group,
            revision: revision.get(),
            configuration,
            term: term.get() + 100,
            issuer: issuer as u64 + 1,
            leader: removed_member.get(),
        },
        original.clone(),
    )
    .await?;
    let result = async {
        seed(&cell, &server, 0).await?;
        let seed_term = {
            let mut state = server.state.lock().map_err(|_| anyhow::anyhow!("wire state poisoned"))?;
            state.fault = Fault::RemovedMember;
            state.advice.term
        };
        let origin = cluster.peers[INGRESS].clone();
        for member in &voters {
            let index = usize::try_from(*member - 1)?;
            if index != issuer {
                cluster.hosts[index].revoke_command_origin(group.cluster, group.namespace, origin.node, 2)?;
            }
        }
        let command = options(2)?;
        let before = ingress_counters(ingress)?;
        let sql_before = stages(cluster)?;
        let callbacks_before = callbacks.load(Ordering::Acquire);
        let rejected = counter(ingress, "cell_command_pre_admission_refusals")?;
        let failure = submit(cell.clone(), command.clone(), command_request_for_web(web, caller, &command)?, None).await?;
        let after = ingress_counters(ingress)?;
        let sql_after = stages(cluster)?;
        let callbacks_after = callbacks.load(Ordering::Acquire);
        let rejected_after = counter(ingress, "cell_command_pre_admission_refusals")?;
        ensure!(callbacks_before == 1 && callbacks_after == callbacks_before && sql_before == sql_after);
        ensure!(after[0] == before[0]+1 && after[2] == before[2] && after[3] == before[3]+1 && after[4] == before[4]+1);
        // The one actual application attempt has a complete, authenticated
        // pre-admission refusal. Bad role advice cannot discard that proof.
        ensure!(rejected_after == rejected+1 && failure["ambiguous"] == false);
        let frames = server.state.lock().map_err(|_| anyhow::anyhow!("wire state poisoned"))?.commands.clone();
        ensure!(frames.len() == 1);
        for member in &voters {
            let index = usize::try_from(*member - 1)?;
            if index != issuer {
                cluster.hosts[index].grant_command_origin(NamespaceCommandOrigin {
                    cluster: group.cluster, namespace: group.namespace, revision: 3, peer: origin.clone(),
                })?;
            }
        }
        let recovered = submit(cell.clone(), command.clone(), command_request_for_web(web, caller, &command)?, Some(2)).await?;
        let receipt = cell.command(&increment, 1_i64.to_be_bytes().to_vec(), command.clone()).await?;
        let replay = cell.command(&increment, 1_i64.to_be_bytes().to_vec(), command.clone()).await?;
        let now = u64::try_from(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_millis())?;
        let keys = Fixture::version_keys()?;
        let authenticated = keys.verify_cell(&receipt.version_token, cell.history(), cluster.fixtures[0].declaration.namespace.schema_digest, now)?;
        let replay_authenticated = keys.verify_cell(&replay.version_token, cell.history(), cluster.fixtures[0].declaration.namespace.schema_digest, now)?;
        ensure!(receipt.id == command.id && receipt.result == 2_i64.to_be_bytes() && receipt.durability == Durability::S3
            && replay.id == receipt.id && replay.result == receipt.result && replay.version == receipt.version
            && replay.witness == receipt.witness && replay.durability == receipt.durability
            && authenticated.version == receipt.version && authenticated.witness == receipt.witness
            && replay_authenticated.version == receipt.version && replay_authenticated.witness == receipt.witness
            && matches!(receipt.version.covers(&original.version), Ok(true))
            && callbacks.load(Ordering::Acquire) == 2);
        let current = cluster.hosts[leader_index].group(group)?.quorum_membership(TIMEOUT).await?;
        ensure!(current.status().term == term && current.status().leader == Some(current_leader)
            && current.membership().revision() == revision && current.configuration() == &configuration_state);
        let after_view = domain.run(directory.observe()).await??;
        let after_range = domain.run(directory.lookup(&after_view, &logical.cell)).await??;
        ensure!(after_view.revision() == before_view.revision() && after_view.digest() == before_view.digest()
            && after_range == before_range);
        let mut evidence = serde_json::Map::new();
        for chunk in [json!({
            "schema":1, "case":"removed_member_advice_cannot_grant_authority", "extents":rooted,
            "ingress":format!("{caller:?}"), "issuer":peer_record(&issuer_peer), "origin":peer_record(&origin),
            "removed_peer":peer_record(&cluster.peers[obsolete]), "removed_member":removed_member.get(),
            "previous_voters":previous_voters, "current_voters":voters,
            "current_voters_outgoing":configuration_state.voters_outgoing, "current_learners":configuration_state.learners,
            "current_learners_next":configuration_state.learners_next, "current_auto_leave":configuration_state.auto_leave,
            "previous_membership_revision":previous_revision.get(), "membership_revision":revision.get(),
            "removed_replica_in_configuration":in_configuration, "removed_replica_member":retained_member.get(),
            "removed_name_resolved":removed_name.as_ref() == Some(&logical),
            "removed_name_without_grant_refused":denied_name.is_err(),
            "configuration_digest":hex::encode(configuration), "term":term.get(),
            "seed_term":seed_term, "seed_member":issuer as u64+1, "fault":"RemovedMember",
        }), json!({
            "command":{"client":hex::encode(command.id.client.bytes()),"sequence":command.id.sequence()},
            "request_timeout_ms":TIMEOUT.as_millis(), "retry_until_ms":command.retry_until_ms,
            "failure":failure, "wire_requests":frames, "routing_before":before, "routing_after_refusal":after,
            "callbacks_before":callbacks_before, "callbacks_after_refusal":callbacks_after, "callbacks_after_replay":callbacks.load(Ordering::Acquire),
            "sql_stages_before":sql_before, "sql_stages_after_refusal":sql_after,
            "proven_rejections_before":rejected, "proven_rejections_after_refusal":rejected_after,
            "directory_before":hex::encode(before_view.digest()), "directory_after":hex::encode(after_view.digest()),
        }), json!({
            "required_durability":"S3", "achieved_durability":format!("{:?}",receipt.durability), "result_be":hex::encode(&receipt.result),
            "replacement_leader":current_leader, "replacement_served_by":replacement.served_by.get(),
            "replacement_result_be":hex::encode(&replacement.result), "replacement_version":format!("{:?}",replacement.version),
            "original_version":format!("{:?}",original.version),
            "replacement_token":replacement.version_token.as_str(),
            "version":format!("{:?}",receipt.version), "witness":format!("{:?}",receipt.witness), "token":receipt.version_token.as_str(),
            "token_observation_version":format!("{:?}",authenticated.version), "token_observation_witness":format!("{:?}",authenticated.witness),
            "replay_token":replay.version_token.as_str(), "replay_token_observation_version":format!("{:?}",replay_authenticated.version),
            "replay_token_observation_witness":format!("{:?}",replay_authenticated.witness),
            "recovered":recovered, "term_after":current.status().term.get(), "membership_revision_after":current.membership().revision().get(),
        })] {
            let serde_json::Value::Object(fields) = chunk else { anyhow::bail!("fixed removed-member evidence object"); };
            evidence.extend(fields);
        }
        cellar::note!("WIRE_REMOVED_AUTHORITY_EVIDENCE {}", serde_json::Value::Object(evidence));
        Ok::<_,anyhow::Error>(())
    }.await;
    let joined = server.close().await;
    result.with_context(|| format!("removed-member wire authority; server close={joined:?}"))?;
    joined
}
