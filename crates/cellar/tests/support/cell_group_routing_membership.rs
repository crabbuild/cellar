// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A real voter replacement repairs membership without discarding cell placement.
use super::*;
use cellar::application::consensus::verification::TransportError;

fn receipt(value: &CommandReceipt) -> serde_json::Value {
    serde_json::json!({
        "client": hex::encode(value.id.client.bytes()), "sequence": value.id.sequence(),
        "result_be": hex::encode(&value.result), "version": format!("{:?}", value.version),
        "group": hex::encode(value.witness.group.group.bytes()),
        "incarnation": hex::encode(value.witness.group.incarnation.bytes()),
        "index": value.witness.position.index.get(), "term": value.witness.position.term.get(),
        "achieved": format!("{:?}", value.durability), "version_token": value.version_token.as_str(),
    })
}

fn evidence(rooted: bool, caller: Caller, event: &str, mut value: serde_json::Value) {
    value["schema"] = 1.into();
    value["case"] = "membership_refresh_keeps_range".into();
    value["extents"] = rooted.into();
    value["caller"] = format!("{caller:?}").into();
    value["event"] = event.into();
    eprintln!("MEMBERSHIP_ROUTING_EVIDENCE {value}");
}

struct Metadata<'a>(&'a fault::Store);

impl Drop for Metadata<'_> {
    fn drop(&mut self) {
        *self.0.denied_prefix.lock().unwrap() = None;
        self.0
            .router_birth_read
            .armed
            .store(false, Ordering::Release);
        self.0.router_birth_read.resume.notify_one();
    }
}

fn observations(host: &RaftCells) -> Result<[u64; 4]> {
    let sample = host.performance();
    ensure!(sample.available && !sample.saturated);
    let mut values = [0; 4];
    for (index, name) in [
        "cell_command_directory_lookups",
        "cell_membership_demand_refreshes",
        "cell_command_rpc_attempts",
        "cell_command_leader_probes",
    ]
    .into_iter()
    .enumerate()
    {
        values[index] = sample
            .counters
            .iter()
            .find(|counter| counter.name == name)
            .with_context(|| format!("membership routing observation {name}"))?
            .value;
    }
    Ok(values)
}

async fn replace(cluster: &ManagedCluster, source: usize, obsolete: usize) -> Result<()> {
    let (_, execution) = movement::admit_replacement(
        cluster,
        movement::ReplacementTarget {
            source,
            obsolete,
            spare: 4,
        },
        [210, 211],
        false,
    )
    .await?;
    let catalog = &cluster.fixtures[0].catalog;
    let observer = &cluster.hosts[source];
    let mut settled = false;
    for turn in 0..8 {
        movement::discover(cluster).await?;
        let cut = catalog.read(TIMEOUT).await?;
        let progress = match catalog
            .reconcile_movement(&cut, &execution, observer, TIMEOUT)
            .await
        {
            Ok(progress) => progress,
            Err(FleetCatalogError::Unconfirmed) => {
                let current = catalog.read(TIMEOUT).await?;
                ensure!(
                    catalog
                        .pending_reservations(&current, TIMEOUT)
                        .await?
                        .iter()
                        .any(|pending| pending.reservation == execution.reservation),
                    "unconfirmed replacement released its selected debit"
                );
                continue;
            }
            Err(error) => {
                return Err(error).with_context(|| format!("membership replacement turn {turn}"))
            }
        };
        eprintln!(
            "public membership replacement turn={turn}, phase={:?}, settled={}",
            progress.phase, progress.settled
        );
        if progress.settled {
            ensure!(progress.phase == FleetMovementPhase::Complete);
            settled = true;
            break;
        }
        ensure!(catalog
            .pending_reservations(&catalog.read(TIMEOUT).await?, TIMEOUT)
            .await?
            .iter()
            .any(|pending| pending.reservation == execution.reservation));
    }
    ensure!(
        settled,
        "replacement did not finish within the original eight turns"
    );
    let cut = catalog.read(TIMEOUT).await?;
    ensure!(catalog
        .pending_reservations(&cut, TIMEOUT)
        .await?
        .is_empty());
    let replay = catalog
        .reconcile_movement(&cut, &execution, observer, TIMEOUT)
        .await?;
    ensure!(replay.settled && replay.phase == FleetMovementPhase::Complete);
    // Settlement advances the rooted cut independently of installed node
    // trust. Wait for that exact observation before inspecting the old disk;
    // both operations share the original inventory window.
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    let node = cluster.fixtures[obsolete].spec.node;
    let removed = tokio::time::timeout_at(deadline, async {
        inspect_current(observer, node, cut.revision()).await?;
        observer
            .inspect_peer_physical_inventory(
                node,
                cut.revision(),
                deadline.saturating_duration_since(tokio::time::Instant::now()),
            )
            .await
            .context("removed replica physical inventory after settlement")
    })
    .await
    .context("original removed replica inventory window")??;
    let inventory = removed
        .inventory
        .context("removed replica physical inventory")?;
    let retained = inventory
        .group_history(&cluster.history)
        .context("retained removed history")?;
    ensure!(
        !retained.in_configuration() && retained.member() == MemberId::new(obsolete as u64 + 1)?
    );
    Ok(())
}

async fn handoff(cluster: &ManagedCluster, source: usize, obsolete: usize) -> Result<()> {
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    tokio::time::timeout_at(deadline, async {
        loop {
            let old = cluster.hosts[source]
                .group(cluster.history)?
                .status()
                .await?;
            let new = cluster.hosts[4].group(cluster.history)?.status().await?;
            if new.ready
                && new.leader == Some(5)
                && old.leader == new.leader
                && old.term == new.term
            {
                return Ok::<_, anyhow::Error>(());
            }
            // Replacement completion is historical matched-progress evidence.
            // Re-observe the actual leader and guarded term/configuration for
            // each transfer attempt inside this one original setup window.
            for index in (0..3).filter(|index| *index != obsolete) {
                let remaining = || {
                    deadline
                        .checked_duration_since(tokio::time::Instant::now())
                        .context("original replacement handoff window")
                };
                let group = cluster.hosts[index].group(cluster.history)?;
                let progress = match group.replication_status(remaining()?).await {
                    Ok(progress) => progress,
                    Err(
                        RuntimeError::NotLeader | RuntimeError::NotReady | RuntimeError::Overloaded,
                    ) => continue,
                    Err(error) => return Err(error.into()),
                };
                match group
                    .transfer_leadership(
                        LeadershipTransfer {
                            target: MemberId::new(5)?,
                            expected_term: progress.term,
                            expected_revision: progress.membership_revision,
                        },
                        remaining()?,
                    )
                    .await
                {
                    Ok(())
                    | Err(
                        RuntimeError::NotLeader
                        | RuntimeError::NotReady
                        | RuntimeError::Overloaded
                        | RuntimeError::DeadlineExceeded,
                    ) => (),
                    Err(error) => return Err(error.into()),
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("replacement voter became the authenticated leader")??;
    Ok(())
}

pub(super) async fn exercise(
    cluster: &ManagedCluster,
    web: &http::Web,
    callbacks: &AtomicUsize,
    sql: &[Arc<SqlCallbacks>; 7],
    rooted: bool,
    caller: Caller,
) -> Result<()> {
    let source = leader(&cluster.hosts[..3], cluster.history).await?;
    let obsolete = (source + 1) % 3;
    let retry = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )? + 60_000;
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(TIMEOUT + Duration::from_secs(1))
        .build()?;
    let ingress = Ingress {
        cluster,
        web,
        client: &client,
        retry,
    };
    let logical = history(cluster.history, "one")?;
    let cell = cluster.hosts[INGRESS].cell(logical.clone())?;
    cluster.hosts[source]
        .cell(logical.clone())?
        .enroll(
            CellEnrollmentRequest {
                creation: ProposalId::new([180; 16])?,
                durability: Durability::S3,
            },
            TIMEOUT,
        )
        .await?;
    let original = cell
        .command(
            &cell.contract("Increment")?,
            1_i64.to_be_bytes().to_vec(),
            options(0, 1, retry)?,
        )
        .await?;
    ensure!(original.result == 1_i64.to_be_bytes() && original.durability == Durability::S3);
    ensure!(callbacks.load(Ordering::Acquire) == 1);
    ensure!(
        cluster.hosts[INGRESS]
            .resource_usage()
            .host
            .controller
            .groups
            == 0
    );
    let mut state = Cell {
        name: "one",
        history: logical.clone(),
        original: original.clone(),
        latest: original,
        value: 1,
        sequence: 2,
    };
    let directory = &cluster.directory;
    let domain = &cluster.fixtures[0].domain;
    let before_view = domain.run(directory.observe()).await??;
    let before_route = domain
        .run(directory.lookup(&before_view, &logical.cell))
        .await??;
    ensure!(before_route.history == cluster.history);
    let before_membership = cluster.hosts[source]
        .group(cluster.history)?
        .quorum_membership(TIMEOUT)
        .await?;
    let revision = before_membership.membership().revision();
    ensure!(before_membership.configuration().voters == [1, 2, 3]);
    evidence(
        rooted,
        caller,
        "warm",
        serde_json::json!({
            "required": "S3", "receipt": receipt(&state.original), "callbacks": 1,
            "range": format!("{before_route:?}"), "membership": format!("{revision:?}"),
            "ingress": format!("{:?}", cluster.hosts[INGRESS].identity()),
            "obsolete": format!("{:?}", cluster.hosts[obsolete].identity()),
            "replacement": format!("{:?}", cluster.hosts[4].identity()),
            "routing": observations(&cluster.hosts[INGRESS])?,
        }),
    );
    drop(before_membership);

    // Block only this ingress's exact birth object. Fleet trust and enrolled
    // endpoint refresh remain live. A later denied background read proves the
    // directory owner has drained any earlier membership observation.
    let (birth, reads) =
        fault::count_reads(domain.run(directory.materialize_selected_history(cluster.history)))
            .await;
    birth??;
    let key = reads
        .keys()
        .find(|key| key.contains("/cell-groups/births/"))
        .context("actual selected birth GET")?;
    let store = &cluster.fixtures[INGRESS].store;
    let _metadata = Metadata(store);
    let denied_before = store.denied_prefix_reads.load(Ordering::Acquire);
    *store.denied_prefix.lock().unwrap() = Some(object_store::path::Path::from(key.as_str()));
    tokio::time::timeout(TIMEOUT, async {
        while store.denied_prefix_reads.load(Ordering::Acquire) == denied_before {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .context("background membership observation drained before replacement")?;
    Box::pin(replace(cluster, source, obsolete)).await?;
    let membership = cluster.hosts[source]
        .group(cluster.history)?
        .quorum_membership(TIMEOUT)
        .await?;
    let mut voters = membership.configuration().voters.clone();
    voters.sort_unstable();
    let expected: Vec<_> = (1..=5)
        .filter(|member| *member != obsolete as u64 + 1 && *member != 4)
        .collect();
    ensure!(
        voters == expected
            && membership.membership().revision() > revision
            && membership.configuration().voters_outgoing.is_empty()
            && membership.configuration().learners.is_empty()
    );
    drop(membership);
    // A still-current cached leader can legitimately execute through a changed
    // configuration. Move leadership to the new voter, which is absent from
    // this ingress's old replica set, to require actual membership repair.
    handoff(cluster, source, obsolete).await?;
    let after_view = domain.run(directory.observe()).await??;
    let after_route = domain
        .run(directory.lookup(&after_view, &logical.cell))
        .await??;
    ensure!(
        after_view.revision() == before_view.revision()
            && after_view.digest() == before_view.digest()
            && after_route == before_route,
        "membership replacement changed cell placement"
    );

    // Re-enable the real birth read behind its physical gate. The command must
    // ask the existing directory owner for current membership and keep its old
    // range observation, original ID, S3 requirement and five-second deadline.
    let gate = &store.router_birth_read;
    let before = observations(&cluster.hosts[INGRESS])?;
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    gate.armed.store(true, Ordering::Release);
    *store.denied_prefix.lock().unwrap() = None;
    let selected = options(0, state.sequence, retry)?;

    enum Reply {
        Sdk(Box<CommandReceipt>),
        Http(serde_json::Value),
    }
    let pending = async {
        match caller {
            Caller::Sdk => Ok::<_, anyhow::Error>(Reply::Sdk(Box::new(
                cell.command(
                    &cell.contract("Increment")?,
                    1_i64.to_be_bytes().to_vec(),
                    selected.clone(),
                )
                .await?,
            ))),
            Caller::Native | Caller::Axum => Ok(Reply::Http(
                http::Web::send(
                    ingress.http_request(matches!(caller, Caller::Axum), state.name, &selected),
                    format!("membership demand repair caller={caller:?}"),
                )
                .await?
                .success()?,
            )),
        }
    };
    let release = async {
        let _release = Metadata(store);
        tokio::time::timeout_at(deadline, async {
            gate.entered.notified().await;
            while observations(&cluster.hosts[INGRESS])?[1] == before[1] {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            ensure!(callbacks.load(Ordering::Acquire) == 1);
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("public command entered membership repair within its original window")?
    };
    let (updated, released) = tokio::join!(pending, release);
    released?;
    let updated = updated?;
    ensure!(
        tokio::time::Instant::now() < deadline,
        "repair response exceeded original deadline"
    );
    let updated = match updated {
        Reply::Sdk(receipt) => *receipt,
        Reply::Http(reply) => {
            http::value(&reply, state.name, 2)?;
            ensure!(
                reply["durability"] == "s3"
                    && reply["group"] == hex::encode(cluster.history.group.bytes())
            );
            // The HTTP invocation has finished. This separate same-ID replay
            // verifies its exact receipt without running another callback.
            let receipt = cell
                .command(
                    &cell.contract("Increment")?,
                    1_i64.to_be_bytes().to_vec(),
                    selected.clone(),
                )
                .await?;
            ensure!(
                reply["index"].as_u64() == Some(receipt.witness.position.index.get())
                    && reply["term"].as_u64() == Some(receipt.witness.position.term.get())
            );
            receipt
        }
    };
    ensure!(
        updated.id == options(0, 2, retry)?.id
            && updated.result == 2_i64.to_be_bytes()
            && updated.durability == Durability::S3
            && updated.witness.group == cluster.history
            && callbacks.load(Ordering::Acquire) == 2
    );
    let after = observations(&cluster.hosts[INGRESS])?;
    ensure!(
        after[0] == before[0] && after[1] > before[1],
        "membership repair did not retain the cached range: {before:?} -> {after:?}"
    );
    evidence(
        rooted,
        caller,
        "membership_repaired",
        serde_json::json!({
            "required": "S3", "receipt": receipt(&updated), "callbacks": 2,
            "routing_before": before, "routing_after": after, "voters": voters,
            "range_before": format!("{before_route:?}"), "range_after": format!("{after_route:?}"),
            "directory_before": hex::encode(before_view.digest()), "directory_after": hex::encode(after_view.digest()),
            "original_timeout_ms": TIMEOUT.as_millis(),
        }),
    );
    state.latest = updated;
    state.value += 1;
    state.sequence += 1;
    for adapter in [None, Some(false), Some(true)] {
        let before = observations(&cluster.hosts[INGRESS])?;
        ingress.write(&mut state, &before_route, 0, adapter).await?;
        let after = observations(&cluster.hosts[INGRESS])?;
        ensure!(
            after[0] == before[0]
                && after[1] == before[1]
                && after[3] == before[3]
                && after[2] - before[2] == if adapter.is_some() { 2 } else { 1 }
        );
    }
    let before_old = (
        sql[obsolete].commands.load(Ordering::Acquire),
        sql[obsolete].queries.load(Ordering::Acquire),
    );
    let old = cluster.hosts[obsolete].cell(logical.clone())?;
    let refused = old
        .query(
            ProposalId::new([240; 16])?,
            &old.contract("Get")?,
            vec![],
            QueryOptions {
                consistency: QueryConsistency::AtLeast(state.original.version_token.clone()),
                routing: QueryRouting::Local,
                timeout: TIMEOUT,
                ..Default::default()
            },
        )
        .await
        .err()
        .context("removed local replica served SQL")?;
    ensure!(
        matches!(
            refused.as_ref(),
            CellDispatchError::Runtime(RuntimeError::NotFound | RuntimeError::NotReady)
        ),
        "removed local query: {refused:?}"
    );
    let local_refusal = format!("{refused:?}");
    let old_member = MemberId::new(obsolete as u64 + 1)?;
    let refused = cell
        .query(
            ProposalId::new([241; 16])?,
            &cell.contract("Get")?,
            vec![],
            QueryOptions {
                consistency: QueryConsistency::AtLeast(state.original.version_token.clone()),
                routing: QueryRouting::Replicas {
                    members: vec![old_member],
                    attempt_timeout: TIMEOUT,
                    leader_fallback: false,
                },
                timeout: TIMEOUT,
                ..Default::default()
            },
        )
        .await
        .err()
        .context("removed member selector served SQL")?;
    ensure!(
        matches!(
            refused.as_ref(),
            CellDispatchError::Transport(TransportError::Identity)
        ),
        "removed selector: {refused:?}"
    );
    for axum in [false, true] {
        http::Web::send(
            client
                .get(web.url(INGRESS, axum, state.name))
                .header("x-read-consistency", "at-least")
                .header("x-min-version", state.original.version_token.as_str())
                .header("x-query-members", old_member.get())
                .header("x-leader-fallback", "false"),
            format!("removed member HTTP axum={axum}"),
        )
        .await?
        .refused(503, "Unavailable")?;
    }
    let old_proof = match cluster.hosts[obsolete].group(cluster.history) {
        Ok(group) => group.quorum_membership(TIMEOUT).await,
        Err(error) => Err(error),
    };
    ensure!(
        matches!(old_proof, Err(RuntimeError::NotFound)),
        "removed quorum proof failure: {:?}",
        old_proof.as_ref().err()
    );

    // Its independent namespace command grant still permits this node to be an
    // ingress. Removal must withdraw the replica without revoking that grant.
    let mut forwarded = options(0, 1, retry)?;
    forwarded.id = CommandId::new(ClientId::new([247; 16])?, 1)?;
    let forwarded_receipt = old
        .command(
            &old.contract("Increment")?,
            1_i64.to_be_bytes().to_vec(),
            forwarded,
        )
        .await?;
    ensure!(
        forwarded_receipt.result == (state.value + 1).to_be_bytes()
            && forwarded_receipt.durability == Durability::S3
    );
    state.latest = forwarded_receipt;
    state.value += 1;
    ensure!(
        (
            sql[obsolete].commands.load(Ordering::Acquire),
            sql[obsolete].queries.load(Ordering::Acquire)
        ) == before_old,
        "removed replica entered an application callback"
    );
    let replay = cell
        .command(
            &cell.contract("Increment")?,
            1_i64.to_be_bytes().to_vec(),
            options(0, 1, retry)?,
        )
        .await?;
    // The result belongs to the original command. Its selected image proof
    // covers that stored receipt and reflects the later committed mutations.
    ensure!(
        replay.id == state.original.id
            && replay.result == state.original.result
            && replay.version == state.latest.version
            && replay.witness == state.latest.witness
            && matches!(replay.version.covers(&state.original.version), Ok(true))
            && replay.durability == Durability::S3
            && callbacks.load(Ordering::Acquire) == usize::try_from(state.value)?
    );
    for voter in [0, 1, 2, 4].into_iter().filter(|voter| *voter != obsolete) {
        let host = &cluster.hosts[voter];
        let local = host.cell(logical.clone())?;
        let read = local
            .query(
                ProposalId::new([230 + voter as u8; 16])?,
                &local.contract("Get")?,
                vec![],
                QueryOptions {
                    consistency: QueryConsistency::AtLeast(state.latest.version_token.clone()),
                    routing: QueryRouting::Local,
                    timeout: TIMEOUT,
                    ..Default::default()
                },
            )
            .await?;
        ensure!(
            read.result == state.value.to_be_bytes()
                && read.witness == state.latest.witness
                && read.served_by == MemberId::new(voter as u64 + 1)?
                && read.served_by_node == host.identity()
        );
    }
    for axum in [false, true] {
        let reply = web
            .query(
                &client,
                (INGRESS, axum),
                state.name,
                "at-least",
                &serde_json::json!({"version_token": state.original.version_token.as_str()}),
                None,
            )
            .await?
            .success()?;
        http::value(&reply, state.name, state.value)?;
    }
    ensure!(callbacks.load(Ordering::Acquire) == usize::try_from(state.value)?);
    ensure!(
        cluster.hosts[INGRESS]
            .resource_usage()
            .host
            .controller
            .groups
            == 0
    );
    for host in &cluster.hosts {
        let profile = host.observe_node(TIMEOUT).await?.node.profile;
        ensure!(
            profile.group_capacity == 2
                && profile.byte_capacity == 512 * 1024 * 1024
                && profile.bytes_per_group == 256 * 1024 * 1024
        );
    }
    evidence(
        rooted,
        caller,
        "fences_and_receipts_verified",
        serde_json::json!({
            "old_member": old_member.get(), "local_refusal": local_refusal,
            "removed_selector_refusal": format!("{refused:?}"),
            "obsolete_callbacks_before": before_old,
            "obsolete_callbacks_after": [sql[obsolete].commands.load(Ordering::Acquire), sql[obsolete].queries.load(Ordering::Acquire)],
            "original_replay": receipt(&replay), "replay_covers_original": true,
            "authorized_old_ingress": receipt(&state.latest),
            "callbacks": callbacks.load(Ordering::Acquire), "value": state.value,
            "ingress_control": cluster.hosts[INGRESS].resource_usage().control,
            "ingress_ordinary": cluster.hosts[INGRESS].resource_usage().host.controller.ordinary,
        }),
    );
    eprintln!("public membership routing caller={caller:?}, extents={rooted}: RF3 replaced member={old_member:?} with member=5; unchanged directory/range/epoch; demand membership repair without command range lookup; SDK/native/Axum exact S3 replay and current voter SQL; removed replica callbacks refused while independent ingress grant retained");
    Ok(())
}
