// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Ordinary ingress retains consensus progress beside a real blocked SQL worker.
use super::*;
use std::sync::{
    atomic::AtomicBool,
    mpsc::{sync_channel, Receiver, SyncSender},
    Mutex,
};
use tokio::sync::Notify;

pub(super) struct Mode {
    pub rooted: bool,
    pub caller: Caller,
}

pub(super) struct CallbackGate {
    armed: AtomicBool,
    started: AtomicBool,
    timed_out: AtomicBool,
    entered: Notify,
    receiver: Mutex<Receiver<()>>,
}

impl CallbackGate {
    pub(super) fn enter(&self) {
        if self.armed.swap(false, Ordering::AcqRel) {
            self.started.store(true, Ordering::Release);
            self.entered.notify_one();
            if self.receiver.lock().unwrap().recv_timeout(TIMEOUT).is_err() {
                self.timed_out.store(true, Ordering::Release);
            }
        }
    }
}

struct HeldCallback<'a> {
    callbacks: &'a SqlCallbacks,
    gate: Arc<CallbackGate>,
    sender: Option<SyncSender<()>>,
}

impl<'a> HeldCallback<'a> {
    fn arm(callbacks: &'a SqlCallbacks) -> Result<Self> {
        let (sender, receiver) = sync_channel(1);
        let gate = Arc::new(CallbackGate {
            armed: AtomicBool::new(true),
            started: AtomicBool::new(false),
            timed_out: AtomicBool::new(false),
            entered: Notify::new(),
            receiver: Mutex::new(receiver),
        });
        let mut selected = callbacks.command_gate.lock().unwrap();
        ensure!(selected.is_none(), "one owned callback gate");
        *selected = Some(gate.clone());
        Ok(Self {
            callbacks,
            gate,
            sender: Some(sender),
        })
    }

    async fn entered(&self) {
        loop {
            let wake = self.gate.entered.notified();
            if self.gate.started.load(Ordering::Acquire) {
                return;
            }
            wake.await;
        }
    }
}

impl Drop for HeldCallback<'_> {
    fn drop(&mut self) {
        drop(self.callbacks.command_gate.lock().unwrap().take());
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(());
        }
    }
}

async fn transfer(
    cluster: &ManagedCluster,
    route: &GroupRoute,
    voters: [usize; 3],
    target: usize,
) -> Result<Term> {
    let member = MemberId::new(
        u64::try_from(
            voters
                .iter()
                .position(|node| *node == target)
                .context("target voter")?,
        )? + 1,
    )?;
    let selected = cluster.hosts[voters[0]]
        .group(route.history)?
        .status()
        .await?;
    let selected_member = selected
        .leader
        .context("selected group leader")?
        .checked_sub(1)
        .context("nonzero leader member")?;
    let source = *voters
        .get(usize::try_from(selected_member)?)
        .context("RF3 leader member")?;
    let handle = cluster.hosts[source].group(route.history)?;
    let before = handle.replication_status(TIMEOUT).await?;
    if source != target {
        handle
            .transfer_leadership(
                LeadershipTransfer {
                    target: member,
                    expected_term: before.term,
                    expected_revision: before.membership_revision,
                },
                TIMEOUT,
            )
            .await?;
    }
    tokio::time::timeout(TIMEOUT, async {
        loop {
            let status = cluster.hosts[target].group(route.history)?.status().await?;
            if status.ready && status.leader == Some(member.get()) {
                if source != target {
                    ensure!(status.term > before.term, "actual leadership transfer term");
                }
                return Ok::<_, anyhow::Error>(status.term);
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .context("shared-worker control handoff")?
}

fn options(value: &CommandOptions) -> serde_json::Value {
    serde_json::json!({
        "client": hex::encode(value.id.client.bytes()), "sequence": value.id.sequence(),
        "retry_until_ms": value.retry_until_ms, "timeout_ms": value.timeout.as_millis(),
        "required": "S3",
    })
}

fn rpc_attempts(host: &RaftCells) -> Result<u64> {
    let sample = host.performance();
    ensure!(sample.available && !sample.saturated);
    Ok(sample
        .counters
        .iter()
        .find(|counter| counter.name == "cell_command_rpc_attempts")
        .context("actual command RPC exchange counter")?
        .value)
}

fn callback_counts(sql: &[Arc<SqlCallbacks>]) -> Vec<usize> {
    sql.iter()
        .map(|local| local.commands.load(Ordering::Acquire))
        .collect()
}

pub(super) async fn exercise(
    cluster: &ManagedCluster,
    web: &http::Web,
    callbacks: &AtomicUsize,
    sql: &[Arc<SqlCallbacks>],
    mode: Mode,
    workers: &mut Vec<RaftFleetTransitionService>,
    roots: &mut Vec<tempfile::TempDir>,
) -> Result<()> {
    let Prepared {
        mut cells,
        order,
        first,
        client,
        ..
    } = prepare(cluster, mode.rooted, workers, roots).await?;
    let ingress = Ingress {
        cluster,
        web,
        client: &client,
    };
    let placements = [[0, 4, 5], [1, 2, 4]];
    // Node four is a real member of both selected children. Its single SQL
    // worker must not become the owner of consensus/control progress.
    ensure!(cluster
        .fixtures
        .iter()
        .all(|fixture| fixture.sql_workers == 1));
    for (route, voters) in first.iter().zip(placements) {
        transfer(cluster, route, voters, 4).await?;
    }
    let hot_index = order[0];
    let cold_index = order[3];
    ensure!(route(&first, &cells[hot_index].history)?.history == first[0].history);
    ensure!(route(&first, &cells[cold_index].history)?.history == first[1].history);
    for index in [hot_index, cold_index] {
        let expected = route(&first, &cells[index].history)?;
        ingress
            .write(&mut cells[index], expected, index, None)
            .await?;
    }
    let adapter = match mode.caller {
        Caller::Sdk => None,
        Caller::Native => Some(false),
        Caller::Axum => Some(true),
    };
    let hot_options = new_options(hot_index, cells[hot_index].sequence)?;
    let cold_options = new_options(cold_index, cells[cold_index].sequence)?;
    let evidence_path = cluster.roots[0].path().join("shared-worker-fairness.json");
    let phase_path = cluster.roots[0].path().join("shared-worker-phase.txt");
    let mut evidence = serde_json::json!({
        "format": 1, "case": "shared_workers_sql_control", "caller": format!("{:?}", mode.caller),
        "extents": mode.rooted, "sql_workers": 1, "phase": "before_owned_callback",
        "hot_request": options(&hot_options), "cold_request": options(&cold_options),
        "hot_group": format!("{:?}", first[0].history), "cold_group": format!("{:?}", first[1].history),
    });
    std::fs::write(&evidence_path, serde_json::to_vec_pretty(&evidence)?)?;
    let held = HeldCallback::arm(&sql[4])?;
    let gate = held.gate.clone();
    let (hot, cold) = if hot_index < cold_index {
        let (left, right) = cells.split_at_mut(cold_index);
        (&mut left[hot_index], &mut right[0])
    } else {
        let (left, right) = cells.split_at_mut(hot_index);
        (&mut right[0], &mut left[cold_index])
    };
    let mut pending = Box::pin(ingress.write_with_options(hot, &first[0], hot_options, adapter));
    tokio::time::timeout(TIMEOUT, async {
        cellar::asyncrt::select! {
            _ = held.entered() => Ok::<_, anyhow::Error>(()),
            result = pending.as_mut() => anyhow::bail!("hot command ended before physical callback entry: {result:?}"),
        }
    }).await.context("actual owned SQL callback entry")??;
    let before_hot = cluster.hosts[4].group(first[0].history)?.status().await?;
    let before_cold = cluster.hosts[4].group(first[1].history)?.status().await?;
    let sql_before = cluster.hosts[4]
        .resource_usage()
        .sql
        .context("held SQL usage")?;
    ensure!(sql_before.running_workers == 1 && sql_before.active_cells >= 1);
    evidence["phase"] = "owned_callback_live".into();
    evidence["held_sql"] = serde_json::to_value(sql_before)?;
    evidence["rpc_attempts_before_cold"] = rpc_attempts(&cluster.hosts[INGRESS])?.into();
    evidence["callbacks_before_cold"] = callback_counts(sql).into();
    std::fs::write(&evidence_path, serde_json::to_vec_pretty(&evidence)?)?;
    let (cold_started, cold_seen) = tokio::sync::oneshot::channel();
    let independent = async {
        std::fs::write(&phase_path, b"cold_leadership_transfer")?;
        let term = transfer(cluster, &first[1], placements[1], 1).await?;
        ensure!(term > before_cold.term);
        std::fs::write(&phase_path, b"cold_s3_write")?;
        let _ = cold_started.send(std::time::Instant::now());
        ingress
            .write_with_options(cold, &first[1], cold_options, adapter)
            .await?;
        std::fs::write(&phase_path, b"hot_group_control_observation")?;
        let after_hot = cluster.hosts[4].group(first[0].history)?.status().await?;
        ensure!(after_hot.applied == before_hot.applied && after_hot.term == before_hot.term);
        ensure!(
            after_hot.ready,
            "blocked SQL must not expire live Raft authority"
        );
        ensure!(
            !gate.timed_out.load(Ordering::Acquire),
            "callback hold exhausted its original bound"
        );
        std::fs::write(&phase_path, b"cold_progress_verified_while_hot_held")?;
        Ok::<_, anyhow::Error>(term)
    };
    let mut independent = Box::pin(independent);
    let mut live_probe = Box::pin(async {
        let started = cold_seen.await?;
        tokio::time::sleep(Duration::from_millis(30)).await;
        Ok::<_, tokio::sync::oneshot::error::RecvError>(started.elapsed())
    });
    let mut sampled = false;
    let transferred_term = loop {
        cellar::asyncrt::select! {
            result = independent.as_mut() => break result?,
            result = pending.as_mut() => {
                evidence["phase"] = "hot_command_ended_before_cold_progress".into();
                evidence["cold_phase"] = std::fs::read_to_string(&phase_path)?.into();
                evidence["rpc_attempts_after_cold"] = rpc_attempts(&cluster.hosts[INGRESS])?.into();
                evidence["callbacks_after_cold"] = callback_counts(sql).into();
                evidence["callback_gate_timed_out"] = gate.timed_out.load(Ordering::Acquire).into();
                evidence["resources_at_failure"] = serde_json::to_value(cluster.hosts.iter().map(|host| host.resource_usage()).collect::<Vec<_>>())?;
                std::fs::write(&evidence_path, serde_json::to_vec_pretty(&evidence)?)?;
                return match result {
                    Ok(()) => Err(anyhow::anyhow!("hot command completed while callback was held")),
                    Err(error) => Err(error.context("hot command ended while callback was held")),
                };
            },
            elapsed = live_probe.as_mut(), if !sampled => {
                sampled = true;
                if let Ok(elapsed) = elapsed {
                    evidence["live_cold_probe"] = serde_json::json!({
                        "elapsed_ms": elapsed.as_millis(),
                        "rpc_attempts": rpc_attempts(&cluster.hosts[INGRESS])?,
                        "callbacks": callback_counts(sql),
                        "gate_timed_out": gate.timed_out.load(Ordering::Acquire),
                        "resources": cluster.hosts.iter().map(|host| host.resource_usage()).collect::<Vec<_>>(),
                    });
                    std::fs::write(&evidence_path, serde_json::to_vec_pretty(&evidence)?)?;
                }
            },
        }
    };
    drop(independent);
    evidence["phase"] = "cold_s3_receipt_while_hot_callback_live".into();
    evidence["cold_term"] = transferred_term.get().into();
    std::fs::write(&evidence_path, serde_json::to_vec_pretty(&evidence)?)?;
    drop(held);
    pending.await?;
    ensure!(!gate.timed_out.load(Ordering::Acquire));
    for (index, cell) in cells.iter().enumerate() {
        let selected = route(&first, &cell.history)?;
        let handle = cluster.hosts[INGRESS].cell(cell.history.clone())?;
        let replay = handle
            .command(
                &handle.contract("Increment")?,
                1_i64.to_be_bytes().to_vec(),
                cell.original_options.clone(),
            )
            .await?;
        ensure!(replay.id == cell.original.id && replay.result == cell.original.result);
        ensure!(replay.durability == Durability::S3);
        ensure!(replay.version.covers(&cell.original.version) == Ok(true));
        let voters = placements[usize::from(selected.history == first[1].history)];
        for (member, node) in voters.into_iter().enumerate() {
            let observed = cluster.hosts[node]
                .group(selected.history)?
                .observe(TIMEOUT)
                .await?;
            ensure!(
                observed.replica.scope == selected.history
                    && observed.replica.member == MemberId::new(u64::try_from(member)? + 1)?
            );
            let local = cluster.hosts[node].cell(cell.history.clone())?;
            let read = local
                .query(
                    ProposalId::new([230 + u8::try_from(index)?; 16])?,
                    &local.contract("Get")?,
                    vec![],
                    QueryOptions {
                        consistency: QueryConsistency::AtLeast(cell.latest.version_token.clone()),
                        routing: QueryRouting::Local,
                        timeout: TIMEOUT,
                        ..Default::default()
                    },
                )
                .await?;
            ensure!(
                read.result == cell.value.to_be_bytes()
                    && read.version.history == cell.history
                    && read.witness == cell.latest.witness
                    && read.served_by == observed.replica.member
                    && read.served_by_node == cluster.hosts[node].identity()
            );
        }
    }
    ensure!(
        cells.iter().map(|cell| cell.value).sum::<i64>()
            == i64::try_from(callbacks.load(Ordering::Acquire))?
    );
    evidence["phase"] = "verified_after_joined_callback".into();
    evidence["callbacks"] = callbacks.load(Ordering::Acquire).into();
    evidence["cells"] = cells.iter().map(|cell| serde_json::json!({
        "name": cell.name, "value": cell.value, "original": receipt(&cell.original), "latest": receipt(&cell.latest),
    })).collect::<Vec<_>>().into();
    evidence["resources"] = serde_json::to_value(
        cluster
            .hosts
            .iter()
            .map(|host| host.resource_usage())
            .collect::<Vec<_>>(),
    )?;
    std::fs::write(&evidence_path, serde_json::to_vec_pretty(&evidence)?)?;
    eprintln!("public shared-worker SQL/control extents={} caller={:?}: one SQL worker, held hot callback, cold leadership transfer and S3 receipt, exact replay and all voter values", mode.rooted, mode.caller);
    Ok(())
}
