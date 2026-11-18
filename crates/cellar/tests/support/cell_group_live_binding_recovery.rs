// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Recover expired binding obligations while all original hosts keep running.

use super::*;

const SETTLEMENTS: &str = "cell_binding_recovery_settlements";

#[derive(Clone, Copy, Debug)]
pub(super) enum Cut {
    Checkpoint,
    Unused,
}

async fn run(
    rooted: bool,
    minimum: Durability,
    store: Arc<catalog_fault::Store>,
    cut: Cut,
) -> Result<()> {
    let checkpoint = Arc::new(Notify::new());
    let rejected = Arc::new(AtomicUsize::new(0));
    let mut cluster = Box::pin(bindings::start_with_recovery_observer(
        rooted,
        store,
        checkpoint.clone(),
        Some(rejected.clone()),
    ))
    .await?;
    let domain = cluster.fixtures[0].domain.clone();
    let work = domain
        .run(async {
            let group = cluster.history;
            let leader = managed_fixture::leader(&cluster.hosts, group).await?;
            let source = cluster.hosts[leader].cell(CellHistory {
                cluster: group.cluster,
                namespace: group.namespace,
                cell: bindings::identity("source")?,
                incarnation: IncarnationId::new([119; 16])?,
            })?;
            source
                .enroll(
                    CellEnrollmentRequest {
                        creation: ProposalId::new([120; 16])?,
                        durability: minimum,
                    },
                    TIMEOUT,
                )
                .await.context("live recovery source enrollment")?;
            let original = bindings::options(1, minimum)?;
            ensure!(original.timeout == TIMEOUT);
            let initial: Vec<_> = cluster.hosts.iter().map(RaftCells::performance).collect();
            let (method, target_name, next_method, before, after) = match cut {
                Cut::Checkpoint => ("Abandon", "held-target", "Again", b"1".as_slice(), 3),
                Cut::Unused => (
                    "Refused",
                    "refused-target",
                    "AgainRefused",
                    b"0".as_slice(),
                    2,
                ),
            };
            let contract = source.contract(method)?;
            let rejected_before_recovery = match cut {
                Cut::Checkpoint => {
                    let call = source
                        .command(&contract, b"{}".to_vec(), original.clone())
                        .boxed();
                    let call = cellar::asyncrt::timeout(TIMEOUT, async {
                        match futures_util::future::select(call, checkpoint.notified().boxed())
                            .await
                        {
                            futures_util::future::Either::Left((result, _)) => {
                                anyhow::bail!(
                                    "binding returned before the target checkpoint: {result:?}"
                                )
                            }
                            futures_util::future::Either::Right(((), call)) => Ok(call),
                        }
                    })
                    .await
                    .context("target checkpoint within the original request budget")??;
                    let error = call
                        .await
                        .err()
                        .context("held child cannot produce a final response")?;
                    ensure!(
                        error.ambiguous,
                        "committed checkpoint must remain unknown: {error}"
                    );
                    0
                }
                Cut::Unused => {
                    let receipt = source
                        .command(&contract, b"{}".to_vec(), original.clone())
                        .await.context("unused-sequence parent before recovery")?;
                    ensure!(receipt.result == b"true");
                    let rejected = rejected.load(Ordering::Acquire);
                    ensure!(
                        rejected != 0,
                        "unused-sequence cut must reach the refusing target"
                    );
                    // The completed parent does not shorten its outgoing intent's
                    // original deadline. Do not ask recovery to steal a live lane.
                    cellar::asyncrt::sleep(TIMEOUT).await;
                    rejected
                }
            };
            // Observe only synchronous host metrics until the node-owned worker
            // confirms settlement. Application probes cannot drive this phase.
            cellar::asyncrt::sleep(Duration::from_secs(2)).await;
            let end = cellar::asyncrt::mono_ms() + 60_000;
            loop {
                let mut observed = false;
                for (host, initial) in cluster.hosts.iter().zip(&initial) {
                    let current = host.performance();
                    ensure!(current.available && !current.saturated && initial.available && !initial.saturated,
                        "recovery observations must be available and unsaturated");
                    ensure!(current.source == initial.source, "a voter owner restarted during live recovery");
                    let before = initial.counters.iter().find(|counter| counter.name == SETTLEMENTS)
                        .map_or(0, |counter| counter.value);
                    let current = current.counters.iter().find(|counter| counter.name == SETTLEMENTS)
                        .context("binding recovery settlement observation is missing")?.value;
                    ensure!(current >= before, "binding recovery counter moved backwards");
                    observed |= current > before;
                }
                if observed {
                    break;
                }
                ensure!(
                    cellar::asyncrt::mono_ms() < end,
                    "live recovery did not confirm settlement without application traffic"
                );
                cellar::asyncrt::sleep(Duration::from_millis(100)).await;
            }
            // The metric is a historical observation, never serving authority.
            // Now check the real committed obligation guard and business state.
            let probe = source.command(&source.contract("RecoveryProbe")?, b"{}".to_vec(),
                bindings::options(2, minimum)?).await.context("readiness after autonomous settlement")?;
            ensure!(probe.result == b"true", "observed settlement left a binding obligation");
            let sequence = 3;
            let target_history = cluster.hosts[leader]
                .resolve_cell_history(
                    group.cluster,
                    group.namespace,
                    bindings::identity(target_name)?,
                    QueryOptions::default(),
                )
                .await?
                .context("recovery must retain or enroll the original target")?;
            let target = cluster.hosts[leader].cell(target_history)?;
            let value = target
                .query(
                    ProposalId::new([130; 16])?,
                    &target.contract("Get")?,
                    vec![],
                    QueryOptions {
                        consistency: QueryConsistency::Linearizable,
                        ..Default::default()
                    },
                )
                .await.context("target state after recovery, before next child")?;
            ensure!(
                value.result == before,
                "recovery executed or repeated the child business mutation"
            );
            let replay = source.command(&contract, b"{}".to_vec(), original).await;
            match cut {
                Cut::Checkpoint => ensure!(
                    replay
                        .err()
                        .context("original parent result must stay unknown")?
                        .ambiguous
                ),
                Cut::Unused => ensure!(
                    replay?.result == b"true",
                    "recovery changed the completed parent receipt"
                ),
            }
            ensure!(
                rejected.load(Ordering::Acquire) == rejected_before_recovery,
                "recovery or parent replay redispatched the refusing target callback"
            );
            let next = source
                .command(
                    &source.contract(next_method)?,
                    b"{}".to_vec(),
                    bindings::options(sequence, minimum)?,
                )
                .await.with_context(|| format!("first {next_method} child after recovered lane, parent sequence {sequence}"))?;
            ensure!(
                serde_json::from_slice::<serde_json::Value>(&next.result)?
                    == json!({"value": after, "name": target_name}),
                "next child did not advance beyond the retired sequence"
            );
            let source_value = source
                .query(
                    ProposalId::new([132; 16])?,
                    &source.contract("Get")?,
                    vec![],
                    QueryOptions {
                        consistency: QueryConsistency::Linearizable,
                        ..Default::default()
                    },
                )
                .await.context("source state after the next child")?;
            ensure!(
                source_value.result == b"0",
                "recovery probes published business writes"
            );
            ensure!(
                cluster.nodes.len() == 3,
                "all original voter owners must stay alive"
            );
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
                "retained live-binding-recovery disk: {}",
                root.keep().display()
            );
        }
    }
    domain.shutdown().await;
    work?;
    joined
}

pub(super) async fn qualify(rooted: bool, real_rustfs: bool, cut: Cut) -> Result<()> {
    let minima = if real_rustfs {
        &[Durability::Quorum, Durability::S3][..]
    } else {
        &[Durability::Quorum][..]
    };
    let mut failures = Vec::new();
    for &minimum in minima {
        let store = Arc::new(if real_rustfs {
            catalog_fault::Store::around(rustfs::store(
                match (cut, minimum) {
                    (Cut::Checkpoint, Durability::S3) => "live-binding-checkpoint-s3",
                    (Cut::Checkpoint, _) => "live-binding-checkpoint-quorum",
                    (Cut::Unused, Durability::S3) => "live-binding-unused-s3",
                    (Cut::Unused, _) => "live-binding-unused-quorum",
                },
                rooted,
            )?)
        } else {
            catalog_fault::Store::default()
        });
        if let Err(error) = Box::pin(run(rooted, minimum, store, cut)).await {
            let error = error.context(format!("live binding cut={cut:?} extents={rooted} RustFS={real_rustfs} minimum={minimum:?}"));
            eprintln!("{error:?}");
            failures.push(error);
        } else {
            eprintln!("live binding cut={cut:?} extents={rooted} RustFS={real_rustfs} minimum={minimum:?}: PASS");
        }
    }
    match failures.into_iter().next() {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

pub(super) async fn qualify_rustfs(rooted: bool) -> Result<()> {
    let mut failures = Vec::new();
    for cut in [Cut::Checkpoint, Cut::Unused] {
        if let Err(error) = qualify(rooted, true, cut).await {
            failures.push(error);
        }
    }
    match failures.into_iter().next() {
        Some(error) => Err(error),
        None => Ok(()),
    }
}
