// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Competing TLS checkpoint restores on one voter while a third group serves SQL.
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

const RECEIVER: usize = 2;
const RECEIVER_MEMBER: u64 = 3;
const REPAIR_CELLS: usize = 12;

#[derive(Default)]
struct Network {
    partitioned: Vec<GroupHistory>,
    repairs: BTreeMap<GroupHistory, Message>,
}

impl Network {
    async fn pump(&mut self, fixture: &mut RuntimeCluster) -> Result<()> {
        fixture
            .pump_with_network(&[], &mut BTreeMap::new(), |group, message| {
                if self.partitioned.contains(&group)
                    && (message.from == RECEIVER_MEMBER || message.to == RECEIVER_MEMBER)
                {
                    return false;
                }
                if message.get_msg_type() == MessageType::MsgSnapshot {
                    assert_eq!(message.to, RECEIVER_MEMBER);
                    self.repairs.insert(group, message.clone());
                    return false;
                }
                true
            })
            .await
    }

    async fn applied(
        &mut self,
        fixture: &mut RuntimeCluster,
        group: GroupHistory,
        index: LogIndex,
        members: &[usize],
    ) -> Result<()> {
        tokio::time::timeout(Duration::from_secs(15), async {
            while members
                .iter()
                .any(|member| fixture.applied[&group][*member] < index)
            {
                self.pump(fixture).await?;
                tokio::task::yield_now().await;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await??;
        Ok(())
    }
}

async fn repair_cut(
    fixture: &mut RuntimeCluster,
    network: &mut Network,
    group: GroupHistory,
    leader: usize,
    pool: &CellGroupCheckpointPool,
) -> Result<GroupManifest> {
    let log = fixture.cluster.voters[leader].log.cell_group(&group)?;
    let route = log
        .group_manifest()?
        .context("repair route")?
        .route()
        .clone();
    let mut target = LogIndex::new(0);
    for round in 0..REPAIR_CELLS {
        let bytes = images::image(
            None,
            &format!("CREATE TABLE value(n); INSERT INTO value VALUES({round});"),
        )?;
        let cell = history(&route, &format!("restore-{round}"))?;
        let proposal = fixture.runtimes[leader]
            .propose(
                group,
                apply::enrollment_bytes(&route, &cell, &bytes, false)?,
            )
            .await?;
        target = proposal.position.index;
        network.applied(fixture, group, target, &[0, 1]).await?;
    }
    let source = fixture.runtimes[leader]
        .capture_checkpoint(group, pool)
        .await?;
    let checkpoint = fixture
        .cluster
        .domain
        .blocking(move || source.prepare_publication())?
        .await??;
    let selected = checkpoint.marker().clone();
    assert_eq!(selected.position().index, target);
    assert_eq!(selected.root().cells(), REPAIR_CELLS as u32);
    fixture.runtimes[leader]
        .publish_checkpoint(group, checkpoint)
        .await?;
    tokio::time::timeout(Duration::from_secs(15), async {
        while log.checkpoint_manifest()?.as_ref() != Some(&selected) {
            network.pump(fixture).await?;
            tokio::task::yield_now().await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    Ok(selected)
}

async fn write_read(
    fixture: &mut RuntimeCluster,
    network: &mut Network,
    route: &GroupRoute,
    cell: &CellHistory,
    class: &Arc<RaftClass>,
    sequence: u8,
) -> Result<()> {
    let group = route.history;
    let pool = fixture.appliers[&(0, group)].0.clone();
    let mut attempt = cell_attempt(130 + sequence)?;
    attempt.request.id = CommandId::new(ClientId::new([81; 16])?, u64::from(sequence))?;
    attempt.request.operation = "Add".into();
    attempt.request.contract_digest = [82; 32];
    attempt.request.request_digest = command_request_digest(&[]);
    let authority = fixture.runtimes[0]
        .execution_authority(group, cell.clone())
        .await?;
    let CellSqlOutcome::Prepared(prepared) = pool
        .stage_registered(
            fixture.cell_options(cell).await?,
            authority,
            attempt,
            class.clone(),
            vec![],
            true,
        )
        .await?
    else {
        anyhow::bail!("expected a new command preparation");
    };
    pool.mark_submitted(cell.clone(), prepared.proposal())
        .await?;
    let accepted = accepted_sql(&fixture.runtimes[0], &prepared).await?;
    network
        .applied(fixture, group, accepted.position.index, &[0, 1, RECEIVER])
        .await?;
    let authority = fixture.runtimes[0]
        .execution_authority(group, cell.clone())
        .await?;
    let receipt = pool.finish(authority).await?;
    let expected = (i64::from(sequence) * 7).to_be_bytes();
    assert_eq!(receipt.receipt().receipt.result(), expected);
    let selected = fixture.runtimes[RECEIVER]
        .read_authority(
            group,
            cell.clone(),
            CellReadConsistency::AtLeast(receipt.receipt().version.clone()),
            reads::deadline(fixture, 5000).await?,
        )
        .await?;
    let output = fixture.appliers[&(RECEIVER, group)]
        .0
        .query_registered(
            fixture.cell_options_at(cell, RECEIVER).await?,
            selected,
            class.clone(),
            class.contract("Get").context("read contract")?,
            vec![],
        )
        .await?;
    assert_eq!(output.observation(), &receipt.receipt().observation());
    assert_eq!(output.result(), expected);
    Ok(())
}

pub(crate) async fn repair_budget_under_load() -> Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let class = Arc::new(
        RaftClass::new("Counter", 1, [81; 32])?
            .command("Add", [82; 32], move |ctx, _| {
                observed.fetch_add(1, Ordering::SeqCst);
                ctx.sql().execute("UPDATE value SET n=n+7", [])?;
                let n: i64 = ctx
                    .sql()
                    .query_row("SELECT n FROM value", [], |row| row.get(0))?;
                Ok(n.to_be_bytes().to_vec())
            })?
            .query("Get", [83; 32], |ctx, _| {
                let n: i64 = ctx.query_row("SELECT n FROM value", [], |row| row.get(0))?;
                Ok(n.to_be_bytes().to_vec())
            })?,
    );
    let (mut fixture, route, traffic, _) = Box::pin(registered::setup(false, &class)).await?;
    let mut groups = Vec::new();
    for number in [71, 72] {
        let history = fixture
            .add_group_with_filesystem(number, |voter| {
                Arc::new(fault::FaultFileSystem {
                    gate: voter.gate.clone(),
                })
            })
            .await?;
        controller::attach_group_appliers(&mut fixture, route.history, history).await?;
        groups.push(history);
    }
    fixture
        .connect_tcp_options(
            None,
            TransportOptions {
                io_timeout_ms: 30_000,
                snapshot_timeout_ms: 60_000,
                ..Default::default()
            },
        )
        .await?;
    let mut pools = Vec::new();
    let mut registrations = Vec::new();
    for node in 0..3 {
        let pool = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
        fixture.transports[node].enable_group_checkpoints(pool.clone())?;
        pools.push(pool);
        for group in &groups {
            registrations.push(
                fixture.transports[node]
                    .register_group_checkpoint(fixture.appliers[&(node, *group)].1.clone())?,
            );
        }
    }

    let mut network = Network {
        partitioned: groups.clone(),
        ..Default::default()
    };
    let publication = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    let mut cuts = Vec::new();
    for (leader, group) in groups.iter().copied().enumerate() {
        fixture.runtimes[leader].campaign(group).await?;
        network
            .applied(&mut fixture, group, LogIndex::new(2), &[0, 1])
            .await?;
        cuts.push(repair_cut(&mut fixture, &mut network, group, leader, &publication).await?);
        assert_eq!(fixture.applied[&group][RECEIVER], LogIndex::new(1));
    }
    network.partitioned.clear();
    tokio::time::timeout(Duration::from_secs(15), async {
        while !groups
            .iter()
            .all(|group| network.repairs.contains_key(group))
        {
            for leader in 0..2 {
                fixture.runtimes[leader].tick().await?;
            }
            network.pump(&mut fixture).await?;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    let first = network
        .repairs
        .remove(&groups[0])
        .context("first Raft repair")?;
    let second = network
        .repairs
        .remove(&groups[1])
        .context("second Raft repair")?;
    assert_eq!(first.from, 1);
    assert_eq!(second.from, 2);

    let gate = fixture.cluster.voters[RECEIVER].gate.clone();
    let hold = gate.arm(fault::HOLD_DEPENDENCY_PREFIX);
    let first_delivery = fixture.transports[0].submit_physical(&groups[0], first)?;
    tokio::time::timeout(Duration::from_secs(10), gate.entered.notified())
        .await
        .context("first TLS restore reached the receiver image spool")?;
    let charged = pools[RECEIVER].usage();
    assert_eq!(charged.sources, 1);
    assert!(charged.retained_bytes > 0);
    assert!(fixture.transports[0].usage().data_items > 0);
    let competitor = fixture.transports[1].submit_physical(&groups[1], second.clone())?;
    assert!(
        tokio::time::timeout(Duration::from_secs(5), competitor.accepted())
            .await?
            .is_err()
    );

    tokio::time::timeout(Duration::from_secs(20), async {
        for sequence in 1..=24 {
            assert_eq!(pools[RECEIVER].usage(), charged);
            write_read(
                &mut fixture,
                &mut network,
                &route,
                &traffic,
                &class,
                sequence,
            )
            .await?;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    for group in &groups {
        assert!(fixture.cluster.voters[RECEIVER]
            .log
            .cell_group(group)?
            .checkpoint_manifest()?
            .is_none());
    }
    assert_eq!(pools[RECEIVER].usage(), charged);
    assert_eq!(calls.load(Ordering::SeqCst), 24);
    drop(hold);
    tokio::time::timeout(Duration::from_secs(10), first_delivery.accepted()).await??;
    network
        .applied(
            &mut fixture,
            groups[0],
            cuts[0].position().index,
            &[RECEIVER],
        )
        .await?;
    controller::wait_for(async || {
        Ok(pools[RECEIVER].usage().sources == 0 && pools[1].usage().sources == 0)
    })
    .await?;
    let second_delivery = fixture.transports[1].submit_physical(&groups[1], second)?;
    tokio::time::timeout(Duration::from_secs(10), second_delivery.accepted()).await??;
    network
        .applied(
            &mut fixture,
            groups[1],
            cuts[1].position().index,
            &[RECEIVER],
        )
        .await?;
    for (group, cut) in groups.into_iter().zip(cuts) {
        let log = fixture.cluster.voters[RECEIVER].log.cell_group(&group)?;
        assert_eq!(log.checkpoint_manifest()?.as_ref(), Some(&cut));
        assert_eq!(log.group_manifest()?.as_ref(), Some(&cut));
        let applier = fixture.appliers[&(RECEIVER, group)].1.clone();
        let recovered = fixture
            .cluster
            .domain
            .blocking(move || applier.recover_selected())?
            .await??;
        assert_eq!(recovered.marker(), &cut);
        assert_eq!(recovered.pinned_images(), REPAIR_CELLS);
    }
    controller::wait_for(async || Ok(pools.iter().all(|pool| pool.usage().retained_bytes == 0)))
        .await?;
    drop(registrations);
    fixture.close().await
}
