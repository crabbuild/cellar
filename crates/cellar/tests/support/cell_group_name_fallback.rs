// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Automatic discovery must use ready peers while a local replica cannot apply.

use super::*;

pub async fn run(
    rooted: bool,
    durability: Durability,
    store: Arc<catalog_fault::Store>,
) -> Result<()> {
    let commands = Arc::new(AtomicUsize::new(0));
    let calls = commands.clone();
    let class = managed_fixture::counter_class_with_callback(move || {
        calls.fetch_add(1, Ordering::AcqRel);
    })?;
    let application =
        Arc::new(Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_class(class)?);
    let declared = configuration::options("name-fallback");
    application.validate_deployment(&configuration::deployment(declared.clone()))?;
    let gate = storage_fault::Gate::new(None);
    let mut fixture = Fixture::with_application(rooted, "Counter", application, store).await?;
    fixture.configuration = Some(declared);
    fixture.filesystem = Arc::new(storage_fault::FaultFileSystem { gate: gate.clone() });
    fixture.sql_workers = 1;
    let mut cluster = Box::pin(ManagedCluster::start_with_applications(
        fixture,
        &[],
        ArchiveSetup::Initialized,
        false,
        None,
        FirstGroupSetup::Normal,
    ))
    .await?;
    let domain = cluster.fixtures[0].domain.clone();
    let work = domain.run(async {
        let group = cluster.history;
        // Peer fixtures deliberately own independent direct I/O. Only the
        // first node carries this fault adapter, so keep it off leadership.
        let follower = 0;
        let leader = cellar::asyncrt::timeout(TIMEOUT, async {
            let mut leader = managed_fixture::leader(&cluster.hosts, group).await?;
            if leader == follower {
                let handle = cluster.hosts[leader].group(group)?;
                let progress = handle.replication_status(TIMEOUT).await?;
                handle.transfer_leadership(LeadershipTransfer {
                    target: MemberId::new(2)?,
                    expected_term: progress.term,
                    expected_revision: progress.membership_revision,
                }, TIMEOUT).await?;
                loop {
                    leader = managed_fixture::leader(&cluster.hosts, group).await?;
                    if leader != follower {
                        break;
                    }
                    cellar::asyncrt::sleep(Duration::from_millis(2)).await;
                }
            }
            Ok::<_, anyhow::Error>(leader)
        }).await.context("place leadership away from the physical fault owner")??;
        let history = CellHistory {
            cluster: group.cluster, namespace: group.namespace,
            cell: CellId::new("Counter:name-fallback")?,
            incarnation: IncarnationId::new([214; 16])?,
        };
        let cell = cluster.hosts[leader].cell(history.clone())?;
        cell.enroll(CellEnrollmentRequest { creation: ProposalId::new([215; 16])?, durability }, TIMEOUT).await?;
        let floor = cluster.hosts[leader].group(group)?.status().await?;
        cellar::asyncrt::timeout(TIMEOUT, async {
            loop {
                let status = cluster.hosts[follower].group(group)?.status().await?;
                if status.term == floor.term && status.applied.index >= floor.applied.index {
                    break Ok::<_, anyhow::Error>(());
                }
                cellar::asyncrt::sleep(Duration::from_millis(2)).await;
            }
        }).await.context("catch up the selected name-lookup follower")??;
        *gate.log_sync_prefix.lock().unwrap() = Some(cluster.nodes[follower].info().data_dir.clone());
        let release = gate.arm(storage_fault::HOLD_SYNC);
        let (published, accepted) = tokio::sync::oneshot::channel();
        let id = CommandId::new(ClientId::new([240; 16])?, 1)?;
        let horizon = u64::try_from(cellar::asyncrt::wall_ms())?.checked_add(60_000).context("retry horizon")?;
        let writing = async {
            let mut options = CommandOptions::new(id, horizon);
            options.durability = durability;
            options.timeout = TIMEOUT;
            let receipt = cell.command(&cell.contract("Add")?, 1i64.to_be_bytes().to_vec(), options).await?;
            ensure!(receipt.id == id && receipt.result == 1i64.to_be_bytes());
            let _ = published.send(receipt.witness.position.index);
            Ok::<_, anyhow::Error>(receipt)
        };
        let lookup = async {
            let result = async {
                cellar::asyncrt::timeout(TIMEOUT, gate.entered.notified()).await
                    .context("hold the selected follower's physical log sync")?;
                let committed = cellar::asyncrt::timeout(TIMEOUT, accepted).await
                    .context("the two unheld voters commit the control write")??;
                let held = cluster.hosts[follower].group(group)?.status().await?;
                ensure!(held.applied.index < committed, "held sync must block this follower's actual apply");
                let before = commands.load(Ordering::Acquire);
                let selected = cluster.hosts[follower].resolve_cell_history(
                    history.cluster, history.namespace, history.cell.clone(), QueryOptions::default(),
                ).await.context("automatic discovery must use a ready physical peer while its local voter is held")?;
                ensure!(selected.as_ref() == Some(&history), "automatic lookup changed incarnation");
                ensure!(commands.load(Ordering::Acquire) == before, "name discovery entered an application command");
                Ok::<_, anyhow::Error>(())
            }.await;
            drop(release);
            result
        };
        let (written, selected) = tokio::join!(writing, lookup);
        let receipt = written.context("write while the unrelated voter is held")?;
        selected?;
        ensure!(receipt.durability.max(durability) == receipt.durability);
        ensure!(commands.load(Ordering::Acquire) == 1);
        Ok::<_, anyhow::Error>(())
    }).await.map_err(anyhow::Error::from).and_then(std::convert::identity);
    let joined =
        managed_fixture::close_group_nodes(std::mem::take(&mut cluster.nodes), &cluster.hosts)
            .await;
    domain.shutdown().await;
    if work.is_err() || joined.is_err() {
        for root in std::mem::take(&mut cluster.roots) {
            eprintln!("retained name-fallback node: {}", root.keep().display());
        }
    }
    work.context(format!(
        "name fallback extents={rooted} durability={durability:?}"
    ))?;
    joined
}
