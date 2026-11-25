// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A control-only host discovers an enrolled voter's retained process change.
use super::*;
use managed_fixture::{
    counter_class_with_callback, leader, movement, ArchiveSetup, FirstGroupSetup, ManagedCluster,
};
use std::sync::atomic::AtomicUsize;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn membership_inspection_discovers_retained_voter_with_one_original_request() -> Result<()> {
    qualify(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rooted_membership_inspection_discovers_retained_voter_with_one_original_request(
) -> Result<()> {
    qualify(true).await
}

fn endpoint_owners(host: &RaftCells) -> Result<u64> {
    let sample = host.performance();
    ensure!(sample.available && !sample.saturated);
    Ok(sample
        .counters
        .iter()
        .find(|v| v.name == "cell_endpoint_discovery_owners")
        .context("endpoint discovery owner counter")?
        .value)
}

async fn qualify(rooted: bool) -> Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let class = counter_class_with_callback(move || {
        count.fetch_add(1, Ordering::AcqRel);
    })?;
    let first = Fixture::with_class(rooted, class).await?;
    let mut cluster = Box::pin(ManagedCluster::start_with_applications(
        first,
        &[],
        ArchiveSetup::Disabled,
        false,
        None,
        FirstGroupSetup::Normal,
    ))
    .await?;
    let mut sender = managed_fixture::peer_fixture(&cluster.fixtures[0], 10).await?;
    sender.catalog = Fixture::open_catalog(
        &sender.domain,
        sender.store.clone(),
        cluster.history.cluster,
    )?;
    let root = tempfile::tempdir()?;
    let mut nodes = Vec::new();
    let mut hosts = Vec::new();
    let result = async {
        let node = start_node(root.path(), sender.application.clone(), &sender.spec.label).await?;
        nodes.push(node);
        let node = nodes.last().context("control-only sender")?;
        let (config, services) = sender.config(node, LogOpenMode::Create, 100).await?;
        hosts.push(node.start_raft_cells(config, services).await?);
        let control = hosts.last().context("control-only sender host")?;
        ensure!(control.observe_node(TIMEOUT).await?.node.groups == 0);
        exercise(&mut cluster, &sender, control).await?;
        ensure!(
            calls.load(Ordering::Acquire) == 0,
            "metadata discovery entered application code"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    sender.store.hold_view_reads.store(false, Ordering::Release);
    sender.store.view_reads_resume.notify_waiters();
    let mut sender_failures = Vec::new();
    for (i, node) in nodes.into_iter().enumerate() {
        if !matches!(
            node.shutdown(ShutdownMode::Preserve).await,
            Ok(NodeOutcome::Stopped(_))
        ) {
            sender_failures.push(format!("sender {i}: node shutdown failed"));
        }
        if let Some(host) = hosts.get(i) {
            match host.close().await {
                Ok(report)
                    if report.groups_total == 0
                        && report.mutation_outcomes
                            == MutationOutcomes::NoUnresolvedLocalProducers =>
                {
                    ()
                }
                result => sender_failures.push(format!("sender {i}: {result:?}")),
            }
        }
    }
    let joined = movement::close(std::mem::take(&mut cluster.nodes), &cluster.hosts).await;
    cluster.fixtures[0].domain.shutdown().await;
    if result.is_err() || !sender_failures.is_empty() || joined.is_err() {
        for root in std::iter::once(root).chain(cluster.roots) {
            eprintln!("retained group endpoint disk: {}", root.keep().display());
        }
    }
    result?;
    ensure!(
        sender_failures.is_empty(),
        "group endpoint sender cleanup: {sender_failures:?}"
    );
    joined
}

async fn transfer(cluster: &ManagedCluster, target: usize) -> Result<()> {
    let source = leader(&cluster.hosts, cluster.history).await?;
    if source == target {
        return Ok(());
    }
    let group = cluster.hosts[source].group(cluster.history)?;
    let member = MemberId::new(target as u64 + 1)?;
    let progress =
        tokio::time::timeout(TIMEOUT, async {
            loop {
                let progress = group.replication_status(TIMEOUT).await?;
                if progress.members.iter().any(|p| {
                    p.member == member && p.recent_active && p.matched >= progress.committed
                }) {
                    return Ok::<_, anyhow::Error>(progress);
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .context("retained endpoint target catch-up")??;
    group
        .transfer_leadership(
            LeadershipTransfer {
                target: member,
                expected_term: progress.term,
                expected_revision: progress.membership_revision,
            },
            TIMEOUT,
        )
        .await?;
    tokio::time::timeout(TIMEOUT, async {
        loop {
            if leader(&cluster.hosts, cluster.history).await? == target {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .context("retained endpoint target leadership")??;
    Ok(())
}

async fn restart(cluster: &mut ManagedCluster, index: usize) -> Result<FleetBootBinding> {
    let previous = cluster.hosts[index]
        .observe_node(TIMEOUT)
        .await?
        .fleet_boot
        .context("old voter boot")?;
    let node = cluster.nodes.remove(index);
    let old = cluster.hosts.remove(index);
    movement::close(vec![node], std::slice::from_ref(&old)).await?;
    let fixture = &cluster.fixtures[index];
    let node = start_node(
        cluster.roots[index].path(),
        fixture.application.clone(),
        &fixture.spec.label,
    )
    .await?;
    let opened = async {
        let (config, services) = fixture.config(&node, LogOpenMode::Resume, 81).await?;
        node.start_raft_cells(config, services).await
    }
    .await;
    let host = match opened {
        Ok(host) => host,
        Err(error) => {
            node.shutdown(ShutdownMode::Preserve).await?;
            return Err(error.into());
        }
    };
    cluster.nodes.insert(index, node);
    cluster.hosts.insert(index, host.clone());
    let boot = host
        .observe_node(TIMEOUT)
        .await?
        .fleet_boot
        .context("retained voter boot")?;
    ensure!(
        boot.node == previous.node
            && boot.disk == previous.disk
            && boot.engine == previous.engine
            && boot.process != previous.process
            && boot.epoch > previous.epoch
    );
    Ok(boot)
}

async fn exercise(
    cluster: &mut ManagedCluster,
    sender: &Fixture,
    control: &RaftCells,
) -> Result<()> {
    transfer(cluster, 0).await?;
    let target = 2;
    let cut = cluster.fixtures[0].catalog.read(TIMEOUT).await?;
    managed_fixture::inspect_current(control, cluster.fixtures[target].spec.node, cut.revision())
        .await?;
    sender.store.hold_view_reads.store(true, Ordering::Release);
    tokio::time::timeout(TIMEOUT, async {
        while sender.store.active_view_reads.load(Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .context("hold control sender's process-view reader")?;
    let boot = restart(cluster, target).await?;
    transfer(cluster, target).await?;
    let cut = cluster.fixtures[0].catalog.read(TIMEOUT).await?;
    let local = control
        .inspect_peer_node(sender.spec.node, cut.revision(), TIMEOUT)
        .await?;
    ensure!(local
        .view
        .fleet_trust_revision
        .is_some_and(|r| r >= cut.revision()));
    ensure!(sender.store.active_view_reads.load(Ordering::Acquire) > 0);
    let authorization = cluster.fixtures[0]
        .catalog
        .authorize_group(&cut, &cluster.history, TIMEOUT)
        .await?;
    let before = endpoint_owners(control)?;
    let request = control.observe_peer_fleet_membership(
        cluster.fixtures[target].spec.node,
        authorization.clone(),
        cut.revision(),
        TIMEOUT,
    );
    tokio::pin!(request);
    let discovery = async {
        while endpoint_owners(control)? == before {
            tokio::task::yield_now().await;
        }
        Ok::<_, anyhow::Error>(())
    };
    tokio::time::timeout(TIMEOUT, async {
        cellar::asyncrt::select_biased! { "premature group inspection completion precedes discovery";
            result = &mut request => anyhow::bail!("group inspection completed before discovering the retained voter: {:?}", result.map(|_| ())),
            result = discovery => result,
        }
    }).await.context("original group inspection did not own endpoint discovery")??;
    sender.store.hold_view_reads.store(false, Ordering::Release);
    sender.store.view_reads_resume.notify_waiters();
    let observed = request
        .await
        .context("one original group inspection must discover the retained voter")?;
    ensure!(
        observed.progress().history == cluster.history
            && observed.node().fleet_boot.as_ref() == Some(&boot)
    );
    ensure!(observed
        .progress()
        .member
        .as_ref()
        .is_some_and(|member| member.is_leader));
    ensure!(endpoint_owners(control)? == before + 1);
    let cached = control
        .observe_peer_fleet_membership(
            cluster.fixtures[target].spec.node,
            authorization,
            cut.revision(),
            TIMEOUT,
        )
        .await?;
    ensure!(cached.node().fleet_boot.as_ref() == Some(&boot));
    ensure!(endpoint_owners(control)? == before + 1);
    Ok(())
}
