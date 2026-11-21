// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A stopped peer's authenticated endpoint remains in the managed directory.

use super::*;

pub async fn run(rooted: bool) -> Result<()> {
    let first = Fixture::new(rooted).await?;
    let second = managed_fixture::peer_fixture(&first, 5).await?;
    let roots = [tempfile::tempdir()?, tempfile::tempdir()?];
    let mut nodes = Vec::new();
    let mut hosts = Vec::new();
    let checked = async {
        let mut previous = Vec::new();
        let mut addresses = Vec::new();
        for (index, fixture) in [&first, &second].into_iter().enumerate() {
            nodes.push(fixture.start_node(roots[index].path()).await?);
            let node = nodes.last().context("new managed peer")?;
            let (config, services) = fixture.config(node, LogOpenMode::Create, 24).await?;
            addresses.push(config.peers[0].control_address);
            let host = node.start_raft_cells(config, services).await?;
            previous.push(
                host.observe_node(TIMEOUT)
                    .await?
                    .fleet_boot
                    .context("original boot")?,
            );
            hosts.push(host);
        }
        while let Some(node) = nodes.pop() {
            let host = hosts.pop().context("original managed host")?;
            stopped(node, &host).await?;
        }
        let cut = first.catalog.read(TIMEOUT).await?;
        let old = first
            .catalog
            .authorize_host(&cut, first.spec.node, TIMEOUT)
            .await?;
        ensure!(old.state() != FleetNodeState::Removed);
        // Hold the forced candidate through public-node startup so an unrelated
        // listener cannot take it before the Raft configuration sees it.
        let candidate = tokio::net::TcpListener::bind(addresses[0]).await?;
        nodes.push(second.start_node(roots[1].path()).await?);
        let node = nodes.last().context("retained managed peer")?;
        let before = second.rejected_peer_addresses();
        let (config, services) = second
            .config_with_control_candidate(node, LogOpenMode::Resume, 25, Some(candidate))
            .await?;
        let selected = config.peers[0].control_address;
        let host = node
            .start_raft_cells(config, services)
            .await
            .context("retained boot after foreign loopback port reuse")?;
        let view = host.observe_node(TIMEOUT).await?;
        let boot = view.fleet_boot.context("retained boot")?;
        hosts.push(host);
        ensure!(
            selected != addresses[0],
            "retained boot reused another node's advertised port"
        );
        ensure!(
            second.rejected_peer_addresses() > before,
            "forced foreign candidate was not checked"
        );
        ensure!(
            boot.node == previous[1].node
                && boot.disk == previous[1].disk
                && boot.engine == previous[1].engine
                && boot.process != previous[1].process
                && boot.epoch > previous[1].epoch
                && view.node.groups == 0
        );
        let released = tokio::net::TcpListener::bind(addresses[0])
            .await
            .context("stopped peer's port remained occupied")?;
        drop(released);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let mut cleanup = Ok(());
    for node in nodes {
        let stopped = node.shutdown(ShutdownMode::Preserve).await;
        let result = stopped.and_then(|outcome| {
            ensure!(matches!(outcome, NodeOutcome::Stopped(_)));
            Ok(())
        });
        cleanup = cleanup.and(result);
    }
    for host in hosts {
        let result = async {
            let report = host.close().await?;
            ensure!(report.groups_total == 0);
            ensure!(report.mutation_outcomes == MutationOutcomes::NoUnresolvedLocalProducers);
            Ok::<_, anyhow::Error>(())
        }
        .await;
        cleanup = cleanup.and(result);
    }
    first.domain.shutdown().await;
    if checked.is_err() || cleanup.is_err() {
        for root in roots {
            eprintln!("retained failed peer port disk: {}", root.keep().display());
        }
    }
    checked?;
    cleanup
}
