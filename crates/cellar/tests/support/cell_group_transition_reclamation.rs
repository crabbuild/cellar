// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real source collection preserves colocated child serving and retained restart.
use super::*;

pub(super) async fn collect(
    cluster: &ManagedCluster,
    rooted: bool,
    roots: &mut Vec<tempfile::TempDir>,
    admission: &RaftFleetTransitionAdmissionRequest,
    request: &RaftFleetTransitionSourceSealRequest,
    colocated: bool,
) -> Result<()> {
    for host in &cluster.hosts[..3] {
        let endpoint = Box::pin(control(cluster, rooted, roots))
            .await?
            .with_node_observer(host.clone());
        let deadline = tokio::time::Instant::now() + TIMEOUT;
        tokio::time::timeout(TIMEOUT, async {
            loop {
                let result = endpoint
                    .reclaim_transition_source(
                        "automatic-test",
                        &request.namespace_id,
                        &admission.operation_id,
                        request,
                        deadline.saturating_duration_since(tokio::time::Instant::now()),
                    )
                    .await;
                match result {
                    Ok(true) => break,
                    Ok(false) => (),
                    Err(error) if error.code == RaftFleetFailureCode::Busy => (),
                    Err(error) => {
                        return Err(anyhow::anyhow!(
                            "source reclamation, extents={rooted}: {error:?}"
                        ))
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await??;
        ensure!(
            endpoint
                .reclaim_transition_source(
                    "automatic-test",
                    &request.namespace_id,
                    &admission.operation_id,
                    request,
                    TIMEOUT
                )
                .await?
        );
        assert_collected(host, cluster.history, colocated).await?;
    }
    Ok(())
}

async fn assert_collected(host: &RaftCells, history: GroupHistory, colocated: bool) -> Result<()> {
    let sample = host
        .inspect_peer_physical_inventory(host.identity().node, 1, TIMEOUT)
        .await?;
    ensure!(
        sample.view.registered_histories == u32::from(colocated)
            && sample.view.unattached_engine_histories == 0
            && sample.view.node.groups == u32::from(colocated),
        "collected physical inventory: {:?}",
        sample.view
    );
    ensure!(sample
        .inventory
        .context("complete post-collection inventory")?
        .group_history(&history)
        .is_none());
    ensure!(matches!(
        host.group(history),
        Err(cellar::application::consensus::RuntimeError::NotFound)
    ));
    ensure!(!host.is_closed());
    Ok(())
}

pub(super) async fn retained_restart(
    cluster: &mut ManagedCluster,
    rooted: bool,
    roots: &mut Vec<tempfile::TempDir>,
    admission: &RaftFleetTransitionAdmissionRequest,
    request: &RaftFleetTransitionSourceSealRequest,
    colocated: bool,
) -> Result<()> {
    let node = cluster.nodes.remove(0);
    let old = cluster.hosts.remove(0);
    ensure!(matches!(
        node.shutdown(ShutdownMode::Preserve).await?,
        NodeOutcome::Stopped(_)
    ));
    ensure!(old.close().await?.mutation_outcomes == MutationOutcomes::NoUnresolvedLocalProducers);
    drop(old);
    let fixture = &cluster.fixtures[0];
    let node = managed_fixture::start_node(
        cluster.roots[0].path(),
        fixture.application.clone(),
        &fixture.spec.label,
    )
    .await?;
    let (mut config, services) = fixture.config(&node, LogOpenMode::Resume, 91).await?;
    managed_fixture::archives::configure(&mut config);
    cluster.peers[0] = config.peers[0].clone();
    let host = node.start_raft_cells(config, services).await?;
    cluster.nodes.insert(0, node);
    cluster.hosts.insert(0, host.clone());
    let endpoint = Box::pin(control(cluster, rooted, roots))
        .await?
        .with_node_observer(host.clone());
    ensure!(
        endpoint
            .reclaim_transition_source(
                "automatic-test",
                &request.namespace_id,
                &admission.operation_id,
                request,
                TIMEOUT
            )
            .await?
    );
    assert_collected(&host, cluster.history, colocated).await?;
    Ok(())
}
