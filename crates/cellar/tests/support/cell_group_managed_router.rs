// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A managed ingress learns replacement membership through the directory.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_router_learns_replacement_voter() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            "cellar::raft::group_host::directory=debug,cellar::raft::query_connection=debug",
        )
        .with_test_writer()
        .try_init();
    for rooted in [false, true] {
        Box::pin(routed_replacement(
            rooted,
            Arc::new(fault::Store::default()),
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_managed_router_learns_replacement_voter() -> Result<()> {
    for rooted in [false, true] {
        let store = Arc::new(fault::Store::around(rustfs::store(
            "router-replacement",
            rooted,
        )?));
        Box::pin(routed_replacement(rooted, store)).await?;
    }
    Ok(())
}

async fn routed_replacement(rooted: bool, store: Arc<fault::Store>) -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut cluster = Box::pin(ManagedCluster::start_with_store(
        rooted,
        &[7],
        ArchiveSetup::Initialized,
        store,
    ))
    .await?;
    let started = async {
        let fixture = peer_fixture(&cluster.fixtures[0], 8).await?;
        let node = start_node(
            root.path(),
            fixture.application.clone(),
            &fixture.spec.label,
        )
        .await?;
        let attachment = async {
            let (mut config, services) = fixture.config(&node, LogOpenMode::Create, 63).await?;
            config.groups.push(RaftGroupAssignment {
                history: cluster.history,
                placement: RaftGroupPlacement::QueryRouter,
            });
            let peer = config.peers[0].clone();
            let ingress = node.start_raft_cells(config, services).await?;
            Ok::<_, anyhow::Error>((peer, ingress))
        }
        .await;
        match attachment {
            Ok((peer, ingress)) => Ok((fixture, node, peer, ingress)),
            Err(error) => {
                node.shutdown(ShutdownMode::Preserve).await?;
                Err(error)
            }
        }
    }
    .await;
    let (fixture, node, peer, ingress) = match started {
        Ok(started) => started,
        Err(error) => {
            let joined = close(std::mem::take(&mut cluster.nodes), &cluster.hosts).await;
            cluster.fixtures[0].domain.shutdown().await;
            for disk in std::iter::once(root).chain(std::mem::take(&mut cluster.roots)) {
                eprintln!(
                    "retained failed router startup disk: {}",
                    disk.keep().display()
                );
            }
            joined?;
            return Err(error);
        }
    };
    let work = async {
        ensure!(ingress.observe_node(TIMEOUT).await?.node.groups == 0);
        let (seed, obsolete) = Box::pin(replace_with_retry(&cluster, false, false)).await?;
        maintenance::handoff_to(&cluster, obsolete, 3).await?;
        let cut = fixture.catalog.read(TIMEOUT).await?;
        for (index, host) in cluster.hosts.iter().enumerate() {
            if index == obsolete {
                continue;
            }
            inspect_current(host, fixture.spec.node, cut.revision()).await?;
            host.grant_query_origin(NamespaceQueryOrigin {
                cluster: cluster.history.cluster,
                namespace: cluster.history.namespace,
                revision: 1,
                peer: peer.clone(),
            })?;
            host.grant_command_origin(NamespaceCommandOrigin {
                cluster: cluster.history.cluster,
                namespace: cluster.history.namespace,
                revision: 1,
                peer: peer.clone(),
            })?;
        }
        let handle = ingress.cell(seed.cells[0].clone())?;
        let contract = handle.contract("Get")?;
        let member = MemberId::new(4)?;
        let old_member = MemberId::new(obsolete as u64 + 1)?;
        let deadline = tokio::time::Instant::now() + TIMEOUT;
        let query = |member, consistency, invocation, timeout: Duration| {
            handle.query(
                invocation,
                &contract,
                vec![],
                QueryOptions {
                    consistency,
                    timeout,
                    routing: QueryRouting::Replicas {
                        members: vec![member],
                        attempt_timeout: timeout,
                        leader_fallback: false,
                    },
                    ..Default::default()
                },
            )
        };
        let mut last = None;
        let read = tokio::time::timeout_at(deadline, async {
            loop {
                let budget = || {
                    deadline
                        .saturating_duration_since(tokio::time::Instant::now())
                        .min(Duration::from_millis(500))
                };
                let old = query(
                    old_member,
                    QueryConsistency::Linearizable,
                    ProposalId::new([211; 16])?,
                    budget(),
                )
                .await;
                last = Some((old.as_ref().err().map(ToString::to_string), None));
                if matches!(
                    old.as_ref().err().map(Arc::as_ref),
                    Some(CellDispatchError::Transport(
                        cellar::application::consensus::verification::TransportError::Identity
                    ))
                ) {
                    let result = query(
                        member,
                        QueryConsistency::AtLeast(seed.receipts[0].version_token.clone()),
                        ProposalId::new([210; 16])?,
                        budget(),
                    )
                    .await;
                    last = Some((
                        old.as_ref().err().map(ToString::to_string),
                        result.as_ref().err().map(ToString::to_string),
                    ));
                    if let Ok(read) = result {
                        return Ok::<_, anyhow::Error>(read);
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .with_context(|| format!("ingress did not replace the retired voter route: {last:?}"))??;
        ensure!(
            read.served_by == member && read.served_by_node.node == cluster.fixtures[3].spec.node
        );
        // Replacement verification commits sequence two (+1) before handoff.
        ensure!(read.result == 8i64.to_be_bytes());
        let original = &seed.receipts[0];
        let add = handle.contract("Add")?;
        let replay = handle
            .command(
                &add,
                7i64.to_be_bytes().to_vec(),
                CommandOptions::new(original.id, seed.retry),
            )
            .await?;
        ensure!(replay.id == original.id && replay.result == original.result);
        let next = CommandId::new(original.id.client, 3)?;
        let written = handle
            .command(
                &add,
                1i64.to_be_bytes().to_vec(),
                CommandOptions::new(next, seed.retry),
            )
            .await?;
        ensure!(written.id == next && written.result == 9i64.to_be_bytes());
        ensure!(written.durability == Durability::Quorum);
        let observed = query(
            member,
            QueryConsistency::AtLeast(written.version_token),
            ProposalId::new([212; 16])?,
            TIMEOUT,
        )
        .await?;
        ensure!(observed.served_by == member && observed.result == written.result);
        ensure!(ingress.resource_usage().host.controller.groups == 0);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let ingress_join = async {
        ensure!(matches!(
            node.shutdown(ShutdownMode::Preserve).await?,
            NodeOutcome::Stopped(_)
        ));
        let report = ingress.close().await?;
        ensure!(report.groups_total == 0);
        ensure!(report.mutation_outcomes == MutationOutcomes::NoUnresolvedLocalProducers);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let joined = close(std::mem::take(&mut cluster.nodes), &cluster.hosts).await;
    cluster.fixtures[0].domain.shutdown().await;
    if work.is_err() || ingress_join.is_err() || joined.is_err() {
        for disk in std::iter::once(root).chain(std::mem::take(&mut cluster.roots)) {
            eprintln!(
                "retained router replacement disk: {}",
                disk.keep().display()
            );
        }
    }
    work?;
    ingress_join?;
    joined
}
