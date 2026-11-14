// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Managed group activation, grants and retained restart for named services.
use super::*;

pub async fn activate(cluster: &ManagedCluster, archive_name: &str) -> Result<GroupHistory> {
    activate_namespace(cluster, 0, archive_name).await
}

pub async fn activate_namespace(
    cluster: &ManagedCluster,
    index: usize,
    archive_name: &str,
) -> Result<GroupHistory> {
    let fixture = &cluster.fixtures[0];
    let declaration = cluster.fixtures[0]
        .additional_namespaces
        .get(index)
        .context("additional namespace")?;
    let identity = 210u8
        .checked_add(
            u8::try_from(index)?
                .checked_mul(2)
                .context("namespace identity")?,
        )
        .context("namespace identity")?;
    let adapter = Catalog::new(
        fixture.store.clone(),
        CatalogOptions {
            prefix: "managed/".into(),
            ..Default::default()
        },
    )?;
    let standby = fixture
        .catalog
        .observe_managed_standby(&adapter, declaration, DirectoryLimits::default(), TIMEOUT)
        .await
        .context("observe additional namespace standby")?;
    // Fleet group IDs are permanent across namespaces. A second primitive
    // service needs its own ID even though its logical history also names a
    // different namespace.
    let group = 181u8
        .checked_add(u8::try_from(index)?)
        .context("additional namespace group identity")?;
    let history = GroupHistory {
        cluster: declaration.namespace.cluster,
        namespace: declaration.namespace.namespace,
        group: GroupId::new([group; 16])?,
        incarnation: IncarnationId::new([182; 16])?,
    };
    let cut = fixture.catalog.read(TIMEOUT).await?;
    let reserved = fixture
        .catalog
        .reserve_managed_first_group(
            &adapter,
            declaration,
            RaftManagedFirstGroupRequest {
                reservation: managed_fixture::request(identity, cut.revision()),
                standby,
                publication: ProposalId::new([183; 16])?,
                group: CellGroupBirthSpec {
                    history,
                    range: CellRange::all(),
                    epoch: RouteEpoch::new(1)?,
                    creation: ProposalId::new([184; 16])?,
                    voters: cluster
                        .fixtures
                        .iter()
                        .enumerate()
                        .map(|(index, fixture)| {
                            Ok(InitialMember {
                                member: MemberId::new(index as u64 + 1)?,
                                node: fixture.spec.node,
                            })
                        })
                        .collect::<Result<_>>()?,
                    learners: vec![],
                },
            },
            DirectoryLimits::default(),
            TIMEOUT,
        )
        .await
        .context("reserve additional namespace group")?;
    let cut = fixture.catalog.read(TIMEOUT).await?;
    let authorization = fixture
        .catalog
        .publish_managed_first_group(
            &reserved,
            &managed_fixture::request(
                identity.checked_add(1).context("namespace identity")?,
                cut.revision(),
            ),
            TIMEOUT,
        )
        .await
        .context("publish additional namespace group")?;
    let mut database_fixture = fixture.clone();
    database_fixture.declaration = declaration.clone();
    let intent_root = cluster.roots[0].path().join(archive_name);
    let filesystem = fixture.filesystem.clone();
    let directory = intent_root.clone();
    fixture
        .domain
        .blocking(move || {
            cellar_ltx::host::create_dir_all_durable(filesystem.as_ref(), &directory)
        })?
        .await??;
    managed_fixture::archives::initialize(&database_fixture, &adapter, history, &intent_root)
        .await?;
    for host in &cluster.hosts {
        host.activate_managed_initial_group(authorization.clone(), TIMEOUT)
            .await?;
    }
    grant(cluster).await?;
    managed_fixture::leader(&cluster.hosts, history).await?;
    Ok(history)
}

pub async fn grant(cluster: &ManagedCluster) -> Result<()> {
    let fixture = &cluster.fixtures[0];
    let cut = fixture.catalog.read(TIMEOUT).await?;
    for (index, host) in cluster.hosts.iter().enumerate() {
        for (peer_index, peer) in cluster.peers.iter().enumerate() {
            managed_fixture::inspect_current(host, peer.node, cut.revision()).await?;
            if index == peer_index {
                continue;
            }
            // Explicit trusted namespace permissions, separate from RF3
            // membership. Refresh only after verifying this boot's endpoints.
            for declaration in
                std::iter::once(&fixture.declaration).chain(&fixture.additional_namespaces)
            {
                host.grant_query_origin(NamespaceQueryOrigin {
                    cluster: declaration.namespace.cluster,
                    namespace: declaration.namespace.namespace,
                    revision: 1,
                    peer: peer.clone(),
                })?;
                host.grant_command_origin(NamespaceCommandOrigin {
                    cluster: declaration.namespace.cluster,
                    namespace: declaration.namespace.namespace,
                    revision: 1,
                    peer: peer.clone(),
                })?;
            }
        }
    }
    Ok(())
}

pub async fn restart(cluster: &mut ManagedCluster) -> Result<()> {
    restart_with_groups(cluster, 2).await
}

pub async fn restart_with_groups(cluster: &mut ManagedCluster, groups: usize) -> Result<()> {
    let old_hosts = std::mem::take(&mut cluster.hosts);
    managed_fixture::close_group_nodes_with_groups(
        std::mem::take(&mut cluster.nodes),
        &old_hosts,
        groups,
    )
    .await?;
    cluster.peers.clear();
    for (index, fixture) in cluster.fixtures.iter().enumerate() {
        let node = fixture.start_node(cluster.roots[index].path()).await?;
        let started = async {
            let (mut config, services) = fixture
                .config(&node, LogOpenMode::Resume, 104 + index as u8)
                .await?;
            managed_fixture::archives::configure(&mut config);
            let peer = config.peers[0].clone();
            Ok::<_, anyhow::Error>((node.start_raft_cells(config, services).await?, peer))
        }
        .await;
        let (host, peer) = match started {
            Ok(started) => started,
            Err(error) => {
                node.shutdown(cellar::sdk::ShutdownMode::Preserve).await?;
                return Err(error);
            }
        };
        cluster.nodes.push(node);
        cluster.hosts.push(host);
        cluster.peers.push(peer);
    }
    grant(cluster).await?;
    Ok(())
}
