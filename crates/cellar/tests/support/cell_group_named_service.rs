// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Managed group activation, grants and retained restart for named services.
use super::*;

pub async fn activate(cluster: &ManagedCluster, archive_name: &str) -> Result<GroupHistory> {
    let fixture = &cluster.fixtures[0];
    let declaration = &fixture.additional_namespaces[0];
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
        .await?;
    let history = GroupHistory {
        cluster: declaration.namespace.cluster,
        namespace: declaration.namespace.namespace,
        group: GroupId::new([181; 16])?,
        incarnation: IncarnationId::new([182; 16])?,
    };
    let cut = fixture.catalog.read(TIMEOUT).await?;
    let reserved = fixture
        .catalog
        .reserve_managed_first_group(
            &adapter,
            declaration,
            RaftManagedFirstGroupRequest {
                reservation: managed_fixture::request(210, cut.revision()),
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
        .await?;
    let cut = fixture.catalog.read(TIMEOUT).await?;
    let authorization = fixture
        .catalog
        .publish_managed_first_group(
            &reserved,
            &managed_fixture::request(211, cut.revision()),
            TIMEOUT,
        )
        .await?;
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
    let old_hosts = std::mem::take(&mut cluster.hosts);
    managed_fixture::close_group_nodes_with_groups(
        std::mem::take(&mut cluster.nodes),
        &old_hosts,
        2,
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
