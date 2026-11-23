// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Managed group activation, grants and retained restart for named services.
use super::*;

pub async fn activate(cluster: &ManagedCluster, archive_name: &str) -> Result<GroupHistory> {
    activate_at(cluster, archive_name, 0).await
}

pub async fn activate_namespace(
    cluster: &ManagedCluster,
    index: usize,
    archive_name: &str,
) -> Result<GroupHistory> {
    activate_at(cluster, archive_name, index).await
}

pub async fn activate_at(
    cluster: &ManagedCluster,
    archive_name: &str,
    index: usize,
) -> Result<GroupHistory> {
    let fixture = &cluster.fixtures[0];
    let declaration = &fixture.additional_namespaces[index];
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
        group: GroupId::new([u8::try_from(181 + 2 * index)?; 16])?,
        incarnation: IncarnationId::new([u8::try_from(182 + 2 * index)?; 16])?,
    };
    let cut = fixture.catalog.read(TIMEOUT).await?;
    let reserved = fixture
        .catalog
        .reserve_managed_first_group(
            &adapter,
            declaration,
            RaftManagedFirstGroupRequest {
                reservation: managed_fixture::request(
                    u8::try_from(210 + 2 * index)?,
                    cut.revision(),
                ),
                standby,
                publication: ProposalId::new([u8::try_from(183 + index)?; 16])?,
                group: CellGroupBirthSpec {
                    history,
                    range: CellRange::all(),
                    epoch: RouteEpoch::new(1)?,
                    creation: ProposalId::new([u8::try_from(184 + index)?; 16])?,
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
        .with_context(|| format!("reserve named service group {index}"))?;
    let cut = fixture.catalog.read(TIMEOUT).await?;
    let authorization = fixture
        .catalog
        .publish_managed_first_group(
            &reserved,
            &managed_fixture::request(u8::try_from(211 + 2 * index)?, cut.revision()),
            TIMEOUT,
        )
        .await
        .with_context(|| format!("publish named service group {index}"))?;
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

pub async fn restart_with_groups(cluster: &mut ManagedCluster, groups: usize) -> Result<()> {
    ensure!(
        groups == 1 + cluster.fixtures[0].additional_namespaces.len(),
        "retained namespace group count"
    );
    restart(cluster).await
}

pub async fn restart(cluster: &mut ManagedCluster) -> Result<()> {
    restart_after_close(cluster, || {}).await
}

pub async fn restart_after_close(
    cluster: &mut ManagedCluster,
    closed: impl FnOnce(),
) -> Result<()> {
    let generation = cluster
        .peers
        .first()
        .context("retained service peers")?
        .process
        .bytes()[0]
        .checked_add(u8::try_from(cluster.fixtures.len())?)
        .context("retained service process generation exhausted")?
        .max(104);
    let old_hosts = std::mem::take(&mut cluster.hosts);
    managed_fixture::close_group_nodes_with_groups(
        std::mem::take(&mut cluster.nodes),
        &old_hosts,
        1 + cluster.fixtures[0].additional_namespaces.len(),
    )
    .await
    .context("close retained service voters before restart")?;
    closed();
    cluster.peers.clear();
    for (index, fixture) in cluster.fixtures.iter().enumerate() {
        let node = fixture
            .start_node(cluster.roots[index].path())
            .await
            .with_context(|| format!("start retained service voter {index}"))?;
        let started = async {
            let (mut config, services) = fixture
                .config(
                    &node,
                    LogOpenMode::Resume,
                    generation
                        .checked_add(u8::try_from(index)?)
                        .context("retained service process generation exhausted")?,
                )
                .await
                .with_context(|| format!("configure retained service voter {index}"))?;
            managed_fixture::archives::configure(&mut config);
            let peer = config.peers[0].clone();
            Ok::<_, anyhow::Error>((
                node.start_raft_cells(config, services)
                    .await
                    .with_context(|| format!("start retained service Raft voter {index}"))?,
                peer,
            ))
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
    grant(cluster)
        .await
        .context("grant retained service peers after restart")?;
    Ok(())
}
