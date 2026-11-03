// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One fixed shared range; durable archive setup never runs as retained boot.
use super::*;
use std::{io::Write as _, path::Path};

pub fn group(cluster: ClusterId, namespace: NamespaceId) -> Result<GroupHistory> {
    Ok(GroupHistory {
        cluster,
        namespace,
        group: GroupId::new([5; 16])?,
        incarnation: IncarnationId::new([4; 16])?,
    })
}

fn namespace(schema: [u8; 32], peers: &[RaftPeerNode], fresh: bool) -> Result<RaftNamespaceConfig> {
    let cluster = ClusterId::new([1; 16])?;
    let namespace = NamespaceId::new([2; 16])?;
    let mut config = NamespaceConfig::new(cluster, namespace, NamespaceMode::Raft, schema);
    config.voters = VoterPolicy::ThreeVoters;
    Ok(RaftNamespaceConfig {
        namespace: config,
        class: "Counter".into(),
        initialize: if fresh {
            Some(RaftNamespaceInitialization {
                operation: ProposalId::new([11; 16])?,
                groups: vec![CellGroupBirthSpec {
                    history: group(cluster, namespace)?,
                    range: CellRange::all(),
                    epoch: RouteEpoch::new(1)?,
                    creation: ProposalId::new([12; 16])?,
                    voters: peers
                        .iter()
                        .enumerate()
                        .map(|(i, peer)| {
                            Ok(InitialMember {
                                node: peer.node,
                                member: MemberId::new(i as u64 + 1)?,
                            })
                        })
                        .collect::<Result<_>>()?,
                    learners: vec![],
                }],
            })
        } else {
            None
        },
    })
}

pub fn retained_namespace(schema: [u8; 32]) -> Result<RaftNamespaceConfig> {
    namespace(schema, &[], false)
}

pub async fn prepare(
    root: &Path,
    store: Arc<dyn object_store::ObjectStore>,
    schema: [u8; 32],
    peers: &[RaftPeerNode],
    fresh: bool,
) -> Result<()> {
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = async {
        let setup = RaftNamespaceProvisioning::open(
            &domain,
            store,
            CatalogOptions {
                prefix: "demo/raft-counter".into(),
                ..Default::default()
            },
            namespace(schema, peers, fresh)?,
            limits().directory,
            CellGroupArchiveLimits::default(),
        )
        .await?;
        let history = group(ClusterId::new([1; 16])?, NamespaceId::new([2; 16])?)?;
        if fresh {
            let initialization = setup.prepare_archive(&domain, history).await?;
            let bytes = initialization.encoded_intent().to_vec();
            let parent = root.to_path_buf();
            domain
                .blocking(move || -> std::io::Result<()> {
                    let mut file = std::fs::OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(parent.join("archive.intent"))?;
                    file.write_all(&bytes)?;
                    file.sync_all()?;
                    std::fs::File::open(parent)?.sync_all()
                })?
                .await??;
            initialization.commit(&domain).await?;
        } else {
            setup.open_archive(&domain, history).await?;
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    domain.shutdown().await;
    result
}

pub async fn enroll(
    services: &[RaftCells],
    cluster: ClusterId,
    namespace: NamespaceId,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    tokio::time::timeout_at(deadline, async {
        for (ordinal, name) in ["one", "two"].into_iter().enumerate() {
            let creation = ProposalId::new([31 + ordinal as u8; 16])?;
            loop {
                let mut leader = None;
                for (index, service) in services.iter().enumerate() {
                    let status = service.group(group(cluster, namespace)?)?.status().await?;
                    if status.ready && status.leader == Some(index as u64 + 1) {
                        leader = Some(index);
                        break;
                    }
                }
                if let Some(index) = leader {
                    let handle =
                        services[index].cell(counter::history(cluster, namespace, name)?)?;
                    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                    match handle
                        .enroll(
                            CellEnrollmentRequest {
                                creation,
                                durability: Durability::Quorum,
                            },
                            remaining,
                        )
                        .await
                    {
                        Ok(_) => break,
                        Err(error)
                            if !error.ambiguous
                                && matches!(
                                    error.cause.as_ref(),
                                    CellDispatchError::Runtime(
                                        RuntimeError::NotLeader | RuntimeError::NotReady
                                    )
                                ) =>
                        {
                            ()
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
        loop {
            let mut ready = true;
            for service in services {
                let usage = service.resource_usage();
                ensure!(
                    usage.host.controller.groups == 1,
                    "counter must own exactly one physical group"
                );
                ready &= usage.retained_manifest_cells == Some(2);
            }
            if ready {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await??;
    Ok(())
}

pub fn limits() -> CellGroupNodeLimits {
    CellGroupNodeLimits {
        namespaces: CellGroupHostLimits {
            max_namespaces: 1,
            namespace_bytes: 32 * 1024 * 1024,
        },
        controller: CellGroupControllerLimits {
            max_groups: 1,
            max_parallel_apply: 1,
            retry_interval_ms: 5,
            max_commands: 8,
            max_command_bytes: 1024 * 1024,
            checkpoints: None,
        },
        sql: CellSqlLimits {
            workers: 2,
            queue_per_worker: 4,
            max_pending_cells: 8,
            group_pending_cells: 8,
            node_bytes: 512 * 1024 * 1024,
            group_bytes: 256 * 1024 * 1024,
        },
        provisioning: CellGroupProvisionLimits {
            max_preparations: 1,
            max_preparation_bytes: 512 * 1024 * 1024,
            max_parallel_preparations: 1,
            max_waiters: 2,
            peer_revision: 1,
        },
        directory: DirectoryLimits {
            max_groups: 1,
            ..Default::default()
        },
        dispatch: CellDispatchLimits {
            max_cached_routes: 32,
            route_cache_bytes: 256 * 1024,
            max_invocations: 8,
            retained_bytes: 4 * 1024 * 1024,
            max_input_bytes: 64 * 1024,
        },
        manifest: ManifestLimits {
            max_cells: 64,
            max_depth: 16,
        },
        entries: Default::default(),
        manifest_pages: 1024,
        manifest_bytes: 16 * 1024 * 1024,
        retained_roots: 16,
        max_effect_rows: 100,
        checkpoints: Some(CellGroupCheckpointLimits {
            max_sources: 1,
            max_retained_bytes: 32 * 1024 * 1024,
            max_cells: 64,
            max_transfer_bytes: 256 * 1024 * 1024,
            max_chunk_bytes: 64 * 1024,
        }),
        archive: Some(CellGroupNodeArchiveOptions {
            io: CellGroupArchiveLimits::default(),
            uploads: CellGroupArchiveUploadLimits::default(),
            tails: CellGroupArchiveTailLimits::default(),
        }),
    }
}
