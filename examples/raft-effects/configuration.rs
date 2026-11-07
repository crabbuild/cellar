// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Explicit development provisioning, separate from retained member startup.
use super::*;
use cellar::application::consensus::verification::{
    cell_groups::{CellGroupArchivePool, RangeDirectory},
    Catalog,
};
use std::io::Write as _;

pub fn namespaces(schemas: [[u8; 32]; 2]) -> Result<Vec<RaftNamespaceConfig>> {
    [false, true]
        .into_iter()
        .enumerate()
        .map(|(index, receiver)| {
            let history = effects::history(receiver)?;
            Ok(RaftNamespaceConfig {
                namespace: NamespaceConfig::new(
                    history.cluster,
                    history.namespace,
                    NamespaceMode::Raft,
                    schemas[index],
                ),
                class: if receiver { "Receiver" } else { "Producer" }.into(),
                initialize: None,
            })
        })
        .collect()
}

pub async fn prepare(
    root: &Path,
    store: Arc<dyn object_store::ObjectStore>,
    schemas: [[u8; 32]; 2],
    peers: &[RaftPeerNode],
    fresh: bool,
) -> Result<()> {
    let domain = cellar::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let catalog = Catalog::new(
                store,
                CatalogOptions {
                    prefix: "demo/raft-effects".into(),
                    ..Default::default()
                },
            )?;
            let archives = CellGroupArchivePool::new(CellGroupArchiveLimits::default())?;
            for (index, namespace) in namespaces(schemas)?.into_iter().enumerate() {
                let namespace = if fresh {
                    catalog.open_namespace(namespace.namespace).await?
                } else {
                    catalog.existing_namespace(namespace.namespace).await?
                };
                let directory = RangeDirectory::new(namespace, DirectoryLimits::default())?;
                let history = effects::group(index == 1)?;
                if fresh {
                    let request = directory.prepare_group_birth(CellGroupBirthSpec {
                        history,
                        range: CellRange::all(),
                        epoch: RouteEpoch::new(1).map_err(|_| anyhow::anyhow!("route epoch"))?,
                        creation: ProposalId::new([index as u8 + 11; 16])?,
                        voters: peers
                            .iter()
                            .enumerate()
                            .map(|(index, peer)| {
                                Ok(InitialMember {
                                    node: peer.node,
                                    member: MemberId::new(index as u64 + 1)?,
                                })
                            })
                            .collect::<Result<_>>()?,
                        learners: vec![],
                    })?;
                    directory.reserve_group_birth(&request).await?;
                    let initialization = directory.prepare_initialization(
                        ProposalId::new([index as u8 + 21; 16])?,
                        &[request.route().clone()],
                    )?;
                    directory.publish(&initialization).await?;
                    let birth = directory.materialize_selected_history(history).await?;
                    let binding = directory
                        .prepare_group_archive_binding(&domain, &archives, &birth)
                        .await?;
                    let bytes = binding.encode().to_vec();
                    let path = root.join(format!("archive-{index}.intent"));
                    let parent = root.to_path_buf();
                    // Persist the one-use intent before creating its counters.
                    // A later startup may only open this identity, never recreate it.
                    domain
                        .blocking(move || -> std::io::Result<()> {
                            let mut file = std::fs::OpenOptions::new()
                                .write(true)
                                .create_new(true)
                                .open(&path)?;
                            file.write_all(&bytes)?;
                            file.sync_all()?;
                            std::fs::File::open(parent)?.sync_all()
                        })?
                        .await??;
                    binding.commit(&domain).await?;
                } else {
                    let birth = directory.materialize_selected_history(history).await?;
                    directory
                        .open_group_archive(&domain, &archives, &birth)
                        .await?;
                }
            }
            Ok::<_, anyhow::Error>(())
        })
        .await;
    domain.shutdown().await;
    result?
}

pub fn limits() -> CellGroupNodeLimits {
    CellGroupNodeLimits {
        namespaces: CellGroupHostLimits {
            max_namespaces: 2,
            namespace_bytes: 32 * 1024 * 1024,
        },
        controller: CellGroupControllerLimits {
            max_groups: 2,
            max_parallel_apply: 2,
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
        directory: DirectoryLimits::default(),
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
            max_sources: 2,
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
