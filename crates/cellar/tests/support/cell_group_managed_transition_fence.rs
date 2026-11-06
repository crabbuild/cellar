// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Fence and erase only the disks owned by the managed host test.
use super::*;
use futures_util::future::BoxFuture;
use std::{collections::BTreeSet, path::PathBuf, sync::atomic::AtomicUsize};

type PhysicalVoter = (NodeId, DiskLineage, IncarnationId);

struct Sources {
    owners: BTreeSet<PhysicalVoter>,
    nodes: Vec<Node>,
    hosts: Vec<RaftCells>,
    disks: Vec<PathBuf>,
}

impl Sources {
    async fn erase(self, domain: Arc<ExecutionDomain>) -> Result<Vec<PathBuf>> {
        let Self {
            nodes,
            hosts,
            disks,
            ..
        } = self;
        // Stop and join every process before removing any source disk.
        Box::pin(super::close(nodes, &hosts, None)).await?;
        ensure!(hosts.iter().all(RaftCells::is_closed));
        drop(hosts);
        domain
            .blocking(move || {
                for disk in &disks {
                    ensure!(disk.join("raft").is_dir(), "owned voter disk is missing");
                    ensure!(!std::fs::symlink_metadata(disk)?.is_symlink());
                }
                for disk in &disks {
                    std::fs::remove_dir_all(disk).context("erase owned old voter disk")?;
                    ensure!(!disk.try_exists()?);
                }
                Ok(disks)
            })?
            .await?
    }
}

#[derive(Default)]
struct FenceState {
    sources: Option<Sources>,
    digest: Option<[u8; 32]>,
    erasing: Option<BoxFuture<'static, Result<Vec<PathBuf>>>>,
    erased: Vec<PathBuf>,
}

#[derive(Default)]
pub(super) struct TransitionFence {
    domain: Option<Arc<ExecutionDomain>>,
    state: tokio::sync::Mutex<FenceState>,
    pub(super) fences: AtomicUsize,
    pub(super) verifications: AtomicUsize,
}

impl TransitionFence {
    pub(super) fn own(
        domain: Arc<ExecutionDomain>,
        fixtures: &[Fixture],
        roots: &mut Vec<tempfile::TempDir>,
        nodes: &mut Vec<Node>,
        hosts: &mut Vec<RaftCells>,
    ) -> Result<Self> {
        let voters = fixtures.len();
        ensure!(
            voters > 0 && roots.len() == voters && nodes.len() == voters && hosts.len() == voters
        );
        let mut owners = BTreeSet::new();
        let mut disks = BTreeSet::new();
        for (((fixture, root), node), host) in
            fixtures.iter().zip(&*roots).zip(&*nodes).zip(&*hosts)
        {
            let spec = &fixture.spec;
            let identity = host.identity();
            ensure!(
                identity.node == spec.node && identity.disk == spec.disk,
                "foreign test voter for {}",
                spec.label
            );
            // NodeDirectory canonicalizes its path when taking ownership.
            let owned_root = root.path().canonicalize()?;
            ensure!(
                node.info().data_dir == owned_root,
                "foreign test disk owner for {}",
                spec.label
            );
            ensure!(owners.insert((spec.node, spec.disk, spec.physical_host)));
            ensure!(disks.insert(root.path()));
        }
        // Keep paths if the fixture fails or a fence call is cancelled.
        // Only the joined erasure job may delete them.
        let disks = std::mem::take(roots)
            .into_iter()
            .map(tempfile::TempDir::keep)
            .collect();
        Ok(Self {
            domain: Some(domain),
            state: tokio::sync::Mutex::new(FenceState {
                sources: Some(Sources {
                    owners,
                    nodes: std::mem::take(nodes),
                    hosts: std::mem::take(hosts),
                    disks,
                }),
                ..Default::default()
            }),
            ..Default::default()
        })
    }

    pub(super) async fn erased_disks(&self) -> usize {
        self.state.lock().await.erased.len()
    }
}

pub(super) async fn check_old_boot_refused(fixture: &Fixture) -> Result<()> {
    let root = tempfile::tempdir()?;
    let node = start_node(
        root.path(),
        fixture.application.clone(),
        &fixture.spec.label,
    )
    .await?;
    let attempted = Box::pin(async {
        let (config, services) = fixture.config(&node, LogOpenMode::Create, 211).await?;
        Box::pin(node.start_raft_cells(config, services)).await
    })
    .await;
    ensure!(matches!(
        node.shutdown(ShutdownMode::Preserve).await?,
        NodeOutcome::Stopped(_)
    ));
    match attempted {
        Err(error) => {
            ensure!(error.chain().any(|cause| matches!(
                cause.downcast_ref::<cellar::application::consensus::CatalogError>(),
                Some(cellar::application::consensus::CatalogError::LostStableState)
            )), "expected the permanent engine pin to refuse an old voter on empty storage: {error:#}");
            Ok(())
        }
        Ok(host) => {
            host.close().await?;
            anyhow::bail!("a new engine booted with an erased voter's identity");
        }
    }
}

impl DisasterFenceAuthority<FleetTransitionRecoveryFenceRequest> for TransitionFence {
    fn fence<'a>(
        &'a self,
        request: &'a FleetTransitionRecoveryFenceRequest,
    ) -> BoxFuture<'a, Result<Vec<u8>>> {
        Box::pin(async move {
            self.fences.fetch_add(1, Ordering::AcqRel);
            let mut state = self.state.lock().await;
            let digest = request.digest();
            if let Some(selected) = state.digest {
                ensure!(selected == digest, "different fence scope");
            }
            if let Some(sources) = &state.sources {
                let required = request
                    .intent()
                    .required_voter_lineages()
                    .iter()
                    .map(|lineage| (lineage.node, lineage.disk, lineage.physical_host))
                    .collect::<BTreeSet<_>>();
                ensure!(
                    sources.owners == required,
                    "incomplete physical writer inventory"
                );
                let domain = self.domain.as_ref().context("fence execution domain")?;
                let sources = state.sources.take().context("owned voters already taken")?;
                let job = domain.spawn(Box::pin(sources.erase(domain.clone())))?;
                state.digest = Some(digest);
                // Keep the task handle in the authority across a lost or
                // cancelled caller. A retry joins the same physical operation.
                state.erasing = Some(Box::pin(async move { job.await? }));
            }
            if let Some(erasing) = &mut state.erasing {
                let erased = erasing.await;
                state.erasing = None;
                state.erased = erased?;
            }
            ensure!(!state.erased.is_empty(), "source erasure is unconfirmed");
            Ok(digest.to_vec())
        })
    }

    fn verify<'a>(
        &'a self,
        request: &'a FleetTransitionRecoveryFenceRequest,
        evidence: &'a [u8],
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.verifications.fetch_add(1, Ordering::AcqRel);
            let digest = request.digest();
            ensure!(evidence == digest, "unbound fence evidence");
            let disks = {
                let state = self.state.lock().await;
                ensure!(
                    state.digest == Some(digest) && !state.erased.is_empty(),
                    "fence not effective"
                );
                state.erased.clone()
            };
            self.domain
                .as_ref()
                .context("fence execution domain")?
                .blocking(move || {
                    for disk in disks {
                        ensure!(!disk.try_exists()?, "erased voter disk reappeared");
                    }
                    Ok(())
                })?
                .await?
        })
    }
}
