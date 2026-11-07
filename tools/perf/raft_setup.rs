// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Explicit setup runs before voter launch. Ordinary and retained startup open
//! existing bindings only. Unknown initialization never allocates another nonce.
use super::*;
use cellar::sdk::consensus::verification::{
    cell_groups::{CellGroupArchivePool, RangeDirectory},
    Catalog,
};
use object_store::ObjectStore;
use std::{
    io::{Read as _, Write as _},
    path::Path,
};

pub fn catalog_options() -> CatalogOptions {
    CatalogOptions {
        prefix: "perf/multi-raft".into(),
        ..Default::default()
    }
}

pub fn namespace(layout: &raft_groups::Layout, schema: [u8; 32]) -> Result<RaftNamespaceConfig> {
    let mut namespace = NamespaceConfig::new(
        ClusterId::new([1; 16])?,
        NamespaceId::new([2; 16])?,
        NamespaceMode::Raft,
        schema,
    );
    namespace.voters = VoterPolicy::ThreeVoters;
    namespace.image_storage = RaftImageStorage::Extents;
    Ok(RaftNamespaceConfig {
        namespace,
        class: layout.class.clone(),
        initialize: None,
    })
}

pub(super) async fn persist(domain: &ExecutionDomain, path: PathBuf, bytes: Vec<u8>) -> Result<()> {
    domain
        .blocking(move || -> Result<()> {
            let parent = path.parent().context("journal parent")?;
            std::fs::create_dir_all(parent)?;
            match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
            {
                Ok(mut file) => {
                    file.write_all(&bytes)?;
                    file.sync_all()?;
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    let mut old = Vec::new();
                    std::fs::File::open(&path)?
                        .take(bytes.len() as u64 + 1)
                        .read_to_end(&mut old)?;
                    ensure!(old == bytes, "initialization journal payload changed");
                }
                Err(error) => return Err(error.into()),
            }
            std::fs::File::open(parent)?.sync_all()?;
            if let Some(parent) = parent.parent() {
                std::fs::File::open(parent)?.sync_all()?;
            }
            Ok(())
        })?
        .await??;
    Ok(())
}

pub async fn prepare(
    root: &Path,
    store: Arc<dyn ObjectStore>,
    declaration: RaftNamespaceConfig,
    layout: &raft_groups::Layout,
    peers: &[RaftPeerNode],
    initialize: bool,
    limits: CellGroupNodeLimits,
) -> Result<()> {
    let placement = Placement::new(u8::try_from(peers.len())?)?;
    ensure!(
        !initialize || layout.generation == 0,
        "replacement history requires fenced recovery"
    );
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain.run(async {
        ensure!(declaration.class == layout.class, "namespace class differs from frozen population");
        let schema = declaration.namespace.schema_digest;
        let catalog = Catalog::new(store, catalog_options())?;
        let namespace = if initialize {
            let mut plan = layout.json(placement);
            plan["schema"] = hex::encode(schema).into();
            persist(&domain, root.join("initialization/layout.json"), serde_json::to_vec(&plan)?).await?;
            catalog.open_namespace(declaration.namespace).await?
        } else { catalog.existing_namespace(declaration.namespace).await? };
        let directory = RangeDirectory::new(namespace, limits.directory)?;
        if initialize {
            let mut routes = Vec::new();
            for ordinal in 0..layout.groups.len() {
                let voters = voters(ordinal, placement, peers)?;
                let request = directory.prepare_group_birth(layout.spec(ordinal, voters)?)?;
                persist(&domain, root.join(format!("initialization/birth-{ordinal}.json")), request.encode().to_vec()).await?;
                directory.reserve_group_birth(&request).await?;
                routes.push(request.route().clone());
            }
            let mutation = directory.prepare_initialization(ProposalId::new([91;16])?, &routes)?;
            persist(&domain, root.join("initialization/ranges.json"), mutation.encode().to_vec()).await?;
            directory.publish(&mutation).await?;
        }
        let view = directory.observe().await?;
        ensure!(view.groups() as usize == layout.groups.len(), "selected physical population changed");
        let archives = CellGroupArchivePool::new(limits.archive.context("archive limits")?.io)?;
        for (ordinal, group) in layout.groups.iter().enumerate() {
            let birth = directory.materialize_selected_history(group.history).await?;
            ensure!(birth.route().range == group.range && birth.route().epoch.get() == u64::from(layout.generation) + 1,
                "selected group range changed");
            ensure!(birth.voters() == voters(ordinal, placement, peers)?, "selected initial voters changed");
            let intent = root.join(format!("initialization/archive-{ordinal}.json"));
            let exists = if initialize {
                let path = intent.clone();
                domain.blocking(move || -> std::io::Result<bool> {
                    match std::fs::metadata(path) {
                        Ok(metadata) if metadata.is_file() => Ok(true),
                        Ok(_) => Err(std::io::Error::other("archive journal is not a file")),
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
                        Err(error) => Err(error),
                    }
                })?.await??
            } else { true };
            if initialize && !exists {
                let prepared = directory.prepare_group_archive_binding(&domain, &archives, &birth).await?;
                persist(&domain, intent, prepared.encode().to_vec()).await?;
                prepared.commit(&domain).await?;
            } else {
                directory.open_group_archive(&domain, &archives, &birth).await
                    .context("existing archive must be complete; retain its exact initialization intent for authority recovery")?;
            }
        }
        Ok::<_,anyhow::Error>(())
    }).await;
    domain.shutdown().await;
    result?
}

pub fn voters(
    ordinal: usize,
    placement: Placement,
    peers: &[RaftPeerNode],
) -> Result<Vec<InitialMember>> {
    let mut voters = placement
        .voters(ordinal)
        .into_iter()
        .map(|member| {
            let peer = peers
                .get(usize::from(member) - 1)
                .context("initial voter endpoint missing")?;
            Ok(InitialMember {
                node: peer.node,
                member: MemberId::new(u64::from(member))?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    // Catalog births canonically order members; leader placement is separate.
    voters.sort_by_key(|member| member.member);
    Ok(voters)
}

pub async fn enroll(
    service: &RaftCells,
    layout: &raft_groups::Layout,
    placement: Placement,
    node: u8,
    timeout: Duration,
) -> Result<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    tokio::time::timeout_at(deadline, async {
        loop {
            let mut complete = true;
            for (group, planned) in layout.groups.iter().enumerate() {
                if !placement.voters(group).contains(&node) {
                    continue;
                }
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                let observed = service
                    .group(planned.history)?
                    .observe(remaining.min(Duration::from_secs(5)))
                    .await?;
                ensure!(
                    observed.cells as usize <= planned.cells,
                    "unexpected cell in frozen range"
                );
                if observed.cells as usize == planned.cells {
                    continue;
                }
                complete = false;
                // Enrollment is a local leader operation. Other voters wait for
                // the replicated manifest; they never initialize a second copy.
                // Only this frozen population can be admitted before HTTP opens.
                if !observed.status.ready
                    || observed.status.leader != Some(observed.replica.member.get())
                {
                    continue;
                }
                for &ordinal in layout.cells_in_group(group)? {
                    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                    let handle = service.cell(layout.history(ordinal)?)?;
                    match handle
                        .enroll(
                            layout.enrollment(ordinal)?,
                            remaining.min(Duration::from_secs(5)),
                        )
                        .await
                    {
                        Ok(_) => {}
                        Err(error)
                            if !error.ambiguous
                                && matches!(
                                    error.cause.as_ref(),
                                    CellDispatchError::Runtime(
                                        RuntimeError::NotLeader | RuntimeError::NotReady
                                    )
                                ) =>
                        {
                            break
                        }
                        // Unknown creation remains owned and must reconcile its
                        // original identity. Do not invent another initializer.
                        Err(error) => return Err(error.into()),
                    }
                }
            }
            if complete {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await??;
    Ok(())
}

#[cfg(test)]
#[path = "raft_setup_tests.rs"]
mod tests;
