// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Paused real pre-copy I/O must retain debt without blocking live Raft work.
use super::*;
use std::sync::mpsc::{sync_channel, SyncSender};

#[path = "raft_ltx_fault.rs"]
mod filesystem;

pub(super) struct Context<'a> {
    pub cluster: &'a ManagedCluster,
    pub directory: &'a RangeDirectory,
    pub transition: &'a ReopenedManagedCellGroupTransition,
    pub parent: &'a CellGroupBirth,
    pub children: &'a [GroupRoute],
    pub held: &'a CellGroupArchiveHold,
    pub controller: &'a FleetCatalog,
    pub archived: &'a CommandReceipt,
    pub rooted: bool,
}

struct Release(Option<SyncSender<()>>);

impl Release {
    fn now(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.try_send(());
        }
    }
}

impl Drop for Release {
    fn drop(&mut self) {
        self.now();
    }
}

impl Context<'_> {
    async fn inputs(
        &self,
        base: &std::path::Path,
        restore: CellGroupRecoveryPool,
        filesystem: Arc<dyn cellar_ltx::FileSystem>,
    ) -> Result<CellGroupTransitionPreparationInputs> {
        let root = base.to_path_buf();
        let parent = self.parent.route().clone();
        let children = self.children.to_vec();
        let rooted = self.rooted;
        let (source, targets) = self.cluster.fixtures[0]
            .domain
            .blocking(move || {
                let source = store(root.join("source"), parent, rooted)?;
                let targets = children
                    .into_iter()
                    .enumerate()
                    .map(|(index, route)| {
                        store_with_filesystem(
                            root.join(format!("child-{index}")),
                            route,
                            rooted,
                            filesystem.clone(),
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok::<_, anyhow::Error>((source, targets))
            })?
            .await??;
        Ok(CellGroupTransitionPreparationInputs {
            archives: CellGroupArchivePool::new(Default::default())?,
            tails: CellGroupArchiveTailPool::new(Default::default())?,
            restore,
            sources: vec![CellGroupTransitionSource {
                birth: self.parent.clone(),
                hold: self.held.clone(),
                receiver: CellGroupCheckpointPool::new(checkpoint_limits())?.receive_archive(
                    source.store,
                    source.scratch,
                    source.directory,
                )?,
            }],
            targets,
        })
    }

    async fn busy(&self, inputs: CellGroupTransitionPreparationInputs) -> Result<()> {
        match Box::pin(self.controller.prepare_managed_transition(
            &self.transition.request,
            self.directory,
            &self.transition.plan,
            inputs,
            TIMEOUT,
        ))
        .await
        {
            Err(FleetCatalogError::Overloaded) => Ok(()),
            Err(error) => anyhow::bail!("copy admission failed for another reason: {error:?}"),
            Ok(_) => anyhow::bail!("a second copy bypassed retained admission"),
        }
    }

    async fn retained(&self, phase: TransitionPhase) -> Result<()> {
        let catalog = self.controller.read(TIMEOUT).await?;
        for child in self.children {
            ensure!(
                *self
                    .controller
                    .history(&catalog, &child.history, TIMEOUT)
                    .await?
                    .state()
                    == FleetHistoryState::Reserved
            );
        }
        ensure!(
            self.controller
                .staged_managed_preparation_holds(
                    self.directory,
                    self.transition.plan.operation(),
                    TIMEOUT,
                )
                .await?
                == Some(vec![self.held.clone()])
        );
        let selected = self
            .controller
            .reopen_managed_transition(self.directory, self.transition.plan.operation(), TIMEOUT)
            .await?
            .context("pre-copy intent must stay owned")?;
        let journal = selected.journal.context("pre-copy journal")?;
        ensure!(
            journal.phase() == phase
                && !journal.parent_holds_released()
                && journal.sealed_sources().is_none()
        );
        Ok(())
    }
}

pub(super) async fn exercise(context: Context<'_>) -> Result<CommandReceipt> {
    let fixture = &context.cluster.fixtures[0];
    let root = tempfile::tempdir()?;
    let limits = CellGroupRecoveryLimits {
        max_operations: 1,
        max_retained_bytes: 64 * 1024 * 1024,
        max_cells: 2,
        max_image_bytes: 16 * 1024 * 1024,
        max_restore_bytes: 64 * 1024 * 1024,
    };
    let pool = CellGroupRecoveryPool::new(limits)?;
    let faults = filesystem::Faults::new(root.path().join("entered/child-0/pages"), None);
    let inputs = context
        .inputs(
            &root.path().join("entered"),
            pool.clone(),
            Arc::new(filesystem::FaultFileSystem {
                faults: faults.clone(),
            }),
        )
        .await?;
    let (entered, observed) = tokio::sync::oneshot::channel();
    let (release, blocked) = sync_channel(1);
    let mut release = Release(Some(release));
    *faults.before_directory_sync.lock().unwrap() = Some(Box::new(move || {
        let _ = entered.send(());
        let _ = blocked.recv();
    }));
    let mut preparing = Box::pin(context.controller.prepare_managed_transition(
        &context.transition.request,
        context.directory,
        &context.transition.plan,
        inputs,
        TIMEOUT,
    ));
    tokio::select! {
        result = &mut preparing => {
            match result {
                Err(error) => return Err(error).context("pre-copy failed before the physical barrier"),
                Ok(_) => anyhow::bail!("pre-copy bypassed the physical barrier"),
            }
        }
        signal = tokio::time::timeout(TIMEOUT, observed) => { signal??; }
    }
    let charged = pool.usage();
    ensure!(
        charged.operations == 1
            && charged.retained_bytes > 0
            && charged.retained_bytes <= limits.max_retained_bytes
    );
    context.retained(TransitionPhase::Planned).await?;

    // This is the existing next business operation, executed while the copy's
    // real blocking worker is paused. It keeps its own original retry window.
    let leader = leader(&context.cluster.hosts[..3], context.parent.route().history).await?;
    let handle = context.cluster.hosts[leader].cell(context.archived.version.history.clone())?;
    let receipt = tokio::time::timeout(
        TIMEOUT,
        handle.command(
            &handle.contract("Add")?,
            1i64.to_be_bytes().to_vec(),
            CommandOptions::new(
                CommandId::new(context.archived.id.client, 3)?,
                retry_deadline()?,
            ),
        ),
    )
    .await
    .context("live quorum write starved behind pre-copy")??;
    ensure!(receipt.result == 10i64.to_be_bytes() && receipt.version != context.archived.version);

    // Dropping the async waiter cannot refund the entered worker's debt.
    drop(preparing);
    ensure!(pool.usage() == charged);
    let competing = context
        .inputs(
            &root.path().join("while-entered"),
            pool.clone(),
            Arc::new(DirectFileSystem),
        )
        .await?;
    context.busy(competing).await?;
    ensure!(pool.usage() == charged);
    let read = tokio::time::timeout(
        TIMEOUT,
        handle.query(
            ProposalId::new([173; 16])?,
            &handle.contract("Get")?,
            vec![],
            QueryOptions::default(),
        ),
    )
    .await
    .context("live quorum read starved behind cancelled pre-copy")??;
    ensure!(read.result == receipt.result);
    context.retained(TransitionPhase::Planned).await?;
    release.now();
    tokio::time::timeout(TIMEOUT, async {
        while pool.usage().operations != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .context("entered pre-copy worker did not release admission after completion")?;
    ensure!(pool.usage().retained_bytes == 0);

    // A different pool allows two operations but only one complete workspace.
    // The byte limit must independently refuse a second physical preparation.
    let byte_limits = CellGroupRecoveryLimits {
        max_operations: 2,
        max_retained_bytes: charged.retained_bytes,
        ..limits
    };
    ensure!(matches!(
        CellGroupRecoveryPool::new(CellGroupRecoveryLimits {
            max_retained_bytes: charged.retained_bytes - 1,
            ..byte_limits
        }),
        Err(CellGroupArchiveError::Limit)
    ));
    let bytes = CellGroupRecoveryPool::new(byte_limits)?;
    let inputs = context
        .inputs(
            &root.path().join("resumed"),
            bytes.clone(),
            Arc::new(DirectFileSystem),
        )
        .await?;
    let prepared = Box::pin(context.controller.prepare_managed_transition(
        &context.transition.request,
        context.directory,
        &context.transition.plan,
        inputs,
        TIMEOUT,
    ))
    .await?;
    ensure!(
        prepared.children().len() == 2 && prepared.children().iter().all(|c| c.genesis().is_none())
    );
    ensure!(bytes.usage() == charged);
    let competing = context
        .inputs(
            &root.path().join("while-retained"),
            bytes.clone(),
            Arc::new(DirectFileSystem),
        )
        .await?;
    context.busy(competing).await?;
    ensure!(bytes.usage() == charged);
    context.retained(TransitionPhase::Prepared).await?;
    drop(prepared);
    ensure!(bytes.usage().operations == 0 && bytes.usage().retained_bytes == 0);
    ensure!(
        fixture
            .domain
            .run(context.directory.observe())
            .await??
            .groups()
            == 1
    );
    Ok(receipt)
}
