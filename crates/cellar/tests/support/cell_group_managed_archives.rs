// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Post-startup archive ownership is exercised through actual managed Nodes.
use super::*;

pub(super) async fn initialize(
    fixture: &Fixture,
    directory: &RangeDirectory,
    history: GroupHistory,
    disk: &Path,
) -> Result<()> {
    let birth = fixture
        .domain
        .run(directory.materialize_selected_history(history))
        .await??;
    let pool = CellGroupArchivePool::new(Default::default())?;
    let prepared = directory
        .prepare_group_archive_binding(fixture.domain.as_ref(), &pool, &birth)
        .await?;
    // Explicit operator setup, never an automatic consequence of missing data.
    let intent = disk.join("archive-initialization");
    std::fs::write(&intent, prepared.encode())?;
    std::fs::File::open(&intent)?.sync_all()?;
    std::fs::File::open(disk)?.sync_all()?;
    prepared.commit(fixture.domain.as_ref()).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_dynamic_group_archives_and_joins_on_close() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(archived_group(rooted)).await?;
    }
    Ok(())
}

async fn archived_group(rooted: bool) -> Result<()> {
    let mut cluster = Box::pin(ManagedCluster::start_with_archives(
        rooted,
        &[],
        ArchiveSetup::Initialized,
    ))
    .await?;
    let work = async {
        let seeded = seed_cells(&cluster.hosts, cluster.history).await?;
        let index = leader(&cluster.hosts, cluster.history).await?;
        let handle = cluster.hosts[index].cell(seeded.cells[0].clone())?;
        let mut options =
            CommandOptions::new(CommandId::new(ClientId::new([201; 16])?, 1)?, seeded.retry);
        options.durability = Durability::S3;
        let receipt = handle
            .command(
                &handle.contract("Add")?,
                3i64.to_be_bytes().to_vec(),
                options,
            )
            .await?;
        ensure!(receipt.durability == Durability::S3 && receipt.result == 10i64.to_be_bytes());
        Ok::<_, anyhow::Error>(())
    }
    .await;
    if let Err(error) = work {
        close_group_nodes(std::mem::take(&mut cluster.nodes), &cluster.hosts).await?;
        cluster.fixtures[0].domain.shutdown().await;
        return Err(error.context("S3 acknowledgement for a post-startup managed group"));
    }
    let store = cluster.fixtures[0].store.clone();
    store
        .group_archive_read
        .armed
        .store(true, Ordering::Release);
    // A fresh ordinary mutation makes a real archive job enter the remote read.
    let index = leader(&cluster.hosts, cluster.history).await?;
    let handle = cluster.hosts[index].cell(CellHistory {
        cluster: cluster.history.cluster,
        namespace: cluster.history.namespace,
        cell: CellId::new("Counter:cold-0")?,
        incarnation: IncarnationId::new([91; 16])?,
    })?;
    let retry = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )? + 60_000;
    let quorum = handle
        .command(
            &handle.contract("Add")?,
            1i64.to_be_bytes().to_vec(),
            CommandOptions::new(CommandId::new(ClientId::new([202; 16])?, 1)?, retry),
        )
        .await?;
    ensure!(quorum.durability == Durability::Quorum && quorum.result == 11i64.to_be_bytes());
    let entered = tokio::time::timeout(TIMEOUT, store.group_archive_read.entered.notified()).await;
    let nodes = std::mem::take(&mut cluster.nodes);
    let hosts = cluster.hosts.clone();
    let mut closing = tokio::spawn(async move {
        futures_util::future::try_join_all(nodes.into_iter().zip(hosts).map(
            |(node, host)| async move {
                node.shutdown(ShutdownMode::Preserve).await?;
                Ok::<_, anyhow::Error>(host.close().await?)
            },
        ))
        .await
    });
    let premature = tokio::time::timeout(Duration::from_millis(100), &mut closing).await;
    let returned_early = premature.is_ok();
    // Releasing the backend lets the admitted job settle and physical owners
    // join. Expiring the close waiter above must not release those owners.
    store
        .group_archive_read
        .armed
        .store(false, Ordering::Release);
    store.group_archive_read.resume.notify_one();
    let reports = if let Ok(result) = premature {
        result??
    } else {
        tokio::time::timeout(TIMEOUT, closing).await???
    };
    cluster.fixtures[0].domain.shutdown().await;
    ensure!(entered.is_ok(), "no admitted archive read was observed");
    ensure!(
        !returned_early,
        "close returned while archive work was held"
    );
    ensure!(reports.iter().all(|r| r.groups_total == 1));
    ensure!(reports
        .iter()
        .any(|r| r.groups.iter().any(|g| g.archived.is_some())));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_dynamic_group_requires_explicit_archive_initialization() -> Result<()> {
    for rooted in [false, true] {
        let mut cluster = Box::pin(ManagedCluster::start_with_archives(
            rooted,
            &[],
            ArchiveSetup::Missing,
        ))
        .await?;
        let f = &cluster.fixtures[0];
        let work = async {
            let birth = f
                .domain
                .run(
                    cluster
                        .directory
                        .materialize_selected_history(cluster.history),
                )
                .await??;
            let pool = CellGroupArchivePool::new(Default::default())?;
            ensure!(cluster
                .directory
                .open_group_archive(f.domain.as_ref(), &pool, &birth)
                .await
                .is_err());
            initialize(
                f,
                &cluster.directory,
                cluster.history,
                cluster.roots[0].path(),
            )
            .await?;
            let cut = f.catalog.read(TIMEOUT).await?;
            for host in &cluster.hosts {
                let authorization = f
                    .catalog
                    .authorize_group(&cut, &cluster.history, TIMEOUT)
                    .await?;
                host.activate_managed_initial_group(authorization, TIMEOUT)
                    .await?;
            }
            seed_cells(&cluster.hosts, cluster.history).await?;
            Ok::<_, anyhow::Error>(())
        }
        .await;
        // Even a failed assertion must join all entered physical owners.
        for (node, host) in std::mem::take(&mut cluster.nodes)
            .into_iter()
            .zip(&cluster.hosts)
        {
            node.shutdown(ShutdownMode::Preserve).await?;
            host.close().await?;
        }
        f.domain.shutdown().await;
        work?;
    }
    Ok(())
}
