// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use cellar_logic::{cell_group::*, consensus::*};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::disallowed_methods)]
async fn archive_journal_reopens_exact_intent_and_refuses_lost_state() -> anyhow::Result<()> {
    let domain = crate::sdk::ExecutionDomain::new(tokio::runtime::Handle::current());
    let history = GroupHistory {
        cluster: ClusterId::new([1; 16])?,
        namespace: NamespaceId::new([2; 16])?,
        group: GroupId::new([3; 16])?,
        incarnation: IncarnationId::new([4; 16])?,
    };
    let archive = CellGroupArchivePool::new(Default::default())?.for_group(
        Arc::new(object_store::memory::InMemory::new()),
        object_store::path::Path::from("journal"),
        history,
    )?;
    let creation = archive
        .prepare_registry_creation(
            &domain,
            GroupRoute {
                history,
                range: CellRange::all(),
                epoch: RouteEpoch::new(1)?,
                birth_digest: [5; 32],
                schema_digest: [6; 32],
            },
        )
        .await?;
    let expected = creation.intent().clone();
    let registry = creation.commit(&domain).await?;
    let intent = registry.prepare_upload(&domain).await?;
    let disk = tempfile::tempdir()?;
    let directory = disk.path().join("owner");
    let path = directory.clone();
    let identity = expected.clone();
    let journal = domain
        .blocking(move || {
            Journal::open(
                Arc::new(cellar_ltx::DirectFileSystem),
                path,
                [7; 16],
                identity,
            )
        })?
        .await??;
    let saved = intent.clone();
    domain
        .blocking(move || -> Result<()> {
            journal.store(&Record {
                pending: Pending::Reserve(saved),
                ..Default::default()
            })?;
            Ok(())
        })?
        .await??;
    let path = directory.clone();
    let identity = expected.clone();
    let reopened = domain
        .blocking(move || {
            Journal::open(
                Arc::new(cellar_ltx::DirectFileSystem),
                path,
                [7; 16],
                identity,
            )
        })?
        .await??;
    let recovered = domain.blocking(move || reopened.load())?.await??;
    let Pending::Reserve(recovered) = recovered.pending else {
        anyhow::bail!("lost reserve intent")
    };
    assert_eq!(recovered, intent);
    let ticket = registry.reserve(&domain, &recovered).await?;
    assert_eq!(registry.reserve(&domain, &intent).await?, ticket);
    let path = directory.clone();
    let identity = expected.clone();
    domain
        .blocking(move || -> Result<()> {
            let filesystem = cellar_ltx::DirectFileSystem;
            filesystem.remove_file(&path.join("state"))?;
            let before = filesystem.read(&path.join("identity"))?;
            assert!(Journal::open(Arc::new(filesystem), path.clone(), [7; 16], identity).is_err());
            assert_eq!(
                cellar_ltx::DirectFileSystem.read(&path.join("identity"))?,
                before
            );
            assert!(cellar_ltx::DirectFileSystem
                .metadata(&path.join("state"))
                .is_err());
            Ok(())
        })?
        .await??;
    registry.abort(&domain, &ticket).await?;
    domain.shutdown().await;
    Ok(())
}
