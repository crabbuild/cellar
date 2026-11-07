// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Fresh catalog owners must recover the immutable archive identity remotely.
use super::*;

pub(super) async fn reopen_directory(
    cluster: &Cluster,
) -> Result<(RangeDirectory, CellGroupBirth)> {
    let route = cluster.birth.route();
    let catalog = Catalog::new(cluster.store.clone(), Default::default())?;
    let namespace = cluster
        .domain
        .run(catalog.existing_namespace(NamespaceConfig::new(
            route.history.cluster,
            route.history.namespace,
            cellar_logic::NamespaceMode::Raft,
            route.schema_digest,
        )))
        .await??;
    let directory = RangeDirectory::new(namespace, Default::default())?;
    let birth = cluster
        .domain
        .run(directory.materialize_selected_group(route))
        .await??;
    Ok((directory, birth))
}

pub(super) async fn identity() -> Result<()> {
    let cluster = Cluster::new().await?;
    let pool = CellGroupArchivePool::new(Default::default())?;
    let before = cluster
        .store
        .list(None)
        .try_collect::<Vec<_>>()
        .await?
        .len();
    assert!(matches!(
        cluster
            .directory
            .open_group_archive(&cluster.domain, &pool, &cluster.birth)
            .await,
        Err(CellGroupArchiveBindingError::Catalog(
            CatalogError::Corrupt("missing group archive binding")
        ))
    ));
    assert_eq!(
        cluster
            .store
            .list(None)
            .try_collect::<Vec<_>>()
            .await?
            .len(),
        before
    );
    let first = cluster
        .directory
        .prepare_group_archive_binding(&cluster.domain, &pool, &cluster.birth)
        .await?;
    let replacement = cluster
        .directory
        .prepare_group_archive_binding(&cluster.domain, &pool, &cluster.birth)
        .await?;
    assert_ne!(first.encode(), replacement.encode());
    let saved = persist_archive_intent(&cluster, &first.encode().to_vec()).await?;
    assert_eq!(saved, first.encode());
    let registry = first.commit(&cluster.domain).await?;
    let intent = registry.prepare_upload(&cluster.domain).await?;
    let saved = persist_archive_intent(&cluster, &intent).await?;
    let ticket = registry.reserve(&cluster.domain, &saved).await?;
    drop(registry);
    assert!(matches!(
        replacement.commit(&cluster.domain).await,
        Err(CellGroupArchiveBindingError::Catalog(
            CatalogError::Conflict("group archive binding")
        ))
    ));
    let (directory, birth) = reopen_directory(&cluster).await?;
    // Even the original typed birth observation belongs to another catalog
    // owner. Materialize and verify it again rather than transferring authority.
    assert!(matches!(
        directory
            .open_group_archive(&cluster.domain, &pool, &cluster.birth)
            .await,
        Err(CellGroupArchiveBindingError::Catalog(
            CatalogError::Conflict("foreign group birth observation")
        ))
    ));
    let reopened = directory
        .open_group_archive(&cluster.domain, &pool, &birth)
        .await?;
    // Persisted exact allocations reconcile through the remotely bound identity.
    assert_eq!(reopened.reserve(&cluster.domain, &saved).await?, ticket);
    reopened.abort(&cluster.domain, &ticket).await?;
    let objects = cluster.store.list(None).try_collect::<Vec<_>>().await?;
    let binding = objects
        .iter()
        .find(|m| m.location.as_ref().ends_with("/binding"))
        .context("remote binding")?
        .location
        .clone();
    let counter = objects
        .iter()
        .find(|m| m.location.as_ref().ends_with("/ownership/registry"))
        .context("remote counters")?
        .location
        .clone();
    let identity = objects
        .iter()
        .find(|m| m.location.as_ref().ends_with("/ownership/identity"))
        .context("remote identity")?
        .location
        .clone();
    let expected_binding = cluster.store.get(&binding).await?.bytes().await?;
    let expected_identity = cluster.store.get(&identity).await?.bytes().await?;
    drop(reopened);
    cluster.store.delete(&counter).await?;
    let before = cluster
        .store
        .list(None)
        .try_collect::<Vec<_>>()
        .await?
        .len();
    assert!(directory
        .open_group_archive(&cluster.domain, &pool, &birth)
        .await
        .is_err());
    assert_eq!(
        cluster
            .store
            .list(None)
            .try_collect::<Vec<_>>()
            .await?
            .len(),
        before
    );
    let replacement = directory
        .prepare_group_archive_binding(&cluster.domain, &pool, &birth)
        .await?;
    assert!(replacement.commit(&cluster.domain).await.is_err());
    assert_eq!(
        cluster.store.get(&binding).await?.bytes().await?,
        expected_binding
    );
    assert_eq!(
        cluster.store.get(&identity).await?.bytes().await?,
        expected_identity
    );
    assert!(cluster.store.get(&counter).await.is_err());
    // A lost binding cannot be inferred from whichever mutable identity remains.
    cluster.store.delete(&binding).await?;
    assert!(directory
        .open_group_archive(&cluster.domain, &pool, &birth)
        .await
        .is_err());
    assert!(cluster.store.get(&binding).await.is_err());
    cluster.close().await
}
