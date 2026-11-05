//! Complete namespace enumeration is a prerequisite, not node absence proof.
use super::*;
use cellar::sdk::ShutdownMode;
use sha2::Digest;

async fn another_namespace(f: &ImportFixture, id: u8) -> Result<NamespaceCatalog> {
    let mut config = f.namespace.config().clone();
    config.namespace = NamespaceId::new([id; 16])?;
    Ok(Catalog::new(
        f.store.clone(),
        CatalogOptions {
            prefix: "partition/test".into(),
            ..Default::default()
        },
    )?
    .open_namespace(config)
    .await?)
}

async fn declare(
    f: &mut ImportFixture,
    namespace: &NamespaceCatalog,
) -> Result<FleetRequestIdentity> {
    let root = f.f.catalog.read(TIMEOUT).await?;
    let request = f.request(root.revision());
    f.f.catalog
        .declare_namespace(&root, &request, namespace, "Counter", TIMEOUT)
        .await?;
    Ok(request)
}

async fn complete_empty(f: &mut ImportFixture, namespace: &NamespaceCatalog) -> Result<()> {
    let cut =
        f.f.domain
            .run(namespace.freeze_births(ProposalId::new(OP)?, TIMEOUT))
            .await??;
    ensure!(cut.is_empty());
    let root = f.f.catalog.read(TIMEOUT).await?;
    let request = f.request(root.revision());
    f.f.catalog
        .begin_namespace_import(&root, &request, namespace, &cut, TIMEOUT)
        .await?;
    for _ in 0..4 {
        let root = f.f.catalog.read(TIMEOUT).await?;
        let request = f.request(root.revision());
        f.f.catalog
            .advance_namespace_import(&root, &request, namespace, None, TIMEOUT)
            .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_inventory_namespace_coverage() -> Result<()> {
    let mut f = ImportFixture::new(false).await?;
    f.begin().await?;
    f.complete().await?;
    let root = f.f.catalog.read(TIMEOUT).await?;
    let view =
        f.f.catalog
            .check_inventory(&root, std::slice::from_ref(&f.namespace), TIMEOUT)
            .await?;
    ensure!(view.revision() == root.revision() && view.namespaces() == 1 && view.histories() == 1);
    let other = another_namespace(&f, 88).await?;
    declare(&mut f, &other).await?;
    let root = f.f.catalog.read(TIMEOUT).await?;
    ensure!(matches!(
        f.f.catalog
            .check_inventory(&root, std::slice::from_ref(&f.namespace), TIMEOUT)
            .await,
        Err(FleetCatalogError::InventoryIncomplete)
    ));
    let both = [f.namespace.clone(), other.clone()];
    ensure!(matches!(
        f.f.catalog.check_inventory(&root, &both, TIMEOUT).await,
        Err(FleetCatalogError::InventoryIncomplete)
    ));
    // A descriptor alone cannot make this otherwise empty namespace complete.
    complete_empty(&mut f, &other).await?;
    let root = f.f.catalog.read(TIMEOUT).await?;
    let view = f.f.catalog.check_inventory(&root, &both, TIMEOUT).await?;
    ensure!(view.namespaces() == 2 && view.histories() == 1);
    ensure!(matches!(
        f.f.catalog
            .check_inventory(&root, &[f.namespace.clone(), f.namespace.clone()], TIMEOUT)
            .await,
        Err(FleetCatalogError::Invalid(_))
    ));
    ensure!(f.f.strong().await? == 5);
    f.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_inventory_retains_offline_and_missing_births() -> Result<()> {
    let mut f = ImportFixture::new(false).await?;
    let cancelled = f.another(88)?;
    for id in 88..103 {
        let cancelled = f.another(id)?;
        f.f.domain
            .run(
                f.namespace
                    .cancel_birth(cancelled, ProposalId::new([211; 16])?, TIMEOUT),
            )
            .await??;
    }
    f.begin().await?;
    f.complete().await?;
    let root = f.f.catalog.read(TIMEOUT).await?;
    // No endpoint/liveness scan substitutes for retained immutable inventory.
    f.f.cluster
        .nodes
        .remove(2)
        .shutdown(ShutdownMode::Preserve)
        .await?;
    let view =
        f.f.catalog
            .check_inventory(&root, std::slice::from_ref(&f.namespace), TIMEOUT)
            .await?;
    ensure!(view.histories() == 16);
    let path = Path::from(format!(
        "partition/test/namespaces/{}/cells/{}/birth",
        hex::encode(cancelled.scope.namespace.bytes()),
        hex::encode(sha2::Sha256::digest(cancelled.scope.cell.as_bytes()))
    ));
    let saved = f.store.inner.get(&path).await?.bytes().await?;
    f.store.inner.delete(&path).await?;
    ensure!(f
        .f
        .catalog
        .check_inventory(&root, std::slice::from_ref(&f.namespace), TIMEOUT)
        .await
        .is_err());
    f.store.inner.put(&path, saved.into()).await?;
    ensure!(
        f.f.catalog
            .check_inventory(&root, std::slice::from_ref(&f.namespace), TIMEOUT)
            .await?
            .histories()
            == 16
    );
    f.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_inventory_namespace_bound() -> Result<()> {
    let mut f = ImportFixture::new(false).await?;
    for id in 90..153 {
        let namespace = another_namespace(&f, id).await?;
        declare(&mut f, &namespace).await?;
    }
    let overflow = another_namespace(&f, 154).await?;
    let root = f.f.catalog.read(TIMEOUT).await?;
    let request = f.request(root.revision());
    ensure!(matches!(
        f.f.catalog
            .declare_namespace(&root, &request, &overflow, "Counter", TIMEOUT)
            .await,
        Err(FleetCatalogError::Limit)
    ));
    ensure!(f.f.catalog.read(TIMEOUT).await?.revision() == root.revision());
    let request = f.request(root.revision());
    ensure!(matches!(
        f.f.catalog
            .declare_namespace(&root, &request, &f.namespace, "Different", TIMEOUT)
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    f.close().await
}
