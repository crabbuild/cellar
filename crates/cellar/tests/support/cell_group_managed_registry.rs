// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Declaration registry replay and corruption oracles on the managed fixture.
use super::*;
use cellar::application::consensus::verification::{
    FleetChange, FleetMutation, FleetTable, NamespaceCatalog,
};
use object_store::{path::Path as ObjectPath, ObjectStore};

async fn open_namespace(f: &Fixture, id: u8, prefix: &str) -> Result<NamespaceCatalog> {
    let mut config = f.declaration.namespace.clone();
    config.namespace = NamespaceId::new([id; 16])?;
    let catalog = Catalog::new(
        f.store.clone(),
        CatalogOptions {
            prefix: prefix.into(),
            ..Default::default()
        },
    )?;
    Ok(f.domain.run(catalog.open_namespace(config)).await??)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_inventory_registry_retries_and_scope() -> Result<()> {
    let f = Fixture::new(false).await?;
    let result = async {
        let other = open_namespace(&f, 88, "managed/").await?;
        let root = f.catalog.read(TIMEOUT).await?;
        let request = request(90, root.revision());
        f.store.fail_fleet_after.store(true, Ordering::Release);
        f.store
            .fail_fleet_confirmation
            .store(true, Ordering::Release);
        ensure!(f
            .catalog
            .declare_namespace(&root, &request, &other, "Counter", TIMEOUT)
            .await
            .is_err());
        f.store.fail_fleet_after.store(false, Ordering::Release);
        f.store
            .fail_fleet_confirmation
            .store(false, Ordering::Release);
        let third = open_namespace(&f, 89, "managed/").await?;
        let later = f.catalog.read(TIMEOUT).await?;
        f.catalog
            .declare_namespace(
                &later,
                &super::request(91, later.revision()),
                &third,
                "Counter",
                TIMEOUT,
            )
            .await?;
        let reopened =
            Fixture::open_catalog(&f.domain, f.store.clone(), f.declaration.namespace.cluster)?;
        let now = reopened.read(TIMEOUT).await?;
        let reply = reopened
            .declare_namespace(&now, &request, &other, "Counter", TIMEOUT)
            .await?;
        ensure!(
            reply.applied_revision < now.revision() && reply.snapshot.revision() == now.revision()
        );
        ensure!(matches!(
            reopened
                .declare_namespace(&now, &request, &other, "Different", TIMEOUT)
                .await,
            Err(FleetCatalogError::PayloadConflict)
        ));
        let conflict = super::request(92, now.revision());
        ensure!(matches!(
            reopened
                .declare_namespace(&now, &conflict, &other, "Different", TIMEOUT)
                .await,
            Err(FleetCatalogError::Conflict)
        ));
        let wrong = open_namespace(&f, 88, "foreign-data/").await?;
        ensure!(matches!(
            reopened
                .declare_namespace(&now, &conflict, &wrong, "Counter", TIMEOUT)
                .await,
            Err(FleetCatalogError::Conflict)
        ));
        Ok::<_, anyhow::Error>(())
    }
    .await;
    f.domain.shutdown().await;
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_inventory_registry_no_reset() -> Result<()> {
    let f = Fixture::new(false).await?;
    let result = async {
        let root = f.catalog.read(TIMEOUT).await?;
        let original = f
            .catalog
            .get(&root, FleetTable::Policy, "compiled-namespaces", TIMEOUT)
            .await?
            .context("registry")?;
        let request = request(90, root.revision());
        let deletion = FleetMutation {
            id: request.id,
            operation: request.operation,
            principal: request.principal,
            expected_revision: request.expected_revision,
            changes: vec![FleetChange {
                table: FleetTable::Policy,
                key: "compiled-namespaces".into(),
                value: None,
            }],
        };
        ensure!(matches!(
            f.catalog.commit(&root, &deletion, TIMEOUT).await,
            Err(FleetCatalogError::Invalid(_))
        ));
        ensure!(f.catalog.read(TIMEOUT).await?.revision() == root.revision());
        let value: serde_json::Value = serde_json::from_slice(&original[13..original.len() - 32])?;
        let index: Vec<u8> = serde_json::from_value(value["index"].clone())?;
        let page = ObjectPath::from(format!(
            "tenant/hosts/raft/{}/fleet/objects/{}",
            hex::encode(f.declaration.namespace.cluster.bytes()),
            hex::encode(index)
        ));
        let saved = f.store.inner.get(&page).await?.bytes().await?;
        f.store.inner.delete(&page).await?;
        let other = open_namespace(&f, 88, "managed/").await?;
        let request = super::request(91, root.revision());
        ensure!(matches!(
            f.catalog
                .declare_namespace(&root, &request, &other, "Counter", TIMEOUT)
                .await,
            Err(FleetCatalogError::Corrupt(_))
        ));
        ensure!(f.catalog.read(TIMEOUT).await?.revision() == root.revision());
        f.store.inner.put(&page, saved.into()).await?;
        f.catalog
            .declare_namespace(&root, &request, &other, "Counter", TIMEOUT)
            .await?;
        let root = f.catalog.read(TIMEOUT).await?;
        let request = super::request(92, root.revision());
        let rollback = FleetMutation {
            id: request.id,
            operation: request.operation,
            principal: request.principal,
            expected_revision: request.expected_revision,
            changes: vec![FleetChange {
                table: FleetTable::Policy,
                key: "compiled-namespaces".into(),
                value: Some(original),
            }],
        };
        ensure!(matches!(
            f.catalog.commit(&root, &rollback, TIMEOUT).await,
            Err(FleetCatalogError::Invalid(_))
        ));
        ensure!(f.catalog.read(TIMEOUT).await?.revision() == root.revision());
        Ok::<_, anyhow::Error>(())
    }
    .await;
    f.domain.shutdown().await;
    result
}
