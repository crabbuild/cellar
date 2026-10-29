use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_registry_compiled_namespace_gate() -> Result<()> {
    let f = Fixture::new().await?;
    let mut other = f.declaration.clone();
    other.namespace.namespace = NamespaceId::new([88; 16])?;
    let namespace = Catalog::new(
        f.store.clone(),
        CatalogOptions {
            prefix: "host-enrollment/".into(),
            ..Default::default()
        },
    )?
    .open_namespace(other.namespace.clone())
    .await?;
    let declarations = vec![f.declaration.clone(), other.clone()];
    let mut spec = f.spec.clone();
    spec.capability_digest = RaftManagedHostOptions::capability_digest(&declarations)?;
    let root = f.catalog.read(TIMEOUT).await?;
    let committed = f
        .catalog
        .register_node(&root, &Fixture::request(3, root.revision()), &spec, TIMEOUT)
        .await?;
    let authorization = f
        .catalog
        .authorize_host(&committed.snapshot, spec.node, TIMEOUT)
        .await?;
    let local = tempfile::tempdir()?;
    let node = public_node::node_named(local.path(), f.definition.application.clone(), &spec.label)
        .await?;
    let (mut config, services) = f.config_for(&node, authorization.clone(), &spec).await?;
    config.host.managed.as_mut().unwrap().namespaces = declarations.clone();
    let error = node
        .start_raft_cells(config, services)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("unregistered compiled namespace booted"))?;
    ensure!(
        format!("{error:#}").contains("InventoryIncomplete"),
        "unexpected refusal: {error:#}"
    );
    ensure!(f.catalog.read(TIMEOUT).await?.revision() == committed.snapshot.revision());
    ensure!(f
        .catalog
        .authorize_host(&committed.snapshot, spec.node, TIMEOUT)
        .await?
        .storage_instance()
        .is_none());
    node.shutdown(ShutdownMode::Preserve).await?;

    let root = f.catalog.read(TIMEOUT).await?;
    f.catalog
        .declare_namespace(
            &root,
            &Fixture::request(249, root.revision()),
            &namespace,
            "Counter",
            TIMEOUT,
        )
        .await?;
    let node = public_node::node_named(local.path(), f.definition.application.clone(), &spec.label)
        .await?;
    let root = f.catalog.read(TIMEOUT).await?;
    let authorization = f.catalog.authorize_host(&root, spec.node, TIMEOUT).await?;
    let (mut config, services) = f.config_for(&node, authorization, &spec).await?;
    config.host.managed.as_mut().unwrap().namespaces = declarations;
    config.infrastructure.log.mode = LogOpenMode::Resume;
    let process = ProcessGeneration::new([99; 16])?;
    config.infrastructure.log.process = process;
    config.peers[0].process = process;
    let host = node.start_raft_cells(config, services).await?;
    let view = host.observe_node(TIMEOUT).await?;
    ensure!(
        view.registered_histories == 0
            && view.node.groups == 0
            && view.startup_enrollment.unwrap().capability_digest == spec.capability_digest
    );
    node.shutdown(ShutdownMode::Preserve).await?;
    f.domain.shutdown().await;
    Ok(())
}
