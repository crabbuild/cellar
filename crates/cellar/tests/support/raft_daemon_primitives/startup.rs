// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Compiled CLI namespace provisioning, capacity and joined daemon startup.
use super::*;

fn operators(domain: Arc<ExecutionDomain>, root: &Path) -> Result<Vec<Arc<Operator>>> {
    let application = application::application()?;
    application.validate_deployment(&application::deployment().primary)?;
    [cellar::deploy::SQL_CLASS, cellar::deploy::KV_CLASS]
        .into_iter()
        .enumerate()
        .map(|(index, class)| {
            let class = application.resolve_raft_class(class)?;
            let mut operator = Operator::open_for_namespace(
                domain.clone(),
                root,
                &class,
                group(index)?.namespace,
            )?;
            operator.namespace.minimum_durability = minimum()?;
            Ok(Arc::new(operator))
        })
        .collect()
}

pub(super) async fn initialize(root: &Path) -> Result<()> {
    let domain = Arc::new(ExecutionDomain::new(tokio::runtime::Handle::current()));
    let result = domain
        .run(async {
            let operators = operators(domain.clone(), root)?;
            let primary = &operators[0];
            if docker::enabled() {
                primary
                    .fleet
                    .initialize_with_replica_separation(FleetReplicaSeparation::Nodes, TIMEOUT)
                    .await?;
                persist(
                    root,
                    DOCKER_CATALOG,
                    hex::encode(primary.fleet.catalog_id()?).as_bytes(),
                )?;
            } else {
                primary.fleet.initialize(TIMEOUT).await?;
            }
            let mut declarations = Vec::new();
            for (index, operator) in operators.iter().enumerate() {
                let snapshot = operator.fleet.read(TIMEOUT).await?;
                let identity = FleetRequestIdentity {
                    operation: [u8::try_from(70 + index)?; 16],
                    ..request(u8::try_from(10 + index * 4)?, snapshot.revision())
                };
                let declaration = provision_compiled_namespace(
                    root,
                    operator,
                    &identity,
                    "provision-primitives",
                    application::configuration(),
                )
                .await?;
                declarations.push(declaration);
            }
            let capability = RaftManagedHostOptions::capability_digest(&declarations)?;
            for index in 0..3 {
                let snapshot = primary.fleet.read(TIMEOUT).await?;
                primary
                    .fleet
                    .register_node(
                        &snapshot,
                        &request(11 + index, snapshot.revision()),
                        &node_spec(index, capability)?,
                        TIMEOUT,
                    )
                    .await?;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await;
    domain.shutdown().await;
    result?
}

async fn provision(operators: &[Arc<Operator>], service: &RaftCells, root: &Path) -> Result<()> {
    let primary = &operators[0];
    let mut directories = Vec::new();
    for operator in operators {
        let declaration = operator.declaration()?;
        let selected = operator
            .domain
            .run(operator.adapter.existing_namespace(declaration.namespace))
            .await??;
        directories.push(RangeDirectory::new(
            selected,
            configuration::limits().directory,
        )?);
    }
    let control = RaftFleetControl::new(&primary.domain, primary.fleet.clone(), directories)?
        .with_node_observer(service.clone());
    let cli = OperatorCli::start(
        &primary.domain,
        primary.bucket.clone(),
        control,
        root,
        PRINCIPAL,
    )
    .await?;
    let result = async {
        for index in 0..3 {
            admit_capacity(&cli, service, root, index).await?;
        }
        for index in 0..2 {
            provision_group_metadata_admitted(
                &cli,
                root,
                group(index)?,
                u8::try_from(30 + index * 10)?,
            )
            .await?;
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    cli.close().await?;
    result
}

pub(crate) fn child(root: PathBuf, mode: &str) -> Result<()> {
    let api = Arc::new(application::Api::default());
    let serving = api.clone();
    let host = Host::local(application::deployment()).register("raft-cli", "test", move || {
        application::application_with_api(serving.clone())
    })?;
    if mode == "provision-primitives" {
        return host.run();
    }
    ensure!(
        std::env::var(ENABLED)? == "1",
        "invalid primitive process gate"
    );
    let (retained, index) = match retained_index(mode) {
        Some(index) => (true, index.parse::<u8>()?),
        None => (
            false,
            mode.strip_prefix("cluster-")
                .context("primitive daemon mode")?
                .parse::<u8>()?,
        ),
    };
    ensure!(index < 3, "primitive voter index");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()?;
    let domain = Arc::new(ExecutionDomain::new(runtime.handle().clone()));
    let result = (|| {
        let operators = operators(domain.clone(), &root)?;
        let declarations = operators
            .iter()
            .map(|operator| operator.declaration())
            .collect::<Result<Vec<_>>>()?;
        let primary = operators[0].clone();
        let prepared_root = root.clone();
        let boot_mode = mode.to_owned();
        let prepared = primary.clone();
        let store = Arc::new(archive::Gate::new(
            primary.bucket.raft_store(),
            &root,
            index,
        ));
        let host = host.raft_cells_with_startup(
            "raft-cli",
            move |native| async move {
                let mut cells = prepared
                    .prepare_declarations(native, index, retained, false, store, declarations)
                    .await?;
                cells.config.shared.sql.workers = 1;
                cells.config.shared.payloads = Some(CellGroupPayloadLimits {
                    max_operations: 2,
                    max_retained_bytes: 600 * 1024 * 1024,
                    max_payload_bytes: 64 * 1024 * 1024 + 4096,
                    io_timeout: Duration::from_secs(10),
                });
                Ok(cells)
            },
            move |service| async move {
                persist(
                    &prepared_root,
                    &format!("primitives-boot-{boot_mode}"),
                    std::env::var(BOOT_TOKEN)?.as_bytes(),
                )?;
                if index == 0 && !retained {
                    provision(&operators, &service, &prepared_root).await?;
                }
                for namespace in 0..2 {
                    let expected = group(namespace)?;
                    let selected = service
                        .start_managed_namespace_first_group(
                            expected.cluster,
                            expected.namespace,
                            Duration::from_secs(20),
                        )
                        .await?;
                    ensure!(selected == expected, "primitive managed history differs");
                }
                let usage = service.resource_usage();
                ensure!(
                    usage.host.controller.groups == 2,
                    "primitive daemon did not install both shared groups"
                );
                let cut = primary.fleet.read(TIMEOUT).await?;
                for peer in 0..3 {
                    inspect_current(&service, node(peer)?, cut.revision()).await?;
                    if peer != index {
                        for namespace in 0..2 {
                            let history = group(namespace)?;
                            service.grant_managed_query_origin(
                                history.cluster,
                                history.namespace,
                                node(peer)?,
                                1,
                            )?;
                            service.grant_managed_command_origin(
                                history.cluster,
                                history.namespace,
                                node(peer)?,
                                1,
                            )?;
                        }
                    }
                }
                if boot_mode == "cluster-resume-0" {
                    persist(&prepared_root, "primitive-startup-held", b"1")?;
                    while !tokio::fs::try_exists(prepared_root.join("primitive-startup-release"))
                        .await?
                    {
                        tokio::time::sleep(Duration::from_millis(25)).await;
                    }
                }
                service.observe_node(TIMEOUT).await?;
                persist(
                    &prepared_root,
                    &format!("primitives-ready-{boot_mode}"),
                    std::process::id().to_string().as_bytes(),
                )?;
                *api.cells
                    .lock()
                    .map_err(|_| anyhow::anyhow!("primitive binding poisoned"))? = Some(service);
                Ok(())
            },
        )?;
        host.run_until(std::future::pending())
    })();
    runtime.block_on(domain.shutdown());
    result
}
