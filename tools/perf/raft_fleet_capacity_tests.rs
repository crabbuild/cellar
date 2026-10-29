// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Native admission and the lab operator wrapper share the canonical receipt.
use super::*;
#[allow(dead_code)]
#[path = "../../crates/cellar/tests/support/raft_node.rs"]
mod public_node;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn managed_capacity_retry_reconciles_before_platform_or_native_probes() -> Result<()> {
    Box::pin(capacity_retry()).await
}

async fn capacity_retry() -> Result<()> {
    let root = tempfile::tempdir()?;
    let bucket = bucket()?;
    let class = counter::definition()?;
    let namespace = NamespaceConfig::new(
        ClusterId::new([1; 16])?,
        NamespaceId::new([2; 16])?,
        NamespaceMode::Raft,
        class.schema_digest(),
    );
    let counter = counter::CounterApi::new(
        "fixture-token-capacity-retry".into(),
        namespace.namespace,
        &class,
    )?;
    let application = counter::application(counter, class)?;
    let managed =
        Arc::new(Managed::open(bucket.clone(), config(), namespace, "Counter", true).await?);
    managed.bootstrap().await?;
    let node = public_node::node_named(root.path(), application.clone(), "raft-node1").await?;
    let listeners = NodeRaftListeners {
        control: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
        data: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
        query: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
    };
    let id = NodeId::new([1; 16])?;
    let disk = DiskLineage::new([11; 16])?;
    let process = ProcessGeneration::new([21; 16])?;
    let mut options = RaftCellsConfig {
        query_origins: vec![],
        cells: vec![],
        host: RaftHostOptions::default(),
        catalog: Managed::catalog_options(),
        peers: vec![RaftPeerNode {
            node: id,
            disk,
            process,
            label: "raft-node1".into(),
            server_name: "localhost".into(),
            control_address: listeners.control.local_addr()?,
            data_address: listeners.data.local_addr()?,
            query_address: listeners.query.local_addr()?,
        }],
        infrastructure: NodeRaftConfig {
            log: ReplicaLogOptions::new(
                node.info().data_dir.join("raft"),
                id,
                disk,
                process,
                LogOpenMode::Create,
            ),
            consensus: RaftRuntimeOptions::default(),
            transport: TransportOptions::default(),
        },
    };
    managed.configure(&mut options, id).await?;
    options = options
        .with_extent_image_limit(managed.config.image_bytes, managed.config.extent_options())?;
    let cells = node
        .start_raft_cells(
            options,
            RaftCellsServices {
                listeners,
                tls: RaftTls::new(
                    vec![
                        include_bytes!("../../crates/cellar/transport/tcp/fixtures/ca.der")
                            .to_vec()
                            .into(),
                    ],
                    vec![
                        include_bytes!("../../crates/cellar/transport/tcp/fixtures/server.der")
                            .to_vec()
                            .into(),
                    ],
                    rustls::pki_types::PrivatePkcs8KeyDer::from(
                        include_bytes!("../../crates/cellar/transport/tcp/fixtures/key.der")
                            .to_vec(),
                    )
                    .into(),
                )?,
                store: bucket.raft_store(),
                application,
                versions: VersionKeys::new(
                    VersionSigningKey::new([29; 32], u64::MAX)?,
                    vec![],
                    Duration::from_secs(86400),
                )?,
            },
        )
        .await?;
    let isolation = FleetDockerIsolation::new(
        managed.catalog.clone(),
        Docker::new(root.path().join("missing-engine.sock")),
        &managed.config.owner,
        vec![managed.namespace.clone()],
    )?;
    let resources = FleetDockerResources::new(
        isolation.clone(),
        cells.clone(),
        managed.config.resources.clone(),
    )?;
    let api = Api {
        managed: managed.clone(),
        cells: cells.clone(),
        isolation,
        resources,
        node: id,
        container: ContainerId::new(
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        )?,
        turn: Mutex::new(()),
        recovery: std::sync::Mutex::new(None),
    };
    let view = wait_trust(&managed, &cells, id).await?.view;
    // Trusted fixture inputs exercise admission, not measured platform qualification.
    let evidence = FleetCapacityAttestation {
        boot: view.fleet_boot.context("missing native boot")?,
        profile_revision: 1,
        profile_digest: fleet_capacity_profile_digest(&view.node.profile)?,
        cpu_millis: 1000,
        memory_bytes: 3 * 1024 * 1024 * 1024,
        durable_bytes: managed.config.durable_bytes,
        engine_slots: managed.config.group_capacity,
        memory_swap_disabled: true,
        used_memory_bytes: view.startup_bounds.bounded_buffer_bytes,
        used_durable_bytes: managed.config.resources.durable_overhead_bytes,
        evidence_digest: [71; 32],
        evidence_reference: "fixture-only/capacity-retry".into(),
    };
    let cut = managed.catalog.read(TIMEOUT).await?;
    api.command(Command::Capacity {
        request: Box::new(FleetCapacityRequest {
            identity: managed.config.request(101, cut.revision()),
            attestation: evidence.clone(),
            action: FleetCapacityAction::Qualify,
        }),
    })
    .await
    .context("qualify native fixture through lab API")?;
    wait_trust(&managed, &cells, id).await?;
    let cut = managed.catalog.read(TIMEOUT).await?;
    let request = FleetCapacityRequest {
        identity: managed.config.request(102, cut.revision()),
        attestation: evidence,
        action: FleetCapacityAction::Activate,
    };
    // A new activation must refuse the missing Engine, retaining Ready and the root.
    ensure!(api
        .command(Command::Capacity {
            request: Box::new(request.clone())
        })
        .await
        .is_err());
    ensure!(managed.catalog.read(TIMEOUT).await?.revision() == cut.revision());
    ensure!(
        managed
            .catalog
            .authorize_host(&cut, id, TIMEOUT)
            .await?
            .state()
            == FleetNodeState::Ready
    );
    // Prepare an acknowledged native admission whose response the lab caller lost.
    // This direct fixture setup is not a substitute for the wrapper's platform check.
    managed
        .catalog
        .admit_node_capacity(
            &cut,
            &request,
            &cells,
            std::slice::from_ref(&managed.namespace),
            TIMEOUT,
        )
        .await
        .context("prepare committed activation receipt")?;
    let applied = managed.catalog.read(TIMEOUT).await?.revision();
    let NodeOutcome::Stopped(report) = node.shutdown(ShutdownMode::Preserve).await? else {
        anyhow::bail!("native fixture did not join");
    };
    ensure!(report
        .raft
        .context("missing raft close report")?
        .result?
        .unfinished
        .is_empty());
    // Neither a live process nor an Engine is available. Only the original
    // committed receipt may succeed; a changed payload must still be refused.
    let response = api
        .command(Command::Capacity {
            request: Box::new(request.clone()),
        })
        .await
        .context("reconcile committed activation after native shutdown")?;
    ensure!(response["revision"] == applied && response["state"] == "Active");
    let mut changed = request;
    changed.attestation.cpu_millis += 1;
    ensure!(api
        .command(Command::Capacity {
            request: Box::new(changed)
        })
        .await
        .is_err());
    ensure!(managed.catalog.read(TIMEOUT).await?.revision() == applied);
    managed.domain.shutdown().await;
    Ok(())
}

async fn wait_trust(
    managed: &Managed,
    cells: &RaftCells,
    node: NodeId,
) -> Result<RaftPeerNodeObservation> {
    let revision = managed.catalog.read(TIMEOUT).await?.revision();
    managed
        .domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            loop {
                match cells.inspect_peer_node(node, revision, TIMEOUT).await {
                    Ok(view) => return Ok(view),
                    Err(InspectionError::StalePeer | InspectionError::Refused) => {
                        cellar::asyncrt::sleep(Duration::from_millis(10)).await;
                    }
                    Err(error) => return Err(error.into()),
                }
            }
        }))
        .await??
}
