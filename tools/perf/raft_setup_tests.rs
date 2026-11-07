// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Exercise the measurement configuration through real public Node owners.
use super::*;
use crate::public_node;

fn tls() -> Result<RaftTls> {
    Ok(RaftTls::new(
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
            include_bytes!("../../crates/cellar/transport/tcp/fixtures/key.der").to_vec(),
        )
        .into(),
    )?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_runner_owns_rf3_enrollment_archives_and_follower_reads() -> Result<()> {
    for (total, occupancy) in [(3, 1), (33, 32), (129, 128)] {
        Box::pin(run(total, occupancy, None, false)).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_runner_rooted32_archive_diagnostics() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(
            "cellar::raft::group_archive=debug",
        ))
        .with_ansi(false)
        .with_test_writer()
        .try_init();
    Box::pin(run(33, 32, None, false)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_runner_reclaims_live_rooted_images_within_the_original_request_budget() -> Result<()>
{
    Box::pin(run(3, 2, Some(16), false)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_runner_keeps_a_bounded_warm_verifier_cache_between_jobs() -> Result<()> {
    Box::pin(run(3, 2, None, true)).await
}

fn namespace_for(
    layout: &crate::raft_groups::Layout,
    schema: [u8; 32],
    rooted: bool,
) -> Result<RaftNamespaceConfig> {
    let mut config = namespace(layout, schema)?;
    if !rooted {
        config.namespace.image_storage = RaftImageStorage::Materialized;
    }
    Ok(config)
}

async fn leader(services: &[RaftCells], history: GroupHistory) -> Result<usize> {
    // The measurement runner chooses an observed leader before each HTTP
    // command. Match that contract; do not assume the first process wins.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    tokio::time::timeout_at(deadline, async {
        loop {
            for (index, service) in services.iter().enumerate() {
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                let observed = service.group(history)?.observe(remaining).await?;
                if observed.status.ready
                    && observed.status.leader == Some(observed.replica.member.get())
                {
                    return Ok::<_, anyhow::Error>(index);
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?
}

async fn run(
    total: usize,
    occupancy: usize,
    root_limit: Option<usize>,
    warm_cache: bool,
) -> Result<()> {
    let target =
        PathBuf::from(std::env::var_os("CARGO_TARGET_DIR").context("checkout target directory")?);
    let retained = target.join("qualification-evidence/perf-runner-native");
    std::fs::create_dir_all(&retained)?;
    let root = tempfile::Builder::new()
        .prefix(&format!("occupancy-{occupancy}-"))
        .tempdir_in(retained)?
        .keep();
    let layout = Arc::new(crate::raft_groups::Layout::new(
        total, occupancy, "Counter", 0,
    )?);
    let placement = Placement::new(3)?;
    let class = counter::definition()?;
    let schema = class.schema_digest();
    let selected = layout.clone();
    let api = counter::CounterApi::with_history_resolver(
        "fixture-shared-perf-secret".into(),
        &class,
        move |name| selected.named_history(name),
    )?;
    let increment = api.increment.clone();
    let get = api.get.clone();
    let application = counter::application(api, class)?;
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let mut peers = Vec::new();
    let mut listeners = Vec::new();
    for node in 1..=3 {
        let bound = NodeRaftListeners {
            control: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
            data: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
            query: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
        };
        peers.push(RaftPeerNode {
            node: NodeId::new([node; 16])?,
            disk: DiskLineage::new([node + 10; 16])?,
            process: ProcessGeneration::new([node + 20; 16])?,
            label: format!("raft-node{node}"),
            server_name: "localhost".into(),
            control_address: bound.control.local_addr()?,
            data_address: bound.data.local_addr()?,
            query_address: bound.query.local_addr()?,
        });
        listeners.push(bound);
    }
    let rooted = occupancy > 1;
    let mut host = RaftHostOptions {
        max_groups: layout.groups.len(),
        ..Default::default()
    };
    if rooted {
        let mut extents = crate::raft_limits::extents(&host)?;
        if let Some(limit) = root_limit {
            extents.inventory.max_roots = limit;
        }
        host.extents = Some(extents);
    }
    host.close_report.max_groups = host.max_groups;
    host.node_profile = Some(RaftNodeProfile {
        pool: "static-perf".into(),
        region: "local".into(),
        zone: "shared-test".into(),
        group_capacity: u32::try_from(host.max_groups)?,
        byte_capacity: host.images.max_bytes,
        bytes_per_group: host.images.max_bytes,
        leader_capacity: u32::try_from(host.max_groups)?,
        draining: false,
    });
    let shared = crate::raft_limits::shared(&host, layout.groups.len(), occupancy)?;
    prepare(
        &root.join("operator"),
        store.clone(),
        namespace_for(&layout, schema, rooted)?,
        &layout,
        &peers,
        true,
        shared,
    )
    .await?;
    // Retrying exact setup cannot allocate another range or archive identity.
    prepare(
        &root.join("operator"),
        store.clone(),
        namespace_for(&layout, schema, rooted)?,
        &layout,
        &peers,
        true,
        shared,
    )
    .await?;
    let mut nodes = Vec::new();
    let mut services = Vec::new();
    let result = async {
        for (ordinal, listeners) in listeners.into_iter().enumerate() {
            let node = public_node::node_named(
                &root.join(format!("node-{ordinal}")),
                application.clone(),
                &peers[ordinal].label,
            )
            .await?;
            nodes.push(node);
            let node = nodes.last().context("owned node")?;
            let config = RaftCellsConfig {
                infrastructure: NodeRaftConfig {
                    log: ReplicaLogOptions::new(
                        node.info().data_dir.join("raft"),
                        peers[ordinal].node,
                        peers[ordinal].disk,
                        peers[ordinal].process,
                        LogOpenMode::Create,
                    ),
                    consensus: RaftRuntimeOptions {
                        max_groups: host.max_groups,
                        tick_interval_ms: Some(100),
                        ..Default::default()
                    },
                    transport: TransportOptions::default(),
                },
                catalog: catalog_options(),
                namespaces: vec![namespace_for(&layout, schema, rooted)?],
                groups: layout
                    .groups
                    .iter()
                    .map(|g| RaftGroupAssignment {
                        history: g.history,
                        placement: RaftGroupPlacement::Replica,
                    })
                    .collect(),
                peers: peers.clone(),
                query_origins: vec![],
                command_origins: vec![],
                shared,
                host: host.clone(),
            };
            services.push(
                node.start_raft_cells(
                    config,
                    RaftCellsServices {
                        recovery: None,
                        listeners,
                        tls: tls()?,
                        store: store.clone(),
                        application: application.clone(),
                        versions: VersionKeys::new(
                            VersionSigningKey::new([29; 32], u64::MAX)?,
                            vec![],
                            Duration::from_secs(86400),
                        )?,
                    },
                )
                .await?,
            );
        }
        let enrollment = futures_util::future::try_join_all(services.iter().enumerate().map(
            |(node, service)| {
                enroll(
                    service,
                    &layout,
                    placement,
                    node as u8 + 1,
                    Duration::from_secs(60),
                )
            },
        ))
        .await;
        if let Err(error) = enrollment {
            let mut observations = String::new();
            for (node, service) in services.iter().enumerate() {
                for (ordinal, group) in layout.groups.iter().enumerate() {
                    observations.push_str(&format!(
                        "node {node} group {ordinal}: {:?}\n",
                        service
                            .group(group.history)?
                            .observe(Duration::from_secs(5))
                            .await
                    ));
                }
            }
            std::fs::write(root.join("enrollment-observations.txt"), &observations)?;
            anyhow::bail!("enrollment failed: {error:#}; {observations}");
        }
        let mut last = vec![0; layout.groups.len()];
        for cell in 0..total {
            last[layout.group_ordinal(cell)?] = cell;
        }
        for cell in 0..total {
            let history = layout.history(cell)?;
            let group = layout.group_ordinal(cell)?;
            let origin = leader(&services, layout.group(cell)?.history).await?;
            let handle = services[origin].cell(history.clone())?;
            let mut options = CommandOptions::new(
                CommandId::new(ClientId::new([41; 16])?, 1)?,
                u64::try_from(cellar::asyncrt::wall_ms())? + 60000,
            );
            if last[group] == cell {
                options.durability = Durability::S3;
            }
            let input = (cell as i64 + 1).to_be_bytes().to_vec();
            let reply = handle
                .command(&increment, input.clone(), options.clone())
                .await
                .with_context(|| {
                    format!(
                        "cell {cell} group {group} durability {:?}",
                        options.durability
                    )
                })?;
            ensure!(
                reply.durability == options.durability
                    && reply.version.history == history
                    && reply.version.sequence.get() == 2
                    && reply.witness.group == layout.group(cell)?.history,
                "logical history or physical witness changed: achieved {:?}, sequence {}",
                reply.durability,
                reply.version.sequence.get()
            );
            ensure!(
                reply.witness.position.index.get() > reply.version.sequence.get(),
                "physical index was replaced by logical sequence"
            );
            let duplicate = handle
                .command(&increment, input, options)
                .await
                .with_context(|| format!("duplicate cell {cell}"))?;
            ensure!(
                duplicate.result == reply.result
                    && duplicate.version == reply.version
                    && duplicate.witness == reply.witness,
                "duplicate advanced the logical cell"
            );
            for (ordinal, service) in services.iter().enumerate() {
                let mut id = [42; 16];
                id[..8].copy_from_slice(&(cell as u64 + 1).to_be_bytes());
                id[8] = ordinal as u8 + 1;
                let observed = service
                    .cell(history.clone())?
                    .query(
                        ProposalId::new(id)?,
                        &get,
                        vec![],
                        QueryOptions {
                            consistency: QueryConsistency::AtLeast(reply.version_token.clone()),
                            ..Default::default()
                        },
                    )
                    .await?;
                ensure!(
                    observed.result == reply.result
                        && observed.version == reply.version
                        && observed.served_by.get() == ordinal as u64 + 1,
                    "follower lost independent cell state"
                );
            }
        }
        // Preserve the original 64-image bank while crossing its cumulative
        // preview count. Completed private work must not retain every preview.
        let mut pressure = None;
        if !rooted || root_limit.is_some() {
            for sequence in 2..=81 {
                let origin = leader(&services, layout.group(0)?.history).await?;
                let handle = services[origin].cell(layout.history(0)?)?;
                let mut options = CommandOptions::new(
                    CommandId::new(ClientId::new([41; 16])?, sequence)?,
                    u64::try_from(cellar::asyncrt::wall_ms())? + 60000,
                );
                if sequence == 81 {
                    options.durability = Durability::S3;
                }
                let reply = handle
                    .command(&increment, 1_i64.to_be_bytes().to_vec(), options.clone())
                    .await?;
                ensure!(
                    reply.version.sequence.get() == sequence + 1
                        && i64::from_be_bytes(
                            reply
                                .result
                                .as_slice()
                                .try_into()
                                .map_err(|_| anyhow::anyhow!("counter width"))?
                        ) == sequence as i64,
                    "private preview pressure changed a committed result"
                );
                pressure = Some((options, reply));
            }
        }
        if let Some(limit) = root_limit {
            let (options, receipt) = pressure.context("rooted pressure receipt")?;
            let origin = leader(&services, layout.group(0)?.history).await?;
            let duplicate = services[origin]
                .cell(layout.history(0)?)?
                .command(&increment, 1_i64.to_be_bytes().to_vec(), options)
                .await?;
            ensure!(
                duplicate.id == receipt.id
                    && duplicate.result == receipt.result
                    && duplicate.version == receipt.version
                    && duplicate.witness == receipt.witness
                    && duplicate.durability == Durability::S3,
                "collection changed the stored result or its S3 proof"
            );
            for (ordinal, service) in services.iter().enumerate() {
                let mut id = [43; 16];
                id[8] = ordinal as u8 + 1;
                let observed = service
                    .cell(layout.history(0)?)?
                    .query(
                        ProposalId::new(id)?,
                        &get,
                        vec![],
                        QueryOptions {
                            consistency: QueryConsistency::AtLeast(receipt.version_token.clone()),
                            ..Default::default()
                        },
                    )
                    .await?;
                ensure!(
                    observed.result == receipt.result
                        && observed.version == receipt.version
                        && observed.served_by.get() == ordinal as u64 + 1,
                    "collection lost the latest committed image on a voter"
                );
                let resources = tokio::time::timeout(Duration::from_secs(5), async {
                    loop {
                        let resources = service.resource_usage();
                        if resources
                            .serving_images
                            .extents
                            .is_some_and(|images| images.objects <= limit)
                        {
                            break resources;
                        }
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                })
                .await
                .context("collector did not drain obsolete image objects")?;
                let collection = resources
                    .collection
                    .context("owned collection observation")?;
                let images = resources
                    .serving_images
                    .extents
                    .context("serving extents")?;
                ensure!(
                    collection.running
                        && collection.files_reclaimed > 0
                        && collection.failures == 0,
                    "node collection did not make healthy physical progress: {collection:?}"
                );
                ensure!(
                    images.max_roots == limit
                        && images.roots <= images.max_roots
                        && images.objects <= limit,
                    "root pressure was escaped by raising the configured inventory"
                );
            }
        }
        if warm_cache {
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let mut caught_up = true;
                    for service in &services {
                        for group in &layout.groups {
                            let handle = service.group(group.history)?;
                            let observed = handle.observe(Duration::from_secs(5)).await?;
                            let archive =
                                handle.archive_status()?.context("archive observation")?;
                            caught_up &= archive.error.is_none()
                                && archive.published.as_ref().is_some_and(|marker| {
                                    marker.position() == observed.status.applied
                                });
                        }
                    }
                    if caught_up {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                // Give the ordinary collector twenty idle polls after all
                // archive jobs have completed and released verification pins.
                tokio::time::sleep(Duration::from_millis(500)).await;
                for service in &services {
                    let resources = service.resource_usage();
                    let images = resources
                        .archive_verifier_images
                        .and_then(|images| images.extents)
                        .context("verifier image accounting")?;
                    ensure!(
                        images.roots > 0 && images.objects > 0,
                        "idle verifier discarded its entire warm cache: {images:?}"
                    );
                    ensure!(
                        images.roots <= images.max_roots
                            && images.objects <= images.max_objects
                            && images.bytes <= images.max_bytes,
                        "warm verifier escaped its physical inventory bounds"
                    );
                }
                Ok::<_, anyhow::Error>(())
            })
            .await??;
        }
        for service in &services {
            let observed = service.observe_node(Duration::from_secs(5)).await?;
            ensure!(
                observed.node.groups as usize == layout.groups.len()
                    && observed.registered_histories as usize == layout.groups.len(),
                "physical group population changed"
            );
            for group in &layout.groups {
                let observed = service
                    .group(group.history)?
                    .observe(Duration::from_secs(5))
                    .await?;
                ensure!(
                    observed.cells as usize == group.cells,
                    "applied occupancy differs from frozen range"
                );
            }
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    if let Err(error) = &result {
        eprintln!("retained failure {}: {error:#}", root.display());
        let mut diagnostic = String::new();
        for (ordinal, service) in services.iter().enumerate() {
            {
                diagnostic.push_str(&format!(
                    "node {ordinal} resources {}\n",
                    serde_json::to_string(&service.resource_usage())?
                ));
                for (group, planned) in layout.groups.iter().enumerate() {
                    let handle = service.group(planned.history)?;
                    diagnostic.push_str(&format!(
                        "node {ordinal} group {group} observed {:?}\n",
                        handle.observe(Duration::from_secs(5)).await
                    ));
                    if let Some(archive) = handle.archive_status()? {
                        diagnostic.push_str(&format!(
                            "archive attempts {} stopped {} published {:?} error {:?}\n",
                            archive.attempts,
                            archive.stopped,
                            archive.published.map(|m| m.position()),
                            archive.error
                        ));
                    }
                }
            }
        }
        std::fs::write(root.join("failure-observations.txt"), &diagnostic)?;
        eprintln!("{diagnostic}");
    }
    let mut cleanup = Ok(());
    for node in nodes {
        let joined = async {
            let NodeOutcome::Stopped(report) = node.shutdown(ShutdownMode::Preserve).await? else {
                anyhow::bail!("native Node cleanup has not joined");
            };
            let closed = report.raft.context("native close report")?.result?;
            ensure!(
                closed.omitted_groups == 0 && closed.groups_total == layout.groups.len(),
                "incomplete joined group inventory"
            );
            Ok::<_, anyhow::Error>(())
        }
        .await;
        if let Err(error) = joined {
            cleanup = Err(error);
        }
    }
    result.with_context(|| {
        format!(
            "shared runner occupancy {occupancy}; retained {}",
            root.display()
        )
    })?;
    cleanup?;
    Ok(())
}
