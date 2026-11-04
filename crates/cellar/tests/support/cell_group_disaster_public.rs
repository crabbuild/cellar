// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Public Node startup owns recovered activation and its shutdown lifetime.
//! These are real files and quorum RPCs; fencing is a contract fixture.
use super::*;
use cellar::sdk::{
    consensus::{
        CellGroupNodeArchiveOptions, CommandOptions, QueryConsistency, QueryOptions, RaftCells,
        RaftCellsConfig, RaftCellsServices, RaftGroupAssignment, RaftGroupPlacement,
        RaftNamespaceConfig, RaftPeerNode, RaftTls,
    },
    Application, Cell, CellIdentity, Context as AppContext, HandlerFuture, Invocation, Node,
    NodeRaftConfig, NodeRaftListeners, Output, ShutdownMode,
};

#[path = "cell_group_disaster_public_node.rs"]
mod node;

struct App(Arc<RaftClass>);

impl Application for App {
    fn invoke(self: Arc<Self>, _: AppContext, _: Invocation) -> HandlerFuture<Output> {
        Box::pin(async { Ok(Output::Done) })
    }

    fn supports_cell(&self, _: &str) -> bool {
        false
    }

    fn create_cell(&self, _: AppContext, _: &CellIdentity) -> Result<Arc<dyn Cell>> {
        anyhow::bail!("only explicit Raft definitions")
    }

    fn resolve_raft_class(&self, name: &str) -> Result<Arc<RaftClass>, RegistrationError> {
        if name == "RecoveryCounter" {
            Ok(self.0.clone())
        } else {
            Err(RegistrationError::UnknownClass(name.into()))
        }
    }
}

fn tls() -> Result<RaftTls> {
    Ok(RaftTls::new(
        vec![include_bytes!("../../transport/tcp/fixtures/ca.der")
            .to_vec()
            .into()],
        vec![include_bytes!("../../transport/tcp/fixtures/server.der")
            .to_vec()
            .into()],
        rustls::pki_types::PrivatePkcs8KeyDer::from(
            include_bytes!("../../transport/tcp/fixtures/key.der").to_vec(),
        )
        .into(),
    )?)
}

async fn listeners() -> Result<NodeRaftListeners> {
    Ok(NodeRaftListeners {
        control: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
        data: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
        query: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
    })
}

fn peer(
    s: &Scenario,
    ordinal: usize,
    endpoints: &NodeRaftListeners,
    generation: u8,
) -> Result<RaftPeerNode> {
    Ok(RaftPeerNode {
        node: s.request.replacement().voters()[ordinal].node,
        disk: DiskLineage::new([210 + ordinal as u8; 16])?,
        process: ProcessGeneration::new([generation; 16])?,
        label: format!("fresh-{ordinal}"),
        server_name: "localhost".into(),
        control_address: endpoints.control.local_addr()?,
        data_address: endpoints.data.local_addr()?,
        query_address: endpoints.query.local_addr()?,
    })
}

async fn start(
    s: &Scenario,
    node: &Node,
    ordinal: usize,
    endpoints: NodeRaftListeners,
    peers: Vec<RaftPeerNode>,
    app: Arc<dyn Application>,
    rooted: bool,
    authority: Option<Arc<Authority>>,
) -> Result<RaftCells> {
    let local = &peers[ordinal];
    let mut log = ReplicaLogOptions::new(
        node.info().data_dir.join("raft"),
        local.node,
        local.disk,
        local.process,
        LogOpenMode::Resume,
    );
    log.max_groups = 2;
    let mut host = cellar::sdk::consensus::RaftHostOptions {
        max_groups: 2,
        max_input_bytes: 1024,
        ..Default::default()
    };
    host.execution.envelope.max_payload_bytes = 1024 * 1024;
    host.archive.limits.envelope = host.execution.envelope;
    host.node_profile = Some(cellar::sdk::consensus::RaftNodeProfile {
        pool: "recovery-fixture".into(),
        region: "local".into(),
        zone: format!("voter-{ordinal}"),
        group_capacity: u32::try_from(host.max_groups)?,
        byte_capacity: host.images.max_bytes * host.max_groups as u64,
        bytes_per_group: host.images.max_bytes,
        leader_capacity: u32::try_from(host.max_groups)?,
        draining: false,
    });
    if rooted {
        host.extents = Some(Default::default());
    }
    let mut shared = node::limits();
    shared.entries.envelope = host.execution.envelope;
    shared.checkpoints = Some(checkpoint_fixture::limits());
    shared.archive = Some(CellGroupNodeArchiveOptions {
        io: Default::default(),
        uploads: Default::default(),
        tails: Default::default(),
    });
    node.start_raft_cells(
        RaftCellsConfig {
            infrastructure: NodeRaftConfig {
                log,
                consensus: RaftRuntimeOptions {
                    max_groups: 2,
                    tick_interval_ms: Some(10),
                    election_seed: Some([17 + ordinal as u8; 32]),
                    ..Default::default()
                },
                transport: Default::default(),
            },
            catalog: CatalogOptions::default(),
            namespaces: vec![RaftNamespaceConfig {
                namespace: s.cluster.directory.namespace_config().clone(),
                class: "RecoveryCounter".into(),
                initialize: None,
            }],
            groups: vec![RaftGroupAssignment {
                history: s.request.replacement().route().history,
                placement: RaftGroupPlacement::Replica,
            }],
            peers,
            query_origins: vec![],
            command_origins: vec![],
            host,
            shared,
        },
        RaftCellsServices {
            recovery: authority
                .map(|a| a as Arc<dyn DisasterFenceAuthority<CellGroupDisasterFenceRequest>>),
            listeners: endpoints,
            tls: tls()?,
            store: s.remote.clone(),
            application: app,
            versions: keys()?,
        },
    )
    .await
}

async fn stopped(nodes: Vec<Node>, hosts: Vec<RaftCells>) -> Result<()> {
    for node in nodes {
        node.shutdown(ShutdownMode::Preserve).await?;
    }
    for host in hosts {
        let report = host.close().await?;
        assert_eq!(
            report.mutation_outcomes,
            cellar::sdk::consensus::MutationOutcomes::NoUnresolvedLocalProducers
        );
    }
    Ok(())
}

async fn leader(s: &Scenario, hosts: &[RaftCells]) -> Result<usize> {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            for (ordinal, host) in hosts.iter().enumerate() {
                let observed = host
                    .group(s.request.replacement().route().history)?
                    .observe(Duration::from_secs(5))
                    .await?;
                if observed.status.ready && observed.role == RaftReplicaRole::Leader {
                    return Ok(ordinal);
                }
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await?
}

async fn pending_shutdown(
    s: &Scenario,
    root: &std::path::Path,
    app: Arc<dyn Application>,
    rooted: bool,
    authority: Arc<Authority>,
) -> Result<()> {
    let node = node::node_named(root, app.clone(), "fresh-0").await?;
    let mut endpoints = vec![listeners().await?, listeners().await?, listeners().await?];
    let peers = endpoints
        .iter()
        .enumerate()
        .map(|(i, e)| peer(s, i, e, 221))
        .collect::<Result<Vec<_>>>()?;
    let host = start(
        s,
        &node,
        0,
        endpoints.remove(0),
        peers,
        app,
        rooted,
        Some(authority.clone()),
    )
    .await?;
    let gate = Arc::new(archive_store::Gate::default());
    *authority.held_verification.lock().unwrap() = Some(gate.clone());
    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified()).await?;
    let mut closing = Box::pin(node.shutdown(ShutdownMode::Preserve));
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut closing)
            .await
            .is_err(),
        "shutdown dropped entered recovery verification"
    );
    gate.release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), closing).await??;
    assert_eq!(
        host.close().await?.mutation_outcomes,
        cellar::sdk::consensus::MutationOutcomes::NoUnresolvedLocalProducers
    );
    assert!(
        s.cluster
            .domain
            .run(s.cluster.directory.group_recovery_pending(&s.request))
            .await??
    );
    Ok(())
}

async fn serving(
    s: &Scenario,
    hosts: &[RaftCells],
    w: &ReceiptWorkload,
    generation: u64,
) -> Result<()> {
    let leader = leader(s, hosts).await?;
    let route = s.request.replacement().route();
    let mut cell = w.old["alpha"].version.history.clone();
    cell.incarnation = route.history.incarnation;
    let handle = hosts[leader].cell(cell.clone())?;
    let read = handle
        .query(
            ProposalId::new([230 + generation as u8; 16])?,
            &handle.contract("Get")?,
            vec![],
            QueryOptions {
                consistency: QueryConsistency::Linearizable,
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(read.result, (3 + generation as i64).to_be_bytes());
    assert_eq!(read.version.history, cell);
    assert_eq!(read.witness.group, route.history);
    let old = &w.old["alpha"];
    let token = keys()?.issue_cell(old, route.schema_digest, w.now)?;
    let now = s
        .cluster
        .domain
        .run(async { cellar::asyncrt::wall_ms().max(0) as u64 })
        .await?;
    assert_eq!(
        keys()?.verify_cell(&token, &old.version.history, route.schema_digest, now)?,
        *old
    );
    assert_eq!(
        keys()?.verify_cell(&token, &cell, route.schema_digest, now),
        Err(VersionError::WrongHistory)
    );
    assert!(handle
        .query(
            ProposalId::new([235 + generation as u8; 16])?,
            &handle.contract("Get")?,
            vec![],
            QueryOptions {
                consistency: QueryConsistency::AtLeast(token),
                ..Default::default()
            }
        )
        .await
        .is_err());
    let before = w.calls.load(Ordering::Acquire);
    // The durable S3 receipt retains its original bytes and stronger minimum.
    let duplicate = handle
        .command(
            &handle.contract("Add")?,
            vec![2],
            CommandOptions::new(
                CommandId::new(ClientId::new([188; 16])?, 2)?,
                w.now + 300_000,
            ),
        )
        .await?;
    assert_eq!(duplicate.result, w.results[&("alpha".into(), 2)]);
    assert_eq!(duplicate.durability, Durability::S3);
    assert_eq!(duplicate.version.history, cell);
    assert_eq!(w.calls.load(Ordering::Acquire), before);
    let expired = handle
        .command(
            &handle.contract("Add")?,
            vec![1],
            CommandOptions::new(
                CommandId::new(ClientId::new([188; 16])?, 1)?,
                w.now + 300_000,
            ),
        )
        .await;
    assert!(expired.is_err());
    let conflict = handle
        .command(
            &handle.contract("Add")?,
            vec![99],
            CommandOptions::new(
                CommandId::new(ClientId::new([188; 16])?, 2)?,
                w.now + 300_000,
            ),
        )
        .await;
    assert!(conflict.is_err());
    assert_eq!(w.calls.load(Ordering::Acquire), before);
    let id = CommandId::new(ClientId::new([189; 16])?, generation)?;
    let added = handle
        .command(
            &handle.contract("Add")?,
            vec![7],
            CommandOptions::new(id, w.now + 300_000),
        )
        .await?;
    assert_eq!(added.version.history, cell);
    assert_eq!(added.witness.group, route.history);
    let mut expected = cell.cell.as_str().as_bytes().to_vec();
    expected.extend_from_slice(&cell.incarnation.bytes());
    expected.push(7);
    assert_eq!(added.result, expected);
    assert_eq!(w.calls.load(Ordering::Acquire), before + 1);
    let replay = handle
        .command(
            &handle.contract("Add")?,
            vec![7],
            CommandOptions::new(id, w.now + 300_000),
        )
        .await?;
    assert_eq!(replay.id, added.id);
    assert_eq!(replay.result, added.result);
    assert_eq!(replay.version, added.version);
    assert_eq!(replay.witness, added.witness);
    assert_eq!(replay.durability, added.durability);
    assert_eq!(w.calls.load(Ordering::Acquire), before + 1);
    let read = handle
        .query(
            ProposalId::new([240 + generation as u8; 16])?,
            &handle.contract("Get")?,
            vec![],
            QueryOptions {
                consistency: QueryConsistency::Linearizable,
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(read.result, (4 + generation as i64).to_be_bytes());
    // Other cells in the same physical group keep independent state and floors.
    let mut beta = w.old["beta"].version.history.clone();
    beta.incarnation = cell.incarnation;
    let handle = hosts[leader].cell(beta.clone())?;
    let read = handle
        .query(
            ProposalId::new([245 + generation as u8; 16])?,
            &handle.contract("Get")?,
            vec![],
            QueryOptions {
                consistency: QueryConsistency::Linearizable,
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(read.result, 2i64.to_be_bytes());
    assert_eq!(read.version.history, beta);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sdk_recovery_owns_activation_after_erasing_voters() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(drill(
            rooted,
            Arc::new(object_store::memory::InMemory::new()),
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires isolated local RustFS"]
async fn rustfs_sdk_recovery_owns_activation_after_erasing_voters() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(drill(rooted, rustfs::store("sdk-recovery", rooted)?)).await?;
    }
    Ok(())
}

async fn drill(rooted: bool, store: Arc<dyn object_store::ObjectStore>) -> Result<()> {
    let mut w = ReceiptWorkload::new()?;
    let class = Arc::try_unwrap(w.class).map_err(|_| anyhow::anyhow!("unshared workload"))?;
    w.class = Arc::new(class.query("Get", [86; 32], |context, _| {
        let n: i64 = context.query_row("SELECT n FROM value", [], |row| row.get(0))?;
        Ok(n.to_be_bytes().to_vec())
    })?);
    let mut s = Box::new(Box::pin(Scenario::with_workload(rooted, store, Some(&mut w))).await?);
    let (seeds, logs, reopen) = super::super::seed_voters(&mut s, rooted).await?;
    drop(seeds);
    for log in &logs {
        log.close().await?;
    }
    drop(logs);
    drop(reopen);
    s.cluster
        .domain
        .run(
            s.cluster
                .directory
                .publish_group_recovery(&s.request, &s.authority),
        )
        .await??;
    let birth = s
        .cluster
        .domain
        .run(
            s.cluster
                .directory
                .materialize_group_birth(s.request.replacement()),
        )
        .await??;
    let binding = s
        .cluster
        .directory
        .prepare_group_archive_binding(&s.cluster.domain, &s.archives, &birth)
        .await?;
    let saved = binding.encode().to_vec();
    let intent = s.cluster._root.path().join("sdk-archive-initialization");
    s.cluster
        .domain
        .blocking(move || -> Result<()> {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&intent)?;
            file.write_all(&saved)?;
            file.sync_all()?;
            std::fs::File::open(intent.parent().context("intent parent")?)?.sync_all()?;
            Ok(())
        })?
        .await??;
    binding.commit(&s.cluster.domain).await?;
    let authority = Arc::new(std::mem::take(&mut s.authority));
    let app: Arc<dyn Application> = Arc::new(App(w.class.clone()));
    let roots = (0..3)
        .map(|n| s.cluster._root.path().join(format!("sdk-fresh-{n}")))
        .collect::<Vec<_>>();
    // Move complete offline stores without rewriting any pin or genesis. These
    // are the exact fresh stores created after all source voter files were erased.
    for (ordinal, root) in roots.iter().enumerate() {
        let node = node::node_named(root, app.clone(), &format!("fresh-{ordinal}")).await?;
        let target = node.info().data_dir.clone();
        node.shutdown(ShutdownMode::Preserve).await?;
        let source = s.cluster._root.path().join(format!("fresh-{ordinal}"));
        let route = s.request.replacement().route().history;
        s.cluster
            .domain
            .blocking(move || -> Result<()> {
                let physical = target.join("raft-cells");
                std::fs::create_dir_all(&physical)?;
                std::fs::rename(source.join("log"), target.join("raft"))?;
                let pages = physical
                    .join("groups")
                    .join(hex::encode(route.cluster.bytes()))
                    .join(hex::encode(route.namespace.bytes()))
                    .join(hex::encode(route.group.bytes()))
                    .join(hex::encode(route.incarnation.bytes()))
                    .join("pages");
                std::fs::create_dir_all(pages.parent().context("page parent")?)?;
                std::fs::rename(source.join("data/pages"), pages)?;
                if rooted {
                    std::fs::rename(
                        source.join("data/extents"),
                        physical.join("extent-inventory"),
                    )?;
                } else {
                    std::fs::rename(source.join("data/images"), physical.join("images"))?;
                }
                Ok(())
            })?
            .await??;
    }
    // Missing authority and a rejected independent fence both refuse the same
    // intact seeded disk; a catalog publication alone cannot grant serving.
    let before_refusals = s
        .cluster
        .domain
        .run(s.cluster.directory.observe())
        .await??
        .digest();
    for supplied in [None, Some(authority.clone())] {
        let before_verifications = authority.verifications.load(Ordering::Acquire);
        let rejected_authority = supplied.is_some();
        authority
            .reject
            .store(supplied.is_some(), Ordering::Release);
        let node = node::node_named(&roots[0], app.clone(), "fresh-0").await?;
        let mut all_endpoints = vec![listeners().await?, listeners().await?, listeners().await?];
        let peers = all_endpoints
            .iter()
            .enumerate()
            .map(|(i, e)| peer(&s, i, e, 221))
            .collect::<Result<Vec<_>>>()?;
        let endpoints = all_endpoints.remove(0);
        let refusal = start(
            &s,
            &node,
            0,
            endpoints,
            peers,
            app.clone(),
            rooted,
            supplied,
        )
        .await;
        let error = refusal
            .err()
            .context("missing or rejected authority must refuse startup")?;
        eprintln!("recovered-startup refusal: {error:#}");
        node.shutdown(ShutdownMode::Preserve).await?;
        assert_eq!(
            authority.verifications.load(Ordering::Acquire) > before_verifications,
            rejected_authority
        );
        assert_eq!(
            s.cluster
                .domain
                .run(s.cluster.directory.observe())
                .await??
                .digest(),
            before_refusals
        );
    }
    authority.reject.store(false, Ordering::Release);
    Box::pin(pending_shutdown(
        &s,
        &roots[0],
        app.clone(),
        rooted,
        authority.clone(),
    ))
    .await?;
    assert_eq!(w.calls.load(Ordering::Acquire), 4);
    for generation in 1..=2 {
        let mut nodes = Vec::new();
        let mut endpoints = Vec::new();
        for (ordinal, root) in roots.iter().enumerate() {
            nodes.push(node::node_named(root, app.clone(), &format!("fresh-{ordinal}")).await?);
            endpoints.push(listeners().await?);
        }
        let peers = endpoints
            .iter()
            .enumerate()
            .map(|(ordinal, e)| peer(&s, ordinal, e, 221 + generation as u8))
            .collect::<Result<Vec<_>>>()?;
        let mut hosts = Vec::new();
        for (ordinal, e) in endpoints.into_iter().enumerate() {
            hosts.push(
                start(
                    &s,
                    &nodes[ordinal],
                    ordinal,
                    e,
                    peers.clone(),
                    app.clone(),
                    rooted,
                    Some(authority.clone()),
                )
                .await?,
            );
            if ordinal == 0 && generation == 1 {
                assert!(
                    s.cluster
                        .domain
                        .run(s.cluster.directory.group_recovery_pending(&s.request))
                        .await??
                );
                let mut cell = w.old["alpha"].version.history.clone();
                cell.incarnation = s.request.replacement().route().history.incarnation;
                let handle = hosts[0].cell(cell)?;
                let refusal = handle
                    .command(
                        &handle.contract("Add")?,
                        vec![7],
                        CommandOptions::new(
                            CommandId::new(ClientId::new([190; 16])?, 1)?,
                            w.now + 300_000,
                        ),
                    )
                    .await;
                assert!(refusal.is_err());
                assert_eq!(w.calls.load(Ordering::Acquire), 4);
            }
        }
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if !s
                    .cluster
                    .domain
                    .run(s.cluster.directory.group_recovery_pending(&s.request))
                    .await??
                {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await??;
        for host in &hosts {
            let group = host
                .group(s.request.replacement().route().history)?
                .observe(Duration::from_secs(5))
                .await?;
            assert_eq!(group.replica.scope, s.request.replacement().route().history);
            assert_eq!(group.cells, 3);
            assert!(group.archive_required);
            assert_eq!(
                host.observe_node(Duration::from_secs(5)).await?.node.groups,
                1
            );
        }
        Box::pin(serving(&s, &hosts, &w, generation)).await?;
        stopped(nodes, hosts).await?;
    }
    s.cluster.close().await
}
