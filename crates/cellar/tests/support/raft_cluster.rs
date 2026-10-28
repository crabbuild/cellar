// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Embedded nodes with real TLS TCP links and optional connection-cut proxies.
#![allow(clippy::disallowed_methods)]
use super::public_node;
use anyhow::{ensure, Result};
use cellar::sdk::{
    consensus::*, Node, NodeOutcome, NodeRaftConfig, NodeRaftListeners, ShutdownMode,
};
use cellar_logic::consensus::{DiskLineage, NodeId, ProcessGeneration};
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
    task::{JoinHandle, JoinSet},
};

#[derive(Clone, Copy, Default)]
struct Fault {
    blocked: bool,
    stopped: bool,
    hold_response: bool,
    hold_after: Option<usize>,
    hold_connection: Option<usize>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Link {
    Control,
    Data,
    Query,
}

struct Proxy {
    address: SocketAddr,
    state: watch::Sender<Fault>,
    active: Arc<AtomicUsize>,
    attempts: Arc<AtomicUsize>,
    held_response_bytes: Arc<AtomicUsize>,
    task: Option<JoinHandle<Result<()>>>,
}

// This shared fixture is also compiled by targets which only cut connections.
#[allow(dead_code)]
pub struct HeldResponses(watch::Sender<Fault>);

impl Drop for HeldResponses {
    fn drop(&mut self) {
        self.0.send_modify(|fault| {
            fault.hold_response = false;
            fault.hold_after = None;
        });
    }
}

async fn connection_cut(mut faults: watch::Receiver<Fault>) {
    loop {
        let fault = *faults.borrow_and_update();
        if fault.blocked || fault.stopped || faults.changed().await.is_err() {
            return;
        }
    }
}

async fn forward_responses(
    mut reader: impl AsyncRead + Unpin,
    mut writer: impl AsyncWrite + Unpin,
    mut faults: watch::Receiver<Fault>,
    held: Arc<AtomicUsize>,
    connection: usize,
) -> std::io::Result<()> {
    let mut buffer = [0; 16 * 1024];
    let mut forwarded = 0usize;
    loop {
        let count = reader.read(&mut buffer).await?;
        if count == 0 {
            return Ok(());
        }
        let mut offset = 0;
        let mut counted = false;
        while offset < count {
            let mut fault = *faults.borrow_and_update();
            if fault.hold_connection.is_some_and(|held| held != connection) {
                fault.hold_response = false;
                fault.hold_after = None;
            }
            let allowance = if fault.hold_response {
                0
            } else {
                fault
                    .hold_after
                    .map_or(count - offset, |after| after.saturating_sub(forwarded))
            };
            if allowance == 0 {
                if !counted {
                    held.fetch_add(count - offset, Ordering::SeqCst);
                    counted = true;
                }
                if faults.changed().await.is_err() {
                    return Ok(());
                }
                continue;
            }
            let end = count.min(offset.saturating_add(allowance));
            writer.write_all(&buffer[offset..end]).await?;
            forwarded += end - offset;
            offset = end;
        }
    }
}

struct Active(Arc<AtomicUsize>);

impl Drop for Active {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Proxy {
    async fn start(destination: SocketAddr) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (state, mut changes) = watch::channel(Fault::default());
        let active = Arc::new(AtomicUsize::new(0));
        let attempts = Arc::new(AtomicUsize::new(0));
        let held_response_bytes = Arc::new(AtomicUsize::new(0));
        let held = held_response_bytes.clone();
        let counted = active.clone();
        let attempted = attempts.clone();
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                cellar::asyncrt::select! {
                    changed = changes.changed() => {
                        if changed.is_err() || changes.borrow().stopped { break; }
                    }
                    done = connections.join_next(), if !connections.is_empty() => { if let Some(done) = done { done?; } }
                    accepted = listener.accept(), if connections.len() < 8 => {
                        let (mut source, _) = accepted?;
                        let connection=attempted.fetch_add(1, Ordering::SeqCst)+1;
                        if changes.borrow().blocked { continue; }
                        let cut = changes.clone();
                        let held = held.clone();
                        let active = counted.clone(); active.fetch_add(1, Ordering::SeqCst);
                        let guard = Active(active);
                        connections.spawn(async move {
                            let _guard = guard;
                            let fault = *cut.borrow();
                            if fault.blocked || fault.stopped { return; }
                            // A cut drops both sockets. A response hold instead
                            // retains one bounded ciphertext chunk until release
                            // or either connection direction closes.
                            cellar::asyncrt::select_biased! { "test fault transitions close sockets before more bytes are forwarded";
                                _ = connection_cut(cut.clone()) => (),
                                _ = async {
                                    if let Ok(target) = TcpStream::connect(destination).await {
                                        let (mut from_source, to_source) = source.split();
                                        let (from_target, mut to_target) = target.into_split();
                                        cellar::asyncrt::select! {
                                            _ = tokio::io::copy(&mut from_source, &mut to_target) => (),
                                            _ = forward_responses(from_target, to_source, cut, held, connection) => (),
                                        }
                                    }
                                } => (),
                            }
                        });
                    }
                }
            }
            connections.abort_all();
            while let Some(result) = connections.join_next().await {
                if let Err(error) = result {
                    ensure!(error.is_cancelled(), "proxy connection panicked: {error}");
                }
            }
            Ok(())
        });
        Ok(Self {
            address,
            state,
            active,
            attempts,
            held_response_bytes,
            task: Some(task),
        })
    }

    fn block(&self, blocked: bool) {
        self.state.send_if_modified(|fault| {
            let changed = fault.blocked != blocked;
            fault.blocked = blocked;
            changed
        });
    }

    async fn close(&mut self) -> Result<()> {
        self.state.send_modify(|fault| fault.stopped = true);
        if let Some(task) = self.task.take() {
            task.await??;
        }
        Ok(())
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.state.send_modify(|fault| fault.stopped = true);
    }
}

/// One globally advertised data endpoint, shared by every source. Directory
/// refresh therefore preserves this fault boundary rather than bypassing it.
#[allow(dead_code)] // Shared fixture targets do not all use global endpoints.
pub struct DataLink(Proxy);

#[allow(dead_code)] // Shared fixture targets do not all use global endpoints.
impl DataLink {
    pub async fn start(destination: SocketAddr) -> Result<Self> {
        Ok(Self(Proxy::start(destination).await?))
    }

    pub fn address(&self) -> SocketAddr {
        self.0.address
    }

    pub async fn block(&self, blocked: bool) -> Result<()> {
        self.0.block(blocked);
        if blocked {
            tokio::time::timeout(Duration::from_secs(5), async {
                while self.0.active.load(Ordering::SeqCst) != 0 {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await?;
        }
        Ok(())
    }

    pub async fn close(&mut self) -> Result<()> {
        self.0.close().await
    }
}

type JoinedResumeCheck =
    Box<dyn FnOnce(RaftCells) -> futures_util::future::BoxFuture<'static, Result<()>> + Send>;

pub type PreloadReplicas = Box<
    dyn FnOnce(
            Vec<ReplicaLogOptions>,
            Arc<dyn object_store::ObjectStore>,
        ) -> futures_util::future::BoxFuture<'static, Result<()>>
        + Send,
>;

enum ExtraNode<'a> {
    None,
    InitialLearner,
    QueryRouter(&'a [(ClusterId, NamespaceId)]),
    Standby(Option<Arc<dyn cellar_ltx::FileSystem>>),
    Preloaded {
        roots: Vec<tempfile::TempDir>,
        prepare: PreloadReplicas,
    },
    Resume {
        roots: Vec<tempfile::TempDir>,
        placements: Vec<RaftCellPlacement>,
        check: JoinedResumeCheck,
        generation: u8,
        target_first: bool,
        retired_owner: Option<usize>,
    },
}

pub struct Cluster {
    pub nodes: Vec<Node>,
    pub services: Vec<RaftCells>,
    // Some targets inspect recovery material after closing all replica nodes.
    #[allow(dead_code)]
    pub store: Arc<dyn object_store::ObjectStore>,
    links: Vec<(usize, usize, Link, Proxy)>,
    #[allow(dead_code)]
    standby: Option<(NodeRaftListeners, RaftCellsConfig)>,
    _roots: Vec<tempfile::TempDir>,
    generation: u8,
    materialized_image_bytes: Option<u64>,
    restart_host: RaftHostOptions,
    retired_owner: Option<usize>,
}

impl Cluster {
    pub async fn start(
        definitions: &[public_node::Counter],
        registrations: Vec<RaftCellRegistration>,
        tick_ms: u64,
    ) -> Result<Self> {
        Self::start_with_host(
            definitions,
            registrations,
            tick_ms,
            RaftHostOptions::default(),
        )
        .await
    }

    pub async fn start_with_host(
        definitions: &[public_node::Counter],
        registrations: Vec<RaftCellRegistration>,
        tick_ms: u64,
        host: RaftHostOptions,
    ) -> Result<Self> {
        Self::start_layout(
            definitions,
            registrations,
            tick_ms,
            host,
            ExtraNode::None,
            TransportOptions::default(),
            None,
        )
        .await
    }

    #[allow(dead_code)]
    pub async fn start_with_initial_learner(
        definitions: &[public_node::Counter],
        registrations: Vec<RaftCellRegistration>,
        tick_ms: u64,
        host: RaftHostOptions,
    ) -> Result<Self> {
        let learner = NodeId::new([4; 16])?;
        ensure!(
            !registrations.is_empty()
                && registrations.iter().all(|registration| {
                    registration
                        .birth
                        .learners
                        .iter()
                        .any(|member| member.member.get() == 4 && member.node == learner)
                        && !registration
                            .birth
                            .voters
                            .iter()
                            .any(|member| member.member.get() == 4 || member.node == learner)
                }),
            "fourth fixture replica must be an initial learner"
        );
        Self::start_layout(
            definitions,
            registrations,
            tick_ms,
            host,
            ExtraNode::InitialLearner,
            TransportOptions::default(),
            None,
        )
        .await
    }

    #[allow(dead_code)]
    pub async fn start_with_transport(
        definitions: &[public_node::Counter],
        registrations: Vec<RaftCellRegistration>,
        tick_ms: u64,
        host: RaftHostOptions,
        transport: TransportOptions,
    ) -> Result<Self> {
        Self::start_layout(
            definitions,
            registrations,
            tick_ms,
            host,
            ExtraNode::None,
            transport,
            None,
        )
        .await
    }

    // Some targets only use the three-voter partition fixture.
    #[allow(dead_code)]
    pub async fn start_with_router(
        definitions: &[public_node::Counter],
        registrations: Vec<RaftCellRegistration>,
        tick_ms: u64,
        host: RaftHostOptions,
        namespaces: &[(ClusterId, NamespaceId)],
    ) -> Result<Self> {
        Self::start_layout(
            definitions,
            registrations,
            tick_ms,
            host,
            ExtraNode::QueryRouter(namespaces),
            TransportOptions::default(),
            None,
        )
        .await
    }

    #[allow(dead_code)]
    pub async fn start_with_standby(
        definitions: &[public_node::Counter],
        registrations: Vec<RaftCellRegistration>,
    ) -> Result<Self> {
        Self::start_layout(
            definitions,
            registrations,
            20,
            RaftHostOptions::default(),
            ExtraNode::Standby(None),
            TransportOptions {
                snapshot_chunk_bytes: 64,
                ..Default::default()
            },
            None,
        )
        .await
    }

    #[allow(dead_code)]
    pub async fn start_with_standby_store(
        definitions: &[public_node::Counter],
        registrations: Vec<RaftCellRegistration>,
        host: RaftHostOptions,
        store: Arc<dyn object_store::ObjectStore>,
    ) -> Result<Self> {
        Self::start_layout(
            definitions,
            registrations,
            20,
            host,
            ExtraNode::Standby(None),
            TransportOptions {
                snapshot_chunk_bytes: 64,
                ..Default::default()
            },
            Some(store),
        )
        .await
    }

    /// Real command, transfer and archive paths with coherent opt-in image bounds.
    #[allow(dead_code)]
    pub async fn start_with_materialized_images(
        definitions: &[public_node::Counter],
        registrations: Vec<RaftCellRegistration>,
        bytes: u64,
        store: Option<Arc<dyn object_store::ObjectStore>>,
    ) -> Result<Self> {
        let mut host = RaftHostOptions::default();
        host.execution.max_image_bytes = bytes;
        Self::start_layout(
            definitions,
            registrations,
            100,
            host,
            ExtraNode::Standby(None),
            TransportOptions::default(),
            store,
        )
        .await
    }

    /// Preload the same verified baseline on all initial replicas before their
    /// native runtimes start. Used for full-capacity recovery tests without
    /// admitting a giant command or weakening ordinary envelope bounds.
    #[allow(dead_code)]
    pub async fn start_with_preloaded_images(
        definitions: &[public_node::Counter],
        registrations: Vec<RaftCellRegistration>,
        host: RaftHostOptions,
        store: Arc<dyn object_store::ObjectStore>,
        roots: Vec<tempfile::TempDir>,
        prepare: PreloadReplicas,
    ) -> Result<Self> {
        Self::start_layout(
            definitions,
            registrations,
            100,
            host,
            ExtraNode::Preloaded { roots, prepare },
            TransportOptions {
                snapshot_timeout_ms: 3_600_000,
                ..Default::default()
            },
            Some(store),
        )
        .await
    }

    #[allow(dead_code)] // Shared with targets that do not run seed crash matrices.
    pub async fn start_seed_crash_fixture(
        definitions: &[public_node::Counter],
        registrations: Vec<RaftCellRegistration>,
        store: Arc<dyn object_store::ObjectStore>,
        filesystem: Arc<dyn cellar_ltx::FileSystem>,
    ) -> Result<Self> {
        Self::start_layout(
            definitions,
            registrations,
            20,
            RaftHostOptions::default(),
            ExtraNode::Standby(Some(filesystem)),
            TransportOptions::default(),
            Some(store),
        )
        .await
    }

    #[allow(dead_code)] // Shared with targets that do not run seed crash matrices.
    pub async fn resume_seed_crash_fixture(
        definitions: &[public_node::Counter],
        registrations: Vec<RaftCellRegistration>,
        store: Arc<dyn object_store::ObjectStore>,
        paths: &[std::path::PathBuf],
        placements: Vec<RaftCellPlacement>,
        target_first: bool,
        check: Option<JoinedResumeCheck>,
    ) -> Result<Self> {
        let mut roots = Vec::new();
        for path in paths {
            let root = tempfile::tempdir()?;
            copy_fixture_directory(path, root.path())?;
            roots.push(root);
        }
        Self::start_layout(
            definitions,
            registrations,
            20,
            RaftHostOptions::default(),
            ExtraNode::Resume {
                roots,
                placements,
                check: check.unwrap_or_else(|| Box::new(|_| Box::pin(async { Ok(()) }))),
                generation: 60,
                target_first,
                retired_owner: None,
            },
            TransportOptions::default(),
            Some(store),
        )
        .await
    }

    #[allow(dead_code)]
    pub async fn restart(
        mut self,
        definitions: &[public_node::Counter],
        registrations: Vec<RaftCellRegistration>,
        placements: Vec<RaftCellPlacement>,
        check: JoinedResumeCheck,
    ) -> Result<Self> {
        ensure!(
            self._roots.len() == 4 && self.services.len() >= 3,
            "restart requires three voters and the replacement directory"
        );
        self.stop_owned().await?;
        let roots = std::mem::take(&mut self._roots);
        let generation = self
            .generation
            .checked_add(40)
            .filter(|generation| *generation <= 252)
            .ok_or_else(|| anyhow::anyhow!("fixture process generations exhausted"))?;
        let mut host = self.restart_host.clone();
        if let Some(bytes) = self.materialized_image_bytes {
            host.execution.max_image_bytes = bytes;
        }
        Self::start_layout(
            definitions,
            registrations,
            20,
            host,
            ExtraNode::Resume {
                roots,
                placements,
                check,
                generation,
                target_first: true,
                retired_owner: self.retired_owner,
            },
            TransportOptions {
                snapshot_chunk_bytes: 64,
                ..Default::default()
            },
            Some(self.store.clone()),
        )
        .await
    }

    /// Recreate the dormant SDK runtime from an empty directory before seeding.
    /// The focused recovery caller invokes this inside its measured interval.
    #[allow(dead_code)] // Shared targets also exercise retained-runtime joins.
    pub async fn join_cold_standby(
        &mut self,
        application: Arc<dyn cellar::sdk::Application>,
        placement: RaftCellPlacement,
    ) -> Result<RaftCells> {
        ensure!(
            self.standby.is_some() && self.retired_owner.is_some(),
            "cold join requires an unused standby after owner loss"
        );
        let previous = self
            .nodes
            .pop()
            .ok_or_else(|| anyhow::anyhow!("missing dormant node"))?;
        let data = previous.info().data_dir.clone();
        ensure!(
            !data.join("raft").exists(),
            "dormant node already owns Raft state"
        );
        ensure!(
            previous.cancel().await? == NodeOutcome::Cancelled,
            "dormant node did not join"
        );
        let erasure = cellar::sdk::ExecutionDomain::new(tokio::runtime::Handle::current());
        let erased = erasure
            .blocking(move || {
                std::fs::remove_dir_all(&data)?;
                ensure!(!data.exists(), "dormant local state survived erasure");
                Ok::<_, anyhow::Error>(())
            })?
            .await?;
        erasure.shutdown().await;
        erased?;
        let root = self
            ._roots
            .get(3)
            .ok_or_else(|| anyhow::anyhow!("missing standby directory"))?;
        let fresh =
            public_node::node_named(root.path(), application.clone(), "raft-cluster-4").await?;
        ensure!(
            !fresh.info().data_dir.join("raft").exists(),
            "fresh node has local Raft state"
        );
        self.nodes.push(fresh);
        self.join_standby(application, placement).await
    }

    #[allow(dead_code)]
    pub async fn join_standby(
        &mut self,
        application: Arc<dyn cellar::sdk::Application>,
        placement: RaftCellPlacement,
    ) -> Result<RaftCells> {
        let (listeners, mut config) = self
            .standby
            .take()
            .ok_or_else(|| anyhow::anyhow!("no standby node"))?;
        for registration in &mut config.cells {
            registration.placement = placement;
        }
        let service = self
            .nodes
            .last()
            .ok_or_else(|| anyhow::anyhow!("missing standby node"))?
            .start_raft_cells(
                config,
                RaftCellsServices {
                    listeners,
                    tls: public_node::tls()?,
                    store: self.store.clone(),
                    application,
                    versions: public_node::version_keys()?,
                },
            )
            .await?;
        self.services.push(service.clone());
        Ok(service)
    }

    async fn start_layout(
        definitions: &[public_node::Counter],
        registrations: Vec<RaftCellRegistration>,
        tick_ms: u64,
        host: RaftHostOptions,
        extra: ExtraNode<'_>,
        transport: TransportOptions,
        store: Option<Arc<dyn object_store::ObjectStore>>,
    ) -> Result<Self> {
        let initial_learner = matches!(&extra, ExtraNode::InitialLearner);
        let (namespaces, standby_node, resume, filesystem, mut preload) = match extra {
            ExtraNode::None | ExtraNode::InitialLearner => (None, false, None, None, None),
            ExtraNode::QueryRouter(namespaces) => (Some(namespaces), false, None, None, None),
            ExtraNode::Standby(filesystem) => (None, true, None, filesystem, None),
            ExtraNode::Preloaded { roots, prepare } => {
                (None, true, None, None, Some((roots, prepare)))
            }
            ExtraNode::Resume {
                roots,
                placements,
                check,
                generation,
                target_first,
                retired_owner,
            } => (
                None,
                false,
                Some((
                    roots,
                    placements,
                    check,
                    generation,
                    target_first,
                    retired_owner,
                )),
                None,
                None,
            ),
        };
        let resuming = resume.is_some();
        let preloaded = preload.is_some();
        let mut prepare_template = None;
        let target_first = resume
            .as_ref()
            .is_some_and(|(_, _, _, _, target_first, _)| *target_first);
        let retired_owner = resume
            .as_ref()
            .and_then(|(_, _, _, _, _, retired)| *retired);
        let count =
            if namespaces.is_some() || standby_node || resuming || initial_learner || preloaded {
                4
            } else {
                3
            };
        ensure!(definitions.len() == count, "incorrect fixture node count");
        let (roots, placements, mut resume_check, generation) =
            if let Some((roots, placements, check, generation, _, _)) = resume {
                ensure!(
                    roots.len() == count && placements.len() == count,
                    "incorrect restart roots/placements"
                );
                (roots, Some(placements), Some(check), generation)
            } else if let Some((roots, prepare)) = preload.take() {
                ensure!(roots.len() == count, "incorrect preload roots");
                prepare_template = Some(prepare);
                (roots, None, None, 20)
            } else {
                (
                    (0..count)
                        .map(|_| tempfile::tempdir())
                        .collect::<Result<Vec<_>, _>>()?,
                    None,
                    None,
                    20,
                )
            };
        let mut nodes = Vec::new();
        let mut listeners = Vec::new();
        let mut peers = Vec::new();
        for i in 0..count {
            let label = format!("raft-cluster-{}", i + 1);
            nodes.push(
                public_node::node_declared_with_filesystem(
                    roots[i].path(),
                    definitions[i].application.clone(),
                    &label,
                    vec![],
                    if i == 3 {
                        filesystem
                            .clone()
                            .unwrap_or_else(|| Arc::new(cellar_ltx::DirectFileSystem))
                    } else {
                        Arc::new(cellar_ltx::DirectFileSystem)
                    },
                )
                .await?,
            );
            let control = TcpListener::bind("127.0.0.1:0").await?;
            let data = TcpListener::bind("127.0.0.1:0").await?;
            let query = TcpListener::bind("127.0.0.1:0").await?;
            peers.push(RaftPeerNode {
                node: NodeId::new([i as u8 + 1; 16])?,
                disk: DiskLineage::new([i as u8 + 10; 16])?,
                process: ProcessGeneration::new([i as u8 + generation; 16])?,
                label,
                server_name: "localhost".into(),
                control_address: control.local_addr()?,
                data_address: data.local_addr()?,
                query_address: query.local_addr()?,
            });
            listeners.push(NodeRaftListeners {
                control,
                data,
                query,
            });
        }
        let mut links = Vec::new();
        let mut routes = Vec::new();
        for source in 0..count {
            let mut route = peers.clone();
            for (target, peer) in route.iter_mut().enumerate() {
                if source == target {
                    continue;
                }
                let control = Proxy::start(peer.control_address).await?;
                let data = Proxy::start(peer.data_address).await?;
                let query = Proxy::start(peer.query_address).await?;
                peer.control_address = control.address;
                peer.data_address = data.address;
                peer.query_address = query.address;
                links.push((source, target, Link::Control, control));
                links.push((source, target, Link::Data, data));
                links.push((source, target, Link::Query, query));
            }
            routes.push(route);
        }
        let store = store.unwrap_or_else(|| Arc::new(object_store::memory::InMemory::new()));
        if let Some(prepare) = prepare_template {
            let logs = (0..3)
                .map(|i| {
                    ReplicaLogOptions::new(
                        nodes[i].info().data_dir.join("raft"),
                        peers[i].node,
                        peers[i].disk,
                        peers[i].process,
                        LogOpenMode::Create,
                    )
                })
                .collect();
            prepare(logs, store.clone()).await?;
        }
        let mut services = vec![None; count];
        let mut standby = None;
        let mut listeners = listeners.into_iter().map(Some).collect::<Vec<_>>();
        let order = if target_first {
            vec![3, 0, 1, 2]
        } else {
            (0..count).collect()
        };
        for i in order {
            // A crashed owner whose disk was erased never resumes its old
            // member identity. Only the quorum-authorized replacement starts.
            if retired_owner == Some(i) {
                continue;
            }
            let listeners = listeners[i]
                .take()
                .ok_or_else(|| anyhow::anyhow!("duplicate listener start"))?;
            let config = RaftCellsConfig {
                query_origins: if i < 3 {
                    namespaces
                        .unwrap_or(&[])
                        .iter()
                        .map(|(cluster, namespace)| NamespaceQueryOrigin {
                            cluster: *cluster,
                            namespace: *namespace,
                            revision: 1,
                            peer: routes[i][3].clone(),
                        })
                        .collect()
                } else {
                    vec![]
                },
                infrastructure: NodeRaftConfig {
                    log: ReplicaLogOptions::new(
                        nodes[i].info().data_dir.join("raft"),
                        peers[i].node,
                        peers[i].disk,
                        peers[i].process,
                        if resuming || (preloaded && i < 3) {
                            LogOpenMode::Resume
                        } else {
                            LogOpenMode::Create
                        },
                    ),
                    consensus: RaftRuntimeOptions {
                        tick_interval_ms: Some(tick_ms),
                        ..Default::default()
                    },
                    transport: transport.clone(),
                },
                catalog: CatalogOptions {
                    prefix: "partition/test".into(),
                    ..Default::default()
                },
                cells: registrations
                    .iter()
                    .cloned()
                    .map(|mut registration| {
                        if let Some(placements) = &placements {
                            registration.placement = placements[i];
                        } else if i == 3 && !initial_learner {
                            registration.placement = RaftCellPlacement::QueryRouter {
                                leader_hint: Some(MemberId::new(1).unwrap()),
                            };
                        }
                        registration
                    })
                    .collect(),
                peers: routes[i].clone(),
                host: host.clone(),
            };
            // Large-image fixtures use the public coupled configuration API,
            // including on the empty replacement node retained below.
            let config = if host.execution.max_image_bytes > 64 * 1024 * 1024 {
                if config
                    .cells
                    .iter()
                    .all(|cell| cell.namespace.image_storage == RaftImageStorage::Extents)
                {
                    config.with_extent_image_limit(
                        host.execution.max_image_bytes,
                        host.extents.ok_or_else(|| {
                            anyhow::anyhow!("large extent fixture needs physical options")
                        })?,
                    )?
                } else {
                    config.with_materialized_image_limit(host.execution.max_image_bytes)?
                }
            } else {
                config
            };
            if i == 3 && standby_node {
                standby = Some((listeners, config));
                continue;
            }
            let service = nodes[i]
                .start_raft_cells(
                    config,
                    RaftCellsServices {
                        listeners,
                        tls: public_node::tls()?,
                        store: store.clone(),
                        application: definitions[i].application.clone(),
                        versions: public_node::version_keys()?,
                    },
                )
                .await?;
            if i == 3 {
                if let Some(check) = resume_check.take() {
                    check(service.clone()).await?;
                }
            }
            services[i] = Some(service);
        }
        let services = services.into_iter().flatten().collect();
        Ok(Self {
            nodes,
            services,
            store,
            links,
            standby,
            _roots: roots,
            generation,
            restart_host: host.clone(),
            retired_owner,
            materialized_image_bytes: (host.execution.max_image_bytes > 64 * 1024 * 1024)
                .then_some(host.execution.max_image_bytes),
        })
    }

    pub async fn isolate(&self, node: usize, retain_outgoing_data: bool) -> Result<()> {
        self.isolate_links(node, retain_outgoing_data, true).await
    }

    // This shared fixture is compiled by targets which only use full isolation.
    #[allow(dead_code)]
    pub async fn isolate_consensus(&self, node: usize) -> Result<()> {
        self.isolate_links(node, false, false).await
    }

    async fn isolate_links(
        &self,
        node: usize,
        retain_outgoing_data: bool,
        queries: bool,
    ) -> Result<()> {
        let blocked = |source: usize, target: usize, lane: Link| {
            (queries || lane != Link::Query)
                && (target == node
                    || (source == node && !(lane == Link::Data && retain_outgoing_data)))
        };
        for (source, target, data, proxy) in &self.links {
            if blocked(*source, *target, *data) {
                proxy.block(true);
            }
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            while self.links.iter().any(|(source, target, data, proxy)| {
                blocked(*source, *target, *data) && proxy.active.load(Ordering::SeqCst) != 0
            }) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await?;
        Ok(())
    }

    pub fn heal(&self) {
        for (_, _, _, proxy) in &self.links {
            proxy.block(false);
        }
    }

    #[allow(dead_code)]
    pub async fn block_control(&self, source: usize, target: usize, blocked: bool) -> Result<()> {
        let proxy = self
            .links
            .iter()
            .find(|(s, t, lane, _)| *s == source && *t == target && *lane == Link::Control)
            .map(|(_, _, _, proxy)| proxy)
            .ok_or_else(|| anyhow::anyhow!("control link not found"))?;
        proxy.block(blocked);
        if blocked {
            tokio::time::timeout(Duration::from_secs(2), async {
                while proxy.active.load(Ordering::SeqCst) != 0 {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await?;
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub fn hold_seed_body(&self, source: usize, target: usize) -> Result<Arc<AtomicUsize>> {
        let proxy = self
            .links
            .iter()
            .find(|(s, t, lane, _)| *s == source && *t == target && *lane == Link::Data)
            .map(|(_, _, _, p)| p)
            .ok_or_else(|| anyhow::anyhow!("seed data link missing"))?;
        proxy.state.send_modify(|fault| {
            fault.hold_connection = Some(2);
            fault.hold_after = Some(4096);
        });
        Ok(proxy.held_response_bytes.clone())
    }

    #[allow(dead_code)]
    pub fn hold_data_responses(&self, source: usize, target: usize) -> Result<HeldResponses> {
        let proxy = self
            .links
            .iter()
            .find(|(s, t, lane, _)| *s == source && *t == target && *lane == Link::Data)
            .map(|(_, _, _, proxy)| proxy)
            .ok_or_else(|| anyhow::anyhow!("data link not found"))?;
        proxy.state.send_modify(|fault| fault.hold_response = true);
        Ok(HeldResponses(proxy.state.clone()))
    }

    #[allow(dead_code)]
    pub fn hold_data_responses_after(
        &self,
        source: usize,
        target: usize,
        after: usize,
    ) -> Result<HeldResponses> {
        let proxy = self
            .links
            .iter()
            .find(|(s, t, lane, _)| *s == source && *t == target && *lane == Link::Data)
            .map(|(_, _, _, proxy)| proxy)
            .ok_or_else(|| anyhow::anyhow!("data link not found"))?;
        proxy
            .state
            .send_modify(|fault| fault.hold_after = Some(after));
        Ok(HeldResponses(proxy.state.clone()))
    }

    #[allow(dead_code)]
    pub fn held_data_response_bytes(&self, source: usize, target: usize) -> usize {
        self.links
            .iter()
            .filter(|(s, t, lane, _)| *s == source && *t == target && *lane == Link::Data)
            .map(|(_, _, _, proxy)| proxy.held_response_bytes.load(Ordering::SeqCst))
            .sum()
    }

    pub fn data_attempts(&self, node: usize) -> usize {
        self.links
            .iter()
            .filter(|(source, _, lane, _)| *source == node && *lane == Link::Data)
            .map(|(_, _, _, proxy)| proxy.attempts.load(Ordering::SeqCst))
            .sum()
    }

    // Other targets using the shared cluster fixture do not delay replies.
    #[allow(dead_code)]
    pub fn hold_query_responses(&self, source: usize, target: usize) -> Result<HeldResponses> {
        let proxy = self
            .links
            .iter()
            .find(|(s, t, lane, _)| *s == source && *t == target && *lane == Link::Query)
            .map(|(_, _, _, proxy)| proxy)
            .ok_or_else(|| anyhow::anyhow!("query link not found"))?;
        proxy.state.send_modify(|fault| fault.hold_response = true);
        Ok(HeldResponses(proxy.state.clone()))
    }

    #[allow(dead_code)]
    pub fn held_query_response_bytes(&self, source: usize, target: usize) -> usize {
        self.links
            .iter()
            .filter(|(s, t, lane, _)| *s == source && *t == target && *lane == Link::Query)
            .map(|(_, _, _, proxy)| proxy.held_response_bytes.load(Ordering::SeqCst))
            .sum()
    }

    /// Force the complete owner domain down, join it, and stop retaining its
    /// service. The caller may then erase this fixture-owned data directory.
    #[allow(dead_code)]
    pub async fn crash_owner(&mut self, index: usize) -> Result<std::path::PathBuf> {
        ensure!(
            self.retired_owner.is_none()
                && index < 3
                && self.nodes.len() == 4
                && self.services.len() == 3,
            "owner crash requires the original three-voter layout"
        );
        let node = self.nodes.remove(index);
        let directory = node.info().data_dir.clone();
        self.services.remove(index);
        ensure!(
            node.cancel().await? == NodeOutcome::Cancelled,
            "owner domain did not cancel and join"
        );
        self.retired_owner = Some(index);
        Ok(directory)
    }

    pub async fn close(mut self) -> Result<()> {
        self.stop_owned().await
    }

    async fn stop_owned(&mut self) -> Result<()> {
        for service in &self.services {
            ensure!(
                service.close().await?.unfinished.is_empty(),
                "unfinished producer after healed partition"
            );
        }
        for node in self.nodes.drain(..) {
            node.shutdown(ShutdownMode::Preserve).await?;
        }
        for (_, _, _, proxy) in &mut self.links {
            proxy.close().await?;
        }
        Ok(())
    }
}

#[allow(dead_code)] // Shared with targets that do not run seed crash matrices.
fn copy_fixture_directory(from: &std::path::Path, to: &std::path::Path) -> Result<()> {
    let mut pending = vec![(from.to_path_buf(), to.to_path_buf())];
    let mut count = 0;
    while let Some((from, to)) = pending.pop() {
        std::fs::create_dir_all(&to)?;
        for entry in std::fs::read_dir(from)? {
            count += 1;
            ensure!(count <= 4096, "crash fixture inventory limit");
            let entry = entry?;
            let kind = entry.file_type()?;
            let target = to.join(entry.file_name());
            if kind.is_dir() {
                pending.push((entry.path(), target));
            } else {
                ensure!(
                    kind.is_file() && entry.metadata()?.len() <= 64 * 1024 * 1024,
                    "unexpected crash fixture file"
                );
                std::fs::copy(entry.path(), target)?;
            }
        }
    }
    Ok(())
}
