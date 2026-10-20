//! Embedded nodes with real TLS TCP links and optional connection-cut proxies.
#![allow(clippy::disallowed_methods)]
use super::public_node;
use anyhow::{ensure, Result};
use cellar::sdk::{consensus::*, Node, NodeRaftConfig, NodeRaftListeners, ShutdownMode};
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
        self.0.send_modify(|fault| fault.hold_response = false);
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
) -> std::io::Result<()> {
    let mut buffer = [0; 16 * 1024];
    loop {
        let count = reader.read(&mut buffer).await?;
        if count == 0 {
            return Ok(());
        }
        if faults.borrow().hold_response {
            held.fetch_add(count, Ordering::SeqCst);
        }
        while faults.borrow_and_update().hold_response {
            if faults.changed().await.is_err() {
                return Ok(());
            }
        }
        writer.write_all(&buffer[..count]).await?;
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
                        attempted.fetch_add(1, Ordering::SeqCst);
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
                                            _ = forward_responses(from_target, to_source, cut, held) => (),
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

pub struct Cluster {
    pub nodes: Vec<Node>,
    pub services: Vec<RaftCells>,
    // Some targets inspect recovery material after closing all replica nodes.
    #[allow(dead_code)]
    pub store: Arc<dyn object_store::ObjectStore>,
    links: Vec<(usize, usize, Link, Proxy)>,
    _roots: Vec<tempfile::TempDir>,
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
        Self::start_layout(definitions, registrations, tick_ms, host, None).await
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
        Self::start_layout(definitions, registrations, tick_ms, host, Some(namespaces)).await
    }

    async fn start_layout(
        definitions: &[public_node::Counter],
        registrations: Vec<RaftCellRegistration>,
        tick_ms: u64,
        host: RaftHostOptions,
        namespaces: Option<&[(ClusterId, NamespaceId)]>,
    ) -> Result<Self> {
        let count = if namespaces.is_some() { 4 } else { 3 };
        ensure!(definitions.len() == count, "incorrect fixture node count");
        let roots = (0..count)
            .map(|_| tempfile::tempdir())
            .collect::<Result<Vec<_>, _>>()?;
        let mut nodes = Vec::new();
        let mut listeners = Vec::new();
        let mut peers = Vec::new();
        for i in 0..count {
            let label = format!("raft-cluster-{}", i + 1);
            nodes.push(
                public_node::node_named(
                    roots[i].path(),
                    definitions[i].application.clone(),
                    &label,
                )
                .await?,
            );
            let control = TcpListener::bind("127.0.0.1:0").await?;
            let data = TcpListener::bind("127.0.0.1:0").await?;
            let query = TcpListener::bind("127.0.0.1:0").await?;
            peers.push(RaftPeerNode {
                node: NodeId::new([i as u8 + 1; 16])?,
                disk: DiskLineage::new([i as u8 + 10; 16])?,
                process: ProcessGeneration::new([i as u8 + 20; 16])?,
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
        let store = Arc::new(object_store::memory::InMemory::new());
        let mut services = Vec::new();
        for (i, listeners) in listeners.into_iter().enumerate() {
            services.push(
                nodes[i]
                    .start_raft_cells(
                        RaftCellsConfig {
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
                                    LogOpenMode::Create,
                                ),
                                consensus: RaftRuntimeOptions {
                                    tick_interval_ms: Some(tick_ms),
                                    ..Default::default()
                                },
                                transport: TransportOptions::default(),
                            },
                            catalog: CatalogOptions {
                                prefix: "partition/test".into(),
                                ..Default::default()
                            },
                            cells: registrations
                                .iter()
                                .cloned()
                                .map(|mut registration| {
                                    if i == 3 {
                                        registration.placement = RaftCellPlacement::QueryRouter {
                                            leader_hint: Some(MemberId::new(1).unwrap()),
                                        };
                                    }
                                    registration
                                })
                                .collect(),
                            peers: routes[i].clone(),
                            host: host.clone(),
                        },
                        RaftCellsServices {
                            listeners,
                            tls: public_node::tls()?,
                            store: store.clone(),
                            application: definitions[i].application.clone(),
                            versions: public_node::version_keys()?,
                        },
                    )
                    .await?,
            );
        }
        Ok(Self {
            nodes,
            services,
            store,
            links,
            _roots: roots,
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

    pub async fn close(mut self) -> Result<()> {
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
