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
    net::{TcpListener, TcpStream},
    sync::watch,
    task::{JoinHandle, JoinSet},
};

#[derive(Clone, Copy, Default)]
struct Fault {
    blocked: bool,
    stopped: bool,
}

struct Proxy {
    address: SocketAddr,
    state: watch::Sender<Fault>,
    active: Arc<AtomicUsize>,
    attempts: Arc<AtomicUsize>,
    task: Option<JoinHandle<Result<()>>>,
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
                        let mut cut = changes.clone();
                        let active = counted.clone(); active.fetch_add(1, Ordering::SeqCst);
                        let guard = Active(active);
                        connections.spawn(async move {
                            let _guard = guard;
                            let fault = *cut.borrow_and_update();
                            if fault.blocked || fault.stopped { return; }
                            // Dropping both sockets on any fault transition cuts
                            // established keep-alives as well as future connects.
                            cellar::asyncrt::select_biased! { "test fault transitions close sockets before more bytes are forwarded";
                                _ = cut.changed() => (),
                                _ = async {
                                    if let Ok(mut target) = TcpStream::connect(destination).await {
                                        let _ = tokio::io::copy_bidirectional(&mut source, &mut target).await;
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
    links: Vec<(usize, usize, bool, Proxy)>,
    _roots: Vec<tempfile::TempDir>,
}

impl Cluster {
    pub async fn start(
        definitions: &[public_node::Counter],
        registrations: Vec<RaftCellRegistration>,
        tick_ms: u64,
    ) -> Result<Self> {
        ensure!(definitions.len() == 3, "fixture requires three nodes");
        let roots = (0..3)
            .map(|_| tempfile::tempdir())
            .collect::<Result<Vec<_>, _>>()?;
        let mut nodes = Vec::new();
        let mut listeners = Vec::new();
        let mut peers = Vec::new();
        for i in 0..3 {
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
            peers.push(RaftPeerNode {
                node: NodeId::new([i as u8 + 1; 16])?,
                disk: DiskLineage::new([i as u8 + 10; 16])?,
                process: ProcessGeneration::new([i as u8 + 20; 16])?,
                label,
                server_name: "localhost".into(),
                control_address: control.local_addr()?,
                data_address: data.local_addr()?,
            });
            listeners.push(NodeRaftListeners { control, data });
        }
        let mut links = Vec::new();
        let mut routes = Vec::new();
        for source in 0..3 {
            let mut route = peers.clone();
            for (target, peer) in route.iter_mut().enumerate() {
                if source == target {
                    continue;
                }
                let control = Proxy::start(peer.control_address).await?;
                let data = Proxy::start(peer.data_address).await?;
                peer.control_address = control.address;
                peer.data_address = data.address;
                links.push((source, target, false, control));
                links.push((source, target, true, data));
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
                            cells: registrations.clone(),
                            peers: routes[i].clone(),
                            host: RaftHostOptions::default(),
                        },
                        RaftCellsServices {
                            listeners,
                            tls: public_node::tls()?,
                            store: store.clone(),
                            application: definitions[i].application.clone(),
                        },
                    )
                    .await?,
            );
        }
        Ok(Self {
            nodes,
            services,
            links,
            _roots: roots,
        })
    }

    pub async fn isolate(&self, node: usize, retain_outgoing_data: bool) -> Result<()> {
        let blocked = |source: usize, target: usize, data: bool| {
            target == node || (source == node && !(data && retain_outgoing_data))
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
            .filter(|(source, _, data, _)| *source == node && *data)
            .map(|(_, _, _, proxy)| proxy.attempts.load(Ordering::SeqCst))
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
