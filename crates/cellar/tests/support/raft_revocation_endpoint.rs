//! Controlled old-process TLS sinks. They never produce a readiness response.
use super::*;
use cellar::application::consensus::verification::NodePublication;
use futures_util::{future::BoxFuture, stream::FuturesUnordered, StreamExt};
use std::{future::Future, pin::Pin};
use tokio::{
    io::AsyncReadExt,
    net::TcpListener,
    sync::{oneshot, watch},
};

pub(super) type Pending =
    Pin<Box<dyn Future<Output = Result<RaftPeerNodeObservation, InspectionError>> + Send>>;

#[derive(Default)]
pub(super) struct Counts {
    pub(super) active: AtomicUsize,
    pub(super) started: AtomicUsize,
    pub(super) closed: AtomicUsize,
    pub(super) metadata_started: AtomicUsize,
    pub(super) metadata_closed: AtomicUsize,
}

struct Session {
    counts: Arc<Counts>,
    metadata: bool,
}

impl Drop for Session {
    fn drop(&mut self) {
        self.counts.active.fetch_sub(1, Ordering::AcqRel);
        self.counts.closed.fetch_add(1, Ordering::AcqRel);
        if self.metadata {
            self.counts.metadata_closed.fetch_add(1, Ordering::AcqRel);
        }
    }
}

pub(super) struct Endpoint {
    pub(super) log: ReplicaLog,
    pub(super) publication: NodePublication,
    pub(super) peer: RaftPeerNode,
    pub(super) counts: Arc<Counts>,
    ready: oneshot::Receiver<()>,
    stop: watch::Sender<bool>,
    servers: Vec<BoxFuture<'static, Result<()>>>,
}

impl Drop for Endpoint {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}

impl Endpoint {
    pub(super) async fn open(
        f: &Fixture,
        path: std::path::PathBuf,
        id: u8,
        generation: u8,
    ) -> Result<Self> {
        let spec = f.peer_spec(id)?;
        let process = ProcessGeneration::new([generation; 16])?;
        let log = f
            .domain
            .run(ReplicaLog::open(
                ReplicaLogOptions::new(path, spec.node, spec.disk, process, LogOpenMode::Resume),
                Arc::new(cellar_ltx::DirectFileSystem),
            ))
            .await??;
        let listeners = [
            TcpListener::bind("127.0.0.1:0").await?,
            TcpListener::bind("127.0.0.1:0").await?,
            TcpListener::bind("127.0.0.1:0").await?,
        ];
        let peer = RaftPeerNode {
            node: spec.node,
            disk: spec.disk,
            process,
            label: spec.label,
            server_name: spec.server_name,
            control_address: listeners[0].local_addr()?,
            data_address: listeners[1].local_addr()?,
            query_address: listeners[2].local_addr()?,
        };
        let namespace = Catalog::new(
            f.store.clone(),
            CatalogOptions {
                prefix: "host-enrollment/".into(),
                ..Default::default()
            },
        )?
        .existing_namespace(f.declaration.namespace.clone())
        .await?;
        let publication = namespace
            .register_node_storage(&log, TIMEOUT)
            .await?
            .publication(peer.clone(), TIMEOUT)
            .await?;
        publication.publish(TIMEOUT).await?;
        let config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_no_client_auth()
        .with_single_cert(
            vec![include_bytes!("../../transport/tcp/fixtures/server.der")
                .to_vec()
                .into()],
            rustls::pki_types::PrivatePkcs8KeyDer::from(
                include_bytes!("../../transport/tcp/fixtures/key.der").to_vec(),
            )
            .into(),
        )?;
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
        let counts = Arc::new(Counts::default());
        let (ready, receiver) = oneshot::channel();
        let ready = Arc::new(std::sync::Mutex::new(Some(ready)));
        let (stop, _) = watch::channel(false);
        let mut servers: Vec<BoxFuture<'static, Result<()>>> = vec![];
        for listener in listeners {
            let acceptor = acceptor.clone();
            let counts = counts.clone();
            let ready = ready.clone();
            let mut stopping = stop.subscribe();
            let task = f.domain.spawn(async move {
                let mut sessions: FuturesUnordered<BoxFuture<'static, std::io::Result<()>>> = FuturesUnordered::new();
                loop {
                    cellar::asyncrt::select_biased! { "stop accepting without cancelling a connection that the test must join";
                        _ = stopping.changed() => break,
                        result = sessions.next(), if !sessions.is_empty() => if let Some(result) = result { disconnected(result)?; },
                        accepted = listener.accept(), if sessions.len() < 16 => {
                            let (stream, _) = accepted?;
                            counts.active.fetch_add(1, Ordering::AcqRel);
                            counts.started.fetch_add(1, Ordering::AcqRel);
                            let mut owner = Session {counts:counts.clone(),metadata:false};
                            let acceptor = acceptor.clone();
                            let ready = ready.clone();
                            sessions.push(Box::pin(async move {
                                let mut stream = acceptor.accept(stream).await?;
                                let mut prefix = [0;20];
                                stream.read_exact(&mut prefix).await?;
                                if &prefix[..4] == b"CLNQ" {
                                    owner.metadata = true;
                                    owner.counts.metadata_started.fetch_add(1,Ordering::AcqRel);
                                    if let Some(ready) = ready.lock().map_err(|_|std::io::Error::other("endpoint readiness lock"))?.take() { let _ = ready.send(()); }
                                }
                                let mut buffer = [0;1024];
                                while stream.read(&mut buffer).await? != 0 {}
                                Ok(())
                            }));
                        },
                    }
                }
                while let Some(result) = sessions.next().await { disconnected(result)?; }
                Ok::<_,anyhow::Error>(())
            })?;
            servers.push(Box::pin(async move {
                task.await??;
                Ok(())
            }));
        }
        Ok(Self {
            log,
            publication,
            peer,
            counts,
            ready: receiver,
            stop,
            servers,
        })
    }

    pub(super) async fn block(
        &mut self,
        f: &Fixture,
        host: &RaftCells,
        revision: u64,
    ) -> Result<Pending> {
        f.domain.run(cellar::asyncrt::timeout(TIMEOUT,async {
            loop {
                let mut pending = host.inspect_peer_node(self.peer.node,revision,Duration::from_secs(30));
                cellar::asyncrt::select_biased! { "keep the exact call whose header reached the old process";
                    ready = &mut self.ready => { ready?; return Ok::<_,anyhow::Error>(pending); },
                    result = &mut pending => match result {
                        Err(InspectionError::Unreachable | InspectionError::Unavailable | InspectionError::StalePeer | InspectionError::Refused) => cellar::asyncrt::sleep(Duration::from_millis(10)).await,
                        result => anyhow::bail!("unexpected sink response: {result:?}"),
                    },
                }
            }
        })).await??
    }

    pub(super) async fn joined(&self, f: &Fixture) -> Result<()> {
        f.domain
            .run(cellar::asyncrt::timeout(TIMEOUT, async {
                while self.counts.active.load(Ordering::Acquire) != 0 {
                    cellar::asyncrt::sleep(Duration::from_millis(5)).await;
                }
            }))
            .await??;
        ensure!(
            self.counts.started.load(Ordering::Acquire)
                == self.counts.closed.load(Ordering::Acquire)
        );
        ensure!(
            self.counts.metadata_started.load(Ordering::Acquire) > 0
                && self.counts.metadata_started.load(Ordering::Acquire)
                    == self.counts.metadata_closed.load(Ordering::Acquire)
        );
        Ok(())
    }

    pub(super) async fn close(mut self, f: &Fixture) -> Result<()> {
        self.joined(f).await?;
        self.stop.send_replace(true);
        for server in self.servers.drain(..) {
            f.domain
                .run(cellar::asyncrt::timeout(TIMEOUT, server))
                .await???;
        }
        self.log.close().await?;
        Ok(())
    }
}

fn disconnected(result: std::io::Result<()>) -> Result<()> {
    match result {
        Ok(()) => Ok(()),
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::UnexpectedEof
                    | std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::BrokenPipe
            ) =>
        {
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}
