// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
#[path = "transport_directory_tests.rs"]
mod directory;
#[path = "transport_pipeline_tests.rs"]
mod pipeline;
#[path = "transport_pipeline_regression_tests.rs"]
mod pipeline_regressions;
#[path = "transport_query_tests.rs"]
mod queries;
use crate::raft::options::{LogOpenMode, RaftRuntimeOptions, ReplicaLogOptions};
use crate::raft::storage::ReplicaLog;
use cellar_logic::consensus::{DiskLineage, ProcessGeneration};
use cellar_logic::consensus::{GroupId, IncarnationId, MemberId, NodeIdentity};

#[derive(Default)]
struct FrameWriter {
    bytes: Vec<u8>,
    writes: usize,
    flushes: usize,
    limit: Option<usize>,
    pending: bool,
}

impl AsyncWrite for FrameWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.as_mut()
            .poll_write_vectored(cx, &[std::io::IoSlice::new(bytes)])
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        slices: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        if self.pending {
            self.pending = false;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        self.writes += 1;
        let mut remaining = self.limit.unwrap_or(usize::MAX);
        let mut written = 0;
        for slice in slices {
            let take = remaining.min(slice.len());
            self.bytes.extend_from_slice(&slice[..take]);
            written += take;
            remaining -= take;
        }
        self.pending = true;
        Poll::Ready(Ok(written))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        self.flushes += 1;
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn frame_head_coalesces_tls_writes_without_changing_bytes() -> anyhow::Result<()> {
    let head = FrameHead {
        prefix: [1; frame::PREFIX],
        header: vec![2; 113],
        tag: [3; frame::TAG],
        payload_len: 0,
    };
    let expected = [&head.prefix[..], &head.header, &head.tag].concat();
    let mut writer = FrameWriter::default();
    write_head(&mut writer, &head).await?;
    assert_eq!(writer.bytes, expected);
    assert_eq!(writer.flushes, 1);
    assert_eq!(
        writer.writes, 1,
        "one frame header should produce one TLS write"
    );
    for limit in [1, 7, 23, 101] {
        let mut writer = FrameWriter {
            limit: Some(limit),
            ..Default::default()
        };
        write_head(&mut writer, &head).await?;
        assert_eq!(writer.bytes, expected);
        assert_eq!(writer.flushes, 1);
    }
    let mut writer = FrameWriter {
        limit: Some(0),
        ..Default::default()
    };
    assert!(matches!(
        write_head(&mut writer, &head).await,
        Err(TransportError::Io(_))
    ));
    assert_eq!(writer.flushes, 0);
    Ok(())
}

#[tokio::test]
async fn partial_frame_writes_preserve_header_and_body_boundaries() -> anyhow::Result<()> {
    let body = vec![4; 239];
    let head = FrameHead {
        prefix: [1; frame::PREFIX],
        header: vec![2; 113],
        tag: [3; frame::TAG],
        payload_len: body.len(),
    };
    let expected = [&head.prefix[..], &head.header, &head.tag, &body].concat();
    for limit in [1, 7, 23, 101, 4096] {
        let mut writer = FrameWriter {
            limit: Some(limit),
            ..Default::default()
        };
        write_frame(&mut writer, &head, &body).await?;
        assert_eq!(writer.bytes, expected);
        assert_eq!(writer.flushes, 1);
        assert_eq!(writer.writes, expected.len().div_ceil(limit));
    }
    Ok(())
}

pub(super) struct Fixture {
    pub(super) domain: crate::asyncrt::ExecutionDomain,
    log: ReplicaLog,
    runtime: RaftRuntime,
    pub(super) transport: RaftTransport,
    _events: crate::raft::runtime::RuntimeEvents,
    _directory: tempfile::TempDir,
}

impl Fixture {
    pub(super) async fn start(
        options: TransportOptions,
        groups: Vec<crate::raft::storage::GroupBootstrap>,
    ) -> anyhow::Result<Self> {
        let directory = tempfile::tempdir()?;
        let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
        let log = domain
            .run(ReplicaLog::open(
                ReplicaLogOptions::new(
                    directory.path().join("log"),
                    NodeId::new([1; 16])?,
                    DiskLineage::new([2; 16])?,
                    ProcessGeneration::new([3; 16])?,
                    LogOpenMode::Create,
                ),
                Arc::new(cellar_ltx::DirectFileSystem),
            ))
            .await??;
        for group in groups {
            log.create_group(group).await?;
        }
        let (runtime, events) =
            RaftRuntime::start(log.clone(), RaftRuntimeOptions::default()).await?;
        let tls = RaftTls::new(
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
        )?;
        let transport = RaftTransport::start(
            runtime.clone(),
            Arc::new(PeerAuth::new([8; 32], "node-1")?),
            tls,
            TcpListener::bind("127.0.0.1:0").await?,
            TcpListener::bind("127.0.0.1:0").await?,
            TcpListener::bind("127.0.0.1:0").await?,
            options,
        )
        .await?;
        Ok(Self {
            domain,
            log,
            runtime,
            transport,
            _events: events,
            _directory: directory,
        })
    }

    pub(super) async fn close(self) -> anyhow::Result<()> {
        self.transport.close().await;
        self.runtime.close().await?;
        self.log.close().await?;
        self.domain.shutdown().await;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connection_resources_count_handshakes_until_physical_exit() -> anyhow::Result<()> {
    let fixture = Fixture::start(
        TransportOptions {
            max_connections_per_lane: 1,
            ..Default::default()
        },
        vec![],
    )
    .await?;
    let observer = fixture.transport.resource_observer();
    let result = fixture
        .domain
        .run(async {
            let control = TcpStream::connect(fixture.transport.control_address()).await?;
            let data = TcpStream::connect(fixture.transport.data_address()).await?;
            let query = TcpStream::connect(fixture.transport.query_address()).await?;
            // No TLS bytes are sent. These own real sockets while awaiting TLS,
            // despite having no authenticated frame or byte-bank reservation.
            crate::asyncrt::timeout(std::time::Duration::from_secs(2), async {
                loop {
                    let usage = observer.capture().unwrap();
                    if usage.incoming_connections
                        == (crate::application::consensus::RaftConnectionReservations {
                            control: 1,
                            data: 1,
                            query: 1,
                        })
                    {
                        assert_eq!(
                            usage.incoming_connection_limits,
                            crate::application::consensus::RaftConnectionReservations {
                                control: 1,
                                data: 1,
                                query: 1
                            }
                        );
                        assert_eq!(
                            usage.incoming.control.items
                                + usage.incoming.data.items
                                + usage.incoming.query.items,
                            0
                        );
                        break;
                    }
                    crate::asyncrt::sleep(std::time::Duration::from_millis(1)).await;
                }
            })
            .await?;
            drop((control, data, query));
            crate::asyncrt::timeout(std::time::Duration::from_secs(2), async {
                loop {
                    let usage = observer.capture().unwrap();
                    if usage.incoming_connections
                        == (crate::application::consensus::RaftConnectionReservations {
                            control: 0,
                            data: 0,
                            query: 0,
                        })
                    {
                        break;
                    }
                    crate::asyncrt::sleep(std::time::Duration::from_millis(1)).await;
                }
            })
            .await?;
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    fixture.close().await?;
    assert!(observer.capture().is_none());
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn poisoned_registry_still_waits_for_resource_owners() -> anyhow::Result<()> {
    let fixture = Fixture::start(TransportOptions::default(), vec![]).await?;
    let (release, wait) = oneshot::channel();
    // Model a resource destructor which is still finishing after admission
    // closes. close() must retain its claim until this owner actually leaves.
    fixture.transport.handle.0.spawn(async move {
        let _ = wait.await;
    })?;
    let poisoned = fixture.transport.handle.0.clone();
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _registry = poisoned.registry.lock().unwrap();
            panic!("injected registry panic");
        }))
        .is_err()
    );
    fixture
        .domain
        .run(async {
            let close = fixture.transport.close();
            tokio::pin!(close);
            assert!(
                crate::asyncrt::timeout(std::time::Duration::from_millis(20), &mut close)
                    .await
                    .is_err()
            );
            assert!(fixture.transport.usage().tasks >= 1);
            assert!(fixture.transport.handle.0.claim.lock().unwrap().is_some());
            release.send(()).unwrap();
            crate::asyncrt::timeout(std::time::Duration::from_secs(2), close).await?;
            assert_eq!(fixture.transport.usage().tasks, 0);
            assert!(fixture.transport.handle.0.claim.lock().unwrap().is_none());
            Ok::<_, anyhow::Error>(())
        })
        .await??;
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn command_grants_bound_tombstones_without_changing_read_permission() -> anyhow::Result<()> {
    use crate::raft::transport_options::NamespaceCommandOrigin;
    let fixture = Fixture::start(
        TransportOptions {
            max_command_origins: 2,
            ..Default::default()
        },
        vec![],
    )
    .await?;
    fixture
        .domain
        .run(async {
            let peer = RaftPeerNode {
                node: NodeId::new([4; 16])?,
                disk: DiskLineage::new([5; 16])?,
                process: ProcessGeneration::new([6; 16])?,
                label: "node-4".into(),
                server_name: "localhost".into(),
                control_address: "127.0.0.1:41000".parse()?,
                data_address: "127.0.0.1:41001".parse()?,
                query_address: "127.0.0.1:41002".parse()?,
            };
            let grant = NamespaceCommandOrigin {
                cluster: ClusterId::new([1; 16])?,
                namespace: NamespaceId::new([2; 16])?,
                revision: 1,
                peer: peer.clone(),
            };
            let key = (grant.cluster, grant.namespace, peer.node);
            let transport = &fixture.transport;
            transport.grant_command_origin(grant.clone())?;
            transport.grant_command_origin(grant.clone())?;
            let old = {
                let registry = transport.handle.0.registry.lock().unwrap();
                assert!(registry.origins.is_empty());
                assert_eq!(registry.command_origins.len(), 1);
                registry.command_origins[&key].clone()
            };
            assert_eq!(transport.usage().peers, 1);
            let mut foreign = grant.clone();
            foreign.namespace = NamespaceId::new([3; 16])?;
            transport.grant_command_origin(foreign.clone())?;
            let mut full = grant.clone();
            full.namespace = NamespaceId::new([7; 16])?;
            assert_eq!(
                transport.grant_command_origin(full.clone()),
                Err(TransportError::Overloaded)
            );
            transport.revoke_command_origin(grant.cluster, grant.namespace, peer.node, 2)?;
            assert!(*old.stop.borrow());
            assert_eq!(
                transport.grant_command_origin(grant.clone()),
                Err(TransportError::StalePeer)
            );
            assert_eq!(
                transport.revoke_command_origin(full.cluster, full.namespace, peer.node, 2),
                Err(TransportError::Overloaded)
            );
            // A read grant neither removes the write tombstone nor grants writes.
            transport.grant_query_origin(NamespaceQueryOrigin {
                cluster: grant.cluster,
                namespace: grant.namespace,
                revision: 1,
                peer: peer.clone(),
            })?;
            {
                let registry = transport.handle.0.registry.lock().unwrap();
                assert_eq!(registry.command_origins.len(), 2);
                assert!(registry.command_origins[&key].peer.is_none());
                assert!(registry.origins[&key].peer.is_some());
            }
            let mut restored = grant.clone();
            restored.revision = 3;
            transport.grant_command_origin(restored)?;
            transport.revoke_command_origin(grant.cluster, grant.namespace, peer.node, 4)?;
            transport.revoke_command_origin(foreign.cluster, foreign.namespace, peer.node, 2)?;
            assert_eq!(transport.usage().peers, 1); // Independent read grant retains its pool.
            transport.revoke_query_origin(grant.cluster, grant.namespace, peer.node, 2)?;
            assert_eq!(transport.usage().peers, 0);
            assert!(fixture.log.groups()?.is_empty());
            Ok::<_, anyhow::Error>(())
        })
        .await??;
    fixture.close().await
}
