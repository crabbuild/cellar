// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! An independent authenticated peer sends adversarial replies to the production receiver.
use super::super::network_probes::Connection;
use super::*;
use cellar::application::consensus::verification::JoiningMember;
use protobuf::Message as _;
use raft::prelude::{ConfChangeTransition, ConfChangeType};
use serde_json::{json, Value};
use tokio::net::TcpListener;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fault {
    Exporter,
    Correlation,
    Nonce,
    Process,
    Digest,
    Barrier,
    Decimal,
    ExtraReply,
    Descriptor,
    ReplyLimit,
    Binding,
    Sequence,
    ChunkDigest,
    ExtraChunk,
    Offset,
    EarlyEnd,
    ChunkLimit,
    Disconnect,
    Replay,
    EndOffset,
    EndBody,
    Valid,
    Revoked,
}

impl Fault {
    fn reply(self) -> bool {
        matches!(
            self,
            Self::Exporter
                | Self::Correlation
                | Self::Nonce
                | Self::Process
                | Self::Digest
                | Self::Barrier
                | Self::Decimal
                | Self::ExtraReply
                | Self::Descriptor
                | Self::ReplyLimit
        )
    }
}

fn expected_error(fault: Fault, error: &CellGroupSeedError) -> bool {
    use CellGroupSeedError::Transport;
    use TransportError::*;
    matches!(
        (fault, error),
        (
            Fault::Exporter
                | Fault::Correlation
                | Fault::Nonce
                | Fault::Process
                | Fault::Digest
                | Fault::Binding
                | Fault::ChunkDigest,
            Transport(Authentication)
        ) | (
            Fault::Decimal
                | Fault::ExtraReply
                | Fault::Descriptor
                | Fault::ExtraChunk
                | Fault::EndOffset
                | Fault::EndBody,
            Transport(Protocol)
        ) | (Fault::ReplyLimit | Fault::ChunkLimit, Transport(Limit))
            | (Fault::Sequence | Fault::Replay, Transport(Replay))
            | (Fault::Offset | Fault::EarlyEnd, Transport(Invalid(_)))
            | (Fault::Disconnect, Transport(Io(_) | Closed))
            | (Fault::Barrier, CellGroupSeedError::Checkpoint(_))
            | (Fault::Revoked, Transport(StalePeer | Closed))
    )
}

struct Part {
    header: Value,
    bytes: Vec<u8>,
}

struct Capture {
    body: Vec<u8>,
    barrier: u64,
    parts: Vec<Part>,
}

async fn capture(fixture: &RuntimeCluster, history: GroupHistory) -> Result<Arc<Capture>> {
    let authority = fixture.runtimes[0]
        .seed_authority(
            seed_install::target(history)?,
            reads::deadline(fixture, 10_000).await?,
        )
        .await?;
    let barrier = authority.barrier().index.get();
    fixture.cluster.domain.blocking(move || {
        let pool = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
        let mut source = authority.capture(&pool)?;
        let body = source.snapshot()?.write_to_bytes()?;
        let mut parts = Vec::new();
        let mut retained = 0;
        // Only this small adversarial test peer buffers a captured cut. Production
        // transfer remains streamed; bound this fixture before retaining bytes.
        source.stream(|chunk| {
            retained += chunk.bytes.len();
            assert!(retained < 2 * 1024 * 1024);
            let part = match chunk.part {
                CellGroupCheckpointPart::Header => json!({"kind":"Header"}),
                CellGroupCheckpointPart::Page(digest) => json!({"kind":"Page", "digest":digest}),
                CellGroupCheckpointPart::Materialized(cell) => json!({"kind":"Materialized", "cell":cell.as_str()}),
                CellGroupCheckpointPart::ExtentManifest(cell) => json!({"kind":"ExtentManifest", "cell":cell.as_str()}),
                CellGroupCheckpointPart::Extent { cell, ordinal, id } => json!({"kind":"Extent", "cell":cell.as_str(), "ordinal":ordinal, "digest":id.bytes()}),
            };
            parts.push(Part { header: json!({"sequence":parts.len(), "part":part,
                "offset":chunk.offset, "length":chunk.length, "digest":<[u8;32]>::from(Sha256::digest(chunk.bytes))}), bytes:chunk.bytes.to_vec() });
            Ok(())
        })?;
        Ok(Arc::new(Capture { body, barrier, parts }))
    })?.await?
}

fn acceptor() -> Result<tokio_rustls::TlsAcceptor> {
    let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
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
    config.alpn_protocols = vec![b"cellar-raft-1".to_vec()];
    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(config)))
}

async fn serve(listener: Arc<TcpListener>, cut: Arc<Capture>, fault: Fault) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(10), async {
        let (tcp, _) = listener.accept().await?;
        let stream = acceptor()?.accept(tcp).await?;
        let exporter = stream.get_ref().1.export_keying_material(
            [0; 32],
            b"EXPORTER-cellar-group-seed-1",
            None,
        )?;
        let mut wire = Connection::from_stream(stream, exporter);
        let (request, _, request_tag) = wire
            .receive(b"CLGJ", b"cellar-group-seed-request-1", &[])
            .await?;
        let mut route = request["route"].clone();
        route["source"] = request["route"]["target"].clone();
        route["target"] = request["route"]["source"].clone();
        let mut body = cut.body.clone();
        let mut reply = json!({"route":route, "request":request_tag, "nonce":request["nonce"],
            "barrier":cut.barrier.to_string(), "digest":<[u8;32]>::from(Sha256::digest(&body))});
        match fault {
            Fault::Exporter => wire.exporter[0] ^= 1,
            Fault::Correlation => reply["request"] = json!(vec![0; 32]),
            Fault::Nonce => reply["nonce"] = json!("999999"),
            Fault::Process => reply["route"]["source"]["process"] = json!(vec![244; 16]),
            Fault::Digest => reply["digest"] = json!(vec![0; 32]),
            Fault::Barrier => reply["barrier"] = json!(u64::MAX.to_string()),
            Fault::Decimal => reply["barrier"] = json!("01"),
            Fault::ExtraReply => reply["extra"] = json!(true),
            Fault::Descriptor => {
                body = vec![255];
                reply["digest"] = json!(<[u8; 32]>::from(Sha256::digest(&body)));
            }
            Fault::ReplyLimit => {
                oversized(&mut wire, b"CLGK").await?;
                return wire.refused().await;
            }
            _ => (),
        }
        let binding = wire
            .frame(
                b"CLGK",
                b"cellar-group-seed-grant-1",
                &request_tag,
                reply,
                &body,
            )
            .await?;
        if fault.reply() || fault == Fault::Revoked {
            return wire.refused().await;
        }
        let (ready, body, _) = wire
            .receive(b"CLGL", b"cellar-group-seed-ready-1", &binding)
            .await?;
        assert_eq!(ready, json!({"sequence":0}));
        assert!(body.is_empty());
        if fault == Fault::ChunkLimit {
            oversized(&mut wire, b"CLGM").await?;
            return wire.refused().await;
        }
        if fault == Fault::EarlyEnd {
            wire.frame(b"CLGM", b"cellar-group-seed-chunk-1", &binding, end(0), &[])
                .await?;
            return wire.refused().await;
        }
        for (i, part) in cut.parts.iter().enumerate() {
            let mut header = part.header.clone();
            let mut bound = binding;
            if i == 0 {
                match fault {
                    Fault::Binding => bound[0] ^= 1,
                    Fault::Sequence => header["sequence"] = json!(1),
                    Fault::ChunkDigest => header["digest"] = json!(vec![0; 32]),
                    Fault::ExtraChunk => header["extra"] = json!(true),
                    Fault::Offset => header["offset"] = json!(1),
                    _ => (),
                }
            }
            wire.frame(
                b"CLGM",
                b"cellar-group-seed-chunk-1",
                &bound,
                header,
                &part.bytes,
            )
            .await?;
            if i == 0 && fault == Fault::Disconnect {
                return Ok(());
            }
            if i == 0
                && matches!(
                    fault,
                    Fault::Binding
                        | Fault::Sequence
                        | Fault::ChunkDigest
                        | Fault::ExtraChunk
                        | Fault::Offset
                        | Fault::Replay
                )
            {
                if fault == Fault::Replay {
                    wire.frame(
                        b"CLGM",
                        b"cellar-group-seed-chunk-1",
                        &binding,
                        part.header.clone(),
                        &part.bytes,
                    )
                    .await?;
                }
                return wire.refused().await;
            }
        }
        let mut header = end(cut.parts.len());
        let body = if fault == Fault::EndBody {
            vec![1]
        } else {
            vec![]
        };
        if fault == Fault::EndOffset {
            header["offset"] = json!(1);
        }
        header["digest"] = json!(<[u8; 32]>::from(Sha256::digest(&body)));
        wire.frame(
            b"CLGM",
            b"cellar-group-seed-chunk-1",
            &binding,
            header,
            &body,
        )
        .await?;
        wire.refused().await // The protocol has no install acknowledgement on this stream.
    })
    .await?
}

fn end(sequence: usize) -> Value {
    json!({"sequence":sequence, "part":null, "offset":0, "length":0, "digest":<[u8;32]>::from(Sha256::digest([]))})
}

async fn oversized<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
    wire: &mut Connection<S>,
    magic: &[u8; 4],
) -> Result<()> {
    let mut prefix = [0; 20];
    prefix[..4].copy_from_slice(magic);
    prefix[4..8].copy_from_slice(&1u32.to_be_bytes());
    prefix[8..12].copy_from_slice(&1u32.to_be_bytes());
    prefix[12..].copy_from_slice(&65537u64.to_be_bytes());
    wire.raw(&prefix).await
}

pub(crate) async fn seed_receiver_faults(rooted: bool) -> Result<()> {
    let class = RaftClass::new("Counter", 1, [81; 32])?;
    let (fixture, route, _, _) = Box::pin(registered::setup(rooted, &class)).await?;
    Box::pin(run(fixture, route, rooted)).await
}

async fn run(mut fixture: RuntimeCluster, route: GroupRoute, rooted: bool) -> Result<()> {
    let mut controllers = controller::start_with_limits(&mut fixture, controller::limits()).await?;
    let local = seed_install::target(route.history)?;
    membership::configure(
        &fixture,
        &route,
        membership::change(
            4,
            ConfChangeType::AddLearnerNode,
            ConfChangeTransition::Auto,
        ),
        &[JoiningMember {
            member: local.member,
            node: local.node,
            disk: local.disk,
        }],
    )
    .await?;
    let cut = capture(&fixture, route.history).await?;
    let listener = Arc::new(TcpListener::bind("127.0.0.1:0").await?);
    let mut peer = fixture.tcp_route(route.history)?.peers[0].clone();
    peer.data_address = listener.local_addr()?;
    let base = fixture.cluster._root.path().join("seed-receiver");
    let log = fixture
        .cluster
        .domain
        .run(ReplicaLog::open(
            ReplicaLogOptions::new(
                base.join("log"),
                local.node,
                local.disk,
                local.process,
                LogOpenMode::Create,
            ),
            Arc::new(DirectFileSystem),
        ))
        .await??;
    let (runtime, events) = CellGroupRuntime::start(
        log.clone(),
        RaftRuntimeOptions {
            tick_interval_ms: None,
            ..Default::default()
        },
    )
    .await?;
    let transport = tcp::start(&runtime, 3).await?;
    let pool = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    transport.enable_group_checkpoints(pool.clone())?;
    let baseline = transport.usage().tasks;
    let mut applier: Option<CellGroupApplier> = None;
    for fault in [
        Fault::Exporter,
        Fault::Correlation,
        Fault::Nonce,
        Fault::Process,
        Fault::Digest,
        Fault::Barrier,
        Fault::Decimal,
        Fault::ExtraReply,
        Fault::Descriptor,
        Fault::ReplyLimit,
        Fault::Binding,
        Fault::Sequence,
        Fault::ChunkDigest,
        Fault::ExtraChunk,
        Fault::Offset,
        Fault::EarlyEnd,
        Fault::ChunkLimit,
        Fault::Disconnect,
        Fault::Replay,
        Fault::EndOffset,
        Fault::EndBody,
        Fault::Valid,
    ] {
        let service = fixture
            .cluster
            .domain
            .spawn(serve(listener.clone(), cut.clone(), fault))?;
        let prepared = Box::pin(transport.prepare_group_seed(
            local.clone(),
            peer.clone(),
            fixture.cluster.directory.clone(),
            fixture.cluster.birth.clone(),
            reads::deadline(&fixture, 5000).await?,
        ))
        .await;
        if fault.reply() {
            let error = prepared
                .err()
                .with_context(|| format!("{fault:?} accepted descriptor"))?;
            assert!(expected_error(fault, &error), "{fault:?}: {error:?}");
            assert!(
                matches!(log.cell_group(&route.history), Err(LogError::NotFound)),
                "{fault:?} created a stage"
            );
        } else {
            let session = prepared.with_context(|| format!("prepare {fault:?}"))?;
            if applier.is_none() {
                let group = session.group().clone();
                let (pages, images) = seed_install::stores(&base, rooted)?;
                let (owner, root) = fixture
                    .cluster
                    .domain
                    .blocking(move || {
                        CellGroupApplier::recover(
                            group,
                            pages,
                            images,
                            ManifestLimits::default(),
                            GroupEntryLimits::default(),
                            100,
                        )
                    })?
                    .await??;
                assert_eq!(root.marker().root().cells(), 0);
                applier = Some(owner);
            }
            let result = session.install(applier.as_ref().unwrap().clone()).await;
            if fault == Fault::Valid {
                let (_permit, root) = result?;
                assert_eq!(root.marker().root().cells(), 2);
            } else {
                let error = result
                    .err()
                    .with_context(|| format!("{fault:?} installed"))?;
                assert!(expected_error(fault, &error), "{fault:?}: {error:?}");
                let group = log.cell_group(&route.history)?;
                assert_eq!(group.group_manifest()?.context("stage")?.root().cells(), 0);
                assert!(group.checkpoint_manifest()?.is_none());
                assert!(fixture
                    .cluster
                    .domain
                    .run(fixture.cluster.directory.bootstrap_joined_group_member(
                        &fixture.cluster.birth,
                        local.member,
                        &log
                    ))
                    .await?
                    .is_err());
            }
        }
        service
            .await?
            .with_context(|| format!("independent peer {fault:?}"))?;
        controller::wait_for(async || {
            Ok(transport.usage().tasks == baseline && pool.usage().sources == 0)
        })
        .await
        .with_context(|| format!("release {fault:?}"))?;
        assert_eq!(transport.usage().data_items, 0);
    }
    // Capture a later valid cut, then replace the source process while its signed
    // grant is admitted at the destination but before installation begins.
    let bytes = images::image(None, "CREATE TABLE value(n);")?;
    let proposal = fixture.runtimes[0]
        .propose(
            route.history,
            apply::enrollment_bytes(
                &route,
                &history(&route, "revoked-later-cut")?,
                &bytes,
                rooted,
            )?,
        )
        .await?;
    controller::applied(&fixture, route.history, proposal.position.index).await?;
    let later = capture(&fixture, route.history).await?;
    let before = log.cell_group(&route.history)?.group_manifest()?;
    let checkpoint = log.cell_group(&route.history)?.checkpoint_manifest()?;
    let service = fixture
        .cluster
        .domain
        .spawn(serve(listener, later, Fault::Revoked))?;
    let session = Box::pin(transport.prepare_group_seed(
        local.clone(),
        peer.clone(),
        fixture.cluster.directory.clone(),
        fixture.cluster.birth.clone(),
        reads::deadline(&fixture, 5000).await?,
    ))
    .await?;
    controllers.remove(0).close().await?;
    let new_process = ProcessGeneration::new([99; 16])?;
    let options = ReplicaLogOptions::new(
        fixture.cluster._root.path().join("node-1/log"),
        peer.replica.node,
        peer.replica.disk,
        new_process,
        LogOpenMode::Resume,
    );
    fixture.cluster.voters[0].log.close().await?;
    let restarted = fixture
        .cluster
        .domain
        .run(ReplicaLog::open(options, Arc::new(DirectFileSystem)))
        .await??;
    let trusted = RaftPeerNode::from(&peer);
    let mut latest = trusted.clone();
    latest.process = new_process;
    let view = fixture
        .cluster
        .domain
        .run(async {
            let catalog = Catalog::new(fixture.cluster.store.clone(), CatalogOptions::default())?;
            let namespace = catalog
                .open_namespace(NamespaceConfig::new(
                    route.history.cluster,
                    route.history.namespace,
                    cellar_logic::NamespaceMode::Raft,
                    route.schema_digest,
                ))
                .await?;
            let registration = namespace
                .register_node_storage(&restarted, Duration::from_secs(5))
                .await?;
            Ok::<_, anyhow::Error>(
                registration
                    .publication(latest, Duration::from_secs(5))
                    .await?
                    .publish(Duration::from_secs(5))
                    .await?,
            )
        })
        .await??;
    transport.accept_node_view(view, &trusted)?;
    let error = session
        .install(applier.clone().unwrap())
        .await
        .err()
        .context("revoked grant installed")?;
    assert!(
        expected_error(Fault::Revoked, &error),
        "revocation: {error:?}"
    );
    service.await??;
    controller::wait_for(async || {
        Ok(transport.usage().tasks == baseline && pool.usage().sources == 0)
    })
    .await?;
    assert_eq!(log.cell_group(&route.history)?.group_manifest()?, before);
    assert_eq!(
        log.cell_group(&route.history)?.checkpoint_manifest()?,
        checkpoint
    );
    assert!(matches!(
        Box::pin(transport.prepare_group_seed(
            local,
            peer,
            fixture.cluster.directory.clone(),
            fixture.cluster.birth.clone(),
            reads::deadline(&fixture, 1000).await?
        ))
        .await,
        Err(CellGroupSeedError::Transport(TransportError::StalePeer))
    ));
    restarted.close().await?;
    transport.close().await;
    runtime.close().await?;
    drop((events, applier));
    log.close().await?;
    for controller in controllers {
        controller.close().await?;
    }
    fixture.close().await
}
