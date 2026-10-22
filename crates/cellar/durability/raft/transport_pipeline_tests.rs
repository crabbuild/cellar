//! Real TLS regression for independent groups queued on one physical peer lane.
use super::*;
use crate::raft::storage::GroupBootstrap;

#[tokio::test]
async fn queued_groups_send_before_first_admission_reply() -> anyhow::Result<()> {
    tls_pipeline(false).await
}

#[tokio::test]
async fn invalid_ack_refuses_window_and_releases_admission() -> anyhow::Result<()> {
    tls_pipeline(true).await
}

async fn tls_pipeline(invalid_ack: bool) -> anyhow::Result<()> {
    let first = GroupScope {
        cluster: ClusterId::new([1; 16])?,
        namespace: NamespaceId::new([2; 16])?,
        cell: "Counter:first".into(),
        group: GroupId::new([3; 16])?,
        incarnation: IncarnationId::new([4; 16])?,
    };
    let mut second = first.clone();
    second.cell = "Counter:second".into();
    second.group = GroupId::new([5; 16])?;
    let groups = [&first, &second]
        .into_iter()
        .map(|scope| {
            Ok(GroupBootstrap {
                scope: scope.clone(),
                member: MemberId::new(1)?,
                birth_digest: [10; 32],
                voters: vec![MemberId::new(1)?, MemberId::new(2)?],
                learners: vec![],
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let peer_address = listener.local_addr()?;
    let data_listener = TcpListener::bind("127.0.0.1:0").await?;
    let query_listener = TcpListener::bind("127.0.0.1:0").await?;
    let fixture = Fixture::start(TransportOptions::default(), groups).await?;
    let result = fixture
        .domain
        .run(async {
            for scope in [&first, &second] {
                fixture.transport.install_group(GroupRoute {
                    scope: scope.clone(),
                    schema_digest: [9; 32],
                    revision: 1,
                    peers: vec![
                        RaftPeer {
                            replica: fixture.log.group(scope)?.identity().clone(),
                            label: "node-1".into(),
                            server_name: "localhost".into(),
                            control_address: fixture.transport.control_address(),
                            data_address: fixture.transport.data_address(),
                            query_address: fixture.transport.query_address(),
                        },
                        RaftPeer {
                            replica: ReplicaIdentity {
                                scope: scope.clone(),
                                member: MemberId::new(2)?,
                                node: NodeId::new([2; 16])?,
                                disk: DiskLineage::new([3; 16])?,
                                process: ProcessGeneration::new([4; 16])?,
                            },
                            label: "node-2".into(),
                            server_name: "localhost".into(),
                            control_address: peer_address,
                            data_address: data_listener.local_addr()?,
                            query_address: query_listener.local_addr()?,
                        },
                    ],
                })?;
            }
            let mut message = Message {
                from: 1,
                to: 2,
                term: 1,
                ..Default::default()
            };
            message.set_msg_type(MessageType::MsgHeartbeat);
            // The current-thread executor cannot run the sender between these two
            // synchronous admissions. Both groups are queued before the first send.
            let one = fixture.transport.submit(&first, message.clone())?;
            let two = fixture.transport.submit(&second, message)?;
            let state = &fixture.transport.handle.0;
            let server = async {
                let (tcp, _) = listener.accept().await?;
                let mut stream = tokio_rustls::TlsAcceptor::from(state.tls.server.clone())
                    .accept(tcp)
                    .await?;
                let exporter = stream.get_ref().1.export_keying_material(
                    [0; 32],
                    frame::EXPORT_LABEL,
                    None,
                )?;
                let mut frames = Vec::new();
                // Withhold all replies until both complete authenticated frames
                // arrive. This tests the real sender path, not a timing estimate.
                for scope in [&first, &second] {
                    let head = crate::asyncrt::timeout(
                        std::time::Duration::from_secs(1),
                        read_head(&mut stream, &state.options, Lane::Control, false),
                    )
                    .await??;
                    let header = head.request(&state.auth, &exporter)?;
                    assert_eq!(header.route.decode()?.0.scope, *scope);
                    let mut body = vec![0; head.payload_len];
                    stream.read_exact(&mut body).await?;
                    header.verify_body(&body, &state.options)?;
                    frames.push((head, header));
                }
                for (index, (mut head, header)) in frames.into_iter().enumerate() {
                    if invalid_ack && index == 1 {
                        head.tag[0] ^= 1;
                    }
                    let reply = frame::response_head(
                        &state.auth,
                        &exporter,
                        &header,
                        &head.tag,
                        Disposition::Accepted,
                        &state.options,
                    )?;
                    write_head(&mut stream, &reply).await?;
                }
                Ok::<_, anyhow::Error>(())
            };
            let (received, sent) = tokio::join!(server, async {
                tokio::join!(one.accepted(), two.accepted())
            });
            received.map_err(|e| {
                anyhow::anyhow!("second group waited for the first admission reply: {e}")
            })?;
            if invalid_ack {
                assert_eq!(sent.0, Err(TransportError::Authentication));
                assert_eq!(sent.1, Err(TransportError::Authentication));
            } else {
                sent.0?;
                sent.1?;
            }
            assert_eq!(fixture.transport.usage().control_items, 0);
            Ok::<_, anyhow::Error>(())
        })
        .await;
    fixture.close().await?;
    result??;
    Ok(())
}

#[tokio::test]
async fn frame_window_drains_replies_with_small_duplex_buffers() -> anyhow::Result<()> {
    let scope = GroupScope {
        cluster: ClusterId::new([1; 16])?,
        namespace: NamespaceId::new([2; 16])?,
        cell: "Counter:duplex".into(),
        group: GroupId::new([3; 16])?,
        incarnation: IncarnationId::new([4; 16])?,
    };
    let replica = |member| ReplicaIdentity {
        scope: scope.clone(),
        member: MemberId::new(member).unwrap(),
        node: NodeId::new([member as u8; 16]).unwrap(),
        disk: DiskLineage::new([member as u8; 16]).unwrap(),
        process: ProcessGeneration::new([9; 16]).unwrap(),
    };
    let route = Route::new(&replica(1), "node-1", &replica(2), "node-2", [5; 32])?;
    let mut message = Message {
        from: 1,
        to: 2,
        term: 1,
        context: vec![3; 4096].into(),
        ..Default::default()
    };
    message.set_msg_type(MessageType::MsgHeartbeat);
    let body = message.write_to_bytes()?;
    let headers = (1..=16)
        .map(|sequence| RequestHeader::new(route.clone(), sequence, Lane::Control, &body))
        .collect::<Vec<_>>();
    let frames = headers
        .iter()
        .map(|header| (header, body.as_slice()))
        .collect::<Vec<_>>();
    let auth = PeerAuth::new([8; 32], "node-1")?;
    let exporter = [7; 32];
    let options = TransportOptions::default();
    let (mut client, mut server) = tokio::io::duplex(64);
    let exchange = exchange_frames(
        &mut client,
        &auth,
        &exporter,
        &options,
        Lane::Control,
        &frames,
    );
    let receiver = async {
        for expected in &headers {
            let head = read_head(&mut server, &options, Lane::Control, false).await?;
            let header = head.request(&auth, &exporter)?;
            assert_eq!(header.sequence()?, expected.sequence()?);
            let mut body = vec![0; head.payload_len];
            server.read_exact(&mut body).await?;
            header.verify_body(&body, &options)?;
            let reply = frame::response_head(
                &auth,
                &exporter,
                &header,
                &head.tag,
                Disposition::Accepted,
                &options,
            )?;
            write_head(&mut server, &reply).await?;
        }
        Ok::<_, anyhow::Error>(())
    };
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(crate::asyncrt::timeout(
            std::time::Duration::from_secs(2),
            async { tokio::join!(exchange, receiver) },
        ))
        .await;
    domain.shutdown().await;
    let (sent, received) = result??;
    received?;
    assert_eq!(sent?, vec![Disposition::Accepted; 16]);
    Ok(())
}
