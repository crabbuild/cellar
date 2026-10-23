//! Real TLS peers hold admission replies to exercise the shared control lane.
use super::*;
use crate::raft::storage::GroupBootstrap;
use std::time::Duration;

const WINDOW: usize = 8;

async fn setup() -> anyhow::Result<(Fixture, TcpListener, Vec<GroupScope>)> {
    let scopes = (1..=2)
        .map(|id| {
            Ok(GroupScope {
                cluster: ClusterId::new([1; 16])?,
                namespace: NamespaceId::new([2; 16])?,
                cell: format!("Counter:pipeline-{id}"),
                group: GroupId::new([id; 16])?,
                incarnation: IncarnationId::new([4; 16])?,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let groups = scopes
        .iter()
        .map(|scope| {
            Ok(GroupBootstrap {
                scope: scope.clone(),
                member: MemberId::new(1)?,
                birth_digest: [10; 32],
                voters: (1..=3).map(MemberId::new).collect::<Result<Vec<_>, _>>()?,
                learners: vec![],
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let fixture = Fixture::start(TransportOptions::default(), groups).await?;
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    // Reserve distinct endpoints throughout setup instead of selecting ports.
    let data = TcpListener::bind("127.0.0.1:0").await?;
    let query = TcpListener::bind("127.0.0.1:0").await?;
    for scope in &scopes {
        fixture.transport.install_group(GroupRoute {
            scope: scope.clone(),
            schema_digest: [9; 32],
            revision: 1,
            peers: (1..=2)
                .map(|member| {
                    Ok(RaftPeer {
                        replica: ReplicaIdentity {
                            scope: scope.clone(),
                            member: MemberId::new(member)?,
                            node: NodeId::new([member as u8; 16])?,
                            disk: DiskLineage::new([member as u8 + 1; 16])?,
                            process: ProcessGeneration::new([member as u8 + 2; 16])?,
                        },
                        label: format!("node-{member}"),
                        server_name: "localhost".into(),
                        control_address: if member == 1 {
                            fixture.transport.control_address()
                        } else {
                            listener.local_addr()?
                        },
                        data_address: if member == 1 {
                            fixture.transport.data_address()
                        } else {
                            data.local_addr()?
                        },
                        query_address: if member == 1 {
                            fixture.transport.query_address()
                        } else {
                            query.local_addr()?
                        },
                    })
                })
                .collect::<anyhow::Result<_>>()?,
        })?;
    }
    Ok((fixture, listener, scopes))
}

fn submit(fixture: &Fixture, scope: &GroupScope, id: u8) -> anyhow::Result<Delivery> {
    let mut message = Message {
        from: 1,
        to: 2,
        term: 1,
        context: vec![id].into(),
        ..Default::default()
    };
    message.set_msg_type(MessageType::MsgHeartbeat);
    Ok(fixture.transport.submit(scope, message)?)
}

type Stream = tokio_rustls::server::TlsStream<TcpStream>;

fn timing(
    fixture: &Fixture,
    name: &str,
) -> crate::application::consensus::RaftOperationPerformance {
    fixture
        .transport
        .handle
        .0
        .performance
        .snapshot()
        .operations
        .into_iter()
        .find(|sample| sample.name == name)
        .expect("missing transport phase timing")
}

async fn accept(fixture: &Fixture, listener: &TcpListener) -> anyhow::Result<(Stream, [u8; 32])> {
    let (tcp, _) = listener.accept().await?;
    tcp.set_nodelay(true)?;
    let stream = tokio_rustls::TlsAcceptor::from(fixture.transport.handle.0.tls.server.clone())
        .accept(tcp)
        .await?;
    let exporter = stream
        .get_ref()
        .1
        .export_keying_material([0; 32], frame::EXPORT_LABEL, None)?;
    Ok((stream, exporter))
}

async fn read<S: AsyncRead + Unpin>(
    fixture: &Fixture,
    stream: &mut S,
    exporter: &[u8; 32],
    id: u8,
) -> anyhow::Result<(RequestHeader, FrameHead)> {
    let state = &fixture.transport.handle.0;
    let head = read_head(stream, &state.options, Lane::Control, false).await?;
    let header = head.request(&state.auth, exporter)?;
    let mut body = vec![0; head.payload_len];
    stream.read_exact(&mut body).await?;
    let message = header.verify_body(&body, &state.options)?;
    anyhow::ensure!(message.context.as_ref() == [id], "frame order changed");
    Ok((header, head))
}

async fn acknowledge<S: AsyncWrite + Unpin>(
    fixture: &Fixture,
    stream: &mut S,
    exporter: &[u8; 32],
    request: &(RequestHeader, FrameHead),
) -> anyhow::Result<()> {
    let state = &fixture.transport.handle.0;
    let response = frame::response_head(
        &state.auth,
        exporter,
        &request.0,
        &request.1.tag,
        Disposition::Accepted,
        &state.options,
    )?;
    write_head(stream, &response).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_window_sends_independent_groups_before_first_ack() -> anyhow::Result<()> {
    let (fixture, listener, scopes) = setup().await?;
    let (ready, wait_ready) = oneshot::channel();
    let (queued, wait_queued) = oneshot::channel();
    let result = fixture
        .domain
        .run(crate::asyncrt::timeout(Duration::from_secs(3), async {
            let initial_commit = [
                fixture.runtime.status(&scopes[0]).await?.committed,
                fixture.runtime.status(&scopes[1]).await?.committed,
            ];
            let server = async {
                let (mut stream, exporter) = accept(&fixture, &listener).await?;
                let primer = read(&fixture, &mut stream, &exporter, 0).await?;
                ready.send(()).unwrap();
                wait_queued.await?;
                let queue = timing(&fixture, "transport_control_queue");
                assert_eq!(
                    (
                        queue.started,
                        queue.succeeded,
                        queue.failed,
                        queue.abandoned
                    ),
                    (10, 1, 0, 0)
                );
                let exchange = timing(&fixture, "transport_control_exchange");
                assert_eq!((exchange.started, exchange.count), (1, 0));
                acknowledge(&fixture, &mut stream, &exporter, &primer).await?;
                let mut window = Vec::new();
                for id in 1..=WINDOW {
                    window.push(
                        crate::asyncrt::timeout(
                            Duration::from_millis(300),
                            read(&fixture, &mut stream, &exporter, id as u8),
                        )
                        .await??,
                    );
                }
                assert!(
                    crate::asyncrt::timeout(
                        Duration::from_millis(20),
                        read(&fixture, &mut stream, &exporter, (WINDOW + 1) as u8)
                    )
                    .await
                    .is_err(),
                    "control window exceeded its bound"
                );
                for request in &window {
                    acknowledge(&fixture, &mut stream, &exporter, request).await?;
                }
                let last = read(&fixture, &mut stream, &exporter, (WINDOW + 1) as u8).await?;
                acknowledge(&fixture, &mut stream, &exporter, &last).await?;
                Ok::<_, anyhow::Error>(())
            };
            let client = async {
                let primer = submit(&fixture, &scopes[0], 0)?;
                wait_ready.await?;
                let deliveries = (1..=WINDOW + 1)
                    .map(|id| submit(&fixture, &scopes[id % 2], id as u8))
                    .collect::<anyhow::Result<Vec<_>>>()?;
                queued.send(()).unwrap();
                primer.accepted().await?;
                for delivery in deliveries {
                    delivery.accepted().await?;
                }
                anyhow::ensure!(fixture.transport.usage().control_items == 0);
                let queue = timing(&fixture, "transport_control_queue");
                assert_eq!(
                    (
                        queue.started,
                        queue.succeeded,
                        queue.failed,
                        queue.abandoned
                    ),
                    (10, 10, 0, 0)
                );
                let exchange = timing(&fixture, "transport_control_exchange");
                assert_eq!(
                    (
                        exchange.started,
                        exchange.succeeded,
                        exchange.failed,
                        exchange.abandoned
                    ),
                    (3, 3, 0, 0)
                );
                assert_eq!(timing(&fixture, "transport_control_admit").started, 0);
                assert_eq!(timing(&fixture, "transport_data_queue").started, 0);
                for (scope, expected) in scopes.iter().zip(&initial_commit) {
                    anyhow::ensure!(
                        fixture.runtime.status(scope).await?.committed == *expected,
                        "frame admission advanced commit"
                    );
                }
                Ok::<_, anyhow::Error>(())
            };
            let (server, client) = tokio::join!(server, client);
            server?;
            client
        }))
        .await;
    fixture.close().await?;
    result???;
    Ok(())
}

#[derive(Clone, Copy)]
enum Failure {
    LostReply,
    ReorderedReply,
    Revoked,
    Timeout,
    Shutdown,
}

async fn failed_window(failure: Failure) -> anyhow::Result<()> {
    let (fixture, listener, scopes) = setup().await?;
    let (ready, wait_ready) = oneshot::channel();
    let (queued, wait_queued) = oneshot::channel();
    let (observed, wait_observed) = oneshot::channel();
    let (resume, wait_resume) = oneshot::channel();
    let (completed, wait_completed) = oneshot::channel();
    let result = fixture
        .domain
        .run(crate::asyncrt::timeout(Duration::from_secs(8), async {
            let initial_commit = [
                fixture.runtime.status(&scopes[0]).await?.committed,
                fixture.runtime.status(&scopes[1]).await?.committed,
            ];
            let server = async {
                let (mut stream, exporter) = accept(&fixture, &listener).await?;
                let primer = read(&fixture, &mut stream, &exporter, 0).await?;
                ready.send(()).unwrap();
                wait_queued.await?;
                acknowledge(&fixture, &mut stream, &exporter, &primer).await?;
                let mut window = Vec::new();
                for id in 1..=WINDOW {
                    window.push(read(&fixture, &mut stream, &exporter, id as u8).await?);
                }
                observed.send(()).unwrap();
                wait_resume.await?;
                match failure {
                    Failure::LostReply | Failure::Revoked => {
                        acknowledge(&fixture, &mut stream, &exporter, &window[0]).await?;
                    }
                    Failure::ReorderedReply => {
                        acknowledge(&fixture, &mut stream, &exporter, &window[1]).await?;
                    }
                    Failure::Timeout => {
                        wait_completed.await?;
                    }
                    Failure::Shutdown => {
                        wait_completed.await?;
                        anyhow::ensure!(
                            stream.read_u8().await.is_err(),
                            "shutdown left a physical connection open"
                        );
                    }
                }
                Ok::<_, anyhow::Error>(())
            };
            let client = async {
                let primer = submit(&fixture, &scopes[0], 0)?;
                wait_ready.await?;
                let deliveries = (1..=WINDOW)
                    .map(|id| submit(&fixture, &scopes[id % 2], id as u8))
                    .collect::<anyhow::Result<Vec<_>>>()?;
                queued.send(()).unwrap();
                primer.accepted().await?;
                wait_observed.await?;
                anyhow::ensure!(fixture.transport.usage().control_items == WINDOW);
                if matches!(failure, Failure::Revoked) {
                    for scope in &scopes {
                        let mut route =
                            (*fixture.transport.handle.0.registry.lock().unwrap().groups[scope])
                                .clone();
                        route.revision += 1;
                        fixture.transport.install_group(route)?;
                    }
                }
                resume.send(()).unwrap();
                if matches!(failure, Failure::Shutdown) {
                    let queued = (10..13)
                        .map(|id| submit(&fixture, &scopes[0], id))
                        .collect::<anyhow::Result<Vec<_>>>()?;
                    drop(queued);
                    drop(deliveries);
                    anyhow::ensure!(
                        fixture.transport.usage().control_items == WINDOW + 3,
                        "cancelled observers released in-flight reservations"
                    );
                    fixture.transport.close().await;
                    anyhow::ensure!(fixture.transport.usage().tasks == 0);
                    anyhow::ensure!(fixture.transport.usage().control_items == 0);
                    let queue = timing(&fixture, "transport_control_queue");
                    assert_eq!(
                        (
                            queue.started,
                            queue.succeeded,
                            queue.failed,
                            queue.abandoned
                        ),
                        (12, 9, 0, 3)
                    );
                    let exchange = timing(&fixture, "transport_control_exchange");
                    assert_eq!(
                        (
                            exchange.started,
                            exchange.succeeded,
                            exchange.failed,
                            exchange.abandoned
                        ),
                        (2, 1, 1, 0)
                    );
                    completed.send(()).unwrap();
                    return Ok::<_, anyhow::Error>(());
                }
                for (index, delivery) in deliveries.into_iter().enumerate() {
                    let result = delivery.accepted().await;
                    match failure {
                        Failure::LostReply if index == 0 => anyhow::ensure!(
                            result.is_ok(),
                            "verified earlier reply was discarded: {result:?}"
                        ),
                        Failure::LostReply => {
                            anyhow::ensure!(result.is_err(), "unobserved reply became acceptance")
                        }
                        Failure::ReorderedReply => anyhow::ensure!(
                            result == Err(TransportError::Authentication),
                            "wrong-request reply was accepted: {result:?}"
                        ),
                        Failure::Revoked => anyhow::ensure!(
                            result == Err(TransportError::StalePeer),
                            "revoked route accepted an old reply: {result:?}"
                        ),
                        Failure::Timeout => anyhow::ensure!(
                            result == Err(TransportError::Timeout),
                            "held reply did not retain the bounded deadline: {result:?}"
                        ),
                        Failure::Shutdown => unreachable!(),
                    }
                }
                anyhow::ensure!(fixture.transport.usage().control_items == 0);
                anyhow::ensure!(fixture.transport.usage().control_bytes == 0);
                let queue = timing(&fixture, "transport_control_queue");
                assert_eq!(
                    (
                        queue.started,
                        queue.succeeded,
                        queue.failed,
                        queue.abandoned
                    ),
                    (9, 9, 0, 0)
                );
                let exchange = timing(&fixture, "transport_control_exchange");
                assert_eq!(
                    (
                        exchange.started,
                        exchange.succeeded,
                        exchange.failed,
                        exchange.abandoned
                    ),
                    (2, 1, 1, 0)
                );
                for (scope, expected) in scopes.iter().zip(&initial_commit) {
                    anyhow::ensure!(fixture.runtime.status(scope).await?.committed == *expected);
                }
                let _ = completed.send(());
                Ok::<_, anyhow::Error>(())
            };
            let (server, client) = tokio::join!(server, client);
            server?;
            client
        }))
        .await;
    fixture.close().await?;
    result???;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_window_lost_reply_preserves_only_observed_admission() -> anyhow::Result<()> {
    failed_window(Failure::LostReply).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_window_reordered_reply_cannot_acknowledge_another_frame() -> anyhow::Result<()> {
    failed_window(Failure::ReorderedReply).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_window_revalidates_routes_after_transmission() -> anyhow::Result<()> {
    failed_window(Failure::Revoked).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_window_held_replies_keep_bounded_deadline() -> anyhow::Result<()> {
    failed_window(Failure::Timeout).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_window_cancellation_retains_permits_until_joined_close() -> anyhow::Result<()> {
    failed_window(Failure::Shutdown).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_window_drives_both_directions_with_one_byte_buffers() -> anyhow::Result<()> {
    let (fixture, _listener, scopes) = setup().await?;
    let state = &fixture.transport.handle.0;
    let mut requests = Vec::new();
    for id in 1..=WINDOW {
        let scope = scopes[id % 2].clone();
        let route = state.registry.lock().unwrap().groups[&scope].clone();
        let peer = route
            .peers
            .iter()
            .find(|peer| peer.replica.member.get() == 2)
            .unwrap()
            .clone();
        let pool = state.registry.lock().unwrap().pools[&peer.replica.node].clone();
        let local = fixture.log.group(&scope)?.identity().clone();
        let mut message = Message {
            from: 1,
            to: 2,
            term: 1,
            context: vec![id as u8].into(),
            ..Default::default()
        };
        message.set_msg_type(MessageType::MsgHeartbeat);
        let body = message.write_to_bytes()?;
        let header = RequestHeader::new(
            Route::new(
                &local,
                "node-1",
                &peer.replica,
                "node-2",
                route.schema_digest,
            )?,
            id as u64,
            Lane::Control,
            &body,
        );
        let charge = body.len() + state.options.max_header_bytes + frame::PREFIX + frame::TAG;
        requests.push(SendRequest {
            scope,
            route,
            peer,
            header,
            body,
            snapshot: false,
            result: None,
            queued: None,
            _node: state.outgoing.control.acquire(charge).unwrap(),
            _peer: pool.outgoing.control.acquire(charge).unwrap(),
        });
    }
    let result = fixture
        .domain
        .run(crate::asyncrt::timeout(Duration::from_secs(3), async {
            let (mut client, mut server) = tokio::io::duplex(1);
            let exporter = [7; 32];
            let mut outcomes = vec![None; WINDOW];
            let receiver = async {
                for id in 1..=WINDOW {
                    let frame = read(&fixture, &mut server, &exporter, id as u8).await?;
                    acknowledge(&fixture, &mut server, &exporter, &frame).await?;
                }
                Ok::<_, anyhow::Error>(())
            };
            let (sent, received) = tokio::join!(
                state.exchange_window(
                    &mut client,
                    &exporter,
                    Lane::Control,
                    &requests,
                    &mut outcomes
                ),
                receiver
            );
            sent?;
            received?;
            anyhow::ensure!(outcomes == vec![Some(Ok(())); WINDOW]);
            Ok::<_, anyhow::Error>(())
        }))
        .await;
    drop(requests);
    anyhow::ensure!(fixture.transport.usage().control_items == 0);
    fixture.close().await?;
    result???;
    Ok(())
}
