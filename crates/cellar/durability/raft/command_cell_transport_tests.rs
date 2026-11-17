// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Local non-submission proof ends at the first request exchange.
use super::*;
use crate::application::consensus::{OperationContract, OperationKind};
use crate::raft::cell_groups::CellCommandRequest;
use crate::raft::storage::CellGroupBootstrap;
use crate::raft::version::VersionSigningKey;
use cellar_logic::cell_group::{CellId, CellRange, GroupHistory, RouteEpoch};
use cellar_logic::consensus::{
    ClientId, CommandId, DiskLineage, Durability, GroupId, IncarnationId, ProcessGeneration,
};

#[derive(Clone, Copy)]
enum Cut {
    BeforeQueue,
    BeforeExchange,
    AfterReply,
}

#[tokio::test]
async fn stale_command_before_queue_proves_no_transmission() -> anyhow::Result<()> {
    stale_command(Cut::BeforeQueue).await
}

#[tokio::test]
async fn stale_queued_command_proves_no_transmission() -> anyhow::Result<()> {
    stale_command(Cut::BeforeExchange).await
}

#[tokio::test]
async fn stale_command_after_authenticated_reply_stays_unknown() -> anyhow::Result<()> {
    stale_command(Cut::AfterReply).await
}

async fn stale_command(cut: Cut) -> anyhow::Result<()> {
    let history = GroupHistory {
        cluster: ClusterId::new([1; 16])?,
        namespace: NamespaceId::new([2; 16])?,
        group: GroupId::new([3; 16])?,
        incarnation: IncarnationId::new([4; 16])?,
    };
    let route = cellar_logic::cell_group::GroupRoute {
        history,
        range: CellRange::all(),
        epoch: RouteEpoch::new(1)?,
        birth_digest: [10; 32],
        schema_digest: [9; 32],
    };
    let mut fixture = super::super::shared_fixture::Fixture::start(
        TransportOptions {
            max_peers: 2,
            ..Default::default()
        },
        vec![CellGroupBootstrap {
            route: route.clone(),
            member: MemberId::new(1)?,
            voters: (1..=3).map(MemberId::new).collect::<Result<Vec<_>, _>>()?,
            learners: vec![],
        }],
    )
    .await?;
    fixture.preserve();
    let result = fixture
        .domain
        .run(async {
            let control = TcpListener::bind("127.0.0.1:0").await?;
            let data = TcpListener::bind("127.0.0.1:0").await?;
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let peer = RaftPeer {
                replica: ReplicaIdentity {
                    scope: history,
                    node: NodeId::new([2; 16])?,
                    member: MemberId::new(2)?,
                    disk: DiskLineage::new([3; 16])?,
                    process: ProcessGeneration::new([4; 16])?,
                },
                label: "node-2".into(),
                server_name: "localhost".into(),
                control_address: control.local_addr()?,
                data_address: data.local_addr()?,
                query_address: listener.local_addr()?,
            };
            let view = GroupRoute {
                scope: history,
                schema_digest: route.schema_digest,
                revision: 1,
                peers: vec![peer.clone()],
            };
            if matches!(cut, Cut::BeforeQueue) {
                // A retained but unattached physical group cannot authorize I/O.
                let mut physical = view.clone();
                physical.peers.push(RaftPeer {
                    replica: fixture.log.cell_group(&history)?.identity().clone(),
                    label: "node-1".into(),
                    server_name: "localhost".into(),
                    control_address: fixture.transport.control_address(),
                    data_address: fixture.transport.data_address(),
                    query_address: fixture.transport.query_address(),
                });
                fixture.transport.install_physical_group(physical)?;
            } else {
                fixture.transport.install_cell_query_group(view.clone())?;
            }
            let state = &fixture.transport.handle.0;
            let keys = VersionKeys::new(
                VersionSigningKey::new([13; 32], 10_000)?,
                vec![],
                Duration::from_millis(1000),
            )?;
            let cell = CellHistory {
                cluster: history.cluster,
                namespace: history.namespace,
                cell: CellId::new("Counter:send-cut")?,
                incarnation: IncarnationId::new([20; 16])?,
            };
            let deadline = state.domain.mono_ms()?.saturating_add(5_000);
            let call = CellCommandCall {
                request: CellCommandRequest {
                    id: CommandId::new(ClientId::new([21; 16])?, 1)?,
                    contract: OperationContract {
                        operation: "Add".into(),
                        digest: [22; 32],
                        kind: OperationKind::Command,
                    },
                    input: 1_i64.to_be_bytes().to_vec(),
                    durability: Durability::Quorum,
                    retry_until_ms: 9000,
                },
                inputs: crate::storage::CommandInputs {
                    now_unix_millis: 1000,
                    random: [23; 32],
                },
                timeout_ms: 5_000,
                route_epoch: route.epoch.get(),
                birth_digest: route.birth_digest,
            };
            let observed = if matches!(cut, Cut::BeforeExchange) {
                let (captured, pool, routing_lifetime) = {
                    let registry = state.registry.lock().unwrap();
                    let captured = registry.cell_query_groups.get(&history).unwrap().clone();
                    let pool = registry.pools.get(&peer.replica.node).unwrap().clone();
                    let lifetime = registry.cell_query_lifetime(&captured).unwrap();
                    (captured, pool, lifetime)
                };
                let (reply, received) = oneshot::channel();
                let (_cancel, cancelled) = watch::channel(false);
                let call = WriteCall::Command(call);
                let charge = queries::reservation(call.input_bytes() + 8192, &state.options)?;
                let request = SendCellWrite {
                    route: captured,
                    peer: peer.clone(),
                    cell: cell.clone(),
                    call,
                    started_at: state.domain.mono_ms()?,
                    leader_generation: None,
                    routing_lifetime,
                    keys: Some(keys),
                    deadline,
                    cancel: cancelled,
                    reply,
                    node: state.outgoing.query.acquire(charge).unwrap(),
                    peer_bytes: pool.outgoing.query.acquire(charge).unwrap(),
                };
                fixture.transport.install_cell_query_group(GroupRoute {
                    revision: 2,
                    ..view.clone()
                })?;
                let mut connection = None;
                assert!(state.send_cell_write(&pool, request, &mut connection).await);
                assert!(connection.is_none());
                received
                    .await?
                    .err()
                    .ok_or_else(|| anyhow::anyhow!("stale queued command accepted"))?
            } else {
                let command = fixture.transport.command_cell(
                    &route,
                    &cell,
                    peer.replica.member,
                    call,
                    keys,
                    deadline,
                );
                if matches!(cut, Cut::AfterReply) {
                    let server = async {
                        let (tcp, _) = listener.accept().await?;
                        let mut stream = tokio_rustls::TlsAcceptor::from(state.tls.server.clone())
                            .accept(tcp)
                            .await?;
                        let exporter = stream.get_ref().1.export_keying_material(
                            [0; 32],
                            wire::EXPORT_LABEL,
                            None,
                        )?;
                        let mut prefix = [0; frame::PREFIX];
                        stream.read_exact(&mut prefix).await?;
                        let (header_len, payload_len) =
                            wire::parse_prefix(&prefix, &state.options, false)?;
                        let mut head = FrameHead {
                            prefix,
                            header: vec![0; header_len],
                            tag: [0; frame::TAG],
                            payload_len,
                        };
                        stream.read_exact(&mut head.header).await?;
                        stream.read_exact(&mut head.tag).await?;
                        let request =
                            wire::read_request(&head, &state.auth, &exporter, &state.options)?;
                        let mut body = vec![0; head.payload_len];
                        stream.read_exact(&mut body).await?;
                        request.verify_body(&body)?;
                        let received = wire::command::CellCommandCall::decode(
                            &body,
                            state.options.max_query_request_bytes,
                        )?;
                        assert_eq!(received.request.id.sequence(), 1);
                        // Change only the captured membership generation after the
                        // complete command has crossed the authenticated channel.
                        fixture.transport.install_cell_query_group(GroupRoute {
                            revision: 2,
                            ..view.clone()
                        })?;
                        let body = wire::command::encode_result(
                            &Err(CommandFailure::Unavailable),
                            state.options.max_query_response_bytes,
                        )?;
                        let reply = wire::response_head(
                            &state.auth,
                            &exporter,
                            &request,
                            &head.tag,
                            &body,
                            &state.options,
                        )?;
                        write_head(&mut stream, &reply).await?;
                        stream.write_all(&body).await?;
                        stream.flush().await?;
                        Ok::<_, anyhow::Error>(())
                    };
                    let client = async {
                        command
                            .await
                            .err()
                            .ok_or_else(|| anyhow::anyhow!("stale reply accepted"))
                    };
                    let (_, error) = crate::asyncrt::timeout(Duration::from_secs(3), async {
                        tokio::try_join!(server, client)
                    })
                    .await??;
                    error
                } else {
                    command
                        .await
                        .err()
                        .ok_or_else(|| anyhow::anyhow!("unclaimed command accepted"))?
                }
            };
            if matches!(cut, Cut::AfterReply) {
                assert!(
                    matches!(observed, TransportError::StalePeer),
                    "{observed:?}"
                );
            } else {
                assert!(
                    matches!(
                        observed,
                        TransportError::CommandNotSent(CommandFailure::Unavailable)
                    ),
                    "{observed:?}"
                );
                assert!(matches!(
                    futures_util::poll!(Box::pin(listener.accept()).as_mut()),
                    Poll::Pending
                ));
            }
            let observer = fixture.transport.resource_observer();
            crate::asyncrt::timeout(Duration::from_secs(3), async {
                loop {
                    let resources = observer.capture().unwrap();
                    if resources.outgoing.query.items == 0
                        && resources.outgoing.query.bytes == 0
                        && resources.outgoing_connections.query == 0
                    {
                        break;
                    }
                    crate::asyncrt::sleep(Duration::from_millis(1)).await;
                }
            })
            .await?;
            Ok::<_, anyhow::Error>(())
        })
        .await;
    fixture.close().await?;
    result??;
    Ok(())
}
