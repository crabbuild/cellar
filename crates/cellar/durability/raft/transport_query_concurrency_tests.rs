// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real authenticated query exchanges at the physical socket ceiling. These
//! read-only probes grant no mutation, serving or quorum authority.
use super::*;
use crate::raft::{
    query_wire as wire,
    storage::CellGroupBootstrap,
    version::{VersionKeys, VersionSigningKey},
};
use cellar_logic::cell_group::{CellHistory, CellId, CellRange, GroupHistory, RouteEpoch};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[tokio::test]
async fn held_query_reuses_available_socket_at_node_ceiling() -> anyhow::Result<()> {
    bounded_queries(Case::Complete).await
}

#[tokio::test]
async fn cancelled_query_closes_socket_and_releases_original_credits() -> anyhow::Result<()> {
    bounded_queries(Case::Cancel).await
}

#[tokio::test]
async fn unclaimed_physical_route_refuses_before_query_io() -> anyhow::Result<()> {
    bounded_queries(Case::UnclaimedPhysical).await
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Case {
    Complete,
    Cancel,
    UnclaimedPhysical,
}

async fn bounded_queries(case: Case) -> anyhow::Result<()> {
    let cancel_first = case == Case::Cancel;
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
    // Failed and successful fixtures remain available for forensic comparison.
    fixture.preserve();
    let result = fixture.domain.run(async {
        let mut listeners = Vec::new();
        for _ in 0..6 {
            listeners.push(TcpListener::bind("127.0.0.1:0").await?);
        }
        let mut peers = Vec::new();
        for member in 1..=3 {
            let addresses = if member == 1 {
                [fixture.transport.control_address(), fixture.transport.data_address(), fixture.transport.query_address()]
            } else {
                let offset = usize::try_from(member - 2)? * 3;
                [listeners[offset].local_addr()?, listeners[offset + 1].local_addr()?, listeners[offset + 2].local_addr()?]
            };
            peers.push(RaftPeer {
                replica: ReplicaIdentity {
                    scope: history,
                    node: NodeId::new([u8::try_from(member)?; 16])?,
                    member: MemberId::new(member)?,
                    disk: DiskLineage::new([u8::try_from(member)? + 1; 16])?,
                    process: ProcessGeneration::new([u8::try_from(member)? + 2; 16])?,
                },
                label: format!("node-{member}"),
                server_name: "localhost".into(),
                control_address: addresses[0],
                data_address: addresses[1],
                query_address: addresses[2],
            });
        }
        let view = GroupRoute { scope: history, schema_digest: route.schema_digest, revision: 1, peers };
        if case == Case::UnclaimedPhysical {
            fixture.transport.install_physical_group(view)?;
        } else {
            // The retained local disk has no attached consensus owner. It is
            // an ingress caller, rather than an active physical replica.
            fixture.transport.install_cell_query_group(GroupRoute {
                peers: view.peers.into_iter().filter(|peer| peer.replica.node != fixture.log.limits().node).collect(),
                ..view
            })?;
        }
        let state = &fixture.transport.handle.0;
        let observer = fixture.transport.resource_observer();
        let original_tasks = observer.capture().expect("live observer").tasks;
        let keys = VersionKeys::new(VersionSigningKey::new([13; 32], 10_000)?, vec![], std::time::Duration::from_millis(1000))?;
        let cell = CellHistory { cluster: history.cluster, namespace: history.namespace, cell: CellId::new("Counter:probe")?, incarnation: IncarnationId::new([20; 16])? };
        let deadline = state.domain.mono_ms()?.saturating_add(5_000);
        let probe = || state.probe_cell_command_leader(&route, &cell, MemberId::new(2).expect("nonzero"), keys.clone(), deadline);
        if case == Case::UnclaimedPhysical {
            assert!(matches!(probe().await, Err(TransportError::StalePeer)));
            let resources = observer.capture().expect("live observer");
            assert_eq!(resources.outgoing_connections.query, 0);
            assert_eq!(resources.outgoing.query.items, 0);
            assert_eq!(resources.outgoing.query.bytes, 0);
            let mut accepted = Box::pin(listeners[2].accept());
            assert!(matches!(futures_util::poll!(accepted.as_mut()), Poll::Pending));
            return Ok::<_, anyhow::Error>(());
        }
        let (first_seen, seen) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let server_phase = AtomicUsize::new(0);
        let client_phase = AtomicUsize::new(0);
        let joined = AtomicBool::new(false);
        let server = async {
            let accept = || async {
                let (tcp, _) = listeners[2].accept().await?;
                let stream = tokio_rustls::TlsAcceptor::from(state.tls.server.clone()).accept(tcp).await?;
                let exporter = stream.get_ref().1.export_keying_material([0; 32], wire::EXPORT_LABEL, None)?;
                Ok::<_, anyhow::Error>((stream, exporter))
            };
            let (mut first, first_exporter) = accept().await?;
            server_phase.store(1, Ordering::Release);
            let first_request = request(&mut first, state, &first_exporter).await?;
            server_phase.store(2, Ordering::Release);
            first_seen.send(()).map_err(|_| anyhow::anyhow!("first waiter ended"))?;
            // The old serial sender cannot reach this second handshake while
            // the first application's response remains deliberately withheld.
            let (mut second, second_exporter) = accept().await?;
            server_phase.store(3, Ordering::Release);
            let second_request = request(&mut second, state, &second_exporter).await?;
            server_phase.store(4, Ordering::Release);
            reply(&mut second, state, &second_exporter, &second_request).await?;
            server_phase.store(5, Ordering::Release);
            // Both physical slots are occupied. The next call must reuse the
            // completed second stream rather than wait for a third socket.
            let third_request = request(&mut second, state, &second_exporter).await?;
            server_phase.store(6, Ordering::Release);
            assert!(third_request.1.sequence()? > second_request.1.sequence()?);
            reply(&mut second, state, &second_exporter, &third_request).await?;
            server_phase.store(7, Ordering::Release);
            released.await?;
            server_phase.store(8, Ordering::Release);
            if cancel_first {
                let mut byte = [0];
                // Dropping the cancelled client need not send close_notify.
                // rustls reports that physical EOF as UnexpectedEof.
                match first.read(&mut byte).await {
                    Ok(0) => (),
                    Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => (),
                    value => anyhow::bail!("cancelled stream did not close: {value:?}"),
                }
            } else {
                reply(&mut first, state, &first_exporter, &first_request).await?;
            }
            server_phase.store(9, Ordering::Release);
            Ok::<_, anyhow::Error>(())
        };
        let client = async {
            client_phase.store(1, Ordering::Release);
            let mut first = Box::pin(probe());
            crate::asyncrt::select! {
                value = first.as_mut() => anyhow::bail!("held query ended before another request: {value:?}"),
                value = seen => value?,
            }
            client_phase.store(2, Ordering::Release);
            let independent = async {
                client_phase.store(3, Ordering::Release);
                assert!(!probe().await?);
                client_phase.store(4, Ordering::Release);
                assert!(!probe().await?);
                client_phase.store(5, Ordering::Release);
                let resources = observer.capture().expect("live observer");
                assert_eq!(resources.tasks, original_tasks);
                assert_eq!(resources.outgoing_connections.query, 2);
                assert!(resources.outgoing.query.items <= resources.outgoing.query.max_items);
                assert!(resources.outgoing.query.bytes <= resources.outgoing.query.max_bytes);
                Ok::<_, anyhow::Error>(())
            };
            crate::asyncrt::select! {
                value = first.as_mut() => anyhow::bail!("held query ended before socket reuse: {value:?}"),
                value = independent => value?,
            }
            client_phase.store(6, Ordering::Release);
            if cancel_first {
                drop(first);
                release.send(()).map_err(|_| anyhow::anyhow!("server ended before cancellation"))?;
            } else {
                release.send(()).map_err(|_| anyhow::anyhow!("server ended before release"))?;
                assert!(!first.await?);
            }
            client_phase.store(7, Ordering::Release);
            Ok::<_, anyhow::Error>(())
        };
        crate::asyncrt::timeout(std::time::Duration::from_secs(3), async {
            tokio::try_join!(server, client)?;
            joined.store(true, Ordering::Release);
            loop {
                let resources = observer.capture().expect("live observer");
                assert!(resources.outgoing_connections.query <= 2);
                if resources.outgoing_connections.query == 0 && resources.outgoing.query.items == 0 && resources.outgoing.query.bytes == 0 { break; }
                crate::asyncrt::sleep(std::time::Duration::from_millis(1)).await;
            }
            Ok::<_, anyhow::Error>(())
        }).await.map_err(|error| anyhow::anyhow!(
            "query socket fixture timeout: server_phase={} client_phase={} joined={} resources={:?}: {error}",
            server_phase.load(Ordering::Acquire), client_phase.load(Ordering::Acquire),
            joined.load(Ordering::Acquire), observer.capture(),
        ))??;
        Ok::<_, anyhow::Error>(())
    }).await;
    fixture.close().await?;
    result??;
    Ok(())
}

async fn request<S: AsyncRead + Unpin>(
    stream: &mut S,
    state: &State,
    exporter: &[u8; 32],
) -> anyhow::Result<(FrameHead, wire::RequestHead)> {
    let mut prefix = [0; frame::PREFIX];
    stream.read_exact(&mut prefix).await?;
    let (header_len, payload_len) = wire::parse_prefix(&prefix, &state.options, false)?;
    let mut head = FrameHead {
        prefix,
        header: vec![0; header_len],
        tag: [0; frame::TAG],
        payload_len,
    };
    stream.read_exact(&mut head.header).await?;
    stream.read_exact(&mut head.tag).await?;
    let header = wire::read_request(&head, &state.auth, exporter, &state.options)?;
    let mut body = vec![0; payload_len];
    stream.read_exact(&mut body).await?;
    header.verify_body(&body)?;
    Ok((head, header))
}

async fn reply<S: AsyncWrite + Unpin>(
    stream: &mut S,
    state: &State,
    exporter: &[u8; 32],
    (head, header): &(FrameHead, wire::RequestHead),
) -> anyhow::Result<()> {
    let body =
        wire::command::encode_probe_result(false, None, state.options.max_query_response_bytes)?;
    let response = wire::response_head(
        &state.auth,
        exporter,
        header,
        &head.tag,
        &body,
        &state.options,
    )?;
    write_head(stream, &response).await?;
    stream.write_all(&body).await?;
    stream.flush().await?;
    Ok(())
}
