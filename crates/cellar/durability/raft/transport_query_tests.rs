// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Actual synced shared engines and transport grants. These authentication
//! observations grant no serving, quorum or logical-cell admission authority.
use super::*;
use crate::raft::storage::CellGroupBootstrap;
use cellar_logic::cell_group::{CellHistory, CellId, CellRange, GroupHistory, RouteEpoch};

use super::super::shared_fixture::Fixture;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn query_grants_are_scoped_revocable_and_never_voting_authority() -> anyhow::Result<()> {
    use crate::raft::query_identity::QueryRoute;
    let first = GroupHistory {
        cluster: ClusterId::new([1; 16])?,
        namespace: NamespaceId::new([2; 16])?,
        group: GroupId::new([3; 16])?,
        incarnation: IncarnationId::new([4; 16])?,
    };
    let mut second = first;
    second.namespace = NamespaceId::new([5; 16])?;
    second.group = GroupId::new([6; 16])?;
    let groups = [&first, &second]
        .into_iter()
        .map(|scope| {
            Ok(CellGroupBootstrap {
                route: cellar_logic::cell_group::GroupRoute {
                    history: *scope,
                    range: CellRange::all(),
                    epoch: RouteEpoch::new(1)?,
                    birth_digest: [10; 32],
                    schema_digest: [9; 32],
                },
                member: MemberId::new(1)?,
                voters: (1..=3).map(MemberId::new).collect::<Result<Vec<_>, _>>()?,
                learners: vec![],
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let mut fixture = Fixture::start(
        TransportOptions {
            max_query_origins: 2,
            ..Default::default()
        },
        groups,
    )
    .await?;
    fixture.preserve();
    for scope in [&first, &second] {
        let peers = (1..=3)
            .map(|member| {
                Ok(RaftPeer {
                    replica: ReplicaIdentity {
                        scope: *scope,
                        node: NodeId::new([member as u8; 16])?,
                        member: MemberId::new(member)?,
                        disk: DiskLineage::new([member as u8 + 1; 16])?,
                        process: ProcessGeneration::new([member as u8 + 2; 16])?,
                    },
                    label: format!("node-{member}"),
                    server_name: "localhost".into(),
                    control_address: if member == 1 {
                        fixture.transport.control_address()
                    } else {
                        format!("127.0.0.1:{}", 40000 + member * 3).parse()?
                    },
                    data_address: if member == 1 {
                        fixture.transport.data_address()
                    } else {
                        format!("127.0.0.1:{}", 40001 + member * 3).parse()?
                    },
                    query_address: if member == 1 {
                        fixture.transport.query_address()
                    } else {
                        format!("127.0.0.1:{}", 40002 + member * 3).parse()?
                    },
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        fixture.transport.install_physical_group(GroupRoute {
            scope: *scope,
            schema_digest: [9; 32],
            revision: 1,
            peers,
        })?;
    }
    let result = fixture
        .domain
        .run(async {
            let peer = RaftPeerNode {
                node: NodeId::new([4; 16])?,
                disk: DiskLineage::new([9; 16])?,
                process: ProcessGeneration::new([10; 16])?,
                label: "node-4".into(),
                server_name: "localhost".into(),
                control_address: "127.0.0.1:41001".parse()?,
                data_address: "127.0.0.1:41002".parse()?,
                query_address: "127.0.0.1:41003".parse()?,
            };
            let origin = NodeIdentity {
                node: peer.node,
                disk: peer.disk,
                process: peer.process,
            };
            let route = |scope: &GroupHistory, source| -> anyhow::Result<_> {
                Ok(QueryRoute::new_shared(
                    source,
                    "node-4",
                    fixture.log.cell_group(scope).unwrap().identity(),
                    "node-1",
                    &CellHistory {
                        cluster: scope.cluster,
                        namespace: scope.namespace,
                        cell: CellId::new("Counter:one")?,
                        incarnation: IncarnationId::new([20; 16])?,
                    },
                    [9; 32],
                )?)
            };
            let first_route = route(&first, origin)?;
            let second_route = route(&second, origin)?;
            let state = &fixture.transport.handle.0;
            assert!(matches!(
                state.authorize_query_route(&first_route),
                Err(TransportError::Refused)
            ));
            let tasks = state.registry.lock().unwrap().tasks;
            let epoch = state.seed_peer_epoch(&peer)?;
            let incoming_query = {
                let registry = state.registry.lock().unwrap();
                assert!(!registry.pools.contains_key(&peer.node));
                assert_eq!(
                    registry.tasks, tasks,
                    "metadata discovery creates no query workers"
                );
                registry.seed_peers[&peer.node].incoming_query.clone()
            };
            assert!(!*epoch.borrow());
            assert_eq!(
                incoming_query.capacity(),
                (
                    state.options.peer_query_bytes,
                    state.options.max_connections_per_lane
                )
            );
            refused_read(&fixture, &first_route, &incoming_query, false).await?;
            refused_read(&fixture, &first_route, &incoming_query, true).await?;
            let mut grant = NamespaceQueryOrigin {
                cluster: first.cluster,
                namespace: first.namespace,
                revision: 1,
                peer: peer.clone(),
            };
            fixture.transport.grant_query_origin(grant.clone())?;
            let (pool, permission) = state.authorize_query_route(&first_route)?;
            assert!(Arc::ptr_eq(&incoming_query, &pool.incoming.query));
            let permission = permission.expect("nonmember requires an explicit grant");
            assert!(matches!(
                state.authorize_query_route(&second_route),
                Err(TransportError::Refused)
            ));
            let mut wrong_process = origin;
            wrong_process.process = ProcessGeneration::new([11; 16])?;
            assert!(matches!(
                state.authorize_query_route(&route(&first, wrong_process)?),
                Err(TransportError::Refused)
            ));
            let mut other = grant.clone();
            other.namespace = second.namespace;
            fixture.transport.grant_query_origin(other.clone())?;
            let (_, other_permission) = state.authorize_query_route(&second_route)?;
            let fake_member = ReplicaIdentity {
                scope: first,
                node: origin.node,
                disk: origin.disk,
                process: origin.process,
                member: MemberId::new(99)?,
            };
            assert!(state
                .authorize_physical(&Route::new_group(
                    &fake_member,
                    "node-4",
                    fixture.log.cell_group(&first)?.identity(),
                    "node-1",
                    [9; 32]
                )?)
                .is_err());
            let (_cancel, cancel) = watch::channel(false);
            let deadline = state.domain.mono_ms()?.saturating_add(1000);
            let waiting = state.query_until(
                &pool,
                deadline,
                cancel.clone(),
                Some(permission.clone()),
                std::future::pending::<Result<(), TransportError>>(),
            );
            tokio::pin!(waiting);
            assert!(futures_util::poll!(&mut waiting).is_pending());
            fixture
                .transport
                .revoke_query_origin(first.cluster, first.namespace, peer.node, 2)?;
            assert!(matches!(
                crate::asyncrt::timeout(std::time::Duration::from_secs(1), waiting).await?,
                Err(TransportError::Refused)
            ));
            assert!(*permission.borrow());
            assert!(
                !*pool.stop.borrow(),
                "another namespace still owns this physical pool"
            );
            assert!(!*other_permission.unwrap().borrow());
            refused_read(&fixture, &first_route, &pool.incoming.query, false).await?;
            refused_read(&fixture, &first_route, &pool.incoming.query, true).await?;
            assert!(matches!(
                fixture.transport.grant_query_origin(grant.clone()),
                Err(TransportError::StalePeer)
            ));
            let mut third = grant.clone();
            third.namespace = NamespaceId::new([7; 16])?;
            assert!(matches!(
                fixture.transport.grant_query_origin(third),
                Err(TransportError::Overloaded)
            ));
            grant.revision = 3;
            fixture.transport.grant_query_origin(grant.clone())?;
            assert!(!*state
                .authorize_query_route(&first_route)?
                .1
                .unwrap()
                .borrow());
            assert!(
                matches!(
                    state
                        .query_until(
                            &pool,
                            deadline,
                            cancel,
                            Some(permission),
                            std::future::ready(Ok::<(), TransportError>(()))
                        )
                        .await,
                    Err(TransportError::Refused)
                ),
                "regrant cannot revive an old in-flight authorization"
            );
            let mut replacement = grant.clone();
            replacement.revision = 4;
            replacement.peer.process = wrong_process.process;
            assert!(
                matches!(
                    fixture.transport.grant_query_origin(replacement.clone()),
                    Err(TransportError::StalePeer)
                ),
                "other namespace still binds the old physical process"
            );
            fixture.transport.revoke_query_origin(
                second.cluster,
                second.namespace,
                peer.node,
                2,
            )?;
            fixture.transport.grant_query_origin(replacement)?;
            assert!(*pool.stop.borrow());
            assert!(state.authorize_query_route(&first_route).is_err());
            assert!(state
                .authorize_query_route(&route(&first, wrong_process)?)
                .is_ok());
            assert_eq!(fixture.log.cell_groups()?.len(), 2);
            Ok::<_, anyhow::Error>(())
        })
        .await;
    let joined = fixture.close().await;
    result??;
    joined
}

async fn refused_read(
    fixture: &Fixture,
    route: &crate::raft::query_identity::QueryRoute,
    peer_credit: &Budget,
    truncated: bool,
) -> anyhow::Result<()> {
    use crate::raft::query_wire as wire;
    let state = &fixture.transport.handle.0;
    let observer = fixture.transport.resource_observer();
    let phase = std::sync::atomic::AtomicUsize::new(0);
    crate::asyncrt::timeout(std::time::Duration::from_secs(3), async {
        let tcp = TcpStream::connect(fixture.transport.query_address()).await?;
        let mut stream = tokio_rustls::TlsConnector::from(state.tls.client.clone())
            .connect(rustls::pki_types::ServerName::try_from("localhost")?, tcp)
            .await?;
        phase.store(1, Ordering::Relaxed);
        let exporter =
            stream
                .get_ref()
                .1
                .export_keying_material([0; 32], wire::EXPORT_LABEL, None)?;
        let auth = PeerAuth::new([8; 32], route.source_label())?;
        // Authorization must refuse before parsing a potentially large payload
        // or entering a query gateway, while its actual I/O remains charged.
        let body = vec![23; 1024 * 1024];
        let request = wire::RequestHead::new(route.clone(), 1, &body);
        let head = wire::request_head(&auth, &exporter, &request, &body, &state.options)?;
        write_head(&mut stream, &head).await?;
        phase.store(2, Ordering::Relaxed);
        if truncated {
            stream.write_all(&body[..body.len() / 2]).await?;
            stream.flush().await?;
            phase.store(3, Ordering::Relaxed);
            let charge = super::super::queries::reservation(body.len(), &state.options)?;
            loop {
                let resources = observer.capture().expect("live observer");
                if resources.incoming.query.items > 0 && peer_credit.usage().1 > 0 {
                    assert_eq!(resources.incoming.query.items, 1);
                    assert_eq!(resources.incoming.query.bytes, charge);
                    assert_eq!(peer_credit.usage(), (charge, 1));
                    phase.store(4, Ordering::Relaxed);
                    break;
                }
                crate::asyncrt::sleep(std::time::Duration::from_millis(1)).await;
            }
            // Keep the read half until the peer observes this incomplete body
            // and terminates its I/O. Local shutdown/flush does not establish
            // delivery of queued body/close bytes. Awaiting peer termination
            // makes cancellation observable before checking reclamation.
            // The original three-second bound covers this join and all credit
            // assertions below; no live I/O may be refunded early.
            stream.shutdown().await?;
            phase.store(9, Ordering::Relaxed);
            let mut extra = [0];
            match stream.read(&mut extra).await {
                Ok(0) => (),
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
                    ) => {}
                value => anyhow::bail!("truncated read peer did not close: {value:?}"),
            }
            phase.store(10, Ordering::Relaxed);
        } else {
            stream.write_all(&body).await?;
            stream.flush().await?;
            phase.store(5, Ordering::Relaxed);
            let mut prefix = [0; frame::PREFIX];
            stream.read_exact(&mut prefix).await?;
            let (header_len, payload_len) = wire::parse_prefix(&prefix, &state.options, true)?;
            let mut reply = FrameHead {
                prefix,
                header: vec![0; header_len],
                tag: [0; frame::TAG],
                payload_len,
            };
            stream.read_exact(&mut reply.header).await?;
            stream.read_exact(&mut reply.tag).await?;
            phase.store(6, Ordering::Relaxed);
            let verified = wire::read_response(
                &reply,
                &auth,
                &exporter,
                &request,
                &head.tag,
                &state.options,
            )?;
            let mut body = vec![0; payload_len];
            stream.read_exact(&mut body).await?;
            assert_eq!(verified.verify(&body), Err(TransportError::Refused));
            phase.store(7, Ordering::Relaxed);
        }
        drop(stream);
        phase.store(8, Ordering::Relaxed);
        loop {
            let resources = observer.capture().expect("live observer");
            if resources.incoming.query.items == 0 && resources.incoming_connections.query == 0 {
                assert_eq!(resources.incoming.query.bytes, 0);
                assert_eq!(peer_credit.usage(), (0, 0));
                break;
            }
            crate::asyncrt::sleep(std::time::Duration::from_millis(1)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .map_err(|error| {
        let node = observer.capture().map(|resources| {
            (
                resources.incoming.query.bytes,
                resources.incoming.query.items,
                resources.incoming_connections.query,
            )
        });
        anyhow::anyhow!(
            "read refusal truncated={truncated}, phase={}, node={node:?}, peer={:?}: {error}",
            phase.load(Ordering::Relaxed),
            peer_credit.usage()
        )
    })??;
    Ok(())
}
