// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A fourth TLS process identity routes reads without any local group or voter.
use super::remote_queries::{dispatcher, gateways, invocation, keys, wait_for};
use super::*;
use cellar::peer_auth::PeerAuth;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex,
};
use tokio::net::TcpListener;

pub(super) struct Router {
    pub(super) log: ReplicaLog,
    runtime: CellGroupRuntime,
    events: CellGroupRuntimeEvents,
    pub(super) transport: RaftTransport,
    pub(super) peer: RaftPeerNode,
}

impl Router {
    pub(super) async fn start(fixture: &RuntimeCluster) -> Result<Self> {
        let node = NodeId::new([4; 16])?;
        let disk = DiskLineage::new([14; 16])?;
        let process = ProcessGeneration::new([20; 16])?;
        let log = fixture
            .cluster
            .domain
            .run(ReplicaLog::open(
                ReplicaLogOptions::new(
                    fixture.cluster._root.path().join("router-log"),
                    node,
                    disk,
                    process,
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
        let transport = RaftTransport::start_shared(
            runtime.clone(),
            Arc::new(PeerAuth::new([17; 32], "node-4".to_owned())?),
            super::super::tcp::tls()?,
            TcpListener::bind("127.0.0.1:0").await?,
            TcpListener::bind("127.0.0.1:0").await?,
            TcpListener::bind("127.0.0.1:0").await?,
            TransportOptions {
                max_groups: 2,
                ..Default::default()
            },
        )
        .await?;
        let peer = RaftPeerNode {
            node,
            disk,
            process,
            label: "node-4".into(),
            server_name: "localhost".into(),
            control_address: transport.control_address(),
            data_address: transport.data_address(),
            query_address: transport.query_address(),
        };
        Ok(Self {
            log,
            runtime,
            events,
            transport,
            peer,
        })
    }

    pub(super) fn grant(&self, fixture: &RuntimeCluster, revision: u64) -> Result<()> {
        let history = fixture.cluster.birth.route().history;
        for transport in &fixture.transports {
            transport.grant_query_origin(NamespaceQueryOrigin {
                cluster: history.cluster,
                namespace: history.namespace,
                peer: self.peer.clone(),
                revision,
            })?;
        }
        Ok(())
    }

    pub(super) async fn close(self) -> Result<()> {
        self.transport.close().await;
        self.runtime.close().await?;
        drop(self.events);
        self.log.close().await?;
        Ok(())
    }
}

pub(crate) async fn nonmember_queries() -> Result<()> {
    for rooted in [false, true] {
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let class = Arc::new(RaftClass::new("Counter", 1, [81; 32])?.query(
            "Get",
            [82; 32],
            move |ctx, _| {
                count.fetch_add(1, Ordering::SeqCst);
                assert_eq!(
                    ctx.query_row("SELECT n FROM value", [], |r| r.get::<_, i64>(0))?,
                    0
                );
                Ok(ctx.identity().cell.as_str().as_bytes().to_vec())
            },
        )?);
        let (mut fixture, route, a, b) = Box::pin(registered::setup(rooted, &class)).await?;
        let controllers = controller::start_with_limits(&mut fixture, controller::limits()).await?;
        let owners = gateways(&fixture, class.clone()).await?;
        let router = Router::start(&fixture).await?;
        let discovery = fixture.tcp_route(route.history)?;
        router
            .transport
            .install_cell_query_group(discovery.clone())?;
        router
            .transport
            .install_cell_query_group(discovery.clone())?;
        assert!(router.log.cell_groups()?.is_empty());
        assert_eq!(router.transport.usage().groups, 0);
        assert_eq!(router.transport.usage().peers, 3);
        assert!(router.runtime.status(route.history).await.is_err());
        let mut heartbeat = Message {
            from: 1,
            to: 2,
            ..Default::default()
        };
        heartbeat.set_msg_type(MessageType::MsgHeartbeat);
        assert!(matches!(
            router.transport.submit_physical(&route.history, heartbeat),
            Err(TransportError::UnknownGroup)
        ));
        assert!(matches!(
            fixture.transports[0].install_cell_query_group(discovery.clone()),
            Err(TransportError::Identity)
        ));
        assert!(matches!(
            router.transport.install_physical_group(discovery.clone()),
            Err(TransportError::UnknownGroup)
        ));
        let dispatch = dispatcher(&fixture, class.clone(), keys(190)?)?;
        let mut denied = invocation(
            &dispatch,
            &class,
            &a,
            CellQueryConsistency::Stale,
            Duration::from_secs(5),
            b"",
        )
        .await?;
        assert!(denied
            .execute_remote(&router.transport, MemberId::new(1)?)
            .await
            .is_err());
        drop(denied);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        // Discovery and a grant in another namespace cannot authorize this read.
        fixture.transports[0].grant_query_origin(NamespaceQueryOrigin {
            cluster: a.cluster,
            namespace: NamespaceId::new([199; 16])?,
            revision: 1,
            peer: router.peer.clone(),
        })?;
        let mut denied = invocation(
            &dispatch,
            &class,
            &a,
            CellQueryConsistency::Stale,
            Duration::from_secs(5),
            b"",
        )
        .await?;
        assert!(denied
            .execute_remote(&router.transport, MemberId::new(1)?)
            .await
            .is_err());
        drop(denied);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let mut wrong_process = router.peer.clone();
        wrong_process.process = ProcessGeneration::new([195; 16])?;
        // Use a different destination: its namespace grant must bind the actual
        // authenticated source process, not merely the router's stable node ID.
        fixture.transports[1].grant_query_origin(NamespaceQueryOrigin {
            cluster: a.cluster,
            namespace: a.namespace,
            revision: 1,
            peer: wrong_process,
        })?;
        let mut denied = invocation(
            &dispatch,
            &class,
            &a,
            CellQueryConsistency::Stale,
            Duration::from_secs(5),
            b"",
        )
        .await?;
        assert!(denied
            .execute_remote(&router.transport, MemberId::new(2)?)
            .await
            .is_err());
        drop(denied);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        router.grant(&fixture, 2)?;
        let mut query = invocation(
            &dispatch,
            &class,
            &a,
            CellQueryConsistency::Linearizable,
            Duration::from_secs(5),
            b"",
        )
        .await?;
        let first_attempt = query.attempt().context("first strong attempt")?.clone();
        assert!(matches!(
            query
                .execute_remote(&router.transport, MemberId::new(2)?)
                .await,
            Err(CellDispatchError::Transport(TransportError::Query(
                CommandFailure::VersionUnavailable
            )))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        query.retry().await?;
        assert_eq!(
            query.attempt().context("leader retry")?.deadline_ms(),
            first_attempt.deadline_ms()
        );
        assert_eq!(query.attempt().context("leader retry")?.ordinal(), 2);
        let reply = query
            .execute_remote(&router.transport, MemberId::new(1)?)
            .await?;
        assert_eq!(reply.result(), a.cell.as_str().as_bytes());
        assert_eq!(reply.observation().version.history, a);
        let token = reply.token().clone();
        assert_eq!(router.transport.usage().query_items, 1);
        drop(reply);
        drop(query);
        for (cell, consistency) in [
            (&a, CellQueryConsistency::AtLeast(token)),
            (&b, CellQueryConsistency::Stale),
        ] {
            let mut query = invocation(
                &dispatch,
                &class,
                cell,
                consistency,
                Duration::from_secs(5),
                b"",
            )
            .await?;
            assert_eq!(
                query
                    .execute_remote(&router.transport, MemberId::new(3)?)
                    .await?
                    .result(),
                cell.cell.as_str().as_bytes()
            );
        }
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        // All discovery routes share the same three physical peer pools and
        // bounded route admission. They never allocate local group storage.
        let mut second = discovery.clone();
        second.scope.group = GroupId::new([198; 16])?;
        for peer in &mut second.peers {
            peer.replica.scope = second.scope;
        }
        router.transport.install_cell_query_group(second.clone())?;
        assert_eq!(router.transport.usage().peers, 3);
        let mut third = second.clone();
        third.scope.group = GroupId::new([197; 16])?;
        for peer in &mut third.peers {
            peer.replica.scope = third.scope;
        }
        assert!(matches!(
            router.transport.install_cell_query_group(third),
            Err(TransportError::Overloaded)
        ));
        let mut changed = discovery.clone();
        changed.revision = 2;
        changed.peers[0].replica.process = ProcessGeneration::new([196; 16])?;
        assert!(matches!(
            router.transport.install_cell_query_group(changed),
            Err(TransportError::StalePeer)
        ));
        let mut changed = discovery.clone();
        changed.revision = 2;
        router.transport.install_cell_query_group(changed)?;
        assert!(matches!(
            router.transport.install_cell_query_group(discovery),
            Err(TransportError::StalePeer)
        ));
        router.transport.remove_cell_query_group(&second.scope)?;
        assert_eq!(router.transport.usage().peers, 3);
        router.transport.remove_cell_query_group(&route.history)?;
        assert_eq!(router.transport.usage().peers, 0);
        let mut removed = invocation(
            &dispatch,
            &class,
            &a,
            CellQueryConsistency::Stale,
            Duration::from_secs(5),
            b"",
        )
        .await?;
        assert!(matches!(
            removed
                .execute_remote(&router.transport, MemberId::new(1)?)
                .await,
            Err(CellDispatchError::Transport(TransportError::UnknownGroup))
        ));
        drop(removed);
        assert_eq!(dispatch.usage(), (0, 0));
        assert!(router.log.cell_groups()?.is_empty());
        router.close().await?;
        for controller in controllers {
            controller.close().await?;
        }
        drop(owners);
        fixture.close().await?;
    }
    Ok(())
}

pub(crate) async fn nonmember_query_revocation(replace_route: bool) -> Result<()> {
    for rooted in [false, true] {
        let (entered, started) = tokio::sync::oneshot::channel();
        let entered = Mutex::new(Some(entered));
        let (release, held) = std::sync::mpsc::sync_channel(1);
        let held = Mutex::new(held);
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let class = Arc::new(RaftClass::new("Counter", 1, [81; 32])?.query(
            "Get",
            [82; 32],
            move |ctx, _| {
                if count.fetch_add(1, Ordering::SeqCst) == 0 {
                    entered
                        .lock()
                        .unwrap()
                        .take()
                        .context("once")?
                        .send(())
                        .map_err(|_| anyhow::anyhow!("observer"))?;
                    held.lock().unwrap().recv_timeout(Duration::from_secs(15))?;
                }
                Ok(ctx.identity().cell.as_str().as_bytes().to_vec())
            },
        )?);
        let (mut fixture, route, a, _) = Box::pin(registered::setup(rooted, &class)).await?;
        let controllers = controller::start_with_limits(&mut fixture, controller::limits()).await?;
        let owners = gateways(&fixture, class.clone()).await?;
        let router = Router::start(&fixture).await?;
        let mut discovery = fixture.tcp_route(route.history)?;
        router
            .transport
            .install_cell_query_group(discovery.clone())?;
        router.grant(&fixture, 1)?;
        let dispatch = dispatcher(&fixture, class.clone(), keys(190)?)?;
        let mut query = invocation(
            &dispatch,
            &class,
            &a,
            CellQueryConsistency::Linearizable,
            Duration::from_secs(10),
            b"",
        )
        .await?;
        let source = router.transport.clone();
        let waiter = fixture.cluster.domain.spawn(async move {
            query
                .execute_remote(&source, MemberId::new(1).unwrap())
                .await
        })?;
        tokio::time::timeout(Duration::from_secs(5), started).await??;
        let pool = &fixture.appliers[&(0, route.history)].0;
        assert_eq!(pool.usage()?.retained_operations, 1);
        if replace_route {
            discovery.revision += 1;
            router.transport.install_cell_query_group(discovery)?;
            // Even a complete authenticated reply cannot escape with a captured
            // discovery view superseded while the remote SQL callback ran.
            release.send(())?;
            assert!(matches!(
                waiter.await?,
                Err(CellDispatchError::Transport(TransportError::StalePeer))
            ));
        } else {
            fixture.transports[0].revoke_query_origin(
                a.cluster,
                a.namespace,
                router.peer.node,
                2,
            )?;
            assert!(tokio::time::timeout(Duration::from_secs(5), waiter)
                .await??
                .is_err());
            wait_for(async || {
                Ok(router.transport.usage().query_items == 0
                    && fixture.transports[0].usage().query_items == 0)
            })
            .await?;
            assert_eq!(pool.usage()?.retained_operations, 1);
            assert!(router.grant(&fixture, 1).is_err());
            let mut denied = invocation(
                &dispatch,
                &class,
                &a,
                CellQueryConsistency::Stale,
                Duration::from_secs(5),
                b"",
            )
            .await?;
            assert!(denied
                .execute_remote(&router.transport, MemberId::new(1)?)
                .await
                .is_err());
            drop(denied);
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            release.send(())?;
            router.grant(&fixture, 3)?;
        }
        wait_for(async || Ok(pool.usage()?.retained_operations == 0)).await?;
        let mut fresh = invocation(
            &dispatch,
            &class,
            &a,
            CellQueryConsistency::Stale,
            Duration::from_secs(5),
            b"",
        )
        .await?;
        assert_eq!(
            fresh
                .execute_remote(&router.transport, MemberId::new(1)?)
                .await?
                .result(),
            a.cell.as_str().as_bytes()
        );
        drop(fresh);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(dispatch.usage(), (0, 0));
        assert!(router.log.cell_groups()?.is_empty());
        router.close().await?;
        for controller in controllers {
            controller.close().await?;
        }
        drop(owners);
        fixture.close().await?;
    }
    Ok(())
}
