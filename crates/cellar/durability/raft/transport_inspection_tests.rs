// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Authorization and physical ownership with real sockets and engine fences.
use super::*;
use crate::raft::inspection::InspectionError;
use crate::raft::transport_options::NamespaceInspectionOrigin;

async fn within<T>(fixture: &Fixture, future: impl Future<Output = T>) -> anyhow::Result<T> {
    Ok(fixture
        .domain
        .run(crate::asyncrt::timeout(Duration::from_secs(2), future))
        .await??)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inspection_grants_stay_separate_and_revoked_across_process_refresh() -> anyhow::Result<()>
{
    let fixture = Fixture::start(TransportOptions::default(), vec![]).await?;
    let trusted = peer(2)?;
    let mut publisher = Publisher::new(trusted.clone()).await?;
    fixture
        .transport
        .accept_node_view(publisher.view().await?, &trusted)?;
    let query = NamespaceQueryOrigin {
        cluster: scope(2)?.cluster,
        namespace: scope(2)?.namespace,
        revision: 1,
        peer: trusted.clone(),
    };
    fixture.transport.grant_query_origin(query.clone())?;
    let key = (query.cluster, query.namespace, trusted.node);
    let state = &fixture.transport.handle.0;
    assert!(!state
        .registry
        .lock()
        .unwrap()
        .inspection_origins
        .contains_key(&key));
    let grant = NamespaceInspectionOrigin {
        cluster: query.cluster,
        namespace: query.namespace,
        revision: 1,
        peer: trusted.clone(),
    };
    fixture.transport.grant_inspection_origin(grant.clone())?;
    let resources = fixture.transport.resource_observer().capture().unwrap();
    assert_eq!(resources.incoming_connections.control, 0);
    assert_eq!(
        resources.incoming_connection_limits.control,
        TransportOptions::default().max_connections_per_lane + inspections::SLOTS
    );
    assert_eq!(
        resources.incoming_connection_limits.data,
        TransportOptions::default().max_connections_per_lane
    );
    let old = state.registry.lock().unwrap().inspection_origins[&key].clone();
    fixture
        .transport
        .revoke_inspection_origin(grant.cluster, grant.namespace, trusted.node, 2)?;
    assert!(*old.stop.borrow());
    assert_eq!(
        fixture.transport.grant_inspection_origin(grant.clone()),
        Err(TransportError::StalePeer)
    );
    publisher.restart().await?;
    fixture
        .transport
        .accept_node_view(publisher.view().await?, &trusted)?;
    {
        let registry = state.registry.lock().unwrap();
        assert!(registry.inspection_origins[&key].peer.is_none());
        assert_eq!(registry.inspection_origins[&key].revision, 2);
        assert_eq!(registry.origins[&key].peer.as_ref(), Some(&publisher.peer));
    }
    let mut next = grant;
    next.peer = publisher.peer.clone();
    next.revision = 3;
    fixture.transport.grant_inspection_origin(next)?;
    fixture
        .transport
        .revoke_query_origin(query.cluster, query.namespace, trusted.node, 2)?;
    assert_eq!(
        state.registry.lock().unwrap().inspection_origins[&key]
            .peer
            .as_ref(),
        Some(&publisher.peer)
    );
    publisher.close().await?;
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inspection_owns_bounded_sockets_through_cancel_refresh_and_shutdown() -> anyhow::Result<()>
{
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let fixture = Fixture::start(TransportOptions::default(), vec![]).await?;
    let mut trusted = peer(2)?;
    trusted.control_address = listener.local_addr()?;
    let mut publisher = Publisher::new(trusted.clone()).await?;
    fixture
        .transport
        .accept_node_view(publisher.view().await?, &trusted)?;
    let state = &fixture.transport.handle.0;
    let begin = |deadline| {
        fixture.transport.inspect_peer(
            scope(2).unwrap(),
            [5; 32],
            [6; 32],
            trusted.clone(),
            deadline,
        )
    };
    let deadline = state.domain.mono_ms()?.saturating_add(5000);
    let first = begin(deadline)?;
    let second = begin(deadline)?;
    assert!(matches!(begin(deadline), Err(InspectionError::Overloaded)));
    let held_first = within(&fixture, listener.accept()).await??;
    let held_second = within(&fixture, listener.accept()).await??;
    assert_eq!(fixture.transport.usage().inspection_items, 2);
    assert_eq!(
        fixture
            .transport
            .resource_observer()
            .capture()
            .unwrap()
            .outgoing_connections
            .control,
        2,
        "physical TLS handshakes own the observed outgoing connections"
    );
    drop(first);
    within(&fixture, async {
        while fixture.transport.usage().inspection_items != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let third = begin(deadline)?;
    let held_third = within(&fixture, listener.accept()).await??;
    publisher.restart().await?;
    fixture
        .transport
        .accept_node_view(publisher.view().await?, &trusted)?;
    assert!(matches!(
        second.receive().await,
        Err(InspectionError::StalePeer)
    ));
    assert!(matches!(
        third.receive().await,
        Err(InspectionError::StalePeer)
    ));
    assert_eq!(fixture.transport.usage().inspection_items, 0);
    assert_eq!(
        fixture
            .transport
            .resource_observer()
            .capture()
            .unwrap()
            .outgoing_connections
            .control,
        0
    );
    // Retain a caller while its new target withholds the entire TLS response.
    let current_listener = TcpListener::bind("127.0.0.1:0").await?;
    // A new publication must represent another actual process, rather than
    // rewriting addresses within an existing process generation.
    publisher.log.close().await?;
    publisher.peer.process = ProcessGeneration::new([45; 16])?;
    publisher.peer.control_address = current_listener.local_addr()?;
    publisher.log = publisher
        .domain
        .run(ReplicaLog::open(
            ReplicaLogOptions::new(
                publisher.root.path().join("log"),
                publisher.peer.node,
                publisher.peer.disk,
                publisher.peer.process,
                LogOpenMode::Resume,
            ),
            Arc::new(cellar_ltx::DirectFileSystem),
        ))
        .await??;
    fixture
        .transport
        .accept_node_view(publisher.view().await?, &trusted)?;
    let waiting = fixture.transport.inspect_peer(
        scope(2)?,
        [5; 32],
        [6; 32],
        publisher.peer.clone(),
        state.domain.mono_ms()?.saturating_add(5000),
    )?;
    let held_current = within(&fixture, current_listener.accept()).await??;
    within(&fixture, fixture.transport.close()).await?;
    assert!(matches!(
        waiting.receive().await,
        Err(InspectionError::Unavailable)
    ));
    assert_eq!(fixture.transport.usage().inspection_items, 0);
    assert_eq!(fixture.transport.usage().tasks, 0);
    drop((held_first, held_second, held_third, held_current));
    publisher.close().await?;
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inspection_deadline_and_missing_discovery_never_report_failed_tcp() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let fixture = Fixture::start(TransportOptions::default(), vec![]).await?;
    let mut trusted = peer(2)?;
    trusted.control_address = listener.local_addr()?;
    let state = &fixture.transport.handle.0;
    let deadline = state.domain.mono_ms()?.saturating_add(300);
    assert!(matches!(
        fixture
            .transport
            .inspect_peer(scope(2)?, [5; 32], [6; 32], trusted.clone(), deadline),
        Err(InspectionError::Refused)
    ));
    let publisher = Publisher::new(trusted.clone()).await?;
    fixture
        .transport
        .accept_node_view(publisher.view().await?, &trusted)?;
    let deadline = state.domain.mono_ms()?.saturating_add(300);
    let waiting =
        fixture
            .transport
            .inspect_peer(scope(2)?, [5; 32], [6; 32], trusted.clone(), deadline)?;
    let held = within(&fixture, listener.accept()).await??;
    assert!(matches!(
        waiting.receive().await,
        Err(InspectionError::Unavailable)
    ));
    assert_eq!(fixture.transport.usage().inspection_items, 0);
    drop((held, listener));
    let failed = fixture.transport.inspect_peer(
        scope(2)?,
        [5; 32],
        [6; 32],
        trusted,
        state.domain.mono_ms()?.saturating_add(1000),
    )?;
    assert!(matches!(
        failed.receive().await,
        Err(InspectionError::Unreachable)
    ));
    publisher.close().await?;
    fixture.close().await
}
