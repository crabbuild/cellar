// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Authorization and physical ownership with real sockets and engine fences.
use super::*;
use crate::raft::transport_options::NamespaceInspectionOrigin;

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
