// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Authorization and physical ownership with real sockets and engine fences.
use super::*;
use crate::application::consensus::client;
use crate::raft::inspection::InspectionError;
use crate::raft::inspection_identity::{InspectionRoute, PhysicalNode};
use crate::raft::transport::snapshots::Wire;
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inspection_revocation_and_process_refresh_cancel_an_admitted_inbound_call(
) -> anyhow::Result<()> {
    for refresh in [false, true] {
        let fixture = Fixture::start(TransportOptions::default(), vec![]).await?;
        let trusted = peer(2)?;
        let mut publisher = Publisher::new(trusted.clone()).await?;
        fixture
            .transport
            .accept_node_view(publisher.view().await?, &trusted)?;
        let scope = scope(2)?;
        fixture
            .transport
            .grant_inspection_origin(NamespaceInspectionOrigin {
                cluster: scope.cluster,
                namespace: scope.namespace,
                revision: 1,
                peer: trusted.clone(),
            })?;
        // Hold only the host's consumer boundary. Authentication, TLS, control
        // admission, cancellation and socket ownership are production paths.
        let (requests, _business) = mailbox::channel(2);
        let (control_requests, mut controls) = mailbox::channel(2);
        let (close, _closes) = mpsc::unbounded_channel();
        let state = &fixture.transport.handle.0;
        let class = Arc::new(crate::application::consensus::RaftClass::new(
            "Counter", 1, [5; 32],
        )?);
        let schema = class.schema_digest();
        let handle = Arc::new(client::Handle {
            performance: state.runtime.log().performance(),
            requests,
            control_requests,
            control_budget: Budget::new(128 * 1024, 4),
            close,
            domain: state.domain.clone(),
            closed: Arc::new(AtomicBool::new(false)),
            closing: Arc::new(AtomicBool::new(false)),
            admission: Arc::new(crate::application::consensus::admission::Admission::default()),
            terminal: Arc::new(client::Terminal {
                result: Mutex::new(None),
                changed: Notify::new(),
            }),
            budget: Budget::new(4096, 1),
            archive_capture_budget: Budget::new(4096, 1),
            query_pins: Budget::new(0, 1),
            sql: None,
            extents: None,
            groups: crate::raft::host::GroupResourceObserver::default(),
            controller: Mutex::new(None),
            transport: fixture.transport.resource_observer(),
            max_input_bytes: 4096,
            bindings: BTreeMap::from([(
                (scope.namespace, scope.cell.clone()),
                client::Binding {
                    scope: scope.clone(),
                    class,
                    minimum: cellar_logic::consensus::Durability::Quorum,
                    member: client::LocalMember::new(None),
                    leader_hint: None,
                },
            )]),
            work: fixture
                .domain
                .run(async { crate::asyncrt::services().application_work_registry() })
                .await?,
        });
        fixture.transport.install_query_gateway(handle.clone())?;
        let source = PhysicalNode {
            identity: NodeIdentity {
                node: trusted.node,
                disk: trusted.disk,
                process: trusted.process,
            },
            storage_instance: publisher.log.storage_instance()?,
        };
        let local = fixture.log.limits();
        let target = PhysicalNode {
            identity: NodeIdentity {
                node: local.node,
                disk: local.disk,
                process: local.process,
            },
            storage_instance: fixture.log.storage_instance()?,
        };
        let route = InspectionRoute::new(
            &scope,
            schema,
            [6; 32],
            &source,
            &trusted.label,
            &target,
            "node-1",
        )?;
        let tcp = TcpStream::connect(fixture.transport.control_address()).await?;
        let mut stream = tokio_rustls::TlsConnector::from(state.tls.client.clone())
            .connect(rustls::pki_types::ServerName::try_from("localhost")?, tcp)
            .await?;
        let exporter =
            stream
                .get_ref()
                .1
                .export_keying_material([0; 32], inspections::EXPORT_LABEL, None)?;
        let wire = Wire {
            auth: &state.auth,
            exporter: &exporter,
            options: &state.options,
            max_body: 64 * 1024,
        };
        let head = wire.encode(
            inspections::REQUEST_MAGIC,
            b"cellar-inspection-request-1",
            &[],
            &serde_json::json!({"route":route,"nonce":"1","timeout_ms":5000}),
            &[],
        )?;
        write_head(&mut stream, &head).await?;
        let admitted = within(&fixture, async {
            crate::asyncrt::select! {
                request = controls.recv() => Ok(request),
                response = stream.read_u8() => Err(anyhow::anyhow!("request refused before host admission: {response:?}")),
            }
        }).await.map_err(|error| anyhow::anyhow!("host admission wait: {error}"))??
            .ok_or_else(|| anyhow::anyhow!("no admitted inspection"))?;
        assert!(matches!(admitted.kind, client::Kind::Inspect));
        assert!(!admitted.control.cancelled());
        if refresh {
            publisher.restart().await?;
            fixture
                .transport
                .accept_node_view(publisher.view().await?, &trusted)?;
        } else {
            fixture.transport.revoke_inspection_origin(
                scope.cluster,
                scope.namespace,
                trusted.node,
                2,
            )?;
        }
        assert!(
            within(&fixture, stream.read_u8())
                .await
                .map_err(|error| anyhow::anyhow!("revoked socket closure: {error}"))?
                .is_err(),
            "revoked request returned metadata"
        );
        assert!(
            admitted.control.cancelled(),
            "target SDK producer retained an uncancelled inspection"
        );
        within(&fixture, async {
            while fixture.transport.usage().inspection_items != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|error| anyhow::anyhow!("metadata ownership release: {error}"))?;
        drop((admitted, handle));
        publisher.close().await?;
        fixture.close().await?;
    }
    Ok(())
}
