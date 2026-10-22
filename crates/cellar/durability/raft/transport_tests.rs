use super::*;
#[path = "transport_directory_tests.rs"]
mod directory;
use crate::raft::options::{LogOpenMode, RaftRuntimeOptions, ReplicaLogOptions};
use crate::raft::storage::ReplicaLog;
use cellar_logic::consensus::{DiskLineage, ProcessGeneration};
use cellar_logic::consensus::{GroupId, IncarnationId, MemberId, NodeIdentity};

struct Fixture {
    domain: crate::asyncrt::ExecutionDomain,
    log: ReplicaLog,
    runtime: RaftRuntime,
    transport: RaftTransport,
    _events: crate::raft::runtime::RuntimeEvents,
    _directory: tempfile::TempDir,
}

impl Fixture {
    async fn start(
        options: TransportOptions,
        groups: Vec<crate::raft::storage::GroupBootstrap>,
    ) -> anyhow::Result<Self> {
        let directory = tempfile::tempdir()?;
        let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
        let log = domain
            .run(ReplicaLog::open(
                ReplicaLogOptions::new(
                    directory.path().join("log"),
                    NodeId::new([1; 16])?,
                    DiskLineage::new([2; 16])?,
                    ProcessGeneration::new([3; 16])?,
                    LogOpenMode::Create,
                ),
                Arc::new(cellar_ltx::DirectFileSystem),
            ))
            .await??;
        for group in groups {
            log.create_group(group).await?;
        }
        let (runtime, events) =
            RaftRuntime::start(log.clone(), RaftRuntimeOptions::default()).await?;
        let tls = RaftTls::new(
            vec![include_bytes!("../../transport/tcp/fixtures/ca.der")
                .to_vec()
                .into()],
            vec![include_bytes!("../../transport/tcp/fixtures/server.der")
                .to_vec()
                .into()],
            rustls::pki_types::PrivatePkcs8KeyDer::from(
                include_bytes!("../../transport/tcp/fixtures/key.der").to_vec(),
            )
            .into(),
        )?;
        let transport = RaftTransport::start(
            runtime.clone(),
            Arc::new(PeerAuth::new([8; 32], "node-1")?),
            tls,
            TcpListener::bind("127.0.0.1:0").await?,
            TcpListener::bind("127.0.0.1:0").await?,
            TcpListener::bind("127.0.0.1:0").await?,
            options,
        )
        .await?;
        Ok(Self {
            domain,
            log,
            runtime,
            transport,
            _events: events,
            _directory: directory,
        })
    }

    async fn close(self) -> anyhow::Result<()> {
        self.transport.close().await;
        self.runtime.close().await?;
        self.log.close().await?;
        self.domain.shutdown().await;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn connection_resources_count_handshakes_until_physical_exit() -> anyhow::Result<()> {
    let fixture = Fixture::start(
        TransportOptions {
            max_connections_per_lane: 1,
            ..Default::default()
        },
        vec![],
    )
    .await?;
    let observer = fixture.transport.resource_observer();
    let result = fixture
        .domain
        .run(async {
            let control = TcpStream::connect(fixture.transport.control_address()).await?;
            let data = TcpStream::connect(fixture.transport.data_address()).await?;
            let query = TcpStream::connect(fixture.transport.query_address()).await?;
            // No TLS bytes are sent. These own real sockets while awaiting TLS,
            // despite having no authenticated frame or byte-bank reservation.
            crate::asyncrt::timeout(std::time::Duration::from_secs(2), async {
                loop {
                    let usage = observer.capture().unwrap();
                    if usage.incoming_connections
                        == (crate::application::consensus::RaftConnectionReservations {
                            control: 1,
                            data: 1,
                            query: 1,
                        })
                    {
                        assert_eq!(
                            usage.incoming_connection_limits,
                            crate::application::consensus::RaftConnectionReservations {
                                control: 1,
                                data: 1,
                                query: 1
                            }
                        );
                        assert_eq!(
                            usage.incoming.control.items
                                + usage.incoming.data.items
                                + usage.incoming.query.items,
                            0
                        );
                        break;
                    }
                    crate::asyncrt::sleep(std::time::Duration::from_millis(1)).await;
                }
            })
            .await?;
            drop((control, data, query));
            crate::asyncrt::timeout(std::time::Duration::from_secs(2), async {
                loop {
                    let usage = observer.capture().unwrap();
                    if usage.incoming_connections
                        == (crate::application::consensus::RaftConnectionReservations {
                            control: 0,
                            data: 0,
                            query: 0,
                        })
                    {
                        break;
                    }
                    crate::asyncrt::sleep(std::time::Duration::from_millis(1)).await;
                }
            })
            .await?;
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    fixture.close().await?;
    assert!(observer.capture().is_none());
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn poisoned_registry_still_waits_for_resource_owners() -> anyhow::Result<()> {
    let fixture = Fixture::start(TransportOptions::default(), vec![]).await?;
    let (release, wait) = oneshot::channel();
    // Model a resource destructor which is still finishing after admission
    // closes. close() must retain its claim until this owner actually leaves.
    fixture.transport.handle.0.spawn(async move {
        let _ = wait.await;
    })?;
    let poisoned = fixture.transport.handle.0.clone();
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _registry = poisoned.registry.lock().unwrap();
            panic!("injected registry panic");
        }))
        .is_err()
    );
    fixture
        .domain
        .run(async {
            let close = fixture.transport.close();
            tokio::pin!(close);
            assert!(
                crate::asyncrt::timeout(std::time::Duration::from_millis(20), &mut close)
                    .await
                    .is_err()
            );
            assert!(fixture.transport.usage().tasks >= 1);
            assert!(fixture.transport.handle.0.claim.lock().unwrap().is_some());
            release.send(()).unwrap();
            crate::asyncrt::timeout(std::time::Duration::from_secs(2), close).await?;
            assert_eq!(fixture.transport.usage().tasks, 0);
            assert!(fixture.transport.handle.0.claim.lock().unwrap().is_none());
            Ok::<_, anyhow::Error>(())
        })
        .await??;
    fixture.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn query_grants_are_scoped_revocable_and_never_voting_authority() -> anyhow::Result<()> {
    use crate::raft::{query_identity::QueryRoute, storage::GroupBootstrap};
    let first = GroupScope {
        cluster: ClusterId::new([1; 16])?,
        namespace: NamespaceId::new([2; 16])?,
        cell: "Counter:one".into(),
        group: GroupId::new([3; 16])?,
        incarnation: IncarnationId::new([4; 16])?,
    };
    let mut second = first.clone();
    second.namespace = NamespaceId::new([5; 16])?;
    second.group = GroupId::new([6; 16])?;
    let groups = [&first, &second]
        .into_iter()
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
    let fixture = Fixture::start(
        TransportOptions {
            max_query_origins: 2,
            ..Default::default()
        },
        groups,
    )
    .await?;
    for scope in [&first, &second] {
        let peers = (1..=3)
            .map(|member| {
                Ok(RaftPeer {
                    replica: ReplicaIdentity {
                        scope: scope.clone(),
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
        fixture.transport.install_group(GroupRoute {
            scope: scope.clone(),
            schema_digest: [9; 32],
            revision: 1,
            peers,
        })?;
    }
    fixture
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
            let route = |scope: &GroupScope, source| {
                QueryRoute::new(
                    source,
                    "node-4",
                    fixture.log.group(scope).unwrap().identity(),
                    "node-1",
                    [9; 32],
                )
            };
            let first_route = route(&first, origin)?;
            let second_route = route(&second, origin)?;
            let state = &fixture.transport.handle.0;
            assert!(matches!(
                state.authorize_query_route(&first_route),
                Err(TransportError::Refused)
            ));
            let mut grant = NamespaceQueryOrigin {
                cluster: first.cluster,
                namespace: first.namespace,
                revision: 1,
                peer: peer.clone(),
            };
            fixture.transport.grant_query_origin(grant.clone())?;
            let (pool, permission) = state.authorize_query_route(&first_route)?;
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
                scope: first.clone(),
                node: origin.node,
                disk: origin.disk,
                process: origin.process,
                member: MemberId::new(99)?,
            };
            assert!(state
                .authorize_route(&Route::new(
                    &fake_member,
                    "node-4",
                    fixture.log.group(&first)?.identity(),
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
            assert_eq!(fixture.log.groups()?.len(), 2);
            Ok::<_, anyhow::Error>(())
        })
        .await??;
    fixture.close().await
}
