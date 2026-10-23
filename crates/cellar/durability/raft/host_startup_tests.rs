//! A startup error owns infrastructure until actual file-lock release.
#![allow(clippy::disallowed_methods)]
use super::*;
use crate::raft::{
    options::{LogOpenMode, RaftRuntimeOptions, ReplicaLogOptions},
    storage::ReplicaLog,
    transport_options::{RaftTls, TransportOptions},
    version::{VersionKeys, VersionSigningKey},
};
use anyhow::Context as _;
use std::time::Duration;
#[path = "../../tests/support/raft_storage_fault.rs"]
#[allow(dead_code)]
mod fault;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_preflight_joins_infrastructure_with_retained_handles() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let gate = fault::Gate::new(None);
    let options = ReplicaLogOptions::new(
        root.path().join("raft"),
        NodeId::new([1; 16])?,
        DiskLineage::new([2; 16])?,
        ProcessGeneration::new([3; 16])?,
        LogOpenMode::Create,
    );
    let log = domain
        .run(ReplicaLog::open(
            options.clone(),
            Arc::new(fault::FaultFileSystem { gate: gate.clone() }),
        ))
        .await??;
    let (runtime, events) =
        RaftRuntime::start_parked(log.clone(), RaftRuntimeOptions::default()).await?;
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
        Arc::new(crate::peer_auth::PeerAuth::new([8; 32], "startup")?),
        tls,
        tokio::net::TcpListener::bind("127.0.0.1:0").await?,
        tokio::net::TcpListener::bind("127.0.0.1:0").await?,
        tokio::net::TcpListener::bind("127.0.0.1:0").await?,
        TransportOptions::default(),
    )
    .await?;
    let service = crate::daemon::NodeRaftService {
        log: log.clone(),
        runtime: runtime.clone(),
        events,
        transport: transport.clone(),
    };
    let release = gate.arm(fault::HOLD_LOCK_DROP);
    let mut startup = Box::pin(domain.run(start(
        service,
        Registrations {
            cells: vec![],
            peers: vec![],
            origins: vec![],
        },
        root.path().join("cells"),
        "startup".into(),
        RaftHostOptions::default(),
        VersionKeys::new(
            VersionSigningKey::new([31; 32], u64::MAX)?,
            vec![],
            Duration::from_secs(86400),
        )?,
    )));
    let early = tokio::time::timeout(Duration::from_millis(30), &mut startup).await;
    let returned_early = early.is_ok();
    if !returned_early {
        let waiting = futures_util::future::select(Box::pin(gate.entered.notified()), &mut startup);
        match tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .context("wait for startup lock-drop gate")?
        {
            futures_util::future::Either::Left(_) => (),
            futures_util::future::Either::Right(_) => {
                anyhow::bail!("startup settled while lock drop was held")
            }
        }
    }
    drop(release);
    let result = match early {
        Ok(result) => result,
        Err(_) => tokio::time::timeout(Duration::from_secs(5), startup)
            .await
            .context("wait for failed startup after lock release")?,
    }?;
    let admission_closed = matches!(
        log.storage_instance(),
        Err(super::super::storage::LogError::Closed)
    );
    if !returned_early {
        // This is deliberately before cleanup through the retained handles.
        let mut resumed = options;
        resumed.mode = LogOpenMode::Resume;
        resumed.process = ProcessGeneration::new([9; 16])?;
        let reopened = domain
            .run(ReplicaLog::open(
                resumed,
                Arc::new(cellar_ltx::DirectFileSystem),
            ))
            .await??;
        reopened.close().await?;
    }
    let _ = runtime.close().await;
    transport.close().await;
    log.close().await?;
    domain.shutdown().await;
    assert!(
        result.is_err(),
        "empty registration unexpectedly started a host"
    );
    assert!(
        !returned_early,
        "failed startup returned while its infrastructure still owned storage"
    );
    assert!(
        admission_closed,
        "failed startup left retained storage handles admitted"
    );
    Ok(())
}

#[allow(dead_code)]
#[path = "../../tests/support/raft_catalog_fault.rs"]
mod catalog_fault;

struct StartupNode {
    root: tempfile::TempDir,
    domain: crate::asyncrt::ExecutionDomain,
    log: ReplicaLog,
    runtime: RaftRuntime,
    events: Option<RuntimeEvents>,
    transport: RaftTransport,
}

impl StartupNode {
    async fn open(id: u8) -> anyhow::Result<Self> {
        let root = tempfile::tempdir()?;
        let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
        let log = domain
            .run(ReplicaLog::open(
                ReplicaLogOptions::new(
                    root.path().join("raft"),
                    NodeId::new([id; 16])?,
                    DiskLineage::new([id + 1; 16])?,
                    ProcessGeneration::new([id + 2; 16])?,
                    LogOpenMode::Create,
                ),
                Arc::new(cellar_ltx::DirectFileSystem),
            ))
            .await??;
        let (runtime, events) = RaftRuntime::start_parked(
            log.clone(),
            RaftRuntimeOptions {
                max_requests: 1,
                max_events: 1,
                tick_interval_ms: None,
                ..Default::default()
            },
        )
        .await?;
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
            Arc::new(crate::peer_auth::PeerAuth::new(
                [8; 32],
                format!("node-{id}"),
            )?),
            tls,
            tokio::net::TcpListener::bind("127.0.0.1:0").await?,
            tokio::net::TcpListener::bind("127.0.0.1:0").await?,
            tokio::net::TcpListener::bind("127.0.0.1:0").await?,
            TransportOptions::default(),
        )
        .await?;
        Ok(Self {
            root,
            domain,
            log,
            runtime,
            events: Some(events),
            transport,
        })
    }

    fn peer(&self) -> RaftPeerNode {
        let limits = self.log.limits();
        RaftPeerNode {
            node: limits.node,
            disk: limits.disk,
            process: limits.process,
            label: format!("node-{}", limits.node.bytes()[0]),
            server_name: "localhost".into(),
            control_address: self.transport.control_address(),
            data_address: self.transport.data_address(),
            query_address: self.transport.query_address(),
        }
    }

    fn service(&mut self) -> crate::daemon::NodeRaftService {
        crate::daemon::NodeRaftService {
            log: self.log.clone(),
            runtime: self.runtime.clone(),
            events: self.events.take().unwrap(),
            transport: self.transport.clone(),
        }
    }

    async fn close(self) -> anyhow::Result<()> {
        self.transport.close().await;
        let _ = self.runtime.close().await;
        self.log.close().await?;
        self.domain.shutdown().await;
        Ok(())
    }
}

struct ReleaseClaim(Arc<catalog_fault::Store>);

impl Drop for ReleaseClaim {
    fn drop(&mut self) {
        self.0.claim_hang.store(false, Ordering::Release);
        self.0.claim_resume.notify_waiters();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peer_traffic_waits_until_all_local_groups_attach() -> anyhow::Result<()> {
    use super::super::transport::TransportError;
    use ::raft::prelude::{Message, MessageType};
    let mut local = StartupNode::open(1).await?;
    let remote = StartupNode::open(2).await?;
    let third = StartupNode::open(3).await?;
    let peers = BTreeMap::from_iter(
        [local.peer(), remote.peer(), third.peer()]
            .into_iter()
            .map(|p| (p.node, p)),
    );
    let store = Arc::new(catalog_fault::Store::default());
    let catalog = Catalog::new(store.clone(), CatalogOptions::default())?;
    let class =
        Arc::new(RaftClass::new("Counter", 1, [7; 32])?.query("Get", [8; 32], |_, _| Ok(vec![]))?);
    let namespace = catalog
        .open_namespace(NamespaceConfig::new(
            ClusterId::new([1; 16])?,
            NamespaceId::new([2; 16])?,
            cellar_logic::NamespaceMode::Raft,
            class.schema_digest(),
        ))
        .await?;
    let mut births = Vec::new();
    for id in 1..=2 {
        births.push(
            namespace
                .resolve_birth(BirthProposal {
                    scope: GroupScope {
                        cluster: namespace.config().cluster,
                        namespace: namespace.config().namespace,
                        cell: format!("Counter:startup-{id}"),
                        group: GroupId::new([id; 16])?,
                        incarnation: IncarnationId::new([9; 16])?,
                    },
                    voters: (1..=3)
                        .map(|member| {
                            Ok(InitialMember {
                                member: MemberId::new(member)?,
                                node: NodeId::new([member as u8; 16])?,
                            })
                        })
                        .collect::<anyhow::Result<_>>()?,
                    learners: vec![],
                })
                .await?,
        );
    }
    let first = births[0].scope().clone();
    let second = births[1].scope().clone();
    namespace
        .bootstrap_member(&births[0], &remote.log)
        .await?
        .attach(&remote.runtime)
        .await?;
    remote.transport.install_group(membership::route(
        &remote.log.group(&first)?,
        &births[0],
        &peers,
        class.schema_digest(),
        "node-2",
        &remote.transport,
    )?)?;
    // Hold the second local member's permanent claim after the first member
    // has attached. Its real peer keeps sending while no host drains events.
    store.hold_nth_claim.store(2, Ordering::Release);
    store.claim_hang.store(true, Ordering::Release);
    let release = ReleaseClaim(store.clone());
    let registrations = Registrations {
        cells: births
            .into_iter()
            .map(|birth| Registration {
                placement: RaftCellPlacement::Replica,
                namespace: namespace.clone(),
                birth,
                class: class.clone(),
            })
            .collect(),
        peers: peers.into_values().collect(),
        origins: vec![],
    };
    let service = local.service();
    let directory = local.root.path().join("cells");
    let domain = local.log.domain();
    let startup = tokio::spawn(async move {
        domain
            .run(start(
                service,
                registrations,
                directory,
                "node-1".into(),
                RaftHostOptions::default(),
                VersionKeys::new(
                    VersionSigningKey::new([31; 32], u64::MAX)?,
                    vec![],
                    Duration::from_secs(86400),
                )?,
            ))
            .await?
    });
    let observed = async {
        tokio::time::timeout(Duration::from_secs(5), store.claim_waiting.notified()).await?;
        let mut responses = Vec::new();
        for _ in 0..16 {
            let mut message = Message {
                from: 2,
                to: 1,
                term: 1,
                ..Default::default()
            };
            message.set_msg_type(MessageType::MsgHeartbeat);
            responses.push(
                tokio::time::timeout(
                    Duration::from_secs(3),
                    remote.transport.submit(&first, message)?.accepted(),
                )
                .await?,
            );
        }
        Ok::<_, anyhow::Error>(responses)
    }
    .await;
    drop(release);
    let started = tokio::time::timeout(Duration::from_secs(10), startup).await??;
    let result = async {
        let responses = observed?;
        anyhow::ensure!(
            responses
                .iter()
                .all(|response| matches!(response, Err(TransportError::UnknownGroup))),
            "peer traffic entered an incomplete host: {responses:?}"
        );
        let host = started
            .as_ref()
            .map_err(|error| anyhow::anyhow!("startup failed: {error:#}"))?;
        for scope in [&first, &second] {
            anyhow::ensure!(host.cell(scope.namespace, &scope.cell).is_ok());
            local.runtime.status(scope).await?;
        }
        let mut message = Message {
            from: 2,
            to: 1,
            term: 1,
            ..Default::default()
        };
        message.set_msg_type(MessageType::MsgHeartbeat);
        tokio::time::timeout(
            Duration::from_secs(3),
            remote.transport.submit(&first, message)?.accepted(),
        )
        .await??;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    if let Ok(host) = started {
        host.close().await?;
    }
    remote.close().await?;
    third.close().await?;
    local.close().await?;
    result
}
