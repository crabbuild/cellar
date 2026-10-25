//! Local empty-host controls are a sub-boundary; remote node readiness and
//! complete managed inventory remain required before FO-03 can finish.
use super::{fault, public_node};
use anyhow::{ensure, Result};
use cellar::application::consensus::verification::{
    Catalog, FleetCatalog, FleetCatalogError, FleetChange, FleetHistory, FleetMutation, FleetTable,
    GroupBootstrap, ReplicaLog,
};
use cellar::bucket::{Bucket, StorageBackend};
use cellar::sdk::{
    consensus::*, ExecutionDomain, NodeOutcome, NodeRaftConfig, NodeRaftListeners, ShutdownMode,
};
use object_store::list::*;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

const TIMEOUT: Duration = Duration::from_secs(5);

#[path = "raft_initial_native.rs"]
mod initial_tests;

#[path = "raft_namespace_registry_hosts.rs"]
mod namespace_registry_tests;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_enrollment_node_probe_retained_age() -> Result<()> {
    let f = Fixture::new().await?;
    let (_root, node, host) = f.empty_host().await?;
    let (_peer_root, peer_node, _peer) = f.peer_host(5).await?;
    let revision = f.catalog.read(TIMEOUT).await?.revision();
    let target = NodeId::new([5; 16])?;
    f.remote(&host, target, revision).await?;
    let pending = host.inspect_peer_node(target, revision, TIMEOUT);
    let retained = Duration::from_millis(150);
    f.domain
        .run(async { cellar::asyncrt::sleep(retained).await })
        .await?;
    let observed = pending.await?;
    ensure!(observed.sample_age_ms >= retained.as_millis() as u64);
    node.shutdown(ShutdownMode::Preserve).await?;
    peer_node.shutdown(ShutdownMode::Preserve).await?;
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_enrollment_node_probe_resource_floor() -> Result<()> {
    let f = Fixture::new().await?;
    let authorization = f.enroll().await?;
    let bad_root = tempfile::tempdir()?;
    let bad = public_node::node_named(
        bad_root.path(),
        f.definition.application.clone(),
        &f.spec.label,
    )
    .await?;
    let (mut config, services) = f.config(&bad, authorization.clone()).await?;
    config.host.max_control_bytes = 4 * 1024 * 1024 + 4096;
    config.host.max_control_requests = 2;
    let failure = bad
        .start_raft_cells(config, services)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("undersized managed control capacity accepted"))?;
    ensure!(format!("{failure:#}")
        .contains("managed discovery requires bounded metadata and control capacity"));
    bad.shutdown(ShutdownMode::Preserve).await?;
    let snapshot = f.catalog.read(TIMEOUT).await?;
    ensure!(f
        .catalog
        .authorize_host(&snapshot, f.spec.node, TIMEOUT)
        .await?
        .storage_instance()
        .is_none());

    let root = tempfile::tempdir()?;
    let node =
        public_node::node_named(root.path(), f.definition.application.clone(), &f.spec.label)
            .await?;
    let (mut config, services) = f.config(&node, authorization).await?;
    config.host.max_control_bytes = 4 * 1024 * 1024 + 64 * 1024 + 4096;
    config.host.max_control_requests = 3;
    let host = node.start_raft_cells(config, services).await?;
    let (_peer_root, peer_node, _peer) = f.peer_host(5).await?;
    let revision = f.catalog.read(TIMEOUT).await?.revision();
    f.remote(&host, NodeId::new([5; 16])?, revision).await?;
    f.store.fleet_read_hang.store(true, Ordering::Release);
    f.store.hold_fleet_reads.store(true, Ordering::Release);
    f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            while f.store.active_fleet_reads.load(Ordering::Acquire) < 2 {
                cellar::asyncrt::sleep(Duration::from_millis(5)).await;
            }
        }))
        .await??;
    let observed = host
        .inspect_peer_node(NodeId::new([5; 16])?, revision, TIMEOUT)
        .await?;
    ensure!(observed.view.current_enrollment.unwrap().revision >= revision);
    ensure!(host.resource_usage().control.bytes == 4 * 1024 * 1024);
    node.shutdown(ShutdownMode::Preserve).await?;
    peer_node.shutdown(ShutdownMode::Preserve).await?;
    ensure!(f.store.active_fleet_reads.load(Ordering::Acquire) == 0);
    ensure!(host.resource_usage().control.bytes == 0);
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_enrollment_node_probe_shutdown() -> Result<()> {
    use tokio::io::AsyncReadExt;
    let f = Fixture::new().await?;
    let (_root, node, host) = f.empty_host().await?;
    let (_peer_root, peer_node, _peer) = f.peer_host(5).await?;
    let revision = f.catalog.read(TIMEOUT).await?.revision();
    f.remote(&host, NodeId::new([5; 16])?, revision).await?;
    let peer_log_path = peer_node.info().data_dir.join("raft");
    peer_node.shutdown(ShutdownMode::Preserve).await?;
    let process = ProcessGeneration::new([99; 16])?;
    let log = f
        .domain
        .run(ReplicaLog::open(
            ReplicaLogOptions::new(
                peer_log_path,
                NodeId::new([5; 16])?,
                DiskLineage::new([15; 16])?,
                process,
                LogOpenMode::Resume,
            ),
            Arc::new(cellar_ltx::DirectFileSystem),
        ))
        .await??;
    let control = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let data = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let query = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = RaftPeerNode {
        node: NodeId::new([5; 16])?,
        disk: DiskLineage::new([15; 16])?,
        process,
        label: "fleet-peer-5".into(),
        server_name: "localhost".into(),
        control_address: control.local_addr()?,
        data_address: data.local_addr()?,
        query_address: query.local_addr()?,
    };
    let namespace = Catalog::new(
        f.store.clone(),
        CatalogOptions {
            prefix: "host-enrollment/".into(),
            ..Default::default()
        },
    )?
    .existing_namespace(f.declaration.namespace.clone())
    .await?;
    namespace
        .register_node_storage(&log, TIMEOUT)
        .await?
        .publication(endpoint, TIMEOUT)
        .await?
        .publish(TIMEOUT)
        .await?;
    let tls = Arc::new(
        rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_no_client_auth()
        .with_single_cert(
            vec![include_bytes!("../../transport/tcp/fixtures/server.der")
                .to_vec()
                .into()],
            rustls::pki_types::PrivatePkcs8KeyDer::from(
                include_bytes!("../../transport/tcp/fixtures/key.der").to_vec(),
            )
            .into(),
        )?,
    );
    let (ready, mut accepted) = tokio::sync::oneshot::channel();
    let server = f.domain.spawn(async move {
        let (tcp, _) = control.accept().await?;
        let mut stream = tokio_rustls::TlsAcceptor::from(tls).accept(tcp).await?;
        let mut byte = [0; 1];
        stream.read_exact(&mut byte).await?;
        let _ = ready.send(());
        let mut buffer = [0; 1024];
        loop {
            match stream.read(&mut buffer).await {
                Ok(0) => break,
                Ok(_) => (),
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
                    ) =>
                {
                    break
                }
                Err(error) => return Err(error.into()),
            }
        }
        Ok::<_, anyhow::Error>(())
    })?;
    let pending = f.domain.run(cellar::asyncrt::timeout(TIMEOUT, async {
        loop {
            let mut pending = host.inspect_peer_node(NodeId::new([5;16])?, revision, TIMEOUT);
            cellar::asyncrt::select! {
                ready = &mut accepted => { ready?; return Ok::<_,anyhow::Error>(pending); },
                result = &mut pending => {
                    match result {
                        Err(InspectionError::Unreachable | InspectionError::Unavailable | InspectionError::StalePeer | InspectionError::Refused) => cellar::asyncrt::sleep(Duration::from_millis(10)).await,
                        result => anyhow::bail!("unexpected blackholed node result: {result:?}"),
                    }
                },
            }
        }
    })).await???;
    ensure!(host
        .resource_usage()
        .transport
        .is_some_and(|usage| usage.inspection_metadata.bytes > 0));
    let report = f
        .domain
        .run(cellar::asyncrt::timeout(TIMEOUT, host.close()))
        .await???;
    ensure!(report.unfinished.is_empty());
    ensure!(host.resource_usage().control.bytes == 4096);
    ensure!(matches!(pending.await, Err(InspectionError::Unavailable)));
    ensure!(host.resource_usage().control.bytes == 0);
    let joined = f
        .domain
        .run(cellar::asyncrt::timeout(TIMEOUT, server))
        .await??;
    joined??;
    log.close().await?;
    node.shutdown(ShutdownMode::Preserve).await?;
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_enrollment_empty_host() -> Result<()> {
    let f = Fixture::new().await?;
    let (_root, node, host) = f.empty_host().await?;
    let (_peer_root, peer_node, peer) = f.peer_host(5).await?;
    let revision = f.catalog.read(TIMEOUT).await?.revision();
    let remote = f.remote(&host, NodeId::new([5; 16])?, revision).await?;
    ensure!(remote.view.node.identity.node == NodeId::new([5; 16])?);
    ensure!(remote.view.node.identity.disk == DiskLineage::new([15; 16])?);
    ensure!(remote.view.node.groups == 0 && remote.view.node.reserved_bytes == 0);
    ensure!(remote.view.registered_histories == 0 && remote.view.dormant_histories == 0);
    ensure!(
        remote.view.pending_history_registrations == 0
            && remote.view.unattached_engine_histories == 0
    );
    let current = remote.view.current_enrollment.unwrap();
    ensure!(current.fleet == [80; 16] && current.revision >= revision && current.epoch == 2);
    ensure!(
        current.state == FleetNodeState::Registered
            && !current.state.eligibility().receive_replicas
    );
    ensure!(
        current.physical_host == f.spec.physical_host
            && current.capability_digest == f.spec.capability_digest
    );
    ensure!(remote.sample_age_ms <= TIMEOUT.as_millis() as u64);
    let reverse = f.remote(&peer, f.spec.node, revision).await?;
    ensure!(reverse.view.node.identity.node == f.spec.node && reverse.view.node.groups == 0);
    ensure!(matches!(
        host.inspect_peer_node(NodeId::new([5; 16])?, revision + 1, TIMEOUT)
            .await,
        Err(InspectionError::StalePeer)
    ));
    ensure!(matches!(
        host.inspect_peer_node(NodeId::new([99; 16])?, revision, TIMEOUT)
            .await,
        Err(InspectionError::Refused)
    ));
    ensure!(matches!(
        host.inspect_peer_node(f.spec.node, revision, TIMEOUT).await,
        Err(InspectionError::Refused)
    ));
    ensure!(f.calls.load(Ordering::Acquire) == 0);
    node.shutdown(ShutdownMode::Preserve).await?;
    peer_node.shutdown(ShutdownMode::Preserve).await?;
    ensure!(host.resource_usage().control.bytes == 0 && peer.resource_usage().control.bytes == 0);
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_enrollment_eligibility_modes() -> Result<()> {
    let f = Fixture::new().await?;
    let (_root, node, host) = f.empty_host().await?;
    let (_peer_root, peer_node, _peer) = f.peer_host(5).await?;
    let target = NodeId::new([5; 16])?;
    for (i, state) in [
        FleetNodeState::Active,
        FleetNodeState::RestartPending,
        FleetNodeState::ResizePending,
        FleetNodeState::Draining,
        FleetNodeState::Evacuated,
    ]
    .into_iter()
    .enumerate()
    {
        let revision = f.state(target, state, 70 + i as u8).await?;
        let observation = f.remote(&host, target, revision).await?;
        ensure!(observation.view.startup_enrollment.unwrap().state == FleetNodeState::Registered);
        let current = observation.view.current_enrollment.unwrap();
        ensure!(
            current.state == state && current.revision >= revision && current.epoch == 3 + i as u64
        );
        ensure!(current.state.eligibility().receive_replicas == (state == FleetNodeState::Active));
        ensure!(
            current.state.eligibility().receive_leadership == (state == FleetNodeState::Active)
        );
        ensure!(
            current.state.eligibility().evacuate_memberships == (state == FleetNodeState::Draining)
        );
        ensure!(observation.view.node.groups == 0);
    }
    node.shutdown(ShutdownMode::Preserve).await?;
    peer_node.shutdown(ShutdownMode::Preserve).await?;
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_enrollment_dynamic_trust_isolation() -> Result<()> {
    let f = Fixture::new().await?;
    let (_root, node, host) = f.empty_host().await?;
    f.store.fleet_read_hang.store(true, Ordering::Release);
    f.store.hold_next_fleet_read.store(true, Ordering::Release);
    f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            while f.store.active_fleet_reads.load(Ordering::Acquire) == 0 {
                cellar::asyncrt::sleep(Duration::from_millis(5)).await;
            }
        }))
        .await??;
    ensure!(host.resource_usage().control.bytes >= 4 * 1024 * 1024);
    // A held background root read cannot own the foreground catalog turn.
    let authorization = f.history(61).await?;
    let cell = host
        .register_managed_history(authorization, TIMEOUT)
        .await?;
    ensure!(host.observe_node(TIMEOUT).await?.dormant_histories == 1);
    drop(cell);
    let report = f
        .domain
        .run(cellar::asyncrt::timeout(TIMEOUT, host.close()))
        .await???;
    ensure!(report.unfinished.is_empty());
    ensure!(f.store.active_fleet_reads.load(Ordering::Acquire) == 0);
    ensure!(host.resource_usage().control.bytes == 0 && host.resource_usage().control.items == 0);
    node.shutdown(ShutdownMode::Preserve).await?;
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_enrollment_dynamic_trust_refresh() -> Result<()> {
    let f = Fixture::new().await?;
    let (_root, node, host) = f.empty_host().await?;
    let initial = host.observe_node(TIMEOUT).await?;
    let installed = initial
        .fleet_trust_revision
        .ok_or_else(|| anyhow::anyhow!("missing rooted trust cut"))?;
    let startup = initial.startup_enrollment.unwrap().revision;
    let mut extra = f.spec.clone();
    extra.node = NodeId::new([5; 16])?;
    extra.disk = DiskLineage::new([15; 16])?;
    extra.label = "fleet-extra".into();
    let snapshot = f.catalog.read(TIMEOUT).await?;
    ensure!(snapshot.revision() == installed);
    let committed = f
        .catalog
        .register_node(
            &snapshot,
            &Fixture::request(60, snapshot.revision()),
            &extra,
            TIMEOUT,
        )
        .await?;
    let revision = committed.snapshot.revision();
    f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            loop {
                let observation = host.observe_node(TIMEOUT).await?;
                if observation.fleet_trust_revision == Some(revision) {
                    ensure!(observation.startup_enrollment.unwrap().revision == startup);
                    ensure!(observation.node.groups == 0 && observation.registered_histories == 0);
                    return Ok::<_, anyhow::Error>(());
                }
                cellar::asyncrt::sleep(Duration::from_millis(10)).await;
            }
        }))
        .await???;
    f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, host.close()))
        .await???;
    let usage = host.resource_usage();
    ensure!(usage.control.bytes == 0 && usage.control.items == 0);
    node.shutdown(ShutdownMode::Preserve).await?;
    f.domain.shutdown().await;
    Ok(())
}

#[derive(Debug)]
struct NoListing;

#[async_trait::async_trait]
impl PaginatedListStore for NoListing {
    async fn list_paginated(
        &self,
        _: Option<&str>,
        _: PaginatedListOptions,
    ) -> object_store::Result<PaginatedListResult> {
        Err(object_store::Error::NotSupported {
            source: "host enrollment never lists".into(),
        })
    }
}

struct Fixture {
    domain: ExecutionDomain,
    store: Arc<fault::Store>,
    catalog: FleetCatalog,
    declaration: RaftNamespaceRegistration,
    spec: FleetNodeSpec,
    definition: public_node::Counter,
    calls: Arc<AtomicUsize>,
    _entered: tokio::sync::mpsc::UnboundedReceiver<()>,
    distinct_hosts: bool,
}

impl Fixture {
    fn peer_spec(&self, id: u8) -> Result<FleetNodeSpec> {
        let mut spec = self.spec.clone();
        spec.node = NodeId::new([id; 16])?;
        spec.disk = DiskLineage::new([id + 10; 16])?;
        spec.label = format!("fleet-peer-{id}");
        if self.distinct_hosts {
            spec.physical_host = IncarnationId::new([id + 90; 16])?;
        }
        Ok(spec)
    }

    async fn peer_host(&self, id: u8) -> Result<(tempfile::TempDir, cellar::sdk::Node, RaftCells)> {
        let spec = self.peer_spec(id)?;
        let snapshot = self.catalog.read(TIMEOUT).await?;
        let commit = self
            .catalog
            .register_node(
                &snapshot,
                &Self::request(id + 50, snapshot.revision()),
                &spec,
                TIMEOUT,
            )
            .await?;
        let auth = self
            .catalog
            .authorize_host(&commit.snapshot, spec.node, TIMEOUT)
            .await?;
        let root = tempfile::tempdir()?;
        let node = public_node::node_named(
            root.path(),
            self.definition.application.clone(),
            &spec.label,
        )
        .await?;
        let (config, services) = self.config_for(&node, auth, &spec).await?;
        let host = node.start_raft_cells(config, services).await?;
        Ok((root, node, host))
    }

    async fn remote(
        &self,
        host: &RaftCells,
        node: NodeId,
        revision: u64,
    ) -> Result<RaftPeerNodeObservation> {
        self.domain
            .run(cellar::asyncrt::timeout(TIMEOUT, async {
                loop {
                    match host.inspect_peer_node(node, revision, TIMEOUT).await {
                        Ok(view) => return Ok::<_, anyhow::Error>(view),
                        Err(
                            InspectionError::Refused
                            | InspectionError::Unavailable
                            | InspectionError::StalePeer
                            | InspectionError::Overloaded,
                        ) => {
                            cellar::asyncrt::sleep(Duration::from_millis(10)).await;
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
            }))
            .await??
    }

    async fn state(&self, node: NodeId, state: FleetNodeState, id: u8) -> Result<u64> {
        let snapshot = self.catalog.read(TIMEOUT).await?;
        let key = hex::encode(node.bytes());
        let bytes = self
            .catalog
            .get(&snapshot, FleetTable::Nodes, &key, TIMEOUT)
            .await?
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes[13..bytes.len() - 32])?;
        let mut payload = std::str::from_utf8(&bytes[13..bytes.len() - 32])?.to_owned();
        let operating = matches!(
            state,
            FleetNodeState::RestartPending
                | FleetNodeState::ResizePending
                | FleetNodeState::Draining
                | FleetNodeState::Evacuated
                | FleetNodeState::Removed
        );
        for (field, replacement) in [
            ("state", serde_json::to_value(state)?),
            (
                "epoch",
                serde_json::json!(value["epoch"].as_u64().unwrap() + 1),
            ),
            (
                "operation",
                if operating {
                    serde_json::json!(vec![100; 16])
                } else {
                    serde_json::Value::Null
                },
            ),
        ] {
            payload = payload.replace(
                &format!("\"{field}\":{}", serde_json::to_string(&value[field])?),
                &format!("\"{field}\":{}", serde_json::to_string(&replacement)?),
            );
        }
        use sha2::{Digest, Sha256};
        let mut updated = bytes[..9].to_vec();
        updated.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        updated.extend_from_slice(payload.as_bytes());
        updated.extend_from_slice(&Sha256::digest(&updated));
        let commit = self
            .catalog
            .commit(
                &snapshot,
                &FleetMutation {
                    id: [id; 16],
                    operation: [100; 16],
                    principal: "enrollment-test".into(),
                    expected_revision: snapshot.revision(),
                    changes: vec![FleetChange {
                        table: FleetTable::Nodes,
                        key,
                        value: Some(updated),
                    }],
                },
                TIMEOUT,
            )
            .await?;
        Ok(commit.snapshot.revision())
    }

    async fn history(&self, id: u8) -> Result<FleetGroupAuthorization> {
        let namespace = Catalog::new(
            self.store.clone(),
            CatalogOptions {
                prefix: "host-enrollment/".into(),
                ..Default::default()
            },
        )?
        .existing_namespace(self.declaration.namespace.clone())
        .await?;
        let scope = GroupScope {
            cluster: self.declaration.namespace.cluster,
            namespace: self.declaration.namespace.namespace,
            cell: format!("Counter:dynamic-{id}"),
            group: GroupId::new([id; 16])?,
            incarnation: IncarnationId::new([3; 16])?,
        };
        let birth = namespace
            .resolve_birth(BirthProposal {
                scope: scope.clone(),
                voters: (1..=3)
                    .map(|id| {
                        Ok(InitialMember {
                            member: MemberId::new(id)?,
                            node: NodeId::new([id as u8; 16])?,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
                learners: vec![],
            })
            .await?;
        let snapshot = self.catalog.read(TIMEOUT).await?;
        let commit = self
            .catalog
            .enroll_history(
                &snapshot,
                &Self::request(id, snapshot.revision()),
                &namespace,
                &birth,
                TIMEOUT,
            )
            .await?;
        Ok(self
            .catalog
            .authorize_group(&commit.snapshot, &scope, TIMEOUT)
            .await?)
    }

    // A malicious/inconsistent rooted import can describe a birth in another
    // namespace root. Dynamic registration must still reject it at its owning
    // class/name/birth boundary rather than clobber an existing binding.
    async fn inconsistent_history(&self, scope: &GroupScope, id: u8) -> Result<Vec<u8>> {
        let namespace = Catalog::new(
            self.store.clone(),
            CatalogOptions {
                prefix: format!("inconsistent-{id}/"),
                ..Default::default()
            },
        )?
        .open_namespace(self.declaration.namespace.clone())
        .await?;
        let birth = namespace
            .resolve_birth(BirthProposal {
                scope: scope.clone(),
                voters: (1..=3)
                    .map(|n| {
                        Ok(InitialMember {
                            member: MemberId::new(n)?,
                            node: NodeId::new([n as u8; 16])?,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
                learners: vec![],
            })
            .await?;
        Ok(FleetHistory::observed(
            self.catalog.group_binding(birth.scope())?,
            &namespace,
            &birth,
            ProposalId::new([id; 16])?,
            ProposalId::new([100; 16])?,
        )?
        .encode()?)
    }

    async fn empty_host(&self) -> Result<(tempfile::TempDir, cellar::sdk::Node, RaftCells)> {
        let authorization = self.enroll().await?;
        let root = tempfile::tempdir()?;
        let node = public_node::node_named(
            root.path(),
            self.definition.application.clone(),
            &self.spec.label,
        )
        .await?;
        let (config, services) = self.config(&node, authorization).await?;
        let host = node.start_raft_cells(config, services).await?;
        Ok((root, node, host))
    }

    async fn birth_waiting(&self) -> Result<()> {
        self.domain
            .run(cellar::asyncrt::timeout(TIMEOUT, async {
                while !self.store.birth_waiting.load(Ordering::Acquire) {
                    cellar::asyncrt::sleep(Duration::from_millis(5)).await;
                }
            }))
            .await??;
        Ok(())
    }

    async fn new() -> Result<Self> {
        Self::with_managed_births(false).await
    }

    async fn with_managed_births(managed: bool) -> Result<Self> {
        Self::with_domains(managed, managed).await
    }

    async fn with_domains(managed: bool, distinct_hosts: bool) -> Result<Self> {
        let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
        let store = Arc::new(fault::Store::default());
        let bucket = Bucket::from_clients(
            store.clone(),
            store.clone(),
            Arc::new(NoListing),
            StorageBackend::S3,
            "enrollment".into(),
            "tenant/".into(),
        )?;
        let catalog = FleetCatalog::new(
            &domain,
            bucket,
            ClusterId::new([1; 16])?,
            IncarnationId::new([80; 16])?,
            "hosts/",
        )?;
        catalog.initialize(TIMEOUT).await?;
        let (gate, entered) = public_node::Gate::new();
        gate.release();
        let calls = Arc::new(AtomicUsize::new(0));
        let definition = public_node::counter(calls.clone(), gate)?;
        let mut declaration = RaftNamespaceRegistration {
            namespace: NamespaceConfig::new(
                ClusterId::new([1; 16])?,
                NamespaceId::new([2; 16])?,
                NamespaceMode::Raft,
                definition.schema,
            ),
            class: "Counter".into(),
        };
        if managed {
            declaration.namespace = catalog.bind_namespace(declaration.namespace)?;
        }
        let namespace = Catalog::new(
            store.clone(),
            CatalogOptions {
                prefix: "host-enrollment/".into(),
                ..Default::default()
            },
        )?
        .open_namespace(declaration.namespace.clone())
        .await?;
        let root = catalog.read(TIMEOUT).await?;
        catalog
            .declare_namespace(
                &root,
                &FleetRequestIdentity {
                    id: [250; 16],
                    operation: [251; 16],
                    principal: "fixture-declaration".into(),
                    expected_revision: root.revision(),
                },
                &namespace,
                "Counter",
                TIMEOUT,
            )
            .await?;
        let spec = FleetNodeSpec {
            node: NodeId::new([4; 16])?,
            disk: DiskLineage::new([14; 16])?,
            physical_host: IncarnationId::new([90; 16])?,
            label: "fleet-empty".into(),
            server_name: "localhost".into(),
            capability_digest: RaftManagedHostOptions::capability_digest(std::slice::from_ref(
                &declaration,
            ))?,
            storage_instance: None,
        };
        Ok(Self {
            domain,
            store,
            catalog,
            declaration,
            spec,
            definition,
            calls,
            _entered: entered,
            distinct_hosts,
        })
    }

    fn request(id: u8, revision: u64) -> FleetRequestIdentity {
        FleetRequestIdentity {
            id: [id; 16],
            operation: [100; 16],
            principal: "enrollment-test".into(),
            expected_revision: revision,
        }
    }

    async fn enroll(&self) -> Result<FleetHostAuthorization> {
        let snapshot = self.catalog.read(TIMEOUT).await?;
        let committed = self
            .catalog
            .register_node(
                &snapshot,
                &Self::request(3, snapshot.revision()),
                &self.spec,
                TIMEOUT,
            )
            .await?;
        Ok(self
            .catalog
            .authorize_host(&committed.snapshot, self.spec.node, TIMEOUT)
            .await?)
    }

    async fn config(
        &self,
        node: &cellar::sdk::Node,
        authorization: FleetHostAuthorization,
    ) -> Result<(RaftCellsConfig, RaftCellsServices)> {
        self.config_for(node, authorization, &self.spec).await
    }

    async fn config_for(
        &self,
        node: &cellar::sdk::Node,
        authorization: FleetHostAuthorization,
        spec: &FleetNodeSpec,
    ) -> Result<(RaftCellsConfig, RaftCellsServices)> {
        let listeners = NodeRaftListeners {
            control: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
            data: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
            query: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
        };
        let process = ProcessGeneration::new([spec.node.bytes()[0] + 20; 16])?;
        let peer = RaftPeerNode {
            node: spec.node,
            disk: spec.disk,
            process,
            label: spec.label.clone(),
            server_name: spec.server_name.clone(),
            control_address: listeners.control.local_addr()?,
            data_address: listeners.data.local_addr()?,
            query_address: listeners.query.local_addr()?,
        };
        let mut host = RaftHostOptions {
            max_groups: 8,
            ..Default::default()
        };
        host.node_profile = Some(RaftNodeProfile {
            pool: "native".into(),
            region: "local".into(),
            zone: "shared-host".into(),
            group_capacity: 8,
            byte_capacity: host.images.max_bytes * 8,
            bytes_per_group: host.images.max_bytes,
            leader_capacity: 8,
            draining: false,
        });
        host.node_directory = Some(RaftNodeDirectoryOptions::default());
        host.managed = Some(RaftManagedHostOptions {
            authorization,
            namespaces: vec![self.declaration.clone()],
        });
        Ok((
            RaftCellsConfig {
                infrastructure: NodeRaftConfig {
                    log: ReplicaLogOptions::new(
                        node.info().data_dir.join("raft"),
                        peer.node,
                        peer.disk,
                        process,
                        LogOpenMode::Create,
                    ),
                    consensus: RaftRuntimeOptions {
                        tick_interval_ms: Some(20),
                        ..Default::default()
                    },
                    transport: TransportOptions::default(),
                },
                catalog: CatalogOptions {
                    prefix: "host-enrollment/".into(),
                    ..Default::default()
                },
                cells: vec![],
                peers: vec![peer],
                query_origins: vec![],
                host,
            },
            RaftCellsServices {
                listeners,
                tls: public_node::tls()?,
                store: self.store.clone(),
                application: self.definition.application.clone(),
                versions: public_node::version_keys()?,
            },
        ))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_enrollment_empty_host_local_controls() -> Result<()> {
    let f = Fixture::new().await?;
    let authorization = f.enroll().await?;
    ensure!(
        authorization.state() == FleetNodeState::Registered
            && !authorization.eligibility().receive_replicas
    );
    let root = tempfile::tempdir()?;
    let node =
        public_node::node_named(root.path(), f.definition.application.clone(), &f.spec.label)
            .await?;
    let (config, services) = f.config(&node, authorization.clone()).await?;
    f.store.fail_head_after.store(true, Ordering::Release);
    let host = node.start_raft_cells(config, services).await?;
    let observation = host.observe_node(TIMEOUT).await?;
    ensure!(
        observation.node.identity.node == f.spec.node
            && observation.node.identity.disk == f.spec.disk
    );
    ensure!(observation.node.groups == 0 && observation.node.reserved_bytes == 0);
    ensure!(
        observation.registered_histories == 0
            && observation.dormant_histories == 0
            && observation.unattached_engine_histories == 0
    );
    ensure!(observation.declared_namespaces == 1);
    let enrollment = observation
        .startup_enrollment
        .ok_or_else(|| anyhow::anyhow!("missing enrollment observation"))?;
    ensure!(
        enrollment.revision > authorization.revision()
            && enrollment.physical_host == f.spec.physical_host
            && enrollment.state == FleetNodeState::Registered
            && enrollment.capability_digest == f.spec.capability_digest
    );
    let snapshot = f.catalog.read(TIMEOUT).await?;
    let pinned = f
        .catalog
        .authorize_host(&snapshot, f.spec.node, TIMEOUT)
        .await?;
    ensure!(pinned.storage_instance() == Some(observation.node.storage_instance));
    ensure!(matches!(
        host.controller_status().await,
        Err(CommandFailure::UnsupportedCapability)
    ));
    let held: Vec<_> = (0..16).map(|_| host.observe_node(TIMEOUT)).collect();
    ensure!(matches!(
        host.observe_node(TIMEOUT).await,
        Err(CommandFailure::Overloaded)
    ));
    drop(held);
    ensure!(host
        .cell(f.declaration.namespace.namespace, "Counter:dummy")
        .is_err());
    ensure!(f.calls.load(Ordering::Acquire) == 0);
    let NodeOutcome::Stopped(report) = node.shutdown(ShutdownMode::Handoff).await? else {
        anyhow::bail!("empty host did not stop");
    };
    let raft = report
        .raft
        .ok_or_else(|| anyhow::anyhow!("missing physical shutdown report"))?;
    ensure!(!raft.startup_incomplete && raft.result?.unfinished.is_empty());
    let drain = raft
        .drain
        .ok_or_else(|| anyhow::anyhow!("missing node-level drain result"))??;
    ensure!(drain.groups_examined == 0 && !drain.deadline_elapsed);
    ensure!(matches!(
        host.observe_node(TIMEOUT).await,
        Err(CommandFailure::Unavailable)
    ));
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_enrollment_stale_host_authorization() -> Result<()> {
    let f = Fixture::new().await?;
    let authorization = f.enroll().await?;
    let snapshot = f.catalog.read(TIMEOUT).await?;
    f.catalog
        .commit(
            &snapshot,
            &FleetMutation {
                id: [5; 16],
                operation: [100; 16],
                expected_revision: snapshot.revision(),
                principal: "enrollment-test".into(),
                changes: vec![FleetChange {
                    table: FleetTable::Nodes,
                    key: hex::encode(f.spec.node.bytes()),
                    value: None,
                }],
            },
            TIMEOUT,
        )
        .await?;
    let root = tempfile::tempdir()?;
    let node =
        public_node::node_named(root.path(), f.definition.application.clone(), &f.spec.label)
            .await?;
    let (config, services) = f.config(&node, authorization).await?;
    let error = node
        .start_raft_cells(config, services)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("saved enrollment survived root withdrawal"))?;
    ensure!(
        error
            .downcast_ref::<FleetCatalogError>()
            .is_some_and(|e| matches!(e, FleetCatalogError::Conflict)),
        "unexpected startup refusal: {error:#}"
    );
    node.shutdown(ShutdownMode::Preserve).await?;
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn fleet_enrollment_node_registration_retry() -> Result<()> {
    let f = Fixture::new().await?;
    let before = f.catalog.read(TIMEOUT).await?;
    ensure!(matches!(
        f.catalog
            .authorize_host(&before, f.spec.node, TIMEOUT)
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    f.store.fail_head_after.store(true, Ordering::Release);
    let request = Fixture::request(3, before.revision());
    let first = f
        .catalog
        .register_node(&before, &request, &f.spec, TIMEOUT)
        .await?;
    let later = f
        .catalog
        .commit(
            &first.snapshot,
            &FleetMutation {
                id: [6; 16],
                operation: [100; 16],
                expected_revision: first.snapshot.revision(),
                principal: "enrollment-test".into(),
                changes: vec![FleetChange {
                    table: FleetTable::Operations,
                    key: "unrelated".into(),
                    value: Some(vec![1]),
                }],
            },
            TIMEOUT,
        )
        .await?;
    let retry = f
        .catalog
        .register_node(&later.snapshot, &request, &f.spec, TIMEOUT)
        .await?;
    ensure!(
        retry.applied_revision == first.applied_revision
            && retry.snapshot.revision() == later.snapshot.revision()
    );
    let mut changed = f.spec.clone();
    changed.physical_host = IncarnationId::new([91; 16])?;
    ensure!(matches!(
        f.catalog
            .register_node(&retry.snapshot, &request, &changed, TIMEOUT)
            .await,
        Err(FleetCatalogError::PayloadConflict)
    ));
    changed = f.spec.clone();
    changed.node = NodeId::new([8; 16])?;
    changed.label = "different-node".into();
    ensure!(
        matches!(
            f.catalog
                .register_node(
                    &retry.snapshot,
                    &Fixture::request(7, retry.snapshot.revision()),
                    &changed,
                    TIMEOUT
                )
                .await,
            Err(FleetCatalogError::Conflict)
        ),
        "duplicate disk was accepted"
    );
    ensure!(retry.snapshot.revision() == f.catalog.read(TIMEOUT).await?.revision());
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_enrollment_host_capability_mismatch() -> Result<()> {
    let f = Fixture::new().await?;
    let authorization = f.enroll().await?;
    let root = tempfile::tempdir()?;
    let node =
        public_node::node_named(root.path(), f.definition.application.clone(), &f.spec.label)
            .await?;
    let (mut config, services) = f.config(&node, authorization).await?;
    config.host.managed.as_mut().unwrap().namespaces[0]
        .namespace
        .schema_digest = [99; 32];
    let error = node
        .start_raft_cells(config, services)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("capability mismatch accepted"))?;
    ensure!(
        error
            .to_string()
            .contains("managed host declarations disagree"),
        "unexpected refusal: {error:#}"
    );
    node.shutdown(ShutdownMode::Preserve).await?;
    ensure!(f.calls.load(Ordering::Acquire) == 0);
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_enrollment_empty_engine_resume() -> Result<()> {
    let f = Fixture::new().await?;
    let authorization = f.enroll().await?;
    let root = tempfile::tempdir()?;
    let node =
        public_node::node_named(root.path(), f.definition.application.clone(), &f.spec.label)
            .await?;
    let (config, services) = f.config(&node, authorization.clone()).await?;
    let host = node.start_raft_cells(config, services).await?;
    let original = host.observe_node(TIMEOUT).await?;
    node.shutdown(ShutdownMode::Preserve).await?;
    let node =
        public_node::node_named(root.path(), f.definition.application.clone(), &f.spec.label)
            .await?;
    let (mut config, services) = f.config(&node, authorization).await?;
    let process = ProcessGeneration::new([25; 16])?;
    config.infrastructure.log.mode = LogOpenMode::Resume;
    config.infrastructure.log.process = process;
    config.peers[0].process = process;
    let host = node.start_raft_cells(config, services).await?;
    let resumed = host.observe_node(TIMEOUT).await?;
    ensure!(
        resumed.node.storage_instance == original.node.storage_instance
            && resumed.node.identity.process == process
    );
    ensure!(resumed.node.groups == 0 && resumed.registered_histories == 0);
    ensure!(
        resumed.startup_enrollment.as_ref().unwrap().revision
            == original.startup_enrollment.as_ref().unwrap().revision
    );
    node.shutdown(ShutdownMode::Handoff).await?;
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_enrollment_replacement_engine_refused() -> Result<()> {
    let f = Fixture::new().await?;
    let authorization = f.enroll().await?;
    let root = tempfile::tempdir()?;
    let node =
        public_node::node_named(root.path(), f.definition.application.clone(), &f.spec.label)
            .await?;
    let (config, services) = f.config(&node, authorization.clone()).await?;
    let host = node.start_raft_cells(config, services).await?;
    let original = host.observe_node(TIMEOUT).await?;
    node.shutdown(ShutdownMode::Preserve).await?;
    // Even a separately initialized namespace catalog cannot bypass the fleet
    // pin by claiming the old node/disk IDs on a fresh engine.
    Catalog::new(
        f.store.clone(),
        CatalogOptions {
            prefix: "other-catalog/".into(),
            ..Default::default()
        },
    )?
    .open_namespace(f.declaration.namespace.clone())
    .await?;
    let other_root = tempfile::tempdir()?;
    let node = public_node::node_named(
        other_root.path(),
        f.definition.application.clone(),
        &f.spec.label,
    )
    .await?;
    let (mut config, services) = f.config(&node, authorization).await?;
    config.catalog.prefix = "other-catalog/".into();
    let error = node
        .start_raft_cells(config, services)
        .await
        .err()
        .ok_or_else(|| anyhow::anyhow!("fresh engine replaced pinned storage"))?;
    ensure!(
        error
            .to_string()
            .contains("managed host physical identity mismatch"),
        "unexpected refusal: {error:#}"
    );
    node.shutdown(ShutdownMode::Preserve).await?;
    let snapshot = f.catalog.read(TIMEOUT).await?;
    let pinned = f
        .catalog
        .authorize_host(&snapshot, f.spec.node, TIMEOUT)
        .await?;
    ensure!(pinned.storage_instance() == Some(original.node.storage_instance));
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_enrollment_dynamic_bounds() -> Result<()> {
    let f = Fixture::new().await?;
    let (_root, node, host) = f.empty_host().await?;
    let mut first = None;
    for id in 40..48 {
        let authorization = f.history(id).await?;
        let cell = host
            .register_managed_history(authorization.clone(), TIMEOUT)
            .await?;
        ensure!(cell.scope().cell == format!("Counter:dynamic-{id}"));
        let observation = cell.inspect().await?;
        ensure!(observation.occupancy == ReplicaOccupancy::Standby && observation.node.groups == 0);
        if first.is_none() {
            first = Some((authorization, cell));
        }
    }
    let (authorization, retained) = first.unwrap();
    let retry = host
        .register_managed_history(authorization, TIMEOUT)
        .await?;
    ensure!(
        retry.scope() == retained.scope()
            && retained.inspect().await?.occupancy == ReplicaOccupancy::Standby
    );
    let mut conflicting = retained.scope().clone();
    conflicting.group = GroupId::new([89; 16])?;
    let binding = f.catalog.group_binding(&conflicting)?;
    let snapshot = f.catalog.read(TIMEOUT).await?;
    let committed = f
        .catalog
        .commit(
            &snapshot,
            &FleetMutation {
                id: [89; 16],
                operation: [100; 16],
                principal: "enrollment-test".into(),
                expected_revision: snapshot.revision(),
                changes: vec![FleetChange {
                    table: FleetTable::Histories,
                    key: binding.key()?,
                    value: Some(f.inconsistent_history(&conflicting, 89).await?),
                }],
            },
            TIMEOUT,
        )
        .await?;
    let conflicting = f
        .catalog
        .authorize_group(&committed.snapshot, &conflicting, TIMEOUT)
        .await?;
    ensure!(matches!(
        host.register_managed_history(conflicting, TIMEOUT).await,
        Err(CommandFailure::CommandConflict)
    ));
    ensure!(
        retry.scope() == retained.scope()
            && retained.inspect().await?.occupancy == ReplicaOccupancy::Standby
    );
    let extra = f.history(48).await?;
    ensure!(matches!(
        host.register_managed_history(extra, TIMEOUT).await,
        Err(CommandFailure::Overloaded)
    ));
    let observation = host.observe_node(TIMEOUT).await?;
    ensure!(
        observation.registered_histories == 8
            && observation.dormant_histories == 8
            && observation.pending_history_registrations == 0
            && observation.node.groups == 0
    );
    ensure!(f.calls.load(Ordering::Acquire) == 0);
    node.shutdown(ShutdownMode::Handoff).await?;
    ensure!(matches!(
        retained.inspect().await,
        Err(CommandFailure::Unavailable)
    ));
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_enrollment_dynamic_cancelled_read() -> Result<()> {
    let f = Fixture::new().await?;
    let (_root, node, host) = f.empty_host().await?;
    let authorization = f.history(40).await?;
    f.store.birth_hang.store(true, Ordering::Release);
    f.store
        .birth_confirmation_pending
        .store(true, Ordering::Release);
    let pending = host.register_managed_history(authorization.clone(), TIMEOUT);
    f.birth_waiting().await?;
    ensure!(
        host.observe_node(TIMEOUT)
            .await?
            .pending_history_registrations
            == 1
    );
    ensure!(matches!(
        host.register_managed_history(authorization.clone(), TIMEOUT)
            .await,
        Err(CommandFailure::Overloaded)
    ));
    drop(pending);
    f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            while host
                .observe_node(TIMEOUT)
                .await?
                .pending_history_registrations
                != 0
            {
                cellar::asyncrt::sleep(Duration::from_millis(5)).await;
            }
            Ok::<_, CommandFailure>(())
        }))
        .await???;
    ensure!(!f.store.birth_waiting.load(Ordering::Acquire));
    let observation = host.observe_node(TIMEOUT).await?;
    ensure!(observation.registered_histories == 0 && observation.node.groups == 0);
    ensure!(host
        .cell(f.declaration.namespace.namespace, "Counter:dynamic-40")
        .is_err());
    f.store.birth_hang.store(false, Ordering::Release);
    f.store.birth_resume.notify_waiters();
    let cell = host
        .register_managed_history(authorization, TIMEOUT)
        .await?;
    ensure!(cell.inspect().await?.occupancy == ReplicaOccupancy::Standby);
    node.shutdown(ShutdownMode::Preserve).await?;
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_enrollment_dynamic_birth_digest() -> Result<()> {
    let f = Fixture::new().await?;
    let (_root, node, host) = f.empty_host().await?;
    let authorization = f.history(40).await?;
    let scope = GroupScope {
        cluster: f.declaration.namespace.cluster,
        namespace: f.declaration.namespace.namespace,
        cell: "Counter:dynamic-40".into(),
        group: GroupId::new([40; 16])?,
        incarnation: IncarnationId::new([3; 16])?,
    };
    let binding = f.catalog.group_binding(&scope)?;
    let root = f.catalog.read(TIMEOUT).await?;
    let bytes = f
        .catalog
        .get(&root, FleetTable::Histories, &binding.key()?, TIMEOUT)
        .await?
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&bytes[13..bytes.len() - 32])?;
    let payload = std::str::from_utf8(&bytes[13..bytes.len() - 32])?.replace(
        &format!("\"birth_digest\":{}", value["birth_digest"]),
        &format!("\"birth_digest\":{}", serde_json::json!(vec![91; 32])),
    );
    use sha2::{Digest, Sha256};
    let mut updated = bytes[..9].to_vec();
    updated.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    updated.extend_from_slice(payload.as_bytes());
    updated.extend_from_slice(&Sha256::digest(&updated));
    f.catalog
        .commit(
            &root,
            &FleetMutation {
                id: [90; 16],
                operation: [100; 16],
                principal: "enrollment-test".into(),
                expected_revision: root.revision(),
                changes: vec![FleetChange {
                    table: FleetTable::Histories,
                    key: binding.key()?,
                    value: Some(updated),
                }],
            },
            TIMEOUT,
        )
        .await?;
    ensure!(matches!(
        host.register_managed_history(authorization, TIMEOUT).await,
        Err(CommandFailure::CommandConflict)
    ));
    ensure!(host.observe_node(TIMEOUT).await?.registered_histories == 0);
    node.shutdown(ShutdownMode::Preserve).await?;
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_enrollment_dynamic_stale_history() -> Result<()> {
    let f = Fixture::new().await?;
    let (_root, node, host) = f.empty_host().await?;
    let authorization = f.history(40).await?;
    let scope = GroupScope {
        cluster: f.declaration.namespace.cluster,
        namespace: f.declaration.namespace.namespace,
        cell: "Counter:dynamic-40".into(),
        group: GroupId::new([40; 16])?,
        incarnation: IncarnationId::new([3; 16])?,
    };
    let snapshot = f.catalog.read(TIMEOUT).await?;
    let binding = f.catalog.group_binding(&scope)?;
    f.catalog
        .commit(
            &snapshot,
            &FleetMutation {
                id: [90; 16],
                operation: [100; 16],
                principal: "enrollment-test".into(),
                expected_revision: snapshot.revision(),
                changes: vec![FleetChange {
                    table: FleetTable::Histories,
                    key: binding.key()?,
                    value: None,
                }],
            },
            TIMEOUT,
        )
        .await?;
    ensure!(matches!(
        host.register_managed_history(authorization, TIMEOUT).await,
        Err(CommandFailure::CommandConflict)
    ));
    ensure!(host.observe_node(TIMEOUT).await?.registered_histories == 0);
    node.shutdown(ShutdownMode::Preserve).await?;
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_enrollment_dynamic_close_joins_read() -> Result<()> {
    let f = Fixture::new().await?;
    let (_root, node, host) = f.empty_host().await?;
    let authorization = f.history(40).await?;
    f.store.birth_hang.store(true, Ordering::Release);
    f.store
        .birth_confirmation_pending
        .store(true, Ordering::Release);
    let pending = host.register_managed_history(authorization, TIMEOUT);
    f.birth_waiting().await?;
    ensure!(host.resource_usage().control.bytes >= 4 * 1024 * 1024);
    let report = f
        .domain
        .run(cellar::asyncrt::timeout(TIMEOUT, host.close()))
        .await???;
    ensure!(report.unfinished.is_empty() && !f.store.birth_waiting.load(Ordering::Acquire));
    // The physical read is joined; the unpolled waiter still owns its 4 KiB
    // request reservation until the caller consumes or drops it.
    ensure!(host.resource_usage().control.bytes == 4096);
    ensure!(matches!(pending.await, Err(CommandFailure::Unavailable)));
    ensure!(host.resource_usage().control.bytes == 0);
    node.shutdown(ShutdownMode::Preserve).await?;
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_enrollment_dynamic_undeclared_class() -> Result<()> {
    let f = Fixture::new().await?;
    let (_root, node, host) = f.empty_host().await?;
    let scope = GroupScope {
        cluster: f.declaration.namespace.cluster,
        namespace: f.declaration.namespace.namespace,
        cell: "Other:unexpected".into(),
        group: GroupId::new([42; 16])?,
        incarnation: IncarnationId::new([3; 16])?,
    };
    let binding = f.catalog.group_binding(&scope)?;
    let snapshot = f.catalog.read(TIMEOUT).await?;
    let committed = f
        .catalog
        .commit(
            &snapshot,
            &FleetMutation {
                id: [90; 16],
                operation: [100; 16],
                principal: "enrollment-test".into(),
                expected_revision: snapshot.revision(),
                changes: vec![FleetChange {
                    table: FleetTable::Histories,
                    key: binding.key()?,
                    value: Some(f.inconsistent_history(&scope, 90).await?),
                }],
            },
            TIMEOUT,
        )
        .await?;
    let authorization = f
        .catalog
        .authorize_group(&committed.snapshot, &scope, TIMEOUT)
        .await?;
    ensure!(matches!(
        host.register_managed_history(authorization, TIMEOUT).await,
        Err(CommandFailure::UnsupportedCapability)
    ));
    ensure!(
        host.observe_node(TIMEOUT).await?.registered_histories == 0
            && f.calls.load(Ordering::Acquire) == 0
    );
    node.shutdown(ShutdownMode::Preserve).await?;
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_enrollment_dynamic_occupied_slot() -> Result<()> {
    let f = Fixture::new().await?;
    let authorization = f.enroll().await?;
    let history = f.history(40).await?;
    let root = tempfile::tempdir()?;
    let node =
        public_node::node_named(root.path(), f.definition.application.clone(), &f.spec.label)
            .await?;
    let (mut config, services) = f.config(&node, authorization).await?;
    let log = f
        .domain
        .run(ReplicaLog::open(
            config.infrastructure.log.clone(),
            Arc::new(cellar_ltx::DirectFileSystem),
        ))
        .await??;
    let scope = GroupScope {
        cluster: f.declaration.namespace.cluster,
        namespace: f.declaration.namespace.namespace,
        cell: "Counter:dynamic-40".into(),
        group: GroupId::new([40; 16])?,
        incarnation: IncarnationId::new([3; 16])?,
    };
    f.domain
        .run(log.create_group(GroupBootstrap {
            scope,
            member: MemberId::new(1)?,
            birth_digest: [9; 32],
            voters: (1..=3).map(MemberId::new).collect::<Result<Vec<_>, _>>()?,
            learners: vec![],
        }))
        .await??;
    log.close().await?;
    config.infrastructure.log.mode = LogOpenMode::Resume;
    let host = node.start_raft_cells(config, services).await?;
    let before = host.observe_node(TIMEOUT).await?;
    ensure!(
        before.node.groups == 1
            && before.unattached_engine_histories == 1
            && before.registered_histories == 0
    );
    ensure!(matches!(
        host.register_managed_history(history, TIMEOUT).await,
        Err(CommandFailure::CommandConflict)
    ));
    let after = host.observe_node(TIMEOUT).await?;
    ensure!(
        after.node.groups == 1
            && after.unattached_engine_histories == 1
            && after.registered_histories == 0
    );
    node.shutdown(ShutdownMode::Preserve).await?;
    f.domain.shutdown().await;
    Ok(())
}
