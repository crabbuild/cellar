//! Local empty-host controls are a sub-boundary; remote node readiness and
//! complete managed inventory remain required before FO-03 can finish.
use super::{fault, public_node};
use anyhow::{ensure, Result};
use cellar::application::consensus::verification::{
    Catalog, FleetCatalog, FleetCatalogError, FleetChange, FleetMutation, FleetTable,
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
}

impl Fixture {
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
        namespace
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
        let binding = FleetGroupBinding::new(IncarnationId::new([80; 16])?, &scope)?;
        let snapshot = self.catalog.read(TIMEOUT).await?;
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
                        table: FleetTable::Histories,
                        key: binding.key()?,
                        value: Some(binding.encode()?),
                    }],
                },
                TIMEOUT,
            )
            .await?;
        Ok(self
            .catalog
            .authorize_group(&commit.snapshot, &scope, TIMEOUT)
            .await?)
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
        let declaration = RaftNamespaceRegistration {
            namespace: NamespaceConfig::new(
                ClusterId::new([1; 16])?,
                NamespaceId::new([2; 16])?,
                NamespaceMode::Raft,
                definition.schema,
            ),
            class: "Counter".into(),
        };
        Catalog::new(
            store.clone(),
            CatalogOptions {
                prefix: "host-enrollment/".into(),
                ..Default::default()
            },
        )?
        .open_namespace(declaration.namespace.clone())
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
        let listeners = NodeRaftListeners {
            control: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
            data: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
            query: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
        };
        let process = ProcessGeneration::new([24; 16])?;
        let peer = RaftPeerNode {
            node: self.spec.node,
            disk: self.spec.disk,
            process,
            label: self.spec.label.clone(),
            server_name: self.spec.server_name.clone(),
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
    let binding = FleetGroupBinding::new(IncarnationId::new([80; 16])?, &conflicting)?;
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
                    value: Some(binding.encode()?),
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
    let binding = FleetGroupBinding::new(IncarnationId::new([80; 16])?, &scope)?;
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
    let binding = FleetGroupBinding::new(IncarnationId::new([80; 16])?, &scope)?;
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
                    value: Some(binding.encode()?),
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
