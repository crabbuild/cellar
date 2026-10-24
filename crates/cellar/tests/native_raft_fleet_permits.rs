//! Managed permits through native SQLite hosts, real TLS links and retained
//! engines. Catalog edits here stand in for the later typed fleet controller.
#[path = "support/raft_cluster.rs"]
#[allow(dead_code)]
mod cluster;
#[path = "support/raft_fleet_observation.rs"]
mod observation;
#[path = "support/raft_node.rs"]
#[allow(dead_code)]
mod public_node;

use anyhow::{ensure, Context as _, Result};
use cellar::application::consensus::verification::{
    Catalog, CatalogOptions, FleetCatalog, FleetCatalogError, FleetChange, FleetMutation,
    FleetTable,
};
use cellar::bucket::{Bucket, StorageBackend};
use cellar::sdk::{consensus::*, ExecutionDomain};
use object_store::{list::*, ObjectStore};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

const TIMEOUT: Duration = Duration::from_secs(10);

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
            source: "permit authority never lists objects".into(),
        })
    }
}

struct Fixture {
    cluster: cluster::Cluster,
    definitions: Vec<public_node::Counter>,
    calls: Vec<Arc<AtomicUsize>>,
    handles: Vec<RaftCellHandle>,
    registration: RaftCellRegistration,
    primary: usize,
    domain: ExecutionDomain,
    catalog: FleetCatalog,
    binding: FleetGroupBinding,
    _entered: tokio::sync::mpsc::UnboundedReceiver<()>,
}

async fn leader(domain: &ExecutionDomain, cells: &[RaftCellHandle]) -> Result<usize> {
    Ok(domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            loop {
                for (index, cell) in cells.iter().enumerate() {
                    if cell.status().await.is_ok_and(|status| status.command_ready) {
                        return index;
                    }
                }
                cellar::asyncrt::sleep(Duration::from_millis(10)).await;
            }
        }))
        .await??)
}

impl Fixture {
    async fn new() -> Result<Self> {
        Self::create(true, true).await
    }

    async fn create(write_image: bool, enroll: bool) -> Result<Self> {
        let (gate, entered) = public_node::Gate::new();
        gate.release();
        let calls: Vec<_> = (0..4).map(|_| Arc::new(AtomicUsize::new(0))).collect();
        let definitions = calls
            .iter()
            .map(|n| public_node::counter(n.clone(), gate.clone()))
            .collect::<Result<Vec<_>>>()?;
        let scope = GroupScope {
            cluster: ClusterId::new([1; 16])?,
            namespace: NamespaceId::new([2; 16])?,
            cell: "Counter:fleet".into(),
            group: GroupId::new([55; 16])?,
            incarnation: IncarnationId::new([3; 16])?,
        };
        let registration = RaftCellRegistration {
            placement: RaftCellPlacement::Replica,
            namespace: NamespaceConfig::new(
                scope.cluster,
                scope.namespace,
                NamespaceMode::Raft,
                definitions[0].schema,
            ),
            class: "Counter".into(),
            birth: BirthProposal {
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
            },
        };
        let cluster =
            cluster::Cluster::start_with_standby(&definitions, vec![registration.clone()]).await?;
        let handles = cluster
            .services
            .iter()
            .map(|s| s.cell(scope.namespace, &scope.cell))
            .collect::<Result<Vec<_>, _>>()?;
        let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
        let primary = leader(&domain, &handles).await?;
        let catalog = Self::open_catalog(&domain, cluster.store.clone())?;
        catalog.initialize(TIMEOUT).await?;
        let binding = FleetGroupBinding::new(IncarnationId::new([80; 16])?, &scope)?;
        let f = Self {
            cluster,
            definitions,
            calls,
            handles,
            registration,
            primary,
            domain,
            catalog,
            binding,
            _entered: entered,
        };
        let namespace = Catalog::new(
            f.cluster.store.clone(),
            CatalogOptions {
                prefix: "partition/test".into(),
                ..Default::default()
            },
        )?
        .existing_namespace(f.registration.namespace.clone())
        .await?;
        let birth = namespace
            .birth(&scope.cell)
            .await?
            .context("selected birth")?;
        let snapshot = f.catalog.read(TIMEOUT).await?;
        f.catalog
            .enroll_history(
                &snapshot,
                &FleetRequestIdentity {
                    id: [10; 16],
                    operation: [99; 16],
                    principal: "permit-test".into(),
                    expected_revision: snapshot.revision(),
                },
                &namespace,
                &birth,
                TIMEOUT,
            )
            .await?;
        // Establish a real SQL image before metadata-only enrollment changes.
        if write_image {
            f.write(1, 5, true).await?;
        }
        let root = f.catalog.read(TIMEOUT).await?;
        let authorization = f.catalog.authorize_group(&root, &scope, TIMEOUT).await?;
        let expected = f.handles[primary].status().await?.membership_revision;
        if enroll {
            let enrolled = f.handles[primary]
                .enroll_fleet(authorization, expected, TIMEOUT)
                .await?;
            ensure!(
                enrolled.binding == f.binding
                    && enrolled.consumed_sequence == 0
                    && enrolled.permit.is_none()
            );
        }
        Ok(f)
    }

    async fn change(&self, id: u8, table: FleetTable, value: Option<Vec<u8>>) -> Result<()> {
        let root = self.catalog.read(TIMEOUT).await?;
        self.catalog
            .commit(
                &root,
                &FleetMutation {
                    id: [id; 16],
                    operation: [99; 16],
                    expected_revision: root.revision(),
                    principal: "native-test-controller".into(),
                    changes: vec![FleetChange {
                        table,
                        key: self.binding.key()?,
                        value,
                    }],
                },
                TIMEOUT,
            )
            .await?;
        Ok(())
    }

    async fn plan(&self, id: u8) -> Result<RepairPlan> {
        Ok(RepairPlan {
            id: ProposalId::new([id; 16])?,
            policy_revision: 1,
            expected_revision: self.handles[self.primary]
                .status()
                .await?
                .membership_revision,
            obsolete: MemberId::new(((self.primary + 1) % 3) as u64 + 1)?,
            replacement: JoiningMember {
                member: MemberId::new(4)?,
                node: NodeId::new([4; 16])?,
                disk: DiskLineage::new([13; 16])?,
            },
        })
    }

    async fn reserve(
        &self,
        sequence: u64,
        id: u8,
        plan: &RepairPlan,
    ) -> Result<FleetPermitAuthorization> {
        let permit = FleetMembershipPermit::new(
            self.binding.clone(),
            sequence,
            ProposalId::new([id; 16])?,
            ProposalId::new([99; 16])?,
            plan,
        )?;
        self.change(id, FleetTable::Admissions, Some(permit.encode()?))
            .await?;
        let root = self.catalog.read(TIMEOUT).await?;
        Ok(self
            .catalog
            .authorize_membership(&root, &self.registration.birth.scope, TIMEOUT)
            .await?)
    }

    async fn write(&self, id: u8, value: i64, s3: bool) -> Result<CommandReceipt> {
        let mut options = public_node::options(u64::from(id))?;
        if s3 {
            options.durability = Durability::S3;
        }
        Ok(self.handles[self.primary]
            .command(
                self.definitions[self.primary].increment.clone(),
                value.to_be_bytes().to_vec(),
                options,
            )
            .await?)
    }

    async fn strong(&self) -> Result<i64> {
        let read = self.handles[self.primary]
            .query(
                self.definitions[self.primary].get.clone(),
                vec![],
                QueryOptions {
                    consistency: QueryConsistency::Linearizable,
                    ..Default::default()
                },
            )
            .await?;
        Ok(i64::from_be_bytes(read.result.as_slice().try_into()?))
    }

    async fn step(
        &self,
        authorization: &FleetPermitAuthorization,
        step: RepairStep,
    ) -> Result<RepairStatus> {
        self.domain
            .run(cellar::asyncrt::timeout(TIMEOUT, async {
                loop {
                    let expected = self.handles[self.primary]
                        .status()
                        .await?
                        .membership_revision;
                    match self.handles[self.primary]
                        .advance_managed_repair(
                            authorization.clone(),
                            step,
                            expected,
                            Duration::from_secs(2),
                        )
                        .await
                    {
                        Ok(status) => return Ok(status),
                        Err(CommandFailure::Unavailable | CommandFailure::Overloaded) => {
                            cellar::asyncrt::sleep(Duration::from_millis(10)).await
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
            }))
            .await?
            .context("managed repair step timed out")?
    }

    fn activation(&self, from_s3: bool) -> Result<LearnerActivation> {
        Ok(LearnerActivation {
            member: MemberId::new(4)?,
            source: MemberId::new(self.primary as u64 + 1)?,
            source_node: NodeId::new([self.primary as u8 + 1; 16])?,
            from_s3,
            timeout: TIMEOUT,
        })
    }

    async fn standby(&mut self) -> Result<RaftCellHandle> {
        let service = self
            .cluster
            .join_standby(
                self.definitions[3].application.clone(),
                RaftCellPlacement::Standby,
            )
            .await?;
        let cell = service.cell(
            self.registration.namespace.namespace,
            &self.registration.birth.scope.cell,
        )?;
        self.handles.push(cell.clone());
        Ok(cell)
    }

    async fn observed(
        &self,
        cell: &RaftCellHandle,
        sequence: u64,
        phase: FleetPermitPhase,
    ) -> Result<FleetMembershipStatus> {
        self.domain
            .run(cellar::asyncrt::timeout(TIMEOUT, async {
                loop {
                    if let Some(status) = cell.fleet_membership_status().await? {
                        if status.consumed_sequence == sequence && status.phase == Some(phase) {
                            return Ok(status);
                        }
                    }
                    cellar::asyncrt::sleep(Duration::from_millis(10)).await;
                }
            }))
            .await?
            .context("expected durable fleet phase not observed")?
    }

    fn open_catalog(domain: &ExecutionDomain, store: Arc<dyn ObjectStore>) -> Result<FleetCatalog> {
        let bucket = Bucket::from_clients(
            store.clone(),
            store,
            Arc::new(NoListing),
            StorageBackend::S3,
            "fleet-permits".into(),
            "".into(),
        )?;
        Ok(FleetCatalog::new(
            domain,
            bucket,
            ClusterId::new([1; 16])?,
            IncarnationId::new([80; 16])?,
            "coordination/",
        )?)
    }

    async fn restart(mut self) -> Result<Self> {
        ensure!(
            self.handles.len() == 4,
            "all retained engines must exist before Resume"
        );
        self.cluster = self
            .cluster
            .restart(
                &self.definitions,
                vec![self.registration.clone()],
                vec![
                    RaftCellPlacement::Replica,
                    RaftCellPlacement::Replica,
                    RaftCellPlacement::Replica,
                    RaftCellPlacement::Standby,
                ],
                Box::new(|_| Box::pin(async { Ok(()) })),
            )
            .await?;
        self.handles = self
            .cluster
            .services
            .iter()
            .map(|s| {
                s.cell(
                    self.registration.namespace.namespace,
                    &self.registration.birth.scope.cell,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.primary = leader(&self.domain, &self.handles).await?;
        self.catalog = Self::open_catalog(&self.domain, self.cluster.store.clone())?;
        Ok(self)
    }

    async fn close(self) -> Result<()> {
        self.cluster.close().await?;
        self.domain.shutdown().await;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_permit_delayed_after_cancel() -> Result<()> {
    let f = Fixture::new().await?;
    let plan = f.plan(40).await?;
    let cached = f.reserve(1, 20, &plan).await?;
    let status = f.handles[f.primary]
        .fence_fleet_permit(cached.clone(), TIMEOUT)
        .await?;
    ensure!(status.consumed_sequence == 1 && status.phase == Some(FleetPermitPhase::Fenced));
    // This quorum read is separate from local status and precedes the simulated
    // controller release. FO-06 owns typed terminal proof/root reconciliation.
    assert_eq!(f.strong().await?, 5);
    f.change(21, FleetTable::Admissions, None).await?;
    let root = f.catalog.read(TIMEOUT).await?;
    ensure!(matches!(
        f.catalog
            .authorize_membership(&root, &f.registration.birth.scope, TIMEOUT)
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    ensure!(matches!(
        f.handles[f.primary]
            .begin_managed_repair(cached.clone(), plan, TIMEOUT)
            .await,
        Err(CommandFailure::CommandConflict)
    ));
    ensure!(f.handles[f.primary].repair_status().await?.is_none());
    assert_eq!(f.handles[f.primary].status().await?.member_high_water, 3);
    assert_eq!(
        f.handles[f.primary]
            .fence_fleet_permit(cached.clone(), TIMEOUT)
            .await?,
        status
    );
    let next = f.plan(41).await?;
    let authorization = f.reserve(2, 22, &next).await?;
    let repair = f.handles[f.primary]
        .begin_managed_repair(authorization.clone(), next, TIMEOUT)
        .await?;
    ensure!(repair.phase == RepairPhase::Seeding);
    ensure!(f.handles[f.primary]
        .fence_fleet_permit(cached, TIMEOUT)
        .await
        .is_err());
    assert_eq!(
        f.step(&authorization, RepairStep::AbortLearner)
            .await?
            .phase,
        RepairPhase::Aborted
    );
    f.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_permit_crash_recovery() -> Result<()> {
    let mut f = Fixture::new().await?;
    // An empty registered host has an actual intact engine to resume; no
    // learner or SQL image is initialized by this step.
    f.standby().await?;
    let plan = f.plan(40).await?;
    f.reserve(1, 20, &plan).await?;
    let calls: Vec<_> = f.calls.iter().map(|n| n.load(Ordering::SeqCst)).collect();
    f = f.restart().await.context("restart after reservation")?;
    assert_eq!(
        f.handles[f.primary]
            .fleet_membership_status()
            .await?
            .unwrap()
            .consumed_sequence,
        0
    );
    let root = f.catalog.read(TIMEOUT).await?;
    let authorization = f
        .catalog
        .authorize_membership(&root, &f.registration.birth.scope, TIMEOUT)
        .await?;
    // An unpolled returned future still represents a submitted configuration.
    let lost =
        f.handles[f.primary].begin_managed_repair(authorization.clone(), plan.clone(), TIMEOUT);
    let consumed = f
        .observed(&f.handles[f.primary], 1, FleetPermitPhase::Seeding)
        .await?;
    drop(lost);
    f = f
        .restart()
        .await
        .context("restart after committed plan/lost response")?;
    let restored = f.handles[f.primary]
        .fleet_membership_status()
        .await?
        .context("fleet guard lost on engine reopen")?;
    assert_eq!(restored, consumed);
    let resumed = f.handles[f.primary]
        .begin_managed_repair(authorization.clone(), plan.clone(), TIMEOUT)
        .await?;
    ensure!(resumed.phase == RepairPhase::Seeding && resumed.changed_at == consumed.changed_at);
    assert_eq!(
        f.calls
            .iter()
            .map(|n| n.load(Ordering::SeqCst))
            .collect::<Vec<_>>(),
        calls
    );
    let terminal = f.step(&authorization, RepairStep::AbortLearner).await?;
    assert_eq!(terminal.phase, RepairPhase::Aborted);
    assert_eq!(f.strong().await?, 5);
    f.change(21, FleetTable::Admissions, None).await?;
    f = f
        .restart()
        .await
        .context("restart after terminal receipt/release")?;
    assert_eq!(
        f.handles[f.primary]
            .begin_managed_repair(authorization, plan, TIMEOUT)
            .await?,
        terminal
    );
    let root = f.catalog.read(TIMEOUT).await?;
    ensure!(matches!(
        f.catalog
            .authorize_membership(&root, &f.registration.birth.scope, TIMEOUT)
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    assert_eq!(
        f.calls
            .iter()
            .map(|n| n.load(Ordering::SeqCst))
            .collect::<Vec<_>>(),
        calls
    );
    f.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_permit_joint_cancel() -> Result<()> {
    let mut f = Fixture::new().await?;
    let plan = f.plan(40).await?;
    let authorization = f.reserve(1, 20, &plan).await?;
    f.handles[f.primary]
        .begin_managed_repair(authorization.clone(), plan.clone(), TIMEOUT)
        .await?;
    let target = f.standby().await?;
    target
        .activate_managed_learner(authorization.clone(), f.activation(false)?)
        .await?;
    assert_eq!(
        f.step(&authorization, RepairStep::PromoteAndRemove)
            .await?
            .phase,
        RepairPhase::Joint
    );
    let joint_guard = f.observed(&target, 1, FleetPermitPhase::Joint).await?;
    ensure!(f.handles[f.primary]
        .fence_fleet_permit(authorization.clone(), TIMEOUT)
        .await
        .is_err());
    let expected = f.handles[f.primary].status().await?.membership_revision;
    ensure!(f.handles[f.primary]
        .advance_managed_repair(
            authorization.clone(),
            RepairStep::AbortLearner,
            expected,
            TIMEOUT
        )
        .await
        .is_err());
    // Preserve only the leader and incoming target: new majority exists, old
    // majority does not. Neither a cancellation nor finish may invent it.
    for (index, service) in f.cluster.services[..3].iter().enumerate() {
        if index != f.primary {
            ensure!(service.close().await?.unfinished.is_empty());
        }
    }
    ensure!(f.handles[f.primary]
        .advance_managed_repair(
            authorization.clone(),
            RepairStep::FinishJoint,
            expected,
            Duration::from_millis(500)
        )
        .await
        .is_err());
    assert_eq!(
        f.handles[f.primary]
            .fleet_membership_status()
            .await?
            .unwrap()
            .phase,
        Some(FleetPermitPhase::Joint)
    );
    let root = f.catalog.read(TIMEOUT).await?;
    ensure!(f
        .catalog
        .get(&root, FleetTable::Admissions, &f.binding.key()?, TIMEOUT)
        .await?
        .is_some());
    let ns = f.registration.namespace.namespace;
    let name = f.registration.birth.scope.cell.clone();
    let get = f.definitions[3].get.clone();
    let group_authorization = f
        .catalog
        .authorize_group(&root, &f.registration.birth.scope, TIMEOUT)
        .await?;
    for cell in [&f.handles[f.primary], &target] {
        ensure!(
            cell.observe_fleet_membership(group_authorization.clone(), Duration::from_millis(500),)
                .await
                .is_err(),
            "a new majority alone cannot authorize a joint observation"
        );
    }
    let reopen_authorization = group_authorization.clone();
    f.cluster = f
        .cluster
        .restart(
            &f.definitions,
            vec![f.registration.clone()],
            vec![
                RaftCellPlacement::Replica,
                RaftCellPlacement::Replica,
                RaftCellPlacement::Replica,
                RaftCellPlacement::Standby,
            ],
            Box::new(move |service| {
                Box::pin(async move {
                    let cell = service.cell(ns, &name)?;
                    assert_eq!(cell.fleet_membership_status().await?, Some(joint_guard));
                    let read = cell
                        .query(
                            get,
                            vec![],
                            QueryOptions {
                                consistency: QueryConsistency::Linearizable,
                                timeout: Duration::from_millis(200),
                                ..Default::default()
                            },
                        )
                        .await;
                    ensure!(
                        matches!(read, Err(CommandFailure::VersionUnavailable)),
                        "retained joint target bypassed the missing old majority: {read:?}"
                    );
                    ensure!(
                        cell.observe_fleet_membership(
                            reopen_authorization,
                            Duration::from_millis(200),
                        )
                        .await
                        .is_err(),
                        "reopened joint metadata bypassed the missing old majority"
                    );
                    Ok(())
                })
            }),
        )
        .await?;
    f.handles = f
        .cluster
        .services
        .iter()
        .map(|s| {
            s.cell(
                f.registration.namespace.namespace,
                &f.registration.birth.scope.cell,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    f.primary = leader(&f.domain, &f.handles).await?;
    assert_eq!(
        f.step(&authorization, RepairStep::FinishJoint).await?.phase,
        RepairPhase::Complete
    );
    let terminal = f
        .observed(&f.handles[f.primary], 1, FleetPermitPhase::Complete)
        .await?;
    ensure!(
        terminal.permit
            == Some(FleetMembershipPermit::new(
                f.binding.clone(),
                1,
                ProposalId::new([20; 16])?,
                ProposalId::new([99; 16])?,
                &plan
            )?)
    );
    let fresh = f.handles[f.primary]
        .observe_fleet_membership(group_authorization, TIMEOUT)
        .await?;
    assert_eq!(fresh.status(), &terminal);
    ensure!(fresh.configuration().outgoing_voters().is_empty());
    ensure!(fresh.applied().index >= fresh.barrier());
    assert_eq!(f.strong().await?, 5);
    f.change(21, FleetTable::Admissions, None).await?;
    f.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_permit_snapshot_roundtrip() -> Result<()> {
    for from_s3 in [false, true] {
        let mut f = Fixture::new().await?;
        let plan = f.plan(40).await?;
        let authorization = f.reserve(1, 20, &plan).await?;
        f.handles[f.primary]
            .begin_managed_repair(authorization.clone(), plan, TIMEOUT)
            .await?;
        let receipt = f.write(2, 1, true).await?;
        let before = f.handles[f.primary].fleet_membership_status().await?;
        let target = f.standby().await?;
        target
            .activate_managed_learner(authorization.clone(), f.activation(from_s3)?)
            .await?;
        let read = target
            .query(
                f.definitions[3].get.clone(),
                vec![],
                QueryOptions {
                    consistency: QueryConsistency::AtLeast(receipt.version_token),
                    ..Default::default()
                },
            )
            .await?;
        assert_eq!(read.result, 6_i64.to_be_bytes());
        assert_eq!(target.fleet_membership_status().await?, before);
        assert_eq!(
            f.calls[3].load(Ordering::SeqCst),
            0,
            "snapshot recovery reran application commands"
        );
        ensure!(target
            .activate_learner(f.activation(from_s3)?)
            .await
            .is_err());
        assert_eq!(
            f.step(&authorization, RepairStep::PromoteAndRemove)
                .await?
                .phase,
            RepairPhase::Joint
        );
        assert_eq!(
            f.step(&authorization, RepairStep::FinishJoint).await?.phase,
            RepairPhase::Complete
        );
        f.observed(&target, 1, FleetPermitPhase::Complete).await?;
        target
            .activate_managed_learner(authorization.clone(), f.activation(from_s3)?)
            .await?;
        ensure!(f.handles[f.primary]
            .fence_fleet_permit(authorization, TIMEOUT)
            .await
            .is_err());
        f.close().await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_permit_quorumless_fence() -> Result<()> {
    let f = Fixture::new().await?;
    let plan = f.plan(40).await?;
    let authorization = f.reserve(1, 20, &plan).await?;
    for (index, service) in f.cluster.services.iter().enumerate() {
        if index != f.primary {
            ensure!(service.close().await?.unfinished.is_empty());
        }
    }
    ensure!(f.handles[f.primary]
        .fence_fleet_permit(authorization.clone(), Duration::from_millis(500))
        .await
        .is_err());
    let status = f.handles[f.primary]
        .fleet_membership_status()
        .await?
        .unwrap();
    assert_eq!(status.consumed_sequence, 0);
    assert!(status.permit.is_none());
    let root = f.catalog.read(TIMEOUT).await?;
    ensure!(f
        .catalog
        .get(&root, FleetTable::Admissions, &f.binding.key()?, TIMEOUT)
        .await?
        .is_some());
    ensure!(f.handles[f.primary]
        .begin_managed_repair(authorization, plan, Duration::from_millis(500))
        .await
        .is_err());
    f.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_permit_no_manual_bypass() -> Result<()> {
    let mut f = Fixture::new().await?;
    let plan = f.plan(40).await?;
    let authorization = f.reserve(1, 20, &plan).await?;
    ensure!(matches!(
        f.handles[f.primary]
            .begin_repair(plan.clone(), TIMEOUT)
            .await,
        Err(CommandFailure::UnsupportedCapability)
    ));
    ensure!(matches!(
        f.handles[f.primary]
            .add_learner(
                plan.replacement.clone(),
                LearnerOptions {
                    expected_revision: plan.expected_revision,
                    timeout: TIMEOUT
                }
            )
            .await,
        Err(CommandFailure::UnsupportedCapability)
    ));
    let mut changed = plan.clone();
    changed.policy_revision += 1;
    ensure!(f.handles[f.primary]
        .begin_managed_repair(authorization.clone(), changed, TIMEOUT)
        .await
        .is_err());
    f.handles[f.primary]
        .begin_managed_repair(authorization.clone(), plan.clone(), TIMEOUT)
        .await?;
    let expected = f.handles[f.primary].status().await?.membership_revision;
    ensure!(matches!(
        f.handles[f.primary]
            .advance_repair(plan.id, RepairStep::AbortLearner, expected, TIMEOUT)
            .await,
        Err(CommandFailure::UnsupportedCapability)
    ));
    ensure!(
        matches!(
            f.handles[f.primary]
                .add_learner(
                    plan.replacement,
                    LearnerOptions {
                        expected_revision: expected,
                        timeout: TIMEOUT
                    }
                )
                .await,
            Err(CommandFailure::UnsupportedCapability)
        ),
        "idempotent learner ingress bypassed the guard"
    );
    let target = f.standby().await?;
    ensure!(target.activate_learner(f.activation(false)?).await.is_err());
    ensure!(matches!(
        target.status().await,
        Err(CommandFailure::UnsupportedCapability)
    ));
    target
        .activate_managed_learner(authorization.clone(), f.activation(false)?)
        .await?;
    ensure!(target.activate_learner(f.activation(false)?).await.is_err());
    assert_eq!(
        f.step(&authorization, RepairStep::AbortLearner)
            .await?
            .phase,
        RepairPhase::Aborted
    );
    f.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_permit_legacy_startup_cannot_seed_managed_history() -> Result<()> {
    for from_s3 in [false, true] {
        let mut f = Fixture::new().await?;
        let plan = f.plan(40).await?;
        let authorization = f.reserve(1, 20, &plan).await?;
        f.handles[f.primary]
            .begin_managed_repair(authorization.clone(), plan, TIMEOUT)
            .await?;
        f.write(2, 1, true).await?;
        let activation = f.activation(from_s3)?;
        let placement = if from_s3 {
            RaftCellPlacement::JoiningFromS3 {
                member: activation.member,
                source: activation.source,
                source_node: activation.source_node,
            }
        } else {
            RaftCellPlacement::Joining {
                member: activation.member,
                source: activation.source,
                source_node: activation.source_node,
            }
        };
        ensure!(f
            .cluster
            .join_standby(f.definitions[3].application.clone(), placement)
            .await
            .is_err());
        use futures_util::TryStreamExt as _;
        let objects = f.cluster.store.list(None).try_collect::<Vec<_>>().await?;
        ensure!(
            !objects.iter().any(|object| object
                .location
                .as_ref()
                .ends_with("/claims/0000000000000004")),
            "unguarded startup claimed a managed learner"
        );
        assert_eq!(f.calls[3].load(Ordering::SeqCst), 0);
        assert_eq!(
            f.step(&authorization, RepairStep::AbortLearner)
                .await?
                .phase,
            RepairPhase::Aborted
        );
        f.close().await?;
    }
    Ok(())
}
