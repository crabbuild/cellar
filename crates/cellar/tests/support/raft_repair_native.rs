//! Actual embedded nodes, authenticated peer seeding and joint consensus.
#![allow(clippy::disallowed_methods)]
use super::{cluster, public_node};
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::consensus::*;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

struct Fixture {
    cluster: cluster::Cluster,
    definitions: Vec<public_node::Counter>,
    handles: Vec<RaftCellHandle>,
    calls: Vec<Arc<AtomicUsize>>,
    registration: RaftCellRegistration,
    primary: usize,
    failed: usize,
    plan: RepairPlan,
    retained: VersionToken,
    _entered: tokio::sync::mpsc::UnboundedReceiver<()>,
}

async fn leader(handles: &[RaftCellHandle]) -> Result<usize> {
    Ok(tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            for (index, cell) in handles.iter().enumerate() {
                if cell.status().await.is_ok_and(|s| s.command_ready) {
                    return index;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?)
}

impl Fixture {
    async fn new(s3: bool) -> Result<Self> {
        let (gate, entered) = public_node::Gate::new();
        gate.release();
        let calls: Vec<_> = (0..4).map(|_| Arc::new(AtomicUsize::new(0))).collect();
        let definitions = calls
            .iter()
            .map(|n| public_node::counter(n.clone(), gate.clone()))
            .collect::<Result<Vec<_>>>()?;
        let namespace = NamespaceConfig::new(
            ClusterId::new([1; 16])?,
            NamespaceId::new([2; 16])?,
            cellar_logic::NamespaceMode::Raft,
            definitions[0].schema,
        );
        let registration = RaftCellRegistration {
            placement: RaftCellPlacement::Replica,
            namespace,
            class: "Counter".into(),
            birth: BirthProposal {
                scope: GroupScope {
                    cluster: ClusterId::new([1; 16])?,
                    namespace: NamespaceId::new([2; 16])?,
                    cell: "Counter:repair".into(),
                    group: GroupId::new([55; 16])?,
                    incarnation: IncarnationId::new([3; 16])?,
                },
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
            .map(|s| {
                s.cell(
                    registration.namespace.namespace,
                    &registration.birth.scope.cell,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let primary = leader(&handles).await?;
        let mut options = public_node::options(1)?;
        if s3 {
            options.durability = Durability::S3;
        }
        let receipt = handles[primary]
            .command(
                definitions[primary].increment.clone(),
                5_i64.to_be_bytes().to_vec(),
                options,
            )
            .await?;
        assert_eq!(receipt.result, 5_i64.to_be_bytes());
        let failed = (primary + 1) % 3;
        let plan = RepairPlan {
            id: ProposalId::new([44; 16])?,
            policy_revision: 1,
            expected_revision: handles[primary].status().await?.membership_revision,
            obsolete: MemberId::new(failed as u64 + 1)?,
            replacement: JoiningMember {
                member: MemberId::new(4)?,
                node: NodeId::new([4; 16])?,
                disk: DiskLineage::new([13; 16])?,
            },
        };
        Ok(Self {
            cluster,
            definitions,
            handles,
            calls,
            registration,
            primary,
            failed,
            plan,
            retained: receipt.version_token,
            _entered: entered,
        })
    }

    async fn fail_obsolete(&self) -> Result<()> {
        ensure!(self.cluster.services[self.failed]
            .close()
            .await?
            .unfinished
            .is_empty());
        Ok(())
    }

    async fn begin(&self) -> Result<RepairStatus> {
        let status = self.handles[self.primary]
            .begin_repair(self.plan.clone(), Duration::from_secs(5))
            .await?;
        assert_eq!(status.plan, self.plan);
        assert_eq!(status.phase, RepairPhase::Seeding);
        Ok(status)
    }

    async fn step(&self, step: RepairStep) -> Result<RepairStatus> {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let revision = self.handles[self.primary]
                    .status()
                    .await?
                    .membership_revision;
                match self.handles[self.primary]
                    .advance_repair(self.plan.id, step, revision, Duration::from_secs(2))
                    .await
                {
                    Ok(status) => return Ok(status),
                    Err(CommandFailure::Unavailable | CommandFailure::Overloaded) => {
                        tokio::time::sleep(Duration::from_millis(20)).await
                    }
                    Err(error) => return Err(error.into()),
                }
            }
        })
        .await?
    }

    async fn join(&mut self) -> Result<()> {
        let service = self
            .cluster
            .join_standby(
                self.definitions[3].application.clone(),
                RaftCellPlacement::Joining {
                    member: self.plan.replacement.member,
                    source: MemberId::new(self.primary as u64 + 1)?,
                    source_node: NodeId::new([self.primary as u8 + 1; 16])?,
                },
            )
            .await?;
        self.handles.push(service.cell(
            self.registration.namespace.namespace,
            &self.registration.birth.scope.cell,
        )?);
        let read = self.handles[3]
            .query(
                self.definitions[3].get.clone(),
                vec![],
                QueryOptions {
                    consistency: QueryConsistency::AtLeast(self.retained.clone()),
                    ..Default::default()
                },
            )
            .await?;
        assert_eq!(read.result, 5_i64.to_be_bytes());
        assert_eq!(self.calls[3].load(Ordering::SeqCst), 0);
        Ok(())
    }
}

pub(super) async fn learner_before_promotion() -> Result<()> {
    let f = Fixture::new(false).await?;
    f.fail_obsolete().await?;
    let started = f.begin().await?;
    let result = f.handles[f.primary]
        .advance_repair(
            f.plan.id,
            RepairStep::PromoteAndRemove,
            started.changed_at,
            Duration::from_secs(2),
        )
        .await;
    ensure!(
        matches!(result, Err(CommandFailure::Unavailable)),
        "empty learner promoted: {result:?}"
    );
    assert_eq!(
        f.handles[f.primary].repair_status().await?.unwrap().phase,
        RepairPhase::Seeding
    );
    assert_eq!(
        f.handles[f.primary].status().await?.membership_revision,
        started.changed_at
    );
    let aborted = f.step(RepairStep::AbortLearner).await?;
    assert_eq!(aborted.phase, RepairPhase::Aborted);
    assert_eq!(f.handles[f.primary].status().await?.member_high_water, 4);
    let receipt = f.handles[f.primary]
        .command(
            f.definitions[f.primary].increment.clone(),
            1_i64.to_be_bytes().to_vec(),
            public_node::options(2)?,
        )
        .await?;
    assert_eq!(receipt.result, 6_i64.to_be_bytes());
    f.cluster.close().await
}

pub(super) async fn stale_plan_idempotence() -> Result<()> {
    let f = Fixture::new(false).await?;
    f.fail_obsolete().await?;
    // Admission starts synchronously even while its returned future is unpolled.
    let lost = f.handles[f.primary].begin_repair(f.plan.clone(), Duration::from_secs(5));
    let observed = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(status) = f.handles[f.primary].repair_status().await? {
                return Ok::<_, anyhow::Error>(status);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    drop(lost);
    assert_eq!(observed.phase, RepairPhase::Seeding);
    let revision = observed.changed_at;
    assert_eq!(f.begin().await?, observed);
    let mut conflicting = f.plan.clone();
    conflicting.id = ProposalId::new([45; 16])?;
    assert!(matches!(
        f.handles[f.primary]
            .begin_repair(conflicting, Duration::from_secs(2))
            .await,
        Err(CommandFailure::CommandConflict)
    ));
    let mut rebound = f.plan.clone();
    rebound.replacement.disk = DiskLineage::new([99; 16])?;
    assert!(matches!(
        f.handles[f.primary]
            .begin_repair(rebound, Duration::from_secs(2))
            .await,
        Err(CommandFailure::CommandConflict)
    ));
    assert_eq!(
        f.handles[f.primary].status().await?.membership_revision,
        revision
    );
    let aborted = f.step(RepairStep::AbortLearner).await?;
    assert_eq!(
        f.handles[f.primary]
            .advance_repair(
                f.plan.id,
                RepairStep::AbortLearner,
                revision,
                Duration::from_secs(2)
            )
            .await?,
        aborted
    );
    f.cluster.close().await
}

pub(super) async fn minority_cannot_reconfigure() -> Result<()> {
    let f = Fixture::new(true).await?;
    for (index, service) in f.cluster.services.iter().enumerate() {
        if index != f.primary {
            ensure!(service.close().await?.unfinished.is_empty());
        }
    }
    let result = f.handles[f.primary]
        .begin_repair(f.plan.clone(), Duration::from_millis(800))
        .await;
    ensure!(
        result.is_err(),
        "minority committed repair with S3 available: {result:?}"
    );
    assert_eq!(f.handles[f.primary].repair_status().await?, None);
    let status = f.handles[f.primary].status().await?;
    assert_eq!(status.member_high_water, 3);
    assert_eq!(status.membership_revision, f.plan.expected_revision);
    use futures_util::StreamExt as _;
    let mut objects = f.cluster.store.list(None);
    while let Some(object) = objects.next().await {
        ensure!(!object?
            .location
            .as_ref()
            .ends_with("/claims/0000000000000004"));
    }
    f.cluster.close().await
}

pub(super) async fn joint_configuration_restart() -> Result<()> {
    let mut f = Fixture::new(false).await?;
    f.fail_obsolete().await?;
    f.begin().await?;
    f.join().await?;
    let joint = f.step(RepairStep::PromoteAndRemove).await?;
    assert_eq!(joint.phase, RepairPhase::Joint);
    // New incoming majority (leader + replacement) survives, but old outgoing
    // majority is absent. It must not finish or authorize a fresh strong read.
    let other = (f.primary + 2) % 3;
    ensure!(f.cluster.services[other]
        .close()
        .await?
        .unfinished
        .is_empty());
    let finish = f.handles[f.primary]
        .advance_repair(
            f.plan.id,
            RepairStep::FinishJoint,
            joint.changed_at,
            Duration::from_millis(800),
        )
        .await;
    ensure!(finish.is_err(), "joint transition bypassed old majority");
    assert_eq!(
        f.handles[f.primary].repair_status().await?.unwrap().phase,
        RepairPhase::Joint
    );
    let read = f.handles[3]
        .query(
            f.definitions[3].get.clone(),
            vec![],
            QueryOptions {
                consistency: QueryConsistency::Linearizable,
                timeout: Duration::from_millis(300),
                ..Default::default()
            },
        )
        .await;
    ensure!(
        matches!(read, Err(CommandFailure::VersionUnavailable)),
        "joint read bypassed old majority: {read:?}"
    );
    let ns = f.registration.namespace.namespace;
    let name = f.registration.birth.scope.cell.clone();
    let plan = f.plan.clone();
    let get = f.definitions[3].get.clone();
    let check = Box::new(
        move |service: RaftCells| -> futures_util::future::BoxFuture<'static, Result<()>> {
            Box::pin(async move {
                let cell = service.cell(ns, &name)?;
                let status = cell
                    .repair_status()
                    .await?
                    .context("joint plan missing before voters return")?;
                assert_eq!(status.plan, plan);
                assert_eq!(status.phase, RepairPhase::Joint);
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
                ensure!(matches!(read, Err(CommandFailure::VersionUnavailable)));
                Ok(())
            })
        },
    );
    let mut placements = vec![RaftCellPlacement::Replica; 3];
    placements.push(RaftCellPlacement::Joining {
        member: f.plan.replacement.member,
        source: MemberId::new(f.primary as u64 + 1)?,
        source_node: NodeId::new([f.primary as u8 + 1; 16])?,
    });
    f.cluster = f
        .cluster
        .restart(
            &f.definitions,
            vec![f.registration.clone()],
            placements,
            check,
        )
        .await?;
    f.handles = f
        .cluster
        .services
        .iter()
        .map(|s| s.cell(ns, &f.registration.birth.scope.cell))
        .collect::<Result<Vec<_>, _>>()?;
    f.primary = leader(&f.handles).await?;
    let complete = f.step(RepairStep::FinishJoint).await?;
    assert_eq!(complete.phase, RepairPhase::Complete);
    // No callback ran during seeding, joint changes, absence or public restart.
    assert_eq!(
        f.calls
            .iter()
            .map(|n| n.load(Ordering::SeqCst))
            .sum::<usize>(),
        1
    );
    let receipt = f.handles[f.primary]
        .command(
            f.definitions[f.primary].increment.clone(),
            1_i64.to_be_bytes().to_vec(),
            public_node::options(2)?,
        )
        .await?;
    assert_eq!(receipt.result, 6_i64.to_be_bytes());
    let reader = (0..4).find(|i| *i != f.failed && *i != f.primary).unwrap();
    let read = f.handles[reader]
        .query(
            f.definitions[reader].get.clone(),
            vec![],
            QueryOptions {
                consistency: QueryConsistency::AtLeast(receipt.version_token),
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(read.result, 6_i64.to_be_bytes());
    f.cluster.close().await
}
