// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

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

struct Setup {
    definitions: Vec<public_node::Counter>,
    calls: Vec<Arc<AtomicUsize>>,
    registration: RaftCellRegistration,
    entered: tokio::sync::mpsc::UnboundedReceiver<()>,
}

impl Setup {
    fn new() -> Result<Self> {
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
        Ok(Self {
            definitions,
            calls,
            registration,
            entered,
        })
    }
}

#[path = "raft_repair_crash.rs"]
mod crashes;
pub(super) use crashes::joint_plan_process_kill_resume;
#[path = "raft_standby_native.rs"]
mod standby;
pub(super) use standby::{
    registered_standby_activation_shutdown_joins,
    registered_standby_peer_activation_retains_handles,
    registered_standby_refuses_quorumless_s3_activation,
    registered_standby_s3_activation_and_restart,
};

pub(super) async fn leadership_transfer_checks_progress() -> Result<()> {
    let Setup {
        definitions,
        registration,
        entered: _entered,
        ..
    } = Setup::new()?;
    let cluster = cluster::Cluster::start(&definitions[..3], vec![registration.clone()], 100)
        .await
        .context("start transfer fixture")?;
    let cells = cluster
        .services
        .iter()
        .map(|service| {
            service.cell(
                registration.namespace.namespace,
                &registration.birth.scope.cell,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let primary = leader(&cells).await?;
    let first = cells[primary]
        .command(
            definitions[primary].increment.clone(),
            5_i64.to_be_bytes().to_vec(),
            public_node::options(1)?,
        )
        .await
        .context("first transfer-fixture command")?;
    for (cell, definition) in cells.iter().zip(&definitions) {
        cell.query(
            definition.get.clone(),
            vec![],
            QueryOptions {
                consistency: QueryConsistency::AtLeast(first.version_token.clone()),
                ..Default::default()
            },
        )
        .await
        .context("initial voter query before transfer")?;
    }
    let target = (primary + 1) % 3;
    cluster
        .isolate_consensus(target)
        .await
        .context("isolate lagging transfer target")?;
    let receipt = cells[primary]
        .command(
            definitions[primary].increment.clone(),
            1_i64.to_be_bytes().to_vec(),
            public_node::options(2)?,
        )
        .await
        .context("partitioned transfer-fixture command")?;
    let before = cells[primary]
        .status()
        .await
        .context("pre-refusal leader status")?;
    let progress = cells[primary]
        .replication_status()
        .await
        .context("pre-refusal replication progress")?;
    let target_member = MemberId::new(target as u64 + 1)?;
    ensure!(progress
        .members
        .iter()
        .any(|p| p.member == target_member && p.matched < progress.committed));
    let request = LeadershipTransfer {
        target: target_member,
        expected_term: before.term,
        expected_revision: before.membership_revision,
    };
    for (index, refused) in [
        request,
        LeadershipTransfer {
            expected_term: Term::new(before.term.get() + 1),
            ..request
        },
        LeadershipTransfer {
            expected_revision: LogIndex::new(before.membership_revision.get() + 1),
            ..request
        },
        LeadershipTransfer {
            target: MemberId::new(primary as u64 + 1)?,
            ..request
        },
        LeadershipTransfer {
            target: MemberId::new(999)?,
            ..request
        },
    ]
    .into_iter()
    .enumerate()
    {
        let result = cells[primary]
            .transfer_leadership(refused, Duration::from_secs(2))
            .await;
        if index == 0 {
            ensure!(
                matches!(result, Err(CommandFailure::Overloaded)),
                "lagging target must yield a retryable readiness refusal: {result:?}"
            );
        } else {
            ensure!(result.is_err());
        }
    }
    let after = cells[primary]
        .status()
        .await
        .context("post-refusal leader status")?;
    ensure!(
        after.command_ready
            && after.term == before.term
            && after.membership_revision == before.membership_revision
    );
    cluster.heal();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if cells[primary].replication_status().await.is_ok_and(|p| {
                p.members.iter().any(|m| {
                    m.member == target_member && m.recent_active && m.matched >= p.committed
                })
            }) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    let read = cells[target]
        .query(
            definitions[target].get.clone(),
            vec![],
            QueryOptions {
                consistency: QueryConsistency::AtLeast(receipt.version_token.clone()),
                ..Default::default()
            },
        )
        .await
        .context("query caught-up transfer target")?;
    assert_eq!(read.result, 6_i64.to_be_bytes());
    cells[primary]
        .transfer_leadership(request, Duration::from_secs(2))
        .await
        .context("transfer to caught-up voter")?;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if cells[target]
                .status()
                .await
                .is_ok_and(|s| s.command_ready && s.term > before.term)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    ensure!(
        !cells[primary]
            .status()
            .await
            .context("old leader status after transfer")?
            .command_ready
    );
    let final_receipt = cells[target]
        .command(
            definitions[target].increment.clone(),
            2_i64.to_be_bytes().to_vec(),
            public_node::options(3)?,
        )
        .await
        .context("successor command after transfer")?;
    assert_eq!(final_receipt.result, 8_i64.to_be_bytes());
    let old_read = cells[primary]
        .query(
            definitions[primary].get.clone(),
            vec![],
            QueryOptions {
                consistency: QueryConsistency::AtLeast(final_receipt.version_token),
                ..Default::default()
            },
        )
        .await
        .context("query old voter after successor write")?;
    assert_eq!(old_read.result, 8_i64.to_be_bytes());
    ensure!(cells[primary]
        .transfer_leadership(request, Duration::from_secs(2))
        .await
        .is_err());
    cluster.close().await.context("join transfer fixture")
}

impl Fixture {
    async fn new(s3: bool) -> Result<Self> {
        Self::new_with_store(s3, None).await
    }

    async fn new_with_store(
        s3: bool,
        store: Option<Arc<dyn object_store::ObjectStore>>,
    ) -> Result<Self> {
        let Setup {
            definitions,
            calls,
            registration,
            entered,
        } = Setup::new()?;
        let cluster = match store {
            Some(store) => {
                cluster::Cluster::start_with_standby_store(
                    &definitions,
                    vec![registration.clone()],
                    RaftHostOptions::default(),
                    store,
                )
                .await?
            }
            None => {
                cluster::Cluster::start_with_standby(&definitions, vec![registration.clone()])
                    .await?
            }
        };
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
            .await
            .context("repair fixture seed command")?;
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
            .await
            .context("begin repair")?;
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
    let f = Fixture::new(false).await.context("start learner fixture")?;
    f.fail_obsolete().await.context("stop obsolete voter")?;
    let started = f.begin().await?;
    let current = f.handles[f.primary].status().await?;
    let healthy = (0..3).find(|i| *i != f.primary && *i != f.failed).unwrap();
    ensure!(
        f.handles[f.primary]
            .transfer_leadership(
                LeadershipTransfer {
                    target: MemberId::new(healthy as u64 + 1)?,
                    expected_term: current.term,
                    expected_revision: current.membership_revision,
                },
                Duration::from_secs(2)
            )
            .await
            .is_err(),
        "leadership balancing competed with an active repair"
    );
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
    let aborted = f
        .step(RepairStep::AbortLearner)
        .await
        .context("abort learner")?;
    assert_eq!(aborted.phase, RepairPhase::Aborted);
    assert_eq!(f.handles[f.primary].status().await?.member_high_water, 4);
    let receipt = f.handles[f.primary]
        .command(
            f.definitions[f.primary].increment.clone(),
            1_i64.to_be_bytes().to_vec(),
            public_node::options(2)?,
        )
        .await
        .context("command after learner abort")?;
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
