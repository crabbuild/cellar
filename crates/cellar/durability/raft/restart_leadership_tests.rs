// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Metadata checks grant no physical group, quorum or lifecycle authority.
use super::*;
use crate::raft::completion::MemberProgress;
use crate::raft::fleet_stop::{BootBinding, LifecyclePurpose, LifecycleUpdate, StopSelector};
use cellar_logic::cell_group::GroupHistory;
use cellar_logic::consensus::*;

fn progress() -> ReplicationStatus<GroupHistory> {
    ReplicationStatus {
        replica: ReplicaIdentity {
            scope: GroupHistory {
                cluster: ClusterId::new([4; 16]).unwrap(),
                namespace: NamespaceId::new([5; 16]).unwrap(),
                group: GroupId::new([6; 16]).unwrap(),
                incarnation: IncarnationId::new([7; 16]).unwrap(),
            },
            node: NodeId::new([1; 16]).unwrap(),
            disk: DiskLineage::new([2; 16]).unwrap(),
            process: ProcessGeneration::new([3; 16]).unwrap(),
            member: MemberId::new(1).unwrap(),
        },
        term: Term::new(8),
        membership_revision: LogIndex::new(9),
        committed: LogIndex::new(10),
        members: (1..=3)
            .map(|id| MemberProgress {
                member: MemberId::new(id).unwrap(),
                matched: LogIndex::new(10),
                voter: true,
                recent_active: true,
            })
            .collect(),
    }
}

fn configuration() -> ConfState {
    ConfState {
        voters: vec![1, 2, 3],
        ..Default::default()
    }
}

fn check() -> RestartLeadershipCheck {
    RestartLeadershipCheck {
        deadline: 1000,
        expected_term: Term::new(8),
        expected_revision: LogIndex::new(9),
        through: LogIndex::new(10),
        survivors: [MemberId::new(1).unwrap(), MemberId::new(2).unwrap()],
        all_voters: true,
        transfer: None,
    }
}

#[test]
fn group_restart_observation_requires_third_voter_matched_and_active() {
    let mut progress = progress();
    let conf = configuration();
    let mut check = check();
    assert!(matches!(
        check_progress(&progress, &conf, &check, false, false),
        Ok(None)
    ));
    progress.members[2].matched = LogIndex::new(9);
    assert!(matches!(
        check_progress(&progress, &conf, &check, false, false),
        Err(RuntimeError::NotReady)
    ));
    check.all_voters = false;
    assert!(matches!(
        check_progress(&progress, &conf, &check, false, false),
        Ok(None)
    ));
    check.all_voters = true;
    progress.members[2].matched = LogIndex::new(10);
    progress.members[2].recent_active = false;
    assert!(matches!(
        check_progress(&progress, &conf, &check, false, false),
        Err(RuntimeError::NotReady)
    ));
    progress.members[2].recent_active = true;
    assert!(matches!(
        check_progress(&progress, &conf, &check, false, false),
        Ok(None)
    ));
}

#[test]
fn group_restart_readiness_rejects_changed_term_revision_and_configuration() {
    for scenario in 0..8 {
        let mut progress = progress();
        let mut conf = configuration();
        let mut check = check();
        let mut pending = false;
        let mut transferring = false;
        match scenario {
            0 => progress.term = Term::new(9),
            1 => progress.membership_revision = LogIndex::new(10),
            2 => conf.voters_outgoing = vec![1, 2, 3],
            3 => conf.learners = vec![4],
            4 => pending = true,
            5 => transferring = true,
            6 => check.survivors[1] = check.survivors[0],
            _ => progress.committed = LogIndex::new(9),
        }
        assert!(
            matches!(
                check_progress(&progress, &conf, &check, pending, transferring),
                Err(RuntimeError::NotReady)
            ),
            "accepted stale readiness scenario {scenario}"
        );
    }
}

fn selector(boot: BootBinding) -> StopSelector {
    StopSelector {
        purpose: LifecyclePurpose::Maintenance,
        boot,
        epoch: 2,
        operation: [11; 16],
        finalization: [12; 16],
        request: [13; 16],
    }
}

#[test]
fn group_restart_transfer_binds_source_process_and_survivor_roles() {
    let progress = progress();
    let conf = configuration();
    let mut check = check();
    let boot = BootBinding {
        catalog: [14; 32],
        fleet: [15; 16],
        node: progress.replica.node.bytes(),
        disk: progress.replica.disk.bytes(),
        engine: [16; 16],
        boot: [17; 16],
        process: progress.replica.process.bytes(),
        epoch: 1,
        join_key: [18; 32],
    };
    check.survivors = [MemberId::new(2).unwrap(), MemberId::new(3).unwrap()];
    check.transfer = Some((MemberId::new(2).unwrap(), selector(boot)));
    let transfer = check_progress(&progress, &conf, &check, false, false)
        .unwrap()
        .unwrap();
    assert_eq!(transfer.target, MemberId::new(2).unwrap());
    assert_eq!(transfer.expected_term, progress.term);
    check.transfer.as_mut().unwrap().1.boot.process = [19; 16];
    assert!(matches!(
        check_progress(&progress, &conf, &check, false, false),
        Err(RuntimeError::NotReady)
    ));
    check.transfer.as_mut().unwrap().1.boot.process = progress.replica.process.bytes();
    check.survivors[0] = progress.replica.member;
    assert!(matches!(
        check_progress(&progress, &conf, &check, false, false),
        Err(RuntimeError::NotReady)
    ));
}

#[tokio::test]
async fn group_restart_gate_requires_exact_synced_quiescence_and_rejects_fence(
) -> anyhow::Result<()> {
    use crate::asyncrt::ExecutionDomain;
    use crate::raft::options::{LogOpenMode, ReplicaLogOptions};
    let root = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let log = domain
        .run(ReplicaLog::open(
            ReplicaLogOptions::new(
                root.path().join("log"),
                NodeId::new([1; 16])?,
                DiskLineage::new([2; 16])?,
                ProcessGeneration::new([3; 16])?,
                LogOpenMode::Create,
            ),
            std::sync::Arc::new(cellar_ltx::DirectFileSystem),
        ))
        .await??;
    let deadline = log.domain().mono_ms()? + 5000;
    let boot = BootBinding {
        catalog: [14; 32],
        fleet: [15; 16],
        node: log.limits().node.bytes(),
        disk: log.limits().disk.bytes(),
        process: log.limits().process.bytes(),
        engine: log.storage_instance()?.bytes(),
        boot: [17; 16],
        epoch: 1,
        join_key: crate::peer_probe::PeerProbeSigner::from_secret([18; 32]).public_key_bytes(),
    };
    log.update_lifecycle(LifecycleUpdate::Boot {
        boot: boot.clone(),
        deadline,
    })
    .await?;
    let selector = selector(boot.clone());
    let mut check = check();
    check.deadline = deadline;
    check.transfer = Some((MemberId::new(2)?, selector.clone()));
    assert!(matches!(
        check_gate(&log, log.domain().mono_ms()?, &check),
        Err(RuntimeError::NotReady)
    ));
    log.update_lifecycle(LifecycleUpdate::Quiesce {
        selector: selector.clone(),
        deadline,
    })
    .await?;
    check_gate(&log, log.domain().mono_ms()?, &check)?;
    check.transfer.as_mut().unwrap().1.request = [20; 16];
    assert!(matches!(
        check_gate(&log, log.domain().mono_ms()?, &check),
        Err(RuntimeError::NotReady)
    ));
    check.transfer.as_mut().unwrap().1 = selector.clone();
    log.update_lifecycle(LifecycleUpdate::Fence {
        boot,
        epoch: selector.epoch,
        operation: selector.operation,
        request: [21; 16],
        deadline,
    })
    .await?;
    assert!(matches!(
        check_gate(&log, log.domain().mono_ms()?, &check),
        Err(RuntimeError::NotReady)
    ));
    assert!(matches!(
        check_gate(&log, deadline, &check),
        Err(RuntimeError::Closed)
    ));
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}
