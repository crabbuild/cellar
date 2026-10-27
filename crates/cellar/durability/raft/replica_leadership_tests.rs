// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Exercise the serialized owner's third-voter check against actual Raft progress.
use super::*;
use crate::asyncrt::ExecutionDomain;
use crate::raft::options::{LogOpenMode, ReplicaLogOptions};
use crate::raft::storage::{GroupBootstrap, ReplicaLog};
use cellar_logic::consensus::*;

#[tokio::test]
async fn runtime_restart_resume_owner_requires_third_voter_matched_and_active() -> anyhow::Result<()>
{
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
            Arc::new(cellar_ltx::DirectFileSystem),
        ))
        .await??;
    let scope = GroupScope {
        cluster: ClusterId::new([1; 16])?,
        namespace: NamespaceId::new([2; 16])?,
        cell: "Counter:resume-matched".into(),
        group: GroupId::new([3; 16])?,
        incarnation: IncarnationId::new([4; 16])?,
    };
    log.create_group(GroupBootstrap {
        scope: scope.clone(),
        member: MemberId::new(1)?,
        birth_digest: [5; 32],
        voters: (1..=3).map(MemberId::new).collect::<Result<_, _>>()?,
        learners: vec![],
    })
    .await?;
    domain
        .run(async {
            let mut replica =
                Replica::new(log.group(&scope)?, &RaftRuntimeOptions::default(), [7; 32])?;
            replica.node.raft.become_candidate();
            replica.node.raft.become_leader();
            let through = replica.node.raft.raft_log.last_index();
            replica.node.raft.raft_log.committed = through;
            for id in 1..=3 {
                let p = replica.node.raft.mut_prs().get_mut(id).unwrap();
                p.matched = through;
                p.recent_active = true;
            }
            let check = || RestartLeadershipCheck {
                deadline: 5000,
                expected_term: Term::new(1),
                expected_revision: LogIndex::new(1),
                through: LogIndex::new(through),
                survivors: [MemberId::new(1).unwrap(), MemberId::new(2).unwrap()],
                all_voters: true,
                transfer: None,
            };
            let term = replica.node.raft.term;
            let revision = replica.log.membership()?.revision();
            let build = || {
                let mut c = check();
                c.expected_term = Term::new(term);
                c.expected_revision = revision;
                c
            };
            replica.node.raft.mut_prs().get_mut(3).unwrap().matched = through - 1;
            assert!(matches!(
                replica.restart_leadership(build()),
                Err(RuntimeError::NotReady)
            ));
            let mut old_pair = build();
            old_pair.all_voters = false;
            replica.restart_leadership(old_pair)?;
            replica.node.raft.mut_prs().get_mut(3).unwrap().matched = through;
            replica
                .node
                .raft
                .mut_prs()
                .get_mut(3)
                .unwrap()
                .recent_active = false;
            assert!(matches!(
                replica.restart_leadership(build()),
                Err(RuntimeError::NotReady)
            ));
            replica
                .node
                .raft
                .mut_prs()
                .get_mut(3)
                .unwrap()
                .recent_active = true;
            replica.restart_leadership(build())?;
            Ok::<_, anyhow::Error>(())
        })
        .await??;
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}
