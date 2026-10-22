//! Idle replicas must not turn each peer request into a node-wide scan.
#![allow(clippy::disallowed_methods)]
use super::*;
use crate::raft::options::{LogOpenMode, ReplicaLogOptions};
use crate::raft::storage::GroupBootstrap;
use cellar_logic::consensus::*;

#[tokio::test]
async fn idle_groups_leave_the_run_queue_and_reactivate_once() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let log = domain
        .run(ReplicaLog::open(
            ReplicaLogOptions::new(
                root.path().join("raft"),
                NodeId::new([1; 16])?,
                DiskLineage::new([2; 16])?,
                ProcessGeneration::new([3; 16])?,
                LogOpenMode::Create,
            ),
            Arc::new(cellar_ltx::DirectFileSystem),
        ))
        .await??;
    let mut scopes = Vec::new();
    for id in 1..=64u8 {
        let scope = GroupScope {
            cluster: ClusterId::new([1; 16])?,
            namespace: NamespaceId::new([2; 16])?,
            cell: format!("Counter:{id}"),
            group: GroupId::new([id; 16])?,
            incarnation: IncarnationId::new([4; 16])?,
        };
        log.create_group(GroupBootstrap {
            scope: scope.clone(),
            member: MemberId::new(1)?,
            birth_digest: [id; 32],
            voters: (1..=3).map(MemberId::new).collect::<Result<_, _>>()?,
            learners: vec![],
        })
        .await?;
        scopes.push(scope);
    }
    let (events, _receiver) = mpsc::channel(256);
    let mut worker = Worker::new(
        log.clone(),
        RaftRuntimeOptions {
            tick_interval_ms: None,
            election_seed: Some([7; 32]),
            ..Default::default()
        },
        events,
        log.domain().clone(),
        true,
    )?;
    assert!(!worker.progress()?);
    assert!(
        worker.order.is_empty(),
        "idle groups must leave the runnable queue"
    );
    let request = |command| {
        let (reply, _) = oneshot::channel();
        Request {
            command,
            reply,
            _permit: None,
            _inbound: None,
        }
    };
    worker.dispatch(request(Command::Status(scopes[0].clone())));
    assert!(
        worker.order.is_empty(),
        "status must not wake unrelated replicas"
    );
    for _ in 0..2 {
        worker.dispatch(request(Command::Group {
            scope: scopes[0].clone(),
            action: GroupAction::Campaign,
        }));
    }
    assert_eq!(worker.order.len(), 1, "one queue slot per active group");
    worker.dispatch(request(Command::Tick));
    assert_eq!(worker.order.len(), scopes.len());
    worker.dispatch(request(Command::Tick));
    assert_eq!(worker.order.len(), scopes.len(), "ticks coalesce");
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn heartbeat_without_durable_changes_does_not_write_the_engine() -> anyhow::Result<()> {
    use raft::prelude::{Message, MessageType};
    let root = tempfile::tempdir()?;
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let log = domain
        .run(ReplicaLog::open(
            ReplicaLogOptions::new(
                root.path().join("raft"),
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
        cell: "Counter:heartbeat".into(),
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
    let options = RaftRuntimeOptions::default();
    domain.run(async {
    for term in [1, 2] {
        let mut replica = Replica::new(log.group(&scope)?, &options, [7; 32])?;
        let mut heartbeat = Message { from: 2, to: 1, term, commit: 1, ..Default::default() };
        heartbeat.set_msg_type(MessageType::MsgHeartbeat);
        replica.execute(GroupAction::Step(heartbeat), &options)?;
        let mut operations = Operations::new();
        let mut outbox = Outbox { events: VecDeque::new(), budget: Budget::new(1024 * 1024, 32) };
        assert!(replica.drive(&mut operations, &mut outbox, &options, true)?);
        if term == 1 {
            assert!(replica.write.is_none(), "unchanged durable state must not require another fsync");
            for _ in 0..8 { replica.drive(&mut operations, &mut outbox, &options, true)?; }
            assert!(outbox.events.iter().any(|event| matches!(&event.kind,
                RuntimeEventKind::Message { message, .. } if message.get_msg_type() == MessageType::MsgHeartbeatResponse)));
        } else {
            assert!(replica.write.is_some(), "new term must cross the persistence boundary");
            for _ in 0..8 { replica.drive(&mut operations, &mut outbox, &options, true)?; }
            assert!(outbox.events.is_empty(), "new-term response must wait for fsync");
        }
    }
    Ok::<_, anyhow::Error>(())
    }).await??;
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}
