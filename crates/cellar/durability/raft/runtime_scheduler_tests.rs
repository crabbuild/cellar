//! Idle replicas must not turn each peer request into a node-wide scan.
#![allow(clippy::disallowed_methods)]
use super::*;
use crate::raft::options::{LogOpenMode, ReplicaLogOptions};
use crate::raft::storage::GroupBootstrap;
use cellar_logic::consensus::*;

#[tokio::test]
async fn completed_status_releases_admission_before_waking_caller() -> anyhow::Result<()> {
    use std::future::Future;
    use std::task::{Context, Wake, Waker};

    struct Caller {
        budget: Arc<Budget>,
        woke: AtomicBool,
        admitted: AtomicBool,
    }

    impl Wake for Caller {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            // A real waiter can run on another executor thread as soon as its
            // reply is published. Probe that exact cut without a timed sleep.
            self.admitted
                .store(self.budget.acquire(4096).is_some(), Ordering::Release);
            self.woke.store(true, Ordering::Release);
        }
    }

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
    let (events, _receiver) = mpsc::channel(1);
    let mut worker = Worker::new(
        log.clone(),
        RaftRuntimeOptions {
            max_requests: 1,
            tick_interval_ms: None,
            ..Default::default()
        },
        events,
        log.domain().clone(),
        false,
    )?;
    let caller = Arc::new(Caller {
        budget: Budget::new(4096, 1),
        woke: AtomicBool::new(false),
        admitted: AtomicBool::new(false),
    });
    let (reply, mut wait) = oneshot::channel();
    let waker = Waker::from(caller.clone());
    assert!(std::pin::Pin::new(&mut wait)
        .poll(&mut Context::from_waker(&waker))
        .is_pending());
    worker.dispatch(Request {
        command: Command::Status(GroupScope {
            cluster: ClusterId::new([1; 16])?,
            namespace: NamespaceId::new([2; 16])?,
            cell: "Counter:missing".into(),
            group: GroupId::new([3; 16])?,
            incarnation: IncarnationId::new([4; 16])?,
        }),
        reply,
        _permit: Some(caller.budget.acquire(4096).unwrap()),
        _inbound: None,
    });
    assert!(matches!(wait.await?, Err(RuntimeError::NotFound)));
    assert!(caller.woke.load(Ordering::Acquire));
    let admitted = caller.admitted.load(Ordering::Acquire);
    log.close().await?;
    domain.shutdown().await;
    assert!(
        admitted,
        "completed status retained admission while waking its caller"
    );
    Ok(())
}

#[tokio::test]
async fn checkpoint_reclaims_cache_ahead_of_capacity_blocked_append() -> anyhow::Result<()> {
    use crate::raft::cache::DurableApply;
    use crate::raft::snapshot::{SnapshotLimits, SnapshotPackage};
    use raft::prelude::Entry;
    use raft::Storage as _;
    let root = tempfile::tempdir()?;
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let mut limits = ReplicaLogOptions::new(
        root.path().join("raft"),
        NodeId::new([1; 16])?,
        DiskLineage::new([2; 16])?,
        ProcessGeneration::new([3; 16])?,
        LogOpenMode::Create,
    );
    limits.max_cache_bytes = 64 * 1024;
    let log = domain
        .run(ReplicaLog::open(
            limits,
            Arc::new(cellar_ltx::DirectFileSystem),
        ))
        .await??;
    let mut scopes = Vec::new();
    for id in 1..=2u8 {
        let scope = GroupScope {
            cluster: ClusterId::new([1; 16])?,
            namespace: NamespaceId::new([2; 16])?,
            cell: format!("Counter:cache-{id}"),
            group: GroupId::new([id; 16])?,
            incarnation: IncarnationId::new([4; 16])?,
        };
        let group = log
            .create_group(GroupBootstrap {
                scope: scope.clone(),
                member: MemberId::new(1)?,
                birth_digest: [id; 32],
                voters: vec![MemberId::new(1)?],
                learners: vec![],
            })
            .await?;
        let mut write = GroupWrite::new(&group);
        // Applied protocol entries have no physical image, but still consume
        // retained log bytes and can be compacted by a verified empty snapshot.
        write.entries = (2..=6)
            .map(|index| Entry {
                index,
                term: 1,
                context: vec![id; 4000].into(),
                ..Default::default()
            })
            .collect();
        let mut hard = group.initial_state()?.hard_state;
        hard.commit = 6;
        write.hard_state = Some(hard);
        write.applied = Some(DurableApply {
            position: LogPosition {
                index: LogIndex::new(6),
                term: Term::new(1),
            },
            image: None,
        });
        log.persist(OperationId::new(id as u64), vec![write])
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
        log.domain(),
        true,
    )?;
    domain
        .run(async {
            worker
                .groups
                .get_mut(&scopes[0])
                .unwrap()
                .replica
                .execute(GroupAction::Campaign, &worker.options)?;
            for _ in 0..64 {
                worker.progress()?;
                if worker.start_batch()? {
                    let done = worker.inflight.as_mut().unwrap().await;
                    worker.finish_batch(done)?;
                }
                worker.outbox.events.clear();
            }
            assert!(worker.groups[&scopes[0]].replica.status()?.protocol_ready);
            let before = log.group(&scopes[0])?.read()?.last_index();
            let checkpoint = SnapshotPackage::capture_empty(
                &log.group(&scopes[1])?,
                [9; 32],
                SnapshotLimits::default(),
            )?;
            let request = |command| {
                let (reply, _) = oneshot::channel();
                Request {
                    command,
                    reply,
                    _permit: None,
                    _inbound: None,
                }
            };
            worker.dispatch(request(Command::Group {
                scope: scopes[0].clone(),
                action: GroupAction::Propose {
                    context: vec![],
                    data: vec![8; 32 * 1024],
                },
            }));
            worker.dispatch(request(Command::Checkpoint(Box::new(
                checkpoint.checkpoint_material(),
            ))));
            worker.progress()?;
            // Deliberately visit the blocked append first, matching a retry turn.
            worker.order = scopes.iter().cloned().collect();
            assert!(worker.start_batch()?);
            let done = worker.inflight.as_mut().unwrap().await;
            assert!(
                done.1.is_ok(),
                "checkpoint must escape a capacity-blocked append batch: {:?}",
                done.1
            );
            worker.finish_batch(done)?;
            for _ in 0..64 {
                worker.progress()?;
                if worker.start_batch()? {
                    let done = worker.inflight.as_mut().unwrap().await;
                    worker.finish_batch(done)?;
                }
                worker.outbox.events.clear();
                if log.group(&scopes[0])?.read()?.last_index() > before {
                    break;
                }
            }
            assert_eq!(
                log.group(&scopes[1])?.read()?.snapshot.get_metadata().index,
                6
            );
            assert!(
                log.group(&scopes[0])?.read()?.last_index() > before,
                "append must persist after reclamation"
            );
            Ok::<_, anyhow::Error>(())
        })
        .await??;
    drop(worker);
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}

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

#[tokio::test]
async fn persistence_fifo_reserves_the_next_batch_for_a_large_waiter() -> anyhow::Result<()> {
    use raft::prelude::{Entry, Message, MessageType};
    use raft::Storage as _;
    let root = tempfile::tempdir()?;
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let mut options = ReplicaLogOptions::new(
        root.path().join("raft"),
        NodeId::new([1; 16])?,
        DiskLineage::new([2; 16])?,
        ProcessGeneration::new([3; 16])?,
        LogOpenMode::Create,
    );
    options.max_batch_bytes = 16 * 1024;
    let log = domain
        .run(ReplicaLog::open(
            options,
            Arc::new(cellar_ltx::DirectFileSystem),
        ))
        .await??;
    let mut scopes = Vec::new();
    for id in 1..=3u8 {
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
        RaftRuntimeOptions::default(),
        events,
        log.domain().clone(),
        true,
    )?;
    domain
        .run(async {
            for (scope, size) in scopes.iter().zip([8000, 11000, 1000]) {
                let mut message = Message {
                    from: 2,
                    to: 1,
                    term: 1,
                    index: 1,
                    log_term: 1,
                    ..Default::default()
                };
                message.set_msg_type(MessageType::MsgAppend);
                message.set_entries(
                    vec![Entry {
                        index: 2,
                        term: 1,
                        data: vec![7; size].into(),
                        ..Default::default()
                    }]
                    .into(),
                );
                let (reply, _) = oneshot::channel();
                worker.dispatch(Request {
                    command: Command::Group {
                        scope: scope.clone(),
                        action: GroupAction::Step(message),
                    },
                    reply,
                    _permit: None,
                    _inbound: None,
                });
            }
            worker.progress()?;
            assert_eq!(
                worker.writes.iter().collect::<Vec<_>>(),
                scopes.iter().collect::<Vec<_>>()
            );
            let sizes = scopes
                .iter()
                .map(|scope| {
                    worker.groups[scope]
                        .replica
                        .write
                        .as_ref()
                        .unwrap()
                        .1
                        .estimated_bytes()
                })
                .collect::<Result<Vec<_>, _>>()?;
            assert!(1024 + sizes[0] + sizes[1] > log.limits().max_batch_bytes);
            assert!(
                1024 + sizes[0] + sizes[2] <= log.limits().max_batch_bytes,
                "small waiter could bypass the large one"
            );
            assert!(1024 + sizes[1] + sizes[2] <= log.limits().max_batch_bytes);
            assert!(worker.start_batch()?);
            assert_eq!(
                worker.writes.iter().collect::<Vec<_>>(),
                scopes[1..].iter().collect::<Vec<_>>()
            );
            // Many unrelated scheduler passes cannot change persistence order.
            for _ in 0..20 {
                worker.progress()?;
            }
            assert_eq!(
                worker.writes.len(),
                2,
                "at most one persistence slot per group"
            );
            let completed = worker.inflight.take().unwrap().await;
            assert_eq!(
                completed.0.iter().map(|job| &job.scope).collect::<Vec<_>>(),
                vec![&scopes[0]]
            );
            worker.finish_batch(completed)?;
            assert!(worker.start_batch()?);
            let completed = worker.inflight.take().unwrap().await;
            assert_eq!(
                completed.0.iter().map(|job| &job.scope).collect::<Vec<_>>(),
                scopes[1..].iter().collect::<Vec<_>>()
            );
            let sequence = completed.1.as_ref().unwrap().sequence;
            assert!(sequence > 0);
            worker.finish_batch(completed)?;
            assert!(worker.writes.is_empty());
            for scope in &scopes {
                assert_eq!(log.group(scope)?.last_index()?, 2);
            }
            Ok::<_, anyhow::Error>(())
        })
        .await??;
    drop(worker);
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}
