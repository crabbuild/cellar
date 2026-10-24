// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Physical persistence waits must not amplify unrelated consensus work.
#![allow(clippy::disallowed_methods)]
use super::*;
use crate::raft::options::{LogOpenMode, ReplicaLogOptions};
use crate::raft::storage::GroupBootstrap;
use cellar_logic::consensus::*;
use raft::prelude::MessageType;

struct Fixture {
    worker: Worker,
    log: ReplicaLog,
    scopes: Vec<GroupScope>,
    domain: crate::asyncrt::ExecutionDomain,
    _events: mpsc::Receiver<RuntimeEvent>,
    _root: tempfile::TempDir,
}

impl Fixture {
    async fn new(groups: u8, batch: usize) -> anyhow::Result<Self> {
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
        for id in 1..=groups {
            let scope = GroupScope {
                cluster: ClusterId::new([1; 16])?,
                namespace: NamespaceId::new([2; 16])?,
                cell: format!("Counter:{id:03}"),
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
        let (events, receiver) = mpsc::channel(256);
        let worker = Worker::new(
            log.clone(),
            RaftRuntimeOptions {
                tick_interval_ms: None,
                election_seed: Some([7; 32]),
                max_batch_groups: batch,
                ..Default::default()
            },
            events,
            log.domain().clone(),
            true,
        )?;
        Ok(Self {
            worker,
            log,
            scopes,
            domain,
            _events: receiver,
            _root: root,
        })
    }

    async fn close(self) -> anyhow::Result<()> {
        drop(self.worker);
        self.log.close().await?;
        self.domain.shutdown().await;
        Ok(())
    }
}

fn request(command: Command) -> (Request, oneshot::Receiver<Result<Response, RuntimeError>>) {
    let (reply, wait) = oneshot::channel();
    (
        Request {
            command,
            reply,
            _permit: None,
            _inbound: None,
        },
        wait,
    )
}

fn heartbeat(scope: &GroupScope, term: u64, context: u8) -> Command {
    let mut message = Message {
        from: 2,
        to: 1,
        term,
        commit: 1,
        context: vec![context].into(),
        ..Default::default()
    };
    message.set_msg_type(MessageType::MsgHeartbeat);
    Command::AdmitStep {
        scope: scope.clone(),
        message,
    }
}

fn advance(worker: &mut Worker) -> anyhow::Result<Vec<RuntimeEventKind>> {
    let mut events = Vec::new();
    for _ in 0..512 {
        let made = worker.progress()?;
        events.extend(worker.outbox.events.drain(..).map(|event| event.kind));
        if !made {
            return Ok(events);
        }
    }
    anyhow::bail!("consensus did not settle within the bounded fixture turn")
}

#[tokio::test]
async fn persistence_waiters_park_while_unrelated_heartbeats_continue() -> anyhow::Result<()> {
    let mut fixture = Fixture::new(33, 8).await?;
    let blocked = &fixture.scopes[..32];
    let free = &fixture.scopes[32];
    fixture
        .domain
        .run(async {
            let worker = &mut fixture.worker;
            assert!(advance(worker)?.is_empty());
            for scope in blocked {
                let (request, mut reply) = request(heartbeat(scope, 2, 0));
                worker.dispatch(request);
                assert!(matches!(reply.try_recv(), Ok(Ok(Response::Unit))));
            }
            assert!(advance(worker)?.is_empty(), "new-term replies need sync");
            assert_eq!(worker.writes.len(), blocked.len());
            assert!(
                worker.order.is_empty(),
                "groups waiting for persistence must not be rescanned for unrelated traffic"
            );
            assert!(worker.start_batch()?);
            // Hold the real persistence future. No SyncReceipt has arrived;
            // queued and in-flight groups must both remain parked.
            for context in 1..=32 {
                worker.tick();
                let (request, mut reply) = request(heartbeat(free, 1, context));
                worker.dispatch(request);
                assert!(matches!(reply.try_recv(), Ok(Ok(Response::Unit))));
                let events = advance(worker)?;
                assert_eq!(events.len(), 1);
                assert!(matches!(&events[0], RuntimeEventKind::Message { replica, message }
                    if &replica.scope == free && message.get_msg_type() == MessageType::MsgHeartbeatResponse
                        && message.context.as_ref() == [context]));
                assert!(worker.order.is_empty());
                assert_eq!(worker.writes.len(), blocked.len() - 8);
            }
            let mut acknowledged = std::collections::BTreeSet::new();
            while let Some(io) = worker.inflight.take() {
                let completed = io.await;
                let batch: Vec<_> = completed.0.iter().map(|job| job.scope.clone()).collect();
                completed.1.as_ref().map_err(Clone::clone)?;
                worker.finish_batch(completed)?;
                let events = advance(worker)?;
                let replies: Vec<_> = events
                    .into_iter()
                    .filter_map(|event| match event {
                        RuntimeEventKind::Message { replica, message }
                            if message.get_msg_type() == MessageType::MsgHeartbeatResponse =>
                        {
                            Some(replica.scope)
                        }
                        _ => None,
                    })
                    .collect();
                assert_eq!(replies.len(), batch.len());
                for scope in replies {
                    assert!(batch.contains(&scope));
                    assert!(acknowledged.insert(scope));
                }
                worker.start_batch()?;
            }
            assert_eq!(acknowledged, blocked.iter().cloned().collect());
            assert!(worker.order.is_empty());
            assert!(worker.writes.is_empty());
            Ok::<_, anyhow::Error>(())
        })
        .await??;
    fixture.close().await
}

#[tokio::test]
async fn persistence_parking_retains_deferred_peer_permits_and_hints() -> anyhow::Result<()> {
    let mut fixture = Fixture::new(1, 1).await?;
    fixture
        .domain
        .run(async {
            let worker = &mut fixture.worker;
            let scope = &fixture.scopes[0];
            worker.dispatch(request(heartbeat(scope, 2, 0)).0);
            assert!(advance(worker)?.is_empty());
            assert!(worker.start_batch()?);
            let inbound = worker.groups[scope].inbound.clone();
            let admission = Budget::new(32 * 4096, 32);
            for context in 1..=32 {
                let (mut request, mut reply) = request(heartbeat(scope, 2, context));
                request._permit = Some(admission.acquire(4096).unwrap());
                worker.dispatch(request);
                assert!(matches!(reply.try_recv(), Ok(Ok(Response::Unit))));
            }
            assert_eq!(inbound.usage().1, 32);
            assert_eq!(admission.usage(), (32 * 4096, 32));
            let (overflow, mut reply) = request(heartbeat(scope, 2, 33));
            worker.dispatch(overflow);
            assert!(matches!(
                reply.try_recv(),
                Ok(Err(RuntimeError::Overloaded))
            ));
            for snapshot in [None, Some(false)] {
                let (hint, mut reply) = request(Command::Hint {
                    scope: scope.clone(),
                    member: 2,
                    snapshot,
                });
                worker.dispatch(hint);
                assert!(matches!(reply.try_recv(), Ok(Ok(Response::Unit))));
            }
            for _ in 0..32 {
                worker.tick();
                assert!(advance(worker)?.is_empty());
                assert!(worker.order.is_empty());
                assert_eq!(
                    inbound.usage().1,
                    32,
                    "parking cannot release admitted work"
                );
                assert_eq!(admission.usage(), (32 * 4096, 32));
            }
            assert!(worker.groups[scope].tick);
            assert_eq!(worker.groups[scope].hints.len(), 1);
            let completed = worker.inflight.take().unwrap().await;
            worker.finish_batch(completed)?;
            let contexts: std::collections::BTreeSet<_> = advance(worker)?
                .into_iter()
                .filter_map(|event| match event {
                    RuntimeEventKind::Message { message, .. }
                        if message.get_msg_type() == MessageType::MsgHeartbeatResponse =>
                    {
                        Some(message.context.to_vec())
                    }
                    _ => None,
                })
                .collect();
            assert_eq!(contexts, (0..=32).map(|context| vec![context]).collect());
            assert_eq!(inbound.usage(), (0, 0));
            assert_eq!(admission.usage(), (0, 0));
            assert!(worker.groups[scope].hints.is_empty());
            assert!(!worker.groups[scope].tick);
            assert!(worker.order.is_empty());
            Ok::<_, anyhow::Error>(())
        })
        .await??;
    fixture.close().await
}

#[tokio::test]
async fn persistence_overload_retries_parked_groups_in_fifo_order() -> anyhow::Result<()> {
    let mut fixture = Fixture::new(3, 2).await?;
    fixture
        .domain
        .run(async {
            let worker = &mut fixture.worker;
            for scope in &fixture.scopes {
                worker.dispatch(request(heartbeat(scope, 2, 0)).0);
            }
            assert!(advance(worker)?.is_empty());
            let jobs = worker
                .writes
                .iter()
                .take(2)
                .map(|scope| {
                    let (ticket, write) = worker.groups[scope].replica.write.as_ref().unwrap();
                    Job {
                        scope: scope.clone(),
                        ticket: ticket.clone(),
                        write: write.clone(),
                    }
                })
                .collect();
            assert!(worker.start_batch()?);
            // Inject backend admission refusal before polling physical I/O.
            // Error completion supplies no receipt or persistence proof.
            drop(worker.inflight.take());
            let before = worker.domain.mono_ms()?;
            worker.finish_batch((jobs, Err(LogError::Overloaded), OperationId::new(1)))?;
            let after = worker.domain.mono_ms()?;
            assert!((before + 10..=after + 10).contains(&worker.retry_at));
            assert_eq!(
                worker.writes.iter().collect::<Vec<_>>(),
                fixture.scopes.iter().collect::<Vec<_>>()
            );
            assert!(worker.order.is_empty());
            // Check the future-deadline gate without racing a ten-millisecond
            // wall-clock window on a loaded test host, then restore its real cut.
            let retry_at = worker.retry_at;
            worker.retry_at = u64::MAX;
            assert!(!worker.start_batch()?, "retry must honor its deadline");
            worker.retry_at = retry_at;
            worker.domain.sleep_until(worker.retry_at)?.await;
            assert!(worker.start_batch()?);
            for expected in [&fixture.scopes[..2], &fixture.scopes[2..]] {
                let completed = worker.inflight.take().unwrap().await;
                assert_eq!(
                    completed.0.iter().map(|job| &job.scope).collect::<Vec<_>>(),
                    expected.iter().collect::<Vec<_>>()
                );
                worker.finish_batch(completed)?;
                let replies: std::collections::BTreeSet<_> = advance(worker)?
                    .into_iter()
                    .filter_map(|event| match event {
                        RuntimeEventKind::Message { replica, message }
                            if message.get_msg_type() == MessageType::MsgHeartbeatResponse =>
                        {
                            Some(replica.scope)
                        }
                        _ => None,
                    })
                    .collect();
                assert_eq!(replies, expected.iter().cloned().collect());
                worker.start_batch()?;
            }
            assert!(worker.writes.is_empty());
            assert!(worker.order.is_empty());
            Ok::<_, anyhow::Error>(())
        })
        .await??;
    fixture.close().await
}

#[tokio::test]
async fn persistence_rejection_wakes_failed_groups_without_blocking_the_next_batch(
) -> anyhow::Result<()> {
    let mut fixture = Fixture::new(2, 1).await?;
    fixture
        .domain
        .run(async {
            let worker = &mut fixture.worker;
            for scope in &fixture.scopes {
                worker.dispatch(request(heartbeat(scope, 2, 0)).0);
            }
            assert!(advance(worker)?.is_empty());
            // Corrupt only the queued candidate. The real engine must reject
            // its term regression without publishing it or an acknowledgement.
            worker.groups.get_mut(&fixture.scopes[0]).unwrap().replica.write.as_mut().unwrap().1.hard_state.as_mut().unwrap().term = 0;
            assert!(worker.start_batch()?);
            let completed = worker.inflight.take().unwrap().await;
            assert!(matches!(completed.1, Err(LogError::Invalid(_))));
            worker.finish_batch(completed)?;
            let events = advance(worker)?;
            assert_eq!(events.len(), 1);
            assert!(matches!(&events[0], RuntimeEventKind::Failed { replica, .. } if replica.scope == fixture.scopes[0]));
            assert_eq!(fixture.log.group(&fixture.scopes[0])?.read()?.hard_state.term, 1);
            assert!(worker.start_batch()?);
            let completed = worker.inflight.take().unwrap().await;
            worker.finish_batch(completed)?;
            assert!(advance(worker)?.iter().any(|event| matches!(event,
                RuntimeEventKind::Message { replica, message } if replica.scope == fixture.scopes[1]
                    && message.get_msg_type() == MessageType::MsgHeartbeatResponse)));
            assert!(worker.order.is_empty());
            Ok::<_, anyhow::Error>(())
        })
        .await??;
    fixture.close().await
}

#[tokio::test]
async fn mismatched_persistence_receipt_cannot_wake_parked_groups() -> anyhow::Result<()> {
    let mut fixture = Fixture::new(2, 2).await?;
    fixture
        .domain
        .run(async {
            let worker = &mut fixture.worker;
            for scope in &fixture.scopes {
                worker.dispatch(request(heartbeat(scope, 2, 0)).0);
            }
            assert!(advance(worker)?.is_empty());
            assert!(worker.start_batch()?);
            let mut completed = worker.inflight.take().unwrap().await;
            completed
                .1
                .as_mut()
                .map_err(|error| error.clone())?
                .groups
                .reverse();
            assert!(matches!(
                worker.finish_batch(completed),
                Err(RuntimeError::StaleCompletion)
            ));
            assert!(worker.order.is_empty());
            assert!(advance(worker)?.is_empty());
            Ok::<_, anyhow::Error>(())
        })
        .await??;
    fixture.close().await
}
