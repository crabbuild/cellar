// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Configuration application cannot invalidate an owned prior-protocol send.
use super::*;
use crate::asyncrt::ExecutionDomain;
use crate::raft::options::{LogOpenMode, ReplicaLogOptions};
use crate::raft::storage::{GroupBootstrap, ReplicaLog};
use cellar_logic::consensus::*;

#[tokio::test]
async fn membership_apply_waits_for_prior_message_delivery() -> anyhow::Result<()> {
    membership_case(ReadyCase::None).await
}

#[tokio::test]
async fn membership_delivery_fence_is_not_extended_by_later_protocol_ready() -> anyhow::Result<()> {
    membership_case(ReadyCase::LaterHeartbeat).await
}

#[tokio::test]
async fn membership_apply_drains_an_already_queued_commit_notice() -> anyhow::Result<()> {
    membership_case(ReadyCase::QueuedCommit).await
}

#[tokio::test]
async fn membership_apply_announces_commit_to_a_paused_peer() -> anyhow::Result<()> {
    membership_case(ReadyCase::PausedPeer).await
}

#[derive(Clone, Copy)]
enum ReadyCase {
    None,
    LaterHeartbeat,
    QueuedCommit,
    PausedPeer,
}

async fn membership_case(ready_case: ReadyCase) -> anyhow::Result<()> {
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
        cell: "Counter:delivery".into(),
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
            let options = RaftRuntimeOptions::default();
            let mut replica = Replica::new(log.group(&scope)?, &options, [7; 32])?;
            let mut operations = Operations::new();
            let mut outbox = Outbox::new(1024 * 1024, 32);
            let mut change = ConfChangeV2 {
                changes: vec![ConfChangeSingle {
                    node_id: 2,
                    change_type: ConfChangeType::RemoveNode,
                    ..Default::default()
                }]
                .into(),
                ..Default::default()
            };
            replica.log.membership()?.bind_change(&mut change, &[])?;
            let mut entry = Entry {
                index: 2,
                term: 1,
                data: change.write_to_bytes()?.into(),
                ..Default::default()
            };
            entry.set_entry_type(EntryType::EntryConfChangeV2);
            replica.apply.push_back(entry);
            let mut message = Message {
                from: 1,
                to: 2,
                term: 1,
                commit: if matches!(ready_case, ReadyCase::QueuedCommit) {
                    1
                } else {
                    2
                },
                ..Default::default()
            };
            message.set_msg_type(MessageType::MsgAppend);
            replica.outbound.push_back(RuntimeEventKind::Message {
                replica: replica.identity().clone(),
                message,
            });
            assert!(replica.drive(&mut operations, &mut outbox, &options, true)?);
            let event = outbox.events.pop_front().unwrap();
            let transport_owner = event._permit.clone();
            drop(event);
            let transport_owner = if matches!(ready_case, ReadyCase::QueuedCommit) {
                drop(transport_owner);
                let mut committed = Message {
                    from: 1,
                    to: 2,
                    term: 1,
                    commit: 2,
                    ..Default::default()
                };
                committed.set_msg_type(MessageType::MsgAppend);
                replica.node.raft.msgs.push(committed);
                let mut owned = None;
                for _ in 0..16 {
                    assert!(
                        replica.write.is_none(),
                        "configuration skipped a queued commit notice"
                    );
                    assert!(replica.drive(&mut operations, &mut outbox, &options, true)?);
                    if let Some(event) = outbox.events.pop_front() {
                        let RuntimeEventKind::Message { message, .. } = &event.kind else {
                            panic!("queued commit notice was not emitted")
                        };
                        assert_eq!((message.to, message.commit), (2, 2));
                        owned = Some(event._permit.clone());
                        break;
                    }
                }
                let owned = owned.expect("queued commit notice did not drain");
                for _ in 0..16 {
                    if replica.idle() && replica.ready_bytes == 0 {
                        break;
                    }
                    assert!(replica.write.is_none());
                    assert!(replica.drive(&mut operations, &mut outbox, &options, true)?);
                }
                vec![owned]
            } else if matches!(ready_case, ReadyCase::PausedPeer) {
                replica.node.raft.state = StateRole::Leader;
                replica.node.raft.mut_prs().get_mut(2).unwrap().pause();
                replica.node.raft.send_append(2);
                assert!(replica.node.raft.msgs.is_empty());
                for _ in 0..16 {
                    if replica.configuration_fence.is_some()
                        && replica.idle()
                        && replica.ready_bytes == 0
                    {
                        break;
                    }
                    assert!(replica.drive(&mut operations, &mut outbox, &options, true)?);
                    assert!(outbox.events.is_empty());
                    assert!(replica.write.is_none());
                }
                assert!(!replica.drive(&mut operations, &mut outbox, &options, true)?);
                assert!(
                    replica.outbound.is_empty(),
                    "commit notice overtook prior send"
                );
                // A new in-memory promise cannot escape before persistence.
                let hard = replica.log.initial_state()?.hard_state;
                for (term, vote) in [(hard.term + 1, hard.vote), (hard.term, 1)] {
                    replica.node.raft.term = term;
                    replica.node.raft.vote = vote;
                    replica.announce_configuration(2)?;
                    assert!(replica.outbound.is_empty());
                    replica.configuration_fence.as_mut().unwrap().announced = false;
                }
                replica.node.raft.term = hard.term;
                replica.node.raft.vote = hard.vote;
                drop(transport_owner);
                let mut owners = Vec::new();
                let mut peers = std::collections::BTreeSet::new();
                for _ in 0..16 {
                    assert!(replica.write.is_none());
                    assert!(replica.drive(&mut operations, &mut outbox, &options, true)?);
                    if let Some(event) = outbox.events.pop_front() {
                        let RuntimeEventKind::Message { message, .. } = &event.kind else {
                            panic!("configuration commit was not announced")
                        };
                        assert_eq!(message.get_msg_type(), MessageType::MsgAppend);
                        assert_eq!(
                            (
                                message.from,
                                message.term,
                                message.index,
                                message.log_term,
                                message.commit
                            ),
                            (1, 1, 2, 1, 2)
                        );
                        assert!(message.entries.is_empty());
                        if message.to == 2 {
                            check_notice_matching(message)?;
                        }
                        assert!(peers.insert(message.to));
                        owners.push(event._permit.clone());
                    }
                    if owners.len() == 2 && replica.idle() && replica.ready_bytes == 0 {
                        break;
                    }
                }
                assert_eq!(peers, [2, 3].into_iter().collect());
                assert!(replica.node.raft.prs().get(2).unwrap().is_paused());
                owners
            } else {
                vec![transport_owner]
            };
            assert!(!replica.drive(&mut operations, &mut outbox, &options, true)?);
            if matches!(ready_case, ReadyCase::LaterHeartbeat) {
                let mut heartbeat = Message {
                    from: 1,
                    to: 3,
                    term: 1,
                    commit: 2,
                    ..Default::default()
                };
                heartbeat.set_msg_type(MessageType::MsgHeartbeat);
                replica.node.raft.msgs.push(heartbeat);
                assert!(replica.node.has_ready());
            }
            for _ in 0..8 {
                assert!(!replica.drive(&mut operations, &mut outbox, &options, true)?);
                assert!(
                    replica.write.is_none(),
                    "configuration revoked a queued message's membership"
                );
                assert!(replica.log.initial_state()?.conf_state.voters.contains(&2));
            }
            // Either accepted or refused delivery releases ownership. Neither is
            // a persistence proof: applying the configuration still requires sync.
            drop(transport_owner);
            assert!(replica.drive(&mut operations, &mut outbox, &options, true)?);
            let (_, write) = replica
                .write
                .as_ref()
                .expect("configuration did not resume");
            assert!(!write.conf_state.as_ref().unwrap().voters.contains(&2));
            assert!(replica.log.initial_state()?.conf_state.voters.contains(&2));
            Ok::<_, anyhow::Error>(())
        })
        .await??;
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}

fn check_notice_matching(notice: &Message) -> anyhow::Result<()> {
    for (present, matching) in [(true, true), (true, false), (false, true)] {
        let store = ::raft::storage::MemStorage::new_with_conf_state((vec![1, 2, 3], vec![]));
        {
            let mut state = store.wl();
            let entries = (1..=if present { 2 } else { 1 })
                .map(|index| Entry {
                    index,
                    term: 1,
                    ..Default::default()
                })
                .collect::<Vec<_>>();
            state.append(&entries)?;
            state.mut_hard_state().term = 1;
            state.mut_hard_state().commit = 1;
        }
        let mut follower = RawNode::new(
            &Config {
                id: 2,
                applied: 1,
                ..Default::default()
            },
            store,
            &slog::Logger::root(slog::Discard, slog::o!()),
        )?;
        let mut message = notice.clone();
        if !matching {
            message.log_term += 1;
        }
        follower.step(message)?;
        assert_eq!(
            follower.raft.raft_log.committed,
            if present && matching { 2 } else { 1 },
            "a commit notification must match the actual receiver log"
        );
    }
    Ok(())
}
