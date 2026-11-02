// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Configuration application cannot invalidate an owned prior-protocol send.
use super::*;
use crate::asyncrt::ExecutionDomain;
use crate::raft::options::{LogOpenMode, ReplicaLogOptions};
use crate::raft::storage::{GroupBootstrap, ReplicaLog};
use cellar_logic::consensus::*;

#[tokio::test]
async fn membership_apply_waits_for_prior_message_delivery() -> anyhow::Result<()> {
    membership_case(false, false, false).await
}

#[tokio::test]
async fn membership_delivery_fence_is_not_extended_by_later_protocol_ready() -> anyhow::Result<()> {
    membership_case(true, false, false).await
}

#[tokio::test]
async fn membership_apply_flushes_an_already_queued_commit_notification() -> anyhow::Result<()> {
    membership_case(false, true, false).await
}

#[tokio::test]
async fn terminal_configuration_notifies_a_paused_replica_without_advancing_progress(
) -> anyhow::Result<()> {
    membership_case(false, false, true).await
}

async fn membership_case(
    later_ready: bool,
    commit_ready: bool,
    paused: bool,
) -> anyhow::Result<()> {
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
            if paused {
                replica.node.raft.become_candidate();
                replica.node.raft.become_leader();
                replica.node.propose_conf_change(vec![], change)?;
                let final_entry = replica
                    .node
                    .raft
                    .raft_log
                    .unstable_entries()
                    .last()
                    .unwrap()
                    .clone();
                replica.node.raft.raft_log.committed = final_entry.index;
                replica.node.raft.msgs.clear();
                let peer = replica.node.raft.mut_prs().get_mut(2).unwrap();
                peer.matched = 1;
                peer.next_idx = final_entry.index + 1;
                peer.paused = true;
                let before = (peer.matched, peer.next_idx, peer.paused, peer.state);
                replica.apply.push_back(final_entry.clone());
                replica.drive(&mut operations, &mut outbox, &options, true)?;
                let Phase::Ready { ready, synced, .. } = &replica.phase else {
                    anyhow::bail!("terminal notification bypassed Ready persistence");
                };
                assert!(!synced);
                assert!(
                    outbox.events.is_empty(),
                    "unpersisted terminal packet escaped"
                );
                assert!(
                    ready
                        .messages()
                        .iter()
                        .chain(ready.persisted_messages())
                        .any(|message| {
                            message.to == 2
                                && message.commit >= final_entry.index
                                && message
                                    .entries
                                    .iter()
                                    .any(|entry| entry.index == final_entry.index)
                        }),
                    "paused replica missed its committed terminal entry"
                );
                let peer = replica.node.raft.prs().get(2).unwrap();
                assert_eq!(
                    (peer.matched, peer.next_idx, peer.paused, peer.state),
                    before
                );
                return Ok::<_, anyhow::Error>(());
            }
            replica.apply.push_back(entry);
            let mut message = Message {
                from: 1,
                to: 2,
                term: 1,
                commit: 2,
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
            if later_ready {
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
            if commit_ready {
                let mut notification = Message {
                    from: 1,
                    to: 2,
                    term: 1,
                    commit: 2,
                    ..Default::default()
                };
                notification.set_msg_type(MessageType::MsgAppend);
                replica.node.raft.msgs.push(notification);
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
            let mut commit_sent = false;
            for _ in 0..32 {
                // Later protocol work must not extend the finite send fence.
                if (later_ready || commit_ready) && replica.idle() {
                    let mut heartbeat = Message {
                        from: 1,
                        to: 3,
                        term: 1,
                        ..Default::default()
                    };
                    heartbeat.set_msg_type(MessageType::MsgHeartbeat);
                    replica.node.raft.msgs.push(heartbeat);
                }
                replica.drive(&mut operations, &mut outbox, &options, true)?;
                while let Some(event) = outbox.events.pop_front() {
                    if let RuntimeEventKind::Message { message, .. } = &event.kind {
                        commit_sent |= message.to == 2 && message.commit == 2;
                    }
                    drop(event);
                }
                if replica.write.is_some() {
                    break;
                }
            }
            if commit_ready {
                assert!(
                    commit_sent,
                    "configuration discarded its queued commit notification"
                );
            }
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
