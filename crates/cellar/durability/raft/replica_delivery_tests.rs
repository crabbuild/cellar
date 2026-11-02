// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Configuration application cannot invalidate an owned prior-protocol send.
use super::*;
use crate::asyncrt::ExecutionDomain;
use crate::raft::options::{LogOpenMode, ReplicaLogOptions};
use crate::raft::storage::{GroupBootstrap, ReplicaLog};
use cellar_logic::consensus::*;

#[tokio::test]
async fn membership_apply_waits_for_prior_message_delivery() -> anyhow::Result<()> {
    membership_case(false).await
}

#[tokio::test]
async fn membership_delivery_fence_is_not_extended_by_later_protocol_ready() -> anyhow::Result<()> {
    membership_case(true).await
}

async fn membership_case(later_ready: bool) -> anyhow::Result<()> {
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
