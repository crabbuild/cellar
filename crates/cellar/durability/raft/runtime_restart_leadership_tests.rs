// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A queued transfer checks the last synced guard at actual Raft-owner execution.
use super::*;
use crate::raft::fleet_stop::{BootBinding, LifecyclePurpose, LifecycleUpdate, StopSelector};
use crate::raft::options::{LogOpenMode, ReplicaLogOptions};
use crate::raft::storage::GroupBootstrap;
use cellar_logic::consensus::*;

struct Fixture {
    _root: tempfile::TempDir,
    domain: crate::asyncrt::ExecutionDomain,
    log: ReplicaLog,
    worker: Worker,
    scope: GroupScope,
    selector: StopSelector,
    _events: mpsc::Receiver<RuntimeEvent>,
}

impl Fixture {
    async fn new() -> anyhow::Result<Self> {
        let root = tempfile::tempdir()?;
        let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
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
        let boot = BootBinding {
            catalog: [4; 32],
            fleet: [5; 16],
            node: log.limits().node.bytes(),
            disk: log.limits().disk.bytes(),
            engine: log.storage_instance()?.bytes(),
            boot: [6; 16],
            process: log.limits().process.bytes(),
            epoch: 1,
            join_key: crate::peer_probe::PeerProbeSigner::from_secret([7; 32]).public_key_bytes(),
        };
        log.update_lifecycle(LifecycleUpdate::Boot {
            boot: boot.clone(),
            deadline: log.domain().mono_ms()? + 5000,
        })
        .await?;
        let selector = StopSelector {
            purpose: LifecyclePurpose::Maintenance,
            boot,
            epoch: 2,
            operation: [8; 16],
            finalization: [9; 16],
            request: [10; 16],
        };
        log.update_lifecycle(LifecycleUpdate::Quiesce {
            selector: selector.clone(),
            deadline: log.domain().mono_ms()? + 5000,
        })
        .await?;
        let scope = GroupScope {
            cluster: ClusterId::new([1; 16])?,
            namespace: NamespaceId::new([2; 16])?,
            cell: "Counter:queued-handoff".into(),
            group: GroupId::new([3; 16])?,
            incarnation: IncarnationId::new([4; 16])?,
        };
        log.create_group(GroupBootstrap {
            scope: scope.clone(),
            member: MemberId::new(1)?,
            birth_digest: [1; 32],
            voters: vec![MemberId::new(1)?, MemberId::new(2)?, MemberId::new(3)?],
            learners: vec![],
        })
        .await?;
        let (events, receiver) = mpsc::channel(256);
        let worker = Worker::new(
            log.clone(),
            RaftRuntimeOptions {
                tick_interval_ms: None,
                election_seed: Some([11; 32]),
                ..Default::default()
            },
            events,
            log.domain(),
            true,
        )?;
        Ok(Self {
            _root: root,
            domain,
            log,
            worker,
            scope,
            selector,
            _events: receiver,
        })
    }

    fn queued(
        &mut self,
        deadline: u64,
    ) -> anyhow::Result<oneshot::Receiver<Result<Response, RuntimeError>>> {
        let (reply, wait) = oneshot::channel();
        self.worker.dispatch(Request {
            command: Command::Group {
                scope: self.scope.clone(),
                action: GroupAction::RestartLeadership(Box::new(RestartLeadershipCheck {
                    deadline,
                    expected_term: Term::new(1),
                    expected_revision: LogIndex::new(1),
                    through: LogIndex::new(1),
                    survivors: [MemberId::new(2)?, MemberId::new(3)?],
                    transfer: Some((MemberId::new(2)?, self.selector.clone())),
                })),
            },
            reply,
            _permit: None,
            _inbound: None,
        });
        Ok(wait)
    }

    async fn close(mut self) -> anyhow::Result<()> {
        self.worker.finish().await?;
        self.log.close().await?;
        self.domain.shutdown().await;
        Ok(())
    }
}

#[tokio::test]
async fn runtime_restart_handoff_queued_request_refuses_synced_fence() -> anyhow::Result<()> {
    let mut f = Fixture::new().await?;
    let deadline = f.log.domain().mono_ms()? + 5000;
    let mut open = f.queued(deadline)?;
    f.worker.progress()?;
    assert!(
        matches!(open.try_recv()?, Err(RuntimeError::NotLeader)),
        "open gate did not reach the follower"
    );
    let mut queued = f.queued(deadline)?;
    f.log
        .update_lifecycle(LifecycleUpdate::Fence {
            boot: f.selector.boot.clone(),
            epoch: f.selector.epoch,
            operation: f.selector.operation,
            request: [12; 16],
            deadline,
        })
        .await?;
    f.worker.progress()?;
    assert!(
        matches!(queued.try_recv()?, Err(RuntimeError::NotReady)),
        "queued transfer bypassed the synced fence"
    );
    assert!(f.log.lifecycle_guard()?.unwrap().accepted().is_none());
    f.close().await
}

#[tokio::test]
async fn runtime_restart_handoff_queued_request_refuses_expired_deadline() -> anyhow::Result<()> {
    let mut f = Fixture::new().await?;
    // Queue with a live budget, then leave it deferred until that budget expires.
    let deadline = f.log.domain().mono_ms()? + 250;
    let mut queued = f.queued(deadline)?;
    f.log.domain().sleep_until(deadline)?.await;
    f.worker.progress()?;
    assert!(matches!(queued.try_recv()?, Err(RuntimeError::Closed)));
    assert_eq!(
        f.log.lifecycle_guard()?.unwrap().quiescing(),
        Some(&f.selector)
    );
    f.close().await
}
