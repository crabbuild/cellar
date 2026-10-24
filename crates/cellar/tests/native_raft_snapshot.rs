// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Physical snapshots and native TLS catch-up over real SQLite/LTX and Raft
//! Engine. Fresh-member authorization and automatic repair remain separate.
#![allow(clippy::disallowed_methods)]
#[path = "support/raft_ltx_fault.rs"]
#[allow(dead_code)]
mod image_fault;
#[path = "support/raft_ltx_fixture.rs"]
#[allow(dead_code)]
mod images;
#[path = "support/raft_storage_fault.rs"]
#[allow(dead_code)]
mod log_fault;

#[path = "support/raft_catalog_fault.rs"]
#[allow(dead_code)]
mod archive_fault;

#[path = "support/raft_cluster.rs"]
#[allow(dead_code)]
mod cluster;
#[path = "support/raft_node.rs"]
#[allow(dead_code)]
mod public_node;

#[path = "support/raft_seed_crash.rs"]
mod seed_crashes;
#[path = "support/raft_seed_fault.rs"]
mod seed_fault;

use anyhow::{ensure, Context as _, Result};
use cellar::application::consensus::verification::*;
use cellar::application::consensus::{CellUpdate, EnvelopeLimits, LtxImage, LtxUpdate};
use cellar::sdk::ExecutionDomain;
use cellar_logic::consensus::*;
use cellar_ltx::generation::{GenerationPin, GenerationStore};
use cellar_ltx::{DirectFileSystem, FileSystem};
use protobuf::Message as _;
use raft::{prelude::Entry, Storage};
use std::{path::Path, sync::Arc};

fn scope() -> GroupScope {
    GroupScope {
        cluster: ClusterId::new([1; 16]).unwrap(),
        namespace: NamespaceId::new([2; 16]).unwrap(),
        cell: "Counter:7".into(),
        group: GroupId::new([7; 16]).unwrap(),
        incarnation: IncarnationId::new([3; 16]).unwrap(),
    }
}

fn native_snapshot_config() -> NamespaceConfig {
    NamespaceConfig::new(
        scope().cluster,
        scope().namespace,
        cellar_logic::NamespaceMode::Raft,
        [3; 32],
    )
}

fn native_snapshot_birth(cell: &str, group: u8) -> BirthProposal {
    BirthProposal {
        scope: GroupScope {
            cell: cell.into(),
            group: GroupId::new([group; 16]).unwrap(),
            ..scope()
        },
        voters: (1..=3)
            .map(|id| InitialMember {
                member: MemberId::new(id).unwrap(),
                node: NodeId::new([id as u8; 16]).unwrap(),
            })
            .collect(),
        learners: vec![],
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum JoinClaimFault {
    None,
    Before,
    Response,
    Confirmation,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn native_fresh_learner_seed_requires_committed_binding_and_resumes_reads() -> Result<()> {
    native_seed_drill(false, false, false, JoinClaimFault::None).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn native_fresh_learner_seed_refuses_missing_quorum() -> Result<()> {
    native_seed_drill(true, false, false, JoinClaimFault::None).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn native_fresh_learner_seed_retries_partial_body_with_fresh_grant() -> Result<()> {
    native_seed_drill(false, true, false, JoinClaimFault::None).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn native_fresh_learner_s3_seed_replays_tail_and_resumes_reads() -> Result<()> {
    native_seed_drill(false, false, true, JoinClaimFault::None).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn native_fresh_learner_s3_seed_refuses_missing_quorum() -> Result<()> {
    native_seed_drill(true, false, true, JoinClaimFault::None).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn native_s3_seed_does_not_wait_for_private_writer() -> Result<()> {
    use cellar::sdk::consensus::*;
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };
    let (gate, mut entered) = public_node::Gate::new();
    let _release = public_node::Release(gate.clone());
    let calls: Vec<_> = (0..4).map(|_| Arc::new(AtomicUsize::new(0))).collect();
    let definitions = calls
        .iter()
        .map(|calls| public_node::counter(calls.clone(), gate.clone()))
        .collect::<Result<Vec<_>>>()?;
    let mut namespace = native_snapshot_config();
    namespace.schema_digest = definitions[0].schema;
    let registration = RaftCellRegistration {
        placement: RaftCellPlacement::Replica,
        namespace: namespace.clone(),
        class: "Counter".into(),
        birth: native_snapshot_birth("Counter:busy-s3-seed", 30),
    };
    let mut cluster =
        cluster::Cluster::start_with_standby(&definitions, vec![registration.clone()]).await?;
    let handles = cluster
        .services
        .iter()
        .map(|service| service.cell(namespace.namespace, &registration.birth.scope.cell))
        .collect::<Result<Vec<_>, _>>()?;
    let primary = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            for (index, handle) in handles.iter().enumerate() {
                if handle.status().await.is_ok_and(|s| s.command_ready) {
                    return index;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    let mut options = public_node::options(1)?;
    options.durability = Durability::S3;
    handles[primary]
        .command(
            definitions[primary].increment.clone(),
            5_i64.to_be_bytes().to_vec(),
            options,
        )
        .await?;
    let revision = handles[primary].status().await?.membership_revision;
    let target = JoiningMember {
        member: MemberId::new(4)?,
        node: NodeId::new([4; 16])?,
        disk: DiskLineage::new([13; 16])?,
    };
    let admitted = handles[primary]
        .add_learner(
            target.clone(),
            LearnerOptions {
                expected_revision: revision,
                timeout: Duration::from_secs(5),
            },
        )
        .await?;
    let selected = Catalog::new(
        cluster.store.clone(),
        CatalogOptions {
            prefix: "partition/test".into(),
            ..Default::default()
        },
    )?
    .existing_namespace(namespace.clone())
    .await?;
    let birth = selected
        .birth(&registration.birth.scope.cell)
        .await?
        .context("busy seed birth missing")?;
    let archive = selected.cell_archive(
        &birth,
        cellar_ltx::LtxHost::default(),
        ArchiveLimits::default(),
    )?;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if archive
                .recovery_metadata()
                .await?
                .is_some_and(|metadata| metadata.position().index >= admitted.joined_at)
            {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    // Queue without polling the waiter: the native producer reaches and owns
    // the blocked callback, whose private increment remains unpublished.
    let write = handles[primary].command(
        definitions[primary].increment.clone(),
        1_i64.to_be_bytes().to_vec(),
        public_node::options(2)?,
    );
    tokio::time::timeout(Duration::from_secs(5), entered.recv())
        .await?
        .context("writer did not enter callback")?;
    let joined = tokio::time::timeout(
        Duration::from_secs(5),
        cluster.join_standby(
            definitions[3].application.clone(),
            RaftCellPlacement::JoiningFromS3 {
                member: target.member,
                source: MemberId::new(primary as u64 + 1)?,
                source_node: NodeId::new([primary as u8 + 1; 16])?,
            },
        ),
    )
    .await
    .context("S3 seed waited for the private writer")??;
    let learner = joined.cell(namespace.namespace, &registration.birth.scope.cell)?;
    let read = learner
        .query(
            definitions[3].get.clone(),
            vec![],
            QueryOptions {
                consistency: QueryConsistency::Linearizable,
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(
        read.result,
        5_i64.to_be_bytes(),
        "seed exposed the private increment"
    );
    assert_eq!(calls[3].load(Ordering::SeqCst), 0);
    gate.release();
    let receipt = write.await?;
    assert_eq!(receipt.result, 6_i64.to_be_bytes());
    let read = learner
        .query(
            definitions[3].get.clone(),
            vec![],
            QueryOptions {
                consistency: QueryConsistency::AtLeast(receipt.version_token),
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(read.result, 6_i64.to_be_bytes());
    cluster.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn native_s3_seed_missing_selected_payload_refuses_newer_head_fallback() -> Result<()> {
    use cellar::sdk::consensus::*;
    use object_store::ObjectStore as _;
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    struct Release(Arc<archive_fault::Store>);

    impl Drop for Release {
        fn drop(&mut self) {
            self.0.release_payload();
        }
    }
    let (gate, _entered) = public_node::Gate::new();
    gate.release();
    let definitions = (0..4)
        .map(|_| public_node::counter(Arc::new(AtomicUsize::new(0)), gate.clone()))
        .collect::<Result<Vec<_>>>()?;
    let mut namespace = native_snapshot_config();
    namespace.schema_digest = definitions[0].schema;
    let registration = RaftCellRegistration {
        placement: RaftCellPlacement::Replica,
        namespace: namespace.clone(),
        class: "Counter".into(),
        birth: native_snapshot_birth("Counter:exact-s3-seed", 31),
    };
    let store = Arc::new(archive_fault::Store::default());
    let _release = Release(store.clone());
    let host = RaftHostOptions {
        archive: RaftArchiveOptions {
            limits: ArchiveLimits {
                max_tails: 1,
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    };
    let mut cluster = cluster::Cluster::start_with_standby_store(
        &definitions,
        vec![registration.clone()],
        host,
        store.clone(),
    )
    .await?;
    let handles = cluster
        .services
        .iter()
        .map(|service| service.cell(namespace.namespace, &registration.birth.scope.cell))
        .collect::<Result<Vec<_>, _>>()?;
    let primary = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            for (index, handle) in handles.iter().enumerate() {
                if handle.status().await.is_ok_and(|s| s.command_ready) {
                    return index;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    let mut options = public_node::options(1)?;
    options.durability = Durability::S3;
    handles[primary]
        .command(
            definitions[primary].increment.clone(),
            5_i64.to_be_bytes().to_vec(),
            options,
        )
        .await?;
    let revision = handles[primary].status().await?.membership_revision;
    let target = JoiningMember {
        member: MemberId::new(4)?,
        node: NodeId::new([4; 16])?,
        disk: DiskLineage::new([13; 16])?,
    };
    let admitted = handles[primary]
        .add_learner(
            target.clone(),
            LearnerOptions {
                expected_revision: revision,
                timeout: Duration::from_secs(5),
            },
        )
        .await?;
    // Settle every existing producer so only the replacement's first payload
    // GET can consume the one-shot fault; metadata lookups read no payloads.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let mut ready = true;
            for handle in &handles {
                ready &= handle
                    .status()
                    .await
                    .is_ok_and(|status| status.archived >= admitted.joined_at);
            }
            if ready {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    let selected = Catalog::new(
        store.clone(),
        CatalogOptions {
            prefix: "partition/test".into(),
            ..Default::default()
        },
    )?
    .existing_namespace(namespace.clone())
    .await?;
    let birth = selected
        .birth(&registration.birth.scope.cell)
        .await?
        .context("exact seed birth missing")?;
    let archive = selected.cell_archive(
        &birth,
        cellar_ltx::LtxHost::default(),
        ArchiveLimits::default(),
    )?;
    let selected_ref = archive
        .recovery_metadata()
        .await?
        .context("exact seed head missing")?
        .reference();
    store.payload_hang.store(true, Ordering::Release);
    store.hold_next_payload.store(true, Ordering::Release);
    let source = (primary + 1) % 3;
    let placement = RaftCellPlacement::JoiningFromS3 {
        member: target.member,
        source: MemberId::new(source as u64 + 1)?,
        source_node: NodeId::new([source as u8 + 1; 16])?,
    };
    let join = cluster.join_standby(definitions[3].application.clone(), placement);
    let fault = async {
        let result = async {
            tokio::time::timeout(Duration::from_secs(5), async {
                while !store.payload_waiting.load(Ordering::Acquire) {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            })
            .await
            .context("S3 seed never reached its selected payload")?;
            for sequence in [2, 3] {
                let mut options = public_node::options(sequence)?;
                options.durability = Durability::S3;
                let receipt = handles[primary]
                    .command(
                        definitions[primary].increment.clone(),
                        1_i64.to_be_bytes().to_vec(),
                        options,
                    )
                    .await?;
                assert_eq!(receipt.result, (sequence as i64 + 4).to_be_bytes());
            }
            let newer = archive
                .recovery_set()
                .await?
                .context("newer S3 head missing")?;
            ensure!(
                newer.reference() != selected_ref && newer.tail_entries() == 0,
                "fixture did not rotate a distinct complete snapshot"
            );
            let path = store
                .held_payload_path
                .lock()
                .map_err(|_| anyhow::anyhow!("payload fault lock"))?
                .clone()
                .context("no held payload path")?;
            store.inner.delete(&path).await?;
            // The new head remains complete even after deleting the old cut's
            // selected payload. A fallback could succeed and must be refused.
            archive
                .recovery_set()
                .await?
                .context("newer head became incomplete")?;
            Ok::<_, anyhow::Error>(())
        }
        .await;
        store.release_payload();
        result
    };
    let (joined, fault) = tokio::join!(join, fault);
    fault?;
    let error = match joined {
        Ok(service) => {
            service.close().await?;
            anyhow::bail!("S3 seed substituted a newer head")
        }
        Err(error) => error,
    };
    ensure!(
        matches!(
            error.downcast_ref::<ArchiveError>(),
            Some(ArchiveError::Corrupt("missing archive object"))
        ),
        "wrong exact-reference refusal: {error:?}"
    );
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let log = domain
        .run(ReplicaLog::open(
            ReplicaLogOptions::new(
                cluster.nodes[3].info().data_dir.join("raft"),
                target.node,
                target.disk,
                ProcessGeneration::new([99; 16])?,
                LogOpenMode::Resume,
            ),
            Arc::new(DirectFileSystem),
        ))
        .await??;
    ensure!(
        log.groups()?.is_empty(),
        "failed exact S3 seed created group state"
    );
    domain.run(log.close()).await??;
    domain.shutdown().await;
    use futures_util::StreamExt as _;
    let mut objects = store.list(None);
    while let Some(object) = objects.next().await {
        ensure!(
            !object?
                .location
                .as_ref()
                .ends_with("/claims/0000000000000004"),
            "failed exact S3 seed created a claim"
        );
    }
    cluster.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn native_s3_seed_reconciles_lost_member_claim_response() -> Result<()> {
    native_seed_drill(false, false, true, JoinClaimFault::Response).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn native_s3_seed_resumes_synced_state_before_member_claim() -> Result<()> {
    native_seed_drill(false, false, true, JoinClaimFault::Before).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn native_s3_seed_resumes_after_lost_claim_confirmation() -> Result<()> {
    native_seed_drill(false, false, true, JoinClaimFault::Confirmation).await
}

async fn native_seed_drill(
    lose_quorum: bool,
    interrupt: bool,
    from_s3: bool,
    claim_fault: JoinClaimFault,
) -> Result<()> {
    use cellar::sdk::consensus::*;
    use object_store::ObjectStore as _;
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };
    let (gate, _entered) = public_node::Gate::new();
    gate.release();
    let calls: Vec<_> = (0..4).map(|_| Arc::new(AtomicUsize::new(0))).collect();
    let definitions = calls
        .iter()
        .map(|calls| public_node::counter(calls.clone(), gate.clone()))
        .collect::<Result<Vec<_>>>()?;
    let mut namespace = native_snapshot_config();
    namespace.schema_digest = definitions[0].schema;
    let registration = RaftCellRegistration {
        placement: RaftCellPlacement::Replica,
        namespace: namespace.clone(),
        class: "Counter".into(),
        birth: native_snapshot_birth("Counter:seed", 29),
    };
    let store = Arc::new(archive_fault::Store::default());
    let mut cluster = cluster::Cluster::start_with_standby_store(
        &definitions,
        vec![registration.clone()],
        RaftHostOptions::default(),
        store.clone(),
    )
    .await
    .context("start seed fixture")?;
    let mut handles = cluster
        .services
        .iter()
        .map(|service| service.cell(namespace.namespace, &registration.birth.scope.cell))
        .collect::<Result<Vec<_>, _>>()?;
    let mut primary = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            for (index, handle) in handles.iter().enumerate() {
                if handle.status().await.is_ok_and(|s| s.command_ready) {
                    return index;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    let mut options = public_node::options(1)?;
    options.durability = Durability::S3;
    let receipt = handles[primary]
        .command(
            definitions[primary].increment.clone(),
            5_i64.to_be_bytes().to_vec(),
            options,
        )
        .await
        .context("initial S3 command")?;
    assert_eq!(receipt.result, 5_i64.to_be_bytes());
    let expected = handles[primary]
        .status()
        .await
        .context("pre-admission status")?
        .membership_revision;
    let target = JoiningMember {
        member: MemberId::new(4)?,
        node: NodeId::new([4; 16])?,
        disk: DiskLineage::new([13; 16])?,
    };
    let admission = LearnerOptions {
        expected_revision: expected,
        timeout: Duration::from_secs(5),
    };
    let mut wrong = target.clone();
    wrong.disk = DiskLineage::new([99; 16])?;
    ensure!(
        matches!(
            handles[primary]
                .add_learner(wrong.clone(), admission.clone())
                .await,
            Err(CommandFailure::CommandConflict)
        ),
        "mismatched disk admitted"
    );
    let admitted = handles[primary]
        .add_learner(target.clone(), admission.clone())
        .await
        .context("commit learner admission")?;
    assert_eq!(admitted.member, target);
    assert_eq!(
        handles[primary]
            .add_learner(target.clone(), admission.clone())
            .await
            .context("reconcile learner admission")?,
        admitted,
        "same admission did not reconcile"
    );
    ensure!(
        matches!(
            handles[primary].add_learner(wrong, admission).await,
            Err(CommandFailure::CommandConflict)
        ),
        "committed binding rebound"
    );
    if from_s3 {
        let selected = Catalog::new(
            cluster.store.clone(),
            CatalogOptions {
                prefix: "partition/test".into(),
                ..Default::default()
            },
        )?
        .existing_namespace(namespace.clone())
        .await?;
        let birth = selected
            .birth(&registration.birth.scope.cell)
            .await?
            .context("S3 seed birth missing")?;
        let archive = selected.cell_archive(
            &birth,
            cellar_ltx::LtxHost::default(),
            ArchiveLimits::default(),
        )?;
        // Wait for the configuration cut before the next data command so the
        // fixture necessarily exercises an archived LTX tail, rather than a
        // producer coalescing configuration and data into one full snapshot.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if archive
                    .recovery_metadata()
                    .await?
                    .is_some_and(|metadata| metadata.position().index >= admitted.joined_at)
                {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await??;
        let mut options = public_node::options(2)?;
        options.durability = Durability::S3;
        let deadline = tokio::time::Instant::now() + options.timeout;
        let tail = loop {
            options.timeout = deadline.saturating_duration_since(tokio::time::Instant::now());
            ensure!(
                !options.timeout.is_zero(),
                "archive tail admission deadline"
            );
            let result = handles[primary]
                .command(
                    definitions[primary].increment.clone(),
                    0_i64.to_be_bytes().to_vec(),
                    options.clone(),
                )
                .await;
            // The archived configuration cut does not imply an idle SQL/status
            // owner. Retry only explicit backpressure, retaining the exact
            // command ID/payload/durability and original operation deadline.
            if matches!(result, Err(CommandFailure::Overloaded)) {
                tokio::time::sleep_until(
                    deadline.min(tokio::time::Instant::now() + Duration::from_millis(5)),
                )
                .await;
                continue;
            }
            break result.context("archive post-join LTX tail")?;
        };
        assert_eq!(tail.result, 5_i64.to_be_bytes());
        let set = archive
            .recovery_set()
            .await?
            .context("S3 seed head missing")?;
        ensure!(
            set.tail_entries() > 0,
            "native S3 seed fixture did not archive an LTX tail"
        );
    }
    let source = if lose_quorum {
        primary
    } else {
        (primary + 1) % 3
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        while !handles[source]
            .status()
            .await
            .is_ok_and(|s| s.membership_revision >= admitted.joined_at)
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    if lose_quorum {
        for (index, service) in cluster.services.iter().enumerate() {
            if index != primary {
                ensure!(service
                    .close()
                    .await
                    .context("close lost-quorum voter")?
                    .unfinished
                    .is_empty());
            }
        }
        tokio::time::sleep(Duration::from_millis(600)).await;
    }
    let held = if interrupt {
        Some(cluster.hold_seed_body(3, source)?)
    } else {
        None
    };
    let placement = if from_s3 {
        RaftCellPlacement::JoiningFromS3 {
            member: target.member,
            source: MemberId::new(source as u64 + 1)?,
            source_node: NodeId::new([source as u8 + 1; 16])?,
        }
    } else {
        RaftCellPlacement::Joining {
            member: target.member,
            source: MemberId::new(source as u64 + 1)?,
            source_node: NodeId::new([source as u8 + 1; 16])?,
        }
    };
    match claim_fault {
        JoinClaimFault::Before => store.fail_join_claim_before.store(true, Ordering::Release),
        JoinClaimFault::Response => store.fail_join_claim_after.store(true, Ordering::Release),
        JoinClaimFault::Confirmation => {
            store.fail_join_claim_after.store(true, Ordering::Release);
            store
                .fail_join_claim_confirmation
                .store(true, Ordering::Release);
        }
        JoinClaimFault::None => (),
    }
    let joined = cluster
        .join_standby(definitions[3].application.clone(), placement)
        .await;
    if lose_quorum {
        let error = joined.err().context("seeded without live quorum")?;
        ensure!(
            matches!(
                error.downcast_ref::<TransportError>(),
                Some(TransportError::Refused | TransportError::Timeout | TransportError::Io(_))
            ),
            "unexpected quorumless startup failure: {error:?}"
        );
        ensure!(
            cluster.data_attempts(3) > 0,
            "quorumless seed made no peer request"
        );
        let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
        let log = domain
            .run(ReplicaLog::open(
                ReplicaLogOptions::new(
                    cluster.nodes[3].info().data_dir.join("raft"),
                    target.node,
                    target.disk,
                    ProcessGeneration::new([99; 16])?,
                    LogOpenMode::Resume,
                ),
                Arc::new(DirectFileSystem),
            ))
            .await?
            .context("inspect refused learner after failed startup")?;
        ensure!(
            matches!(
                log.group(&registration.birth.scope),
                Err(LogError::NotFound)
            ),
            "refused seed created group state"
        );
        domain.run(log.close()).await??;
        domain.shutdown().await;
        use futures_util::StreamExt as _;
        let mut objects = cluster.store.list(None);
        while let Some(object) = objects.next().await {
            ensure!(
                !object?
                    .location
                    .as_ref()
                    .ends_with("/claims/0000000000000004"),
                "refused seed created a member claim"
            );
        }
    } else {
        let recovering_claim = matches!(
            claim_fault,
            JoinClaimFault::Before | JoinClaimFault::Confirmation
        );
        let joined = if recovering_claim {
            let error = match joined {
                Ok(service) => {
                    service.close().await?;
                    anyhow::bail!("claim fault was not exercised")
                }
                Err(error) => error,
            };
            ensure!(
                matches!(
                    error.downcast_ref::<CatalogError>(),
                    Some(CatalogError::Storage(_))
                ),
                "wrong claim refusal: {error:?}"
            );
            // Observe the synced engine without repairing or reopening images;
            // the following public Node resume must perform image recovery.
            let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
            let log = domain
                .run(ReplicaLog::open(
                    ReplicaLogOptions::new(
                        cluster.nodes[3].info().data_dir.join("raft"),
                        target.node,
                        target.disk,
                        ProcessGeneration::new([99; 16])?,
                        LogOpenMode::Resume,
                    ),
                    Arc::new(DirectFileSystem),
                ))
                .await??;
            let group = log.group(&registration.birth.scope)?;
            assert_eq!(group.initial_state()?.hard_state.vote, 0);
            assert_eq!(
                group.membership()?.joined(target.member)?,
                Some((target.clone(), admitted.joined_at))
            );
            ensure!(group.applied()?.position.index >= admitted.joined_at);
            domain.run(log.close()).await??;
            domain.shutdown().await;
            use futures_util::StreamExt as _;
            let mut objects = store.list(None);
            let mut claimed = false;
            while let Some(object) = objects.next().await {
                claimed |= object?
                    .location
                    .as_ref()
                    .ends_with("/claims/0000000000000004");
            }
            assert_eq!(claimed, claim_fault == JoinClaimFault::Confirmation);
            let ns = namespace.namespace;
            let name = registration.birth.scope.cell.clone();
            let get = definitions[3].get.clone();
            let retained = receipt.version_token.clone();
            let check = Box::new(
                move |service: RaftCells| -> futures_util::future::BoxFuture<'static, Result<()>> {
                    Box::pin(async move {
                        let cell = service.cell(ns, &name)?;
                        let strong = cell
                            .query(
                                get.clone(),
                                vec![],
                                QueryOptions {
                                    consistency: QueryConsistency::Linearizable,
                                    timeout: Duration::from_millis(200),
                                    ..Default::default()
                                },
                            )
                            .await;
                        ensure!(matches!(strong, Err(CommandFailure::VersionUnavailable)));
                        let read = cell
                            .query(
                                get,
                                vec![],
                                QueryOptions {
                                    consistency: QueryConsistency::AtLeast(retained),
                                    ..Default::default()
                                },
                            )
                            .await?;
                        assert_eq!(read.result, 5_i64.to_be_bytes());
                        Ok(())
                    })
                },
            );
            let placement = RaftCellPlacement::JoiningFromS3 {
                member: target.member,
                source: MemberId::new(1)?,
                source_node: NodeId::new([1; 16])?,
            };
            cluster = tokio::time::timeout(
                Duration::from_secs(15),
                cluster.restart(
                    &definitions,
                    vec![registration.clone()],
                    vec![
                        RaftCellPlacement::Replica,
                        RaftCellPlacement::Replica,
                        RaftCellPlacement::Replica,
                        placement,
                    ],
                    check,
                ),
            )
            .await
            .context("public partial-claim resume")??;
            handles = cluster
                .services
                .iter()
                .take(3)
                .map(|service| service.cell(namespace.namespace, &registration.birth.scope.cell))
                .collect::<Result<Vec<_>, _>>()?;
            primary = tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    for (index, handle) in handles.iter().enumerate() {
                        if handle
                            .status()
                            .await
                            .is_ok_and(|status| status.command_ready)
                        {
                            return index;
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await?;
            cluster.services[3].clone()
        } else {
            joined.context("fresh learner join")?
        };
        if let Some(held) = held {
            ensure!(
                held.load(Ordering::SeqCst) > 0,
                "fault did not hold a partial body"
            );
        }

        let learner = joined.cell(namespace.namespace, &registration.birth.scope.cell)?;
        ensure!(
            !learner
                .status()
                .await
                .context("seeded learner status")?
                .command_ready,
            "learner became a writer"
        );
        let result = learner
            .query(
                definitions[3].get.clone(),
                vec![],
                QueryOptions {
                    consistency: QueryConsistency::Linearizable,
                    ..Default::default()
                },
            )
            .await
            .context("seeded learner linearizable query")?;
        assert_eq!(result.result, 5_i64.to_be_bytes());
        assert_eq!(
            calls[3].load(Ordering::SeqCst),
            0,
            "seed invoked business command callback"
        );
        let next = handles[primary]
            .command(
                definitions[primary].increment.clone(),
                1_i64.to_be_bytes().to_vec(),
                public_node::options(if from_s3 { 3 } else { 2 })?,
            )
            .await
            .context("command after learner seed")?;
        let result = learner
            .query(
                definitions[3].get.clone(),
                vec![],
                QueryOptions {
                    consistency: QueryConsistency::AtLeast(next.version_token.clone()),
                    ..Default::default()
                },
            )
            .await
            .context("learner token query after replication")?;
        assert_eq!(result.result, 6_i64.to_be_bytes());
        ensure!(joined.close().await?.unfinished.is_empty());
        let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
        let log = domain
            .run(ReplicaLog::open(
                ReplicaLogOptions::new(
                    cluster.nodes[3].info().data_dir.join("raft"),
                    target.node,
                    target.disk,
                    ProcessGeneration::new([99; 16])?,
                    LogOpenMode::Resume,
                ),
                Arc::new(DirectFileSystem),
            ))
            .await??;
        let group = log.group(&registration.birth.scope)?;
        let state = group.initial_state()?;
        assert_eq!(state.hard_state.vote, 0, "learner copied a source vote");
        assert!(state.conf_state.learners.contains(&4));
        assert_eq!(
            group.membership()?.joined(target.member)?,
            Some((target.clone(), admitted.joined_at))
        );
        let image_path = cluster.nodes[3]
            .info()
            .data_dir
            .join("raft-cells")
            .join(hex::encode(namespace.namespace.bytes()))
            .join(hex::encode(registration.birth.scope.group.bytes()))
            .join(hex::encode(registration.birth.scope.incarnation.bytes()))
            .join("images");
        let images = GenerationStore::open(
            image_path,
            Arc::new(DirectFileSystem),
            cellar::sdk::consensus::RaftHostOptions::default().images,
        )?;
        let applier = ImageApplier::recover(
            group,
            images,
            definitions[3].schema,
            EnvelopeLimits::default(),
        )?;
        assert_eq!(
            value(applier.current().context("resumed learner image")?.pin())?,
            6
        );
        drop(applier);
        domain.run(log.close()).await??;
        domain.shutdown().await;
        if from_s3 && !recovering_claim {
            let ns = namespace.namespace;
            let cell_name = registration.birth.scope.cell.clone();
            let get = definitions[3].get.clone();
            let retained = next.version_token.clone();
            let before: Vec<_> = calls
                .iter()
                .map(|calls| calls.load(Ordering::SeqCst))
                .collect();
            let placement = RaftCellPlacement::JoiningFromS3 {
                member: target.member,
                source: MemberId::new(1)?,
                source_node: NodeId::new([1; 16])?,
            };
            // Node four resumes first. Its sources and voting majority have
            // not started; stable local state must avoid a fresh seed RPC.
            let check = Box::new(
                move |service: RaftCells| -> futures_util::future::BoxFuture<'static, Result<()>> {
                    Box::pin(async move {
                        let cell = service.cell(ns, &cell_name)?;
                        let strong = cell
                            .query(
                                get.clone(),
                                vec![],
                                QueryOptions {
                                    consistency: QueryConsistency::Linearizable,
                                    timeout: Duration::from_millis(200),
                                    ..Default::default()
                                },
                            )
                            .await;
                        ensure!(
                            matches!(strong, Err(CommandFailure::VersionUnavailable)),
                            "resumed data invented a fresh read barrier: {strong:?}"
                        );
                        let historical = cell
                            .query(
                                get.clone(),
                                vec![],
                                QueryOptions {
                                    consistency: QueryConsistency::Stale,
                                    stale_policy: StaleReadPolicy::DisconnectedHistorical,
                                    ..Default::default()
                                },
                            )
                            .await?;
                        assert_eq!(historical.result, 6_i64.to_be_bytes());
                        let covered = cell
                            .query(
                                get,
                                vec![],
                                QueryOptions {
                                    consistency: QueryConsistency::AtLeast(retained),
                                    ..Default::default()
                                },
                            )
                            .await?;
                        assert_eq!(covered.result, 6_i64.to_be_bytes());
                        Ok(())
                    })
                },
            );
            cluster = tokio::time::timeout(
                Duration::from_secs(15),
                cluster.restart(
                    &definitions,
                    vec![registration.clone()],
                    vec![
                        RaftCellPlacement::Replica,
                        RaftCellPlacement::Replica,
                        RaftCellPlacement::Replica,
                        placement,
                    ],
                    check,
                ),
            )
            .await
            .context("public joined-node restart")??;
            assert_eq!(
                calls
                    .iter()
                    .map(|calls| calls.load(Ordering::SeqCst))
                    .collect::<Vec<_>>(),
                before,
                "restart reran a business callback"
            );
            let resumed =
                cluster.services[3].cell(namespace.namespace, &registration.birth.scope.cell)?;
            let read = resumed
                .query(
                    definitions[3].get.clone(),
                    vec![],
                    QueryOptions {
                        consistency: QueryConsistency::Linearizable,
                        ..Default::default()
                    },
                )
                .await?;
            assert_eq!(read.result, 6_i64.to_be_bytes());
        }
        let root = tempfile::tempdir()?;
        let blank = public_node::node_named(
            root.path(),
            definitions[3].application.clone(),
            "blank-seed-claim",
        )
        .await?;
        let mut registration = registration.clone();
        registration.placement = RaftCellPlacement::Joining {
            member: target.member,
            source: MemberId::new(1)?,
            source_node: NodeId::new([1; 16])?,
        };
        let reset = blank
            .start_raft_cells(
                RaftCellsConfig {
                    infrastructure: cellar::sdk::NodeRaftConfig {
                        log: ReplicaLogOptions::new(
                            blank.info().data_dir.join("raft"),
                            target.node,
                            target.disk,
                            ProcessGeneration::new([100; 16])?,
                            LogOpenMode::Create,
                        ),
                        consensus: RaftRuntimeOptions::default(),
                        transport: TransportOptions::default(),
                    },
                    catalog: CatalogOptions {
                        prefix: "partition/test".into(),
                        ..Default::default()
                    },
                    cells: vec![registration],
                    peers: vec![],
                    query_origins: vec![],
                    host: Default::default(),
                },
                RaftCellsServices {
                    listeners: cellar::sdk::NodeRaftListeners {
                        control: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
                        data: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
                        query: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
                    },
                    tls: public_node::tls()?,
                    store: cluster.store.clone(),
                    application: definitions[3].application.clone(),
                    versions: public_node::version_keys()?,
                },
            )
            .await;
        let error = match reset {
            Ok(service) => {
                service.close().await?;
                anyhow::bail!("claimed joined identity restarted blank")
            }
            Err(error) => error,
        };
        ensure!(
            matches!(
                error.downcast_ref::<CatalogError>(),
                Some(CatalogError::LostStableState)
            ),
            "wrong claimed-state refusal: {error:?}"
        );
        blank.shutdown(cellar::sdk::ShutdownMode::Preserve).await?;
    }
    cluster.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn native_snapshot_catchup_resumes_follower_reads() -> Result<()> {
    native_snapshot_drill(false, false, HealthyLeader::Any).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn native_snapshot_interrupted_transfer_retries_without_blocking_other_groups() -> Result<()>
{
    native_snapshot_drill(true, false, HealthyLeader::Any).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn native_snapshot_shutdown_joins_partial_transfer() -> Result<()> {
    native_snapshot_drill(true, true, HealthyLeader::Any).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn native_snapshot_recovering_node_leads_independent_group() -> Result<()> {
    native_snapshot_drill(true, false, HealthyLeader::RecoveringNode).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn native_snapshot_shutdown_joins_recovering_healthy_leader() -> Result<()> {
    native_snapshot_drill(true, true, HealthyLeader::RecoveringNode).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_snapshot_admission_refuses_insufficient_restore_capacity() -> Result<()> {
    use futures_util::StreamExt as _;
    use object_store::ObjectStore as _;
    let (gate, _entered) = public_node::Gate::new();
    gate.release();
    let definition = public_node::counter(Arc::new(std::sync::atomic::AtomicUsize::new(0)), gate)?;
    for profile in ["encoded", "generations", "copies"] {
        let root = tempfile::tempdir()?;
        let node = public_node::node(root.path(), definition.application.clone()).await?;
        let store = Arc::new(object_store::memory::InMemory::new());
        let mut host = cellar::sdk::consensus::RaftHostOptions::default();
        let mut transport = TransportOptions::default();
        match profile {
            "encoded" => transport.max_snapshot_bytes = host.execution.max_image_bytes as usize,
            "generations" => host.images.max_generations = 5,
            "copies" => host.images.max_bytes = host.execution.max_image_bytes * 7,
            _ => unreachable!(),
        }
        let mut namespace = native_snapshot_config();
        namespace.schema_digest = definition.schema;
        let registration = cellar::sdk::consensus::RaftCellRegistration {
            placement: cellar::sdk::consensus::RaftCellPlacement::Replica,
            namespace,
            class: "Counter".into(),
            birth: native_snapshot_birth("Counter:snapshot", 5),
        };
        let result = public_node::cells_with_transport(
            &node,
            definition.application.clone(),
            store.clone(),
            registration,
            false,
            host,
            transport,
        )
        .await;
        ensure!(
            result.is_err(),
            "invalid snapshot admission {profile} started a member"
        );
        ensure!(
            !node.info().data_dir.join("raft").exists(),
            "invalid {profile} opened permanent member storage"
        );
        ensure!(
            store.list(None).next().await.is_none(),
            "invalid {profile} created catalog/claim objects"
        );
        node.shutdown(cellar::sdk::ShutdownMode::Preserve).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn missing_quorum_restore_refused() -> Result<()> {
    use cellar::sdk::consensus::*;
    use std::{sync::atomic::AtomicUsize, time::Duration};
    let (gate, _entered) = public_node::Gate::new();
    gate.release();
    let definitions = (0..3)
        .map(|_| public_node::counter(Arc::new(AtomicUsize::new(0)), gate.clone()))
        .collect::<Result<Vec<_>>>()?;
    let mut namespace = native_snapshot_config();
    namespace.schema_digest = definitions[0].schema;
    let registration = RaftCellRegistration {
        placement: RaftCellPlacement::Replica,
        namespace: namespace.clone(),
        class: "Counter".into(),
        birth: native_snapshot_birth("Counter:quorum-restore", 19),
    };
    let cluster = cluster::Cluster::start(&definitions, vec![registration.clone()], 20).await?;
    let handles = cluster
        .services
        .iter()
        .map(|service| service.cell(namespace.namespace, &registration.birth.scope.cell))
        .collect::<Result<Vec<_>, _>>()?;
    let primary = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            for (index, handle) in handles.iter().enumerate() {
                if handle
                    .status()
                    .await
                    .is_ok_and(|status| status.command_ready)
                {
                    return index;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    let mut options = public_node::options(1)?;
    options.durability = Durability::S3;
    let receipt = handles[primary]
        .command(
            definitions[primary].increment.clone(),
            1_i64.to_be_bytes().to_vec(),
            options,
        )
        .await?;
    let selected = Catalog::new(
        cluster.store.clone(),
        CatalogOptions {
            prefix: "partition/test".into(),
            ..Default::default()
        },
    )?
    .existing_namespace(namespace.clone())
    .await?;
    let birth = selected
        .birth(&registration.birth.scope.cell)
        .await?
        .context("missing birth")?;
    let archive = selected.cell_archive(
        &birth,
        cellar_ltx::LtxHost::default(),
        ArchiveLimits::default(),
    )?;
    let set = archive
        .recovery_set()
        .await?
        .context("S3 acknowledgement has no archive")?;
    ensure!(set.position().index >= receipt.version.position.index);
    let archived = set.position();
    // Retain a real voter and available archive bytes, but remove both other
    // consensus owners. No bucket record may replace their missing votes.
    for (index, service) in cluster.services.iter().enumerate() {
        if index != primary {
            ensure!(service.close().await?.unfinished.is_empty());
        }
    }
    tokio::time::sleep(Duration::from_millis(600)).await;
    let read = handles[primary]
        .query(
            definitions[primary].get.clone(),
            vec![],
            QueryOptions {
                consistency: QueryConsistency::Linearizable,
                timeout: Duration::from_millis(400),
                ..Default::default()
            },
        )
        .await;
    ensure!(
        matches!(read, Err(CommandFailure::VersionUnavailable)),
        "S3 bytes established a strong-read barrier without quorum"
    );
    let historical = handles[primary]
        .query(
            definitions[primary].get.clone(),
            vec![],
            QueryOptions {
                consistency: QueryConsistency::Stale,
                stale_policy: StaleReadPolicy::DisconnectedHistorical,
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(historical.result, 1_i64.to_be_bytes());
    let mut options = public_node::options(2)?;
    options.durability = Durability::S3;
    options.timeout = Duration::from_millis(400);
    ensure!(
        handles[primary]
            .command(
                definitions[primary].increment.clone(),
                1_i64.to_be_bytes().to_vec(),
                options
            )
            .await
            .is_err(),
        "S3 availability acknowledged a command without quorum"
    );
    assert_eq!(
        archive
            .recovery_set()
            .await?
            .context("archive disappeared")?
            .position(),
        archived
    );

    // Physically restore the archived cell image into blank replacement node
    // directories. Neither the old disk identity, a different disk identity,
    // nor an unassigned node can turn those bytes into normal membership.
    for (node_id, disk, expected) in [(1u8, 10u8, "lost"), (1, 91, "claim"), (4, 94, "member")] {
        let root = tempfile::tempdir()?;
        let image_dir = root
            .path()
            .join("raft-cells")
            .join(hex::encode(namespace.namespace.bytes()))
            .join(hex::encode(registration.birth.scope.group.bytes()))
            .join(hex::encode(registration.birth.scope.incarnation.bytes()));
        let images =
            GenerationStore::open(image_dir, Arc::new(DirectFileSystem), Default::default())?;
        let restored = set.restore(&images, images::id(91))?;
        assert_eq!(value(restored)?, 1);
        drop(images);
        let node = public_node::node_named(
            root.path(),
            definitions[0].application.clone(),
            &format!("raft-node-{node_id}"),
        )
        .await?;
        let result = node
            .start_raft_cells(
                RaftCellsConfig {
                    infrastructure: cellar::sdk::NodeRaftConfig {
                        log: ReplicaLogOptions::new(
                            node.info().data_dir.join("raft"),
                            NodeId::new([node_id; 16])?,
                            DiskLineage::new([disk; 16])?,
                            ProcessGeneration::new([99; 16])?,
                            LogOpenMode::Create,
                        ),
                        consensus: RaftRuntimeOptions {
                            tick_interval_ms: Some(20),
                            ..Default::default()
                        },
                        transport: TransportOptions::default(),
                    },
                    catalog: CatalogOptions {
                        prefix: "partition/test".into(),
                        ..Default::default()
                    },
                    cells: vec![registration.clone()],
                    peers: vec![],
                    query_origins: vec![],
                    host: Default::default(),
                },
                RaftCellsServices {
                    listeners: cellar::sdk::NodeRaftListeners {
                        control: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
                        data: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
                        query: tokio::net::TcpListener::bind("127.0.0.1:0").await?,
                    },
                    tls: public_node::tls()?,
                    store: cluster.store.clone(),
                    application: definitions[0].application.clone(),
                    versions: public_node::version_keys()?,
                },
            )
            .await;
        let error = match result {
            Ok(service) => {
                service.close().await?;
                anyhow::bail!("restored bytes admitted unauthorized member");
            }
            Err(error) => error,
        };
        ensure!(
            error.chain().any(|cause| matches!(
                (expected, cause.downcast_ref::<CatalogError>()),
                ("lost", Some(CatalogError::LostStableState))
                    | ("claim", Some(CatalogError::Conflict(_)))
                    | ("member", Some(CatalogError::NotInitialMember))
            )),
            "unexpected replacement refusal: {error:#}"
        );
        let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
        let log = domain
            .run(ReplicaLog::open(
                ReplicaLogOptions::new(
                    node.info().data_dir.join("raft"),
                    NodeId::new([node_id; 16])?,
                    DiskLineage::new([disk; 16])?,
                    ProcessGeneration::new([100; 16])?,
                    LogOpenMode::Resume,
                ),
                Arc::new(DirectFileSystem),
            ))
            .await??;
        ensure!(
            log.groups()?.is_empty(),
            "failed restore manufactured a group or voting state"
        );
        log.close().await?;
        domain.shutdown().await;
        node.shutdown(cellar::sdk::ShutdownMode::Preserve).await?;
    }
    cluster.close().await
}

enum HealthyLeader {
    Any,
    RecoveringNode,
}

async fn native_snapshot_drill(
    interrupt: bool,
    close_pending: bool,
    healthy_leader: HealthyLeader,
) -> Result<()> {
    use cellar::sdk::consensus::*;
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };
    // The recovering node may lead the independent group. Count each group's
    // callbacks separately so that legitimate work cannot hide snapshot replay.
    let calls: Vec<_> = (0..3).map(|_| Arc::new(AtomicUsize::new(0))).collect();
    let healthy_calls: Vec<_> = (0..3).map(|_| Arc::new(AtomicUsize::new(0))).collect();
    let (gate, _entered) = public_node::Gate::new();
    gate.release();
    let definitions = calls
        .iter()
        .zip(&healthy_calls)
        .map(|(calls, healthy_calls)| {
            let calls = calls.clone();
            let healthy_calls = healthy_calls.clone();
            public_node::counter_observed(
                move |scope| {
                    let counter = match scope.cell.as_str() {
                        "Counter:snapshot" => &calls,
                        "Counter:healthy" => &healthy_calls,
                        _ => anyhow::bail!("unexpected snapshot fixture group"),
                    };
                    counter.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                },
                gate.clone(),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let mut namespace = native_snapshot_config();
    namespace.schema_digest = definitions[0].schema;
    let host = RaftHostOptions {
        checkpoint_entries: 4,
        ..Default::default()
    };
    let cluster = cluster::Cluster::start_with_transport(
        &definitions,
        vec![
            RaftCellRegistration {
                placement: RaftCellPlacement::Replica,
                namespace,
                class: "Counter".into(),
                birth: native_snapshot_birth("Counter:snapshot", 5),
            },
            RaftCellRegistration {
                placement: RaftCellPlacement::Replica,
                namespace: {
                    let mut namespace = native_snapshot_config();
                    namespace.schema_digest = definitions[0].schema;
                    namespace
                },
                class: "Counter".into(),
                birth: native_snapshot_birth("Counter:healthy", 6),
            },
        ],
        20,
        host,
        TransportOptions {
            snapshot_chunk_bytes: 257,
            io_timeout_ms: 1000,
            snapshot_timeout_ms: 1500,
            ..Default::default()
        },
    )
    .await?;
    let handles = cluster
        .services
        .iter()
        .map(|service| service.cell(NamespaceId::new([2; 16]).unwrap(), "Counter:snapshot"))
        .collect::<Result<Vec<_>, _>>()?;
    let primary = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            for (i, handle) in handles.iter().enumerate() {
                if handle
                    .status()
                    .await
                    .is_ok_and(|status| status.command_ready)
                {
                    return i;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("snapshot fixture initial leader")?;
    let follower = (primary + 1) % 3;
    let before = handles[follower]
        .status()
        .await
        .context("initial follower status")?
        .applied;
    cluster.isolate(follower, false).await?;
    let mut last = None;
    for sequence in 1..=12 {
        let options = public_node::options(sequence)?;
        let mut last_failure = None;
        let receipt = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match handles[primary]
                    .command(
                        definitions[primary].increment.clone(),
                        1i64.to_be_bytes().to_vec(),
                        options.clone(),
                    )
                    .await
                {
                    Ok(receipt) => return Ok::<_, anyhow::Error>(receipt),
                    Err(error @ (
                        CommandFailure::Unavailable
                        | CommandFailure::Overloaded
                        | CommandFailure::NotLeader { .. }
                        | CommandFailure::Ambiguous { .. }
                    )) => {
                        last_failure = Some(error);
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    Err(error) => return Err(error.into()),
                }
            }
        })
        .await
        .with_context(|| format!("snapshot fixture command {sequence}, primary {primary}, last failure {last_failure:?}"))??;
        assert_eq!(receipt.result, (sequence as i64).to_be_bytes());
        last = Some(receipt);
    }
    let last = last.unwrap();
    let cut = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let status = handles[primary]
                .status()
                .await
                .context("snapshot source checkpoint status")?;
            if status.checkpoint.index > before.index {
                return Ok::<_, anyhow::Error>(status.checkpoint);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("snapshot checkpoint past isolated follower")??;
    assert_eq!(calls[follower].load(Ordering::SeqCst), 0);
    let before_attempts = cluster.data_attempts(follower);
    let hold = if interrupt {
        Some(cluster.hold_data_responses_after(follower, primary, 4096)?)
    } else {
        None
    };
    cluster.heal();
    if interrupt {
        tokio::time::timeout(Duration::from_secs(10), async {
            while cluster.held_data_response_bytes(follower, primary) == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .context("partial snapshot response hold")?;
        let blocked = handles[follower]
            .query(
                definitions[follower].get.clone(),
                vec![],
                QueryOptions {
                    consistency: QueryConsistency::AtLeast(last.version_token.clone()),
                    timeout: Duration::from_millis(100),
                    ..Default::default()
                },
            )
            .await;
        ensure!(
            matches!(
                blocked,
                Err(CommandFailure::VersionUnavailable | CommandFailure::Unavailable)
            ),
            "an unfinished transfer supplied a query result: {blocked:?}"
        );
        let healthy = cluster
            .services
            .iter()
            .map(|service| service.cell(NamespaceId::new([2; 16]).unwrap(), "Counter:healthy"))
            .collect::<Result<Vec<_>, _>>()?;
        if matches!(healthy_leader, HealthyLeader::RecoveringNode) {
            // Exercise independent-group progress on the same node whose
            // snapshot download is held, rather than depending on election luck.
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            tokio::time::timeout_at(deadline, async {
                loop {
                    if healthy[follower]
                        .status()
                        .await
                        .context("recovering node healthy-group leadership status")?
                        .command_ready
                    {
                        return Ok::<_, anyhow::Error>(());
                    }
                    for (node, handle) in healthy.iter().enumerate() {
                        // The target can become leader after the observation
                        // above. Recheck it on the next loop; never request a
                        // transfer from the target back to itself.
                        if node == follower {
                            continue;
                        }
                        let status = handle.status().await.with_context(|| {
                            format!("healthy-group transfer source status on node {node}")
                        })?;
                        if status.command_ready {
                            match handle
                                .transfer_leadership(
                                    LeadershipTransfer {
                                        target: MemberId::new(follower as u64 + 1)?,
                                        expected_term: status.term,
                                        expected_revision: status.membership_revision,
                                    },
                                    deadline.saturating_duration_since(tokio::time::Instant::now()),
                                )
                                .await
                            {
                                Ok(())
                                | Err(
                                    CommandFailure::Overloaded
                                    | CommandFailure::NotLeader { .. }
                                    | CommandFailure::CommandConflict,
                                ) => (),
                                Err(error) => return Err(anyhow::Error::new(error).context(format!(
                                    "healthy-group transfer from node {node} to recovering node {follower}, term {:?}, revision {:?}",
                                    status.term, status.membership_revision
                                ))),
                            }
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .context("place healthy leader on snapshot recovery node")??;
        }
        let mut observed = [None, None, None];
        let healthy_primary = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                for (node, handle) in healthy.iter().enumerate() {
                    // Leadership belongs to each group. Recovering a snapshot
                    // for another group does not exclude this node's leader.
                    let status = handle.status().await;
                    let ready = status.as_ref().is_ok_and(|status| status.command_ready);
                    observed[node] = Some(status);
                    if ready {
                        return node;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .with_context(|| {
            format!("healthy group leader discovery during snapshot transfer: {observed:?}")
        })?;
        if matches!(healthy_leader, HealthyLeader::RecoveringNode) {
            assert_eq!(healthy_primary, follower, "forced healthy leader changed");
        }
        let options = public_node::options(1)?;
        let result = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match healthy[healthy_primary]
                    .command(
                        definitions[healthy_primary].increment.clone(),
                        1i64.to_be_bytes().to_vec(),
                        options.clone(),
                    )
                    .await
                {
                    Ok(receipt) => return Ok::<_, anyhow::Error>(receipt),
                    Err(
                        CommandFailure::Unavailable
                        | CommandFailure::Overloaded
                        | CommandFailure::NotLeader { .. }
                        | CommandFailure::Ambiguous { .. },
                    ) => tokio::time::sleep(Duration::from_millis(10)).await,
                    Err(error) => return Err(error.into()),
                }
            }
        })
        .await
        .context("healthy group progress during transfer")??;
        assert_eq!(result.result, 1i64.to_be_bytes());
        assert_eq!(healthy_calls[healthy_primary].load(Ordering::SeqCst), 1);
        assert_eq!(
            healthy_calls
                .iter()
                .map(|calls| calls.load(Ordering::SeqCst))
                .sum::<usize>(),
            1
        );
        assert_eq!(
            calls[follower].load(Ordering::SeqCst),
            0,
            "snapshot recovery replayed a business command"
        );
        assert_eq!(
            calls
                .iter()
                .map(|calls| calls.load(Ordering::SeqCst))
                .sum::<usize>(),
            12
        );
        if close_pending {
            tokio::time::timeout(Duration::from_secs(5), cluster.close())
                .await
                .context("joined native snapshot shutdown")??;
            drop(hold);
            return Ok(());
        }
        tokio::time::timeout(Duration::from_secs(10), async {
            while cluster.data_attempts(follower) < before_attempts + 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .context("snapshot retry after deadline")?;
    }
    drop(hold);
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if handles[follower]
                .status()
                .await
                .is_ok_and(|status| status.checkpoint.index >= cut.index)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("native snapshot installation")?;
    let read_started = tokio::time::Instant::now();
    let read = handles[follower]
        .query(
            definitions[follower].get.clone(),
            vec![],
            QueryOptions {
                consistency: QueryConsistency::AtLeast(last.version_token.clone()),
                timeout: Duration::from_secs(15),
                ..Default::default()
            },
        )
        .await;
    let read = match read {
        Ok(read) => read,
        Err(error) => {
            let elapsed = read_started.elapsed();
            // Observe each node independently so one unresponsive status call
            // cannot hide the other nodes or prevent an attempted joined stop.
            // These observations diagnose failure; they supply no read proof.
            let statuses = futures_util::future::join_all(
                handles
                    .iter()
                    .map(|handle| tokio::time::timeout(Duration::from_secs(1), handle.status())),
            )
            .await;
            let attempts = cluster.data_attempts(follower);
            let close = tokio::time::timeout(Duration::from_secs(5), cluster.close()).await;
            anyhow::bail!(
                "restored follower query: {error:?}; elapsed={elapsed:?}, primary={primary}, \
                 follower={follower}, cut={cut:?}, required={:?}, statuses={statuses:?}, \
                 transfer_attempts={attempts}, close={close:?}",
                last.version,
            );
        }
    };
    assert_eq!(read.result, 12i64.to_be_bytes());
    let status = handles[follower].status().await?;
    ensure!(
        status.checkpoint.index >= cut.index,
        "the compacted prefix must arrive through an installed snapshot"
    );
    ensure!(status.applied.index >= last.version.position.index);
    assert_eq!(
        calls[follower].load(Ordering::SeqCst),
        0,
        "followers must not replay business commands"
    );
    assert_eq!(
        calls
            .iter()
            .map(|calls| calls.load(Ordering::SeqCst))
            .sum::<usize>(),
        12
    );
    assert_eq!(
        healthy_calls
            .iter()
            .map(|calls| calls.load(Ordering::SeqCst))
            .sum::<usize>(),
        usize::from(interrupt),
        "healthy group execution count changed during snapshot recovery"
    );
    cluster.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_member_binding_and_retirement_survive_restart() -> Result<()> {
    let root = tempfile::tempdir()?;
    let source = Fixture::open(&root.path().join("source"), 1, LogOpenMode::Create).await?;
    let target = Fixture::open(&root.path().join("target"), 2, LogOpenMode::Create).await?;
    source.empty_configuration_cut().await?;
    let joined = SnapshotPackage::capture_empty(&source.group, [9; 32], SnapshotLimits::default())?;
    target.install(&joined).await?;
    let state = target.group.membership()?;
    let binding = state
        .joined(MemberId::new(4)?)?
        .context("missing committed binding")?;
    assert_eq!(binding.0.node, NodeId::new([4; 16])?);
    assert_eq!(binding.0.disk, DiskLineage::new([14; 16])?);
    assert_eq!(binding.1, LogIndex::new(3));
    target.close().await?;
    let target = Fixture::open(&root.path().join("target"), 2, LogOpenMode::Resume).await?;
    assert_eq!(target.group.membership()?, state);
    source.retire_learner().await?;
    let retired =
        SnapshotPackage::capture_empty(&source.group, [9; 32], SnapshotLimits::default())?;
    source
        .log
        .persist(OperationId::new(91), vec![retired.checkpoint()])
        .await?;
    assert_eq!(
        source.group.first_index()?,
        5,
        "configuration entries must be compacted away"
    );
    target.install(&retired).await?;
    assert_eq!(target.group.membership()?.high_water(), 4);
    assert!(target
        .group
        .membership()?
        .joined(MemberId::new(4)?)?
        .is_none());
    drop(joined);
    drop(retired);
    target.close().await?;
    source.close().await?;
    let target = Fixture::open(&root.path().join("target"), 2, LogOpenMode::Resume).await?;
    let state = target.group.membership()?;
    assert_eq!(state.high_water(), 4);
    let before = target.group.initial_state()?.conf_state;
    let mut after = before.clone();
    after.learners.push(4);
    let mut reuse = raft::prelude::ConfChangeV2::default();
    reuse.mut_changes().push(raft::prelude::ConfChangeSingle {
        node_id: 4,
        change_type: raft::prelude::ConfChangeType::AddLearnerNode,
        ..Default::default()
    });
    state.bind_change(
        &mut reuse,
        &[JoiningMember {
            member: MemberId::new(4)?,
            node: NodeId::new([8; 16])?,
            disk: DiskLineage::new([88; 16])?,
        }],
    )?;
    ensure!(
        state
            .applied(&reuse, &before, &after, LogIndex::new(5))
            .is_err(),
        "compaction/restart allowed a retired ID to be reused"
    );
    target.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_refuses_rebound_binding_and_lost_id_watermark() -> Result<()> {
    let root = tempfile::tempdir()?;
    let source = Fixture::open(&root.path().join("source"), 1, LogOpenMode::Create).await?;
    let target = Fixture::open(&root.path().join("target"), 2, LogOpenMode::Create).await?;
    source.empty_configuration_cut().await?;
    let joined = SnapshotPackage::capture_empty(&source.group, [9; 32], SnapshotLimits::default())?;
    target.install(&joined).await?;
    let mut rebound = joined.snapshot();
    let bytes = String::from_utf8(rebound.data.to_vec())?;
    let old = format!("\"disk\":{}", serde_json::to_string(&[14u8; 16])?);
    let new = format!("\"disk\":{}", serde_json::to_string(&[99u8; 16])?);
    ensure!(bytes.matches(&old).count() == 1);
    rebound.data = bytes
        .replace(&old, &new)
        .replace("\"index\":3,", "\"index\":4,")
        .replace("\"revision\":3,", "\"revision\":4,")
        .into_bytes()
        .into();
    rebound.mut_metadata().index = 4;
    assert!(
        matches!(
            SnapshotDownload::new(&target.group, [9; 32], rebound, SnapshotLimits::default()),
            Err(SnapshotError::Log(LogError::Invalid(_)))
        ),
        "a canonical higher snapshot rebound an existing identity"
    );
    source.retire_learner().await?;
    let retired =
        SnapshotPackage::capture_empty(&source.group, [9; 32], SnapshotLimits::default())?;
    target.install(&retired).await?;
    let mut lost = retired.snapshot();
    let bytes = String::from_utf8(lost.data.to_vec())?;
    ensure!(bytes.matches("\"high_water\":4,").count() == 1);
    lost.data = bytes
        .replace("\"high_water\":4,", "\"high_water\":3,")
        .replace("\"index\":4,", "\"index\":5,")
        .replace("\"revision\":4,", "\"revision\":5,")
        .into_bytes()
        .into();
    lost.mut_metadata().index = 5;
    assert!(
        matches!(
            SnapshotDownload::new(&target.group, [9; 32], lost, SnapshotLimits::default()),
            Err(SnapshotError::Log(LogError::Invalid(_)))
        ),
        "a canonical higher snapshot lost the retired-ID watermark"
    );
    ensure!(
        target.images.usage()?.generations == 0,
        "binding refusal allocated image state"
    );
    drop(joined);
    drop(retired);
    source.close().await?;
    target.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_same_cut_requires_member_binding_marker() -> Result<()> {
    let root = tempfile::tempdir()?;
    let source = Fixture::open(&root.path().join("source"), 1, LogOpenMode::Create).await?;
    let target = Fixture::open(&root.path().join("target"), 2, LogOpenMode::Create).await?;
    source.empty_configuration_cut().await?;
    source.retire_learner().await?;
    let package =
        SnapshotPackage::capture_empty(&source.group, [9; 32], SnapshotLimits::default())?;
    let snapshot = package.snapshot();
    let position = LogPosition {
        index: LogIndex::new(4),
        term: Term::new(2),
    };
    let mut write = GroupWrite::new(&target.group);
    write.snapshot = Some(SnapshotChange::Install {
        snapshot,
        applied: DurableApply {
            position,
            image: None,
        },
    });
    let mut hard = target.group.initial_state()?.hard_state;
    hard.term = 2;
    hard.commit = 4;
    write.hard_state = Some(hard);
    // Mechanical storage accepts opaque metadata. Normal image recovery must
    // independently refuse a snapshot whose binding marker was not installed.
    target
        .log
        .persist(OperationId::new(91), vec![write])
        .await?;
    ensure!(target.applier().is_err());
    drop(package);
    target.close().await?;
    let target = Fixture::open(&root.path().join("target"), 2, LogOpenMode::Resume).await?;
    ensure!(
        target.applier().is_err(),
        "restart admitted a mixed snapshot/member marker"
    );
    source.close().await?;
    target.close().await
}

impl Fixture {
    async fn retire_learner(&self) -> Result<()> {
        let before = self.group.initial_state()?.conf_state;
        let mut after = before.clone();
        after.learners.retain(|id| *id != 4);
        let mut change = raft::prelude::ConfChangeV2::default();
        change.mut_changes().push(raft::prelude::ConfChangeSingle {
            node_id: 4,
            change_type: raft::prelude::ConfChangeType::RemoveNode,
            ..Default::default()
        });
        let state = self.group.membership()?;
        state.bind_change(&mut change, &[])?;
        let index = self.group.applied()?.position.index.get() + 1;
        let mut write = GroupWrite::new(&self.group);
        write.membership = Some(state.applied(&change, &before, &after, LogIndex::new(index))?);
        write.entries.push(Entry {
            index,
            term: 2,
            entry_type: raft::prelude::EntryType::EntryConfChangeV2,
            data: change.write_to_bytes()?.into(),
            ..Default::default()
        });
        write.conf_state = Some(after);
        write.applied = Some(DurableApply {
            position: LogPosition {
                index: LogIndex::new(index),
                term: Term::new(2),
            },
            image: None,
        });
        let mut hard = self.group.initial_state()?.hard_state;
        hard.commit = index;
        write.hard_state = Some(hard);
        self.log.persist(OperationId::new(90), vec![write]).await?;
        Ok(())
    }
}

fn marker(pin: &GenerationPin) -> CommittedImage {
    let state = pin.state();
    CommittedImage {
        name: format!("{}.sqlite", hex::encode(pin.id().bytes())),
        digest: state.digest,
        length: state.length,
        ltx_epoch: state.epoch,
        txid: state.txid.0,
        checksum: state.checksum,
        page_size: state.page_size,
    }
}

struct Fixture {
    domain: ExecutionDomain,
    log: ReplicaLog,
    group: GroupLog,
    images: GenerationStore,
    bytes: Vec<u8>,
    txid: u8,
}

impl Fixture {
    async fn empty_configuration_cut(&self) -> Result<()> {
        let mut conf = self.group.initial_state()?.conf_state;
        conf.learners = vec![4];
        let mut change = raft::prelude::ConfChangeV2 {
            changes: vec![raft::prelude::ConfChangeSingle {
                node_id: 4,
                change_type: raft::prelude::ConfChangeType::AddLearnerNode,
                ..Default::default()
            }]
            .into(),
            ..Default::default()
        };
        let before = self.group.initial_state()?.conf_state;
        let membership = self.group.membership()?;
        membership.bind_change(
            &mut change,
            &[JoiningMember {
                member: MemberId::new(4)?,
                node: NodeId::new([4; 16])?,
                disk: DiskLineage::new([14; 16])?,
            }],
        )?;
        let mut write = GroupWrite::new(&self.group);
        write.membership = Some(membership.applied(&change, &before, &conf, LogIndex::new(3))?);
        write.entries = vec![
            Entry {
                index: 2,
                term: 2,
                ..Default::default()
            },
            Entry {
                index: 3,
                term: 2,
                entry_type: raft::prelude::EntryType::EntryConfChangeV2,
                data: change.write_to_bytes()?.into(),
                ..Default::default()
            },
        ];
        let mut hard = self.group.initial_state()?.hard_state;
        hard.term = 2;
        hard.vote = 1;
        hard.commit = 3;
        write.hard_state = Some(hard);
        write.conf_state = Some(conf);
        write.applied = Some(DurableApply {
            position: LogPosition {
                index: LogIndex::new(3),
                term: Term::new(2),
            },
            image: None,
        });
        self.log.persist(OperationId::new(8), vec![write]).await?;
        Ok(())
    }

    async fn open(root: &Path, member: u8, mode: LogOpenMode) -> Result<Self> {
        Self::with_filesystems(
            root,
            member,
            mode,
            Arc::new(DirectFileSystem),
            Arc::new(DirectFileSystem),
        )
        .await
    }

    async fn with_filesystems(
        root: &Path,
        member: u8,
        mode: LogOpenMode,
        image_fs: Arc<dyn FileSystem>,
        log_fs: Arc<dyn FileSystem>,
    ) -> Result<Self> {
        Self::open_born(root, member, mode, image_fs, log_fs, [9; 32]).await
    }

    async fn open_born(
        root: &Path,
        member: u8,
        mode: LogOpenMode,
        image_fs: Arc<dyn FileSystem>,
        log_fs: Arc<dyn FileSystem>,
        birth_digest: [u8; 32],
    ) -> Result<Self> {
        let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
        let log = domain
            .run(ReplicaLog::open(
                ReplicaLogOptions::new(
                    root.join("raft"),
                    NodeId::new([member; 16])?,
                    DiskLineage::new([member + 40; 16])?,
                    ProcessGeneration::new([member + 80; 16])?,
                    mode,
                ),
                log_fs,
            ))
            .await??;
        let group = match mode {
            LogOpenMode::Create => {
                log.create_group(GroupBootstrap {
                    scope: scope(),
                    member: MemberId::new(u64::from(member))?,
                    birth_digest,
                    voters: (1..=3).map(|id| MemberId::new(id).unwrap()).collect(),
                    learners: vec![],
                })
                .await?
            }
            LogOpenMode::Resume => log.group(&scope())?,
        };
        let limits = cellar::sdk::consensus::RaftHostOptions::default().images;
        let images = GenerationStore::open(root.join("images"), image_fs, limits)?;
        Ok(Self {
            domain,
            log,
            group,
            images,
            bytes: vec![],
            txid: 0,
        })
    }

    fn applier(&self) -> Result<ImageApplier> {
        Ok(ImageApplier::recover(
            self.group.clone(),
            self.images.clone(),
            [9; 32],
            EnvelopeLimits::default(),
        )?)
    }

    async fn commit(&mut self, value: i64) -> Result<()> {
        let next = images::image(
            (!self.bytes.is_empty()).then_some(self.bytes.as_slice()),
            &if self.txid == 0 {
                format!("CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES({value})")
            } else {
                format!("UPDATE counter SET value={value}")
            },
        )?;
        let current = self.applier()?.current();
        let checksums = current
            .as_ref()
            .is_none_or(|image| image.pin().state().checksum != 0);
        let cut = images::Cut::new(&self.bytes, &next, u64::from(self.txid), checksums, true);
        let pin = self.images.prepare(
            images::id(self.txid + 1),
            current.as_ref().map(|image| image.pin()).as_ref(),
            &cut.update(),
        )?;
        let position = LogPosition {
            index: LogIndex::new(self.group.applied()?.position.index.get() + 1),
            term: Term::new(2),
        };
        let payload = self.payload(&cut)?;
        let mut write = GroupWrite::new(&self.group);
        write.entries.push(Entry {
            index: position.index.get(),
            term: position.term.get(),
            data: payload.into(),
            ..Default::default()
        });
        let mut hard = self.group.initial_state()?.hard_state;
        hard.term = 2;
        hard.vote = 1;
        hard.commit = position.index.get();
        write.hard_state = Some(hard);
        write.applied = Some(DurableApply {
            position,
            image: Some(marker(&pin)),
        });
        self.log.persist(OperationId::new(1), vec![write]).await?;
        self.bytes = next;
        self.txid += 1;
        Ok(())
    }

    fn payload(&self, cut: &images::Cut) -> Result<Vec<u8>> {
        let ltx_image = |state: cellar_ltx::apply::ImageState| LtxImage {
            epoch: state.epoch,
            txid: state.txid.0,
            checksum: state.checksum,
            image_digest: state.digest,
        };
        Ok(CellUpdate {
            scope: scope(),
            application_digest: [9; 32],
            proposal: ProposalId::new([self.txid + 1; 16])?,
            base: ltx_image(cut.base),
            result: ltx_image(cut.result),
            files: vec![LtxUpdate {
                min_txid: cut.base.txid.0 + 1,
                max_txid: cut.result.txid.0,
                page_count: cut.pages.len() as u64,
                decoded_bytes: cut.pages.len() as u64 * 512,
                bytes: cut.bytes.clone(),
            }],
        }
        .encode(EnvelopeLimits::default())?)
    }

    fn package(&self) -> Result<SnapshotPackage> {
        Ok(SnapshotPackage::capture(
            &self.group,
            self.applier()?.current().unwrap(),
            SnapshotLimits::default(),
        )?)
    }

    async fn install(&self, package: &SnapshotPackage) -> Result<()> {
        let snapshot = package.snapshot();
        let mut write = GroupWrite::new(&self.group);
        write.membership = Some(package.membership());
        let position = LogPosition {
            index: LogIndex::new(snapshot.get_metadata().index),
            term: Term::new(snapshot.get_metadata().term),
        };
        write.snapshot = Some(SnapshotChange::Install {
            snapshot,
            applied: DurableApply {
                position,
                image: package.source_pin().as_ref().map(marker),
            },
        });
        let mut hard = self.group.initial_state()?.hard_state;
        hard.term = hard.term.max(position.term.get());
        hard.commit = position.index.get();
        write.hard_state = Some(hard);
        self.log.persist(OperationId::new(2), vec![write]).await?;
        Ok(())
    }

    async fn close(self) -> Result<()> {
        self.log.close().await?;
        self.domain.shutdown().await;
        Ok(())
    }
}

fn download(package: &SnapshotPackage, target: &Fixture) -> Result<PreparedSnapshot> {
    let mut download = SnapshotDownload::new(
        &target.group,
        [9; 32],
        package.snapshot(),
        SnapshotLimits::default(),
    )?;
    for offset in (0..package.encoded_length()).step_by(257) {
        let bytes = package.chunk(offset, 257.min(package.encoded_length() - offset))?;
        download.append(offset, bytes)?;
    }
    Ok(download.finish(&target.images)?)
}

fn value(pin: GenerationPin) -> Result<i64> {
    let query = images::query(pin)?;
    Ok(query
        .connection()
        .query_row("SELECT value FROM counter", [], |row| row.get(0))?)
}

async fn checkpointed(events: &mut RuntimeEvents) -> Result<ApplyTicket> {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            match events.recv().await {
                Some(RuntimeEvent {
                    kind: RuntimeEventKind::Checkpointed { ticket },
                    ..
                }) => return Ok(ticket),
                Some(RuntimeEvent {
                    kind: RuntimeEventKind::Failed { error, .. },
                    ..
                }) => anyhow::bail!("checkpoint failed: {error}"),
                Some(_) => (),
                None => anyhow::bail!("checkpoint event stream closed"),
            }
        }
    })
    .await?
}

fn runtime_options() -> RaftRuntimeOptions {
    RaftRuntimeOptions {
        tick_interval_ms: None,
        election_seed: Some([18; 32]),
        ..Default::default()
    }
}

async fn next_kind(events: &mut RuntimeEvents) -> Result<RuntimeEventKind> {
    let event = tokio::time::timeout(std::time::Duration::from_secs(10), events.recv())
        .await?
        .ok_or_else(|| anyhow::anyhow!("runtime event stream closed"))?;
    if let RuntimeEventKind::Failed { error, .. } = event.kind {
        anyhow::bail!("runtime failed: {error}");
    }
    Ok(event.kind)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_abort_preserves_vote_and_committed_image() -> Result<()> {
    for incoming_term in [3, 4] {
        let root = tempfile::tempdir()?;
        let mut source = Fixture::open(&root.path().join("source"), 1, LogOpenMode::Create).await?;
        let mut target = Fixture::open(&root.path().join("target"), 2, LogOpenMode::Create).await?;
        source.commit(1).await?;
        source.commit(2).await?;
        target.commit(1).await?;
        let mut hard = target.group.initial_state()?.hard_state;
        hard.term = 3;
        hard.vote = 2;
        let mut write = GroupWrite::new(&target.group);
        write.hard_state = Some(hard);
        target.log.persist(OperationId::new(4), vec![write]).await?;
        let (runtime, mut events) = target
            .domain
            .run(RaftRuntime::start(target.log.clone(), runtime_options()))
            .await??;
        runtime
            .checkpoint(target.package()?.checkpoint_material())
            .await?;
        checkpointed(&mut events).await?;
        let package = source.package()?;
        runtime
            .step(
                &scope(),
                raft::prelude::Message {
                    from: 1,
                    to: 2,
                    term: incoming_term,
                    msg_type: raft::prelude::MessageType::MsgSnapshot,
                    snapshot: Some(package.snapshot()).into(),
                    ..Default::default()
                },
            )
            .await?;
        let ticket = loop {
            if let RuntimeEventKind::Snapshot { ticket, .. } = next_kind(&mut events).await? {
                break ticket;
            }
        };
        let prepared = download(&package, &target)?;
        let late = prepared.completion(&ticket)?;
        runtime.abort_snapshot(ticket.clone()).await?;
        loop {
            match next_kind(&mut events).await? {
                RuntimeEventKind::SnapshotAborted {
                    ticket: aborted,
                    applied,
                } => {
                    assert_eq!(aborted, ticket);
                    assert_eq!(applied.position.index.get(), 2);
                    break;
                }
                RuntimeEventKind::SnapshotInstalled { .. } => {
                    anyhow::bail!("aborted snapshot installed")
                }
                RuntimeEventKind::Message { message, .. } => ensure!(
                    message.get_msg_type() != raft::prelude::MessageType::MsgAppendResponse,
                    "aborted snapshot must not acknowledge installation"
                ),
                _ => (),
            }
        }
        ensure!(matches!(
            runtime.complete_snapshot(late).await,
            Err(RuntimeError::StaleCompletion)
        ));
        ensure!(prepared.publish(&mut target.applier()?).is_err());
        let hard = target.group.initial_state()?.hard_state;
        assert_eq!(hard.term, incoming_term);
        assert_eq!(hard.vote, if incoming_term == 3 { 2 } else { 0 });
        assert_eq!(hard.commit, 2);
        assert_eq!(target.group.snapshot(0, 1)?.get_metadata().index, 2);
        assert_eq!(value(target.applier()?.current().unwrap().pin())?, 1);
        runtime
            .step(
                &scope(),
                raft::prelude::Message {
                    from: 3,
                    to: 2,
                    term: incoming_term,
                    index: 2,
                    log_term: 2,
                    msg_type: raft::prelude::MessageType::MsgRequestVote,
                    ..Default::default()
                },
            )
            .await?;
        loop {
            if let RuntimeEventKind::Message { message, .. } = next_kind(&mut events).await? {
                if message.get_msg_type() == raft::prelude::MessageType::MsgRequestVoteResponse {
                    assert_eq!(message.to, 3);
                    assert_eq!(message.reject, incoming_term == 3);
                    assert_eq!(
                        target.group.initial_state()?.hard_state.vote,
                        if incoming_term == 3 { 2 } else { 3 }
                    );
                    break;
                }
            }
        }
        runtime.close().await?;
        target.close().await?;
        let target = Fixture::open(&root.path().join("target"), 2, LogOpenMode::Resume).await?;
        assert_eq!(value(target.applier()?.current().unwrap().pin())?, 1);
        assert_eq!(target.group.initial_state()?.hard_state.commit, 2);
        assert_eq!(target.group.snapshot(0, 1)?.get_metadata().index, 2);
        target.close().await?;
        drop(package);
        source.close().await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_abort_joins_older_apply_and_allows_retry() -> Result<()> {
    let root = tempfile::tempdir()?;
    let gate = log_fault::Gate::new(None);
    let mut source = Fixture::open(&root.path().join("source"), 1, LogOpenMode::Create).await?;
    let target = Fixture::with_filesystems(
        &root.path().join("target"),
        2,
        LogOpenMode::Create,
        Arc::new(DirectFileSystem),
        Arc::new(log_fault::FaultFileSystem { gate: gate.clone() }),
    )
    .await?;
    for value in 1..=4 {
        source.commit(value).await?;
    }
    let package = source.package()?;
    let entries = source
        .group
        .entries(2, 4, None, raft::GetEntriesContext::empty(false))?;
    let (runtime, mut events) = target
        .domain
        .run(RaftRuntime::start(target.log.clone(), runtime_options()))
        .await??;
    runtime
        .step(
            &scope(),
            raft::prelude::Message {
                from: 1,
                to: 2,
                term: 2,
                index: 1,
                log_term: 1,
                commit: 2,
                entries: entries.into(),
                msg_type: raft::prelude::MessageType::MsgAppend,
                ..Default::default()
            },
        )
        .await?;
    let (apply_ticket, entry) = loop {
        if let RuntimeEventKind::Apply { ticket, entry } = next_kind(&mut events).await? {
            break (ticket, entry);
        }
    };
    assert_eq!(target.group.last_index()?, 3);
    let snapshot_message = raft::prelude::Message {
        from: 1,
        to: 2,
        term: 3,
        msg_type: raft::prelude::MessageType::MsgSnapshot,
        snapshot: Some(package.snapshot()).into(),
        ..Default::default()
    };
    runtime.step(&scope(), snapshot_message.clone()).await?;
    let ticket = loop {
        if let RuntimeEventKind::Snapshot { ticket, .. } = next_kind(&mut events).await? {
            break ticket;
        }
    };
    runtime.abort_snapshot(ticket.clone()).await?;
    while let Ok(Some(event)) =
        tokio::time::timeout(std::time::Duration::from_millis(20), events.recv()).await
    {
        ensure!(
            matches!(event.kind, RuntimeEventKind::Persisted { .. }),
            "abort must await the physical owner: {:?}",
            event.kind
        );
    }
    assert_eq!(target.group.applied()?.position.index.get(), 1);
    let mut applier = target.applier()?;
    let completion = applier.prepare(apply_ticket.clone(), &entry, images::id(1))?;
    let release = gate.arm(log_fault::HOLD_SYNC);
    runtime.complete_apply(completion.clone()).await?;
    tokio::time::timeout(std::time::Duration::from_secs(10), gate.entered.notified()).await?;
    ensure!(matches!(
        runtime.complete_apply(completion).await,
        Err(RuntimeError::StaleCompletion)
    ));
    runtime.abort_snapshot(ticket.clone()).await?;
    drop(release);
    let mut published = false;
    loop {
        match next_kind(&mut events).await? {
            RuntimeEventKind::Applied {
                ticket: old,
                applied,
            } => {
                assert_eq!(old, apply_ticket);
                assert_eq!(applied.position.index.get(), 2);
                assert_eq!(value(applier.publish(&old)?.pin())?, 1);
                published = true;
            }
            RuntimeEventKind::SnapshotAborted {
                ticket: aborted,
                applied,
            } => {
                assert_eq!(aborted, ticket);
                ensure!(
                    published,
                    "older image publication must precede abort completion"
                );
                assert_eq!(applied.position.index.get(), 2);
                break;
            }
            RuntimeEventKind::SnapshotInstalled { .. } => {
                anyhow::bail!("aborted snapshot installed")
            }
            _ => (),
        }
    }
    let hard = target.group.initial_state()?.hard_state;
    assert_eq!((hard.term, hard.vote, hard.commit), (3, 0, 2));
    assert_eq!(
        target.group.last_index()?,
        3,
        "uncommitted suffix is retained"
    );
    let stale = download(&package, &target)?.completion(&ticket)?;
    runtime.step(&scope(), snapshot_message).await?;
    let retry = loop {
        if let RuntimeEventKind::Snapshot { ticket, .. } = next_kind(&mut events).await? {
            break ticket;
        }
    };
    ensure!(
        retry != ticket,
        "a retry must receive a fresh operation capability"
    );
    ensure!(matches!(
        runtime.complete_snapshot(stale).await,
        Err(RuntimeError::StaleCompletion)
    ));
    let prepared = download(&package, &target)?;
    runtime
        .complete_snapshot(prepared.completion(&retry)?)
        .await?;
    ensure!(matches!(
        runtime.abort_snapshot(retry.clone()).await,
        Err(RuntimeError::StaleCompletion)
    ));
    loop {
        if let RuntimeEventKind::SnapshotInstalled { ticket, applied } =
            next_kind(&mut events).await?
        {
            assert_eq!(ticket, retry);
            assert_eq!(applied.position.index.get(), 5);
            break;
        }
    }
    assert_eq!(value(prepared.publish(&mut applier)?.pin())?, 4);
    runtime.close().await?;
    drop(applier);
    drop(package);
    source.close().await?;
    target.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_abort_sync_failure_refuses_progress_and_recovers_prefix() -> Result<()> {
    let root = tempfile::tempdir()?;
    let gate = log_fault::Gate::new(None);
    let mut source = Fixture::open(&root.path().join("source"), 1, LogOpenMode::Create).await?;
    let mut target = Fixture::with_filesystems(
        &root.path().join("target"),
        2,
        LogOpenMode::Create,
        Arc::new(DirectFileSystem),
        Arc::new(log_fault::FaultFileSystem { gate: gate.clone() }),
    )
    .await?;
    source.commit(1).await?;
    source.commit(2).await?;
    target.commit(1).await?;
    let package = source.package()?;
    let prepared = download(&package, &target)?;
    let (runtime, mut events) = target
        .domain
        .run(RaftRuntime::start(target.log.clone(), runtime_options()))
        .await??;
    runtime
        .step(
            &scope(),
            raft::prelude::Message {
                from: 1,
                to: 2,
                term: 3,
                msg_type: raft::prelude::MessageType::MsgSnapshot,
                snapshot: Some(package.snapshot()).into(),
                ..Default::default()
            },
        )
        .await?;
    let ticket = loop {
        if let RuntimeEventKind::Snapshot { ticket, .. } = next_kind(&mut events).await? {
            break ticket;
        }
    };
    let _release = gate.arm(log_fault::FAIL_SYNC);
    runtime.abort_snapshot(ticket).await?;
    loop {
        // An engine-wide failure closes the worker's event stream; a scoped
        // failure may instead emit Failed. Neither may release an install/abort
        // completion or a dependent Raft acknowledgement.
        let Some(event) =
            tokio::time::timeout(std::time::Duration::from_secs(10), events.recv()).await?
        else {
            break;
        };
        match event.kind {
            RuntimeEventKind::Failed { .. } => break,
            RuntimeEventKind::SnapshotInstalled { .. }
            | RuntimeEventKind::SnapshotAborted { .. }
            | RuntimeEventKind::Message { .. } => anyhow::bail!("failed abort released progress"),
            _ => (),
        }
    }
    ensure!(runtime.status(&scope()).await.is_err());
    ensure!(runtime.close().await.is_err());
    ensure!(target.log.close().await.is_err());
    target.domain.shutdown().await;
    drop(prepared);
    drop(target);
    let target = Fixture::open(&root.path().join("target"), 2, LogOpenMode::Resume).await?;
    let hard = target.group.initial_state()?.hard_state;
    ensure!(
        (hard.term, hard.vote) == (2, 1) || (hard.term, hard.vote) == (3, 0),
        "an uncertain write may preserve either coherent HardState"
    );
    assert_eq!(hard.commit, 2);
    assert_eq!(target.group.snapshot(0, 1)?.get_metadata().index, 1);
    assert_eq!(value(target.applier()?.current().unwrap().pin())?, 1);
    assert_eq!(target.images.usage()?.generations, 1);
    target.close().await?;
    drop(package);
    source.close().await
}

/// Exactly the admitted 64-MiB image, with incompressible overflow pages. The
/// SQLite record header has six bytes and 39 local bytes; each overflow page
/// carries 508 bytes. Three root pages plus the chain fill 131,072 pages.
fn maximum_image() -> Result<(Vec<u8>, usize)> {
    use rand::{RngCore, SeedableRng};
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("maximum.sqlite");
    let connection = rusqlite::Connection::open(&path)?;
    connection.execute_batch("PRAGMA page_size=512;PRAGMA journal_mode=MEMORY;PRAGMA synchronous=OFF;PRAGMA max_page_count=131072;CREATE TABLE counter(value INTEGER);INSERT INTO counter VALUES(1);CREATE TABLE payloads(id INTEGER PRIMARY KEY,value BLOB);")?;
    let length = (131_072 - 3) * 508 + 33;
    let mut payload = vec![0; length];
    rand::rngs::StdRng::seed_from_u64(212).fill_bytes(&mut payload);
    connection.execute("INSERT INTO payloads VALUES(1,?1)", [payload])?;
    assert_eq!(
        connection.query_row("PRAGMA integrity_check", [], |row| row.get::<_, String>(0))?,
        "ok"
    );
    drop(connection);
    let bytes = std::fs::read(path)?;
    assert_eq!(bytes.len(), 64 * 1024 * 1024);
    Ok((bytes, length))
}

#[test]
fn snapshot_limits_require_full_encoded_image_capacity() -> Result<()> {
    let mut limits = SnapshotLimits::default();
    let bound = cellar_ltx::ltx::full_snapshot_bound(limits.max_image_bytes, 512).unwrap();
    ensure!(bound > limits.max_image_bytes as usize);
    limits.max_encoded_bytes = bound - 1;
    ensure!(limits.validate().is_err());
    limits.max_encoded_bytes = bound;
    limits.validate()?;
    ensure!(cellar_ltx::ltx::full_snapshot_bound(u64::MAX, 512).is_none());
    ensure!(cellar_ltx::ltx::full_snapshot_bound(512, 513).is_none());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn maximum_image_retained_snapshot_and_archive_roundtrip() -> Result<()> {
    let root = tempfile::tempdir()?;
    let store = Arc::new(object_store::memory::InMemory::new());
    let namespace = Catalog::new(
        store,
        CatalogOptions {
            prefix: "maximum-image".into(),
            ..Default::default()
        },
    )?
    .open_namespace(NamespaceConfig::new(
        scope().cluster,
        scope().namespace,
        cellar_logic::NamespaceMode::Raft,
        [9; 32],
    ))
    .await?;
    let birth = namespace
        .resolve_birth(BirthProposal {
            scope: scope(),
            voters: (1..=3)
                .map(|member| InitialMember {
                    member: MemberId::new(member).unwrap(),
                    node: NodeId::new([member as u8; 16]).unwrap(),
                })
                .collect(),
            learners: vec![],
        })
        .await?;
    let mut source = Fixture::open_born(
        &root.path().join("source"),
        1,
        LogOpenMode::Create,
        Arc::new(DirectFileSystem),
        Arc::new(DirectFileSystem),
        birth.digest(),
    )
    .await?;
    let target = Fixture::open_born(
        &root.path().join("target"),
        2,
        LogOpenMode::Create,
        Arc::new(DirectFileSystem),
        Arc::new(DirectFileSystem),
        birth.digest(),
    )
    .await?;
    let (bytes, payload_length) = maximum_image()?;
    let cut = images::Cut::new(&[], &bytes, 0, false, true);
    ensure!(
        cut.bytes.len() > 64 * 1024 * 1024,
        "the encoded payload must exercise the former limit"
    );
    assert!(
        cut.bytes.len() <= cellar_ltx::ltx::full_snapshot_bound(bytes.len() as u64, 512).unwrap()
    );
    let pin = source.images.prepare(images::id(1), None, &cut.update())?;
    drop(cut);
    let mut write = GroupWrite::new(&source.group);
    write.entries.push(Entry {
        index: 2,
        term: 2,
        ..Default::default()
    });
    let mut hard = source.group.initial_state()?.hard_state;
    hard.term = 2;
    hard.vote = 1;
    hard.commit = 2;
    write.hard_state = Some(hard);
    // This component fixture selects a real, completely verified SQLite image.
    // The seed is not evidence of a 64-MiB SDK command/envelope admission.
    write.applied = Some(DurableApply {
        position: LogPosition {
            index: LogIndex::new(2),
            term: Term::new(2),
        },
        image: Some(marker(&pin)),
    });
    source.log.persist(OperationId::new(1), vec![write]).await?;
    drop(pin);
    source.bytes = bytes;
    source.txid = 1;
    let package = source.package()?;
    ensure!(package.encoded_length() > source.bytes.len());
    let expected = package.snapshot();
    let (runtime, mut events) = source
        .domain
        .run(RaftRuntime::start(source.log.clone(), runtime_options()))
        .await??;
    runtime.checkpoint(package.checkpoint_material()).await?;
    checkpointed(&mut events).await?;
    runtime.close().await?;
    drop(package);

    let archive = source
        .domain
        .run(async {
            namespace.cell_archive(
                &birth,
                cellar_ltx::LtxHost::default(),
                ArchiveLimits::default(),
            )
        })
        .await??;
    let verifier = ArchiveVerifier::new(
        GenerationStore::open(
            root.path().join("verifier"),
            Arc::new(DirectFileSystem),
            cellar::sdk::consensus::RaftHostOptions::default().images,
        )?,
        images::id(91),
    )?;
    let published = source.applier()?.current().unwrap();
    let cut =
        CommittedArchiveCut::capture(&source.group, published, vec![], ArchiveLimits::default())?;
    let set = archive.publish(cut, &verifier).await?;
    assert_eq!(set.position().index.get(), 2);
    drop(set);

    source.commit(2).await?;
    assert_eq!(value(source.applier()?.current().unwrap().pin())?, 2);
    let package = SnapshotPackage::retained(
        &source.group,
        &source.images,
        [9; 32],
        &expected,
        SnapshotLimits::default(),
    )?;
    assert_eq!(package.snapshot(), expected);
    let mut download =
        SnapshotDownload::new(&target.group, [9; 32], expected, SnapshotLimits::default())?;
    for offset in (0..package.encoded_length()).step_by(1024 * 1024) {
        download.append(
            offset,
            package.chunk(offset, (1024 * 1024).min(package.encoded_length() - offset))?,
        )?;
    }
    let prepared = download.finish(&target.images)?;
    target.install(&package).await?;
    let mut applier = target.applier()?;
    let image = prepared.publish(&mut applier)?;
    assert_eq!(
        value(image.pin())?,
        1,
        "retained cut must not silently become the newer image"
    );
    assert_eq!(image.pin().state().length, 64 * 1024 * 1024);
    let query = images::query(image.pin())?;
    assert_eq!(
        query
            .connection()
            .query_row("SELECT length(value) FROM payloads", [], |row| row
                .get::<_, usize>(0))?,
        payload_length
    );
    drop(query);
    drop(image);
    drop(applier);
    drop(package);

    let cut = CommittedArchiveCut::capture(
        &source.group,
        source.applier()?.current().unwrap(),
        vec![],
        ArchiveLimits::default(),
    )?;
    let set = archive.publish(cut, &verifier).await?;
    assert_eq!(set.position().index.get(), 3);
    assert_eq!(set.tail_entries(), 1);
    drop(set);
    drop(verifier);
    drop(archive);
    source.close().await?;
    target.close().await?;
    std::fs::remove_dir_all(root.path().join("source"))?;
    std::fs::remove_dir_all(root.path().join("target"))?;

    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let archive = domain
        .run(async {
            namespace.cell_archive(
                &birth,
                cellar_ltx::LtxHost::default(),
                ArchiveLimits::default(),
            )
        })
        .await??;
    let set = archive.recovery_set().await?.unwrap();
    assert_eq!(set.position().index.get(), 3);
    let restored = GenerationStore::open(
        root.path().join("isolated-restore"),
        Arc::new(DirectFileSystem),
        cellar::sdk::consensus::RaftHostOptions::default().images,
    )?;
    let pin = set.restore(&restored, images::id(93))?;
    assert_eq!(value(pin.clone())?, 2);
    assert_eq!(pin.state().length, 64 * 1024 * 1024);
    let query = images::query(pin)?;
    assert_eq!(
        query
            .connection()
            .query_row("SELECT length(value) FROM payloads", [], |row| row
                .get::<_, usize>(0))?,
        payload_length
    );
    drop(query);
    domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retained_checkpoint_capture_never_substitutes_newer_state() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut source = Fixture::open(&root.path().join("source"), 1, LogOpenMode::Create).await?;
    let target = Fixture::open(&root.path().join("target"), 2, LogOpenMode::Create).await?;
    source.commit(1).await?;
    let original = source.package()?;
    let expected = original.snapshot();
    let (runtime, mut events) = source
        .domain
        .run(RaftRuntime::start(source.log.clone(), runtime_options()))
        .await??;
    runtime.checkpoint(original.checkpoint_material()).await?;
    checkpointed(&mut events).await?;
    runtime.close().await?;
    drop(original);
    source.commit(2).await?;
    let retained = SnapshotPackage::retained(
        &source.group,
        &source.images,
        [9; 32],
        &expected,
        SnapshotLimits::default(),
    )?;
    assert_eq!(retained.snapshot(), expected);
    ensure!(SnapshotPackage::retained(
        &source.group,
        &source.images,
        [8; 32],
        &expected,
        SnapshotLimits::default()
    )
    .is_err());
    let next = source.package()?;
    let (runtime, mut events) = source
        .domain
        .run(RaftRuntime::start(source.log.clone(), runtime_options()))
        .await??;
    runtime.checkpoint(next.checkpoint_material()).await?;
    checkpointed(&mut events).await?;
    drop(source.applier()?);
    source.images.retire(images::id(1))?;
    source.images.reclaim(4096)?;
    ensure!(
        SnapshotPackage::retained(
            &source.group,
            &source.images,
            [9; 32],
            &expected,
            SnapshotLimits::default()
        )
        .is_err(),
        "a new request for the replaced cut must not return a different checkpoint"
    );
    let prepared = download(&retained, &target)?;
    target.install(&retained).await?;
    let mut applier = target.applier()?;
    assert_eq!(
        value(prepared.publish(&mut applier)?.pin())?,
        1,
        "an already captured transfer remains coherent after checkpoint replacement and GC"
    );
    drop(applier);
    drop(retained);
    drop(next);
    runtime.close().await?;
    source.close().await?;
    target.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retained_empty_checkpoint_has_no_image_or_payload() -> Result<()> {
    let root = tempfile::tempdir()?;
    let source = Fixture::open(root.path(), 1, LogOpenMode::Create).await?;
    source.empty_configuration_cut().await?;
    let original =
        SnapshotPackage::capture_empty(&source.group, [9; 32], SnapshotLimits::default())?;
    let expected = original.snapshot();
    let (runtime, mut events) = source
        .domain
        .run(RaftRuntime::start(source.log.clone(), runtime_options()))
        .await??;
    runtime.checkpoint(original.checkpoint_material()).await?;
    checkpointed(&mut events).await?;
    drop(original);
    let retained = SnapshotPackage::retained(
        &source.group,
        &source.images,
        [9; 32],
        &expected,
        SnapshotLimits::default(),
    )?;
    assert_eq!(retained.snapshot(), expected);
    assert!(retained.source_pin().is_none());
    assert_eq!(retained.encoded_length(), 0);
    assert_eq!(source.images.usage()?.generations, 0);
    drop(retained);
    runtime.close().await?;
    source.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_configuration_snapshot_installs_then_applies_first_ltx() -> Result<()> {
    let root = tempfile::tempdir()?;
    let source = Fixture::open(&root.path().join("source"), 1, LogOpenMode::Create).await?;
    let target = Fixture::open(&root.path().join("target"), 3, LogOpenMode::Create).await?;
    source.empty_configuration_cut().await?;
    let package =
        SnapshotPackage::capture_empty(&source.group, [9; 32], SnapshotLimits::default())?;
    assert!(package.source_pin().is_none());
    assert_eq!(package.encoded_length(), 0);
    let (source_runtime, mut source_events) = source
        .domain
        .run(RaftRuntime::start(source.log.clone(), runtime_options()))
        .await??;
    source_runtime
        .checkpoint(package.checkpoint_material())
        .await?;
    assert_eq!(
        checkpointed(&mut source_events)
            .await?
            .position()
            .index
            .get(),
        3
    );
    assert_eq!(source.images.usage()?.generations, 0);
    let (runtime, mut events) = target
        .domain
        .run(RaftRuntime::start(target.log.clone(), runtime_options()))
        .await??;
    let mut applier = target.applier()?;
    let mut message = raft::prelude::Message {
        from: 1,
        to: 3,
        term: 2,
        ..Default::default()
    };
    message.set_msg_type(raft::prelude::MessageType::MsgSnapshot);
    message.set_snapshot(package.snapshot());
    runtime.step(&scope(), message).await?;
    let mut prepared = None;
    let mut installed = false;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    while !installed && tokio::time::Instant::now() < deadline {
        if let Ok(event) = events.try_recv() {
            match event.kind {
                RuntimeEventKind::Snapshot {
                    ticket, snapshot, ..
                } => {
                    let image = SnapshotDownload::new(
                        &target.group,
                        [9; 32],
                        snapshot,
                        SnapshotLimits::default(),
                    )?
                    .finish(&target.images)?;
                    let completion = image.completion(&ticket)?;
                    assert!(completion.applied.image.is_none());
                    runtime.complete_snapshot(completion).await?;
                    prepared = Some(image);
                }
                RuntimeEventKind::SnapshotInstalled { .. } => {
                    prepared.take().unwrap().publish_empty(&mut applier)?;
                    installed = true;
                }
                RuntimeEventKind::Failed { error, .. } => {
                    anyhow::bail!("empty snapshot failed: {error}")
                }
                _ => (),
            }
        }
        tokio::task::yield_now().await;
    }
    ensure!(installed, "empty snapshot must install");
    assert!(applier.current().is_none());
    assert_eq!(target.images.usage()?.generations, 0);
    assert_eq!(target.group.initial_state()?.conf_state.learners, vec![4]);
    assert_eq!(target.group.applied()?.position.index.get(), 3);

    let bytes = images::image(
        None,
        "CREATE TABLE counter(value INTEGER);INSERT INTO counter VALUES(1)",
    )?;
    let cut = images::Cut::new(&[], &bytes, 0, true, true);
    let mut message = raft::prelude::Message {
        from: 1,
        to: 3,
        term: 2,
        index: 3,
        log_term: 2,
        commit: 4,
        entries: vec![Entry {
            index: 4,
            term: 2,
            data: source.payload(&cut)?.into(),
            ..Default::default()
        }]
        .into(),
        ..Default::default()
    };
    message.set_msg_type(raft::prelude::MessageType::MsgAppend);
    runtime.step(&scope(), message).await?;
    let mut applied = false;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    while !applied && tokio::time::Instant::now() < deadline {
        if let Ok(event) = events.try_recv() {
            match event.kind {
                RuntimeEventKind::Apply { ticket, entry } => {
                    runtime
                        .complete_apply(applier.prepare(ticket, &entry, images::id(1))?)
                        .await?;
                }
                RuntimeEventKind::Applied { ticket, .. } => {
                    assert_eq!(value(applier.publish(&ticket)?.pin())?, 1);
                    applied = true;
                }
                RuntimeEventKind::Failed { error, .. } => {
                    anyhow::bail!("first image failed: {error}")
                }
                _ => (),
            }
        }
        tokio::task::yield_now().await;
    }
    ensure!(applied, "first LTX command after empty install must apply");
    runtime.close().await?;
    source_runtime.close().await?;
    drop(applier);
    drop(prepared);
    drop(package);
    source.close().await?;
    target.close().await?;
    let target = Fixture::open(&root.path().join("target"), 3, LogOpenMode::Resume).await?;
    assert_eq!(value(target.applier()?.current().unwrap().pin())?, 1);
    assert_eq!(target.group.snapshot(0, 1)?.get_metadata().index, 3);
    assert_eq!(target.images.usage()?.generations, 1);
    target.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_snapshot_refuses_data_and_cannot_erase_committed_image() -> Result<()> {
    let root = tempfile::tempdir()?;
    let source = Fixture::open(&root.path().join("source"), 1, LogOpenMode::Create).await?;
    let mut target = Fixture::open(&root.path().join("target"), 2, LogOpenMode::Create).await?;
    source.empty_configuration_cut().await?;
    target.commit(9).await?;
    ensure!(
        SnapshotPackage::capture_empty(&target.group, [9; 32], SnapshotLimits::default()).is_err()
    );
    let package =
        SnapshotPackage::capture_empty(&source.group, [9; 32], SnapshotLimits::default())?;
    let mut download = SnapshotDownload::new(
        &target.group,
        [9; 32],
        package.snapshot(),
        SnapshotLimits::default(),
    )?;
    ensure!(download.append(0, &[1]).is_err());
    let prepared = download.finish(&target.images)?;
    let mut applier = target.applier()?;
    ensure!(prepared.publish_empty(&mut applier).is_err());
    ensure!(target.install(&package).await.is_err());
    assert_eq!(value(applier.current().unwrap().pin())?, 9);
    assert_eq!(target.group.applied()?.position.index.get(), 2);
    drop(applier);
    drop(package);
    source.close().await?;
    target.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_handoff_survives_newer_apply_late_retirement_and_restart() -> Result<()> {
    let root = tempfile::tempdir()?;
    let gate = log_fault::Gate::new(None);
    let mut source = Fixture::with_filesystems(
        root.path(),
        1,
        LogOpenMode::Create,
        Arc::new(DirectFileSystem),
        Arc::new(log_fault::FaultFileSystem { gate: gate.clone() }),
    )
    .await?;
    source.commit(1).await?;
    let package = source.package()?;
    let checkpoint = package.checkpoint_material();
    let old_state = package.source_pin().unwrap().state();
    source.commit(2).await?;
    assert_eq!(value(source.applier()?.current().unwrap().pin())?, 2);
    drop(package);
    source.images.retire(images::id(1))?;
    let (runtime, mut events) = source
        .domain
        .run(RaftRuntime::start(source.log.clone(), runtime_options()))
        .await??;
    let release = gate.arm(log_fault::HOLD_SYNC);
    runtime.checkpoint(checkpoint).await?;
    tokio::time::timeout(std::time::Duration::from_secs(10), gate.entered.notified()).await?;
    assert_eq!(source.group.snapshot(0, 1)?.get_metadata().index, 1);
    assert!(
        events.try_recv().is_err(),
        "unsynced checkpoint must not report completion"
    );
    assert_eq!(source.images.reclaim(4096)?.files, 0);
    drop(release);
    let ticket = checkpointed(&mut events).await?;
    assert_eq!(ticket.position().index.get(), 2);
    assert_eq!(source.group.snapshot(0, 1)?.get_metadata().index, 2);
    assert_eq!(source.group.applied()?.position.index.get(), 3);
    // A publisher that captured older metadata can retire after checkpoint sync.
    // The independent reference, not a retained process-local pin, must win.
    source.images.retire(images::id(1))?;
    assert_eq!(source.images.reclaim(4096)?.files, 0);
    let old = source.images.adopt(images::id(1), old_state)?;
    assert_eq!(value(old.clone())?, 1);
    drop(old);
    assert!(
        !runtime.status(&scope()).await?.protocol_ready,
        "a local checkpoint is not leader authority"
    );
    runtime.close().await?;
    source.close().await?;

    let source = Fixture::open(root.path(), 1, LogOpenMode::Resume).await?;
    let mut applier = source.applier()?;
    assert_eq!(value(applier.current().unwrap().pin())?, 2);
    assert_eq!(source.images.usage()?.generations, 2);
    let package = source.package()?;
    let (runtime, mut events) = source
        .domain
        .run(RaftRuntime::start(source.log.clone(), runtime_options()))
        .await??;
    runtime.checkpoint(package.checkpoint_material()).await?;
    assert_eq!(checkpointed(&mut events).await?.position().index.get(), 3);
    applier.refresh_snapshot()?;
    source.images.retire(images::id(1))?;
    assert_eq!(source.images.reclaim(4096)?.files, 1);
    assert_eq!(source.images.usage()?.generations, 1);
    runtime.close().await?;
    drop(package);
    drop(applier);
    source.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_completion_loss_still_retains_durable_image() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut source = Fixture::open(root.path(), 1, LogOpenMode::Create).await?;
    source.commit(1).await?;
    let package = source.package()?;
    let checkpoint = package.checkpoint_material();
    source.commit(2).await?;
    drop(source.applier()?);
    drop(package);
    let (runtime, events) = source
        .domain
        .run(RaftRuntime::start(source.log.clone(), runtime_options()))
        .await??;
    // The runtime must settle retention at storage completion, independently of
    // whether its consumer polls or retains the eventual notification.
    runtime.checkpoint(checkpoint).await?;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    while source.group.snapshot(0, 1)?.get_metadata().index != 2 {
        ensure!(
            tokio::time::Instant::now() < deadline,
            "checkpoint did not sync"
        );
        tokio::task::yield_now().await;
    }
    runtime.close().await?;
    drop(events);
    source.images.retire(images::id(1))?;
    source.images.reclaim(4096)?;
    assert_eq!(source.images.usage()?.generations, 2);
    source.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_refuses_material_from_reopened_engine() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut source = Fixture::open(root.path(), 1, LogOpenMode::Create).await?;
    source.commit(1).await?;
    let package = source.package()?;
    let checkpoint = package.checkpoint_material();
    source.log.close().await?;
    source.log = source
        .domain
        .run(ReplicaLog::open(
            ReplicaLogOptions::new(
                root.path().join("raft"),
                NodeId::new([1; 16])?,
                DiskLineage::new([41; 16])?,
                ProcessGeneration::new([99; 16])?,
                LogOpenMode::Resume,
            ),
            Arc::new(DirectFileSystem),
        ))
        .await??;
    source.group = source.log.group(&scope())?;
    let (runtime, _events) = source
        .domain
        .run(RaftRuntime::start(source.log.clone(), runtime_options()))
        .await??;
    ensure!(matches!(
        runtime.checkpoint(checkpoint).await,
        Err(RuntimeError::Invalid(_))
    ));
    assert_eq!(source.group.snapshot(0, 1)?.get_metadata().index, 1);
    assert_eq!(value(source.applier()?.current().unwrap().pin())?, 1);
    runtime.close().await?;
    drop(package);
    source.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn checkpoint_failed_sync_preserves_prospective_recovery_image() -> Result<()> {
    let root = tempfile::tempdir()?;
    let gate = log_fault::Gate::new(None);
    let mut source = Fixture::with_filesystems(
        root.path(),
        1,
        LogOpenMode::Create,
        Arc::new(DirectFileSystem),
        Arc::new(log_fault::FaultFileSystem { gate: gate.clone() }),
    )
    .await?;
    source.commit(1).await?;
    let package = source.package()?;
    let checkpoint = package.checkpoint_material();
    source.commit(2).await?;
    drop(source.applier()?);
    drop(package);
    source.images.retire(images::id(1))?;
    let (runtime, _events) = source
        .domain
        .run(RaftRuntime::start(source.log.clone(), runtime_options()))
        .await??;
    let _release = gate.arm(log_fault::FAIL_SYNC);
    runtime.checkpoint(checkpoint).await?;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    while runtime.status(&scope()).await.is_ok() {
        ensure!(
            tokio::time::Instant::now() < deadline,
            "failed checkpoint must close consensus"
        );
        tokio::task::yield_now().await;
    }
    ensure!(runtime.close().await.is_err());
    ensure!(source.log.close().await.is_err());
    source.domain.shutdown().await;
    // The write may have reached the engine despite its failed sync response.
    // Joining and losing the producer must not make those image bytes reclaimable.
    source.images.retire(images::id(1))?;
    assert_eq!(source.images.reclaim(4096)?.files, 0);
    assert_eq!(source.images.usage()?.generations, 2);
    drop(source);
    let source = Fixture::open(root.path(), 1, LogOpenMode::Resume).await?;
    assert_eq!(value(source.applier()?.current().unwrap().pin())?, 2);
    let index = source.group.snapshot(0, 1)?.get_metadata().index;
    ensure!(index == 1 || index == 2);
    assert_eq!(
        source.images.usage()?.generations,
        if index == 1 { 1 } else { 2 }
    );
    source.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_wrong_history() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut source = Fixture::open(&root.path().join("source"), 1, LogOpenMode::Create).await?;
    let mut target = Fixture::open(&root.path().join("target"), 2, LogOpenMode::Create).await?;
    source.commit(1).await?;
    let package = source.package()?;
    for (field, replacement) in [
        ("birth", serde_json::json!(vec![10; 32])),
        ("schema", serde_json::json!(vec![11; 32])),
        ("index", serde_json::json!(99)),
        ("format", serde_json::json!(2)),
    ] {
        let mut snapshot = package.snapshot();
        let descriptor: serde_json::Value = serde_json::from_slice(&snapshot.data)?;
        let original = String::from_utf8(snapshot.data.to_vec())?;
        snapshot.data = original
            .replacen(
                &format!("\"{field}\":{}", descriptor[field]),
                &format!("\"{field}\":{replacement}"),
                1,
            )
            .into_bytes()
            .into();
        ensure!(
            SnapshotDownload::new(&target.group, [9; 32], snapshot, SnapshotLimits::default())
                .is_err()
        );
    }
    for field in ["cluster", "namespace", "group", "incarnation"] {
        let mut snapshot = package.snapshot();
        let descriptor: serde_json::Value = serde_json::from_slice(&snapshot.data)?;
        let original = String::from_utf8(snapshot.data.to_vec())?;
        snapshot.data = original
            .replacen(
                &format!("\"{field}\":{}", descriptor["scope"][field]),
                &format!("\"{field}\":\"{}\"", hex::encode([22; 16])),
                1,
            )
            .into_bytes()
            .into();
        ensure!(
            SnapshotDownload::new(&target.group, [9; 32], snapshot, SnapshotLimits::default())
                .is_err()
        );
    }
    let mut other = package.snapshot();
    other.mut_metadata().mut_conf_state().voters.push(4);
    ensure!(
        SnapshotDownload::new(&target.group, [9; 32], other, SnapshotLimits::default()).is_err()
    );
    let mut state = SnapshotDownload::new(
        &target.group,
        [9; 32],
        package.snapshot(),
        SnapshotLimits::default(),
    )?;
    let mut bytes = package.chunk(0, package.encoded_length())?.to_vec();
    bytes[0] ^= 0xff;
    state.append(0, &bytes)?;
    ensure!(state.finish(&target.images).is_err());
    assert_eq!(target.group.applied()?.position.index.get(), 1);
    assert_eq!(target.images.usage()?.generations, 0);
    target.commit(999).await?;
    ensure!(
        download(&package, &target).is_err(),
        "a colliding image name cannot replace unrelated bytes"
    );
    assert_eq!(value(target.applier()?.current().unwrap().pin())?, 999);
    drop(package);
    source.close().await?;
    target.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compacted_boundary_term() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut source = Fixture::open(root.path(), 1, LogOpenMode::Create).await?;
    source.commit(1).await?;
    let package = source.package()?;
    source
        .log
        .persist(OperationId::new(3), vec![package.checkpoint()])
        .await?;
    assert_eq!(source.group.term(2)?, 2);
    assert_eq!(source.group.first_index()?, 3);
    ensure!(source.group.term(1).is_err());
    let mut write = GroupWrite::new(&source.group);
    write.entries.push(Entry {
        index: 3,
        term: 2,
        data: b"uncommitted suffix".to_vec().into(),
        ..Default::default()
    });
    source.log.persist(OperationId::new(4), vec![write]).await?;
    drop(package);
    source.close().await?;
    let source = Fixture::open(root.path(), 1, LogOpenMode::Resume).await?;
    assert_eq!(source.group.term(2)?, 2);
    assert_eq!(source.group.last_index()?, 3);
    assert_eq!(source.group.initial_state()?.hard_state.commit, 2);
    assert_eq!(value(source.applier()?.current().unwrap().pin())?, 1);
    source.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_old_reader() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut source = Fixture::open(&root.path().join("source"), 1, LogOpenMode::Create).await?;
    let mut target = Fixture::open(&root.path().join("target"), 2, LogOpenMode::Create).await?;
    source.commit(1).await?;
    source.commit(2).await?;
    target.commit(1).await?;
    let mut applier = target.applier()?;
    let old = applier.current().unwrap().pin();
    let reader = images::query(old.clone())?;
    let package = source.package()?;
    ensure!(
        download(&package, &target)?.publish(&mut applier).is_err(),
        "sealed bytes alone must not publish"
    );
    assert_eq!(value(applier.current().unwrap().pin())?, 1);
    let prepared = download(&package, &target)?;
    target.install(&package).await?;
    let current = prepared.publish(&mut applier)?;
    target.images.reclaim(4096)?;
    assert_eq!(value(current.pin())?, 2);
    assert_eq!(
        reader
            .connection()
            .query_row("SELECT value FROM counter", [], |r| r.get::<_, i64>(0))?,
        1
    );
    assert_eq!(value(old.clone())?, 1);
    drop(reader);
    drop(old);
    target.images.reclaim(4096)?;
    assert_eq!(target.images.usage()?.generations, 1);
    // Repeated installation adopts the same sealed generation and does not
    // consume another name, image slot or business command execution.
    let repeat = download(&package, &target)?;
    target.install(&package).await?;
    assert_eq!(value(repeat.publish(&mut applier)?.pin())?, 2);
    assert_eq!(target.images.usage()?.generations, 1);
    drop(current);
    drop(applier);
    drop(package);
    source.close().await?;
    target.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_download_bounds_and_incomplete_refusal() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut source = Fixture::open(&root.path().join("source"), 1, LogOpenMode::Create).await?;
    let target = Fixture::open(&root.path().join("target"), 2, LogOpenMode::Create).await?;
    source.commit(1).await?;
    let package = source.package()?;
    let limits = SnapshotLimits {
        max_chunk_bytes: 32,
        ..Default::default()
    };
    let mut state = SnapshotDownload::new(&target.group, [9; 32], package.snapshot(), limits)?;
    ensure!(state.append(1, &[0]).is_err());
    ensure!(state.append(0, &[]).is_err());
    ensure!(state.append(0, &[0; 33]).is_err());
    state.append(0, package.chunk(0, 32)?)?;
    ensure!(state.append(0, &[0]).is_err());
    ensure!(state.finish(&target.images).is_err());
    assert_eq!(target.images.usage()?.generations, 0);
    drop(package);
    source.close().await?;
    target.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_reference_survives_newer_head_and_restart() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut source = Fixture::open(root.path(), 1, LogOpenMode::Create).await?;
    source.commit(1).await?;
    source.commit(2).await?;
    let package = source.package()?;
    source
        .log
        .persist(OperationId::new(3), vec![package.checkpoint()])
        .await?;
    drop(package);
    source.commit(3).await?;
    source.close().await?;

    let source = Fixture::open(root.path(), 1, LogOpenMode::Resume).await?;
    let mut applier = source.applier()?;
    source.images.reclaim(4096)?;
    assert_eq!(value(applier.current().unwrap().pin())?, 3);
    let old_state = images::state(
        &images::image(
            None,
            "CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES(1)",
        )?,
        1,
        true,
    );
    ensure!(source.images.adopt(images::id(1), old_state).is_err());
    assert_eq!(
        source.images.usage()?.generations,
        2,
        "the older durable snapshot must remain retained"
    );
    drop(applier);
    source.images.reclaim(4096)?;
    assert_eq!(
        source.images.usage()?.generations,
        2,
        "durable retention must outlive process-local pins"
    );

    applier = source.applier()?;
    let package = source.package()?;
    source
        .log
        .persist(OperationId::new(4), vec![package.checkpoint()])
        .await?;
    applier.refresh_snapshot()?;
    source.images.reclaim(4096)?;
    assert_eq!(
        source.images.usage()?.generations,
        1,
        "a newer durable snapshot replaces the old reference"
    );
    drop(package);
    drop(applier);
    source.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_runtime_installs_physical_image_with_exact_ticket() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut source = Fixture::open(&root.path().join("source"), 1, LogOpenMode::Create).await?;
    let target = Fixture::open(&root.path().join("target"), 2, LogOpenMode::Create).await?;
    source.commit(1).await?;
    source.commit(2).await?;
    let package = source.package()?;
    source
        .log
        .persist(OperationId::new(3), vec![package.checkpoint()])
        .await?;
    let options = |seed| RaftRuntimeOptions {
        tick_interval_ms: None,
        election_seed: Some([seed; 32]),
        ..Default::default()
    };
    let (leader, mut leader_events) = source
        .domain
        .run(RaftRuntime::start(source.log.clone(), options(11)))
        .await??;
    let (follower, mut follower_events) = target
        .domain
        .run(RaftRuntime::start(target.log.clone(), options(12)))
        .await??;
    let mut applier = target.applier()?;
    let mut prepared = None;
    let mut installed = false;
    leader.campaign(&scope()).await?;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    while !installed && tokio::time::Instant::now() < deadline {
        for (node, events) in [(1, &mut leader_events), (2, &mut follower_events)] {
            while let Ok(event) = events.try_recv() {
                match event.kind {
                    RuntimeEventKind::Message { message, .. } => match message.to {
                        1 => leader.step(&scope(), message).await?,
                        2 => follower.step(&scope(), message).await?,
                        _ => (),
                    },
                    RuntimeEventKind::Snapshot {
                        ticket, snapshot, ..
                    } => {
                        assert_eq!(node, 2);
                        assert_eq!(snapshot, package.snapshot());
                        assert_eq!(target.group.applied()?.position.index.get(), 1);
                        ensure!(applier.current().is_none());
                        let image = download(&package, &target)?;
                        let completion = image.completion(&ticket)?;
                        follower.complete_snapshot(completion.clone()).await?;
                        ensure!(follower.complete_snapshot(completion).await.is_err());
                        prepared = Some(image);
                    }
                    RuntimeEventKind::SnapshotInstalled { ticket, applied } => {
                        assert_eq!(node, 2);
                        assert_eq!(ticket.position(), applied.position);
                        target.images.retire(images::id(2))?;
                        drop(prepared.take().unwrap());
                        assert_eq!(target.images.reclaim(4096)?.files, 0,
                            "durable retention must outlive the transfer owner and delayed retirement");
                        let image = download(&package, &target)?.publish(&mut applier)?;
                        assert_eq!(value(image.pin())?, 2);
                        installed = true;
                    }
                    RuntimeEventKind::Failed { error, .. } => {
                        anyhow::bail!("runtime failed: {error}")
                    }
                    RuntimeEventKind::Apply { .. } => {
                        anyhow::bail!("compacted data must arrive through the snapshot")
                    }
                    _ => (),
                }
            }
        }
        tokio::task::yield_now().await;
    }
    ensure!(
        installed,
        "a live majority must install the physical snapshot"
    );
    leader.close().await?;
    follower.close().await?;
    drop(prepared);
    drop(applier);
    drop(package);
    source.close().await?;
    target.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restart_committed_replay() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut source = Fixture::open(root.path(), 1, LogOpenMode::Create).await?;
    source.commit(1).await?;
    let package = source.package()?;
    source
        .log
        .persist(OperationId::new(3), vec![package.checkpoint()])
        .await?;
    let bytes = images::image(Some(&source.bytes), "UPDATE counter SET value=2")?;
    let cut = images::Cut::new(&source.bytes, &bytes, 1, true, true);
    let mut write = GroupWrite::new(&source.group);
    write.entries = vec![
        Entry {
            index: 3,
            term: 2,
            data: source.payload(&cut)?.into(),
            ..Default::default()
        },
        Entry {
            index: 4,
            term: 2,
            data: b"uncommitted suffix".to_vec().into(),
            ..Default::default()
        },
    ];
    let mut hard = source.group.initial_state()?.hard_state;
    hard.commit = 3;
    write.hard_state = Some(hard);
    source.log.persist(OperationId::new(4), vec![write]).await?;
    assert_eq!(source.group.applied()?.position.index.get(), 2);
    drop(package);
    source.close().await?;

    let source = Fixture::open(root.path(), 1, LogOpenMode::Resume).await?;
    let mut applier = source.applier()?;
    assert_eq!(value(applier.current().unwrap().pin())?, 1);
    let (runtime, mut events) = source
        .domain
        .run(RaftRuntime::start(
            source.log.clone(),
            RaftRuntimeOptions {
                tick_interval_ms: None,
                election_seed: Some([12; 32]),
                ..Default::default()
            },
        ))
        .await??;
    let mut published = false;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    while !published && tokio::time::Instant::now() < deadline {
        while let Ok(event) = events.try_recv() {
            match event.kind {
                RuntimeEventKind::Apply { ticket, entry } => {
                    assert_eq!(entry.index, 3, "the uncommitted suffix must never execute");
                    let package = source.package()?;
                    assert_eq!(
                        tokio::time::timeout(
                            std::time::Duration::from_secs(5),
                            runtime.checkpoint(package.checkpoint_material())
                        )
                        .await?,
                        Err(RuntimeError::NotReady),
                        "checkpoint admission must not wait behind the apply it would block"
                    );
                    let completion = applier.prepare(ticket, &entry, images::id(2))?;
                    runtime.complete_apply(completion).await?;
                }
                RuntimeEventKind::Applied { ticket, .. } => {
                    assert_eq!(value(applier.publish(&ticket)?.pin())?, 2);
                    published = true;
                }
                RuntimeEventKind::Failed { error, .. } => anyhow::bail!("replay failed: {error}"),
                _ => (),
            }
        }
        tokio::task::yield_now().await;
    }
    ensure!(
        published,
        "the durable committed tail must replay after restart"
    );
    assert_eq!(source.group.last_index()?, 4);
    assert_eq!(source.group.initial_state()?.hard_state.commit, 3);
    assert_eq!(source.group.applied()?.position.index.get(), 3);
    runtime.close().await?;
    drop(applier);
    source.close().await?;

    let source = Fixture::open(root.path(), 1, LogOpenMode::Resume).await?;
    assert_eq!(value(source.applier()?.current().unwrap().pin())?, 2);
    assert_eq!(
        source.images.usage()?.generations,
        2,
        "the snapshot and newer applied image both survive"
    );
    assert_eq!(source.group.last_index()?, 4);
    assert_eq!(source.group.initial_state()?.hard_state.commit, 3);
    source.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_same_cut_cannot_name_an_unrelated_applied_image() -> Result<()> {
    let root = tempfile::tempdir()?;
    let mut source = Fixture::open(&root.path().join("source"), 1, LogOpenMode::Create).await?;
    let mut target = Fixture::open(&root.path().join("target"), 2, LogOpenMode::Create).await?;
    source.commit(1).await?;
    source.commit(2).await?;
    target.commit(1).await?;
    let old = target.group.applied()?.image;
    let package = source.package()?;
    let prepared = download(&package, &target)?;
    let snapshot = package.snapshot();
    let mut write = GroupWrite::new(&target.group);
    write.snapshot = Some(SnapshotChange::Install {
        snapshot,
        applied: DurableApply {
            position: LogPosition {
                index: LogIndex::new(3),
                term: Term::new(2),
            },
            image: old,
        },
    });
    let mut hard = target.group.initial_state()?.hard_state;
    hard.commit = 3;
    write.hard_state = Some(hard);
    // The mechanical engine API deliberately accepts opaque snapshot data.
    // Application recovery must independently refuse its mismatched image.
    target.log.persist(OperationId::new(9), vec![write]).await?;
    ensure!(target.applier().is_err());
    assert_eq!(
        target.images.usage()?.generations,
        2,
        "failed reference validation must not partly reclaim inventory"
    );
    drop(prepared);
    drop(package);
    source.close().await?;
    target.close().await
}

struct Child(std::process::Child);

impl Drop for Child {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_install_crash_matrix() -> Result<()> {
    for cut in [
        "image-after-write",
        "image-before-sync",
        "image-after-file-sync",
        "image-after-directory-sync",
        "engine-before-sync",
        "engine-after-sync",
        "engine-after-reply",
        "after-publication",
    ] {
        let root = tempfile::tempdir()?;
        let mut source = Fixture::open(&root.path().join("source"), 1, LogOpenMode::Create).await?;
        let mut target = Fixture::open(&root.path().join("target"), 2, LogOpenMode::Create).await?;
        source.commit(1).await?;
        source.commit(2).await?;
        target.commit(1).await?;
        source.close().await?;
        target.close().await?;
        let signal = root.path().join("child.signal");
        let output = root.path().join("child.log");
        let file = std::fs::File::create(&output)?;
        let mut child = Child(
            std::process::Command::new(std::env::current_exe()?)
                .args([
                    "--ignored",
                    "--exact",
                    "snapshot_process_fixture",
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env("CELLAR_SNAPSHOT_TEST_ROOT", root.path())
                .env("CELLAR_SNAPSHOT_TEST_CUT", cut)
                .env("CELLAR_SNAPSHOT_TEST_SIGNAL", &signal)
                .stdout(file.try_clone()?)
                .stderr(file)
                .spawn()?,
        );
        tokio::time::timeout(std::time::Duration::from_secs(20), async {
            while !signal.exists() {
                if let Some(status) = child.0.try_wait()? {
                    anyhow::bail!(
                        "child exited before {cut}: {status}; {}",
                        std::fs::read_to_string(&output)?
                    );
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await??;
        child.0.kill()?;
        ensure!(!child.0.wait()?.success(), "fixture must be killed");
        drop(child);

        let target = Fixture::open(&root.path().join("target"), 2, LogOpenMode::Resume).await?;
        let applier = target.applier()?;
        let image = applier.current().unwrap();
        let snapshot_index = target.group.snapshot(0, 1)?.get_metadata().index;
        match image.position().index.get() {
            2 => {
                ensure!(
                    cut.starts_with("image-") || cut == "engine-before-sync",
                    "synced install lost at {cut}"
                );
                assert_eq!(snapshot_index, 1);
                assert_eq!(value(image.pin())?, 1);
                assert_eq!(target.group.initial_state()?.hard_state.commit, 2);
            }
            3 => {
                ensure!(
                    !cut.starts_with("image-"),
                    "an unsubmitted install appeared at {cut}"
                );
                assert_eq!(snapshot_index, 3);
                assert_eq!(value(image.pin())?, 2);
                assert_eq!(target.group.initial_state()?.hard_state.commit, 3);
            }
            index => anyhow::bail!("incoherent install at {cut}: {index}"),
        }
        assert_eq!(
            target.images.usage()?.generations,
            1,
            "unreferenced interrupted images must be reclaimed at {cut}"
        );
        drop(image);
        drop(applier);
        target.close().await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "process-kill fixture, invoked by snapshot_install_crash_matrix"]
async fn snapshot_process_fixture() -> Result<()> {
    use std::sync::atomic::Ordering;
    let root = std::path::PathBuf::from(std::env::var("CELLAR_SNAPSHOT_TEST_ROOT")?);
    let cut = std::env::var("CELLAR_SNAPSHOT_TEST_CUT")?;
    let signal = std::path::PathBuf::from(std::env::var("CELLAR_SNAPSHOT_TEST_SIGNAL")?);
    let source = Fixture::open(&root.join("source"), 1, LogOpenMode::Resume).await?;
    let image_faults = image_fault::Faults::new(root.join("target/images"), Some(signal.clone()));
    let log_gate = log_fault::Gate::new(Some(signal.clone()));
    let target = Fixture::with_filesystems(
        &root.join("target"),
        2,
        LogOpenMode::Resume,
        Arc::new(image_fault::FaultFileSystem {
            faults: image_faults.clone(),
        }),
        Arc::new(log_fault::FaultFileSystem {
            gate: log_gate.clone(),
        }),
    )
    .await?;
    let mut applier = target.applier()?;
    let package = source.package()?;
    let image_mode = match cut.as_str() {
        "image-before-sync" => image_fault::BEFORE_IMAGE_SYNC,
        "image-after-write" => image_fault::AFTER_IMAGE_WRITE,
        "image-after-file-sync" => image_fault::AFTER_IMAGE_SYNC,
        "image-after-directory-sync" => image_fault::AFTER_DIRECTORY_SYNC,
        _ => 0,
    };
    image_faults.mode.store(image_mode, Ordering::Release);
    let prepared = download(&package, &target)?;
    let _release = match cut.as_str() {
        "engine-before-sync" => Some(log_gate.arm(log_fault::CRASH_BEFORE_SYNC)),
        "engine-after-sync" => Some(log_gate.arm(log_fault::CRASH_AFTER_SYNC)),
        _ => None,
    };
    target.install(&package).await?;
    if cut == "after-publication" {
        assert_eq!(value(prepared.publish(&mut applier)?.pin())?, 2);
    }
    let mut file = std::fs::File::create(signal)?;
    std::io::Write::write_all(&mut file, b"reached")?;
    file.sync_all()?;
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_peer_seed_crash_matrix() -> Result<()> {
    seed_crashes::crash_matrix(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_s3_seed_crash_matrix() -> Result<()> {
    seed_crashes::crash_matrix(true).await
}
