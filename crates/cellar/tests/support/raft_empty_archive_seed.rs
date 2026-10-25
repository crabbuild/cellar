//! A cold group can seed from its actual archive without any dummy SQL command.
use super::*;
use cellar::sdk::consensus::*;
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn native_empty_archive_seed_then_first_s3_write() -> Result<()> {
    run(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn native_empty_archive_seed_refuses_minority() -> Result<()> {
    run(true).await
}

async fn run(minority: bool) -> Result<()> {
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
        birth: native_snapshot_birth("Counter:cold-seed", 41),
    };
    let store = Arc::new(archive_fault::Store::default());
    let mut cluster = cluster::Cluster::start_with_standby_store(
        &definitions,
        vec![registration.clone()],
        RaftHostOptions::default(),
        store.clone(),
    )
    .await?;
    let handles = cluster
        .services
        .iter()
        .map(|h| h.cell(namespace.namespace, &registration.birth.scope.cell))
        .collect::<Result<Vec<_>, _>>()?;
    let p = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            for (p, h) in handles.iter().enumerate() {
                if h.status().await.is_ok_and(|s| s.command_ready) {
                    return p;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    let joined = handles[p]
        .add_learner(
            JoiningMember {
                member: MemberId::new(4)?,
                node: NodeId::new([4; 16])?,
                disk: DiskLineage::new([13; 16])?,
            },
            LearnerOptions {
                expected_revision: handles[p].status().await?.membership_revision,
                timeout: Duration::from_secs(5),
            },
        )
        .await?;
    let selected = Catalog::new(
        store,
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
        .context("cold birth")?;
    let archive = selected.cell_archive(
        &birth,
        cellar_ltx::LtxHost::default(),
        ArchiveLimits::default(),
    )?;
    let set = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(set) = archive.recovery_set().await? {
                if set.position().index >= joined.joined_at {
                    return Ok::<_, anyhow::Error>(set);
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    let scratch = tempfile::tempdir()?;
    let images = GenerationStore::open(
        scratch.path().to_path_buf(),
        Arc::new(DirectFileSystem),
        cellar_ltx::generation::GenerationLimits::default(),
    )?;
    ensure!(set.restore_state(&images, images::id(97))?.is_none());
    ensure!(images.usage()?.generations == 0);
    ensure!(calls.iter().all(|c| c.load(Ordering::Acquire) == 0));
    let source = if minority { p } else { (p + 1) % 3 };
    if minority {
        for (i, h) in cluster.services.iter().enumerate() {
            if i != p {
                ensure!(h.close().await?.unfinished.is_empty());
            }
        }
        tokio::time::sleep(Duration::from_millis(600)).await;
    }
    let joining = cluster
        .join_standby(
            definitions[3].application.clone(),
            RaftCellPlacement::JoiningFromS3 {
                member: MemberId::new(4)?,
                source: MemberId::new(source as u64 + 1)?,
                source_node: NodeId::new([source as u8 + 1; 16])?,
            },
        )
        .await;
    if minority {
        let error = joining
            .err()
            .context("available empty archive replaced live quorum")?;
        ensure!(
            matches!(
                error.downcast_ref::<TransportError>(),
                Some(TransportError::Refused | TransportError::Timeout | TransportError::Io(_))
            ),
            "unexpected quorumless failure: {error:?}"
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
                    NodeId::new([4; 16])?,
                    DiskLineage::new([13; 16])?,
                    ProcessGeneration::new([99; 16])?,
                    LogOpenMode::Resume,
                ),
                Arc::new(DirectFileSystem),
            ))
            .await??;
        ensure!(
            matches!(
                log.group(&registration.birth.scope),
                Err(LogError::NotFound)
            ),
            "refused cold seed created group state"
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
                "refused cold seed created a member claim"
            );
        }
        ensure!(calls.iter().all(|c| c.load(Ordering::Acquire) == 0));
    } else {
        let service = joining?;
        let learner = service.cell(namespace.namespace, &registration.birth.scope.cell)?;
        ensure!(calls[3].load(Ordering::Acquire) == 0);
        let receipt = handles[p]
            .command(
                definitions[p].increment.clone(),
                6_i64.to_be_bytes().to_vec(),
                public_node::archived_options(1)?,
            )
            .await?;
        ensure!(receipt.durability == Durability::S3 && receipt.result == 6_i64.to_be_bytes());
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
        ensure!(read.result == 6_i64.to_be_bytes());
        ensure!(
            calls
                .iter()
                .map(|c| c.load(Ordering::Acquire))
                .sum::<usize>()
                == 1
        );
        ensure!(service.close().await?.unfinished.is_empty());
    }
    cluster.close().await?;
    drop(images);
    Ok(())
}
