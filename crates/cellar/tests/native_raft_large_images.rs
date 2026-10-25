// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Cross the old logical image ceiling through real execution, quorum apply,
//! warm takeover and a fresh learner's object-store recovery.
#![allow(clippy::disallowed_methods)]
#[path = "support/raft_cluster.rs"]
#[allow(dead_code)]
mod cluster;
#[path = "support/raft_node.rs"]
#[allow(dead_code)]
mod public_node;

use anyhow::{ensure, Context as _, Result};
use cellar::application::consensus::verification::Catalog;
use cellar::sdk::{consensus::*, *};
use std::{sync::Arc, time::Duration};

fn application() -> Result<public_node::Counter> {
    let class = RaftClass::new("Counter", 1, [61; 32])?
        .command("Grow", [62; 32], |context, input| {
            let bytes = u64::from_be_bytes(input.try_into()?);
            ensure!(bytes <= 36 * 1024 * 1024);
            context.sql().execute_batch(
                "CREATE TABLE IF NOT EXISTS chunks(value BLOB);
                 CREATE TABLE IF NOT EXISTS counter(value INTEGER);
                 INSERT INTO counter SELECT 0 WHERE NOT EXISTS(SELECT 1 FROM counter);
                 UPDATE counter SET value=value+1;",
            )?;
            ensure!(bytes.is_multiple_of(1024 * 1024));
            // Keep each value inside the ordinary application SQLite limits;
            // the regression concerns total cell size, not oversized values.
            for _ in 0..bytes / (1024 * 1024) {
                context
                    .sql()
                    .execute("INSERT INTO chunks VALUES(zeroblob(1048576))", [])?;
            }
            let count: u64 = context
                .sql()
                .query_row("SELECT value FROM counter", [], |row| row.get(0))?;
            Ok(count.to_be_bytes().to_vec())
        })?
        .query("Get", [63; 32], |context, _| {
            let (count, bytes): (u64, u64) = context.query_row(
                "SELECT (SELECT value FROM counter), coalesce(sum(length(value)),0) FROM chunks",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            Ok([count.to_be_bytes(), bytes.to_be_bytes()].concat())
        })?;
    let schema = class.schema_digest();
    let increment = class.contract("Grow").unwrap();
    let get = class.contract("Get").unwrap();
    Ok(public_node::Counter {
        application: Arc::new(
            Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_class(class)?,
        ),
        schema,
        increment,
        get,
    })
}

async fn leader(handles: &[RaftCellHandle], excluded: Option<usize>) -> Result<usize> {
    Ok(tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            for (index, handle) in handles.iter().enumerate() {
                if Some(index) != excluded && handle.status().await.is_ok_and(|s| s.command_ready) {
                    return index;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?)
}

fn options(sequence: u64) -> Result<CommandOptions> {
    let mut options = public_node::archived_options(sequence)?;
    options.timeout = Duration::from_secs(60);
    Ok(options)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn image_above_64_mib_survives_takeover_and_empty_worker_s3_seed() -> Result<()> {
    large_image_drill(true, None).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn image_above_64_mib_survives_takeover_and_empty_worker_peer_seed() -> Result<()> {
    large_image_drill(false, None).await
}

/// Run explicitly with a disposable S3-compatible prefix. Credentials use the
/// ordinary AWS environment; the test never prints them or touches other keys.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires CELLAR_LARGE_IMAGE_ENDPOINT, CELLAR_LARGE_IMAGE_BUCKET and AWS credentials"]
async fn rustfs_image_above_64_mib_survives_takeover_and_empty_worker_seed() -> Result<()> {
    let store = object_store::aws::AmazonS3Builder::from_env()
        .with_bucket_name(std::env::var("CELLAR_LARGE_IMAGE_BUCKET")?)
        .with_endpoint(std::env::var("CELLAR_LARGE_IMAGE_ENDPOINT")?)
        .with_allow_http(true)
        .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch)
        .build()?;
    let prefix = object_store::path::Path::from(format!(
        "large-image-qualification/{:032x}",
        rand::random::<u128>()
    ));
    let store = object_store::prefix::PrefixStore::new(store, prefix);
    large_image_drill(true, Some(Arc::new(store))).await
}

async fn large_image_drill(
    from_s3: bool,
    store: Option<Arc<dyn object_store::ObjectStore>>,
) -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("cellar::raft=debug")
        .with_test_writer()
        .try_init();
    let definitions = (0..4).map(|_| application()).collect::<Result<Vec<_>>>()?;
    let namespace = NamespaceConfig::new(
        ClusterId::new([51; 16])?,
        NamespaceId::new([52; 16])?,
        cellar_logic::NamespaceMode::Raft,
        definitions[0].schema,
    );
    let registration = RaftCellRegistration {
        placement: RaftCellPlacement::Replica,
        namespace: namespace.clone(),
        class: "Counter".into(),
        birth: BirthProposal {
            scope: GroupScope {
                cluster: namespace.cluster,
                namespace: namespace.namespace,
                cell: "Counter:large-image".into(),
                group: GroupId::new([53; 16])?,
                incarnation: IncarnationId::new([54; 16])?,
            },
            voters: (1..=3)
                .map(|id| InitialMember {
                    member: MemberId::new(id).unwrap(),
                    node: NodeId::new([id as u8; 16]).unwrap(),
                })
                .collect(),
            learners: vec![],
        },
    };
    let mut cluster = cluster::Cluster::start_with_materialized_images(
        &definitions,
        vec![registration.clone()],
        128 * 1024 * 1024,
        store,
    )
    .await?;
    let handles = cluster
        .services
        .iter()
        .map(|service| service.cell(namespace.namespace, &registration.birth.scope.cell))
        .collect::<Result<Vec<_>, _>>()?;
    let primary = leader(&handles, None).await?;
    let chunk = (36_u64 * 1024 * 1024).to_be_bytes().to_vec();
    let second_options = options(2)?;
    let mut accepted_version = None;
    for sequence in 1..=2 {
        let receipt = handles[primary]
            .command(
                definitions[primary].increment.clone(),
                chunk.clone(),
                if sequence == 2 {
                    second_options.clone()
                } else {
                    options(sequence)?
                },
            )
            .await
            .with_context(|| format!("grow command {sequence}"))?;
        assert_eq!(receipt.result, sequence.to_be_bytes());
        accepted_version = Some(receipt.version);
    }
    // Remove the leader while both remaining voters already own committed
    // physical images. Takeover must preserve the durable retry receipt.
    cluster.services[primary].close().await?;
    let replacement = leader(&handles, Some(primary)).await?;
    let retry = handles[replacement]
        .command(
            definitions[replacement].increment.clone(),
            chunk,
            second_options,
        )
        .await?;
    assert_eq!(retry.result, 2_u64.to_be_bytes());
    assert_eq!(Some(retry.version), accepted_version);

    let target = JoiningMember {
        member: MemberId::new(4)?,
        node: NodeId::new([4; 16])?,
        disk: DiskLineage::new([13; 16])?,
    };
    let admitted = handles[replacement]
        .add_learner(
            target.clone(),
            LearnerOptions {
                expected_revision: handles[replacement].status().await?.membership_revision,
                timeout: Duration::from_secs(30),
            },
        )
        .await?;
    let catalog = Catalog::new(
        cluster.store.clone(),
        CatalogOptions {
            prefix: "partition/test".into(),
            ..Default::default()
        },
    )?
    .existing_namespace(namespace.clone())
    .await?;
    let birth = catalog
        .birth(&registration.birth.scope.cell)
        .await?
        .context("birth")?;
    // This data compresses well. A reader retaining the default logical limit
    // must still refuse its manifest before downloading or decoding the body.
    assert!(catalog
        .cell_archive(
            &birth,
            cellar_ltx::LtxHost::default(),
            ArchiveLimits::default()
        )?
        .recovery_metadata()
        .await
        .is_err());
    let archive = catalog.cell_archive(
        &birth,
        cellar_ltx::LtxHost::default(),
        ArchiveLimits {
            max_image_bytes: 128 * 1024 * 1024,
            max_object_bytes: 256 * 1024 * 1024,
            max_set_bytes: 512 * 1024 * 1024,
            ..Default::default()
        },
    )?;
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            if archive
                .recovery_metadata()
                .await?
                .is_some_and(|m| m.position().index >= admitted.joined_at)
            {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await??;
    let placement = if from_s3 {
        RaftCellPlacement::JoiningFromS3 {
            member: target.member,
            source: MemberId::new(replacement as u64 + 1)?,
            source_node: NodeId::new([replacement as u8 + 1; 16])?,
        }
    } else {
        RaftCellPlacement::Joining {
            member: target.member,
            source: MemberId::new(replacement as u64 + 1)?,
            source_node: NodeId::new([replacement as u8 + 1; 16])?,
        }
    };
    let joined = cluster
        .join_standby(definitions[3].application.clone(), placement)
        .await
        .context("restore 72 MiB onto empty worker")?;
    let learner = joined.cell(namespace.namespace, &registration.birth.scope.cell)?;
    let expected = [2_u64.to_be_bytes(), (72_u64 * 1024 * 1024).to_be_bytes()].concat();
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
    assert_eq!(read.result, expected);
    // New writes still replicate to the recovered image after its initial seed.
    let next = handles[replacement]
        .command(
            definitions[replacement].increment.clone(),
            0_u64.to_be_bytes().to_vec(),
            options(3)?,
        )
        .await?;
    assert_eq!(next.result, 3_u64.to_be_bytes());
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
        [3_u64.to_be_bytes(), (72_u64 * 1024 * 1024).to_be_bytes()].concat()
    );
    let get = definitions[3].get.clone();
    let namespace_id = namespace.namespace;
    let cell = registration.birth.scope.cell.clone();
    // Restart the seeded node first. Its historical read must use the retained
    // snapshot and newer applied image before any peer can supply replacement
    // bytes. This is not a freshness or quorum-read assertion.
    let cluster = cluster
        .restart(
            &definitions,
            vec![registration],
            vec![
                RaftCellPlacement::Replica,
                RaftCellPlacement::Replica,
                RaftCellPlacement::Replica,
                placement,
            ],
            Box::new(move |service| {
                Box::pin(async move {
                    let read = service
                        .cell(namespace_id, &cell)?
                        .query(
                            get,
                            vec![],
                            QueryOptions {
                                consistency: QueryConsistency::Stale,
                                stale_policy: StaleReadPolicy::DisconnectedHistorical,
                                ..Default::default()
                            },
                        )
                        .await?;
                    assert_eq!(
                        read.result,
                        [3_u64.to_be_bytes(), (72_u64 * 1024 * 1024).to_be_bytes()].concat()
                    );
                    Ok(())
                })
            }),
        )
        .await
        .context("restart seeded worker with retained snapshot and newer image")?;
    cluster.close().await?;
    Ok(())
}
