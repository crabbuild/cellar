//! Every child runs public Node startup with a real shared log and SQLite image.
use super::*;
use cellar::sdk::consensus::*;
use std::{
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

#[derive(serde::Serialize, serde::Deserialize)]
struct State {
    directories: Vec<PathBuf>,
    source: usize,
}

fn registration(schema: [u8; 32]) -> RaftCellRegistration {
    let mut namespace = native_snapshot_config();
    namespace.schema_digest = schema;
    RaftCellRegistration {
        placement: RaftCellPlacement::Replica,
        namespace,
        class: "Counter".into(),
        birth: native_snapshot_birth("Counter:seed-crash", 31),
    }
}

fn placement(s3: bool, source: usize) -> Result<RaftCellPlacement> {
    let member = MemberId::new(4)?;
    let source_node = NodeId::new([source as u8 + 1; 16])?;
    let source = MemberId::new(source as u64 + 1)?;
    Ok(if s3 {
        RaftCellPlacement::JoiningFromS3 {
            member,
            source,
            source_node,
        }
    } else {
        RaftCellPlacement::Joining {
            member,
            source,
            source_node,
        }
    })
}

async fn leader(cells: &[RaftCellHandle]) -> Result<usize> {
    Ok(tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            for (index, cell) in cells.iter().enumerate() {
                if cell.status().await.is_ok_and(|s| s.command_ready) {
                    return index;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?)
}

pub(super) async fn crash_matrix(s3: bool) -> Result<()> {
    for cut in [
        "image-before-sync",
        "image-after-rename",
        "image-after-directory-sync",
        "engine-before-sync",
        "engine-after-sync",
        "engine-after-reply",
        "after-claim",
        "after-attachment",
    ] {
        let root = tempfile::tempdir()?;
        let root_path = root.path().canonicalize()?;
        let signal = root.path().join("signal");
        let output = root.path().join("child.log");
        let file = std::fs::File::create(&output)?;
        let mut child = Child(
            std::process::Command::new(std::env::current_exe()?)
                .args([
                    "--ignored",
                    "--exact",
                    "seed_crashes::native_seed_process_fixture",
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env("CELLAR_SEED_CRASH_ROOT", &root_path)
                .env("CELLAR_SEED_CRASH_CUT", cut)
                .env("CELLAR_SEED_CRASH_S3", if s3 { "1" } else { "0" })
                .env("TMPDIR", &root_path)
                .stdout(file.try_clone()?)
                .stderr(file)
                .spawn()?,
        );
        let reached = tokio::time::timeout(Duration::from_secs(45), async {
            while !signal.exists() {
                if let Some(status) = child.0.try_wait()? {
                    anyhow::bail!(
                        "seed child exited before {cut}: {status}; {}",
                        std::fs::read_to_string(&output)?
                    );
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await;
        reached.with_context(|| {
            format!(
                "seed cut not reached: s3={s3} {cut}; {}",
                std::fs::read_to_string(&output).unwrap_or_default()
            )
        })??;
        child.0.kill()?;
        ensure!(!child.0.wait()?.success(), "fixture must be killed");
        drop(child);
        let state: State = serde_json::from_slice(&std::fs::read(root.path().join("state.json"))?)?;
        ensure!(
            state.directories.len() == 4
                && state.directories.iter().all(|p| p.starts_with(&root_path)),
            "fixture directory scope"
        );
        let (gate, _entered) = public_node::Gate::new();
        gate.release();
        let calls: Vec<_> = (0..4).map(|_| Arc::new(AtomicUsize::new(0))).collect();
        let definitions = calls
            .iter()
            .map(|calls| public_node::counter(calls.clone(), gate.clone()))
            .collect::<Result<Vec<_>>>()?;
        let registration = registration(definitions[0].schema);
        // Observe engine atomicity without repairing images or catalog state.
        let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
        let log = domain
            .run(ReplicaLog::open(
                ReplicaLogOptions::new(
                    state.directories[3].join("raft"),
                    NodeId::new([4; 16])?,
                    DiskLineage::new([13; 16])?,
                    ProcessGeneration::new([99; 16])?,
                    LogOpenMode::Resume,
                ),
                Arc::new(DirectFileSystem),
            ))
            .await??;
        let seeded = match log.group(&registration.birth.scope) {
            Ok(group) => {
                ensure!(
                    !cut.starts_with("image-"),
                    "group appeared before seed submission at {cut}"
                );
                let initial = group.initial_state()?;
                assert_eq!(initial.hard_state.vote, 0);
                assert_eq!(
                    initial.hard_state.commit,
                    group.applied()?.position.index.get()
                );
                ensure!(initial.conf_state.learners.contains(&4));
                assert_eq!(
                    group
                        .membership()?
                        .joined(MemberId::new(4)?)?
                        .map(|(b, _)| b.disk),
                    Some(DiskLineage::new([13; 16])?)
                );
                ensure!(group.applied()?.image.is_some());
                true
            }
            Err(LogError::NotFound) => {
                ensure!(
                    cut.starts_with("image-") || cut == "engine-before-sync",
                    "synced seed lost at {cut}"
                );
                false
            }
            Err(error) => return Err(error.into()),
        };
        domain.run(log.close()).await??;
        domain.shutdown().await;
        let store = Arc::new(
            seed_fault::Store::open(
                root.path().join("objects"),
                String::new(),
                root.path().join("unused"),
            )
            .await?,
        );
        use futures_util::StreamExt as _;
        use object_store::ObjectStore as _;
        let mut objects = store.list(None);
        let mut claimed = false;
        while let Some(object) = objects.next().await {
            claimed |= object?
                .location
                .as_ref()
                .ends_with("/claims/0000000000000004");
        }
        ensure!(
            !claimed || seeded,
            "claim appeared before local seed at {cut}"
        );
        if matches!(cut, "after-claim" | "after-attachment") {
            ensure!(claimed, "completed claim lost at {cut}");
        } else {
            ensure!(!claimed, "early claim at {cut}");
        }
        let mut placements = vec![RaftCellPlacement::Replica; 3];
        placements.push(placement(s3, state.source)?);
        let cluster = cluster::Cluster::resume_seed_crash_fixture(
            &definitions,
            vec![registration.clone()],
            store,
            &state.directories,
            placements,
            seeded,
            None,
        )
        .await
        .with_context(|| format!("public restart after {cut}, s3={s3}, seeded={seeded}"))?;
        let cells = cluster
            .services
            .iter()
            .map(|s| {
                s.cell(
                    registration.namespace.namespace,
                    &registration.birth.scope.cell,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let read = cells[3]
            .query(
                definitions[3].get.clone(),
                vec![],
                QueryOptions {
                    consistency: QueryConsistency::Linearizable,
                    timeout: Duration::from_secs(10),
                    ..Default::default()
                },
            )
            .await?;
        assert_eq!(read.result, 5_i64.to_be_bytes(), "state after {cut}");
        ensure!(
            calls.iter().all(|n| n.load(Ordering::SeqCst) == 0),
            "recovery reran command at {cut}"
        );
        let primary = leader(&cells[..3]).await?;
        let receipt = cells[primary]
            .command(
                definitions[primary].increment.clone(),
                1_i64.to_be_bytes().to_vec(),
                public_node::options(3)?,
            )
            .await?;
        assert_eq!(receipt.result, 6_i64.to_be_bytes());
        let read = cells[3]
            .query(
                definitions[3].get.clone(),
                vec![],
                QueryOptions {
                    consistency: QueryConsistency::AtLeast(receipt.version_token),
                    timeout: Duration::from_secs(10),
                    ..Default::default()
                },
            )
            .await?;
        assert_eq!(read.result, 6_i64.to_be_bytes());
        assert_eq!(calls[3].load(Ordering::SeqCst), 0);
        cluster.close().await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "process-kill fixture, invoked by native peer/S3 seed crash matrices"]
async fn native_seed_process_fixture() -> Result<()> {
    let root = PathBuf::from(std::env::var("CELLAR_SEED_CRASH_ROOT")?);
    let cut = std::env::var("CELLAR_SEED_CRASH_CUT")?;
    let s3 = std::env::var("CELLAR_SEED_CRASH_S3")? == "1";
    let (gate, _entered) = public_node::Gate::new();
    gate.release();
    let definitions = (0..4)
        .map(|_| public_node::counter(Arc::new(AtomicUsize::new(0)), gate.clone()))
        .collect::<Result<Vec<_>>>()?;
    let registration = registration(definitions[0].schema);
    let filesystem = Arc::new(seed_fault::Filesystem::default());
    let store = Arc::new(
        seed_fault::Store::open(root.join("objects"), cut.clone(), root.join("signal")).await?,
    );
    let mut cluster = cluster::Cluster::start_seed_crash_fixture(
        &definitions,
        vec![registration.clone()],
        store,
        filesystem.clone(),
    )
    .await
    .context("start seed crash fixture")?;
    let cells = cluster
        .services
        .iter()
        .map(|s| {
            s.cell(
                registration.namespace.namespace,
                &registration.birth.scope.cell,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let primary = leader(&cells).await?;
    let mut options = public_node::options(1)?;
    options.durability = Durability::S3;
    let receipt = cells[primary]
        .command(
            definitions[primary].increment.clone(),
            5_i64.to_be_bytes().to_vec(),
            options,
        )
        .await
        .with_context(|| format!("initial S3 command before {cut}, s3={s3}"))?;
    assert_eq!(receipt.result, 5_i64.to_be_bytes());
    cells[primary]
        .add_learner(
            JoiningMember {
                member: MemberId::new(4)?,
                node: NodeId::new([4; 16])?,
                disk: DiskLineage::new([13; 16])?,
            },
            LearnerOptions {
                expected_revision: cells[primary].status().await?.membership_revision,
                timeout: Duration::from_secs(5),
            },
        )
        .await
        .with_context(|| format!("add learner before {cut}, s3={s3}"))?;
    // The S3 proof forces an archive cut covering the learner configuration.
    let mut options = public_node::options(2)?;
    options.durability = Durability::S3;
    let deadline = tokio::time::Instant::now() + options.timeout;
    let revision = cells[primary].status().await?.membership_revision;
    // Adding the learner starts a background archive capture on the same SQL
    // worker. This crash fixture needs its command admitted before arming the
    // seed cut; it is not an overload retry test. Wait for that capture's
    // actual proof, charging this wait to the original command budget.
    tokio::time::timeout_at(deadline, async {
        loop {
            let status = cells[primary].status().await?;
            if status.archived >= revision && status.command_ready {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .with_context(|| format!("learner configuration archive before {cut}, s3={s3}"))??;
    options.timeout = deadline.saturating_duration_since(tokio::time::Instant::now());
    cells[primary]
        .command(
            definitions[primary].increment.clone(),
            0_i64.to_be_bytes().to_vec(),
            options,
        )
        .await
        .with_context(|| format!("archive learner configuration before {cut}, s3={s3}"))?;
    let directory = cluster.nodes[3].info().data_dir.clone();
    let image_directory = directory
        .join("raft-cells")
        .join(hex::encode(registration.birth.scope.namespace.bytes()))
        .join(hex::encode(registration.birth.scope.group.bytes()))
        .join(hex::encode(registration.birth.scope.incarnation.bytes()))
        .join("images");
    filesystem.arm(&directory, image_directory, &cut, root.join("signal"));
    let state = State {
        directories: cluster
            .nodes
            .iter()
            .map(|n| n.info().data_dir.clone())
            .collect(),
        source: primary,
    };
    let mut file = std::fs::File::create(root.join("state.json"))?;
    std::io::Write::write_all(&mut file, &serde_json::to_vec(&state)?)?;
    file.sync_all()?;
    let joined = cluster
        .join_standby(definitions[3].application.clone(), placement(s3, primary)?)
        .await
        .with_context(|| format!("join standby at {cut}, s3={s3}"))?;
    ensure!(
        cut == "after-attachment",
        "seed cut was not exercised: {cut}"
    );
    let cell = joined.cell(
        registration.namespace.namespace,
        &registration.birth.scope.cell,
    )?;
    let read = cell
        .query(
            definitions[3].get.clone(),
            vec![],
            QueryOptions {
                consistency: QueryConsistency::Linearizable,
                ..Default::default()
            },
        )
        .await?;
    assert_eq!(read.result, 5_i64.to_be_bytes());
    seed_fault::signal(&root.join("signal"))?;
    std::future::pending::<()>().await;
    Ok(())
}
