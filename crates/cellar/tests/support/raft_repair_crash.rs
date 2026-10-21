//! Abruptly kill every node after a committed explicit joint transition, then
//! resume preserved bytes through public Node startup and finish the same plan.
use super::super::seed_fault;
use super::*;
use std::path::PathBuf;

#[derive(serde::Serialize, serde::Deserialize)]
struct Saved {
    paths: Vec<PathBuf>,
    source: usize,
    failed: usize,
}

struct Child(std::process::Child);

impl Drop for Child {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

pub(in super::super) async fn joint_plan_process_kill_resume() -> Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().canonicalize()?;
    let signal = path.join("signal");
    let output = path.join("child.log");
    let file = std::fs::File::create(&output)?;
    let mut child = Child(
        std::process::Command::new(std::env::current_exe()?)
            .args([
                "--ignored",
                "--exact",
                "native_repairs::crashes::joint_process_fixture",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("CELLAR_JOINT_CRASH_ROOT", &path)
            .env("TMPDIR", &path)
            .stdout(file.try_clone()?)
            .stderr(file)
            .spawn()?,
    );
    let reached = tokio::time::timeout(Duration::from_secs(45), async {
        while !signal.exists() {
            if let Some(status) = child.0.try_wait()? {
                anyhow::bail!(
                    "joint child exited: {status}; {}",
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
            "joint cut not reached; {}",
            std::fs::read_to_string(&output).unwrap_or_default()
        )
    })??;
    child.0.kill()?;
    ensure!(!child.0.wait()?.success());
    drop(child);
    let saved: Saved = serde_json::from_slice(&std::fs::read(path.join("state.json"))?)?;
    ensure!(
        saved.paths.len() == 4 && saved.paths.iter().all(|p| p.starts_with(&path)),
        "joint fixture directory scope"
    );
    let Setup {
        definitions,
        calls,
        registration,
        entered: _entered,
    } = Setup::new()?;
    let store = Arc::new(
        seed_fault::Store::open(path.join("objects"), String::new(), path.join("unused")).await?,
    );
    let mut placements = vec![RaftCellPlacement::Replica; 3];
    placements.push(RaftCellPlacement::Joining {
        member: MemberId::new(4)?,
        source: MemberId::new(saved.source as u64 + 1)?,
        source_node: NodeId::new([saved.source as u8 + 1; 16])?,
    });
    let ns = registration.namespace.namespace;
    let name = registration.birth.scope.cell.clone();
    let get = definitions[3].get.clone();
    let obsolete = MemberId::new(saved.failed as u64 + 1)?;
    let check = Box::new(
        move |service: RaftCells| -> futures_util::future::BoxFuture<'static, Result<()>> {
            Box::pin(async move {
                let cell = service.cell(ns, &name)?;
                let state = cell
                    .repair_status()
                    .await?
                    .context("joint checkpoint missing before voters start")?;
                assert_eq!(state.phase, RepairPhase::Joint);
                assert_eq!(state.plan.obsolete, obsolete);
                let read = cell
                    .query(
                        get,
                        vec![],
                        QueryOptions {
                            consistency: QueryConsistency::Linearizable,
                            timeout: Duration::from_millis(200),
                            ..Default::default()
                        },
                    )
                    .await;
                ensure!(matches!(read, Err(CommandFailure::VersionUnavailable)));
                Ok(())
            })
        },
    );
    // The learner-turned-voter resumes its actual joint checkpoint before any
    // peer service starts; a fresh quorum read remains unavailable.
    let cluster = cluster::Cluster::resume_seed_crash_fixture(
        &definitions,
        vec![registration.clone()],
        store,
        &saved.paths,
        placements,
        true,
        Some(check),
    )
    .await?;
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
    let status = cells[primary]
        .repair_status()
        .await?
        .context("joint plan lost after process kill")?;
    assert_eq!(status.phase, RepairPhase::Joint);
    assert_eq!(
        status.plan.obsolete,
        MemberId::new(saved.failed as u64 + 1)?
    );
    assert_eq!(status.plan.replacement.member, MemberId::new(4)?);
    let complete = cells[primary]
        .advance_repair(
            status.plan.id,
            RepairStep::FinishJoint,
            status.changed_at,
            Duration::from_secs(5),
        )
        .await?;
    assert_eq!(complete.phase, RepairPhase::Complete);
    ensure!(
        calls.iter().all(|c| c.load(Ordering::SeqCst) == 0),
        "process recovery reran business callbacks"
    );
    let read = cells[3]
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
    let receipt = cells[primary]
        .command(
            definitions[primary].increment.clone(),
            1_i64.to_be_bytes().to_vec(),
            public_node::options(2)?,
        )
        .await?;
    assert_eq!(receipt.result, 6_i64.to_be_bytes());
    let reader = (0..4)
        .find(|i| *i != saved.failed && *i != primary)
        .unwrap();
    let read = cells[reader]
        .query(
            definitions[reader].get.clone(),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "process-kill fixture, invoked by joint_plan_process_kill_resume"]
async fn joint_process_fixture() -> Result<()> {
    let root = PathBuf::from(std::env::var("CELLAR_JOINT_CRASH_ROOT")?);
    let store = Arc::new(
        seed_fault::Store::open(root.join("objects"), String::new(), root.join("unused")).await?,
    );
    let mut f = Fixture::new_with_store(false, Some(store)).await?;
    f.fail_obsolete().await?;
    f.begin().await?;
    f.join().await?;
    let joint = f.step(RepairStep::PromoteAndRemove).await?;
    assert_eq!(joint.phase, RepairPhase::Joint);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if f.handles[3]
                .repair_status()
                .await?
                .is_some_and(|r| r.phase == RepairPhase::Joint)
            {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    let saved = Saved {
        paths: f
            .cluster
            .nodes
            .iter()
            .map(|n| n.info().data_dir.clone())
            .collect(),
        source: f.primary,
        failed: f.failed,
    };
    let mut file = std::fs::File::create(root.join("state.json"))?;
    std::io::Write::write_all(&mut file, &serde_json::to_vec(&saved)?)?;
    file.sync_all()?;
    seed_fault::signal(&root.join("signal"))?;
    std::future::pending::<()>().await;
    Ok(())
}
