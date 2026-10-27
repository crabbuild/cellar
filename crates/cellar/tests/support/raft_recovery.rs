// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Focused immediate repair. The first timed query is an indexed one-row read;
//! the broad payload oracle runs after the replacement's first durable write.

use super::{cluster, full_capacity as fixture, public_node, *};
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{Seek, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicUsize, Ordering},
    time::Instant,
};

const PHASES: &[&str] = &[
    "fault",
    "owner_joined",
    "local_state_erased",
    "admission_started",
    "admitted",
    "archive_wait_started",
    "archive_ready",
    "seed_started",
    "seed_installed",
    "hydrated",
    "first_read",
    "promoted",
    "first_write_verified",
    "receipt_verified",
    "payloads_verified",
    "cleanup_joined",
];
const CHECKS: &[&str] = &[
    "payloads_verified",
    "version_verified",
    "first_write_verified",
    "receipt_verified",
    "local_state_erased",
    "replacement_started_cold",
    "served_locally",
    "callbacks_once",
    "cleanup_joined",
];

fn keep_node_state() -> Result<bool> {
    match std::env::var("CELLAR_RECOVERY_KEEP_NODE_STATE") {
        Ok(value) if value == "1" => Ok(true),
        Ok(value) if value == "0" => Ok(false),
        Err(std::env::VarError::NotPresent) => Ok(false),
        _ => anyhow::bail!("CELLAR_RECOVERY_KEEP_NODE_STATE must be 0 or 1"),
    }
}

struct Ledger {
    started: Instant,
    file: Option<File>,
    value: serde_json::Value,
}

impl Ledger {
    fn new(backend: &str, image_bytes: u64) -> Result<Self> {
        let file = std::env::var_os("CELLAR_RECOVERY_EVIDENCE")
            .map(|path| {
                let mut options = OpenOptions::new();
                options.create_new(true).write(true);
                #[cfg(unix)]
                {
                    use std::os::unix::fs::OpenOptionsExt as _;
                    options.mode(0o600);
                }
                options.open(path)
            })
            .transpose()?;
        let checks: BTreeMap<_, _> = CHECKS.iter().map(|name| (*name, false)).collect();
        Ok(Self {
            started: Instant::now(),
            file,
            value: serde_json::json!({
                "format": 1, "backend": backend, "storage": "materialized",
                "image_bytes": image_bytes, "failure_mode": "joined_domain_loss",
                "events": [], "checks": checks, "complete": false,
            }),
        })
    }

    fn mark(&mut self, phase: &str) -> Result<()> {
        let events = self.value["events"].as_array_mut().context("events")?;
        ensure!(
            PHASES.get(events.len()) == Some(&phase),
            "recovery phase order: {phase}"
        );
        let elapsed_us = u64::try_from(self.started.elapsed().as_micros())?;
        events.push(serde_json::json!({"name": phase, "elapsed_us": elapsed_us}));
        eprintln!("cold-recovery phase={phase} elapsed_us={elapsed_us}");
        self.save()
    }

    fn check(&mut self, name: &str) -> Result<()> {
        ensure!(CHECKS.contains(&name), "unknown recovery check");
        self.value["checks"][name] = true.into();
        Ok(())
    }

    fn save(&mut self) -> Result<()> {
        if let Some(file) = self.file.as_mut() {
            file.rewind()?;
            file.set_len(0)?;
            file.write_all(&serde_json::to_vec(&self.value)?)?;
            file.flush()?;
        }
        Ok(())
    }
}

async fn hot_read(
    handle: &RaftCellHandle,
    definition: &public_node::Counter,
    samples: &fixture::PayloadSamples,
    expected: u64,
    consistency: QueryConsistency,
    member: MemberId,
) -> Result<()> {
    let result = handle
        .query(
            definition.get.clone(),
            samples.ids[0].to_be_bytes().to_vec(),
            QueryOptions {
                stale_policy: if consistency == QueryConsistency::Stale {
                    StaleReadPolicy::DisconnectedHistorical
                } else {
                    StaleReadPolicy::RecentSource
                },
                consistency,
                timeout: Duration::from_secs(60),
                ..Default::default()
            },
        )
        .await?;
    let mut bytes = expected.to_be_bytes().to_vec();
    bytes.extend_from_slice(&samples.hashes[0]);
    ensure!(
        result.result == bytes && result.served_by == member,
        "hot read value/version/member differs"
    );
    Ok(())
}

async fn transfer_observed(
    sources: &[RaftCellHandle],
    target: &RaftCellHandle,
    member: MemberId,
) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            if target
                .status()
                .await
                .is_ok_and(|status| status.command_ready)
            {
                return Ok(());
            }
            // Admission can step the sampled leader down before the target's
            // readiness is visible. Refresh the source on every iteration;
            // only the target's observed readiness completes this handoff.
            for source in sources {
                let status = match source.status().await {
                    Ok(status) => status,
                    Err(CommandFailure::Unavailable | CommandFailure::NotLeader { .. }) => continue,
                    Err(error) => return Err(error.into()),
                };
                if !status.command_ready || status.leader == Some(member) {
                    continue;
                }
                match source
                    .transfer_leadership(
                        LeadershipTransfer {
                            target: member,
                            expected_term: status.term,
                            expected_revision: status.membership_revision,
                        },
                        Duration::from_secs(30),
                    )
                    .await
                {
                    Ok(()) => break,
                    Err(
                        CommandFailure::Overloaded
                        | CommandFailure::Unavailable
                        | CommandFailure::NotLeader { .. },
                    ) => (),
                    Err(error) => return Err(error.into()),
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .context("leadership handoff did not become ready")?
}

async fn observed_repair(
    live: &[RaftCellHandle],
    id: cellar_logic::consensus::ProposalId,
    step: RepairStep,
    quota: u64,
) -> Result<RepairStatus> {
    let limit = if quota <= 12 * 1024 * 1024 { 60 } else { 1800 };
    let mut observed_at = Instant::now();
    tokio::time::timeout(Duration::from_secs(limit), async {
        loop {
            // A seed source need not be the leader. Re-resolve the leader for
            // each mutation; role samples never replace the server's fences.
            for handle in live {
                let status = handle.status().await?;
                if !status.command_ready {
                    continue;
                }
                match handle
                    .advance_repair(
                        id,
                        step,
                        status.membership_revision,
                        Duration::from_secs(30),
                    )
                    .await
                {
                    Ok(repair) => return Ok(repair),
                    Err(
                        CommandFailure::NotLeader { .. }
                        | CommandFailure::Unavailable
                        | CommandFailure::Overloaded,
                    ) => (),
                    Err(error) => return Err(error.into()),
                }
            }
            if observed_at.elapsed() >= Duration::from_secs(5) {
                for handle in live {
                    eprintln!(
                        "cold-recovery repair={step:?} status={:?} progress={:?}",
                        handle.status().await,
                        handle.replication_status().await
                    );
                }
                observed_at = Instant::now();
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .context("focused repair exceeded its phase budget")?
}

async fn drill(
    source: PathBuf,
    workdir: &Path,
    secondary: Option<&Path>,
    quota: u64,
    store: Arc<dyn object_store::ObjectStore>,
    backend: &str,
    force_leader_change: bool,
) -> Result<serde_json::Value> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("cellar::raft::archive=info")
        .with_ansi(false)
        .with_writer(std::io::stderr)
        .try_init();
    let keep = keep_node_state()?;
    let mut source_root = tempfile::tempdir_in(workdir)?;
    source_root.disable_cleanup(keep);
    let path = source_root.path().to_path_buf();
    let preparation = ExecutionDomain::new(tokio::runtime::Handle::current());
    let receipt_reserve = (quota / 16).min(32 * 1024 * 1024) / 512 * 512;
    let prepared = preparation
        .blocking(move || fixture::prepared_template(&source, &path, quota, receipt_reserve))?
        .await?;
    preparation.shutdown().await;
    let (template, image, payload_bytes, samples) = prepared?;
    let image_bytes = image.length;
    let calls: Vec<_> = (0..4).map(|_| Arc::new(AtomicUsize::new(0))).collect();
    let (definitions, verify): (Vec<_>, Vec<_>) = calls
        .iter()
        .cloned()
        .map(fixture::recovery_definition)
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .unzip();
    let namespace = NamespaceConfig::new(
        ClusterId::new([81; 16])?,
        NamespaceId::new([82; 16])?,
        cellar_logic::NamespaceMode::Raft,
        definitions[0].schema,
    );
    let registration = RaftCellRegistration {
        placement: RaftCellPlacement::Replica,
        class: "Counter".into(),
        namespace: namespace.clone(),
        birth: BirthProposal {
            scope: GroupScope {
                cluster: namespace.cluster,
                namespace: namespace.namespace,
                cell: "Counter:full-capacity".into(),
                group: GroupId::new([83; 16])?,
                incarnation: IncarnationId::new([84; 16])?,
            },
            voters: (1..=3)
                .map(|id| {
                    Ok(InitialMember {
                        member: MemberId::new(id)?,
                        node: NodeId::new([id as u8; 16])?,
                    })
                })
                .collect::<Result<_>>()?,
            learners: vec![],
        },
    };
    let roots = (0..4)
        .map(|index| {
            let mut root = tempfile::tempdir_in(if index % 2 == 1 {
                secondary.unwrap_or(workdir)
            } else {
                workdir
            })?;
            root.disable_cleanup(keep);
            Ok(root)
        })
        .collect::<std::io::Result<Vec<_>>>()?;
    let mut host = RaftHostOptions::default();
    host.execution.max_image_bytes = quota;
    host.execution.receipt_reserve_bytes = receipt_reserve;
    host.execution.max_retry_window_ms = 86_400_000;
    let mut cluster = cluster::Cluster::start_with_preloaded_images(
        &definitions,
        vec![registration.clone()],
        host,
        store,
        roots,
        fixture::preload(template, image, registration.clone(), quota),
    )
    .await?;
    let preflight = async {
        let handles = cluster
            .services
            .iter()
            .map(|service| service.cell(namespace.namespace, &registration.birth.scope.cell))
            .collect::<Result<Vec<_>, _>>()?;
        let primary = fixture::ready(&handles, None).await?;
        let first_options = fixture::command_options(1, 1)?;
        let first = handles[primary]
            .command(
                definitions[primary].increment.clone(),
                0_u64.to_be_bytes().to_vec(),
                first_options.clone(),
            )
            .await?;
        ensure!(first.result == 8_u64.to_be_bytes());
        let ledger = Ledger::new(backend, image_bytes)?;
        Ok::<_, anyhow::Error>((handles, primary, first_options, first, ledger))
    }
    .await;
    let (handles, primary, first_options, first, mut ledger) = match preflight {
        Ok(prepared) => prepared,
        Err(error) => {
            if let Err(cleanup) = cluster.close().await {
                return Err(error.context(format!("recovery preflight cleanup failed: {cleanup}")));
            }
            return Err(error);
        }
    };
    // Always join the whole fixture, including on failed assertions or admission.
    let result: Result<()> = async {
        ledger.mark("fault")?;
        let lost = cluster.crash_owner(primary).await?;
        ledger.mark("owner_joined")?;
        let erasure = ExecutionDomain::new(tokio::runtime::Handle::current());
        let erased = erasure
            .blocking(move || {
                std::fs::remove_dir_all(&lost)?;
                ensure!(!lost.exists(), "owner local state survived erasure");
                Ok::<_, anyhow::Error>(())
            })?
            .await?;
        erasure.shutdown().await;
        erased?;
        ledger.check("local_state_erased")?;
        ledger.mark("local_state_erased")?;
        ledger.mark("admission_started")?;
        let surviving: Vec<_> = handles
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != primary)
            .map(|(_, handle)| handle.clone())
            .collect();
        let source = fixture::ready(&handles, Some(primary)).await?;
        if force_leader_change {
            let current = fixture::ready(&handles, Some(primary)).await?;
            if current == source {
                let next = (0..3)
                    .find(|index| *index != primary && *index != source)
                    .context("other live voter")?;
                transfer_observed(&surviving, &handles[next], MemberId::new(next as u64 + 1)?)
                    .await?;
            }
            ensure!(
                !handles[source].status().await?.command_ready,
                "repair source did not become a follower"
            );
        }
        // The captured source can remain a caught-up follower. Membership
        // mutations still go to the current leader and retain their exact plan.
        let admission_source = fixture::ready(&handles, Some(primary)).await?;
        let target = JoiningMember {
            member: MemberId::new(4)?,
            node: NodeId::new([4; 16])?,
            disk: DiskLineage::new([13; 16])?,
        };
        let repair = RepairPlan {
            id: cellar_logic::consensus::ProposalId::new([85; 16])?,
            policy_revision: 1,
            expected_revision: handles[admission_source]
                .status()
                .await?
                .membership_revision,
            obsolete: MemberId::new(primary as u64 + 1)?,
            replacement: target.clone(),
        };
        let admitted = handles[admission_source]
            .begin_repair(repair.clone(), Duration::from_secs(1800))
            .await?;
        ledger.mark("admitted")?;
        ledger.mark("archive_wait_started")?;
        let from_s3 = backend != "peer";
        if from_s3 {
            let namespace = Catalog::new(
                cluster.store.clone(),
                CatalogOptions {
                    prefix: "partition/test".into(),
                    ..Default::default()
                },
            )?
            .existing_namespace(namespace.clone())
            .await?;
            let birth = namespace
                .birth(&registration.birth.scope.cell)
                .await?
                .context("birth")?;
            let archive = namespace.cell_archive(
                &birth,
                cellar_ltx::LtxHost::default(),
                ArchiveLimits {
                    max_image_bytes: quota,
                    max_snapshot_bytes: 12_000_000_000,
                    ..Default::default()
                },
            )?;
            tokio::time::timeout(Duration::from_secs(1800), async {
                loop {
                    let observed = archive.recovery_metadata().await?;
                    let index = observed.as_ref().map(|head| head.position().index.get());
                    if ledger.value["archive_observed_index"] != serde_json::json!(index) {
                        ledger.value["archive_observed_index"] = serde_json::json!(index);
                        ledger.save()?;
                    }
                    if observed.is_some_and(|head| head.position().index >= admitted.started_at) {
                        return Ok::<_, anyhow::Error>(());
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            })
            .await??;
        }
        ledger.mark("archive_ready")?;
        ensure!(
            !cluster
                .nodes
                .last()
                .context("standby")?
                .info()
                .data_dir
                .join("raft")
                .exists(),
            "replacement has local Raft state"
        );
        let source_member = MemberId::new(source as u64 + 1)?;
        let source_node = NodeId::new([source as u8 + 1; 16])?;
        let placement = if from_s3 {
            RaftCellPlacement::JoiningFromS3 {
                member: target.member,
                source: source_member,
                source_node,
            }
        } else {
            RaftCellPlacement::Joining {
                member: target.member,
                source: source_member,
                source_node,
            }
        };
        ledger.mark("seed_started")?;
        let joined = cluster
            .join_cold_standby(definitions[3].application.clone(), placement)
            .await?;
        ledger.check("replacement_started_cold")?;
        ledger.mark("seed_installed")?;
        // Materialized installation already verified the complete physical base.
        ledger.mark("hydrated")?;
        let learner = joined.cell(namespace.namespace, &registration.birth.scope.cell)?;
        hot_read(
            &learner,
            &definitions[3],
            &samples,
            8,
            QueryConsistency::Stale,
            target.member,
        )
        .await?;
        ledger.check("served_locally")?;
        ledger.mark("first_read")?;
        let live: Vec<_> = handles
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != primary)
            .map(|(_, handle)| handle.clone())
            .chain(std::iter::once(learner.clone()))
            .collect();
        ensure!(
            observed_repair(&live, repair.id, RepairStep::PromoteAndRemove, quota)
                .await
                .context("promote the cold replacement")?
                .phase
                == RepairPhase::Joint
        );
        ensure!(
            observed_repair(&live, repair.id, RepairStep::FinishJoint, quota)
                .await
                .context("finish the replacement's joint configuration")?
                .phase
                == RepairPhase::Complete
        );
        if force_leader_change {
            let current = fixture::ready(&live, None).await?;
            // Deliberately make the sampled leader stale before the final
            // handoff. Repair completion and an accepted transfer do not pin
            // leadership; the controller must refresh it until the target is
            // actually command-ready.
            let mut next = None;
            for (index, handle) in handles.iter().enumerate() {
                if index != primary && !handle.status().await?.command_ready {
                    next = Some(index);
                    break;
                }
            }
            let next = next.context("another surviving handoff target")?;
            transfer_observed(&live, &handles[next], MemberId::new(next as u64 + 1)?)
                .await
                .context("inject a stale handoff leader")?;
            ensure!(
                !live[current].status().await?.command_ready,
                "handoff did not invalidate the sampled leader"
            );
        }
        transfer_observed(&live, &learner, target.member)
            .await
            .context("transfer leadership to the cold replacement")?;
        fixture::ready(std::slice::from_ref(&learner), None).await?;
        ledger.mark("promoted")?;
        let resumed_options = fixture::command_options(2, 1)?;
        let resumed = learner
            .command(
                definitions[3].increment.clone(),
                0_u64.to_be_bytes().to_vec(),
                resumed_options.clone(),
            )
            .await?;
        ensure!(resumed.result == 9_u64.to_be_bytes());
        hot_read(
            &learner,
            &definitions[3],
            &samples,
            9,
            QueryConsistency::AtLeast(resumed.version_token.clone()),
            target.member,
        )
        .await?;
        ledger.check("first_write_verified")?;
        ledger.check("version_verified")?;
        ledger.mark("first_write_verified")?;
        let retry = learner
            .command(
                definitions[3].increment.clone(),
                0_u64.to_be_bytes().to_vec(),
                resumed_options,
            )
            .await?;
        ensure!(
            retry.id == resumed.id
                && retry.result == resumed.result
                && retry.version == resumed.version
                && retry.durability == resumed.durability
        );
        let old = learner
            .command(
                definitions[3].increment.clone(),
                0_u64.to_be_bytes().to_vec(),
                first_options,
            )
            .await?;
        // A retained receipt is proven by the current committed image. After
        // a later command its covering version may advance without reexecution.
        ensure!(
            old.id == first.id && old.result == first.result && old.durability == first.durability
        );
        ensure!(old.version.covers(&first.version)?);
        ensure!(
            calls[3].load(Ordering::SeqCst) == 1
                && calls
                    .iter()
                    .map(|count| count.load(Ordering::SeqCst))
                    .sum::<usize>()
                    == 2,
            "recovery or retry executed a callback twice"
        );
        ledger.check("receipt_verified")?;
        ledger.check("callbacks_once")?;
        ledger.mark("receipt_verified")?;
        fixture::verify_payloads(
            &learner,
            verify[3].clone(),
            9,
            payload_bytes,
            &samples,
            QueryConsistency::AtLeast(resumed.version_token),
            target.member,
        )
        .await?;
        ledger.check("payloads_verified")?;
        ledger.mark("payloads_verified")?;
        Ok(())
    }
    .await;
    // Capture failed and in-flight work too. Observability must not disappear
    // precisely when a recovery stage fails; incomplete ledgers still fail the
    // runner's acceptance checks.
    let observations = serde_json::to_value(
        cluster
            .services
            .iter()
            .map(RaftCells::performance)
            .collect::<Vec<_>>(),
    );
    if let Err(error) = &result {
        ledger.value["failure"] = format!("{error:#}").into();
    }
    let closed = cluster.close().await;
    if let Ok(value) = &observations {
        ledger.value["observations"] = value.clone();
    }
    if closed.is_ok() {
        ledger.check("cleanup_joined")?;
    }
    let saved = ledger.save();
    match (result, closed) {
        (Err(error), Err(cleanup)) => {
            return Err(error.context(format!("recovery cleanup also failed: {cleanup}")))
        }
        (Err(error), _) => return Err(error),
        (_, Err(error)) => return Err(error),
        (Ok(()), Ok(())) => (),
    }
    observations?;
    saved?;
    ledger.check("cleanup_joined")?;
    ledger.mark("cleanup_joined")?;
    ensure!(ledger.value["events"].as_array().context("events")?.len() == PHASES.len());
    ledger.value["complete"] = true.into();
    ledger.save()?;
    if let Some(file) = ledger.file.as_mut() {
        file.sync_all()?;
    }
    Ok(ledger.value)
}

async fn small(backend: &str, force_leader_change: bool) -> Result<()> {
    let mut root = match std::env::var_os("CELLAR_RECOVERY_WORKDIR") {
        Some(directory) => tempfile::tempdir_in(directory)?,
        None => tempfile::tempdir()?,
    };
    root.disable_cleanup(keep_node_state()?);
    let quota = 12 * 1024 * 1024;
    let source = fixture::make_template(root.path(), quota, false)?;
    let evidence = drill(
        source,
        root.path(),
        None,
        quota,
        Arc::new(object_store::memory::InMemory::new()),
        backend,
        force_leader_change,
    )
    .await?;
    ensure!(
        evidence["complete"] == true
            && evidence["events"].as_array().context("events")?.len() == PHASES.len()
    );
    let observations = evidence["observations"]
        .as_array()
        .context("observations")?;
    for phase in [
        "recovery_authorize",
        "recovery_install",
        if backend == "peer" {
            "recovery_peer_verify"
        } else {
            "recovery_archive_replay"
        },
    ] {
        ensure!(
            observations
                .iter()
                .any(|owner| owner["operations"]
                    .as_array()
                    .is_some_and(|operations| operations.iter().any(|operation| operation
                        ["name"]
                        == phase
                        && operation["succeeded"].as_str().is_some_and(|count| count
                            .parse::<u64>()
                            .is_ok_and(|count| count > 0))))),
            "missing physical recovery observation {phase}"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn small_peer_recovers_with_complete_phase_ledger() -> Result<()> {
    small("peer", false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn small_archive_recovers_with_complete_phase_ledger() -> Result<()> {
    small("memory_archive", false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn small_peer_leader_change_does_not_strand_repair() -> Result<()> {
    small("peer", true).await
}

async fn physical(from_s3: bool, quota: u64, reuse_fixture: bool) -> Result<()> {
    let workdir = PathBuf::from(std::env::var_os("CELLAR_LARGE_IMAGE_WORKDIR").context("workdir")?);
    let mut fixture_directory = tempfile::tempdir_in(&workdir)?;
    fixture_directory.disable_cleanup(keep_node_state()?);
    let supplied = reuse_fixture
        .then(|| std::env::var_os("CELLAR_LARGE_IMAGE_FIXTURE"))
        .flatten();
    let source = if let Some(source) = supplied {
        PathBuf::from(source)
    } else {
        let path = fixture_directory.path().to_path_buf();
        let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
        let result = domain
            .blocking(move || fixture::make_template(&path, quota, true))?
            .await?;
        domain.shutdown().await;
        result?
    };
    let store = object_store::aws::AmazonS3Builder::from_env()
        .with_bucket_name(std::env::var("CELLAR_LARGE_IMAGE_BUCKET")?)
        .with_endpoint(std::env::var("CELLAR_LARGE_IMAGE_ENDPOINT")?)
        .with_allow_http(true)
        .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch)
        .build()?;
    let prefix = object_store::path::Path::from(format!(
        "cold-recovery-qualification/{:032x}",
        rand::random::<u128>()
    ));
    eprintln!("cold-recovery object_prefix={prefix}");
    let store = Arc::new(object_store::prefix::PrefixStore::new(store, prefix));
    let secondary = std::env::var_os("CELLAR_LARGE_IMAGE_SECONDARY_WORKDIR").map(PathBuf::from);
    drill(
        source,
        &workdir,
        secondary.as_deref(),
        quota,
        store,
        if from_s3 { "rustfs" } else { "peer" },
        false,
    )
    .await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires near-quota 10 GB fixture, workspace disk and RustFS credentials"]
async fn decimal_ten_gb_peer_recovers_with_complete_phase_ledger() -> Result<()> {
    physical(false, 10_000_000_000, true).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires near-quota 10 GB fixture, workspace disk and RustFS credentials"]
async fn decimal_ten_gb_rustfs_recovers_with_complete_phase_ledger() -> Result<()> {
    physical(true, 10_000_000_000, true).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires actual 272 MiB fixture, workspace disk and RustFS credentials"]
async fn buffered_archive_rustfs_recovers_with_complete_phase_ledger() -> Result<()> {
    physical(true, 272 * 1024 * 1024, false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires actual 1,280 MiB fixture, workspace disk and RustFS credentials"]
async fn gigabyte_archive_rustfs_recovers_with_complete_phase_ledger() -> Result<()> {
    physical(true, 1280 * 1024 * 1024, false).await
}
