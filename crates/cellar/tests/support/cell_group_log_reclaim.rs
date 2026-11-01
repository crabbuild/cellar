// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Physical Raft file reclamation, independent archive verification and retained
//! restart. Filesystem observations are assertions, not cache-debt estimates.
use super::*;
use raft::{GetEntriesContext, Storage as _};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

const PREFIX: u64 = 8;
const TOTAL: u64 = 12;

fn log_limits() -> ClusterLogLimits {
    ClusterLogLimits {
        target_file_bytes: 4096,
        purge_threshold_bytes: 8192,
        purge_interval_ms: None,
        ..Default::default()
    }
}

fn payload(sequence: u64, cell: usize) -> Vec<u8> {
    let mut bytes = sequence.to_be_bytes().to_vec();
    for block in 0..512_u64 {
        let mut hash = Sha256::new();
        hash.update(sequence.to_be_bytes());
        hash.update((cell as u64).to_be_bytes());
        hash.update(block.to_be_bytes());
        bytes.extend_from_slice(&hash.finalize());
    }
    bytes
}

fn attempt(sequence: u64, cell: usize, input: &[u8]) -> Result<CommandAttempt> {
    let mut request = cell_attempt(100 + (sequence as u8) * 2 + cell as u8)?;
    request.request.id = CommandId::new(ClientId::new([81; 16])?, sequence)?;
    request.request.operation = "Write".into();
    request.request.contract_digest = [83; 32];
    request.request.request_digest = command_request_digest(input);
    Ok(request)
}

fn read_image(pin: &CellImagePin) -> Result<Vec<u8>> {
    Ok(match pin {
        CellImagePin::Empty => Vec::new(),
        CellImagePin::Materialized(pin) => {
            let mut reader = pin.open_reader()?;
            let mut bytes = Vec::new();
            while (bytes.len() as u64) < reader.length() {
                let count = (reader.length() - bytes.len() as u64).min(64 * 1024) as usize;
                bytes.extend(reader.read_exact_at(bytes.len() as u64, count)?);
            }
            bytes
        }
        CellImagePin::Extents(pin) => {
            let mut bytes = Vec::new();
            for n in 0..pin.manifest().geometry().extent_count() {
                bytes.extend(
                    pin.view()
                        .read_extent(pin.manifest().extent(n).context("extent")?)?,
                );
            }
            bytes
        }
    })
}

fn assert_image(root: &PreparedGroupManifest, name: &CellId, count: u64) -> Result<()> {
    let (leaf, pin) = root.pin_cell(name).context("selected SQL cell")?;
    assert_eq!(leaf.version.sequence.get(), count + 1);
    assert_eq!(leaf.effects.next_alarm_unix_millis, Some(5000));
    let temp = tempfile::tempdir()?;
    let file = temp.path().join("oracle.sqlite");
    std::fs::write(&file, read_image(&pin)?)?;
    let db =
        rusqlite::Connection::open_with_flags(file, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    assert_eq!(
        db.query_row("SELECT n FROM value", [], |r| r.get::<_, u64>(0))?,
        count
    );
    assert_eq!(
        db.query_row("SELECT COUNT(*) FROM payload", [], |r| r.get::<_, u64>(0))?,
        count
    );
    Ok(())
}

async fn command(
    fixture: &RuntimeCluster,
    class: Arc<RaftClass>,
    cell: &CellHistory,
    ordinal: usize,
    sequence: u64,
) -> Result<LogIndex> {
    let group = fixture.cluster.birth.route().history;
    let pool = &fixture.appliers[&(0, group)].0;
    let input = payload(sequence, ordinal);
    let expected = Sha256::digest(&input).to_vec();
    let authority = fixture.runtimes[0]
        .execution_authority(group, cell.clone())
        .await?;
    let CellSqlOutcome::Prepared(prepared) = pool
        .stage_registered(
            fixture.cell_options(cell).await?,
            authority,
            attempt(sequence, ordinal, &input)?,
            class,
            input,
            true,
        )
        .await?
    else {
        anyhow::bail!("new registered command must prepare")
    };
    pool.mark_submitted(cell.clone(), prepared.proposal())
        .await?;
    let accepted = accepted_sql(&fixture.runtimes[0], &prepared).await?;
    controller::applied(fixture, group, accepted.position.index).await?;
    let authority = fixture.runtimes[0]
        .execution_authority(group, cell.clone())
        .await?;
    let receipt = pool.finish(authority).await?;
    assert_eq!(receipt.receipt().receipt.result(), expected);
    assert_eq!(receipt.receipt().version.history, *cell);
    assert_eq!(receipt.receipt().version.sequence.get(), sequence + 1);
    Ok(accepted.position.index)
}

#[derive(Clone)]
struct SelectedLog {
    marker: GroupManifest,
    checkpoint: Option<GroupManifest>,
    entries: Vec<raft::prelude::Entry>,
}

impl SelectedLog {
    fn capture(log: &ReplicaLog, history: GroupHistory) -> Result<Self> {
        let group = log.cell_group(&history)?;
        Ok(Self {
            marker: group.group_manifest()?.context("selected manifest")?,
            checkpoint: group.checkpoint_manifest()?,
            entries: group.entries(
                group.first_index()?,
                group.last_index()? + 1,
                None,
                GetEntriesContext::empty(false),
            )?,
        })
    }

    fn check(&self, log: &ReplicaLog) -> Result<()> {
        let actual = Self::capture(log, self.marker.route().history)?;
        assert_eq!(actual.marker, self.marker);
        assert_eq!(actual.checkpoint, self.checkpoint);
        assert_eq!(actual.entries, self.entries);
        Ok(())
    }
}

async fn files(domain: &ExecutionDomain, path: PathBuf) -> Result<BTreeMap<String, u64>> {
    domain
        .blocking(move || {
            let mut files = BTreeMap::new();
            for item in std::fs::read_dir(path)? {
                let item = item?;
                let name = item.file_name().to_string_lossy().into_owned();
                if name.ends_with(".raftlog") || name.ends_with(".rewrite") {
                    files.insert(name, item.metadata()?.len());
                }
            }
            Ok(files)
        })?
        .await?
}

async fn checkpoint(
    fixture: &RuntimeCluster,
    pool: &CellGroupCheckpointPool,
    marker: &GroupManifest,
) -> Result<()> {
    let history = marker.route().history;
    let source = fixture.runtimes[0]
        .capture_checkpoint(history, pool)
        .await?;
    let prepared = fixture
        .cluster
        .domain
        .blocking(move || source.prepare_publication())?
        .await??;
    assert_eq!(prepared.marker(), marker);
    fixture.runtimes[0]
        .publish_checkpoint(history, prepared)
        .await?;
    controller::wait_for(async || {
        Ok(fixture.cluster.voters[0]
            .log
            .cell_group(&history)?
            .checkpoint_manifest()?
            .as_ref()
            == Some(marker)
            && pool.usage().sources == 0)
    })
    .await
}

async fn recover_primary(
    disk: tempfile::TempDir,
    directory: RangeDirectory,
    birth: CellGroupBirth,
    store: Arc<dyn object_store::ObjectStore>,
    rooted: bool,
    saved: &[Vec<SelectedLog>],
) -> Result<RuntimeCluster> {
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let mut voters = Vec::new();
    let mut appliers = Vec::new();
    for (node, selected) in saved.iter().enumerate() {
        let number = node as u8 + 1;
        let base = disk.path().join(format!("node-{number}"));
        let mut options = ReplicaLogOptions::new(
            base.join("log"),
            NodeId::new([number; 16])?,
            DiskLineage::new([number + 10; 16])?,
            ProcessGeneration::new([91; 16])?,
            LogOpenMode::Resume,
        );
        options.target_file_bytes = log_limits().target_file_bytes;
        options.purge_threshold_bytes = log_limits().purge_threshold_bytes;
        options.purge_interval_ms = None;
        let log = domain
            .run(ReplicaLog::open(options, Arc::new(DirectFileSystem)))
            .await??;
        for expected in selected {
            expected.check(&log)?;
        }
        let images = GenerationStore::open(
            base.join("images"),
            Arc::new(DirectFileSystem),
            Default::default(),
        )?;
        let extents = if rooted {
            Some(NodeExtentStore::open(
                disk.path().join(format!("rooted-{node}")),
                Arc::new(DirectFileSystem),
                Default::default(),
            )?)
        } else {
            None
        };
        let config = checkpoint_receive::config(images.clone(), extents.clone(), &base)?;
        let group = log.cell_group(&birth.route().history)?;
        let (applier, root) = domain
            .blocking(move || -> Result<_> {
                let pages = ManifestPageStore::open(
                    &base.join("pages"),
                    Arc::new(DirectFileSystem),
                    1024,
                    16 * 1024 * 1024,
                    16,
                )?;
                Ok(CellGroupApplier::recover(
                    group,
                    pages,
                    config,
                    Default::default(),
                    Default::default(),
                    100,
                )?)
            })?
            .await??;
        let sibling = selected[1].clone();
        let sibling_group = log.cell_group(&sibling.marker.route().history)?;
        let sibling_base = disk.path().join(format!("extra-31-node-{node}"));
        domain
            .blocking(move || -> Result<()> {
                let pages = ManifestPageStore::open(
                    &sibling_base.join("pages"),
                    Arc::new(DirectFileSystem),
                    1024,
                    16 * 1024 * 1024,
                    16,
                )?;
                let images = GenerationStore::open(
                    sibling_base.join("images"),
                    Arc::new(DirectFileSystem),
                    Default::default(),
                )?;
                let (applier, root) = CellGroupApplier::recover(
                    sibling_group,
                    pages,
                    checkpoint_receive::config(images, None, &sibling_base)?,
                    Default::default(),
                    Default::default(),
                    100,
                )?;
                assert_eq!(root.marker(), &sibling.marker);
                let (_, pin) = root
                    .pin_cell(&CellId::new("live-sibling")?)
                    .context("sibling image")?;
                assert_eq!(
                    read_image(&pin)?,
                    images::image(
                        None,
                        "CREATE TABLE value(n); INSERT INTO value VALUES(197);"
                    )?
                );
                drop(root);
                drop(applier);
                Ok(())
            })?
            .await??;
        let permit = domain
            .run(directory.bootstrap_group_member(&birth, &log))
            .await??;
        let owner = domain
            .run(CellGroupOwner::start(
                permit,
                root,
                RaftRuntimeOptions::default(),
                [number + 70; 32],
            ))
            .await??;
        let pool = CellGroupSqlPool::start(&log, sql_limits())?;
        appliers.push((pool, applier));
        voters.push(Voter {
            log,
            owner: Some(owner),
            pages: None,
            images,
            extents,
            tree: ManifestTree::new(birth.route().clone(), Default::default())?,
            gate: fault::Gate::new(None),
            paused: false,
            held: None,
        });
    }
    let history = birth.route().history;
    let cluster = Cluster {
        _root: disk,
        domain,
        directory: directory.clone(),
        store,
        birth,
        voters,
        network: VecDeque::new(),
        reads: Vec::new(),
        options: RaftRuntimeOptions::default(),
    };
    let mut fixture = Box::pin(RuntimeCluster::from_cluster(cluster, 256, 256, 1_000_000)).await?;
    for (node, applier) in appliers.into_iter().enumerate() {
        fixture.appliers.insert((node, history), applier);
    }
    Ok(fixture)
}

fn sql_limits() -> CellSqlLimits {
    CellSqlLimits {
        workers: 1,
        queue_per_worker: 2,
        max_pending_cells: 2,
        group_pending_cells: 2,
        node_bytes: 512 * 1024 * 1024,
        group_bytes: 512 * 1024 * 1024,
    }
}

pub(in super::super) async fn physical_log_reclamation(
    rooted: bool,
    store: Arc<dyn object_store::ObjectStore>,
) -> Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let class = Arc::new(
        RaftClass::new("Counter", 1, [81; 32])?
            .alarm("Wake", [82; 32], |_, _| Ok(()))?
            .command("Write", [83; 32], move |context, input| {
                observed.fetch_add(1, Ordering::SeqCst);
                let sequence = u64::from_be_bytes(input[..8].try_into()?);
                context.sql().execute_batch(
                    "CREATE TABLE IF NOT EXISTS payload(seq INTEGER PRIMARY KEY, body BLOB);",
                )?;
                context.sql().execute("UPDATE value SET n=n+1", [])?;
                context.sql().execute(
                    "INSERT INTO payload VALUES(?1,?2)",
                    rusqlite::params![sequence, &input[8..]],
                )?;
                context.set_alarm("Wake", 5000, b"reclaimed-but-retained")?;
                Ok(Sha256::digest(input).to_vec())
            })?,
    );
    let cluster = Box::pin(Cluster::with_store_log_limits(
        RaftRuntimeOptions::default(),
        tempfile::tempdir()?,
        Arc::new(DirectFileSystem),
        class.schema_digest(),
        store,
        Durability::Quorum,
        log_limits(),
    ))
    .await?;
    let (mut fixture, route, a, b) = Box::pin(registered::setup_cluster(
        rooted,
        cluster,
        Arc::new(DirectFileSystem),
    ))
    .await?;
    let second = controller::add_second(&mut fixture, route.history).await?;
    let (archive, remote) = archive_controller::registration(&fixture, &route, rooted).await?;
    let controllers = controller::start_with_limits(&mut fixture, controller::limits()).await?;
    fixture.runtimes[0].campaign(second).await?;
    controller::wait_for(async || Ok(fixture.runtimes[0].status(second).await?.ready)).await?;
    let sibling_route = fixture.cluster.voters[0]
        .log
        .cell_group(&second)?
        .group_manifest()?
        .context("sibling root")?
        .route()
        .clone();
    let sibling = history(&sibling_route, "live-sibling")?;
    let sibling_image = images::image(
        None,
        "CREATE TABLE value(n); INSERT INTO value VALUES(197);",
    )?;
    let accepted = fixture.runtimes[0]
        .propose(
            second,
            apply::enrollment_bytes(&sibling_route, &sibling, &sibling_image, false)?,
        )
        .await?;
    controller::applied(&fixture, second, accepted.position.index).await?;
    controllers[0].archive(archive.clone())?.response().await?;
    let mut reader = None;
    for sequence in 1..=PREFIX {
        for (ordinal, cell) in [&a, &b].into_iter().enumerate() {
            command(&fixture, class.clone(), cell, ordinal, sequence).await?;
        }
        if sequence == 1 {
            reader = Some(fixture.appliers[&(0, route.history)].1.recover_selected()?);
        }
    }
    let reader = reader.context("old physical reader")?;
    let (_, pin) = reader.pin_cell(&a.cell).context("old image")?;
    let old_bytes = read_image(&pin)?;
    drop(pin);
    let group = fixture.cluster.voters[0].log.cell_group(&route.history)?;
    let cut = group.group_manifest()?.context("covered cut")?;
    archive_controller::published(&controllers[0], route.history, cut.position().index).await?;
    let coverage =
        archive_controller::verified_current(&fixture, &archive, rooted, "reclaim-audit").await?;
    assert_eq!(coverage.marker(), &cut);
    assert_image(coverage.recovery().manifest(), &a.cell, PREFIX)?;
    assert_image(coverage.recovery().manifest(), &b.cell, PREFIX)?;
    let pool = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    let log_path = fixture.cluster._root.path().join("node-1/log/log");
    let before = files(&fixture.cluster.domain, log_path.clone()).await?;
    assert!(before.len() > 8, "actual file rotation is required");
    checkpoint(&fixture, &pool, &cut).await?;
    assert_eq!(group.first_index()?, cut.position().index.get() + 1);
    let current = fixture.appliers[&(0, route.history)].1.recover_selected()?;
    let mut retained_source = pool.capture_snapshot(&group, &current, &group.snapshot(0, 3)?)?;
    drop(current);
    remote.outage.store(true, Ordering::Release);
    for sequence in PREFIX + 1..=TOTAL {
        for (ordinal, cell) in [&a, &b].into_iter().enumerate() {
            command(&fixture, class.clone(), cell, ordinal, sequence).await?;
        }
    }
    let saved: Vec<_> = fixture
        .cluster
        .voters
        .iter()
        .map(|voter| {
            Ok(vec![
                SelectedLog::capture(&voter.log, route.history)?,
                SelectedLog::capture(&voter.log, second)?,
            ])
        })
        .collect::<Result<_>>()?;
    assert_eq!(saved[0][0].entries.len(), ((TOTAL - PREFIX) * 2) as usize);
    assert!(!saved[0][1].entries.is_empty());
    let mut after = before.clone();
    for _ in 0..64 {
        fixture.cluster.voters[0].log.purge().await?;
        for expected in &saved[0] {
            expected.check(&fixture.cluster.voters[0].log)?;
        }
        let (_, pin) = reader.pin_cell(&a.cell).context("retained old reader")?;
        assert_eq!(read_image(&pin)?, old_bytes);
        after = files(&fixture.cluster.domain, log_path.clone()).await?;
        if before.keys().any(|name| !after.contains_key(name))
            && after.values().sum::<u64>() < before.values().sum::<u64>()
        {
            break;
        }
    }
    let removed: Vec<_> = before
        .keys()
        .filter(|name| !after.contains_key(*name))
        .cloned()
        .collect();
    assert!(
        !removed.is_empty(),
        "purge must remove actual pre-checkpoint files"
    );
    assert!(
        after.values().sum::<u64>() < before.values().sum::<u64>(),
        "actual log-file length must decrease"
    );
    let receiver_pool = CellGroupCheckpointPool::new(checkpoint_fixture::limits())?;
    let mut receiver = receiver_pool.receive(
        fixture.appliers[&(2, route.history)].1.clone(),
        checkpoint_receive::scratch()?,
        fixture.cluster._root.path().into(),
    )?;
    let received = fixture
        .cluster
        .domain
        .blocking(move || {
            retained_source.stream(|chunk| receiver.append(chunk))?;
            receiver.finish()
        })?
        .await??;
    assert_eq!(received.marker(), &cut);
    assert_eq!(pool.usage().sources, 0);
    assert_eq!(receiver_pool.usage().sources, 1);
    drop(received);
    assert_eq!(receiver_pool.usage().sources, 0);
    assert_eq!(pool.usage().sources, 0);
    remote.outage.store(false, Ordering::Release);
    archive_controller::published(
        &controllers[0],
        route.history,
        saved[0][0].marker.position().index,
    )
    .await?;
    for controller in controllers {
        controller.close().await?;
    }
    // Quiesce all node-zero SQL owners, then reconcile the complete two-group
    // image inventory. Old readers and the selected checkpoint still pin their
    // closures while obsolete image/page files can be physically collected.
    let inventory: Vec<_> = fixture
        .appliers
        .iter()
        .filter(|((node, _), _)| *node == 0)
        .map(|(_, (_, applier))| applier.clone())
        .collect();
    fixture.appliers.retain(|(node, _), _| *node != 0);
    let mut image_files = 0;
    let mut page_files = 0;
    for _ in 0..32 {
        let report = fixture.cluster.voters[0]
            .log
            .reclaim_cell_storage(
                inventory.clone(),
                vec![],
                CellGroupReclaimLimits {
                    max_selected_cells: 64,
                    max_files: 1,
                    max_page_scan: 64,
                },
            )
            .await?;
        assert_eq!(report.groups, 2);
        assert!(report.image_files + report.page_files <= 1);
        image_files += report.image_files;
        page_files += report.page_files;
        let (_, pin) = reader
            .pin_cell(&a.cell)
            .context("reader during collection")?;
        assert_eq!(read_image(&pin)?, old_bytes);
    }
    assert!(image_files + page_files > 0, "actual orphan collection");
    let birth = fixture.cluster.birth.clone();
    let directory = fixture.cluster.directory.clone();
    let store = fixture.cluster.store.clone();
    let disk = std::mem::replace(&mut fixture.cluster._root, tempfile::tempdir()?);
    drop(reader);
    drop(group);
    drop(coverage);
    drop(archive);
    drop(inventory);
    fixture.close().await?;
    let mut fixture = Box::pin(recover_primary(
        disk, directory, birth, store, rooted, &saved,
    ))
    .await?;
    for node in 0..3 {
        let root = fixture.appliers[&(node, route.history)]
            .1
            .recover_selected()?;
        assert_image(&root, &a.cell, TOTAL)?;
        assert_image(&root, &b.cell, TOTAL)?;
    }
    fixture.runtimes[0].campaign(route.history).await?;
    fixture.drive(None).await?;
    for sequence in 1..=TOTAL {
        for (ordinal, cell) in [&a, &b].into_iter().enumerate() {
            let input = payload(sequence, ordinal);
            let authority = fixture.runtimes[0]
                .execution_authority(route.history, cell.clone())
                .await?;
            let CellSqlOutcome::Receipt(receipt) = fixture.appliers[&(0, route.history)]
                .0
                .stage_registered(
                    fixture.cell_options(cell).await?,
                    authority,
                    attempt(sequence, ordinal, &input)?,
                    class.clone(),
                    input.clone(),
                    false,
                )
                .await?
            else {
                anyhow::bail!("retained receipt")
            };
            assert_eq!(
                receipt.receipt().receipt.result(),
                Sha256::digest(input).to_vec()
            );
        }
    }
    assert_eq!(calls.load(Ordering::SeqCst), TOTAL as usize * 2);
    let source = fixture.runtimes[0]
        .capture_checkpoint(route.history, &pool)
        .await?;
    let prepared = fixture
        .cluster
        .domain
        .blocking(move || source.prepare_publication())?
        .await??;
    let group = fixture.cluster.voters[0].log.cell_group(&route.history)?;
    let mut write = GroupWrite::new(&group);
    write.snapshot = Some(SnapshotChange::CellGroupCheckpoint(prepared));
    assert!(
        matches!(
            fixture.cluster.voters[0]
                .log
                .persist_cells(cellar_logic::consensus::OperationId::new(912), vec![write])
                .await,
            Err(LogError::Overloaded)
        ),
        "reopen must not trust the lost archive frontier"
    );
    drop(group);
    let controllers = controller::start_with_limits(&mut fixture, controller::limits()).await?;
    for (ordinal, cell) in [&a, &b].into_iter().enumerate() {
        command(&fixture, class.clone(), cell, ordinal, TOTAL + 1).await?;
    }
    assert_eq!(calls.load(Ordering::SeqCst), (TOTAL + 1) as usize * 2);
    for node in 0..3 {
        let root = fixture.appliers[&(node, route.history)]
            .1
            .recover_selected()?;
        assert_image(&root, &a.cell, TOTAL + 1)?;
        assert_image(&root, &b.cell, TOTAL + 1)?;
    }
    for controller in controllers {
        controller.close().await?;
    }
    println!(
        "cell_group_log_reclamation_observations {}",
        serde_json::json!({
            "mode": if rooted { "extents" } else { "materialized" },
            "files_before": before.len(), "files_after": after.len(), "removed": removed,
            "bytes_before": before.values().sum::<u64>(), "bytes_after": after.values().sum::<u64>(),
            "checkpoint": cut.position().index.get(), "retained_suffix_entries": saved[0][0].entries.len(),
            "receipts_retried_without_callback": TOTAL * 2, "voters_reopened": 3, "groups_per_engine": 2,
            "obsolete_image_files_removed": image_files, "obsolete_page_files_removed": page_files,
            "new_commands_after_restart": 2
        })
    );
    fixture.close().await
}
