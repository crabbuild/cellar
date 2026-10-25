// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Full-size native serving starts from an identical verified template on all
//! initial replicas. Ordinary SDK writes retain normal small envelope bounds.
//! Fresh learner recovery still requires live quorum grants and member claims.

use super::*;
use cellar::application::consensus::verification::{
    CommittedImage, DurableApply, GroupWrite, ReplicaLog,
};
use cellar_logic::consensus::OperationId;
use cellar_ltx::apply::ImageState;
use cellar_ltx::generation::{GenerationLimits, GenerationStore, ImageId};
use cellar_ltx::{DirectFileSystem, FileSystem, TXID};
use raft::{prelude::Entry, Storage as _};
use sha2::{Digest, Sha256};
use std::{
    io::Read as _,
    path::{Path, PathBuf},
    sync::atomic::{AtomicUsize, Ordering},
};

const QUOTA: u64 = 10_000_000_000;

fn definition(calls: Arc<AtomicUsize>) -> Result<public_node::Counter> {
    let class = RaftClass::new("Counter", 1, [91; 32])?
        .command("Change", [92; 32], move |context, input| {
            calls.fetch_add(1, Ordering::SeqCst);
            match u64::from_be_bytes(input.try_into()?) {
                0 => (),
                1 => {
                    context
                        .sql()
                        .execute("INSERT INTO items(value) VALUES(zeroblob(1048576))", [])?;
                }
                2 => {
                    context
                        .sql()
                        .execute("DELETE FROM items WHERE id=(SELECT max(id) FROM items)", [])?;
                }
                _ => anyhow::bail!("invalid fixture action"),
            }
            context
                .sql()
                .execute("UPDATE counter SET value=value+1", [])?;
            let value: u64 = context
                .sql()
                .query_row("SELECT value FROM counter", [], |r| r.get(0))?;
            Ok(value.to_be_bytes().to_vec())
        })?
        .query("Read", [93; 32], |context, _| {
            let (value, bytes): (u64, u64) = context.query_row(
                "SELECT (SELECT value FROM counter),coalesce(sum(length(value)),0) FROM items",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            Ok([value.to_be_bytes(), bytes.to_be_bytes()].concat())
        })?;
    Ok(public_node::Counter {
        schema: class.schema_digest(),
        increment: class.contract("Change").unwrap(),
        get: class.contract("Read").unwrap(),
        application: Arc::new(
            Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_class(class)?,
        ),
    })
}

fn image_state(path: &Path) -> Result<ImageState> {
    let mut file = std::fs::File::open(path)?;
    let length = file.metadata()?.len();
    let mut buffer = vec![0; 65536];
    let mut hash = Sha256::new();
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    Ok(ImageState {
        epoch: 1,
        txid: TXID(1),
        checksum: 0,
        page_size: 4096,
        length,
        digest: hash.finalize().into(),
    })
}

fn prepared_template(
    source: &Path,
    directory: &Path,
    quota: u64,
) -> Result<(PathBuf, ImageState, u64)> {
    let path = directory.join("template.sqlite");
    match DirectFileSystem.clone_read_write(source, &path)? {
        Some(file) => drop(file),
        None => {
            std::fs::copy(source, &path)?;
        }
    }
    let db = rusqlite::Connection::open(&path)?;
    // The input fixture is full. Release only its sub-megabyte tail, keeping
    // room for framework setup/receipts while an ordinary 1 MiB growth fails.
    db.execute_batch(
        "DELETE FROM quota_tail;CREATE TABLE counter(value INTEGER);INSERT INTO counter VALUES(7);",
    )?;
    let bytes: u64 = db.query_row("SELECT sum(length(value)) FROM items", [], |r| r.get(0))?;
    drop(db);
    let state = image_state(&path)?;
    ensure!(
        state.length == quota / 4096 * 4096,
        "template must reach the page-rounded quota"
    );
    Ok((path, state, bytes))
}

fn preload(
    template: PathBuf,
    state: ImageState,
    registration: RaftCellRegistration,
    quota: u64,
) -> cluster::PreloadReplicas {
    Box::new(move |logs, store| {
        Box::pin(async move {
            let namespace = Catalog::new(
                store,
                CatalogOptions {
                    prefix: "partition/test".into(),
                    ..Default::default()
                },
            )?
            .open_namespace(registration.namespace)
            .await?;
            let birth = namespace.resolve_birth(registration.birth).await?;
            for options in logs {
                let data = options
                    .directory
                    .parent()
                    .context("replica data directory")?
                    .to_path_buf();
                let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
                let filesystem: Arc<dyn FileSystem> = Arc::new(DirectFileSystem);
                let log = domain
                    .run(ReplicaLog::open(options, filesystem.clone()))
                    .await??;
                // Real permanent initial-member claims precede the fixture's
                // template installation. No runtime has started or acknowledged a
                // command; all three replicas receive exactly the same baseline.
                let permit = namespace.bootstrap_member(&birth, &log).await?;
                let group = log.group(birth.scope())?;
                let image_directory = data
                    .join("raft-cells")
                    .join(hex::encode(birth.scope().namespace.bytes()))
                    .join(hex::encode(birth.scope().group.bytes()))
                    .join(hex::encode(birth.scope().incarnation.bytes()))
                    .join("images");
                let template = template.clone();
                let (images, pin) = domain
                    .blocking(move || {
                        let mut limits = GenerationLimits::default();
                        limits.apply.decode.max_image_bytes = quota;
                        limits.max_bytes = limits.max_bytes.max(8 * quota);
                        let images = GenerationStore::open(image_directory, filesystem, limits)?;
                        let pin =
                            images.prepare_image_file(ImageId::new([1; 16])?, &template, state)?;
                        Ok::<_, anyhow::Error>((images, pin))
                    })?
                    .await??;
                let mut write = GroupWrite::new(&group);
                write.entries.push(Entry {
                    index: 2,
                    term: 2,
                    ..Default::default()
                });
                let mut hard = group.initial_state()?.hard_state;
                hard.term = 2;
                hard.vote = 0;
                hard.commit = 2;
                write.hard_state = Some(hard);
                write.applied = Some(DurableApply {
                    position: LogPosition {
                        index: LogIndex::new(2),
                        term: Term::new(2),
                    },
                    image: Some(CommittedImage {
                        name: format!("{}.sqlite", hex::encode(pin.id().bytes())),
                        digest: state.digest,
                        length: state.length,
                        ltx_epoch: state.epoch,
                        txid: state.txid.0,
                        checksum: state.checksum,
                        page_size: state.page_size,
                    }),
                });
                log.persist(OperationId::new(1), vec![write]).await?;
                drop(permit);
                drop(pin);
                drop(images);
                log.close().await?;
                domain.shutdown().await;
            }
            Ok(())
        })
    })
}

fn command_options(client: u8, sequence: u64) -> Result<CommandOptions> {
    let mut options = public_node::options(sequence)?;
    options.id = CommandId::new(ClientId::new([client; 16])?, sequence)?;
    options.retry_until_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis() as u64
        + 86_400_000;
    options.timeout = Duration::from_secs(1800);
    Ok(options)
}

async fn ready(handles: &[RaftCellHandle], excluded: Option<usize>) -> Result<usize> {
    Ok(tokio::time::timeout(Duration::from_secs(1800), async {
        loop {
            for (index, handle) in handles.iter().enumerate() {
                if Some(index) != excluded && handle.status().await.is_ok_and(|s| s.command_ready) {
                    return index;
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await?)
}

async fn read(
    handle: &RaftCellHandle,
    contract: OperationContract,
    value: u64,
    bytes: u64,
    consistency: QueryConsistency,
) -> Result<()> {
    let stale_policy = if matches!(&consistency, QueryConsistency::Stale) {
        StaleReadPolicy::DisconnectedHistorical
    } else {
        StaleReadPolicy::RecentSource
    };
    let result = handle
        .query(
            contract,
            vec![],
            QueryOptions {
                consistency,
                timeout: Duration::from_secs(60),
                stale_policy,
                ..Default::default()
            },
        )
        .await?;
    ensure!(result.result == [value.to_be_bytes(), bytes.to_be_bytes()].concat());
    Ok(())
}

async fn drill(
    source: PathBuf,
    workdir: PathBuf,
    quota: u64,
    store: Arc<dyn object_store::ObjectStore>,
    from_s3: bool,
    secondary_workdir: Option<PathBuf>,
) -> Result<()> {
    let started = std::time::Instant::now();
    let source_root = tempfile::tempdir_in(&workdir)?;
    let path = source_root.path().to_path_buf();
    let preparation = ExecutionDomain::new(tokio::runtime::Handle::current());
    let (template, state, payload_bytes) = preparation
        .blocking(move || prepared_template(&source, &path, quota))?
        .await??;
    preparation.shutdown().await;
    eprintln!(
        "full-native stage=template bytes={} elapsed_s={:.3}",
        state.length,
        started.elapsed().as_secs_f64()
    );
    let calls: Vec<_> = (0..4).map(|_| Arc::new(AtomicUsize::new(0))).collect();
    let definitions = calls
        .iter()
        .cloned()
        .map(definition)
        .collect::<Result<Vec<_>>>()?;
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
                .map(|id| InitialMember {
                    member: MemberId::new(id).unwrap(),
                    node: NodeId::new([id as u8; 16]).unwrap(),
                })
                .collect(),
            learners: vec![],
        },
    };
    let roots = (0..4)
        .map(|index| {
            let directory = if index % 2 == 1 {
                secondary_workdir.as_ref().unwrap_or(&workdir)
            } else {
                &workdir
            };
            tempfile::tempdir_in(directory)
        })
        .collect::<std::io::Result<Vec<_>>>()?;
    let mut host = RaftHostOptions::default();
    host.execution.max_image_bytes = quota;
    host.execution.max_retry_window_ms = 86_400_000;
    let mut cluster = cluster::Cluster::start_with_preloaded_images(
        &definitions,
        vec![registration.clone()],
        host,
        store,
        roots,
        preload(template, state, registration.clone(), quota),
    )
    .await?;
    let handles = cluster
        .services
        .iter()
        .map(|s| s.cell(namespace.namespace, &registration.birth.scope.cell))
        .collect::<Result<Vec<_>, _>>()?;
    let primary = ready(&handles, None).await?;
    eprintln!(
        "full-native stage=ready elapsed_s={:.3}",
        started.elapsed().as_secs_f64()
    );
    read(
        &handles[primary],
        definitions[primary].get.clone(),
        7,
        payload_bytes,
        QueryConsistency::Linearizable,
    )
    .await?;
    let first_options = command_options(1, 1)?;
    let first = handles[primary]
        .command(
            definitions[primary].increment.clone(),
            0_u64.to_be_bytes().to_vec(),
            first_options.clone(),
        )
        .await?;
    ensure!(first.result == 8_u64.to_be_bytes());
    cluster.services[primary].close().await?;
    let replacement = ready(&handles, Some(primary)).await?;
    let retry = handles[replacement]
        .command(
            definitions[replacement].increment.clone(),
            0_u64.to_be_bytes().to_vec(),
            first_options,
        )
        .await?;
    ensure!(retry.result == first.result && retry.version == first.version);
    let second_options = command_options(1, 2)?;
    let second = handles[replacement]
        .command(
            definitions[replacement].increment.clone(),
            0_u64.to_be_bytes().to_vec(),
            second_options.clone(),
        )
        .await?;
    ensure!(second.result == 9_u64.to_be_bytes());
    eprintln!(
        "full-native stage=takeover-and-retry elapsed_s={:.3}",
        started.elapsed().as_secs_f64()
    );
    let full = handles[replacement]
        .command(
            definitions[replacement].increment.clone(),
            1_u64.to_be_bytes().to_vec(),
            command_options(1, 3)?,
        )
        .await;
    ensure!(
        matches!(full, Err(CommandFailure::StorageFull)),
        "growth beyond the full quota must report StorageFull: {full:?}"
    );
    read(
        &handles[replacement],
        definitions[replacement].get.clone(),
        9,
        payload_bytes,
        QueryConsistency::Linearizable,
    )
    .await?;
    let freed = handles[replacement]
        .command(
            definitions[replacement].increment.clone(),
            2_u64.to_be_bytes().to_vec(),
            command_options(2, 1)?,
        )
        .await?;
    ensure!(freed.result == 10_u64.to_be_bytes());
    let reused = handles[replacement]
        .command(
            definitions[replacement].increment.clone(),
            1_u64.to_be_bytes().to_vec(),
            command_options(2, 2)?,
        )
        .await?;
    ensure!(reused.result == 11_u64.to_be_bytes());
    ensure!(
        calls
            .iter()
            .map(|c| c.load(Ordering::SeqCst))
            .sum::<usize>()
            == 5,
        "a callback was replayed"
    );
    eprintln!(
        "full-native stage=quota-cleanup-reuse elapsed_s={:.3}",
        started.elapsed().as_secs_f64()
    );
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
                timeout: Duration::from_secs(1800),
            },
        )
        .await?;
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
            .context("birth")?;
        let archive = selected.cell_archive(
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
                if archive
                    .recovery_metadata()
                    .await?
                    .is_some_and(|m| m.position().index >= admitted.joined_at)
                {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        })
        .await??;
    }
    ensure!(
        !cluster.nodes[3].info().data_dir.join("raft").exists(),
        "replacement must have no local Raft state"
    );
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
        .await?;
    let learner = joined.cell(namespace.namespace, &registration.birth.scope.cell)?;
    read(
        &learner,
        definitions[3].get.clone(),
        11,
        payload_bytes,
        QueryConsistency::Linearizable,
    )
    .await?;
    ensure!(calls[3].load(Ordering::SeqCst) == 0);
    let duplicate = handles[replacement]
        .command(
            definitions[replacement].increment.clone(),
            0_u64.to_be_bytes().to_vec(),
            second_options,
        )
        .await?;
    // A reconstructed receipt retains its original result while its token may
    // name the newer covering image (the documented application contract).
    ensure!(duplicate.result == second.result);
    ensure!(duplicate
        .version
        .covers(&second.version)
        .is_ok_and(|covers| covers));
    ensure!(duplicate.id == second.id && duplicate.durability == second.durability);
    ensure!(
        calls
            .iter()
            .map(|c| c.load(Ordering::SeqCst))
            .sum::<usize>()
            == 5
    );
    eprintln!(
        "full-native stage=empty-worker-recovered elapsed_s={:.3}",
        started.elapsed().as_secs_f64()
    );
    let contract = definitions[3].get.clone();
    let namespace_id = namespace.namespace;
    let cell = registration.birth.scope.cell.clone();
    cluster = cluster
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
                    let handle = service.cell(namespace_id, &cell)?;
                    read(
                        &handle,
                        contract,
                        11,
                        payload_bytes,
                        QueryConsistency::Stale,
                    )
                    .await
                })
            }),
        )
        .await?;
    eprintln!(
        "full-native stage=restart-verified elapsed_s={:.3}",
        started.elapsed().as_secs_f64()
    );
    cluster.close().await
}

fn make_template(directory: &Path, quota: u64, incompressible: bool) -> Result<PathBuf> {
    let path = directory.join("source.sqlite");
    let db = rusqlite::Connection::open(&path)?;
    db.execute_batch("PRAGMA page_size=4096;PRAGMA synchronous=OFF;CREATE TABLE items(id INTEGER PRIMARY KEY,value BLOB);CREATE TABLE quota_tail(value BLOB);")?;
    db.pragma_update(None, "max_page_count", quota / 4096)?;
    let mut payload = vec![0; 1048576];
    if incompressible {
        let mut seed = 0x6a09_e667_f3bc_c909_u64;
        for bytes in payload.chunks_exact_mut(8) {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            bytes.copy_from_slice(&seed.to_le_bytes());
        }
    }
    loop {
        match db.execute("INSERT INTO items(value) VALUES(?1)", [&payload]) {
            Ok(_) => (),
            Err(rusqlite::Error::SqliteFailure(e, _))
                if e.code == rusqlite::ErrorCode::DiskFull =>
            {
                break
            }
            Err(e) => return Err(e.into()),
        }
    }
    loop {
        match db.execute("INSERT INTO quota_tail VALUES(zeroblob(2048))", []) {
            Ok(_) => (),
            Err(rusqlite::Error::SqliteFailure(e, _))
                if e.code == rusqlite::ErrorCode::DiskFull =>
            {
                break
            }
            Err(e) => return Err(e.into()),
        }
    }
    drop(db);
    Ok(path)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn preloaded_template_exercises_native_quota_failover_seed_and_restart() -> Result<()> {
    small_drill(true).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn preloaded_template_exercises_native_quota_failover_peer_seed_and_restart() -> Result<()> {
    small_drill(false).await
}

async fn small_drill(from_s3: bool) -> Result<()> {
    let root = tempfile::tempdir()?;
    let quota = 12 * 1024 * 1024;
    let source = make_template(root.path(), quota, false)?;
    drill(
        source,
        root.path().to_path_buf(),
        quota,
        Arc::new(object_store::memory::InMemory::new()),
        from_s3,
        None,
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires a real page-rounded 10 GB fixture, workspace disk and RustFS/AWS credentials"]
async fn decimal_ten_gb_rustfs_restores_native_serving_after_owner_and_local_state_loss(
) -> Result<()> {
    rustfs_drill(true).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires a real page-rounded 10 GB fixture, workspace disk and RustFS/AWS credentials"]
async fn decimal_ten_gb_peer_restores_native_serving_after_owner_and_local_state_loss() -> Result<()>
{
    // Peer transfer is the recovery path; the real store also bounds background
    // archive memory. An in-memory store would retain ten-gigabyte objects.
    rustfs_drill(false).await
}

async fn rustfs_drill(from_s3: bool) -> Result<()> {
    let workdir = PathBuf::from(std::env::var_os("CELLAR_LARGE_IMAGE_WORKDIR").context("workdir")?);
    let fixture_directory = tempfile::tempdir_in(&workdir)?;
    let source = match std::env::var_os("CELLAR_LARGE_IMAGE_FIXTURE") {
        Some(path) => PathBuf::from(path),
        None => {
            let path = fixture_directory.path().to_path_buf();
            let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
            let source = domain
                .blocking(move || make_template(&path, QUOTA, true))?
                .await??;
            domain.shutdown().await;
            source
        }
    };
    let store = object_store::aws::AmazonS3Builder::from_env()
        .with_bucket_name(std::env::var("CELLAR_LARGE_IMAGE_BUCKET")?)
        .with_endpoint(std::env::var("CELLAR_LARGE_IMAGE_ENDPOINT")?)
        .with_allow_http(true)
        .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch)
        .build()?;
    let prefix = object_store::path::Path::from(format!(
        "full-capacity-qualification/{:032x}",
        rand::random::<u128>()
    ));
    eprintln!("full-native prefix={prefix}");
    drill(
        source,
        workdir,
        QUOTA,
        Arc::new(object_store::prefix::PrefixStore::new(store, prefix)),
        from_s3,
        std::env::var_os("CELLAR_LARGE_IMAGE_SECONDARY_WORKDIR").map(PathBuf::from),
    )
    .await
}
