// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Observe the sealed source checkpoint, then cut its real archive publication.
use super::*;
use cellar_ltx::{
    DirectFileSystem, FileSystem, HostDirEntry, HostDirIterator, HostFile, HostFileLock,
    HostMetadata,
};
use sha2::{Digest, Sha256};
use std::{io, path::Path, sync::atomic::AtomicBool};

#[derive(Debug)]
struct Checkpoint {
    intents: u64,
    reserved: bool,
    minimum: u8,
}

struct Files {
    first: Mutex<Option<std::result::Result<Checkpoint, String>>>,
    store: Arc<catalog_fault::Store>,
    cut: AtomicBool,
}

impl Files {
    fn inspect(&self, path: &Path) -> Result<Option<Checkpoint>> {
        let mut url =
            reqwest::Url::from_file_path(path).map_err(|_| anyhow::anyhow!("image URI"))?;
        url.query_pairs_mut()
            .append_pair("mode", "ro")
            .append_pair("immutable", "1");
        let db = rusqlite::Connection::open_with_flags(
            url.as_str(),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                | rusqlite::OpenFlags::SQLITE_OPEN_URI
                | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        let source: bool = db.query_row(
            "SELECT count(*)=2 FROM sqlite_schema WHERE name IN ('__sys_outbox_lanes','__sys_raft_native_work')",
            [],
            |r| r.get(0),
        )?;
        if !source {
            return Ok(None);
        }
        let claimed: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM __sys_raft_native_work WHERE reason='outbox' AND claimed=1)",
            [],
            |r| r.get(0),
        )?;
        if !claimed {
            return Ok(None);
        }
        let first = 1u64.to_be_bytes();
        let receipt = db.query_row(
            "SELECT count(*),min(minimum_durability) FROM __sys_raft_receipts
             WHERE operation='__sys_raft_native_work' AND rpc_step=?1",
            [first.as_slice()],
            |r| Ok((r.get::<_, u64>(0)?, r.get::<_, Option<u8>>(1)?)),
        )?;
        if receipt.0 == 0 {
            return Ok(None);
        }
        let has_intents: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name='__sys_raft_binding_intents')",
            [],
            |r| r.get(0),
        )?;
        let intents = if has_intents {
            db.query_row(
                "SELECT count(*) FROM __sys_raft_binding_intents i JOIN __sys_raft_receipts r
             ON i.parent_client=r.client_id AND i.parent_sequence=r.sequence
             WHERE r.operation='__sys_raft_native_work' AND r.rpc_step=?1 AND i.phase=0",
                [first.as_slice()],
                |r| r.get(0),
            )?
        } else {
            0
        };
        let reserved = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM __sys_outbox_lanes WHERE dispatch_generation=1 AND attempts=1 AND retry_at_ms>0)",
            [],
            |r| r.get(0),
        )?;
        Ok(Some(Checkpoint {
            intents,
            reserved,
            minimum: receipt.1.context("work receipt minimum")?,
        }))
    }
}

impl FileSystem for Files {
    fn create_read_write(&self, path: &Path) -> io::Result<HostFile> {
        DirectFileSystem.create_read_write(path)
    }

    fn open_read_write(&self, path: &Path) -> io::Result<HostFile> {
        DirectFileSystem.open_read_write(path)
    }

    fn clone_read_write(&self, from: &Path, to: &Path) -> io::Result<Option<HostFile>> {
        DirectFileSystem.clone_read_write(from, to)
    }

    fn temporary_file(&self, directory: Option<&Path>) -> io::Result<HostFile> {
        DirectFileSystem.temporary_file(directory)
    }

    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        DirectFileSystem.read(path)
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<HostDirEntry>> {
        DirectFileSystem.read_dir(path)
    }

    fn read_dir_iter(&self, path: &Path) -> io::Result<HostDirIterator> {
        DirectFileSystem.read_dir_iter(path)
    }

    fn metadata(&self, path: &Path) -> io::Result<HostMetadata> {
        DirectFileSystem.metadata(path)
    }

    fn symlink_metadata(&self, path: &Path) -> io::Result<HostMetadata> {
        DirectFileSystem.symlink_metadata(path)
    }

    fn create(&self, path: &Path) -> io::Result<HostFile> {
        DirectFileSystem.create(path)
    }

    fn create_new(&self, path: &Path) -> io::Result<HostFile> {
        DirectFileSystem.create_new(path)
    }

    fn open(&self, path: &Path) -> io::Result<HostFile> {
        DirectFileSystem.open(path)
    }

    fn write(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        DirectFileSystem.write(path, bytes)
    }

    fn sync_all(&self, path: &Path) -> io::Result<()> {
        DirectFileSystem.sync_all(path)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        DirectFileSystem.rename(from, to)?;
        if to.extension().is_some_and(|e| e == "sqlite")
            && to.to_string_lossy().contains("/raft-cells/images/")
            && !to.to_string_lossy().contains("/archive-verifier/")
        {
            // Passive observation only: no I/O is held and no serving authority
            // is inferred from this uncommitted local generation.
            let mut first = self.first.lock().unwrap();
            if first.is_none() {
                match self.inspect(to) {
                    Ok(None) => {}
                    value => {
                        *first = Some(value.map(|v| v.unwrap()).map_err(|e| format!("{e:#}")));
                        if self.cut.load(Ordering::Acquire) {
                            self.store
                                .deny_sandbox_parent_writes
                                .store(true, Ordering::Release);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        DirectFileSystem.remove_file(path)
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        DirectFileSystem.remove_dir_all(path)
    }

    fn remove_dir_batch(&self, path: &Path, limit: usize) -> io::Result<bool> {
        DirectFileSystem.remove_dir_batch(path, limit)
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        DirectFileSystem.create_dir_all(path)
    }

    fn lock_file(&self, path: &Path) -> io::Result<HostFileLock> {
        DirectFileSystem.lock_file(path)
    }
}

pub async fn qualify() -> Result<()> {
    for minimum in [Durability::Quorum, Durability::S3] {
        let store = Arc::new(catalog_fault::Store::default());
        let files = Arc::new(Files {
            first: Mutex::new(None),
            store: store.clone(),
            cut: AtomicBool::new(false),
        });
        let (application, declared) = application()?;
        let definitions = [
            "Counter",
            cellar::deploy::EVENT_STREAM_CLASS,
            cellar::deploy::EVENT_CATALOG_CLASS,
        ]
        .into_iter()
        .map(|name| {
            Ok((
                application.resolve_raft_class(name)?,
                if name == "Counter" {
                    minimum
                } else {
                    Durability::Quorum
                },
            ))
        })
        .collect::<Result<Vec<_>>>()?;
        let mut fixture =
            Fixture::with_definition_policies(false, definitions, application, store.clone())
                .await?;
        fixture.configuration = Some(declared);
        fixture.event_streams = vec![definition()];
        fixture.sql_workers = 1;
        fixture.max_input_bytes = 384 << 10;
        fixture.cell_limits.events.max_event_bytes = 2048;
        fixture.cell_limits.events.max_batch_bytes = 4096;
        fixture.cell_limits.events.max_outbox_bytes = 64 << 10;
        fixture.effects = Some(RaftEffectOptions {
            callback_timeout_ms: 5000,
            ..Default::default()
        });
        let domain = fixture.domain.clone();
        let mut cluster = Box::pin(ManagedCluster::start_with_filesystems(
            fixture,
            vec![files.clone(); 3],
        ))
        .await?;
        let work = domain
            .run(async {
                named_service::activate_namespace(&cluster, 0, "stream-archive").await?;
                named_service::activate_namespace(&cluster, 1, "catalog-archive").await?;
                for node in &cluster.nodes {
                    node.activate_event_resources().await?;
                }
                let fixture = &cluster.fixtures[0];
                let catalog = verification::Catalog::new(
                    fixture.store.clone(),
                    verification::CatalogOptions {
                        prefix: "managed/".into(),
                        ..Default::default()
                    },
                )?;
                let namespace = catalog
                    .existing_namespace(fixture.declaration.namespace.clone())
                    .await?;
                let group = cluster.history;
                let mut digest = Sha256::new();
                digest.update(b"cellar-cell-group-archive-history-1\0");
                for id in [
                    group.cluster.bytes(),
                    group.namespace.bytes(),
                    group.group.bytes(),
                    group.incarnation.bytes(),
                ] {
                    digest.update(id);
                }
                *store.sandbox_parent_archive_prefix.lock().unwrap() =
                    Some(object_store::path::Path::from(format!(
                        "{}cell-group-archives/cellar-cell-group-archive-checkpoint-1/{}",
                        namespace.data_prefix(),
                        hex::encode(digest.finalize())
                    )));
                let leader = managed_fixture::leader(&cluster.hosts, group).await?;
                let scope = cellar_native::cell_id::Namespace::new(
                    "Counter",
                    format!("cells:v1:{}:{SCRIPT}:Counter", SCRIPT.len()),
                )
                .id_from_name("producer")
                .scope();
                let cell = cluster.hosts[leader].cell(CellHistory {
                    cluster: group.cluster,
                    namespace: group.namespace,
                    cell: CellId::new(scope)?,
                    incarnation: IncarnationId::new([226; 16])?,
                })?;
                cell.enroll(
                    CellEnrollmentRequest {
                        creation: ProposalId::new([227; 16])?,
                        durability: minimum,
                    },
                    TIMEOUT,
                )
                .await?;
                files
                    .cut
                    .store(minimum == Durability::S3, Ordering::Release);
                let retry = u64::try_from(cellar::asyncrt::wall_ms())? + 60_000;
                let options =
                    CommandOptions::new(CommandId::new(ClientId::new([228; 16])?, 1)?, retry);
                let emitted = cell
                    .command(&cell.contract("Emit")?, b"[]".to_vec(), options.clone())
                    .await;
                if let Err(error) = emitted {
                    ensure!(
                        minimum == Durability::S3
                            && matches!(
                                error.cause.as_ref(),
                                CellDispatchError::Runtime(RuntimeError::DeadlineExceeded)
                            ),
                        "unexpected Emit failure: {error:?}"
                    );
                }
                cellar::asyncrt::timeout(TIMEOUT, async {
                    while files.first.lock().unwrap().is_none() {
                        cellar::asyncrt::sleep(Duration::from_millis(10)).await;
                    }
                })
                .await?;
                {
                    let first = files.first.lock().unwrap();
                    let checkpoint = first
                        .as_ref()
                        .unwrap()
                        .as_ref()
                        .map_err(|e| anyhow::anyhow!(e.clone()))?;
                    ensure!(
                        checkpoint.intents == 1
                            && checkpoint.reserved
                            && checkpoint.minimum == u8::from(minimum == Durability::S3),
                        "first outbox proof split claim from intent: {checkpoint:?}"
                    );
                }
                if minimum == Durability::S3 {
                    // Observe the actual destination through its public read API
                    // across the original five-second callback; never query or
                    // mutate the source to prompt a recovery turn.
                    let until = cellar::asyncrt::instant() + TIMEOUT;
                    while cellar::asyncrt::instant() < until {
                        ensure!(
                            read(&cluster, leader).await? == json!([]),
                            "unproved outbox reached destination"
                        );
                        cellar::asyncrt::sleep(Duration::from_millis(100)).await;
                    }
                    ensure!(
                        store.denied_sandbox_parent_writes.load(Ordering::Acquire) > 0,
                        "source archive cut was not exercised"
                    );
                    store
                        .deny_sandbox_parent_writes
                        .store(false, Ordering::Release);
                }
                cellar::asyncrt::timeout(Duration::from_secs(60), async {
                    loop {
                        let records = read(&cluster, leader).await?;
                        let envelopes: Vec<events::Envelope> = serde_json::from_value(records)?;
                        if envelopes.len() == 1 {
                            ensure!(envelopes[0].key == "order-1", "unexpected event key");
                            return Ok::<_, anyhow::Error>(());
                        }
                        ensure!(envelopes.is_empty(), "duplicate destination events");
                        cellar::asyncrt::sleep(Duration::from_millis(100)).await;
                    }
                })
                .await
                .context("outbox did not autonomously recover its source proof")??;
                let replay = cell
                    .command(&cell.contract("Emit")?, b"[]".to_vec(), options)
                    .await?;
                ensure!(replay.result == b"true", "Emit receipt replay");
                let counts = cell
                    .query(
                        ProposalId::new([229; 16])?,
                        &cell.contract("Counts")?,
                        vec![],
                        QueryOptions::default(),
                    )
                    .await?;
                ensure!(
                    serde_json::from_slice::<serde_json::Value>(&counts.result)?
                        == json!([1, null]),
                    "source callback repeated"
                );
                Ok::<_, anyhow::Error>(())
            })
            .await
            .map_err(anyhow::Error::from)
            .and_then(std::convert::identity);
        store
            .deny_sandbox_parent_writes
            .store(false, Ordering::Release);
        let close = managed_fixture::close_group_nodes_with_groups(
            std::mem::take(&mut cluster.nodes),
            &cluster.hosts,
            3,
        )
        .await;
        if work.is_err() || close.is_err() {
            for root in std::mem::take(&mut cluster.roots) {
                eprintln!("retained outbox proof disk: {}", root.keep().display());
            }
        }
        domain.shutdown().await;
        work?;
        close?;
        eprintln!("outbox_work_proof_configuration minimum={minimum:?} materialized=true passed");
    }
    Ok(())
}
