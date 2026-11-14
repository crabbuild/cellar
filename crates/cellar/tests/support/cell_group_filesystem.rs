// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Filesystem isolation, transaction authority and retained RF3 recovery.

use super::*;
use cellar::sdk::filesystem::{Errno, File, OpenOptions, Policy};

#[derive(Default)]
struct Files {
    escaped: Mutex<Option<File>>,
    writes: Arc<AtomicUsize>,
}

fn snapshot(storage: &Storage) -> Result<Vec<u8>> {
    let bytes = storage.fs()?.read_file("/value")?;
    ensure!(storage.get_json("value")? == Some(json!(bytes)));
    Ok(bytes)
}

fn retained_receipt(original: &CommandReceipt, replay: &CommandReceipt) -> Result<()> {
    ensure!(
        replay.id == original.id && replay.result == original.result,
        "retry changed the stored command identity or result"
    );
    // Versions select an image covering the receipt. Autonomous maintenance
    // can advance that image without executing the original command again.
    ensure!(
        replay.version.history == original.version.history
            && replay.version.sequence.get() >= original.version.sequence.get(),
        "retry lost original logical history or committed coverage"
    );
    ensure!(
        replay.witness.group == original.witness.group && replay.durability == original.durability
    );
    Ok(())
}

impl Cell for Files {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::CellRpc { method, args } = invocation else {
                anyhow::bail!("maintenance entered application code")
            };
            let storage = context.storage()?;
            let fs = context.fs()?;
            if let Some(old) = self.escaped.lock().unwrap().take() {
                ensure!(
                    old.fstat().is_err(),
                    "descriptor crossed invocation authority"
                );
            }
            let RpcData::Json(args) = args else {
                anyhow::bail!("JSON required")
            };
            let bytes: Vec<u8> = serde_json::from_str(&args)?;
            match method.as_str() {
                "Write" => {
                    self.writes.fetch_add(1, Ordering::AcqRel);
                    storage.tx(|tx| {
                        ensure!(
                            fs.stat("/value").is_err(),
                            "outer capability entered a savepoint"
                        );
                        tx.fs()?.write_file("/value", &bytes)?;
                        tx.put_json("value", &json!(bytes))?;
                        Ok(())
                    })?;
                    let rolled: Result<()> = storage.tx(|tx| {
                        tx.fs()?.write_file("/value", b"uncommitted")?;
                        tx.put_json("value", &json!(false))?;
                        anyhow::bail!("roll back file and business state")
                    });
                    ensure!(rolled.is_err() && snapshot(&storage)? == bytes);
                    let directory = fs.opendir("/")?;
                    let file = fs.openat(&directory, "value", OpenOptions::read_only())?;
                    let mut actual = vec![0; bytes.len()];
                    ensure!(file.read(&mut actual)? == bytes.len() && actual == bytes);
                    let duplicate = file.dup()?;
                    ensure!(duplicate.read(&mut [0; 1])? == 0, "dup lost shared offset");
                    duplicate.close()?;
                    directory.closedir()?;
                    file.fsync().await?;
                    *self.escaped.lock().unwrap() = Some(file);
                }
                "Orphan" => {
                    fs.write_file("/garbage", &vec![7; 3 * 64 * 1024])?;
                    let held = fs.open("/garbage", OpenOptions::read_only())?;
                    fs.unlink("/garbage")?;
                    fs.collect_garbage()?;
                    ensure!(held.fstat()?.nlink == 0, "live inode was reclaimed");
                    held.close()?;
                }
                "Read" => (),
                _ => anyhow::bail!("unknown filesystem operation"),
            }
            Ok(Output::Rpc(RpcData::Json(
                json!(snapshot(&storage)?).to_string(),
            )))
        })
    }
}

fn application(writes: Arc<AtomicUsize>) -> Result<Arc<Registry>> {
    let mut class = RaftClass::new("Counter", 1, [151; 32])?
        .filesystem(Policy::default())?
        .initializer([152; 32], |_, transaction| {
            let storage = transaction.storage()?;
            storage.fs()?.write_file("/value", b"initial")?;
            storage.fs()?.mkdir("/dir", 0o755)?;
            storage.put_json("value", &json!(b"initial"))?;
            Ok(())
        })?
        .query("Snapshot", [153; 32], |query, _| {
            let storage = query.storage();
            ensure!(
                storage
                    .fs()?
                    .write_file("/value", b"refused")
                    .unwrap_err()
                    .errno
                    == Errno::EROFS
            );
            snapshot(&storage)
        })?
        .query("Usage", [154; 32], |query, _| {
            let usage = query.storage().fs()?.statvfs()?;
            Ok(serde_json::to_vec(&json!([
                usage.inodes,
                usage.allocated_bytes
            ]))?)
        })?
        .query("Cursor", [169; 32], |query, _| {
            let cursor = query
                .storage()
                .fs()?
                .read_dir("/", 1, None)?
                .next
                .context("directory continuation")?;
            Ok(serde_json::to_vec(&cursor)?)
        })?
        .query("UseCursor", [170; 32], |query, input| {
            let cursor = serde_json::from_slice(input)?;
            Ok(vec![u8::from(
                query
                    .storage()
                    .fs()?
                    .read_dir("/", 1, Some(&cursor))
                    .is_ok(),
            )])
        })?;
    for (method, digest) in [("Write", 155), ("Read", 156), ("Orphan", 157)] {
        class = class.json_cell_rpc(method, [digest; 32])?;
    }
    Ok(Arc::new(
        Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_cell(
            class,
            move |_, _| {
                Ok(Arc::new(Files {
                    escaped: Mutex::new(None),
                    writes: writes.clone(),
                }))
            },
        )?,
    ))
}

pub async fn qualify(real_rustfs: bool) -> Result<()> {
    for rooted in [false, true] {
        let store = Arc::new(if real_rustfs {
            catalog_fault::Store::around(rustfs::store("filesystem", rooted)?)
        } else {
            catalog_fault::Store::default()
        });
        let writes = Arc::new(AtomicUsize::new(0));
        let application = application(writes.clone())?;
        let declared = configuration::options("quorum-filesystem");
        application.validate_configuration(&declared)?;
        let mut fixture = Fixture::with_application(rooted, "Counter", application, store).await?;
        fixture.configuration = Some(declared);
        fixture.sql_workers = 1;
        fixture.effects = Some(RaftEffectOptions {
            callback_timeout_ms: 5000,
            ..Default::default()
        });
        let domain = fixture.domain.clone();
        let mut cluster = Box::pin(ManagedCluster::start_with_applications(
            fixture,
            &[],
            ArchiveSetup::Initialized,
            false,
            None,
            FirstGroupSetup::Normal,
        ))
        .await?;
        let work = domain
            .run(async {
                let group = cluster.history;
                let leader = managed_fixture::leader(&cluster.hosts, group).await?;
                let retry = u64::try_from(cellar::asyncrt::wall_ms())? + 60_000;
                let mut histories = Vec::new();
                for index in 0..2u8 {
                    let history = CellHistory {
                        cluster: group.cluster,
                        namespace: group.namespace,
                        cell: CellId::new(format!("Counter:files-{index}"))?,
                        incarnation: IncarnationId::new([158 + index; 16])?,
                    };
                    let cell = cluster.hosts[leader].cell(history.clone())?;
                    cell.enroll(
                        CellEnrollmentRequest {
                            creation: ProposalId::new([160 + index; 16])?,
                            durability: Durability::Quorum,
                        },
                        TIMEOUT,
                    )
                    .await?;
                    let bytes = vec![index + 1; 17];
                    let options = CommandOptions {
                        durability: if index == 0 {
                            Durability::Quorum
                        } else {
                            Durability::S3
                        },
                        ..CommandOptions::new(
                            CommandId::new(ClientId::new([162 + index; 16])?, 1)?,
                            retry,
                        )
                    };
                    let input = serde_json::to_vec(&bytes)?;
                    let contract = cell.contract("Write")?;
                    let receipt = cell
                        .command(&contract, input.clone(), options.clone())
                        .await
                        .with_context(|| {
                            format!("initial filesystem write extents={rooted} cell={index}")
                        })?;
                    ensure!(serde_json::from_slice::<Vec<u8>>(&receipt.result)? == bytes);
                    ensure!(
                        receipt.witness.group == group && receipt.durability == options.durability
                    );
                    let duplicate = cell
                        .command(&contract, input.clone(), options.clone())
                        .await?;
                    retained_receipt(&receipt, &duplicate)?;
                    ensure!(
                        writes.load(Ordering::Acquire) == usize::from(index) + 1,
                        "receipt lookup re-executed the write handler"
                    );
                    cell.command(
                        &cell.contract("Orphan")?,
                        b"[]".to_vec(),
                        CommandOptions::new(
                            CommandId::new(ClientId::new([164 + index; 16])?, 1)?,
                            retry,
                        ),
                    )
                    .await?;
                    histories.push((history, input, options, receipt));
                }
                cellar::asyncrt::timeout(Duration::from_secs(20), async {
                    loop {
                        let mut complete = true;
                        for (history, _, _, _) in &histories {
                            let cell = cluster.hosts[leader].cell(history.clone())?;
                            let value = cell
                                .query(
                                    ProposalId::new([166; 16])?,
                                    &cell.contract("Usage")?,
                                    vec![],
                                    QueryOptions::default(),
                                )
                                .await?;
                            let usage: Vec<u64> = serde_json::from_slice(&value.result)?;
                            complete &= usage[0] == 3 && usage[1] == 64 * 1024;
                        }
                        if complete {
                            return Ok::<_, anyhow::Error>(());
                        }
                        cellar::asyncrt::sleep(Duration::from_millis(100)).await;
                    }
                })
                .await
                .context("autonomous filesystem GC deadline")??;
                let first = cluster.hosts[leader].cell(histories[0].0.clone())?;
                let cursor = first
                    .query(
                        ProposalId::new([171; 16])?,
                        &first.contract("Cursor")?,
                        vec![],
                        QueryOptions::default(),
                    )
                    .await?
                    .result;
                for (index, (history, _, _, _)) in histories.iter().enumerate() {
                    let cell = cluster.hosts[leader].cell(history.clone())?;
                    let accepted = cell
                        .query(
                            ProposalId::new([172; 16])?,
                            &cell.contract("UseCursor")?,
                            cursor.clone(),
                            QueryOptions::default(),
                        )
                        .await?;
                    ensure!(
                        accepted.result == [u8::from(index == 0)],
                        "directory cursor crossed cell history"
                    );
                }
                bindings::restart(&mut cluster).await?;
                let resumed_leader = managed_fixture::leader(&cluster.hosts, group).await?;
                let refusal_history = histories[0].0.clone();
                for (history, input, options, receipt) in histories {
                    let cell = cluster.hosts[resumed_leader].cell(history.clone())?;
                    let replay = cell
                        .command(&cell.contract("Write")?, input, options)
                        .await
                        .context("filesystem receipt replay after retained restart")?;
                    retained_receipt(&receipt, &replay)?;
                    ensure!(
                        writes.load(Ordering::Acquire) == 2,
                        "retained restart re-executed the write handler"
                    );
                    let observed = cell
                        .query(
                            ProposalId::new([167; 16])?,
                            &cell.contract("Snapshot")?,
                            vec![],
                            QueryOptions {
                                consistency: QueryConsistency::AtLeast(
                                    replay.version_token.clone(),
                                ),
                                ..Default::default()
                            },
                        )
                        .await?;
                    ensure!(observed.result == serde_json::from_slice::<Vec<u8>>(&receipt.result)?);
                }
                let cell = cluster.hosts[0].cell(refusal_history)?;
                let stopped = cluster.nodes.split_off(1);
                for node in stopped {
                    node.shutdown(cellar::sdk::ShutdownMode::Preserve).await?;
                }
                let refused = cell
                    .command(
                        &cell.contract("Write")?,
                        b"[9]".to_vec(),
                        CommandOptions::new(CommandId::new(ClientId::new([168; 16])?, 1)?, retry),
                    )
                    .await;
                ensure!(refused.is_err(), "filesystem write bypassed quorum");
                Ok::<_, anyhow::Error>(())
            })
            .await
            .map_err(anyhow::Error::from)
            .and_then(std::convert::identity);
        let mut cleanup = Vec::new();
        for node in std::mem::take(&mut cluster.nodes) {
            if let Err(error) = node.shutdown(cellar::sdk::ShutdownMode::Preserve).await {
                cleanup.push(error.to_string());
            }
        }
        for host in &cluster.hosts {
            if let Err(error) = host.close().await {
                cleanup.push(error.to_string());
            }
        }
        if work.is_err() || !cleanup.is_empty() {
            for root in std::mem::take(&mut cluster.roots) {
                eprintln!("retained filesystem disk: {}", root.keep().display());
            }
        }
        domain.shutdown().await;
        work?;
        ensure!(cleanup.is_empty(), "filesystem cleanup: {cleanup:?}");
    }
    Ok(())
}
