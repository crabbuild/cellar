// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Opt-in dense payload qualification through public SDK turns and bucket proof.
use super::{inputs, ready, response, Executor};
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{filesystem::*, *};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

const BLOCK: usize = 1 << 20;
const PATCH: usize = 4096;

fn payload(index: usize) -> Vec<u8> {
    let mut bytes = vec![0; BLOCK];
    for (word, part) in bytes.as_chunks_mut::<8>().0.iter_mut().enumerate() {
        let mut value = (index as u64 + 1).wrapping_mul(0x9e3779b97f4a7c15) ^ word as u64;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
        part.copy_from_slice(&(value ^ (value >> 31)).to_le_bytes());
    }
    bytes
}

fn digest(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn patch_offset(mib: usize, sequence: usize) -> usize {
    match sequence {
        0 => 65536 - 3,
        1 => mib * BLOCK / 2 - 7,
        2 => mib * BLOCK - PATCH,
        _ => unreachable!("fixture validates patch sequence"),
    }
}

fn expected(mib: usize, index: usize, patched: bool) -> String {
    let mut bytes = payload(index);
    if patched {
        let start = index * BLOCK;
        for sequence in 0..3 {
            let patch = patch_offset(mib, sequence);
            let first = start.max(patch);
            let last = (start + BLOCK).min(patch + PATCH);
            if first < last {
                bytes[first - start..last - start].fill(0xd0 + sequence as u8);
            }
        }
    }
    digest(&bytes)
}

struct Dense;

impl Cell for Dense {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = invocation else {
                return Ok(Output::Done);
            };
            let url = url::Url::parse(&request.url)?;
            let parts: Vec<_> = url.path().split('/').filter(|s| !s.is_empty()).collect();
            ensure!(
                parts.len() == 3 || parts.len() == 4,
                "invalid dense fixture route"
            );
            let mib: usize = parts[1].parse()?;
            ensure!(
                [10, 100, 1024].contains(&mib),
                "unsupported dense fixture size"
            );
            let index = parts
                .get(3)
                .map(|n| n.parse::<usize>())
                .transpose()?
                .unwrap_or(0);
            let storage = context.storage()?;
            let value = match parts[2] {
                "write" => {
                    ensure!(
                        request.method == "POST" && index <= mib,
                        "invalid block write"
                    );
                    let data = payload(index);
                    let hash = digest(&data);
                    storage.tx(|tx| {
                        let key = format!("block:{index}");
                        if let Some(receipt) = tx.get_json(&key)? {
                            ensure!(receipt["sha256"] == hash, "block identity conflict");
                            return Ok(receipt);
                        }
                        let next = tx.get_json("progress")?.and_then(|v| v.as_u64()).unwrap_or(0);
                        ensure!(next == index as u64, "out-of-order dense block");
                        let fs = tx.fs()?;
                        let file = fs.open("/dense.bin", OpenOptions::read_write().create(true))?;
                        ensure!(file.fstat()?.size == (index * BLOCK) as u64, "unexpected dense prefix");
                        ensure!(file.pwrite(&data, (index * BLOCK) as u64)? == BLOCK, "short block write");
                        let stat = file.fstat()?;
                        file.close()?;
                        let receipt = json!({"block":index,"sha256":hash,"inode":stat.inode,"version":stat.version});
                        tx.put_json(&key, &receipt)?;
                        tx.put_json("progress", &json!(index + 1))?;
                        Ok(receipt)
                    })?
                }
                "patch" => {
                    ensure!(request.method == "POST" && index < 3, "invalid patch");
                    storage.tx(|tx| {
                        let key = format!("patch:{index}");
                        if let Some(receipt) = tx.get_json(&key)? {
                            return Ok(receipt);
                        }
                        let fs = tx.fs()?;
                        let before = fs.stat("/dense.bin")?;
                        ensure!(
                            before.size == (mib * BLOCK) as u64,
                            "patch before dense completion"
                        );
                        fs.edit_file(
                            "/dense.bin",
                            FileVersion::from(&before),
                            &Edit {
                                offset: patch_offset(mib, index) as u64,
                                delete_bytes: PATCH as u64,
                                insert: vec![0xd0 + index as u8; PATCH],
                            },
                        )?;
                        let stat = fs.stat("/dense.bin")?;
                        let receipt =
                            json!({"patch":index,"inode":stat.inode,"version":stat.version});
                        tx.put_json(&key, &receipt)?;
                        Ok(receipt)
                    })?
                }
                "read" => {
                    ensure!(
                        request.method == "GET" && index <= mib,
                        "invalid block read"
                    );
                    let fs = context.fs()?;
                    let file = fs.open("/dense.bin", OpenOptions::read_only())?;
                    let mut bytes = vec![0; BLOCK];
                    ensure!(
                        file.pread(&mut bytes, (index * BLOCK) as u64)? == BLOCK,
                        "short dense block"
                    );
                    let stat = file.fstat()?;
                    file.close()?;
                    json!({"sha256":digest(&bytes),"inode":stat.inode,"version":stat.version})
                }
                "trim" => {
                    ensure!(request.method == "POST", "invalid trim");
                    storage.tx(|tx| {
                        tx.fs()?.truncate("/dense.bin", (mib * BLOCK) as u64)?;
                        tx.put_json("trim", &json!({"size":mib * BLOCK}))?;
                        Ok(json!({"trimmed":true}))
                    })?
                }
                "stat" => {
                    let fs = context.fs()?;
                    let stat = fs.stat("/dense.bin")?;
                    let usage = fs.statvfs()?;
                    ensure!(
                        fs.read_file("/dense.bin").unwrap_err().errno == Errno::EFBIG,
                        "whole-file helper admitted dense data"
                    );
                    json!({"size":stat.size,"allocated":stat.allocated_bytes,"inode":stat.inode,"version":stat.version,
                        "total_allocated":usage.allocated_bytes,"progress":storage.get_json("progress")?,
                        "last_receipt":storage.get_json(&format!("block:{}",mib-1))?,
                        "patches":[storage.get_json("patch:0")?,storage.get_json("patch:1")?,storage.get_json("patch:2")?]})
                }
                _ => anyhow::bail!("unknown dense fixture operation"),
            };
            Ok(response(value))
        })
    }
}

fn applications() -> Result<CompiledApplications> {
    CompiledApplications::default().register("embedded-node", "test", || {
        Ok(Arc::new(
            Registry::new(|context, invocation| {
                Box::pin(async move {
                    let Invocation::Fetch(request) = invocation else {
                        return Ok(Output::Done);
                    };
                    let url = url::Url::parse(&request.url)?;
                    let mib = url
                        .path()
                        .split('/')
                        .nth(2)
                        .context("missing dense fixture size")?;
                    let cell = context
                        .namespace("ACCOUNTS")?
                        .get_by_name(&format!("dense-{mib}"));
                    Ok(Output::Http(cell.fetch(&context, request).await?))
                })
            })
            .cell("Account", |_, _| Ok(Arc::new(Dense)))?
            .filesystem("Account", Policy::default())?,
        ))
    })
}

async fn start(
    name: &str,
    path: &Path,
    core: &Executor,
    bucket: cellar::bucket::Bucket,
) -> Result<Node> {
    let (mut config, services) = inputs(name, path, core, Some(bucket), 0).await?;
    config.startup.bootstrap_timeout = Duration::from_secs(180);
    config.startup.recovery_attempt_timeout = Duration::from_secs(120);
    config.startup.authority_timeout = Duration::from_secs(30);
    config.runtime.handler_budget = Duration::from_secs(120);
    config.shutdown = ShutdownPolicy {
        total_ms: 60_000,
        token_wait_ms: 0,
        no_progress_ms: 30_000,
    };
    config.cell_limits.filesystem.max_file_bytes = 2 << 30;
    config.cell_limits.filesystem.max_allocated_bytes = 2 << 30;
    let node = Node::start(config, services, super::graph(), applications()?).await?;
    ready(&node).await?;
    Ok(node)
}

async fn stop(node: Node) -> Result<()> {
    let result = tokio::time::timeout(
        Duration::from_secs(90),
        node.shutdown(ShutdownMode::Handoff),
    )
    .await??;
    let NodeOutcome::Stopped(report) = result else {
        anyhow::bail!("dense node failed: {result:?}")
    };
    ensure!(
        report.ingress_drained
            && report.application_work_drained
            && report.cells_drained
            && report.local_durability_closed
            && report.log_quiesced
            && !report.drain_timed_out
            && !report.deadline_elapsed
            && report.pending_dispatches == 0
            && report.pending_application_events == 0,
        "incomplete dense shutdown: {report:?}"
    );
    Ok(())
}

async fn call(
    http: &reqwest::Client,
    node: &Node,
    mib: usize,
    command: &str,
    write: bool,
    recovering: bool,
) -> Result<Value> {
    let until = Instant::now() + Duration::from_secs(180);
    loop {
        let url = format!(
            "http://{}/dense/{mib}/{command}",
            node.info().public_address
        );
        let reply = if write { http.post(url) } else { http.get(url) }
            .send()
            .await?;
        let status = reply.status();
        let bytes = reply.bytes().await?;
        if recovering
            && !write
            && status == reqwest::StatusCode::SERVICE_UNAVAILABLE
            && Instant::now() < until
        {
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        }
        ensure!(
            status == 200,
            "dense {command}: {status}: {}",
            String::from_utf8_lossy(&bytes)
        );
        return Ok(serde_json::from_slice(&bytes)?);
    }
}

fn percentiles(samples: &[u64]) -> Value {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let p = |n: usize| sorted[((sorted.len() * n).div_ceil(100) - 1).min(sorted.len() - 1)];
    json!({"count":sorted.len(),"p50_us":p(50),"p95_us":p(95),"p99_us":p(99),"max_us":sorted[sorted.len()-1]})
}

fn disk(path: &Path) -> Result<(u64, u64, u64)> {
    let mut total = (0, 0, 0);
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let metadata = match entry.metadata() {
            Ok(value) => value,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        let sizes = if metadata.is_dir() {
            disk(&entry.path())?
        } else {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            (
                metadata.len(),
                if name.ends_with("-wal") {
                    metadata.len()
                } else {
                    0
                },
                if name.ends_with(".ltx") {
                    metadata.len()
                } else {
                    0
                },
            )
        };
        total.0 += sizes.0;
        total.1 += sizes.1;
        total.2 += sizes.2;
    }
    Ok(total)
}

async fn profile(mib: usize) -> Result<Value> {
    let directory = tempfile::tempdir()?;
    let management = ExecutionDomain::new(tokio::runtime::Handle::current());
    let bucket = management
        .run(async { cellar::bucket::Bucket::open_dev(&directory.path().join("bucket.sqlite")) })
        .await??;
    let core = Executor::new("filesystem-dense-core")?;
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(180))
        .build()?;
    let old = directory.path().join("old");
    let owner = start("dense-owner", &old, &core, bucket.clone()).await?;
    let written = async {
        let mut writes = Vec::new();
        let mut reads = Vec::new();
        let mut disk_peak = (0, 0, 0);
        let started = Instant::now();
        let mut last = Value::Null;
        for index in 0..mib {
            let at = Instant::now();
            last = call(&http, &owner, mib, &format!("write/{index}"), true, false).await?;
            writes.push(at.elapsed().as_micros() as u64);
            ensure!(
                last["sha256"] == expected(mib, index, false),
                "write receipt content mismatch"
            );
            if index % 16 == 0 || index + 1 == mib {
                let sample = disk(&old)?;
                disk_peak = (
                    disk_peak.0.max(sample.0),
                    disk_peak.1.max(sample.1),
                    disk_peak.2.max(sample.2),
                );
            }
        }
        let write_elapsed_ms = started.elapsed().as_millis();
        let before = call(&http, &owner, mib, "stat", false, false).await?;
        ensure!(
            before["size"] == mib * BLOCK
                && before["allocated"] == mib * BLOCK
                && before["progress"] == mib
        );
        ensure!(
            call(
                &http,
                &owner,
                mib,
                &format!("write/{}", mib - 1),
                true,
                false
            )
            .await?
                == last,
            "write receipt replay changed"
        );
        ensure!(
            call(&http, &owner, mib, "stat", false, false).await? == before,
            "write replay mutated filesystem"
        );
        let mut patches = Vec::new();
        for sequence in 0..3 {
            let at = Instant::now();
            call(
                &http,
                &owner,
                mib,
                &format!("patch/{sequence}"),
                true,
                false,
            )
            .await?;
            patches.push(at.elapsed().as_micros() as u64);
        }
        for index in 0..mib {
            let at = Instant::now();
            let value = call(&http, &owner, mib, &format!("read/{index}"), false, false).await?;
            reads.push(at.elapsed().as_micros() as u64);
            ensure!(
                value["sha256"] == expected(mib, index, true),
                "warm block {index} differs"
            );
        }
        let state = call(&http, &owner, mib, "stat", false, false).await?;
        ensure!(state["allocated"] == mib * BLOCK && state["last_receipt"] == last);
        Ok::<_, anyhow::Error>((state, writes, reads, patches, write_elapsed_ms, disk_peak))
    }
    .await;
    let closed = stop(owner).await;
    let (state, writes, reads, patches, write_elapsed_ms, disk_peak) = match written {
        Ok(value) => value,
        Err(error) => {
            management.shutdown().await;
            return Err(retain_failed_fixture(
                directory,
                error.context(format!("warm dense phase; shutdown: {closed:?}")),
            ));
        }
    };
    closed?;
    std::fs::remove_dir_all(&old)?;
    let recovery_at = Instant::now();
    let fresh = directory.path().join("fresh");
    let successor = start("dense-successor", &fresh, &core, bucket.clone()).await?;
    let verified = async {
        let mut cold_reads = Vec::new();
        let mut first_read_ms = 0;
        let mut commitment = Sha256::new();
        for index in 0..mib {
            let at = Instant::now();
            let value = call(&http,&successor,mib,&format!("read/{index}"),false,true).await?;
            cold_reads.push(at.elapsed().as_micros() as u64);
            if index == 0 { first_read_ms = recovery_at.elapsed().as_millis(); }
            let hash = expected(mib,index,true);
            ensure!(value["sha256"] == hash,"cold block {index} differs");
            commitment.update(hex::decode(hash)?);
        }
        ensure!(call(&http,&successor,mib,"stat",false,false).await? == state,"cold file/state/receipt differs");
        let at = Instant::now();
        let appended = call(&http,&successor,mib,&format!("write/{mib}"),true,false).await?;
        let first_proven_write_us = at.elapsed().as_micros();
        ensure!(appended["sha256"] == expected(mib,mib,false));
        ensure!(call(&http,&successor,mib,&format!("read/{mib}"),false,false).await?["sha256"] == appended["sha256"]);
        call(&http,&successor,mib,"trim",true,false).await?;
        tokio::time::timeout(Duration::from_secs(15),async {
            loop {
                let value = call(&http,&successor,mib,"stat",false,false).await?;
                ensure!(value["size"] == mib*BLOCK);
                if value["total_allocated"] == mib*BLOCK { break Ok::<_,anyhow::Error>(()) }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }).await??;
        Ok::<_,anyhow::Error>(json!({"first_admitted_read_ms":first_read_ms,"cold_reads":percentiles(&cold_reads),
            "first_proven_write_us":first_proven_write_us,"block_digest_commitment":hex::encode(commitment.finalize()),
            "recovered_file_and_business_state":state,"fresh_node_file_bytes":disk(&fresh)?.0}))
    }.await;
    let closed = stop(successor).await;
    let recovered = match verified {
        Ok(value) => value,
        Err(error) => {
            management.shutdown().await;
            return Err(retain_failed_fixture(
                directory,
                error.context(format!("cold dense phase; shutdown: {closed:?}")),
            ));
        }
    };
    closed?;
    let objects = management.run(bucket.list("")).await??;
    let object_bytes: u64 = objects.iter().map(|o| o.size).sum();
    let ltx_bytes: u64 = objects
        .iter()
        .filter(|o| o.location.to_string().ends_with(".ltx"))
        .map(|o| o.size)
        .sum();
    management.shutdown().await;
    Ok(
        json!({"mib":mib,"application_bytes_written":mib*BLOCK+BLOCK+3*PATCH,"write_elapsed_ms":write_elapsed_ms,
        "writes":percentiles(&writes),"warm_reads":percentiles(&reads),"patches":percentiles(&patches),
        "node_file_bytes_peak_sampled":disk_peak.0,"wal_bytes_peak_sampled":disk_peak.1,"local_ltx_bytes_peak_sampled":disk_peak.2,
        "bucket_object_bytes_current":object_bytes,"bucket_ltx_object_bytes_current":ltx_bytes,"recovery":recovered}),
    )
}

fn retain_failed_fixture(directory: tempfile::TempDir, error: anyhow::Error) -> anyhow::Error {
    if std::env::var("CELLAR_FS_CAPACITY_KEEP_FAILED").as_deref() == Ok("1") {
        let path = directory.keep();
        error.context(format!(
            "retained private dense fixture at {}",
            path.display()
        ))
    } else {
        error
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "dense capacity qualification; uses private development buckets and real payloads"]
async fn filesystem_dense_capacity_cold_recovery() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(std::env::var("RUST_LOG").unwrap_or_else(|_| "warn".into()))
        .with_test_writer()
        .try_init();
    let sizes = std::env::var("CELLAR_FS_CAPACITY_MIB")
        .unwrap_or_else(|_| "10,100,1024".into())
        .split(',')
        .map(str::parse::<usize>)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    ensure!(
        !sizes.is_empty() && sizes.len() <= 3 && sizes.iter().all(|n| [10, 100, 1024].contains(n)),
        "select 10,100,1024 MiB profiles"
    );
    let mut executable = std::fs::File::open(std::env::current_exe()?)?;
    let mut executable_hash = Sha256::new();
    let mut buffer = [0; 65536];
    loop {
        let count = std::io::Read::read(&mut executable, &mut buffer)?;
        if count == 0 {
            break;
        }
        executable_hash.update(&buffer[..count]);
    }
    let executable_sha256 = hex::encode(executable_hash.finalize());
    let sources = json!({
        "fixture":digest(include_str!("node_filesystem_capacity.rs").as_bytes()),
        "sdk":digest(include_str!("../../application/filesystem/mod.rs").as_bytes()),
        "backend":digest(include_str!("../../storage/sqlite/filesystem/mod.rs").as_bytes()),
        "content":digest(include_str!("../../storage/sqlite/filesystem/content.rs").as_bytes()),
        "namespace":digest(include_str!("../../storage/sqlite/filesystem/namespace.rs").as_bytes()),
        "gc":digest(include_str!("../../storage/sqlite/filesystem/gc.rs").as_bytes()),
        "schema":digest(include_str!("../../storage/sqlite/filesystem/schema.sql").as_bytes()),
        "replica_storage":digest(include_str!("../../durability/replication/storage.rs").as_bytes()),
    });
    let mut results = Vec::new();
    for mib in sizes {
        let before = cellar::memory::resident_bytes();
        let peak = Arc::new(std::sync::atomic::AtomicU64::new(before));
        let sampled = peak.clone();
        let (stop, done) = tokio::sync::oneshot::channel::<()>();
        let sampler = tokio::spawn(async move {
            tokio::pin!(done);
            loop {
                sampled.fetch_max(
                    cellar::memory::resident_bytes(),
                    std::sync::atomic::Ordering::Relaxed,
                );
                cellar::asyncrt::select! { _ = &mut done => break, _ = tokio::time::sleep(Duration::from_millis(10)) => {} }
            }
        });
        let outcome = profile(mib).await;
        let _ = stop.send(());
        sampler.await?;
        let mut result = outcome?;
        result["process_rss_before"] = json!((before > 0).then_some(before));
        let sampled = peak.load(std::sync::atomic::Ordering::Relaxed);
        result["process_rss_peak_sampled"] = json!((sampled > 0).then_some(sampled));
        let after = cellar::memory::resident_bytes();
        result["process_rss_after"] = json!((after > 0).then_some(after));
        cellar::cli_output::Output::new(cellar::cli_output::Format::Text)
            .line(format_args!("DENSE_FILESYSTEM {result}"))?;
        results.push(result);
        if let Ok(path) = std::env::var("CELLAR_FS_CAPACITY_REPORT") {
            std::fs::write(
                path,
                serde_json::to_vec_pretty(&json!({"format":1,"profiles":results,
                "executable_sha256":executable_sha256,"source_sha256":sources,"debug_assertions":cfg!(debug_assertions),
                "limits":"Native SDK, single-node development-bucket proof, fixed 64 KiB chunks, 1 MiB requests, 2 GiB file/allocation ceilings, 120 s recovery/handler budgets. Latency includes routing, hashing and proof; RSS and file lengths are sampled, not hard peak bounds. No external-store, peer-fault, guest, chunk-size comparison or many-cell qualification."}))?,
            )?;
        }
    }
    Ok(())
}
