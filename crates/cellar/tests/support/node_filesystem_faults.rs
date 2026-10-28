// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real replica refusal, owner fencing and failed-recovery admission.
use super::node_events::{ReplicaPublicationFault, StoreOutage};
use super::{call, client, graph, inputs, ready, response, stopped, Executor};
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{filesystem::*, *};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::Notify;

const NAME: &str = "filesystem-fault";

fn scope() -> String {
    let id = cellar_native::cell_id::for_name("cells:v1:13:embedded-node:Account", NAME);
    format!("Account:{}", cellar_native::cell_id::encode_hex(&id))
}

fn payload(generation: u8) -> Vec<u8> {
    let mut bytes = vec![generation; 3 * 64 * 1024 + 17];
    bytes[64 * 1024 - 1] = generation + 8;
    bytes[128 * 1024] = generation + 16;
    bytes
}

#[derive(Default)]
struct Signals {
    constructors: AtomicUsize,
    local_commit: Notify,
    local_receipt: Mutex<Option<Value>>,
    old_file: Mutex<Option<File>>,
    old_context: Mutex<Option<Context>>,
    stale_checks: AtomicUsize,
}

struct Release(Arc<Signals>);

impl Drop for Release {
    fn drop(&mut self) {
        self.0.old_file.lock().unwrap().take();
        self.0.old_context.lock().unwrap().take();
    }
}

fn write(context: &Context, generation: u8) -> Result<Value> {
    let data = payload(generation);
    let hash = hex::encode(Sha256::digest(&data));
    context.storage()?.tx(|tx| {
        let fs = tx.fs()?;
        fs.write_file("/ledger.bin", &data)?;
        let metadata = fs.stat("/ledger.bin")?;
        let receipt = json!({"generation":generation,"sha256":hash,"inode":metadata.inode.to_string(),"version":metadata.version.to_string()});
        tx.put_json("generation", &json!(generation))?;
        tx.put_json(&format!("receipt:{generation}"), &receipt)?;
        Ok(receipt)
    })
}

struct Workspace(Arc<Signals>);

impl Cell for Workspace {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = invocation else {
                return Ok(Output::Done);
            };
            if request.url.ends_with("/seed") || request.url.ends_with("/takeover-write") {
                let generation = if request.url.ends_with("/seed") { 1 } else { 3 };
                let receipt = write(&context, generation)?;
                context.fs()?.sync().await?;
                return Ok(response(receipt));
            }
            if request.url.ends_with("/unproven") {
                let receipt = write(&context, 2)?;
                let file = context
                    .fs()?
                    .open("/ledger.bin", OpenOptions::read_write())?;
                *self.0.old_file.lock().unwrap() = Some(file);
                *self.0.old_context.lock().unwrap() = Some(context.clone());
                *self.0.local_receipt.lock().unwrap() = Some(receipt.clone());
                self.0.local_commit.notify_one();
                context.fs()?.sync().await?;
                return Ok(response(receipt));
            }
            let storage = context.storage()?;
            let fs = context.fs()?;
            let generation = storage
                .get_json("generation")?
                .context("missing business state")?
                .as_u64()
                .context("invalid generation")? as u8;
            let data = fs.read_file("/ledger.bin")?;
            let receipt = storage
                .get_json(&format!("receipt:{generation}"))?
                .context("missing file receipt")?;
            ensure!(data == payload(generation));
            ensure!(receipt["sha256"] == hex::encode(Sha256::digest(&data)));
            let metadata = fs.stat("/ledger.bin")?;
            ensure!(receipt["version"] == metadata.version.to_string());
            ensure!(receipt["inode"] == metadata.inode.to_string());
            ensure!(
                storage.get_json("receipt:2")?.is_none(),
                "unpublished receipt escaped recovery"
            );
            if let Some(old) = self.0.old_file.lock().unwrap().take() {
                for error in [
                    old.fstat().unwrap_err(),
                    old.pwrite(b"stale", 0).unwrap_err(),
                ] {
                    ensure!(error.errno == Errno::EBADF && error.outcome == Outcome::NotApplied);
                }
                old.close()?;
                self.0.stale_checks.fetch_add(1, Ordering::SeqCst);
            }
            if let Some(old) = self.0.old_context.lock().unwrap().take() {
                let error = old
                    .fs()
                    .err()
                    .context("fenced context acquired a filesystem")?;
                ensure!(
                    error.outcome == Outcome::NotApplied && error.operation == "filesystem.acquire"
                );
            }
            ensure!(
                fs.read_file("/ledger.bin")? == data,
                "stale operation changed recovered bytes"
            );
            fs.sync().await?;
            Ok(response(receipt))
        })
    }
}

fn applications(signals: Arc<Signals>) -> Result<CompiledApplications> {
    CompiledApplications::default().register("embedded-node", "test", move || {
        let factory = signals.clone();
        Ok(Arc::new(
            Registry::new(|context, invocation| {
                Box::pin(async move {
                    let Invocation::Fetch(request) = invocation else {
                        return Ok(Output::Done);
                    };
                    let cell = context.namespace("ACCOUNTS")?.get_by_name(NAME);
                    Ok(Output::Http(cell.fetch(&context, request).await?))
                })
            })
            .cell("Account", move |_, _| {
                factory.constructors.fetch_add(1, Ordering::SeqCst);
                Ok(Arc::new(Workspace(factory.clone())))
            })?
            .filesystem("Account", Policy::default())?,
        ))
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn filesystem_unproven_write_is_fenced_and_failed_recovery_refuses_admission() -> Result<()> {
    scenario(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires a private CELLAR_FS_S3_BUCKET and CELLAR_FS_S3_ENDPOINT with AWS credentials"]
async fn filesystem_s3_unproven_write_is_fenced_and_failed_recovery_refuses_admission() -> Result<()>
{
    scenario(true).await
}

async fn scenario(s3: bool) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let management = ExecutionDomain::new(tokio::runtime::Handle::current());
    let bucket = management
        .run(async {
            if s3 {
                let bucket = std::env::var("CELLAR_FS_S3_BUCKET")?;
                let endpoint = std::env::var("CELLAR_FS_S3_ENDPOINT")?;
                ensure!(
                    bucket.starts_with("cellar-filesystem-"),
                    "use a private filesystem qualification bucket"
                );
                cellar::bucket::Bucket::open(
                    &bucket,
                    Some(&endpoint),
                    "us-east-1",
                    Some(cellar::bucket::StaticCredentials {
                        access_key_id: std::env::var("AWS_ACCESS_KEY_ID")?,
                        secret_access_key: std::env::var("AWS_SECRET_ACCESS_KEY")?,
                        session_token: None,
                    }),
                    Some("filesystem-qualification"),
                )
            } else {
                cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite"))
            }
        })
        .await??;
    let signals = Arc::new(Signals::default());
    let _release = Release(signals.clone());
    let core = Executor::new("filesystem-outage-owner")?;
    let (isolated, outage) = StoreOutage::wrap(&bucket)?;
    let (config, mut services) = inputs(
        "filesystem-outage-owner",
        &directory.path().join("old"),
        &core,
        Some(isolated),
        0,
    )
    .await?;
    let publication = ReplicaPublicationFault::with_prefixes(
        bucket.store.clone(),
        format!("{}cells/{}/ltx/", bucket.prefix, scope()),
        Vec::new(),
    );
    let fleet = services.fleet.as_mut().context("fleet missing")?;
    fleet.fleet_durability = false;
    fleet.replica_storage = ReplicaStorage::new(
        publication.clone(),
        ReplicaStorageOptions {
            prefix: bucket.prefix.clone(),
            ..Default::default()
        },
    )?;
    let owner = Node::start(config, services, graph(), applications(signals.clone())?).await?;
    ready(&owner).await?;
    let http = client()?;
    let seed: Value =
        serde_json::from_str(&call(&http, &owner, reqwest::Method::POST, "/seed").await?)?;
    ensure!(seed["generation"] == 1);
    publication.block_and_drain().await;
    let address = owner.info().public_address;
    let old_services = owner.services();
    let request = async {
        let reply = http
            .post(format!("http://{address}/unproven"))
            .send()
            .await?;
        let status = reply.status();
        Ok::<_, anyhow::Error>((status, reply.text().await?))
    };
    let control = async {
        let progress = tokio::time::timeout(Duration::from_secs(5), async {
            signals.local_commit.notified().await;
            while publication.refused() == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        if let Err(error) = progress {
            owner.shutdown(ShutdownMode::Handoff).await?;
            return Err(error.into());
        }
        outage.isolate();
        tokio::time::timeout(
            Duration::from_secs(10),
            owner.run_until(std::future::pending()),
        )
        .await?
    };
    let (reply, outcome) = futures_util::future::join(request, control).await;
    ensure!(
        outcome?
            == NodeOutcome::Fenced {
                code: 3,
                reason: FenceReason::NodeLeaseExpired
            }
    );
    let ambiguous = match reply {
        Ok((status, body)) => {
            ensure!(
                !status.is_success(),
                "unproven file write was acknowledged: {status} {body}"
            );
            json!({"http_status":status.as_u16(),"outcome":"unknown"})
        }
        Err(_) => json!({"transport_error":true,"outcome":"unknown"}),
    };
    ensure!(publication.refused() > 0 && outage.refused() > 0);
    {
        let local = signals.local_receipt.lock().unwrap();
        let local = local.as_ref().unwrap();
        ensure!(local["generation"] == 2 && local["inode"] == seed["inode"]);
        ensure!(local["sha256"] == hex::encode(Sha256::digest(payload(2))));
        ensure!(local["version"] != seed["version"]);
    }
    ensure!(signals.constructors.load(Ordering::SeqCst) == 1);
    ensure!(old_services.application_work().pending_events == 0);
    ensure!(old_services
        .configure_metrics(Arc::new(HostMetricsSample::default))
        .is_err());
    ensure!(core.handle().spawn(async { 7 }).await? == 7);
    std::fs::remove_dir_all(directory.path().join("old"))?;

    // The node lease remains healthy, but the supplied replica reader cannot
    // restore this cell. No application constructor or empty filesystem may
    // be admitted as a substitute for the acknowledged checkpoint.
    let bad_core = Executor::new("filesystem-outage-bad-recovery")?;
    let (unreadable, read_fault) = StoreOutage::wrap(&bucket)?;
    read_fault.isolate();
    let (mut config, mut services) = inputs(
        "filesystem-bad-recovery",
        &directory.path().join("bad"),
        &bad_core,
        Some(bucket.clone()),
        0,
    )
    .await?;
    config.startup.recovery_retry_backoff = Duration::from_secs(10);
    services.fleet.as_mut().unwrap().replica_storage = ReplicaStorage::new(
        unreadable.store.clone(),
        ReplicaStorageOptions {
            prefix: bucket.prefix.clone(),
            ..Default::default()
        },
    )?;
    let bad = Node::start(config, services, graph(), applications(signals.clone())?).await?;
    ready(&bad).await?;
    let refused = http
        .get(format!("http://{}/recovered", bad.info().public_address))
        .send()
        .await;
    let stopped_bad = stopped(bad).await;
    let refused = refused?;
    let status = refused.status();
    let body = refused.text().await?;
    // This application propagates CellRef::fetch failure from its root
    // handler, whose existing public response contract is a worker 500.
    ensure!(
        status == reqwest::StatusCode::INTERNAL_SERVER_ERROR,
        "recovery refusal {status}: {body}"
    );
    ensure!(read_fault.refused() > 0 && signals.constructors.load(Ordering::SeqCst) == 1);
    stopped_bad?;

    let new_core = Executor::new("filesystem-outage-successor")?;
    let new_path = directory.path().join("successor");
    let (config, services) = inputs(
        "filesystem-outage-successor",
        &new_path,
        &new_core,
        Some(bucket.clone()),
        0,
    )
    .await?;
    let successor = Node::start(config, services, graph(), applications(signals.clone())?).await?;
    ready(&successor).await?;
    let recovered: Value =
        serde_json::from_str(&call(&http, &successor, reqwest::Method::GET, "/recovered").await?)?;
    ensure!(recovered == seed && signals.stale_checks.load(Ordering::SeqCst) == 1);
    let proven: Value = serde_json::from_str(
        &call(&http, &successor, reqwest::Method::POST, "/takeover-write").await?,
    )?;
    ensure!(
        proven["generation"] == 3
            && proven["version"] != seed["version"]
            && proven["inode"] == seed["inode"]
    );
    stopped(successor).await?;
    std::fs::remove_dir_all(&new_path)?;
    let final_core = Executor::new("filesystem-outage-verifier")?;
    let (config, services) = inputs(
        "filesystem-outage-verifier",
        &directory.path().join("final"),
        &final_core,
        Some(bucket),
        0,
    )
    .await?;
    let verifier = Node::start(config, services, graph(), applications(signals.clone())?).await?;
    ready(&verifier).await?;
    let final_receipt = call(&http, &verifier, reqwest::Method::GET, "/recovered").await;
    let final_stop = stopped(verifier).await;
    management.shutdown().await;
    final_stop?;
    let final_receipt: Value = serde_json::from_str(&final_receipt?)?;
    ensure!(final_receipt == proven);
    ensure!(signals.constructors.load(Ordering::SeqCst) == 3);
    if let Some(path) = std::env::var_os("CELLAR_FS_FAULT_REPORT") {
        let report = json!({
            "format":1,"scope":scope(),"mode":"bucket proof; supplied-store fault injection",
            "backend":if s3 {"s3"} else {"development"},
            "acknowledged":[seed,proven],"ambiguous_response":ambiguous,
            "unacknowledged_local_commit":signals.local_receipt.lock().unwrap().as_ref().unwrap().clone(),
            "recovered_after_outage":recovered,"recovered_after_takeover":final_receipt,
            "publication_refusals":publication.refused(),"lease_store_refusals":outage.refused(),
            "recovery_read_refusals":read_fault.refused(),"failed_recovery_http_status":status.as_u16(),
            "application_constructors":signals.constructors.load(Ordering::SeqCst),
            "stale_descriptor_checks":signals.stale_checks.load(Ordering::SeqCst),
            "predecessor_local_data_removed":true,"successor_local_data_removed":true,
            "owner_fenced":true,"caller_executor_preserved":true,"remaining_nodes_joined":true,
        });
        std::fs::write(
            std::path::PathBuf::from(path),
            serde_json::to_vec_pretty(&report)?,
        )?;
    }
    Ok(())
}
