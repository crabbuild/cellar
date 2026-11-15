// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Ordinary Blob provider contracts, independently of metadata quorum proofs.

use super::*;
use cellar::bucket::{Bucket, StorageBackend};
use cellar::sdk::blob::{GetOptions, GetResult, ObjectBody, WriteOptions};

struct Objects {
    signals: Arc<blob_contract::Signals>,
    calls: Arc<AtomicUsize>,
    body: Mutex<Option<ObjectBody>>,
}

impl Cell for Objects {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::CellRpc {
                method,
                args: RpcData::Json(path),
            } = invocation
            else {
                anyhow::bail!("Blob RPC required");
            };
            self.calls.fetch_add(1, Ordering::AcqRel);
            let storage = context.storage()?;
            let calls = storage
                .get_json("calls")?
                .and_then(|value| value.as_u64())
                .unwrap_or(0)
                + 1;
            storage.put_json("calls", &json!(calls))?;
            match method.as_str() {
                "Exercise" => {
                    let path: String = serde_json::from_str(&path)?;
                    let Output::Http(result) =
                        blob_contract::run_path(context, &path, self.signals.clone(), false)
                            .await?
                    else {
                        anyhow::bail!("Blob conformance response");
                    };
                    ensure!(
                        result.status == 200
                            && result.body == b"checked"
                            && result.stream.is_none()
                    );
                }
                "Read" => {
                    let GetResult::Body(mut body) = context
                        .blob("DATA")?
                        .get(&context, "record", GetOptions::default())
                        .await?
                    else {
                        anyhow::bail!("missing Blob after restart");
                    };
                    ensure!(body.bytes(&context).await? == b"alpha");
                }
                "HoldBody" => {
                    let GetResult::Body(body) = context
                        .blob("DATA")?
                        .get(&context, "record", GetOptions::default())
                        .await?
                    else {
                        anyhow::bail!("missing held Blob");
                    };
                    *self.body.lock().unwrap() = Some(body);
                }
                "ExpiredBody" => {
                    let mut body = self.body.lock().unwrap().take().context("held Blob body")?;
                    ensure!(
                        body.bytes(&context).await.is_err(),
                        "Blob body crossed its private invocation"
                    );
                }
                "Probe" => {
                    let bucket = context.blob("DATA")?;
                    bucket
                        .put(
                            &context,
                            "probe",
                            b"ready".to_vec(),
                            WriteOptions::default(),
                        )
                        .await?
                        .context("single Blob put")?;
                    ensure!(bucket.head(&context, "probe").await?.is_some());
                }
                "Marker" => {
                    let path: String = serde_json::from_str(&path)?;
                    let key = if path.is_empty() {
                        "quorum-outage"
                    } else {
                        &path
                    };
                    context
                        .blob("DATA")?
                        .put(
                            &context,
                            key,
                            b"provider bytes".to_vec(),
                            WriteOptions::default(),
                        )
                        .await?
                        .context("Blob marker publication")?;
                }
                _ => anyhow::bail!("unknown Blob method"),
            }
            Ok(Output::Rpc(RpcData::Json("\"checked\"".into())))
        })
    }
}

fn application(
    calls: Arc<AtomicUsize>,
    signals: Arc<blob_contract::Signals>,
) -> Result<Arc<Registry>> {
    let mut class = RaftClass::new("Counter", 1, [185; 32])?
        .initializer([184; 32], |_, transaction| {
            transaction.storage()?.put_json("calls", &json!(0))
        })?;
    for (index, method) in [
        "Exercise",
        "Read",
        "HoldBody",
        "ExpiredBody",
        "Marker",
        "Probe",
    ]
    .into_iter()
    .enumerate()
    {
        class = class.json_cell_rpc(method, [186 + index as u8; 32])?;
    }
    Ok(Arc::new(
        Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_cell(
            class,
            move |_, _| {
                Ok(Arc::new(Objects {
                    calls: calls.clone(),
                    signals: signals.clone(),
                    body: Mutex::new(None),
                }))
            },
        )?,
    ))
}

async fn call(
    cell: &RaftCellHandle,
    method: &str,
    path: &str,
    id: u8,
    durability: Durability,
    retry: u64,
) -> Result<(CommandReceipt, Vec<u8>, CommandOptions)> {
    let input = serde_json::to_vec(path)?;
    let options = CommandOptions {
        durability,
        // Shared conformance phases perform many separately proven provider calls.
        timeout: if method == "Exercise" {
            Duration::from_secs(20)
        } else {
            Duration::from_secs(5)
        },
        ..CommandOptions::new(CommandId::new(ClientId::new([id; 16])?, 1)?, retry)
    };
    let result = cell
        .command(&cell.contract(method)?, input.clone(), options.clone())
        .await?;
    ensure!(result.durability == durability && result.result == b"\"checked\"");
    Ok((result, input, options))
}

pub async fn qualify(real_rustfs: bool) -> Result<()> {
    for rooted in [false, true] {
        let provider_root = tempfile::tempdir()?;
        let mut provider_faults = None;
        let provider = if real_rustfs {
            let config = rustfs::StoreConfig::load("blob-provider", rooted)?;
            let raw = Arc::new(
                object_store::aws::AmazonS3Builder::new()
                    .with_bucket_name(&config.bucket)
                    .with_endpoint(&config.endpoint)
                    .with_access_key_id(&config.access)
                    .with_secret_access_key(&config.secret)
                    .with_region("us-east-1")
                    .with_allow_http(true)
                    .with_conditional_put(object_store::aws::S3ConditionalPut::ETagMatch)
                    .with_retry(object_store::RetryConfig {
                        max_retries: 0,
                        ..Default::default()
                    })
                    .build()?,
            );
            let listing = raw.clone();
            let raw = Arc::new(catalog_fault::Store::around(raw));
            provider_faults = Some(raw.clone());
            Bucket::from_clients(
                raw.clone(),
                raw.clone(),
                listing,
                StorageBackend::S3,
                config.bucket,
                format!("{}/provider/", config.prefix),
            )?
        } else {
            Bucket::open_dev(&provider_root.path().join("blob.sqlite"))?
        };
        let store = Arc::new(if real_rustfs {
            catalog_fault::Store::around(rustfs::store("blob-source", rooted)?)
        } else {
            catalog_fault::Store::default()
        });
        let calls = Arc::new(AtomicUsize::new(0));
        let app = application(calls.clone(), Arc::new(blob_contract::Signals::default()))?;
        let mut configured = configuration::options("quorum-blob");
        configured.blob_bindings = vec![
            ("DATA".into(), "native-objects".into()),
            ("OTHER".into(), "other-objects".into()),
        ];
        app.validate_configuration(&configured)?;
        let mut fixture = Fixture::with_application(rooted, "Counter", app, store.clone()).await?;
        fixture.configuration = Some(configured);
        fixture.blob_bucket = Some(provider.clone());
        fixture.sql_workers = 1;
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
                let leader = managed_fixture::leader(&cluster.hosts, cluster.history).await?;
                let history = CellHistory {
                    cluster: cluster.history.cluster,
                    namespace: cluster.history.namespace,
                    cell: CellId::new("Counter:blob-contract")?,
                    incarnation: IncarnationId::new([191; 16])?,
                };
                let cell = cluster.hosts[leader].cell(history.clone())?;
                cell.enroll(
                    CellEnrollmentRequest {
                        creation: ProposalId::new([192; 16])?,
                        durability: Durability::Quorum,
                    },
                    TIMEOUT,
                )
                .await?;
                let retry = u64::try_from(cellar::asyncrt::wall_ms())? + 180_000;
                call(&cell, "Probe", "", 208, Durability::Quorum, retry)
                    .await
                    .context("single Blob operation control")?;
                let mut original = None;
                for (index, path) in ["/basic", "/preflight", "/list", "/multipart", "/stream"]
                    .into_iter()
                    .enumerate()
                {
                    let result = call(
                        &cell,
                        "Exercise",
                        path,
                        193 + index as u8,
                        Durability::Quorum,
                        retry,
                    )
                    .await
                    .with_context(|| {
                        format!("Blob {path} RustFS={real_rustfs} extents={rooted}")
                    })?;
                    if index == 0 {
                        original = Some(result);
                    }
                }
                call(&cell, "HoldBody", "", 198, Durability::Quorum, retry).await?;
                call(&cell, "ExpiredBody", "", 199, Durability::Quorum, retry).await?;
                if !real_rustfs {
                    call(
                        &cell,
                        "Exercise",
                        "/stage-owned",
                        200,
                        Durability::Quorum,
                        retry,
                    )
                    .await?;
                    call(
                        &cell,
                        "Exercise",
                        "/stage-foreign",
                        201,
                        Durability::Quorum,
                        retry,
                    )
                    .await?;
                }
                if let Some(faults) = &provider_faults {
                    for (id, key, lost) in [
                        (206, "provider-denied", false),
                        (207, "provider-ambiguous", true),
                    ] {
                        if lost {
                            faults.fail_after.store(true, Ordering::Release);
                        } else {
                            faults.fail_before.store(true, Ordering::Release);
                        }
                        let input = serde_json::to_vec(key)?;
                        let options = CommandOptions::new(
                            CommandId::new(ClientId::new([id; 16])?, 1)?,
                            retry,
                        );
                        let contract = cell.contract("Marker")?;
                        let before = calls.load(Ordering::Acquire);
                        ensure!(
                            cell.command(&contract, input.clone(), options.clone())
                                .await
                                .is_err(),
                            "provider failure returned success"
                        );
                        ensure!(
                            !faults.fail_after.load(Ordering::Acquire)
                                && !faults.fail_before.load(Ordering::Acquire),
                            "provider fault was not entered"
                        );
                        ensure!(
                            provider
                                .head_blob(&format!("blob/native-objects/{key}"))
                                .await?
                                .is_some()
                                == lost,
                            "provider outcome did not match the injected fault"
                        );
                        ensure!(cell.command(&contract, input, options).await.is_err());
                        ensure!(
                            calls.load(Ordering::Acquire) == before + 1,
                            "unknown provider outcome reexecuted the native handler"
                        );
                    }
                }
                store
                    .deny_group_archive_writes
                    .store(true, Ordering::Release);
                call(&cell, "Marker", "", 202, Durability::Quorum, retry).await?;
                ensure!(provider
                    .head_blob("blob/native-objects/quorum-outage")
                    .await?
                    .is_some());
                let options = CommandOptions {
                    durability: Durability::S3,
                    ..CommandOptions::new(CommandId::new(ClientId::new([203; 16])?, 1)?, retry)
                };
                let denied = cell
                    .command(
                        &cell.contract("Exercise")?,
                        b"\"/rejected\"".to_vec(),
                        options,
                    )
                    .await;
                ensure!(denied.is_err(), "source S3 outage returned a Blob success");
                ensure!(store.denied_group_archive_writes.load(Ordering::Acquire) != 0);
                ensure!(
                    provider
                        .head_blob("blob/native-objects/rejected")
                        .await?
                        .is_none(),
                    "unproven source reached the Blob provider"
                );
                store
                    .deny_group_archive_writes
                    .store(false, Ordering::Release);
                call(&cell, "Read", "", 204, Durability::S3, retry).await?;
                let before = calls.load(Ordering::Acquire);
                bindings::restart(&mut cluster).await?;
                let leader = managed_fixture::leader(&cluster.hosts, cluster.history).await?;
                let resumed = cluster.hosts[leader].cell(history)?;
                let (original, input, options) = original.context("first Blob receipt")?;
                let replay = resumed
                    .command(&resumed.contract("Exercise")?, input, options)
                    .await?;
                ensure!(
                    replay.id == original.id
                        && replay.result == original.result
                        && calls.load(Ordering::Acquire) == before,
                    "Blob receipt replay reentered provider code"
                );
                call(&resumed, "Read", "", 205, Durability::S3, retry).await?;
                Ok::<_, anyhow::Error>(())
            })
            .await
            .map_err(anyhow::Error::from)
            .and_then(std::convert::identity);
        store
            .deny_group_archive_writes
            .store(false, Ordering::Release);
        let joined =
            managed_fixture::close_group_nodes(std::mem::take(&mut cluster.nodes), &cluster.hosts)
                .await;
        if work.is_err() || joined.is_err() {
            for root in std::mem::take(&mut cluster.roots) {
                eprintln!("retained Blob voter: {}", root.keep().display());
            }
            eprintln!("retained Blob provider: {}", provider_root.keep().display());
        }
        domain.shutdown().await;
        work?;
        joined?;
    }
    Ok(())
}
