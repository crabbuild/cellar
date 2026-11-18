// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A lost S3 birth response retains the original, independently recoverable ID.

use super::*;

struct ReleaseArchive(Arc<catalog_fault::Store>);

impl Drop for ReleaseArchive {
    fn drop(&mut self) {
        self.0.group_archive_writes.release();
    }
}

pub(super) fn reply(context: &Context, error: anyhow::Error) -> Result<Output> {
    let birth = error
        .downcast_ref::<cellar::sdk::CellBindingEnrollmentError>()
        .with_context(|| format!("missing typed birth identity: {error:#}"))?;
    ensure!(
        birth.ambiguous && birth.minimum == Durability::S3,
        "birth must be pending at S3: {birth:?}"
    );
    let metadata = json!({
        "kind":"enrollment",
        "creation":hex::encode(birth.creation.bytes()),
        "cluster":hex::encode(birth.target.cluster.bytes()),
        "namespace":hex::encode(birth.target.namespace.bytes()),
        "cell":birth.target.cell.as_str(),
        "incarnation":hex::encode(birth.target.incarnation.bytes()),
        "minimum":"S3", "ambiguous":birth.ambiguous,
        "cause":birth.cause.to_string(),
    });
    Ok(Output::Http(
        Response::new(
            503,
            serde_json::to_vec(&metadata)?,
            vec![("content-type".into(), "application/json".into())],
        )
        .into_http(context)?,
    ))
}

async fn pending(cluster: &ManagedCluster) -> Result<serde_json::Value> {
    let response = reqwest::Client::builder()
        .no_proxy()
        .timeout(TIMEOUT)
        .build()?
        .patch(format!(
            "http://{}/enrollment-timeout",
            cluster.nodes[0].info().public_address
        ))
        .body(BODY.to_vec())
        .send()
        .await?;
    let status = response.status();
    let bytes = response.bytes().await?;
    ensure!(
        status.as_u16() == 503,
        "birth timeout status={status}: {}",
        String::from_utf8_lossy(&bytes)
    );
    Ok(serde_json::from_slice(&bytes)?)
}

fn id(value: &serde_json::Value, field: &str) -> Result<[u8; 16]> {
    hex::decode(value[field].as_str().context("birth identity field")?)?
        .try_into()
        .map_err(|_| anyhow::anyhow!("invalid birth identity width: {field}"))
}

pub(super) async fn work(
    cluster: &mut ManagedCluster,
    store: Arc<catalog_fault::Store>,
    callbacks: &AtomicUsize,
    initializers: &AtomicUsize,
) -> Result<()> {
    let initialized = initializers.load(Ordering::Acquire);
    let entered = store.group_archive_writes.entered.notified();
    tokio::pin!(entered);
    entered.as_mut().enable();
    store.group_archive_writes.arm();
    let release = ReleaseArchive(store.clone());
    // The source's unchanged 3s SDK deadline must return while admitted archive
    // PUTs remain held. The independent HTTP client retains its original 5s.
    let (observed, result) = tokio::join!(
        cellar::asyncrt::timeout(TIMEOUT, &mut entered),
        pending(cluster),
    );
    let callback_count = callbacks.load(Ordering::Acquire);
    let initializer_count = initializers.load(Ordering::Acquire);
    drop(release);
    observed.context("archive write did not enter")?;
    let metadata = result?;
    ensure!(
        metadata["kind"] == "enrollment"
            && metadata["ambiguous"] == true
            && metadata["minimum"] == "S3",
        "unexpected birth metadata: {metadata}"
    );
    ensure!(
        metadata["cause"]
            .as_str()
            .context("birth cause")?
            .contains("DeadlineExceeded"),
        "birth deadline cause: {metadata}"
    );
    ensure!(
        callback_count == 0,
        "business input ran before birth's S3 proof"
    );
    ensure!(
        initializer_count == initialized + 1,
        "initializer count while archive writes remained held: before={initialized}, observed={initializer_count}"
    );
    let group = cluster.history;
    ensure!(
        metadata["cluster"] == hex::encode(group.cluster.bytes())
            && metadata["namespace"] == hex::encode(group.namespace.bytes())
            && metadata["cell"] == identity("target")?.as_str()
    );
    let history = CellHistory {
        cluster: group.cluster,
        namespace: group.namespace,
        cell: identity("target")?,
        incarnation: IncarnationId::new(id(&metadata, "incarnation")?)?,
    };
    let creation = ProposalId::new(id(&metadata, "creation")?)?;
    // Wait for original admitted creation calls to retire without more app
    // traffic. Snapshots supply no serving or archive proof to recovery.
    cellar::asyncrt::timeout(Duration::from_secs(60), async {
        while cluster
            .hosts
            .iter()
            .any(|host| host.resource_usage().host.controller.retained_commands != 0)
        {
            cellar::asyncrt::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("birth owner did not settle after archive release")?;
    let target = cluster.hosts[0].cell(history.clone())?;
    let recovered = target
        .enroll(
            CellEnrollmentRequest {
                creation,
                durability: Durability::S3,
            },
            TIMEOUT,
        )
        .await?;
    ensure!(
        recovered.record().creation() == creation
            && recovered.record().cell() == &history
            && recovered.durability() == Durability::S3
    );
    drop(recovered);
    let query = target.contract("Get")?;
    drop(target);
    ensure!(
        initializers.load(Ordering::Acquire) == initialized + 1,
        "exact birth recovery invoked initialization again"
    );
    ensure!(
        callbacks.load(Ordering::Acquire) == 0,
        "birth recovery replayed business input"
    );
    ensure!(request(cluster, 1).await? == 2);
    ensure!(callbacks.load(Ordering::Acquire) == 1);
    Box::pin(named_service::restart(cluster)).await?;
    ensure!(request(cluster, 2).await? == 4);
    ensure!(
        callbacks.load(Ordering::Acquire) == 2
            && initializers.load(Ordering::Acquire) == initialized + 1
    );
    let result = cluster.hosts[0]
        .cell(history)?
        .query(
            ProposalId::new([210; 16])?,
            &query,
            vec![],
            QueryOptions::default(),
        )
        .await?;
    ensure!(result.result == b"4");
    Ok(())
}

pub(crate) async fn qualify(real_rustfs: bool, external: bool) -> Result<()> {
    // DEBUG-cg17-init-entry-1400: correlate receiver refusal and initializer entry.
    let _ = tracing_subscriber::fmt()
        .with_env_filter("cellar::raft::sql_ingress=debug,cellar::raft::enrollment=debug")
        .with_test_writer()
        .try_init();
    let mut failures = Vec::new();
    for rooted in [false, true] {
        let result = async {
            let store = if real_rustfs {
                Arc::new(catalog_fault::Store::around(rustfs::store(
                    if external {
                        "birth-timeout-axum"
                    } else {
                        "birth-timeout-native"
                    },
                    rooted,
                )?))
            } else {
                Arc::new(catalog_fault::Store::default())
            };
            Box::pin(run(
                rooted,
                Durability::S3,
                external,
                store,
                Scenario::EnrollmentTimeout,
            ))
            .await
        }
        .await;
        match result {
            Ok(()) => eprintln!(
                "birth timeout RustFS={real_rustfs} Axum={external} extents={rooted}: PASS"
            ),
            Err(error) => {
                eprintln!("birth timeout RustFS={real_rustfs} Axum={external} extents={rooted}: FAILED: {error:#}");
                failures.push(error);
            }
        }
    }
    ensure!(
        failures.is_empty(),
        "{} birth timeout configurations failed",
        failures.len()
    );
    Ok(())
}
