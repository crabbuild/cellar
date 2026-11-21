// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Conditional edits cannot borrow another logical cell's revision, including
//! when both valid histories deliberately share an incarnation and counter.
use super::*;

const OTHER: &str = "shared/other-cache";
const KEY: &str = "revision-scope";

fn edit(revision: serde_json::Value, value: &str) -> serde_json::Value {
    json!({"op":"edit","key":KEY,"condition":{"revision":revision},
        "value":value,"tag":"bytes","metadata":"null",
        "expiration_mode":{"mode":"never"}})
}

async fn get(cluster: &ManagedCluster, namespace: &str) -> Result<serde_json::Value> {
    request(cluster, namespace, json!({"op":"get","key":KEY})).await
}

async fn request(
    cluster: &ManagedCluster,
    namespace: &str,
    body: serde_json::Value,
) -> Result<serde_json::Value> {
    let op = body["op"].as_str().unwrap_or("invalid").to_owned();
    let started = cellar::asyncrt::mono_ms();
    let result = admin_at(cluster, namespace, body).await;
    eprintln!(
        "KV revision operation namespace={namespace} op={op} elapsed_ms={} success={}",
        cellar::asyncrt::mono_ms() - started,
        result.is_ok()
    );
    result.with_context(|| format!("KV revision {namespace} {op}"))
}

async fn foreign_refuses(
    cluster: &ManagedCluster,
    from: &serde_json::Value,
    to: &serde_json::Value,
) -> Result<()> {
    let outcome = request(cluster, OTHER, edit(from["revision"].clone(), "cG9pc29u")).await?;
    let observed = get(cluster, OTHER).await?;
    ensure!(
        outcome["conflict"] == true && observed == *to,
        "foreign KV revision satisfied another cell's condition: outcome={outcome}, before={to}, after={observed}"
    );
    Ok(())
}

pub(super) async fn qualify(real_rustfs: bool) -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("warn,cellar::raft::binding=debug,cellar::raft::checkpoint=debug,cellar::raft::sql_ingress=debug")
        .with_test_writer()
        .try_init();
    let mut failures = Vec::new();
    for rooted in [false, true] {
        for minimum in [Durability::Quorum, Durability::S3] {
            let store = if real_rustfs {
                Arc::new(catalog_fault::Store::around(rustfs::store(
                    &format!("kv-revision-scope-{minimum:?}"),
                    rooted,
                )?))
            } else {
                Arc::new(catalog_fault::Store::default())
            };
            match Box::pin(run(rooted, minimum, store)).await {
                Ok(()) => eprintln!(
                    "KV revision scope RustFS={real_rustfs} extents={rooted} minimum={minimum:?}: PASS"
                ),
                Err(error) => {
                    eprintln!("KV revision scope RustFS={real_rustfs} extents={rooted} minimum={minimum:?}: FAILED: {error:#}");
                    failures.push(format!("extents={rooted} minimum={minimum:?}: {error:#}"));
                }
            }
        }
    }
    ensure!(
        failures.is_empty(),
        "KV revision qualification: {}",
        failures.join("; ")
    );
    Ok(())
}

async fn run(rooted: bool, minimum: Durability, store: Arc<catalog_fault::Store>) -> Result<()> {
    let (application, declared) = application(Arc::new(AtomicUsize::new(0)))?;
    let mut fixture = Fixture::with_definition_policies(
        rooted,
        vec![
            (
                application.resolve_raft_class("Counter")?,
                Durability::Quorum,
            ),
            (
                application.resolve_raft_class(cellar::deploy::KV_CLASS)?,
                minimum,
            ),
        ],
        application,
        store,
    )
    .await?;
    fixture.configuration = Some(declared);
    fixture.sql_workers = 1;
    fixture.max_input_bytes = 16 * 1024;
    fixture.max_client_identities = 4;
    fixture.payloads = Some(CellGroupPayloadLimits {
        max_operations: 2,
        max_retained_bytes: 600 * 1024 * 1024,
        max_payload_bytes: 64 * 1024 * 1024 + 4096,
        io_timeout: Duration::from_secs(10),
    });
    let domain = fixture.domain.clone();
    let started = Box::pin(ManagedCluster::start_with_applications(
        fixture,
        &[],
        ArchiveSetup::Initialized,
        false,
        None,
        FirstGroupSetup::Normal,
    ))
    .await;
    let mut cluster = match started {
        Ok(cluster) => cluster,
        Err(error) => {
            domain.shutdown().await;
            return Err(error);
        }
    };
    let work = domain
        .run(async {
            let group = named_service::activate(&cluster, "kv-revision-archive")
                .await
                .context("activate KV revision group")?;
            let leader = managed_fixture::leader(&cluster.hosts, group).await?;
            let incarnation = IncarnationId::new([93; 16])?;
            for (namespace, tag) in [(NAMESPACE, 94), (OTHER, 95)] {
                let history = CellHistory {
                    cluster: group.cluster,
                    namespace: group.namespace,
                    cell: CellId::new(cellar::engine_api::kv_cell_scope(namespace, 0))?,
                    incarnation,
                };
                let enrolled = cluster.hosts[leader]
                    .cell(history)?
                    .enroll(
                        CellEnrollmentRequest {
                            creation: ProposalId::new([tag; 16])?,
                            durability: minimum,
                        },
                        TIMEOUT,
                    )
                    .await
                    .with_context(|| format!("enroll KV revision {namespace}"))?;
                ensure!(enrolled.durability() >= minimum);
            }
            ensure!(request(&cluster, NAMESPACE, edit(json!(null), "QQ==")).await?["ok"] == true);
            ensure!(request(&cluster, OTHER, edit(json!(null), "Qg==")).await?["ok"] == true);
            let a = get(&cluster, NAMESPACE).await?;
            let b = get(&cluster, OTHER).await?;
            ensure!(
                a["value"] != b["value"],
                "independent KV cells must retain different values"
            );
            for value in [&a, &b] {
                ensure!(
                    value["revision"]
                        .as_str()
                        .context("revision")?
                        .rsplit_once(':')
                        .context("qualified revision")?
                        .1
                        == "1"
                );
            }
            foreign_refuses(&cluster, &a, &b).await?;
            for (namespace, original) in [(NAMESPACE, &a), (OTHER, &b)] {
                ensure!(
                    request(
                        &cluster,
                        namespace,
                        edit(original["revision"].clone(), "b3du")
                    )
                    .await?["ok"]
                        == true
                );
                ensure!(
                    request(
                        &cluster,
                        namespace,
                        edit(original["revision"].clone(), "c3RhbGU=")
                    )
                    .await?["conflict"]
                        == true
                );
            }
            let a = get(&cluster, NAMESPACE).await?;
            let b = get(&cluster, OTHER).await?;
            let current = cluster.hosts[leader]
                .group(group)?
                .replication_status(TIMEOUT)
                .await?;
            let next = (leader + 1) % 3;
            cluster.hosts[leader]
                .group(group)?
                .transfer_leadership(
                    LeadershipTransfer {
                        target: MemberId::new(next as u64 + 1)?,
                        expected_term: current.term,
                        expected_revision: current.membership_revision,
                    },
                    TIMEOUT,
                )
                .await?;
            cellar::asyncrt::timeout(TIMEOUT, async {
                loop {
                    if managed_fixture::leader(&cluster.hosts, group).await? == next {
                        break;
                    }
                    cellar::asyncrt::sleep(Duration::from_millis(10)).await;
                }
                Ok::<_, anyhow::Error>(())
            })
            .await??;
            ensure!(
                get(&cluster, NAMESPACE).await? == a && get(&cluster, OTHER).await? == b,
                "leadership change invalidated logical KV revisions"
            );
            named_service::restart(&mut cluster)
                .await
                .context("KV revision retained restart")?;
            managed_fixture::leader(&cluster.hosts, group).await?;
            ensure!(
                get(&cluster, NAMESPACE).await? == a && get(&cluster, OTHER).await? == b,
                "retained restart changed logical KV revisions"
            );
            foreign_refuses(&cluster, &a, &b).await?;
            ensure!(
                request(&cluster, OTHER, edit(b["revision"].clone(), "cmVzdGFydGVk")).await?["ok"]
                    == true
            );
            Ok::<_, anyhow::Error>(())
        })
        .await
        .map_err(anyhow::Error::from)
        .and_then(std::convert::identity);
    let joined = managed_fixture::close_group_nodes_with_groups(
        std::mem::take(&mut cluster.nodes),
        &cluster.hosts,
        2,
    )
    .await;
    domain.shutdown().await;
    if work.is_err() || joined.is_err() {
        for root in std::mem::take(&mut cluster.roots) {
            eprintln!("retained KV revision disk: {}", root.keep().display());
        }
    }
    work.and(joined)
}
