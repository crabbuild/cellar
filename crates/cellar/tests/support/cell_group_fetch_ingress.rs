// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Stateless public HTTP shares the native admitted command identity driver.

use super::fetch_bindings::{identity, FetchCell, BODY, SCRIPT};
use super::*;
use cellar::engine_api::RequestBody;
use cellar::sdk::{http::Response, Request};

async fn request(cluster: &ManagedCluster, node: usize) -> Result<u64> {
    let response = reqwest::Client::builder()
        .no_proxy()
        .timeout(TIMEOUT)
        .build()?
        .patch(format!(
            "http://{}/fetch",
            cluster.nodes[node].info().public_address
        ))
        .body(BODY.to_vec())
        .send()
        .await?;
    let status = response.status();
    if status.as_u16() != 201 {
        anyhow::bail!("public Fetch {status}: {}", response.text().await?);
    }
    let value = response
        .headers()
        .get("x-value")
        .context("HTTP result header")?
        .to_str()?
        .parse()?;
    let cookies = response
        .headers()
        .get_all("set-cookie")
        .iter()
        .map(|value| value.to_str().map(str::to_owned))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    ensure!(cookies == ["a=1", "b=2"]);
    ensure!(response.bytes().await?.as_ref() == BODY);
    Ok(value)
}

async fn run(
    rooted: bool,
    minimum: Durability,
    external: bool,
    store: Arc<catalog_fault::Store>,
) -> Result<()> {
    let callbacks = Arc::new(AtomicUsize::new(0));
    let class = RaftClass::new("FetchCell", 1, [201; 32])?
        .initializer([202; 32], |_, tx| {
            tx.storage()?.put_json("value", &json!(0))
        })?
        .cell_fetch([204; 32])?
        .query("Get", [205; 32], |q, _| {
            Ok(serde_json::to_vec(&q.storage().get_json("value")?)?)
        })?;
    let observed = callbacks.clone();
    let application = Arc::new(
        Registry::new(|context, invocation| {
            Box::pin(async move {
                ensure!(context.identity().is_none());
                let Invocation::Fetch(incoming) = invocation else {
                    anyhow::bail!("public HTTP required")
                };
                let mut reader = incoming.into_body_reader(&context)?;
                let body = reader.bytes(&context).await?;
                ensure!(body == BODY);
                let response = context
                    .namespace("CELLS")?
                    .get_by_name("target")
                    .fetch(
                        &context,
                        Request {
                            method: "patch".into(),
                            url: "https://cellar.test/counter?q=1".into(),
                            headers: vec![
                                ("x-request".into(), "first".into()),
                                ("x-request".into(), "second".into()),
                            ],
                            body: RequestBody::Bytes(body.into()),
                            entrypoint: None,
                        },
                    )
                    .await?;
                Ok(Output::Http(
                    Response::from_http(response).into_http(&context)?,
                ))
            })
        })
        .raft_cell(class, move |context, _| {
            Ok(Arc::new(FetchCell(context.storage()?, observed.clone())))
        })?,
    );
    let mut declared = configuration::options(SCRIPT);
    declared.do_classes = vec!["FetchCell".into()];
    declared.bindings = vec![("CELLS".into(), "FetchCell".into())];
    application.validate_deployment(&configuration::deployment(declared.clone()))?;
    let mut fixture = Fixture::with_definition_policies(
        rooted,
        vec![(application.resolve_raft_class("FetchCell")?, minimum)],
        application,
        store,
    )
    .await?;
    fixture.configuration = Some(declared);
    fixture.sql_workers = 1;
    let http = external.then(|| Arc::new(managed_fixture::http::Owner::default()));
    fixture.external_http = http.clone();
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
            if let Some(http) = http {
                http.close().await?;
            }
            return Err(error);
        }
    };
    let domain = cluster.fixtures[0].domain.clone();
    let work = domain
        .run(async {
            ensure!(request(&cluster, 0).await.context("cold stateless Fetch")? == 2);
            ensure!(
                request(&cluster, 1)
                    .await
                    .context("identical request must be a new mutation")?
                    == 4
            );
            let (first, second) = tokio::join!(request(&cluster, 0), request(&cluster, 2));
            let mut values = [
                first.context("first concurrent ingress")?,
                second.context("second concurrent ingress")?,
            ];
            values.sort();
            ensure!(
                values == [6, 8],
                "identical concurrent requests lost an operation: {values:?}"
            );
            ensure!(callbacks.load(Ordering::Acquire) == 4);
            Box::pin(bindings::restart(&mut cluster)).await?;
            ensure!(
                request(&cluster, 2)
                    .await
                    .context("retained stateless Fetch")?
                    == 10
            );
            ensure!(callbacks.load(Ordering::Acquire) == 5);
            let group = cluster.history;
            let leader = managed_fixture::leader(&cluster.hosts, group).await?;
            let history = cluster.hosts[leader]
                .resolve_cell_history(
                    group.cluster,
                    group.namespace,
                    identity("target")?,
                    QueryOptions::default(),
                )
                .await?
                .context("retained HTTP target")?;
            let target = cluster.hosts[leader].cell(history)?;
            let result = target
                .query(
                    ProposalId::new([209; 16])?,
                    &target.contract("Get")?,
                    vec![],
                    QueryOptions {
                        consistency: QueryConsistency::Linearizable,
                        ..Default::default()
                    },
                )
                .await?;
            ensure!(result.result == b"10");
            Ok::<_, anyhow::Error>(())
        })
        .await
        .map_err(anyhow::Error::from)
        .and_then(std::convert::identity);
    let joined =
        managed_fixture::close_group_nodes(std::mem::take(&mut cluster.nodes), &cluster.hosts)
            .await;
    let closed = match http {
        Some(http) => http.close().await,
        None => Ok(()),
    };
    if work.is_err() || joined.is_err() || closed.is_err() {
        for root in std::mem::take(&mut cluster.roots) {
            eprintln!("retained stateless Fetch disk: {}", root.keep().display());
        }
    }
    domain.shutdown().await;
    work?;
    joined?;
    closed
}

pub(super) async fn qualify(real_rustfs: bool, external: bool) -> Result<()> {
    let mut failures = Vec::new();
    for rooted in [false, true] {
        for minimum in [Durability::Quorum, Durability::S3] {
            let result = async {
                let store = if real_rustfs {
                    Arc::new(catalog_fault::Store::around(rustfs::store(
                        match (external, minimum) {
                            (false, Durability::Quorum) => "stateless-fetch-native-quorum",
                            (false, _) => "stateless-fetch-native-s3",
                            (true, Durability::Quorum) => "stateless-fetch-axum-quorum",
                            (true, _) => "stateless-fetch-axum-s3",
                        },
                        rooted,
                    )?))
                } else {
                    Arc::new(catalog_fault::Store::default())
                };
                Box::pin(run(rooted, minimum, external, store)).await
            }
            .await;
            match result {
                Ok(()) => eprintln!("stateless Fetch RustFS={real_rustfs} Axum={external} extents={rooted} target={minimum:?}: PASS"),
                Err(error) => {
                    eprintln!("stateless Fetch RustFS={real_rustfs} Axum={external} extents={rooted} target={minimum:?}: FAILED: {error:?}");
                    failures.push(error);
                }
            }
        }
    }
    let count = failures.len();
    match failures.into_iter().next() {
        None => Ok(()),
        Some(error) => Err(error.context(format!(
            "{count} of four public stateless Fetch configurations failed"
        ))),
    }
}
