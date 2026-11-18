// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Ordinary buffered Fetch retains child work and its HTTP response across replay.

use super::*;
use cellar::engine_api::RequestBody;
use cellar::sdk::{http::Response, Request};
use futures_util::StreamExt;

const SCRIPT: &str = "cell-fetch-service";
const BODY: &[u8] = &[0, 0xff, 0x80, 42];

struct FetchCell(Storage, Arc<AtomicUsize>);

impl Cell for FetchCell {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            match invocation {
                Invocation::Fetch(request) => {
                    ensure!(
                        request.method == "PATCH"
                            && request.url == "https://cellar.test/counter?q=1"
                    );
                    ensure!(
                        request.headers
                            == vec![
                                ("x-request".into(), "first".into()),
                                ("x-request".into(), "second".into())
                            ]
                    );
                    let mut reader = request.into_body_reader(&context)?;
                    let body = reader.bytes(&context).await?;
                    ensure!(body == BODY);
                    let mut stream = context.take_body(RequestBody::Bytes(body.into()))?;
                    ensure!(
                        stream
                            .next()
                            .await
                            .context("owned chunk")?
                            .map_err(anyhow::Error::msg)?
                            == BODY
                    );
                    ensure!(stream.next().await.is_none());
                    let refused = context
                        .take_body(RequestBody::Stream(1))
                        .err()
                        .context("host stream identity requires its session owner")?;
                    ensure!(refused.is::<CommandFailure>());
                    ensure!(
                        context
                            .identity()
                            .context("target identity")?
                            .name
                            .as_deref()
                            == Some("target")
                    );
                    let refused =
                        Response::streaming(200, Box::pin(futures_util::stream::empty()), vec![])
                            .into_http(&context)
                            .err()
                            .context("streaming needs its own session owner")?;
                    ensure!(refused.is::<CommandFailure>());
                    let value = self
                        .0
                        .get_json("value")?
                        .context("target value")?
                        .as_u64()
                        .context("integer")?
                        + 2;
                    self.0.put_json("value", &json!(value))?;
                    self.1.fetch_add(1, Ordering::AcqRel);
                    Ok(Output::Http(
                        Response::new(
                            201,
                            BODY,
                            vec![
                                ("x-value".into(), value.to_string()),
                                ("set-cookie".into(), "a=1".into()),
                                ("set-cookie".into(), "b=2".into()),
                            ],
                        )
                        .into_http(&context)?,
                    ))
                }
                Invocation::CellRpc { method, .. } => {
                    ensure!(method == "Relay");
                    self.0.put_json("before", &json!(true))?;
                    let response = context
                        .namespace("CELLS")?
                        .get_by_name("target")
                        .fetch(
                            &context,
                            Request {
                                url: "https://cellar.test/counter?q=1".into(),
                                method: "patch".into(),
                                headers: vec![
                                    ("x-request".into(), "first".into()),
                                    ("x-request".into(), "second".into()),
                                ],
                                body: RequestBody::Bytes(bytes::Bytes::from_static(BODY)),
                                entrypoint: None,
                            },
                        )
                        .await?;
                    ensure!(response.status == 201 && response.body == BODY);
                    ensure!(response.stream.is_none() && response.websocket.is_none());
                    ensure!(
                        response.write_position.is_none() && response.observed_position.is_none()
                    );
                    ensure!(response.headers.len() == 3);
                    ensure!(response.headers[0].0 == "x-value");
                    ensure!(
                        response.headers[1..]
                            == [
                                ("set-cookie".into(), "a=1".into()),
                                ("set-cookie".into(), "b=2".into())
                            ]
                    );
                    let value: u64 = response.headers[0].1.parse()?;
                    let mut owned = Response::from_http(response);
                    ensure!(owned.bytes(&context).await? == BODY);
                    ensure!(self.0.get_json("before")? == Some(json!(true)));
                    // The original writer must resume after the independently
                    // proven child; these writes share the final parent proof.
                    self.0.put_json("value", &json!(value))?;
                    Ok(Output::Rpc(RpcData::Json(value.to_string())))
                }
                _ => anyhow::bail!("unexpected invocation"),
            }
        })
    }
}

fn identity(name: &str) -> Result<CellId> {
    let namespace = cellar_native::cell_id::Namespace::new(
        "FetchCell",
        format!("cells:v1:{}:{SCRIPT}:FetchCell", SCRIPT.len()),
    );
    Ok(CellId::new(namespace.id_from_name(name).scope())?)
}

async fn run(rooted: bool, minimum: Durability, store: Arc<catalog_fault::Store>) -> Result<()> {
    let callbacks = Arc::new(AtomicUsize::new(0));
    let class = RaftClass::new("FetchCell", 1, [201; 32])?
        .initializer([202; 32], |_, tx| {
            tx.storage()?.put_json("value", &json!(0))
        })?
        .json_cell_rpc("Relay", [203; 32])?
        .cell_fetch([204; 32])?
        .query("Get", [205; 32], |q, _| {
            Ok(serde_json::to_vec(&q.storage().get_json("value")?)?)
        })?;
    let observed = callbacks.clone();
    let application = Arc::new(
        Registry::new(|_, _| Box::pin(async { Ok(Output::Done) }))
            .raft_cell(class, move |context, _| {
                Ok(Arc::new(FetchCell(context.storage()?, observed.clone())))
            })?,
    );
    let mut declared = configuration::options(SCRIPT);
    declared.do_classes = vec!["FetchCell".into()];
    declared.bindings = vec![("CELLS".into(), "FetchCell".into())];
    application.validate_deployment(&configuration::deployment(declared.clone()))?;
    let mut fixture = Fixture::with_application(rooted, "FetchCell", application, store).await?;
    fixture.configuration = Some(declared);
    fixture.sql_workers = 1;
    let mut cluster = Box::pin(ManagedCluster::start_with_applications(
        fixture,
        &[],
        ArchiveSetup::Initialized,
        false,
        None,
        FirstGroupSetup::Normal,
    ))
    .await?;
    let domain = cluster.fixtures[0].domain.clone();
    let work = domain
        .run(async {
            let group = cluster.history;
            let leader = managed_fixture::leader(&cluster.hosts, group).await?;
            let source = cluster.hosts[leader].cell(CellHistory {
                cluster: group.cluster,
                namespace: group.namespace,
                cell: identity("source")?,
                incarnation: IncarnationId::new([206; 16])?,
            })?;
            source
                .enroll(
                    CellEnrollmentRequest {
                        creation: ProposalId::new([207; 16])?,
                        durability: minimum,
                    },
                    TIMEOUT,
                )
                .await?;
            let contract = source.contract("Relay")?;
            let options = bindings::options(1, minimum)?;
            ensure!(options.timeout == TIMEOUT);
            let first = source
                .command(&contract, b"{}".to_vec(), options.clone())
                .await?;
            ensure!(first.result == b"2" && first.durability >= minimum);
            let replay = source
                .command(&contract, b"{}".to_vec(), options.clone())
                .await?;
            ensure!(replay.result == first.result && callbacks.load(Ordering::Acquire) == 1);
            let history = source.history().clone();
            drop(source);
            Box::pin(bindings::restart(&mut cluster)).await?;
            let leader = managed_fixture::leader(&cluster.hosts, group).await?;
            let source = cluster.hosts[leader].cell(history)?;
            let replay = source.command(&contract, b"{}".to_vec(), options).await?;
            ensure!(replay.result == first.result && callbacks.load(Ordering::Acquire) == 1);
            let next = source
                .command(&contract, b"{}".to_vec(), bindings::options(2, minimum)?)
                .await?;
            ensure!(next.result == b"4" && next.durability >= minimum);
            ensure!(callbacks.load(Ordering::Acquire) == 2);
            let target = cluster.hosts[leader]
                .resolve_cell_history(
                    group.cluster,
                    group.namespace,
                    identity("target")?,
                    QueryOptions::default(),
                )
                .await?
                .context("auto-enrolled HTTP target")?;
            let target = cluster.hosts[leader].cell(target)?;
            let value = target
                .query(
                    ProposalId::new([208; 16])?,
                    &target.contract("Get")?,
                    vec![],
                    QueryOptions {
                        consistency: QueryConsistency::Linearizable,
                        ..Default::default()
                    },
                )
                .await?;
            ensure!(value.result == b"4");
            Ok::<_, anyhow::Error>(())
        })
        .await
        .map_err(anyhow::Error::from)
        .and_then(std::convert::identity);
    let joined =
        managed_fixture::close_group_nodes(std::mem::take(&mut cluster.nodes), &cluster.hosts)
            .await;
    if work.is_err() || joined.is_err() {
        for root in std::mem::take(&mut cluster.roots) {
            eprintln!("retained Fetch binding disk: {}", root.keep().display());
        }
    }
    cluster.fixtures[0].domain.shutdown().await;
    work?;
    joined
}

pub(super) async fn qualify(real_rustfs: bool) -> Result<()> {
    let mut failures = Vec::new();
    for rooted in [false, true] {
        for minimum in [Durability::Quorum, Durability::S3] {
            let store = if real_rustfs {
                Arc::new(catalog_fault::Store::around(rustfs::store(
                    if minimum == Durability::S3 {
                        "fetch-binding-s3"
                    } else {
                        "fetch-binding-quorum"
                    },
                    rooted,
                )?))
            } else {
                Arc::new(catalog_fault::Store::default())
            };
            match Box::pin(run(rooted, minimum, store)).await {
                Ok(()) => eprintln!("Fetch binding extents={rooted} source={minimum:?}: PASS"),
                Err(error) => {
                    eprintln!(
                        "Fetch binding extents={rooted} source={minimum:?}: FAILED: {error:?}"
                    );
                    failures.push(error.context(format!(
                        "Fetch binding RustFS={real_rustfs} extents={rooted} source={minimum:?}"
                    )));
                }
            }
        }
    }
    let count = failures.len();
    match failures.into_iter().next() {
        None => Ok(()),
        Some(error) => Err(error.context(format!(
            "{count} of four Fetch binding configurations failed"
        ))),
    }
}
