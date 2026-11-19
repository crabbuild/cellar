// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Count real application future polls across a quorum-proven checkpoint.

use super::*;
use std::future::Future;

struct SyncCell {
    storage: Storage,
    invocations: Arc<AtomicUsize>,
}

impl Cell for SyncCell {
    fn invoke(self: Arc<Self>, _context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        self.invocations.fetch_add(1, Ordering::AcqRel);
        let polls = Arc::new(AtomicUsize::new(0));
        let observed = polls.clone();
        let mut future = Box::pin(async move {
            let Invocation::CellRpc {
                method,
                args: RpcData::Json(input),
            } = invocation
            else {
                anyhow::bail!("checkpoint poll RPC required")
            };
            ensure!(method == "Tick");
            let input: serde_json::Value = serde_json::from_str(&input)?;
            let value = input["value"].as_u64().context("checkpoint input value")?;
            self.storage.put_json("before", &json!(value))?;
            self.storage.sync().await?;
            self.storage.put_json("after", &json!(value))?;
            Ok(Output::Rpc(RpcData::Json(
                json!({"value":value,"polls":observed.load(Ordering::Acquire)}).to_string(),
            )))
        });
        Box::pin(futures_util::future::poll_fn(move |cx| {
            polls.fetch_add(1, Ordering::AcqRel);
            future.as_mut().poll(cx)
        }))
    }
}

pub async fn qualify() -> Result<()> {
    let mut failures = Vec::new();
    for rooted in [false, true] {
        for durability in [Durability::Quorum, Durability::S3] {
            if let Err(error) = run(rooted, durability).await {
                failures.push(format!(
                    "rooted={rooted} durability={durability:?}: {error:#}"
                ));
            }
        }
    }
    ensure!(
        failures.is_empty(),
        "checkpoint reply poll qualification: {failures:?}"
    );
    Ok(())
}

async fn run(rooted: bool, durability: Durability) -> Result<()> {
    let invocations = Arc::new(AtomicUsize::new(0));
    let counted = invocations.clone();
    let class = RaftClass::new("Counter", 1, [231; 32])?
        .initializer([232; 32], |_, tx| {
            tx.storage()?.put_json("before", &json!(0))?;
            tx.storage()?.put_json("after", &json!(0))
        })?
        .json_cell_rpc("Tick", [233; 32])?
        .query("Get", [234; 32], |query, _| {
            Ok(serde_json::to_vec(&json!([
                query.storage().get_json("before")?,
                query.storage().get_json("after")?
            ]))?)
        })?;
    let application = Arc::new(
        Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_cell(
            class,
            move |context, _| {
                Ok(Arc::new(SyncCell {
                    storage: context.storage()?,
                    invocations: counted.clone(),
                }))
            },
        )?,
    );
    let configuration = configuration::options("checkpoint-poll-service");
    application.validate_deployment(&configuration::deployment(configuration.clone()))?;
    let mut fixture = Fixture::with_application(
        rooted,
        "Counter",
        application,
        Arc::new(catalog_fault::Store::default()),
    )
    .await?;
    fixture.configuration = Some(configuration);
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
    let work = cluster.fixtures[0]
        .domain
        .run(async {
            let group = cluster.history;
            let leader = managed_fixture::leader(&cluster.hosts, group).await?;
            let cell = cluster.hosts[leader].cell(CellHistory {
                cluster: group.cluster,
                namespace: group.namespace,
                cell: CellId::new("Counter:checkpoint-reply-polls")?,
                incarnation: IncarnationId::new([235; 16])?,
            })?;
            cell.enroll(
                CellEnrollmentRequest {
                    creation: ProposalId::new([236; 16])?,
                    durability,
                },
                TIMEOUT,
            )
            .await?;
            let tick = cell.contract("Tick")?;
            let get = cell.contract("Get")?;
            let client = ClientId::new([237; 16])?;
            let mut observed = Vec::new();
            for sequence in 1..=3 {
                let id = CommandId::new(client, sequence)?;
                let options = CommandOptions {
                    durability,
                    ..CommandOptions::new(id, u64::try_from(cellar::asyncrt::wall_ms())? + 60_000)
                };
                let input = serde_json::to_vec(&json!({"value":sequence}))?;
                let receipt = cell.command(&tick, input.clone(), options.clone()).await?;
                ensure!(receipt.id == id && receipt.durability == durability);
                let value: serde_json::Value = serde_json::from_slice(&receipt.result)?;
                ensure!(value["value"].as_u64() == Some(sequence));
                observed.push(
                    value["polls"]
                        .as_u64()
                        .context("application future polls")?,
                );
                let output = cell
                    .query(
                        ProposalId::new([240 + sequence as u8; 16])?,
                        &get,
                        Vec::new(),
                        QueryOptions::default(),
                    )
                    .await?;
                ensure!(
                    serde_json::from_slice::<serde_json::Value>(&output.result)?
                        == json!([sequence, sequence])
                );
                ensure!(output.version == receipt.version && output.witness == receipt.witness);
                let replay = cell.command(&tick, input, options).await?;
                ensure!(
                    replay.id == id
                        && replay.durability == durability
                        && replay.result == receipt.result
                );
                ensure!(
                    invocations.load(Ordering::Acquire) == sequence as usize,
                    "receipt replay reinvoked the application"
                );
            }
            Ok::<_, anyhow::Error>(observed)
        })
        .await
        .map_err(anyhow::Error::from)
        .and_then(std::convert::identity);
    let joined =
        managed_fixture::close_group_nodes(std::mem::take(&mut cluster.nodes), &cluster.hosts)
            .await;
    let observed = work.as_ref().ok();
    let expected = vec![2; 3];
    let matches = observed.is_some_and(|polls| polls == &expected);
    eprintln!(
        "CHECKPOINT_REPLY_POLLS {}",
        json!({"extents":rooted,"durability":format!("{durability:?}"),"polls":observed,"invocations":invocations.load(Ordering::Acquire),"valuesAndReplayVerified":work.is_ok(),"joinedClose":joined.is_ok()})
    );
    if work.is_err() || joined.is_err() || !matches {
        for root in std::mem::take(&mut cluster.roots) {
            eprintln!("retained checkpoint poll disk: {}", root.keep().display());
        }
    }
    cluster.fixtures[0].domain.shutdown().await;
    let observed = work?;
    joined?;
    ensure!(observed == expected, "completed checkpoint reply bridge required extra handler polls: {observed:?}, expected {expected:?}");
    Ok(())
}
