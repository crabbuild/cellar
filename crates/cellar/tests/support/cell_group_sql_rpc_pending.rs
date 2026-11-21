// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A final RPC proposal may wait behind its own committed checkpoint receipt.
use super::*;
use cellar::application::{
    Application, Cell, Context as CellContext, HandlerFuture, Invocation, Output, Registry, Storage,
};
use cellar::engine_api::RpcData;
use std::sync::atomic::{AtomicUsize, Ordering};

struct CheckpointCell {
    storage: Storage,
    calls: Arc<AtomicUsize>,
}

impl Cell for CheckpointCell {
    fn invoke(self: Arc<Self>, _: CellContext, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            anyhow::ensure!(
                matches!(invocation, Invocation::CellRpc { ref method, .. } if method == "Run")
            );
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.storage.sql("UPDATE value SET n=n+1", &[])?;
            self.storage.sync().await?;
            self.storage.sql("UPDATE value SET n=n+10", &[])?;
            Ok(Output::Rpc(RpcData::Json("11".into())))
        })
    }
}

pub(crate) async fn rpc_checkpoint_pending_progress(rooted: bool) -> Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let application = Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_cell(
        RaftClass::new("Counter", 1, [224; 32])?.json_cell_rpc("Run", [225; 32])?,
        move |context, _| {
            Ok(Arc::new(CheckpointCell {
                storage: context.storage()?,
                calls: counted.clone(),
            }))
        },
    )?;
    let class = application.resolve_raft_class("Counter")?;
    let (mut fixture, route, a, _) = Box::pin(registered::setup(rooted, &class)).await?;
    let pool = fixture.appliers[&(0, route.history)].0.clone();
    let runtime = fixture.runtimes[0].clone();
    let dispatcher = CellGroupDispatcher::new(
        &fixture.cluster.domain,
        fixture.cluster.directory.clone(),
        class.clone(),
        VersionKeys::new(
            VersionSigningKey::new([226; 32], u64::MAX)?,
            vec![],
            Duration::from_secs(30),
        )?,
        CellDispatchLimits {
            max_cached_routes: 32,
            route_cache_bytes: 256 * 1024,
            max_invocations: 3,
            retained_bytes: 1024 * 1024,
            max_input_bytes: 1024,
        },
    )?;
    let now = fixture
        .cluster
        .domain
        .run(async { cellar::asyncrt::wall_ms() as u64 })
        .await?;
    let request = CellCommandRequest {
        id: CommandId::new(ClientId::new([227; 16])?, 1)?,
        contract: class.contract("Run").context("Run")?,
        input: b"{}".to_vec(),
        durability: Durability::Quorum,
        retry_until_ms: now + 60_000,
    };
    let limits = CellRoutingLimits {
        timeout: Duration::from_secs(15),
        max_attempts: 4,
    };
    let options = fixture.cell_options(&a).await?;
    let mut invocation = dispatcher
        .command(a.clone(), request.clone(), limits, None)
        .await?;
    invocation.begin().await?;
    let prepared = {
        let mut preparation = Box::pin(invocation.prepare(&runtime, &pool, options.clone()));
        loop {
            if let std::task::Poll::Ready(result) = futures_util::poll!(&mut preparation) {
                break result.with_context(|| {
                    format!(
                        "prepare final RPC after checkpoint; callback entries={}",
                        calls.load(Ordering::SeqCst),
                    )
                })?;
            }
            // A completed RPC must not cancel a pump turn that still owns
            // another replica's physical apply work.
            fixture.pump(&[], &mut BTreeMap::new()).await?;
            tokio::task::yield_now().await;
        }
    };
    anyhow::ensure!(matches!(prepared, CellCommandPreparation::Prepared));
    let checkpoint = runtime
        .reconciliation_authority(route.history, a.clone())
        .await?;
    anyhow::ensure!(
        checkpoint.cell().version.sequence.get() >= 2 && calls.load(Ordering::SeqCst) == 1
    );
    let checkpoint_version = checkpoint.cell().version.clone();
    reconciliation::apply_through(&mut fixture, checkpoint.cell().last_mutation.index).await?;
    let accepted = command_dispatch::accepted(&mut invocation).await?;
    let before = fixture.cluster.voters[0].log.performance_snapshot();
    // Do not forward the final proposal. Its earlier root checkpoint is real
    // committed state, but cannot supply the final application response.
    for _ in 0..4 {
        let error = invocation
            .settle()
            .await
            .err()
            .context("unfinished RPC returned output")?;
        anyhow::ensure!(error.ambiguous);
        anyhow::ensure!(matches!(
            error.cause.as_ref(),
            CellDispatchError::Execution(ExecutionError::Uncertain)
        ));
        anyhow::ensure!(pool.usage()?.active_cells == 1 && calls.load(Ordering::SeqCst) == 1);
    }
    let after = fixture.cluster.voters[0].log.performance_snapshot();
    let work = (|| -> Result<()> {
        anyhow::ensure!(
            pending::calls(&after, "sql_publish")? - pending::calls(&before, "sql_publish")? == 1,
            "unchanged RPC checkpoint reopened SQL reconciliation"
        );
        anyhow::ensure!(
            pending::calls(&after, "sql_other")? == pending::calls(&before, "sql_other")?
        );
        Ok(())
    })();
    fixture.drive(Some(accepted.position.index)).await?;
    let output = command_dispatch::committed(&mut invocation).await?;
    let receipt = output.local_receipt().context("local final RPC receipt")?;
    anyhow::ensure!(receipt.receipt.result() == b"11");
    anyhow::ensure!(receipt.version.sequence.get() == checkpoint_version.sequence.get() + 1);
    for replica in &fixture.runtimes {
        let current = replica
            .reconciliation_authority(route.history, a.clone())
            .await?;
        anyhow::ensure!(
            current.cell().version == receipt.version && current.cell().witness == receipt.witness
        );
    }
    anyhow::ensure!(checkpoint.check().is_err());
    drop(output);
    drop(invocation);
    drop(checkpoint);
    let authority = reads::selected(&mut fixture, 0, &a, CellReadConsistency::Linearizable).await?;
    let output = pool
        .query(options.clone(), authority, |db, _| {
            let n = db.query_row("SELECT n FROM value", [], |row| row.get::<_, i64>(0))?;
            Ok(n.to_le_bytes().to_vec())
        })
        .await?;
    anyhow::ensure!(output.result() == 11_i64.to_le_bytes());
    drop(output);
    let mut duplicate = dispatcher.command(a, request, limits, None).await?;
    duplicate.begin().await?;
    let CellCommandPreparation::Committed(output) =
        duplicate.prepare(&runtime, &pool, options).await?
    else {
        anyhow::bail!("completed RPC replay entered a callback");
    };
    anyhow::ensure!(
        output
            .local_receipt()
            .context("replayed RPC receipt")?
            .receipt
            .result()
            == b"11"
    );
    drop(output);
    drop(duplicate);
    anyhow::ensure!(calls.load(Ordering::SeqCst) == 1 && pool.usage()?.active_cells == 0);
    anyhow::ensure!(dispatcher.usage() == (0, 0));
    drop(dispatcher);
    drop(pool);
    drop(runtime);
    fixture.close().await?;
    work
}
