//! Actual node-wide pin admission across two SQL workers and joined shutdown.
use super::{faults, public_node, two_registrations, wait_status};
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{consensus::*, NodeOutcome, Output, Registry, ShutdownMode};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

fn definition(
    gate: Arc<public_node::Gate>,
    calls: Arc<AtomicUsize>,
) -> Result<public_node::Counter> {
    let class = RaftClass::new("Counter", 1, [21; 32])?
        .command("Increment", [22; 32], |context, input| {
            let delta = i64::from_be_bytes(input.try_into()?);
            context.sql().execute_batch("CREATE TABLE IF NOT EXISTS counter(value INTEGER); INSERT INTO counter SELECT 0 WHERE NOT EXISTS(SELECT 1 FROM counter)")?;
            context.sql().execute("UPDATE counter SET value=value+?1", [delta])?;
            let value: i64 = context.sql().query_row("SELECT value FROM counter", [], |row| row.get(0))?;
            Ok(value.to_be_bytes().to_vec())
        })?
        .query("Get", [23; 32], move |context, input| {
            calls.fetch_add(1, Ordering::SeqCst);
            if input == b"hold" { gate.wait()?; }
            let value: i64 = context.query_row("SELECT value FROM counter", [], |row| row.get(0))?;
            Ok(value.to_be_bytes().to_vec())
        })?;
    Ok(public_node::Counter {
        schema: class.schema_digest(),
        increment: class.contract("Increment").context("command contract")?,
        get: class.contract("Get").context("query contract")?,
        application: Arc::new(
            Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_class(class)?,
        ),
    })
}

pub async fn run(shutdown_held: bool) -> Result<()> {
    let (gate, mut entered) = public_node::Gate::new();
    let _release = public_node::Release(gate.clone());
    let calls = Arc::new(AtomicUsize::new(0));
    let definition = definition(gate.clone(), calls.clone())?;
    let registrations = two_registrations(definition.schema)?;
    let root = tempfile::tempdir()?;
    let node = public_node::node(root.path(), definition.application.clone()).await?;
    let service = public_node::cells_many(
        &node,
        definition.application.clone(),
        Arc::new(faults::Store::default()),
        registrations.clone(),
        false,
        RaftHostOptions {
            blocking_workers: 2,
            max_query_pins: 1,
            ..Default::default()
        },
        TransportOptions::default(),
    )
    .await?;
    let result = async {
        let handles = registrations
            .iter()
            .map(|registration| {
                service.cell(
                    registration.namespace.namespace,
                    &registration.birth.scope.cell,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let initial = service.resource_usage();
        ensure!(
            initial.query_pins
                == RaftQueryPinUsage {
                    in_use: 0,
                    limit: 1
                }
        );
        ensure!(initial.business.max_items == 128 && initial.control.max_items == 16);
        ensure!(initial.archive_capture.max_items == 1 && initial.archive_capture.max_bytes > 0);
        let mut written = Vec::new();
        for handle in &handles {
            public_node::ready(handle).await?;
            written.push(
                handle
                    .command(
                        definition.increment.clone(),
                        1i64.to_be_bytes().to_vec(),
                        public_node::options(1)?,
                    )
                    .await?,
            );
            wait_status(handle, |status| {
                status.command_ready && status.queries.historical
            })
            .await?;
        }
        // The first and second registrations have distinct physical SQL workers.
        let held = tokio::spawn(handles[0].query(
            definition.get.clone(),
            b"hold".to_vec(),
            QueryOptions {
                consistency: QueryConsistency::AtLeast(written[0].version_token.clone()),
                ..Default::default()
            },
        ));
        tokio::time::timeout(Duration::from_secs(5), entered.recv())
            .await?
            .context("query callback did not enter")?;
        ensure!(service.resource_usage().query_pins.in_use == 1);
        held.abort();
        ensure!(held.await.is_err_and(|error| error.is_cancelled()));
        ensure!(service.resource_usage().query_pins.in_use == 1);
        super::metrics::pin_count(&node, 1).await?;
        let refused = handles[1]
            .query(
                definition.get.clone(),
                vec![],
                QueryOptions {
                    consistency: QueryConsistency::AtLeast(written[1].version_token.clone()),
                    ..Default::default()
                },
            )
            .await;
        ensure!(
            matches!(refused, Err(CommandFailure::Overloaded)),
            "pin saturation: {refused:?}"
        );
        ensure!(
            calls.load(Ordering::SeqCst) == 1,
            "refused query ran a callback"
        );
        // Reserved control traffic and actual command/consensus/apply on the
        // other owner remain live while the cancelled reader is still held.
        public_node::ready(&handles[1]).await?;
        let newer = handles[1]
            .command(
                definition.increment.clone(),
                8i64.to_be_bytes().to_vec(),
                public_node::options(2)?,
            )
            .await?;
        ensure!(newer.result == 9i64.to_be_bytes());
        wait_status(&handles[1], |status| status.command_ready).await?;
        ensure!(service.resource_usage().query_pins.in_use == 1);
        if !shutdown_held {
            gate.release();
            tokio::time::timeout(Duration::from_secs(5), async {
                while service.resource_usage().query_pins.in_use != 0 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await?;
            let read = handles[1]
                .query(
                    definition.get.clone(),
                    vec![],
                    QueryOptions {
                        consistency: QueryConsistency::AtLeast(newer.version_token.clone()),
                        ..Default::default()
                    },
                )
                .await?;
            ensure!(read.result == 9i64.to_be_bytes() && read.version.covers(&newer.version)?);
            ensure!(calls.load(Ordering::SeqCst) == 2);
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let mut stop = Box::pin(node.shutdown(ShutdownMode::Preserve));
    let held_observation = if shutdown_held && result.is_ok() {
        let still_owned = tokio::time::timeout(Duration::from_millis(100), &mut stop)
            .await
            .is_err();
        Some((still_owned, service.resource_usage().query_pins.in_use))
    } else {
        None
    };
    // Always release and physically join, including a failed test assertion.
    gate.release();
    let joined = tokio::time::timeout(Duration::from_secs(15), stop).await??;
    result?;
    if let Some((still_owned, pins)) = held_observation {
        ensure!(still_owned && pins == 1);
    }
    let NodeOutcome::Stopped(report) = joined else {
        anyhow::bail!("unexpected stop: {joined:?}")
    };
    report
        .raft
        .context("missing Raft close observation")?
        .result?;
    ensure!(
        service.resource_usage().query_pins
            == RaftQueryPinUsage {
                in_use: 0,
                limit: 1
            }
    );
    Ok(())
}
