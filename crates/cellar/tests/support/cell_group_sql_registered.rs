// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Registered callbacks, logical identity and receipts through real shared Raft.
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

pub(crate) async fn commands(rooted: bool) -> Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let class = Arc::new(
        RaftClass::new("Counter", 1, [41; 32])?
            .alarm("Wake", [42; 32], |_, _| Ok(()))?
            .command("Increment", [43; 32], move |context, input| {
                observed.fetch_add(1, Ordering::SeqCst);
                assert_eq!(context.command().sequence(), 1);
                assert_eq!(context.inputs().random, [17; 32]);
                assert_eq!(context.identity().incarnation, IncarnationId::new([8; 16])?);
                let delta = i64::from_be_bytes(input.try_into()?);
                context.sql().execute("UPDATE value SET n=n+?1", [delta])?;
                context.set_alarm("Wake", 50, b"wake")?;
                Ok(context.identity().cell.as_str().as_bytes().to_vec())
            })?,
    );
    let cluster = Cluster::with_storage_schema(
        RaftRuntimeOptions::default(),
        tempfile::tempdir()?,
        Arc::new(DirectFileSystem),
        class.schema_digest(),
    )
    .await?;
    let mut fixture = Box::pin(RuntimeCluster::from_cluster(cluster, 256, 256, 1_000_000)).await?;
    let route = fixture.cluster.birth.route().clone();
    if rooted {
        for (n, voter) in fixture.cluster.voters.iter_mut().enumerate() {
            voter.extents = Some(NodeExtentStore::open(
                fixture.cluster._root.path().join(format!("rooted-{n}")),
                Arc::new(DirectFileSystem),
                Default::default(),
            )?);
        }
    }
    let a = history(&route, "registered-a")?;
    let b = history(&route, "registered-b")?;
    for node in 0..3 {
        apply::install(&mut fixture, &a, node).await?;
    }
    fixture.runtimes[0].campaign(route.history).await?;
    fixture.drive(None).await?;
    let bytes = images::image(None, "CREATE TABLE value(n); INSERT INTO value VALUES(0);")?;
    for cell in [&a, &b] {
        let accepted = fixture.runtimes[0]
            .propose(
                route.history,
                apply::enrollment_bytes(&route, cell, &bytes, rooted)?,
            )
            .await?;
        fixture.drive(Some(accepted.position.index)).await?;
    }
    let pool = fixture.appliers[&(0, route.history)].0.clone();
    let input = 7_i64.to_be_bytes().to_vec();
    let attempt = |tag| -> Result<CommandAttempt> {
        let mut attempt = cell_attempt(tag)?;
        attempt.request.operation = "Increment".into();
        attempt.request.contract_digest = [43; 32];
        attempt.request.request_digest = command_request_digest(&input);
        Ok(attempt)
    };
    for (n, cell) in [&a, &b].into_iter().enumerate() {
        let authority = fixture.runtimes[0]
            .execution_authority(route.history, cell.clone())
            .await?;
        let CellSqlOutcome::Prepared(prepared) = pool
            .stage_registered(
                fixture.cell_options(cell).await?,
                authority,
                attempt(150 + n as u8)?,
                class.clone(),
                input.clone(),
                true,
            )
            .await?
        else {
            anyhow::bail!("prepared registered command")
        };
        pool.mark_submitted(cell.clone(), prepared.proposal())
            .await?;
        let accepted = accepted_sql(&fixture.runtimes[0], &prepared).await?;
        fixture.drive(Some(accepted.position.index)).await?;
        let authority = fixture.runtimes[0]
            .execution_authority(route.history, cell.clone())
            .await?;
        let receipt = pool.finish(authority).await?;
        assert_eq!(
            receipt.receipt().receipt.result(),
            cell.cell.as_str().as_bytes()
        );
        assert_eq!(receipt.receipt().version.history, *cell);
        assert_eq!(receipt.receipt().version.sequence.get(), 2);
        drop(receipt);
        drop(prepared);
        for runtime in &fixture.runtimes {
            let selected = runtime
                .reconciliation_authority(route.history, cell.clone())
                .await?;
            assert_eq!(selected.cell().effects.next_alarm_unix_millis, Some(50));
            assert_eq!(selected.cell().version.sequence.get(), 2);
        }
    }
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    // A's exact receipt remains valid after B advances the group; no callback reruns.
    let authority = fixture.runtimes[0]
        .execution_authority(route.history, a.clone())
        .await?;
    let CellSqlOutcome::Receipt(receipt) = pool
        .stage_registered(
            fixture.cell_options(&a).await?,
            authority,
            attempt(155)?,
            class.clone(),
            input.clone(),
            false,
        )
        .await?
    else {
        anyhow::bail!("registered duplicate")
    };
    assert_eq!(
        receipt.receipt().receipt.result(),
        a.cell.as_str().as_bytes()
    );
    drop(receipt);
    for fault in 0..4 {
        let mut request = attempt(156 + fault)?;
        let mut supplied = input.clone();
        let mut definition = class.clone();
        match fault {
            0 => request.request.contract_digest = [44; 32],
            1 => supplied[0] ^= 1,
            2 => request.request.operation = "Missing".into(),
            _ => {
                definition = Arc::new(RaftClass::new("Counter", 1, [45; 32])?.command(
                    "Increment",
                    [43; 32],
                    |_, _| panic!("wrong schema callback"),
                )?)
            }
        }
        let authority = fixture.runtimes[0]
            .execution_authority(route.history, a.clone())
            .await?;
        let result = pool
            .stage_registered(
                fixture.cell_options(&a).await?,
                authority,
                request,
                definition,
                supplied,
                false,
            )
            .await;
        match fault {
            0 | 3 => assert!(matches!(
                result,
                Err(ExecutionError::Registration(
                    RegistrationError::ContractMismatch
                ))
            )),
            1 => assert!(matches!(
                result,
                Err(ExecutionError::Refused(CommandFailure::CommandConflict))
            )),
            2 => assert!(matches!(
                result,
                Err(ExecutionError::Registration(
                    RegistrationError::UnknownOperation(_)
                ))
            )),
            _ => unreachable!(),
        }
        assert_eq!(pool.usage()?.active_cells, 0);
    }
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(pool.usage()?.retained_bytes, 0);
    drop(pool);
    fixture.close().await
}
