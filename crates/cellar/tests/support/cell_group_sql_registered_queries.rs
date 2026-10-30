// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Registered queries keep actual cell versions, immutable images and ownership.
use super::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex,
};

fn verify(
    output: &CellSqlQueryResult,
    value: i64,
    expected: &CellVersionObservation,
    schema: [u8; 32],
) -> Result<()> {
    let bytes = output.result();
    assert_eq!(bytes.len(), 24);
    assert_eq!(i64::from_le_bytes(bytes[..8].try_into()?), value);
    assert_eq!(
        u64::from_le_bytes(bytes[8..16].try_into()?),
        expected.version.sequence.get()
    );
    assert_eq!(
        u64::from_le_bytes(bytes[16..].try_into()?),
        expected.witness.position.index.get()
    );
    assert_eq!(output.observation(), expected);
    reads::verify_observation(output, schema)
}

pub(crate) async fn queries(rooted: bool) -> Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let (entered, started) = tokio::sync::oneshot::channel();
    let entered = Mutex::new(Some(entered));
    let (release, wait) = std::sync::mpsc::sync_channel(1);
    let wait = Mutex::new(wait);
    let class = Arc::new(RaftClass::new("Counter", 1, [51; 32])?
        .command("Increment", [52; 32], |context, _| {
            context.sql().execute("UPDATE value SET n=n+7", [])?;
            Ok(vec![7])
        })?
        .query("Get", [53; 32], move |context, input| {
            observed.fetch_add(1, Ordering::SeqCst);
            let QueryVersion::Cell(observation) = context.version() else { anyhow::bail!("shared query received a group cut") };
            assert_eq!(context.identity(), &observation.version.history);
            for sql in ["UPDATE value SET n=999 RETURNING n", "PRAGMA user_version=8", "ATTACH DATABASE ':memory:' AS other", "SELECT count(*) FROM __sys_raft_receipts"] {
                assert!(context.query_row(sql, [], |row| row.get::<_, i64>(0)).is_err(), "{sql}");
            }
            if input == b"panic" { panic!("contained registered query panic"); }
            if input == b"spin" {
                let _: i64 = context.query_row("WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n WHERE x<1000000000) SELECT sum(x) FROM n", [], |row| row.get(0))?;
            }
            if input.starts_with(b"hold") {
                entered.lock().unwrap().take().context("one held invocation")?.send(()).map_err(|_| anyhow::anyhow!("held receiver"))?;
                wait.lock().unwrap().recv_timeout(Duration::from_secs(10))?;
            }
            let value: i64 = context.query_row("SELECT n FROM value", [], |row| row.get(0))?;
            let mut result = value.to_le_bytes().to_vec();
            result.extend_from_slice(&observation.version.sequence.get().to_le_bytes());
            result.extend_from_slice(&observation.witness.position.index.get().to_le_bytes());
            Ok(result)
        })?);
    let (mut fixture, route, a, b) = Box::pin(registered::setup(rooted, &class)).await?;
    let pool = fixture.appliers[&(0, route.history)].0.clone();
    let contract = class.contract("Get").context("Get")?;
    let options = fixture.cell_options(&a).await?;
    let old = reads::selected(&mut fixture, 0, &a, CellReadConsistency::Stale).await?;
    let original = old.cell().observation();
    for (n, cell) in [&a, &b].into_iter().enumerate() {
        let mut attempt = cell_attempt(170 + n as u8)?;
        attempt.request.operation = "Increment".into();
        attempt.request.contract_digest = [52; 32];
        attempt.request.request_digest = command_request_digest(&[]);
        let authority = fixture.runtimes[0]
            .execution_authority(route.history, cell.clone())
            .await?;
        let CellSqlOutcome::Prepared(prepared) = pool
            .stage_registered(
                fixture.cell_options(cell).await?,
                authority,
                attempt,
                class.clone(),
                vec![],
                true,
            )
            .await?
        else {
            anyhow::bail!("command preparation")
        };
        pool.mark_submitted(cell.clone(), prepared.proposal())
            .await?;
        let accepted = accepted_sql(&fixture.runtimes[0], &prepared).await?;
        fixture.drive(Some(accepted.position.index)).await?;
        let authority = fixture.runtimes[0]
            .execution_authority(route.history, cell.clone())
            .await?;
        drop(pool.finish(authority).await?);
        drop(prepared);
    }
    let output = pool
        .query_registered(
            options.clone(),
            old,
            class.clone(),
            contract.clone(),
            vec![],
        )
        .await?;
    verify(&output, 0, &original, route.schema_digest)?;
    drop(output);
    let fresh = reads::selected(&mut fixture, 0, &a, CellReadConsistency::Linearizable).await?;
    let latest = fresh.cell().observation();
    assert_eq!(latest.version.sequence.get(), 2);
    assert!(
        latest.witness.position.index
            < fixture.runtimes[0]
                .status(route.history)
                .await?
                .applied
                .index
    );
    let output = pool
        .query_registered(
            options.clone(),
            fresh,
            class.clone(),
            contract.clone(),
            vec![],
        )
        .await?;
    verify(&output, 7, &latest, route.schema_digest)?;
    drop(output);
    let before = calls.load(Ordering::SeqCst);
    for fault in 0..6 {
        let authority = reads::selected(&mut fixture, 0, &a, CellReadConsistency::Stale).await?;
        let mut request = contract.clone();
        let mut definition = class.clone();
        let mut supplied = vec![];
        let mut config = options.clone();
        match fault {
            0 => request.digest = [54; 32],
            1 => request.kind = OperationKind::Command,
            2 => request.operation = "Missing".into(),
            3 => {
                definition = Arc::new(RaftClass::new("Counter", 1, [55; 32])?.query(
                    "Get",
                    [53; 32],
                    |_, _| panic!("wrong class query"),
                )?)
            }
            4 => {
                match &mut config {
                    CellSqlOptions::Materialized(o) => o.limits.envelope.max_payload_bytes = 1024,
                    CellSqlOptions::Extents(o) => o.limits.envelope.max_payload_bytes = 1024,
                }
                supplied = vec![0; 1025];
            }
            _ => request.operation = "x".repeat(129),
        }
        let result = pool
            .query_registered(config, authority, definition, request, supplied)
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
                Err(ExecutionError::Registration(
                    RegistrationError::KindMismatch
                ))
            )),
            2 => assert!(matches!(
                result,
                Err(ExecutionError::Registration(
                    RegistrationError::UnknownOperation(_)
                ))
            )),
            4 => assert!(matches!(
                result,
                Err(ExecutionError::Refused(CommandFailure::Overloaded))
            )),
            _ => assert!(matches!(
                result,
                Err(ExecutionError::Registration(RegistrationError::Invalid(
                    "class/operation name"
                )))
            )),
        }
        assert_eq!(calls.load(Ordering::SeqCst), before);
        assert_eq!(pool.usage()?.retained_operations, 0);
    }
    let authority = reads::selected(&mut fixture, 0, &a, CellReadConsistency::Stale).await?;
    assert!(matches!(
        pool.query_registered(
            options.clone(),
            authority,
            class.clone(),
            contract.clone(),
            b"panic".to_vec()
        )
        .await,
        Err(ExecutionError::Storage(_))
    ));
    let deadline = reads::deadline(&fixture, 100).await?;
    let authority = fixture.runtimes[0]
        .read_authority(
            route.history,
            a.clone(),
            CellReadConsistency::Stale,
            deadline,
        )
        .await?;
    assert!(tokio::time::timeout(
        Duration::from_secs(3),
        pool.query_registered(
            options.clone(),
            authority,
            class.clone(),
            contract.clone(),
            b"spin".to_vec()
        )
    )
    .await?
    .is_err());
    let authority = reads::selected(&mut fixture, 0, &a, CellReadConsistency::Stale).await?;
    let output = pool
        .query_registered(
            options.clone(),
            authority,
            class.clone(),
            contract.clone(),
            vec![],
        )
        .await?;
    verify(&output, 7, &latest, route.schema_digest)?;
    drop(output);
    let authority = reads::selected(&mut fixture, 0, &a, CellReadConsistency::Stale).await?;
    let caller = pool.clone();
    let waiter = fixture.cluster.domain.spawn(async move {
        let mut input = vec![0; 256 * 1024];
        input[..4].copy_from_slice(b"hold");
        caller
            .query_registered(options, authority, class, contract, input)
            .await
    })?;
    tokio::time::timeout(Duration::from_secs(5), started).await??;
    waiter.abort();
    assert!(waiter.await.is_err());
    assert_eq!(pool.usage()?.retained_operations, 1);
    assert!(pool.usage()?.retained_bytes >= 256 * 1024);
    assert!(
        tokio::time::timeout(Duration::from_millis(30), pool.close())
            .await
            .is_err()
    );
    release.send(())?;
    tokio::time::timeout(Duration::from_secs(5), pool.close()).await??;
    assert_eq!(pool.usage()?.retained_operations, 0);
    assert_eq!(pool.usage()?.retained_bytes, 0);
    drop(pool);
    fixture.close().await
}
