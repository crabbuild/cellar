// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Known local execution still needs real quorum; hints cannot authorize RPC.
use super::remote_queries::{dispatcher, gateways, invocation, keys};
use super::*;
use object_store::{path::Path, ObjectStore};

pub(crate) async fn cached_dispatch_execution() -> Result<()> {
    for rooted in [false, true] {
        let class = Arc::new(
            RaftClass::new("Counter", 1, [81; 32])?
                .initializer([90; 32], |_, tx| {
                    tx.execute_batch("CREATE TABLE value(n); INSERT INTO value VALUES(7);")?;
                    Ok(())
                })?
                .command("Add", [82; 32], |context, _| {
                    context.sql().execute("UPDATE value SET n=n+1", [])?;
                    Ok(b"added".to_vec())
                })?
                .query("Get", [83; 32], |context, _| {
                    Ok(context
                        .query_row("SELECT n FROM value", [], |r| r.get::<_, i64>(0))?
                        .to_le_bytes()
                        .to_vec())
                })?,
        );
        let (mut fixture, route, a, b) = Box::pin(registered::setup(rooted, &class)).await?;
        let controllers = controller::start_with_limits(&mut fixture, controller::limits()).await?;
        let gateways = gateways(&fixture, class.clone()).await?;
        let dispatch = dispatcher(&fixture, class.clone(), keys(190)?)?;
        let mut warm = invocation(
            &dispatch,
            &class,
            &a,
            CellQueryConsistency::Linearizable,
            Duration::from_secs(5),
            b"",
        )
        .await?;
        let pool = &fixture.appliers[&(0, route.history)].0;
        assert_eq!(
            warm.execute(&fixture.runtimes[0], pool, fixture.cell_options(&a).await?)
                .await?
                .result(),
            &0_i64.to_le_bytes()
        );
        drop(warm);
        assert_eq!(dispatch.route_cache_usage()?.entries, 1);
        let head = Path::from(format!(
            "namespaces/{}/cell-groups/head",
            hex::encode(a.namespace.bytes())
        ));
        let saved = fixture.cluster.store.get(&head).await?.bytes().await?;
        fixture.cluster.store.delete(&head).await?;
        assert!(dispatch.refresh_routes().await.is_err());
        // The range covers B and an as-yet unenrolled C; no per-cell cache entry
        // or fresh metadata read is needed for these captured invocations.
        let mut other = invocation(
            &dispatch,
            &class,
            &b,
            CellQueryConsistency::Stale,
            Duration::from_secs(5),
            b"",
        )
        .await?;
        assert_eq!(
            other
                .execute(&fixture.runtimes[0], pool, fixture.cell_options(&b).await?)
                .await?
                .result(),
            &0_i64.to_le_bytes()
        );
        drop(other);
        let retry_until_ms = fixture
            .cluster
            .domain
            .run(async { cellar::asyncrt::wall_ms() as u64 + 60_000 })
            .await?;
        let command = dispatch
            .command(
                a.clone(),
                CellCommandRequest {
                    id: CommandId::new(ClientId::new([191; 16])?, 1)?,
                    contract: class.contract("Add").context("Add")?,
                    input: vec![],
                    durability: Durability::Quorum,
                    retry_until_ms,
                },
                CellRoutingLimits {
                    timeout: Duration::from_secs(5),
                    max_attempts: 2,
                },
                None,
            )
            .await?;
        let result = controllers[0]
            .command(command, fixture.cell_options(&a).await?)?
            .response()
            .await?;
        assert_eq!(result.receipt().receipt.result(), b"added");
        drop(result);
        let c = history(&route, "cache-enrolled")?;
        let enrollment = dispatch
            .enroll(
                c.clone(),
                CellEnrollmentRequest {
                    creation: ProposalId::new([192; 16])?,
                    durability: Durability::Quorum,
                },
                CellRoutingLimits {
                    timeout: Duration::from_secs(5),
                    max_attempts: 2,
                },
                None,
            )
            .await?;
        let enrolled = controllers[0]
            .enroll(enrollment, fixture.cell_options(&c).await?)?
            .response()
            .await?;
        drop(enrolled);
        for (cell, expected) in [(&a, 1_i64), (&c, 7_i64)] {
            let mut read = invocation(
                &dispatch,
                &class,
                cell,
                CellQueryConsistency::Linearizable,
                Duration::from_secs(5),
                b"",
            )
            .await?;
            assert_eq!(
                read.execute(
                    &fixture.runtimes[0],
                    pool,
                    fixture.cell_options(cell).await?
                )
                .await?
                .result(),
                &expected.to_le_bytes()
            );
        }
        // The RPC destination deliberately keeps fresh catalog admission until
        // transfer-seal authority is integrated. Sender hints cannot bypass it.
        let mut remote = invocation(
            &dispatch,
            &class,
            &a,
            CellQueryConsistency::Stale,
            Duration::from_secs(5),
            b"",
        )
        .await?;
        assert!(matches!(
            remote
                .execute_remote(&fixture.transports[1], MemberId::new(1)?)
                .await,
            Err(CellDispatchError::Transport(TransportError::Query(
                CommandFailure::Unavailable
            )))
        ));
        drop(remote);
        assert_eq!(dispatch.route_cache_usage()?.entries, 1);
        assert_eq!(dispatch.usage(), (0, 0));
        fixture.cluster.store.put(&head, saved.into()).await?;
        for controller in controllers {
            controller.close().await?;
        }
        drop(gateways);
        fixture.close().await?;
    }
    Ok(())
}
