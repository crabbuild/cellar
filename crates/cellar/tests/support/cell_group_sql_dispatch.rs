// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Catalog-selected invocations use real Raft reads and registered SQL workers.
use super::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex,
};

fn limits() -> CellRoutingLimits {
    CellRoutingLimits {
        timeout: Duration::from_secs(5),
        max_attempts: 2,
    }
}

fn request(class: &RaftClass, consistency: CellQueryConsistency) -> Result<CellQueryRequest> {
    Ok(CellQueryRequest {
        invocation: ProposalId::new([201; 16])?,
        contract: class.contract("Get").context("Get")?,
        input: vec![7],
        consistency,
    })
}

async fn execute(
    fixture: &mut RuntimeCluster,
    query: &mut CellQueryInvocation,
    cell: &CellHistory,
    node: usize,
) -> Result<CellQueryResponse> {
    let runtime = fixture.runtimes[node].clone();
    let pool = fixture.appliers[&(node, fixture.cluster.birth.route().history)]
        .0
        .clone();
    let options = fixture.cell_options_at(cell, node).await?;
    let work = query.execute(&runtime, &pool, options);
    tokio::pin!(work);
    loop {
        cellar::asyncrt::select! {
            result = &mut work => return result.map_err(Into::into),
            result = async {
                fixture.pump(&[], &mut BTreeMap::new()).await?;
                tokio::task::yield_now().await;
                Ok::<_, anyhow::Error>(())
            } => result?,
        }
    }
}

pub(crate) async fn routed(rooted: bool) -> Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let (entered, started) = tokio::sync::oneshot::channel();
    let entered = Mutex::new(Some(entered));
    let (release, waiting) = std::sync::mpsc::sync_channel(1);
    let waiting = Mutex::new(waiting);
    let class = Arc::new(RaftClass::new("Counter", 1, [81; 32])?.query(
        "Get",
        [82; 32],
        move |context, input| {
            count.fetch_add(1, Ordering::SeqCst);
            let QueryVersion::Cell(observation) = context.version() else {
                anyhow::bail!("group cut")
            };
            assert_eq!(context.identity(), &observation.version.history);
            if input.starts_with(b"hold") {
                entered
                    .lock()
                    .unwrap()
                    .take()
                    .context("one held query")?
                    .send(())
                    .map_err(|_| anyhow::anyhow!("entered"))?;
                waiting
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(10))?;
            }
            let n: i64 = context.query_row("SELECT n FROM value", [], |row| row.get(0))?;
            Ok(n.to_le_bytes().to_vec())
        },
    )?);
    let (mut fixture, route, a, b) = Box::pin(registered::setup(rooted, &class)).await?;
    let keys = VersionKeys::new(
        VersionSigningKey::new([83; 32], u64::MAX)?,
        vec![],
        Duration::from_secs(10),
    )?;
    let bounds = CellDispatchLimits {
        max_cached_routes: 32,
        route_cache_bytes: 256 * 1024,
        max_invocations: 1,
        retained_bytes: 1024 * 1024,
        max_input_bytes: 256 * 1024,
    };
    let dispatcher = CellGroupDispatcher::new(
        &fixture.cluster.domain,
        fixture.cluster.directory.clone(),
        class.clone(),
        keys.clone(),
        bounds,
    )?;
    let mut query = dispatcher.query(
        a.clone(),
        request(&class, CellQueryConsistency::Linearizable)?,
        limits(),
        None,
    )?;
    assert!(matches!(
        dispatcher.clone().query(
            b.clone(),
            request(&class, CellQueryConsistency::Stale)?,
            limits(),
            None
        ),
        Err(CellDispatchError::Runtime(RuntimeError::Overloaded))
    ));
    query.begin().await?;
    let first = query.attempt().context("attempt")?.clone();
    assert_eq!(first.route(), &route);
    let response = execute(&mut fixture, &mut query, &a, 0).await?;
    assert_eq!(response.result(), &0_i64.to_le_bytes());
    let now = fixture
        .cluster
        .domain
        .run(async { cellar::asyncrt::wall_ms() as u64 })
        .await?;
    assert_eq!(
        keys.verify_cell(response.token(), &a, route.schema_digest, now)?,
        *response.observation()
    );
    let token = response.token().clone();
    let expected = response.observation().clone();
    drop(response); // Lost response: retry the same immutable query and budget.
    let pool = fixture.appliers[&(0, route.history)].0.clone();
    assert!(matches!(
        query
            .execute(&fixture.runtimes[0], &pool, fixture.cell_options(&a).await?)
            .await,
        Err(CellDispatchError::Invalid("consumed route attempt"))
    ));
    query.retry().await?;
    let second = query.attempt().context("second")?;
    assert_eq!(second.request(), first.request());
    assert_eq!(second.deadline_ms(), first.deadline_ms());
    assert_eq!(second.ordinal(), 2);
    let repeated = execute(&mut fixture, &mut query, &a, 0).await?;
    assert_eq!(repeated.observation(), &expected);
    drop(repeated);
    assert!(matches!(
        query.retry().await,
        Err(CellDispatchError::Routing(CellRoutingError {
            reason: CellRoutingFailure::AttemptsExhausted,
            ..
        }))
    ));
    drop(query);
    assert_eq!(dispatcher.usage(), (0, 0));

    let mut at_least = dispatcher.query(
        a.clone(),
        request(&class, CellQueryConsistency::AtLeast(token.clone()))?,
        limits(),
        Some(first.cached_route()),
    )?;
    at_least.begin().await?;
    let output = execute(&mut fixture, &mut at_least, &a, 1).await?;
    assert_eq!(output.observation(), &expected);
    drop(output);
    drop(at_least);
    let before = calls.load(Ordering::SeqCst);
    let mut wrong = dispatcher.query(
        b.clone(),
        request(&class, CellQueryConsistency::AtLeast(token))?,
        limits(),
        None,
    )?;
    wrong.begin().await?;
    assert!(matches!(
        wrong
            .execute(&fixture.runtimes[0], &pool, fixture.cell_options(&b).await?)
            .await,
        Err(CellDispatchError::Version(
            cellar::application::consensus::VersionError::WrongHistory
        ))
    ));
    drop(wrong);
    assert_eq!(calls.load(Ordering::SeqCst), before);

    let mut wrong_engine = dispatcher.query(
        a.clone(),
        request(&class, CellQueryConsistency::Stale)?,
        limits(),
        Some(first.cached_route()),
    )?;
    wrong_engine.begin().await?;
    let other_pool = fixture.appliers[&(1, route.history)].0.clone();
    assert!(matches!(
        wrong_engine
            .execute(
                &fixture.runtimes[0],
                &other_pool,
                fixture.cell_options(&a).await?
            )
            .await,
        Err(CellDispatchError::Execution(ExecutionError::Invalid(
            "cell query engine/schema"
        )))
    ));
    drop(wrong_engine);
    drop(other_pool);
    let mut invalid_hint = dispatcher.query(
        a.clone(),
        request(&class, CellQueryConsistency::Stale)?,
        limits(),
        Some(first.cached_route()),
    )?;
    invalid_hint.begin().await?;
    let mut hint = route.clone();
    hint.epoch = RouteEpoch::new(2)?;
    hint.schema_digest = [86; 32];
    assert!(matches!(
        invalid_hint.redirect(&hint).await,
        Err(CellDispatchError::Routing(CellRoutingError {
            reason: CellRoutingFailure::InvalidHint,
            ..
        }))
    ));
    assert!(matches!(
        invalid_hint
            .execute(&fixture.runtimes[0], &pool, fixture.cell_options(&a).await?)
            .await,
        Err(CellDispatchError::Routing(CellRoutingError {
            reason: CellRoutingFailure::StaleAttempt,
            ..
        }))
    ));
    drop(invalid_hint);
    assert_eq!(calls.load(Ordering::SeqCst), before);

    let foreign_domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let foreign = CellGroupDispatcher::new(
        &foreign_domain,
        fixture.cluster.directory.clone(),
        class.clone(),
        keys.clone(),
        bounds,
    )?;
    let mut wrong = foreign.query(
        a.clone(),
        request(&class, CellQueryConsistency::Stale)?,
        limits(),
        None,
    )?;
    wrong.begin().await?;
    assert!(matches!(
        wrong
            .execute(&fixture.runtimes[0], &pool, fixture.cell_options(&a).await?)
            .await,
        Err(CellDispatchError::Invalid("foreign execution domain"))
    ));
    drop(wrong);
    drop(foreign);
    foreign_domain.shutdown().await;
    let bad_class = Arc::new(RaftClass::new("Counter", 1, [84; 32])?);
    assert!(matches!(
        CellGroupDispatcher::new(
            &fixture.cluster.domain,
            fixture.cluster.directory.clone(),
            bad_class,
            keys,
            bounds
        ),
        Err(CellDispatchError::Registration(
            RegistrationError::ContractMismatch
        ))
    ));

    let mut expired = dispatcher.query(
        a.clone(),
        request(&class, CellQueryConsistency::Stale)?,
        CellRoutingLimits {
            timeout: Duration::from_millis(10),
            max_attempts: 2,
        },
        Some(first.cached_route()),
    )?;
    expired.begin().await?;
    let original_deadline = expired.attempt().context("deadline")?.deadline_ms();
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(matches!(
        expired.retry().await,
        Err(CellDispatchError::Routing(CellRoutingError {
            reason: CellRoutingFailure::Deadline,
            ..
        }))
    ));
    assert_eq!(
        expired.attempt().context("same deadline")?.deadline_ms(),
        original_deadline
    );
    drop(expired);
    assert_eq!(calls.load(Ordering::SeqCst), before);

    // Confirm a replacement against the real directory, but do not invent its
    // physical owner or claim this metadata-only fixture performs a live move.
    let directory = fixture.cluster.directory.clone();
    let replacement = directory.prepare_group_birth(CellGroupBirthSpec {
        history: GroupHistory {
            group: GroupId::new([203; 16])?,
            ..route.history
        },
        range: route.range.clone(),
        epoch: RouteEpoch::new(2)?,
        creation: ProposalId::new([204; 16])?,
        voters: fixture.cluster.birth.voters().to_vec(),
        learners: vec![],
    })?;
    fixture
        .cluster
        .domain
        .run(directory.reserve_group_birth(&replacement))
        .await??;
    let mut relocated = dispatcher.query(
        a.clone(),
        request(&class, CellQueryConsistency::Stale)?,
        limits(),
        Some(first.cached_route()),
    )?;
    relocated.begin().await?;
    let original = relocated.attempt().context("original")?.clone();
    let view = fixture.cluster.domain.run(directory.observe()).await??;
    let publish = directory.prepare_replacement(
        &view,
        ProposalId::new([205; 16])?,
        std::slice::from_ref(&route),
        &[replacement.route().clone()],
    )?;
    fixture
        .cluster
        .domain
        .run(directory.publish(&publish))
        .await??;
    relocated.redirect(replacement.route()).await?;
    assert_eq!(
        relocated.attempt().context("redirect")?.deadline_ms(),
        original.deadline_ms()
    );
    assert_eq!(
        relocated.attempt().context("redirect")?.request(),
        original.request()
    );
    assert_eq!(
        relocated.attempt().context("redirect")?.route(),
        replacement.route()
    );
    assert!(matches!(
        relocated
            .execute(&fixture.runtimes[0], &pool, fixture.cell_options(&a).await?)
            .await,
        Err(CellDispatchError::Runtime(RuntimeError::NotFound))
    ));
    drop(relocated);
    assert_eq!(calls.load(Ordering::SeqCst), before);

    // A known route needs no new catalog lookup; actual destination authority is
    // still mandatory. Here the old physical owner has not been transitioned.
    let mut held_request = request(&class, CellQueryConsistency::Stale)?;
    held_request.input = vec![0; 256 * 1024];
    held_request.input[..4].copy_from_slice(b"hold");
    let mut held = dispatcher.query(
        a.clone(),
        held_request,
        limits(),
        Some(first.cached_route()),
    )?;
    held.begin().await?;
    assert!(dispatcher.usage().0 >= 512 * 1024);
    let runtime = fixture.runtimes[0].clone();
    let sql = pool.clone();
    let options = fixture.cell_options(&a).await?;
    let waiter = fixture
        .cluster
        .domain
        .spawn(async move { held.execute(&runtime, &sql, options).await })?;
    tokio::time::timeout(Duration::from_secs(5), started).await??;
    waiter.abort();
    assert!(waiter.await.is_err());
    assert_eq!(dispatcher.usage(), (0, 0));
    assert_eq!(pool.usage()?.retained_operations, 1);
    assert!(pool.usage()?.retained_bytes >= 256 * 1024);
    assert!(
        tokio::time::timeout(Duration::from_millis(30), pool.close())
            .await
            .is_err()
    );
    release.send(())?;
    tokio::time::timeout(Duration::from_secs(5), pool.close()).await??;
    assert_eq!(pool.usage()?.retained_bytes, 0);
    drop(pool);
    drop(dispatcher);
    fixture.close().await
}
