// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real catalog calls qualify bounded hints, not permission to serve a cell.
use super::*;
use cellar::application::consensus::{RaftClass, VersionKeys, VersionSigningKey};
use cellar_logic::consensus::{ClientId, CommandId, Durability};

async fn setup(
    count: usize,
    capacity: usize,
    bytes: usize,
) -> Result<(
    Fixture,
    Arc<RaftClass>,
    CellGroupDispatcher,
    Vec<GroupRoute>,
)> {
    let class = Arc::new(
        RaftClass::new("Cache", 1, [161; 32])?
            .query("Get", [162; 32], |_, _| Ok(vec![]))?
            .command("Add", [163; 32], |_, _| Ok(vec![]))?,
    );
    let f = Fixture::with_schema(
        DirectoryLimits {
            max_cached_pages: 0,
            max_cache_bytes: 0,
            ..Default::default()
        },
        class.schema_digest(),
    )
    .await?;
    let mut routes = ranges(count);
    for route in &mut routes {
        route.schema_digest = class.schema_digest();
    }
    f.reserve(&mut routes).await?;
    let mutation = f
        .directory
        .prepare_initialization(operation(990), &routes)?;
    f.run(f.directory.publish(&mutation)).await?;
    let dispatcher = CellGroupDispatcher::new(
        &f.domain,
        f.directory.clone(),
        class.clone(),
        VersionKeys::new(
            VersionSigningKey::new([164; 32], u64::MAX)?,
            vec![],
            Duration::from_secs(30),
        )?,
        CellDispatchLimits {
            max_cached_routes: capacity,
            route_cache_bytes: bytes,
            max_invocations: 8,
            retained_bytes: 1024 * 1024,
            max_input_bytes: 1024,
        },
    )?;
    Ok((f, class, dispatcher, routes))
}

fn cell(route: &GroupRoute, tag: usize) -> CellHistory {
    let cell = (0..100_000)
        .map(|i| CellId::new(format!("cache-{tag}-{i}")).unwrap())
        .find(|cell| {
            route
                .range
                .contains(&cell_routing_key(route.history.namespace, cell))
        })
        .unwrap();
    CellHistory {
        cluster: route.history.cluster,
        namespace: route.history.namespace,
        cell,
        incarnation: IncarnationId::new([165; 16]).unwrap(),
    }
}

fn query(
    dispatcher: &CellGroupDispatcher,
    class: &RaftClass,
    cell: CellHistory,
) -> Result<CellQueryInvocation> {
    Ok(dispatcher.query(
        cell,
        CellQueryRequest {
            invocation: operation(991),
            contract: class.contract("Get").context("Get")?,
            input: vec![],
            consistency: CellQueryConsistency::Stale,
        },
        routing_limits(4),
        None,
    )?)
}

pub(crate) async fn cached_operation_discovery() -> Result<()> {
    let (f, class, dispatcher, routes) = setup(1, 2, 16 * 1024).await?;
    let first = cell(&routes[0], 0);
    let mut warm = query(&dispatcher, &class, first.clone())?;
    let (result, reads) = fault::count_reads(warm.begin()).await;
    result?;
    assert!(!reads.is_empty());
    drop(warm);
    f.store.fail_read.store(true, Ordering::Release);
    for n in 1..65 {
        let mut query = query(&dispatcher.clone(), &class, cell(&routes[0], n))?;
        let (result, reads) = fault::count_reads(query.begin()).await;
        result?;
        assert!(reads.is_empty());
        assert_eq!(query.attempt().context("cached")?.route(), &routes[0]);
    }
    let mut command = dispatcher
        .command(
            first,
            CellCommandRequest {
                id: CommandId::new(ClientId::new([166; 16])?, 1)?,
                contract: class.contract("Add").context("Add")?,
                input: vec![],
                durability: Durability::Quorum,
                retry_until_ms: u64::MAX,
            },
            routing_limits(2),
            None,
        )
        .await?;
    let (result, reads) = fault::count_reads(command.begin()).await;
    result?;
    assert!(reads.is_empty());
    assert_eq!(command.attempt().context("command")?.route(), &routes[0]);
    drop(command);
    let usage = dispatcher.route_cache_usage()?;
    assert_eq!(usage.entries, 1);
    assert!(usage.reserved_bytes <= 16 * 1024);
    assert!(dispatcher.refresh_routes().await.is_err());
    assert_eq!(dispatcher.route_cache_usage()?, usage);
    assert_eq!(dispatcher.usage(), (0, 0));
    f.domain.shutdown().await;
    Ok(())
}

pub(crate) async fn bounded_route_hints() -> Result<()> {
    let (f, class, dispatcher, routes) = setup(4, 64, 16 * 1024).await?;
    assert_eq!(dispatcher.route_cache_usage()?.capacity, 2);
    for (index, hit) in [(0, false), (1, false), (0, true), (2, false), (1, false)] {
        let mut query = query(&dispatcher, &class, cell(&routes[index], index))?;
        let (result, reads) = fault::count_reads(query.begin()).await;
        result?;
        assert_eq!(reads.is_empty(), hit);
        assert!(dispatcher.route_cache_usage()?.entries <= 2);
        assert!(dispatcher.route_cache_usage()?.reserved_bytes <= 16 * 1024);
    }
    let selected = cell(&routes[0], 0);
    let next = publish_route_split(&f, &routes[0], &selected.cell).await?;
    assert_eq!(dispatcher.refresh_routes().await?, 2);
    assert_eq!(dispatcher.route_cache_usage()?.entries, 0);
    let mut moved = query(&dispatcher, &class, selected)?;
    let (result, reads) = fault::count_reads(moved.begin()).await;
    result?;
    assert!(!reads.is_empty());
    assert_eq!(moved.attempt().context("successor")?.route(), &next);
    assert_eq!(dispatcher.route_cache_usage()?.revision, Some(2));
    drop(moved);
    f.domain.shutdown().await;
    Ok(())
}

pub(crate) async fn stale_cache_refill() -> Result<()> {
    let (f, class, dispatcher, routes) = setup(1, 2, 16 * 1024).await?;
    let selected = cell(&routes[0], 0);
    let view = f.run(f.directory.observe()).await?;
    let (result, reads) =
        fault::count_reads(f.run(f.directory.lookup(&view, &selected.cell))).await;
    result?;
    let page = reads
        .keys()
        .find(|path| path.contains("/ranges/"))
        .context("range page")?
        .clone();
    let gate = Arc::new(fault::RegistryGate::default());
    gate.armed.store(true, Ordering::Release);
    let wait_gate = gate.clone();
    let mut old = query(&dispatcher, &class, selected.clone())?;
    let waiter = f
        .domain
        .spawn(async move { fault::gate_read(page, wait_gate, old.begin()).await })?;
    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified()).await?;
    let successor = publish_route_split(&f, &routes[0], &selected.cell).await?;
    assert_eq!(dispatcher.refresh_routes().await?, 2);
    gate.resume.notify_one();
    let error = waiter.await?.err().context("old lookup must refuse")?;
    assert!(matches!(
        error,
        CellDispatchError::Routing(CellRoutingError {
            reason: CellRoutingFailure::Catalog(CatalogError::Conflict(
                "superseded routing observation"
            )),
            ..
        })
    ));
    assert_eq!(dispatcher.route_cache_usage()?.entries, 0);
    let mut fresh = query(&dispatcher, &class, selected)?;
    fresh.begin().await?;
    assert_eq!(fresh.attempt().context("current")?.route(), &successor);
    drop(fresh);
    assert_eq!(dispatcher.usage(), (0, 0));
    f.domain.shutdown().await;
    Ok(())
}
