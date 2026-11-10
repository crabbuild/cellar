// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Actual catalog publications, bounded proof reads and delayed cache repair.
use super::*;

async fn warm(
    dispatcher: &CellGroupDispatcher,
    class: &RaftClass,
    routes: &[GroupRoute],
) -> Result<()> {
    for (index, route) in routes.iter().enumerate() {
        let mut invocation = query(dispatcher, class, cell(route, index))?;
        invocation.begin().await?;
    }
    assert_eq!(dispatcher.route_cache_usage()?.entries, routes.len());
    assert_eq!(dispatcher.usage(), (0, 0));
    Ok(())
}

async fn replace(f: &Fixture, parent: &GroupRoute, id: u64) -> Result<(Vec<GroupRoute>, [u8; 32])> {
    let view = f.run(f.directory.observe()).await?;
    let mut children = split(parent, id);
    for child in &mut children {
        child.schema_digest = parent.schema_digest;
    }
    f.reserve(&mut children).await?;
    let mutation = f.directory.prepare_replacement(
        &view,
        operation(id + 100_000),
        std::slice::from_ref(parent),
        &children,
    )?;
    f.run(f.directory.publish(&mutation)).await?;
    Ok((children, mutation.digest()))
}

pub(crate) async fn repair_deadline_keeps_the_observed_publication() -> Result<()> {
    let (f, class, dispatcher, routes) = setup(3, 8, 64 * 1024).await?;
    warm(&dispatcher, &class, &routes[1..]).await?;
    let selected = cell(&routes[0], 0);
    let before = f.run(f.directory.observe()).await?;
    let (result, reads) =
        fault::count_reads(f.run(f.directory.lookup(&before, &selected.cell))).await;
    result?;
    let page = reads
        .keys()
        .find(|path| path.contains("/ranges/"))
        .context("old range path")?
        .clone();
    let old_gate = Arc::new(fault::RegistryGate::default());
    old_gate.armed.store(true, Ordering::Release);
    let wait_gate = old_gate.clone();
    let mut invocation = query(&dispatcher, &class, selected.clone())?;
    let old = f
        .domain
        .spawn(async move { fault::gate_read(page, wait_gate, invocation.begin()).await })?;
    f.run(cellar::asyncrt::timeout(
        Duration::from_secs(5),
        old_gate.entered.notified(),
    ))
    .await?;
    let old_charge = dispatcher.usage();
    let (children, digest) = replace(&f, &routes[0], 8400).await?;
    let proof_gate = Arc::new(fault::RegistryGate::default());
    proof_gate.armed.store(true, Ordering::Release);
    let path = f
        .key(&format!("changes/{}", hex::encode(digest)))
        .to_string();
    let result = f
        .run(cellar::asyncrt::timeout(
            Duration::from_millis(200),
            fault::gate_read(path, proof_gate.clone(), dispatcher.refresh_routes()),
        ))
        .await;
    assert!(result.is_err());
    assert!(
        !proof_gate.armed.load(Ordering::Acquire),
        "repair must have entered the held read"
    );
    assert_eq!(dispatcher.route_cache_usage()?.revision, Some(2));
    assert_eq!(dispatcher.route_cache_usage()?.entries, 0);
    assert_eq!(dispatcher.usage(), old_charge);
    old_gate.resume.notify_one();
    let error = old
        .await?
        .err()
        .context("old lookup must not refill after cancelled repair")?;
    assert!(matches!(
        error,
        CellDispatchError::Routing(CellRoutingError {
            reason: CellRoutingFailure::Catalog(CatalogError::Conflict(
                "superseded routing observation"
            )),
            ..
        })
    ));
    assert_eq!(dispatcher.usage(), (0, 0));
    assert_eq!(dispatcher.route_cache_usage()?.entries, 0);
    let key = cell_routing_key(selected.namespace, &selected.cell);
    let expected = children
        .iter()
        .find(|route| route.range.contains(&key))
        .context("selected replacement")?;
    let mut fresh = query(&dispatcher, &class, selected)?;
    fresh.begin().await?;
    assert_eq!(fresh.attempt().context("fresh lookup")?.route(), expected);
    drop(fresh);
    f.domain.shutdown().await;
    Ok(())
}

pub(crate) async fn changed_interval_preserves_unaffected_routes() -> Result<()> {
    let (f, class, dispatcher, routes) = setup(16, 16, 128 * 1024).await?;
    warm(&dispatcher, &class, &routes).await?;
    let changed = 7;
    let (children, _) = replace(&f, &routes[changed], 8000).await?;
    assert_eq!(dispatcher.refresh_routes().await?, 2);
    assert_eq!(dispatcher.route_cache_usage()?.entries, 15);
    for (index, route) in routes.iter().enumerate().filter(|(i, _)| *i != changed) {
        let mut invocation = query(&dispatcher, &class, cell(route, index))?;
        let (result, reads) = fault::count_reads(invocation.begin()).await;
        result?;
        assert!(reads.is_empty(), "unaffected interval {index}: {reads:?}");
        assert_eq!(invocation.attempt().context("unaffected")?.route(), route);
    }
    let selected = cell(&routes[changed], changed);
    let key = cell_routing_key(selected.namespace, &selected.cell);
    let expected = children
        .iter()
        .find(|route| route.range.contains(&key))
        .context("selected child")?;
    let mut invocation = query(&dispatcher, &class, selected.clone())?;
    let (result, reads) = fault::count_reads(invocation.begin()).await;
    result?;
    assert!(!reads.is_empty());
    let attempt = invocation.attempt().context("replacement")?;
    assert_eq!(attempt.route(), expected);
    assert_eq!(attempt.request().cell, selected);
    assert_eq!(attempt.route().epoch.get(), routes[changed].epoch.get() + 1);
    assert_eq!(dispatcher.route_cache_usage()?.reserved_bytes, 128 * 1024);
    drop(invocation);
    assert_eq!(dispatcher.usage(), (0, 0));
    assert_eq!(f.store.listing_calls.load(Ordering::Acquire), 0);
    f.domain.shutdown().await;
    Ok(())
}

pub(crate) async fn retained_revision_honors_invocation_floor() -> Result<()> {
    let (f, class, dispatcher, routes) = setup(3, 8, 64 * 1024).await?;
    warm(&dispatcher, &class, &routes).await?;
    replace(&f, &routes[0], 8100).await?;
    assert_eq!(dispatcher.refresh_routes().await?, 2);
    let unchanged = cell(&routes[2], 2);
    let mut pinned = f.directory.routing_session(
        &f.domain,
        CellRouteRequest {
            cell: unchanged.clone(),
            operation: CellRouteOperation::Query(operation(8110)),
            fingerprint: [89; 32],
        },
        routing_limits(4),
        None,
    )?;
    let observed_at_two = pinned.begin().await?.cached_route();
    replace(&f, &routes[1], 8120).await?;
    assert_eq!(dispatcher.refresh_routes().await?, 3);
    assert_eq!(dispatcher.route_cache_usage()?.entries, 1);
    let mut old_floor = query(&dispatcher, &class, unchanged.clone())?;
    let (result, reads) = fault::count_reads(old_floor.begin()).await;
    result?;
    assert!(reads.is_empty());
    drop(old_floor);
    // The retained row was verified at revision 1, not promoted to 3. An
    // invocation carrying a revision-2 floor therefore needs a fresh proof.
    let mut newer_floor = dispatcher.query(
        unchanged,
        CellQueryRequest {
            invocation: operation(8111),
            contract: class.contract("Get").context("Get")?,
            input: vec![],
            consistency: CellQueryConsistency::Stale,
        },
        routing_limits(4),
        Some(observed_at_two),
    )?;
    let (result, reads) = fault::count_reads(newer_floor.begin()).await;
    result?;
    assert!(!reads.is_empty());
    assert_eq!(
        newer_floor.attempt().context("current proof")?.route(),
        &routes[2]
    );
    drop(newer_floor);
    assert_eq!(dispatcher.usage(), (0, 0));
    f.domain.shutdown().await;
    Ok(())
}

pub(crate) async fn repair_failure_invalidates_all() -> Result<()> {
    // Missing proof, proof work beyond the bounded history window, and parent
    // admission exhaustion all choose full invalidation without larger banks.
    for reason in 0..4 {
        let (f, class, dispatcher, routes) = setup(6, 8, 64 * 1024).await?;
        warm(&dispatcher, &class, &routes).await?;
        let mut pending = Vec::new();
        if reason == 2 {
            for _ in 0..8 {
                pending.push(query(&dispatcher, &class, cell(&routes[5], 5))?);
            }
            assert_eq!(dispatcher.usage().1, 8);
        }
        let publications = if reason == 1 { 5 } else { 1 };
        for (index, route) in routes[..publications].iter().enumerate() {
            let (_, digest) = replace(&f, route, 8200 + index as u64 * 10).await?;
            if reason == 0 {
                f.store
                    .inner
                    .delete(&f.key(&format!("changes/{}", hex::encode(digest))))
                    .await?;
            } else if reason == 3 {
                let path = f.key(&format!("changes/{}", hex::encode(digest)));
                let mut bytes = f.store.inner.get(&path).await?.bytes().await?.to_vec();
                bytes[0] ^= 1;
                f.store.inner.put(&path, bytes.into()).await?;
            }
        }
        let before = dispatcher.usage();
        let (result, reads) = fault::count_reads(dispatcher.refresh_routes()).await;
        assert_eq!(result?, publications as u64 + 1);
        assert_eq!(dispatcher.route_cache_usage()?.entries, 0);
        assert_eq!(dispatcher.usage(), before);
        if matches!(reason, 1 | 2) {
            assert!(reads.keys().all(|key| !key.contains("/changes/")));
        }
        drop(pending);
        let mut invocation = query(&dispatcher, &class, cell(&routes[5], 5))?;
        let (result, reads) = fault::count_reads(invocation.begin()).await;
        result?;
        assert!(!reads.is_empty());
        assert_eq!(
            invocation.attempt().context("targeted refill")?.route(),
            &routes[5]
        );
        drop(invocation);
        assert_eq!(dispatcher.usage(), (0, 0));
        assert_eq!(f.store.listing_calls.load(Ordering::Acquire), 0);
        f.domain.shutdown().await;
    }
    Ok(())
}

pub(crate) async fn old_proof_cannot_cross_a_fence() -> Result<()> {
    let (f, class, dispatcher, routes) = setup(3, 8, 64 * 1024).await?;
    warm(&dispatcher, &class, &routes).await?;
    let (_, digest) = replace(&f, &routes[0], 8300).await?;
    let gate = Arc::new(fault::RegistryGate::default());
    gate.armed.store(true, Ordering::Release);
    let pending = dispatcher.clone();
    let wait_gate = gate.clone();
    let path = f
        .key(&format!("changes/{}", hex::encode(digest)))
        .to_string();
    let waiter = f
        .domain
        .spawn(async move { fault::gate_read(path, wait_gate, pending.refresh_routes()).await })?;
    f.run(cellar::asyncrt::timeout(
        Duration::from_secs(5),
        gate.entered.notified(),
    ))
    .await?;
    let held = dispatcher.usage();
    assert_eq!(held, (512 * 1024, 1));
    let progress = query(&dispatcher, &class, cell(&routes[2], 2))?;
    assert_eq!(dispatcher.usage().1, 2);
    drop(progress);
    assert_eq!(dispatcher.usage(), held);
    f.run(f.directory.freeze_group_births(operation(8310)))
        .await?;
    assert_eq!(dispatcher.refresh_routes().await?, 3);
    assert_eq!(dispatcher.route_cache_usage()?.entries, 0);
    assert_eq!(dispatcher.usage(), held);
    gate.resume.notify_one();
    let error = waiter
        .await?
        .err()
        .context("superseded proof must refuse")?;
    assert!(matches!(
        error,
        CellDispatchError::Routing(CellRoutingError {
            reason: CellRoutingFailure::Catalog(CatalogError::Conflict(
                "superseded routing observation"
            )),
            ..
        })
    ));
    assert_eq!(dispatcher.route_cache_usage()?.revision, Some(3));
    assert_eq!(dispatcher.route_cache_usage()?.entries, 0);
    assert_eq!(dispatcher.usage(), (0, 0));
    f.domain.shutdown().await;
    Ok(())
}
