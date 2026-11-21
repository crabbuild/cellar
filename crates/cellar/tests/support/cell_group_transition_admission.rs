// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Force the real observer to refresh trust while its original admission is live.
use super::*;

pub(super) struct HeldInspection {
    stores: Vec<Arc<fault::Store>>,
    // Calling observe_node eagerly admits its response. Type erasure retains
    // that exact unpolled future, without executing or retrying another read.
    _response: Box<dyn Send>,
}

impl Drop for HeldInspection {
    fn drop(&mut self) {
        for store in &self.stores {
            store.hold_next_fleet_read.store(false, Ordering::Release);
            store.fleet_read_hang.store(false, Ordering::Release);
            store.fleet_read_resume.notify_waiters();
        }
    }
}

pub(super) async fn hold_observer_behind_cut(
    fixtures: &[Fixture],
    hosts: &[RaftCells],
) -> Result<HeldInspection> {
    let fixture = fixtures.first().context("inspection source fixture")?;
    let observer = hosts.first().context("inspection source host")?;
    ensure!(fixtures.len() == hosts.len() && hosts.len() == 6);
    let before = fixture.catalog.read(TIMEOUT).await?;
    for host in hosts {
        let local = host
            .inspect_peer_node(host.identity().node, before.revision(), TIMEOUT)
            .await?;
        ensure!(local
            .view
            .fleet_trust_revision
            .is_some_and(|revision| revision >= before.revision()));
    }
    let stores: Vec<_> = fixtures
        .iter()
        .map(|fixture| {
            fixture
                .host_store
                .as_ref()
                .unwrap_or(&fixture.store)
                .clone()
        })
        .collect();
    ensure!(stores
        .iter()
        .all(|store| !store.fleet_read_hang.load(Ordering::Acquire)
            && !store.hold_next_fleet_read.load(Ordering::Acquire)));
    let initial = observer.resource_usage().control;
    ensure!(initial.max_bytes == 16 * 1024 * 1024 && initial.max_items == 16);
    let held = HeldInspection {
        stores,
        _response: Box::new(observer.observe_node(TIMEOUT)),
    };
    let charged = observer.resource_usage().control;
    ensure!(charged.bytes == initial.bytes + 1024 * 1024 && charged.items == initial.items + 1);
    for store in &held.stores {
        store.fleet_read_hang.store(true, Ordering::Release);
        store.hold_next_fleet_read.store(true, Ordering::Release);
    }
    tokio::time::timeout(TIMEOUT, async {
        while !held
            .stores
            .iter()
            .all(|store| store.active_fleet_reads.load(Ordering::Acquire) == 1)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .context("hold the voters' entered background trust reads")?;

    // The admitted transition and its request identities stay unchanged.
    // A distinct known policy mutation advances only the required trust cut.
    let catalog = Fixture::open_catalog(
        &fixture.domain,
        fixture.store.clone(),
        fixture.declaration.namespace.cluster,
    )?;
    let current = catalog.read(TIMEOUT).await?;
    let request = request(228, current.revision());
    let cut = catalog
        .commit(
            &current,
            &FleetMutation {
                id: request.id,
                operation: request.operation,
                expected_revision: request.expected_revision,
                principal: request.principal,
                changes: vec![FleetChange {
                    table: FleetTable::Policy,
                    key: "seal-inspection-admission-control".into(),
                    value: Some(vec![1]),
                }],
            },
            TIMEOUT,
        )
        .await?
        .snapshot;
    let local = observer.observe_node(TIMEOUT).await?;
    ensure!(local
        .fleet_trust_revision
        .is_some_and(|revision| revision < cut.revision()));
    ensure!(held
        .stores
        .iter()
        .all(|store| store.active_fleet_reads.load(Ordering::Acquire) == 1));
    Ok(held)
}
