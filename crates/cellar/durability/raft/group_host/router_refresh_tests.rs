// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Discovery ownership, exact generations and the unchanged parent admission.
#![allow(clippy::disallowed_methods)]
use super::*;
use crate::asyncrt::ExecutionDomain;
use cellar_logic::consensus::{ClusterId, GroupId, IncarnationId, NamespaceId};
use futures_util::FutureExt;

struct Fixture {
    domain: ExecutionDomain,
    client: RouterRefreshClient,
    requests: mpsc::Receiver<RouterRefreshRequest>,
    memory: Arc<Budget>,
}

impl Fixture {
    async fn new(bytes: usize, items: usize) -> anyhow::Result<Self> {
        let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
        let token = domain
            .run(async { crate::asyncrt::services().domain_token().unwrap() })
            .await?;
        let performance = domain.run(async { Registry::new(token.clone()) }).await?;
        let memory = Budget::new(bytes, items);
        let (client, requests) = RouterRefreshClient::channel(memory.clone(), token, performance)?;
        Ok(Self {
            domain,
            client,
            requests,
            memory,
        })
    }

    fn deadline(&self) -> u64 {
        self.client.domain.mono_ms().unwrap() + 5000
    }

    fn owner(&mut self) -> RouterRefreshOwner {
        match self.requests.try_recv().unwrap() {
            RouterRefreshRequest::Observe { owner } => owner,
            RouterRefreshRequest::Track { .. } => panic!("expected owned observation"),
        }
    }
}

fn history(id: u8) -> GroupHistory {
    GroupHistory {
        cluster: ClusterId::new([1; 16]).unwrap(),
        namespace: NamespaceId::new([2; 16]).unwrap(),
        group: GroupId::new([id; 16]).unwrap(),
        incarnation: IncarnationId::new([4; 16]).unwrap(),
    }
}

fn cold(id: u8) -> RouterRefreshKey {
    RouterRefreshKey::new(history(id), None, None)
}

#[tokio::test]
async fn concurrent_waiters_share_one_owner_and_cancellation_retains_work() -> anyhow::Result<()> {
    let mut fixture = Fixture::new(128 * 1024, 8).await?;
    let client = fixture.client.clone();
    let deadline = fixture.deadline();
    let mut first = Box::pin(client.request(cold(3), deadline));
    assert!(first.as_mut().now_or_never().is_none());
    let owner = fixture.owner();
    let mut second = Box::pin(client.request(cold(3), deadline + 1000));
    assert!(second.as_mut().now_or_never().is_none());
    assert!(fixture.requests.try_recv().is_err());
    assert_eq!(owner.deadline(), deadline);
    assert_eq!(fixture.memory.usage(), (OWNER_BYTES + WAITER_BYTES, 2));
    drop(first);
    assert_eq!(fixture.memory.usage(), (OWNER_BYTES + WAITER_BYTES, 2));
    owner.complete(Ok(RouterRefreshOutcome::StalePlacement));
    // An undelivered response still owns its shared result reservation.
    assert_eq!(fixture.memory.usage(), (OWNER_BYTES + WAITER_BYTES, 2));
    assert_eq!(second.await, Ok(RouterRefreshOutcome::StalePlacement));
    assert_eq!(fixture.memory.usage(), (0, 0));
    assert!(client.shared.pending.lock().unwrap().is_empty());
    let snapshot = client.performance.snapshot();
    assert_eq!(
        snapshot
            .counters
            .iter()
            .find(|c| c.name == "cell_membership_discovery_owners")
            .unwrap()
            .value,
        1
    );
    assert_eq!(
        snapshot
            .counters
            .iter()
            .find(|c| c.name == "cell_membership_refresh_joins")
            .unwrap()
            .value,
        1
    );
    fixture.domain.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn waiter_deadlines_neither_cancel_nor_extend_the_original_owner() -> anyhow::Result<()> {
    let mut fixture = Fixture::new(128 * 1024, 8).await?;
    let client = fixture.client.clone();
    let deadline = fixture.deadline();
    let mut first = Box::pin(client.request(cold(3), deadline));
    assert!(first.as_mut().now_or_never().is_none());
    let owner = fixture.owner();
    let short = client.domain.mono_ms()? + 30;
    assert_eq!(
        client.request(cold(3), short).await,
        Err(RuntimeError::DeadlineExceeded)
    );
    assert_eq!(fixture.memory.usage(), (OWNER_BYTES, 1));
    let mut longer = Box::pin(client.request(cold(3), deadline + 5000));
    assert!(longer.as_mut().now_or_never().is_none());
    assert_eq!(owner.deadline(), deadline);
    owner.complete(Err(RuntimeError::DeadlineExceeded));
    assert_eq!(longer.await, Err(RuntimeError::DeadlineExceeded));
    assert_eq!(first.await, Err(RuntimeError::DeadlineExceeded));
    assert_eq!(fixture.memory.usage(), (0, 0));
    fixture.domain.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn parent_byte_and_item_limits_bound_all_waiters_without_new_banks() -> anyhow::Result<()> {
    for (bytes, items) in [(OWNER_BYTES + WAITER_BYTES, 8), (128 * 1024, 2)] {
        let mut fixture = Fixture::new(bytes, items).await?;
        let client = fixture.client.clone();
        let deadline = fixture.deadline();
        let mut first = Box::pin(client.request(cold(3), deadline));
        assert!(first.as_mut().now_or_never().is_none());
        let owner = fixture.owner();
        let mut second = Box::pin(client.request(cold(3), deadline));
        assert!(second.as_mut().now_or_never().is_none());
        let charged = fixture.memory.usage();
        for _ in 0..16 {
            assert_eq!(
                client.request(cold(3), deadline).await,
                Err(RuntimeError::Overloaded)
            );
            assert_eq!(fixture.memory.usage(), charged);
        }
        assert_eq!(fixture.memory.capacity(), (bytes, items));
        drop(first);
        drop(second);
        assert_eq!(fixture.memory.usage(), (OWNER_BYTES, 1));
        owner.complete(Ok(RouterRefreshOutcome::Current));
        assert_eq!(fixture.memory.usage(), (0, 0));
        fixture.domain.shutdown().await;
    }
    // The smallest supported managed control bank still accommodates the
    // permanent directory lease, SDK admission and one physical discovery.
    let mut fixture = Fixture::new(4 * 1024 * 1024 + OWNER_BYTES + 4096, 3).await?;
    let directory = fixture.memory.acquire(4 * 1024 * 1024).unwrap();
    let sdk = fixture.memory.acquire(4096).unwrap();
    let client = fixture.client.clone();
    let mut first = Box::pin(client.request(cold(3), fixture.deadline()));
    assert!(first.as_mut().now_or_never().is_none());
    fixture.owner().complete(Ok(RouterRefreshOutcome::Current));
    assert_eq!(first.await, Ok(RouterRefreshOutcome::Current));
    drop(sdk);
    drop(directory);
    assert_eq!(fixture.memory.usage(), (0, 0));
    fixture.domain.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn route_configuration_and_history_changes_select_different_owners() -> anyhow::Result<()> {
    let mut fixture = Fixture::new(256 * 1024, 12).await?;
    let client = fixture.client.clone();
    let route = Arc::new(GroupRoute {
        scope: history(3),
        schema_digest: [8; 32],
        revision: 1,
        peers: vec![],
    });
    let replacement = Arc::new((*route).clone());
    let configuration = Arc::new(());
    let changed = Arc::new(());
    let keys = [
        RouterRefreshKey::new(
            history(3),
            Some(&route),
            Some(Arc::downgrade(&configuration)),
        ),
        RouterRefreshKey::new(
            history(3),
            Some(&replacement),
            Some(Arc::downgrade(&configuration)),
        ),
        RouterRefreshKey::new(history(3), Some(&route), Some(Arc::downgrade(&changed))),
        cold(7),
    ];
    let mut waiters = Vec::new();
    let mut owners = Vec::new();
    for key in keys {
        let mut waiter = Box::pin(client.request(key, fixture.deadline()));
        assert!(waiter.as_mut().now_or_never().is_none());
        owners.push(fixture.owner());
        waiters.push(waiter);
    }
    assert_eq!(Arc::strong_count(&route), 1);
    assert_eq!(Arc::strong_count(&configuration), 1);
    drop(route);
    drop(replacement);
    drop(configuration);
    drop(changed);
    assert_eq!(fixture.memory.usage(), (4 * OWNER_BYTES, 4));
    for owner in owners {
        owner.complete(Ok(RouterRefreshOutcome::Current));
    }
    for waiter in waiters {
        assert_eq!(waiter.await, Ok(RouterRefreshOutcome::Current));
    }
    assert_eq!(fixture.memory.usage(), (0, 0));
    fixture.domain.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn completion_retires_key_while_old_delivery_stays_charged() -> anyhow::Result<()> {
    let mut fixture = Fixture::new(128 * 1024, 8).await?;
    let client = fixture.client.clone();
    let mut old = Box::pin(client.request(cold(3), fixture.deadline()));
    assert!(old.as_mut().now_or_never().is_none());
    fixture.owner().complete(Err(RuntimeError::NotReady));
    let mut new = Box::pin(client.request(cold(3), fixture.deadline()));
    assert!(new.as_mut().now_or_never().is_none());
    let owner = fixture.owner();
    assert_eq!(fixture.memory.usage(), (2 * OWNER_BYTES, 2));
    assert_eq!(old.await, Err(RuntimeError::NotReady));
    owner.complete(Ok(RouterRefreshOutcome::Current));
    assert_eq!(new.await, Ok(RouterRefreshOutcome::Current));
    assert_eq!(fixture.memory.usage(), (0, 0));
    fixture.domain.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn closed_queue_wakes_joined_waiters_and_releases_actual_owners() -> anyhow::Result<()> {
    let fixture = Fixture::new(128 * 1024, 8).await?;
    let client = fixture.client.clone();
    let deadline = fixture.deadline();
    let mut first = Box::pin(client.request(cold(3), deadline));
    assert!(first.as_mut().now_or_never().is_none());
    let mut second = Box::pin(client.request(cold(3), deadline));
    assert!(second.as_mut().now_or_never().is_none());
    drop(fixture.requests);
    assert_eq!(first.await, Err(RuntimeError::Closed));
    assert_eq!(second.await, Err(RuntimeError::Closed));
    assert_eq!(fixture.memory.usage(), (0, 0));
    assert!(client.shared.pending.lock().unwrap().is_empty());
    assert_eq!(
        client.request(cold(3), deadline).await,
        Err(RuntimeError::Closed)
    );
    assert_eq!(fixture.memory.usage(), (0, 0));
    assert!(client.shared.pending.lock().unwrap().is_empty());
    fixture.domain.shutdown().await;
    Ok(())
}

fn endpoint(process: u8, revision: u64) -> RouterRefreshKey {
    RouterRefreshKey::endpoint(
        history(3).cluster,
        NodeIdentity {
            node: NodeId::new([5; 16]).unwrap(),
            disk: cellar_logic::consensus::DiskLineage::new([6; 16]).unwrap(),
            process: cellar_logic::consensus::ProcessGeneration::new([process; 16]).unwrap(),
        },
        Some((EngineInstanceId::new([7; 16]).unwrap(), revision)),
    )
}

#[tokio::test]
async fn endpoint_waiters_share_the_node_owner_without_joining_membership() -> anyhow::Result<()> {
    let mut fixture = Fixture::new(256 * 1024, 8).await?;
    let client = fixture.client.clone();
    let deadline = fixture.deadline();
    let mut first = Box::pin(client.request(endpoint(8, 9), deadline));
    assert!(first.as_mut().now_or_never().is_none());
    let physical = fixture.owner();
    assert_eq!(
        physical.target(),
        RouterRefreshTarget::Endpoint {
            cluster: history(3).cluster,
            node: NodeId::new([5; 16])?,
        }
    );
    let mut second = Box::pin(client.request(endpoint(8, 9), deadline + 1000));
    assert!(second.as_mut().now_or_never().is_none());
    assert!(fixture.requests.try_recv().is_err());
    assert_eq!(physical.deadline(), deadline);
    let mut membership = Box::pin(client.request(cold(3), deadline));
    assert!(membership.as_mut().now_or_never().is_none());
    let group = fixture.owner();
    assert_eq!(group.target(), RouterRefreshTarget::Membership(history(3)));
    assert_eq!(fixture.memory.usage(), (2 * OWNER_BYTES + WAITER_BYTES, 3));
    drop(first);
    drop(second);
    assert_eq!(fixture.memory.usage(), (2 * OWNER_BYTES, 2));
    physical.complete(Ok(RouterRefreshOutcome::Current));
    assert_eq!(fixture.memory.usage(), (OWNER_BYTES, 1));
    group.complete(Ok(RouterRefreshOutcome::Current));
    assert_eq!(membership.await, Ok(RouterRefreshOutcome::Current));
    assert_eq!(fixture.memory.usage(), (0, 0));
    let counters = client.performance.snapshot().counters;
    for (name, expected) in [
        ("cell_endpoint_discovery_owners", 1),
        ("cell_endpoint_refresh_joins", 1),
        ("cell_membership_discovery_owners", 1),
        ("cell_membership_refresh_joins", 0),
    ] {
        assert_eq!(
            counters.iter().find(|c| c.name == name).unwrap().value,
            expected
        );
    }
    fixture.domain.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn endpoint_boot_and_view_changes_fence_pending_metadata_generations() -> anyhow::Result<()> {
    let mut fixture = Fixture::new(256 * 1024, 8).await?;
    let client = fixture.client.clone();
    let deadline = fixture.deadline();
    let mut old = Box::pin(client.request(endpoint(8, 9), deadline));
    assert!(old.as_mut().now_or_never().is_none());
    let old_owner = fixture.owner();
    let mut boot = Box::pin(client.request(endpoint(10, 9), deadline));
    assert!(boot.as_mut().now_or_never().is_none());
    let boot_owner = fixture.owner();
    let mut view = Box::pin(client.request(endpoint(8, 11), deadline));
    assert!(view.as_mut().now_or_never().is_none());
    let view_owner = fixture.owner();
    old_owner.complete(Ok(RouterRefreshOutcome::Current));
    assert_eq!(old.await, Ok(RouterRefreshOutcome::Current));
    assert!(boot.as_mut().now_or_never().is_none());
    assert!(view.as_mut().now_or_never().is_none());
    assert_eq!(fixture.memory.usage(), (2 * OWNER_BYTES, 2));
    boot_owner.complete(Err(RuntimeError::NotReady));
    view_owner.complete(Ok(RouterRefreshOutcome::Current));
    assert_eq!(boot.await, Err(RuntimeError::NotReady));
    assert_eq!(view.await, Ok(RouterRefreshOutcome::Current));
    assert_eq!(fixture.memory.usage(), (0, 0));
    fixture.domain.shutdown().await;
    Ok(())
}
