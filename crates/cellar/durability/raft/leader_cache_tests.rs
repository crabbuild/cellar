// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Advice races, role/identity isolation and the actual parent-bank reservation.
use super::*;
use cellar_logic::consensus::{
    ClusterId, DiskLineage, GroupId, IncarnationId, NamespaceId, ProcessGeneration,
};

fn route(id: u8) -> Arc<GroupRoute<GroupHistory>> {
    let history = GroupHistory {
        cluster: ClusterId::new([1; 16]).unwrap(),
        namespace: NamespaceId::new([2; 16]).unwrap(),
        group: GroupId::new([id; 16]).unwrap(),
        incarnation: IncarnationId::new([4; 16]).unwrap(),
    };
    Arc::new(GroupRoute {
        scope: history,
        schema_digest: [8; 32],
        revision: 20,
        peers: (1..=3)
            .map(|id| RaftPeer {
                replica: ReplicaIdentity {
                    scope: history,
                    member: MemberId::new(id).unwrap(),
                    node: NodeId::new([id as u8; 16]).unwrap(),
                    disk: DiskLineage::new([10 + id as u8; 16]).unwrap(),
                    process: ProcessGeneration::new([20 + id as u8; 16]).unwrap(),
                },
                label: format!("node-{id}"),
                server_name: "localhost".into(),
                control_address: format!("127.0.0.1:{}", 30000 + id * 3).parse().unwrap(),
                data_address: format!("127.0.0.1:{}", 30001 + id * 3).parse().unwrap(),
                query_address: format!("127.0.0.1:{}", 30002 + id * 3).parse().unwrap(),
            })
            .collect(),
    })
}

fn configuration() -> Configuration {
    Configuration {
        revision: 3,
        digest: [9; 32],
        incoming: vec![MemberId::new(1).unwrap(), MemberId::new(2).unwrap()],
    }
}

fn advice(route: &GroupRoute<GroupHistory>, member: u64, term: u64) -> LeaderAdvice {
    LeaderAdvice {
        history: route.scope,
        membership_revision: 3,
        configuration_digest: [9; 32],
        term,
        leader: MemberId::new(member).unwrap(),
    }
}

fn setup() -> (Cache, Arc<Budget>, Arc<GroupRoute<GroupHistory>>, Weak<()>) {
    let options = TransportOptions {
        max_groups: 1,
        max_peers: 4,
        queue_items: 1,
        ..Default::default()
    };
    let memory = Budget::new(
        options.node_control_bytes,
        options.queue_items * options.max_peers,
    );
    let mut cache = Cache::new(&options);
    let route = route(3);
    cache.install(&route, configuration(), &memory, 0).unwrap();
    let generation = cache.generation(&route).unwrap();
    (cache, memory, route, generation)
}

#[test]
fn lower_and_conflicting_terms_never_replace_newer_usable_advice() {
    let (mut cache, _, route, generation) = setup();
    cache.remember(&route, &generation, advice(&route, 1, 5), 10, 11, 100);
    assert_eq!(
        cache.lookup(&route, 12, 100),
        LeaderLookup::Known(MemberId::new(1).unwrap())
    );
    cache.remember(&route, &generation, advice(&route, 2, 4), 12, 13, 100);
    assert_eq!(
        cache.lookup(&route, 14, 100),
        LeaderLookup::Known(MemberId::new(1).unwrap())
    );
    cache.remember(&route, &generation, advice(&route, 2, 5), 14, 15, 100);
    assert_eq!(cache.lookup(&route, 16, 100), LeaderLookup::Unknown);
    cache.remember(&route, &generation, advice(&route, 1, 5), 16, 17, 100);
    assert_eq!(cache.lookup(&route, 18, 100), LeaderLookup::Unknown);
    cache.remember(&route, &generation, advice(&route, 2, 6), 18, 19, 100);
    assert_eq!(
        cache.lookup(&route, 20, 100),
        LeaderLookup::Known(MemberId::new(2).unwrap())
    );
}

#[test]
fn expiry_uses_attempt_start_and_metadata_reobservation_cannot_renew_it() {
    let (mut cache, memory, route, generation) = setup();
    cache.remember(&route, &generation, advice(&route, 1, 5), 10, 11, 100);
    cache
        .install(&route, configuration(), &memory, 109)
        .unwrap();
    assert_eq!(
        cache.lookup(&route, 109, 100),
        LeaderLookup::Known(MemberId::new(1).unwrap())
    );
    assert_eq!(cache.lookup(&route, 110, 100), LeaderLookup::Unknown);
    cache.remember(&route, &generation, advice(&route, 2, 4), 109, 110, 100);
    assert_eq!(cache.lookup(&route, 111, 100), LeaderLookup::Unknown);
    cache.remember(&route, &generation, advice(&route, 1, 6), 10, 200, 100);
    assert_eq!(cache.lookup(&route, 200, 100), LeaderLookup::Unknown);
    cache.remember(&route, &generation, advice(&route, 1, 6), 201, 202, 100);
    cache.remember(&route, &generation, advice(&route, 1, 6), 199, 203, 100);
    assert_eq!(
        cache.lookup(&route, 300, 100),
        LeaderLookup::Known(MemberId::new(1).unwrap())
    );
    assert_eq!(cache.lookup(&route, 301, 100), LeaderLookup::Unknown);
}

#[test]
fn advice_cannot_invent_history_configuration_or_incoming_voter_roles() {
    for kind in 0..5 {
        let (mut cache, memory, route, generation) = setup();
        let mut invalid = advice(&route, 1, 5);
        match kind {
            0 => invalid.history.incarnation = IncarnationId::new([17; 16]).unwrap(),
            1 => invalid.membership_revision = 4,
            2 => invalid.configuration_digest = [12; 32],
            3 => invalid.leader = MemberId::new(3).unwrap(),
            _ => invalid.leader = MemberId::new(4).unwrap(),
        }
        cache.remember(&route, &generation, invalid, 10, 11, 100);
        assert!(!matches!(
            cache.lookup(&route, 12, 100),
            LeaderLookup::Known(_)
        ));
        cache.install(&route, configuration(), &memory, 13).unwrap();
        let fresh = cache.generation(&route).unwrap();
        cache.remember(&route, &fresh, advice(&route, 1, 5), 14, 15, 100);
        assert_eq!(
            cache.lookup(&route, 16, 100),
            LeaderLookup::Known(MemberId::new(1).unwrap())
        );
    }
}

#[test]
fn eviction_retains_no_owner_and_old_inflight_advice_cannot_reenter_new_generation() {
    let (mut cache, memory, route, generation) = setup();
    cache.remember(&route, &generation, advice(&route, 1, 10), 10, 11, 100);
    let other = self::route(5);
    cache.install(&other, configuration(), &memory, 12).unwrap();
    assert_eq!(memory.usage(), (ENTRY_BYTES, 1));
    assert_eq!(Arc::strong_count(&route), 1);
    cache.install(&route, configuration(), &memory, 13).unwrap();
    cache.remember(&route, &generation, advice(&route, 2, 9), 10, 14, 100);
    assert_eq!(cache.lookup(&route, 15, 100), LeaderLookup::Unknown);
    let fresh = cache.generation(&route).unwrap();
    cache.remember(&route, &fresh, advice(&route, 1, 11), 16, 17, 100);
    cache.suspect(&route, &generation, MemberId::new(1).unwrap());
    assert_eq!(
        cache.lookup(&route, 18, 100),
        LeaderLookup::Known(MemberId::new(1).unwrap())
    );
    cache.clear();
    assert_eq!(memory.usage(), (0, 0));
}

#[test]
fn endpoint_replacement_and_revocation_keep_configuration_but_fence_late_advice() {
    let (mut cache, _, route, generation) = setup();
    cache.remember(&route, &generation, advice(&route, 1, 10), 10, 11, 100);
    let mut routes = BTreeMap::from([(route.scope, route.clone())]);
    cache.refresh_endpoint(&routes, route.peers[0].replica.node);
    assert_eq!(
        cache.lookup(&route, 12, 100),
        LeaderLookup::Known(MemberId::new(1).unwrap())
    );
    let mut next = (*route).clone();
    next.peers[0].replica.process = ProcessGeneration::new([77; 16]).unwrap();
    let next = Arc::new(next);
    routes.insert(next.scope, next.clone());
    cache.refresh_endpoint(&routes, next.peers[0].replica.node);
    cache.remember(&next, &generation, advice(&next, 1, 10), 13, 14, 100);
    assert_eq!(cache.lookup(&next, 15, 100), LeaderLookup::Unknown);
    let fresh = cache.generation(&next).unwrap();
    cache.remember(&next, &fresh, advice(&next, 1, 10), 16, 17, 100);
    cache.fence_peers(|peer| peer.node != next.peers[0].replica.node);
    cache.remember(&next, &fresh, advice(&next, 1, 11), 18, 19, 100);
    assert_eq!(cache.lookup(&next, 20, 100), LeaderLookup::Unknown);
}

#[test]
fn metadata_charge_plateaus_and_preserves_parent_progress_capacity() {
    let (mut cache, memory, _, _) = setup();
    for id in 10..100 {
        cache
            .install(&route(id), configuration(), &memory, id as u64)
            .unwrap();
    }
    assert_eq!(cache.entries.len(), cache.capacity);
    assert_eq!(memory.usage(), (ENTRY_BYTES, 1));
    let progress: Vec<_> = (0..3).map(|_| memory.acquire(1).unwrap()).collect();
    assert!(memory.acquire(1).is_none());
    drop(progress);
    cache.clear();
    assert_eq!(memory.usage(), (0, 0));
}

#[test]
fn route_reinstallation_preserves_configuration_term_floor_and_conflicts() {
    let (mut cache, memory, route, generation) = setup();
    cache.remember(&route, &generation, advice(&route, 1, 10), 10, 11, 100);
    cache.remember(&route, &generation, advice(&route, 2, 10), 12, 13, 100);
    let mut next = (*route).clone();
    next.revision += 1;
    let next = Arc::new(next);
    cache.install(&next, configuration(), &memory, 14).unwrap();
    assert_eq!(memory.usage(), (ENTRY_BYTES, 1));
    let fresh = cache.generation(&next).unwrap();
    // The replacement generation fences both a delayed higher-term reply and
    // a new lower-term or conflicting reply within the unchanged configuration.
    cache.remember(&next, &generation, advice(&next, 1, 11), 14, 15, 100);
    cache.remember(&next, &fresh, advice(&next, 1, 9), 16, 17, 100);
    cache.remember(&next, &fresh, advice(&next, 1, 10), 18, 19, 100);
    assert_eq!(cache.lookup(&next, 20, 100), LeaderLookup::Unknown);
    cache.remember(&next, &fresh, advice(&next, 2, 11), 21, 22, 100);
    assert_eq!(
        cache.lookup(&next, 23, 100),
        LeaderLookup::Known(MemberId::new(2).unwrap())
    );
}

#[test]
fn verified_configuration_cannot_regress_or_fork_at_the_same_revision() {
    let (mut cache, memory, route, _) = setup();
    let mut old = configuration();
    old.revision = 2;
    assert_eq!(
        cache.install(&route, old, &memory, 1),
        Err(TransportError::StalePeer)
    );
    let mut fork = configuration();
    fork.digest = [11; 32];
    assert_eq!(
        cache.install(&route, fork, &memory, 2),
        Err(TransportError::Identity)
    );
    assert_eq!(memory.usage(), (ENTRY_BYTES, 1));
}
