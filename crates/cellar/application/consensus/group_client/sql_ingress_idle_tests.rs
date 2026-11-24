// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::raft::budget::Budget;
use futures_util::poll;

fn cell(name: &str) -> CellHistory {
    CellHistory {
        cluster: ClusterId::new([1; 16]).unwrap(),
        namespace: NamespaceId::new([2; 16]).unwrap(),
        cell: CellId::new(name).unwrap(),
        incarnation: cellar_logic::consensus::IncarnationId::new([3; 16]).unwrap(),
    }
}

fn command(sequence: u64) -> CommandId {
    CommandId::new(
        super::super::super::sql_ingress_identity::client(),
        sequence,
    )
    .unwrap()
}

fn enter(lanes: &Arc<Lanes>, budget: &Arc<Budget>, history: &CellHistory) -> Lane {
    lanes.enter(history, budget.acquire(8192).unwrap()).unwrap()
}

#[tokio::test]
async fn idle_receipt_reuses_once_without_an_idle_owner_or_admission() {
    let lanes = Arc::new(Lanes::default());
    let budget = Budget::new(8192, 1);
    let history = cell("__Queue:a");
    let first = enter(&lanes, &budget, &history);
    first
        .lock()
        .await
        .acknowledged(&history, command(5), [4; 32], 100, 10);
    drop(first);
    assert!(lanes.active.lock().unwrap().is_empty());
    assert_eq!(budget.usage(), (0, 0));
    assert!(std::mem::size_of::<super::super::sql_ingress_hints::Hints>() <= 512);
    let later = enter(&lanes, &budget, &history);
    let mut turn = later.lock().await;
    assert_eq!(turn.take_successor(&history, [4; 32], 20), None);
    assert_eq!(turn.take_node_successor(&history, [4; 32], 20), Some(6));
    assert_eq!(turn.take_node_successor(&history, [4; 32], 20), None);
    drop(turn);
    drop(later);
    let unknown = enter(&lanes, &budget, &history);
    assert_eq!(
        unknown
            .lock()
            .await
            .take_node_successor(&history, [4; 32], 21),
        None
    );
    drop(unknown);
    assert_eq!(budget.usage(), (0, 0));
}

#[tokio::test]
async fn eight_idle_targets_reuse_successors_without_retaining_admission() {
    let lanes = Arc::new(Lanes::default());
    let budget = Budget::new(8192, 1);
    assert!(std::mem::size_of::<super::super::sql_ingress_hints::Hints>() <= 512);
    for round in 0..3 {
        for target in 0..8 {
            let history = cell(&format!("__Queue:{target}"));
            let lane = enter(&lanes, &budget, &history);
            let mut turn = lane.lock().await;
            assert_eq!(turn.take_successor(&history, [4; 32], 20), None);
            assert_eq!(
                turn.take_node_successor(&history, [4; 32], 20),
                (round != 0).then_some(5 + round)
            );
            assert_eq!(turn.take_node_successor(&history, [4; 32], 20), None);
            turn.acknowledged(&history, command(5 + round), [4; 32], 100, 20);
            drop(turn);
            drop(lane);
            assert!(lanes.active.lock().unwrap().is_empty());
            assert_eq!(budget.usage(), (0, 0));
        }
    }
}

#[tokio::test]
async fn acquiring_then_cancelling_consumes_both_copies_of_the_hint() {
    let lanes = Arc::new(Lanes::default());
    let budget = Budget::new(16_384, 2);
    let history = cell("__Queue:a");
    let first = enter(&lanes, &budget, &history);
    let second = enter(&lanes, &budget, &history);
    first
        .lock()
        .await
        .acknowledged(&history, command(5), [4; 32], 100, 10);
    // No receipt is delivered by this turn, even though it never selects a hint.
    drop(second.lock().await);
    let mut third = first.lock().await;
    assert_eq!(third.take_successor(&history, [4; 32], 20), None);
    assert_eq!(third.take_node_successor(&history, [4; 32], 20), None);
    drop(third);
    drop((first, second));
    assert_eq!(budget.usage(), (0, 0));
}

#[tokio::test]
async fn cancelling_before_acquisition_does_not_consume_the_predecessors_hint() {
    let lanes = Arc::new(Lanes::default());
    let budget = Budget::new(16_384, 2);
    let history = cell("__Queue:a");
    let first = enter(&lanes, &budget, &history);
    let second = enter(&lanes, &budget, &history);
    let mut turn = first.lock().await;
    let mut waiting = Box::pin(second.lock());
    assert!(poll!(waiting.as_mut()).is_pending());
    turn.acknowledged(&history, command(5), [4; 32], 100, 10);
    drop(waiting);
    drop(second);
    drop(turn);
    drop(first);
    assert_eq!(budget.usage(), (0, 0));
    let later = enter(&lanes, &budget, &history);
    assert_eq!(
        later
            .lock()
            .await
            .take_node_successor(&history, [4; 32], 20),
        Some(6)
    );
}

#[tokio::test]
async fn changed_scope_or_expiry_consumes_advice_without_replenishing() {
    let history = cell("__Queue:a");
    let mut incarnation = history.clone();
    incarnation.incarnation = cellar_logic::consensus::IncarnationId::new([9; 16]).unwrap();
    for (target, schema, now) in [
        (incarnation, [4; 32], 20),
        (history.clone(), [9; 32], 20),
        (history.clone(), [4; 32], 100),
    ] {
        let lanes = Arc::new(Lanes::default());
        let budget = Budget::new(16_384, 2);
        let first = enter(&lanes, &budget, &history);
        let second = enter(&lanes, &budget, &target);
        first
            .lock()
            .await
            .acknowledged(&history, command(5), [4; 32], 100, 10);
        let mut turn = second.lock().await;
        assert_eq!(turn.take_successor(&target, schema, now), None);
        assert_eq!(turn.take_node_successor(&target, schema, now), None);
        assert_eq!(turn.take_successor(&history, [4; 32], 20), None);
        assert_eq!(turn.take_node_successor(&history, [4; 32], 20), None);
        drop(turn);
        drop((first, second));
        assert_eq!(budget.usage(), (0, 0));
    }
}

#[tokio::test]
async fn other_logical_targets_never_receive_or_consume_each_others_advice() {
    let history = cell("__Queue:a");
    let mut cluster = history.clone();
    cluster.cluster = ClusterId::new([9; 16]).unwrap();
    let mut namespace = history.clone();
    namespace.namespace = NamespaceId::new([9; 16]).unwrap();
    for other in [cell("__Queue:b"), cluster, namespace] {
        let lanes = Arc::new(Lanes::default());
        let budget = Budget::new(8192, 1);
        let first = enter(&lanes, &budget, &history);
        first
            .lock()
            .await
            .acknowledged(&history, command(5), [4; 32], 100, 10);
        drop(first);
        let wrong = enter(&lanes, &budget, &other);
        assert_eq!(
            wrong.lock().await.take_node_successor(&other, [4; 32], 20),
            None
        );
        drop(wrong);
        let correct = enter(&lanes, &budget, &history);
        assert_eq!(
            correct
                .lock()
                .await
                .take_node_successor(&history, [4; 32], 20),
            Some(6)
        );
    }
}

#[tokio::test]
async fn terminal_wrong_client_or_expired_receipt_does_not_replenish() {
    for (id, until) in [
        (command(u64::MAX), 100),
        (
            CommandId::new(cellar_logic::consensus::ClientId::new([9; 16]).unwrap(), 5).unwrap(),
            100,
        ),
        (command(5), 10),
    ] {
        let lanes = Arc::new(Lanes::default());
        let budget = Budget::new(8192, 1);
        let history = cell("__Queue:a");
        let first = enter(&lanes, &budget, &history);
        first
            .lock()
            .await
            .acknowledged(&history, id, [4; 32], until, 10);
        drop(first);
        let later = enter(&lanes, &budget, &history);
        let mut turn = later.lock().await;
        assert_eq!(turn.take_successor(&history, [4; 32], 20), None);
        assert_eq!(turn.take_node_successor(&history, [4; 32], 20), None);
    }
}

#[tokio::test]
async fn fixed_capacity_eviction_drops_advice_and_releases_every_active_owner() {
    let lanes = Arc::new(Lanes::default());
    let budget = Budget::new(8192, 1);
    for i in 0..9 {
        let history = cell(&format!("__Queue:{i}"));
        let lane = enter(&lanes, &budget, &history);
        lane.lock()
            .await
            .acknowledged(&history, command(5), [4; 32], 100, 10);
    }
    assert!(lanes.active.lock().unwrap().is_empty());
    assert_eq!(budget.usage(), (0, 0));
    let mut hits = 0;
    for i in 0..9 {
        let history = cell(&format!("__Queue:{i}"));
        let lane = enter(&lanes, &budget, &history);
        hits += usize::from(
            lane.lock()
                .await
                .take_node_successor(&history, [4; 32], 20)
                .is_some(),
        );
    }
    assert_eq!(hits, 8);
    assert!(lanes.active.lock().unwrap().is_empty());
    assert_eq!(budget.usage(), (0, 0));
}

#[tokio::test]
async fn close_discards_advice_and_a_late_receipt_cannot_repopulate_it() {
    let lanes = Arc::new(Lanes::default());
    let budget = Budget::new(8192, 1);
    let history = cell("__Queue:a");
    let first = enter(&lanes, &budget, &history);
    first
        .lock()
        .await
        .acknowledged(&history, command(5), [4; 32], 100, 10);
    lanes.close_hints();
    assert_eq!(budget.usage(), (8192, 1));
    first
        .lock()
        .await
        .acknowledged(&history, command(6), [4; 32], 100, 20);
    drop(first);
    let later = enter(&lanes, &budget, &history);
    assert_eq!(
        later
            .lock()
            .await
            .take_node_successor(&history, [4; 32], 30),
        None
    );
    drop(later);
    assert_eq!(budget.usage(), (0, 0));
    let restarted = Arc::new(Lanes::default());
    let fresh = enter(&restarted, &budget, &history);
    assert_eq!(
        fresh
            .lock()
            .await
            .take_node_successor(&history, [4; 32], 30),
        None
    );
}
