// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::raft::budget::Budget;
use cellar_logic::consensus::IncarnationId;
use futures_util::poll;

fn cell(name: &str, incarnation: u8) -> CellHistory {
    CellHistory {
        cluster: ClusterId::new([1; 16]).unwrap(),
        namespace: NamespaceId::new([2; 16]).unwrap(),
        cell: CellId::new(name).unwrap(),
        incarnation: IncarnationId::new([incarnation; 16]).unwrap(),
    }
}

#[tokio::test]
async fn same_logical_target_waits_fifo_across_incarnation_hints() {
    let lanes = Arc::new(Lanes::default());
    let budget = Budget::new(24_576, 3);
    let first = lanes
        .enter(&cell("__Queue:a", 3), budget.acquire(8192).unwrap())
        .unwrap();
    let second = lanes
        .enter(&cell("__Queue:a", 4), budget.acquire(8192).unwrap())
        .unwrap();
    let third = lanes
        .enter(&cell("__Queue:a", 3), budget.acquire(8192).unwrap())
        .unwrap();
    let held = first.lock().await;
    let mut second_wait = Box::pin(second.lock());
    let mut third_wait = Box::pin(third.lock());
    assert!(poll!(second_wait.as_mut()).is_pending());
    assert!(poll!(third_wait.as_mut()).is_pending());
    assert_eq!(lanes.active.lock().unwrap().len(), 1);
    assert_eq!(budget.usage(), (24_576, 3));
    drop(held);
    let held = second_wait.await;
    assert!(poll!(third_wait.as_mut()).is_pending());
    drop(held);
    drop(third_wait.await);
    drop((first, second, third));
    assert!(lanes.active.lock().unwrap().is_empty());
    assert_eq!(budget.usage(), (0, 0));
}

#[tokio::test]
async fn unrelated_targets_make_progress_independently() {
    let lanes = Arc::new(Lanes::default());
    let budget = Budget::new(16_384, 2);
    let first = lanes
        .enter(&cell("__Queue:a", 3), budget.acquire(8192).unwrap())
        .unwrap();
    let second = lanes
        .enter(&cell("__Queue:b", 3), budget.acquire(8192).unwrap())
        .unwrap();
    let held = first.lock().await;
    let mut other = Box::pin(second.lock());
    assert!(poll!(other.as_mut()).is_ready());
    drop(other);
    drop(held);
    drop((first, second));
    assert!(lanes.active.lock().unwrap().is_empty());
    assert_eq!(budget.usage(), (0, 0));
}

#[tokio::test]
async fn cancelled_waiter_refunds_its_existing_admission_and_keeps_the_live_lane() {
    let lanes = Arc::new(Lanes::default());
    let budget = Budget::new(16_384, 2);
    let first = lanes
        .enter(&cell("__Queue:a", 3), budget.acquire(8192).unwrap())
        .unwrap();
    let second = lanes
        .enter(&cell("__Queue:a", 3), budget.acquire(8192).unwrap())
        .unwrap();
    let held = first.lock().await;
    let mut waiting = Box::pin(second.lock());
    assert!(poll!(waiting.as_mut()).is_pending());
    assert!(budget.acquire(1).is_none());
    drop(waiting);
    drop(second);
    assert_eq!(budget.usage(), (8192, 1));
    assert_eq!(lanes.active.lock().unwrap().len(), 1);
    drop(held);
    drop(first);
    assert_eq!(budget.usage(), (0, 0));
    assert!(lanes.active.lock().unwrap().is_empty());
}

#[test]
fn concurrent_last_drops_remove_idle_registry_entries() {
    const TARGETS: usize = 4096;
    let lanes = Arc::new(Lanes::default());
    let budget = Budget::new(TARGETS * 2 * 8192, TARGETS * 2);
    let mut first = Vec::with_capacity(TARGETS);
    let mut second = Vec::with_capacity(TARGETS);
    for target in 0..TARGETS {
        let target = cell(&format!("__Queue:{target}"), 3);
        first.push(lanes.enter(&target, budget.acquire(8192).unwrap()).unwrap());
        second.push(lanes.enter(&target, budget.acquire(8192).unwrap()).unwrap());
    }
    let barrier = std::sync::Barrier::new(2);
    std::thread::scope(|scope| {
        for callers in [first, second] {
            let barrier = &barrier;
            scope.spawn(move || {
                for caller in callers {
                    barrier.wait();
                    drop(caller);
                    barrier.wait();
                }
            });
        }
    });
    assert_eq!(budget.usage(), (0, 0));
    assert!(lanes.active.lock().unwrap().is_empty());
}
