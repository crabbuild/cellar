// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

#[tokio::test]
async fn joined_task_distinguishes_cancellation_from_panic_and_success() {
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let mut cancelled = domain.spawn(std::future::pending::<()>()).unwrap();
    cancelled.abort();
    assert!((&mut cancelled).await.is_err());
    assert!(cancelled.was_cancelled());
    let mut panicked = domain
        .spawn(async { panic!("task panic fixture") })
        .unwrap();
    assert!((&mut panicked).await.is_err());
    assert!(!panicked.was_cancelled());
    let mut successful = domain.spawn(async { 7 }).unwrap();
    assert_eq!((&mut successful).await.unwrap(), 7);
    assert!(!successful.was_cancelled());
    domain.shutdown().await;
}
