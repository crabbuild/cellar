// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::disallowed_methods)]
async fn dropped_async_waiter_still_requires_physical_join() {
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let execution = ControllerExecution::new(domain.services().domain_token().unwrap());
    let (entered, observed) = tokio::sync::oneshot::channel();
    let (release, blocked) = std::sync::mpsc::sync_channel(1);
    let task = execution
        .blocking(move || {
            entered.send(()).unwrap();
            blocked.recv().unwrap();
        })
        .unwrap();
    observed.await.unwrap();
    drop(task);
    let join = execution.join();
    tokio::pin!(join);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(25), join.as_mut())
            .await
            .is_err()
    );
    release.send(()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), join)
        .await
        .unwrap();
    domain.shutdown().await;
}
