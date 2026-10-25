// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::application::rpc::{Code, EffectOutcome, Failure};
use std::sync::atomic::AtomicUsize;

#[test]
fn cancellation_reaches_descendants_without_revoking_parent_or_siblings() {
    let root = InvocationCancellation::new(None);
    let child = InvocationCancellation::new(Some(&root));
    let grandchild = InvocationCancellation::new(Some(&child));
    let sibling = InvocationCancellation::new(Some(&root));
    child.revoke();
    assert!(child.is_revoked() && grandchild.is_revoked());
    assert!(!root.is_revoked() && !sibling.is_revoked());
    assert!(InvocationCancellation::new(Some(&child)).is_revoked());
    root.revoke();
    assert!(sibling.is_revoked());
}

#[test]
fn completed_reply_detaches_background_authority_without_reviving_cancelled_work() {
    let parent = InvocationCancellation::new(None);
    let replied = InvocationCancellation::new(Some(&parent));
    replied.detach();
    let background = InvocationCancellation::new(Some(&replied));
    parent.revoke();
    assert!(!replied.is_revoked() && !background.is_revoked());
    replied.revoke();
    assert!(background.is_revoked());
    background.detach();
    assert!(background.is_revoked());
}

#[test]
fn sequential_calls_do_not_retain_finished_weak_registrations() {
    let parent = InvocationCancellation::new(None);
    for _ in 0..10_000 {
        let child = InvocationCancellation::new(Some(&parent));
        let weak = Arc::downgrade(&child);
        child.revoke();
        drop(child);
        assert!(weak.upgrade().is_none());
    }
    let _active = InvocationCancellation::new(Some(&parent));
    assert_eq!(parent.children.lock().unwrap().len(), 1);
}

#[test]
fn deep_cancellation_and_drop_are_iterative() {
    let root = InvocationCancellation::new(None);
    let mut scopes = vec![root.clone()];
    for _ in 0..20_000 {
        scopes.push(InvocationCancellation::new(scopes.last()));
    }
    root.revoke();
    assert!(scopes.iter().all(|scope| scope.is_revoked()));
    drop(scopes);
}

#[test]
fn registration_and_overlapping_subtree_revocations_are_serialized() {
    let root = InvocationCancellation::new(None);
    let scopes = std::thread::scope(|threads| {
        let mut tasks = Vec::new();
        for _ in 0..4 {
            let root = root.clone();
            tasks.push(threads.spawn(move || {
                (0..500)
                    .map(|_| {
                        let child = InvocationCancellation::new(Some(&root));
                        let leaf = InvocationCancellation::new(Some(&child));
                        child.revoke();
                        assert!(leaf.is_revoked());
                        leaf
                    })
                    .collect::<Vec<_>>()
            }));
        }
        root.revoke();
        tasks
            .into_iter()
            .flat_map(|task| task.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert!(scopes.iter().all(|scope| scope.is_revoked()));
}

#[tokio::test]
async fn notifications_work_before_and_after_wait_registration() {
    use futures_util::FutureExt;
    let scope = InvocationCancellation::new(None);
    let mut waiting = Box::pin(scope.cancelled());
    assert!(waiting.as_mut().now_or_never().is_none());
    scope.revoke();
    assert!(waiting.as_mut().now_or_never().is_some());
    assert!(scope.cancelled().now_or_never().is_some());
}

struct Dropped(Arc<AtomicUsize>);

#[tokio::test]
async fn replied_rpc_background_outlives_its_caller_until_the_driver_finishes() -> Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let parent = super::super::RequestCancellationLifetime::stateless();
            let requests = Arc::new(Mutex::new(super::super::RemoteRequestRegistry::default()));
            let id = crate::engine_api::next_request_id();
            let child = requests
                .lock()
                .unwrap()
                .active_with_parent(id, Some(&parent.authority));
            let driver = super::super::RequestCancellationGuard::shared(child.clone());
            let mut reply = super::super::RpcCancellationGuard {
                requests: requests.clone(),
                lifetime: child.clone(),
                completed: false,
            };
            reply.complete();
            parent.finish();
            assert!(
                !child.authority.is_revoked(),
                "caller retirement cancelled a completed callee's background work"
            );
            assert!(!requests.lock().unwrap().abort(id));
            assert!(
                !child.authority.is_revoked(),
                "late caller abort reached completed RPC work"
            );
            drop(driver);
            assert!(child.authority.is_revoked());
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}

impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn cancelled_calls_do_not_poll_operations_and_release_owned_work() -> Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let controller = cellar_native::web::AbortController::new();
            controller.abort(None)?;
            let drops = Arc::new(AtomicUsize::new(0));
            let polls = AtomicUsize::new(0);
            let owned = Dropped(drops.clone());
            let operation = async {
                let _owned = owned;
                polls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            };
            let error = receive(None, Some(&controller.signal()), operation)
                .await
                .unwrap_err();
            let failure = error.downcast_ref::<Failure>().unwrap();
            assert_eq!(failure.code(), Code::Cancelled);
            assert_eq!(failure.outcome(), EffectOutcome::Unknown);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            assert_eq!(polls.load(Ordering::SeqCst), 0);
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn consuming_a_driver_wake_or_finishing_cannot_revive_sdk_authority() -> Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let lifetime = super::super::RequestCancellationLifetime::stateless();
            lifetime.publish_abort();
            assert!(lifetime
                .gates
                .take_request_cancellation(Some(lifetime.request_id)));
            assert!(lifetime.authority.is_revoked());
            lifetime.finish();
            let next =
                super::super::RequestCancellationLifetime::from_request_id(lifetime.request_id);
            lifetime.publish_abort();
            assert!(!next.authority.is_revoked());
            assert!(!next.gates.take_request_cancellation(Some(next.request_id)));
            next.finish();
            assert!(next.authority.is_revoked());
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}
