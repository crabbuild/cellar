// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use futures_util::FutureExt as _;
use std::sync::{atomic::AtomicUsize, Arc};

fn code(error: &anyhow::Error) -> ErrorCode {
    error.downcast_ref::<DeliveryError>().unwrap().code
}

fn untagged(error: &anyhow::Error) {
    assert!(error
        .downcast_ref::<super::super::RemoteRpcTransportFailure>()
        .is_none());
}

#[tokio::test]
async fn expired_original_budget_never_polls_an_unsubmitted_operation() -> Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let budget = Budget::until(0);
            let polls = AtomicUsize::new(0);
            let operation = async {
                polls.fetch_add(1, Ordering::SeqCst);
                Ok(7)
            };
            let error = receive(operation, None, Some(&budget)).await.unwrap_err();
            assert_eq!(code(&error), ErrorCode::Unavailable);
            untagged(&error);
            assert_eq!(polls.load(Ordering::SeqCst), 0);
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}

struct Dropped(Arc<AtomicUsize>);

impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn routing_and_definite_refusal_deadlines_preserve_non_execution_certainty() -> Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            for (attempt, expected) in [
                (None, ErrorCode::Unavailable),
                (
                    Some(cellar_logic::routing::Attempt::NotOwner),
                    ErrorCode::OwnershipLost,
                ),
                (
                    Some(cellar_logic::routing::Attempt::NeverConnected),
                    ErrorCode::Unavailable,
                ),
            ] {
                let budget = Budget::new(10);
                let drops = Arc::new(AtomicUsize::new(0));
                let guard = Dropped(drops.clone());
                let mut operation = Box::pin(async {
                    let _guard = guard;
                    if let Some(attempt) = attempt {
                        budget.submitted();
                        budget.refused(attempt);
                    }
                    std::future::pending::<Result<()>>().await
                });
                // These cases start after their named routing/refusal state.
                // Keep the original budget, but establish that state before
                // testing how an already-pending wait expires.
                assert!(operation.as_mut().now_or_never().is_none());
                let error = crate::asyncrt::timeout(
                    std::time::Duration::from_secs(2),
                    receive(operation, None, Some(&budget)),
                )
                .await?
                .unwrap_err();
                assert_eq!(code(&error), expected);
                untagged(&error);
                assert_eq!(drops.load(Ordering::SeqCst), 1);
            }
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn a_submitted_timeout_cannot_undo_an_already_committed_effect() -> Result<()> {
    submitted_timeout_case(false).await
}

#[tokio::test]
async fn submitted_effect_survives_wait_installation_after_the_original_deadline() -> Result<()> {
    submitted_timeout_case(true).await
}

async fn wait_for_original_deadline(budget: &Budget) {
    loop {
        let remaining = budget
            .deadline_ms()
            .saturating_sub(crate::asyncrt::mono_ms());
        if remaining == 0 {
            return;
        }
        crate::asyncrt::sleep(std::time::Duration::from_millis(remaining)).await;
    }
}

#[tokio::test]
async fn expired_wait_setup_never_submits_or_commits_the_operation() -> Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let budget = Budget::new(10);
            let effects = AtomicUsize::new(0);
            let drops = Arc::new(AtomicUsize::new(0));
            let guard = Dropped(drops.clone());
            let operation = async {
                let _guard = guard;
                budget.submitted();
                effects.fetch_add(1, Ordering::SeqCst);
                std::future::pending::<Result<()>>().await
            };
            wait_for_original_deadline(&budget).await;
            let error = receive(operation, None, Some(&budget)).await.unwrap_err();
            assert_eq!(code(&error), ErrorCode::Unavailable);
            untagged(&error);
            assert_eq!(effects.load(Ordering::SeqCst), 0);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            Ok::<_, anyhow::Error>(())
        })
        .await;
    domain.shutdown().await;
    result?
}

async fn submitted_timeout_case(delay_setup: bool) -> Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let budget = Budget::new(10);
            let effects = AtomicUsize::new(0);
            let drops = Arc::new(AtomicUsize::new(0));
            let guard = Dropped(drops.clone());
            let mut operation = Box::pin(async {
                let _guard = guard;
                budget.submitted();
                effects.fetch_add(1, Ordering::SeqCst);
                std::future::pending::<Result<()>>().await
            });
            // Model an operation that actually submitted and committed an
            // effect before installing its wait. Poll the owned future once
            // and retain it, so cancellation still has to drop its guard.
            assert!(operation.as_mut().now_or_never().is_none());
            assert_eq!(effects.load(Ordering::SeqCst), 1);
            assert_eq!(drops.load(Ordering::SeqCst), 0);
            if delay_setup {
                wait_for_original_deadline(&budget).await;
            }
            let error = crate::asyncrt::timeout(
                std::time::Duration::from_secs(2),
                receive(operation, None, Some(&budget)),
            )
            .await?
            .unwrap_err();
            assert_eq!(
                code(&error),
                ErrorCode::Ambiguous,
                "delay_setup={delay_setup}, deadline={}, now={}, effects={}, drops={}",
                budget.deadline_ms(),
                crate::asyncrt::mono_ms(),
                effects.load(Ordering::SeqCst),
                drops.load(Ordering::SeqCst)
            );
            untagged(&error);
            assert_eq!(effects.load(Ordering::SeqCst), 1);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn cancellation_wins_ready_result_and_deadline_before_submission() -> Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            for closed in [true, false] {
                let budget = Budget::until(0);
                let polls = AtomicUsize::new(0);
                let (sender, cancel) = oneshot::channel();
                if closed {
                    drop(sender);
                } else {
                    sender.send(()).unwrap();
                }
                let error = receive(
                    async {
                        polls.fetch_add(1, Ordering::SeqCst);
                        Ok(7)
                    },
                    Some(cancel),
                    Some(&budget),
                )
                .await
                .unwrap_err();
                assert_eq!(code(&error), ErrorCode::Unavailable);
                untagged(&error);
                assert!(error.to_string().contains("cancelled"));
                assert_eq!(polls.load(Ordering::SeqCst), 0);
            }
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn cancellation_after_submission_drops_the_wait_without_inventing_rollback() -> Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let budget = Budget::new(1000);
            let effects = AtomicUsize::new(0);
            let drops = Arc::new(AtomicUsize::new(0));
            let guard = Dropped(drops.clone());
            let (entered, entry) = oneshot::channel();
            let (sender, cancel) = oneshot::channel();
            let operation = async {
                let _guard = guard;
                budget.submitted();
                effects.fetch_add(1, Ordering::SeqCst);
                entered.send(()).unwrap();
                std::future::pending::<Result<()>>().await
            };
            let cancelling = async {
                entry.await.unwrap();
                sender.send(()).unwrap();
            };
            let (result, ()) = futures_util::future::join(
                receive(operation, Some(cancel), Some(&budget)),
                cancelling,
            )
            .await;
            let error = result.unwrap_err();
            assert_eq!(code(&error), ErrorCode::Ambiguous);
            untagged(&error);
            assert_eq!(effects.load(Ordering::SeqCst), 1);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn existing_typed_outcomes_survive_and_large_budgets_do_not_reset() -> Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let budget = Budget::until(u64::MAX);
            let error = receive(
                async {
                    Err::<(), _>(DeliveryError::new(ErrorCode::StreamFull, "pressure").into())
                },
                None,
                Some(&budget),
            )
            .await
            .unwrap_err();
            assert_eq!(code(&error), ErrorCode::StreamFull);
            assert_eq!(budget.deadline_ms(), u64::MAX);
            assert_eq!(receive(async { Ok(7) }, None, Some(&budget)).await?, 7);
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}

fn app() -> Result<(
    crate::actor::AppHandle,
    tokio::sync::mpsc::UnboundedReceiver<crate::actor::Message>,
)> {
    let (tx, receiver) = tokio::sync::mpsc::unbounded_channel();
    let (websockets, _) = tokio::sync::mpsc::unbounded_channel();
    Ok((
        crate::actor::AppHandle {
            tx,
            runtime: None,
            #[cfg(all(test, cellar_internal_tests))]
            scripted_activity_host: None,
            reload: crate::generation::reload_channel().0,
            peer_http: reqwest::Client::builder().no_proxy().build()?,
            peer_auth: Arc::new(crate::peer_auth::PeerAuth::new([0; 32], "test")?),
            advertise: "127.0.0.1:1".into(),
            websockets,
            max_outbound_websockets: 1,
            draining: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            fleet_ready: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            public_in_flight: Arc::new(AtomicUsize::new(0)),
            drain_pins: Default::default(),
            follower: None,
            trust_forwarded_headers: false,
            max_request_body_bytes: 1024,
            operation_deadline_ms: 10,
        },
        receiver,
    ))
}

#[tokio::test]
async fn cancellation_of_an_unpolled_local_wait_releases_the_actual_activity() -> Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let (app, mut messages) = app()?;
            let budget = Budget::until(0);
            let local = app.local_request(42, "Order:a".into(), None, "test");
            let polls = AtomicUsize::new(0);
            let operation = async {
                let _completed = local
                    .run(async {
                        polls.fetch_add(1, Ordering::SeqCst);
                        Ok(crate::engine_api::RpcOutcome {
                            data: crate::engine_api::RpcData::Json("null".into()),
                            write_position: None,
                            observed_position: None,
                        })
                    })
                    .await;
                Ok(())
            };
            let (sender, cancel) = oneshot::channel();
            sender.send(()).unwrap();
            let error = receive(operation, Some(cancel), Some(&budget))
                .await
                .unwrap_err();
            assert_eq!(code(&error), ErrorCode::Unavailable);
            assert_eq!(polls.load(Ordering::SeqCst), 0);
            assert!(matches!(
                messages.try_recv()?,
                crate::actor::Message::ActivityFinished { request: 42, .. }
            ));
            assert!(messages.try_recv().is_err());
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn real_dispatch_route_timeout_cancels_the_pending_actor_request() -> Result<()> {
    use crate::application::event_stream::{Authority, Binding};
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain.run(async {
        let (app, mut messages) = app()?;
        let binding: Binding = serde_json::from_value(serde_json::json!({
            "environment": "EVENTS",
            "resource": {"tenant": "t", "application": "a", "id": "a".repeat(32), "name": "e", "family": "e"},
            "principal": "test", "grant": "grant", "read": true
        }))?;
        let authority = Authority::resolution(&binding)?;
        let (reply, result) = oneshot::channel();
        let call = crate::host_channels::RpcCallReq {
            authority: Some(authority.into()), request_id: None, cancel: None,
            scope: crate::application::event_stream::cell_scope(&binding.resource)?,
            name: None, method: "__streamResolve".into(),
            args: crate::engine_api::RpcData::Json("[]".into()), reply,
        };
        let dispatch = super::super::dispatch_rpc_call(app, call);
        let actor = async {
            let crate::actor::Message::Request { request, reply, .. } = messages.recv().await.unwrap()
                else { panic!("routing request missing") };
            let cancelled = messages.recv().await.unwrap();
            assert!(matches!(cancelled, crate::actor::Message::CancelRoute { request: observed } if observed == request));
            drop(reply);
        };
        crate::asyncrt::timeout(std::time::Duration::from_secs(2), futures_util::future::join(dispatch, actor)).await?;
        let error = result.await?.err().expect("RPC unexpectedly succeeded");
        assert_eq!(code(&error), ErrorCode::Unavailable);
        untagged(&error);
        Ok::<_, anyhow::Error>(())
    }).await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn ambiguous_attempt_discards_its_route_without_replay_or_cleanup_wait() -> Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let (app, mut messages) = app()?;
            let mut route = super::super::RemoteRouteRetry::new(1000);
            assert!(!route.observe("old-node", 7, None));
            let operation = route.retry(
                &app,
                "Order:a",
                cellar_logic::routing::Attempt::Ambiguous,
                None,
                Some("rpc"),
            );
            let outcome = futures_util::FutureExt::now_or_never(operation)
                .expect("stopping an ambiguous request must not wait for cache cleanup");
            assert_eq!(outcome, super::super::RemoteRetryOutcome::Stop);
            let message = messages
                .try_recv()
                .expect("an ambiguous failure retained its dead route");
            let crate::actor::Message::InvalidateRemote {
                cell,
                node,
                epoch,
                capacity_sample_ms,
                reply,
            } = message
            else {
                panic!("a stopped request was replayed instead of invalidated")
            };
            assert_eq!(
                (cell.as_str(), node.as_str(), epoch),
                ("Order:a", "old-node", 7)
            );
            assert_eq!(capacity_sample_ms, None);
            assert!(
                reply.send(()).is_err(),
                "stopped caller retained the cleanup wait"
            );
            assert!(
                messages.try_recv().is_err(),
                "stopped caller queued another dispatch"
            );
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}
