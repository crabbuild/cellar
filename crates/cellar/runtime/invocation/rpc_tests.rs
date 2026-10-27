// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Distinguish a request that never leaves from a lost result after submission.
use super::*;
use crate::application::event_stream::{Authority, Binding, DeliveryError, ErrorCode};
use std::sync::Arc;

fn authority() -> Result<crate::runtime::RpcAuthority> {
    let binding: Binding = serde_json::from_value(serde_json::json!({
        "resource":{"tenant":"tenant","application":"application","id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","name":"events","family":"events"},
        "environment":"EVENTS","principal":"test","grant":"grant","publish":false,"read":true,"subscriptions":[],"administer":false
    }))?;
    Ok(Authority::resolution(&binding)?.into())
}

#[tokio::test]
async fn typed_reply_limit_survives_queue_and_reservation_cloning() -> Result<()> {
    use std::future::Future as _;
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
            crate::host_channels::set_rpc_call_tx(sender);
            let mut call = Box::pin(call_scoped(
                EgressGate::NoCell,
                "Counter:alice".into(),
                None,
                "example.Counter/GetCount".into(),
                RpcData::Json("[]".into()),
                None,
                CallScope::default().with_reply_limit(2176),
            ));
            std::future::poll_fn(|cx| {
                assert!(call.as_mut().poll(cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            let queued = receiver.try_recv()?;
            let held = queued.reservation.as_ref().unwrap().clone();
            assert_eq!(held.reply_limit(), Some(2176));
            drop(call);
            drop(queued);
            assert_eq!(held.reply_limit(), Some(2176));
            assert_eq!(domain.services().outbound_rpc_usage().active_requests, 1);
            drop(held);
            assert_eq!(domain.services().outbound_rpc_usage(), Default::default());
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn native_rpc_source_queue_refuses_while_a_previous_request_is_queued() -> Result<()> {
    use std::future::Future as _;
    let domain = Arc::new(crate::asyncrt::ExecutionDomain::new(
        tokio::runtime::Handle::current(),
    ));
    domain
        .services()
        .configure_runtime(crate::runtime_policy::RuntimePolicy {
            max_outbound_rpc_requests: 1,
            ..Default::default()
        })?;
    let result = domain
        .run(async {
            let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
            crate::host_channels::set_rpc_call_tx(sender);
            let digest = "a".repeat(64);
            let args = crate::application::rpc::encode_envelope(&digest, vec![], 128)?;
            let mut first = Box::pin(call(
                EgressGate::NoCell,
                "Counter:first".into(),
                None,
                "test.Counter/GetCount".into(),
                args.clone(),
                None,
            ));
            let mut second = Box::pin(call(
                EgressGate::NoCell,
                "Counter:second".into(),
                None,
                "test.Counter/GetCount".into(),
                args,
                None,
            ));
            let reply = std::future::poll_fn(|cx| {
                assert!(first.as_mut().poll(cx).is_pending());
                assert_eq!(receiver.len(), 1);
                match second.as_mut().poll(cx) {
                    std::task::Poll::Ready(reply) => std::task::Poll::Ready(reply),
                    std::task::Poll::Pending => panic!(
                        "source quota queued {} requests instead of refusing the second",
                        receiver.len()
                    ),
                }
            })
            .await?;
            let failure =
                crate::application::rpc::decode_reply(&digest, &reply, Default::default())
                    .unwrap_err();
            assert_eq!(
                failure.code(),
                crate::application::rpc::Code::ResourceExhausted
            );
            assert_eq!(
                failure.outcome(),
                crate::application::rpc::EffectOutcome::NotCommitted
            );
            assert_eq!(receiver.len(), 1);
            drop((first, second, receiver));
            Ok::<(), anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}

async fn reservation_lifetime(policy: crate::runtime_policy::RuntimePolicy) -> Result<()> {
    use std::{future::Future as _, task::Poll};
    let domain = Arc::new(crate::asyncrt::ExecutionDomain::new(
        tokio::runtime::Handle::current(),
    ));
    domain.services().configure_runtime(policy)?;
    let result = domain
        .run(async {
            let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
            crate::host_channels::set_rpc_call_tx(sender);
            let digest = "a".repeat(64);
            let args = crate::application::rpc::encode_envelope(&digest, vec![], 128)?;
            let request = || {
                Box::pin(call(
                    EgressGate::NoCell,
                    "Counter:alice".into(),
                    None,
                    "test.Counter/GetCount".into(),
                    args.clone(),
                    None,
                ))
            };
            let mut first = request();
            std::future::poll_fn(|cx| {
                assert!(first.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            let held = domain.services().outbound_rpc_usage();
            assert_eq!(held.active_requests, 1);
            assert!(held.reserved_request_bytes > 64 * 1024);
            assert!(held.reserved_request_bytes <= policy.max_outbound_rpc_bytes);
            // Caller cancellation cannot free a request still owned by the queue.
            drop(first);
            assert_eq!(domain.services().outbound_rpc_usage(), held);
            let queued = receiver.try_recv()?;
            assert!(receiver.is_empty());
            // Draining the queue moves ownership into routing; it does not retire it.
            assert_eq!(domain.services().outbound_rpc_usage(), held);
            let refused = request().await?;
            let failure =
                crate::application::rpc::decode_reply(&digest, &refused, Default::default())
                    .unwrap_err();
            assert_eq!(
                failure.code(),
                crate::application::rpc::Code::ResourceExhausted
            );
            assert_eq!(
                failure.outcome(),
                crate::application::rpc::EffectOutcome::NotCommitted
            );
            assert!(receiver.is_empty());
            assert_eq!(domain.services().outbound_rpc_usage(), held);
            drop(queued);
            assert_eq!(domain.services().outbound_rpc_usage(), Default::default());

            // An undelivered result still has an owner even after dispatch retires.
            let mut completed = request();
            std::future::poll_fn(|cx| {
                assert!(completed.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            let queued = receiver.try_recv()?;
            assert!(queued.reply.send(Ok(args.clone())).is_ok());
            drop(queued.reservation);
            assert_eq!(domain.services().outbound_rpc_usage(), held);
            let refused = request().await?;
            assert_eq!(
                crate::application::rpc::decode_reply(&digest, &refused, Default::default())
                    .unwrap_err()
                    .code(),
                crate::application::rpc::Code::ResourceExhausted
            );
            completed.await?;
            assert_eq!(domain.services().outbound_rpc_usage(), Default::default());
            Ok::<(), anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn native_rpc_count_bound_tracks_queue_routing_and_undelivered_reply() -> Result<()> {
    reservation_lifetime(crate::runtime_policy::RuntimePolicy {
        max_outbound_rpc_requests: 1,
        ..Default::default()
    })
    .await
}

#[tokio::test]
async fn native_rpc_byte_bound_tracks_queue_routing_and_undelivered_reply() -> Result<()> {
    reservation_lifetime(crate::runtime_policy::RuntimePolicy {
        max_outbound_rpc_bytes: 128 * 1024,
        ..Default::default()
    })
    .await
}

#[tokio::test]
async fn pending_source_gate_retains_admission_and_cancellation_releases_it() -> Result<()> {
    use std::{future::Future as _, task::Poll};
    let domain = Arc::new(crate::asyncrt::ExecutionDomain::new(
        tokio::runtime::Handle::current(),
    ));
    domain
        .services()
        .configure_runtime(crate::runtime_policy::RuntimePolicy {
            max_outbound_rpc_requests: 1,
            ..Default::default()
        })?;
    let result = domain
        .run(async {
            let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
            crate::host_channels::set_rpc_call_tx(sender);
            let (gate_sender, mut gates) = tokio::sync::mpsc::unbounded_channel();
            crate::host_channels::set_gate_tx(gate_sender);
            let mut first = Box::pin(call(
                EgressGate::Wrote("source".into(), cellar_logic::Channel::Response, 1, None),
                "scope".into(),
                None,
                "method".into(),
                RpcData::Json("[]".into()),
                Some(authority()?),
            ));
            std::future::poll_fn(|cx| {
                assert!(first.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            let mut gate = gates.try_recv()?;
            assert!(receiver.is_empty());
            assert_eq!(domain.services().outbound_rpc_usage().active_requests, 1);
            let error = call(
                EgressGate::NoCell,
                "other".into(),
                None,
                "method".into(),
                RpcData::Json("[]".into()),
                Some(authority()?),
            )
            .await
            .err()
            .unwrap();
            assert_eq!(
                error.downcast_ref::<DeliveryError>().unwrap().code,
                ErrorCode::Unavailable
            );
            assert!(receiver.is_empty());
            drop(first);
            gate.reply.closed().await;
            assert_eq!(domain.services().outbound_rpc_usage(), Default::default());
            Ok::<(), anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn refused_source_gate_never_submits_event_rpc() -> Result<()> {
    let domain = Arc::new(crate::asyncrt::ExecutionDomain::new(
        tokio::runtime::Handle::current(),
    ));
    let result = domain
        .run(async {
            let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
            crate::host_channels::set_rpc_call_tx(sender);
            let error = call(
                EgressGate::Unpersisted("test refusal".into()),
                "scope".into(),
                None,
                "method".into(),
                RpcData::Json("[]".into()),
                Some(authority()?),
            )
            .await
            .err()
            .expect("event RPC unexpectedly succeeded");
            assert_eq!(
                error.downcast_ref::<DeliveryError>().unwrap().code,
                ErrorCode::Unavailable
            );
            assert!(matches!(
                receiver.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            ));
            Ok::<(), anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn dropped_submitted_result_is_ambiguous_and_cancels_exact_request() -> Result<()> {
    let domain = Arc::new(crate::asyncrt::ExecutionDomain::new(
        tokio::runtime::Handle::current(),
    ));
    let result = domain
        .run(async {
            let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
            crate::host_channels::set_rpc_call_tx(sender);
            let request = call(
                EgressGate::NoCell,
                "scope".into(),
                None,
                "method".into(),
                RpcData::Json("[]".into()),
                Some(authority()?),
            );
            let responder = async {
                let submitted = receiver.recv().await.unwrap();
                let cancel = submitted.cancel.unwrap();
                drop(submitted.reply);
                assert!(
                    cancel.await.is_ok(),
                    "lost result did not cancel its submitted request"
                );
            };
            let (result, ()) = futures_util::future::join(request, responder).await;
            assert_eq!(
                result
                    .err()
                    .expect("event RPC unexpectedly succeeded")
                    .downcast_ref::<DeliveryError>()
                    .unwrap()
                    .code,
                ErrorCode::Ambiguous
            );
            Ok::<(), anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn pending_source_gate_times_out_without_submitting() -> Result<()> {
    let domain = Arc::new(crate::asyncrt::ExecutionDomain::new(
        tokio::runtime::Handle::current(),
    ));
    domain.services().pin_operation_deadline(10)?;
    let result = domain
        .run(async {
            let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
            crate::host_channels::set_rpc_call_tx(sender);
            let (gate_sender, mut gates) = tokio::sync::mpsc::unbounded_channel();
            crate::host_channels::set_gate_tx(gate_sender);
            let request = call(
                EgressGate::Wrote("source".into(), cellar_logic::Channel::Response, 1, None),
                "scope".into(),
                None,
                "method".into(),
                RpcData::Json("[]".into()),
                Some(authority()?),
            );
            let held_gate = async {
                let mut gate = gates.recv().await.expect("gate request missing");
                gate.reply.closed().await;
            };
            let (result, ()) = futures_util::future::join(request, held_gate).await;
            let error = result.err().expect("event RPC unexpectedly succeeded");
            assert_eq!(
                error.downcast_ref::<DeliveryError>().unwrap().code,
                ErrorCode::Unavailable
            );
            assert!(matches!(
                receiver.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            ));
            Ok::<(), anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn queued_source_timeout_cancels_the_submitted_request() -> Result<()> {
    let domain = Arc::new(crate::asyncrt::ExecutionDomain::new(
        tokio::runtime::Handle::current(),
    ));
    domain.services().pin_operation_deadline(10)?;
    let result = domain
        .run(async {
            let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
            crate::host_channels::set_rpc_call_tx(sender);
            let result = call(
                EgressGate::NoCell,
                "scope".into(),
                None,
                "method".into(),
                RpcData::Json("[]".into()),
                Some(authority()?),
            )
            .await;
            let error = result.err().expect("event RPC unexpectedly succeeded");
            assert_eq!(
                error.downcast_ref::<DeliveryError>().unwrap().code,
                ErrorCode::Ambiguous
            );
            let submitted = receiver.try_recv().expect("request was never submitted");
            assert!(submitted
                .cancel
                .expect("request has no cancellation")
                .await
                .is_ok());
            Ok::<(), anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn declared_poll_wait_survives_a_short_transport_budget() -> Result<()> {
    let domain = Arc::new(crate::asyncrt::ExecutionDomain::new(
        tokio::runtime::Handle::current(),
    ));
    domain.services().pin_operation_deadline(10)?;
    let result = domain
        .run(async {
            let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
            crate::host_channels::set_rpc_call_tx(sender);
            let args =
                crate::application::encode_rpc(&cellar_native::clone::StructuredValue::from_json(
                    serde_json::json!([{"wait_ms":"30000"}]),
                )?)?;
            let request = call(
                EgressGate::NoCell,
                "scope".into(),
                None,
                "__streamNext".into(),
                args,
                Some(authority()?),
            );
            let responder = async {
                let submitted = receiver.recv().await.expect("RPC was not submitted");
                crate::asyncrt::sleep(std::time::Duration::from_millis(25)).await;
                assert!(
                    submitted
                        .reply
                        .send(Ok(RpcData::Json("null".into())))
                        .is_ok(),
                    "poll wait was truncated by the transport-only budget"
                );
            };
            let (result, ()) = futures_util::future::join(request, responder).await;
            assert!(matches!(result?, RpcData::Json(text) if text == "null"));
            Ok::<(), anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}
