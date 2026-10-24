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
