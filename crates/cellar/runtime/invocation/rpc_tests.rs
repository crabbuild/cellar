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
