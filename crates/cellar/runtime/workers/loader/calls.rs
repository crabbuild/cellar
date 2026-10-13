// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Adapted for Cellar from Celld: crates/celld/js.rs.

//! Loaded-worker invocations over the shared turn driver.

use crate::engine_api::HttpResponse;
use crate::http_streams::RequestBodyGuard;
use crate::pool::Slot;
use crate::runtime::RequestCancellationLifetime;
use crate::WorkerJob;
use std::sync::Arc;
use tokio::sync::oneshot;

pub(crate) async fn fetch(
    slot: Arc<Slot>,
    make_job: impl FnOnce(oneshot::Sender<anyhow::Result<HttpResponse>>) -> WorkerJob,
    mut body_guard: RequestBodyGuard,
    cancellation: Arc<RequestCancellationLifetime>,
    cancel: Option<oneshot::Receiver<()>>,
) -> Result<HttpResponse, String> {
    let (reply, receive) = oneshot::channel();
    let job = make_job(reply);
    let driving = crate::asyncrt::spawn(cancellation.clone().drive_stateless_job(slot, job));
    let response = async {
        match receive.await {
            Ok(response) => response,
            Err(_) => Err(match driving.await {
                Err(error) => anyhow::anyhow!("loaded worker task died: {error}"),
                Ok(()) => anyhow::anyhow!("loaded worker dropped response"),
            }),
        }
    };
    let response = crate::runtime::receive_service_response(response, cancellation, cancel)
        .await
        .map_err(|error| format!("{error}"))?;
    // The child installed its request context and owns any unread body tail
    // through its background work, including when the caller is disposed.
    body_guard.disarm();
    Ok(response)
}

pub(crate) async fn rpc(
    slot: Arc<Slot>,
    make_job: impl FnOnce(oneshot::Sender<anyhow::Result<Vec<u8>>>) -> WorkerJob,
) -> Result<Vec<u8>, String> {
    let (reply, receive) = oneshot::channel();
    let cancellation = RequestCancellationLifetime::stateless();
    let driving = crate::asyncrt::spawn(
        cancellation
            .clone()
            .drive_stateless_job(slot, make_job(reply)),
    );
    let response = async {
        match receive.await {
            Ok(result) => result,
            Err(_) => Err(match driving.await {
                Err(error) => anyhow::anyhow!("loaded worker task died: {error}"),
                Ok(()) => anyhow::anyhow!("loaded worker dropped RPC result"),
            }),
        }
    };
    crate::runtime::receive_service_response(response, cancellation, None)
        .await
        .map_err(|error| error.to_string())
}
