// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Native-owned non-HTTP drivers whose result precedes background completion.
use anyhow::{Context as _, Result};
use cellar::sdk::{Context, Output};
use std::{
    future::Future,
    sync::{Arc, Mutex},
};
use tokio::sync::oneshot;

pub(crate) type Reply = Arc<Mutex<Option<oneshot::Sender<Result<Output>>>>>;

pub(crate) fn publish(reply: &Reply, result: Result<Output>) {
    if let Some(reply) = reply.lock().unwrap().take() {
        // A disconnected caller cannot revoke the driver's owned native work.
        let _ = reply.send(result);
    }
}

pub(crate) async fn serve<F, Fut>(context: &Context, make: F) -> Result<Output>
where
    F: FnOnce(Reply) -> Fut + Send + 'static,
    Fut: Future<Output = Result<Output>> + Send + 'static,
{
    let (send, receive) = oneshot::channel();
    let reply = Arc::new(Mutex::new(Some(send)));
    context.wait_until(async move {
        match make(reply.clone()).await {
            Ok(output) => {
                publish(&reply, Ok(output));
                Ok(())
            }
            Err(error) => {
                publish(&reply, Err(anyhow::anyhow!("Workers driver failed")));
                Err(error)
            }
        }
    })?;
    receive.await.context("Workers result producer stopped")?
}
