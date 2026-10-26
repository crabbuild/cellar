// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Native ownership probe around the same application used by the JS fixture.
use anyhow::{ensure, Result};
use cellar::sdk::*;
use std::sync::{Arc, Mutex};

pub struct ApplicationProbe(pub Arc<dyn Application>);

struct CellProbe {
    inner: Arc<dyn Cell>,
    socket: Mutex<Option<websocket::WebSocket>>,
    retire: Arc<tokio::sync::Notify>,
}

impl Application for ApplicationProbe {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            if let Invocation::Fetch(request) = event {
                if request.url.contains("/native/") {
                    return Ok(Output::Http(
                        context
                            .namespace("COUNTERS")?
                            .get_by_name("native-owner")
                            .fetch(&context, request)
                            .await?,
                    ));
                }
                self.0
                    .clone()
                    .invoke(context, Invocation::Fetch(request))
                    .await
            } else {
                self.0.clone().invoke(context, event).await
            }
        })
    }

    fn supports_cell(&self, class: &str) -> bool {
        self.0.supports_cell(class)
    }

    fn validate_deployment(&self, deployment: &cellar::fleet::LoadedDeployment) -> Result<()> {
        self.0.validate_deployment(deployment)
    }

    fn create_cell(&self, context: Context, identity: &CellIdentity) -> Result<Arc<dyn Cell>> {
        Ok(Arc::new(CellProbe {
            inner: self.0.create_cell(context, identity)?,
            socket: Mutex::new(None),
            retire: Arc::new(tokio::sync::Notify::new()),
        }))
    }
}

impl Cell for CellProbe {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = event else {
                return self.inner.clone().invoke(context, event).await;
            };
            if request.url.ends_with("/native/open") {
                let pair = context.websocket_pair()?;
                pair.server.accept_polled(&context)?;
                ensure!(
                    pair.server.accept_hibernating(&context, vec![]).is_err(),
                    "polled server became hibernatable"
                );
                *self.socket.lock().unwrap() = Some(pair.server);
                let retire = self.retire.clone();
                context.wait_until(async move {
                    retire.notified().await;
                    Ok(())
                })?;
                return Ok(Output::Http(pair.client.into_response(&context, vec![])?));
            }
            if request.url.ends_with("/native/borrow") {
                let socket = self.socket.lock().unwrap().as_ref().unwrap().clone();
                ensure!(
                    socket.send_text(&context, "forbidden").is_err(),
                    "later event sent a frame"
                );
                ensure!(
                    socket.close(&context, 1000, "forbidden").is_err(),
                    "later event closed the socket"
                );
                ensure!(
                    socket.next(&context).await.is_err(),
                    "later event read a frame"
                );
                ensure!(
                    socket.closed(&context).await.is_err(),
                    "later event observed the close"
                );
                ensure!(
                    socket.accept_polled(&context).is_err(),
                    "later event accepted the socket"
                );
                return Ok(Output::Http(
                    http::Response::new(200, b"refused".to_vec(), vec![]).into_http(&context)?,
                ));
            }
            if request.url.ends_with("/native/retire") {
                self.retire.notify_one();
                return Ok(Output::Http(
                    http::Response::new(200, b"retired".to_vec(), vec![]).into_http(&context)?,
                ));
            }
            self.inner
                .clone()
                .invoke(context, Invocation::Fetch(request))
                .await
        })
    }
}
