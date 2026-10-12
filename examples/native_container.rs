// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//! One Python HTTP process per business cell, controlled by the Rust SDK.
use anyhow::{bail, Result};
use cellar::sdk::{
    containers::StartOptions,
    http::{OutboundRequest, Redirect, Response},
    Application, Cell, Context, HandlerFuture, Host, Invocation, Output, Registry,
};
use std::sync::Arc;

#[derive(Default)]
struct Sandbox {
    startup: tokio::sync::Mutex<()>,
}
impl Cell for Sandbox {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = event else {
                return Ok(Output::Done);
            };
            let container = context.container()?;
            {
                let _startup = self.startup.lock().await;
                if !container.running(&context)? {
                    container.start(&context, StartOptions::default()).await?;
                    container
                        .port(&context, 8080)?
                        .wait_ready(&context, std::time::Duration::from_secs(20))
                        .await?;
                }
            }
            let response = container
                .port(&context, 8080)?
                .fetch(
                    &context,
                    OutboundRequest {
                        url: request.url,
                        method: request.method,
                        headers: request.headers,
                        body: Some(request.body),
                        redirect: Redirect::Manual,
                    },
                )
                .await?;
            Ok(Output::Http(response))
        })
    }
}
fn application() -> Result<Arc<dyn Application>> {
    Ok(Arc::new(
        Registry::new(|context, event| {
            Box::pin(async move {
                let Invocation::Fetch(mut request) = event else {
                    bail!("sandbox ingress expects HTTP");
                };
                let mut url = url::Url::parse(&request.url)?;
                let path = url.path().to_string();
                let Some(path) = path.strip_prefix("/sandboxes/") else {
                    return Ok(Output::Http(
                        Response::new(404, "use /sandboxes/NAME/PATH", Vec::new())
                            .into_http(&context)?,
                    ));
                };
                let (name, path) = path.split_once('/').unwrap_or((path, ""));
                if name.is_empty() {
                    bail!("sandbox name must not be empty");
                }
                url.set_path(&format!("/{path}"));
                request.url = url.into();
                Ok(Output::Http(
                    context
                        .namespace("SANDBOXES")?
                        .get_by_name(name)
                        .fetch(&context, request)
                        .await?,
                ))
            })
        })
        .cell("Sandbox", |_, _| Ok(Arc::new(Sandbox::default())))?,
    ))
}
fn main() -> Result<()> {
    Host::fleet()
        .register("native-container", "v1", application)?
        .run()
}
