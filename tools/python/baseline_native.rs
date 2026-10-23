//! Native reference with the same checked update and HTTP contract as Python.
use super::*;
use cellar::sdk::{
    http::{Response, Url},
    Cell, HandlerFuture, Invocation, Output, Registry, Request,
};

struct Entity;

async fn echo(context: &Context, request: Request) -> Result<Output> {
    let body = request.into_body_reader(context)?.bytes(context).await?;
    Ok(Output::Http(
        Response::new(
            200,
            body,
            vec![("content-type".into(), "application/octet-stream".into())],
        )
        .into_http(context)?,
    ))
}

impl Cell for Entity {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = event else {
                anyhow::bail!("expected HTTP");
            };
            if Url::new(&request.url, None)?.pathname() == "/cell-echo" && request.method == "POST"
            {
                return echo(&context, request).await;
            }
            let storage = context.storage()?;
            if request.method == "GET" {
                if let Some((_, query)) = request.url.split_once('?') {
                    let count = match query {
                        "fields=2&batch=0" | "fields=2&batch=1" => 2,
                        "fields=8&batch=0" | "fields=8&batch=1" => 8,
                        _ => anyhow::bail!("invalid measured read query"),
                    };
                    let mut total = 0_u64;
                    for index in 0..count {
                        let key = if index == 0 {
                            "count".to_owned()
                        } else {
                            format!("field-{index}")
                        };
                        total += storage
                            .get_json(&key)?
                            .and_then(|v| v.as_u64())
                            .context("missing measured value")?;
                    }
                    return Ok(Output::Http(
                        Response::from_json(&json!({"value":total}))?.into_http(&context)?,
                    ));
                }
            }
            let previous = storage.get_json("count")?;
            for index in 1..8 {
                ensure!(
                    storage.get_json(&format!("field-{index}"))? == previous,
                    "recovered fields differ"
                );
            }
            let mut value = match &previous {
                None => 0,
                Some(value) => value
                    .as_u64()
                    .context("stored counter must be an unsigned integer")?,
            };
            match request.method.as_str() {
                "POST" => {
                    value = value.checked_add(1).context("counter overflow")?;
                    storage.transaction_sync(|storage| {
                        ensure!(
                            storage.get_json("count")? == previous,
                            "checked update conflict"
                        );
                        for index in 1..8 {
                            storage.put_json(&format!("field-{index}"), &json!(value))?;
                        }
                        storage.put_json("count", &json!(value))
                    })?;
                }
                "GET" => {}
                _ => anyhow::bail!("expected GET or POST"),
            }
            Ok(Output::Http(
                Response::from_json(&json!({"value":value}))?.into_http(&context)?,
            ))
        })
    }
}

pub(super) fn application() -> Result<Arc<dyn Application>> {
    Ok(Arc::new(
        Registry::new(|context, event| {
            Box::pin(async move {
                let Invocation::Fetch(request) = event else {
                    anyhow::bail!("expected HTTP");
                };
                let url = Url::new(&request.url, None)?;
                if url.pathname() == "/echo" && request.method == "POST" {
                    return echo(&context, request).await;
                }
                if url.pathname() == "/cell-echo" && request.method == "POST" {
                    return Ok(Output::Http(
                        context
                            .namespace("ENTITIES")?
                            .get_by_name("echo")
                            .fetch(&context, request)
                            .await?,
                    ));
                }
                let name = url
                    .pathname()
                    .strip_prefix("/cells/")
                    .filter(|name| !name.is_empty())
                    .context("expected /cells/NAME")?
                    .to_owned();
                let cell = context.namespace("ENTITIES")?.get_by_name(&name);
                Ok(Output::Http(cell.fetch(&context, request).await?))
            })
        })
        .cell("Entity", |_, _| Ok(Arc::new(Entity)))?,
    ))
}
