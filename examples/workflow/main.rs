use crate::support;
use anyhow::Result;
use cellar::sdk::primitives::clone::StructuredValue;
use cellar::sdk::{
    streams::{http_byte_stream, IdentityTransformStream},
    workflow::{CreateOptions, Event, Step},
    Context, Output, Request,
};

pub async fn fetch(context: &Context, request: Request) -> Result<Output> {
    support::method(&request, &["GET", "POST"])?;
    let url = url::Url::parse(&request.url)?;
    let id = support::name(url.path().strip_prefix("/workflows/").unwrap_or_default())?;
    let binding = context.workflow("REPORTS")?;
    if request.method == "POST" {
        let params = support::payload(context, request).await?;
        let instance = binding
            .create(
                context,
                CreateOptions {
                    id: Some(id.into()),
                    params: Some(StructuredValue::from_json(params)?),
                    ..Default::default()
                },
            )
            .await?;
        return support::json_status(context, 202, serde_json::json!({ "id": instance.id() }));
    }
    let status = binding.get(context, id).await?.status(context).await?;
    support::json(
        context,
        serde_json::json!({
            "state": format!("{:?}", status.state),
            "output": status.output.map(|v| v.to_json()).transpose()?,
        }),
    )
}

pub async fn run(context: Context, event: Event, step: Step) -> Result<StructuredValue> {
    let payload = event.payload.to_json()?;
    let prepared = step
        .run("prepare-report", move |_| async move {
            StructuredValue::from_json(serde_json::json!({
                "report": payload,
                "format": "json",
            }))
        })
        .await?;
    let value = prepared.to_json()?;
    let bytes = serde_json::to_vec(&value)?;
    let producer_context = context.clone();
    let report = step
        .run_stream("render-report", move |_| async move {
            let pipe = IdentityTransformStream::new();
            let writer = pipe.writable.get_writer()?;
            producer_context.wait_until_byte_stream(&pipe.readable, async move {
                writer.write(bytes).await?;
                writer.close().await?;
                Ok(())
            })?;
            Ok(pipe.readable)
        })
        .await?;
    step.sleep("delivery-delay", 100.0).await?;
    let id = event.instance_id;
    step.run("publish-report", move |_| async move {
        // A workflow step may retry after an external write: use a stable object key.
        let readable = report.into_readable(&context)?;
        context
            .blob("BLOBS")?
            .put_stream(
                &context,
                &format!("report-{id}"),
                http_byte_stream(&readable)?,
                Default::default(),
            )
            .await?;
        StructuredValue::from_json(serde_json::json!({
            "key": format!("report-{id}"),
            "report": value,
        }))
    })
    .await
}
