use crate::support;
use anyhow::Result;
use cellar::sdk::primitives::clone::StructuredValue;
use cellar::sdk::{
    workflow::{CreateOptions, Event, Steps},
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
        return support::json_status(context, 202, serde_json::json!({"id":instance.id()}));
    }
    let status = binding.get(context, id).await?.status(context).await?;
    support::json(
        context,
        serde_json::json!({"state":format!("{:?}",status.state),"output":status.output.map(|v| v.to_json()).transpose()?}),
    )
}
pub async fn run(context: Context, event: Event, steps: Steps) -> Result<StructuredValue> {
    let payload = event.payload.to_json()?;
    let prepared = steps
        .do_step("prepare-report", move |_| async move {
            StructuredValue::from_json(serde_json::json!({"report":payload,"format":"json"}))
        })
        .await?;
    steps
        .sleep(
            "delivery-delay",
            cellar::sdk::workflow::Duration::Milliseconds(100.0),
        )
        .await?;
    let id = event.instance_id;
    steps
        .do_step("publish-report", move |_| async move {
            // A workflow step may retry after an external write: use a stable object key.
            let value = prepared.to_json()?;
            context
                .blob("BLOBS")?
                .put(
                    &context,
                    &format!("report-{id}"),
                    serde_json::to_vec(&value)?,
                    Default::default(),
                )
                .await?;
            StructuredValue::from_json(
                serde_json::json!({"key":format!("report-{id}"),"report":value}),
            )
        })
        .await
}
