use crate::support;
use anyhow::Result;
use cellar::sdk::{
    blob::{GetOptions, GetResult, WriteOptions},
    Context, Output, Request,
};
pub async fn fetch(context: &Context, request: Request) -> Result<Output> {
    let url = url::Url::parse(&request.url)?;
    let key = support::name(url.path().strip_prefix("/blobs/").unwrap_or_default())?;
    let bucket = context.blob("BLOBS")?;
    support::method(&request, &["GET", "PUT", "DELETE"])?;
    match request.method.as_str() {
        "PUT" => {
            let bytes = support::bytes(context, request).await?;
            let object = bucket
                .put(context, key, bytes, WriteOptions::default())
                .await?;
            support::json(context, serde_json::json!({"object":object}))
        }
        "DELETE" => {
            bucket.delete(context, &[key.to_string()]).await?;
            support::text(context, 200, "deleted")
        }
        _ => match bucket.get(context, key, GetOptions::default()).await? {
            GetResult::Body(body) => Ok(Output::Http(body.into_response(
                context,
                200,
                Vec::new(),
            )?)),
            GetResult::Missing => support::text(context, 404, "missing object"),
            GetResult::Unmet(_) => support::text(context, 412, "condition failed"),
        },
    }
}
