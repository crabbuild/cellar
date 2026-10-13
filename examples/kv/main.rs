use crate::support;
use anyhow::Result;
use cellar::sdk::{
    kv::{ListOptions, PutOptions, ReadType, ReadValue},
    Context, Output, Request,
};
pub async fn fetch(context: &Context, request: Request) -> Result<Output> {
    let url = url::Url::parse(&request.url)?;
    let namespace = context.kv("KV")?;
    if url.path() == "/kv" {
        support::method(&request, &["GET"])?;
        let cursor = url
            .query_pairs()
            .find(|(k, _)| k == "cursor")
            .map(|(_, v)| v.into_owned());
        let page = namespace
            .list(
                context,
                ListOptions {
                    limit: Some(20),
                    cursor,
                    ..Default::default()
                },
            )
            .await?;
        return support::json(
            context,
            serde_json::json!({"keys":page.keys.iter().map(|key| &key.name).collect::<Vec<_>>(),"cursor":page.cursor,"complete":page.list_complete}),
        );
    }
    let key = support::name(url.path().strip_prefix("/kv/").unwrap_or_default())?;
    support::method(&request, &["GET", "PUT", "DELETE"])?;
    match request.method.as_str() {
        "PUT" => {
            let bytes = support::bytes(context, request).await?;
            namespace
                .put_bytes(
                    context,
                    key,
                    bytes,
                    PutOptions {
                        expiration_ttl: Some(60.0),
                        metadata: Some(serde_json::json!({"example":"rust"})),
                        ..Default::default()
                    },
                )
                .await?;
            support::json(context, serde_json::json!({"stored":key,"ttl_seconds":60}))
        }
        "DELETE" => {
            namespace.delete(context, key).await?;
            support::text(context, 200, "deleted")
        }
        _ => match namespace.get(context, key, ReadType::Bytes).await? {
            Some(ReadValue::Bytes(bytes)) => Ok(Output::Http(
                cellar::sdk::http::Response::new(200, bytes, Vec::new()).into_http(context)?,
            )),
            _ => support::text(context, 404, "missing key"),
        },
    }
}
