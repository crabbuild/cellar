use crate::support;
use anyhow::Result;
use cellar::sdk::{http::Headers, Context, Output, Request};
use sha2::{Digest, Sha256};

pub fn fetch(context: &Context, request: Request) -> Result<Output> {
    support::method(&request, &["GET"])?;
    let url = request.parsed_url()?;
    let headers = Headers::from_pairs([("x-example", "rust")])?;
    support::json(
        context,
        serde_json::json!({
            "pathname": url.pathname(),
            "header": headers.get("x-example")?,
            "sha256": format!("{:x}", Sha256::digest(b"cellar")),
        }),
    )
}
