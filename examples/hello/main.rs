use crate::support;
use anyhow::Result;
use cellar::sdk::{Context, Output, Request};

pub fn fetch(context: &Context, request: Request) -> Result<Output> {
    support::method(&request, &["GET"])?;
    support::json(
        context,
        serde_json::json!({
            "hello": "Cellar",
            "language": "Rust",
        }),
    )
}
