//! Native HTML processing replaces the upstream Python wrapper.

use crate::support;
use anyhow::Result;
use cellar::sdk::{
    html_rewriter::{ContentType, ElementHandlers, HtmlRewriter},
    http::Response,
    Context, Output, Request,
};

pub async fn fetch(context: &Context, request: Request) -> Result<Output> {
    support::method(&request, &["POST"])?;
    let bytes = support::bytes(context, request).await?;
    let rewriter = HtmlRewriter::new().on(
        "h1",
        ElementHandlers::new().element(|_, element| {
            Box::pin(async move {
                element.set_inner_content("Processed by native Rust", ContentType::Text)?;
                Ok(())
            })
        }),
    );
    let response = rewriter.transform(
        context,
        Response::new(
            200,
            bytes,
            vec![("content-type".into(), "text/html; charset=utf-8".into())],
        ),
    )?;
    Ok(Output::Http(response.into_http(context)?))
}
