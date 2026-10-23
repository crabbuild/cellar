pub(crate) use cellar_guest::values::{output, queue_body};

pub(crate) async fn request(
    context: &cellar::sdk::Context,
    request: cellar::sdk::Request,
    limit: usize,
) -> anyhow::Result<serde_json::Value> {
    cellar_guest::values::request(context, request, limit)
        .await
        .map_err(|error| {
            if error.is::<cellar_guest::values::RequestBodyLimit>() {
                anyhow::anyhow!("Python request body exceeds limit")
            } else {
                error
            }
        })
}
