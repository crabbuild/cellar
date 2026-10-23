pub(crate) use cellar_guest::operations::{is_read, State};

pub(crate) async fn execute(
    context: &cellar::sdk::Context,
    state: &State,
    name: &str,
    value: serde_json::Value,
    options: &crate::runtime::Options,
) -> anyhow::Result<serde_json::Value> {
    cellar_guest::operations::execute(
        context,
        state,
        name,
        value,
        &cellar_guest::Limits {
            max_body_bytes: options.max_body_bytes,
            max_batch_operations: options.max_batch_operations,
        },
    )
    .await
}
