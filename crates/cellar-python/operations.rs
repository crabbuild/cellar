pub(crate) use cellar_guest::operations::{is_read, State};

pub(crate) enum Output {
    Value(serde_json::Value, Option<Vec<u8>>),
    ReadBatch(crate::read_batch::Reply),
}

pub(crate) async fn execute(
    context: &cellar::sdk::Context,
    state: &State,
    name: &str,
    value: serde_json::Value,
    body: Option<Vec<u8>>,
    options: &crate::runtime::Options,
) -> anyhow::Result<Output> {
    use anyhow::Context as _;
    let limits = cellar_guest::Limits {
        max_body_bytes: options.max_body_bytes,
        max_batch_operations: options.max_batch_operations,
    };
    if name == "cell.fetch" {
        let body = body
            .context("Python cell fetch requires a byte body")
            .context(crate::failure::Validation)?;
        let (value, body) =
            cellar_guest::operations::fetch_bytes(context, value, body, &limits).await?;
        return Ok(Output::Value(value, Some(body)));
    }
    if body.is_some() {
        return Err(
            anyhow::anyhow!("unexpected Python operation body").context(crate::failure::Validation)
        );
    }
    if name == "storage.read_many" {
        return crate::read_batch::execute(context, value, options).map(Output::ReadBatch);
    }
    cellar_guest::operations::execute(context, state, name, value, &limits)
        .await
        .map(|value| Output::Value(value, None))
}
