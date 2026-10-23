pub(crate) use cellar_guest::failure::{Validation, MIN_RESULT_BYTES};

pub(crate) fn result(
    name: &str,
    result: anyhow::Result<(serde_json::Value, Option<Vec<u8>>)>,
    limit: usize,
) -> anyhow::Result<(crate::runtime::ReplyValue, Option<Vec<u8>>)> {
    let (encoded, body, compact) = cellar_guest::failure::result_parts_with(
        name,
        result,
        limit,
        crate::runtime::prepare_reply,
    )?;
    Ok((crate::runtime::ReplyValue::new(encoded, compact), body))
}
