pub(crate) use cellar_guest::failure::{Validation, MIN_RESULT_BYTES};

pub(crate) fn result(
    name: &str,
    result: anyhow::Result<crate::operations::Output>,
    limit: usize,
) -> anyhow::Result<(String, Option<Vec<u8>>)> {
    use crate::operations::Output;
    let result = match result {
        Ok(Output::ReadBatch(reply)) => return Ok((reply.into_string(), None)),
        Ok(Output::Value(value, body)) => Ok((value, body)),
        Err(error) => Err(error),
    };
    cellar_guest::failure::result_parts(name, result, limit)
}
