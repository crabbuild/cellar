use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{Context, Request};
pub(crate) use cellar_guest::values::{envelope_size, queue_body};
use serde_json::Value;

pub(crate) async fn request(
    context: &Context,
    request: Request,
    limit: usize,
) -> Result<(Value, Vec<u8>)> {
    cellar_guest::values::request_parts(context, request, limit)
        .await
        .map_err(|error| {
            if error.is::<cellar_guest::values::RequestBodyLimit>() {
                anyhow::anyhow!("Python request body exceeds limit")
            } else {
                error
            }
        })
}

pub(crate) fn output(
    context: &Context,
    value: &Value,
    body: Option<Vec<u8>>,
    limit: usize,
) -> Result<cellar::engine_api::HttpResponse> {
    ensure!(
        cellar_guest::values::field(value, "body")?.is_empty(),
        "Python response metadata contains a body"
    );
    let bytes = body.context("Python response needs a byte body")?;
    cellar_guest::values::output_body(context, value, bytes, limit)
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    use serde_json::json;

    #[test]
    fn binary_http_charge_matches_encoded_envelope() {
        for size in [0, 1, 2, 3, 4, 255, 256, 257, 4095, 4096, 4097] {
            let body: Vec<u8> = (0..size).map(|byte| byte as u8).collect();
            for metadata in [
                json!({"url":"http://cell/é", "headers":[["x", "\"\\"]], "body":""}),
                json!({"status":200, "headers":[["x", "é\"\\"]], "body":""}),
            ] {
                let mut encoded = metadata.clone();
                encoded["body"] = json!(STANDARD.encode(&body));
                assert_eq!(
                    envelope_size(serde_json::to_vec(&metadata).unwrap().len(), body.len())
                        .unwrap(),
                    serde_json::to_vec(&encoded).unwrap().len()
                );
            }
        }
        assert!(envelope_size(usize::MAX, 1).is_err());
        assert!(envelope_size(0, usize::MAX).is_err());
        assert!(envelope_size(0, usize::MAX / 4 * 3 + 1).is_err());
    }
}
