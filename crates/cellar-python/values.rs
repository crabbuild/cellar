use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{Context, Request};
pub(crate) use cellar_guest::values::queue_body;
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

/// Preserve encoded-envelope admission while carrying an HTTP body as bytes.
/// Base64 has no JSON escapes, so its length can be charged without encoding it.
pub(crate) fn envelope_size(metadata: usize, body: usize) -> Result<usize> {
    body.checked_add(2)
        .map(|rounded| rounded / 3)
        .and_then(|groups| groups.checked_mul(4))
        .and_then(|encoded| metadata.checked_add(encoded))
        .context("Python HTTP envelope size overflow")
}

// Every admitted direct response has a three-digit status and an empty metadata
// body. Header order changes no length. Retain CPython ensure_ascii charging,
// including DEL, control escapes and two UTF-16 escapes for astral characters.
pub(crate) const HTTP_METADATA_BASE_BYTES: usize =
    br#"{"status":200,"headers":[],"body":""}"#.len();

pub(crate) fn response_header_size(name: &str, value: &str) -> Result<usize> {
    fn string_size(value: &str) -> Result<usize> {
        value.chars().try_fold(2usize, |size, ch| {
            let escaped = match ch {
                '"' | '\\' | '\u{8}' | '\u{c}' | '\n' | '\r' | '\t' => 2,
                '\u{20}'..='\u{7e}' => 1,
                '\0'..='\u{ffff}' => 6,
                _ => 12,
            };
            size.checked_add(escaped)
                .context("Python HTTP metadata size overflow")
        })
    }
    string_size(name)?
        .checked_add(string_size(value)?)
        .and_then(|bytes| bytes.checked_add(3)) // brackets and comma
        .context("Python HTTP metadata size overflow")
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    use serde_json::json;

    #[test]
    fn direct_metadata_charge_matches_python_ascii_json() {
        // Golden strings use CPython json.dumps(..., separators=(",", ":"))
        // with its default ensure_ascii=True, not serde_json's UTF-8 output.
        let cases = [
            ("", "", r#"["",""]"#),
            ("x", "plain /~", r#"["x","plain /~"]"#),
            (
                "x",
                "\0\u{8}\u{c}\n\r\t\"\\\u{7f}é☃😀",
                r#"["x","\u0000\b\f\n\r\t\"\\\u007f\u00e9\u2603\ud83d\ude00"]"#,
            ),
        ];
        for (name, value, encoded) in cases {
            assert_eq!(response_header_size(name, value).unwrap(), encoded.len());
        }
        let encoded = r#"{"status":599,"headers":[["x","plain /~"],["",""]],"body":""}"#;
        assert_eq!(
            HTTP_METADATA_BASE_BYTES
                + 1
                + response_header_size("x", "plain /~").unwrap()
                + response_header_size("", "").unwrap(),
            encoded.len()
        );
    }

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
