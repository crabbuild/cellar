use anyhow::{ensure, Context as _, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use cellar::sdk::{http::Response, Context, Request};
use futures_util::StreamExt;
use serde_json::{json, Value};

pub(crate) async fn request(context: &Context, request: Request, limit: usize) -> Result<Value> {
    headers(&request.headers)?;
    let mut stream = context.take_body(request.body)?;
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(anyhow::Error::msg)?;
        ensure!(
            bytes.len().saturating_add(chunk.len()) <= limit,
            "Python request body exceeds limit"
        );
        bytes.extend(chunk);
    }
    Ok(
        json!({ "url":request.url, "method":request.method, "headers":request.headers, "body":STANDARD.encode(bytes) }),
    )
}

pub(crate) fn input(value: &Value, limit: usize) -> Result<Request> {
    let bytes = STANDARD.decode(field(value, "body")?)?;
    ensure!(bytes.len() <= limit, "Python request body exceeds limit");
    let request_headers: Vec<(String, String)> = serde_json::from_value(value["headers"].clone())?;
    headers(&request_headers)?;
    Ok(Request {
        url: field(value, "url")?.into(),
        method: field(value, "method")?.into(),
        headers: request_headers,
        body: bytes.into(),
        entrypoint: None,
    })
}

pub(crate) fn output(
    context: &Context,
    value: &Value,
    limit: usize,
) -> Result<cellar::engine_api::HttpResponse> {
    let status = value["status"]
        .as_u64()
        .context("Python response needs an integer status")?;
    ensure!(
        (200..=599).contains(&status),
        "Python response status must be 200..599"
    );
    let bytes = STANDARD.decode(field(value, "body")?)?;
    ensure!(bytes.len() <= limit, "Python response body exceeds limit");
    let response_headers: Vec<(String, String)> = serde_json::from_value(value["headers"].clone())?;
    headers(&response_headers)?;
    Response::new(status as u16, bytes, response_headers).into_http(context)
}

pub(crate) async fn response(
    mut response: cellar::engine_api::HttpResponse,
    limit: usize,
) -> Result<Value> {
    if let Some(mut stream) = response.stream.take() {
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(anyhow::Error::msg)?;
            ensure!(
                response.body.len().saturating_add(chunk.len()) <= limit,
                "Python bridged response exceeds limit"
            );
            response.body.extend(chunk);
        }
    }
    ensure!(
        response.websocket.is_none(),
        "Python adapter does not support WebSocket upgrades"
    );
    ensure!(
        response.body.len() <= limit,
        "Python bridged response exceeds limit"
    );
    headers(&response.headers)?;
    Ok(
        json!({"status":response.status,"headers":response.headers,"body":STANDARD.encode(response.body)}),
    )
}

fn headers(headers: &[(String, String)]) -> Result<()> {
    ensure!(headers.len() <= 128, "too many Python HTTP headers");
    let bytes = headers.iter().fold(0usize, |bytes, (key, value)| {
        bytes.saturating_add(key.len()).saturating_add(value.len())
    });
    ensure!(bytes <= 32768, "Python HTTP headers exceed byte budget");
    Ok(())
}

pub(crate) fn field<'a>(value: &'a Value, name: &str) -> Result<&'a str> {
    value[name]
        .as_str()
        .with_context(|| format!("{name} must be a string"))
}

/// Match the facade's depth limit at the native effect boundary too. The JSON
/// parser's own recursion bound is deliberately not this SDK value contract.
pub(crate) fn check_depth(value: &Value) -> Result<()> {
    fn visit(value: &Value, depth: usize) -> Result<()> {
        ensure!(depth <= 64, "Python SDK value exceeds nesting limit");
        match value {
            Value::Array(values) => {
                for value in values {
                    visit(value, depth + 1)?;
                }
            }
            Value::Object(values) => {
                for value in values.values() {
                    visit(value, depth + 1)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    visit(value, 0)
}

/// JSON messages retain their shape; binary queue messages use an explicit tag.
pub(crate) fn queue_body(body: &cellar::sdk::queue::Body) -> Result<Value> {
    use cellar::sdk::queue::Body;
    Ok(match body {
        Body::Json(value) => value.clone(),
        Body::Text(value) => json!(value),
        Body::Bytes(value) => json!({"$bytes":STANDARD.encode(value)}),
        Body::JsonValue(value) | Body::Structured(value) => value.to_json()?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nested_request_body_and_header_limits_have_exact_boundaries() {
        let mut request = json!({"url":"http://cell/", "method":"POST", "headers":[], "body":STANDARD.encode(vec![0; 4096])});
        assert!(input(&request, 4096).is_ok());
        request["body"] = json!(STANDARD.encode(vec![0; 4097]));
        assert!(input(&request, 4096).is_err());
        assert!(headers(&vec![("x".into(), "y".into()); 128]).is_ok());
        assert!(headers(&vec![("x".into(), "y".into()); 129]).is_err());
        assert!(headers(&[("x".into(), "y".repeat(32767))]).is_ok());
        assert!(headers(&[("x".into(), "y".repeat(32768))]).is_err());
    }
}
