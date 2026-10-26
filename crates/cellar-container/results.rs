// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Validate provider results after dispatch without claiming effect rollback.
use crate::Operation;
use anyhow::{ensure, Context as _, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde_json::Value;

fn binary(value: &Value, limit: usize) -> Result<usize> {
    let encoded = value
        .as_str()
        .context("provider binary result must be base64")?;
    ensure!(
        encoded.len() <= limit.div_ceil(3).saturating_mul(4),
        "provider binary result exceeds limit"
    );
    let bytes = STANDARD.decode(encoded)?;
    ensure!(bytes.len() <= limit, "provider binary result exceeds limit");
    Ok(bytes.len())
}

pub(crate) fn validate(operation: Operation, value: &Value, limit: usize) -> Result<()> {
    use Operation::*;
    match operation {
        ReadStdout | ReadStderr | ReadSocket | ReadFile => {
            binary(value, limit)?;
        }
        WriteFile => ensure!(value.is_null(), "file write result must be null"),
        StatFile => crate::files::validate_metadata(value, limit)?,
        ExecOutput => {
            let stdout = binary(&value["stdout"], limit)?;
            binary(&value["stderr"], limit - stdout)?;
            ensure!(
                value["exit_code"].as_i64().is_some(),
                "invalid provider exit code"
            );
        }
        Fetch => {
            binary(&value["body"], limit)?;
        }
        FetchStream => {
            binary(&value["body"], limit)?;
            ensure!(
                value["status"]
                    .as_u64()
                    .is_some_and(|status| (200..=599).contains(&status)),
                "invalid response status"
            );
            let headers: Vec<(String, String)> = serde_json::from_value(value["headers"].clone())?;
            crate::values::headers(&headers)?;
            let stream = value
                .get("stream")
                .context("missing response stream handle")?;
            ensure!(
                stream.is_null() || stream.as_str().is_some_and(|id| !id.is_empty()),
                "invalid response stream handle"
            );
        }
        ReadResponse => {
            let size = binary(&value["body"], limit)?;
            let done = value["done"]
                .as_bool()
                .context("invalid response completion")?;
            ensure!(!done || size == 0, "completed response has body bytes");
        }
        CancelResponse => ensure!(value.is_null(), "response cancellation must return null"),
        ConnectWebSocket if value["upgraded"] == true => {
            ensure!(
                value["id"].as_str().is_some_and(|id| !id.is_empty()),
                "invalid WebSocket handle"
            );
            ensure!(value["protocol"].is_string(), "invalid WebSocket protocol");
            let headers: Vec<(String, String)> = serde_json::from_value(value["headers"].clone())?;
            crate::values::headers(&headers)?;
        }
        ConnectWebSocket if value["upgraded"] == false => {
            binary(&value["response"]["body"], limit)?;
        }
        ConnectWebSocket => anyhow::bail!("invalid WebSocket upgrade result"),
        ReceiveWebSocket if value["type"] == "binary" => {
            binary(&value["data"], limit)?;
        }
        ReceiveWebSocket if value["type"] == "text" => {
            ensure!(
                value["text"]
                    .as_str()
                    .is_some_and(|text| text.len() <= limit),
                "provider text result exceeds limit"
            );
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Validation;
    use serde_json::json;

    #[test]
    fn upgraded_sockets_require_headers_without_pre_effect_failure_provenance() {
        let valid = json!({"upgraded":true,"id":"socket-1","protocol":"","headers":[["upgrade","websocket"]]});
        assert!(validate(Operation::ConnectWebSocket, &valid, 256).is_ok());
        for invalid in [
            json!({"upgraded":true,"id":"socket-1","protocol":""}),
            json!({"upgraded":true,"id":"","protocol":"","headers":[]}),
            json!({"upgraded":"true","id":"socket-1","protocol":"","headers":[]}),
        ] {
            let error = validate(Operation::ConnectWebSocket, &invalid, 256).unwrap_err();
            assert!(!error.is::<Validation>());
        }
    }

    #[test]
    fn provider_results_share_combined_binary_budget_without_refusal_provenance() {
        let value =
            json!({"exit_code":7,"stdout":STANDARD.encode([0,255]),"stderr":STANDARD.encode([42])});
        assert!(validate(Operation::ExecOutput, &value, 3).is_ok());
        let error = validate(Operation::ExecOutput, &value, 2).unwrap_err();
        assert!(!error.is::<Validation>());
        assert!(validate(Operation::ReadSocket, &json!("!!!!"), 3).is_err());
        assert!(validate(
            Operation::ReceiveWebSocket,
            &json!({"type":"text","text":"é"}),
            1
        )
        .is_err());
    }

    #[test]
    fn provider_response_stream_frames_require_bounded_bytes_and_explicit_eof() {
        assert!(validate(Operation::ReadResponse, &json!({"done":false,"body":""}), 0).is_ok());
        for frame in [
            json!({"body":""}),
            json!({"done":true,"body":"AA=="}),
            json!({"done":false,"body":"!!!!"}),
            json!({"done":false,"body":"AAAA"}),
        ] {
            assert!(validate(Operation::ReadResponse, &frame, 2).is_err());
        }
        let mut head = json!({"status":200,"headers":[],"body":"","stream":"1"});
        assert!(validate(Operation::FetchStream, &head, 2).is_ok());
        let mut missing = head.clone();
        missing.as_object_mut().unwrap().remove("stream");
        assert!(validate(Operation::FetchStream, &missing, 2).is_err());
        head["stream"] = json!(false);
        assert!(validate(Operation::FetchStream, &head, 2).is_err());
        head["stream"] = Value::Null;
        head["status"] = json!(101);
        assert!(validate(Operation::FetchStream, &head, 2).is_err());
    }
}
