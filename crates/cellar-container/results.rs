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
        ConnectWebSocket if value["upgraded"] == false => {
            binary(&value["response"]["body"], limit)?;
        }
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
}
