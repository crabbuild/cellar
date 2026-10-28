// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Named allocation calls retain the original parent invocation authority.
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{
    containers::{SandboxAllocation, SandboxCatalog},
    Context,
};
use serde::Deserialize;
use serde_json::{json, Value};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Start {
    name: String,
    binding: String,
    parameters: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Name {
    name: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Page {
    after: Option<String>,
    limit: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Expected {
    allocation: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Execute {
    allocation: Value,
    command: String,
    input: Value,
}

fn decode<T: serde::de::DeserializeOwned>(value: Value) -> Result<T> {
    serde_json::from_value(value).context(crate::failure::Validation)
}

/// Revisions and generations cross all language bridges as decimal strings.
/// JavaScript cannot represent the full native signed-SQLite integer range.
fn wire<T: serde::Serialize>(value: T) -> Result<Value> {
    let mut value = serde_json::to_value(value)?;
    for field in ["revision", "generation"] {
        if let Some(number) = value.get(field).and_then(Value::as_u64) {
            value[field] = json!(number.to_string());
        }
    }
    Ok(value)
}

fn allocation(mut value: Value) -> Result<SandboxAllocation> {
    for field in ["revision", "generation"] {
        let text = value
            .get(field)
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow::Error::new(crate::failure::Validation))?;
        let number = text.parse::<u64>().context(crate::failure::Validation)?;
        ensure!(
            number > 0 && number <= i64::MAX as u64 && number.to_string() == text,
            crate::failure::Validation
        );
        value[field] = json!(number);
    }
    decode(value)
}

pub(crate) async fn execute(context: &Context, operation: &str, value: Value) -> Result<Value> {
    crate::values::check_depth(&value).context(crate::failure::Validation)?;
    ensure!(
        serde_json::to_vec(&value)?.len() <= cellar::sdk::containers::MAX_SANDBOX_MESSAGE_BYTES,
        crate::failure::Validation
    );
    let catalog = SandboxCatalog::open(context)?;
    match operation {
        "sandbox.start" => {
            let input: Start = decode(value)?;
            wire(
                catalog
                    .start_child(context, &input.name, &input.binding, input.parameters)
                    .await?,
            )
        }
        "sandbox.get" => {
            let input: Name = decode(value)?;
            catalog
                .get(context, &input.name)?
                .map(wire)
                .transpose()
                .map(|value| value.unwrap_or(Value::Null))
        }
        "sandbox.list" => {
            let input: Page = decode(value)?;
            let page = catalog.list(context, input.after.as_deref(), input.limit)?;
            Ok(
                json!({"allocations":page.allocations.into_iter().map(wire).collect::<Result<Vec<_>>>()?,"next":page.next}),
            )
        }
        "sandbox.inspect" | "sandbox.destroy" => {
            let input: Expected = decode(value)?;
            let expected = allocation(input.allocation)?;
            if operation == "sandbox.inspect" {
                catalog.inspect_child(context, &expected).await
            } else {
                wire(catalog.destroy_child(context, &expected).await?)
            }
        }
        "sandbox.execute" => {
            let input: Execute = decode(value)?;
            catalog
                .execute_child(
                    context,
                    &allocation(input.allocation)?,
                    &input.command,
                    input.input,
                )
                .await
        }
        "sandbox.parent" | "sandbox.retire" | "sandbox.erase" => {
            ensure!(
                value.as_object().is_some_and(|value| value.is_empty()),
                crate::failure::Validation
            );
            match operation {
                "sandbox.parent" => catalog
                    .parent(context)?
                    .map(wire)
                    .transpose()
                    .map(|value| value.unwrap_or(Value::Null)),
                "sandbox.retire" => wire(catalog.begin_retirement(context).await?),
                _ => {
                    catalog.request_erasure(context).await?;
                    Ok(Value::Null)
                }
            }
        }
        _ => Err(anyhow::Error::new(crate::failure::Validation)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocation_wire_never_rounds_large_generations_or_accepts_extra_authority() {
        let value = json!({"parent_incarnation":"a".repeat(32),"name":"build","binding":"SANDBOXES","parameters_digest":"b".repeat(64),"generation":i64::MAX,"revision":9007199254740993_u64,"execution_name":"child","execution_scope":"Sandbox:child","state":"ready"});
        let encoded = wire(value).unwrap();
        assert_eq!(encoded["revision"], "9007199254740993");
        let decoded = allocation(encoded.clone()).unwrap();
        assert_eq!(decoded.generation, i64::MAX as u64);
        for invalid in [
            json!(1),
            json!("01"),
            json!("+1"),
            json!("0"),
            json!("9223372036854775808"),
        ] {
            let mut changed = encoded.clone();
            changed["generation"] = invalid;
            assert!(allocation(changed).is_err());
        }
        let mut forged = encoded;
        forged["authority"] = json!("parent");
        assert!(allocation(forged).is_err());
    }
}
