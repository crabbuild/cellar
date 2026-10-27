// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use serde_json::json;

const MAX_BYTES: usize = 16 * 1024;

fn metadata() -> Value {
    json!({"binding":"COUNTERS","name":"alice","method":"test.Counter/GetCount","contract_digest":"a".repeat(64)})
}

#[test]
fn rpc_control_refuses_oversized_wire_before_json_parsing() {
    let malformed = format!("{{{}", " ".repeat(MAX_BYTES));
    let error = decode(&malformed).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("RPC control metadata exceeds limit"),
        "oversized input reached the JSON parser: {error}"
    );
}

#[test]
fn rpc_control_wire_limit_includes_whitespace() {
    let mut payload = metadata().to_string();
    payload.extend(std::iter::repeat_n(' ', MAX_BYTES + 1 - payload.len()));
    assert!(
        decode(&payload).is_err(),
        "oversized raw control bypassed the compact-value limit"
    );
}

#[test]
fn rpc_control_rejects_duplicate_authority_fields() {
    let payload = metadata().to_string();
    let payload = format!("{},\"binding\":\"OTHER\"}}", payload.trim_end_matches('}'));
    assert!(
        decode(&payload).is_err(),
        "duplicate control silently replaced the original binding"
    );
}

#[test]
fn rpc_control_refuses_unknown_graphs_before_traversal() {
    let payload = metadata().to_string();
    let payload = format!(
        "{},\"extra\":{}{}}}",
        payload.trim_end_matches('}'),
        "[".repeat(1024),
        "]".repeat(1024)
    );
    let error = decode(&payload).unwrap_err();
    assert!(
        error.to_string().contains("unknown field"),
        "unknown control graph reached recursive JSON decoding: {error}"
    );
}

#[test]
fn rpc_control_accepts_the_exact_wire_limit_and_rejects_the_next_byte() {
    let mut value = metadata();
    let overhead = value.to_string().len() - value["name"].as_str().unwrap().len();
    value["name"] = Value::String("x".repeat(MAX_BYTES - overhead));
    let exact = value.to_string();
    assert_eq!(exact.len(), MAX_BYTES);
    assert_eq!(decode(&exact).unwrap(), value);
    assert!(decode(&(exact + " ")).is_err());
    assert!(validate(&json!({"binding":"C","name":"\0".repeat(4096),"method":"M","contract_digest":"a".repeat(64)})).is_err());
}

#[test]
fn rpc_control_keeps_unicode_and_optional_timeout_semantics() {
    let mut value = metadata();
    value["name"] = Value::String("\u{feff}😀".repeat(1500));
    for timeout in [Value::Null, Value::from(0), Value::from(u32::MAX)] {
        value["timeout_ms"] = timeout;
        assert_eq!(decode(&value.to_string()).unwrap(), value);
    }
    for timeout in [
        Value::from(-1),
        Value::from(u64::from(u32::MAX) + 1),
        Value::Bool(true),
    ] {
        value["timeout_ms"] = timeout;
        assert!(decode(&value.to_string()).is_err());
    }
    assert!(decode("[]").is_err());
    assert!(decode("{}").is_err());
}

#[test]
fn rpc_control_decode_credit_precedes_parsing_and_is_owned_until_drop() {
    use std::cell::Cell;

    struct Lease<'a>(&'a Cell<usize>);

    impl Drop for Lease<'_> {
        fn drop(&mut self) {
            self.0.set(self.0.get() - 1);
        }
    }
    let held = Cell::new(0);
    let payload = metadata().to_string();
    let (value, lease) = crate::operations::decode_metadata_with("cell.call", &payload, |bytes| {
        assert!(bytes > payload.len() && bytes <= MAX_BYTES * 3 + 1024);
        held.set(held.get() + 1);
        Ok(Lease(&held))
    })
    .unwrap();
    assert_eq!(value, metadata());
    assert_eq!(held.get(), 1);
    drop(lease);
    assert_eq!(held.get(), 0);
    let error = crate::operations::decode_metadata_with::<()>("cell.call", "{invalid", |_| {
        anyhow::bail!("private quota detail")
    })
    .unwrap_err();
    let failure = crate::failure::envelope("cell.call", &error);
    assert_eq!(failure["code"], "ResourceExhausted");
    assert_eq!(failure["outcome"], "not_committed");
    assert!(!failure.to_string().contains("private quota detail"));
    let gated = error.context(cellar::cell_dispatch::RoutedRequestError(
        cellar_logic::RequestError::DurabilityUnproven,
    ));
    let failure = crate::failure::envelope("cell.call", &gated);
    assert_eq!(
        failure["outcome"], "unknown",
        "an outer proof refusal inherited a control's pre-dispatch outcome"
    );
    let (_, lease) = crate::operations::decode_metadata_with(
        "storage.apply",
        &format!("{}{}", payload, " ".repeat(MAX_BYTES)),
        |_| -> anyhow::Result<()> { panic!("ordinary SDK values borrowed the RPC control pool") },
    )
    .unwrap();
    assert!(lease.is_none());
}

#[test]
fn rpc_control_refuses_oversize_before_reserving_and_refunds_parse_failure() {
    let error = crate::operations::decode_metadata_with(
        "cell.call",
        &" ".repeat(MAX_BYTES + 1),
        |_| -> anyhow::Result<()> { panic!("oversize control acquired decode capacity") },
    )
    .unwrap_err();
    assert!(error.is::<crate::failure::Validation>());
    let refusal = crate::failure::envelope("cell.call", &error);
    assert_eq!(refusal["code"], "InvalidArgument");
    assert_eq!(refusal["outcome"], "not_committed");
    let gated = error.context(cellar::cell_dispatch::RoutedRequestError(
        cellar_logic::RequestError::DurabilityUnproven,
    ));
    assert_eq!(
        crate::failure::envelope("cell.call", &gated)["outcome"],
        "unknown"
    );
    let budget = std::sync::Arc::new(tokio::sync::Semaphore::new(32 * 1024));
    assert!(
        crate::operations::decode_metadata_with("cell.call", "{invalid", |bytes| {
            Ok(budget
                .clone()
                .try_acquire_many_owned(u32::try_from(bytes)?)?)
        })
        .is_err()
    );
    assert_eq!(budget.available_permits(), 32 * 1024);
}
