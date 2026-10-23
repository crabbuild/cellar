//! Classify only proven refusals. Unknown submitted mutations stay ambiguous.
use anyhow::Error;
use cellar::cell_dispatch::RoutedRequestError;
use cellar_logic::RequestError;
use serde_json::{json, Value};

/// Enough room for every fixed protocol failure, even at the minimum limit.
pub const MIN_RESULT_BYTES: usize = 128;

/// Result validation happens after native execution. A rejected result cannot
/// establish rollback, even when its size/depth is the reason for refusal.
pub fn result(name: &str, result: anyhow::Result<Value>, limit: usize) -> anyhow::Result<String> {
    result_parts(name, result.map(|value| (value, None)), limit).map(|(encoded, _)| encoded)
}

/// Preserve result limits and uncertain mutation outcomes for binary transports.
/// The byte body is charged as base64 without constructing the encoded copy.
pub fn result_parts(
    name: &str,
    result: anyhow::Result<(Value, Option<Vec<u8>>)>,
    limit: usize,
) -> anyhow::Result<(String, Option<Vec<u8>>)> {
    result_parts_with(name, result, limit, |_, _| None::<()>)
        .map(|(encoded, body, _)| (encoded, body))
}

/// Prepare an optional alternate representation of an accepted success value.
/// The callback runs only after the canonical JSON and binary-body limits pass;
/// its second argument is the canonical envelope length. Errors and unavailable
/// results keep their original sanitized JSON envelope and uncertain outcome.
pub fn result_parts_with<T>(
    name: &str,
    result: anyhow::Result<(Value, Option<Vec<u8>>)>,
    limit: usize,
    prepare: impl FnOnce(&Value, usize) -> Option<T>,
) -> anyhow::Result<(String, Option<Vec<u8>>, Option<T>)> {
    let result = result.and_then(|(value, body)| {
        crate::values::check_depth(&value)?;
        Ok((value, body))
    });
    let (value, body, success) = match result {
        Ok((value, body)) => (json!({"ok":true,"value":value}), body, true),
        Err(error) => (envelope(name, &error), None, false),
    };
    let encoded = encode(&value, limit).and_then(|encoded| {
        anyhow::ensure!(
            crate::values::envelope_size(encoded.len(), body.as_ref().map_or(0, Vec::len))?
                <= limit,
            "SDK result exceeds value limit"
        );
        Ok(encoded)
    });
    match encoded {
        Ok(encoded) => {
            let alternate = success
                .then(|| prepare(&value["value"], encoded.len()))
                .flatten();
            Ok((encoded, body, alternate))
        }
        Err(_) => encode(
            &envelope(name, &anyhow::anyhow!("SDK result unavailable")),
            limit,
        )
        .map(|encoded| (encoded, None, None)),
    }
}

fn encode(value: &Value, limit: usize) -> anyhow::Result<String> {
    struct Bounded {
        bytes: Vec<u8>,
        limit: usize,
    }

    impl std::io::Write for Bounded {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
                return Err(std::io::Error::other("SDK result exceeds value limit"));
            }
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut output = Bounded {
        bytes: Vec::new(),
        limit,
    };
    serde_json::to_writer(&mut output, value)?;
    Ok(String::from_utf8(output.bytes)?)
}

/// Attached only to validation completed before any native effect starts.
#[derive(Debug)]
pub struct Validation;

impl std::fmt::Display for Validation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("invalid Cellar operation")
    }
}

impl std::error::Error for Validation {}

pub fn envelope(name: &str, error: &Error) -> Value {
    let (code, outcome) = if error.is::<crate::storage::Conflict>() {
        ("Conflict", "not_committed")
    } else if error.is::<Validation>() {
        ("ValidationError", "not_committed")
    } else if let Some(RoutedRequestError(verdict)) = error.downcast_ref::<RoutedRequestError>() {
        match verdict {
            RequestError::NodeFenced => ("OwnershipLost", "unknown"),
            // A callee may propagate its nested call's admission refusal after
            // committing its own writes. The verdict has no call provenance.
            RequestError::CapacityExhausted => ("Overloaded", "unknown"),
            RequestError::DurabilityUnproven => ("Ambiguous", "unknown"),
            _ => submitted(name),
        }
    } else if cellar::sdk::ApplicationOverloaded::matches(error)
        || error.chain().any(|cause| {
            cause
                .downcast_ref::<cellar::engine_api::CellOverloaded>()
                .is_some()
                || cause
                    .to_string()
                    .contains(cellar::engine_api::CELL_OVERLOAD_ERROR_MARKER)
        })
    {
        // A parent may have committed before its nested invocation was refused.
        ("Overloaded", "unknown")
    } else {
        submitted(name)
    };
    // Never copy backend exception text, SQL, request values or local paths.
    json!({"ok":false,"error":code,"code":code,"outcome":outcome})
}

fn submitted(name: &str) -> (&'static str, &'static str) {
    if crate::operations::is_read(name) {
        ("HostError", "unknown")
    } else {
        ("Ambiguous", "unknown")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alternate_values_cannot_bypass_result_limits_or_failure_classification() {
        let value = json!({"present": true, "value": "é\"".repeat(100)});
        let encoded = result("storage.read", Ok(value.clone()), usize::MAX).unwrap();
        let calls = std::cell::Cell::new(0);
        let prepare = |actual: &Value, length| {
            calls.set(calls.get() + 1);
            assert_eq!(actual, &value);
            assert_eq!(length, encoded.len());
            Some(42)
        };
        let accepted = result_parts_with(
            "storage.read",
            Ok((value.clone(), None)),
            encoded.len(),
            prepare,
        )
        .unwrap();
        assert_eq!(accepted, (encoded.clone(), None, Some(42)));
        assert_eq!(calls.get(), 1);
        let refused = result_parts_with(
            "storage.sql",
            Ok((value, None)),
            encoded.len() - 1,
            |_, _| -> Option<()> { panic!("oversized result reached alternate encoder") },
        )
        .unwrap();
        assert!(refused.1.is_none() && refused.2.is_none());
        assert_eq!(
            serde_json::from_str::<Value>(&refused.0).unwrap()["code"],
            "Ambiguous"
        );
        let failure = result_parts_with(
            "storage.read",
            Err(anyhow::anyhow!("private")),
            MIN_RESULT_BYTES,
            |_, _| -> Option<()> { panic!("failure reached alternate encoder") },
        )
        .unwrap();
        assert!(failure.2.is_none());
        assert!(!failure.0.contains("private"));
        let binary = result_parts_with(
            "cell.fetch",
            Ok((Value::Null, Some(vec![0; 1024]))),
            MIN_RESULT_BYTES,
            |_, _| -> Option<()> { panic!("oversized body reached alternate encoder") },
        )
        .unwrap();
        assert!(binary.1.is_none() && binary.2.is_none());
        assert_eq!(
            serde_json::from_str::<Value>(&binary.0).unwrap()["code"],
            "Ambiguous"
        );
    }

    #[test]
    fn binary_results_preserve_exact_encoded_limits_and_uncertainty() {
        use base64::{engine::general_purpose::STANDARD, Engine as _};
        for size in [0, 1, 2, 3, 4095, 4096, 4097] {
            let bytes: Vec<u8> = (0..size).map(|value| value as u8).collect();
            let metadata = json!({"status":200,"headers":[["x","é\"".repeat(40)]],"body":""});
            let mut wire = metadata.clone();
            wire["body"] = json!(STANDARD.encode(&bytes));
            let legacy = result("cell.fetch", Ok(wire), usize::MAX).unwrap();
            let (encoded, body) = result_parts(
                "cell.fetch",
                Ok((metadata.clone(), Some(bytes.clone()))),
                legacy.len(),
            )
            .unwrap();
            assert_eq!(body.as_deref(), Some(bytes.as_slice()));
            assert_eq!(
                serde_json::from_str::<Value>(&encoded).unwrap(),
                json!({"ok":true,"value":metadata})
            );
            assert_eq!(
                crate::values::envelope_size(encoded.len(), bytes.len()).unwrap(),
                legacy.len()
            );

            let (encoded, body) =
                result_parts("cell.fetch", Ok((metadata, Some(bytes))), legacy.len() - 1).unwrap();
            assert!(body.is_none(), "refused result retained its binary payload");
            let value: Value = serde_json::from_str(&encoded).unwrap();
            assert_eq!(
                value,
                json!({"ok":false,"error":"Ambiguous","code":"Ambiguous","outcome":"unknown"})
            );
            assert!(encoded.len() <= MIN_RESULT_BYTES);
        }
    }

    #[test]
    fn invalid_binary_metadata_drops_body_without_claiming_rollback() {
        let mut value = Value::Null;
        for _ in 0..65 {
            value = json!([value]);
        }
        let (encoded, body) =
            result_parts("cell.fetch", Ok((value, Some(vec![42; 4096]))), 65536).unwrap();
        assert!(body.is_none());
        let value: Value = serde_json::from_str(&encoded).unwrap();
        assert_eq!(value["code"], "Ambiguous");
        assert_eq!(value["outcome"], "unknown");
    }

    #[test]
    fn result_limits_preserve_uncertainty_and_exact_utf8_boundaries() {
        let value = json!("é\"".repeat(100));
        let length = serde_json::to_vec(&json!({"ok":true,"value":value}))
            .unwrap()
            .len();
        assert_eq!(
            result("storage.read", Ok(value.clone()), length)
                .unwrap()
                .len(),
            length
        );
        for (name, code) in [("storage.read", "HostError"), ("storage.sql", "Ambiguous")] {
            let encoded = result(name, Ok(value.clone()), length - 1).unwrap();
            let failure: Value = serde_json::from_str(&encoded).unwrap();
            assert_eq!(failure["code"], code);
            assert_eq!(failure["outcome"], "unknown");
            assert!(encoded.len() <= MIN_RESULT_BYTES);
            assert!(!encoded.contains('é'));
        }
        let mut value = Value::Null;
        for _ in 0..64 {
            value = json!([value]);
        }
        assert!(serde_json::from_str::<Value>(
            &result("storage.read", Ok(value.clone()), 4096).unwrap()
        )
        .unwrap()["ok"]
            .as_bool()
            .unwrap());
        let failure: Value =
            serde_json::from_str(&result("storage.read", Ok(json!([value])), 4096).unwrap())
                .unwrap();
        assert_eq!(failure["code"], "HostError");
        for verdict in [
            RequestError::NodeFenced,
            RequestError::CapacityExhausted,
            RequestError::DurabilityUnproven,
        ] {
            let encoded = result(
                "cell.fetch",
                Err(Error::new(RoutedRequestError(verdict))),
                MIN_RESULT_BYTES,
            )
            .unwrap();
            assert!(encoded.len() <= MIN_RESULT_BYTES);
            assert_eq!(
                serde_json::from_str::<Value>(&encoded).unwrap()["outcome"],
                "unknown"
            );
        }
    }

    #[test]
    fn typed_verdicts_survive_context_without_guessing_rollback() {
        for (verdict, code, outcome) in [
            (RequestError::NodeFenced, "OwnershipLost", "unknown"),
            (RequestError::CapacityExhausted, "Overloaded", "unknown"),
            (RequestError::DurabilityUnproven, "Ambiguous", "unknown"),
            (RequestError::RuntimeFailed, "Ambiguous", "unknown"),
        ] {
            let error = Error::new(RoutedRequestError(verdict)).context("private path and input");
            let value = envelope("cell.fetch", &error);
            assert_eq!(value["code"], code);
            assert_eq!(value["outcome"], outcome);
            assert!(!value.to_string().contains("private"));
        }
        let error = anyhow::anyhow!("unknown committed response loss");
        assert_eq!(envelope("kv.put", &error)["code"], "Ambiguous");
        assert_eq!(envelope("kv.get", &error)["code"], "HostError");
        assert_eq!(
            envelope("storage.apply", &error.context(Validation))["outcome"],
            "not_committed"
        );
    }

    #[test]
    fn application_overload_never_guesses_a_parent_rollback() {
        let refused = Error::new(cellar::sdk::ApplicationOverloaded).context("private parent");
        let value = envelope("cell.fetch", &refused);
        assert_eq!(
            value,
            json!({"ok":false,"error":"Overloaded","code":"Overloaded","outcome":"unknown"})
        );
        let gated = refused.context(RoutedRequestError(RequestError::DurabilityUnproven));
        assert_eq!(envelope("cell.fetch", &gated)["code"], "Ambiguous");
    }

    #[test]
    fn cell_capacity_refusal_stays_typed_across_guest_and_peer_boundaries() {
        for refused in [
            Error::new(cellar::engine_api::CellOverloaded).context("private caller"),
            anyhow::anyhow!(
                "peer handler failed: {}",
                cellar::engine_api::CellOverloaded
            ),
        ] {
            assert_eq!(
                envelope("cell.fetch", &refused),
                json!({"ok":false,"error":"Overloaded","code":"Overloaded","outcome":"unknown"})
            );
            let gated = refused.context(RoutedRequestError(RequestError::DurabilityUnproven));
            assert_eq!(envelope("cell.fetch", &gated)["code"], "Ambiguous");
        }
        assert_eq!(
            envelope("cell.fetch", &anyhow::anyhow!("cell overload"))["code"],
            "Ambiguous"
        );
    }
}
