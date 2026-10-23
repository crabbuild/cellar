//! Classify only proven refusals. Unknown submitted mutations stay ambiguous.
use anyhow::Error;
use cellar::cell_dispatch::RoutedRequestError;
use cellar_logic::RequestError;
use serde_json::{json, Value};

/// Enough room for every fixed protocol failure, even at the minimum limit.
pub(crate) const MIN_RESULT_BYTES: usize = 128;

/// Result validation happens after native execution. A rejected result cannot
/// establish rollback, even when its size/depth is the reason for refusal.
pub(crate) fn result(
    name: &str,
    result: anyhow::Result<Value>,
    limit: usize,
) -> anyhow::Result<String> {
    let result = result.and_then(|value| {
        crate::values::check_depth(&value)?;
        Ok(value)
    });
    let value = match result {
        Ok(value) => json!({"ok":true,"value":value}),
        Err(error) => envelope(name, &error),
    };
    match encode(&value, limit) {
        Ok(encoded) => Ok(encoded),
        Err(_) => encode(
            &envelope(name, &anyhow::anyhow!("SDK result unavailable")),
            limit,
        ),
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
pub(crate) struct Validation;

impl std::fmt::Display for Validation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("invalid Cellar operation")
    }
}

impl std::error::Error for Validation {}

pub(crate) fn envelope(name: &str, error: &Error) -> Value {
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
    } else if cellar::sdk::ApplicationOverloaded::matches(error) {
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
}
