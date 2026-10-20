//! Classify only proven refusals. Unknown submitted mutations stay ambiguous.
use anyhow::Error;
use cellar::cell_dispatch::RoutedRequestError;
use cellar_logic::RequestError;
use serde_json::{json, Value};

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
}
