// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Fixed entered-handler categories carry no evidence about committed effects.
use cellar::sdk::rpc::{Code, Failure};

const PAYLOADS: &[(&str, Code)] = &[
    (r#"{"rpc_error":"InvalidArgument"}"#, Code::InvalidArgument),
    (r#"{"rpc_error":"MethodNotFound"}"#, Code::MethodNotFound),
    (
        r#"{"rpc_error":"ContractMismatch"}"#,
        Code::ContractMismatch,
    ),
    (
        r#"{"rpc_error":"ResourceExhausted"}"#,
        Code::ResourceExhausted,
    ),
    (r#"{"rpc_error":"ContextExpired"}"#, Code::ContextExpired),
    (r#"{"rpc_error":"Cancelled"}"#, Code::Cancelled),
    (
        r#"{"rpc_error":"DeadlineExceeded"}"#,
        Code::DeadlineExceeded,
    ),
    (r#"{"rpc_error":"Unavailable"}"#, Code::Unavailable),
    (r#"{"rpc_error":"OwnershipLost"}"#, Code::OwnershipLost),
    (
        r#"{"rpc_error":"DurabilityUnproven"}"#,
        Code::DurabilityUnproven,
    ),
    (r#"{"rpc_error":"InvalidResponse"}"#, Code::InvalidResponse),
    (r#"{"rpc_error":"Internal"}"#, Code::Internal),
];

/// Return only a canonical, bounded category. Never forward exception text,
/// extra fields, guest-claimed outcomes or arbitrary JSON to the host channel.
pub fn payload(value: &str) -> Option<&'static str> {
    PAYLOADS
        .iter()
        .find(|(candidate, _)| *candidate == value)
        .map(|(value, _)| *value)
}

/// Called only for entered typed handlers. A category is not admission proof;
/// even a forged category can produce only an unknown effect outcome.
pub fn failure(value: &str) -> Failure {
    Failure::new(
        PAYLOADS
            .iter()
            .find(|(candidate, _)| *candidate == value)
            .map_or(Code::Internal, |(_, code)| *code),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use cellar::sdk::rpc::EffectOutcome;

    #[test]
    fn every_fixed_category_has_unknown_effects() {
        for (encoded, code) in PAYLOADS {
            assert_eq!(payload(encoded), Some(*encoded));
            assert_eq!(failure(encoded).code(), *code);
            assert_eq!(failure(encoded).outcome(), EffectOutcome::Unknown);
        }
    }

    #[test]
    fn forged_outcomes_and_unrecognized_payloads_cannot_establish_rollback() {
        for value in [
            r#"{"rpc_error":"MethodNotFound","outcome":"not_committed"}"#,
            r#"{"rpc_error":"Unspecified"}"#,
            r#"{"rpc_error":"private-text"}"#,
            "private-text",
            "null",
        ] {
            assert_eq!(payload(value), None);
            assert_eq!(failure(value).code(), Code::Internal);
            assert_eq!(failure(value).outcome(), EffectOutcome::Unknown);
        }
    }
}
