//! Event RPC failures retain execution certainty without authorizing replay.
use crate::application::event_stream::{DeliveryError, ErrorCode};
use crate::cell_dispatch::RoutedRequestError;
use cellar_logic::{routing::Attempt, RequestError};

pub(super) const HEADER: &str = "x-cellar-event-error";

pub(super) fn route_code(verdict: RequestError) -> ErrorCode {
    match verdict {
        RequestError::NodeFenced => ErrorCode::OwnershipLost,
        RequestError::PeerIncompatible => ErrorCode::UnsupportedProtocol,
        RequestError::DurabilityUnproven => ErrorCode::Ambiguous,
        _ => ErrorCode::Unavailable,
    }
}

pub(super) fn gate_code(verdict: RequestError) -> ErrorCode {
    match verdict {
        RequestError::NodeFenced => ErrorCode::OwnershipLost,
        RequestError::PeerIncompatible => ErrorCode::UnsupportedProtocol,
        _ => ErrorCode::Ambiguous,
    }
}

pub(super) fn annotate(error: anyhow::Error, code: ErrorCode) -> anyhow::Error {
    if error.downcast_ref::<DeliveryError>().is_some() {
        return error;
    }
    // Keep the original chain (including the activation transport tag), but
    // never copy its payloads, credentials or arbitrary text into the outcome.
    error.context(DeliveryError::new(
        code,
        "event RPC did not return a proven result",
    ))
}

pub(super) fn dispatched(error: anyhow::Error) -> anyhow::Error {
    let code = if let Some(verdict) = error.downcast_ref::<RoutedRequestError>() {
        gate_code(verdict.0)
    } else if error
        .downcast_ref::<crate::engine_api::CellOverloaded>()
        .is_some()
    {
        ErrorCode::Unavailable
    } else {
        // A missing result after dispatch does not establish non-execution.
        ErrorCode::Ambiguous
    };
    annotate(error, code)
}

pub(super) fn transport(error: anyhow::Error, attempt: Attempt) -> anyhow::Error {
    annotate(
        error,
        match attempt {
            Attempt::NotOwner => ErrorCode::OwnershipLost,
            Attempt::NeverConnected => ErrorCode::Unavailable,
            Attempt::Ambiguous => ErrorCode::Ambiguous,
        },
    )
}

pub(super) fn header(code: ErrorCode) -> &'static str {
    match code {
        ErrorCode::OwnershipLost => "OwnershipLost",
        ErrorCode::UnsupportedProtocol => "UnsupportedProtocol",
        ErrorCode::Unavailable => "Unavailable",
        _ => "Ambiguous",
    }
}

pub(super) fn response_code(headers: &hyper::HeaderMap, status: hyper::StatusCode) -> ErrorCode {
    // These replies come from the authenticated peer tunnel, never public HTTP.
    // Only explicit host refusals establish a definite result. Arbitrary 5xx
    // replies and malformed/missing markers cannot make a retry safe.
    match headers.get(HEADER).and_then(|value| value.to_str().ok()) {
        Some("Ambiguous") => ErrorCode::Ambiguous,
        Some("OwnershipLost") => ErrorCode::OwnershipLost,
        Some("UnsupportedProtocol") => ErrorCode::UnsupportedProtocol,
        Some("Unavailable") => ErrorCode::Unavailable,
        _ if status == hyper::StatusCode::FORBIDDEN
            || status == hyper::StatusCode::UNAUTHORIZED =>
        {
            ErrorCode::Forbidden
        }
        _ => ErrorCode::Ambiguous,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn code(error: &anyhow::Error) -> ErrorCode {
        error.downcast_ref::<DeliveryError>().unwrap().code
    }

    #[test]
    fn transport_certainty_survives_without_changing_replay_classification() {
        for (attempt, expected) in [
            (Attempt::NotOwner, ErrorCode::OwnershipLost),
            (Attempt::NeverConnected, ErrorCode::Unavailable),
            (Attempt::Ambiguous, ErrorCode::Ambiguous),
        ] {
            let failure = transport(
                super::super::remote_rpc_failure(anyhow::Error::new(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "test",
                ))),
                attempt,
            );
            assert_eq!(code(&failure), expected);
            assert!(failure
                .downcast_ref::<super::super::RemoteRpcTransportFailure>()
                .is_some());
            assert!(!failure
                .downcast_ref::<DeliveryError>()
                .unwrap()
                .is_permanent());
        }
    }

    #[test]
    fn local_gate_and_peer_gate_agree_on_typed_outcome() {
        for verdict in [
            RequestError::NodeFenced,
            RequestError::PeerIncompatible,
            RequestError::DurabilityUnproven,
            RequestError::NodeUnavailable,
        ] {
            let local = dispatched(anyhow::Error::new(RoutedRequestError(verdict)));
            let peer = super::super::peer_gate_failure(verdict, None);
            assert_eq!(code(&local), response_code(peer.headers(), peer.status()));
        }
        assert_eq!(
            code(&dispatched(anyhow::anyhow!("handler reply missing"))),
            ErrorCode::Ambiguous
        );
        let original: anyhow::Error =
            DeliveryError::new(ErrorCode::StreamFull, "bounded pressure").into();
        assert_eq!(code(&dispatched(original)), ErrorCode::StreamFull);
    }

    #[test]
    fn unknown_peer_failures_never_claim_non_execution() {
        let mut headers = hyper::HeaderMap::new();
        headers.insert(
            HEADER,
            hyper::header::HeaderValue::from_static("retry-safe"),
        );
        assert_eq!(
            response_code(&headers, hyper::StatusCode::INTERNAL_SERVER_ERROR),
            ErrorCode::Ambiguous
        );
        assert_eq!(
            response_code(&headers, hyper::StatusCode::FORBIDDEN),
            ErrorCode::Forbidden
        );
    }
}
