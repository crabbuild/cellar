// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

#[test]
fn typed_shutdown_results_require_an_exact_unknown_failure() {
    for code in [
        "Unavailable",
        "ContextExpired",
        "Cancelled",
        "DeadlineExceeded",
    ] {
        let value = json!({"code":code,"outcome":"unknown"});
        assert_eq!(shutdown_reply(Ok(value.clone()), true).unwrap(), value);
    }
    for value in [
        json!({"code":"Unavailable","outcome":"not_committed"}),
        json!({"code":"Unavailable","outcome":"Unknown"}),
        json!({"code":"Internal","outcome":"unknown"}),
        json!({"code":"PrivateMessage","outcome":"unknown"}),
        json!({"code":"Unavailable","outcome":"unknown","value":"committed"}),
        json!({"value":"committed"}),
    ] {
        assert!(shutdown_reply(Ok(value), false).is_err());
    }
}

fn http(status: reqwest::StatusCode, body: Value) -> anyhow::Error {
    HttpFailure {
        status,
        body: body.to_string(),
    }
    .into()
}

#[test]
fn only_a_local_server_cut_may_record_a_missing_rpc_reply() {
    let result = shutdown_reply(
        Err(http(
            reqwest::StatusCode::SERVICE_UNAVAILABLE,
            json!({"error":"closing"}),
        )),
        false,
    )
    .unwrap();
    assert_eq!(
        result,
        json!({"http_status":503,"rpc_reply_unavailable_after_submission":true,"outcome":"unknown"})
    );
    assert!(shutdown_reply(
        Err(http(
            reqwest::StatusCode::SERVICE_UNAVAILABLE,
            json!({"error":"closing"})
        )),
        true
    )
    .is_err());
    assert!(shutdown_reply(
        Err(http(
            reqwest::StatusCode::BAD_REQUEST,
            json!({"error":"bad input"})
        )),
        false
    )
    .is_err());
    assert!(shutdown_reply(
        Err(http(
            reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            json!({"outcome":"not_committed"})
        )),
        false
    )
    .is_err());
    assert!(shutdown_reply(Err(anyhow::anyhow!("unrelated failure")), false).is_err());
}
