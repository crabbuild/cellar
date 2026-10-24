//! Poll only a fresh read while proof failure asynchronously resets admission.
use anyhow::{ensure, Context, Result};
use serde_json::Value;
use std::time::Duration;

fn transient(status: reqwest::StatusCode, body: &str) -> bool {
    // The native fixture leaves read errors on its ordinary application error
    // channel. Its HTTP ingress can therefore use 500 or 503 for the same typed
    // RPC refusal. Arbitrary 5xx errors do not establish transient admission.
    status.is_server_error()
        && ["Cellar RPC Unavailable (", "Cellar RPC DeadlineExceeded ("]
            .iter()
            .any(|message| body.contains(message))
}

pub(crate) async fn read(client: &reqwest::Client, url: &str, target: &str) -> Result<Value> {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let response = client
                .get(format!("{url}/rust/read?target={target}"))
                .send()
                .await?;
            let status = response.status();
            let text = response.text().await?;
            if status.is_success() {
                return Ok::<_, anyhow::Error>(serde_json::from_str(&text)?);
            }
            ensure!(
                transient(status, &text),
                "recovery read failed outside a transient RPC admission refusal: {status}: {text}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .context("proof recovery did not reopen read admission")?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_named_rpc_admission_failures_can_be_polled() {
        for status in [500, 503] {
            assert!(transient(
                reqwest::StatusCode::from_u16(status).unwrap(),
                "Worker failed: Cellar RPC Unavailable (Unknown)"
            ));
        }
        assert!(transient(
            reqwest::StatusCode::GATEWAY_TIMEOUT,
            "Worker failed: Cellar RPC DeadlineExceeded (Unknown)"
        ));
        assert!(!transient(
            reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            "Worker panicked"
        ));
        assert!(!transient(
            reqwest::StatusCode::BAD_REQUEST,
            "Cellar RPC Unavailable (Unknown)"
        ));
        assert!(!transient(
            reqwest::StatusCode::OK,
            "Cellar RPC Unavailable (Unknown)"
        ));
        assert!(!transient(
            reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            "Cellar RPC InvalidResponse (Unknown)"
        ));
    }
}
