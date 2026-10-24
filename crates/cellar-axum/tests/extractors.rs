// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use anyhow::{ensure, Result};
use axum::{routing::get, Router};
use cellar_axum::{CellContext, CurrentCell};
use tower::ServiceExt;

#[tokio::test]
async fn missing_invocation_context_is_a_rejection_in_an_ordinary_axum_router() -> Result<()> {
    for router in [
        Router::new().route("/", get(|_: CellContext| async { "invalid" })),
        Router::new().route("/", get(|_: CurrentCell| async { "invalid" })),
    ] {
        let response = router
            .oneshot(
                axum::http::Request::builder()
                    .uri("/")
                    .body(axum::body::Body::empty())?,
            )
            .await?;
        ensure!(
            response.status() == 500,
            "missing Cellar authority was accepted"
        );
    }
    Ok(())
}
