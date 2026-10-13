mod support;
use anyhow::{ensure, Result};
use axum::{
    body::Body,
    http::Request,
    middleware::{self, Next},
    response::Response,
    routing::get,
    Router,
};
use cellar_axum::{ApplicationBuilder, AxumCell, CellarService};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tower::ServiceExt;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outer_middleware_can_reject_before_admission_and_closed_clones_return_503() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let calls = Arc::new(AtomicUsize::new(0));
    let factory = calls.clone();
    let host = support::start(directory.path(), true, "", move || {
        let calls = factory.clone();
        ApplicationBuilder::new(Router::new().route(
            "/count",
            get(move || {
                let calls = calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    "ok"
                }
            }),
        ))
        .cell("Counter", |_, _| Ok(Arc::new(AxumCell::new(Router::new()))))?
        .build()
    })
    .await?;
    let service = CellarService::new(host.ingress.as_ref().unwrap().clone());
    let app = Router::new()
        .fallback_service(service.clone())
        .layer(middleware::from_fn(
            |request: Request<Body>, next: Next| async move {
                if request.headers().contains_key("authorization") {
                    next.run(request).await
                } else {
                    Response::builder().status(401).body(Body::empty()).unwrap()
                }
            },
        ));
    ensure!(
        app.clone()
            .oneshot(Request::builder().uri("/count").body(Body::empty())?)
            .await?
            .status()
            == 401
    );
    ensure!(calls.load(Ordering::SeqCst) == 0);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/count")
                .header("authorization", "test")
                .body(Body::empty())?,
        )
        .await?;
    ensure!(response.status() == 200 && calls.load(Ordering::SeqCst) == 1);
    drop(response);
    host.stop().await?;
    let response = service
        .oneshot(Request::builder().uri("/count").body(Body::empty())?)
        .await?;
    ensure!(response.status() == 503 && response.headers()["retry-after"] == "1");
    Ok(())
}
