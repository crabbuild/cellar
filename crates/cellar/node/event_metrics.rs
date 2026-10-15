//! Fleet-authenticated host observations; no resource identities or payloads.

use super::*;

pub(super) async fn handle(app: &AppHandle, request: Request<Incoming>) -> HttpReply {
    if request.method() != hyper::Method::GET {
        return peer_response(response(
            StatusCode::METHOD_NOT_ALLOWED,
            "event metrics require GET",
        ));
    }
    let json = match request.uri().query() {
        None => false,
        Some("format=json") => true,
        _ => {
            return peer_response(response(
                StatusCode::BAD_REQUEST,
                "invalid event metrics format",
            ))
        }
    };
    let method = request.method().clone();
    let path = request
        .uri()
        .path_and_query()
        .expect("metrics path")
        .as_str()
        .to_owned();
    let headers = request.headers().clone();
    let body = match collect_limited_body(request.into_body(), 0).await {
        Ok(body) => body,
        Err(error) => return peer_response(body_read_error("event metrics", error)),
    };
    if let Err(error) =
        app.peer_auth
            .verify(&method, &path, &headers, &body, app.peer_auth.source())
    {
        return peer_response(response(error.status(), error.message()));
    }
    let Some(runtime) = &app.runtime else {
        return peer_response(response(
            StatusCode::SERVICE_UNAVAILABLE,
            "event metrics require a runtime",
        ));
    };
    let snapshot = runtime.host_services().event_metrics();
    let (content_type, body) = if json {
        (
            "application/json",
            serde_json::to_string(&snapshot).expect("fixed metrics serialize"),
        )
    } else {
        (
            "text/plain; version=0.0.4; charset=utf-8",
            snapshot.prometheus(),
        )
    };
    peer_response(
        Response::builder()
            .status(StatusCode::OK)
            .header(hyper::header::CONTENT_TYPE, content_type)
            .body(
                Full::new(Bytes::from(body))
                    .map_err(|never| match never {})
                    .boxed_unsync(),
            )
            .expect("metrics response"),
    )
}
