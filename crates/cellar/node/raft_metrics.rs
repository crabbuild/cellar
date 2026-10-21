//! Generation-bound, authenticated fixed-cardinality local resource samples.
use super::*;

fn options(query: Option<&str>) -> Option<(String, bool)> {
    let query = query.filter(|query| query.len() <= 512)?;
    let url = reqwest::Url::parse(&format!("http://cellar.invalid/?{query}")).ok()?;
    let mut generation = None;
    let mut json = None;
    for (key, value) in url.query_pairs() {
        match key.as_ref() {
            "generation" if generation.is_none() && !value.is_empty() && value.len() <= 128 => {
                generation = Some(value.into_owned());
            }
            "format" if json.is_none() && value == "json" => json = Some(true),
            _ => return None,
        }
    }
    Some((generation?, json.unwrap_or(false)))
}

pub(super) async fn handle(app: &AppHandle, request: Request<Incoming>) -> HttpReply {
    if request.method() != hyper::Method::GET {
        return peer_response(response(
            StatusCode::METHOD_NOT_ALLOWED,
            "Raft metrics require GET",
        ));
    }
    let (generation, json) = match options(request.uri().query()) {
        Some(options) => options,
        None => {
            return peer_response(response(
                StatusCode::BAD_REQUEST,
                "invalid Raft metrics options",
            ))
        }
    };
    let method = request.method().clone();
    let path = request
        .uri()
        .path_and_query()
        .map_or(request.uri().path(), |path| path.as_str())
        .to_owned();
    let headers = request.headers().clone();
    let body = match collect_limited_body(request.into_body(), 0).await {
        Ok(body) => body,
        Err(error) => return peer_response(body_read_error("Raft metrics", error)),
    };
    if let Err(error) =
        app.peer_auth
            .verify(&method, &path, &headers, &body, app.peer_auth.source())
    {
        return peer_response(response(error.status(), error.message()));
    }
    let services = crate::asyncrt::services();
    if services.actor_process_generation() != Some(generation.as_str()) {
        return peer_response(response(
            StatusCode::CONFLICT,
            "node generation changed; refresh before continuing",
        ));
    }
    let snapshot = match services.raft_lifecycle().resource_usage() {
        Ok(Some(snapshot)) => snapshot,
        Ok(None) => {
            return peer_response(response(
                StatusCode::NOT_FOUND,
                "no Raft cell host registered",
            ))
        }
        Err(_) => {
            return peer_response(response(
                StatusCode::SERVICE_UNAVAILABLE,
                "Raft resource sample unavailable",
            ))
        }
    };
    let (content_type, body) = if json {
        (
            "application/json",
            serde_json::to_string(&snapshot).expect("fixed resource sample serializes"),
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
