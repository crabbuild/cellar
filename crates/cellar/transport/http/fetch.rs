// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Adapted for Cellar from Celld: crates/celld/js.rs.
//! Direct HTTP transport shared by application backends.
use crate::engine_api::{HttpResponse, RequestBody};
use crate::host_gates::{await_egress_gate, EgressGate};
use crate::host_io::{IoContext, RedirectSubrequestLimit};
use crate::http_streams::{
    http_stream_service, HttpStreamService, HttpStreamSource, RequestBodyGuard,
    HTTP_STREAM_REGISTRATION_CLOSED,
};
use std::sync::Arc;

fn fetch_request_error(error: &reqwest::Error) -> String {
    let mut source: &(dyn std::error::Error + 'static) = error;
    loop {
        if let Some(limit) = source.downcast_ref::<RedirectSubrequestLimit>() {
            return limit.0.clone();
        }
        let Some(next) = source.source() else {
            return format!("fetch: {error}");
        };
        source = next;
    }
}

pub(crate) struct ResponseHead {
    pub(crate) status: u16,
    pub(crate) headers: Vec<(String, String)>,
    source: Option<(Arc<HttpStreamService>, u64)>,
}

impl ResponseHead {
    #[allow(dead_code)]
    pub(crate) fn into_json(mut self) -> String {
        let (_, id) = self.source.take().expect("registered response source");
        serde_json::json!({"status":self.status,"streamId":id,"headers":self.headers}).to_string()
    }

    pub(crate) fn into_http(mut self) -> Result<HttpResponse, String> {
        let (service, id) = self.source.as_ref().expect("registered response source");
        let claim = service
            .claim(*id)
            .ok_or("fetch: response stream is no longer registered")?;
        let stream = RequestBodyGuard::transferred(claim).take_stream(*id)?;
        self.source.take();
        Ok(HttpResponse {
            status: self.status,
            headers: std::mem::take(&mut self.headers),
            body: Vec::new(),
            stream: Some(stream),
            websocket: None,
            write_position: None,
            observed_position: None,
        })
    }
}

impl Drop for ResponseHead {
    fn drop(&mut self) {
        if let Some((service, id)) = self.source.take() {
            service.cancel_source(id);
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn fetch(
    context: Arc<IoContext>,
    method: String,
    url: String,
    body: Option<RequestBody>,
    mut headers: Vec<(String, String)>,
    redirect: &str,
    mut body_guard: RequestBodyGuard,
    gate: EgressGate,
    cancel: Option<tokio::sync::oneshot::Receiver<()>>,
    trace: Option<crate::telemetry::TraceContext>,
) -> impl std::future::Future<Output = Result<ResponseHead, String>> + Send + 'static {
    // Select the client only for direct egress: a broker must not initialize
    // an unused network client and its TLS stack.
    let client = match redirect {
        "follow" if context.has_subrequest_limit() => context.redirect_counting_client(),
        "follow" | "manual" | "error" => context.host_services().http().client(redirect),
        _ => unreachable!("the redirect mode was validated above"),
    };
    let fetch_timeout = context.host_services().http().policy.request_timeout;
    // The child's ids are minted before the request leaves so the
    // traceparent header carries them: whatever this fetch reaches can
    // join the trace cellar is part of.
    let child = trace.as_ref().map(crate::telemetry::child_context);
    if let Some(child) = child.as_ref() {
        if !headers
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case("traceparent"))
        {
            headers.push(("traceparent".into(), crate::telemetry::traceparent(child)));
        }
    }
    let recording = trace.and_then(crate::telemetry::TraceContext::recording_ids);
    let child_recording = child.and_then(crate::telemetry::TraceContext::recording_ids);
    let span_url = recording.map(|_| url.clone());
    let stream_service = http_stream_service();
    async move {
        let span_started = recording.map(|_| crate::telemetry::now_unix_us());
        let mut span = recording.zip(child_recording).map(|(parent, child)| {
            let mut span =
                crate::telemetry::Span::new(child, "fetch", crate::telemetry::KIND_CLIENT);
            span.parent_span_id = Some(parent.span_id);
            span.parent_remote = Some(false);
            span.url = span_url;
            span
        });
        let mut finish = |ok: bool, status: Option<u16>, error: Option<String>| {
            if let Some(mut span) = span.take() {
                span.start_unix_us = span_started.unwrap_or_default();
                span.duration_us = crate::telemetry::now_unix_us() - span.start_unix_us;
                span.ok = ok;
                span.http_status = status;
                span.error = error;
                crate::telemetry::record(span);
            }
        };
        await_egress_gate(gate).await.inspect_err(|error| {
            finish(false, None, Some(error.clone()));
        })?;
        let client = client.inspect_err(|error| finish(false, None, Some(error.clone())))?;
        let m = reqwest::Method::from_bytes(method.as_bytes()).unwrap_or(reqwest::Method::GET);
        // a timeout bounds a black-hole host: the future settles (Err) instead
        // of parking run_loop forever — the hang the Drop guard can't catch.
        let mut rb = client.request(m, &url).timeout(fetch_timeout);
        // reqwest omits Content-Length for an empty Vec, which collapses the
        // wire representation of `Some([])` into the representation of
        // `None`. Install the zero length here unless the Worker supplied a
        // framing header, so the body distinction survives the HTTP client.
        let empty_body_needs_length = body
            .as_ref()
            .is_some_and(|body| matches!(body, RequestBody::Bytes(bytes) if bytes.is_empty()))
            && !headers.iter().any(|(name, _)| {
                name.eq_ignore_ascii_case("content-length")
                    || name.eq_ignore_ascii_case("transfer-encoding")
            });
        for (name, value) in headers {
            rb = rb.header(name, value);
        }
        if let Some(body) = body {
            rb = match body {
                RequestBody::Bytes(bytes) => rb.body(bytes),
                RequestBody::Stream(stream_id) => match body_guard.take_stream(stream_id) {
                    Ok(stream) => rb.body(reqwest::Body::wrap_stream(stream)),
                    Err(error) => {
                        finish(false, None, Some(error.clone()));
                        return Err(error);
                    }
                },
            };
        }
        if empty_body_needs_length {
            rb = rb.header(reqwest::header::CONTENT_LENGTH, 0);
        }
        // The cancellable window is the request itself: connect, send, and wait
        // for the response head. Dropping the send future here closes the
        // connection, which is what tells the upstream to stop producing. After
        // this point the response body is a registered stream, and
        // `__http_stream_cancel` is what ends it.
        let sent = match cancel {
            Some(cancel) => crate::asyncrt::select_biased! {
                "a cancel that is ready wins a tie, so an aborted request never \
                 hands back a response and never leaves a body stream nobody reads";
                _ = cancel => None,
                result = rb.send() => Some(result),
            },
            None => Some(rb.send().await),
        };
        let Some(sent) = sent else {
            let error = "fetch: the request was aborted".to_string();
            finish(false, None, Some(error.clone()));
            return Err(error);
        };
        match sent {
            Ok(resp) => {
                let status = resp.status().as_u16();
                let headers = resp
                    .headers()
                    .iter()
                    .map(|(name, value)| {
                        (
                            name.as_str().to_string(),
                            // A header value is decoded as UTF-8 with
                            // replacement, as workerd decodes it.
                            String::from_utf8_lossy(value.as_bytes()).into_owned(),
                        )
                    })
                    .collect::<Vec<_>>();
                let Some(stream_id) =
                    stream_service.register_source(HttpStreamSource::Response(resp))
                else {
                    let error = format!("fetch: {HTTP_STREAM_REGISTRATION_CLOSED}");
                    finish(false, Some(status), Some(error.clone()));
                    return Err(error);
                };
                finish(true, Some(status), None);
                Ok(ResponseHead {
                    status,
                    headers,
                    source: Some((stream_service, stream_id)),
                })
            }
            Err(e) => {
                let error = fetch_request_error(&e);
                finish(false, None, Some(error.clone()));
                Err(error)
            }
        }
    }
}
