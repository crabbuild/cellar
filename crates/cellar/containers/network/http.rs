// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! HTTP messages are routed independently, including on retained connections.

use super::Run;
use crate::{
    application::service::HttpHandler,
    engine_api::{HttpResponse, RequestBody},
};
use anyhow::{ensure, Context as _, Result};
use bytes::Bytes;
use cellar_egress::{
    control::Routes,
    transport::{Incoming as Connections, Link, Stream},
    wire, Destination, Host, Protocol,
};
use futures_util::{stream::FuturesUnordered, StreamExt};
use http_body_util::{combinators::UnsyncBoxBody, BodyExt, Full, StreamBody};
use hyper::{
    body::{Body, Frame, Incoming},
    Request, Response,
};
use hyper_util::rt::{TokioIo, TokioTimer};
use std::{
    future::Future,
    io,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::io::{AsyncBufReadExt as _, AsyncRead, AsyncWrite, BufReader};

const MAX_HEADERS: usize = 128;
const MAX_HEAD_BYTES: usize = 64 * 1024;
const MAX_URL_BYTES: usize = 8192;
const MAX_BUFFERED_RESPONSE: usize = 1024 * 1024;

type HttpBody = UnsyncBoxBody<Bytes, io::Error>;
type Connection = Pin<Box<dyn Future<Output = Result<()>> + Send>>;
type DirectConnection = Pin<Box<dyn Future<Output = std::result::Result<(), hyper::Error>> + Send>>;

pub(super) async fn serve(
    mut incoming: Connections,
    run: Arc<Run>,
    routes: Routes<Box<dyn HttpHandler>>,
    link: Link,
) -> Result<()> {
    let mut serving = FuturesUnordered::<Connection>::new();
    loop {
        crate::asyncrt::select_biased! { "consume completed HTTP connections while accepting bounded helper streams";
            done = serving.next(), if !serving.is_empty() => {
                if done.is_some_and(|result| result.is_err()) {
                    tracing::debug!("container outbound connection refused");
                }
            },
            stream = incoming.next() => {
                let Some(stream) = stream else {
                    serving.clear();
                    ensure!(run.retired.load(super::Ordering::Acquire), "network helper stopped accepting HTTP streams");
                    return Ok(());
                };
                serving.push(Box::pin(connection(stream, run.clone(), routes.clone(), link.clone())));
            },
        }
    }
}

async fn connection(
    mut stream: Stream,
    run: Arc<Run>,
    routes: Routes<Box<dyn HttpHandler>>,
    link: Link,
) -> Result<()> {
    let open =
        crate::asyncrt::timeout(Duration::from_secs(5), wire::read_open(&mut stream)).await??;
    let wire::Open::Intercept { destination } = open else {
        anyhow::bail!("invalid helper HTTP stream direction")
    };
    ensure!(run.active(), "container network has retired");
    let mut stream = BufReader::new(stream);
    let first = crate::asyncrt::timeout(Duration::from_secs(5), stream.fill_buf())
        .await??
        .first()
        .copied()
        .context("empty intercepted connection")?;
    if first == 22 {
        let tls = run
            .tls
            .get()
            .context("native HTTPS interception is not enabled")?;
        let hello = crate::asyncrt::timeout(
            Duration::from_secs(5),
            tokio_rustls::LazyConfigAcceptor::new(rustls::server::Acceptor::default(), stream),
        )
        .await??;
        let identity = match hello.client_hello().server_name() {
            Some(name) => Host::parse(name)?.as_str().to_owned(),
            None => destination.ip().to_string(),
        };
        let config = tls.authority.server(identity.clone()).await?;
        ensure!(run.active(), "TLS handshake belongs to a retired run");
        let mut stream =
            crate::asyncrt::timeout(Duration::from_secs(5), hello.into_stream(config)).await??;
        stream.get_mut().1.set_buffer_limit(Some(MAX_HEAD_BYTES));
        serve_http(
            stream,
            destination,
            Protocol::Https,
            Some(identity),
            run,
            routes,
            link,
        )
        .await
    } else {
        serve_http(stream, destination, Protocol::Http, None, run, routes, link).await
    }
}

async fn serve_http<I: AsyncRead + AsyncWrite + Send + Unpin + 'static>(
    stream: I,
    destination: SocketAddr,
    protocol: Protocol,
    identity: Option<String>,
    run: Arc<Run>,
    routes: Routes<Box<dyn HttpHandler>>,
    link: Link,
) -> Result<()> {
    let service = hyper::service::service_fn(move |request| {
        let (run, routes, link, identity) =
            (run.clone(), routes.clone(), link.clone(), identity.clone());
        async move {
            let response =
                match route(request, destination, protocol, identity, run, routes, link).await {
                    Ok(response) => response,
                    // Do not expose a gateway's error text or captured properties.
                    Err(_) => failure(),
                };
            Ok::<_, std::convert::Infallible>(response)
        }
    });
    hyper::server::conn::http1::Builder::new()
        .max_headers(MAX_HEADERS)
        .max_buf_size(MAX_HEAD_BYTES)
        .timer(TokioTimer::new())
        .header_read_timeout(Duration::from_secs(5))
        .serve_connection(TokioIo::new(stream), service)
        .await?;
    Ok(())
}

async fn route(
    request: Request<Incoming>,
    destination: SocketAddr,
    protocol: Protocol,
    identity: Option<String>,
    run: Arc<Run>,
    routes: Routes<Box<dyn HttpHandler>>,
    link: Link,
) -> Result<Response<HttpBody>> {
    ensure!(run.active(), "container network has retired");
    validate_head(&request)?;
    let authority = request
        .headers()
        .get(hyper::header::HOST)
        .map(|value| value.to_str())
        .transpose()?
        .map(str::to_owned)
        .unwrap_or_else(|| destination.to_string());
    let scheme = if protocol == Protocol::Https {
        "https"
    } else {
        "http"
    };
    let uri: hyper::Uri = format!("{scheme}://{authority}").parse()?;
    let authority = uri.authority().context("HTTP request has no authority")?;
    ensure!(
        authority.port_u16().unwrap_or(protocol.default_port()) == destination.port(),
        "HTTP authority port disagrees with the intercepted destination"
    );
    let host_name = authority.host();
    let host = if host_name
        .trim_matches(['[', ']'])
        .parse::<std::net::IpAddr>()
        .is_ok()
    {
        None
    } else {
        Some(Host::parse(host_name)?)
    };
    if let Some(identity) = &identity {
        let current = match &host {
            Some(host) => host.as_str().to_owned(),
            None => host_name
                .trim_matches(['[', ']'])
                .parse::<std::net::IpAddr>()?
                .to_string(),
        };
        ensure!(
            current == *identity,
            "HTTP authority disagrees with TLS identity"
        );
    }
    let selected = routes.resolve(&Destination {
        protocol,
        socket: destination,
        host: host.as_ref(),
    });
    let Some(handler) = selected else {
        let tls = identity.map(|identity| (identity, run.tls.get().unwrap().upstream.clone()));
        return direct(request, destination, link, tls).await;
    };
    let url = format!("{scheme}://{authority}{}", request.uri());
    ensure!(
        url.len() <= MAX_URL_BYTES,
        "HTTP URL exceeds the routing limit"
    );
    let (head, body) = request.into_parts();
    let headers = headers(&head.headers)?;
    let body =
        crate::http_streams::register_body_stream(Box::pin(body.into_data_stream().map(|chunk| {
            chunk
                .map(|bytes| bytes.to_vec())
                .map_err(|_| "container request body failed".into())
        })))
        .map_err(anyhow::Error::msg)?;
    let body = RequestBody::Stream(body);
    let guard = crate::http_streams::RequestBodyGuard::of(&body);
    let response = handler
        .dispatch(
            crate::application::Request {
                url,
                method: head.method.to_string(),
                headers,
                body,
                entrypoint: None,
            },
            guard,
            Arc::new(move || {
                ensure!(run.active(), "container network has retired");
                Ok(())
            }),
        )
        .await?;
    native_response(response)
}

fn validate_head(request: &Request<Incoming>) -> Result<()> {
    ensure!(
        request.method() != hyper::Method::CONNECT,
        "CONNECT is not an intercepted HTTP request"
    );
    ensure!(
        request.uri().scheme().is_none()
            && request.uri().authority().is_none()
            && request.uri().path().starts_with('/')
            && request.uri().to_string().len() <= MAX_URL_BYTES,
        "intercepted HTTP requires a bounded origin-form URL"
    );
    ensure!(
        request
            .headers()
            .get_all(hyper::header::HOST)
            .iter()
            .count()
            <= 1,
        "duplicate HTTP Host"
    );
    ensure!(
        !request.headers().contains_key(hyper::header::UPGRADE)
            && !request.headers().contains_key(hyper::header::TRAILER),
        "HTTP upgrades and request trailers are unsupported"
    );
    headers(request.headers())?;
    Ok(())
}

fn headers(input: &hyper::HeaderMap) -> Result<Vec<(String, String)>> {
    ensure!(input.len() <= MAX_HEADERS, "too many HTTP headers");
    let mut bytes = 0usize;
    let connection: Vec<_> = input
        .get_all(hyper::header::CONNECTION)
        .iter()
        .map(|value| value.to_str())
        .collect::<std::result::Result<Vec<_>, _>>()?
        .into_iter()
        .flat_map(|value| value.split(','))
        .map(|name| name.trim().to_ascii_lowercase())
        .collect();
    let mut output = Vec::new();
    for (name, value) in input {
        bytes = bytes
            .checked_add(name.as_str().len())
            .and_then(|bytes| bytes.checked_add(value.len()))
            .context("HTTP headers exceed the routing limit")?;
        ensure!(
            bytes <= MAX_HEAD_BYTES,
            "HTTP headers exceed the routing limit"
        );
        if matches!(
            name.as_str(),
            "connection"
                | "keep-alive"
                | "proxy-authenticate"
                | "proxy-authorization"
                | "te"
                | "trailer"
                | "transfer-encoding"
                | "upgrade"
        ) || connection.iter().any(|hop| hop == name.as_str())
        {
            continue;
        }
        output.push((name.to_string(), value.to_str()?.to_owned()));
    }
    Ok(output)
}

fn native_response(response: HttpResponse) -> Result<Response<HttpBody>> {
    ensure!(
        response.websocket.is_none() && response.status != 101,
        "gateway returned an unsupported upgrade"
    );
    ensure!(
        response.body.len() <= MAX_BUFFERED_RESPONSE,
        "gateway buffered response exceeds the routing limit"
    );
    ensure!(
        response.headers.len() <= MAX_HEADERS
            && response
                .headers
                .iter()
                .try_fold(0usize, |bytes, (name, value)| bytes
                    .checked_add(name.len())?
                    .checked_add(value.len()))
                .is_some_and(|bytes| bytes <= MAX_HEAD_BYTES),
        "gateway headers exceed the routing limit"
    );
    let mut head = hyper::HeaderMap::new();
    for (name, value) in response.headers {
        head.append(
            hyper::header::HeaderName::from_bytes(name.as_bytes())?,
            value.parse()?,
        );
    }
    let mut builder = Response::builder().status(response.status);
    for (name, value) in headers(&head)? {
        builder = builder.header(name, value);
    }
    let first =
        futures_util::stream::iter((!response.body.is_empty()).then_some(Ok(response.body)));
    let stream = first.chain(
        response
            .stream
            .unwrap_or_else(|| Box::pin(futures_util::stream::empty())),
    );
    let body = StreamBody::new(stream.map(|chunk| {
        chunk
            .map(Bytes::from)
            .map(Frame::data)
            .map_err(io::Error::other)
    }))
    .boxed_unsync();
    Ok(builder.body(body)?)
}

async fn direct(
    mut request: Request<Incoming>,
    destination: SocketAddr,
    link: Link,
    tls: Option<(String, Arc<rustls::ClientConfig>)>,
) -> Result<Response<HttpBody>> {
    let mut stream = link.open().await?;
    wire::write_open(
        &mut stream,
        &wire::Open::Direct {
            host: destination.ip().to_string(),
            port: destination.port(),
        },
    )
    .await?;
    ensure!(
        matches!(
            wire::read_direct_reply(&mut stream).await?,
            wire::DirectReply::Connected
        ),
        "direct helper connection refused"
    );
    let mut head = hyper::HeaderMap::new();
    for (name, value) in headers(request.headers())? {
        head.append(
            hyper::header::HeaderName::from_bytes(name.as_bytes())?,
            value.parse()?,
        );
    }
    *request.headers_mut() = head;

    trait Tunnel: AsyncRead + AsyncWrite + Send + Unpin {}

    impl<T: AsyncRead + AsyncWrite + Send + Unpin> Tunnel for T {}
    let stream: Box<dyn Tunnel> = if let Some((name, config)) = tls {
        let name = rustls::pki_types::ServerName::try_from(name)?;
        let mut stream = crate::asyncrt::timeout(
            Duration::from_secs(5),
            tokio_rustls::TlsConnector::from(config).connect(name, stream),
        )
        .await??;
        stream.get_mut().1.set_buffer_limit(Some(MAX_HEAD_BYTES));
        Box::new(stream)
    } else {
        Box::new(stream)
    };
    let (mut sender, connection) = hyper::client::conn::http1::Builder::new()
        .max_headers(MAX_HEADERS)
        .max_buf_size(MAX_HEAD_BYTES)
        .handshake(TokioIo::new(stream))
        .await?;
    let mut driver = Box::pin(connection);
    let mut finished = false;
    let response = {
        let response = sender.send_request(request);
        tokio::pin!(response);
        crate::asyncrt::select_biased! { "prefer an already received HTTP head over a later connection completion";
            response = &mut response => response?,
            result = &mut driver => {
                finished = true;
                // Driving the connection may have just made its head ready.
                let ready = futures_util::future::poll_fn(|cx| Poll::Ready(response.as_mut().poll(cx))).await;
                if let Poll::Ready(response) = ready { response? }
                else { result?; anyhow::bail!("direct helper HTTP connection ended before its response") }
            },
        }
    };
    let (mut head, body) = response.into_parts();
    ensure!(
        head.status != hyper::StatusCode::SWITCHING_PROTOCOLS,
        "direct helper returned an unsupported upgrade"
    );
    let mut filtered = hyper::HeaderMap::new();
    for (name, value) in headers(&head.headers)? {
        filtered.append(
            hyper::header::HeaderName::from_bytes(name.as_bytes())?,
            value.parse()?,
        );
    }
    head.headers = filtered;
    Ok(Response::from_parts(
        head,
        DrivenBody {
            incoming: body,
            driver: (!finished).then_some(driver),
            failed: false,
        }
        .boxed_unsync(),
    ))
}

struct DrivenBody {
    incoming: Incoming,
    driver: Option<DirectConnection>,
    failed: bool,
}

impl Body for DrivenBody {
    type Data = Bytes;
    type Error = io::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<std::result::Result<Frame<Bytes>, io::Error>>> {
        if let Poll::Ready(frame) = Pin::new(&mut self.incoming).poll_frame(cx) {
            if frame.is_none() {
                self.driver.take();
            }
            return Poll::Ready(frame.map(|result| result.map_err(io::Error::other)));
        }
        if let Some(driver) = &mut self.driver {
            if let Poll::Ready(result) = driver.as_mut().poll(cx) {
                self.failed = result.is_err();
                self.driver.take();
            }
        }
        match Pin::new(&mut self.incoming).poll_frame(cx) {
            Poll::Pending if self.failed => Poll::Ready(Some(Err(io::Error::other(
                "direct helper HTTP stream failed",
            )))),
            Poll::Pending => Poll::Pending,
            Poll::Ready(frame) => Poll::Ready(frame.map(|result| result.map_err(io::Error::other))),
        }
    }
}

fn failure() -> Response<HttpBody> {
    let mut response = Response::new(
        Full::new(Bytes::from_static(b"Container outbound request failed"))
            .map_err(|never| match never {})
            .boxed_unsync(),
    );
    *response.status_mut() = hyper::StatusCode::BAD_GATEWAY;
    response.headers_mut().insert(
        hyper::header::CONNECTION,
        hyper::header::HeaderValue::from_static("close"),
    );
    response
}

#[cfg(test)]
#[path = "http_tests.rs"]
mod tests;
