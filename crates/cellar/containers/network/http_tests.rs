// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real HTTP framing over the production multiplexer; the helper wire peer is
//! deliberately synthetic. Published SDK/image qualification is separate.

use super::*;
use cellar_egress::{control, transport, HttpTarget};
use futures_util::FutureExt;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex,
};
use tokio::sync::Notify;

type Client = hyper::client::conn::http1::SendRequest<Full<Bytes>>;
type ClientTask = crate::asyncrt::TaskHandle<std::result::Result<(), hyper::Error>>;

#[derive(Default)]
struct Observed {
    bodies: Mutex<Vec<Vec<u8>>>,
    entered: Notify,
    dropped: AtomicUsize,
}

struct Handler {
    phase: &'static str,
    observed: Arc<Observed>,
}

struct Active(Arc<Observed>);

impl Drop for Active {
    fn drop(&mut self) {
        self.0.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

impl HttpHandler for Handler {
    fn dispatch(
        &self,
        request: crate::application::Request,
        mut guard: crate::http_streams::RequestBodyGuard,
        valid: Arc<dyn Fn() -> Result<()> + Send + Sync>,
    ) -> crate::application::HandlerFuture<HttpResponse> {
        let phase = self.phase;
        let observed = self.observed.clone();
        Box::pin(async move {
            valid()?;
            let _active = Active(observed.clone());
            observed.entered.notify_one();
            if phase == "pending" {
                std::future::pending::<()>().await;
            }
            assert_eq!(request.url, "http://route.test/message");
            assert!(!request
                .headers
                .iter()
                .any(|(name, _)| name == "connection" || name == "x-hop"));
            let mut bytes = Vec::new();
            match request.body {
                RequestBody::Bytes(body) => bytes.extend_from_slice(&body),
                RequestBody::Stream(id) => {
                    let mut body = guard.take_stream(id).map_err(anyhow::Error::msg)?;
                    while let Some(chunk) = body.next().await {
                        bytes.extend(chunk.map_err(anyhow::Error::msg)?);
                    }
                }
            }
            observed.bodies.lock().unwrap().push(bytes.clone());
            valid()?;
            Ok(HttpResponse {
                status: 200,
                body: phase.as_bytes().to_vec(),
                stream: Some(Box::pin(futures_util::stream::iter([Ok(bytes)]))),
                headers: vec![("x-route".into(), phase.into())],
                websocket: None,
                write_position: None,
                observed_position: None,
            })
        })
    }
}

struct Fixture {
    run: Arc<Run>,
    installer: control::Installer<Box<dyn HttpHandler>>,
    helper: Link,
    host: crate::asyncrt::TaskHandle<Result<()>>,
    driver: crate::asyncrt::TaskHandle<Result<()>>,
    control: crate::asyncrt::TaskHandle<Result<()>>,
    fallback: crate::asyncrt::TaskHandle<Result<()>>,
}

impl Fixture {
    async fn new() -> Result<Self> {
        let run = Run::new(1)?;
        let (left, right) = tokio::io::duplex(128 * 1024);
        let (host, mut incoming, host_driver) = transport::session(left, transport::Role::Host);
        let (helper, mut direct, helper_driver) =
            transport::session(right, transport::Role::Container);
        let (installer, routes, controller) =
            control::channel::<Box<dyn HttpHandler>>(host.clone(), run.session);
        *run.control.lock().unwrap() = Some(routes.clone());
        let serving = run.clone();
        let host = crate::asyncrt::spawn(async move {
            let protocol = async {
                let control = incoming.next().await.context("control missing")?;
                tokio::try_join!(
                    controller.run(control),
                    serve(incoming, serving, routes, host)
                )?;
                Ok::<_, anyhow::Error>(())
            };
            tokio::try_join!(host_driver.run(), protocol).map(|_| ())
        });
        let driver = crate::asyncrt::spawn(helper_driver.run());
        let control_link = helper.clone();
        let session = run.session;
        let control_run = run.clone();
        let control = crate::asyncrt::spawn(async move {
            let mut io = control_link.open().await?;
            wire::write_open(&mut io, &wire::Open::Control { session }).await?;
            loop {
                let command = match wire::read_control(&mut io).await {
                    Ok(command) => command,
                    Err(error)
                        if control_run.retired.load(Ordering::Acquire)
                            && error.downcast_ref::<io::Error>().is_some_and(|error| {
                                matches!(
                                    error.kind(),
                                    io::ErrorKind::UnexpectedEof
                                        | io::ErrorKind::ConnectionAborted
                                        | io::ErrorKind::ConnectionReset
                                        | io::ErrorKind::BrokenPipe
                                )
                            }) =>
                    {
                        return Ok(())
                    }
                    Err(error) => return Err(error),
                };
                let wire::Control::Install { revision, .. } = command else {
                    anyhow::bail!("unexpected helper control")
                };
                wire::write_control(&mut io, &wire::Control::Installed { revision }).await?;
            }
        });
        let fallback = crate::asyncrt::spawn(async move {
            while let Some(mut io) = direct.next().await {
                assert_eq!(
                    wire::read_open(&mut io).await?,
                    wire::Open::Direct {
                        host: "203.0.113.7".into(),
                        port: 80
                    }
                );
                wire::write_direct_reply(&mut io, &wire::DirectReply::Connected).await?;
                let service = hyper::service::service_fn(|_request| async {
                    Ok::<_, std::convert::Infallible>(
                        Response::builder()
                            .header("x-route", "direct")
                            .body(Full::new(Bytes::from_static(b"fallback")))
                            .unwrap(),
                    )
                });
                hyper::server::conn::http1::Builder::new()
                    .keep_alive(false)
                    .serve_connection(TokioIo::new(io), service)
                    .await?;
            }
            Ok::<_, anyhow::Error>(())
        });
        installer.ready().await?;
        run.ready.store(true, Ordering::Release);
        Ok(Self {
            run,
            installer,
            helper,
            host,
            driver,
            control,
            fallback,
        })
    }

    async fn install(&self, phase: &'static str, observed: Arc<Observed>) -> Result<()> {
        self.installer
            .install(
                HttpTarget::parse("route.test")?,
                Box::new(Handler { phase, observed }),
            )
            .await
    }

    async fn client(&self) -> Result<(Client, ClientTask)> {
        let mut io = self.helper.open().await?;
        wire::write_open(
            &mut io,
            &wire::Open::Intercept {
                destination: "203.0.113.7:80".parse()?,
            },
        )
        .await?;
        let (client, driver) = hyper::client::conn::http1::handshake(TokioIo::new(io)).await?;
        Ok((client, crate::asyncrt::spawn(driver)))
    }

    async fn close(mut self) -> Result<()> {
        if !self.run.retired.load(Ordering::Acquire) {
            assert!(
                (&mut self.control).now_or_never().is_none(),
                "helper protocol stopped before retirement"
            );
        }
        self.run.retire();
        self.fallback.abort();
        let _ = self.fallback.await;
        self.helper.close();
        self.control.await??;
        self.host.await??;
        self.driver.await??;
        Ok(())
    }
}

fn request(bytes: Vec<u8>) -> Request<Full<Bytes>> {
    Request::builder()
        .method("POST")
        .uri("/message")
        .header("host", "route.test")
        .header("connection", "x-hop")
        .header("x-hop", "private-transport-field")
        .body(Full::new(Bytes::from(bytes)))
        .unwrap()
}

#[tokio::test]
async fn existing_http_connections_use_new_handlers_and_preserve_streamed_bytes() -> Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(crate::asyncrt::timeout(Duration::from_secs(10), async {
            let fixture = Fixture::new().await?;
            let observed = Arc::new(Observed::default());
            let (mut client, driver) = fixture.client().await?;
            let first = client.send_request(request(Vec::new())).await?;
            assert_eq!(first.headers()["x-route"], "direct");
            assert_eq!(
                first.into_body().collect().await?.to_bytes(),
                b"fallback"[..]
            );
            let bytes = (0..96 * 1024)
                .map(|value| (value % 251) as u8)
                .collect::<Vec<_>>();
            for phase in ["one", "two"] {
                fixture.install(phase, observed.clone()).await?;
                let response = client.send_request(request(bytes.clone())).await?;
                assert_eq!(response.headers()["x-route"], phase);
                let actual = response.into_body().collect().await?.to_bytes();
                assert_eq!(&actual[..phase.len()], phase.as_bytes());
                assert_eq!(&actual[phase.len()..], bytes);
            }
            assert_eq!(observed.bodies.lock().unwrap().len(), 2);
            driver.abort();
            let _ = driver.await;
            fixture.close().await
        }))
        .await;
    domain.shutdown().await;
    result??
}

#[tokio::test]
async fn malformed_authority_refuses_before_handler_and_retirement_cancels_pending_requests(
) -> Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(crate::asyncrt::timeout(Duration::from_secs(10), async {
            let fixture = Fixture::new().await?;
            let observed = Arc::new(Observed::default());
            fixture.install("pending", observed.clone()).await?;
            let (mut client, driver) = fixture.client().await?;
            let mut invalid = request(Vec::new());
            invalid.headers_mut().insert(
                hyper::header::HOST,
                hyper::header::HeaderValue::from_static("route.test:81"),
            );
            let rejected = client.send_request(invalid).await?;
            assert_eq!(rejected.status(), hyper::StatusCode::BAD_GATEWAY);
            assert!(observed.bodies.lock().unwrap().is_empty());
            driver.abort();
            let _ = driver.await;
            let (mut client, driver) = fixture.client().await?;
            let request =
                crate::asyncrt::spawn(
                    async move { client.send_request(request(Vec::new())).await },
                );
            observed.entered.notified().await;
            fixture.run.retire();
            assert!(
                request.await?.is_err(),
                "retirement delivered a response from a pending handler"
            );
            driver.abort();
            let _ = driver.await;
            fixture.close().await?;
            assert_eq!(observed.dropped.load(Ordering::SeqCst), 1);
            Ok::<_, anyhow::Error>(())
        }))
        .await;
    domain.shutdown().await;
    result??
}
