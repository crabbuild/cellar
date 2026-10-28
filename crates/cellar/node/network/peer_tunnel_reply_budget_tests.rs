// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Published reply payloads must share the receiving host's byte admission.
use super::*;
use http_body_util::StreamBody;
use hyper::body::Frame;

fn policy() -> crate::runtime_policy::RuntimePolicy {
    crate::runtime_policy::RuntimePolicy {
        max_inbound_rpc_bytes: 128 * 1024,
        ..Default::default()
    }
}

#[tokio::test]
async fn rpc_reply_bytes_refuse_exhausted_host_capacity_before_publication() -> anyhow::Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    domain.services().configure_runtime(policy())?;
    let result = domain
        .run(async {
            let services = crate::asyncrt::services();
            let connection = services.admit_rpc_tunnel("peer").unwrap();
            let request = connection.request().unwrap();
            // The dense input plus its possible native copy and control allowance
            // already own all 128 KiB. Even a one-byte reply needs admission.
            request.buffer(32 * 1024).unwrap();
            let reply = RetainedRpcBody::new(full_body(Bytes::from_static(b"x")), request, None);
            anyhow::ensure!(
                matches!(reply, Err(crate::application::rpc::Code::ResourceExhausted)),
                "published business bytes after the host byte budget was exhausted"
            );
            drop(connection);
            anyhow::ensure!(
                services.inbound_rpc_usage() == Default::default(),
                "refused reply retained admission"
            );
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn rpc_reply_bytes_follow_the_last_transport_slice() -> anyhow::Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    domain.services().configure_runtime(policy())?;
    let result = domain
        .run(async {
            let services = crate::asyncrt::services();
            let first = services.admit_rpc_tunnel("first").unwrap();
            let second = services.admit_rpc_tunnel("second").unwrap();
            let request = first.request().unwrap();
            // Control owns half the budget; this reply occupies the other half.
            let mut body =
                RetainedRpcBody::new(full_body(Bytes::from(vec![b'x'; 64 * 1024])), request, None)
                    .unwrap();
            let data = body.frame().await.unwrap()?.into_data().unwrap();
            let last = data.slice(0..1);
            drop((data, body, first));
            anyhow::ensure!(
                second.request().is_none(),
                "a transport alias released its reply byte reservation"
            );
            drop(last);
            let next = second
                .request()
                .context("last reply alias did not release host capacity")?;
            drop((next, second));
            anyhow::ensure!(
                services.inbound_rpc_usage() == Default::default(),
                "reply alias cleanup retained admission"
            );
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn rpc_reply_bytes_bound_frames_without_a_declared_length() -> anyhow::Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    domain.services().configure_runtime(policy())?;
    let result = domain
        .run(async {
            let services = crate::asyncrt::services();
            let connection = services.admit_rpc_tunnel("peer").unwrap();
            let request = connection.request().unwrap();
            let frames = [32 * 1024, 32 * 1024, 1]
                .map(|size| Ok::<_, std::io::Error>(Frame::data(Bytes::from(vec![b'x'; size]))));
            let source = StreamBody::new(futures_util::stream::iter(frames)).boxed_unsync();
            anyhow::ensure!(
                source.size_hint().exact().is_none(),
                "fixture unexpectedly declared a body length"
            );
            let mut body = RetainedRpcBody::new(source, request, None).unwrap();
            for _ in 0..2 {
                let frame = body
                    .frame()
                    .await
                    .context("missing admitted reply frame")??;
                anyhow::ensure!(frame.data_ref().unwrap().len() == 32 * 1024);
                drop(frame);
            }
            anyhow::ensure!(
                matches!(body.frame().await, Some(Err(_))),
                "undeclared reply frames bypassed the shared host byte budget"
            );
            drop((body, connection));
            anyhow::ensure!(
                services.inbound_rpc_usage() == Default::default(),
                "failed reply stream retained admission"
            );
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn rpc_reply_bytes_refuse_new_frames_after_the_owner_stops() -> anyhow::Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let mut body = domain
        .run(async {
            let connection = crate::asyncrt::services().admit_rpc_tunnel("peer").unwrap();
            let request = connection.request().unwrap();
            RetainedRpcBody::new(full_body(Bytes::from_static(b"unpublished")), request, None)
                .unwrap()
        })
        .await?;
    let services = domain.services();
    domain.shutdown().await;
    // No ambient execution domain is installed here. Payload admission must
    // use the original weak owner, just like final alias refunds do.
    anyhow::ensure!(
        matches!(body.frame().await, Some(Err(_))),
        "a stopped owner published a new business frame"
    );
    drop(body);
    anyhow::ensure!(
        services.inbound_rpc_usage() == Default::default(),
        "stopped reply retained admission"
    );
    Ok(())
}

struct UnderreportedBody(UnsyncBoxBody<Bytes, std::io::Error>);

impl Body for UnderreportedBody {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        std::pin::Pin::new(&mut self.0).poll_frame(cx)
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        hyper::body::SizeHint::with_exact(0)
    }
}

#[tokio::test]
async fn rpc_reply_bytes_check_actual_frames_when_the_hint_is_inaccurate() -> anyhow::Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    domain.services().configure_runtime(policy())?;
    let result = domain
        .run(async {
            let services = crate::asyncrt::services();
            let connection = services.admit_rpc_tunnel("peer").unwrap();
            let request = connection.request().unwrap();
            let source =
                UnderreportedBody(full_body(Bytes::from(vec![b'x'; 64 * 1024 + 1]))).boxed_unsync();
            let mut body = RetainedRpcBody::new(source, request, None).unwrap();
            anyhow::ensure!(
                matches!(body.frame().await, Some(Err(_))),
                "an inaccurate reply length bypassed admission"
            );
            drop((body, connection));
            anyhow::ensure!(services.inbound_rpc_usage() == Default::default());
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn rpc_reply_bytes_return_unknown_capacity_refusal_over_the_real_tunnel() -> anyhow::Result<()>
{
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    domain.services().configure_runtime(policy())?;
    let result = domain
        .run(async {
            let (mut app, mut routes) = super::super::capacity_tests::app("peer")?;
            app.operation_deadline_ms = 1_000;
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let address = listener.local_addr()?.to_string();
            let server = crate::asyncrt::spawn(async move {
                while let Ok((socket, _)) = listener.accept().await {
                    let app = app.clone();
                    crate::asyncrt::spawn(async move {
                        let service = service_fn(move |request| {
                            let reply = accept(request, app.clone());
                            async move { Ok::<_, Infallible>(reply) }
                        });
                        let _ = hyper::server::conn::http1::Builder::new()
                            .serve_connection(TokioIo::new(socket), service)
                            .with_upgrades()
                            .await;
                    })
                    .detach();
                }
            });
            let http = reqwest::Client::builder().no_proxy().build()?;
            let auth = PeerAuth::new([0; 32], "origin")?;
            let peer = Peer {
                http: &http,
                auth: &auth,
                addr: &address,
                node: "peer",
            };
            let key = tunnel_key(&peer, "rpc");
            let result = async {
                // The wire upload exactly fills dense input/copy/control capacity.
                // The runtime-free fixture's normal refusal still needs reply bytes;
                // capacity failure must retain its unknown effect outcome on the wire.
                let args = crate::engine_api::RpcData::Structured(Bytes::from(vec![0; 32 * 1024]));
                let reply = crate::asyncrt::timeout(
                    std::time::Duration::from_secs(2),
                    rpc(
                        &peer,
                        "Counter:reply-limit",
                        None,
                        "example.counter.v1.Counter/GetCount",
                        &args,
                        RpcControl {
                            reservation: None,
                            request_id: None,
                            authority: None,
                            deadline_ms: None,
                        },
                    ),
                )
                .await??;
                let failure = rpc_failure::remote(reply.status(), reply.headers())
                    .context("missing fixed receiving quota failure")?;
                anyhow::ensure!(failure.code() == crate::application::rpc::Code::ResourceExhausted);
                anyhow::ensure!(
                    failure.outcome() == crate::application::rpc::EffectOutcome::Unknown
                );
                reply.into_body().collect().await?;
                anyhow::ensure!(
                    matches!(
                        routes.try_recv(),
                        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
                    ),
                    "runtime-free fixture unexpectedly entered native routing"
                );
                Ok::<_, anyhow::Error>(())
            }
            .await;
            drop(checkout(&key));
            server.abort();
            let _ = server.await;
            result
        })
        .await?;
    domain.shutdown().await;
    anyhow::ensure!(
        domain.services().inbound_rpc_usage() == Default::default(),
        "wire refusal retained receiving admission after join"
    );
    result
}
