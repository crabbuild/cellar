// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use http_body_util::StreamBody;
use hyper::body::Frame;
use std::convert::Infallible;

#[tokio::test]
async fn tiny_fragments_use_one_dense_capacity_reservation() -> anyhow::Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    domain
        .run(async {
            let services = crate::asyncrt::services();
            let connection = services.admit_rpc_tunnel("peer").unwrap();
            let request = connection.request().unwrap();
            let frames =
                (0..100_000).map(|_| Ok::<_, Infallible>(Frame::data(Bytes::from_static(b"x"))));
            let result = receive(
                StreamBody::new(futures_util::stream::iter(frames)),
                100_000,
                &request,
            )
            .await;
            let bytes = result.ok().expect("fragmented request was refused");
            assert_eq!(bytes.len(), 100_000);
            assert!(bytes.iter().all(|byte| *byte == b'x'));
            assert!(
                services.inbound_rpc_usage().reserved_request_bytes <= 64 * 1024 + 4 * bytes.len()
            );
            drop(bytes);
            let native = request.ticket();
            let held = services.inbound_rpc_usage();
            drop(request);
            assert_eq!(services.inbound_rpc_usage(), held);
            drop(native);
            drop(connection);
            assert_eq!(services.inbound_rpc_usage(), Default::default());
        })
        .await?;
    domain.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn byte_saturation_refuses_before_the_next_buffer_growth() -> anyhow::Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    domain
        .services()
        .configure_runtime(crate::runtime_policy::RuntimePolicy {
            max_inbound_rpc_bytes: 128 * 1024,
            ..Default::default()
        })?;
    domain
        .run(async {
            let services = crate::asyncrt::services();
            let connection = services.admit_rpc_tunnel("peer").unwrap();
            let request = connection.request().unwrap();
            let frames =
                (0..100_000).map(|_| Ok::<_, Infallible>(Frame::data(Bytes::from_static(b"x"))));
            assert!(matches!(
                receive(
                    StreamBody::new(futures_util::stream::iter(frames)),
                    100_000,
                    &request
                )
                .await,
                Err(Error::Exhausted)
            ));
            assert_eq!(
                services.inbound_rpc_usage().reserved_request_bytes,
                128 * 1024
            );
            drop((request, connection));
            assert_eq!(services.inbound_rpc_usage(), Default::default());
        })
        .await?;
    domain.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn endless_empty_chunks_cannot_starve_the_receiving_deadline() -> anyhow::Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    domain
        .run(async {
            let services = crate::asyncrt::services();
            let connection = services.admit_rpc_tunnel("peer").unwrap();
            let request = connection.request().unwrap();
            let frames = futures_util::stream::repeat_with(|| {
                Ok::<_, Infallible>(Frame::data(Bytes::new()))
            });
            let deadline = crate::asyncrt::mono_ms() + 10;
            let result = super::super::rpc_deadline::receive(Some(deadline), async {
                Ok(receive(StreamBody::new(frames), 1_024, &request).await)
            })
            .await;
            assert_eq!(
                result
                    .err()
                    .unwrap()
                    .downcast_ref::<crate::application::rpc::Failure>()
                    .unwrap()
                    .code(),
                crate::application::rpc::Code::DeadlineExceeded
            );
            assert_eq!(
                services.inbound_rpc_usage().reserved_request_bytes,
                64 * 1024
            );
            drop((request, connection));
            assert_eq!(services.inbound_rpc_usage(), Default::default());
        })
        .await?;
    domain.shutdown().await;
    Ok(())
}
