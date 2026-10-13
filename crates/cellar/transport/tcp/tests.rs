use super::*;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use tokio::io::{AsyncReadExt, ReadBuf};

// Process-runner tests share their fallback domain; keep competing removals
// out of the reentrant destructor assertion. Async cases share a Tokio lock; the sync
// destructor probe takes its blocking form outside a runtime.
static TRANSPORT_TESTS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct CheckedTransport {
    inner: tokio::io::DuplexStream,
    drops: Arc<AtomicUsize>,
}

impl Drop for CheckedTransport {
    fn drop(&mut self) {
        assert!(
            registry().try_lock().is_ok(),
            "transport dropped under the registry lock"
        );
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

impl AsyncRead for CheckedTransport {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for CheckedTransport {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

// This peer probe uses its own test runtime outside the execution domain.
#[allow(clippy::disallowed_methods)]
#[tokio::test]
async fn default_invocation_cleanup_closes_transport_and_allows_reentrant_drop() {
    let _serial = TRANSPORT_TESTS.lock().await;
    let (drops, id, mut peer) = {
        let drops = Arc::new(AtomicUsize::new(0));
        let id = next_id();
        let (transport, peer) = tokio::io::duplex(8);
        insert(
            id,
            Box::new(CheckedTransport {
                inner: transport,
                drops: drops.clone(),
            }),
        );
        let context = crate::host_io::IoContext::new();
        context.tcp_sockets.lock().unwrap().push(id);
        drop(context);
        (drops, id, peer)
    };
    let mut byte = [0];
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(1), peer.read(&mut byte))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(!registry().lock().unwrap().contains_key(&id));
    free(id);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn replaced_transport_is_released_outside_the_registry_lock() {
    let _serial = TRANSPORT_TESTS.blocking_lock();
    let drops = Arc::new(AtomicUsize::new(0));
    let id = next_id();
    let (first, _peer) = tokio::io::duplex(8);
    insert(
        id,
        Box::new(CheckedTransport {
            inner: first,
            drops: drops.clone(),
        }),
    );
    let (second, _peer) = tokio::io::duplex(8);
    insert(
        id,
        Box::new(CheckedTransport {
            inner: second,
            drops: drops.clone(),
        }),
    );
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    free(id);
    assert_eq!(drops.load(Ordering::SeqCst), 2);
}

fn acceptor() -> tokio_rustls::TlsAcceptor {
    let certificate =
        rustls::pki_types::CertificateDer::from(include_bytes!("fixtures/server.der").to_vec());
    let key =
        rustls::pki_types::PrivatePkcs8KeyDer::from(include_bytes!("fixtures/key.der").to_vec());
    tokio_rustls::TlsAcceptor::from(Arc::new(
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![certificate], key.into())
            .unwrap(),
    ))
}

fn args(port: u16, secure: bool, host: Option<&str>) -> ConnectArgs {
    ConnectArgs {
        hostname: "127.0.0.1".into(),
        port,
        secure,
        expected_host: host.map(str::to_owned),
    }
}

// Called by the direct native driver test, which owns the library process's
// one host runtime. This uses the public SDK through real native event turns.
#[allow(clippy::disallowed_methods)]
pub(crate) async fn native_tls_application<F, Fut>(mut run: F) -> anyhow::Result<()>
where
    F: FnMut(crate::application::Registry) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<()>>,
{
    use crate::application::{sockets::*, Invocation, Output, Registry};
    let _serial = TRANSPORT_TESTS.lock().await;

    struct Reset;

    impl Drop for Reset {
        fn drop(&mut self) {
            *test_root().lock().unwrap() = None;
        }
    }
    let _reset = Reset;
    *test_root().lock().unwrap() = Some(include_bytes!("fixtures/ca.der").to_vec());
    for (starttls, truncated) in [(false, false), (true, false), (false, true)] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await?;
            let mut stream = acceptor().accept(stream).await?;
            let mut bytes = [0; 3];
            stream.read_exact(&mut bytes).await?;
            assert_eq!(&bytes, b"tls");
            if truncated {
                // TCP EOF without TLS close_notify must error all queued reads.
                return Ok::<_, anyhow::Error>(());
            }
            stream.write_all(b"yes").await?;
            stream.shutdown().await?;
            Ok::<_, anyhow::Error>(())
        });
        let application = Registry::new(move |context, invocation| {
            Box::pin(async move {
                assert!(matches!(invocation, Invocation::Fetch(_)));
                let socket = context.connect(
                    SocketAddress::new("localhost", port)?,
                    SocketOptions {
                        secure_transport: if starttls {
                            SecureTransport::StartTls
                        } else {
                            SecureTransport::On
                        },
                        ..SocketOptions::default()
                    },
                )?;
                let info = socket.opened().await?;
                assert!(info.remote_address.is_some());
                let socket = if starttls {
                    let upgraded = socket.start_tls(&context, Some("localhost".into()))?;
                    assert!(socket.start_tls(&context, None).is_err());
                    let old_reader = socket.readable.get_reader()?;
                    assert!(old_reader.read(&context).await?.is_empty());
                    let old_writer = socket.writable.get_writer()?;
                    assert!(old_writer.write(&context, b"old".to_vec()).await.is_err());
                    assert!(upgraded.opened().await?.remote_address.is_none());
                    socket.closed().await?;
                    upgraded
                } else {
                    socket
                };
                assert!(socket.start_tls(&context, None).is_err());
                let writer = socket.writable.get_writer()?;
                writer.write(&context, b"tls".to_vec()).await?;
                writer.release_lock();
                let reader = socket.readable.get_reader()?;
                if truncated {
                    let first = reader.read(&context);
                    let second = reader.read(&context);
                    let error = second.await.unwrap_err().to_string();
                    assert!(error.contains("read failed"), "{error}");
                    assert_eq!(first.await.unwrap_err().to_string(), error);
                    assert_eq!(reader.closed().await.unwrap_err().to_string(), error);
                    assert_eq!(socket.closed().await.unwrap_err().to_string(), error);
                    reader.release_lock();
                    let next = socket.readable.get_reader()?;
                    assert_eq!(next.read(&context).await.unwrap_err().to_string(), error);
                    return Ok(Output::Http(crate::engine_api::HttpResponse {
                        status: 200,
                        body: b"yes".to_vec(),
                        stream: None,
                        headers: Vec::new(),
                        websocket: None,
                        write_position: None,
                        observed_position: None,
                    }));
                }
                let mut bytes = Vec::new();
                let mut buffer = [0; 2];
                loop {
                    let size = reader.read_into(&context, &mut buffer).await?;
                    if size == 0 {
                        break;
                    }
                    bytes.extend_from_slice(&buffer[..size]);
                }
                reader.closed().await?;
                socket.closed().await?;
                assert_eq!(bytes, b"yes");
                Ok(Output::Http(crate::engine_api::HttpResponse {
                    status: 200,
                    body: bytes,
                    stream: None,
                    headers: Vec::new(),
                    websocket: None,
                    write_position: None,
                    observed_position: None,
                }))
            })
        });
        run(application).await?;
        server.await??;
    }
    Ok(())
}

// Test peers use their own runtime rather than the application's execution domain.
#[allow(clippy::disallowed_methods)]
#[tokio::test]
async fn transport_reservations_tls_starttls_and_cancellation() {
    let _serial = TRANSPORT_TESTS.lock().await;
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let id = next_id();
        let owner = reserve(id);
        free(id);
        let (transport, mut peer) = tokio::io::duplex(8);
        assert!(publish(id, &owner, Box::new(transport)).is_err());
        assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
        assert_eq!(owner.closed().await, Ok(()));
        assert!(connect(id, args(1, false, None), EgressGate::NoCell)
            .await
            .is_err());

        let id = next_id();
        let owner = reserve(id);
        let (transport, mut peer) = tokio::io::duplex(8);
        publish(id, &owner, Box::new(transport)).unwrap();
        let mut pending = Box::pin(read(id, 1));
        assert!(futures_util::poll!(pending.as_mut()).is_pending());
        free(id);
        assert!(pending.await.is_err());
        assert_eq!(owner.closed().await, Ok(()));
        assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);

        // Refusal before a TLS handshake still consumes and settles plaintext.
        let old = next_id();
        let original = reserve(old);
        let (transport, mut peer) = tokio::io::duplex(8);
        publish(old, &original, Box::new(transport)).unwrap();
        let new = next_id();
        let upgraded = reserve(new);
        let error = start_tls(
            old,
            new,
            "localhost".into(),
            EgressGate::Unpersisted("upgrade proof failed".into()),
        )
        .await
        .unwrap_err();
        assert!(error.contains("upgrade proof failed"));
        assert_eq!(original.closed().await, Err(error.clone()));
        assert_eq!(upgraded.closed().await, Err(error));
        assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);

        // Cancellation of the new reservation before polling the upgrade also
        // releases the original transport rather than stranding a detached handle.
        let old = next_id();
        let original = reserve(old);
        let (transport, mut peer) = tokio::io::duplex(8);
        publish(old, &original, Box::new(transport)).unwrap();
        let new = next_id();
        reserve(new);
        free(new);
        assert!(start_tls(old, new, "localhost".into(), EgressGate::NoCell)
            .await
            .is_err());
        assert!(original.closed().await.unwrap_err().contains("cancelled"));
        assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);

        // A failed durability proof must prevent even the TCP dial.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let id = next_id();
        let owner = reserve(id);
        let error = connect(
            id,
            args(listener.local_addr().unwrap().port(), false, None),
            EgressGate::Unpersisted("test proof failed".into()),
        )
        .await
        .unwrap_err();
        assert!(error.contains("test proof failed"));
        assert_eq!(owner.closed().await, Err(error));
        assert!(futures_util::poll!(Box::pin(listener.accept()).as_mut()).is_pending());

        // Dropping an opening TLS future must close its actual network peer.
        let id = next_id();
        let owner = reserve(id);
        let port = listener.local_addr().unwrap().port();
        let opening = tokio::spawn(connect(
            id,
            args(port, true, Some("localhost")),
            EgressGate::NoCell,
        ));
        let (mut peer, _) = listener.accept().await.unwrap();
        let mut hello = [0; 4096];
        assert!(peer.read(&mut hello).await.unwrap() > 0);
        opening.abort();
        assert!(opening.await.unwrap_err().is_cancelled());
        assert!(owner.closed().await.unwrap_err().contains("cancelled"));
        peer.read_to_end(&mut Vec::new()).await.unwrap();
        assert!(!registry().lock().unwrap().contains_key(&id));

        if let Ok(listener) = tokio::net::TcpListener::bind("[::1]:0").await {
            let port = listener.local_addr().unwrap().port();
            let peer = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                stream.write_all(b"ipv6").await.unwrap();
            });
            let id = next_id();
            reserve(id);
            let mut request = args(port, false, None);
            request.hostname = "[::1]".into();
            assert!(connect(id, request, EgressGate::NoCell)
                .await
                .unwrap()
                .remote_address
                .unwrap()
                .starts_with("[::1]:"));
            assert_eq!(read(id, 4).await.unwrap(), b"ipv6");
            free(id);
            peer.await.unwrap();
        }

        *test_root().lock().unwrap() = Some(include_bytes!("fixtures/ca.der").to_vec());
        for starttls in [false, true] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                if starttls {
                    let mut plain = [0; 6];
                    stream.read_exact(&mut plain).await.unwrap();
                    assert_eq!(&plain, b"START\n");
                    stream.write_all(b"OK\n").await.unwrap();
                }
                let mut stream = acceptor().accept(stream).await.unwrap();
                let mut bytes = [0; 3];
                stream.read_exact(&mut bytes).await.unwrap();
                assert_eq!(&bytes, b"tls");
                stream.write_all(b"yes").await.unwrap();
                stream.shutdown().await.unwrap();
            });
            let id = next_id();
            let original = reserve(id);
            let info = connect(
                id,
                args(port, !starttls, Some("localhost")),
                EgressGate::NoCell,
            )
            .await
            .unwrap();
            assert!(info.local_address.is_some());
            assert!(info.remote_address.is_some());
            let id = if starttls {
                write(id, b"START\n".to_vec(), EgressGate::NoCell)
                    .await
                    .unwrap();
                assert_eq!(read(id, 3).await.unwrap(), b"OK\n");
                // A parked plaintext read must release its half for the handshake.
                let mut pending = Box::pin(read(id, 1));
                assert!(futures_util::poll!(pending.as_mut()).is_pending());
                let upgraded = next_id();
                reserve(upgraded);
                let (old_read, info) = tokio::join!(
                    pending,
                    start_tls(id, upgraded, "localhost".into(), EgressGate::NoCell)
                );
                assert!(old_read.is_err());
                let info = info.unwrap();
                assert_eq!(info.id, upgraded);
                assert!(info.remote_address.is_none());
                assert_eq!(original.closed().await, Ok(()));
                assert!(write(id, vec![1], EgressGate::NoCell).await.is_err());
                upgraded
            } else {
                id
            };
            write(id, b"tls".to_vec(), EgressGate::NoCell)
                .await
                .unwrap();
            let mut bytes = Vec::new();
            loop {
                let chunk = read(id, 2).await.unwrap();
                if chunk.is_empty() {
                    break;
                }
                bytes.extend(chunk);
            }
            assert_eq!(bytes, b"yes");
            free(id);
            server.await.unwrap();
        }

        // Both ordinary TLS and STARTTLS verify the requested server identity.
        for starttls in [false, true] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.unwrap();
                assert!(acceptor().accept(stream).await.is_err());
            });
            let id = next_id();
            let original = reserve(id);
            let error = if starttls {
                connect(id, args(port, false, None), EgressGate::NoCell)
                    .await
                    .unwrap();
                let new_id = next_id();
                let upgraded = reserve(new_id);
                let error = start_tls(id, new_id, "wrong.invalid".into(), EgressGate::NoCell)
                    .await
                    .unwrap_err();
                assert_eq!(upgraded.closed().await, Err(error.clone()));
                error
            } else {
                connect(
                    id,
                    args(port, true, Some("wrong.invalid")),
                    EgressGate::NoCell,
                )
                .await
                .unwrap_err()
            };
            assert!(error.contains("TLS handshake failed"), "{error}");
            assert_eq!(original.closed().await, Err(error));
            server.await.unwrap();
        }

        *test_root().lock().unwrap() = None;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            assert!(acceptor().accept(stream).await.is_err());
        });
        let id = next_id();
        reserve(id);
        assert!(
            connect(id, args(port, true, Some("localhost")), EgressGate::NoCell)
                .await
                .unwrap_err()
                .contains("TLS handshake failed")
        );
        server.await.unwrap();

        // Event cleanup during an upgrade settles both lifetimes and drops the TLS peer.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let old_id = next_id();
        let original = reserve(old_id);
        let new_id = next_id();
        let upgraded = reserve(new_id);
        connect(
            old_id,
            args(listener.local_addr().unwrap().port(), false, None),
            EgressGate::NoCell,
        )
        .await
        .unwrap();
        let (mut peer, _) = listener.accept().await.unwrap();
        let upgrade = tokio::spawn(start_tls(
            old_id,
            new_id,
            "localhost".into(),
            EgressGate::NoCell,
        ));
        assert!(peer.read(&mut [0; 4096]).await.unwrap() > 0);
        free(new_id);
        assert!(upgrade.await.unwrap().is_err());
        assert!(original.closed().await.unwrap_err().contains("cancelled"));
        assert_eq!(upgraded.closed().await, Ok(()));
        peer.read_to_end(&mut Vec::new()).await.unwrap();
        assert!(!registry().lock().unwrap().contains_key(&old_id));
    })
    .await
    .unwrap();
}

#[cfg(not(cellar_internal_tests))]
#[tokio::test]
async fn transport_cleanup_and_domain_shutdown_keep_colliding_socket_ids_separate() {
    use crate::asyncrt::ExecutionDomain;
    let first = ExecutionDomain::new(tokio::runtime::Handle::current());
    let second = ExecutionDomain::new(tokio::runtime::Handle::current());
    let first_service = first.services().tcp();
    let second_service = second.services().tcp();
    let first_id = first_service.next_id();
    let second_id = second_service.next_id();
    assert_eq!(first_id, second_id);
    let first_lifetime = reserve_in(&first_service, first_id);
    let second_lifetime = reserve_in(&second_service, second_id);
    let (first_transport, mut first_peer) = tokio::io::duplex(8);
    let (second_transport, mut second_peer) = tokio::io::duplex(8);
    publish_in(
        &first_service,
        first_id,
        &first_lifetime,
        Box::new(first_transport),
    )
    .unwrap();
    publish_in(
        &second_service,
        second_id,
        &second_lifetime,
        Box::new(second_transport),
    )
    .unwrap();
    let first_context = first
        .run(async {
            let context = crate::host_io::IoContext::new();
            context.tcp_sockets.lock().unwrap().push(first_id);
            context
        })
        .await
        .unwrap();
    second.run(async { drop(first_context) }).await.unwrap();
    assert_eq!(first_lifetime.closed().await, Ok(()));
    assert_eq!(first_peer.read(&mut [0]).await.unwrap(), 0);
    assert!(second_service
        .registry
        .lock()
        .unwrap()
        .contains_key(&second_id));
    second
        .run(async {
            write_in(
                &second_service,
                second_id,
                b"B".to_vec(),
                EgressGate::NoCell,
            )
            .await
            .unwrap();
        })
        .await
        .unwrap();
    assert_eq!(second_peer.read(&mut [0]).await.unwrap(), 1);
    // A retained transport with no live request is closed by domain shutdown.
    let retained_id = first_service.next_id();
    let retained = reserve_in(&first_service, retained_id);
    let (transport, mut peer) = tokio::io::duplex(8);
    publish_in(&first_service, retained_id, &retained, Box::new(transport)).unwrap();
    first.shutdown().await;
    assert_eq!(
        retained.closed().await,
        Err("execution domain is closed".into())
    );
    assert_eq!(peer.read(&mut [0]).await.unwrap(), 0);
    let refused = reserve_in(&first_service, first_service.next_id());
    assert!(refused.closed().await.is_err());
    assert!(first_service.registry.lock().unwrap().is_empty());
    assert!(second_service
        .registry
        .lock()
        .unwrap()
        .contains_key(&second_id));
    second.shutdown().await;
    assert!(second_lifetime.closed().await.is_err());
}

#[cfg(not(cellar_internal_tests))]
#[tokio::test]
async fn pending_tls_opening_dropped_in_another_domain_closes_only_its_reserved_socket() {
    use crate::asyncrt::ExecutionDomain;
    let first = ExecutionDomain::new(tokio::runtime::Handle::current());
    let second = ExecutionDomain::new(tokio::runtime::Handle::current());
    let first_service = first.services().tcp();
    let second_service = second.services().tcp();
    let first_id = first_service.next_id();
    let second_id = second_service.next_id();
    assert_eq!(first_id, second_id);
    let first_lifetime = reserve_in(&first_service, first_id);
    let second_lifetime = reserve_in(&second_service, second_id);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut opening = Box::pin(connect_in(
        first_service.clone(),
        first_id,
        args(
            listener.local_addr().unwrap().port(),
            true,
            Some("localhost"),
        ),
        EgressGate::NoCell,
    ));
    first
        .run(async {
            assert!(futures_util::poll!(opening.as_mut()).is_pending());
        })
        .await
        .unwrap();
    let (mut peer, _) = listener.accept().await.unwrap();
    first.run(async {
        crate::asyncrt::timeout(std::time::Duration::from_secs(2), async {
            let mut hello = [0];
            crate::asyncrt::select_biased! { "drive the TLS client until its real peer observes the handshake";
                result = opening.as_mut() => panic!("TLS opening unexpectedly settled: {result:?}"),
                result = peer.read_exact(&mut hello) => { result.unwrap(); },
            }
        }).await.unwrap();
    }).await.unwrap();

    second.run(async { drop(opening) }).await.unwrap();
    assert_eq!(
        first_lifetime.closed().await,
        Err("socket opening was cancelled".into())
    );
    let mut bytes = Vec::new();
    peer.read_to_end(&mut bytes).await.unwrap();
    assert!(second_service
        .registry
        .lock()
        .unwrap()
        .contains_key(&second_id));
    first.shutdown().await;
    second.shutdown().await;
    assert!(second_lifetime.closed().await.is_err());
}
