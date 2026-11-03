// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Forced closure can arrive during the supervisor's current shutdown poll.
#![allow(clippy::disallowed_methods)]
use super::*;
use futures_util::FutureExt as _;

#[tokio::test]
async fn closed_shutdown_acceptor_releases_its_listener_without_panicking() {
    let domain = Arc::new(crate::asyncrt::ExecutionDomain::new(
        tokio::runtime::Handle::current(),
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (connections, mut received) = mpsc::channel(1);
    let closing = domain.clone();
    let outcome = std::panic::AssertUnwindSafe(domain.run(async move {
        // No await separates closure from admission. This reproduces closure
        // racing a supervisor poll which already passed its cancellation check.
        closing.close();
        #[cfg(not(cellar_internal_tests))]
        let _ = spawn_shutdown_acceptor(listener, HttpSurface::Internal, connections);
        #[cfg(cellar_internal_tests)]
        let _ = spawn_shutdown_acceptor(listener, HttpSurface::Internal, connections, false);
        assert!(matches!(
            received.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ));
        let rebound = std::net::TcpListener::bind(address).unwrap();
        drop(rebound);
    }))
    .catch_unwind()
    .await;
    domain.shutdown().await;
    assert!(outcome.is_ok(), "shutdown admission panicked after closure");
    assert!(domain.is_closed());
    assert_eq!(tokio::spawn(async { 7 }).await.unwrap(), 7);
}

#[tokio::test]
async fn open_shutdown_acceptor_delivers_its_owned_connection_and_joins() {
    let domain = Arc::new(crate::asyncrt::ExecutionDomain::new(
        tokio::runtime::Handle::current(),
    ));
    let outcome = domain
        .run(async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (connections, mut received) = mpsc::channel(1);
            #[cfg(not(cellar_internal_tests))]
            let _ = spawn_shutdown_acceptor(listener, HttpSurface::Internal, connections);
            #[cfg(cellar_internal_tests)]
            let _ = spawn_shutdown_acceptor(listener, HttpSurface::Internal, connections, false);
            let client = tokio::net::TcpStream::connect(address).await.unwrap();
            let connection = received.recv().await.unwrap();
            assert_eq!(connection.surface.name(), "internal");
            assert!(connection.received.send(()).is_ok());
            drop(connection.stream);
            drop(client);
        })
        .await;
    domain.shutdown().await;
    outcome.unwrap();
}
