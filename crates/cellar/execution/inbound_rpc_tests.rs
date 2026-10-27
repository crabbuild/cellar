// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

#[tokio::test]
async fn last_alias_drops_use_the_receiving_domains_clock() -> anyhow::Result<()> {
    let owner = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let (connection, ticket, before) = owner
        .run(async {
            let connection = crate::asyncrt::services().admit_rpc_tunnel("peer").unwrap();
            let request = connection.request().unwrap();
            let ticket = request.ticket();
            drop(request);
            crate::asyncrt::sleep(Duration::from_millis(20)).await;
            (connection, ticket, crate::asyncrt::mono_ms())
        })
        .await?;
    let foreign = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    foreign.run(async { drop(ticket) }).await?;
    assert!(
        connection.0.touched.load(Ordering::SeqCst) >= before,
        "dropping a reply alias installed another execution domain's idle timestamp"
    );
    drop(connection);
    assert_eq!(owner.services().inbound_rpc_usage(), Usage::default());
    foreign.shutdown().await;
    owner.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn last_alias_drops_after_the_receiving_domain_stops() -> anyhow::Result<()> {
    let owner = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let ticket = owner
        .run(async {
            let connection = crate::asyncrt::services().admit_rpc_tunnel("peer").unwrap();
            connection.request().unwrap().ticket()
        })
        .await?;
    owner.shutdown().await;
    assert_eq!(owner.services().inbound_rpc_usage().active_requests, 1);
    drop(ticket);
    assert_eq!(owner.services().inbound_rpc_usage(), Usage::default());
    Ok(())
}

#[tokio::test]
async fn quotas_follow_the_last_native_owner_and_remove_peer_entries() -> anyhow::Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    domain.services().configure_runtime(RuntimePolicy {
        max_inbound_rpc_requests: 1,
        max_inbound_rpc_bytes: 128 * 1024,
        max_inbound_rpc_tunnels: 2,
        max_rpc_tunnels_per_peer: 1,
        ..Default::default()
    })?;
    domain
        .run(async {
            let services = crate::asyncrt::services();
            let first = services.admit_rpc_tunnel("first").unwrap();
            assert!(services.admit_rpc_tunnel("first").is_none());
            let second = services.admit_rpc_tunnel("second").unwrap();
            assert!(services.admit_rpc_tunnel("third").is_none());
            let request = first.request().unwrap();
            request.buffer(32 * 1024).unwrap();
            let held = services.inbound_rpc_usage();
            assert_eq!(held.reserved_request_bytes, 128 * 1024);
            assert!(request.buffer(32 * 1024 + 1).is_none());
            assert!(second.request().is_none());
            assert_eq!(services.inbound_rpc_usage(), held);
            let native = request.ticket();
            drop((request, first));
            assert_eq!(services.inbound_rpc_usage(), held);
            drop(native);
            assert_eq!(
                services.inbound_rpc_usage(),
                Usage {
                    open_tunnels: 1,
                    peers: 1,
                    ..Default::default()
                }
            );
            assert!(second.request().is_some());
            drop(second);
            assert_eq!(services.inbound_rpc_usage(), Usage::default());
        })
        .await?;
    domain.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn active_work_prevents_idle_retirement_until_its_last_owner_drops() -> anyhow::Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    domain.services().configure_runtime(RuntimePolicy {
        rpc_tunnel_idle_timeout: Duration::from_millis(20),
        ..Default::default()
    })?;
    domain
        .run(async {
            let connection = crate::asyncrt::services().admit_rpc_tunnel("peer").unwrap();
            let request = connection.request().unwrap();
            assert!(
                crate::asyncrt::timeout(Duration::from_millis(50), connection.idle())
                    .await
                    .is_err()
            );
            drop(request);
            crate::asyncrt::timeout(Duration::from_secs(1), connection.idle()).await?;
            Ok::<_, anyhow::Error>(())
        })
        .await??;
    domain.shutdown().await;
    Ok(())
}
