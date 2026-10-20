//! Real TLS transport and three child processes. No SQLite/S3 qualification.
#![allow(clippy::disallowed_methods)]
#[path = "support/raft_network.rs"]
mod network;

use anyhow::{ensure, Result};
use cellar::application::consensus::verification::*;
use cellar_logic::consensus::{GroupId, IncarnationId, ProcessGeneration};
use network::*;
use raft::prelude::{Entry, Message, MessageType};
use raft::Storage;
use serde_json::json;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

fn heartbeat() -> Message {
    let mut m = Message {
        from: 1,
        to: 2,
        term: 1,
        ..Default::default()
    };
    m.set_msg_type(MessageType::MsgHeartbeat);
    m
}

fn append(bytes: usize) -> Message {
    let mut m = heartbeat();
    m.set_msg_type(MessageType::MsgAppend);
    m.index = 1;
    m.log_term = 1;
    let entry = Entry {
        index: 2,
        term: 1,
        data: vec![9; bytes].into(),
        ..Default::default()
    };
    m.entries.push(entry);
    m
}

fn addresses(a: &Host, b: &Host) -> Vec<(std::net::SocketAddr, std::net::SocketAddr)> {
    vec![
        a.addresses(),
        b.addresses(),
        (
            "127.0.0.1:1".parse().unwrap(),
            "127.0.0.1:2".parse().unwrap(),
        ),
    ]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peer_response_authenticated() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let a = Host::new(
        &dir.path().join("a"),
        1,
        &[scope(10)],
        TransportOptions::default(),
    )
    .await?;
    let fake = TcpListener::bind("127.0.0.1:0").await?;
    let fake_address = fake.local_addr()?;
    let attack = tokio::spawn(async move {
        let (tcp, _) = fake.accept().await?;
        let mut tls = server_tls()?.accept(tcp).await?;
        let mut prefix = [0; 20];
        tls.read_exact(&mut prefix).await?;
        let header = u32::from_be_bytes(prefix[8..12].try_into()?) as usize;
        let body = u64::from_be_bytes(prefix[12..].try_into()?) as usize;
        let mut request = vec![0; header + 32 + body];
        tls.read_exact(&mut request).await?;
        let mut response = [0; 20];
        response[..4].copy_from_slice(b"CLRA");
        response[4..8].copy_from_slice(&1_u32.to_be_bytes());
        response[8..12].copy_from_slice(&2_u32.to_be_bytes());
        tls.write_all(&response).await?;
        tls.write_all(b"{}").await?;
        tls.write_all(&[0; 32]).await?;
        tls.flush().await?;
        Ok::<_, anyhow::Error>(())
    });
    let peers = vec![
        a.addresses(),
        (fake_address, "127.0.0.1:2".parse()?),
        ("127.0.0.1:3".parse()?, "127.0.0.1:4".parse()?),
    ];
    a.transport.install_group(route(&scope(10), &peers))?;
    let before = a.log.group(&scope(10))?.initial_state()?.hard_state;
    assert_eq!(
        a.transport
            .submit(&scope(10), heartbeat())?
            .accepted()
            .await,
        Err(TransportError::Authentication)
    );
    attack.await??;
    let after = a.log.group(&scope(10))?.initial_state()?.hard_state;
    assert_eq!(
        before, after,
        "forged transport acceptance cannot become Raft progress"
    );
    ensure!(a.transport.usage().failed == 1);
    a.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_group_no_allocation() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let a = Host::new(
        &dir.path().join("a"),
        1,
        &[scope(10)],
        TransportOptions::default(),
    )
    .await?;
    let b = Host::new(&dir.path().join("b"), 2, &[], TransportOptions::default()).await?;
    a.transport
        .install_group(route(&scope(10), &addresses(&a, &b)))?;
    assert_eq!(
        a.transport
            .submit(&scope(10), heartbeat())?
            .accepted()
            .await,
        Err(TransportError::UnknownGroup)
    );
    assert!(b.log.groups()?.is_empty());
    assert_eq!(b.transport.usage().groups, 0);
    assert_eq!(b.transport.usage().peers, 0);
    assert_eq!(b.transport.usage().data_bytes, 0);
    assert!(b
        .transport
        .install_group(route(&scope(10), &addresses(&a, &b)))
        .is_err());
    a.close().await?;
    b.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_incarnation_message() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let old = scope(10);
    let mut current = old.clone();
    current.group = GroupId::new([20; 16])?;
    current.incarnation = IncarnationId::new([44; 16])?;
    let a = Host::new(
        &dir.path().join("a"),
        1,
        &[old.clone(), current.clone()],
        TransportOptions::default(),
    )
    .await?;
    let b = Host::new(
        &dir.path().join("b"),
        2,
        &[current.clone()],
        TransportOptions::default(),
    )
    .await?;
    let peers = addresses(&a, &b);
    a.transport.install_group(route(&old, &peers))?;
    b.transport.install_group(route(&current, &peers))?;
    let before = b.log.group(&current)?.initial_state()?.hard_state;
    assert_eq!(
        a.transport.submit(&old, heartbeat())?.accepted().await,
        Err(TransportError::UnknownGroup)
    );
    // Remove the old view before installing a conflicting process descriptor.
    a.transport.remove_group(&old)?;
    let mut stale = route(&current, &peers);
    stale.peers[1].replica.process = ProcessGeneration::new([8; 16])?;
    a.transport.install_group(stale)?;
    assert_eq!(
        a.transport.submit(&current, heartbeat())?.accepted().await,
        Err(TransportError::StalePeer)
    );
    assert_eq!(b.log.groups()?.len(), 1);
    assert_eq!(b.log.group(&current)?.initial_state()?.hard_state, before);
    assert_eq!(b.transport.usage().accepted, 0);
    a.close().await?;
    b.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_traffic_reserved() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let options = TransportOptions {
        max_message_bytes: 64 * 1024,
        max_control_bytes: 4096,
        peer_data_bytes: 80 * 1024,
        node_data_bytes: 256 * 1024,
        io_timeout_ms: 30_000,
        ..Default::default()
    };
    let a = Host::new(&dir.path().join("a"), 1, &[scope(10)], options.clone()).await?;
    let b = Host::new(&dir.path().join("b"), 2, &[scope(10)], options).await?;
    let data_stall = TcpListener::bind("127.0.0.1:0").await?;
    let mut peers = addresses(&a, &b);
    peers[1].1 = data_stall.local_addr()?;
    a.transport.install_group(route(&scope(10), &peers))?;
    b.transport
        .install_group(route(&scope(10), &addresses(&a, &b)))?;
    let (entered, wait) = tokio::sync::oneshot::channel();
    let stall = tokio::spawn(async move {
        let (tcp, _) = data_stall.accept().await?;
        let mut tls = server_tls()?.accept(tcp).await?;
        let mut prefix = [0; 20];
        tls.read_exact(&mut prefix).await?;
        let _ = entered.send(());
        std::future::pending::<()>().await;
        Ok::<_, anyhow::Error>(())
    });
    let pending = a.transport.submit(&scope(10), append(60 * 1024))?;
    tokio::time::timeout(Duration::from_secs(5), wait).await??;
    assert!(matches!(
        a.transport.submit(&scope(10), append(60 * 1024)),
        Err(TransportError::Overloaded)
    ));
    let usage = a.transport.peer_usage(identity(&scope(10), 2).node)?;
    assert!(usage.data_bytes > 60 * 1024);
    assert_eq!(usage.data_items, 1);
    tokio::time::timeout(
        Duration::from_secs(2),
        a.transport.submit(&scope(10), heartbeat())?.accepted(),
    )
    .await??;
    assert_eq!(
        a.transport
            .peer_usage(identity(&scope(10), 2).node)?
            .data_items,
        1,
        "control delivery must not depend on draining bulk data"
    );
    tokio::time::timeout(Duration::from_secs(2), a.transport.close()).await?;
    assert!(matches!(
        pending.accepted().await,
        Err(TransportError::Closed)
    ));
    assert_eq!(a.transport.usage().data_bytes, 0);
    assert_eq!(
        a.transport
            .peer_usage(identity(&scope(10), 2).node)?
            .data_items,
        0
    );
    assert_eq!(a.transport.usage().tasks, 0);
    stall.abort();
    let _ = stall.await;
    a.close().await?;
    b.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transport_byte_limits() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let options = TransportOptions {
        max_message_bytes: 64 * 1024,
        max_control_bytes: 4096,
        ..Default::default()
    };
    let a = Host::new(&dir.path().join("a"), 1, &[scope(10)], options).await?;
    assert!(matches!(
        a.transport.submit(&scope(10), append(65 * 1024)),
        Err(TransportError::Limit)
    ));
    for header_length in [8193_u32, u32::MAX] {
        let tcp = tokio::net::TcpStream::connect(a.addresses().0).await?;
        let mut tls = client_tls()?.connect("localhost".try_into()?, tcp).await?;
        let mut prefix = [0; 20];
        prefix[..4].copy_from_slice(b"CLRP");
        prefix[4..8].copy_from_slice(&1_u32.to_be_bytes());
        prefix[8..12].copy_from_slice(&header_length.to_be_bytes());
        prefix[12..20].copy_from_slice(&u64::MAX.to_be_bytes());
        tls.write_all(&prefix).await?;
        tls.flush().await?;
        let mut response = [0; 1];
        let closed = tokio::time::timeout(Duration::from_secs(2), tls.read(&mut response)).await?;
        assert!(!matches!(closed, Ok(1..)));
    }
    assert_eq!(a.transport.usage().data_bytes, 0);
    assert_eq!(a.transport.usage().control_bytes, 0);
    assert_eq!(a.log.groups()?.len(), 1);
    a.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_way_partition() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let mut a = Process::start(&dir.path().join("a"), 1).await?;
    let mut b = Process::start(&dir.path().join("b"), 2).await?;
    let mut c = Process::start(&dir.path().join("c"), 3).await?;
    let addresses: Vec<_> = [a.addresses, b.addresses, c.addresses]
        .into_iter()
        .map(|(a, b)| json!([a.to_string(), b.to_string()]))
        .collect();
    for process in [&mut a, &mut b, &mut c] {
        process
            .rpc(json!({"op":"install","addresses":addresses}))
            .await?;
    }
    // One direction is cut: the majority cannot deliver to A, while A's vote
    // traffic can reach both majority processes through the production TLS path.
    b.rpc(json!({"op":"fault","blocked":[1],"duplicate":true}))
        .await?;
    c.rpc(json!({"op":"fault","blocked":[1],"duplicate":true}))
        .await?;
    a.rpc(json!({"op":"campaign","group":10})).await?;
    b.rpc(json!({"op":"campaign","group":10})).await?;
    b.ready(10).await?;
    c.rpc(json!({"op":"campaign","group":11})).await?;
    c.ready(11).await?;
    let p = b
        .rpc(json!({"op":"propose","group":10,"data":"majority survives one-way partition"}))
        .await?;
    let index = p["index"].as_u64().unwrap();
    b.committed(10, index).await?;
    c.committed(10, index).await?;
    let q = c
        .rpc(json!({"op":"propose","group":11,"data":"other leader on same pooled peers"}))
        .await?;
    let second = q["index"].as_u64().unwrap();
    c.committed(11, second).await?;
    b.committed(11, second).await?;
    assert_eq!(a.status(10).await?["committed"], 1);
    assert_eq!(a.status(11).await?["committed"], 1);
    assert_eq!(
        b.status(10).await?["peers"],
        2,
        "pools count physical peers, not groups"
    );
    // Heal and explicitly reorder held, duplicated Raft messages. Every retry
    // gets a fresh authenticated frame; raw signed-frame replay is forbidden.
    b.rpc(json!({"op":"fault","hold":true,"duplicate":true}))
        .await?;
    c.rpc(json!({"op":"fault","hold":true,"duplicate":true}))
        .await?;
    a.rpc(json!({"op":"tick"})).await?;
    b.rpc(json!({"op":"tick"})).await?;
    b.rpc(json!({"op":"tick"})).await?;
    c.rpc(json!({"op":"tick"})).await?;
    c.rpc(json!({"op":"tick"})).await?;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let released_b = b.rpc(json!({"op":"release"})).await?["released"]
        .as_u64()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    let release_c = c.rpc(json!({"op":"release"})).await?;
    let released_c = release_c["released"].as_u64().unwrap();
    ensure!(
        released_b + released_c > 0,
        "fixture must actually reorder held messages"
    );
    ensure!(
        release_c["responses"].as_u64().unwrap() > 0,
        "fixture must delay actual Raft responses"
    );
    for _ in 0..10 {
        b.rpc(json!({"op":"tick"})).await?;
        c.rpc(json!({"op":"tick"})).await?;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    a.committed(10, index).await?;
    a.committed(11, second).await?;
    a.close().await?;
    b.close().await?;
    c.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transport_owner_and_domain_shutdown() -> Result<()> {
    use cellar::peer_auth::PeerAuth;
    use std::sync::Arc;
    let dir = tempfile::tempdir()?;
    let a = Host::new(&dir.path().join("a"), 1, &[], TransportOptions::default()).await?;
    let duplicate = RaftTransport::start(
        a.runtime.clone(),
        Arc::new(PeerAuth::new([17; 32], "node-1")?),
        tls()?,
        TcpListener::bind("127.0.0.1:0").await?,
        TcpListener::bind("127.0.0.1:0").await?,
        TransportOptions::default(),
    )
    .await;
    assert!(matches!(
        duplicate,
        Err(TransportError::Runtime(RuntimeError::AlreadyAttached))
    ));
    a.transport.close().await;
    // Retaining a closed handle must not retain the runtime's transport claim.
    let replacement = RaftTransport::start(
        a.runtime.clone(),
        Arc::new(PeerAuth::new([17; 32], "node-1")?),
        tls()?,
        TcpListener::bind("127.0.0.1:0").await?,
        TcpListener::bind("127.0.0.1:0").await?,
        TransportOptions::default(),
    )
    .await?;
    let control = replacement.control_address();
    let data = replacement.data_address();
    tokio::time::timeout(Duration::from_secs(2), a.domain.shutdown()).await?;
    tokio::time::timeout(Duration::from_secs(2), replacement.close()).await?;
    assert_eq!(replacement.usage().tasks, 0);
    assert_eq!(replacement.usage().control_bytes, 0);
    assert_eq!(replacement.usage().data_bytes, 0);
    // Listeners were released before close's task count reached zero.
    let _control = TcpListener::bind(control).await?;
    let _data = TcpListener::bind(data).await?;
    assert!(matches!(a.log.groups(), Err(LogError::Closed)));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn protected_transport_refuses_wrong_hostname_and_plaintext() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let a = Host::new(
        &dir.path().join("a"),
        1,
        &[scope(10)],
        TransportOptions::default(),
    )
    .await?;
    let b = Host::new(
        &dir.path().join("b"),
        2,
        &[scope(10)],
        TransportOptions::default(),
    )
    .await?;
    let peers = addresses(&a, &b);
    let mut wrong_server = route(&scope(10), &peers);
    wrong_server.peers[1].server_name = "wrong.example".into();
    a.transport.install_group(wrong_server)?;
    b.transport.install_group(route(&scope(10), &peers))?;
    assert!(matches!(
        a.transport
            .submit(&scope(10), heartbeat())?
            .accepted()
            .await,
        Err(TransportError::Tls(_))
    ));
    let mut plaintext = tokio::net::TcpStream::connect(b.addresses().0).await?;
    plaintext
        .write_all(b"CLRPthis is not a TLS handshake")
        .await?;
    let mut response = [0; 32];
    if let Ok(length) =
        tokio::time::timeout(Duration::from_secs(2), plaintext.read(&mut response)).await?
    {
        if length > 0 {
            // A pre-handshake TLS rejection may emit a plaintext TLS alert
            // record. It must never emit a Raft frame or admit the message.
            assert_eq!(response[0], 21, "expected only a TLS alert");
            assert!(!response[..length].starts_with(b"CLRA"));
        }
    }
    assert_eq!(b.transport.usage().accepted, 0);
    assert_eq!(
        b.log.group(&scope(10))?.initial_state()?.hard_state.commit,
        1
    );
    a.close().await?;
    b.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_senders_preserve_connection_sequence() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let a = Host::new(
        &dir.path().join("a"),
        1,
        &[scope(10)],
        TransportOptions::default(),
    )
    .await?;
    let b = Host::new(
        &dir.path().join("b"),
        2,
        &[scope(10)],
        TransportOptions::default(),
    )
    .await?;
    let peers = addresses(&a, &b);
    a.transport.install_group(route(&scope(10), &peers))?;
    b.transport.install_group(route(&scope(10), &peers))?;
    let deliveries = std::thread::scope(|threads| {
        let senders: Vec<_> = (0..32)
            .map(|_| {
                let transport = a.transport.clone();
                threads.spawn(move || transport.submit(&scope(10), heartbeat()))
            })
            .collect();
        senders
            .into_iter()
            .map(|sender| sender.join().unwrap())
            .collect::<Result<Vec<_>, _>>()
    })?;
    for delivery in deliveries {
        tokio::time::timeout(Duration::from_secs(3), delivery.accepted()).await??;
    }
    assert_eq!(a.transport.usage().failed, 0);
    assert_eq!(a.transport.usage().control_bytes, 0);
    a.close().await?;
    b.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "child entry point; invoked by the parent process test"]
async fn raft_transport_process() -> Result<()> {
    process_main().await
}
