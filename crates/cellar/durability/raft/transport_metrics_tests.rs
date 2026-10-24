// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#![allow(clippy::disallowed_methods)]
use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn socket_counts_partial_vectored_progress_without_pending_or_failed_bytes(
) -> anyhow::Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let registry = domain
        .run(async { Registry::new(crate::asyncrt::services().domain_token().unwrap()) })
        .await?;
    for (lane, label) in [
        (Lane::Control, "control"),
        (Lane::Data, "data"),
        (Lane::Query, "query"),
    ] {
        let (local, mut remote) = tokio::io::duplex(3);
        let mut local = Socket::new(local, registry.clone(), lane);
        let bytes = |suffix| {
            registry
                .snapshot()
                .counters
                .into_iter()
                .find(|counter| counter.name == format!("transport_{label}_{suffix}_bytes"))
                .unwrap()
                .value
        };
        assert!(local.is_write_vectored());
        assert_eq!(
            local
                .write_vectored(&[IoSlice::new(b"ab"), IoSlice::new(b"cdef")])
                .await?,
            3
        );
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), local.write_all(b"d"))
                .await
                .is_err()
        );
        assert_eq!(bytes("write"), 3);
        let mut delivered = [0; 3];
        remote.read_exact(&mut delivered).await?;
        assert_eq!(&delivered, b"abc");
        assert_eq!(remote.write(b"12345").await?, 3);
        let mut response = [0; 8];
        let mut buffer = tokio::io::ReadBuf::new(&mut response);
        buffer.put_slice(b"prefix");
        futures_util::future::poll_fn(|cx| Pin::new(&mut local).poll_read(cx, &mut buffer)).await?;
        assert_eq!(buffer.filled(), b"prefix12");
        // Count only newly filled bytes, excluding the caller's existing prefix.
        assert_eq!(bytes("read"), 2);
        let mut last = [0];
        local.read_exact(&mut last).await?;
        assert_eq!(last, [b'3']);
        assert_eq!(bytes("read"), 3);
        drop(remote);
        assert!(local.write_all(b"failed").await.is_err());
        assert_eq!(bytes("write"), 3);
    }
    domain.shutdown().await;
    Ok(())
}
